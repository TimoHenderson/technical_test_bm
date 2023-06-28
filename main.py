# I've made the assumption that the time difference between the request and HANDLE indicates the frontend speed
# I've also made the assumption that the time difference between HANDLE and RESPOND indicates the worker speed

import pandas as pd
import cProfile


def read_csv_and_calculate_durations(filename):
    # Define the data frame columns
    log_columns = ['datestamp','request_guid', 'action_type','requested_url','status','server_worker_id']

    # Ignore the spacer columns
    included_columns = [0,1,2,3,5,7]

    # Read the log file into a data frame
    log_data_frame = pd.read_csv(filename,
                                delimiter=' ',
                                header = None,
                                names = log_columns,
                                usecols = included_columns,
                                parse_dates=['datestamp'],
                                )

    # Calculate the duration of each operation by taking the difference between the current and previous datestamp
    log_data_frame['duration'] = (log_data_frame
                                .groupby('request_guid')['datestamp']
                                .diff(periods=-1)
                                .dt.total_seconds()
                                .abs())
    return log_data_frame


def find_culprits(data_frame, prefix):
    # Find the mean duration for each server_worker_id that starts with the prefix
    reduced_data_frame = (data_frame
                        .where(data_frame['server_worker_id'].str.startswith(prefix))
                        .groupby('server_worker_id')['duration']
                        .mean()
                        .reset_index()
                        .rename(columns={'duration': 'mean_duration'}))

    # Find the mean duration for all server_worker_ids 
    total_mean_duration = reduced_data_frame['mean_duration'].mean()

    # Find the server_worker_ids that have a mean duration greater than the total mean duration
    culprits = (reduced_data_frame
                .where(reduced_data_frame['mean_duration'] > total_mean_duration)
                .dropna())

    # Add a column to show how much longer the mean duration is than the total mean duration
    culprits['time_over_mean'] = culprits['mean_duration'] - total_mean_duration

    return culprits


def get_culprits(log_filename):
    log_data_frame = read_csv_and_calculate_durations(log_filename)
    frontend_culprits = find_culprits(log_data_frame, 'frontend')
    worker_culprits = find_culprits(log_data_frame, 'worker')
    return pd.concat([frontend_culprits, worker_culprits])
    
    
if __name__ == "__main__":
    print(get_culprits('sample.log'))