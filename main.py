import pandas as pd

pd.set_option('display.max_rows', None)
# I've made the assumption that the time difference between the call to the endpoint and the handle indicates the frontend speed
# I've also made the assumption that the time difference between the call to the handle and the respond indicates the backend speed

# Define the data frame columns
log_columns = ['datestamp','request_guid', 'action_type','requested_url','status','server_worker_id']

# Ignore the spacer columns
included_columns = [0,1,2,3,5,7]

# Read the log file into a data frame and sort by request_guid then datestamp
log_data_frame = pd.read_csv('sample.log',
                            delimiter=' ',
                            header = None,
                            names = log_columns,
                            usecols = included_columns,
                            parse_dates=['datestamp'],
                            ).sort_values(by=['request_guid','datestamp'])

# Calculate the duration of each operation by taking the difference between the current and previous datestamp
log_data_frame['duration'] = (log_data_frame
                              .groupby('request_guid')['datestamp']
                              .diff(periods=-1)
                              .dt.total_seconds()
                              .abs())

# Calculate the mean duration of all operations for each server/worker id
worker_data_frame = (log_data_frame
                    .where(log_data_frame['server_worker_id'].str.startswith('worker'))
                     .groupby('server_worker_id')['duration']
                     .mean()
                     .reset_index(name='mean_duration'))
                    #  .sort_values(by=['mean_duration'], ascending=False)
                    #  .reset_index(drop=True))

worker_mean_duration = worker_data_frame['mean_duration'].mean()

print(worker_data_frame['server_worker_id'].where(worker_data_frame['mean_duration'] > worker_mean_duration).dropna())

worker_data_frame = (log_data_frame
                    .where(log_data_frame['server_worker_id'].str.startswith('frontend'))
                     .groupby('server_worker_id')['duration']
                     .mean()
                     .reset_index(name='mean_duration'))
                    #  .sort_values(by=['mean_duration'], ascending=False)
                    #  .reset_index(drop=True))

worker_mean_duration = worker_data_frame['mean_duration'].mean()

print(worker_data_frame.where(worker_data_frame['mean_duration'] > worker_mean_duration).dropna())

print()

# mean_frontend_duration = server_data_frame.



# print (worker_data_frame.head(70))