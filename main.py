import pandas as pd

log_columns = ['timestamp','guid', 'action','endpoint','statusCode','name']
included_columns = [0,1,2,3,5,7]
log_data_frame = pd.read_csv('sample.log',
                            delimiter=' ',
                            header = None,
                            names = log_columns,
                            usecols = included_columns,
                            parse_dates=['timestamp'],
                            ).sort_values(by=['guid','timestamp'])



print(log_data_frame.head(10))