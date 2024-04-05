import concurrent.futures
import os
import pandas as pd
from google.cloud import bigquery

key_path = ''
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
project_id = 'testtasks-419216'
client = bigquery.Client(project=project_id)

project_name = 'bigquery-public-data'
dataset_name = 'google_analytics_sample'
table_names = [
    'ga_sessions_20170101',
    'ga_sessions_20170201',
    'ga_sessions_20170301',
    'ga_sessions_20170401',
    'ga_sessions_20170501',
    'ga_sessions_20170601',
    'ga_sessions_20170701',
    'ga_sessions_20170801'
]

query_template = """
SELECT 
    *
FROM 
    `bigquery-public-data.google_analytics_sample.ga_sessions_{}`
"""

time_periods = [
    '20170101',
    '20170201',
    '20170301',
    '20170401',
    '20170501',
    '20170601',
    '20170701',
    '20170801'
]

dataframes = []


def fetch_data_from_table(time_period):
    query = query_template.format(time_period)
    df = client.query(query).to_dataframe()
    return df


def fetch_all_data():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_data_from_table, time_period) for time_period in time_periods]
        for future in concurrent.futures.as_completed(futures):
            dataframes.append(future.result())


if __name__ == "__main__":
    fetch_all_data()
    combined_df = pd.concat(dataframes)
