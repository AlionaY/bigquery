import ast
import concurrent.futures
import os
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials

SHEET_NAME = 'Bigquery tables'
PROJECT_NAME = 'bigquery-public-data'
DATASET_NAME = 'google_analytics_sample'

# add path to file
key_path = ''
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
project_id = 'testtasks-419216'
bigquery_client = bigquery.Client(project=project_id)

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
google_client = gspread.authorize(credentials)

query_template = """
SELECT 
    *
FROM 
    `bigquery-public-data.google_analytics_sample.ga_sessions_{}`
"""

time_periods = []
dataframes = []


def fetch_all_data():
    generate_time_periods()
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(get_dataframe_from_table, time_period) for time_period in time_periods]
        for future in concurrent.futures.as_completed(futures):
            dataframes.append(future.result())

    return pd.concat(dataframes)


def generate_time_periods(start='20170101', end='20170331'):
    start_date = datetime.strptime(start, '%Y%m%d')
    end_date = datetime.strptime(end, '%Y%m%d')

    current_date = start_date
    while current_date <= end_date:
        formatted_date = current_date.strftime('%Y%m%d')
        time_periods.append(formatted_date)
        current_date += timedelta(days=1)


def get_dataframe_from_table(time_period):
    query = query_template.format(time_period)
    return bigquery_client.query(query).to_dataframe()


def convert_col_type_to_dict(cols):
    for col in cols:
        combined_df[col] = combined_df[col].apply(ast.literal_eval)


def write_to_google_sheet():
    df_list = get_df_list()
    num_workers = len(df_list)
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(update_sheet, get_worksheet(idx), df) for (idx, df) in enumerate(df_list)]
    if concurrent.futures.as_completed(futures):
        end_time = time.time()
        print(f'spent time for writing data: {end_time - start_time}')


def get_df_list():
    transactions_by_date_df = transactions_by_date(combined_df)
    traffic_by_date_df = traffic_by_date(combined_df)
    transactions_by_browser_df = transactions_by_browser(combined_df)
    transactions_by_traffic_source_df = transactions_by_traffic_source(combined_df)
    return [transactions_by_date_df, traffic_by_date_df, transactions_by_browser_df, transactions_by_traffic_source_df]


def extract_series_from_dict_param(df, dict_param, param):
    param_series = pd.Series(df[dict_param])
    transactions_values = param_series.apply(lambda x: x.get(param))
    return transactions_values


def transactions_by_date(data):
    df = data.copy()
    transactions_values = extract_series_from_dict_param(df, 'totals', 'transactions')
    df['total_transactions'] = transactions_values

    result = df.groupby('date')['total_transactions'].sum().reset_index()
    result = result.sort_values(by='total_transactions', ascending=False)
    return result


def traffic_by_date(data):
    """
    Determines how much traffic a website has received over a certain period of time
    :param data: dataframe with general data
    :return: a dataframe
    """
    df = data.copy()
    total_page_views = extract_series_from_dict_param(df, 'totals', 'pageviews')
    df['total_page_views'] = total_page_views

    result = df.groupby('date')['total_page_views'].sum().reset_index()
    result = result.sort_values(by='date')
    return result


def transactions_by_browser(data):
    """
    Determines which browsers on specific dates had the most transactions
    :param data: a dataframe with general data
    :return: a dataframe
    """
    df = data.copy()

    transactions_values = extract_series_from_dict_param(df, 'totals', 'transactions')
    df['total_transactions'] = transactions_values

    browser_values = extract_series_from_dict_param(df, 'device', 'browser')
    df['browser'] = browser_values

    result = df.groupby(['date', 'browser'])['total_transactions'].sum().reset_index()
    result = result.sort_values(by=['total_transactions'], ascending=[False])
    print(result.head())
    return result


def transactions_by_traffic_source(data):
    """
    Determines traffic sources in terms of transactions for January - March 2017
    :param data: a dataframe with general data
    :return: a dataframe
    """
    df = data.copy()

    transactions_values = extract_series_from_dict_param(df, 'totals', 'transactions')
    df['total_transactions'] = transactions_values

    traffic_source_values = extract_series_from_dict_param(df, 'trafficSource', 'source')
    df['traffic_source'] = traffic_source_values

    result = df.groupby(['traffic_source'])['total_transactions'].sum().reset_index()
    result = result.sort_values(by=['traffic_source'], ascending=[False])
    return result


def update_sheet(worksheet, dataframe):
    values = [dataframe.columns.values.tolist()] + dataframe.values.tolist()
    range_str = 'A1'

    worksheet.update(
        range_str,
        values,
        value_input_option='USER_ENTERED',
    )


def get_worksheet(worksheet_index, sheet_title=SHEET_NAME):
    client = gspread.authorize(credentials)
    sheet = client.open(sheet_title)
    worksheet = sheet.get_worksheet(worksheet_index)
    return worksheet


if __name__ == "__main__":
    combined_df = fetch_all_data()
    combined_df.rename(columns={'Unnamed: 0': 'index'}, inplace=True)
    combined_df['date'] = combined_df['date'].apply(lambda x: datetime.strptime(str(x), '%Y%m%d').strftime('%Y-%m-%d'))

    dict_cols = ['totals', 'device', 'trafficSource']
    convert_col_type_to_dict(dict_cols)

    write_to_google_sheet()
