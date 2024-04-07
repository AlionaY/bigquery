import concurrent.futures
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import pandas as pd

from auth import authenticate_google_sheets, authenticate_bigquery


def fetch_data_from_bigquery():
    dataframes = []

    time_periods = generate_time_periods()
    start_time = time.time()

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(get_dataframe_from_table, time_period) for time_period in time_periods
        ]
        for future in concurrent.futures.as_completed(futures):
            dataframes.append(future.result())

    if concurrent.futures.as_completed(futures):
        end_time = time.time()
        print(f'time spent on getting data: {int(end_time - start_time)} sec')

    return pd.concat(dataframes)


def generate_time_periods(start='20170101', end='20170131'):
    time_periods = []
    start_date = datetime.strptime(start, '%Y%m%d')
    end_date = datetime.strptime(end, '%Y%m%d')

    current_date = start_date
    while current_date <= end_date:
        formatted_date = current_date.strftime('%Y%m%d')
        time_periods.append(formatted_date)
        current_date += timedelta(days=1)

    return time_periods


def get_dataframe_from_table(time_period):
    bigquery_client = authenticate_bigquery()

    with open('query.sql', 'r') as file:
        query = file.read()
    query_with_date = query.format(time_period)
    return bigquery_client.query(query_with_date).to_dataframe()


def write_to_google_sheet(data):
    df_list = get_df_list(data)
    num_workers = len(df_list)
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(update_sheet, get_worksheet(idx), df) for (idx, df) in enumerate(df_list)]

    if concurrent.futures.as_completed(futures):
        end_time = time.time()
        print(f'time spent on writing data: {int(end_time - start_time)} sec')


def get_df_list(df):
    transactions_by_date_df = get_transactions_by_date(df)
    traffic_by_date_df = get_traffic_by_date(df)
    transactions_by_browser_df = get_transactions_by_browser(df)
    transactions_by_traffic_source_df = get_transactions_by_traffic_source(df)

    return [transactions_by_date_df, traffic_by_date_df, transactions_by_browser_df, transactions_by_traffic_source_df]


def extract_series_from_dict_param(df, dict_param, param):
    param_series = pd.Series(df[dict_param])
    transactions_values = param_series.apply(lambda x: x.get(param))
    return transactions_values


def get_transactions_by_date(data):
    df = data.copy()
    transactions_values = extract_series_from_dict_param(df, 'totals', 'transactions')
    df['total_transactions'] = transactions_values

    result = df.groupby('date')['total_transactions'].sum().reset_index()
    result = result.sort_values(by='total_transactions', ascending=False)
    return result


def get_traffic_by_date(data):
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


def get_transactions_by_browser(data):
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
    return result


def get_transactions_by_traffic_source(data):
    """
    Determines traffic sources in terms of transactions for January 2017
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


def get_worksheet(worksheet_index):
    sheet = authenticate_google_sheets()
    worksheet = sheet.get_worksheet(worksheet_index)
    return worksheet


def main():
    combined_df = fetch_data_from_bigquery()
    combined_df.rename(columns={'Unnamed: 0': 'index'}, inplace=True)
    combined_df['date'] = combined_df['date'].apply(lambda x: datetime.strptime(str(x), '%Y%m%d').strftime('%Y-%m-%d'))

    write_to_google_sheet(combined_df)


if __name__ == "__main__":
    main()
