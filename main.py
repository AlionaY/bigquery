import concurrent.futures
import os

from google.cloud import bigquery

key_path = ''
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
project_id = 'testtasks-419216'
client = bigquery.Client(project=project_id)


project = 'bigquery-public-data'
bucket_name = ''
dataset_id = 'google_analytics_sample'
table_id = 'ga_sessions_20170731'

queries = [
    "SELECT * FROM bigquery-public-data.google_analytics_sample.ga_sessions_20170731 LIMIT 10"
]


def retrieve_data():
    query = "SELECT * FROM bigquery-public-data.google_analytics_sample.ga_sessions_20170731 LIMIT 10"

    query_job = client.query(query)
    results = query_job.result()
    [print(row) for row in results]


def extract_data():
    destination_uri = 'gs://{}/{}'.format(bucket_name, 'extracted_stories_data.csv')
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)
    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        location='US'
    )
    extract_job.result()
    print('exported')


# def bigquery_to_csv():
#     df = client.query(queries).to_dataframe()


if __name__ == "__main__":
    retrieve_data()
    # # Create a ThreadPoolExecutor with 5 worker threads
    # with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    #     # Submit each query to the executor
    #     future_to_query = {executor.submit(retrieve_data, query): query for query in queries}
    #     # Retrieve the results as they become available
    #     for future in concurrent.futures.as_completed(future_to_query):
    #         queries = future_to_query[future]
    #         try:
    #             data = future.result()
    #             # Process the retrieved data as needed
    #             print(f"Data for query '{queries}': {data}")
    #         except Exception as e:
    #             print(f"Error occurred while retrieving data for query '{queries}': {e}")
