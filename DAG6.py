#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage, bigquery
import requests
import pandas as pd

# Define default_args and DAG configuration
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_data_pipeline4',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Set your desired schedule interval
)

def fetch_transform_weather_data(**kwargs):
    token = {'token': '#######################'} #ADD YOUR TOKEN 

    today_date = datetime.now()
    day7_before_today = (today_date - timedelta(7)).strftime('%Y-%m-%d')

    stations_params = {
        'limit': 1000,
        'offset': 1,
    }

    data_california = []

    url = f"https://www.ncei.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&locationid=FIPS:06&startdate={day7_before_today}&enddate={day7_before_today}"
    station_data_response = requests.get(url, headers=token, params=stations_params)
    initial_json = station_data_response.json()

    if 'metadata' in initial_json and 'resultset' in initial_json['metadata']:
        total_stations = initial_json['metadata']['resultset']['count']
        num_requests = -(-total_stations // stations_params['limit'])

        for offset in range(1, num_requests + 1):
            stations_params['offset'] = offset
            response = requests.get(url, headers=token, params=stations_params)
            json_data = response.json()

            if 'results' in json_data:
                california_stations_data = json_data['results']
                data_california.extend([(dt['date'], dt['datatype'],
                                         dt['station'], dt['value']) for dt in california_stations_data])

    data_california_df = pd.DataFrame(data_california)
    column_names_california_df = ['date', 'metric', 'station', 'values']
    data_california_df.columns = column_names_california_df

    filtered_data = data_california_df[data_california_df['metric'].isin(['TMAX', 'TMIN', 'SNOW', 'PRCP', 'TAVG'])].drop_duplicates(['station', 'date', 'metric'], keep='last')

    df_pivot = filtered_data.pivot(index=['station', 'date'], columns='metric', values='values')
    df_pivot.reset_index(inplace=True)

    kwargs['ti'].xcom_push(key='weather_data', value=df_pivot.to_csv(index=False))

fetch_transform_weather_data_task = PythonOperator(
    task_id='fetch_transform_weather_data',
    python_callable=fetch_transform_weather_data,
    provide_context=True,
    dag=dag,
)

def save_to_cloud_storage(**kwargs):
    client = storage.Client()
    bucket_name = 'us-east1-weather-data-compo-856c750c-bucket'  # Replace with your actual bucket name
    blob_name = 'california_real_time4.csv'  # Specify the desired blob name
    
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    # Retrieve data from XCom
    ti = kwargs['ti']
    weather_data_csv = ti.xcom_pull(task_ids='fetch_transform_weather_data', key='weather_data')
    
    blob.upload_from_string(weather_data_csv, content_type='application/csv')

save_to_cloud_storage_task = PythonOperator(
    task_id='save_to_cloud_storage',
    python_callable=save_to_cloud_storage,
    provide_context=True,
    dag=dag,
)

def save_to_bigquery(**kwargs):
    # Replace 'your-project-id' with your actual GCP project ID
    project_id = 'i-multiplexer-406919'

    client = bigquery.Client(project=project_id)

    bucket_name = 'us-east1-weather-data-compo-856c750c-bucket'
    source_blob_name = 'california_real_time4.csv'
    destination_dataset = 'california_real_time_data_usingdag'
    destination_table = 'california_real_time_table1'

    bucket = storage.Client().get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    temp_file_path = '/tmp/california_real_time4.csv'
    blob.download_to_filename(temp_file_path)

    dataset_ref = client.dataset(destination_dataset)
    table_ref = dataset_ref.table(destination_table)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,  # You can set schema if needed
        schema=None,  # Add your schema if needed
        write_disposition='WRITE_TRUNCATE',  # Choose WRITE_APPEND if you want to append
    )

    with open(temp_file_path, 'rb') as source_file:
        load_job = client.load_table_from_file(
            source_file, table_ref, job_config=job_config
        )

    load_job.result()  # Wait for the job to complete
    

save_to_bigquery_task = PythonOperator(
    task_id='save_to_bigquery',
    python_callable=save_to_bigquery,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
fetch_transform_weather_data_task >> save_to_cloud_storage_task >> save_to_bigquery_task

