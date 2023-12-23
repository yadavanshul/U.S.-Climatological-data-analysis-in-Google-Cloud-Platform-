#import libraries
from airflow import models
from airflow import DAG
import airflow
from datetime import datetime, timedelta
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'anshuly81',
    # 'start_date':airflow.utils.dates.days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(seconds=50),
	'dataflow_default_options': {
        'project': 'i-multiplexer-406919',
        'region': 'us-east1',
		'runner': 'DataflowRunner'
    }
}
   
dag = DAG(
        dag_id='Weather_archived',
        default_args=default_args,
        schedule_interval= '@once',
        start_date=airflow.utils.dates.days_ago(1),
        catchup=False,
        description="DAG for data ingestion and transformation"
)
    
start=DummyOperator(
task_id="start_task_id",
dag=dag
)

dataflow_task = DataFlowPythonOperator(
    task_id='pythontaskdataflow',
    py_file='gs://us-east1-weather-data-compo-856c750c-bucket/dataflow_script_Archived_weather.py',
    options={'input' : 'gs://us-east1-weather-data-compo-856c750c-bucket/data/Archive Data/2023_US_NOOA_WeatherData.csv'},
	dag=dag
)

end=DummyOperator(
task_id="end_task_id",
dag=dag
)  


start >> dataflow_task >> end
