import functions_framework
import logging
import os
import traceback
import re

from google.cloud import bigquery
from google.cloud import storage

import yaml

with open("./schemas.yaml") as schema_file:
     config = yaml.load(schema_file, Loader=yaml.Loader)

print("Config", config)
PROJECT_ID = os.getenv('My First Project')
print(f"Project id: {PROJECT_ID}")
BQ_DATASET = 'staging'
CS = storage.Client()
BQ = bigquery.Client()
job_config = bigquery.LoadJobConfig()


def streaming(data):
     bucketname = data['bucket'] 
     print("Bucket name",bucketname)
     filename = data['name'][:-4]
     print("File name",filename)  
     timeCreated = data['timeCreated']
     print("Time Created",timeCreated) 
     try:
          for config_entry in config:
               config_entry_name = config_entry.get('name')
               print("Config entry name",config_entry_name)
               if re.search(config_entry_name.replace('_', '-'), filename) or re.search(config_entry_name, filename):
                    print("Config entry name matched!!")
                    config_entry_schema = config_entry.get('schema')
                    _check_if_table_exists(filename,config_entry_schema)
                    config_entry_format = config_entry.get('format')
                    if config_entry_format == 'NEWLINE_DELIMITED_JSON':
                         _load_table_from_uri(data['bucket'], data['name'], config_entry_schema, filename)
                    elif config_entry_format == 'CSV':
                      _load_table_from_csv(data['bucket'], data['name'], config_entry_schema, filename)
          print("Try block FINISHED.")

     except Exception:
          print('Error streaming file. Cause: %s' % (traceback.format_exc()))

     print("***Exited try except***")

def _check_if_table_exists(tableName,tableSchema):
     print("Checking if table exists!")

     table_id = BQ.dataset(BQ_DATASET).table(tableName)

     try:
          BQ.get_table(table_id)
          print("table already exists!")
     except Exception:
          print("table does not exist, creating now...")
          logging.warn('Creating table: %s' % (tableName))
          schema = create_schema_from_yaml(tableSchema)
          table = bigquery.Table(table_id, schema=schema)
          table = BQ.create_table(table)
          print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

def _load_table_from_uri(bucket_name, file_name, tableSchema, tableName):

     uri = 'gs://%s/%s' % (bucket_name, file_name)
     table_id = BQ.dataset(BQ_DATASET).table(tableName)

     schema = create_schema_from_yaml(tableSchema) 
     print("trying to read file with Schema:",schema)
     job_config.schema = schema

     job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
     job_config.write_disposition = 'WRITE_APPEND',

     print("About to load data from file.")
     load_job = BQ.load_table_from_uri(
     uri,
     table_id,
     job_config=job_config,
     ) 
          
     load_job.result()
     print("data from file loaded\nJob finished.")

def _load_table_from_csv(bucket_name, file_name, tableSchema, tableName):

     uri = 'gs://%s/%s' % (bucket_name, file_name)
     table_id = BQ.dataset(BQ_DATASET).table(tableName)

     schema = create_schema_from_yaml(tableSchema) 
     print("trying to read file with Schema:",schema)

     job_config_csv = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV,skip_leading_rows=1,autodetect=True)

     print("About to load data from file.")
     load_job = BQ.load_table_from_uri(
     uri,
     table_id,
     job_config=job_config_csv,
     ) 
          
     load_job.result()
     print("data from file loaded\nJob finished.")

def create_schema_from_yaml(table_schema):
     schema = []
     for column in table_schema:
          
          schemaField = bigquery.SchemaField(column['name'], column['type'], column['mode'])

          schema.append(schemaField)

          if column['type'] == 'RECORD':
               schemaField._fields = create_schema_from_yaml(column['fields'])
     return schema

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")

    streaming(data)
