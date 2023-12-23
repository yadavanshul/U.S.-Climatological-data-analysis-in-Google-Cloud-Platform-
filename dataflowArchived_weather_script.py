import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse
from google.cloud import bigquery
from apache_beam.runners.runner import PipelineState

#project-id:dataset_id.table_id
weather_data_2023 = 'i-multiplexer-406919.weather_archive_data.weather_2023'

parser = argparse.ArgumentParser()

parser.add_argument('--input',
                      dest='input',
                      required=True,
                      help='Input file to process.')
                      
path_args, pipeline_args = parser.parse_known_args()

inputs_pattern = path_args.input

def print_row(row):
    print("final value for current row is :", row)
    return row

def print_raw_row(row):
    print("raw row is :", row)
    return row

def print_split_row(row):
    print("split row is :", row)
    return row

def print_transformed_row(row):
    print("transformed row is :", row)
    return row



options = PipelineOptions(pipeline_args)

p = beam.Pipeline(options = options)
print("transformation started")

weather_2023 = (
    p
    | 'ReadFromText' >> beam.io.ReadFromText(inputs_pattern, skip_header_lines=1)
    | 'Split' >> beam.Map(lambda line: line.split(","))
    | 'TransformFields' >> beam.Map(lambda fields: [fields[0], fields[1], fields[2], fields[3] if fields[3] else "0", fields[4], fields[5], fields[6], fields[7], fields[8]])
    | 'Distinct' >> beam.Distinct()
    | 'FormatCSV' >> beam.Map(lambda fields: ",".join(fields))
)



print("creating bigquery client")
# BigQuery 
client = bigquery.Client()
dataset_id = "{}.weather_archive_data".format(client.project)

try:
    print("checking if dataset exists")
    client.get_dataset(dataset_id)
    print("dataset exists")
     
	
except:
    dataset = bigquery.Dataset(dataset_id)  
    print("creating dataset")
    
    dataset.location = "US"
    dataset.description = "dataset for weather archive data"
    
    dataset_ref = client.create_dataset(dataset, timeout=30)
    print("dataset created")
	
def to_json(csv_str):
    fields = csv_str.split(',')
    
    json_str = {"int64_field_0":fields[0],
                 "station_id": fields[1],
                 "date": fields[2],
                 "PRCP": fields[3],
                 "SNOW": fields[4],
                 "TAVG": fields[5],
                 "TMAX": fields[6],
                 "TMIN": fields[7],
                 "country_code": fields[8]
                 }

    print("json is: ", json_str)
    return json_str
	
table_schema = 'int64_field_0:INTEGER,station_id:STRING,date:INTEGER,PRCP:FLOAT,SNOW:FLOAT,TAVG:STRING,TMAX:STRING,TMIN:STRING,country_code:STRING'
print("Schema: ",table_schema)


# Now, apply the transformations for printing and writing to BigQuery
(weather_2023
 | 'print delivered count' >> beam.Map(print_row)
 | 'delivered to json' >> beam.Map(to_json)
 | 'write delivered' >> beam.io.WriteToBigQuery(
    weather_data_2023,
    schema=table_schema,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
  )
)


try:
    ret = p.run()
    if ret.state == PipelineState.DONE:
        print('Success!!!')
    else:
        print(f'Error Running beam pipeline. State: {ret.state}')
except RuntimeError as e:
    print(f'Error during pipeline execution: {e}')
