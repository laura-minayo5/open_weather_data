from google.cloud import bigquery
from google.api_core.exceptions import Conflict, GoogleAPICallError
from google.cloud.exceptions import GoogleCloudError
from dotenv import load_dotenv
from google.cloud import storage
import io
import os,sys
import logging
# set up authentication
# set the GOOGLE_APPLICATION_CREDENTIALS environment variable
# to the path of your downloaded JSON Key file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service-key_google-cloud.json'
load_dotenv()
project_id = os.getenv('project_id')
# data path to save raw data and cleaned data in gcs
bucket = 'open_weather_data_trial'
raw_data_path = 'raw/raw_data.csv'
cleaned_data_path1 = 'cleaned/dim_data.csv'
cleaned_data_path2 = 'cleaned/fact_data.csv'

 # Define the BigQuery dataset and tables details
dataset_name= os.getenv('dataset')
cities_table = os.getenv('cities_table')
measurements_table = os.getenv('measurements_table')
location = os.getenv('location')
project_id = os.getenv('project_id')

# function that creates a bigquery dataset
def create_bigquery_dataset(dataset_id, project_id, location = location):
        # Create a client object
        client = bigquery.Client(project= project_id)
        # Create a DatasetReference
        dataset_ref = client.dataset(dataset_id, project_id)
        # Create a Dataset object
        dataset = bigquery.Dataset(dataset_ref)

        try:
             # Use the exists_ok argument to avoid errors if the dataset already exists
             created_dataset = client.create_dataset(dataset, timeout=30, exists_ok= True)
             print(f"Dataset {created_dataset.dataset_id} created successfully.")
          
        except Exception as e:
             print(f"Failed to create dataset: {e}")

        return dataset

# function that creates big query table1
def create_bq_table1( project_id, dataset_id, table_id ):
    # Create a bigquery client object
    client = bigquery.Client(project= project_id)
    # Specify the schema for the table
    schema = [
         bigquery.SchemaField("city_id", "INTEGER", mode= "REQUIRED"),
         bigquery.SchemaField("city_name", "STRING", mode= "REQUIRED"),
         bigquery.SchemaField("country", "STRING", mode= "REQUIRED"),
         bigquery.SchemaField("latitude", "FLOAT", mode= "NULLABLE"),
         bigquery.SchemaField("longitude", "FLOAT", mode = "NULLABLE"),
    ]
    # Construct a full table reference id
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    # Create a Table object
    table = bigquery.Table(table_ref, schema=schema)
    # Create the table in BigQuery
    try:
         # Attempt to create the table
         table= client.create_table(table)
         print(f"Created table {table.table_id} successfully")
    except Conflict:
         # Handle case where the table already exists
         print(f"Table {table_id} already exists.")
    except GoogleAPICallError as e:
         print(f"Failed to create table: {e}")
    finally:
         # Cleanup or final actions
         print("Table creation process completed.")
    return table

# function that creates bigquery table 2
def create_bq_table2( project_id, dataset_id, table_id):
    # Create a bigquery client object
    client = bigquery.Client(project= project_id)
    # Specify the schema for the table
    schema = [
         bigquery.SchemaField("date", "DATE", mode= "NULLABLE"),
         bigquery.SchemaField("time", "TIMESTAMP", mode= "NULLABlE"),
         bigquery.SchemaField("city_id", "INTEGER", mode= "REQUIRED"),
         bigquery.SchemaField("country", "STRING", mode= "REQUIRED"),
         bigquery.SchemaField("temperature", "FLOAT", mode= "NULLABLE"),
         bigquery.SchemaField("humidity", "FLOAT", mode= "NULLABLE"),
         bigquery.SchemaField("pressure", "FLOAT", mode= "NULLABLE"),
         bigquery.SchemaField("wind_speed", "FLOAT", mode= "NULLABLE"),
    ]
    # Construct a full table reference
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    # Create a Table object
    table = bigquery.Table(table_ref, schema=schema)

    # Create the table in BigQuery
    try:
         # Attempt to create the table
         table= client.create_table(table)
         print(f"Created table {table.table_id} successfully")
    except Conflict:
         # Handle case where the table already exists
         print(f"Table {table_id} already exists.")
    except GoogleAPICallError as e:
         print(f"Failed to create table: {e}")
    finally:
         # Cleanup or final actions
         print("Table creation process completed.")
    return table
# function that loads csv files from gcs to big query created tables using load_table_from_uri() method
def load_csv_to_bigquery(project_id, dataset_id, table_id1, table_id2):
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)
    # Define the table references
    table_ref1 = f"{project_id}.{dataset_id}.{table_id1}"
    table_ref2 = f"{project_id}.{dataset_id}.{table_id2}"
    job_config = bigquery.LoadJobConfig(
          source_format=bigquery.SourceFormat.CSV,
          skip_leading_rows=1, # Skip header row
          autodetect=True, # Automatically infer schema
          write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
          null_marker = ''
    )
    
    # Load data from GCS to BigQuery
    gcs_uri1= f"gs://{bucket}/{cleaned_data_path1}"
    gcs_uri2= f"gs://{bucket}/{cleaned_data_path2}"
    try:
         load_job1 = client.load_table_from_uri(
          gcs_uri1, table_ref1, job_config=job_config
    )
         load_job2 = client.load_table_from_uri(
          gcs_uri2, table_ref2, job_config=job_config
    )
         print("Starting table load jobs...")
         # waits for the load job to finish
         load_job1.result() 
         load_job2.result()
         # Confirm success
         destination_table1 = client.get_table(table_ref1)
         destination_table2 = client.get_table(table_ref2)
         print(f'Loaded {destination_table1.num_rows} rows in table {cities_table} and {destination_table2.num_rows} rows in table {measurements_table}')
    except GoogleCloudError as e:
         # Handle BigQuery-specific errors
         print(f"BigQuery error occurred: {e}")
    except Exception as e:
         # Handle other unexpected errors
         print(f"An unexpected error occurred: {e}")
    finally:
         # final clean up
         print(f"Loading Process Completed Succefully")
# function call to create dataset, tables and load the csv files to bigquery dataset
create_bigquery_dataset(dataset_id= dataset_name, project_id= project_id, location= location)
create_bq_table1(project_id= project_id, dataset_id = dataset_name, table_id = cities_table)
create_bq_table2(project_id= project_id, dataset_id = dataset_name, table_id = measurements_table)
load_csv_to_bigquery(project_id=project_id, dataset_id = dataset_name, table_id1 = cities_table, table_id2 = measurements_table)