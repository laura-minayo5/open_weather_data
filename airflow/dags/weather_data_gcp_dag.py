from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, UTC
import os,sys,logging
from dotenv import load_dotenv
from google.cloud import storage
import os,sys
import logging
# set up authentication
# set the GOOGLE_APPLICATION_CREDENTIALS environment variable
# to the path of your downloaded JSON Key file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service-key_google-cloud.json'
load_dotenv()

# Configure the basic logging settings
logging.basicConfig(level=logging.INFO)
# logger instance with unique logger name
logger = logging.getLogger(__name__)

# importing our three modules for extracting, transforming and loading data
from etl_gcp import extract_to_gcs, transform_to_gcs, load_gcp_bq
raw_data = extract_to_gcs
clean_data = transform_to_gcs
load_data = load_gcp_bq

api_endpoint = "https://api.openweathermap.org/data/2.5/weather"
# data path to save raw data and cleaned data
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

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# function for the ETL process
def etl_process():
        try:
            raw = raw_data.extract_openweather_data(url= api_endpoint, bucket_name= bucket, destination_blob_path= raw_data_path)
            if not raw:
                 raise ValueError("No data fetched from the API")
            clean_data.transform_weather_data(bucket_name= bucket, source_blob_path= raw_data_path, cleaned_blob_path1= cleaned_data_path1, cleaned_blob_path2= cleaned_data_path2)
            load_data.create_bigquery_dataset(dataset_id= dataset_name, project_id= project_id, location= location)
            load_data.create_bq_table1(project_id= project_id, dataset_id = dataset_name, table_id = cities_table)
            load_data.create_bq_table2(project_id= project_id, dataset_id = dataset_name, table_id = measurements_table)
            load_data.load_csv_to_bigquery(project_id=project_id, dataset_id = dataset_name, table_id1 = cities_table, table_id2 = measurements_table)
        except Exception as e:
             print(f"Error in ETL process: {str(e)}")

# Creating DAG Object
dag = DAG(
    dag_id ='weather_data_etl_GCP',
    default_args = default_args, # refers to the default_args dictionary
    description = 'An ETL workflow for weather data using Pandas and uploads the data to GCP',
    start_date = datetime(2025, 5, 6, 13, 0, 0), # datetime() class requires three parameters to create a date: year, month, day
    schedule_interval = None, # takes cron(* * * * * ) or timedelta(days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0) values
    catchup = False # tells scheduler not to backfill all missed DAG runs between the current date and the start date when the DAG is unpaused.
)

# Python operator for the etl task
etl_task = PythonOperator(
        task_id='weather_etl_process_to_GCP',
        python_callable= etl_process,
        dag = dag
)

etl_task


