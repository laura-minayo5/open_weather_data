from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta, UTC
import psycopg2
import os,sys
import logging
import requests
import pandas as pd
import json
from dotenv import load_dotenv
from google.cloud import storage
import io
# set up authentication
# set the GOOGLE_APPLICATION_CREDENTIALS environment variable
# to the path of your downloaded JSON Key file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service-key_google-cloud.json'
load_dotenv()

# Configure the basic logging settings
logging.basicConfig(level=logging.INFO)
# logger instance with unique logger name
logger = logging.getLogger(__name__)

# initializing default arguments to be used in our dag object
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# data path to save raw data and cleaned data in gcs
bucket = 'open_weather_data'
raw_data_path = 'raw/raw_data.csv'
cleaned_data_path1 = 'cleaned/dim_data.csv'
cleaned_data_path2 = 'cleaned/fact_data.csv'

 # Define the BigQuery dataset
bigquery_dataset = "open_weather_data"
cities_table ="cities"
dim_table = "measurements"
project_id = os.getenv('project_id')

api_endpoint = "https://api.openweathermap.org/data/2.5/weather"

cities = [
    {"name": "Kampala", "country": "UG"},
    {"name": "Kigali", "country": "RW"},
    {"name": "Nairobi", "country": "KE"},
    {"name": "Zurich", "country": "CH"},
    {"name": "Warsaw", "country": "PL"}
]
def extract_openweather_data(url, bucket_name, destination_blob_path):

    weather_list = []
    for city in cities:
        api_params = {
            'q': f"{city['name']},{city['country']}",
            'appid': os.getenv("api_key")
        }
        response = requests.get(url, params=api_params) # python requests using get method to retrieve info from openweather api
        if response.status_code == 200:
            data = response.json() # returns a JSON object of the response returned from a request
            weather_list.append({
                'weather_description': data['weather'][0]['description'],
                'city_name': city['name'],
                'country': city['country'],
                'latitude': data['coord']['lat'],
                'longitude': data['coord']['lon'],
                'temperature': data['main']['temp'],
                'feels_like_farenheit': data["main"]["feels_like"],
                'min_temp_fahrenheit': data["main"]["temp_min"],
                'max_temp_farenheit': data["main"]["temp_max"],
                'humidity': data['main']['humidity'],
                'pressure': data['main']['pressure'],
                'wind_speed': data['wind']['speed'],
                'datetime': datetime.fromtimestamp(data['dt']).isoformat()
                  })
            weather_df = pd.DataFrame(weather_list)
            current_timestamp = datetime.now()
            weather_df['data_received_at'] = current_timestamp
            # converting our df to csv
            csv_buffer = io.StringIO()
            weather_df.to_csv(csv_buffer, index= False)
            csv_weather_data = csv_buffer.getvalue()

            # initializing the client, authentication will be handled automatically
            client = storage.Client()
            # The bucket on GCS in which to write the CSV file
            bucket = client.bucket(bucket_name)
            # The name assigned to the CSV file on GCS raw folder
            blob = bucket.blob(destination_blob_path)
            # write our pandas df to GCS as CSV file
            blob.upload_from_string(csv_weather_data, content_type='text/csv')
            print(f"Uploaded weather dataframe to gcs://{bucket_name}/{destination_blob_path} as CSV file")
            

        else:
            print(f"Error fetching data for {city['name']}: {response.status_code}")

# function to convert temperature in fahrenheit to celcius
def fahrenheit_to_celsius(temp_in_fahrenheit):
    celsius = (temp_in_fahrenheit - 32) * 5 / 9
    return celsius
# function to convert speed from m/s to km/h
def m_s_to_km_h(speed_in_m_s):
    speed_in_km_h = speed_in_m_s * 3.6
    return speed_in_km_h
# function to transform weather data using Pandas and uploads it to gcp bucket
def transform_weather_data(bucket_name, source_blob_path, cleaned_blob_path1, cleaned_blob_path2):

    # Initialize the Google Cloud Storage client
    client = storage.Client()
    # Get the bucket and blob
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_path)
    # Download the file content as string
    weather_data = blob.download_as_string()
    
    # Read the CSV file into a pandas DataFrame without writing it to disk
    df = pd.read_csv(io.BytesIO(weather_data))

    # transforming weather data using pandas
    df['date'] = pd.to_datetime(df['datetime'], errors = 'coerce')
    df['time'] = pd.to_datetime(df['datetime'], errors = 'coerce')
    df['temperature']= round(fahrenheit_to_celsius(temp_in_fahrenheit= df['temperature']), 2)
    df['temp_feels_like']= round(fahrenheit_to_celsius(temp_in_fahrenheit= df['feels_like_farenheit']), 2)
    df['wind_speed']= round(m_s_to_km_h(df['wind_speed']), 2)
    df['city_id']= df.groupby('weather_description')['wind_speed'].rank(method= 'dense', ascending= True)
    # selecting specific columns and storing them in dim_df and fact_df dataframe
    dim_df = df[['date', 'time', 'city_id', 'temperature', 'humidity', 'pressure', 'wind_speed']]
    fact_df = df[['city_id', 'city_name', 'country', 'latitude', 'longitude']]
    dataframes = [dim_df, fact_df]
    file_names = ['cleaned/dim_data.csv', 'cleaned/fact_data.csv']
    # Writing DataFrames to CSV files in a loop
    for df, filename in zip(dataframes, file_names):
            # converting our df to csv
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index= False)
            transformed_csv_weather_data = csv_buffer.getvalue()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(filename)
            # write our pandas df to GCS as CSV file
            blob.upload_from_string(transformed_csv_weather_data, content_type='text/csv')
    
    print(f"Uploaded dim_df dataframe to gcs://{bucket_name}/{cleaned_blob_path1} as CSV file")
    print(f"Uploaded fact_df dataframe to gcs://{bucket_name}/{cleaned_blob_path2} as CSV file")
            

    logger.info("weather data transformed successfully")



# Creating DAG Object
dag = DAG(
    dag_id ='weather_data_etl_gcs',
    default_args = default_args, # refers to the default_args dictionary
    description = 'An ETL workflow for weather data using Pandas and GCS',
    start_date = datetime(2025, 5, 6, 13, 0, 0), # datetime() class requires three parameters to create a date: year, month, day
    schedule_interval = None, #"10 * * * *", # takes cron(* * * * * ) or timedelta(days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0) values
    catchup = False # tells scheduler not to backfill all missed DAG runs between the current date and the start date when the DAG is unpaused.
)

# defining task using Airflow PythonOperator for raw data extraction
get_raw_data = PythonOperator(
    task_id='get_raw_data',
    python_callable= extract_openweather_data, # A reference to get_recall_data function that is callable
    # a dictionary of keyword arguments that will get unpacked in your function when referenced.
    op_kwargs={
        'url': api_endpoint,  # refer to raw_url and raw_data_path at the begining of the code
        'bucket_name': bucket,
        'destination_blob_path': raw_data_path
    },
    dag=dag,
)


# defining a task using Airflow PythonOperator for cleaning raw data
clean_raw_data = PythonOperator(
    task_id='clean_raw_data',
    python_callable= transform_weather_data, # A reference to clean_recall_data function that is callable
    # a dictionary of keyword arguments that will get unpacked in your function when referenced.
    op_kwargs={
        'bucket_name': bucket,
        'source_blob_path': raw_data_path, # refers to raw_data_path at the  beginning of code
        'cleaned_blob_path1': cleaned_data_path1, # refers to cleaned_data_path1 at the beginning of code
        'cleaned_blob_path2': cleaned_data_path2 # refers to cleaned_data_path2 at the beginning of code
    },
    dag= dag,
)
# Create the dataset if it doesn't exist
create_dataset = BigQueryCreateEmptyDatasetOperator(
     task_id="create_dataset", 
     dataset_id= bigquery_dataset,
     project_id = project_id,
     #gcp_conn_id="google_cloud_default",
     location= "US",
     exists_ok="True",
     dag = dag
)
create_table1 = BigQueryCreateEmptyTableOperator(
    task_id="create_table1",
    dataset_id= bigquery_dataset,
    table_id="cities_table",
    #google_cloud_conn_id= "google_cloud_default",
    project_id = project_id,
    schema_fields=[
        {'name': 'city_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'city_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'latitude', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'longitude', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    ],
    exists_ok="True",
    dag = dag
)
create_table2 = BigQueryCreateEmptyTableOperator(
    task_id="create_table2",
    dataset_id= bigquery_dataset,
    table_id="measurements_table",
    # google_cloud_conn_id= "google_cloud_default",
    project_id = project_id,
    schema_fields=[
        {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'time', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'city_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'temperature', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'humidity', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'pressure', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'wind_speed', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        
    ],
    exists_ok="True",
    dag = dag

)

# Load data from GCS to BigQuery using GCSToBigQueryOperator
load_data_to_bigquery_table1 = GCSToBigQueryOperator(
    bucket = bucket,
    task_id='load_data1_to_cities',
    source_objects = [f"gs://{bucket}/{cleaned_data_path1}"],
    destination_project_dataset_table=f"{bigquery_dataset}.{cities_table}",
    source_format = 'csv',
    skip_leading_rows = 1, # Skip header row
    field_delimiter=';', # Delimiter for CSV, default is ','
    write_disposition='WRITE_TRUNCATE', # Automatically infer schema from the file
    create_disposition='CREATE_IF_NEEDED',
    schema_fields=[
        {'name': 'city_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'city_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'latitude', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'longitude', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    ],
    # autodetect=True,  # Automatically infer schema from the file
    #google_cloud_conn_id= "google_cloud_default",  # Replace with your Airflow GCP connection ID if not default
    project_id= project_id,
    dag=dag
)
# Load data from GCS to BigQuery using GCSToBigQueryOperator
load_data_to_bigquery_table2 = GCSToBigQueryOperator(
    bucket = bucket,
    task_id='load_data_to_measurements_table',
    source_objects = [f"gs://{bucket}/{cleaned_data_path2}"],
    destination_project_dataset_table=f"{bigquery_dataset}.{dim_table}",
    source_format = 'csv',
    skip_leading_rows = 1, # Skip header row
    field_delimiter=';', # Delimiter for CSV, default is ','
    write_disposition='WRITE_TRUNCATE', # Automatically infer schema from the file
    create_disposition='CREATE_IF_NEEDED',
    schema_fields=[
        {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'time', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'city_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'temperature', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'humidity', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'pressure', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'wind_speed', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        
    ],
    # autodetect=True,  # Automatically infer schema from the file
    #google_cloud_conn_id= "google_cloud_default",  # Replace with your Airflow GCP connection ID if not default
    project_id = project_id,
    dag=dag
)

# task dependencies
(
   get_raw_data 
   >> clean_raw_data 
   >> create_dataset 
   >> create_table1 
   >> create_table2 
   >> load_data_to_bigquery_table1
   >> load_data_to_bigquery_table2

)
 