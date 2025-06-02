from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, UTC
import os,sys,logging
from dotenv import load_dotenv
load_dotenv()

# Configure the basic logging settings
logging.basicConfig(level=logging.INFO)
# logger instance with unique logger name
logger = logging.getLogger(__name__)

# importing our three modules for extracting, transforming and loading data
from etl_files import extract_to_file, transform_to_file, load_file_to_db
raw_data = extract_to_file
clean_data = transform_to_file
load_data = load_file_to_db

api_endpoint = "https://api.openweathermap.org/data/2.5/weather"
# data path to save raw data and cleaned data
raw_data_path = './weather_data/raw/raw_data.csv'
cleaned_data_path1 = './weather_data/cleaned/dim_data.csv'
cleaned_data_path2 = './weather_data/cleaned/fact_data.csv'

# define PostgreSQL database connection credentials
db_name = os.getenv('name')
db_user = os.getenv('user')
db_password = os.getenv('password')
db_host =os.getenv('host')
port = os.getenv('port')
db_schema = os.getenv('schema')

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
            raw = raw_data.extract_openweather_data(url= api_endpoint, raw_output_path=raw_data_path)
            if not raw:
                 raise ValueError("No data fetched from the API")
            clean_data.transform_weather_data(raw_input_path= raw_data_path, cleaned_output_path1= cleaned_data_path1, cleaned_output_path2= cleaned_data_path2)
            load_data.load_to_db(db_name, db_user, db_password, db_host, port)
        except Exception as e:
             print(f"Error in ETL process: {str(e)}")

# Creating DAG Object
dag = DAG(
    dag_id ='weather_data_etl',
    default_args = default_args, # refers to the default_args dictionary
    description = 'An ETL workflow for weather data using Pandas',
    start_date = datetime(2025, 5, 6, 13, 0, 0), # datetime() class requires three parameters to create a date: year, month, day
    schedule_interval = None, # takes cron(* * * * * ) or timedelta(days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0) values
    catchup = False # tells scheduler not to backfill all missed DAG runs between the current date and the start date when the DAG is unpaused.
)

# Python operator for the etl task
etl_task = PythonOperator(
        task_id='weather_etl_process',
        python_callable= etl_process,
        dag = dag
)

etl_task


