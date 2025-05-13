from datetime import datetime, timedelta, UTC
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

# data path to save raw data and cleaned data and bucket details
bucket = 'open_weather_data_trial'
raw_data_path = 'raw/raw_data.csv'
cleaned_data_path1 = 'cleaned/dim_data.csv'
cleaned_data_path2 = 'cleaned/fact_data.csv'

# function to convert temperature in fahrenheit to celcius
def fahrenheit_to_celsius(temp_in_fahrenheit):
    celsius = (temp_in_fahrenheit - 32) * 5 / 9
    return celsius
# function to convert speed from m/s to km/h
def m_s_to_km_h(speed_in_m_s):
    speed_in_km_h = speed_in_m_s * 3.6
    return speed_in_km_h
# function to transform weather data using Pandas
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
    # using dense rank to rank wind_speed grouped by weather_description in ascending order
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

transform_weather_data(bucket_name= bucket, source_blob_path= raw_data_path, cleaned_blob_path1= cleaned_data_path1, cleaned_blob_path2= cleaned_data_path2)