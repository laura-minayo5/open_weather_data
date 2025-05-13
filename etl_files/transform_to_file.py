from datetime import datetime, timedelta, UTC
import os,sys
import logging
import requests
import pandas as pd
import json
from dotenv import load_dotenv
load_dotenv()

# Configure the basic logging settings
logging.basicConfig(level=logging.INFO)
# logger instance with unique logger name
logger = logging.getLogger(__name__)

api_endpoint = "https://api.openweathermap.org/data/2.5/weather"
# data path to save raw data and cleaned data
raw_data_path = 'weather_data/raw/raw_data.csv'
cleaned_data_path1 = 'weather_data/cleaned/dim_data.csv'
cleaned_data_path2 = 'weather_data/cleaned/fact_data.csv'
# Function to convert temperature in kelvin to fahrenheit
def fahrenheit_to_celsius(temp_in_fahrenheit):
    celsius = (temp_in_fahrenheit - 32) * 5 / 9
    return celsius
# function to convert speed from m/s to km/h
def m_s_to_km_h(speed_in_m_s):
    speed_in_km_h = speed_in_m_s * 3.6
    return speed_in_km_h
# function to transform weather data using Pandas
def transform_weather_data(raw_input_path, cleaned_output_path1, cleaned_output_path2):
     df = pd.read_csv(raw_input_path) 
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
     dim_df.to_csv(cleaned_output_path1, index=False)
     fact_df.to_csv(cleaned_output_path2, index=False)

     logger.info("weather data transformed successfully")

transform_weather_data(raw_input_path= raw_data_path, cleaned_output_path1= cleaned_data_path1, cleaned_output_path2= cleaned_data_path2)