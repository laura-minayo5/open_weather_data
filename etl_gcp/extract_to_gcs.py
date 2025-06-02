import json
from dotenv import load_dotenv
from google.cloud import storage
import io
from datetime import datetime, timedelta, UTC
import os,sys
import logging
import requests
import pandas as pd
# set up authentication
# set the GOOGLE_APPLICATION_CREDENTIALS environment variable
# to the path of your downloaded JSON Key file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service-key_google-cloud.json'
load_dotenv()

# Configure the basic logging settings
logging.basicConfig(level=logging.INFO)
# logger instance with unique logger name
logger = logging.getLogger(__name__)

# data path to save raw data and cleaned data
bucket = 'open_weather_data_trial'
raw_data_path = 'raw/raw_data.csv'
cleaned_data_path1 = 'cleaned/dim_data.csv'
cleaned_data_path2 = 'cleaned/fact_data.csv'

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
        # python requests using get method to retrieve info from openweather api
        response = requests.get(url, params=api_params) 
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
            # initializing the client, authentication will be handled automatically
            client = storage.Client()
            # The bucket on GCS in which to write the CSV file
            bucket = client.bucket(bucket_name)
            # The name assigned to the CSV file on GCS raw folder
            blob = bucket.blob(destination_blob_path)
            # write our pandas df to GCS as CSV file
            blob.upload_from_string(weather_df.to_csv(), 'text/csv')
            print(f"Uploaded weather dataframe to gcs://{bucket_name}/{destination_blob_path} as CSV file")
            

        else:
            print(f"Error fetching data for {data['name']}: {response.status_code}")

if __name__ == '__main__':
    extract_openweather_data(url= api_endpoint, bucket_name= bucket, destination_blob_path= raw_data_path)