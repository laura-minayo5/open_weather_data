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
# data path to save raw data
raw_data_path = 'weather_data/raw/raw_data.csv'


cities = [
    {"name": "Kampala", "country": "UG"},
    {"name": "Kigali", "country": "RW"},
    {"name": "Nairobi", "country": "KE"},
    {"name": "Zurich", "country": "CH"},
    {"name": "Warsaw", "country": "PL"}
]
def extract_openweather_data(url, raw_output_path):
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
            weather_df.to_csv(raw_output_path, header=True, index=False)

        else:
            print(f"Error fetching data for {data['name']}: {response.status_code}")
extract_openweather_data(url= api_endpoint, raw_output_path=raw_data_path)