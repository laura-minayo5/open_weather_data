from datetime import datetime, timedelta, UTC
from sqlalchemy import create_engine
import psycopg2
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

# define PostgreSQL database connection credentials
db_name = os.getenv('name')
db_user = os.getenv('user')
db_password = os.getenv('password')
db_host =os.getenv('host')
port = os.getenv('port')
db_schema = os.getenv('schema')
# data path to save raw data and cleaned data
raw_data_path = 'weather_data/raw/raw_data.csv'
cleaned_data_path1 = 'weather_data/cleaned/dim_data.csv'
cleaned_data_path2 = 'weather_data/cleaned/fact_data.csv'
# define a function to load the data to postgresql database
# and use pd.to_sql() to load the transformed data to the database directly
def load_to_db(db_host, db_name, db_user, db_password, db_port): # (op_kwargs)
     
    files = ['weather_data/cleaned/dim_data.csv', 'weather_data/cleaned/fact_data.csv']
    # Read CSV files from List and storing them into a single df
    df = pd.concat(pd.read_csv(file) for file in files)
    current_timestamp = datetime.now()
    df['data_ingested_at'] = current_timestamp
    
    # establishing a connection with the postgresql database using sqlalchemy by defining conn. engine
    # engine = create_engine('postgresql+psycopg2://user:password@hostname/database_name') refer to op_kwargs for db details
    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}/{db_name}")

    # create the schema in postgresql database open_weather_data before loading data
    # converting the cleaned dataframe df into an SQL database so we can query it later using sql
    # DataFrame.to_sql(name, con, *, schema=None, if_exists='fail', index=True, index_label=None, chunksize=None, dtype=None, method=None)
    df.to_sql( name='open_weather_data', con=engine, schema=db_schema, if_exists='replace', index=False)
    
    logger.info("weather data loaded successfully!")
    if __name__ == "__main__":
        load_to_db(db_name, db_user, db_password, db_host, port)