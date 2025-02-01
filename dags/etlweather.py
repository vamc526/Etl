from airflow import DAG
import requests
import json
from airflow.providers.https.hooks.http import httpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task 
from airflow.utils.dates import days_ago



#This is an ETL project we are extracting weather data thru an API, doing some transformations as API data is in JSON format to an RDBMS storage(postgresqlDb)

# Configuring which data to extract, Source and target connections

LATITUDE = '39.76838'
LONGITUDE = '-86.15804'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'


# Defining DAGs 
default_args={
    'owner':'airflow'
    'start_date':days_ago(1)
}

with DAG(dag_id = 'weather_etl_pipeline',
         default_args=default_args,
         schedule_interval = '@daily',
         catchup = False)as dags:
    


# Defining Tasks

#Task 1 : Extracting weather data from the api config connection first and get the response.
    @task()
    def extract_weather_date():
        http_hook = httpHook(http_conn_id= API_CONN_ID, method = 'GET')

        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else :
            raise Exception(f"Failed to fetch the weather data:{response.status_code}")


#Task 2 : The response from the previous step is n JSON format need to transform it into tabular format.

    @task()
    def transform_weather_data(weather_data):
        current_weather = weather_data["current_weather"]
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }
        return transformed_data

#Task 3 : The transformed data in then pushed to RDBMS system.
# 
# Using Cursor to create the table if it doesn't exist and Inserting transformed values inside the table.     
    @task()
    def load_weather_data(transformed_data)
        pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Insert transformed data into the table
        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))

        conn.commit()
        cursor.close()

#Defining workflow  etl pipeline. 
    weather_data = extract_weather_date()
    transformed_data=transform_weather_data(weather_data)
    load_weather_data(transformed_data)
