from airflow import DAG
import requests
from airflow.providers.https.hooks.http import httpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task 
from airflow.utils.dates import days_ago


LATITUDE = '39.76838'
LONGITUDE = '-86.15804'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'


default_args={
    'owner':'airflow'
    'start_date':days_ago(1)
}

with DAG(dag_id = 'weather_etl_pipeline',
         default_args=default_args,
         schedule_interval = '@daily',
         catchup = False)as dags:
    
    @task()
    def extract_weather_date():
        http_hook = httpHook(http_conn_id= API_CONN_ID, method = 'GET')

        endpoint =  