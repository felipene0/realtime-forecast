import logging
import requests
import os
import json
import time
from dotenv import load_dotenv
from datetime import datetime
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer, KafkaException
from main import kafka_to_mongo

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

load_dotenv()

KAFKA_SERVER = os.getenv('KAFKA_SERVER')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')

default_args = {
    'author': 'felipe',
    'start_date': datetime(2025, 1, 1)
}

API = 'https://api.open-meteo.com/v1/forecast'
params = {
    "latitude": 38.73,
    "longitude": -9.14,
    "current": "temperature_2m",
}

kafka_conf = {
    'bootstrap.servers': KAFKA_SERVER,
    'client.id': 'forecast-producer',
    'session.timeout.ms': 6000,
}

@dag(
    dag_id="seek_temperature",
    default_args=default_args,
    schedule="@daily",
    catchup=False
)
def main():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
        
    @task(task_id='get_data')
    def get_data():
        try:
            response = requests.get(API, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f'Error fetching data: {e}')
            return None
        
    @task(task_id='stream_data')
    def stream_data(response):
        if response is None:
            logging.error("No data received")
            return 
        producer = Producer(kafka_conf)
        try:
            msg = json.dumps(response).encode('utf-8')
            producer.produce(KAFKA_TOPIC, value=msg)
            producer.flush()
            logging.info('Message sent to Kafka sucessfully')
        except KafkaException as e:
            logging.info(f'Kafka error: {e}')
            
    spark_task = PythonOperator(
        task_id='process_kafka_to_mongo',
        python_callable=kafka_to_mongo
    )
    
    get_data_task = get_data()
    stream_data_task = stream_data(get_data_task)

    start >> get_data_task >> stream_data_task >> spark_task >> end   

main()
