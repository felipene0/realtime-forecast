import logging
import requests
import os
import json
import time
from dotenv import load_dotenv
from datetime import datetime
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from confluent_kafka import Producer, KafkaException

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

# ¨¨¨¨
# @dag(
#     dag_id = 'seek_temperature',
#     default_args = default_args,
#     schedule = '@daily',
#     start_date=datetime(2025,1,1),
#     catchup=False
# )
# def main():
    
#     start = EmptyOperator(task_id='start')
#     end = EmptyOperator(task_id='end')
    
# @task(task_id='get_data')
def get_data():
    try:
        data = requests.get(API, params= params)
        data.raise_for_status()
        data = data.json()
        
        return data
        # logging.info(data)

    except requests.RequestException as e:
        logging.error(f'Error fetching data: {e}')
        
# @task(task_id='stream_data')
def stream_data(data):
    producer = Producer(kafka_conf)
    try:
        msg = json.dumps(data).encode('utf-8')
        producer.produce(KAFKA_TOPIC, value=msg)
        producer.flush()
        logging.info('Message sent to Kafka sucessfully')
    except KafkaException as e:
        logging.info(f'Kafka error: {e}')
        
# get_data >> stream_data
        
if __name__ == '__main__':
    res = get_data()
    stream_data(res)
    
    # main()