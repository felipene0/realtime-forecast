import logging
import requests
import os
import json
import time
import pymongo
from dotenv import load_dotenv
from datetime import datetime
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from confluent_kafka import Producer, KafkaException
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

load_dotenv()

KAFKA_SERVER = os.getenv('KAFKA_SERVER')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
MONGODB_URI = os.getenv('MONGODB_URI')
MONGO_DATABASE = os.getenv('MONGO_DATABASE')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION')

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
    # TODO create a second file to deal with spark connections 
    def create_spark_connection():
        try:
            s_conn = (SparkSession.builder
                      .appName('SparkStream')
                      .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
                      .config("spark.mongodb.write.connection.uri", MONGODB_URI)
                      .config("spark.mongodb.write.database", MONGO_DATABASE)
                      .config("spark.mongodb.write.collection", MONGO_COLLECTION)
                      .getOrCreate())
            logging.info("Spark connection created!")
            return s_conn
        except Exception as e:
            logging.error(f"Couldn't create Spark session: {e}")
            return None
            
    def spark_connection_to_kafka(s_conn):
        df = None
        try:
            df = (s_conn.readStream
                  .format('kafka')
                  .option('kafka.bootstrap.servers', KAFKA_SERVER)
                  .option('subscribe', KAFKA_TOPIC)
                  .option('startingOffsets', 'earliest')
                  .load())           
            logging.info('Kafka dataframe created!') 
            return df
        except Exception as e:
            logging.warning(f"Couldn't create kafka dataframe: {e}")
            return None
            
    @task(task_id='kafka_to_mongo')
    def kafka_to_mongo():
        # Create spark session
        s_conn = create_spark_connection()
        if s_conn is None:
            logging.error("Failed to create SparkSession")
            return
        # Create kafka dataframe
        df = spark_connection_to_kafka(s_conn)
        if df is None:
            logging.error("Failed to create Kafka DataFrame")
            s_conn.stop()
            return
        
        if df is None or s_conn is None:
            logging.error("No dataframe or Spark Session available")
            return
    
        schema = StructType([
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("generationtime_ms", DoubleType(), False),
            StructField("timezone", StringType(), False),
            StructField("elevation", DoubleType(), False),
            StructField("current_units", MapType(StringType(), StringType()), False),
            StructField("current", StructType([
                StructField("time", StringType(), False),
                StructField("temperature_2m", DoubleType(), False)
            ]), False),
        ])
        
        df = (df.selectExpr("CAST(value AS STRING)")
              .select(from_json(col("value"), schema).alias("data"))
              .select("data.*"))
        
        query = (df.writeStream
                 .option("checkpointLocation", "/tmp/checkpoints/")
                 .option("spark.mongodb.connection.uri", MONGODB_URI)
                 .option("spark.mongodb.database", MONGO_DATABASE)
                 .option("spark.mongodb.collection", MONGO_COLLECTION)
                 .outputMode("append")
                 .start())
        
        query.awaitTermination()
        
    get_data_task = get_data()
    stream_data_task = stream_data(get_data_task)
    process_task = kafka_to_mongo()

    start >> get_data_task >> stream_data_task >> process_task >> end   

main()
