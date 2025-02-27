import logging
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

load_dotenv()

def create_spark_connection():
    try:
        MONGODB_URI = os.getenv('MONGODB_URI')
        MONGO_DATABASE = os.getenv('MONGO_DATABASE')
        MONGO_COLLECTION = os.getenv('MONGO_COLLECTION')
        s_conn = (SparkSession.builder
                    .appName('SparkStream')
                    .config("spark.jars.packages", 
                            "org.mongodb.spark:mongo-spark-connector:10.0.5,"
                            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1")
                    .config("spark.mongodb.write.connection.uri", MONGODB_URI)
                    .config("spark.mongodb.write.database", MONGO_DATABASE)
                    .config("spark.mongodb.write.collection", MONGO_COLLECTION)
                    .getOrCreate())
        logging.info("Spark connection created!")
        return s_conn
    except Exception as e:
        logging.error(f"Couldn't create Spark session: {e}")
        return None