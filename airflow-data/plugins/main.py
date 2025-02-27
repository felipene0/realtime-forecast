import logging
import os
from dotenv import load_dotenv
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
from spark_conn import create_spark_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

load_dotenv()

KAFKA_SERVER = os.getenv('KAFKA_SERVER')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
        
def spark_connection_to_kafka(s_conn):
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