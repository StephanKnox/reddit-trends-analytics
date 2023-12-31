import logging
import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from dotenv import load_dotenv
import os


load_dotenv() 

S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')
KAFKA_BROKER = os.getenv('KAFKA_BROKER') 
KAFKA_TOPIC  = os.getenv('KAFKA_TOPIC') 
SPARK_APP = os.getenv('SPARK_APP')  
        
# Initialize logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")


def initialize_spark_session(app_name=SPARK_APP, access_key=S3_ACCESS_KEY, secret_key=S3_SECRET_KEY):
        spark = pyspark.sql.SparkSession.builder \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config('spark.hadoop.fs.s3a.access.key', access_key)  \
            .config('spark.hadoop.fs.s3a.secret.key', secret_key)  \
            .config("spark.hadoop.fs.s3a.path.style.access", True) \
            .config('spark.hadoop.fs.s3a.connection.ssl.enabled', "false") \
            .appName("StreamingApp") \
            .enableHiveSupport() \
            .getOrCreate()
        return spark


def get_streaming_dataframe(spark, brokers=KAFKA_BROKER, topic=KAFKA_TOPIC):
    """
    Get a streaming dataframe from Kafka.
    
    :param spark: Initialized Spark session.
    :param brokers: Comma-separated list of Kafka brokers.
    :param topic: Kafka topic to subscribe to.
    :return: Dataframe object or None if there's an error.
    """
    try:
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", brokers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("delimiter", ",") \
            .load()
        logger.info("Streaming dataframe fetched successfully")
        
        return df

    except Exception as e:
        logger.warning(f"Failed to fetch streaming dataframe. Error: {e}")
        return None

def main():
    checkpoint_location = "s3a://proj-1-sftp-s3-bucket/spark/data"
    path = "s3a://proj-1-sftp-s3-bucket/data"


    spark = initialize_spark_session(SPARK_APP, S3_ACCESS_KEY, S3_SECRET_KEY)
    if spark:
        df = get_streaming_dataframe(spark, KAFKA_BROKER, KAFKA_TOPIC)
        if df:
            logger.warning('Starting stream write to bucket ...')

            df.writeStream \
                    .format("parquet") \
                    .trigger(processingTime='10 second') \
                    .option("path", path) \
                    .option("checkpointLocation", checkpoint_location) \
                    .start() \
                    .awaitTermination()
         


# Execute the main function if this script is run as the main module
if __name__ == '__main__':
    main()