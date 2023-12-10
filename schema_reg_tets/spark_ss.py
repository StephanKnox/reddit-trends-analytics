import logging
import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
#from minio import Minio

S3_ENDPOINT='localhost:9000'
S3_ACCESS_KEY='Cb5bODHLhocuw9gH'
S3_SECRET_KEY='3utk358B2rHGwMegTiFY01FUsbBWHcVj'

# KAFKA config
KAFKA_BROKER='kafka_broker:19092'
KAFKA_TOPIC='names_topic'

# SPARK config
SPARK_APP='SparkStreaming'
        
# Initialize logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")


def initialize_spark_session(app_name=SPARK_APP, access_key=S3_ACCESS_KEY, secret_key=S3_SECRET_KEY, endpoint=S3_ENDPOINT):
    """
    Initialize the Spark Session with provided configurations.
    
    :param app_name: Name of the spark application.
    :param access_key: Access key for S3.
    :param secret_key: Secret key for S3.
    :return: Spark session object or None if there's an error.
    """
    #.master('spark://spark-master:7077') \
    try:
        spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config('spark.hadoop.fs.s3a.access.key', S3_ACCESS_KEY)  \
        .config('spark.hadoop.fs.s3a.secret.key', S3_SECRET_KEY)  \
        .config('spark.hadoop.fs.s3a.endpoint', S3_ENDPOINT)  \
        .config('fs.s3a.connecion.timeout', 30) \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', "false") \
        .enableHiveSupport().getOrCreate()
        #.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        #.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
               #.config('spark.hadoop.fs.s3a.endpoint', S3_ENDPOINT) \

        spark.sparkContext.setLogLevel("ERROR")
        logger.info('Spark session initialized successfully')
        return spark

    except Exception as e:
        logger.error(f"Spark session initialization failed. Error: {e}")
        return None


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
    # Local path wont work, should point to docker volume I guess ?
    #path = "/opt/bitnami/spark/data"
    checkpoint_location = "/opt/bitnami/spark/data"

    path = "/opt/bitnami/spark/data"#"s3a://streamingsink"

    

    spark = initialize_spark_session(SPARK_APP, S3_ACCESS_KEY, S3_SECRET_KEY)
    if spark:
        df = get_streaming_dataframe(spark, KAFKA_BROKER, KAFKA_TOPIC)
        if df:
            #transformed_df = transform_streaming_data(df)
            #initiate_streaming_to_bucket(transformed_df, path, checkpoint_location)
            #df.show()
            #logger.warning(f"Objects in streamingsink bucket: {df.count()  }")
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