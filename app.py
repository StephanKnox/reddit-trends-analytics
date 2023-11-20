import pyspark
from pyspark.sql.functions import *



def _get_spark_session():
        #builder = pyspark.sql.SparkSession.builder.appName("StreamingApp") \
        #.enableHiveSupport() \
        #.getOrCreate()
        #.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        #.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        #.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        #.config('spark.hadoop.fs.s3a.access.key', self.access_key)  \
        #.config('spark.hadoop.fs.s3a.secret.key', self.secret_key)  \
        #.config('spark.hadoop.fs.s3a.endpoint', self.endpoint) \
        #.config("spark.hadoop.fs.s3a.path.style.access", True) \
        #.config('spark.hadoop.fs.s3a.connection.ssl.enabled', "false") \
        #.enableHiveSupport()
        
        my_packages = ["org.apache.hadoop:hadoop-aws:3.3.2"]
        #spark = configure_spark_with_delta_pip(builder, extra_packages=my_packages).getOrCreate()
        spark = pyspark.sql.SparkSession.builder \
            .appName("StreamingApp") \
            .config('spark.sql.warehouse.dir', '/Users/ctac/Documents/warehouse') \
            .enableHiveSupport().getOrCreate()

        return spark


spark = _get_spark_session()

lines = (spark \
       .readStream.format('socket') \
       .option('host', 'localhost') \
       .option('port', '9999') \
       .load())


words = lines.select(explode(split(col('value'), '\\s')).alias('word'))
counts = words.groupBy('word').count()


checkpointDir = '/Users/ctac/Documents/checkpointdir'
streamingQuery = (counts \
                 .writeStream \
                 .format('console') \
                 .outputMode('complete') \
                 .trigger(processingTime='1 second') \
                 .option('checkpointLocation', checkpointDir) \
                 .start())


streamingQuery.awaitTermination()
