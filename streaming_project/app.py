import pyspark
from pyspark.sql.functions import *

# launch stream in console command via Net Cat (utility for TCP and UDP on Mac)
# nc -lk 9999    

def get_spark_session():
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
        
        #my_packages = ["org.apache.hadoop:hadoop-aws:3.3.2"]
        #spark = configure_spark_with_delta_pip(builder, extra_packages=my_packages).getOrCreate()
        spark = pyspark.sql.SparkSession.builder \
            .appName("StreamingApp") \
            .config('spark.sql.warehouse.dir', '/Users/ctac/Documents/warehouse') \
            .enableHiveSupport() \
            .getOrCreate()

        return spark


spark = get_spark_session()

lines = (spark \
       .readStream.format('socket') \
       .option('host', 'localhost') \
       .option('port', '9999') \
       .load())

filteredLines = lines.filter(col('value') != 'Stas')

words = filteredLines.select(explode(split(col('value'), '\\s')).alias('word'))
counts = words.groupBy('word').count()


checkpointDir = '/Users/ctac/Documents/checkpointdir1'
streamingQuery = (counts \
                 .writeStream \
                 .format('console') \
                 .outputMode('complete') \
                 .trigger(processingTime='1 second') \
                 .option('checkpointLocation', checkpointDir) \
                 .option('numRows', 200) \
                 .start())


streamingQuery.awaitTermination()


from pyspark.sql.types import *
inputDirOfJsonFiles = ''

fileSchema = (StructType() \
        .add(StructField('key', IntegerType())) \
        .add(StructField('value', IntegerType()))
)

inputDf = (spark \
           .readStream \
           .format('json') \
           .schema(fileSchema) \
           .load(inputDirOfJsonFiles))


outputDir = ''
checkpointDir = ''
resultDf = ...

streamingQuery = (resultDf.writeStream \
                  .format('parquet') \
                  .option('path', outputDir) \
                  .option('checkpointLocation', checkpointDir) \
                  .start()) 


inputDf = spark.readStream \
           .format('kafka') \
           .option('kafka.bootstrap.servers', 'host1:port1,host2:port2') \
           .option('subscribe', 'events') \
           .load()
          

counts = ... # DataFrame [word: string, count:long]
streamingQuery = counts \
        .selectExpr(
                "cast(word as string) as key",
                "cast(count as string) as value") \
        .writeStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', "host1:port1,host2:port2") \
        .option('topic', "wordCounts") \
        .outputMode('update') \
        .option('checkPointLocation', checkpointDir) \
        .start()
    