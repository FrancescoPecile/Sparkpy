from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime

spark = SparkSession.builder.appName("myApp").master("local[*]") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.2') \
    .getOrCreate()

accessKeyId = "AKIAZD43FV3QNGN6R5VF"
secretAccessKey = "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"

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", accessKeyId)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secretAccessKey)
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3-eu-west-1.amazonaws.com")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")

day_added = datetime.timedelta(days=1)

today_datetime = datetime.datetime(2021, 6, 30)
tomorrow_datetime = today_datetime + day_added
yesterday_datetime = today_datetime - day_added

today = today_datetime.strftime("%Y/%m/%d")
tomorrow = tomorrow_datetime.strftime("%Y/%m/%d")
yesterday = yesterday_datetime.strftime("%Y/%m/%d")
url = "s3a://lsred-analytics/data-json/"

jsonSchema = spark.read.option("multiLine", True).json("s3a://lsred-analytics/data-json/2021/test/*").schema

df3 = spark.readStream.schema(jsonSchema).json("s3a://lsred-analytics/data-json/2021/test/*")

# df.writeStream
#       .option("checkpointLocation", "s3a://checkpoint/dir")
#       .option("tableSpec","my-project:my_dataset.my_table")
#       .format("com.samelamin.spark.bigquery")
#       .start()

df7 = df3.filter('EVENT_TYPE=="wall-impression"').withColumn('DATE', col('ISOTIMESTAMP').cast('date')) \
    .groupby('BRAND', 'WALL_ID', 'WALLGROUP_ID', 'CAMPAIGN_ID', 'EVENT_TYPE', 'DATE') \
    .count().withColumnRenamed("count", "IMPRESSIONS29") \
    .withColumnRenamed("EVENT_TYPE", "EVENT_TYPE29")


# df7 = df3.filter('EVENT_TYPE=="wall-impression"').withColumn("isotimestamp",
#         to_timestamp(col("ISOTIMESTAMP")))\
#     .withWatermark('isotimestamp', "10 minutes") \
#     .groupby(window('isotimestamp', "10 minutes", "5 minutes"), 'BRAND', 'WALL_ID',
#              'WALLGROUP_ID', 'CAMPAIGN_ID', 'EVENT_TYPE') \
#     .count().withColumnRenamed("count", "IMPRESSIONS29") \
#     .withColumnRenamed("EVENT_TYPE", "EVENT_TYPE29")


# collectionName = "streamingForever"
# dbMode = "append"

# query = df7 \
#     .writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .start()

def write_mongo_row(df):
    mongoURL = "mongodb://127.0.0.1/Spark.streamingForever"
    df.write.format("mongo").outputMode("append").option("uri", mongoURL).save()
    pass


query = df7.writeStream.foreach(write_mongo_row).start()

query.awaitTermination()
