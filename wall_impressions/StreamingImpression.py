from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
from pymongo import MongoClient

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

jsonSchema = spark.read.option("multiLine", True).json("s3a://lsred-analytics/data-json/2021/06/30/01").schema

df3 = spark.readStream.schema(jsonSchema).json("s3a://lsred-analytics/data-json/2021/06/30/01")

# df.writeStream
#       .option("checkpointLocation", "s3a://checkpoint/dir")
#       .option("tableSpec","my-project:my_dataset.my_table")
#       .format("com.samelamin.spark.bigquery")
#       .start()

df7 = df3.filter('EVENT_TYPE=="wall-impression"').withColumn('DATE', col('ISOTIMESTAMP').cast('timestamp')) \
    .withWatermark("DATE", "60 minutes") \
    .groupby(window(df3.ISOTIMESTAMP, "1 day", "1 day"), 'BRAND', 'WALL_ID', 'WALLGROUP_ID', 'CAMPAIGN_ID',
             'EVENT_TYPE', 'DAY') \
    .count().withColumnRenamed("count", "IMPRESSIONS29") \
    .withColumnRenamed("EVENT_TYPE", "EVENT_TYPE29")


class ForeachWriter:

    def open(self, partition_id, epoch_id):
        self.connection = MongoClient("mongodb://127.0.0.1/")
        self.db = self.connection['wallStreamingLegit']
        self.coll = self.db['windowDIZIONARIO']
        print(epoch_id)
        print(partition_id)
        return True

    def process(self, row):
        # Write row to connection. This method is NOT optional in Python.
        # self.coll.insert_one(row.asDict())
        dizionario = row.asDict()
        self.coll.replace_one({"BRAND": dizionario["BRAND"],"WALL_ID": dizionario["WALL_ID"],
                              "WALLGROUP_ID": dizionario["WALLGROUP_ID"],
                              "CAMPAIGN_ID": dizionario["CAMPAIGN_ID"],"DAY": dizionario["DAY"]},dizionario,upsert=True)

    def close(self, error):
        # Close the connection. This method in optional in Python.
        print(error)


query = df7.writeStream.foreach(ForeachWriter()) \
    .trigger(processingTime='1 seconds').outputMode("update").option("truncate", "false").start()

query.awaitTermination()
