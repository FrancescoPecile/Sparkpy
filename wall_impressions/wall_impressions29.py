from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime

spark = SparkSession.builder.appName("myApp").master("local[*]") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Spark.wall_impressions2") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Spark.wall_impressions2") \
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

df1 = spark.read.json(url + today + "/*")
df2 = spark.read.json(url + yesterday + "/23/*")
df3 = spark.read.json(url + tomorrow + "/00/*")

df4 = df1.unionByName(df2, allowMissingColumns=True)
df5 = df4.unionByName(df3, allowMissingColumns=True)

df6 = df5.filter('DAY=="30"')

df7 = df6.filter('EVENT_TYPE=="wall-impression"').withColumn('DATE', col('ISOTIMESTAMP').cast('date')) \
    .groupby('BRAND', 'WALL_ID', 'WALLGROUP_ID', 'CAMPAIGN_ID', 'EVENT_TYPE', 'DATE') \
    .count().withColumnRenamed("count", "IMPRESSIONS29")

df7.write.format("mongo").mode("overwrite").save()

