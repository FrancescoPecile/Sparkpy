from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import product_ctr30
import product_ctr29

spark = SparkSession.builder.appName("myApp").master("local[*]") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.2') \
    .getOrCreate()

spark.sparkContext.addPyFile("product_ctr29.py")
spark.sparkContext.addPyFile("product_ctr30.py")

df1 = product_ctr29.df9
df2 = product_ctr30.df9

df3 = df1.join(df2, ['BRAND', 'WALL_ID', 'WALLGROUP_ID', 'CAMPAIGN_ID'], 'full')

df3.createOrReplaceTempView("input")

df4 = spark.sql(" SELECT BRAND,WALL_ID,WALLGROUP_ID,CAMPAIGN_ID," +
                " (100*(SUM(ENGAGEMENT_RATE30)-SUM(ENGAGEMENT_RATE29))/SUM(ENGAGEMENT_RATE29)) AS ENGDIFFERENCE," +
                " FROM input" +
                " GROUP BY BRAND,WALL_ID, WALLGROUP_ID, CAMPAIGN_ID,ENGDIFFERENCE")

collectionName = "engagementRateInsights"
dbMode = "append"

df4.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode(dbMode).option("database", "ls-analytics").option("collection", collectionName) \
    .option("ordered", "false").save()

df4.write.format("mongo").mode("append").save()
