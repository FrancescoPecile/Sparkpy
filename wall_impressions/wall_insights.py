from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import wall_impressions30
import wall_impressions29

spark = SparkSession.builder.appName("myApp").master("local[*]") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.2') \
    .getOrCreate()

spark.sparkContext.addPyFile("wall_impressions30.py")
spark.sparkContext.addPyFile("wall_impressions29.py")

df1 = wall_impressions29.df7
df2 = wall_impressions30.df7

df3 = df1.join(df2)
df3.show()

df3.createOrReplaceTempView("input")

df4 = spark.sql("SELECT BRAND ,WALL_ID ,WALLGROUP_ID ,CAMPAIGN_ID,EVENT_TYPE," +
                " (100*(SUM(IMPRESSIONS30)-SUM(IMPRESSIONS29))/SUM(IMPRESSIONS29))AS DIFFERENCE" +
                " FROM input" +
                " WHERE BRAND = BRAND29 AND WALL_ID = WALL_ID29 AND WALLGROUP_ID = WALLGROUP_ID29" +
                " AND CAMPAIGN_ID = CAMPAIGN_ID29 AND EVENT_TYPE = EVENT_TYPE29" +
                " GROUP BY BRAND,WALL_ID, WALLGROUP_ID, CAMPAIGN_ID, EVENT_TYPE")

collectionName = "insights2930"
dbMode = "append"
df4.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode(dbMode).option("database", "ls-analytics").option("collection", collectionName) \
    .option("ordered", "false").save()

df4.write.format("mongo").mode("append").save()
