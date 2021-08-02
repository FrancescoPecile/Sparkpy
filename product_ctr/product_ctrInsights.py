from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import product_ctr30
import product_ctr29

spark = SparkSession.builder.appName("myApp").master("local[*]") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.2') \
    .getOrCreate()

spark.sparkContext.addPyFile("wall_impressions30.py")
spark.sparkContext.addPyFile("wall_impressions29.py")

df1 = product_ctr29.df9
df2 = product_ctr30.df9

df3 = df1.join(df2)
df3.show()

df3.createOrReplaceTempView("input")

df4 = spark.sql(" SELECT BRAND29,WALL_ID29,WALLGROUP_ID29,CAMPAIGN_ID29,EVENT_TYPE29," +
                " SUM(IMPRESSIONS30) - SUM(IMPRESSIONS29) AS IMPDIFFERENCE," +
                " SUM(CLICKS30) - SUM(CLICKS29) AS CLICKDIFFERENCE" +
                " FROM result29_30" +
                " WHERE BRAND29 = BRAND30=BRAND=BRAND1 AND WALL_ID29 = WALL_ID30 = WALL_ID1 = WALL_ID" +
                " AND WALLGROUP_ID29 = WALLGROUP_ID30 = WALLGROUP_ID1 = WALLGROUP_ID" +
                " AND CAMPAIGN_ID29 = CAMPAIGN_ID30 = CAMPAIGN_ID1 = CAMPAIGN_ID " +
                " AND EVENT_TYPE29 = EVENT_TYPE30 = EVENT_TYPE1 = EVENT_TYPE " +
                " GROUP BY BRAND29,WALL_ID29, WALLGROUP_ID29, CAMPAIGN_ID29, EVENT_TYPE29")

collectionName = "insightsCTR2930"
dbMode = "append"

df4.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode(dbMode).option("database", "ls-analytics").option("collection", collectionName) \
    .option("ordered", "false").save()

df4.write.format("mongo").mode("append").save()
