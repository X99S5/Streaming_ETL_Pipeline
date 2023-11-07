
#Need to rebuild dockerfile everytime for this code, as it is copied over in docker file. Way to copy it over in docker compose???
#Y
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import functions as func
from time import sleep
import logging


logging.info("Starting Consumer1 function...")

spark = SparkSession.builder.master("local[*]").appName("MySparkApp") \
.config('spark.jars','/home/airflow/.local/lib/python3.8/site-packages/pyspark/jars/spark-streaming-kafka-0-10_2.12-3.5.0.jar') \
.config('spark.jars' ,'/home/airflow/.local/lib/python3.8/site-packages/pyspark/jars/kafka-clients-3.5.0.jar') \
.config('spark.jars', '/home/airflow/.local/lib/python3.8/site-packages/pyspark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar') \
.config('spark.jars', '/home/airflow/.local/lib/python3.8/site-packages/pyspark/jars/spark-tags_2.12-3.5.0.jar') \
.config('spark.jars','/home/airflow/.local/lib/python3.8/site-packages/pyspark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar') \
.config('spark.jars','/home/airflow/.local/lib/python3.8/site-packages/pyspark/jars/commons-pool2-2.12.0.jar') \
.getOrCreate()

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka1:29092").option("subscribe", "topic_a").option("startingOffsets", "earliest").option("includeHeaders", "true").load()

# Selects the keys and values and Casts them to string from bytes
df = df.selectExpr('CAST(key AS STRING)', 'CAST(value AS STRING)' , 'timestamp')


# casting value as double
df = df.withColumn("value", col("value").cast("double"))

#Multiplying by 10
#df = df.withColumn("value", col("value") * 1)
# Does a running sum
df = df.agg(sum("value").alias("total_value"))
#Change key to date
df.printSchema()
print(df.printSchema())

query = df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()



query.awaitTermination()



