
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
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

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka1:29092 ,kafka2:29093").option("subscribe", "topic_a").option("startingOffsets", "earliest").load()


df2 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

query = df2.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()


# for i in range(0,20):    
#     output_df = spark.sql("SELECT * FROM my_table_name")
#     output_df.show()
#     sleep(2)

query.awaitTermination()
# if query.isActive:
#     # The query is still running
#     query.awaitTermination()
# else:
#     print(f"Query terminated with status: {query.status}")