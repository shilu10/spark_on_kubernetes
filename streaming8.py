import os
from pyspark.sql.types import StructType, IntegerType, FloatType, ArrayType, StringType
from pyspark.sql.functions import col, udf
from pyspark.sql import SparkSession 
from pyspark.streaming import StreamingContext
import json

scala_version = '2.12'
spark_version = '3.1.2'
kafka_server = "my-cluster-kafka-bootstrap:9092"
topic_name = "demo1"

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1',
    'org.apache.spark.sql.kafka010.KafkaBatchInputPartition'
]

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

spark = SparkSession.builder\
   .appName("kafka-example")\
   .config("spark.jars.packages", ",".join(packages))\
   .getOrCreate()


df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", topic_name) \
    .load()

def processing_func(batch_df, batch_id): 
    batch_df = batch_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    batch_df.show()

writer = df.writeStream.trigger(processingTime='30 seconds').foreachBatch(processing_func).start().awaitTermination()
