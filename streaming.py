import os
from pyspark.sql.types import StructType, IntegerType, FloatType, ArrayType, StringType
from pyspark.sql.functions import col, udf
from pyspark.sql import SparkSession 
from pyspark.streaming import StreamingContext
import json
from tensorflow.keras.models import load_model
from abc import abstractmethod, abstractstaticmethod
from keras.preprocessing.sequence import TimeseriesGenerator
import numpy as np
import pandas as pd
import joblib

scala_version = '2.12'
spark_version = '3.1.2'
kafka_server = "172.23.0.1:9092"
topic_name = "top"

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1'
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


query = df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()


