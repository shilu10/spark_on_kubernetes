import sys
import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.avro.functions import *
from pyspark.sql import SparkSession

#source=sys.argv[1]
#target=sys.argv[2]

spark = SparkSession\
    .builder\
    .appName("repartition-job")\
    .getOrCreate()

rdd = spark.sparkContext.parallelize([1, 2])
print(rdd.collect())

spark.stop()
