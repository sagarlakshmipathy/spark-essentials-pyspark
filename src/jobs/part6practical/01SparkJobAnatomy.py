from pyspark.sql import SparkSession

from src.utils import config_loader

spark = SparkSession.builder \
    .appName("Spark Job Anatomy") \
    .master("local[2]") \
    .getOrCreate()

sc = spark.sparkContext

config = config_loader("/Users/sagarl/projects/spark-essentials-pyspark/src/config.json")
data_path = config["dataPath"]

rdd1 = sc.parallelize(range(1, 1000000), 1)
print(rdd1.count())
