from pyspark.sql import SparkSession
from pyspark.sql.types import *

from src.utils import config_loader

spark = SparkSession.builder \
    .appName("Spark Job Anatomy") \
    .master("local[2]") \
    .getOrCreate()

sc = spark.sparkContext

config = config_loader("/Users/sagarl/projects/spark-essentials-pyspark/src/config.json")
data_path = config["dataPath"]

# create an RDD
rdd1 = sc.parallelize(range(1, 1000000))

# multiply each item with 2
rdd1.map(lambda x: x * 2)

# repartition
rdd1.repartition(23)

# create another RDD
rdd2 = sc.parallelize(range(1, 1000000))

# schema for DF conversion
schema = StructType([
    StructField("number", IntegerType(), True)
])


# all the ways to convert RDD to DF
def to_row(x):
    return Row(number=x)
df1 = spark.createDataFrame(rdd1.map(to_row))
df1.explain()
# my fav
df2 = spark.createDataFrame(rdd1.map(lambda x: (x,)), schema=schema)
df2.explain()

df3 = spark.createDataFrame(rdd1.map(lambda x: Row(x))).toDF("number")
df3.explain()

df4 = spark.createDataFrame(rdd1.map(lambda x: Row(x)), schema)
df4.explain()

# or create a DF directly
ds1 = spark.range(1, 1000000).toDF("number")
ds1.explain()
ds1.show()

# /**
# *
# * Complex job 1
# * The Spark optimizer is able to pre-determine the job/stage/task planning before running any code.
# */

ds2 = spark.range(1, 100000, 2).toDF("number")
ds3 = ds1.repartition(7)
ds4 = ds2.repartition(9)
ds5 = ds3.selectExpr("number * 5 as number")
joined = ds5.join(ds4, "number")
add = joined.selectExpr("sum(number)")
add.show()

# /**
# * Complex job 2
# *
# * The default number of partitions for a joined DF (and any unspecified repartition) is 200.
# * You can change it by setting the `spark.sql.shuffle.partitions` config.
# */

df1 = spark.range(1, 1000000).toDF("value").repartition(7)
df2 = spark.range(1, 100000).toDF("value").repartition(9)
df3 = df1.selectExpr("value * 5 as value")
df4 = df3.join(df2, "value")
add2 = df4.selectExpr("sum(value)")
add2.show()
