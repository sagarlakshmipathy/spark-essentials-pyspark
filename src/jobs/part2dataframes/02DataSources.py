from pyspark.sql import SparkSession

from src.jobs.common.schema import carsSchema, stocksSchema
from src.utils import config_loader

spark: SparkSession = SparkSession.builder \
    .config("spark.master", "local") \
    .config("spark.jars", "/Users/sagarl/projects/dependencies/pyspark/postgresql-42.6.0.jar") \
    .appName("Data Sources and Formats") \
    .getOrCreate()

config = config_loader("/Users/sagarl/projects/spark-streaming-pyspark/src/config.json")
data_path = config["dataPath"]
output_path = config["outputPath"]
driver = config["driver"]
url = config["url"]
user = config["user"]
password = config["password"]


# create a dataframe
carsDF = spark.read \
    .format("json") \
    .schema(carsSchema) \
    .load(f"{data_path}/cars.json")

# carsDF.show()


# write to output
# carsDF.write \
#     .format(source="json") \
#     .mode(saveMode="overwrite") \
#     .save(f"{output_path}/cars.json")


# read config
spark.read \
    .format("json") \
    .schema(carsSchema) \
    .option("dateFormat", "YYYY-MM-DD") \
    .option("allowSingleQuotes", True) \
    .option("compression", "uncompressed") \
    .load(f"{data_path}/cars.json")

spark.read \
    .format(source="csv") \
    .schema(schema=stocksSchema) \
    .option("header", True) \
    .option("sep", ",") \
    .option("nullValue", "") \
    .load(f"{data_path}/stocks.csv")


# save as parquet
# carsDF.write \
#     .format("parquet") \
#     .mode(saveMode="overwrite") \
#     .save(f"{output_path}/cars.parquet")


# read from jdbc
employees_df = spark.read \
    .format(source="jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("user", user) \
    .option("password", password) \
    .option("dbtable", "public.employees") \
    .load()

# employeesDF.show()


# write as csv and parquet
movies_df = spark \
    .read \
    .format(source="json") \
    .option("inferSchema", True) \
    .load(f"{data_path}/movies.json")

movies_df.show()

# movies_df.write \
#     .format(source="csv") \
#     .mode(saveMode="overwrite") \
#     .option("header", True) \
#     .option("sep", "\t") \
#     .save(f"{outputPath}/movies.csv")

# movies_df.write \
#     .format("parquet") \
#     .mode(saveMode="overwrite") \
#     .save(f"{outputPath}/movies.parquet")


# write to jdbc
# moviesDF.write \
#     .format(source="jdbc") \
#     .option("url", url) \
#     .option("driver", driver) \
#     .option("user", user) \
#     .option("password", password) \
#     .option("dbtable", "public.movies") \
#     .save()
