from pyspark.sql import SparkSession
from pyspark.sql.types import *

from src.jobs.common.schema import carsSchema
from src.utils import config_loader

spark = SparkSession.builder \
    .appName("DataFrame Basics") \
    .config("spark.master", "local") \
    .getOrCreate()

config = config_loader("/Users/sagarl/projects/spark-streaming-pyspark/src/config.json")
data_path = config["dataPath"]
output_path = config["outputPath"]
driver = config["driver"]
url = config["url"]
user = config["user"]
password = config["password"]

first_df = spark.read \
    .format(source="json") \
    .option("inferSchema", True) \
    .load(f"{data_path}/cars.json")

# first_df.show()
# first_df.printSchema()

# view the first 10 rows
first_ten_rows = first_df.take(10)
for line in first_ten_rows:
    # print(line)
    pass

# two ways to create a dataframe
cars_df_schema = first_df.schema

cars_df_with_schema = spark.read \
    .format("json") \
    .schema(cars_df_schema) \
    .load(f"{data_path}/cars.json")

# cars_df_with_schema.show()

cars_df_with_typed_schema = spark.read \
    .format("json") \
    .schema(carsSchema) \
    .load(f"{data_path}/cars.json")

# cars_df_with_typed_schema.show()

# create a dataset manually
my_row: Row = Row(
    Name="chevrolet chevelle malibu",
    Miles_per_Gallon=18,
    Cylinders=8,
    Displacement=307,
    Horsepower=130,
    Weight_in_lbs=3504,
    Acceleration=12,
    Year="1970-01-01",
    Origin="USA"
)

# add multiple rows to a dataframe
cars_data = [
    ("chevrolet chevelle malibu", 18.0, 8, 307.0, 130, 3504, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15.0, 8, 350.0, 165, 3693, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18.0, 8, 318.0, 150, 3436, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16.0, 8, 304.0, 150, 3433, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17.0, 8, 302.0, 140, 3449, 10.5, "1970-01-01", "USA")
]

self_cars_data = [
    Row(Name="chevrolet chevelle malibu",
        Miles_per_Gallon=18.0,
        Cylinders=8,
        Displacement=307.0,
        Horsepower=130,
        Weight_in_lbs=3504,
        Acceleration=12.0,
        Year="1970-01-01",
        Origin="USA")
]

# spark.createDataFrame(data=self_cars_data, schema=carsSchema).show()


# either pass in the schema manually and let spark infer the data types
# manual_cars_df_with_auto_schema = spark.createDataFrame(cars, [
#     "Name",
#     "Miles_per_Gallon",
#     "Cylinders",
#     "Displacement",
#     "Horsepower",
#     "Weight_in_lbs",
#     "Acceleration",
#     "Year",
#     "Origin"
# ])

# or pass the schema yourself
# manual_cars_df_with_typed_schema = spark.createDataFrame(data=cars_data, schema=carsSchema)
# manual_cars_df_with_typed_schema.show()

# you can also create a rdd with the rows and create a dataframe
# cars_rdd = spark.sparkContext.parallelize(cars_data)
# manual_cars_df = cars_rdd.toDF()
# manual_cars_df.show()

# exercise
# smartphones = [
#     ("Samsung", "Galaxy S10", "Android", 12),
#     ("Apple", "iPhone X", "iOS", 13),
#     ("Nokia", "3310", "THE BEST", 0)
# ]

# turn the data to a rdd and create a dataframe
# smartphone_rdd = spark.sparkContext.parallelize(smartphones)
# smartphone_df_from_rdd = smartphone_rdd.toDF()
# smartphone_df_from_rdd.show()

# read df from rows with auto and typed schema
# smartphones_df_with_auto_schema = spark.createDataFrame(smartphones, ["Name", "Model", "OS", "Version"])
# smartphones_df_with_typed_schema = spark.createDataFrame(data=smartphones, schema=smartphoneSchema)

moviesDF = spark.read.format("json").option("inferSchema", True).load(f"{data_path}/movies.json")
moviesDF.printSchema()
print(moviesDF.count())
