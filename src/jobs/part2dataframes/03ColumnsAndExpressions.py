import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Columns and Expressions") \
    .config("spark.master", "local") \
    .getOrCreate()

with open("/Users/sagarl/projects/spark-essentials-pyspark/src/config.json") as file:
    config = json.load(file)
    dataPath = config["dataPath"]
    outputPath = config["outputPath"]
    resourcePath = config["resourcePath"]

carsDF = spark.read \
    .format("json") \
    .option("inferSchema", True) \
    .load(f"{dataPath}/cars.json")

print(carsDF.columns)

# select columns
carsDF.select(col("Name")).show()

# different ways to select columns
carsDF.select(
    col("Acceleration"),
    column("Cylinders"),
    "Displacement"
).show()

carsDF.select("Name", "Year").show()

# adding expressions
carsDF.select(
    col("Name"),
    (col("Weight_in_lbs") / 2.2).alias("Weight_in_kgs"),
    col("Weight_in_lbs")
).show()

# add columns
carsDFWithWeightInKgs = carsDF.withColumn(
    "Weight_in_kgs", col("Weight_in_lbs") / 2.2
)

carsDFWithWeightInKgs.show()

# rename columns
carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight_in_pounds")

carsWithColumnRenamed.show()

# drop columns
carsWithColumnRenamed.drop("Displacement", "Acceleration").show()

# filtering
carsDF.filter(col("Origin") == "USA").show()
print(carsDF.filter(col("Origin") == "USA").count())

carsDF.where(col("Origin") == "USA").show()
print(carsDF.where(col("Origin") == "USA").count())

# filtering and chaining
americanPowerfulCars = carsDF.where(col("Origin") == "USA").where(col("Horsepower") >= 150)
americanPowerfulCars.show()

# union and union all
moreCarsDF = spark.read \
    .format("json") \
    .option("inferSchema", True) \
    .load(f"{dataPath}/more_cars.json")

carsDF.union(moreCarsDF).show()
print(carsDF.union(moreCarsDF).count())  # returns distinct records from both the tables

carsDF.unionAll(moreCarsDF).show()
print(carsDF.unionAll(moreCarsDF).count())  # returns all the records from both tables

# distinct
print(carsDF.select(col("Acceleration")).distinct().count())
print(carsDF.select(col("Acceleration")).count())

# Exercises
# 1. Read the movies DF and select 2 columns of your choice
# 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
# 3. Select all COMEDY movies with IMDB rating above 6
#
# Use as many versions as possible

moviesDF = spark.read \
    .format("json") \
    .option("inferSchema", True) \
    .load(f"{dataPath}/movies.json")

moviesDF.select(
    col("Title"),
    col("Director")
).show()

moviesDF.printSchema()

moviesProfitDF = moviesDF.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross")).alias("Total_Gross")
)

moviesProfitDF.show()

moviesDF \
    .select(col("Title"), col("US_Gross"), col("Worldwide_Gross"), col("US_DVD_Sales")) \
    .withColumn("Total_Profit", col("US_Gross") + col("Worldwide_Gross")).show()

goodMoviesDF = moviesDF \
    .select(col("Title"), col("IMDB_Rating")) \
    .where(col("IMDB_Rating") > 6.0)

goodMoviesDF.show()
