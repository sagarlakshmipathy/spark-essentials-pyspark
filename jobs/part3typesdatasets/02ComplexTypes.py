from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from src.utils import config_loader

spark = SparkSession.builder \
    .appName("Complex spark types") \
    .config("spark.master", "local") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

config = config_loader("/Users/sagarl/projects/spark-streaming-pyspark/src/config.json")
data_path = config["dataPath"]

movies_df = spark.read \
    .format("json") \
    .option("inferSchema", True) \
    .load(f"{data_path}/movies.json")

# dates
movies_with_release_dates = movies_df \
    .select(col("Title"), to_date(col("Release_Date"), 'dd-MMM-yy').alias("Actual_Release"))

movies_with_release_dates.show()

movies_with_release_dates \
    .withColumn("Today", current_date()) \
    .withColumn("Right_Now", current_timestamp()) \
    .withColumn("Years_Since_Release", round(datediff(current_date(), col("Actual_Release")) / 365, 2)) \
    .show()

# movies without release dates
movies_with_release_dates \
    .select(col("*")) \
    .where(col("Actual_Release").isNull()) \
    .show()

# Exercise
# 1. How do we deal with multiple date formats?
# 2. Read the stocks DF and parse the dates

stocks_df = spark.read \
    .format("csv") \
    .option("header", True) \
    .load(f"{data_path}/stocks.csv")

stocks_df \
    .withColumn("actual_date", to_date(col("date"), "MMM dd yyyy")) \
    .drop("date") \
    .show()

# structures

# with col operators
movies_df \
    .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).alias("Profit")) \
    .show()

movies_df \
    .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).alias("Profit")) \
    .select(col("Title"), col("Profit").getField("US_Gross").alias("US_Profit")) \
    .show()

# arrays
# finds a space or a comma, when it finds either one, pyspark splits the words
movies_with_words_df = movies_df \
    .select(col("Title"), split(col("Title"), " |,").alias("Title_Words"))

movies_with_words_df.show(truncate=False)

# finds a space, when it finds one, pyspark splits the words
movies_df \
    .select(col("Title"), split(col("Title"), " ").alias("Title_Words")) \
    .show(truncate=False)

# finds a comma, when it finds one, pyspark splits the words
movies_df \
    .select(col("Title"), split(col("Title"), " ,").alias("Title_Words")) \
    .show(truncate=False)

movies_with_words_df.select(
    col("Title"),
    (col("Title_Words")[0]).alias("First_Word_Of_Title"),
    array_contains(col("Title_Words"), "Love").alias("Movies_Titled_Love")) \
    .show(truncate=False)

movies_with_words_df.select(
    col("Title"),
    (col("Title_Words")[0]).alias("First_Word_Of_Title"),
    array_contains(col("Title_Words"), "Love").alias("Movies_Titled_Love")) \
    .where(col("Movies_Titled_Love")) \
    .drop("Movies_Titled_Love") \
    .show(truncate=False)
