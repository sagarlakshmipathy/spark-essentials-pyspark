from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from src.utils import config_loader

spark = SparkSession.builder \
    .appName("Aggregations and Grouping") \
    .config("spark.master", "local") \
    .getOrCreate()

config = config_loader("/Users/sagarl/projects/spark-streaming-pyspark/src/config.json")
data_path = config["dataPath"]

moviesDF: DataFrame = spark.read \
    .format("json") \
    .option("inferSchema", True) \
    .load(f"{data_path}/movies.json")

# count genre as an int
genreCount = moviesDF.select(col("Major_Genre")).count()
print(genreCount)

# count as a DF
genreCountDF = moviesDF.select(count(col("Major_Genre")))
genreCountDF.show()

# count all
totalRowCountDF = moviesDF.select(count("*"))
totalRowCountDF.show()

totalRowCount = moviesDF.count()
print(totalRowCount)

# count distinct
distinctGenreDF = moviesDF.select(
    count_distinct(col("Major_Genre")).alias("major_genre_count")
)
distinctGenreDF.show()

# approx count
moviesDF.select(
    approx_count_distinct(col("Major_Genre")).alias("approx_major_genre_count")
).show()

# min max

moviesDF.select(min(col("IMDB_Rating")).alias("min_rating")).show()

minRatingDF: DataFrame = moviesDF.select(min(col("IMDB_Rating")).alias("min_rating"))

# Get the minimum rating value
minRating: object = minRatingDF.first()["min_rating"]

# Filter rows with the minimum IMDB rating and select desired columns
minRatingDF = moviesDF.filter(col("IMDB_Rating") == minRating).select(col("Title"), col("IMDB_Rating"))

# Show the DataFrame
minRatingDF.show()

# another way
minRating = moviesDF.select(min(col("IMDB_Rating"))).first()[0]
print(minRating)

moviesDF \
    .where(col("IMDB_Rating") == minRating) \
    .select(col("Title"), col("IMDB_Rating")) \
    .show()

# sum
moviesDF.select(sum(col("US_Gross")).alias("Total_US_Gross")).show()

# avg
moviesDF.select(avg(col("Rotten_Tomatoes_Rating")).alias("avg_rt_rating")).show()

# data science
moviesDF.select(mean(col("Rotten_Tomatoes_Rating")).alias("mean_rt_rating")).show()
moviesDF.select(stddev(col("Rotten_Tomatoes_Rating")).alias("mean_rt_rating")).show()

# grouping
# count
countByGenreDF: object = moviesDF \
    .groupBy(col("Major_Genre")) \
    .count()

countByGenreDF.show()

# avg
moviesDF \
    .groupBy(col("Major_Genre")) \
    .agg(round(avg(col("IMDB_Rating")), 2).alias("avg_imdb_rating")) \
    .show()

moviesDF \
    .groupBy(col("Major_Genre")) \
    .agg(
        count("*").alias("no_of_movies"),
        round(avg(col("IMDB_Rating")), 2).alias("avg_rating")) \
    .orderBy(col("avg_rating").desc_nulls_last()) \
    .show()

# Exercises
#
# 1. Sum up ALL the profits of ALL the movies in the DF
# 2. Count how many distinct directors we have
# 3. Show the mean and standard deviation of US gross revenue for the movies
# 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR

moviesDF \
    .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).alias("Total_Profit")) \
    .select(sum(col("Total_Profit"))) \
    .show()

moviesDF \
    .select(count_distinct(col("Director")).alias("no_of_directors")) \
    .show()

moviesDF \
    .select(
        mean(col("US_Gross")).alias("mean_us_gross"),
        stddev(col("US_Gross")).alias("stddev_us_gross"))\
    .show()

moviesDF \
    .groupBy(col("Director")) \
    .agg(
        avg(col("IMDB_Rating")).alias("avg_imdb_rating"),
        avg(col("US_Gross")).alias("avg_us_gross")) \
    .select(col("Director"), col("avg_imdb_rating"), col("avg_us_gross")) \
    .orderBy(col("avg_imdb_rating")) \
    .show()

moviesDF \
    .groupBy(col("Director")) \
    .agg(
        avg(col("IMDB_Rating")).alias("avg_imdb_rating"),
        avg(col("US_Gross")).alias("avg_us_gross"))\
    .orderBy(col("avg_imdb_rating")) \
    .show()
