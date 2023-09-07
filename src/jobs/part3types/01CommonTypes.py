from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from src.utils import config_loader

spark = SparkSession.builder \
    .appName("Common Spark Types") \
    .config("spark.master", "local") \
    .getOrCreate()

config = config_loader("/Users/sagarl/projects/spark-streaming-pyspark/src/config.json")
data_path = config["dataPath"]
output_path = config["outputPath"]
driver = config["driver"]
url = config["url"]
user = config["user"]
password = config["password"]

movies_df = spark.read \
    .format("json") \
    .option("inferSchema", True) \
    .load(f"{data_path}/movies.json")

# create a plain value column to a DF
movies_df.select(col("Title"), lit(47).alias("plain_value")).show()

# booleans
dramaFilter = col("Major_Genre") == "Drama"
goodRatingFilter = col("IMDB_Rating") >= 7.0
preferredFilter = dramaFilter & goodRatingFilter

movies_df.select(col("Title"), col("Major_Genre"), col("IMDB_Rating")).where(dramaFilter).show()
movies_df.select(col("Title"), col("Major_Genre"), col("IMDB_Rating")).where(goodRatingFilter).show()
movies_df.select(col("Title"), col("Major_Genre"), col("IMDB_Rating")).where(preferredFilter).show()

movies_with_goodness_flags_df = movies_df.select(col("Title"), preferredFilter.alias("good_movie"))
movies_with_goodness_flags_df.show()

# moviesWithGoodnessFlagsDF.where(col("good_movie")).show()

# negations
# moviesWithGoodnessFlagsDF.where(~col("good_movie")).show()

# numbers
# math operators
movies_avg_rating_df = movies_df.select(
    col("Title"),
    col("IMDB_Rating"),
    col("Rotten_Tomatoes_Rating"),
    (((col("Rotten_Tomatoes_Rating") / 10) + col("IMDB_Rating")) / 2).alias("avg_rating")
)
# moviesAvgRatingDF.show()

# correlation
print(movies_df.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating"))

# strings
cars_df = spark.read \
    .format("json") \
    .option("inferSchema", True) \
    .load(f"{data_path}/cars.json")

# initcap, lower, upper
cars_df.select(initcap(col("Name"))).show()

# select(*) is not needed
cars_df \
    .where(col("Name").contains("volkswagen")) \
    .select("*") \
    .show()

# regex
regex_string = "volkswagen|vw"
vw_df = cars_df.select(
    col("Name"),
    regexp_extract(col("Name"), regex_string, 0).alias("regex_extract")
).where(col("regex_extract") != "").drop("regex_extract")

vw_df.show()

vw_df.select(
    col("Name"),
    regexp_replace(col("Name"), regex_string, "People's Car").alias("regex_replace"))\
    .show()

# Exercise
#
# Filter the cars DF by a list of car names obtained by an API call
# Versions:
#   - contains
#   - regexes

car_names = ["Volkswagen", "Mercedes-Benz", "Ford"]
lower_car_names = list(map(str.lower, car_names))
print(lower_car_names)

# print([carName.lower() for carName in carNames])

filter_condition = None
for carName in lower_car_names:
    if filter_condition is None:
        filter_condition = (col("Name").contains(carName))
        print(filter_condition)
    else:
        filter_condition |= (col("Name").contains(carName))
        print(filter_condition)

print(filter_condition)

filtered_cars_contains_df = cars_df \
    .filter(filter_condition)

filtered_cars_contains_df.show()

# 2
regex_pattern = "|".join(map(str.lower, car_names))
# "|".join([carName.lower() for carName in carNames])
# "|".join(list(map(str.lower, carNames)))


filtered_cars_regex_df = cars_df \
    .select(
        col("*"),
        regexp_extract(col("Name"), regex_pattern, 0).alias("regex_extract")) \
    .where(col("regex_extract") != "") \
    .drop("regex_extract")

filtered_cars_regex_df.show()
