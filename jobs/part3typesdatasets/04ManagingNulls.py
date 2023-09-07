from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from src.utils import config_loader

spark = SparkSession.builder \
    .appName("Managing Nulls") \
    .config("spark.master", "local") \
    .getOrCreate()

config = config_loader("/Users/sagarl/projects/spark-streaming-pyspark/src/config.json")
data_path = config["dataPath"]

movies_df = spark.read \
    .format("json") \
    .option("inferSchema", True) \
    .load(f"{data_path}/movies.json")

# select the first non-null value between RT and IMDB * 10 Rating
movies_df.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10).alias("Rating")
)

# selecting null value rows
movies_df.select("*").where(col("Rotten_Tomatoes_Rating").isNull())

# ordering nulls
movies_df.select("*").orderBy(col("IMDB_Rating").desc_nulls_last())

# drop nulls - na.drop() drops any row where either of the columns have NaN or null values
movies_df.select(col("Title"), col("IMDB_Rating")).na.drop()
print(movies_df.select(col("IMDB_Rating")).na.drop().count())

# verify
print(movies_df.where(col("IMDB_Rating").isNotNull()).count())

# replace nulls
movies_df.na.fill(0, ["IMDB_Rating", "Rotten_Tomatoes_Rating"])

movies_df.na.fill({
    "IMDB_Rating": 0,
    "Rotten_Tomatoes_Rating": 10,
    "Director": "Unknown"
    }
)

movies_df.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    # coalesce - selects the first value if "non-null", else the second value
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10).alias("coalesce"),
    # nanvl - returns the first value if not "NaN" else second value
    nanvl(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10).alias("nanvl"),
    when(col("Rotten_Tomatoes_Rating") == col("IMDB_Rating") * 10, None).otherwise(col("Rotten_Tomatoes_Rating")).alias(
        "nullif"),
    when(col("Rotten_Tomatoes_Rating") != col("IMDB_Rating") * 10, col("IMDB_Rating")).otherwise(0).alias("nvl2")
).show()
