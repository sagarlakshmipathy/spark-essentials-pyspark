from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def main():
    if len(sys.argv) != 3:
        print("Need input path and output path")
        sys.exit(1)

    spark = SparkSession.builder \
        .appName("Test Deploy App") \
        .master("local[2]") \
        .getOrCreate()

    movies_df = spark.read \
        .format("json") \
        .option("inferSchema", True) \
        .load(sys.argv[1])

    good_comedies_df = movies_df.select(
            col("Title"),
            col("IMDB_Rating").alias("Rating"),
            col("Release_Date").alias("Release")) \
        .where((col("Major_Genre") == "Comedy") & (col("IMDB_Rating") > 6.5)) \
        .orderBy(col("Rating").desc_nulls_last())

    good_comedies_df.show()

    good_comedies_df.write \
        .mode("overwrite") \
        .format("json") \
        .save(sys.argv[2])


if __name__ == '__main__':
    main()
