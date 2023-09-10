from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from src.utils import config_loader

spark = SparkSession.builder \
    .appName("Taxi Application") \
    .master("local[2]") \
    .getOrCreate()

config = config_loader("/Users/sagarl/projects/spark-essentials-pyspark/src/config.json")
data_path = config["dataPath"]

big_taxi_df = spark.read \
    .format("parquet") \
    .load("/Volumes/Extreme SSD/Datasets/NYC_taxi_2009-2016.parquet")

taxi_df = spark.read \
    .format("parquet") \
    .load(f"{data_path}/yellow_taxi_jan_25_2018")

taxi_df.createOrReplaceTempView("taxi")

taxi_zones_df = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load(f"{data_path}/taxi_zones.csv")

taxi_zones_df.createOrReplaceTempView("taxi_zones")

# /**
# * Questions:
# *
# * 1. Which zones have the most pickups/dropoffs overall?
# * 2. What are the peak hours for taxi?
# * 3. How are the trips distributed by length? Why are people taking the cab?
# * 4. What are the peak hours for long/short trips?
# * 5. What are the top 3 pickup/dropoff zones for long/short trips?
# * 6. How are people paying for the ride, on long/short trips?
# * 7. How is the payment type evolving with time?
# * 8. Can we explore a ride-sharing opportunity by grouping close short trips?
# *
# */

# * 1. Which zones have the most pickups/dropoffs overall?

taxi_df.show()
taxi_zones_df.show()

join_condition = taxi_df["PULocationID"] == taxi_zones_df["LocationID"]

joined_taxi_df = taxi_df.join(taxi_zones_df, join_condition, "left")
joined_taxi_df.createOrReplaceTempView("joined_taxi")

# joined_taxi_df.show()

popular_pickup_zones_api = joined_taxi_df \
    .groupBy(col("Zone"), col("PULocationID")) \
    .agg(count("*").alias("total_pickups")) \
    .orderBy(col("total_pickups").desc_nulls_last())

# popular_pickup_zones_api.show()

popular_pickup_zones_sql = spark.sql("""
    SELECT PULocationID as pu_location_id, Zone as zone, COUNT(*) as count
    FROM joined_taxi
    GROUP BY zone, pu_location_id
    ORDER BY count DESC
""")

# popular_pickup_zones_sql.show()

popular_pickup_zones_pre_join = taxi_df \
    .groupBy(col("PULocationID")) \
    .agg(count("*").alias("total_pickups")) \
    .join(taxi_zones_df, join_condition, "inner") \
    .drop("Borough", "LocationID", "service_zone") \
    .orderBy(col("total_pickups").desc_nulls_last())

# popular_pickup_zones_pre_join.show()

# * 2. What are the peak hours for taxi?

peak_taxi_hours_api = taxi_df \
    .groupBy(hour(col("tpep_pickup_datetime")).alias("pickup_hour")) \
    .agg(count("*").alias("num_pickups")) \
    .orderBy(col("num_pickups").desc_nulls_last())

# peak_taxi_hours_api.show()

peak_taxi_hours_sql = spark.sql("""
    SELECT HOUR(tpep_pickup_datetime) as pickup_hour, COUNT(*) AS num_pickups
    FROM taxi
    GROUP BY pickup_hour
    ORDER BY num_pickups DESC
""")

# peak_taxi_hours_sql.show()

# * 3. How are the trips distributed by length? Why are people taking the cab?

trip_distance_df = taxi_df.select(col("trip_distance").alias("distance"))
long_distance_threshold = 30
trip_distance_stats_df = trip_distance_df.select(
    count("*").alias("count"),
    lit(long_distance_threshold).alias("threshold"),
    mean(col("distance")).alias("mean"),
    stddev(col("distance")).alias("stddev"),
    min(col("distance")).alias("min"),
    max(col("distance")).alias("max")
)

trips_with_length_df = taxi_df.withColumn("is_long", col("trip_distance") >= 30)
trips_with_length_df.createOrReplaceTempView("trips_with_length")
trips_by_length_df = trips_with_length_df \
    .groupBy(col("is_long")) \
    .count()

# trip_distance_stats_df.show()

# * 4. What are the peak hours for long/short trips?

peak_hours_for_long_trips = taxi_df \
    .where(col("trip_distance") >= long_distance_threshold) \
    .groupBy(hour(col("tpep_pickup_datetime")).alias("pickup_hour")) \
    .agg(count("*").alias("num_pickups")) \
    .orderBy(col("num_pickups").desc_nulls_last())

# peak_hours_for_long_trips.show()

# another way
peak_hours_for_long_trips_2 = trips_with_length_df \
    .withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
    .groupBy(col("pickup_hour"), col("is_long")) \
    .agg(count("*").alias("num_pickups")) \
    .orderBy(col("num_pickups").desc_nulls_last())

# peak_hours_for_long_trips_2.show()

peak_hours_for_long_trips_sql = spark.sql("""
    SELECT HOUR(tpep_pickup_datetime) AS pickup_time, COUNT(*) AS num_pickups, is_long
    FROM trips_with_length
    GROUP BY pickup_time, is_long
    ORDER BY num_pickups DESC 
""")

# peak_hours_for_long_trips_sql.show()

# * 5. What are the top 3 pickup/dropoff zones for long/short trips?

top_3_pickups_api = trips_with_length_df \
    .groupBy(col("PULocationID"), col("is_long")) \
    .agg(count("*").alias("num_pickups")) \
    .join(taxi_zones_df, trips_with_length_df["PULocationID"] == taxi_zones_df["LocationID"]) \
    .drop("Borough", "service_zone", "LocationID") \
    .orderBy(col("num_pickups").desc_nulls_last()) \
    .limit(3)

# top_3_pickups_api.show()

top_3_pickups_sql = spark.sql("""
    SELECT twl.PULocationID as location_id, tz.Zone as zone, COUNT(tpep_pickup_datetime) as num_pickups, is_long
    FROM trips_with_length twl
    INNER JOIN taxi_zones tz
    ON twl.PULocationID = tz.LocationID
    GROUP BY location_id, zone, is_long
    ORDER BY num_pickups DESC
    LIMIT 3
""")
#
# top_3_pickups_sql.show()

# * 6. How are people paying for the ride, on long/short trips?

ratecode_distribution_df = taxi_df \
    .groupBy(col("RatecodeID")) \
    .agg(count("*").alias("total_trips")) \
    .orderBy(col("total_trips").desc_nulls_last())

# ratecode_distribution_df.show()

# * 7. How is the payment type evolving with time?

rate_code_evolution = taxi_df \
    .groupBy(to_date(col("tpep_pickup_datetime")).alias("date"), col("RatecodeID")) \
    .agg(count("*").alias("payment_count")) \
    .orderBy(col("date"))

# rate_code_evolution.show()

# * 8. Can we explore a ride-sharing opportunity by grouping close short trips?
passenger_count_df = taxi_df \
    .where(col("passenger_count") < 3) \
    .select(count("*")) \

# passenger_count_df.show()

group_attempts_df = taxi_df \
    .select(round(unix_timestamp(col("tpep_pickup_datetime")) / 300).cast("integer").alias("five_min_id"),
            col("PULocationID"),
            col("total_amount")) \
    .where(col("passenger_count") < 3) \
    .groupBy(col("five_min_id"), col("PULocationID")) \
    .agg(count("*").alias("total_trips"), sum(col("total_amount")).alias("total_amount")) \
    .orderBy(col("total_trips").desc_nulls_last()) \
    .withColumn("approx_datetime", from_unixtime(col("five_min_id") * 300)) \
    .drop("five_min_id") \
    .join(taxi_zones_df, col("PULocationID") == col("LocationID")) \
    .drop("LocationID", "service_zone")

# group_attempts_df.show()
