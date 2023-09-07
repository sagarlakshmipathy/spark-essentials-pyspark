from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.utils import config_loader

spark = SparkSession.builder \
    .appName("Spark SQL") \
    .master("local[2]") \
    .config("spark.sql.warehouse.dir", "/Users/sagarl/projects/spark-essentials-pyspark/src/resources/warehouse") \
    .getOrCreate()

config = config_loader("/Users/sagarl/projects/spark-essentials-pyspark/src/config.json")
data_path = config["dataPath"]

cars_df = spark.read \
    .format("json") \
    .option("inferSchema", True) \
    .load(f"{data_path}/cars.json")


# regular DF API
usa_cars_api = cars_df \
    .select(col("Name")) \
    .where(col("Origin") == "USA")

# usa_cars_api.show()


# spark SQL
cars_df.createOrReplaceTempView("cars")
usa_cars_sql = spark.sql("""
    SELECT Name
    FROM cars
    WHERE Origin = "USA"
""")
usa_cars_sql.show()
