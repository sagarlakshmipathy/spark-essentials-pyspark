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
driver = config["driver"]
url = config["url"]
user = config["user"]
password = config["password"]

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
# usa_cars_sql.show()

# all kinds of SQL commands will work
spark.sql("CREATE DATABASE rtjvm")
spark.sql("USE rtjvm")
databases_df = spark.sql("SHOW DATABASES")
# databases_df.show()


def read_table(table_name):
    return spark.read \
        .format("jdbc") \
        .option("driver", driver) \
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("table", table_name) \
        .load()


def transfer_tables(table_names):
    for table_name in table_names:
        table_df = read_table(table_name)
        table_df.createOrReplaceTempView(f"{table_name}")

        table_df.write \
            .mode("overwrite") \
            .saveAsTable(table_name)

transfer_tables([
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager"])
