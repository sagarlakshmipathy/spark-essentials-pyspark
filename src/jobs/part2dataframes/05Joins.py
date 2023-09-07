from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from src.utils import config_loader

spark = SparkSession.builder \
    .appName("Joins") \
    .config("spark.master", "local") \
    .config("spark.jars", "/Users/sagarl/projects/spark-essentials-pyspark/dependencies/postgresql-42.6.0.jar") \
    .getOrCreate()

config = config_loader("/Users/sagarl/projects/spark-streaming-pyspark/src/config.json")
data_path = config["dataPath"]
driver = config["driver"]
url = config["url"]
user = config["user"]
password = config["password"]

guitars_df = spark.read \
    .format("json") \
    .option("inferSchema", True) \
    .load(f"{data_path}/guitars.json")
guitars_df.printSchema()

guitar_players_df = spark.read \
    .format("json") \
    .option("inferSchema", True) \
    .load(f"{data_path}/guitarPlayers.json")
guitar_players_df.printSchema()

bands_df = spark.read \
    .format("json") \
    .option("inferSchema", True) \
    .load(f"{data_path}/bands.json")
bands_df.printSchema()

# join condition
guitar_players_bands_join_condition = guitar_players_df.band == bands_df.id

# inner join
guitar_players_bands_df = guitar_players_df.join(bands_df, guitar_players_bands_join_condition, "inner")
guitar_players_bands_df.show()

# full outer join
guitar_players_df.join(bands_df, guitar_players_bands_join_condition, "outer").show()

# left outer
guitar_players_df.join(bands_df, guitar_players_bands_join_condition, "left_outer").show()

# right outer
guitar_players_df.join(bands_df, guitar_players_bands_join_condition, "right_outer").show()

# left semi
guitar_players_df.join(bands_df, guitar_players_bands_join_condition, "left_semi").show()

# anti joins
guitar_players_df.join(bands_df, guitar_players_bands_join_condition, "left_anti").show()

# this will crash because the join condition created an ambiguous situation while joining because of "id"
# guitarPlayersBandsDF.select(col("id"), col("band")).show()

# option 1 - change the column name while joining
guitar_players_df.join(bands_df.withColumnRenamed("id", "band"), "band", "inner").show()

# option 2 - rename the offending column and keep the data
bands_mod_df = bands_df.withColumnRenamed("id", "band_id")
guitar_players_bands_mod_df = guitar_players_df.join(
    bands_mod_df, guitar_players_df.band == bands_mod_df.band_id, "inner"
)

guitar_players_bands_mod_df.show()

guitar_players_bands_mod_df.select(col("id"), col("band")).show()

# using complex types
guitars_mod_df = guitars_df.withColumnRenamed("id", "guitar_id")

# option 1 - convert the array of longs/ints to string
# you have to modify the list of long as strings
guitar_player_mod_df = guitar_players_df.withColumn("guitars", col("guitars").cast("string"))

# implicitly contains does the comparison after converting the col(guitars) to string
guitar_player_mod_df.join(guitars_mod_df, col("guitars").contains(col("guitar_id"))).show()

# option 2 - use expr
guitar_players_df.join(guitars_mod_df, expr("array_contains(guitars, guitar_id)")).show()

# Exercises
#
# 1. Show all employees and their max salary
# 2. Show all employees who were never managers
# 3. Find the job titles of the best paid 10 employees in the company


employees_df = spark.read \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("user", user) \
    .option("password", password) \
    .option("dbtable", "public.employees") \
    .load()

salaries_df = spark.read \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("user", user) \
    .option("password", password) \
    .option("dbtable", "public.salaries") \
    .load()

dept_managers_df = spark.read \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("user", user) \
    .option("password", password) \
    .option("dbtable", "public.dept_manager") \
    .load()

titles_df = spark.read \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("user", user) \
    .option("password", password) \
    .option("dbtable", "public.titles") \
    .load()

# 1
employees_salaries_df = employees_df.join(salaries_df, "emp_no")

employees_max_salaries_df = employees_salaries_df \
    .groupBy(col("emp_no")) \
    .agg(max(col("salary")).alias("max_salary")) \
    .select(col("emp_no"), col("max_salary")) \
    .orderBy(desc_nulls_last(col("max_salary")))

# employeesMaxSalariesDF.show()

# 2
employees_dept_managers_df = employees_df.join(dept_managers_df, "emp_no", "left_anti")

employees_not_managers_df = employees_dept_managers_df \
    .select(col("emp_no"))

# employeesNotManagersDF.show()

# 3

employees_salaries_titles_df = employees_salaries_df.join(titles_df, "emp_no")

top_paid_titles_df = employees_salaries_titles_df \
    .where(titles_df.to_date == "9999-01-01") \
    .orderBy(desc_nulls_last(col("salary"))) \
    .select(col("emp_no"), col("title"), col("salary")) \
    .limit(10)

top_paid_titles_df.show()
