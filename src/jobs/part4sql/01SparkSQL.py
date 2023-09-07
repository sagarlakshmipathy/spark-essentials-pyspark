from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.utils import config_loader

spark = SparkSession.builder \
    .appName("Spark SQL") \
    .master("local[2]") \
    .config("spark.sql.warehouse.dir", "/Users/sagarl/projects/spark-essentials-pyspark/src/resources/warehouse") \
    .config("spark.jars", "/Users/sagarl/projects/dependencies/pyspark/postgresql-42.6.0.jar") \
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
        .option("dbtable", table_name) \
        .load()


def transfer_tables(table_names):
    for table_name in table_names:
        table_df = read_table(table_name)
        table_df.createOrReplaceTempView(f"{table_name}")

        # table_df.write \
        #     .mode("overwrite") \
        #     .saveAsTable(table_name)


transfer_tables([
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager"])

employee_table_from_wh = spark.read.table("employees")
# employee_table_from_wh.show()

employee_table_thru_sql = spark.sql("""
    SELECT *
    FROM employees    
""")
# employee_table_thru_sql.show()

# /**
# * Exercises
# *
# * 1. Read the movies DF and store it as a Spark table in the rtjvm database.
# * 2. Count how many employees were hired in between Jan 1 1999 and Jan 1 2000.
# * 3. Show the average salaries for the employees hired in between those dates, grouped by department.
# * 4. Show the name of the best-paying department for employees hired in between those dates.
# */

# first
movies_df = spark.read \
    .format("json") \
    .option("inferSchema", True) \
    .load(f"{data_path}/movies.json")

# movies_df.write \
#     .mode("overwrite") \
#     .saveAsTable("movies")

# second
employees_hired_in_1999 = spark.sql("""
    SELECT COUNT(*) as num_employees
    FROM employees
    WHERE hire_date BETWEEN "1999-01-01" AND "2000-01-01"
""")
# employees_hired_in_1999.show()

# third
avg_salaries_by_dept_in_19999 = spark.sql("""
    SELECT de.dept_no, ROUND(AVG(s.salary), 2) as avg_salary
    FROM employees e
    INNER JOIN dept_emp de
    ON e.emp_no = de.emp_no
    INNER JOIN salaries s
    ON e.emp_no = s.emp_no
    WHERE e.hire_date BETWEEN "1999-01-01" AND "2000-01-01"
    GROUP BY de.dept_no
    ORDER BY avg_salary DESC
""")

# avg_salaries_by_dept_in_19999.show()

# fourth
best_paying_dept_in_1999 = spark.sql("""
    SELECT d.dept_no, d.dept_name, sub.max_salary
    FROM 
        (SELECT de.dept_no, ROUND(MAX(s.salary), 2) as max_salary
        FROM employees e
        INNER JOIN dept_emp de
        ON e.emp_no = de.emp_no
        INNER JOIN salaries s
        ON e.emp_no = s.emp_no
        WHERE e.hire_date BETWEEN "1999-01-01" AND "2000-01-01"
        GROUP BY de.dept_no
        ORDER BY max_salary DESC
        LIMIT 1) as sub
    INNER JOIN departments d
    ON sub.dept_no = d.dept_no
""")

# best_paying_dept_in_1999.show()
