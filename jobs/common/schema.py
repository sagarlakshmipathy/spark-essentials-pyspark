from pyspark.sql.types import *

carsSchema = StructType([
    StructField("Name", StringType()),
    StructField("Miles_per_Gallon", DoubleType()),
    StructField("Cylinders", LongType()),
    StructField("Displacement", DoubleType()),
    StructField("Horsepower", LongType()),
    StructField("Weight_in_lbs", LongType()),
    StructField("Acceleration", DoubleType()),
    StructField("Year", StringType()),
    StructField("Origin", StringType())
])

smartphoneSchema = StructType([
    StructField("Brand", StringType()),
    StructField("Model", StringType()),
    StructField("OS", StringType()),
    StructField("Version", IntegerType())
])

stocksSchema = StructType([
    StructField("symbol", StringType()),
    StructField("date", DateType()),
    StructField("price", DoubleType())
])