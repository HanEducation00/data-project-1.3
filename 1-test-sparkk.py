from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Simple Kafka Test") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .master("local[*]") \
    .getOrCreate()

# Print Spark version and master info
print(f"Spark Version: {spark.version}")
print(f"Spark Master: {spark.sparkContext.master}")
print(f"Available Executors: {spark.sparkContext.defaultParallelism}")

# Create a simple DataFrame
data = [
    (1, "John", 25, "New York", 5000),
    (2, "Lisa", 35, "Chicago", 6000),
    (3, "Mike", 45, "San Francisco", 7500),
    (4, "Emily", 30, "Boston", 4500),
    (5, "David", 40, "Seattle", 8000)
]

schema = ["id", "name", "age", "city", "salary"]
df = spark.createDataFrame(data, schema)

# Show the DataFrame
print("\nOriginal DataFrame:")
df.show()

# Perform some operations
# 1. Filter rows
filtered_df = df.filter(col("age") > 30)
print("\nEmployees older than 30:")
filtered_df.show()

# 2. Group by and aggregate
by_city = df.groupBy("city").agg(
    {"salary": "avg", "age": "avg"}
).withColumnRenamed("avg(salary)", "avg_salary") \
 .withColumnRenamed("avg(age)", "avg_age")

print("\nAverage salary and age by city:")
by_city.show()

# 3. Create a new column
df_with_bonus = df.withColumn("bonus", expr("salary * 0.1"))
print("\nEmployees with bonus:")
df_with_bonus.show()

# Stop the Spark Session
spark.stop()
print("\nSpark session stopped successfully.")


