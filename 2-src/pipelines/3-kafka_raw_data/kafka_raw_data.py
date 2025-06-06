#!/usr/bin/env python3
"""
Optimized Kafka to PostgreSQL Pipeline
Minimal, efficient schema
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

spark = SparkSession.builder \
    .appName("Optimized Kafka to PostgreSQL") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.6.0") \
    .master("local[*]") \
    .getOrCreate()

# Loglama seviyesini ayarla
spark.sparkContext.setLogLevel("WARN")

# Tek kayıt için schema (eski koddaki record_schema'ya benzer)
record_schema = StructType([
    StructField("raw_data", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("line_number", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("profile_type", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", StringType(), True),
    StructField("month_num", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("date", StringType(), True),
    StructField("interval_id", StringType(), True),
    StructField("interval_idx", IntegerType(), True),
    StructField("hour", IntegerType(), True),
    StructField("minute", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("load_percentage", DoubleType(), True),
    StructField("batch_id", StringType(), True),
    StructField("processing_timestamp", StringType(), True)
])

# Ana JSON schema (eski koddaki json_schema'ya benzer)
json_schema = StructType([
    StructField("interval_id", StringType(), True),
    StructField("batch_id", IntegerType(), True),
    StructField("record_count", IntegerType(), True),
    StructField("processing_timestamp", StringType(), True),
    StructField("records", ArrayType(record_schema), True)
])

jdbc_url = "jdbc:postgresql://development-postgres:5432/datawarehouse"
connection_properties = {
    "user": "datauser",
    "password": "datapass",
    "driver": "org.postgresql.Driver"
}

try:
    # Kafka stream
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "development-kafka1:9092") \
        .option("subscribe", "sensor-data") \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 10000) \
        .load()
    
    print("Successfully connected to Kafka!")
    
    # JSON formatındaki veriyi parse et
    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json_data") \
        .select(from_json("json_data", json_schema).alias("data")) \
        .select("data.*")
    
    # Records array'ini explode et (her bir kaydı ayrı bir satır yap)
    exploded_df = parsed_df \
        .selectExpr("batch_id", "record_count", "processing_timestamp", "explode(records) as record")
    
    # Final processing - yeni tablo yapısına göre kolonları seç
    final_df = exploded_df \
        .select(
            col("record.customer_id"),
            col("record.profile_type"),
            col("record.day").alias("day_num"),
            col("record.hour"),
            col("record.minute"),
            col("record.interval_idx"),
            col("record.load_percentage"),
            to_timestamp(col("record.timestamp")).alias("full_timestamp"),
            col("record.batch_id"),
            to_timestamp(col("record.processing_timestamp")).alias("processing_timestamp")
        ) \
        .withColumn("created_at", current_timestamp())
    
    def write_to_postgres(batch_df, batch_id):
        if batch_df.isEmpty():
            print(f"Batch {batch_id}: Empty")
            return
        
        count = batch_df.count()
        print(f"Batch {batch_id}: Processing {count} records")
        
        # Debug: İlk 5 kaydı göster
        print("Sample records:")
        batch_df.show(5, truncate=False)
        
        # Write to optimized table
        batch_df.write \
            .jdbc(
                url=jdbc_url,
                table="kafka_raw_data",
                mode="append",
                properties=connection_properties
            )
        
        print(f"Batch {batch_id}: {count} records written to kafka_raw_data")
    
    query = final_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .start()
    
    print("🚀 Optimized streaming started!")
    query.awaitTermination()

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

spark.stop()