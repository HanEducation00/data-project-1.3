#!/usr/bin/env python3
"""
FIXED: Kafka Connection Issue - Based on Working Example
Enhanced ML Pipeline with correct Kafka configuration
"""
import os
import math
import mlflow
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, current_timestamp, explode,
    date_format, month, dayofweek, quarter, when, lit, sin, cos,
    avg as spark_avg, min as spark_min, max as spark_max, count as spark_count,
    coalesce, isnan, isnull, abs, to_date, sum as spark_sum, mean as spark_mean,
    get_json_object)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

# Logger setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ FIXED: Spark session with CORRECT Kafka package version
spark = SparkSession.builder \
    .appName("FIXED ML Prediction Pipeline") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ✅ FIXED: Schema based on working example
record_schema = ArrayType(StructType([
    StructField("customer_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("load_percentage", DoubleType(), True),
    StructField("date", StringType(), True),
    StructField("month", StringType(), True),
    StructField("month_num", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("hour", IntegerType(), True),
    StructField("minute", IntegerType(), True)
]))

# Database configs
jdbc_url = "jdbc:postgresql://development-postgres:5432/datawarehouse"
connection_properties = {
    "user": "datauser",
    "password": "datapass",
    "driver": "org.postgresql.Driver"
}

# MLflow config
mlflow.set_tracking_uri("http://mlflow-server:5000")
models_cache = {}
current_season = None

def get_current_season():
    """Determine current season based on month"""
    month = datetime.now().month
    if month in [12, 1, 2]:
        return "winter"
    elif month in [3, 4, 5]:
        return "spring"
    elif month in [6, 7, 8]:
        return "summer"
    else:
        return "autumn"

def load_kafka_data_working_method(spark):
    """✅ WORKING METHOD: Based on successful example"""
    logger.info("📁 LOADING DATA FROM KAFKA - WORKING METHOD...")
    
    # ✅ CORRECT: Kafka address from working example
    kafka_address = "development-kafka1:9092"
    logger.info(f"🔍 Kafka address: {kafka_address}")
    
    try:
        # ✅ WORKING: Read from Kafka (same as successful code)
        kafka_df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_address) \
            .option("subscribe", "sensor-data") \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()
        
        # ✅ WORKING: Parse JSON (same method)
        parsed_df = kafka_df.select(
            col("value").cast("string").alias("json_data"),
            col("timestamp").alias("kafka_timestamp")
        )
        
        # ✅ WORKING: Extract records
        level1_df = parsed_df.withColumn(
            "records_json", get_json_object(col("json_data"), "$.records")
        )
        
        # ✅ WORKING: Explode records
        exploded_df = level1_df.withColumn(
            "records_array", from_json(col("records_json"), record_schema)
        ).select(
            explode(col("records_array")).alias("record")
        ).select(
            col("record.customer_id").alias("customer_id"),
            col("record.timestamp").alias("timestamp_str"),
            col("record.load_percentage").alias("load_percentage"),
            col("record.date").alias("date"),
            col("record.month").alias("month"),
            col("record.month_num").alias("month_num")
        )
        
        # ✅ WORKING: Convert timestamp
        final_df = exploded_df.withColumn(
            "full_timestamp", to_timestamp(col("timestamp_str"), "yyyy-MM-dd HH:mm:ss")
        ).select("customer_id", "full_timestamp", "load_percentage")
        
        final_df.cache()
        count = final_df.count()
        
        logger.info(f"✅ Kafka data loaded: {count:,} records")
        
        # Show sample
        logger.info("📋 Sample Kafka data:")
        final_df.show(5, truncate=False)
        
        return final_df, count
        
    except Exception as e:
        logger.error(f"❌ Error loading Kafka data: {e}")
        import traceback
        traceback.print_exc()
        return None, 0

def create_daily_aggregation(df):
    """Create daily energy aggregation for ML"""
    logger.info("📊 Creating daily aggregation...")
    
    # Daily aggregation
    daily_agg = df.groupBy(
        to_date(col("full_timestamp")).alias("date")
    ).agg(
        spark_sum("load_percentage").alias("daily_total_energy"),
        spark_avg("load_percentage").alias("daily_avg_energy"),
        spark_count("*").alias("total_customers"),
        spark_min("load_percentage").alias("daily_min"),
        spark_max("load_percentage").alias("daily_max")
    ).withColumn("daily_peak", col("daily_max"))
    
    # Add temporal features
    daily_agg = daily_agg.withColumn("month", month("date")) \
                         .withColumn("dayofweek", dayofweek("date")) \
                         .withColumn("quarter", quarter("date")) \
                         .withColumn("is_weekend",
                                    when(col("dayofweek").isin([1, 7]), 1).otherwise(0)) \
                         .withColumn("season",
                                    when(col("month").isin([12, 1, 2]), 0)
                                    .when(col("month").isin([3, 4, 5]), 1)
                                    .when(col("month").isin([6, 7, 8]), 2)
                                    .otherwise(3))
    
    # Add cyclical features
    PI = lit(math.pi)
    daily_agg = daily_agg.withColumn("avg_sin_month",
                                    coalesce(sin(col("month") * (2.0 * PI / 12)), lit(0.0))) \
                         .withColumn("avg_cos_month",
                                    coalesce(cos(col("month") * (2.0 * PI / 12)), lit(0.0)))
    
    # Add lag features (simplified)
    daily_agg = daily_agg.withColumn("avg_lag_1h", lit(0.0)) \
                         .withColumn("avg_lag_6h", lit(0.0)) \
                         .withColumn("avg_lag_24h", lit(0.0)) \
                         .withColumn("avg_rolling_4h", coalesce(col("daily_avg_energy"), lit(0.0))) \
                         .withColumn("avg_rolling_24h", coalesce(col("daily_avg_energy"), lit(0.0))) \
                         .withColumn("avg_sin_hour", lit(0.0)) \
                         .withColumn("avg_cos_hour", lit(0.0))
    
    return daily_agg

def simple_ml_prediction(df):
    """Simple ML prediction for testing"""
    logger.info("🤖 Making simple predictions...")
    
    # Add a simple prediction (for testing)
    predicted_df = df.withColumn("prediction", 
                                col("daily_avg_energy") * 1.1)  # Simple 10% increase
    
    return predicted_df

def main_fixed():
    """Fixed main execution based on working example"""
    try:
        logger.info("🔧 FIXED PIPELINE STARTING...")
        logger.info("=" * 60)
        
        # 1. Load data using working method
        df, count = load_kafka_data_working_method(spark)
        
        if count == 0:
            logger.error("❌ No data loaded from Kafka!")
            return
        
        logger.info(f"✅ Data loaded successfully: {count:,} records")
        
        # 2. Create daily aggregation
        daily_df = create_daily_aggregation(df)
        daily_count = daily_df.count()
        
        logger.info(f"📊 Daily aggregation: {daily_count} days")
        daily_df.show(5, truncate=False)
        
        # 3. Simple ML prediction
        predicted_df = simple_ml_prediction(daily_df)
        
        logger.info("📈 Predictions sample:")
        predicted_df.select("date", "daily_total_energy", "prediction").show(5, truncate=False)
        
        # 4. Save to database (optional)
        try:
            predicted_df.select(
                col("date"),
                col("daily_total_energy").alias("actual_daily_total"),
                col("prediction").alias("predicted_daily_total"),
                col("total_customers")
            ).write \
             .jdbc(
                 url=jdbc_url,
                 table="daily_predictions_test",
                 mode="overwrite",
                 properties=connection_properties
             )
            
            logger.info("✅ Predictions saved to database!")
            
        except Exception as e:
            logger.warning(f"⚠️ Database save failed: {e}")
        
        logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        
    except Exception as e:
        logger.error(f"❌ Pipeline error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()
        logger.info("✅ Spark session stopped")

if __name__ == "__main__":
    main_fixed()
