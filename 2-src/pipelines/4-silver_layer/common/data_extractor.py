#!/usr/bin/env python3
"""
Data Extractor for Silver Layer
Extract and aggregate daily data from PostgreSQL kafka_raw_data
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg as spark_avg, max as spark_max, 
    count, countDistinct, to_date, date_format, month, year
)
from .config import JDBC_URL, CONNECTION_PROPERTIES, logger

def create_spark_session(app_name="Silver Layer Data Extractor"):
    """Create Spark session with PostgreSQL driver"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .master("local[*]") \
        .getOrCreate()

def extract_daily_aggregated_data(spark, start_date=None, end_date=None, season=None):
    """
    Extract and aggregate daily data from kafka_raw_data table
    Schema: customer_id | profile_type | day_num | hour | minute | interval_idx | 
            load_percentage | full_timestamp | batch_id | processing_timestamp | created_at
    """
    logger.info("📊 Extracting daily aggregated data from PostgreSQL...")
    
    # Base query
    base_query = """
    SELECT customer_id, profile_type, day_num, hour, minute, interval_idx,
           load_percentage, full_timestamp, batch_id, processing_timestamp, created_at
    FROM kafka_raw_data
    WHERE full_timestamp IS NOT NULL
    """
    
    # Add date filters if provided
    conditions = []
    if start_date:
        conditions.append(f"DATE(full_timestamp) >= '{start_date}'")
    if end_date:
        conditions.append(f"DATE(full_timestamp) <= '{end_date}'")
    
    if conditions:
        base_query += " AND " + " AND ".join(conditions)
    
    base_query += " ORDER BY full_timestamp"
    
    logger.info(f"🔍 SQL Query: {base_query}")
    
    # Read from PostgreSQL
    raw_df = spark.read \
        .jdbc(
            url=JDBC_URL,
            table=f"({base_query}) as subquery",
            properties=CONNECTION_PROPERTIES
        )
    
    logger.info(f"📥 Raw records loaded: {raw_df.count()}")
    
    # Create date column for grouping
    raw_df = raw_df.withColumn("date", to_date(col("full_timestamp")))
    
    # Daily aggregation
    daily_df = raw_df.groupBy("date") \
        .agg(
            spark_sum("load_percentage").alias("total_daily_energy"),
            spark_avg("load_percentage").alias("avg_daily_load"),
            spark_max("load_percentage").alias("peak_daily_load"),
            countDistinct("customer_id").alias("customer_count"),
            count("*").alias("record_count")
        ) \
        .orderBy("date")
    
    # Add date components
    daily_df = daily_df \
        .withColumn("year", year("date")) \
        .withColumn("month", month("date")) \
        .withColumn("date_str", date_format("date", "yyyy-MM-dd"))
    
    logger.info(f"📈 Daily aggregated records: {daily_df.count()}")
    
    # Show sample data
    logger.info("📋 Sample daily aggregated data:")
    daily_df.show(5, truncate=False)
    
    return daily_df

def filter_seasonal_data(daily_df, season):
    """Filter data by season using the exact month logic"""
    logger.info(f"🗓️ Filtering data for season: {season}")
    
    if season == "spring":
        # Ocak-Mayıs (1,2,3,4,5)
        seasonal_df = daily_df.filter(col("month").isin([1, 2, 3, 4, 5]))
    elif season == "summer":
        # Haziran-Eylül (6,7,8,9)
        seasonal_df = daily_df.filter(col("month").isin([6, 7, 8, 9]))
    elif season == "autumn":
        # Ekim-Aralık (10,11,12)
        seasonal_df = daily_df.filter(col("month").isin([10, 11, 12]))
    else:
        # All data for general model
        seasonal_df = daily_df
    
    record_count = seasonal_df.count()
    logger.info(f"📊 {season} season records: {record_count}")
    
    if record_count == 0:
        logger.warning(f"⚠️ No data found for {season} season")
        return None
    
    return seasonal_df

def get_training_date_range(season, target_year=2016):
    """Get date range for seasonal training"""
    if season == "spring":
        start_date = f"{target_year}-01-01"
        end_date = f"{target_year}-05-31"
    elif season == "summer":
        start_date = f"{target_year}-06-01"
        end_date = f"{target_year}-09-30"
    elif season == "autumn":
        start_date = f"{target_year}-10-01"
        end_date = f"{target_year}-12-31"
    else:
        start_date = f"{target_year}-01-01"
        end_date = f"{target_year}-12-31"
    
    logger.info(f"📅 {season} training period: {start_date} to {end_date}")
    return start_date, end_date
