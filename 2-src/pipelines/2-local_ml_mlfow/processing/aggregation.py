import time
from pyspark.sql.functions import (
    col, sum as spark_sum, avg as spark_avg, max as spark_max, min as spark_min,
    count as spark_count, date_format, hour, dayofweek, month, year
)
from pyspark.sql.types import TimestampType

def create_system_level_aggregation(df):
    """
    Aggregate individual customer data to system level (per timestamp)
    """
    print("🔧 CREATING SYSTEM LEVEL AGGREGATION...")
    start_time = time.time()
    
    # Group by timestamp and aggregate all customers
    system_df = df.groupBy("full_timestamp") \
        .agg(
            spark_sum("load_percentage").alias("total_system_load"),
            spark_avg("load_percentage").alias("avg_system_load"),
            spark_max("load_percentage").alias("max_system_load"),
            spark_min("load_percentage").alias("min_system_load"),
            spark_count("customer_id").alias("active_customers")
        )
    
    # Add time-based features
    system_df = system_df \
        .withColumn("date", date_format("full_timestamp", "yyyy-MM-dd")) \
        .withColumn("hour", hour("full_timestamp")) \
        .withColumn("month", month("full_timestamp")) \
        .withColumn("year", year("full_timestamp")) \
        .withColumn("dayofweek", dayofweek("full_timestamp"))
    
    system_df.cache()
    
    processing_time = time.time() - start_time
    record_count = system_df.count()
    
    print(f"✅ System aggregation completed in {processing_time:.1f}s")
    print(f"📊 System records: {record_count:,} timestamps")
    
    # Show sample
    print("📋 Sample system data:")
    system_df.select("full_timestamp", "total_system_load", "avg_system_load", "active_customers").show(5)
    
    return system_df, processing_time

def create_daily_total_energy(system_df):
    """
    Create daily total energy consumption (sum all timestamps per day)
    """
    print("📊 CREATING DAILY TOTAL ENERGY...")
    start_time = time.time()
    
    # Group by date and sum all timestamps for that day
    daily_df = system_df.groupBy("date") \
        .agg(
            spark_sum("total_system_load").alias("total_daily_energy"),  # Main target!
            spark_avg("total_system_load").alias("avg_daily_load"),
            spark_max("total_system_load").alias("peak_daily_load"),
            spark_min("total_system_load").alias("min_daily_load"),
            spark_sum("active_customers").alias("total_daily_customers"),
            spark_avg("active_customers").alias("avg_daily_customers")
        )
    
    # Add date features
    daily_df = daily_df \
        .withColumn("timestamp", col("date").cast(TimestampType())) \
        .withColumn("month", month(col("timestamp"))) \
        .withColumn("year", year(col("timestamp"))) \
        .withColumn("dayofweek", dayofweek(col("timestamp")))
    
    # Sort by date
    daily_df = daily_df.orderBy("date")
    
    daily_df.cache()
    
    processing_time = time.time() - start_time
    day_count = daily_df.count()
    
    print(f"✅ Daily aggregation completed in {processing_time:.1f}s")
    print(f"📊 Total days: {day_count}")
    
    # Show sample
    print("📋 Sample daily data:")
    daily_df.select("date", "total_daily_energy", "avg_daily_load", "peak_daily_load").show(10)
    
    # Show energy statistics
    energy_stats = daily_df.select(
        spark_avg("total_daily_energy").alias("avg_daily_energy"),
        spark_min("total_daily_energy").alias("min_daily_energy"),
        spark_max("total_daily_energy").alias("max_daily_energy")
    ).collect()[0]
    
    print(f"📈 DAILY ENERGY STATISTICS:")
    print(f"   Average: {energy_stats['avg_daily_energy']:,.1f}")
    print(f"   Minimum: {energy_stats['min_daily_energy']:,.1f}")
    print(f"   Maximum: {energy_stats['max_daily_energy']:,.1f}")
    
    return daily_df, day_count, processing_time
