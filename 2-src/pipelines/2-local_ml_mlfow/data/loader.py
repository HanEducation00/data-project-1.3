import os
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from .generator import generate_full_year_data

def load_smart_meter_data(spark, data_path="/home/han/data/smart-ds/2016/AUS/P1R/load_timeseries/*.txt"):
    """
    Load smart meter data - try local files first, fallback to generated data
    """
    print("📁 LOADING SMART METER DATA...")
    print(f"🔍 Checking path: {data_path}")
    
    # Check if local data exists
    local_dir = "/home/han/data/smart-ds/2016/AUS/P1R/load_timeseries/"
    
    if os.path.exists(local_dir) and len(os.listdir(local_dir)) > 0:
        print("✅ Local smart meter files found!")
        return load_local_files(spark, data_path)
    else:
        print("❌ Local files not found. Creating full year sample data...")
        return generate_full_year_data(spark)

def load_local_files(spark, data_path):
    """Load actual smart meter txt files"""
    print(f"📂 Loading from: {data_path}")
    
    try:
        # Read txt files (adjust schema based on actual file format)
        df = spark.read \
            .option("header", "false") \
            .option("delimiter", ",") \
            .csv(data_path)
        
        # Assuming format: customer_id, timestamp, load_percentage
        df = df.select(
            col("_c0").alias("customer_id"),
            col("_c1").cast(TimestampType()).alias("full_timestamp"),
            col("_c2").cast(DoubleType()).alias("load_percentage")
        )
        
        # Filter valid data
        df = df.filter(
            col("full_timestamp").isNotNull() & 
            col("load_percentage").isNotNull() &
            col("customer_id").isNotNull()
        )
        
        df.cache()
        count = df.count()
        
        print(f"✅ Local files loaded: {count:,} records")
        
        return df, count
        
    except Exception as e:
        print(f"❌ Error loading local files: {e}")
        print("🔄 Fallback to generated data...")
        return generate_full_year_data(spark)
