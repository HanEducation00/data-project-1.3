import time
import numpy as np
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def generate_full_year_data(spark):
    """Generate realistic full year smart meter data"""
    print("🏭 GENERATING FULL YEAR SMART METER DATA...")
    start_time = time.time()
    
    # Generate full year 2016 with 30-minute intervals
    dates = pd.date_range(start='2016-01-01', end='2016-12-31 23:30:00', freq='30min')
    customers = [f"AUS_CUSTOMER_{i:04d}" for i in range(1, 151)]  # 150 customers
    
    print(f"📅 Generating {len(dates):,} timestamps × {len(customers)} customers")
    print(f"⏱️  Expected records: ~{len(dates) * len(customers) * 0.3:,.0f} (30% participation per timestamp)")
    
    data = []
    
    for i, timestamp in enumerate(dates):
        # Each timestamp: 30-50 random customers (realistic participation)
        num_active = np.random.randint(30, 51)
        active_customers = np.random.choice(customers, size=num_active, replace=False)
        
        for customer in active_customers:
            # Realistic load patterns
            hour = timestamp.hour
            month = timestamp.month
            day_of_week = timestamp.weekday()
            
            # Base consumption
            base_load = 35
            
            # Seasonal patterns (kWh-like scaling)
            if month in [12, 1, 2]:  # Winter
                seasonal = 1.4
            elif month in [6, 7, 8]:  # Summer  
                seasonal = 1.3
            else:  # Spring/Fall
                seasonal = 0.9
            
            # Daily patterns
            if 6 <= hour <= 9 or 17 <= hour <= 22:  # Peak hours
                daily = 1.5
            elif 23 <= hour or hour <= 5:  # Night
                daily = 0.5
            else:  # Off-peak
                daily = 1.0
            
            # Weekend vs weekday
            weekend_factor = 0.85 if day_of_week >= 5 else 1.0
            
            # Random variation
            noise = np.random.normal(0, 5)
            
            # Final load calculation
            load = base_load * seasonal * daily * weekend_factor + noise
            load = max(10, min(90, load))  # Realistic bounds
            
            data.append({
                'customer_id': str(customer),  # ← FIX: Explicit string conversion
                'full_timestamp': timestamp,
                'load_percentage': float(load)  # ← FIX: Explicit float conversion
            })
        
        # Progress tracking
        if i % 5000 == 0:
            print(f"   Generated {i:,}/{len(dates):,} timestamps ({i/len(dates)*100:.1f}%)")
    
    # Convert to Spark DataFrame with explicit type conversion
    print(f"📊 Converting {len(data):,} records to Spark DataFrame...")
    pandas_df = pd.DataFrame(data)
    
    # ✅ FIX: Explicit type conversion
    pandas_df['customer_id'] = pandas_df['customer_id'].astype(str)
    pandas_df['load_percentage'] = pandas_df['load_percentage'].astype(float)
    pandas_df['full_timestamp'] = pd.to_datetime(pandas_df['full_timestamp'])
    
    # Create Spark DataFrame with explicit schema
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("full_timestamp", TimestampType(), True),
        StructField("load_percentage", DoubleType(), True)
    ])
    
    spark_df = spark.createDataFrame(pandas_df, schema=schema)
    
    spark_df.cache()
    
    generation_time = time.time() - start_time
    final_count = spark_df.count()
    
    print(f"✅ FULL YEAR DATA GENERATED!")
    print(f"📊 Total records: {final_count:,}")
    print(f"⏱️  Generation time: {generation_time:.1f}s")
    print(f"📈 Generation rate: {final_count/generation_time:,.0f} records/second")
    
    # Show sample
    print("📋 Sample data:")
    spark_df.show(5, truncate=False)
    
    # Show date range
    from pyspark.sql.functions import min as spark_min, max as spark_max
    date_stats = spark_df.select(
        spark_min("full_timestamp").alias("start_date"),
        spark_max("full_timestamp").alias("end_date")
    ).collect()[0]
    
    print(f"📅 Date range: {date_stats['start_date']} to {date_stats['end_date']}")
    
    return spark_df, final_count
