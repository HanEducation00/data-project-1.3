import time
from pyspark.sql.functions import (
    col, lag, sin, cos, when, avg as spark_avg, max as spark_max, min as spark_min
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def create_advanced_features(daily_df):
    """
    Create lag features, rolling averages, and time-based features
    """
    print("🧠 CREATING ADVANCED FEATURES...")
    start_time = time.time()
    
    # Window for lag and rolling features
    window_spec = Window.orderBy("date")
    
    # Lag features (previous days)
    daily_df = daily_df \
        .withColumn("energy_lag_1", lag("total_daily_energy", 1).over(window_spec)) \
        .withColumn("energy_lag_2", lag("total_daily_energy", 2).over(window_spec)) \
        .withColumn("energy_lag_3", lag("total_daily_energy", 3).over(window_spec)) \
        .withColumn("energy_lag_7", lag("total_daily_energy", 7).over(window_spec))
    
    # Rolling averages (moving windows)
    rolling_window_3 = Window.orderBy("date").rowsBetween(-2, 0)
    rolling_window_7 = Window.orderBy("date").rowsBetween(-6, 0)
    rolling_window_30 = Window.orderBy("date").rowsBetween(-29, 0)
    
    daily_df = daily_df \
        .withColumn("rolling_avg_3d", spark_avg("total_daily_energy").over(rolling_window_3)) \
        .withColumn("rolling_avg_7d", spark_avg("total_daily_energy").over(rolling_window_7)) \
        .withColumn("rolling_avg_30d", spark_avg("total_daily_energy").over(rolling_window_30)) \
        .withColumn("rolling_max_7d", spark_max("total_daily_energy").over(rolling_window_7)) \
        .withColumn("rolling_min_7d", spark_min("total_daily_energy").over(rolling_window_7))
    
    # Cyclical time features (sine/cosine encoding)
    daily_df = daily_df \
        .withColumn("month_sin", sin(col("month") * 2 * 3.14159 / 12)) \
        .withColumn("month_cos", cos(col("month") * 2 * 3.14159 / 12)) \
        .withColumn("dayofweek_sin", sin(col("dayofweek") * 2 * 3.14159 / 7)) \
        .withColumn("dayofweek_cos", cos(col("dayofweek") * 2 * 3.14159 / 7))
    
    # Derived features
    daily_df = daily_df \
        .withColumn("is_weekend", when(col("dayofweek").isin([1, 7]), 1).otherwise(0)) \
        .withColumn("is_summer", when(col("month").isin([12, 1, 2]), 1).otherwise(0)) \
        .withColumn("is_winter", when(col("month").isin([6, 7, 8]), 1).otherwise(0)) \
        .withColumn("peak_to_avg_ratio", col("peak_daily_load") / col("avg_daily_load"))
    
    # Trend features
    daily_df = daily_df \
        .withColumn("energy_trend_3d", col("total_daily_energy") - col("rolling_avg_3d")) \
        .withColumn("energy_trend_7d", col("total_daily_energy") - col("rolling_avg_7d"))
    
    daily_df.cache()
    
    processing_time = time.time() - start_time
    
    print(f"✅ Advanced features created in {processing_time:.1f}s")
    
    # Show feature summary
    feature_columns = [c for c in daily_df.columns if c not in ['date', 'timestamp', 'total_daily_energy']]
    print(f"📊 Total features created: {len(feature_columns)}")
    print(f"🔧 Feature list: {', '.join(feature_columns[:10])}...")
    
    return daily_df, feature_columns, processing_time

def prepare_ml_dataset(daily_df, feature_columns, test_days=60):
    """
    Prepare dataset for machine learning with train/val/test splits
    """
    print(f"🎯 PREPARING ML DATASET...")
    print(f"📝 Test period: Last {test_days} days")
    
    # Remove rows with null values (due to lag features)
    clean_df = daily_df.dropna()
    
    total_rows = clean_df.count()
    print(f"📊 Clean dataset: {total_rows} days")
    
    if total_rows < test_days + 30:
        print(f"⚠️  WARNING: Not enough data! Only {total_rows} days available")
        test_days = max(10, total_rows // 4)
        print(f"🔄 Adjusted test days to: {test_days}")
    
    # Create row numbers for splitting
    window_spec = Window.orderBy("date")
    indexed_df = clean_df.withColumn("row_num", row_number().over(window_spec))
    
    # Split into train/validation/test
    train_size = total_rows - test_days - 15  # Reserve 15 days for validation
    val_size = 15
    
    train_df = indexed_df.filter(col("row_num") <= train_size)
    val_df = indexed_df.filter((col("row_num") > train_size) & (col("row_num") <= train_size + val_size))
    test_df = indexed_df.filter(col("row_num") > train_size + val_size)
    
    train_count = train_df.count()
    val_count = val_df.count()
    test_count = test_df.count()
    
    print(f"✅ DATA SPLITS:")
    print(f"   🏋️  Training: {train_count} days ({train_count/total_rows*100:.1f}%)")
    print(f"   🔍 Validation: {val_count} days ({val_count/total_rows*100:.1f}%)")
    print(f"   🧪 Test: {test_count} days ({test_count/total_rows*100:.1f}%)")
    
    return train_df, val_df, test_df, train_count, val_count, test_count
