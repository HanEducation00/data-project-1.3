#!/usr/bin/env python3
"""
Feature Engineering for Silver Layer
Using the exact create_daily_features pattern
"""
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, month, dayofweek, year, lag, coalesce,
    avg as spark_avg, max as spark_max, min as spark_min,
    sin, cos, when, lit, date_format)
from .config import logger, FEATURE_COLUMNS, TARGET_COLUMN

def create_daily_features(daily_df):
    """Create features matching the training pipeline (using your exact logic)"""
    logger.info("🧠 Creating daily features...")
    
    # Sort by date for proper lag calculations
    daily_df = daily_df.orderBy("date")
    
    # Window for lag features
    window_spec = Window.orderBy("date")
    
    # Basic temporal features
    daily_df = daily_df.withColumn("month", month("date")) \
                      .withColumn("dayofweek", dayofweek("date")) \
                      .withColumn("year", year("date"))
    
    # Lag features
    daily_df = daily_df.withColumn("energy_lag_1",
                                  coalesce(lag("total_daily_energy", 1).over(window_spec),
                                          col("total_daily_energy"))) \
                      .withColumn("energy_lag_2",
                                  coalesce(lag("total_daily_energy", 2).over(window_spec),
                                          col("total_daily_energy"))) \
                      .withColumn("energy_lag_3",
                                  coalesce(lag("total_daily_energy", 3).over(window_spec),
                                          col("total_daily_energy"))) \
                      .withColumn("energy_lag_7",
                                  coalesce(lag("total_daily_energy", 7).over(window_spec),
                                          col("total_daily_energy")))
    
    # Rolling averages
    rolling_window_3 = Window.orderBy("date").rowsBetween(-2, 0)
    rolling_window_7 = Window.orderBy("date").rowsBetween(-6, 0)
    rolling_window_30 = Window.orderBy("date").rowsBetween(-29, 0)
    
    daily_df = daily_df.withColumn("rolling_avg_3d", spark_avg("total_daily_energy").over(rolling_window_3)) \
                      .withColumn("rolling_avg_7d", spark_avg("total_daily_energy").over(rolling_window_7)) \
                      .withColumn("rolling_avg_30d", spark_avg("total_daily_energy").over(rolling_window_30)) \
                      .withColumn("rolling_max_7d", spark_max("total_daily_energy").over(rolling_window_7)) \
                      .withColumn("rolling_min_7d", spark_min("total_daily_energy").over(rolling_window_7))
    
    # Cyclical features
    daily_df = daily_df.withColumn("month_sin", sin(col("month") * 2 * 3.14159 / 12)) \
                      .withColumn("month_cos", cos(col("month") * 2 * 3.14159 / 12)) \
                      .withColumn("dayofweek_sin", sin(col("dayofweek") * 2 * 3.14159 / 7)) \
                      .withColumn("dayofweek_cos", cos(col("dayofweek") * 2 * 3.14159 / 7))
    
    # Derived features
    daily_df = daily_df.withColumn("is_weekend", when(col("dayofweek").isin([1, 7]), 1).otherwise(0)) \
                      .withColumn("is_summer", when(col("month").isin([12, 1, 2]), 1).otherwise(0)) \
                      .withColumn("is_winter", when(col("month").isin([6, 7, 8]), 1).otherwise(0)) \
                      .withColumn("peak_to_avg_ratio",
                                  when(col("avg_daily_load") > 0, col("peak_daily_load") / col("avg_daily_load"))
                                 .otherwise(lit(1.0)))
    
    # Trend features
    daily_df = daily_df.withColumn("energy_trend_3d", col("total_daily_energy") - col("rolling_avg_3d")) \
                      .withColumn("energy_trend_7d", col("total_daily_energy") - col("rolling_avg_7d"))
    
    # Fill any remaining nulls
    daily_df = daily_df.fillna(0)
    
    logger.info("✅ Daily features created successfully")
    
    # Show feature summary
    logger.info("📊 Feature columns created:")
    for feature in FEATURE_COLUMNS:
        if feature in daily_df.columns:
            logger.info(f"  ✓ {feature}")
        else:
            logger.warning(f"  ✗ Missing: {feature}")
    
    return daily_df

def prepare_training_data(daily_df, min_records=30):
    """Prepare final training dataset"""
    logger.info("🎯 Preparing training data...")
    
    # Apply feature engineering
    featured_df = create_daily_features(daily_df)
    
    # Filter out early records with insufficient lag data
    featured_df = featured_df.filter(col("rolling_avg_30d").isNotNull())
    
    # Select only required columns
    training_columns = FEATURE_COLUMNS + [TARGET_COLUMN, "date"]
    available_columns = [c for c in training_columns if c in featured_df.columns]
    
    training_df = featured_df.select(*available_columns)
    
    record_count = training_df.count()
    logger.info(f"📈 Training records after feature engineering: {record_count}")
    
    if record_count < min_records:
        logger.error(f"❌ Insufficient training data: {record_count} < {min_records}")
        return None
    
    # Show training data sample
    logger.info("📋 Sample training data:")
    training_df.show(3, truncate=False)
    
    return training_df

#
