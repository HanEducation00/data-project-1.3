#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ULTIMATE CHALLENGE: FULL YEAR BIG DATA ML PIPELINE
PostgreSQL → 6.3M records → Enterprise-Scale Spark ML
"""

import sys
import os
import math
import time
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'data_processing'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg as spark_avg, max as spark_max, min as spark_min,
    date_format, dayofweek, month, quarter, hour, minute,
    when, lit, sin, cos, count, lag, rand
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

def create_ultimate_export_directory():
    """
    Ultimate export directory for full year
    """
    export_dir = "/tmp/ultimate_full_year"
    os.makedirs(export_dir, exist_ok=True)
    print(f"📁 ULTIMATE export directory: {export_dir}")
    return export_dir

def export_full_year_postgresql_data(export_dir):
    """
    PostgreSQL'den FULL YEAR export (6.3M records!)
    """
    print("📤 EXPORTING FULL YEAR FROM POSTGRESQL...")
    print("📊 ULTIMATE TARGET: 6,340,608 records (Jan-Dec 2016)")
    print("🏆 ENTERPRISE-SCALE DATA PROCESSING!")
    print("="*80)
    
    from data_loader import ElectricityDataLoader
    
    # ALL 12 months configuration
    months_config = {
        1: ("2016-01-01", "2016-01-31", "January"),
        2: ("2016-02-01", "2016-02-29", "February"), 
        3: ("2016-03-01", "2016-03-31", "March"),
        4: ("2016-04-01", "2016-04-30", "April"),
        5: ("2016-05-01", "2016-05-31", "May"),
        6: ("2016-06-01", "2016-06-30", "June"),
        7: ("2016-07-01", "2016-07-31", "July"),
        8: ("2016-08-01", "2016-08-31", "August"),
        9: ("2016-09-01", "2016-09-30", "September"),
        10: ("2016-10-01", "2016-10-31", "October"),
        11: ("2016-11-01", "2016-11-30", "November"),
        12: ("2016-12-01", "2016-12-31", "December")
    }
    
    exported_files = []
    total_exported = 0
    start_time = time.time()
    
    print(f"🗓️  Processing {len(months_config)} months...")
    
    for month_num, (start_date, end_date, month_name) in months_config.items():
        try:
            month_start = time.time()
            print(f"\n📅 EXPORTING {month_name.upper()} (Month {month_num}/12)...")
            print(f"📅 Date range: {start_date} to {end_date}")
            
            # Progress indicator
            progress = (month_num / 12) * 100
            print(f"📊 Progress: {progress:.1f}% complete")
            
            # Load from PostgreSQL (NO LIMIT - get everything!)
            loader = ElectricityDataLoader()
            monthly_df = loader.load_raw_data(
                start_date=start_date,
                end_date=end_date,
                limit=None  # 🔥 ULTIMATE: No limits!
            )
            
            record_count = monthly_df.count()
            print(f"📊 {month_name}: {record_count:,} records loaded")
            
            # Convert timestamp to string (proven approach)
            print("🔧 Converting timestamp...")
            monthly_df_fixed = monthly_df.withColumn(
                "full_timestamp_str", 
                date_format("full_timestamp", "yyyy-MM-dd HH:mm:ss")
            ).select(
                col("full_timestamp_str").alias("full_timestamp"),
                "customer_id", 
                "load_percentage"
            )
            
            # Export to CSV with progress monitoring
            output_file = f"{export_dir}/month_{month_num:02d}_{month_name.lower()}.csv"
            print(f"💾 Writing CSV: {month_name}...")
            
            # Optimized CSV writer for big data
            monthly_df_fixed.coalesce(2).write \
                .mode("overwrite") \
                .option("header", "true") \
                .option("compression", "gzip") \
                .csv(f"{output_file}_temp")
            
            # Move to final location
            import glob
            import shutil
            temp_files = glob.glob(f"{output_file}_temp/*.csv.gz")
            if temp_files:
                # Rename with .gz extension
                final_file = f"{output_file}.gz"
                shutil.move(temp_files[0], final_file)
                shutil.rmtree(f"{output_file}_temp")
                output_file = final_file
            
            exported_files.append(output_file)
            total_exported += record_count
            
            month_time = time.time() - month_start
            avg_time_per_month = (time.time() - start_time) / month_num
            eta_remaining = avg_time_per_month * (12 - month_num)
            
            print(f"✅ {month_name}: {record_count:,} records in {month_time:.1f}s")
            print(f"📊 Running total: {total_exported:,} records")
            print(f"⏱️  ETA remaining: {eta_remaining:.1f} seconds")
            
            loader.close()
            
        except Exception as e:
            print(f"❌ Error exporting {month_name}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    total_time = time.time() - start_time
    
    print(f"\n🎉 ULTIMATE FULL YEAR EXPORT COMPLETED!")
    print("="*80)
    print(f"📁 Files exported: {len(exported_files)}")
    print(f"📊 Total records: {total_exported:,}")
    print(f"⏱️  Total export time: {total_time:.1f} seconds")
    print(f"📈 Export rate: {total_exported/total_time:,.0f} records/second")
    print(f"🎯 Target achieved: {total_exported == 6340608}")
    
    return exported_files, total_exported

def create_enterprise_spark_session():
    """
    Enterprise-grade SparkSession for 6.3M records
    """
    print("⚡ Creating ENTERPRISE SparkSession for ULTIMATE scale...")
    
    spark = SparkSession.builder \
        .appName("UltimateFullYearML") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB") \
        .getOrCreate()
    
    # Set higher checkpoint directory for large operations
    spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")
    
    print(f"✅ ENTERPRISE SparkSession created!")
    print(f"📊 Spark version: {spark.version}")
    print(f"📊 Default parallelism: {spark.sparkContext.defaultParallelism}")
    print(f"📊 Adaptive query execution: ENABLED")
    print(f"📊 Advanced optimizations: ENABLED")
    
    return spark

def load_ultimate_full_year_data(spark, export_dir):
    """
    Ultimate 6.3M records loading with enterprise monitoring
    """
    print("📁 LOADING ULTIMATE FULL YEAR DATA...")
    print(f"📂 Directory: {export_dir}")
    print("🏆 Target: 6,340,608 records")
    
    start_time = time.time()
    
    # CSV files pattern (including compressed files)
    csv_pattern = f"{export_dir}/*.csv*"
    print(f"🔍 Reading pattern: {csv_pattern}")
    
    # Enterprise-grade loading with optimizations
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("multiline", "false") \
        .option("compression", "gzip") \
        .csv(csv_pattern)
    
    # Force evaluation with progress monitoring
    print("📊 Counting ULTIMATE dataset (forcing evaluation)...")
    print("⚡ This may take a moment for 6.3M records...")
    
    record_count = df.count()
    
    load_time = time.time() - start_time
    
    print(f"\n🎉 ULTIMATE DATA LOADED!")
    print("="*80)
    print(f"📊 Records: {record_count:,}")
    print(f"🎯 Target hit: {record_count == 6340608}")
    print(f"⏱️  Load time: {load_time:.1f} seconds")
    print(f"📈 Load rate: {record_count/load_time:,.0f} records/second")
    
    # Convert timestamp and cache for multiple operations
    print("🔄 Converting timestamps and caching...")
    df = df.withColumn("full_timestamp", col("full_timestamp").cast(TimestampType()))
    
    # Cache with MEMORY_AND_DISK for large dataset
    df.persist()
    
    # Trigger caching
    df.count()
    
    print("📊 SAMPLE ULTIMATE DATASET:")
    df.show(5, truncate=False)
    
    return df

def add_seasonal_features_full_year(df):
    """
    Enhanced temporal features for full year analysis
    """
    print("🕐 Adding SEASONAL features for full year...")
    start_time = time.time()
    
    # Basic temporal features
    df = df.withColumn("hour", hour("full_timestamp")) \
          .withColumn("month", month("full_timestamp")) \
          .withColumn("dayofweek", dayofweek("full_timestamp")) \
          .withColumn("quarter", quarter("full_timestamp"))
    
    # Enhanced weekend/weekday
    df = df.withColumn("is_weekend", 
                      when(col("dayofweek").isin([1, 7]), 1).otherwise(0))
    
    # Detailed seasonal features (4 seasons)
    df = df.withColumn("season", 
        when(col("month").isin([12, 1, 2]), 0)      # Winter
        .when(col("month").isin([3, 4, 5]), 1)      # Spring
        .when(col("month").isin([6, 7, 8]), 2)      # Summer
        .otherwise(3)                               # Fall
    )
    
    # Enhanced cyclical features
    PI = lit(math.pi)
    df = df.withColumn("sin_hour", sin(col("hour") * (2.0 * PI / 24))) \
          .withColumn("cos_hour", cos(col("hour") * (2.0 * PI / 24))) \
          .withColumn("sin_month", sin(col("month") * (2.0 * PI / 12))) \
          .withColumn("cos_month", cos(col("month") * (2.0 * PI / 12))) \
          .withColumn("sin_dayofweek", sin(col("dayofweek") * (2.0 * PI / 7))) \
          .withColumn("cos_dayofweek", cos(col("dayofweek") * (2.0 * PI / 7)))
    
    feature_time = time.time() - start_time
    print(f"✅ SEASONAL features added in {feature_time:.1f}s!")
    
    return df

def add_advanced_lag_features_ultimate(df):
    """
    Advanced lag features for ultimate scale
    """
    print("🔄 Adding ADVANCED lag features for ultimate scale...")
    start_time = time.time()
    
    window_spec = Window.partitionBy("customer_id").orderBy("full_timestamp")
    
    # Multiple lag horizons for full year
    df = df.withColumn("load_lag_1h", lag("load_percentage", 4).over(window_spec)) \
          .withColumn("load_lag_6h", lag("load_percentage", 24).over(window_spec)) \
          .withColumn("load_lag_24h", lag("load_percentage", 96).over(window_spec))
    
    # Rolling features with different windows
    df = df.withColumn("rolling_avg_4h", 
                      spark_avg("load_percentage").over(window_spec.rowsBetween(-16, 0))) \
          .withColumn("rolling_avg_24h", 
                      spark_avg("load_percentage").over(window_spec.rowsBetween(-96, 0))) \
          .withColumn("rolling_max_24h", 
                      spark_max("load_percentage").over(window_spec.rowsBetween(-96, 0)))
    
    # Fill nulls efficiently
    df = df.na.fill({
        "load_lag_1h": 0,
        "load_lag_6h": 0, 
        "load_lag_24h": 0,
        "rolling_avg_4h": 0,
        "rolling_avg_24h": 0,
        "rolling_max_24h": 0
    })
    
    lag_time = time.time() - start_time
    print(f"✅ ADVANCED lag features added in {lag_time:.1f}s!")
    
    return df

def create_daily_aggregates_ultimate(df):
    """
    Daily aggregation for ultimate full year dataset
    """
    print("📊 Creating daily aggregates from ULTIMATE dataset...")
    start_time = time.time()
    
    daily_df = df.groupBy(
        date_format("full_timestamp", "yyyy-MM-dd").alias("date"),
        "customer_id"
    ).agg(
        # Energy metrics
        spark_sum("load_percentage").alias("daily_energy"),
        spark_avg("load_percentage").alias("daily_avg"),
        spark_max("load_percentage").alias("daily_peak"),
        spark_min("load_percentage").alias("daily_min"),
        
        # Enhanced temporal features
        spark_avg("sin_hour").alias("avg_sin_hour"),
        spark_avg("cos_hour").alias("avg_cos_hour"),
        spark_avg("sin_month").alias("avg_sin_month"),
        spark_avg("cos_month").alias("avg_cos_month"),
        spark_avg("season").alias("season"),
        spark_avg("is_weekend").alias("is_weekend"),
        
        # Advanced lag features
        spark_avg("load_lag_1h").alias("avg_lag_1h"),
        spark_avg("load_lag_6h").alias("avg_lag_6h"),
        spark_avg("load_lag_24h").alias("avg_lag_24h"),
        spark_avg("rolling_avg_4h").alias("avg_rolling_4h"),
        spark_avg("rolling_avg_24h").alias("avg_rolling_24h"),
        
        # Context
        count("*").alias("hourly_count")
    )
    
    # Add date features
    daily_df = daily_df.withColumn("date", col("date").cast("timestamp")) \
                      .withColumn("month", month("date")) \
                      .withColumn("dayofweek", dayofweek("date")) \
                      .withColumn("quarter", quarter("date"))
    
    daily_count = daily_df.count()
    agg_time = time.time() - start_time
    
    print(f"✅ ULTIMATE daily aggregates: {daily_count:,} records in {agg_time:.1f}s")
    
    return daily_df

def create_enterprise_train_val_test_splits(daily_df):
    """
    Enterprise train/validation/test splits
    """
    print("🔄 Creating ENTERPRISE Train/Validation/Test splits...")
    
    # Add random column for splitting
    daily_df = daily_df.withColumn("random", rand(seed=42))
    
    # 70/15/15 split for maximum data utilization
    train_df = daily_df.filter(col("random") < 0.70)
    validation_df = daily_df.filter((col("random") >= 0.70) & (col("random") < 0.85))
    test_df = daily_df.filter(col("random") >= 0.85)
    
    # Force evaluation with timing
    print("📊 Evaluating split sizes...")
    split_start = time.time()
    
    train_count = train_df.count()
    validation_count = validation_df.count()
    test_count = test_df.count()
    total_count = train_count + validation_count + test_count
    
    split_time = time.time() - split_start
    
    print(f"📊 ENTERPRISE DATA SPLITS (completed in {split_time:.1f}s):")
    print(f"   Train:      {train_count:,} records ({train_count/total_count*100:.1f}%)")
    print(f"   Validation: {validation_count:,} records ({validation_count/total_count*100:.1f}%)")
    print(f"   Test:       {test_count:,} records ({test_count/total_count*100:.1f}%)")
    print(f"   Total:      {total_count:,} records")
    
    # Remove random column and cache for reuse
    train_df = train_df.drop("random").cache()
    validation_df = validation_df.drop("random").cache()
    test_df = test_df.drop("random").cache()
    
    return train_df, validation_df, test_df

def create_enterprise_ml_pipeline():
    """
    Enterprise-grade ML Pipeline for ultimate scale
    """
    print("🔧 Creating ENTERPRISE ML pipeline...")
    
    # Enhanced feature set for full year
    features = [
        "month", "dayofweek", "quarter", "season", "is_weekend",
        "avg_sin_hour", "avg_cos_hour", 
        "avg_sin_month", "avg_cos_month",
        "avg_lag_1h", "avg_lag_6h", "avg_lag_24h",
        "avg_rolling_4h", "avg_rolling_24h",
        "hourly_count", "daily_min", "daily_peak"
    ]
    
    target = "daily_energy"
    
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
    gbt = GBTRegressor(
        featuresCol="scaled_features", 
        labelCol=target, 
        predictionCol="prediction",
        maxIter=50,       # More iterations for complex patterns
        maxDepth=10,      # Deeper trees for full year complexity
        stepSize=0.05,    # Smaller steps for stability
        subsamplingRate=0.8,
        featureSubsetStrategy="sqrt"  # Feature subsampling
    )
    
    pipeline = Pipeline(stages=[assembler, scaler, gbt])
    
    print(f"📊 ENTERPRISE Features: {len(features)}")
    print(f"🎯 Target: {target}")
    print(f"🌳 GBT: maxIter=50, maxDepth=10, advanced settings")
    
    return pipeline, features, target

def ultimate_full_year_ml_pipeline():
    """
    ULTIMATE: Full Year ML Pipeline (6.3M records!)
    """
    print("🚀 ULTIMATE CHALLENGE: FULL YEAR ML PIPELINE!")
    print("📊 ENTERPRISE TARGET: 6,340,608 records (Full 2016)")
    print("🏆 ULTIMATE BIG DATA EXPERIENCE!")
    print("="*90)
    
    overall_start = time.time()
    
    # 1. ULTIMATE EXPORT PHASE
    print("\n1️⃣ ULTIMATE FULL YEAR EXPORT...")
    export_dir = create_ultimate_export_directory()
    exported_files, total_exported = export_full_year_postgresql_data(export_dir)
    
    if not exported_files:
        raise ValueError("No files exported from PostgreSQL!")
    
    if total_exported != 6340608:
        print(f"⚠️  Warning: Expected 6,340,608 but got {total_exported:,} records")
    
    # 2. ENTERPRISE SPARK SESSION
    print("\n2️⃣ ENTERPRISE SPARK SESSION...")
    spark = create_enterprise_spark_session()
    
    try:
        # 3. ULTIMATE DATA LOADING
        print("\n3️⃣ ULTIMATE DATA LOADING...")
        ultimate_df = load_ultimate_full_year_data(spark, export_dir)
        
        # 4. SEASONAL FEATURE ENGINEERING
        print("\n4️⃣ SEASONAL FEATURE ENGINEERING...")
        featured_df = add_seasonal_features_full_year(ultimate_df)
        featured_df = add_advanced_lag_features_ultimate(featured_df)
        
        # 5. ULTIMATE DAILY AGGREGATION
        print("\n5️⃣ ULTIMATE DAILY AGGREGATION...")
        daily_df = create_daily_aggregates_ultimate(featured_df)
        
        # 6. ENTERPRISE SPLITS
        print("\n6️⃣ ENTERPRISE TRAIN/VALIDATION/TEST SPLITS...")
        train_df, validation_df, test_df = create_enterprise_train_val_test_splits(daily_df)
        
        # 7. ENTERPRISE ML PIPELINE
        print("\n7️⃣ ENTERPRISE ML PIPELINE...")
        pipeline, features, target = create_enterprise_ml_pipeline()
        
        # 8. ULTIMATE MODEL TRAINING
        print("\n8️⃣ ULTIMATE MODEL TRAINING...")
        print("🌳 Training enterprise GBT on FULL YEAR data...")
        print("⚡ This will take several minutes for 6.3M records...")
        
        training_start = time.time()
        model = pipeline.fit(train_df)
        training_time = time.time() - training_start
        
        print(f"✅ ULTIMATE model training completed in {training_time:.1f}s!")
        
        # 9. COMPREHENSIVE EVALUATION
        print("\n9️⃣ COMPREHENSIVE EVALUATION...")
        
        evaluator = RegressionEvaluator(labelCol=target, predictionCol="prediction")
        
        # Train metrics
        train_predictions = model.transform(train_df)
        train_rmse = evaluator.evaluate(train_predictions, {evaluator.metricName: "rmse"})
        evaluator.setMetricName("r2")
        train_r2 = evaluator.evaluate(train_predictions)
        
        # Validation metrics
        validation_predictions = model.transform(validation_df)
        evaluator.setMetricName("rmse")
        val_rmse = evaluator.evaluate(validation_predictions)
        evaluator.setMetricName("r2")
        val_r2 = evaluator.evaluate(validation_predictions)
        
        # Test metrics
        test_predictions = model.transform(test_df)
        evaluator.setMetricName("rmse")
        test_rmse = evaluator.evaluate(test_predictions)
        evaluator.setMetricName("r2")
        test_r2 = evaluator.evaluate(test_predictions)
        
        metrics = {
            "train": {"rmse": train_rmse, "r2": train_r2},
            "validation": {"rmse": val_rmse, "r2": val_r2},
            "test": {"rmse": test_rmse, "r2": test_r2}
        }
        
        # 10. ULTIMATE RESULTS
        overall_time = time.time() - overall_start
        
        print("\n🎉 ULTIMATE FULL YEAR RESULTS!")
        print("="*90)
        print(f"🏆 ENTERPRISE-SCALE METRICS:")
        print(f"   TRAIN:      RMSE={metrics['train']['rmse']:,.2f}, R²={metrics['train']['r2']:.4f}")
        print(f"   VALIDATION: RMSE={metrics['validation']['rmse']:,.2f}, R²={metrics['validation']['r2']:.4f}")
        print(f"   TEST:       RMSE={metrics['test']['rmse']:,.2f}, R²={metrics['test']['r2']:.4f}")
        
        # Sample predictions
        print(f"\n📅 ULTIMATE TEST PREDICTIONS:")
        sample = test_predictions.select("date", "customer_id", target, "prediction").limit(5)
        sample.show(5, truncate=False)
        
        # Feature importance
        if hasattr(model.stages[-1], 'featureImportances'):
            print(f"\n🌟 ULTIMATE FEATURE IMPORTANCE:")
            importances = model.stages[-1].featureImportances.toArray()
            feature_importance = list(zip(features, importances))
            feature_importance.sort(key=lambda x: x[1], reverse=True)
            
            for i, (feature, importance) in enumerate(feature_importance[:10]):
                print(f"   {i+1}. {feature}: {importance:.4f}")
        
        print(f"\n🔥 ULTIMATE CHALLENGE COMPLETED!")
        print("="*90)
        print(f"📤 PostgreSQL exported: {total_exported:,} records")
        print(f"📊 Spark processed: {ultimate_df.count():,} records")
        print(f"📊 Daily aggregates: {daily_df.count():,} records")
        print(f"📊 Test R²: {metrics['test']['r2']:.4f}")
        print(f"⏱️  Total pipeline time: {overall_time:.1f} seconds ({overall_time/60:.1f} minutes)")
        print(f"🏆 ENTERPRISE BIG DATA MASTERY ACHIEVED!")
        
        return {
            "success": True,
            "model": model,
            "metrics": metrics,
            "exported_records": total_exported,
            "processed_records": ultimate_df.count(),
            "daily_records": daily_df.count(),
            "pipeline_time": overall_time
        }
        
    except Exception as e:
        print(f"❌ Error during ultimate processing: {e}")
        import traceback
        traceback.print_exc()
        raise
        
    finally:
        print(f"\n🧹 ULTIMATE CLEANUP...")
        spark.stop()
        print("✅ Enterprise SparkSession stopped!")

if __name__ == "__main__":
    print("⚡ ULTIMATE FULL YEAR BIG DATA ML PIPELINE")
    print("🏆 PostgreSQL → 6.3M records → Enterprise Spark ML")
    
    try:
        result = ultimate_full_year_ml_pipeline()
        
        if result["success"]:
            print(f"\n🎊 ULTIMATE CHALLENGE CONQUIRED!")
            print(f"📤 Exported: {result['exported_records']:,} records")
            print(f"📊 Processed: {result['processed_records']:,} records")  
            print(f"📊 Test R²: {result['metrics']['test']['r2']:.4f}")
            print(f"⏱️  Pipeline: {result['pipeline_time']:.1f} seconds")
            print(f"\n✅ ENTERPRISE BIG DATA ARCHITECTURE MASTERED! 🏆")
        else:
            print("❌ Ultimate challenge failed!")
            
    except Exception as e:
        print(f"❌ Ultimate error: {e}")
        import traceback
        traceback.print_exc()
