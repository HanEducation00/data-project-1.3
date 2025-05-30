#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCALE UP: 6 MONTHS BIG DATA ML PIPELINE
PostgreSQL → 3.2M records → Spark ML with Train/Validation/Test
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

def create_export_directory():
    """
    Export directory for 6-month scale up
    """
    export_dir = "/tmp/scale_up_6months"
    os.makedirs(export_dir, exist_ok=True)
    print(f"📁 Scale-up export directory: {export_dir}")
    return export_dir

def export_6months_postgresql_data(export_dir):
    """
    PostgreSQL'den 6 aylık veri export et (3.2M records!)
    """
    print("📤 EXPORTING 6 MONTHS FROM POSTGRESQL...")
    print("📊 Target: ~3.2M records (Jan-June 2016)")
    print("="*70)
    
    from data_loader import ElectricityDataLoader
    
    # 6 months to export
    months_config = {
        1: ("2016-01-01", "2016-01-31", "January"),
        2: ("2016-02-01", "2016-02-29", "February"), 
        3: ("2016-03-01", "2016-03-31", "March"),
        4: ("2016-04-01", "2016-04-30", "April"),
        5: ("2016-05-01", "2016-05-31", "May"),
        6: ("2016-06-01", "2016-06-30", "June")
    }
    
    exported_files = []
    total_exported = 0
    start_time = time.time()
    
    for month_num, (start_date, end_date, month_name) in months_config.items():
        try:
            month_start = time.time()
            print(f"\n📅 EXPORTING {month_name.upper()} (Month {month_num})...")
            print(f"📅 Date range: {start_date} to {end_date}")
            
            # Load from PostgreSQL (NO LIMIT for scale up!)
            loader = ElectricityDataLoader()
            monthly_df = loader.load_raw_data(
                start_date=start_date,
                end_date=end_date,
                limit=None  # 🔥 NO LIMIT! Get all data!
            )
            
            record_count = monthly_df.count()
            print(f"📊 Loaded {month_name}: {record_count:,} records")
            
            # Convert timestamp to string (pandas fix)
            print("🔧 Converting timestamp to string...")
            monthly_df_fixed = monthly_df.withColumn(
                "full_timestamp_str", 
                date_format("full_timestamp", "yyyy-MM-dd HH:mm:ss")
            ).select(
                col("full_timestamp_str").alias("full_timestamp"),
                "customer_id", 
                "load_percentage"
            )
            
            # Export to CSV
            output_file = f"{export_dir}/month_{month_num:02d}_{month_name.lower()}.csv"
            print(f"💾 Writing to CSV: {output_file}")
            
            # Spark CSV writer
            monthly_df_fixed.coalesce(1).write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(f"{output_file}_temp")
            
            # Move file to final location
            import glob
            import shutil
            temp_files = glob.glob(f"{output_file}_temp/*.csv")
            if temp_files:
                shutil.move(temp_files[0], output_file)
                shutil.rmtree(f"{output_file}_temp")
            
            exported_files.append(output_file)
            total_exported += record_count
            
            month_time = time.time() - month_start
            print(f"✅ {month_name} exported: {record_count:,} records in {month_time:.1f}s")
            print(f"📊 Running total: {total_exported:,} records")
            
            loader.close()
            
        except Exception as e:
            print(f"❌ Error exporting {month_name}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    total_time = time.time() - start_time
    
    print(f"\n🎉 6-MONTH POSTGRESQL EXPORT COMPLETED!")
    print("="*70)
    print(f"📁 Files exported: {len(exported_files)}")
    print(f"📊 Total records: {total_exported:,}")
    print(f"⏱️  Total time: {total_time:.1f} seconds")
    print(f"📈 Records/second: {total_exported/total_time:,.0f}")
    
    return exported_files, total_exported

def create_optimized_spark_session():
    """
    Optimized SparkSession for 3.2M records
    """
    print("⚡ Creating OPTIMIZED SparkSession for big data...")
    
    spark = SparkSession.builder \
        .appName("ScaleUp6MonthsML") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    print(f"✅ Optimized SparkSession created! Version: {spark.version}")
    print(f"📊 Default parallelism: {spark.sparkContext.defaultParallelism}")
    
    return spark

def load_6months_data_with_monitoring(spark, export_dir):
    """
    6 aylık veriyi monitoring ile yükle
    """
    print("📁 LOADING 6-MONTH BIG DATA...")
    print(f"📂 Directory: {export_dir}")
    
    start_time = time.time()
    
    # CSV files pattern
    csv_pattern = f"{export_dir}/*.csv"
    print(f"🔍 Reading pattern: {csv_pattern}")
    
    # Load with optimized settings
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("multiline", "false") \
        .csv(csv_pattern)
    
    # Force evaluation and count
    print("📊 Counting records (forcing evaluation)...")
    record_count = df.count()
    
    load_time = time.time() - start_time
    
    print(f"✅ BIG DATA LOADED!")
    print(f"📊 Records: {record_count:,}")
    print(f"⏱️  Load time: {load_time:.1f} seconds")
    print(f"📈 Records/second: {record_count/load_time:,.0f}")
    
    # Convert timestamp back
    df = df.withColumn("full_timestamp", col("full_timestamp").cast(TimestampType()))
    
    # Cache for reuse
    df.cache()
    
    # Show sample
    print("\n📊 SAMPLE BIG DATA:")
    df.show(5, truncate=False)
    
    return df

def add_temporal_features_big_data(df):
    """
    Temporal features for big data
    """
    print("🕐 Adding temporal features to BIG DATA...")
    start_time = time.time()
    
    df = df.withColumn("hour", hour("full_timestamp")) \
          .withColumn("month", month("full_timestamp")) \
          .withColumn("dayofweek", dayofweek("full_timestamp")) \
          .withColumn("quarter", quarter("full_timestamp"))
    
    df = df.withColumn("is_weekend", 
                      when(col("dayofweek").isin([1, 7]), 1).otherwise(0))
    
    df = df.withColumn("season", 
        when(col("month").isin([12, 1, 2]), 0)
        .when(col("month").isin([3, 4, 5]), 1)  
        .when(col("month").isin([6, 7, 8]), 2)
        .otherwise(3)
    )
    
    # Cyclical features
    PI = lit(math.pi)
    df = df.withColumn("sin_hour", sin(col("hour") * (2.0 * PI / 24))) \
          .withColumn("cos_hour", cos(col("hour") * (2.0 * PI / 24)))
    
    feature_time = time.time() - start_time
    print(f"✅ Temporal features added in {feature_time:.1f}s!")
    
    return df

def add_lag_features_big_data(df):
    """
    Lag features for big data with optimization
    """
    print("🔄 Adding lag features to BIG DATA...")
    start_time = time.time()
    
    window_spec = Window.partitionBy("customer_id").orderBy("full_timestamp")
    
    # Add lag features
    df = df.withColumn("load_lag_1h", lag("load_percentage", 4).over(window_spec)) \
          .withColumn("rolling_avg_4h", 
                      spark_avg("load_percentage").over(window_spec.rowsBetween(-16, 0)))
    
    # Fill nulls efficiently
    df = df.na.fill({
        "load_lag_1h": 0,
        "rolling_avg_4h": 0
    })
    
    lag_time = time.time() - start_time
    print(f"✅ Lag features added in {lag_time:.1f}s!")
    
    return df

def create_daily_aggregates_big_data(df):
    """
    Daily aggregation for big data
    """
    print("📊 Creating daily aggregates from BIG DATA...")
    start_time = time.time()
    
    daily_df = df.groupBy(
        date_format("full_timestamp", "yyyy-MM-dd").alias("date"),
        "customer_id"
    ).agg(
        # Energy metrics
        spark_sum("load_percentage").alias("daily_energy"),
        spark_avg("load_percentage").alias("daily_avg"),
        spark_max("load_percentage").alias("daily_peak"),
        
        # Temporal features
        spark_avg("sin_hour").alias("avg_sin_hour"),
        spark_avg("cos_hour").alias("avg_cos_hour"),
        spark_avg("season").alias("season"),
        spark_avg("is_weekend").alias("is_weekend"),
        
        # Lag features
        spark_avg("load_lag_1h").alias("avg_lag_1h"),
        spark_avg("rolling_avg_4h").alias("avg_rolling_4h"),
        
        # Context
        count("*").alias("hourly_count")
    )
    
    # Add date features
    daily_df = daily_df.withColumn("date", col("date").cast("timestamp")) \
                      .withColumn("month", month("date")) \
                      .withColumn("dayofweek", dayofweek("date"))
    
    daily_count = daily_df.count()
    agg_time = time.time() - start_time
    
    print(f"✅ Daily aggregates: {daily_count:,} records in {agg_time:.1f}s")
    
    return daily_df

def create_train_validation_test_splits(daily_df):
    """
    Train/Validation/Test splits for big data
    """
    print("🔄 Creating Train/Validation/Test splits...")
    
    # Add random column for splitting
    daily_df = daily_df.withColumn("random", rand(seed=42))
    
    # 70/15/15 split
    train_df = daily_df.filter(col("random") < 0.70)
    validation_df = daily_df.filter((col("random") >= 0.70) & (col("random") < 0.85))
    test_df = daily_df.filter(col("random") >= 0.85)
    
    # Force evaluation
    train_count = train_df.count()
    validation_count = validation_df.count()
    test_count = test_df.count()
    
    print(f"📊 DATA SPLITS:")
    print(f"   Train:      {train_count:,} records ({train_count/(train_count+validation_count+test_count)*100:.1f}%)")
    print(f"   Validation: {validation_count:,} records ({validation_count/(train_count+validation_count+test_count)*100:.1f}%)")
    print(f"   Test:       {test_count:,} records ({test_count/(train_count+validation_count+test_count)*100:.1f}%)")
    
    # Remove random column
    train_df = train_df.drop("random")
    validation_df = validation_df.drop("random")
    test_df = test_df.drop("random")
    
    return train_df, validation_df, test_df

def create_ml_pipeline_big_data():
    """
    ML Pipeline optimized for big data
    """
    print("🔧 Creating ML pipeline for BIG DATA...")
    
    features = [
        "month", "dayofweek", "season", "is_weekend",
        "avg_sin_hour", "avg_cos_hour", 
        "avg_lag_1h", "avg_rolling_4h",
        "hourly_count"
    ]
    
    target = "daily_energy"
    
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
    gbt = GBTRegressor(
        featuresCol="scaled_features", 
        labelCol=target, 
        predictionCol="prediction",
        maxIter=30,      # More iterations for big data
        maxDepth=8,      # Deeper trees for complex patterns
        stepSize=0.1,
        subsamplingRate=0.8  # Subsampling for big data
    )
    
    pipeline = Pipeline(stages=[assembler, scaler, gbt])
    
    print(f"📊 Features: {len(features)}")
    print(f"🎯 Target: {target}")
    print(f"🌳 GBT: maxIter=30, maxDepth=8")
    
    return pipeline, features, target

def evaluate_model_with_validation(model, train_df, validation_df, test_df, target):
    """
    Model evaluation with validation set
    """
    print("📊 MODEL EVALUATION WITH VALIDATION...")
    
    evaluator = RegressionEvaluator(labelCol=target, predictionCol="prediction")
    
    # Train predictions
    train_predictions = model.transform(train_df)
    train_rmse = evaluator.evaluate(train_predictions, {evaluator.metricName: "rmse"})
    evaluator.setMetricName("r2")
    train_r2 = evaluator.evaluate(train_predictions)
    
    # Validation predictions
    validation_predictions = model.transform(validation_df)
    evaluator.setMetricName("rmse")
    val_rmse = evaluator.evaluate(validation_predictions)
    evaluator.setMetricName("r2")
    val_r2 = evaluator.evaluate(validation_predictions)
    
    # Test predictions
    test_predictions = model.transform(test_df)
    evaluator.setMetricName("rmse")
    test_rmse = evaluator.evaluate(test_predictions)
    evaluator.setMetricName("r2")
    test_r2 = evaluator.evaluate(test_predictions)
    
    return {
        "train": {"rmse": train_rmse, "r2": train_r2},
        "validation": {"rmse": val_rmse, "r2": val_r2},
        "test": {"rmse": test_rmse, "r2": test_r2}
    }, test_predictions

def scale_up_6months_ml_pipeline():
    """
    MAIN: 6-Month Scale Up ML Pipeline (3.2M records!)
    """
    print("🚀 SCALE UP: 6-MONTH BIG DATA ML PIPELINE!")
    print("📊 Target: ~3.2M records (Jan-June 2016)")
    print("🎯 Train/Validation/Test: 70/15/15 split")
    print("="*80)
    
    overall_start = time.time()
    
    # 1. EXPORT PHASE (6 MONTHS)
    print("\n1️⃣ 6-MONTH POSTGRESQL EXPORT...")
    export_dir = create_export_directory()
    exported_files, total_exported = export_6months_postgresql_data(export_dir)
    
    if not exported_files:
        raise ValueError("No files exported from PostgreSQL!")
    
    # 2. OPTIMIZED SPARK SESSION
    print("\n2️⃣ OPTIMIZED SPARK SESSION...")
    spark = create_optimized_spark_session()
    
    try:
        # 3. BIG DATA LOADING
        print("\n3️⃣ BIG DATA LOADING...")
        big_df = load_6months_data_with_monitoring(spark, export_dir)
        
        # 4. FEATURE ENGINEERING
        print("\n4️⃣ BIG DATA FEATURE ENGINEERING...")
        featured_df = add_temporal_features_big_data(big_df)
        featured_df = add_lag_features_big_data(featured_df)
        
        # 5. DAILY AGGREGATION
        print("\n5️⃣ BIG DATA DAILY AGGREGATION...")
        daily_df = create_daily_aggregates_big_data(featured_df)
        
        # 6. TRAIN/VALIDATION/TEST SPLITS
        print("\n6️⃣ TRAIN/VALIDATION/TEST SPLITS...")
        train_df, validation_df, test_df = create_train_validation_test_splits(daily_df)
        
        # 7. ML PIPELINE
        print("\n7️⃣ BIG DATA ML PIPELINE...")
        pipeline, features, target = create_ml_pipeline_big_data()
        
        # 8. MODEL TRAINING
        print("\n8️⃣ BIG DATA MODEL TRAINING...")
        print("🌳 Training GBT on 6-month big data...")
        training_start = time.time()
        
        model = pipeline.fit(train_df)
        
        training_time = time.time() - training_start
        print(f"✅ Big data model training completed in {training_time:.1f}s!")
        
        # 9. EVALUATION
        print("\n9️⃣ BIG DATA EVALUATION...")
        metrics, test_predictions = evaluate_model_with_validation(
            model, train_df, validation_df, test_df, target)
        
        # 10. RESULTS
        print("\n🎉 6-MONTH BIG DATA ML RESULTS:")
        print("="*80)
        print(f"📊 BIG DATA METRICS:")
        print(f"   TRAIN:      RMSE={metrics['train']['rmse']:,.2f}, R²={metrics['train']['r2']:.4f}")
        print(f"   VALIDATION: RMSE={metrics['validation']['rmse']:,.2f}, R²={metrics['validation']['r2']:.4f}")
        print(f"   TEST:       RMSE={metrics['test']['rmse']:,.2f}, R²={metrics['test']['r2']:.4f}")
        
        # 11. SAMPLE PREDICTIONS
        print(f"\n📅 BIG DATA TEST PREDICTIONS:")
        sample = test_predictions.select("date", "customer_id", target, "prediction").limit(5)
        sample.show(5, truncate=False)
        
        # 12. FEATURE IMPORTANCE
        if hasattr(model.stages[-1], 'featureImportances'):
            print(f"\n🌟 BIG DATA FEATURE IMPORTANCE:")
            importances = model.stages[-1].featureImportances.toArray()
            feature_importance = list(zip(features, importances))
            feature_importance.sort(key=lambda x: x[1], reverse=True)
            
            for i, (feature, importance) in enumerate(feature_importance):
                print(f"   {i+1}. {feature}: {importance:.4f}")
        
        overall_time = time.time() - overall_start
        
        print(f"\n🔥 6-MONTH SCALE UP BAŞARILI!")
        print("="*80)
        print(f"📤 PostgreSQL exported: {total_exported:,} records")
        print(f"📊 Spark processed: {big_df.count():,} records")
        print(f"📊 Daily aggregates: {daily_df.count():,} records")
        print(f"📊 Test R²: {metrics['test']['r2']:.4f}")
        print(f"⏱️  Total pipeline time: {overall_time:.1f} seconds")
        print(f"✅ BIG DATA ML SUCCESS! 🚀")
        
        return {
            "success": True,
            "model": model,
            "metrics": metrics,
            "exported_records": total_exported,
            "processed_records": big_df.count(),
            "daily_records": daily_df.count(),
            "pipeline_time": overall_time
        }
        
    except Exception as e:
        print(f"❌ Error during big data processing: {e}")
        import traceback
        traceback.print_exc()
        raise
        
    finally:
        print(f"\n🧹 CLEANUP...")
        spark.stop()
        print("✅ SparkSession stopped!")

if __name__ == "__main__":
    print("⚡ 6-MONTH BIG DATA ML PIPELINE")
    print("📊 PostgreSQL → 3.2M records → Spark ML")
    
    try:
        result = scale_up_6months_ml_pipeline()
        
        if result["success"]:
            print(f"\n🎊 6-MONTH BIG DATA SCALE UP MÜKEMMEL!")
            print(f"📤 Exported: {result['exported_records']:,} records")
            print(f"📊 Processed: {result['processed_records']:,} records")
            print(f"📊 Test R²: {result['metrics']['test']['r2']:.4f}")
            print(f"⏱️  Pipeline: {result['pipeline_time']:.1f} seconds")
            print(f"\n✅ BIG DATA ARCHITECTURE EXPERIENCE COMPLETED! 🚀")
        else:
            print("❌ Big data scale up failed!")
            
    except Exception as e:
        print(f"❌ Scale up error: {e}")
        import traceback
        traceback.print_exc()
