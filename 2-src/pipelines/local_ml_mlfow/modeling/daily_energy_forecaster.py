#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HYBRID APPROACH FIXED: POSTGRESQL → LOCAL FILES → SPARK ML
PostgreSQL timestamp cast fix + Memory-safe processing
"""

import sys
import os
import math
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'data_processing'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg as spark_avg, max as spark_max, min as spark_min,
    date_format, dayofweek, month, quarter, hour, minute,
    when, lit, sin, cos, count, lag
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

def create_export_directory():
    """
    Export directory oluştur
    """
    export_dir = "/tmp/real_energy_export"
    os.makedirs(export_dir, exist_ok=True)
    print(f"📁 Export directory: {export_dir}")
    return export_dir

def export_postgresql_to_files_fixed(export_dir, months_to_export=[1], limit_per_month=50000):
    """
    PostgreSQL'den batch batch al, local CSV files'a kaydet (TIMESTAMP FIX!)
    """
    print("📤 EXPORTING FROM POSTGRESQL TO LOCAL FILES (FIXED)...")
    print(f"📊 Months: {months_to_export}")
    print(f"📊 Limit per month: {limit_per_month:,}")
    print("="*60)
    
    from data_loader import ElectricityDataLoader
    
    exported_files = []
    total_exported = 0
    
    for month_num in months_to_export:
        try:
            print(f"\n📅 Exporting Month {month_num}...")
            
            # Month date ranges
            month_ranges = {
                1: ("2016-01-01", "2016-01-31"),
                2: ("2016-02-01", "2016-02-29"), 
                3: ("2016-03-01", "2016-03-31")
            }
            
            if month_num not in month_ranges:
                print(f"❌ Invalid month: {month_num}")
                continue
                
            start_date, end_date = month_ranges[month_num]
            print(f"📅 Date range: {start_date} to {end_date}")
            
            # Load from PostgreSQL (small batch)
            loader = ElectricityDataLoader()
            monthly_df = loader.load_raw_data(
                start_date=start_date,
                end_date=end_date,
                limit=limit_per_month
            )
            
            record_count = monthly_df.count()
            print(f"📊 Loaded from PostgreSQL: {record_count:,} records")
            
            # 🔧 FIX: Convert timestamp to string before pandas conversion
            print("🔧 Converting timestamp to string (pandas fix)...")
            monthly_df_fixed = monthly_df.withColumn(
                "full_timestamp_str", 
                date_format("full_timestamp", "yyyy-MM-dd HH:mm:ss")
            ).select(
                col("full_timestamp_str").alias("full_timestamp"),
                "customer_id", 
                "load_percentage"
            )
            
            # Export to local CSV file using Spark CSV writer (more reliable)
            output_file = f"{export_dir}/month_{month_num:02d}.csv"
            
            print(f"💾 Writing to CSV using Spark writer...")
            
            # Use Spark CSV writer instead of pandas (more reliable for large data)
            monthly_df_fixed.coalesce(1).write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(f"{output_file}_temp")
            
            # Move the single CSV file to final location
            import glob
            temp_files = glob.glob(f"{output_file}_temp/*.csv")
            if temp_files:
                import shutil
                shutil.move(temp_files[0], output_file)
                shutil.rmtree(f"{output_file}_temp")
            
            exported_files.append(output_file)
            total_exported += record_count
            
            print(f"✅ Exported to: {output_file}")
            print(f"📊 Records: {record_count:,}")
            
            # Close loader
            loader.close()
            
        except Exception as e:
            print(f"❌ Error exporting month {month_num}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    print(f"\n🎉 POSTGRESQL EXPORT COMPLETED!")
    print(f"📁 Files exported: {len(exported_files)}")
    print(f"📊 Total records: {total_exported:,}")
    
    return exported_files, total_exported

def create_simple_spark_session():
    """
    Simple SparkSession (başarılı approach)
    """
    print("⚡ Creating simple SparkSession...")
    spark = SparkSession.builder.appName("HybridRealDataMLFixed").getOrCreate()
    print(f"✅ SparkSession created! Version: {spark.version}")
    return spark

def load_real_data_from_exported_files(spark, export_dir):
    """
    Exported CSV files'dan Spark ile yükle (memory safe textFile approach)
    """
    print("📁 LOADING REAL DATA FROM EXPORTED FILES...")
    print(f"📂 Directory: {export_dir}")
    
    # CSV files pattern
    csv_pattern = f"{export_dir}/*.csv"
    print(f"🔍 Reading pattern: {csv_pattern}")
    
    # Use Spark CSV reader (more reliable than textFile for CSV)
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(csv_pattern)
    
    final_count = df.count()
    print(f"✅ Real DataFrame created: {final_count:,} records")
    
    # Convert timestamp back to TimestampType
    df = df.withColumn("full_timestamp", col("full_timestamp").cast(TimestampType()))
    
    # Show sample
    print("\n📊 SAMPLE REAL DATA:")
    df.show(5, truncate=False)
    
    return df

def add_temporal_features(df):
    """
    Temporal features (same as before)
    """
    print("🕐 Adding temporal features to real data...")
    
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
    
    print("✅ Temporal features added to real data!")
    return df

def add_lag_features(df):
    """
    Simple lag features for real data
    """
    print("🔄 Adding lag features to real data...")
    
    window_spec = Window.partitionBy("customer_id").orderBy("full_timestamp")
    
    df = df.withColumn("load_lag_1h", lag("load_percentage", 4).over(window_spec)) \
          .withColumn("rolling_avg_4h", 
                      spark_avg("load_percentage").over(window_spec.rowsBetween(-16, 0)))
    
    # Fill nulls
    df = df.na.fill({
        "load_lag_1h": 0,
        "rolling_avg_4h": 0
    })
    
    print("✅ Lag features added to real data!")
    return df

def create_daily_aggregates_real(df):
    """
    Daily aggregation for real data
    """
    print("📊 Creating daily aggregates from real data...")
    
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
    print(f"✅ Daily aggregates from real data: {daily_count:,} records")
    
    # Show sample daily data
    print("\n📊 SAMPLE DAILY AGGREGATES:")
    daily_df.show(5, truncate=False)
    
    return daily_df

def create_ml_pipeline_real():
    """
    ML Pipeline for real data
    """
    print("🔧 Creating ML pipeline for real data...")
    
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
        maxIter=20,     # More iterations for real data
        maxDepth=6,     # Deeper trees for real patterns
        stepSize=0.1
    )
    
    pipeline = Pipeline(stages=[assembler, scaler, gbt])
    
    print(f"📊 Features: {len(features)}")
    print(f"🎯 Target: {target}")
    
    return pipeline, features, target

def evaluate_real_model(predictions, target):
    """
    Model evaluation for real data
    """
    print("📊 Evaluating model on real data...")
    
    evaluator = RegressionEvaluator(labelCol=target, predictionCol="prediction")
    
    rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
    evaluator.setMetricName("mae")
    mae = evaluator.evaluate(predictions)
    evaluator.setMetricName("r2")
    r2 = evaluator.evaluate(predictions)
    
    return {"rmse": rmse, "mae": mae, "r2": r2}

def hybrid_postgresql_to_spark_ml_fixed():
    """
    MAIN: Hybrid PostgreSQL → Local Files → Spark ML Pipeline (FIXED!)
    """
    print("🚀 HYBRID POSTGRESQL → LOCAL FILES → SPARK ML (FIXED)!")
    print("📊 Real PostgreSQL data + Timestamp fix + Memory-safe processing")
    print("="*70)
    
    # 1. EXPORT PHASE (FIXED)
    print("\n1️⃣ POSTGRESQL EXPORT PHASE (FIXED)...")
    export_dir = create_export_directory()
    
    # Export 1 month with 50K limit (memory safe)
    exported_files, total_exported = export_postgresql_to_files_fixed(
        export_dir=export_dir,
        months_to_export=[1],  # Start with January
        limit_per_month=50000  # Memory safe
    )
    
    if not exported_files:
        raise ValueError("No files exported from PostgreSQL!")
    
    # 2. SPARK PROCESSING PHASE
    print("\n2️⃣ SPARK PROCESSING PHASE...")
    spark = create_simple_spark_session()
    
    try:
        # 3. LOAD FROM FILES (memory safe CSV approach)
        print("\n3️⃣ LOAD REAL DATA FROM FILES...")
        real_df = load_real_data_from_exported_files(spark, export_dir)
        
        # 4. FEATURE ENGINEERING
        print("\n4️⃣ FEATURE ENGINEERING ON REAL DATA...")
        featured_df = add_temporal_features(real_df)
        featured_df = add_lag_features(featured_df)
        
        # 5. DAILY AGGREGATION  
        print("\n5️⃣ DAILY AGGREGATION...")
        daily_df = create_daily_aggregates_real(featured_df)
        
        # 6. TRAIN/TEST SPLIT
        print("\n6️⃣ TRAIN/TEST SPLIT...")
        train_df, test_df = daily_df.randomSplit([0.8, 0.2], seed=42)
        
        train_count = train_df.count()
        test_count = test_df.count()
        
        print(f"📅 Train: {train_count} records")
        print(f"📅 Test: {test_count} records")
        
        # 7. ML PIPELINE
        print("\n7️⃣ ML PIPELINE FOR REAL DATA...")
        pipeline, features, target = create_ml_pipeline_real()
        
        # 8. MODEL TRAINING
        print("\n8️⃣ MODEL TRAINING ON REAL DATA...")
        print("🌳 Training GBT on real energy consumption data...")
        
        model = pipeline.fit(train_df)
        print("✅ Model training on real data completed!")
        
        # 9. PREDICTIONS
        print("\n9️⃣ PREDICTIONS...")
        predictions = model.transform(test_df)
        
        # 10. EVALUATION
        print("\n🔟 EVALUATION...")
        metrics = evaluate_real_model(predictions, target)
        
        # 11. RESULTS
        print("\n🎉 HYBRID REAL DATA ML RESULTS (FIXED):")
        print("="*70)
        print(f"📊 REAL DATA METRICS:")
        print(f"   RMSE: {metrics['rmse']:,.2f}")
        print(f"   MAE:  {metrics['mae']:,.2f}")
        print(f"   R²:   {metrics['r2']:.4f}")
        
        # 12. SAMPLE PREDICTIONS
        print(f"\n📅 REAL DATA PREDICTIONS:")
        sample = predictions.select("date", "customer_id", target, "prediction").limit(5)
        sample.show(5, truncate=False)
        
        # 13. FEATURE IMPORTANCE
        if hasattr(model.stages[-1], 'featureImportances'):
            print(f"\n🌟 REAL DATA FEATURE IMPORTANCE:")
            importances = model.stages[-1].featureImportances.toArray()
            feature_importance = list(zip(features, importances))
            feature_importance.sort(key=lambda x: x[1], reverse=True)
            
            for i, (feature, importance) in enumerate(feature_importance):
                print(f"   {i+1}. {feature}: {importance:.4f}")
        
        print(f"\n🔥 HYBRID APPROACH BAŞARILI (FIXED)!")
        print(f"📤 PostgreSQL records exported: {total_exported:,}")
        print(f"📁 Files processed: {len(exported_files)}")
        print(f"📊 Spark records processed: {real_df.count():,}")
        print(f"📊 Daily records: {train_count + test_count:,}")
        print(f"📊 Real data R²: {metrics['r2']:.4f}")
        print(f"✅ PostgreSQL → Files → Spark ML SUCCESS (FIXED)! 🚀")
        
        return {
            "success": True,
            "model": model,
            "metrics": metrics,
            "exported_records": total_exported,
            "processed_records": real_df.count(),
            "daily_records": train_count + test_count,
            "exported_files": exported_files
        }
        
    except Exception as e:
        print(f"❌ Error during Spark processing: {e}")
        raise
        
    finally:
        print(f"\n🧹 CLEANUP...")
        spark.stop()
        print("✅ SparkSession stopped!")

if __name__ == "__main__":
    print("⚡ HYBRID POSTGRESQL → SPARK ML PIPELINE (FIXED)")
    print("📊 Timestamp fix + Real data + Memory-safe approach")
    
    try:
        result = hybrid_postgresql_to_spark_ml_fixed()
        
        if result["success"]:
            print(f"\n🎊 HYBRID APPROACH MÜKEMMEL BAŞARILI (FIXED)!")
            print(f"📤 Exported: {result['exported_records']:,} records")
            print(f"📊 Processed: {result['processed_records']:,} records")
            print(f"📊 Test R²: {result['metrics']['r2']:.4f}")
            print(f"📁 Files: {len(result['exported_files'])}")
            print(f"\n✅ Real PostgreSQL data ile ML model eğitildi (FIXED)! 🚀")
        else:
            print("❌ Hybrid approach failed!")
            
    except Exception as e:
        print(f"❌ Hybrid error: {e}")
        import traceback
        traceback.print_exc()
