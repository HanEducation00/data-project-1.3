#!/usr/bin/env python3
"""
SYSTEM TOTAL ENERGY FORECASTING PIPELINE
Predict daily total system energy consumption from smart meter data
"""

import os
import time
import numpy as np
import pandas as pd
from datetime import datetime

# Spark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg as spark_avg, max as spark_max, min as spark_min,
    count as spark_count, date_format, hour, dayofweek, month, year,
    lag, row_number, sin, cos, lit, when, sqrt, abs as spark_abs
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# ML imports
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# MLflow imports
import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient

def setup_mlflow():
    """Setup MLflow tracking"""
    tracking_uri = "http://mlflow-server:5000"
    experiment_name = "System_Total_Energy_Forecasting"
    
    mlflow.set_tracking_uri(tracking_uri)
    
    try:
        experiment_id = mlflow.create_experiment(experiment_name)
        print(f"✅ Created new experiment: {experiment_name}")
    except:
        experiment = mlflow.get_experiment_by_name(experiment_name)
        experiment_id = experiment.experiment_id
        print(f"✅ Using existing experiment: {experiment_name}")
    
    mlflow.set_experiment(experiment_name)
    
    print(f"📁 MLflow tracking URI: {tracking_uri}")
    print(f"🧪 Experiment: {experiment_name}")
    print(f"🆔 Experiment ID: {experiment_id}")
    
    return tracking_uri, experiment_name, experiment_id

def create_spark_session():
    """Create optimized Spark session"""
    print("⚡ CREATING SPARK SESSION...")
    
    spark = SparkSession.builder \
        .appName("System_Total_Energy_Forecasting") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256MB") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.default.parallelism", "8") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✅ Spark session created: {spark.version}")
    return spark

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
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
    
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
    date_stats = spark_df.select(
        spark_min("full_timestamp").alias("start_date"),
        spark_max("full_timestamp").alias("end_date")
    ).collect()[0]
    
    print(f"📅 Date range: {date_stats['start_date']} to {date_stats['end_date']}")
    
    return spark_df, final_count



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


def create_ml_pipeline(feature_columns):
    """
    Create ML pipeline with feature scaling and GBT regressor
    """
    print("🤖 CREATING ML PIPELINE...")
    
    # Vector assembler
    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="raw_features"
    )
    
    # Feature scaler
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="scaled_features",
        withStd=True,
        withMean=True
    )
    
    # GBT Regressor
    gbt = GBTRegressor(
        featuresCol="scaled_features",
        labelCol="total_daily_energy",
        predictionCol="prediction",
        maxDepth=6,
        maxBins=32,
        maxIter=100,
        stepSize=0.1,
        subsamplingRate=0.8,
        featureSubsetStrategy="sqrt",
        seed=42
    )
    
    # Create pipeline
    pipeline = Pipeline(stages=[assembler, scaler, gbt])
    
    print(f"✅ Pipeline created with {len(feature_columns)} features")
    print(f"🎯 Target: total_daily_energy")
    print(f"🌳 Algorithm: Gradient Boosted Trees")
    
    return pipeline

def train_and_evaluate_model(pipeline, train_df, val_df, test_df):
    """
    Train model and evaluate performance
    """
    print("🚀 TRAINING MODEL...")
    train_start = time.time()
    
    # Train the pipeline
    model = pipeline.fit(train_df)
    
    training_time = time.time() - train_start
    print(f"✅ Training completed in {training_time:.1f}s")
    
    # Make predictions
    print("📊 EVALUATING MODEL...")
    
    train_predictions = model.transform(train_df)
    val_predictions = model.transform(val_df)
    test_predictions = model.transform(test_df)
    
    # Evaluator
    evaluator = RegressionEvaluator(
        labelCol="total_daily_energy",
        predictionCol="prediction"
    )
    
    # Calculate metrics
    def calculate_metrics(predictions_df, dataset_name):
        r2 = evaluator.evaluate(predictions_df, {evaluator.metricName: "r2"})
        rmse = evaluator.evaluate(predictions_df, {evaluator.metricName: "rmse"})
        mae = evaluator.evaluate(predictions_df, {evaluator.metricName: "mae"})
        
        return {
            f"{dataset_name}_r2": r2,
            f"{dataset_name}_rmse": rmse,
            f"{dataset_name}_mae": mae
        }
    
    # Get metrics for all sets
    train_metrics = calculate_metrics(train_predictions, "train")
    val_metrics = calculate_metrics(val_predictions, "val")
    test_metrics = calculate_metrics(test_predictions, "test")
    
    # Combine all metrics
    all_metrics = {**train_metrics, **val_metrics, **test_metrics, "training_time": training_time}
    
    # Print results
    print(f"🎯 MODEL PERFORMANCE:")
    print(f"   📚 Train  R²: {train_metrics['train_r2']:.4f} | RMSE: {train_metrics['train_rmse']:.2f} | MAE: {train_metrics['train_mae']:.2f}")
    print(f"   🔍 Val    R²: {val_metrics['val_r2']:.4f} | RMSE: {val_metrics['val_rmse']:.2f} | MAE: {val_metrics['val_mae']:.2f}")
    print(f"   🧪 Test   R²: {test_metrics['test_r2']:.4f} | RMSE: {test_metrics['test_rmse']:.2f} | MAE: {test_metrics['test_mae']:.2f}")
    
    # Overfitting analysis
    train_test_gap = train_metrics['train_r2'] - test_metrics['test_r2']
    print(f"   🔬 Overfitting Gap: {train_test_gap:.4f}")
    
    if train_test_gap > 0.1:
        print("   ⚠️  Model might be overfitting")
    else:
        print("   ✅ Good generalization")
    
    return model, all_metrics, test_predictions

def get_feature_importance(model, feature_columns):
    """
    Extract and display feature importance
    """
    print("📈 ANALYZING FEATURE IMPORTANCE...")
    
    try:
        # Get the GBT model (last stage in pipeline)
        gbt_model = model.stages[-1]
        importance_scores = gbt_model.featureImportances.toArray()
        
        # Create importance dictionary
        importance_dict = dict(zip(feature_columns, importance_scores))
        
        # Sort by importance
        sorted_features = sorted(importance_dict.items(), key=lambda x: x[1], reverse=True)
        
        print(f"🔝 TOP 10 MOST IMPORTANT FEATURES:")
        for i, (feature, score) in enumerate(sorted_features[:10], 1):
            print(f"   {i:2d}. {feature:<25} = {score:.4f}")
        
        return importance_dict
        
    except Exception as e:
        print(f"❌ Could not extract feature importance: {e}")
        return {}

def log_to_mlflow(metrics, feature_columns, importance_dict, hyperparams, model=None):
    """Log comprehensive results to MLflow"""
    print("📝 LOGGING TO MLFLOW...")
    
    # Log hyperparameters
    for param, value in hyperparams.items():
        mlflow.log_param(param, value)
    
    # Log metrics
    for metric, value in metrics.items():
        mlflow.log_metric(metric, value)
    
    # Log feature info
    mlflow.log_param("total_features", len(feature_columns))
    mlflow.log_param("feature_list", ", ".join(feature_columns[:10]))  # First 10
    
    # Log feature importance
    for feature, importance in importance_dict.items():
        mlflow.log_metric(f"feature_importance_{feature}", importance)
    
    # ✅ FIX: SAVE MODEL TO MLFLOW
    try:
        if model is not None:
            mlflow.spark.log_model(
                spark_model=model,
                artifact_path="energy_forecasting_model",
                registered_model_name="SystemEnergyForecaster"
            )
            print("✅ Model saved to MLflow!")
            
            # Also save locally as backup
            model_path = "/workspace/models/energy_forecaster_gbt"
            model.write().overwrite().save(model_path)
            print(f"✅ Model saved locally: {model_path}")
            
    except Exception as e:
        print(f"⚠️  MLflow model save failed: {e}")
        
        # Fallback: Save locally only
        try:
            model_path = "/workspace/models/energy_forecaster_gbt" 
            model.write().overwrite().save(model_path)
            print(f"✅ Model saved locally: {model_path}")
            mlflow.log_param("local_model_path", model_path)
        except Exception as e2:
            print(f"❌ Local model save also failed: {e2}")
    
    print("✅ MLflow logging completed")


def main():
    """Main pipeline execution"""
    print("🌟 SYSTEM TOTAL ENERGY FORECASTING PIPELINE")
    print("="*80)
    
    # Setup MLflow
    tracking_uri, experiment_name, experiment_id = setup_mlflow()
    
    # Start MLflow run
    with mlflow.start_run(run_name=f"System_Energy_FullYear_{datetime.now().strftime('%Y%m%d_%H%M%S')}") as run:
        
        print(f"🆔 MLflow Run ID: {run.info.run_id}")
        pipeline_start = time.time()
        
        try:
            # 1-6. Existing data pipeline steps...
            spark = create_spark_session()
            df, record_count = load_smart_meter_data(spark)
            system_df, system_time = create_system_level_aggregation(df)
            daily_df, day_count, daily_time = create_daily_total_energy(system_df)
            feature_df, feature_columns, feature_time = create_advanced_features(daily_df)
            train_df, val_df, test_df, train_count, val_count, test_count = prepare_ml_dataset(
                feature_df, feature_columns, test_days=60
            )
            
            # 7. Create and train ML pipeline
            ml_pipeline = create_ml_pipeline(feature_columns)
            
            # ✅ FIX: Define hyperparams BEFORE logging
            hyperparams = {
                "algorithm": "GBTRegressor",
                "maxDepth": 6,
                "maxBins": 32,
                "maxIter": 100,
                "stepSize": 0.1,
                "subsamplingRate": 0.8,
                "featureSubsetStrategy": "sqrt",
                "seed": 42,
                "test_days": 60,
                "total_features": len(feature_columns),
                "spark_version": spark.version
            }
            
            model, metrics, test_predictions = train_and_evaluate_model(
                ml_pipeline, train_df, val_df, test_df
            )
            
            # 8. Feature importance analysis
            importance_dict = get_feature_importance(model, feature_columns)
            
            # 9. ✅ FIX: Single MLflow logging call with model
            log_to_mlflow(metrics, feature_columns, importance_dict, hyperparams, model)
            
            # 10. Additional metrics (not duplicate logging)
            mlflow.log_metric("total_raw_records", record_count)
            mlflow.log_metric("total_days", day_count)
            mlflow.log_metric("train_days", train_count)
            mlflow.log_metric("val_days", val_count)
            mlflow.log_metric("test_days", test_count)
            mlflow.log_metric("system_aggregation_time", system_time)
            mlflow.log_metric("daily_aggregation_time", daily_time)
            mlflow.log_metric("feature_engineering_time", feature_time)
            
            # 11. Final summary
            total_pipeline_time = time.time() - pipeline_start
            mlflow.log_metric("total_pipeline_time", total_pipeline_time)
            
            # Success logging
            mlflow.log_param("pipeline_status", "SUCCESS")
            
            print("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
            return True
            
        except Exception as e:
            print(f"❌ PIPELINE ERROR: {e}")
            import traceback
            traceback.print_exc()
            
            # Error logging
            mlflow.log_param("pipeline_status", "FAILED")
            mlflow.log_param("error_message", str(e))
            return False
            
        finally:
            if 'spark' in locals():
                spark.stop()

def log_to_mlflow(metrics, feature_columns, importance_dict, hyperparams, model=None):
    """✅ IMPROVED: Log comprehensive results to MLflow"""
    print("📝 LOGGING TO MLFLOW...")
    
    # Log hyperparameters
    for param, value in hyperparams.items():
        mlflow.log_param(param, value)
    
    # Log metrics
    for metric, value in metrics.items():
        mlflow.log_metric(metric, value)
    
    # Log feature info
    mlflow.log_param("total_features", len(feature_columns))
    mlflow.log_param("feature_list", ", ".join(feature_columns[:10]))
    
    # Log feature importance
    for feature, importance in importance_dict.items():
        mlflow.log_metric(f"feature_importance_{feature}", importance)
    
    # ✅ IMPROVED: Model saving with better error handling
    if model is not None:
        try:
            # 1. Save to MLflow Model Registry
            mlflow.spark.log_model(
                spark_model=model,
                artifact_path="energy_forecasting_model",
                registered_model_name="SystemEnergyForecaster"
            )
            print("✅ Model registered in MLflow Model Registry!")
            
            # 2. ✅ FIX: Create local backup directory
            import os
            model_dir = "/workspace/models"
            os.makedirs(model_dir, exist_ok=True)
            
            model_path = f"{model_dir}/energy_forecaster_pipeline"
            model.write().overwrite().save(model_path)
            print(f"✅ Model saved locally: {model_path}")
            
            # Log local path for reference
            mlflow.log_param("local_model_path", model_path)
            mlflow.log_param("model_save_status", "SUCCESS")
            
        except Exception as e:
            print(f"⚠️ Model save failed: {e}")
            mlflow.log_param("model_save_status", "FAILED")
            mlflow.log_param("model_save_error", str(e))
    
    print("✅ MLflow logging completed")

if __name__ == "__main__":
    main()



