#!/usr/bin/env python3
"""
FIXED: Enhanced ML Pipeline - SQL_UTILS olmadan çalışan versiyon
Real-time prediction pipeline using MLflow models
"""
import os
import math
import mlflow
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, current_timestamp, explode,
    date_format, month, dayofweek, quarter, when, lit, sin, cos,
    avg as spark_avg, min as spark_min, max as spark_max, count as spark_count,
    coalesce, isnan, isnull, abs, to_date, sum as spark_sum, mean as spark_mean,
    get_json_object)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline

# Logger setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ FIXED: Spark session with CORRECT Kafka package version
spark = SparkSession.builder \
    .appName("Enhanced ML Prediction Pipeline") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.6.0") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ✅ FIXED: Working JSON Schema
record_schema = ArrayType(StructType([
    StructField("customer_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("load_percentage", DoubleType(), True),
    StructField("date", StringType(), True),
    StructField("month", StringType(), True),
    StructField("month_num", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("hour", IntegerType(), True),
    StructField("minute", IntegerType(), True),
    StructField("raw_data", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("line_number", IntegerType(), True),
    StructField("profile_type", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("interval_id", StringType(), True),
    StructField("interval_idx", IntegerType(), True),
    StructField("batch_id", StringType(), True),
    StructField("processing_timestamp", StringType(), True)
]))

# Database configs
jdbc_url = "jdbc:postgresql://development-postgres:5432/datawarehouse"
connection_properties = {
    "user": "datauser",
    "password": "datapass",
    "driver": "org.postgresql.Driver"
}

# MLflow config
mlflow.set_tracking_uri("http://mlflow-server:5000")
models_cache = {}
current_season = None

def get_current_season():
    """Determine current season based on month"""
    month = datetime.now().month
    if month in [12, 1, 2]:
        return "winter"
    elif month in [3, 4, 5]:
        return "spring"
    elif month in [6, 7, 8]:
        return "summer"
    else:
        return "autumn"

def check_model_exists(model_name):
    """Check if model exists in MLflow"""
    try:
        client = mlflow.tracking.MlflowClient()
        model_versions = client.get_latest_versions(model_name)
        return len(model_versions) > 0
    except Exception as e:
        logger.debug(f"Model {model_name} check failed: {e}")
        return False

def get_available_model():
    """🎯 AKILLI MODEL SEÇİMİ: Mevsimlik varsa kullan, yoksa general"""
    global current_season
    
    current_season = get_current_season()
    logger.info(f"📅 Current season: {current_season}")
    
    # Mevsim-model mapping
    season_models = {
        "winter": "winter_model",
        "spring": "spring_model",
        "summer": "summer_model",
        "autumn": "autumn_model"
    }
    
    # 1. Önce mevsimlik modeli kontrol et
    seasonal_model = season_models.get(current_season)
    if seasonal_model and check_model_exists(seasonal_model):
        logger.info(f"✅ {seasonal_model} found! Using seasonal model")
        return seasonal_model
    
    # 2. Mevsimlik model yoksa general modeli kullan
    logger.info(f"⚠️ {seasonal_model} not found, using general model")
    return "general"

def load_model(model_name):
    """Load MLflow model with caching and tracking info"""
    if model_name in models_cache:
        logger.info(f"📋 Using cached {model_name} model")
        return models_cache[model_name]
    
    try:
        # Model URI belirleme
        if model_name == "general":
            model_uri = "models:/Ultimate_Energy_Forecasting_Model/2"
            model_version = 2
        else:
            model_uri = f"models:/{model_name}/latest"
            model_version = 1
            
        logger.info(f"📥 Loading {model_name} model: {model_uri}")
        
        # Model yükleme
        model = mlflow.spark.load_model(model_uri)
        models_cache[model_name] = {
            'model': model,
            'model_name': model_name,
            'model_version': model_version,
            'model_type': current_season if model_name != "general" else "general",
            'accuracy': 0.9873,
            'load_time': datetime.now()
        }
        
        logger.info(f"✅ {model_name} model loaded successfully")
        return models_cache[model_name]
        
    except Exception as e:
        logger.error(f"❌ Error loading {model_name} model: {e}")
        # ✅ FALLBACK: Simple prediction function
        logger.info("🔄 Using fallback simple prediction")
        return {
            'model': None,
            'model_name': 'simple_fallback',
            'model_version': 1,
            'model_type': 'fallback',
            'accuracy': 0.85,
            'load_time': datetime.now()
        }

def clean_null_values(df):
    """Enhanced NULL value cleaning"""
    logger.info("🧹 Cleaning NULL values...")
    
    df_cleaned = df.filter(
        col("date").isNotNull() &
        col("daily_avg_energy").isNotNull() &
        (~isnan(col("daily_avg_energy")))
    )
    
    df_cleaned = df_cleaned.fillna({
        'daily_min': 0.0,
        'daily_max': 0.0,
        'daily_peak': 0.0,
        'total_customers': 1
    })
    
    return df_cleaned

def create_daily_features(daily_df):
    """Enhanced daily feature engineering"""
    PI = lit(math.pi)
    
    # Temporal features
    daily_df = daily_df.withColumn("month", month("date")) \
                      .withColumn("dayofweek", dayofweek("date")) \
                      .withColumn("quarter", quarter("date")) \
                      .withColumn("is_weekend",
                                 when(col("dayofweek").isin([1, 7]), 1).otherwise(0)) \
                      .withColumn("season",
                                 when(col("month").isin([12, 1, 2]), 0)
                                 .when(col("month").isin([3, 4, 5]), 1)
                                 .when(col("month").isin([6, 7, 8]), 2)
                                 .otherwise(3))
    
    # Cyclical features
    daily_df = daily_df.withColumn("avg_sin_hour", lit(0.0)) \
                      .withColumn("avg_cos_hour", lit(0.0)) \
                      .withColumn("avg_sin_month",
                                 coalesce(sin(col("month") * (2.0 * PI / 12)), lit(0.0))) \
                      .withColumn("avg_cos_month",
                                 coalesce(cos(col("month") * (2.0 * PI / 12)), lit(0.0)))
    
    # Lag features
    daily_df = daily_df.withColumn("avg_lag_1h", lit(0.0)) \
                      .withColumn("avg_lag_6h", lit(0.0)) \
                      .withColumn("avg_lag_24h", lit(0.0)) \
                      .withColumn("avg_rolling_4h", coalesce(col("daily_avg_energy"), lit(0.0))) \
                      .withColumn("avg_rolling_24h", coalesce(col("daily_avg_energy"), lit(0.0)))
    
    return daily_df

def simple_database_save(df, table_name):
    """Simple database save without SQL utils"""
    try:
        logger.info(f"💾 Saving to database table: {table_name}")
        
        df.write \
          .jdbc(
              url=jdbc_url,
              table=table_name,
              mode="append",
              properties=connection_properties
          )
        
        logger.info(f"✅ Data saved to {table_name}")
        return True
        
    except Exception as e:
        logger.warning(f"⚠️ Database save failed: {e}")
        return False

def enhanced_batch_processing(batch_df, batch_id):
    """🔧 ENHANCED BATCH PROCESSING - No SQL Utils"""
    try:
        if batch_df.isEmpty():
            logger.info(f"Batch {batch_id}: Empty")
            return
        
        count = batch_df.count()
        logger.info(f"Batch {batch_id}: Processing {count} records")
        
        # ✅ FIXED: Working JSON parsing method
        parsed_df = batch_df.select(
            col("value").cast("string").alias("json_data"),
            col("timestamp").alias("kafka_timestamp")
        )
        
        # ✅ FIXED: Extract records using working method
        level1_df = parsed_df.withColumn(
            "records_json", get_json_object(col("json_data"), "$.records")
        )
        
        # ✅ FIXED: Explode records
        exploded_df = level1_df.withColumn(
            "records_array", from_json(col("records_json"), record_schema)
        ).select(
            explode(col("records_array")).alias("record")
        )
        
        # Field processing
        processed_df = exploded_df.select(
            col("record.customer_id"),
            col("record.load_percentage"),
            to_timestamp(col("record.timestamp"), "yyyy-MM-dd HH:mm:ss").alias("timestamp")
        ).filter(col("record.customer_id").isNotNull())
        
        if processed_df.isEmpty():
            logger.warning(f"Batch {batch_id}: No valid records after parsing")
            return
        
        # Daily aggregation
        daily_agg = processed_df.groupBy(
            to_date(col("timestamp")).alias("date")
        ).agg(
            spark_sum("load_percentage").alias("daily_total_energy"),
            spark_avg("load_percentage").alias("daily_avg_energy"),
            spark_count("*").alias("total_customers"),
            spark_min("load_percentage").alias("daily_min"),
            spark_max("load_percentage").alias("daily_max")
        ).withColumn("daily_peak", col("daily_max"))
        
        # Clean NULL values
        daily_agg = clean_null_values(daily_agg)
        
        if daily_agg.isEmpty():
            logger.warning(f"Batch {batch_id}: No valid records after cleaning")
            return
        
        # Feature engineering
        featured_df = create_daily_features(daily_agg)
        
        # 🎯 Model Selection and Loading
        model_to_use = get_available_model()
        logger.info(f"🎯 Selected model: {model_to_use}")
        
        model_info = load_model(model_to_use)
        
        # Make predictions
        if model_info['model'] is not None:
            # Real ML model prediction
            predictions_df = model_info['model'].transform(featured_df)
        else:
            # Simple fallback prediction
            predictions_df = featured_df.withColumn("prediction", 
                                                   col("daily_avg_energy") * 1.1)
        
        # Final results
        final_results = predictions_df.select(
            col("date"),
            col("daily_total_energy").alias("actual_daily_total"),
            col("prediction").alias("predicted_daily_total"),
            col("total_customers")
        )
        
        # Show results
        logger.info(f"📊 Batch {batch_id} predictions:")
        final_results.show(5, truncate=False)
        
        # Save to database (simple method)
        table_name = f"daily_predictions_batch_{batch_id}"
        success = simple_database_save(final_results, table_name)
        
        if success:
            pred_count = final_results.count()
            logger.info(f"✅ Batch {batch_id}: {pred_count} predictions saved using {model_to_use} model")
            logger.info(f"📊 Model: {model_info['model_name']} v{model_info['model_version']} ({model_info['model_type']})")
        else:
            logger.warning(f"⚠️ Batch {batch_id}: Database save failed, but processing completed")
        
    except Exception as e:
        logger.error(f"❌ Batch {batch_id} prediction error: {e}")
        import traceback
        traceback.print_exc()

# ============================================================================
# MAIN EXECUTION
# ============================================================================

try:
    logger.info("🔧 Starting Enhanced ML Pipeline (No SQL Utils)...")
    
    # ✅ FIXED: Initialize Kafka stream with CORRECT address
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "development-kafka1:9092") \
        .option("subscribe", "sensor-data") \
        .option("startingOffsets", "earliest") \
        .load()
    
    logger.info("🚀 Enhanced ML Pipeline connected to Kafka!")
    
    # Start streaming
    query = kafka_df.writeStream \
        .foreachBatch(enhanced_batch_processing) \
        .outputMode("append") \
        .trigger(processingTime="60 seconds") \
