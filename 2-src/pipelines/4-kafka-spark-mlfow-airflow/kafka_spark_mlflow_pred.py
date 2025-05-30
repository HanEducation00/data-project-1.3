#!/usr/bin/env python3
"""
Enhanced ML Pipeline with Season-based Model Loading
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
    coalesce, isnan, isnull, abs, to_date, sum as spark_sum, mean as spark_mean)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline

# Import SQL utilities
from sql_utils import execute_sql_script, setup_database

# Logger setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Spark session
spark = SparkSession.builder \
    .appName("Enhanced ML Prediction Pipeline") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.6.0") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# JSON Schema
record_schema = StructType([
    StructField("raw_data", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("line_number", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("profile_type", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", StringType(), True),
    StructField("month_num", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("date", StringType(), True),
    StructField("interval_id", StringType(), True),
    StructField("interval_idx", IntegerType(), True),
    StructField("hour", IntegerType(), True),
    StructField("minute", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("load_percentage", DoubleType(), True),
    StructField("batch_id", StringType(), True),
    StructField("processing_timestamp", StringType(), True)
])

json_schema = StructType([
    StructField("interval_id", StringType(), True),
    StructField("batch_id", IntegerType(), True),
    StructField("record_count", IntegerType(), True),
    StructField("processing_timestamp", StringType(), True),
    StructField("records", ArrayType(record_schema), True)
])

# Database configs
jdbc_url = "jdbc:postgresql://postgres:5432/datawarehouse"
connection_properties = {
    "user": "datauser",
    "password": "datapass",
    "driver": "org.postgresql.Driver"
}

# MLflow config
mlflow.set_tracking_uri("http://mlflow:5000")
models_cache = {}
current_season = None


def get_current_season():
    """Determine current season based on month"""
    month = datetime.now().month
    if month in [12, 1, 2]:  # Aralık, Ocak, Şubat
        return "winter"
    elif month in [3, 4, 5]:  # Mart, Nisan, Mayıs  
        return "spring"
    elif month in [6, 7, 8]:  # Haziran, Temmuz, Ağustos
        return "summer"
    else:  # Eylül, Ekim, Kasım
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
        "winter": "winter_model",    # Kış modeli
        "spring": "spring_model",    # İlkbahar modeli
        "summer": "summer_model",    # Yaz modeli
        "autumn": "autumn_model"     # Sonbahar modeli
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
            model_version = 1  # Latest version for seasonal models
            
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
        return None

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

def enhanced_batch_processing(batch_df, batch_id):
    """🔧 ENHANCED BATCH PROCESSING - Optimized with UPSERT"""
    try:
        if batch_df.isEmpty():
            logger.info(f"Batch {batch_id}: Empty")
            return
        
        count = batch_df.count()
        logger.info(f"Batch {batch_id}: Processing {count} records")
        
        # JSON parsing
        parsed_df = batch_df \
            .selectExpr("CAST(value AS STRING) as json_data") \
            .select(from_json("json_data", json_schema).alias("data")) \
            .select("data.*")
        
        # Records array'ini explode et
        exploded_df = parsed_df \
            .selectExpr("batch_id", "record_count", "processing_timestamp", "explode(records) as record")
        
        # Field processing
        processed_df = exploded_df \
            .select(
                col("record.customer_id"),
                col("record.load_percentage"),
                to_timestamp(col("record.timestamp")).alias("timestamp")
            ) \
            .filter(col("record.customer_id").isNotNull())
        
        if processed_df.isEmpty():
            logger.warning(f"Batch {batch_id}: No valid records after parsing")
            return
        
        # Daily aggregation - GÜNLÜK TOPLAM (customer_id olmadan)
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
        
        # Double conversion for features
        double_cols = ["month", "dayofweek", "quarter", "season", "is_weekend", "total_customers"]
        for col_name in double_cols:
            featured_df = featured_df.withColumn(f"{col_name}_double", col(col_name).cast("double"))
        
        # Model expected column names
        featured_df = featured_df.withColumnRenamed("total_customers", "hourly_count") \
                                 .withColumnRenamed("total_customers_double", "hourly_count_double")
        
        # 🎯 Model Selection and Loading (single call)
        model_to_use = get_available_model()
        logger.info(f"🎯 Selected model: {model_to_use}")
        
        model_info = load_model(model_to_use)
        if not model_info:
            logger.error(f"❌ Failed to load {model_to_use} model")
            return
        
        model = model_info['model']
        
        # Make predictions
        predictions_df = model.transform(featured_df)
        
        # Temp table name
        temp_table = f"temp_predictions_batch_{batch_id}"
        
        # Final results with correct column names for UPSERT
        final_results = predictions_df.select(
            col("date"),
            col("daily_total_energy").alias("actual_daily_total"),
            col("prediction").alias("predicted_daily_total"),
            col("hourly_count").alias("total_customers")
        )
        
        # Write to temporary table
        final_results.write \
            .jdbc(
                url=jdbc_url,
                table=temp_table,
                mode="overwrite",
                properties=connection_properties
            )
        
        # 🔧 UPSERT using SQL script with model tracking
        success = execute_sql_script(
            "upsert_daily_predictions.sql",
            temp_table=temp_table,
            batch_id=batch_id,
            model_type=model_info['model_type'],
            model_name=model_info['model_name'],
            model_version=model_info['model_version']
        )
        
        if success:
            pred_count = final_results.count()
            logger.info(f"✅ Batch {batch_id}: {pred_count} predictions UPSERTED using {model_to_use} model")
            logger.info(f"📊 Model: {model_info['model_name']} v{model_info['model_version']} ({model_info['model_type']})")
        else:
            logger.error(f"❌ Batch {batch_id}: UPSERT failed")
        
    except Exception as e:
        logger.error(f"❌ Batch {batch_id} prediction error: {e}")
        import traceback
        traceback.print_exc()

# ============================================================================
# MAIN EXECUTION
# ============================================================================

try:
    # Setup database first
    logger.info("🔧 Setting up database...")
    setup_database()
    
    # Initialize Kafka stream
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:9092") \
        .option("subscribe", "sensor-data") \
        .option("startingOffsets", "earliest") \
        .load()
    
    logger.info("🚀 Enhanced ML Pipeline connected to Kafka!")
    
    # Start streaming
    query = kafka_df.writeStream \
        .foreachBatch(enhanced_batch_processing) \
        .outputMode("append") \
        .trigger(processingTime="60 seconds") \
        .start()
    
    logger.info("🎯 Enhanced ML streaming started with UPSERT support!")
    query.awaitTermination()

except Exception as e:
    logger.error(f"❌ Pipeline error: {e}")
    import traceback
    traceback.print_exc()

spark.stop()
