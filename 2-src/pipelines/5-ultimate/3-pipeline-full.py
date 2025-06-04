#!/usr/bin/env python3
"""
KAFKA STREAMING ML PIPELINE - Using Your Successful Pattern
"""

import os
import math
import mlflow
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

# Logger setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Senin pattern'in - Spark session
spark = SparkSession.builder \
    .appName("Kafka_ML_Pipeline_Fixed") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.6.0") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ✅ Senin pattern'in - JSON Schema
energy_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("energy_consumption", DoubleType(), True),
    StructField("batch_id", StringType(), True)
])

# ✅ Senin pattern'in - Database configs
jdbc_url = "jdbc:postgresql://postgres-db:5432/energy_db"
connection_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# ✅ Senin pattern'in - MLflow config
mlflow.set_tracking_uri("http://mlflow-server:5000")
models_cache = {}

def check_model_exists(model_name):
    """✅ Senin pattern'in - Model existence check"""
    try:
        client = mlflow.tracking.MlflowClient()
        model_versions = client.get_latest_versions(model_name)
        return len(model_versions) > 0
    except Exception as e:
        logger.debug(f"Model {model_name} check failed: {e}")
        return False

def get_available_model():
    """✅ Senin pattern'in - Smart model selection"""
    # Simple model selection for now
    if check_model_exists("energy_forecaster"):
        logger.info("✅ energy_forecaster found!")
        return "energy_forecaster"
    
    logger.info("⚠️ Using fallback model")
    return "fallback"

def load_model(model_name):
    """✅ Senin pattern'in - Model loading with caching"""
    if model_name in models_cache:
        logger.info(f"📋 Using cached {model_name} model")
        return models_cache[model_name]
    
    try:
        if model_name == "energy_forecaster":
            model_uri = "models:/SystemEnergyForecaster/latest"
        else:
            # Create a simple fallback model
            return create_fallback_model()
        
        logger.info(f"📥 Loading {model_name} model: {model_uri}")
        
        model = mlflow.spark.load_model(model_uri)
        models_cache[model_name] = {
            'model': model,
            'model_name': model_name,
            'load_time': datetime.now()
        }
        
        logger.info(f"✅ {model_name} model loaded successfully")
        return models_cache[model_name]
        
    except Exception as e:
        logger.error(f"❌ Error loading {model_name} model: {e}")
        return create_fallback_model()

def create_fallback_model():
    """Create simple fallback model"""
    from pyspark.ml.regression import LinearRegression
    
    # Simple pipeline
    assembler = VectorAssembler(
        inputCols=["hour", "day_of_week"],
        outputCol="features"
    )
    
    lr = LinearRegression(
        featuresCol="features",
        labelCol="daily_total_energy"
    )
    
    pipeline = Pipeline(stages=[assembler, lr])
    
    return {
        'model': pipeline,
        'model_name': 'fallback',
        'load_time': datetime.now()
    }

def create_features(df):
    """✅ Senin pattern'in - Feature engineering"""
    PI = lit(math.pi)
    
    # Add time features
    df = df.withColumn("hour", hour("timestamp")) \
           .withColumn("day_of_week", dayofweek("timestamp")) \
           .withColumn("month", month("timestamp"))
    
    # Cyclical features
    df = df.withColumn("hour_sin", sin(col("hour") * (2.0 * PI / 24))) \
           .withColumn("hour_cos", cos(col("hour") * (2.0 * PI / 24)))
    
    return df

def enhanced_batch_processing(batch_df, batch_id):
    """✅ Senin pattern'in - Enhanced batch processing"""
    try:
        if batch_df.isEmpty():
            logger.info(f"Batch {batch_id}: Empty")
            return
        
        count = batch_df.count()
        logger.info(f"Batch {batch_id}: Processing {count} records")
        
        # ✅ Parse JSON data
        parsed_df = batch_df \
            .selectExpr("CAST(value AS STRING) as json_data") \
            .select(from_json("json_data", energy_schema).alias("data")) \
            .select("data.*")
        
        # ✅ Convert timestamp
        processed_df = parsed_df \
            .withColumn("timestamp", to_timestamp(col("timestamp"))) \
            .filter(col("customer_id").isNotNull())
        
        if processed_df.isEmpty():
            logger.warning(f"Batch {batch_id}: No valid records after parsing")
            return
        
        # ✅ Daily aggregation
        daily_agg = processed_df.groupBy(
            to_date(col("timestamp")).alias("date")
        ).agg(
            sum("energy_consumption").alias("daily_total_energy"),
            avg("energy_consumption").alias("daily_avg_energy"),
            count("*").alias("total_customers")
        )
        
        # ✅ Feature engineering
        featured_df = create_features(daily_agg)
        
        # ✅ Model selection and loading
        model_to_use = get_available_model()
        logger.info(f"🎯 Selected model: {model_to_use}")
        
        model_info = load_model(model_to_use)
        if not model_info:
            logger.error(f"❌ Failed to load {model_to_use} model")
            return
        
        # ✅ Make predictions (if model is trained)
        try:
            model = model_info['model']
            if hasattr(model, 'transform'):  # Trained model
                predictions_df = model.transform(featured_df)
            else:  # Untrained pipeline
                logger.info("⚠️ Using fallback predictions")
                predictions_df = featured_df.withColumn("prediction", col("daily_avg_energy") * 1.1)
        except Exception as e:
            logger.warning(f"⚠️ Prediction failed, using fallback: {e}")
            predictions_df = featured_df.withColumn("prediction", col("daily_avg_energy") * 1.1)
        
        # ✅ Save results
        final_results = predictions_df.select(
            col("date"),
            col("daily_total_energy").alias("actual"),
            col("prediction").alias("predicted"),
            col("total_customers")
        )
        
        # Save to PostgreSQL
        temp_table = f"predictions_batch_{batch_id}"
        final_results.write \
            .jdbc(
                url=jdbc_url,
                table=temp_table,
                mode="overwrite",
                properties=connection_properties
            )
        
        pred_count = final_results.count()
        logger.info(f"✅ Batch {batch_id}: {pred_count} predictions saved using {model_to_use}")
        
    except Exception as e:
        logger.error(f"❌ Batch {batch_id} processing error: {e}")
        import traceback
        traceback.print_exc()

# ✅ Senin pattern'in - Main execution
try:
    logger.info("🔧 Setting up Kafka stream...")
    
    # ✅ Initialize Kafka stream
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "development-kafka1:9092") \
        .option("subscribe", "energy-data") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    logger.info("🚀 Kafka ML Pipeline connected!")
    
    # ✅ Start streaming with your pattern
    query = kafka_df.writeStream \
        .foreachBatch(enhanced_batch_processing) \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    logger.info("🎯 ML streaming started!")
    logger.info("⏳ Waiting for Kafka data... (Press Ctrl+C to stop)")
    
    query.awaitTermination()
    
except KeyboardInterrupt:
    logger.info("🛑 Pipeline stopped by user")
except Exception as e:
    logger.error(f"❌ Pipeline error: {e}")
    import traceback
    traceback.print_exc()
finally:
    spark.stop()
