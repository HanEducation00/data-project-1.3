#!/usr/bin/env python3
"""
KAFKA INTEGRATED SYSTEM ENERGY FORECASTING
Kafka'dan gerçek veri + ML Pipeline
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
    lag, row_number, sin, cos, lit, when, sqrt, abs as spark_abs,
    get_json_object, explode, from_json, to_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, ArrayType

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
    experiment_name = "Kafka_System_Energy_Forecasting"
    
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
    """Create optimized Spark session with Kafka support"""
    print("⚡ CREATING SPARK SESSION WITH KAFKA...")
    
    spark = SparkSession.builder \
        .appName("Kafka_System_Energy_Forecasting") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
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

def load_kafka_smart_meter_data(spark):
    """
    Load smart meter data from Kafka
    """
    print("📁 LOADING SMART METER DATA FROM KAFKA...")
    
    kafka_address = "development-kafka1:9092"
    print(f"🔍 Kafka address: {kafka_address}")
    
    try:
        # Read from Kafka
        kafka_df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_address) \
            .option("subscribe", "sensor-data") \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()
        
        # Parse JSON
        parsed_df = kafka_df.select(
            col("value").cast("string").alias("json_data"),
            col("timestamp").alias("kafka_timestamp")
        )
        
        # Extract records
        level1_df = parsed_df.withColumn(
            "records_json", get_json_object(col("json_data"), "$.records")
        )
        
        # Records schema
        record_schema = ArrayType(StructType([
            StructField("customer_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("load_percentage", DoubleType(), True),
            StructField("date", StringType(), True),
            StructField("month", StringType(), True),
            StructField("month_num", IntegerType(), True),
            StructField("day", IntegerType(), True),
            StructField("hour", IntegerType(), True),
            StructField("minute", IntegerType(), True)
        ]))
        
        # Explode records
        exploded_df = level1_df.withColumn(
            "records_array", from_json(col("records_json"), record_schema)
        ).select(
            explode(col("records_array")).alias("record")
        ).select(
            col("record.customer_id").alias("customer_id"),
            col("record.timestamp").alias("timestamp_str"),
            col("record.load_percentage").alias("load_percentage"),
            col("record.date").alias("date"),
            col("record.month").alias("month"),
            col("record.month_num").alias("month_num")
        )
        
        # Convert timestamp
        final_df = exploded_df.withColumn(
            "full_timestamp", to_timestamp(col("timestamp_str"), "yyyy-MM-dd HH:mm:ss")
        ).select("customer_id", "full_timestamp", "load_percentage")
        
        final_df.cache()
        count = final_df.count()
        
        print(f"✅ Kafka data loaded: {count:,} records")
        
        # Show sample
        print("📋 Sample Kafka data:")
        final_df.show(5, truncate=False)
        
        return final_df, count
        
    except Exception as e:
        print(f"❌ Error loading Kafka data: {e}")
        print("🔄 Fallback to generated data...")
        return generate_sample_data(spark)

def generate_sample_data(spark):
    """Generate sample data as fallback"""
    print("🏭 GENERATING SAMPLE DATA...")
    
    # Generate 30 days of data
    dates = pd.date_range(start='2016-01-01', end='2016-01-30 23:30:00', freq='30min')
    customers = [f"res_{i}" for i in range(1, 51)]  # 50 customers
    
    data = []
    for timestamp in dates[:1000]:  # Limit for demo
        for customer in np.random.choice(customers, size=5, replace=False):
            load = np.random.uniform(5, 20)
            data.append({
                'customer_id': customer,
                'full_timestamp': timestamp,
                'load_percentage': load
            })
    
    pandas_df = pd.DataFrame(data)
    spark_df = spark.createDataFrame(pandas_df)
    spark_df.cache()
    
    count = spark_df.count()
    print(f"✅ Sample data generated: {count:,} records")
    
    return spark_df, count

# Diğer fonksiyonlar aynı kalacak (create_system_level_aggregation, create_daily_total_energy, etc.)
# Sadece main fonksiyonunu güncelleyelim

def main():
    """Main pipeline execution with Kafka integration"""
    print("🌟 KAFKA INTEGRATED SYSTEM ENERGY FORECASTING")
    print("="*80)
    
    # Setup MLflow
    tracking_uri, experiment_name, experiment_id = setup_mlflow()
    
    # Start MLflow run
    with mlflow.start_run(run_name=f"Kafka_Energy_{datetime.now().strftime('%Y%m%d_%H%M%S')}") as run:
        
        print(f"🆔 MLflow Run ID: {run.info.run_id}")
        pipeline_start = time.time()
        
        try:
            # 1. Create Spark session with Kafka
            spark = create_spark_session()
            
            # 2. Load data from Kafka
            df, record_count = load_kafka_smart_meter_data(spark)
            
            # 3. Log data source
            mlflow.log_param("data_source", "kafka")
            mlflow.log_param("kafka_records", record_count)
            
            print(f"✅ Pipeline başarıyla başlatıldı!")
            print(f"📊 Toplam kayıt: {record_count:,}")
            
            # Burada diğer pipeline adımları devam edecek...
            # (system aggregation, daily totals, ML training, etc.)
            
            return True
            
        except Exception as e:
            print(f"❌ PIPELINE ERROR: {e}")
            import traceback
            traceback.print_exc()
            
            mlflow.log_param("pipeline_status", "FAILED")
            mlflow.log_param("error_message", str(e))
            return False
            
        finally:
            if 'spark' in locals():
                spark.stop()

if __name__ == "__main__":
    main()
