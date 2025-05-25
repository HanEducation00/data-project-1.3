#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Veri Şema Tanımlamaları

Bu modül, uygulama içinde kullanılan tüm veri şemalarını ve yapılarını tanımlar.
Kafka mesajları, ham veri yapısı ve işlenmiş veri yapıları için şemaları içerir."""

from pyspark.sql.types import *
from utils.logger import get_logger

# Logger oluştur
logger = get_logger(__name__)

# Veritabanı tablo tanımları
RAW_DATA_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw_data (
    id SERIAL PRIMARY KEY,
    raw_json JSONB NOT NULL,
    kafka_topic VARCHAR(100) NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL, 
    ingestion_timestamp TIMESTAMP NOT NULL,
    processing_timestamp TIMESTAMP NOT NULL,
    processing_status VARCHAR(20) NOT NULL,
    error_message TEXT,
    UNIQUE(kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_raw_data_status ON raw_data(processing_status);
CREATE INDEX IF NOT EXISTS idx_raw_data_timestamp ON raw_data(ingestion_timestamp);
"""

LOAD_DATA_DETAIL_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS load_data_detail (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    interval_timestamp TIMESTAMP NOT NULL,
    load_percentage DOUBLE PRECISION NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    hour INTEGER NOT NULL,
    minute INTEGER NOT NULL,
    batch_id VARCHAR(50) NOT NULL,
    raw_data_id INTEGER REFERENCES raw_data(id),
    processing_timestamp TIMESTAMP NOT NULL,
    is_valid BOOLEAN NOT NULL DEFAULT true,
    UNIQUE(customer_id, interval_timestamp, batch_id)
);

CREATE INDEX IF NOT EXISTS idx_load_data_detail_customer_id ON load_data_detail(customer_id);
CREATE INDEX IF NOT EXISTS idx_load_data_detail_timestamp ON load_data_detail(interval_timestamp);
"""

MONTHLY_AVERAGE_CONSUMPTION_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS monthly_average_consumption (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    avg_load DOUBLE PRECISION NOT NULL,
    min_load DOUBLE PRECISION NOT NULL,
    max_load DOUBLE PRECISION NOT NULL,
    std_deviation DOUBLE PRECISION NOT NULL,
    customer_count INTEGER NOT NULL,
    record_count INTEGER NOT NULL,
    last_updated TIMESTAMP NOT NULL,
    UNIQUE(customer_id, year, month)
);

CREATE INDEX IF NOT EXISTS idx_monthly_avg_customer ON monthly_average_consumption(customer_id);
CREATE INDEX IF NOT EXISTS idx_monthly_avg_date ON monthly_average_consumption(year, month);
"""

CUSTOMER_FEATURES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS customer_features (
    customer_id VARCHAR(50) NOT NULL,
    feature_date DATE NOT NULL,
    avg_daily_consumption DOUBLE PRECISION,
    peak_hour_consumption DOUBLE PRECISION,
    weekend_weekday_ratio DOUBLE PRECISION,
    last_updated TIMESTAMP NOT NULL,
    PRIMARY KEY(customer_id, feature_date)
);

CREATE INDEX IF NOT EXISTS idx_customer_features_date ON customer_features(feature_date);
"""

MODEL_PREDICTIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS model_predictions (
    id SERIAL PRIMARY KEY,
    prediction_timestamp TIMESTAMP NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    predicted_value DOUBLE PRECISION NOT NULL,
    actual_value DOUBLE PRECISION,
    error_metric DOUBLE PRECISION,
    features JSONB
);

CREATE INDEX IF NOT EXISTS idx_model_predictions_customer ON model_predictions(customer_id);
CREATE INDEX IF NOT EXISTS idx_model_predictions_timestamp ON model_predictions(prediction_timestamp);
CREATE INDEX IF NOT EXISTS idx_model_predictions_model ON model_predictions(model_version);
"""

def get_kafka_message_schema():
    """
    Kafka'dan gelen ham mesajların şemasını döndürür.
    
    Returns:
        StructType: Kafka mesaj şeması
    """
    return StructType([
        StructField("raw_data", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("line_number", IntegerType(), True)
    ])

# ... (dosyanın geri kalanı değişmiyor)

def get_raw_data_schema():
    """
    Ham elektrik yük verilerinin şemasını döndürür.
    
    Returns:
        StructType: Ham veri şeması
    """
    return StructType([
        StructField("customer_id", StringType(), True),
        StructField("profile_type", StringType(), True),
        StructField("raw_data", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("line_number", IntegerType(), True),
        StructField("processing_time", TimestampType(), True)
    ])

def get_processed_data_schema():
    """
    İşlenmiş elektrik yük verilerinin şemasını döndürür.
    
    Returns:
        StructType: İşlenmiş veri şeması
    """
    return StructType([
        StructField("customer_id", StringType(), False),
        StructField("profile_type", StringType(), True),
        StructField("year", StringType(), True),
        StructField("month_num", IntegerType(), True),
        StructField("month_name", StringType(), True),
        StructField("day", IntegerType(), True),
        StructField("date", DateType(), True),
        StructField("timestamp", StringType(), True),
        StructField("hour", IntegerType(), True),
        StructField("minute", IntegerType(), True),
        StructField("interval_idx", IntegerType(), True),
        StructField("load_percentage", DoubleType(), True)
    ])

def get_monthly_average_schema():
    """
    Aylık ortalama yük verilerinin şemasını döndürür.
    
    Returns:
        StructType: Aylık ortalama şeması
    """
    return StructType([
        StructField("month_num", IntegerType(), False),
        StructField("month_name", StringType(), True),
        StructField("avg_load_percentage", DoubleType(), True)
    ])

def parse_kafka_json(df):
    """
    Kafka'dan gelen JSON mesajlarını ayrıştırır.
    
    Args:
        df (DataFrame): Kafka'dan gelen ham DataFrame
    
    Returns:
        DataFrame: Ayrıştırılmış DataFrame
    """
    from pyspark.sql.functions import from_json, col
    
    try:
        # JSON mesajını şemaya göre ayrıştır
        schema = get_kafka_message_schema()
        parsed_df = df.select(from_json("json_value", schema).alias("data")).select("data.*")
        
        logger.info("Kafka JSON mesajları başarıyla ayrıştırıldı")
        return parsed_df
    
    except Exception as e:
        logger.error(f"JSON ayrıştırma hatası: {str(e)}")
        raise
