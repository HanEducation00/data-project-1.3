#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Veri Şema Tanımlamaları

Bu modül, uygulama içinde kullanılan tüm veri şemalarını ve yapılarını tanımlar.
Kafka mesajları, ham veri yapısı ve işlenmiş veri yapıları için şemaları içerir.
"""

from pyspark.sql.types import *
from utils.logger import get_logger

# Logger oluştur
logger = get_logger(__name__)

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
