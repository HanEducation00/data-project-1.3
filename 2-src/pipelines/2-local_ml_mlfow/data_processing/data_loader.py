#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Loader - PostgreSQL'den elektrik yük verilerini Spark ile çeker
Aggregate load forecasting için elektrik tüketim verilerini yükler ve hazırlar.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, min as spark_min, max as spark_max
from pyspark.sql.functions import to_timestamp, date_format, hour, dayofweek, month
from pyspark.sql.types import *
import sys
import os
from datetime import datetime, date

# Config import
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from config import JDBC_URL, JDBC_PROPERTIES, TABLE_NAME, SPARK_CONFIG
from logger import get_logger

class ElectricityDataLoader:
    """Elektrik yük verilerini PostgreSQL'den Spark ile yükler ve hazırlar"""
    
    def __init__(self, spark_session=None):
        """
        Args:
            spark_session: Mevcut Spark session (opsiyonel)
        """
        self.logger = get_logger(self.__class__.__name__)
        self.spark = spark_session or self._create_spark_session()
        self.logger.info("ElectricityDataLoader initialized")
        
    def _create_spark_session(self):
        """Spark Session oluştur"""
        try:
            spark = SparkSession.builder \
                .appName(SPARK_CONFIG["app_name"]) \
                .config("spark.jars.packages", SPARK_CONFIG["packages"]) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
            
            self.logger.info("Spark Session created successfully")
            return spark
            
        except Exception as e:
            self.logger.error(f"Failed to create Spark Session: {e}")
            raise
    
    def load_raw_data(self, start_date=None, end_date=None, limit=None):
        """
        PostgreSQL'den raw elektrik yük verilerini çek
        
        Args:
            start_date (str): Başlangıç tarihi (YYYY-MM-DD)
            end_date (str): Bitiş tarihi (YYYY-MM-DD)
            limit (int): Maksimum satır sayısı
            
        Returns:
            pyspark.sql.DataFrame: Raw elektrik yük verileri
        """
        try:
            self.logger.info("Loading raw electricity data from PostgreSQL...")
            
            # Query oluştur
            query = f"(SELECT * FROM {TABLE_NAME}"
            
            # Date filtering ekle
            conditions = []
            if start_date:
                conditions.append(f"DATE(full_timestamp) >= '{start_date}'")
            if end_date:
                conditions.append(f"DATE(full_timestamp) <= '{end_date}'")
                
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            # Sıralama ekle (time series için önemli)
            query += " ORDER BY full_timestamp, customer_id"
            
            # Limit ekle
            if limit:
                query += f" LIMIT {limit}"
                
            query += ") AS raw_data"
            
            self.logger.info(f"Executing query: {query}")
            
            # Spark DataFrame olarak çek
            df = self.spark.read.jdbc(
                url=JDBC_URL,
                table=query,
                properties=JDBC_PROPERTIES
            )
            
            # Temel bilgileri logla
            total_count = df.count()
            self.logger.info(f"Successfully loaded {total_count:,} records")
            
            if total_count > 0:
                # Tarih aralığını kontrol et
                date_stats = df.select(
                    spark_min("full_timestamp").alias("min_date"),
                    spark_max("full_timestamp").alias("max_date")
                ).collect()[0]
                
                self.logger.info(f"Date range: {date_stats['min_date']} to {date_stats['max_date']}")
                
                # Müşteri sayısını kontrol et
                customer_count = df.select("customer_id").distinct().count()
                self.logger.info(f"Unique customers: {customer_count:,}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to load raw data: {e}")
            raise
    
    def validate_data_quality(self, df):
        """
        Veri kalitesi kontrolleri yapar
        
        Args:
            df (pyspark.sql.DataFrame): Kontrol edilecek DataFrame
            
        Returns:
            dict: Veri kalitesi raporu
        """
        try:
            self.logger.info("Performing data quality validation...")
            
            # Temel istatistikler
            total_count = df.count()
            
            if total_count == 0:
                self.logger.warning("DataFrame is empty!")
                return {"status": "empty", "total_count": 0}
            
            # Null değer kontrolleri
            null_counts = {}
            for column in df.columns:
                null_count = df.filter(col(column).isNull()).count()
                null_counts[column] = null_count
                
                if null_count > 0:
                    null_percentage = (null_count / total_count) * 100
                    self.logger.warning(f"Column '{column}': {null_count:,} null values ({null_percentage:.2f}%)")
            
            # load_percentage değer aralığı kontrolü
            load_stats = df.select(
                spark_min("load_percentage").alias("min_load"),
                spark_max("load_percentage").alias("max_load"),
                avg("load_percentage").alias("avg_load")
            ).collect()[0]
            
            # Anomali kontrolü
            anomalies = []
            if load_stats["min_load"] < 0:
                anomalies.append(f"Negative load values found: min={load_stats['min_load']}")
            if load_stats["max_load"] > 100:
                anomalies.append(f"Load percentage > 100 found: max={load_stats['max_load']}")
            
            # Müşteri bazlı kontroller
            customer_stats = df.groupBy("customer_id").agg(
                count("*").alias("record_count"),
                avg("load_percentage").alias("avg_customer_load")
            )
            
            customer_count = customer_stats.count()
            avg_records_per_customer = customer_stats.agg(avg("record_count")).collect()[0][0]
            
            # Rapor oluştur
            quality_report = {
                "status": "valid" if not anomalies else "issues_found",
                "total_count": total_count,
                "customer_count": customer_count,
                "avg_records_per_customer": round(avg_records_per_customer, 2),
                "null_counts": null_counts,
                "load_stats": {
                    "min": round(load_stats["min_load"], 4),
                    "max": round(load_stats["max_load"], 4),
                    "avg": round(load_stats["avg_load"], 4)
                },
                "anomalies": anomalies
            }
            
            self.logger.info(f"Data quality validation completed: {quality_report['status']}")
            
            return quality_report
            
        except Exception as e:
            self.logger.error(f"Data quality validation failed: {e}")
            raise
    
    def preview_data(self, df, sample_size=10):
        """
        Verinin önizlemesini göster
        
        Args:
            df (pyspark.sql.DataFrame): Önizlenecek DataFrame
            sample_size (int): Gösterilecek satır sayısı
        """
        try:
            self.logger.info(f"Data Preview (first {sample_size} rows):")
            df.show(sample_size, truncate=False)
            
            self.logger.info("Schema:")
            df.printSchema()
            
            self.logger.info("Column statistics:")
            df.describe().show()
            
        except Exception as e:
            self.logger.error(f"Failed to preview data: {e}")
    
    def get_date_range_info(self, df):
        """
        Verideki tarih aralığı bilgilerini döndür
        
        Args:
            df (pyspark.sql.DataFrame): Analiz edilecek DataFrame
            
        Returns:
            dict: Tarih aralığı bilgileri
        """
        try:
            # Timestamp'i date'e çevir
            df_with_date = df.withColumn("date", date_format("full_timestamp", "yyyy-MM-dd"))
            
            # Günlük kayıt sayıları
            daily_counts = df_with_date.groupBy("date").agg(
                count("*").alias("daily_records"),
                col("date").alias("date_key")
            ).orderBy("date")
            
            # Tarih istatistikleri
            date_stats = daily_counts.select(
                spark_min("date").alias("start_date"),
                spark_max("date").alias("end_date"),
                count("date").alias("total_days"),
                avg("daily_records").alias("avg_daily_records")
            ).collect()[0]
            
            return {
                "start_date": date_stats["start_date"],
                "end_date": date_stats["end_date"],
                "total_days": date_stats["total_days"],
                "avg_daily_records": round(date_stats["avg_daily_records"], 2)
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get date range info: {e}")
            return {}
    
    def close(self):
        """Spark session'ı kapat"""
        if self.spark:
            self.spark.stop()
            self.logger.info("Spark Session closed")

# Convenience function
def load_electricity_data(start_date=None, end_date=None, limit=None, spark_session=None):
    """
    Elektrik verilerini yüklemek için kolaylık fonksiyonu
    
    Args:
        start_date (str): Başlangıç tarihi (YYYY-MM-DD)
        end_date (str): Bitiş tarihi (YYYY-MM-DD)  
        limit (int): Maksimum satır sayısı
        spark_session: Mevcut Spark session
        
    Returns:
        tuple: (DataFrame, DataLoader instance)
    """
    loader = ElectricityDataLoader(spark_session)
    df = loader.load_raw_data(start_date, end_date, limit)
    return df, loader

if __name__ == "__main__":
    """Test amaçlı doğrudan çalıştırma"""
    
    # Test verisi yükle
    print("🔗 Testing ElectricityDataLoader...")
    
    loader = ElectricityDataLoader()
    
    # Küçük bir örnek yükle
    df = loader.load_raw_data(limit=1000)
    
    # Veri kalitesi kontrolü
    quality_report = loader.validate_data_quality(df)
    print(f"📊 Quality Report: {quality_report}")
    
    # Önizleme
    loader.preview_data(df, 5)
    
    # Tarih bilgileri
    date_info = loader.get_date_range_info(df)
    print(f"📅 Date Info: {date_info}")
    
    loader.close()
    print("✅ Test completed!")
