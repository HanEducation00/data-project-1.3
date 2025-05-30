#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Aggregator - Raw elektrik verilerini saatlik toplaştırır
Aggregate load forecasting için saatlik toplam yük hesaplar.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, min as spark_min, max as spark_max,
    hour, dayofweek, month, dayofmonth, year,
    date_format, to_timestamp, when, isnan, isnull, concat, lit, struct
)
from pyspark.sql.types import *
import sys
import os

# Config import
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from config import MODEL_CONFIG
from logger import get_logger

class ElectricityAggregator:
    """Raw elektrik verilerini saatlik seviyede toplaştırır"""
    
    def __init__(self, spark_session=None):
        self.logger = get_logger(self.__class__.__name__)
        self.spark = spark_session
        self.target_variable = MODEL_CONFIG["target_variable"]  # "total_load_mw"
        
    def aggregate_hourly_load(self, raw_df):
        """
        Raw verileri saatlik toplam yüke çevir
        
        Args:
            raw_df (pyspark.sql.DataFrame): Raw elektrik verileri
            
        Returns:
            pyspark.sql.DataFrame: Saatlik aggregate veriler
        """
        try:
            self.logger.info("Starting hourly load aggregation...")
            
            # Timestamp'den tarih-saat bilgilerini çıkar
            df_with_time = raw_df.withColumn("year", year("full_timestamp")) \
                                 .withColumn("month", month("full_timestamp")) \
                                 .withColumn("day", dayofmonth("full_timestamp")) \
                                 .withColumn("hour", hour("full_timestamp")) \
                                 .withColumn("dayofweek", dayofweek("full_timestamp")) \
                                 .withColumn("date", date_format("full_timestamp", "yyyy-MM-dd"))
            
            # Saatlik agregasyon
            hourly_agg = df_with_time.groupBy(
                "year", "month", "day", "hour", "dayofweek", "date"
            ).agg(
                # Ana target variable: toplam yük
                spark_sum("load_percentage").alias(self.target_variable),
                
                # Destek metrikleri
                count("*").alias("total_records"),
                count("customer_id").alias("active_customers"),
                avg("load_percentage").alias("avg_load_per_customer"),
                spark_min("load_percentage").alias("min_load"),
                spark_max("load_percentage").alias("max_load")
                
            ).orderBy("year", "month", "day", "hour")
            
            # Datetime column ekle (ML için)
            hourly_agg = hourly_agg.withColumn(
                "datetime",
                to_timestamp(
                    concat(
                        col("date").cast("string"), 
                        lit(" "), 
                        col("hour").cast("string"), 
                        lit(":00:00")
                    ),
                    "yyyy-MM-dd H:mm:ss"
                )
            )
            
            total_hours = hourly_agg.count()
            self.logger.info(f"✅ Hourly aggregation completed: {total_hours:,} hours")
            
            return hourly_agg
            
        except Exception as e:
            self.logger.error(f"Hourly aggregation failed: {e}")
            raise
    
    def add_time_features(self, hourly_df):
        """
        ML için zaman bazlı features ekle
        
        Args:
            hourly_df (pyspark.sql.DataFrame): Saatlik aggregate veriler
            
        Returns:
            pyspark.sql.DataFrame: Time features eklenmiş veriler
        """
        try:
            self.logger.info("Adding time-based features...")
            
            # Cyclical features (trigonometric encoding)
            import math
            
            # Hour cyclical (0-23 → 0-2π)
            df_features = hourly_df.withColumn(
                "hour_sin", 
                col("hour") * (2 * math.pi / 24)
            ).withColumn(
                "hour_cos",
                col("hour") * (2 * math.pi / 24)
            )
            
            # Month cyclical (1-12 → 0-2π)
            df_features = df_features.withColumn(
                "month_sin",
                col("month") * (2 * math.pi / 12)
            ).withColumn(
                "month_cos", 
                col("month") * (2 * math.pi / 12)
            )
            
            # Day of week cyclical (1-7 → 0-2π)
            df_features = df_features.withColumn(
                "dayofweek_sin",
                col("dayofweek") * (2 * math.pi / 7)
            ).withColumn(
                "dayofweek_cos",
                col("dayofweek") * (2 * math.pi / 7)
            )
            
            # Binary features
            df_features = df_features.withColumn(
                "is_weekend",
                when((col("dayofweek") == 1) | (col("dayofweek") == 7), 1).otherwise(0)
            ).withColumn(
                "is_business_hour",
                when((col("hour") >= 8) & (col("hour") <= 18), 1).otherwise(0)
            ).withColumn(
                "is_peak_hour",
                when((col("hour") >= 17) & (col("hour") <= 20), 1).otherwise(0)
            )
            
            self.logger.info("✅ Time features added successfully")
            
            return df_features
            
        except Exception as e:
            self.logger.error(f"Adding time features failed: {e}")
            raise
    
    def validate_aggregation(self, raw_df, aggregated_df):
        """
        Agregasyon sonuçlarını validate et
        
        Args:
            raw_df (pyspark.sql.DataFrame): Orjinal raw veriler
            aggregated_df (pyspark.sql.DataFrame): Aggregate edilmiş veriler
        """
        try:
            self.logger.info("Validating aggregation results...")
            
            # Raw data toplam yük
            raw_total_load = raw_df.agg(spark_sum("load_percentage")).collect()[0][0]
            
            # Aggregated data toplam yük
            agg_total_load = aggregated_df.agg(spark_sum(self.target_variable)).collect()[0][0]
            
            # Fark kontrolü
            difference = abs(raw_total_load - agg_total_load)
            difference_percentage = (difference / raw_total_load) * 100 if raw_total_load > 0 else 0
            
            self.logger.info(f"Raw total load: {raw_total_load:,.2f}")
            self.logger.info(f"Aggregated total load: {agg_total_load:,.2f}")
            self.logger.info(f"Difference: {difference:,.2f} ({difference_percentage:.4f}%)")
            
            # Tolerans kontrolü
            if difference_percentage > 0.01:  # 0.01% tolerans
                self.logger.warning(f"Aggregation difference too high: {difference_percentage:.4f}%")
            else:
                self.logger.info("✅ Aggregation validation passed")
                
            # Kayıt sayısı karşılaştırması
            raw_count = raw_df.count()
            unique_hours = aggregated_df.count()
            
            self.logger.info(f"Raw records: {raw_count:,}")
            self.logger.info(f"Unique hours: {unique_hours:,}")
            self.logger.info(f"Avg records per hour: {raw_count/unique_hours:.1f}")
            
        except Exception as e:
            self.logger.error(f"Aggregation validation failed: {e}")
    
    def get_aggregation_summary(self, aggregated_df):
        """
        Agregasyon özeti bilgileri
        
        Args:
            aggregated_df (pyspark.sql.DataFrame): Aggregate veriler
            
        Returns:
            dict: Özet bilgiler
        """
        try:
            # Target variable istatistikleri
            load_stats = aggregated_df.select(
                spark_min(self.target_variable).alias("min_load"),
                spark_max(self.target_variable).alias("max_load"),
                avg(self.target_variable).alias("avg_load")
            ).collect()[0]
            
            # Tarih aralığı
            date_stats = aggregated_df.select(
                spark_min("datetime").alias("start_datetime"),
                spark_max("datetime").alias("end_datetime"),
                count("*").alias("total_hours")
            ).collect()[0]
            
            # Müşteri istatistikleri
            customer_stats = aggregated_df.select(
                avg("active_customers").alias("avg_customers_per_hour"),
                spark_min("active_customers").alias("min_customers"),
                spark_max("active_customers").alias("max_customers")
            ).collect()[0]
            
            summary = {
                "total_hours": date_stats["total_hours"],
                "date_range": {
                    "start": date_stats["start_datetime"],
                    "end": date_stats["end_datetime"]
                },
                "load_statistics": {
                    "min": round(load_stats["min_load"], 2),
                    "max": round(load_stats["max_load"], 2), 
                    "avg": round(load_stats["avg_load"], 2)
                },
                "customer_statistics": {
                    "avg_per_hour": round(customer_stats["avg_customers_per_hour"], 1),
                    "min_per_hour": customer_stats["min_customers"],
                    "max_per_hour": customer_stats["max_customers"]
                }
            }
            
            self.logger.info(f"Aggregation Summary: {summary}")
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Failed to generate summary: {e}")
            return {}

# Convenience function
def aggregate_electricity_data(raw_df, spark_session=None):
    """
    Elektrik verilerini aggregate etmek için kolaylık fonksiyonu
    
    Args:
        raw_df (pyspark.sql.DataFrame): Raw elektrik verileri
        spark_session: Spark session
        
    Returns:
        pyspark.sql.DataFrame: Aggregate edilmiş veriler
    """
    aggregator = ElectricityAggregator(spark_session)
    
    # Saatlik agregasyon
    hourly_df = aggregator.aggregate_hourly_load(raw_df)
    
    # Time features ekle
    featured_df = aggregator.add_time_features(hourly_df)
    
    # Validation
    aggregator.validate_aggregation(raw_df, featured_df)
    
    # Özet bilgiler
    summary = aggregator.get_aggregation_summary(featured_df)
    
    return featured_df

if __name__ == "__main__":
    """Test amaçlı"""
    
    from data_loader import load_electricity_data
    
    print("🔄 Testing ElectricityAggregator...")
    
    # Test verisi yükle
    raw_df, loader = load_electricity_data(limit=5000)
    
    # Aggregate et
    aggregated_df = aggregate_electricity_data(raw_df, loader.spark)
    
    # Sonuçları göster
    print("📊 Aggregated Data Preview:")
    aggregated_df.show(10, truncate=False)
    
    print("📈 Target Variable Statistics:")
    aggregated_df.select("total_load_mw").describe().show()
    
    loader.close()
    print("✅ Aggregator test completed!")
