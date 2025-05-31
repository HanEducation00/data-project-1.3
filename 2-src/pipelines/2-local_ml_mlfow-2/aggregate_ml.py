#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PART 1: AGGREGATE LOAD FORECASTING - DATA PROCESSING & FEATURE ENGINEERING
Müşteri bazında değil, TOPLAM şebeke yükü için veri hazırlama
"""

import sys
import os
import math
import time
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'data_processing'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg as spark_avg, max as spark_max, min as spark_min,
    count, countDistinct, stddev, percentile_approx,
    date_format, dayofweek, month, quarter, hour, dayofyear,
    when, lit, sin, cos, to_timestamp, concat,
    lag, row_number, desc
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.window import Window

class AggregateLoadProcessor:
    """
    Aggregate load forecasting için veri işleme sınıfı
    """
    
    def __init__(self, spark_session=None):
        from data_loader import ElectricityDataLoader
        
        self.loader = ElectricityDataLoader(spark_session)
        self.spark = self.loader.spark
        print("✅ AggregateLoadProcessor initialized")
    
    def load_full_data(self, months_range=(1, 12), year=2016):
        """
        Tam yıl verisi yükle (aggregate için)
        """
        print(f"📤 Loading FULL YEAR data for aggregate forecasting...")
        print(f"📅 Year: {year}, Months: {months_range[0]}-{months_range[1]}")
        
        start_date = f"{year}-{months_range[0]:02d}-01"
        end_date = f"{year}-{months_range[1]:02d}-31"
        
        print(f"📊 Date range: {start_date} to {end_date}")
        
        # Tüm veriyi yükle (limit yok!)
        raw_df = self.loader.load_raw_data(
            start_date=start_date,
            end_date=end_date,
            limit=None  # FULL DATA!
        )
        
        total_records = raw_df.count()
        print(f"📊 Total raw records loaded: {total_records:,}")
        
        return raw_df
    
    def create_hourly_aggregates(self, raw_df):
        """
        SAATLİK TOPLAM YÜK (tüm müşteriler birleşik)
        """
        print("📊 Creating HOURLY aggregate load (ALL customers combined)...")
        start_time = time.time()
        
        # ✅ DOĞRU YAKLAŞIM: customer_id'ye göre GRUPLAMA YOK!
        hourly_agg = raw_df.groupBy(
            date_format("full_timestamp", "yyyy-MM-dd").alias("date"),
            hour("full_timestamp").alias("hour")
            # ❌ customer_id yok - tüm müşteriler birleşik!
        ).agg(
            # 🎯 ANA METRİK: TOPLAM SAATLİK YÜK
            spark_sum("load_percentage").alias("total_hourly_load_mw"),
            
            # Destek metrikleri
            spark_avg("load_percentage").alias("avg_customer_load"),
            spark_max("load_percentage").alias("peak_customer_load"), 
            spark_min("load_percentage").alias("min_customer_load"),
            
            # Müşteri istatistikleri
            count("*").alias("total_readings"),
            countDistinct("customer_id").alias("active_customers"),
            
            # Yük dağılım istatistikleri
            stddev("load_percentage").alias("load_std"),
            percentile_approx("load_percentage", 0.25).alias("load_p25"),
            percentile_approx("load_percentage", 0.75).alias("load_p75"),
            percentile_approx("load_percentage", 0.90).alias("load_p90")
        )
        
        # Datetime oluştur
        hourly_agg = hourly_agg.withColumn(
            "datetime",
            to_timestamp(concat(col("date"), lit(" "), col("hour").cast("string"), lit(":00:00")))
        )
        
        # Sırala (time series için önemli)
        hourly_agg = hourly_agg.orderBy("datetime")
        
        hourly_count = hourly_agg.count()
        processing_time = time.time() - start_time
        
        print(f"✅ Hourly aggregates created: {hourly_count:,} hours in {processing_time:.1f}s")
        
        return hourly_agg
    
    def create_daily_aggregates(self, raw_df):
        """
        GÜNLİK TOPLAM YÜK (tüm müşteriler birleşik)
        """
        print("📊 Creating DAILY aggregate load (ALL customers combined)...")
        start_time = time.time()
        
        # ✅ DOĞRU YAKLAŞIM: Sadece tarihe göre grupla
        daily_agg = raw_df.groupBy(
            date_format("full_timestamp", "yyyy-MM-dd").alias("date")
            # ❌ customer_id yok - tüm müşteriler birleşik!
        ).agg(
            # 🎯 ANA METRİK: TOPLAM GÜNLİK YÜK
            spark_sum("load_percentage").alias("total_daily_load_mw"),
            
            # Günlük istatistikler
            spark_avg("load_percentage").alias("avg_daily_customer_load"),
            spark_max("load_percentage").alias("daily_peak_load"),
            spark_min("load_percentage").alias("daily_min_load"),
            
            # Müşteri metrikleri
            count("*").alias("total_daily_readings"),
            countDistinct("customer_id").alias("daily_active_customers"),
            
            # Dağılım metrikleri
            stddev("load_percentage").alias("daily_load_std"),
            percentile_approx("load_percentage", 0.95).alias("daily_load_p95"),
            percentile_approx("load_percentage", 0.05).alias("daily_load_p05")
        )
        
        # Date tipini düzelt
        daily_agg = daily_agg.withColumn("date", col("date").cast("timestamp"))
        
        # Sırala
        daily_agg = daily_agg.orderBy("date")
        
        daily_count = daily_agg.count()
        processing_time = time.time() - start_time
        
        print(f"✅ Daily aggregates created: {daily_count:,} days in {processing_time:.1f}s")
        
        return daily_agg
    
    def add_temporal_features(self, df, level="daily"):
        """
        Zaman bazlı özellikler ekle (aggregate forecasting için)
        """
        print(f"🕐 Adding temporal features for {level} aggregate forecasting...")
        
        if level == "hourly":
            date_col = "datetime"
        else:
            date_col = "date"
        
        # Temel zaman özellikleri
        df = df.withColumn("year", year(date_col)) \
              .withColumn("month", month(date_col)) \
              .withColumn("dayofweek", dayofweek(date_col)) \
              .withColumn("quarter", quarter(date_col)) \
              .withColumn("day_of_year", dayofyear(date_col))
        
        if level == "hourly":
            df = df.withColumn("hour", hour(date_col))
        
        # Mevsimsel özellikler
        df = df.withColumn("season", 
            when(col("month").isin([12, 1, 2]), 0)      # Kış
            .when(col("month").isin([3, 4, 5]), 1)      # İlkbahar  
            .when(col("month").isin([6, 7, 8]), 2)      # Yaz
            .otherwise(3)                               # Sonbahar
        )
        
        # İkili özellikler
        df = df.withColumn("is_weekend", 
                          when(col("dayofweek").isin([1, 7]), 1).otherwise(0)) \
              .withColumn("is_summer", 
                          when(col("month").isin([6, 7, 8]), 1).otherwise(0)) \
              .withColumn("is_winter", 
                          when(col("month").isin([12, 1, 2]), 1).otherwise(0)) \
              .withColumn("is_spring", 
                          when(col("month").isin([3, 4, 5]), 1).otherwise(0)) \
              .withColumn("is_fall", 
                          when(col("month").isin([9, 10, 11]), 1).otherwise(0))
        
        # Cyclical encoding (trigonometric)
        PI = lit(math.pi)
        
        # Ay cyclical
        df = df.withColumn("month_sin", sin(col("month") * (2.0 * PI / 12))) \
              .withColumn("month_cos", cos(col("month") * (2.0 * PI / 12)))
        
        # Haftanın günü cyclical
        df = df.withColumn("dayofweek_sin", sin(col("dayofweek") * (2.0 * PI / 7))) \
              .withColumn("dayofweek_cos", cos(col("dayofweek") * (2.0 * PI / 7)))
        
        # Yılın günü cyclical
        df = df.withColumn("dayofyear_sin", sin(col("day_of_year") * (2.0 * PI / 365))) \
              .withColumn("dayofyear_cos", cos(col("day_of_year") * (2.0 * PI / 365)))
        
        if level == "hourly":
            # Saat cyclical
            df = df.withColumn("hour_sin", sin(col("hour") * (2.0 * PI / 24))) \
                  .withColumn("hour_cos", cos(col("hour") * (2.0 * PI / 24)))
            
            # İş saatleri özellikleri
            df = df.withColumn("is_business_hour", 
                              when((col("hour") >= 8) & (col("hour") <= 18), 1).otherwise(0)) \
                  .withColumn("is_peak_hour", 
                              when((col("hour") >= 17) & (col("hour") <= 20), 1).otherwise(0)) \
                  .withColumn("is_night", 
                              when((col("hour") >= 22) | (col("hour") <= 6), 1).otherwise(0)) \
                  .withColumn("is_morning_peak", 
                              when((col("hour") >= 7) & (col("hour") <= 9), 1).otherwise(0)) \
                  .withColumn("is_evening_peak", 
                              when((col("hour") >= 17) & (col("hour") <= 19), 1).otherwise(0))
        
        print(f"✅ Temporal features added for {level} level")
        
        return df
    
    def add_lag_features(self, df, level="daily"):
        """
        Geçmiş yük değerlerini lag features olarak ekle
        """
        print(f"🔄 Adding lag features for {level} aggregate forecasting...")
        
        if level == "daily":
            target_col = "total_daily_load_mw"
            order_col = "date"
            # Günlük için lag periods (günler)
            lag_periods = [1, 2, 3, 7, 14, 30]  # 1 gün, 2 gün, 1 hafta, 2 hafta, 1 ay önce
        else:
            target_col = "total_hourly_load_mw"
            order_col = "datetime"
            # Saatlik için lag periods (saatler)
            lag_periods = [1, 2, 6, 12, 24, 48, 168]  # 1h, 2h, 6h, 12h, 1d, 2d, 1w önce
        
        # Window specification (partition yok - aggregate data)
        window_spec = Window.orderBy(order_col)
        
        # Lag features ekle
        for lag_period in lag_periods:
            df = df.withColumn(f"load_lag_{lag_period}", 
                              lag(target_col, lag_period).over(window_spec))
        
        # Rolling features (hareketli ortalamalar)
        if level == "daily":
            rolling_windows = [3, 7, 14, 30]  # 3, 7, 14, 30 günlük ortalamalar
        else:
            rolling_windows = [6, 12, 24, 168]  # 6, 12, 24, 168 saatlik ortalamalar
        
        for window_size in rolling_windows:
            window_rolling = Window.orderBy(order_col).rowsBetween(-window_size + 1, 0)
            
            df = df.withColumn(f"rolling_avg_{window_size}", 
                              spark_avg(target_col).over(window_rolling)) \
                  .withColumn(f"rolling_max_{window_size}", 
                              spark_max(target_col).over(window_rolling)) \
                  .withColumn(f"rolling_min_{window_size}", 
                              spark_min(target_col).over(window_rolling)) \
                  .withColumn(f"rolling_std_{window_size}", 
                              stddev(target_col).over(window_rolling))
        
        # Null değerleri doldur (ilk değerler için)
        lag_columns = [f"load_lag_{p}" for p in lag_periods]
        rolling_columns = []
        for w in rolling_windows:
            rolling_columns.extend([f"rolling_avg_{w}", f"rolling_max_{w}", 
                                   f"rolling_min_{w}", f"rolling_std_{w}"])
        
        fill_columns = lag_columns + rolling_columns
        
        # Null değerleri 0 ile doldur
        for col_name in fill_columns:
            df = df.fillna({col_name: 0})
        
        print(f"✅ Lag features added: {len(lag_periods)} lags, {len(rolling_windows)} rolling windows")
        
        return df
    
    def create_trend_features(self, df, level="daily"):
        """
        Trend ve büyüme özellikleri ekle
        """
        print(f"📈 Adding trend features for {level} forecasting...")
        
        if level == "daily":
            target_col = "total_daily_load_mw"
            order_col = "date"
        else:
            target_col = "total_hourly_load_mw"
            order_col = "datetime"
        
        window_spec = Window.orderBy(order_col)
        
        # Trend özellikleri
        df = df.withColumn("load_diff_1", 
                          col(target_col) - lag(target_col, 1).over(window_spec)) \
              .withColumn("load_diff_7", 
                          col(target_col) - lag(target_col, 7).over(window_spec)) \
              .withColumn("load_pct_change_1", 
                          (col(target_col) - lag(target_col, 1).over(window_spec)) / 
                          lag(target_col, 1).over(window_spec) * 100)
        
        # Row number ekle (trend için)
        df = df.withColumn("time_index", row_number().over(window_spec))
        
        # Null değerleri doldur
        df = df.fillna({
            "load_diff_1": 0,
            "load_diff_7": 0, 
            "load_pct_change_1": 0
        })
        
        print("✅ Trend features added")
        
        return df
    
    def get_feature_summary(self, df, level="daily"):
        """
        Oluşturulan özelliklerin özetini göster
        """
        print(f"\n📊 FEATURE SUMMARY - {level.upper()} AGGREGATE FORECASTING:")
        print("="*60)
        
        feature_groups = {
            "Target": [],
            "Temporal": [],
            "Seasonal": [],
            "Cyclical": [], 
            "Lag": [],
            "Rolling": [],
            "Trend": [],
            "Statistical": []
        }
        
        for col_name in df.columns:
            if "total_" in col_name and ("load" in col_name or "mw" in col_name):
                feature_groups["Target"].append(col_name)
            elif any(x in col_name for x in ["month", "day", "year", "quarter", "season"]):
                if "sin" in col_name or "cos" in col_name:
                    feature_groups["Cyclical"].append(col_name)
                else:
                    feature_groups["Temporal"].append(col_name)
            elif any(x in col_name for x in ["weekend", "summer", "winter", "business", "peak"]):
                feature_groups["Seasonal"].append(col_name)
            elif "lag_" in col_name:
                feature_groups["Lag"].append(col_name)
            elif "rolling_" in col_name:
                feature_groups["Rolling"].append(col_name)
            elif any(x in col_name for x in ["diff", "pct_change", "time_index"]):
                feature_groups["Trend"].append(col_name)
            elif any(x in col_name for x in ["std", "p25", "p75", "p90", "avg_", "min_", "max_"]):
                feature_groups["Statistical"].append(col_name)
        
        total_features = 0
        for group, features in feature_groups.items():
            if features:
                print(f"{group:12}: {len(features):2d} features")
                total_features += len(features)
                if len(features) <= 10:  # Kısa listeleri göster
                    print(f"             {features}")
        
        print(f"{'TOTAL':12}: {total_features:2d} features")
        print(f"Records: {df.count():,}")
        
        return feature_groups
    
    def close(self):
        """Kaynakları temizle"""
        self.loader.close()

if __name__ == "__main__":
    print("🚀 PART 1: AGGREGATE LOAD DATA PROCESSING")
    print("📊 Testing aggregate data processing...")
    
    # Test
    processor = AggregateLoadProcessor()
    
    try:
        # Küçük test verisi
        raw_df = processor.load_full_data(months_range=(1, 2))  # 2 ay test
        
        # Daily aggregates
        daily_df = processor.create_daily_aggregates(raw_df)
        daily_df = processor.add_temporal_features(daily_df, "daily")
        daily_df = processor.add_lag_features(daily_df, "daily")
        daily_df = processor.create_trend_features(daily_df, "daily")
        
        # Feature summary
        processor.get_feature_summary(daily_df, "daily")
        
        # Sample göster
        print("\n📊 SAMPLE AGGREGATE DAILY DATA:")
        daily_df.select("date", "total_daily_load_mw", "month", "is_weekend", 
                       "load_lag_1", "rolling_avg_7").show(10)
        
        print("✅ Part 1 test completed successfully!")
        
    finally:
        processor.close()