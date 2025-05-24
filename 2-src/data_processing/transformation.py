#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Veri Dönüştürme İşlemleri

Bu modül, ham verileri işlenmiş formata dönüştüren fonksiyonları içerir.
"""

from pyspark.sql.functions import col, split, element_at, expr, when, lit, concat, lpad, posexplode, slice
from pyspark.sql.types import IntegerType, DoubleType
from utils.logger import get_logger

# Logger oluştur
logger = get_logger(__name__)

def process_raw_data(df):
    """
    Ham elektrik yük verilerini işler ve yapılandırılmış bir DataFrame'e dönüştürür.
    
    İşlemler:
    1. Ham veriyi parçalara ayırır
    2. Meta bilgileri (müşteri ID, profil tipi, tarih, vb.) çıkarır
    3. Ay adını ay numarasına dönüştürür
    4. 15 dakikalık aralıklar için ayrı satırlar oluşturur
    5. Yük değerlerini sayısal formata dönüştürür
    
    Args:
        df (DataFrame): İşlenecek ham veri DataFrame'i
    
    Returns:
        DataFrame: İşlenmiş ve yapılandırılmış DataFrame
    """
    try:
        # raw_data'yı parçalara ayır
        processed_df = df.withColumn("parts", split(col("raw_data"), ","))
        
        # Meta bilgileri çıkar
        processed_df = processed_df \
            .withColumn("customer_id", element_at(col("parts"), 1)) \
            .withColumn("profile_type", element_at(col("parts"), 2)) \
            .withColumn("year", element_at(col("parts"), 7)) \
            .withColumn("month_name", element_at(col("parts"), 8)) \
            .withColumn("day", element_at(col("parts"), 9).cast(IntegerType()))
        
        # Ay adını ay numarasına dönüştür
        processed_df = processed_df.withColumn("month_num", lit(0))  # Başlangıç değeri
        
        # Her ay için ayrı bir when ifadesi
        processed_df = processed_df \
            .withColumn("month_num",
                      when(col("month_name").rlike("(?i)^JANUARY|OCAK"), 1)
                      .when(col("month_name").rlike("(?i)^FEBRUARY|ŞUBAT"), 2)
                      .when(col("month_name").rlike("(?i)^MARCH|MART"), 3)
                      .when(col("month_name").rlike("(?i)^APRIL|NİSAN"), 4)
                      .when(col("month_name").rlike("(?i)^MAY|MAYIS"), 5)
                      .when(col("month_name").rlike("(?i)^JUNE|HAZİRAN"), 6)
                      .when(col("month_name").rlike("(?i)^JULY|TEMMUZ"), 7)
                      .when(col("month_name").rlike("(?i)^AUGUST|AĞUSTOS"), 8)
                      .when(col("month_name").rlike("(?i)^SEPTEMBER|EYLÜL"), 9)
                      .when(col("month_name").rlike("(?i)^OCTOBER|EKİM"), 10)
                      .when(col("month_name").rlike("(?i)^NOVEMBER|KASIM"), 11)
                      .when(col("month_name").rlike("(?i)^DECEMBER|ARALIK"), 12)
                      .otherwise(0))
        
        # Tarih oluştur (2016-01-01 başlangıç)
        processed_df = processed_df.withColumn(
            "date",
            expr(f"date_add('2016-01-01', (month_num - 1) * 30 + day - 1)")
        )
        
        # 15 dakikalık değerleri çıkar (11. indeksten başlayarak)
        # explode kullanarak her 15 dakikalık değer için ayrı bir satır oluştur
        load_values_df = processed_df.select(
            "customer_id", "profile_type", "year", "month_num", "month_name", "day", "date",
            posexplode(slice(col("parts"), 12, 96)).alias("interval_idx", "load_value")
        )
        
        # interval_idx'i 0'dan başlat
        load_values_df = load_values_df.withColumn("interval_idx", col("interval_idx"))
        
        # Saat ve dakika bilgilerini hesapla
        load_values_df = load_values_df \
            .withColumn("hour", (col("interval_idx") * 15) / 60) \
            .withColumn("minute", (col("interval_idx") * 15) % 60) \
            .withColumn("timestamp", concat(
                lpad(col("hour").cast("integer"), 2, "0"),
                lit(":"),
                lpad(col("minute").cast("integer"), 2, "0")
            ))
        
        # Yük değerini sayısal formata dönüştür
        load_values_df = load_values_df \
            .withColumn("load_percentage",
                       when(col("load_value").rlike("^[0-9]*\\.?[0-9]*$"),
                            col("load_value").cast("double")).otherwise(lit(None)))
        
        # Sonuç dataframe'i döndür
        result_df = load_values_df.select(
            "customer_id", "profile_type", "year", "month_num", "month_name", "day", "date",
            "timestamp", "hour", "minute", "interval_idx", "load_percentage"
        )
        
        logger.info(f"Ham veri işlendi: {df.count()} satırdan {result_df.count()} detay satırı oluşturuldu")
        return result_df
    
    except Exception as e:
        logger.error(f"Veri işleme hatası: {str(e)}")
        raise
