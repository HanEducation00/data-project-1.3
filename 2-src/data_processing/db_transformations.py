#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Veritabanı Şema Dönüşümleri

Bu modül, DataFrame'leri PostgreSQL tablolarına yazmak için
uygun formata dönüştüren fonksiyonları içerir.
"""

from pyspark.sql.functions import lit, current_timestamp, to_json, struct, col
from pyspark.sql.functions import to_timestamp, concat, min, max, stddev, count
from utils.logger import get_logger

# Logger oluştur
logger = get_logger(__name__)

def prepare_raw_data_df(df):
    """
    Ham DataFrame'i raw_data tablosuna uygun formata dönüştürür.
    """
    try:
        logger.info("DataFrame raw_data tablosuna uygun hale getiriliyor")
        
        # Import gerekli fonksiyonlar
        from pyspark.sql.functions import to_json, struct, current_timestamp, lit, monotonically_increasing_id
        
        # DÜZELTME: struct içine kolon isimlerini açıkça belirtelim ve tüm sütunları ekleyelim
        columns = df.columns
        
        # JSON string oluştur - boş {} yerine gerçek içeriği dönüştür
        result_df = df.select(
            to_json(struct(*[df[c] for c in columns])).alias("raw_json")
        )
        
        # Benzersiz tanımlayıcılar üret
        import time
        current_time = int(time.time())
        
        # Veritabanı tablosu için gerekli diğer sütunları ekle
        result_df = result_df.withColumn("kafka_topic", lit("sensor-data"))
        result_df = result_df.withColumn("kafka_partition", lit(0))
        result_df = result_df.withColumn(
            "kafka_offset", 
            monotonically_increasing_id() + lit(current_time)
        )
        
        result_df = result_df.withColumn("ingestion_timestamp", current_timestamp())
        result_df = result_df.withColumn("processing_timestamp", current_timestamp())
        result_df = result_df.withColumn("processing_status", lit("PROCESSED"))
        result_df = result_df.withColumn("error_message", lit(None).cast("string"))
        
        # Sonuçları göster
        logger.info("Oluşturulan raw_data içeriği:")
        result_df.select("raw_json").show(5, False)
        
        logger.info("DataFrame raw_data tablosuna uygun hale getirildi")
        return result_df
    
    except Exception as e:
        logger.error(f"Raw data dönüşüm hatası: {str(e)}")
        raise



def prepare_load_data_detail_df(df, raw_data_id=None):
    """
    İşlenmiş DataFrame'i load_data_detail tablosuna uygun formata dönüştürür.
    
    Args:
        df (DataFrame): İşlenmiş veri DataFrame'i
        raw_data_id (int, optional): raw_data tablosundaki referans ID
    
    Returns:
        DataFrame: load_data_detail tablosu şemasına uygun DataFrame
    """
    try:
        logger.info("DataFrame load_data_detail tablosuna uygun hale getiriliyor")
        
        # Tarih ve saat birleştirerek tam timestamp oluştur
        if "date" in df.columns and "timestamp" in df.columns:
            result_df = df.withColumn(
                "interval_timestamp",
                to_timestamp(concat(col("date").cast("string"), lit(" "), col("timestamp")))
            )
        else:
            # Eğer gereken sütunlar yoksa, işlem yapma
            result_df = df
            logger.warning("date veya timestamp sütunları bulunamadı, interval_timestamp oluşturulamadı")
        
        # Batch ID oluştur (eğer yoksa)
        if "batch_id" not in result_df.columns:
            batch_id = current_timestamp().cast("string")
            result_df = result_df.withColumn("batch_id", lit(batch_id))
        
        # raw_data_id ekle
        if raw_data_id is not None:
            result_df = result_df.withColumn("raw_data_id", lit(raw_data_id))
        elif "raw_data_id" not in result_df.columns:
            result_df = result_df.withColumn("raw_data_id", lit(None).cast("integer"))
        
        # İşleme zamanı ekle
        if "processing_timestamp" not in result_df.columns:
            result_df = result_df.withColumn("processing_timestamp", current_timestamp())
        
        # Geçerlilik ekle
        if "is_valid" not in result_df.columns:
            result_df = result_df.withColumn("is_valid", lit(True))
        
        # Sütun isimlerini veritabanı şemasına uygun hale getir
        if "month_num" in result_df.columns and "month" not in result_df.columns:
            result_df = result_df.withColumnRenamed("month_num", "month")
        
        # ÖNEMLİ: Sadece veritabanı tablosunda bulunan sütunları seç
        # Bu kısım önemli - tablo şemasıyla eşleşmesi için
        result_df = result_df.select(
            "customer_id", 
            "interval_timestamp", 
            "load_percentage", 
            "year", 
            "month", 
            "day", 
            "hour", 
            "minute", 
            "batch_id", 
            "raw_data_id", 
            "processing_timestamp", 
            "is_valid"
        )
        
        logger.info("DataFrame load_data_detail tablosuna uygun hale getirildi")
        return result_df
    
    except Exception as e:
        logger.error(f"Load data detail dönüşüm hatası: {str(e)}")
        raise

def prepare_monthly_average_df(df):
    """
    Aylık ortalama DataFrame'ini monthly_average_consumption tablosuna uygun formata dönüştürür.
    
    Args:
        df (DataFrame): Aylık ortalama içeren DataFrame
    
    Returns:
        DataFrame: monthly_average_consumption tablosu şemasına uygun DataFrame
    """
    try:
        logger.info("DataFrame monthly_average_consumption tablosuna uygun hale getiriliyor")
        
        # Aylık ortalama hesaplayan DataFrame'i kullan
        result_df = df
        
        # Sütun isimlerini kontrol et ve gerekirse değiştir
        if "avg_load_percentage" in result_df.columns and "avg_load" not in result_df.columns:
            result_df = result_df.withColumnRenamed("avg_load_percentage", "avg_load")
        
        if "month_num" in result_df.columns and "month" not in result_df.columns:
            result_df = result_df.withColumnRenamed("month_num", "month")
        
        # Eksik sütunları ekle
        
        # Eğer customer_id yoksa, 'ALL' olarak ayarla (tüm müşteriler için toplu ortalama)
        if "customer_id" not in result_df.columns:
            result_df = result_df.withColumn("customer_id", lit("ALL"))
        
        # Eğer year yoksa, 2026 olarak ayarla (örnekteki yıl)
        if "year" not in result_df.columns:
            result_df = result_df.withColumn("year", lit(2026))
        
        # min_load, max_load ve std_deviation için varsayılan değerler ekle
        if "min_load" not in result_df.columns:
            result_df = result_df.withColumn("min_load", col("avg_load"))
        
        if "max_load" not in result_df.columns:
            result_df = result_df.withColumn("max_load", col("avg_load"))
        
        if "std_deviation" not in result_df.columns:
            result_df = result_df.withColumn("std_deviation", lit(0.0))
        
        # Sayaçları ekle
        if "customer_count" not in result_df.columns:
            result_df = result_df.withColumn("customer_count", lit(1))
        
        if "record_count" not in result_df.columns:
            result_df = result_df.withColumn("record_count", lit(1))
        
        # Son güncelleme zamanını ekle
        result_df = result_df.withColumn("last_updated", current_timestamp())
        
        # ÖNEMLİ: Sadece veritabanı tablosunda bulunan sütunları seç
        # Bu kısım önemli - tablo şemasıyla eşleşmesi için
        result_df = result_df.select(
            "customer_id",
            "year",
            "month",
            "avg_load",
            "min_load",
            "max_load", 
            "std_deviation",
            "customer_count",
            "record_count",
            "last_updated"
        )
        
        logger.info("DataFrame monthly_average_consumption tablosuna uygun hale getirildi")
        return result_df
    
    except Exception as e:
        logger.error(f"Monthly average dönüşüm hatası: {str(e)}")
        raise