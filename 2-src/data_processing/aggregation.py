#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Veri Toplama İşlemleri

Bu modül, işlenmiş verileri özetleyen ve toplayan fonksiyonları içerir.
Aylık ortalama hesaplamaları, toplam tüketim analizleri vb.
"""

from pyspark.sql.functions import avg, sum, count, col, window
from utils.logger import get_logger

# Logger oluştur
logger = get_logger(__name__)

def calculate_monthly_averages(df):
    """
    Aylık ortalama yük yüzdelerini hesaplar.
    
    Args:
        df (DataFrame): İşlenmiş yük verileri DataFrame'i
    
    Returns:
        DataFrame: Aylık ortalama yük yüzdeleri
    """
    try:
        # Aylık ortalama hesapla
        monthly_avg = df \
            .groupBy("month_num", "month_name") \
            .agg(avg("load_percentage").alias("avg_load_percentage"))
        
        # Ay numarasına göre sırala
        monthly_avg = monthly_avg.orderBy("month_num")
        
        logger.info("Aylık ortalama yük yüzdeleri hesaplandı")
        return monthly_avg
    
    except Exception as e:
        logger.error(f"Aylık ortalama hesaplama hatası: {str(e)}")
        raise

def calculate_daily_load_profile(df):
    """
    Günlük yük profilini hesaplar (saatlik ortalamalar).
    
    Args:
        df (DataFrame): İşlenmiş yük verileri DataFrame'i
    
    Returns:
        DataFrame: Saatlik ortalama yük yüzdeleri
    """
    try:
        # Saatlik ortalama hesapla
        hourly_avg = df \
            .groupBy("hour") \
            .agg(avg("load_percentage").alias("avg_load_percentage"))
        
        # Saate göre sırala
        hourly_avg = hourly_avg.orderBy("hour")
        
        logger.info("Günlük yük profili hesaplandı")
        return hourly_avg
    
    except Exception as e:
        logger.error(f"Günlük yük profili hesaplama hatası: {str(e)}")
        raise

def calculate_customer_profiles(df):
    """
    Müşteri bazlı tüketim profillerini hesaplar.
    
    Args:
        df (DataFrame): İşlenmiş yük verileri DataFrame'i
    
    Returns:
        DataFrame: Müşteri bazlı ortalama tüketim profilleri
    """
    try:
        # Müşteri bazlı ortalama hesapla
        customer_avg = df \
            .groupBy("customer_id", "profile_type") \
            .agg(
                avg("load_percentage").alias("avg_load_percentage"),
                count("*").alias("data_points")
            )
        
        # Ortalama tüketime göre sırala
        customer_avg = customer_avg.orderBy(col("avg_load_percentage").desc())
        
        logger.info("Müşteri profilleri hesaplandı")
        return customer_avg
    
    except Exception as e:
        logger.error(f"Müşteri profili hesaplama hatası: {str(e)}")
        raise
