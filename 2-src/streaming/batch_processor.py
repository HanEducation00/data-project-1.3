#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Batch İşleme Modülü

Bu modül, her bir stream batch'i işleyen fonksiyonları içerir.
"""

from utils.logger import get_logger
from utils.connections import write_to_postgres
from utils.config import DB_TABLES
from data_processing.validation import validate_data, validate_numeric_values
from data_processing.transformation import process_raw_data
from data_processing.aggregation import calculate_monthly_averages, calculate_daily_load_profile

# Logger oluştur
logger = get_logger(__name__)

def process_batch(df, epoch_id):
    """
    Her batch için çalışacak işleme fonksiyonu.
    
    Bu fonksiyon şu adımları gerçekleştirir:
    1. Veri doğrulama
    2. Ham veri işleme
    3. Aylık ortalamaları hesaplama
    4. Sonuçları PostgreSQL'e yazma
    
    Args:
        df (DataFrame): İşlenecek batch DataFrame'i
        epoch_id (int): Batch kimlik numarası
    """
    try:
        logger.info(f"Batch {epoch_id} işleniyor - {df.count()} satır")
        
        # 1. Veri doğrulama
        validated_df = validate_data(df)
        
        # 2. Ham veriyi işle
        processed_df = process_raw_data(validated_df)
        
        # İşlenmiş veriyi geçici tablo olarak kaydet
        processed_df.createOrReplaceTempView("load_data")
        
        # Sayısal değerleri doğrula
        validated_numeric_df = validate_numeric_values(processed_df)
        
        # 3. Aylık ortalamaları hesapla
        monthly_avg = calculate_monthly_averages(validated_numeric_df)
        
        # Günlük yük profilini hesapla (ek analiz)
        daily_load = calculate_daily_load_profile(validated_numeric_df)
        
        # 4. Sonuçları göster
        logger.info(f"Batch {epoch_id} - Aylık Ortalama Elektrik Tüketimi:")
        monthly_avg.show()
        
        logger.info(f"Batch {epoch_id} - Günlük Yük Profili:")
        daily_load.show()
        
        # 5. Sonuçları PostgreSQL'e yaz
        # Ham veriyi kaydet (isteğe bağlı)
        write_to_postgres(df, DB_TABLES["raw_data"])
        
        # İşlenmiş detay veriyi kaydet
        write_to_postgres(processed_df, DB_TABLES["load_data_detail"])
        
        # Aylık ortalamaları kaydet
        write_to_postgres(monthly_avg, DB_TABLES["monthly_average"])
        
        logger.info(f"Batch {epoch_id} başarıyla işlendi")
    
    except Exception as e:
        logger.error(f"Batch {epoch_id} işlenirken hata oluştu: {str(e)}")
        # Hatayı yukarı ilet, böylece ana uygulama uygun şekilde ele alabilir
        raise
