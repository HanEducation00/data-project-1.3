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
from data_processing.db_transformations import prepare_raw_data_df, prepare_load_data_detail_df, prepare_monthly_average_df

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
        
        # Ham DataFrame'in içeriğini göster (debug için)
        logger.info(f"Ham DataFrame sütunları: {df.columns}")
        logger.info(f"Ham DataFrame örnek içerik (ilk 5 satır):")
        df.show(5, truncate=False)
        
        # 1. Veri doğrulama
        validated_df = validate_data(df)
        logger.info(f"Doğrulanmış veri örnek içerik:")
        validated_df.show(5, truncate=False)
        
        # 2. Ham veriyi işle
        processed_df = process_raw_data(validated_df)
        logger.info(f"İşlenmiş veri örnek içerik:")
        processed_df.show(5, truncate=False)
        
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
        
        # ------------ YENİ KOD BAŞLANGICI ------------
        
        # Ham veriyi raw_data tablosuna yaz
        raw_df = prepare_raw_data_df(df)
        write_to_postgres(raw_df, DB_TABLES["raw_data"])
        
        # İşlenmiş detay veriyi load_data_detail tablosuna yaz
        detail_df = prepare_load_data_detail_df(processed_df)
        write_to_postgres(detail_df, DB_TABLES["load_data_detail"])
        
        # Aylık ortalamaları monthly_average_consumption tablosuna yaz
        monthly_df = prepare_monthly_average_df(monthly_avg)
        write_to_postgres(monthly_df, DB_TABLES["monthly_average"])
        
        # ------------ YENİ KOD SONU ------------
        
        logger.info(f"Batch {epoch_id} başarıyla işlendi")
    
    except Exception as e:
        logger.error(f"Batch {epoch_id} işlenirken hata oluştu: {str(e)}")
        # Hatayı yukarı ilet, böylece ana uygulama uygun şekilde ele alabilir
        raise

