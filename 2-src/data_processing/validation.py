#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Veri Doğrulama İşlemleri

Bu modül, ham verilerin doğruluğunu kontrol eden ve geçerli verileri 
filtreleyen fonksiyonları içerir.
"""

import logging
from pyspark.sql.functions import col, split, array_contains, transform, size, substring

logger = logging.getLogger(__name__)

def validate_data(df):
    """
    Veri doğrulama işlemlerini uygular
    """
    try:
        # DataFrame'i parse edip doğrulama sütunları ekleyelim
        validated_df = df.withColumn("parts", split(col("raw_data"), ",", -1)) \
            .withColumn("is_valid_id", array_contains(transform(col("parts"), 
                lambda x: substring(x, 1, 4)), "res_")) \
            .withColumn("values_count", size(col("parts"))) \
            .withColumn("is_valid_count", col("values_count") >= 107)
        
        # Toplam valid olma durumu için yeni bir sütun ekleyelim
        validated_df = validated_df.withColumn("is_valid", 
            col("is_valid_id") & col("is_valid_count"))
        
        # Geçersiz verileri logla
        invalid_count = validated_df.filter(~col("is_valid")).count()
        if invalid_count > 0:
            logger.warning(f"{invalid_count} geçersiz veri bulundu ve filtrelendi")
        
        # Sadece geçerli verileri döndür
        valid_df = validated_df.filter(col("is_valid"))
        
        # Doğrulama sonucunu logla
        total_count = df.count()
        valid_count = valid_df.count()
        
        # Sıfıra bölme hatasını önle
        if total_count > 0:
            logger.info(f"Veri doğrulama: Toplam {total_count} satırdan {valid_count} satır geçerli ({valid_count/total_count*100:.2f}%)")
        else:
            logger.info(f"Veri doğrulama: İşlenecek veri yok (toplam satır sayısı: 0)")
        
        return valid_df
        
    except Exception as e:
        logger.error(f"Veri doğrulama hatası: {str(e)}")
        raise



def validate_numeric_values(df, column_name="load_percentage"):
    """
    Sayısal değerlerin geçerliliğini kontrol eder ve null değerleri filtreler.
    
    Args:
        df (DataFrame): Kontrol edilecek DataFrame
        column_name (str): Kontrol edilecek sütun adı
    
    Returns:
        DataFrame: Geçerli sayısal değerleri içeren DataFrame
    """
    try:
        # Null olmayan değerleri filtrele
        validated_df = df.filter(col(column_name).isNotNull())
        
        # Negatif değerleri kontrol et
        negative_count = validated_df.filter(col(column_name) < 0).count()
        if negative_count > 0:
            logger.warning(f"{negative_count} negatif {column_name} değeri bulundu")
        
        # Aşırı değerleri kontrol et (örneğin yük yüzdesi 100'den büyük olmamalı)
        if column_name == "load_percentage":
            extreme_count = validated_df.filter(col(column_name) > 100).count()
            if extreme_count > 0:
                logger.warning(f"{extreme_count} aşırı {column_name} değeri bulundu (>100)")
        
        return validated_df
    
    except Exception as e:
        logger.error(f"Sayısal değer doğrulama hatası: {str(e)}")
        raise
