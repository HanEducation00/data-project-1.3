#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dosya Parse İşlemleri

Bu modül local elektrik yük dosyalarını okur ve parse eder.
"""

import os
import time  # ← EKLENEN

from ..utils.config import DATA_CONFIG, TABLE_NAME  # ← TABLE_NAME EKLENEN
from ..utils.logger import get_logger
from ..utils.connections import (  # ← EKLENEN BLOK
    get_spark_session, test_postgresql_connection, 
    table_exists, write_dataframe_to_postgresql,
    create_table_if_not_exists, cleanup_connections
)
from ..schemas import RAW_LOAD_SCHEMA, FILE_HEADER_FORMAT, EXPECTED_COLUMN_COUNT, POSTGRES_TABLE_SCHEMA  # ← POSTGRES_TABLE_SCHEMA EKLENEN
from datetime import datetime, timedelta 

logger = get_logger(__name__)

def run_pipeline():
    """
    Ana pipeline işlemi
    
    Returns:
        bool: Başarılı ise True
    """
    logger.log_processing_start("LOCAL RAW TO DB PIPELINE")
    start_time = time.time()
    
    spark = None  # ← EKLENEN
    
    try:
        # 1. Bağlantıları test et
        logger.info("Bağlantılar test ediliyor...")
        if not test_postgresql_connection():
            logger.error("PostgreSQL bağlantısı başarısız!")
            return False
        
        logger.info("Tüm bağlantılar başarılı")
        
        # 2. Tablolar kontrol et ve oluştur
        logger.info("Tablolar kontrol ediliyor...")
        if not table_exists(TABLE_NAME):
            logger.info(f"Tablo bulunamadı: {TABLE_NAME}, oluşturuluyor...")
            
            if create_table_if_not_exists(POSTGRES_TABLE_SCHEMA):
                logger.info(f"Tablo başarıyla oluşturuldu: {TABLE_NAME}")
            else:
                logger.error(f"Tablo oluşturulamadı: {TABLE_NAME}")
                return False
        else:
            logger.info(f"Tablo mevcut: {TABLE_NAME}")
        
        # 3. Spark session al
        spark = get_spark_session()
        
        # 4. Dosyaları bul
        logger.info("Veri dosyaları aranıyor...")
        file_paths = get_sample_day_files()
        
        if not file_paths:
            logger.error("Hiç veri dosyası bulunamadı!")
            return False
        
        # 5. Dosyaları parse et
        logger.info("Dosyalar parse ediliyor...")
        parsed_df = parse_multiple_files(spark, file_paths)
        
        if parsed_df is None:
            logger.error("Dosya parse işlemi başarısız!")
            return False
        
        # 6. Veritabanına yaz
        logger.info("Veriler PostgreSQL'e yazılıyor...")
        if write_dataframe_to_postgresql(parsed_df, TABLE_NAME, mode="append"):
            logger.info("Pipeline başarıyla tamamlandı!")
            
            # Sonuç istatistikleri
            end_time = time.time()
            duration = end_time - start_time
            logger.log_processing_end("LOCAL RAW TO DB PIPELINE", duration)
            
            return True
        else:
            logger.error("Veritabanına yazma başarısız!")
            return False
        
    except Exception as e:
        logger.log_error_with_details(e, "PIPELINE")
        return False
        
    finally:  # ← EKLENEN BLOK
        # Spark session'ı temizle
        if spark is not None:
            try:
                spark.stop()
                logger.info("Spark session kapatıldı")
            except Exception as e:
                logger.warning(f"Spark session kapatma hatası: {e}")
        
        # Diğer bağlantıları temizle
        try:
            cleanup_connections()
        except Exception as e:
            logger.warning(f"Connection cleanup hatası: {e}")


def parse_day_file(spark, file_path):
    """
    Günlük yük dosyasını parse et ve DataFrame'e dönüştür (eski çalışan kod yaklaşımı)
    
    Args:
        spark: Spark session
        file_path (str): Dosya yolu
        
    Returns:
        DataFrame veya None: Parse edilmiş veri DataFrame'i
    """
    logger.info(f"Dosya işleniyor: {file_path}")
    
    try:
        # Dosya var mı kontrol et
        if not os.path.exists(file_path):
            logger.error(f"Dosya bulunamadı: {file_path}")
            return None
        
        # Header doğrulaması (Python ile)
        if not validate_file_header(spark, file_path):
            logger.error(f"Geçersiz dosya formatı: {file_path}")
            return None
        
        # Dosyayı Python ile oku (Spark RDD yerine)
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Veri satırlarını al
        data_lines = [line for line in lines if line.strip().startswith('res_')]
        
        if len(data_lines) == 0:
            logger.warning(f"Veri satırı bulunamadı: {file_path}")
            return None
        
        # Gün numarasını çıkar
        day_num = extract_day_number(file_path)
        
        # Veri satırlarını parse et
        all_rows = []
        for line in data_lines:
            parsed_rows = parse_line(line, day_num)
            all_rows.extend(parsed_rows)
        
        if len(all_rows) == 0:
            logger.warning(f"Hiç geçerli veri satırı bulunamadı: {file_path}")
            return None
        
        # DataFrame oluştur
        df = spark.createDataFrame(all_rows, RAW_LOAD_SCHEMA)
        
        record_count = len(all_rows)  # count() yerine len() kullan
        logger.info(f"Dosya başarıyla işlendi: {file_path}, {record_count} kayıt")
        
        return df
        
    except Exception as e:
        logger.error(f"Dosya parse hatası {file_path}: {e}")
        import traceback
        traceback.print_exc()
        return None


def parse_multiple_files(spark, file_paths):
    """
    Birden fazla dosyayı parse et ve birleştir
    
    Args:
        spark: Spark session
        file_paths (list): Dosya yolları listesi
        
    Returns:
        DataFrame veya None: Birleştirilmiş DataFrame
    """
    logger.info(f"Toplam {len(file_paths)} dosya işlenecek")
    
    all_data = None
    successful_files = 0
    
    for i, file_path in enumerate(file_paths, 1):
        logger.info(f"İşleniyor ({i}/{len(file_paths)}): {os.path.basename(file_path)}")
        
        day_df = parse_day_file(spark, file_path)
        
        if day_df is not None:
            if all_data is None:
                all_data = day_df
            else:
                all_data = all_data.union(day_df)
            successful_files += 1
            logger.info(f"✅ Başarılı: {os.path.basename(file_path)}")
        else:
            logger.error(f"❌ Başarısız: {os.path.basename(file_path)}")
    
    if all_data is not None:
        # count() işlemini en sonda yap
        total_records = all_data.count()
        logger.info(f"Toplam {successful_files}/{len(file_paths)} dosya başarıyla işlendi")
        logger.info(f"Toplam {total_records} kayıt oluşturuldu")
    else:
        logger.error("Hiçbir dosya başarıyla işlenemedi")
    
    return all_data

def get_sample_day_files(sample_days=None):
    """
    Örneklem günlerine ait dosya yollarını getir
    
    Args:
        sample_days (list, optional): Örneklem günleri listesi
        
    Returns:
        list: Mevcut dosya yolları listesi
    """
    if sample_days is None:
        sample_days = DATA_CONFIG["sample_days"]
    
    file_pattern = DATA_CONFIG["file_pattern"]
    
    # Ana path
    data_dir = "/data/smart-ds/2016/AUS/P1R/load_timeseries"
    logger.info(f"Veri dizini: {data_dir}")
    
    # Dizin var mı kontrol et
    if not os.path.exists(data_dir):
        logger.error(f"HATA: Veri dizini bulunamadı: {data_dir}")
        
        # Alternatif dizinleri dene
        alt_dirs = [
            "/workspace/data-project-1/0-data/raw_data/2016/AUS/P1R/load_timeseries",
            "/workspace/data/raw_data/2016/AUS/P1R/load_timeseries",
            "/data/0-data/raw_data/2016/AUS/P1R/load_timeseries",
            "/workspace/0-data/raw_data/2016/AUS/P1R/load_timeseries"
        ]
        
        for alt_dir in alt_dirs:
            logger.info(f"Alternatif dizin deneniyor: {alt_dir}")
            if os.path.exists(alt_dir):
                data_dir = alt_dir
                logger.info(f"Alternatif dizin bulundu: {data_dir}")
                break
        else:
            logger.error("Hiçbir veri dizini bulunamadı!")
            return []
    
    # Dosyaları bul
    existing_files = []
    missing_files = []
    
    logger.info(f"Toplam {len(sample_days)} gün için dosyalar aranıyor...")
    
    for day in sample_days:
        file_name = file_pattern.format(day)
        file_path = os.path.join(data_dir, file_name)
        
        if os.path.exists(file_path):
            existing_files.append(file_path)
        else:
            missing_files.append(file_path)
    
    # Sonuçları logla
    logger.info(f"Bulunan dosyalar: {len(existing_files)}")
    logger.info(f"Eksik dosyalar: {len(missing_files)}")
    
    if missing_files:
        logger.warning(f"İlk 5 eksik dosya:")
        for missing_file in missing_files[:5]:
            logger.warning(f"  Eksik: {os.path.basename(missing_file)}")
    
    if existing_files:
        logger.info(f"İlk 5 bulunan dosya:")
        for found_file in existing_files[:5]:
            logger.info(f"  Bulundu: {os.path.basename(found_file)}")
    
    return existing_files


def validate_file_header(spark, file_path):
    """
    Dosya başlığını doğrula (eski çalışan kod yaklaşımı)
    
    Args:
        spark: Spark session
        file_path: Dosya yolu
        
    Returns:
        bool: Dosya geçerli mi
    """
    try:
        logger.info(f"Header doğrulanıyor: {file_path}")
        
        # Dosya var mı kontrol et
        if not os.path.exists(file_path):
            logger.error(f"Dosya bulunamadı: {file_path}")
            return False
        
        # Dosyayı Python ile oku (Spark RDD yerine)
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        if len(lines) < 2:
            logger.error(f"Dosya çok kısa: {file_path}")
            return False
        
        # FORMAT= satırını ara
        format_found = False
        for line in lines[:10]:  # İlk 10 satırda ara
            if line.strip().startswith('FORMAT='):
                format_found = True
                break
        
        if not format_found:
            logger.error(f"FORMAT= satırı bulunamadı: {file_path}")
            return False
        
        # res_ ile başlayan veri satırları var mı kontrol et
        data_lines = [line for line in lines if line.strip().startswith('res_')]
        
        if len(data_lines) == 0:
            logger.error(f"Veri satırı bulunamadı (res_ ile başlayan): {file_path}")
            return False
        
        logger.info(f"Header doğrulandı: {file_path} - {len(data_lines)} veri satırı")
        return True
        
    except Exception as e:
        logger.error(f"Header doğrulama hatası: {e}")
        return False



def extract_day_number(file_path):
    """
    Dosya adından gün numarasını çıkar
   
    Args:
        file_path (str): Dosya yolu
       
    Returns:
        int: Gün numarası
    """
    try:
        filename = os.path.basename(file_path)
        day_num = int(filename.split('_')[-1].split('.')[0])
        return day_num
    except Exception as e:
        logger.warning(f"Gün numarası çıkarılamadı {file_path}: {e}")
        return 1


def parse_line(line, day_num):
    """
    Tek veri satırını parse et
    
    Args:
        line (str): Veri satırı
        day_num (int): Gün numarası
        
    Returns:
        list: Parse edilmiş satır verileri listesi
    """
    try:
        parts = line.split(',')
        
        if len(parts) < 12:
            return []
        
        customer_id = parts[0]
        profile_type = parts[1] if len(parts) > 1 and parts[1].strip() else None
        
        load_values = []
        for val in parts[11:]:
            if val.strip():
                try:
                    if val.strip().upper() in ['NONE', 'NULL', '']:
                        load_values.append(None)
                    else:
                        load_values.append(float(val.strip()))
                except ValueError:
                    load_values.append(None)
        
        # Base tarih: 2016-01-01  ← EKLENEN BLOK
        base_date = datetime(2016, 1, 1)
        actual_date = base_date + timedelta(days=day_num - 1)
        
        rows = []
        for i, load_value in enumerate(load_values):
            if load_value is not None:
                hour = (i * 15) // 60
                minute = (i * 15) % 60
                
                # Full timestamp hesapla  ← EKLENEN
                full_timestamp = actual_date + timedelta(hours=hour, minutes=minute)
                
                row = {
                    'customer_id': customer_id,
                    'profile_type': profile_type,
                    'day_num': day_num,
                    'hour': hour,
                    'minute': minute,
                    'interval_idx': i,
                    'load_percentage': load_value,
                    'full_timestamp': full_timestamp  # ← EKLENEN
                }
                rows.append(row)
        
        return rows
        
    except Exception as e:
        logger.error(f"Satır parse hatası: {e}")
        return []

