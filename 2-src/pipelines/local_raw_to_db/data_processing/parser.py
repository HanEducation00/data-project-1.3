#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dosya Parse İşlemleri

Bu modül local elektrik yük dosyalarını okur ve parse eder.
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dosya Parse İşlemleri

Bu modül local elektrik yük dosyalarını okur ve parse eder.
"""

import os
from ..utils.config import DATA_CONFIG
from ..utils.logger import get_logger
from ..schemas import RAW_LOAD_SCHEMA, FILE_HEADER_FORMAT, EXPECTED_COLUMN_COUNT  # 3 NOKTA!

logger = get_logger(__name__)

def parse_day_file(spark, file_path):
    """
    Günlük yük dosyasını parse et ve DataFrame'e dönüştür
    
    Args:
        spark: Spark session
        file_path (str): Dosya yolu
        
    Returns:
        DataFrame veya None: Parse edilmiş veri DataFrame'i
    """
    logger.info(f"Dosya işleniyor: {file_path}")
    
    # Dosya var mı kontrol et
    if not os.path.exists(file_path):
        logger.error(f"Dosya bulunamadı: {file_path}")
        return None
    
    try:
        # Dosyayı oku
        raw_data = spark.sparkContext.textFile(file_path)
        
        # Header kontrolü
        if not validate_header(raw_data):
            logger.error(f"Geçersiz dosya formatı: {file_path}")
            return None
        
        # Veri satırlarını al
        data_lines = raw_data.filter(lambda line: line.startswith('res_'))
        
        if data_lines.count() == 0:
            logger.warning(f"Veri satırı bulunamadı: {file_path}")
            return None
        
        # Dosya adından gün numarasını çıkar
        day_num = extract_day_number(file_path)
        
        # Satırları parse et
        parsed_data = data_lines.flatMap(lambda line: parse_line(line, day_num))
        
        # DataFrame oluştur
        df = spark.createDataFrame(parsed_data, RAW_LOAD_SCHEMA)
        
        record_count = df.count()
        logger.info(f"Dosya başarıyla işlendi: {file_path}, {record_count} kayıt")
        
        return df
        
    except Exception as e:
        logger.error(f"Dosya parse hatası {file_path}: {e}")
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
    
    for file_path in file_paths:
        day_df = parse_day_file(spark, file_path)
        
        if day_df is not None:
            if all_data is None:
                all_data = day_df
            else:
                all_data = all_data.union(day_df)
            successful_files += 1
    
    if all_data is not None:
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
        sample_days (list): Gün numaraları listesi. None ise config'den alınır.
        
    Returns:
        list: Mevcut dosya yolları listesi
    """
    if sample_days is None:
        sample_days = DATA_CONFIG["sample_days"]
    
    base_path = DATA_CONFIG["base_path"]
    file_pattern = DATA_CONFIG["file_pattern"]
    
    logger.info(f"Veri dizini: {base_path}")
    logger.info(f"Örneklem günleri: {len(sample_days)} gün")
    
    # Dizin var mı kontrol et
    if not os.path.exists(base_path):
        logger.error(f"Veri dizini bulunamadı: {base_path}")
        return []
    
    # Mevcut dosyaları bul
    existing_files = []
    missing_files = []
    
    for day in sample_days:
        file_name = file_pattern.format(day)
        file_path = os.path.join(base_path, file_name)
        
        if os.path.exists(file_path):
            existing_files.append(file_path)
        else:
            missing_files.append(file_path)
    
    logger.info(f"Bulunan dosyalar: {len(existing_files)}")
    if missing_files:
        logger.warning(f"Eksik dosyalar: {len(missing_files)}")
        for missing_file in missing_files[:5]:  # İlk 5 tanesini logla
            logger.warning(f"Eksik: {missing_file}")
    
    return existing_files

def validate_header(raw_data):
    """
    Dosya header'ını doğrula
    
    Args:
        raw_data: Spark RDD
        
    Returns:
        bool: Header geçerli ise True
    """
    try:
        header_lines = raw_data.filter(lambda line: line.startswith('FORMAT='))
        
        if header_lines.count() == 0:
            return False
        
        header_line = header_lines.first()
        
        # Basit format kontrolü
        return "FORMAT=" in header_line and "ID" in header_line
        
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
        # cyme_load_timeseries_day_123.txt -> 123
        filename = os.path.basename(file_path)
        day_num = int(filename.split('_')[-1].split('.')[0])
        return day_num
    except Exception as e:
        logger.warning(f"Gün numarası çıkarılamadı {file_path}: {e}")
        return 1  # Varsayılan değer

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
        
        # Minimum kolon sayısı kontrolü
        if len(parts) < 12:  # En az ID + 11 meta alan olmalı
            return []
        
        customer_id = parts[0]
        profile_type = parts[1] if len(parts) > 1 and parts[1].strip() else None
        
        # Yük değerlerini al (11. sütundan sonra)
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
        
        # Her 15 dakikalık dilim için bir satır oluştur
        rows = []
        for i, load_value in enumerate(load_values):
            if load_value is not None:
                # Zaman hesapla (15 dakikalık intervals)
                hour = (i * 15) // 60
                minute = (i * 15) % 60
                
                # Satır oluştur
                row = {
                    'customer_id': customer_id,
                    'profile_type': profile_type,
                    'day_num': day_num,
                    'hour': hour,
                    'minute': minute,
                    'interval_idx': i,
                    'load_percentage': load_value
                }
                rows.append(row)
        
        return rows
        
    except Exception as e:
        logger.error(f"Satır parse hatası: {e}")
        return []
