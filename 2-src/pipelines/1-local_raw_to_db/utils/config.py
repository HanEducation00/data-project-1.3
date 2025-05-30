#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Local Raw to DB Pipeline Konfigürasyonu

Local dosyalardan ham veriyi okuyup PostgreSQL'e yazan pipeline için konfigürasyon.
"""

# PostgreSQL konfigürasyonu
POSTGRES_CONFIG = {
    "host": "postgres",
    "port": "5432",
    "database": "datawarehouse",
    "user": "datauser",
    "password": "datapass",
    "driver": "org.postgresql.Driver"
}

# PostgreSQL JDBC URL
JDBC_URL = f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"

# PostgreSQL bağlantı özellikleri
JDBC_PROPERTIES = {
    "user": POSTGRES_CONFIG["user"],
    "password": POSTGRES_CONFIG["password"],
    "driver": POSTGRES_CONFIG["driver"]
}

# Spark konfigürasyonu
SPARK_CONFIG = {
    "app_name": "Local Raw to DB Pipeline",
    "master": "local[*]",
    "log_level": "INFO",
    "packages": "org.postgresql:postgresql:42.6.0"
}

# Veri dosyaları konfigürasyonu
DATA_CONFIG = {
    "file_pattern": "cyme_load_timeseries_day_{}.txt"
}

# Veritabanı tablo ismi
TABLE_NAME = "raw_load_data"

# Batch işleme konfigürasyonu
BATCH_CONFIG = {
    "batch_size": 10000,  # Her batch'te kaç satır işlenecek
    "repartition_count": 4  # DataFrame kaç partition'a bölünecek
}

# Örneklem günlerini hesapla
def get_sample_days():
    """Her aydan 8 gün seç (4 günde bir)"""
    sample_days = []
    month_starts = [1, 32, 61, 92, 122, 153, 183, 214, 245, 275, 306, 336]
    
    for start_day in month_starts:
        # Her aydan 8 gün seç
        month_days = list(range(start_day, start_day + 30, 4))[:8]
        sample_days.extend(month_days)
    
    return sample_days

# Sample days'i config'e ekle
DATA_CONFIG["sample_days"] = get_sample_days()