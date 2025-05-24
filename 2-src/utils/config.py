#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Konfigürasyon Dosyası

Bu dosya, uygulama genelinde kullanılan tüm konfigürasyon değişkenlerini içerir.
Farklı ortamlar (geliştirme, test, üretim) için değerler burada merkezi olarak yönetilir.
"""

# Kafka konfigürasyonu
KAFKA_CONFIG = {
    "bootstrap_servers": "kafka1:9092,kafka2:9092,kafka3:9092",
    "topic": "sensor-data",  # Generator'ın yazdığı topic ismi
    "starting_offsets": "earliest"
}


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
    "app_name": "Electric Load Forecasting - Streaming",
    "master": "local[*]",
    "log_level": "ERROR",
    "packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.6.0"
}

# Checkpoint yolu
CHECKPOINT_PATH = "/workspace/checkpoints/kafka-spark-streaming"

# Veritabanı tablo isimleri
DB_TABLES = {
    "raw_data": "raw_data",
    "load_data_detail": "load_data_detail",
    "monthly_average": "monthly_average_consumption"
}

# Streaming konfigürasyonu
STREAMING_CONFIG = {
    "processing_time": "1 minute"
}
