#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database ve Spark Bağlantı Fonksiyonları

Bu modül PostgreSQL ve Spark bağlantılarını yönetir.
"""

from pyspark.sql import SparkSession
import os
from .config import POSTGRES_CONFIG, SPARK_CONFIG, JDBC_URL, JDBC_PROPERTIES
from .logger import get_logger

logger = get_logger(__name__)

# Global Spark session
_spark_session = None

def get_spark_session():
    """
    Spark session oluştur ve döndür (eski çalışan kod ayarları)
    
    Returns:
        SparkSession: Yapılandırılmış Spark session
    """
    logger.info("Spark session oluşturuluyor...")
    
    try:
        # Eski çalışan kodun ayarları
        spark = SparkSession.builder \
            .appName("Electricity Load Forecasting") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .master("local[*]") \
            .getOrCreate()
        
        # Log seviyesini INFO'ya ayarla
        spark.sparkContext.setLogLevel("INFO")
        
        logger.info("Spark session başarıyla oluşturuldu")
        logger.info(f"Spark version: {spark.version}")
        logger.info(f"Master: {spark.sparkContext.master}")
        
        return spark
        
    except Exception as e:
        logger.error(f"Spark session oluşturma hatası: {e}")
        raise e
def stop_spark_session():
    """Spark session'ı kapat"""
    global _spark_session
    
    if _spark_session is not None:
        logger.info("Spark session kapatılıyor...")
        _spark_session.stop()
        _spark_session = None
        logger.info("Spark session kapatıldı")

def test_postgresql_connection():
    """PostgreSQL bağlantısını test et"""
    try:
        logger.info("PostgreSQL bağlantısı test ediliyor...")
        
        spark = get_spark_session()
        test_df = spark.read.jdbc(
            url=JDBC_URL,
            table="(SELECT 1 as test) AS test_query",
            properties=JDBC_PROPERTIES
        )
        
        # Test sorgusu çalıştır
        test_df.collect()
        
        logger.info("PostgreSQL bağlantısı başarılı!")
        return True
        
    except Exception as e:
        logger.error(f"PostgreSQL bağlantı hatası: {e}")
        return False

def table_exists(table_name):
    """Tablo var mı kontrol et"""
    try:
        spark = get_spark_session()
        tables_df = spark.read.jdbc(
            url=JDBC_URL,
            table=f"(SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table_name}') AS tables",
            properties=JDBC_PROPERTIES
        )
        
        return tables_df.count() > 0
        
    except Exception as e:
        logger.error(f"Tablo kontrol hatası: {e}")
        return False
    
def create_table_if_not_exists(table_schema):
    """
    Tablo yoksa oluştur
    
    Args:
        table_schema (str): CREATE TABLE SQL'i
        
    Returns:
        bool: Başarılı ise True
    """
    try:
        logger.info("Tablo oluşturuluyor...")
        
        # psycopg2 ile direkt PostgreSQL bağlantısı
        import psycopg2
        
        conn = psycopg2.connect(
            host=POSTGRES_CONFIG["host"],
            port=POSTGRES_CONFIG["port"],
            database=POSTGRES_CONFIG["database"],
            user=POSTGRES_CONFIG["user"],
            password=POSTGRES_CONFIG["password"]
        )
        
        cursor = conn.cursor()
        
        # Tabloyu oluştur
        sql_statements = table_schema.strip().split(';')
        
        for sql in sql_statements:
            sql = sql.strip()
            if sql and not sql.startswith('--'):  # Boş ve comment satırları atla
                logger.debug(f"SQL çalıştırılıyor: {sql[:50]}...")
                cursor.execute(sql)
        
        # INDEX'leri oluştur - BU KISIM EKLENDİ
        from ..schemas import POSTGRES_INDEXES
        
        for index_sql in POSTGRES_INDEXES:
            try:
                logger.info(f"Index oluşturuluyor: {index_sql[:50]}...")
                cursor.execute(index_sql)
                logger.info("Index başarıyla oluşturuldu")
            except Exception as e:
                logger.warning(f"Index oluşturma hatası (devam ediliyor): {e}")
        
        # Değişiklikleri kaydet
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("Tablo başarıyla oluşturuldu!")
        return True
        
    except Exception as e:
        logger.error(f"Tablo oluşturma hatası: {e}")
        return False
    

def write_dataframe_to_postgresql(df, table_name, mode="append"):
    """DataFrame'i PostgreSQL'e yaz"""
    try:
        logger.info(f"Veri PostgreSQL'e yazılıyor: {table_name} tablosu, {df.count()} kayıt")
        
        df.write.jdbc(
            url=JDBC_URL,
            table=table_name,
            mode=mode,
            properties=JDBC_PROPERTIES
        )
        
        logger.info(f"Veri başarıyla PostgreSQL'e yazıldı: {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"PostgreSQL'e veri yazma hatası: {e}")
        return False

def get_table_count(table_name):
    """Tablodaki kayıt sayısını getir"""
    try:
        spark = get_spark_session()
        count_df = spark.read.jdbc(
            url=JDBC_URL,
            table=f"(SELECT COUNT(*) as count FROM {table_name}) AS count_query",
            properties=JDBC_PROPERTIES
        )
        
        return count_df.collect()[0]["count"]
        
    except Exception as e:
        logger.error(f"Tablo sayım hatası: {e}")
        return 0

def cleanup_connections():
    """Tüm bağlantıları temizle"""
    stop_spark_session()
    logger.info("Tüm bağlantılar temizlendi")

# Backward compatibility için
def get_spark_manager():
    """Eski API uyumluluğu için"""
    class SparkManager:
        def get_spark_session(self):
            return get_spark_session()
        def stop_spark_session(self):
            return stop_spark_session()
    
    return SparkManager()

def get_postgres_manager():
    """Eski API uyumluluğu için"""
    class PostgresManager:
        def test_connection(self):
            return test_postgresql_connection()
        def table_exists(self, table_name):
            return table_exists(table_name)
        def write_dataframe(self, df, table_name, mode="append"):
            return write_dataframe_to_postgresql(df, table_name, mode)
        def get_table_count(self, table_name):
            return get_table_count(table_name)
    
    return PostgresManager()
