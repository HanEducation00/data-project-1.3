#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bağlantı Yönetimi

Bu modül, uygulama içinde kullanılan tüm harici bağlantıları (Spark, Kafka, PostgreSQL)
yönetir. Bağlantı oluşturma, yapılandırma ve kapatma işlevlerini içerir.
"""

from pyspark.sql import SparkSession
import psycopg2
from psycopg2 import pool
import logging
from . import config

# Logger modülü henüz oluşturulmadığından basit bir logger tanımlayalım
logger = logging.getLogger(__name__)

def create_spark_session():
    """
    Spark Session oluşturur ve yapılandırır - geliştirme ortamı için local mode.
    
    Returns:
        SparkSession: Yapılandırılmış spark session nesnesi
    """
    try:
        # Spark session'ı oluştur - geliştirme için local mode
        spark = SparkSession.builder \
            .appName(config.SPARK_CONFIG["app_name"]) \
            .config("spark.jars.packages", config.SPARK_CONFIG["packages"]) \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .master("local[*]") \
            .getOrCreate()
        
        # Log seviyesini ayarla
        spark.sparkContext.setLogLevel(config.SPARK_CONFIG["log_level"])
        
        logger.info(f"Spark session oluşturuldu (local[*] mode). Versiyon: {spark.version}")
        return spark
    
    except Exception as e:
        logger.error(f"Spark session oluşturulurken hata: {str(e)}")
        raise

def create_postgres_connection():
    """
    PostgreSQL veritabanına doğrudan bağlantı oluşturur.
    
    Returns:
        connection: PostgreSQL bağlantı nesnesi
    """
    try:
        # Bağlantıyı oluştur
        connection = psycopg2.connect(
            host=config.POSTGRES_CONFIG["host"],
            port=config.POSTGRES_CONFIG["port"],
            database=config.POSTGRES_CONFIG["database"],
            user=config.POSTGRES_CONFIG["user"],
            password=config.POSTGRES_CONFIG["password"]
        )
        
        logger.info("PostgreSQL bağlantısı başarıyla oluşturuldu")
        return connection
    
    except Exception as e:
        logger.error(f"PostgreSQL bağlantısı oluşturulurken hata: {str(e)}")
        raise

def create_postgres_connection_pool(min_conn=1, max_conn=10):
    """
    PostgreSQL bağlantı havuzu oluşturur.
    
    Args:
        min_conn (int): Minimum bağlantı sayısı
        max_conn (int): Maksimum bağlantı sayısı
    
    Returns:
        pool: PostgreSQL bağlantı havuzu
    """
    try:
        # Bağlantı havuzunu oluştur
        connection_pool = psycopg2.pool.ThreadedConnectionPool(
            min_conn,
            max_conn,
            host=config.POSTGRES_CONFIG["host"],
            port=config.POSTGRES_CONFIG["port"],
            database=config.POSTGRES_CONFIG["database"],
            user=config.POSTGRES_CONFIG["user"],
            password=config.POSTGRES_CONFIG["password"]
        )
        
        logger.info(f"PostgreSQL bağlantı havuzu oluşturuldu (min: {min_conn}, max: {max_conn})")
        return connection_pool
    
    except Exception as e:
        logger.error(f"PostgreSQL bağlantı havuzu oluşturulurken hata: {str(e)}")
        raise

def read_kafka_stream(spark):
    """
    Kafka'dan veri akışını okur.
    
    Args:
        spark (SparkSession): Spark session nesnesi
    
    Returns:
        DataFrame: Kafka'dan okunan ve ayrıştırılan veri akışı
    """
    try:
        # Kafka'dan stream oku
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config.KAFKA_CONFIG["bootstrap_servers"]) \
            .option("subscribe", config.KAFKA_CONFIG["topic"]) \
            .option("startingOffsets", config.KAFKA_CONFIG["starting_offsets"]) \
            .option("failOnDataLoss", "false") \
            .load()
        
        # JSON değerini string olarak al
        kafka_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")
        
        logger.info("Kafka stream başarıyla okundu")
        return kafka_df
    
    except Exception as e:
        logger.error(f"Kafka stream okunurken hata: {str(e)}")
        raise

def write_to_postgres(df, table_name, mode="append"):
    """
    DataFrame'i PostgreSQL veritabanına yazar.
    
    Args:
        df (DataFrame): Yazılacak DataFrame
        table_name (str): Hedef tablo adı
        mode (str): Yazma modu (append, overwrite, vb.)
    """
    try:
        logger.info(f"Veriler {table_name} tablosuna yazılıyor (mod: {mode})...")
        
        # JDBC URL'yi oluştur
        jdbc_url = f"jdbc:postgresql://{config.POSTGRES_CONFIG['host']}:{config.POSTGRES_CONFIG['port']}/{config.POSTGRES_CONFIG['database']}"
        
        # DataFrame'i PostgreSQL'e yaz
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", config.POSTGRES_CONFIG["user"]) \
            .option("password", config.POSTGRES_CONFIG["password"]) \
            .option("driver", config.POSTGRES_CONFIG["driver"]) \
            .option("stringtype", "unspecified") \
            .mode(mode) \
            .save()
        
        logger.info(f"Veriler {table_name} tablosuna yazıldı (mod: {mode})")
    
    except Exception as e:
        logger.error(f"{table_name} tablosuna yazılırken hata: {str(e)}")
        raise
