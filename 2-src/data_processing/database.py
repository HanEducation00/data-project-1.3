import json
from datetime import datetime
import psycopg2
from psycopg2.extras import Json
from contextlib import contextmanager

from utils.config import POSTGRES_CONFIG
from utils.logger import get_logger
from data_processing.schema import (
    RAW_DATA_TABLE_SQL,
    LOAD_DATA_DETAIL_TABLE_SQL,
    MONTHLY_AVERAGE_CONSUMPTION_TABLE_SQL,
    CUSTOMER_FEATURES_TABLE_SQL,
    MODEL_PREDICTIONS_TABLE_SQL
)

logger = get_logger(__name__)

@contextmanager
def get_db_connection():
    """Veritabanı bağlantısı oluşturan ve yöneten context manager"""
    conn = None
    try:
        # PostgreSQL bağlantısı
        conn = psycopg2.connect(
            host=POSTGRES_CONFIG.get("host", "localhost"),
            port=POSTGRES_CONFIG.get("port", 5432),
            database=POSTGRES_CONFIG.get("database", "dataplatform"),
            user=POSTGRES_CONFIG.get("user", "postgres"),
            password=POSTGRES_CONFIG.get("password", "postgres")
        )
        yield conn
    except Exception as e:
        logger.error(f"Veritabanı bağlantı hatası: {e}")
        raise
    finally:
        if conn:
            conn.close()

def create_tables():
    """Veritabanında gerekli tüm tabloları oluşturur"""
    with get_db_connection() as conn:
        try:
            # Otomatik commit'i kapat
            conn.autocommit = False
            cursor = conn.cursor()
            
            # Tüm tabloları oluştur
            cursor.execute(RAW_DATA_TABLE_SQL)
            cursor.execute(LOAD_DATA_DETAIL_TABLE_SQL)
            cursor.execute(MONTHLY_AVERAGE_CONSUMPTION_TABLE_SQL)
            cursor.execute(CUSTOMER_FEATURES_TABLE_SQL)
            cursor.execute(MODEL_PREDICTIONS_TABLE_SQL)
            
            # Değişiklikleri kaydet
            conn.commit()
            logger.info("Tüm veritabanı tabloları başarıyla oluşturuldu")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Tablo oluşturma hatası: {e}")
            raise

# 1. Raw Data Tablosu Fonksiyonları
def insert_raw_data(topic, partition, offset, raw_json, ingestion_timestamp=None):
    """Raw data tablosuna kayıt ekler"""
    if ingestion_timestamp is None:
        ingestion_timestamp = datetime.now()
    
    with get_db_connection() as conn:
        try:
            cursor = conn.cursor()
            
            # Kayıt ekle
            cursor.execute("""
                INSERT INTO raw_data 
                (raw_json, kafka_topic, kafka_partition, kafka_offset, 
                 ingestion_timestamp, processing_timestamp, processing_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
                RETURNING id
            """, (
                Json(raw_json) if isinstance(raw_json, dict) else raw_json, 
                topic, 
                partition, 
                offset, 
                ingestion_timestamp,
                ingestion_timestamp,  # processing_timestamp başlangıçta aynı
                'PENDING'  # başlangıç durumu
            ))
            
            result = cursor.fetchone()
            conn.commit()
            
            if result:
                logger.info(f"Raw data kaydedildi, id: {result[0]}")
                return result[0]
            else:
                logger.info("Kayıt zaten mevcut, ekleme atlandı")
                return None
                
        except Exception as e:
            conn.rollback()
            logger.error(f"Raw data ekleme hatası: {e}")
            raise

def update_raw_data_status(raw_data_id, status, error_message=None):
    """Raw data işleme durumunu günceller"""
    with get_db_connection() as conn:
        try:
            cursor = conn.cursor()
            
            if error_message:
                cursor.execute("""
                    UPDATE raw_data 
                    SET processing_status = %s, 
                        processing_timestamp = %s,
                        error_message = %s
                    WHERE id = %s
                """, (status, datetime.now(), error_message, raw_data_id))
            else:
                cursor.execute("""
                    UPDATE raw_data 
                    SET processing_status = %s, 
                        processing_timestamp = %s
                    WHERE id = %s
                """, (status, datetime.now(), raw_data_id))
            
            conn.commit()
            logger.info(f"Raw data durumu güncellendi, id: {raw_data_id}, durum: {status}")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Raw data durum güncelleme hatası: {e}")
            raise

# 2. Load Data Detail Tablosu Fonksiyonları
def insert_load_data_detail(customer_id, interval_timestamp, load_percentage, 
                          year, month, day, hour, minute, 
                          batch_id, raw_data_id, is_valid=True):
    """İşlenmiş yük verilerini load_data_detail tablosuna ekler"""
    with get_db_connection() as conn:
        try:
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO load_data_detail
                (customer_id, interval_timestamp, load_percentage, 
                 year, month, day, hour, minute, 
                 batch_id, raw_data_id, processing_timestamp, is_valid)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (customer_id, interval_timestamp, batch_id) 
                DO UPDATE SET 
                    load_percentage = EXCLUDED.load_percentage,
                    is_valid = EXCLUDED.is_valid,
                    processing_timestamp = EXCLUDED.processing_timestamp
                RETURNING id
            """, (
                customer_id, 
                interval_timestamp, 
                load_percentage,
                year, 
                month, 
                day, 
                hour, 
                minute, 
                batch_id, 
                raw_data_id, 
                datetime.now(), 
                is_valid
            ))
            
            result = cursor.fetchone()
            conn.commit()
            
            if result:
                logger.debug(f"Load data eklendi/güncellendi, id: {result[0]}")
                return result[0]
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Load data ekleme hatası: {e}")
            raise

def get_load_data_for_customer(customer_id, start_date=None, end_date=None, limit=1000):
    """Belirli bir müşteri için yük verilerini getirir"""
    with get_db_connection() as conn:
        try:
            cursor = conn.cursor()
            
            query = """
                SELECT id, customer_id, interval_timestamp, load_percentage, 
                       year, month, day, hour, minute 
                FROM load_data_detail 
                WHERE customer_id = %s AND is_valid = true
            """
            
            params = [customer_id]
            
            if start_date:
                query += " AND interval_timestamp >= %s"
                params.append(start_date)
                
            if end_date:
                query += " AND interval_timestamp <= %s"
                params.append(end_date)
                
            query += " ORDER BY interval_timestamp DESC LIMIT %s"
            params.append(limit)
            
            cursor.execute(query, params)
            return cursor.fetchall()
            
        except Exception as e:
            logger.error(f"Load data getirme hatası: {e}")
            raise

# 3. Monthly Average Consumption Tablosu Fonksiyonları
def insert_monthly_average(customer_id, year, month, avg_load, min_load, 
                          max_load, std_deviation, customer_count, record_count):
    """Aylık ortalama tüketim verilerini ekler veya günceller"""
    with get_db_connection() as conn:
        try:
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO monthly_average_consumption
                (customer_id, year, month, avg_load, min_load, max_load, 
                 std_deviation, customer_count, record_count, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (customer_id, year, month) 
                DO UPDATE SET 
                    avg_load = EXCLUDED.avg_load,
                    min_load = EXCLUDED.min_load,
                    max_load = EXCLUDED.max_load,
                    std_deviation = EXCLUDED.std_deviation,
                    customer_count = EXCLUDED.customer_count,
                    record_count = EXCLUDED.record_count,
                    last_updated = EXCLUDED.last_updated
                RETURNING id
            """, (
                customer_id,
                year,
                month,
                avg_load,
                min_load,
                max_load,
                std_deviation,
                customer_count,
                record_count,
                datetime.now()
            ))
            
            result = cursor.fetchone()
            conn.commit()
            
            if result:
                logger.info(f"Aylık ortalama eklendi/güncellendi, id: {result[0]}")
                return result[0]
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Aylık ortalama ekleme hatası: {e}")
            raise

def get_monthly_averages(customer_id=None, year=None, month=None):
    """Aylık ortalama tüketim verilerini getirir"""
    with get_db_connection() as conn:
        try:
            cursor = conn.cursor()
            
            query = """
                SELECT customer_id, year, month, avg_load, min_load, max_load, 
                       std_deviation, customer_count, record_count, last_updated 
                FROM monthly_average_consumption 
                WHERE 1=1
            """
            
            params = []
            
            if customer_id:
                query += " AND customer_id = %s"
                params.append(customer_id)
                
            if year:
                query += " AND year = %s"
                params.append(year)
                
            if month:
                query += " AND month = %s"
                params.append(month)
                
            query += " ORDER BY year DESC, month DESC"
            
            cursor.execute(query, params)
            return cursor.fetchall()
            
        except Exception as e:
            logger.error(f"Aylık ortalama getirme hatası: {e}")
            raise

# 4. Customer Features Tablosu Fonksiyonları
def insert_customer_feature(customer_id, feature_date, avg_daily_consumption, 
                           peak_hour_consumption, weekend_weekday_ratio):
    """Müşteri özelliklerini ekler veya günceller"""
    with get_db_connection() as conn:
        try:
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO customer_features
                (customer_id, feature_date, avg_daily_consumption, 
                 peak_hour_consumption, weekend_weekday_ratio, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (customer_id, feature_date) 
                DO UPDATE SET 
                    avg_daily_consumption = EXCLUDED.avg_daily_consumption,
                    peak_hour_consumption = EXCLUDED.peak_hour_consumption,
                    weekend_weekday_ratio = EXCLUDED.weekend_weekday_ratio,
                    last_updated = EXCLUDED.last_updated
            """, (
                customer_id,
                feature_date,
                avg_daily_consumption,
                peak_hour_consumption,
                weekend_weekday_ratio,
                datetime.now()
            ))
            
            conn.commit()
            logger.info(f"Müşteri özellikleri eklendi/güncellendi: {customer_id}, {feature_date}")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Müşteri özelliği ekleme hatası: {e}")
            raise

def get_customer_features(customer_id, feature_date=None):
    """Müşteri özelliklerini getirir"""
    with get_db_connection() as conn:
        try:
            cursor = conn.cursor()
            
            query = """
                SELECT customer_id, feature_date, avg_daily_consumption, 
                       peak_hour_consumption, weekend_weekday_ratio, last_updated 
                FROM customer_features 
                WHERE customer_id = %s
            """
            
            params = [customer_id]
            
            if feature_date:
                query += " AND feature_date = %s"
                params.append(feature_date)
            else:
                query += " ORDER BY feature_date DESC LIMIT 30"
                
            cursor.execute(query, params)
            return cursor.fetchall()
            
        except Exception as e:
            logger.error(f"Müşteri özellikleri getirme hatası: {e}")
            raise

# 5. Model Predictions Tablosu Fonksiyonları
def insert_model_prediction(customer_id, model_version, predicted_value, 
                           features, actual_value=None, error_metric=None):
    """Model tahminlerini kaydeder"""
    with get_db_connection() as conn:
        try:
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO model_predictions
                (prediction_timestamp, customer_id, model_version, 
                 predicted_value, actual_value, error_metric, features)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                datetime.now(),
                customer_id,
                model_version,
                predicted_value,
                actual_value,
                error_metric,
                Json(features) if isinstance(features, dict) else features
            ))
            
            result = cursor.fetchone()
            conn.commit()
            
            if result:
                logger.info(f"Model tahmini kaydedildi, id: {result[0]}")
                return result[0]
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Model tahmini kaydetme hatası: {e}")
            raise

def update_prediction_actual_value(prediction_id, actual_value, error_metric):
    """Tahmin için gerçek değeri ve hata metriğini günceller"""
    with get_db_connection() as conn:
        try:
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE model_predictions
                SET actual_value = %s, error_metric = %s
                WHERE id = %s
            """, (actual_value, error_metric, prediction_id))
            
            conn.commit()
            logger.info(f"Tahmin gerçek değeri güncellendi, id: {prediction_id}")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Tahmin güncelleme hatası: {e}")
            raise

def get_model_performance(model_version, start_date=None, end_date=None):
    """Model performans metriklerini getirir"""
    with get_db_connection() as conn:
        try:
            cursor = conn.cursor()
            
            query = """
                SELECT 
                    model_version,
                    COUNT(*) as prediction_count,
                    AVG(error_metric) as avg_error,
                    STDDEV(error_metric) as std_error,
                    MIN(error_metric) as min_error,
                    MAX(error_metric) as max_error
                FROM model_predictions
                WHERE model_version = %s
                  AND actual_value IS NOT NULL
            """
            
            params = [model_version]
            
            if start_date:
                query += " AND prediction_timestamp >= %s"
                params.append(start_date)
                
            if end_date:
                query += " AND prediction_timestamp <= %s"
                params.append(end_date)
                
            query += " GROUP BY model_version"
            
            cursor.execute(query, params)
            return cursor.fetchone()
            
        except Exception as e:
            logger.error(f"Model performansı getirme hatası: {e}")
            raise
