#!/usr/bin/env python3
"""
KAFKA SIMPLE TEST - COUNT YAPMADAN
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def simple_kafka_read():
    """Count yapmadan Kafka okuma testi"""
    
    spark = SparkSession.builder \
        .appName("Simple_Kafka_Test") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .master("local[2]") \
        .getOrCreate()
    
    try:
        print("📨 Kafka'dan veri okuma denemesi...")
        
        kafka_df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "development-kafka1:19092") \
            .option("subscribe", "sensor-data") \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()
        
        print("✅ Kafka DataFrame oluşturuldu")
        
        # Count yerine take(1) kullan
        print("🔍 İlk mesajı almaya çalışıyor...")
        
        try:
            first_message = kafka_df.take(1)
            
            if len(first_message) > 0:
                print("✅ Mesaj bulundu!")
                
                # İlk mesajı göster
                sample_df = kafka_df.select(
                    col("value").cast("string").alias("json_data"),
                    "topic", "partition", "offset"
                ).limit(1)
                
                sample_df.show(truncate=False)
                
            else:
                print("⚠️ Kafka'da mesaj bulunamadı!")
                print("💡 Data generator çalıştırılması gerekiyor")
                
        except Exception as e:
            print(f"⚠️ Mesaj okuma hatası: {e}")
            print("💡 Muhtemelen Kafka'da veri yok")
        
        return True
        
    except Exception as e:
        print(f"❌ Kafka bağlantı hatası: {e}")
        return False
    finally:
        spark.stop()

if __name__ == "__main__":
    simple_kafka_read()
