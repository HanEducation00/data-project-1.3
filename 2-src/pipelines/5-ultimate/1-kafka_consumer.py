#!/usr/bin/env python3
"""
KAFKA CONSUMER - DOCKER NETWORK İÇİN FİX
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    """Docker network için Spark session"""
    return SparkSession.builder \
        .appName("Seasonal_Energy_Kafka_Consumer") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .config("spark.sql.adaptive.enabled", "true") \
        .master("local[2]") \
        .getOrCreate()

def setup_kafka_consumer(spark, topic_name="sensor-data"):
    """Docker network içinde Kafka consumer"""
    
    kafka_config = {
        # ✅ FIX: Docker network isimleri
        "kafka.bootstrap.servers": "development-kafka1:19092,development-kafka2:29092,development-kafka3:39092",
        "subscribe": topic_name,
        "startingOffsets": "earliest",
        "endingOffsets": "latest",
        "failOnDataLoss": "false"
    }
    
    print(f"📨 Kafka'ya bağlanıyor: {topic_name}")
    print(f"🔗 Brokers: {kafka_config['kafka.bootstrap.servers']}")
    
    try:
        kafka_df = spark.read \
            .format("kafka") \
            .options(**kafka_config) \
            .load()
        
        print(f"✅ Kafka consumer hazır!")
        return kafka_df
        
    except Exception as e:
        print(f"❌ Kafka bağlantı hatası: {e}")
        raise

def simple_kafka_test():
    """Basit Kafka bağlantı testi"""
    
    print("🧪 KAFKA BAĞLANTI TESTİ")
    print("="*40)
    
    spark = None
    try:
        spark = create_spark_session()
        print("✅ Spark session oluşturuldu")
        
        kafka_df = setup_kafka_consumer(spark, "sensor-data")
        
        # Sadece schema kontrolü (count yapmadan)
        print("📋 Kafka DataFrame schema:")
        kafka_df.printSchema()
        
        print("✅ Kafka bağlantı testi başarılı!")
        return True
        
    except Exception as e:
        print(f"❌ Test başarısız: {e}")
        return False
        
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    simple_kafka_test()
