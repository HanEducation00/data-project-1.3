#!/usr/bin/env python3
"""
Test 1 - Spark Kafka Producer: 5 örnek ev verisi gönder
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, lit
from datetime import datetime
import time

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka1:9092,kafka2:9092,kafka3:9092"
KAFKA_TOPIC = "test-house-data"

def create_spark_session():
    """Spark Session with Kafka support"""
    return SparkSession.builder \
        .appName("test-kafka-producer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()

def create_sample_house_data(spark):
    """5 örnek ev verisi DataFrame olarak oluştur"""
    
    current_time = datetime.now().isoformat()
    
    sample_data = [
        (1, 85.0, 7.2, 320000, current_time),
        (2, 120.5, 8.5, 450000, current_time),
        (3, 95.8, 6.8, 380000, current_time),
        (4, 150.2, 9.1, 620000, current_time),
        (5, 75.5, 6.0, 280000, current_time)
    ]
    
    schema = ["house_id", "house_size", "location_score", "price", "timestamp"]
    
    return spark.createDataFrame(sample_data, schema)

def send_data_to_kafka():
    """Spark ile verileri Kafka'ya gönder"""
    
    spark = None
    
    try:
        print("🔗 TEST 1 - SPARK KAFKA PRODUCER BAŞLIYOR!")
        print("=" * 50)
        
        # Spark Session oluştur
        spark = create_spark_session()
        print("✅ Spark Session oluşturuldu")
        
        print(f"📊 Kafka Topic: {KAFKA_TOPIC}")
        print(f"📊 Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
        
        # Sample data oluştur
        df = create_sample_house_data(spark)
        
        print("✅ Sample data oluşturuldu:")
        df.show()
        
        # DataFrame'i Kafka formatına çevir
        kafka_df = df.select(
            df.house_id.cast("string").alias("key"),  # Key olarak house_id
            to_json(struct("*")).alias("value")       # Tüm kolonları JSON'a
        )
        
        print("✅ Kafka formatına çevrildi:")
        kafka_df.show(truncate=False)
        
        # Kafka'ya yaz
        kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", KAFKA_TOPIC) \
            .save()
        
        print("🎉 KAFKA PRODUCER TEST TAMAMLANDI!")
        print(f"✅ {df.count()} ev verisi başarıyla gönderildi")
        print()
        print("🔍 Mesajları kontrol etmek için:")
        print(f"docker exec kafka1 /kafka/bin/kafka-console-consumer.sh \\")
        print(f"    --bootstrap-server localhost:9092 \\")
        print(f"    --topic {KAFKA_TOPIC} \\")
        print(f"    --from-beginning --max-messages 5")
        
        return True
        
    except Exception as e:
        print(f"❌ SPARK KAFKA PRODUCER HATASI: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    success = send_data_to_kafka()
    exit(0 if success else 1)