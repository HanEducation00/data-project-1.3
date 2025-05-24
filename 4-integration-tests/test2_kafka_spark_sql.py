#!/usr/bin/env python3
"""
Test 2 - Spark Consumer: Kafka'dan oku → PostgreSQL'e yaz
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka1:9092,kafka2:9092,kafka3:9092"
KAFKA_TOPIC = "test-house-data"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/datawarehouse"
POSTGRES_TABLE = "test_house_data"
POSTGRES_USER = "datauser"
POSTGRES_PASSWORD = "datapass"

def create_spark_session():
    """Spark Session with Kafka and PostgreSQL support"""
    return SparkSession.builder \
        .appName("test-kafka-to-postgres") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.7.0") \
        .getOrCreate()

def define_house_schema():
    """House data JSON schema tanımla"""
    return StructType([
        StructField("house_id", IntegerType(), True),
        StructField("house_size", DoubleType(), True),
        StructField("location_score", DoubleType(), True),
        StructField("price", IntegerType(), True),
        StructField("timestamp", StringType(), True)
    ])

def create_postgres_table(spark):
    """PostgreSQL'de test tablosu oluştur"""
    try:
        print("📊 PostgreSQL'de tablo oluşturuluyor...")
        
        # SQL komutu
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS test_house_data (
            house_id INTEGER,
            house_size DECIMAL(10,2),
            location_score DECIMAL(3,1),
            price INTEGER,
            timestamp TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (house_id)
        )
        """
        
        # JDBC connection ile tablo oluştur
        connection_properties = {
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }
        
        # Dummy DataFrame oluştur ve connection test et
        test_df = spark.createDataFrame([(1,)], ["test"])
        
        # Connection test
        test_df.write \
            .mode("overwrite") \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "(SELECT 1 as test_connection) as test") \
            .options(**connection_properties) \
            .save()
        
        print("✅ PostgreSQL bağlantısı başarılı!")
        
        # Manuel olarak tablo oluşturma SQL'i göster
        print("🔸 Manuel tablo oluşturmak için:")
        print("docker exec postgres psql -U datauser -d datawarehouse -c \"" + create_table_sql.replace('\n', ' ').strip() + "\"")
        
        return True
        
    except Exception as e:
        print(f"❌ PostgreSQL tablo oluşturma hatası: {e}")
        print("🔸 Manuel olarak tabloyu oluşturun:")
        print("docker exec postgres psql -U datauser -d datawarehouse")
        return False

def read_from_kafka_and_save_to_postgres():
    """Kafka'dan oku ve PostgreSQL'e kaydet"""
    
    spark = None
    
    try:
        print("🔗 TEST 2 - SPARK CONSUMER BAŞLIYOR!")
        print("=" * 50)
        
        # Spark Session oluştur
        spark = create_spark_session()
        print("✅ Spark Session oluşturuldu")
        
        print(f"📊 Kafka Topic: {KAFKA_TOPIC}")
        print(f"📊 PostgreSQL Table: {POSTGRES_TABLE}")
        
        # PostgreSQL tablo kontrolü
        create_postgres_table(spark)
        
        # House data schema
        house_schema = define_house_schema()
        
        # Kafka'dan batch olarak oku
        print("📊 Kafka'dan veriler okunuyor...")
        
        kafka_df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "earliest") \
            .load()
        
        print(f"✅ Kafka'dan {kafka_df.count()} mesaj okundu")
        
        # JSON parsing
        parsed_df = kafka_df.select(
            col("key").cast("string").alias("kafka_key"),
            from_json(col("value").cast("string"), house_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select(
            col("kafka_key"),
            col("data.house_id"),
            col("data.house_size"),
            col("data.location_score"),
            col("data.price"),
            col("data.timestamp").alias("house_timestamp"),
            col("kafka_timestamp")
        )
        
        print("✅ JSON parsing tamamlandı:")
        parsed_df.show()
        
        # PostgreSQL'e kaydet
        print("📊 PostgreSQL'e kaydediliyor...")
        
        # Sadece house data kolonlarını seç ve timestamp'i düzelt
        house_df = parsed_df.select(
            "house_id",
            "house_size", 
            "location_score",
            "price",
            to_timestamp(col("house_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS").alias("timestamp")
        )
        
        house_df.write \
            .mode("append") \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", POSTGRES_TABLE) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .save()
        
        print("🎉 TEST 2 TAMAMLANDI!")
        print(f"✅ {house_df.count()} ev verisi PostgreSQL'e kaydedildi")
        print()
        print("🔍 PostgreSQL'de kontrol etmek için:")
        print("docker exec postgres psql -U datauser -d datawarehouse -c \"SELECT * FROM test_house_data;\"")
        
        return True
        
    except Exception as e:
        print(f"❌ SPARK CONSUMER HATASI: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    success = read_from_kafka_and_save_to_postgres()
    exit(0 if success else 1)