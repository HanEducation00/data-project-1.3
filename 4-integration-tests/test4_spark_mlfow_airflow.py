#!/usr/bin/env python3
"""
Test 4 - ML Inference: MLflow'dan model çek → Test verisi → Tahmin → Sonuç kaydet
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, col, round as spark_round, lit, current_timestamp
import mlflow
import mlflow.spark
from datetime import datetime
import random

# Configuration
MLFLOW_TRACKING_URI = "http://mlflow-server:5000"
EXPERIMENT_NAME = "test-experiments"
MODEL_NAME = "test-linear-regression-v1"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/datawarehouse"
POSTGRES_TABLE = "test_predictions"
POSTGRES_USER = "datauser"
POSTGRES_PASSWORD = "datapass"

def create_spark_session():
    """Spark Session with MLflow and PostgreSQL support"""
    return SparkSession.builder \
        .appName("test-ml-inference") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def load_model_from_mlflow():
    """MLflow'dan en son model'i yükle"""
    try:
        print("📊 MLflow'dan model yükleniyor...")
        
        # MLflow tracking URI
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        
        # En son model version'ını bul
        client = mlflow.MlflowClient()
        
        try:
            # Registered model'dan en son version'ı al
            latest_version = client.get_latest_versions(
                MODEL_NAME, 
                stages=["None", "Staging", "Production"]
            )[0]
            
            model_uri = f"models:/{MODEL_NAME}/{latest_version.version}"
            print(f"✅ Model bulundu: {MODEL_NAME} v{latest_version.version}")
            
        except Exception:
            # Eğer registered model yoksa, son run'dan model al
            print("🔍 Registered model bulunamadı, son experiment run'ını arıyor...")
            
            experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
            runs = mlflow.search_runs(
                experiment_ids=[experiment.experiment_id],
                order_by=["start_time DESC"],
                max_results=1
            )
            
            if len(runs) == 0:
                raise Exception("Hiç model run'ı bulunamadı!")
            
            latest_run_id = runs.iloc[0]['run_id']
            model_uri = f"runs:/{latest_run_id}/model"
            print(f"✅ Son run'dan model alındı: {latest_run_id}")
        
        # Model'i yükle
        model = mlflow.spark.load_model(model_uri)
        print(f"✅ Model başarıyla yüklendi: {model_uri}")
        
        return model
        
    except Exception as e:
        print(f"❌ Model yükleme hatası: {e}")
        raise

def generate_test_data(spark, num_samples=20):
    """Test için yeni ev verisi üret"""
    try:
        print(f"🏠 Test için {num_samples} yeni ev verisi oluşturuluyor...")
        
        # Farklı seed kullan (gerçek test verisi simülasyonu)
        test_seed = int(datetime.now().timestamp()) % 1000
        
        # Test DataFrame oluştur
        test_df = spark.range(num_samples).select(
            (col("id") + 1000).alias("house_id"),  # Test house ID'leri 1000'den başlasın
            # house_size: 60-180 m² arası (biraz farklı range)
            spark_round((rand(seed=test_seed) * 120 + 60), 1).alias("house_size"),
            # location_score: 2-9 arası 
            spark_round((rand(seed=test_seed+1) * 7 + 2), 1).alias("location_score"),
            # age_years: 0-25 yıl arası
            spark_round((rand(seed=test_seed+2) * 25), 0).alias("age_years"),
            # rooms: 2-5 oda arası
            spark_round((rand(seed=test_seed+3) * 3 + 2), 0).alias("rooms")
        )
        
        print("✅ Test verisi oluşturuldu:")
        test_df.show(10)
        
        return test_df
        
    except Exception as e:
        print(f"❌ Test verisi oluşturma hatası: {e}")
        raise

def make_predictions(spark, model, test_df):
    """Model ile tahminler yap"""
    try:
        print("🔮 Tahminler yapılıyor...")
        
        # Feature assembly (training'dekiyle aynı sıra)
        from pyspark.ml.feature import VectorAssembler
        
        assembler = VectorAssembler(
            inputCols=["house_size", "location_score", "age_years", "rooms"],
            outputCol="features"
        )
        
        # Features hazırla
        test_features = assembler.transform(test_df)
        
        # Tahminler yap
        predictions = model.transform(test_features)
        
        # Sonuçları düzenle
        result_df = predictions.select(
            "house_id",
            "house_size",
            "location_score", 
            "age_years",
            "rooms",
            spark_round("prediction", 0).alias("predicted_price"),
            current_timestamp().alias("prediction_timestamp")
        )
        
        print("✅ Tahminler tamamlandı:")
        result_df.show(10)
        
        # İstatistikler
        print("📊 Tahmin İstatistikleri:")
        from pyspark.sql.functions import min as spark_min, max as spark_max, avg as spark_avg
        
        stats = result_df.agg(
            spark_min("predicted_price").alias("min_price"),
            spark_max("predicted_price").alias("max_price"),
            spark_avg("predicted_price").alias("avg_price")
        ).collect()[0]
        
        print(f"   Min fiyat: {stats['min_price']:,.0f} TL")
        print(f"   Max fiyat: {stats['max_price']:,.0f} TL") 
        print(f"   Ortalama fiyat: {stats['avg_price']:,.0f} TL")
        
        return result_df
        
    except Exception as e:
        print(f"❌ Tahmin yapma hatası: {e}")
        raise

def create_predictions_table(spark):
    """PostgreSQL'de predictions tablosu oluştur"""
    try:
        print("📊 PostgreSQL'de predictions tablosu kontrol ediliyor...")
        
        # Test connection
        connection_properties = {
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }
        
        test_df = spark.createDataFrame([(1,)], ["test"])
        test_df.write \
            .mode("overwrite") \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "(SELECT 1 as test_connection) as test") \
            .options(**connection_properties) \
            .save()
        
        print("✅ PostgreSQL bağlantısı başarılı!")
        print("🔸 Manuel tablo oluşturmak için:")
        print(f'docker exec postgres psql -U {POSTGRES_USER} -d datawarehouse -c "')
        print(f'CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (')
        print('    house_id INTEGER,')
        print('    house_size DECIMAL(10,2),')
        print('    location_score DECIMAL(3,1),')
        print('    age_years INTEGER,')
        print('    rooms INTEGER,') 
        print('    predicted_price INTEGER,')
        print('    prediction_timestamp TIMESTAMP,')
        print('    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
        print(');"')
        
        return True
        
    except Exception as e:
        print(f"❌ PostgreSQL bağlantı hatası: {e}")
        return False

def save_predictions(predictions_df):
    """Tahminleri PostgreSQL'e kaydet"""
    try:
        print("💾 Tahminler PostgreSQL'e kaydediliyor...")
        
        predictions_df.write \
            .mode("append") \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", POSTGRES_TABLE) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .save()
        
        print(f"✅ {predictions_df.count()} tahmin PostgreSQL'e kaydedildi!")
        print(f"🔍 Kontrol etmek için:")
        print(f'docker exec postgres psql -U {POSTGRES_USER} -d datawarehouse -c "SELECT * FROM {POSTGRES_TABLE} ORDER BY prediction_timestamp DESC LIMIT 10;"')
        
        return True
        
    except Exception as e:
        print(f"❌ PostgreSQL kayıt hatası: {e}")
        print("💡 Manuel tablo oluşturmayı deneyin")
        return False

def run_ml_inference():
    """Ana ML inference fonksiyonu"""
    
    spark = None
    
    try:
        print("🔗 TEST 4 - ML INFERENCE BAŞLIYOR!")
        print("=" * 50)
        print(f"🎯 Airflow tarafından tetiklendi: {datetime.now()}")
        
        # Spark Session oluştur
        spark = create_spark_session()
        print("✅ Spark Session oluşturuldu")
        
        # 1. MLflow'dan model yükle
        model = load_model_from_mlflow()
        
        # 2. Test verisi üret
        test_df = generate_test_data(spark, num_samples=15)
        
        # 3. Tahminler yap
        predictions_df = make_predictions(spark, model, test_df)
        
        # 4. PostgreSQL tablo kontrolü
        create_predictions_table(spark)
        
        # 5. Tahminleri kaydet
        save_success = save_predictions(predictions_df)
        
        # 6. Sonuç özeti
        print("🎉 TEST 4 TAMAMLANDI!")
        print("✅ MLflow'dan model yüklendi")
        print(f"✅ {test_df.count()} test verisi işlendi")
        print(f"✅ Tahminler yapıldı")
        if save_success:
            print("✅ Sonuçlar PostgreSQL'e kaydedildi")
        else:
            print("⚠️  PostgreSQL kaydı başarısız (tablo eksik olabilir)")
        
        # Final stats
        print("\n📊 FINAL İSTATİSTİKLER:")
        from pyspark.sql.functions import min as spark_min, max as spark_max, avg as spark_avg
        
        predictions_df.agg(
            spark_min("predicted_price").alias("min_price"),
            spark_max("predicted_price").alias("max_price"),
            spark_avg("predicted_price").alias("avg_price")
        ).show()
        
        return True
        
    except Exception as e:
        print(f"❌ ML INFERENCE HATASI: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    success = run_ml_inference()
    exit(0 if success else 1)