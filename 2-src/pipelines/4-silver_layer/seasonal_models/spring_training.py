#!/usr/bin/env python3
"""
Bahar Sezonu (Ocak-Mayıs) ML Model Eğitimi
Spring Season Energy Consumption Model Training
Spark Client'te Local Mode ile Çalışacak Versiyon
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import max as spark_max, col, avg, min as spark_min, count
from datetime import datetime

import mlflow
import mlflow.spark

# 🎯 TRACKING URI EKLEME - SUMMER GİBİ!
mlflow.set_tracking_uri("http://mlflow-server:5000")

# ✅ BRONZ LAYER GİBİ LOCAL MODE - TÜM CORE'LARI KULLAN
spark = SparkSession.builder \
    .appName("Spring Season Model Training") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Loglama seviyesini ayarla
spark.sparkContext.setLogLevel("WARN")

# PostgreSQL bağlantı bilgileri
jdbc_url = "jdbc:postgresql://development-postgres:5432/datawarehouse"
connection_properties = {
    "user": "datauser",
    "password": "datapass",
    "driver": "org.postgresql.Driver"
}

def check_data_availability(target_year=2016):
    """31 Mayıs tarihine kadar veri geldi mi kontrol et"""
    print("🔍 31 Mayıs tarihine kadar veri kontrolü...")
    
    try:
        # Son veri tarihini çek
        max_date_df = spark.read \
            .jdbc(
                url=jdbc_url,
                table="kafka_raw_data",
                properties=connection_properties
            ) \
            .select(spark_max("full_timestamp").alias("max_date"))
        
        max_date_result = max_date_df.collect()[0]["max_date"]
        
        if max_date_result is None:
            print("❌ Hiç veri bulunamadı!")
            return False
            
        # 31 Mayıs kontrolü
        target_end_date = f"{target_year}-05-31"
        max_date_str = max_date_result.strftime("%Y-%m-%d")
        
        print(f"📅 Son veri tarihi: {max_date_str}")
        print(f"🎯 Hedef son tarih: {target_end_date}")
        
        if max_date_str >= target_end_date:
            print("✅ 31 Mayıs tarihine kadar veri mevcut!")
            return True
        else:
            print(f"⏳ Henüz 31 Mayıs verisi gelmedi. Son: {max_date_str}")
            return False
            
    except Exception as e:
        print(f"❌ Veri kontrol hatası: {e}")
        return False

def extract_spring_data(target_year=2016):
    """Bahar sezonu verisi çek"""
    print(f"📊 {target_year} yılı bahar verisi çekiliyor...")
    
    try:
        # Bahar sezonu: Ocak-Mayıs (1-5. aylar)
        query = f"""
        SELECT 
            DATE(full_timestamp) as date,
            AVG(load_percentage) as avg_load_percentage,
            MIN(load_percentage) as min_load_percentage,
            MAX(load_percentage) as max_load_percentage,
            COUNT(*) as record_count,
            EXTRACT(MONTH FROM full_timestamp) as month,
            EXTRACT(DAY FROM full_timestamp) as day,
            EXTRACT(DOW FROM full_timestamp) as day_of_week
        FROM kafka_raw_data 
        WHERE EXTRACT(YEAR FROM full_timestamp) = {target_year}
        AND EXTRACT(MONTH FROM full_timestamp) BETWEEN 1 AND 5
        GROUP BY DATE(full_timestamp), 
                 EXTRACT(MONTH FROM full_timestamp),
                 EXTRACT(DAY FROM full_timestamp),
                 EXTRACT(DOW FROM full_timestamp)
        ORDER BY date
        """
        
        daily_df = spark.read \
            .jdbc(
                url=jdbc_url,
                table=f"({query}) as spring_data",
                properties=connection_properties
            )
        
        count = daily_df.count()
        print(f"📈 Toplam {count} günlük kayıt bulundu")
        
        if count == 0:
            print("❌ Bahar sezonu verisi bulunamadı!")
            return None
            
        return daily_df
        
    except Exception as e:
        print(f"❌ Veri çekme hatası: {e}")
        return None

def prepare_features(df):
    """Feature engineering"""
    print("🔧 Feature engineering yapılıyor...")
    
    try:
        # Basit feature'lar ekle
        feature_df = df.withColumn("month_sin", 
                                 col("month") * 3.14159 / 6) \
                      .withColumn("day_sin", 
                                 col("day") * 3.14159 / 15) \
                      .withColumn("is_weekend", 
                                 (col("day_of_week").isin([0, 6])).cast("int"))
        
        # Feature kolonları
        feature_cols = ["month", "day", "day_of_week", "month_sin", "day_sin", "is_weekend"]
        target_col = "avg_load_percentage"
        
        # VectorAssembler
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        feature_df = assembler.transform(feature_df)
        
        print("✅ Feature engineering tamamlandı")
        return feature_df, target_col
        
    except Exception as e:
        print(f"❌ Feature engineering hatası: {e}")
        return None, None

def train_model(feature_df, target_col, model_name="spring_energy_model", target_year=2016):
    """Model eğitimi"""
    print("🏃‍♂️ Model eğitimi başlıyor...")
    
    try:
        # Train/test split
        train_df, test_df = feature_df.randomSplit([0.8, 0.2], seed=42)
        
        print(f"🎯 Eğitim verisi: {train_df.count()} kayıt")
        print(f"🧪 Test verisi: {test_df.count()} kayıt")
        
        # Random Forest modeli
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol=target_col,
            numTrees=50,
            maxDepth=8,
            seed=42
        )
        
        # Model eğitimi
        model = rf.fit(train_df)
        
        # Tahmin
        predictions = model.transform(test_df)
        
        # Değerlendirme
        evaluator = RegressionEvaluator(
            labelCol=target_col,
            predictionCol="prediction"
        )
        
        rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
        mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
        r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
        
        print("📊 Model performansı:")
        print(f"  RMSE: {rmse:.4f}")
        print(f"  MAE: {mae:.4f}")
        print(f"  R²: {r2:.4f}")
        
        # Örnek tahminler
        print("🔮 Örnek tahminler:")
        predictions.select("date", target_col, "prediction").show(10)
        
        # ✅ SUMMER GİBİ EXPERIMENT OLUŞTUR/SEÇ
        mlflow.set_experiment("seasonal-energy-models")
        mlflow.start_run(run_name=f"{model_name}_{target_year}")
        mlflow.log_metrics({
            "rmse": float(rmse),
            "mae": float(mae),
            "r2": float(r2),
            "training_records": int(train_df.count()),
            "test_records": int(test_df.count())
        })
        mlflow.log_params({
            "num_trees": 50,
            "max_depth": 8,
            "target_year": target_year,
            "season": "spring",
            "spark_mode": "local[*]"
        })
        mlflow.spark.log_model(model, "model")
        mlflow.end_run()
        
        print("✅ Model MLflow'a kaydedildi!")
        return True
        
    except Exception as e:
        print(f"❌ Model eğitim hatası: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Ana fonksiyon"""
    target_year = int(sys.argv[1]) if len(sys.argv) > 1 else 2016
    model_name = "spring_energy_model"
    
    print("🌸 Bahar sezonu model eğitimi başlıyor...")
    print(f"🔧 Spark Mode: Local[*] (Tüm core'lar kullanılacak)")
    print(f"📍 Çalışma Ortamı: Spark Client Container")
    print(f"🎯 Hedef Yıl: {target_year}")
    print(f"🏷️ Model Adı: {model_name}")
    
    try:
        # 1. Veri uygunluk kontrolü
        if not check_data_availability(target_year):
            print("⏳ Veri henüz hazır değil, model eğitimi atlanıyor...")
            sys.exit(0)
        
        # 2. Veri çekme
        daily_df = extract_spring_data(target_year)
        if daily_df is None:
            sys.exit(1)
        
        # 3. Feature engineering
        feature_df, target_col = prepare_features(daily_df)
        if feature_df is None:
            sys.exit(1)
        
        # 4. Model eğitimi
        success = train_model(feature_df, target_col, model_name, target_year)
        
        if success:
            print("🌸 Bahar sezonu model eğitimi tamamlandı!")
            print("📍 Model eğitimi Spark Client'te başarıyla tamamlandı")
        else:
            print("❌ Bahar sezonu model eğitimi başarısız!")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Genel hata: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
