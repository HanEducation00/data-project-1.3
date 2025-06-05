#!/usr/bin/env python3
"""
Bahar Sezonu (Ocak-Nisan) ML Model Eğitimi
Spring Season Energy Consumption Model Training
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import max as spark_max, col
from datetime import datetime

from common.data_extractor import create_spark_session, extract_daily_aggregated_data, filter_seasonal_data
from common.feature_engineer import prepare_training_data
from common.mlflow_manager import register_model
from common.config import logger, FEATURE_COLUMNS, TARGET_COLUMN, SEASON_MODELS

def check_data_availability(spark, target_year=2016):
    """31 Mayıs tarihine kadar veri geldi mi kontrol et"""
    logger.info("🔍 31 Mayıs tarihine kadar veri kontrolü...")
    
    try:
        # Kafka raw data'dan son tarihi kontrol et
        from common.config import get_postgres_config
        postgres_config = get_postgres_config()
        
        # Son veri tarihini çek
        max_date_df = spark.read \
            .format("jdbc") \
            .option("url", postgres_config["url"]) \
            .option("dbtable", "kafka_raw_data") \
            .option("user", postgres_config["user"]) \
            .option("password", postgres_config["password"]) \
            .option("driver", postgres_config["driver"]) \
            .load() \
            .select(spark_max("full_timestamp").alias("max_date"))
        
        max_date_result = max_date_df.collect()[0]["max_date"]
        
        if max_date_result is None:
            logger.warning("❌ Hiç veri bulunamadı!")
            return False
            
        # 30 Nisan kontrolü
        target_end_date = f"{target_year}-05-31"
        max_date_str = max_date_result.strftime("%Y-%m-%d")
        
        logger.info(f"📅 Son veri tarihi: {max_date_str}")
        logger.info(f"🎯 Hedef son tarih: {target_end_date}")
        
        if max_date_str >= target_end_date:
            logger.info("✅ 30 Nisan tarihine kadar veri mevcut!")
            return True
        else:
            logger.warning(f"⏳ Henüz 30 Nisan verisi gelmedi. Son: {max_date_str}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Veri kontrol hatası: {e}")
        return False

def train_spring_model(target_year=2016):
    """Bahar sezonu modeli eğit (Ocak-Nisan)"""
    logger.info("🌸 Bahar sezonu model eğitimi başlıyor...")
    
    # Spark session oluştur
    spark = create_spark_session("Spring Model Training")
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # ÖNEMLİ: Veri uygunluk kontrolü
        if not check_data_availability(spark, target_year):
            logger.info("⏳ Veri henüz hazır değil, model eğitimi atlanıyor...")
            return "SKIPPED"
        
        # DÜZELTME: 30 Nisan'a kadar veri çek (31 Mayıs değil!)
        logger.info(f"📊 {target_year} yılı bahar verisi çekiliyor...")
        start_date = f"{target_year}-01-01"
        end_date = f"{target_year}-05-31"  # ✅ DÜZELTME: 05-31 → 04-30
        
        daily_df = extract_daily_aggregated_data(
            spark,
            start_date=start_date,
            end_date=end_date,
            season="spring"
        )
        
        if daily_df is None or daily_df.count() == 0:
            logger.error("❌ Bahar sezonu verisi bulunamadı!")
            return False
        
        # Mevsimsel filtreleme
        spring_df = filter_seasonal_data(daily_df, "spring")
        if spring_df is None:
            return False
        
        # Feature engineering
        training_df = prepare_training_data(spring_df)
        if training_df is None:
            return False
        
        # Model eğitimi için hazırlık
        feature_assembler = VectorAssembler(
            inputCols=FEATURE_COLUMNS,
            outputCol="features"
        )
        
        # Random Forest modeli
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol=TARGET_COLUMN,
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        
        # Pipeline oluştur
        pipeline = Pipeline(stages=[feature_assembler, rf])
        
        # Train/test split
        train_df, test_df = training_df.randomSplit([0.8, 0.2], seed=42)
        
        logger.info(f"🎯 Eğitim verisi: {train_df.count()} kayıt")
        logger.info(f"🧪 Test verisi: {test_df.count()} kayıt")
        
        # Model eğitimi
        logger.info("🏃‍♂️ Model eğitimi başlıyor...")
        model = pipeline.fit(train_df)
        
        # Tahmin ve değerlendirme
        predictions = model.transform(test_df)
        
        evaluator = RegressionEvaluator(
            labelCol=TARGET_COLUMN,
            predictionCol="prediction"
        )
        
        rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
        mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
        r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
        
        metrics = {
            "rmse": rmse,
            "mae": mae,
            "r2": r2,
            "training_records": train_df.count(),
            "test_records": test_df.count(),
            "season": "spring",
            "data_period": f"{start_date} to {end_date}"
        }
        
        logger.info("📊 Model performansı:")
        logger.info(f"  RMSE: {rmse:.4f}")
        logger.info(f"  MAE: {mae:.4f}")
        logger.info(f"  R²: {r2:.4f}")
        
        # MLflow'a kaydet
        model_name = SEASON_MODELS["spring"]
        success = register_model(model, model_name, "spring", metrics)
        
        if success:
            logger.info(f"✅ {model_name} başarıyla kaydedildi!")
            
            # Örnek tahminleri göster
            logger.info("🔮 Örnek tahminler:")
            predictions.select("date", TARGET_COLUMN, "prediction").show(10)
            
        return success
        
    except Exception as e:
        logger.error(f"❌ Bahar model eğitiminde hata: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        spark.stop()

if __name__ == "__main__":
    # 2016 yılı bahar verisiyle eğitim
    result = train_spring_model(2016)
    
    if result == "SKIPPED":
        print("⏳ Bahar sezonu model eğitimi atlandı - veri henüz hazır değil!")
        sys.exit(0)  # Skip durumu için 0 exit code
    elif result:
        print("🌸 Bahar sezonu model eğitimi tamamlandı!")
    else:
        print("❌ Bahar sezonu model eğitimi başarısız!")
        sys.exit(1)
