#!/usr/bin/env python3
"""
Yaz Sezonu (Haziran-Eylül) ML Model Eğitimi
Summer Season Energy Consumption Model Training
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import max as spark_max, col

from common.data_extractor import create_spark_session, extract_daily_aggregated_data, filter_seasonal_data
from common.feature_engineer import prepare_training_data
from common.mlflow_manager import register_model
from common.config import logger, FEATURE_COLUMNS, TARGET_COLUMN, SEASON_MODELS

def check_data_availability(spark, target_year=2016):
    """30 Eylül tarihine kadar veri geldi mi kontrol et"""
    logger.info("🔍 30 Eylül tarihine kadar veri kontrolü...")
    
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
            
        # 30 Eylül kontrolü
        target_end_date = f"{target_year}-09-30"
        max_date_str = max_date_result.strftime("%Y-%m-%d")
        
        logger.info(f"📅 Son veri tarihi: {max_date_str}")
        logger.info(f"🎯 Hedef son tarih: {target_end_date}")
        
        if max_date_str >= target_end_date:
            logger.info("✅ 30 Eylül tarihine kadar veri mevcut!")
            return True
        else:
            logger.warning(f"⏳ Henüz 30 Eylül verisi gelmedi. Son: {max_date_str}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Veri kontrol hatası: {e}")
        return False

def train_summer_model(target_year=2016):
    """Yaz sezonu modeli eğit (Haziran-Eylül)"""
    logger.info("☀️ Yaz sezonu model eğitimi başlıyor...")
    
    spark = create_spark_session("Summer Model Training")
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # ÖNEMLİ: Veri uygunluk kontrolü
        if not check_data_availability(spark, target_year):
            logger.info("⏳ Veri henüz hazır değil, model eğitimi atlanıyor...")
            return "SKIPPED"
        
        # Yaz verisi çek
        start_date = f"{target_year}-06-01"
        end_date = f"{target_year}-09-30"
        
        daily_df = extract_daily_aggregated_data(
            spark,
            start_date=start_date,
            end_date=end_date,
            season="summer"
        )
        
        if daily_df is None or daily_df.count() == 0:
            logger.error("❌ Yaz sezonu verisi bulunamadı!")
            return False
        
        # Mevsimsel filtreleme
        summer_df = filter_seasonal_data(daily_df, "summer")
        if summer_df is None:
            return False
        
        # Feature engineering
        training_df = prepare_training_data(summer_df)
        if training_df is None:
            return False
        
        # Model pipeline (aynı yapı)
        feature_assembler = VectorAssembler(
            inputCols=FEATURE_COLUMNS,
            outputCol="features"
        )
        
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol=TARGET_COLUMN,
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        
        pipeline = Pipeline(stages=[feature_assembler, rf])
        
        # Train/test split
        train_df, test_df = training_df.randomSplit([0.8, 0.2], seed=42)
        
        logger.info(f"🎯 Eğitim verisi: {train_df.count()} kayıt")
        logger.info(f"🧪 Test verisi: {test_df.count()} kayıt")
        
        # Model eğitimi
        logger.info("🏃‍♂️ Model eğitimi başlıyor...")
        model = pipeline.fit(train_df)
        
        # Değerlendirme
        predictions = model.transform(test_df)
        evaluator = RegressionEvaluator(labelCol=TARGET_COLUMN, predictionCol="prediction")
        
        rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
        mae = evaluator.evaluate(predictions, {evaluator.metric
