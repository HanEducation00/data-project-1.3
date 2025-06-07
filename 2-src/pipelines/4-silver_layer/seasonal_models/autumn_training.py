#!/usr/bin/env python3
"""
Sonbahar Sezonu (Ekim-Aralık) ML Model Eğitimi
Autumn Season Energy Consumption Model Training
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from common.data_extractor import create_spark_session, extract_daily_aggregated_data, filter_seasonal_data
from common.feature_engineer import prepare_training_data
from common.mlflow_manager import register_model
from common.config import logger, FEATURE_COLUMNS, TARGET_COLUMN, SEASON_MODELS

# 🎯 TRACKING URI EKLEME - SPRING VE SUMMER GİBİ!
import mlflow
import mlflow.spark
mlflow.set_tracking_uri("http://mlflow-server:5000")

def train_autumn_model(target_year=2016):
    """Sonbahar sezonu modeli eğit (Ekim-Aralık)"""
    logger.info("🍂 Sonbahar sezonu model eğitimi başlıyor...")
    
    spark = create_spark_session("Autumn Model Training")
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Sonbahar verisi çek
        start_date = f"{target_year}-10-01"
        end_date = f"{target_year}-12-31"
        
        daily_df = extract_daily_aggregated_data(
            spark,
            start_date=start_date,
            end_date=end_date,
            season="autumn"
        )
        
        if daily_df is None or daily_df.count() == 0:
            logger.error("❌ Sonbahar sezonu verisi bulunamadı!")
            return False
        
        # Mevsimsel filtreleme
        autumn_df = filter_seasonal_data(daily_df, "autumn")
        if autumn_df is None:
            return False
        
        # Feature engineering
        training_df = prepare_training_data(autumn_df)
        if training_df is None:
            return False
        
        # Model pipeline
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
        mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
        r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
        
        metrics = {
            "rmse": rmse,
            "mae": mae,
            "r2": r2,
            "training_records": train_df.count(),
            "test_records": test_df.count(),
            "season": "autumn",
            "data_period": f"{start_date} to {end_date}"
        }
        
        logger.info("📊 Sonbahar sezonu model performansı:")
        logger.info(f"  RMSE: {rmse:.4f}")
        logger.info(f"  MAE: {mae:.4f}")
        logger.info(f"  R²: {r2:.4f}")
        
        # ✅ SPRING VE SUMMER GİBİ EXPERIMENT OLUŞTUR/SEÇ + DIRECT MLFLOW
        mlflow.set_experiment("seasonal-energy-models")
        mlflow.start_run(run_name=f"autumn_energy_model_{target_year}")
        mlflow.log_metrics({
            "rmse": float(rmse),
            "mae": float(mae),
            "r2": float(r2),
            "training_records": int(train_df.count()),
            "test_records": int(test_df.count())
        })
        mlflow.log_params({
            "num_trees": 100,
            "max_depth": 10,
            "target_year": target_year,
            "season": "autumn",
            "spark_mode": "pipeline",
            "data_period": f"{start_date} to {end_date}"
        })
        mlflow.spark.log_model(model, "model")
        mlflow.end_run()
        
        logger.info("✅ Model MLflow'a kaydedildi!")
        
        # Feature importance göster
        rf_model = model.stages[-1]
        feature_importance = rf_model.featureImportances.toArray()
        
        logger.info("🎯 En önemli özellikler:")
        for i, importance in enumerate(feature_importance[:5]):
            if i < len(FEATURE_COLUMNS):
                logger.info(f"  {FEATURE_COLUMNS[i]}: {importance:.4f}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Sonbahar model eğitiminde hata: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        spark.stop()

if __name__ == "__main__":
    success = train_autumn_model(2016)
    
    if success:
        print("🍂 Sonbahar sezonu model eğitimi tamamlandı!")
    else:
        print("❌ Sonbahar sezonu model eğitimi başarısız!")
        sys.exit(1)
