#!/usr/bin/env python3
"""
Günlük Enerji Tüketimi Tahmin DAG'ı
MLflow model entegrasyonu ile gerçek tahmin
"""
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

# ✅ Doğru path
SILVER_LAYER_PATH = "/home/han/projects/data-project-1.3/2-src/pipelines/4-silver_layer"
sys.path.append(SILVER_LAYER_PATH)

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'daily_energy_prediction',
    default_args=default_args,
    description='Günlük enerji tüketimi tahmini - MLflow model ile',
    schedule_interval='0 6 * * *',  # Her gün sabah 6:00
    catchup=False,
    max_active_runs=1,
    tags=['prediction', 'daily', 'energy-forecasting', 'mlflow']
)

def get_model_for_date(prediction_date):
    """Tarihe göre hangi model kullanılacağını belirle"""
    month = prediction_date.month
    
    if month in [1, 2, 3, 4, 5]:  # Ocak-Mayıs
        return "spring", "energy_spring_model"
    elif month in [6, 7, 8, 9]:  # Haziran-Eylül
        return "summer", "energy_summer_model"
    elif month in [10, 11, 12]:  # Ekim-Aralık
        return "autumn", "energy_autumn_model"
    else:
        raise ValueError(f"Geçersiz ay: {month}")

def make_daily_prediction(**context):
    """Gerçek günlük tahmin yap"""
    from common.data_extractor import create_spark_session, extract_daily_aggregated_data
    from common.feature_engineer import prepare_prediction_data
    from common.mlflow_manager import load_production_model
    from common.config import logger, get_postgres_config
    import mlflow
    import pandas as pd
    
    execution_date = context['execution_date']
    prediction_date_str = execution_date.strftime('%Y-%m-%d')
    
    logger.info(f"🔮 {prediction_date_str} için enerji tüketimi tahmini başlıyor...")
    
    spark = None
    try:
        # Model seçimi
        season, model_name = get_model_for_date(execution_date)
        logger.info(f"📊 Seçilen model: {model_name} ({season} sezonu)")
        
        # Spark session
        spark = create_spark_session("Daily Energy Prediction")
        spark.sparkContext.setLogLevel("WARN")
        
        # Tahmin için veri hazırla
        # Önceki 7 günlük veriyi al (feature engineering için)
        start_date = (execution_date - timedelta(days=7)).strftime('%Y-%m-%d')
        end_date = prediction_date_str
        
        logger.info(f"📅 Veri aralığı: {start_date} - {end_date}")
        
        # Veri çek
        daily_df = extract_daily_aggregated_data(
            spark,
            start_date=start_date,
            end_date=end_date,
            season=season
        )
        
        if daily_df is None or daily_df.count() == 0:
            logger.warning("❌ Tahmin için veri bulunamadı!")
            return False
        
        # Feature engineering
        prediction_df = prepare_prediction_data(daily_df, prediction_date_str)
        if prediction_df is None:
            logger.error("❌ Feature engineering başarısız!")
            return False
        
        # MLflow'dan model yükle
        logger.info(f"🤖 {model_name} modeli yükleniyor...")
        model = load_production_model(model_name)
        
        if model is None:
            logger.error(f"❌ {model_name} modeli yüklenemedi!")
            return False
        
        # Tahmin yap
        logger.info("🔮 Tahmin yapılıyor...")
        predictions = model.transform(prediction_df)
        
        # Sonuçları topla
        prediction_results = predictions.select(
            "customer_id", "prediction", "day_num", "hour"
        ).collect()
        
        if not prediction_results:
            logger.warning("❌ Tahmin sonucu boş!")
            return False
        
        # PostgreSQL'e kaydet
        logger.info("💾 Tahmin sonuçları kaydediliyor...")
        postgres_config = get_postgres_config()
        
        # Sonuçları DataFrame'e çevir
        results_data = []
        for row in prediction_results:
            results_data.append({
                'prediction_date': prediction_date_str,
                'customer_id': row['customer_id'],
                'predicted_consumption': float(row['prediction']),
                'day_num': int(row['day_num']),
                'hour': int(row['hour']),
                'model_used': model_name,
                'season': season,
                'created_at': datetime.now()
            })
        
        results_df = spark.createDataFrame(results_data)
        
        # PostgreSQL'e yaz
        results_df.write \
            .format("jdbc") \
            .option("url", postgres_config["url"]) \
            .option("dbtable", "daily_predictions") \
            .option("user", postgres_config["user"]) \
            .option("password", postgres_config["password"]) \
            .option("driver", postgres_config["driver"]) \
            .mode("append") \
            .save()
        
        total_predictions = len(results_data)
        avg_consumption = sum([r['predicted_consumption'] for r in results_data]) / total_predictions
        
        logger.info("✅ Tahmin tamamlandı!")
        logger.info(f"📊 Toplam tahmin: {total_predictions}")
        logger.info(f"📈 Ortalama tüketim: {avg_consumption:.2f}")
        logger.info(f"🤖 Kullanılan model: {model_name}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Tahmin hatası: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        if spark:
            spark.stop()

# Task tanımı
prediction_task = PythonOperator(
    task_id='daily_prediction',
    python_callable=make_daily_prediction,
    dag=dag
)
