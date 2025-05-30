#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ML Model Development Pipeline Konfigürasyonu
PostgreSQL'den veri okuyup ML model geliştiren ve MLflow'a kaydeden pipeline için konfigürasyon.
"""

# PostgreSQL konfigürasyonu (AYNI KALDI)
POSTGRES_CONFIG = {
    "host": "postgres",
    "port": "5432", 
    "database": "datawarehouse",
    "user": "datauser",
    "password": "datapass",
    "driver": "org.postgresql.Driver"
}

# PostgreSQL JDBC URL (AYNI KALDI)
JDBC_URL = f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"

# PostgreSQL bağlantı özellikleri (AYNI KALDI)
JDBC_PROPERTIES = {
    "user": POSTGRES_CONFIG["user"],
    "password": POSTGRES_CONFIG["password"],
    "driver": POSTGRES_CONFIG["driver"]
}

# Spark konfigürasyonu (APP NAME GÜNCELLENDİ)
SPARK_CONFIG = {
    "app_name": "ML Model Development Pipeline",
    "master": "local[*]",
    "log_level": "INFO",
    "packages": "org.postgresql:postgresql:42.6.0"
}

# =============================================================================
# 🆕 MLflow KONFIGÜRASYONU
# =============================================================================

# MLflow tracking konfigürasyonu
MLFLOW_CONFIG = {
    "tracking_uri": "sqlite:///mlflow.db",  # Local SQLite database
    "artifact_root": "./mlflow_artifacts",   # Model artifacts klasörü
    "experiment_name": "electricity_load_forecasting",
    "run_name_prefix": "aggregate_forecasting"
}

# Model konfigürasyonu
MODEL_CONFIG = {
    "model_name": "aggregate_load_forecasting",
    "model_type": "lightgbm",
    "target_variable": "total_load_mw",
    "prediction_horizon": "1_hour",  # 1 saat sonrasını tahmin
    "aggregation_level": "hourly"     # Saatlik agregasyon
}

# Experiment tracking konfigürasyonu
EXPERIMENT_CONFIG = {
    "track_metrics": ["mae", "mape", "rmse", "r2"],
    "track_params": ["n_estimators", "learning_rate", "max_depth", "num_leaves"],
    "track_artifacts": ["feature_importance", "residual_plots", "prediction_plots"],
    "auto_log": True  # MLflow autolog aktif
}

# =============================================================================
# MEVCUT KONFIGÜRASYONLAR (AYNI KALDI)
# =============================================================================

# Veri dosyaları konfigürasyonu
DATA_CONFIG = {
    "file_pattern": "cyme_load_timeseries_day_{}.txt"
}

# Veritabanı tablo ismi
TABLE_NAME = "raw_load_data"

# Batch işleme konfigürasyonu
BATCH_CONFIG = {
    "batch_size": 10000,
    "repartition_count": 4
}

# Örneklem günlerini hesapla
def get_sample_days():
    """Her aydan 8 gün seç (4 günde bir)"""
    sample_days = []
    month_starts = [1, 32, 61, 92, 122, 153, 183, 214, 245, 275, 306, 336]
    
    for start_day in month_starts:
        month_days = list(range(start_day, start_day + 30, 4))[:8]
        sample_days.extend(month_days)
    
    return sample_days

# Sample days'i config'e ekle
DATA_CONFIG["sample_days"] = get_sample_days()

# =============================================================================
# 🆕 ML PIPELINE KONFIGÜRASYONU
# =============================================================================

# Feature engineering konfigürasyonu
FEATURE_CONFIG = {
    "cyclical_features": ["hour", "month", "dayofweek"],
    "lag_features": [1, 4, 24, 168],  # 15min, 1h, 1day, 1week (intervals)
    "rolling_windows": [4, 24, 168],   # Rolling statistics windows
    "target_encoding_features": ["customer_profile_type"]
}

# Model training konfigürasyonu  
TRAINING_CONFIG = {
    "test_size": 0.2,
    "validation_split": 0.1,
    "random_state": 42,
    "time_based_split": True  # Chronological split
}

# LightGBM hyperparameters
LIGHTGBM_PARAMS = {
    "objective": "regression",
    "metric": "mae",
    "boosting_type": "gbdt",
    "num_leaves": 31,
    "learning_rate": 0.05,
    "feature_fraction": 0.9,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "verbose": 0,
    "random_state": 42
}
