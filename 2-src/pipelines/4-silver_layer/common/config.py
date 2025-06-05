#!/usr/bin/env python3
"""
Silver Layer Configuration
Using existing MLflow and database patterns
"""
import mlflow
import logging

# Database Configuration
JDBC_URL = "jdbc:postgresql://development-postgres:5432/datawarehouse"
CONNECTION_PROPERTIES = {
    "user": "datauser",
    "password": "datapass",
    "driver": "org.postgresql.Driver"
}

# MLflow Configuration
MLFLOW_TRACKING_URI = "http://mlflow-server:5000"
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

# Model Cache (using your pattern)
models_cache = {}
current_season = None

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Season Configuration (using your exact logic)
def get_current_season_by_month(month_num):
    """Determine season based on month number"""
    if month_num in [1, 2, 3, 4, 5]:  # Ocak-Mayıs
        return "spring"
    elif month_num in [6, 7, 8, 9]:   # Haziran-Eylül
        return "summer"
    elif month_num in [10, 11, 12]:   # Ekim-Aralık
        return "autumn"
    else:
        return "general"

# Season-Model Mapping
SEASON_MODELS = {
    "spring": "spring_energy_model",    # Ocak-Mayıs
    "summer": "summer_energy_model",    # Haziran-Eylül
    "autumn": "autumn_energy_model"     # Ekim-Aralık
}

# Feature Columns (for training consistency)
FEATURE_COLUMNS = [
    "month", "dayofweek", "year",
    "energy_lag_1", "energy_lag_2", "energy_lag_3", "energy_lag_7",
    "rolling_avg_3d", "rolling_avg_7d", "rolling_avg_30d",
    "rolling_max_7d", "rolling_min_7d",
    "month_sin", "month_cos", "dayofweek_sin", "dayofweek_cos",
    "is_weekend", "is_summer", "is_winter",
    "peak_to_avg_ratio", "energy_trend_3d", "energy_trend_7d",
    "avg_daily_load", "peak_daily_load", "customer_count"
]

TARGET_COLUMN = "total_daily_energy"

# ✅ YENİ EKLENDİ: PostgreSQL Config Function
def get_postgres_config():
    """PostgreSQL konfigürasyonunu döndür"""
    logger.info("🔗 PostgreSQL konfigürasyonu hazırlanıyor...")
    return {
        "url": JDBC_URL,
        "user": CONNECTION_PROPERTIES["user"],
        "password": CONNECTION_PROPERTIES["password"],  
        "driver": CONNECTION_PROPERTIES["driver"]
    }
