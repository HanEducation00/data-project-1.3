#!/usr/bin/env python3
"""
MLflow Manager for Silver Layer
Using existing model management patterns
"""
import mlflow
import mlflow.spark
from datetime import datetime
from .config import logger, models_cache, current_season, SEASON_MODELS

def check_model_exists(model_name):
    """Check if model exists in MLflow"""
    try:
        client = mlflow.tracking.MlflowClient()
        model_versions = client.get_latest_versions(model_name)
        return len(model_versions) > 0
    except Exception as e:
        logger.debug(f"Model {model_name} check failed: {e}")
        return False

def get_available_model(month_num=None):
    """Smart model selection based on season"""
    global current_season
    
    # Use current month if not specified
    if month_num is None:
        month_num = datetime.now().month
    
    from .config import get_current_season_by_month
    current_season = get_current_season_by_month(month_num)
    logger.info(f"📅 Season for month {month_num}: {current_season}")
    
    # Try seasonal model first
    seasonal_model = SEASON_MODELS.get(current_season)
    if seasonal_model and check_model_exists(seasonal_model):
        logger.info(f"✅ {seasonal_model} found! Using seasonal model")
        return seasonal_model
    
    # Fallback to general model
    logger.info(f"⚠️ {seasonal_model} not found, using general model")
    return "general_energy_model"

# ✅ YENİ EKLENDİ: Production Model Loader
def load_production_model(model_name):
    """Production model yükle (DAG'lar için)"""
    try:
        # Önce Production stage'i dene
        model_uri = f"models:/{model_name}/Production"
        logger.info(f"🚀 Loading production model: {model_uri}")
        
        try:
            model = mlflow.spark.load_model(model_uri)
            logger.info(f"✅ {model_name} production model loaded")
        except Exception:
            # Production stage yoksa latest'i dene
            logger.warning(f"⚠️ Production stage bulunamadı, latest deneniyor...")
            model_uri = f"models:/{model_name}/latest"
            model = mlflow.spark.load_model(model_uri)
            logger.info(f"✅ {model_name} latest model loaded")
            
        return model
        
    except Exception as e:
        logger.error(f"❌ Production model yükleme hatası: {e}")
        return None

def register_model(model, model_name, season, metrics):
    """Register trained model to MLflow"""
    try:
        # Create experiment if not exists
        experiment_name = f"silver_layer_{season}_training"
        try:
            experiment = mlflow.get_experiment_by_name(experiment_name)
            if experiment is None:
                mlflow.create_experiment(experiment_name)
        except:
            mlflow.create_experiment(experiment_name)
        
        mlflow.set_experiment(experiment_name)
        
        with mlflow.start_run():
            # Log parameters
            mlflow.log_param("season", season)
            mlflow.log_param("model_type", "RandomForestRegressor")
            mlflow.log_param("training_date", datetime.now().isoformat())
            
            # Log metrics
            for metric_name, metric_value in metrics.items():
                mlflow.log_metric(metric_name, metric_value)
            
            # Log model
            mlflow.spark.log_model(
                spark_model=model,
                artifact_path="model",
                registered_model_name=model_name
            )
            
            logger.info(f"✅ Model {model_name} registered successfully")
            return True
        
    except Exception as e:
        logger.error(f"❌ Error registering model {model_name}: {e}")
        return False

def load_model_for_inference(model_name):
    """Load MLflow model with caching (using your pattern)"""
    if model_name in models_cache:
        logger.info(f"📋 Using cached {model_name} model")
        return models_cache[model_name]
    
    try:
        model_uri = f"models:/{model_name}/latest"
        logger.info(f"📥 Loading {model_name} model: {model_uri}")
        
        # Load model
        model = mlflow.spark.load_model(model_uri)
        models_cache[model_name] = {
            'model': model,
            'model_name': model_name,
            'model_type': current_season if model_name != "general_energy_model" else "general",
            'load_time': datetime.now()
        }
        
        logger.info(f"✅ {model_name} model loaded successfully")
        return models_cache[model_name]
        
    except Exception as e:
        logger.error(f"❌ Error loading {model_name} model: {e}")
        return None
