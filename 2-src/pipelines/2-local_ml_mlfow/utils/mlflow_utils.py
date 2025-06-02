import mlflow
import mlflow.spark

def log_to_mlflow(metrics, feature_columns, importance_dict, hyperparams, model=None):
    """✅ IMPROVED: Log comprehensive results to MLflow"""
    print("📝 LOGGING TO MLFLOW...")
    
    # Log hyperparameters
    for param, value in hyperparams.items():
        mlflow.log_param(param, value)
    
    # Log metrics
    for metric, value in metrics.items():
        mlflow.log_metric(metric, value)
    
    # Log feature info
    mlflow.log_param("total_features", len(feature_columns))
    mlflow.log_param("feature_list", ", ".join(feature_columns[:10]))
    
    # Log feature importance
    for feature, importance in importance_dict.items():
        mlflow.log_metric(f"feature_importance_{feature}", importance)
    
    # ✅ IMPROVED: Model saving with better error handling
    if model is not None:
        try:
            # 1. Save to MLflow Model Registry
            mlflow.spark.log_model(
                spark_model=model,
                artifact_path="energy_forecasting_model",
                registered_model_name="SystemEnergyForecaster"
            )
            print("✅ Model registered in MLflow Model Registry!")
            
            # 2. ✅ FIX: Create local backup directory
            import os
            model_dir = "/workspace/models"
            os.makedirs(model_dir, exist_ok=True)
            
            model_path = f"{model_dir}/energy_forecaster_pipeline"
            model.write().overwrite().save(model_path)
            print(f"✅ Model saved locally: {model_path}")
            
            # Log local path for reference
            mlflow.log_param("local_model_path", model_path)
            mlflow.log_param("model_save_status", "SUCCESS")
            
        except Exception as e:
            print(f"⚠️ Model save failed: {e}")
            mlflow.log_param("model_save_status", "FAILED")
            mlflow.log_param("model_save_error", str(e))
    
    print("✅ MLflow logging completed")
