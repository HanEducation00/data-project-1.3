#!/usr/bin/env python3
"""
SYSTEM TOTAL ENERGY FORECASTING PIPELINE
Predict daily total system energy consumption from smart meter data
"""

import time
import mlflow
from datetime import datetime

# Import all modules
from config.settings import setup_mlflow
from utils.spark_utils import create_spark_session
from utils.mlflow_utils import log_to_mlflow
from data.loader import load_smart_meter_data
from processing.aggregation import create_system_level_aggregation, create_daily_total_energy
from processing.features import create_advanced_features, prepare_ml_dataset
from ml.pipeline import create_ml_pipeline, train_and_evaluate_model
from ml.evaluation import get_feature_importance

def main():
    """Main pipeline execution"""
    print("🌟 SYSTEM TOTAL ENERGY FORECASTING PIPELINE")
    print("="*80)
    
    # Setup MLflow
    tracking_uri, experiment_name, experiment_id = setup_mlflow()
    
    # Start MLflow run
    with mlflow.start_run(run_name=f"System_Energy_FullYear_{datetime.now().strftime('%Y%m%d_%H%M%S')}") as run:
        
        print(f"🆔 MLflow Run ID: {run.info.run_id}")
        pipeline_start = time.time()
        
        try:
            # 1-6. Existing data pipeline steps...
            spark = create_spark_session()
            df, record_count = load_smart_meter_data(spark)
            system_df, system_time = create_system_level_aggregation(df)
            daily_df, day_count, daily_time = create_daily_total_energy(system_df)
            feature_df, feature_columns, feature_time = create_advanced_features(daily_df)
            train_df, val_df, test_df, train_count, val_count, test_count = prepare_ml_dataset(
                feature_df, feature_columns, test_days=60
            )
            
            # 7. Create and train ML pipeline
            ml_pipeline = create_ml_pipeline(feature_columns)
            
            # ✅ FIX: Define hyperparams BEFORE logging
            hyperparams = {
                "algorithm": "GBTRegressor",
                "maxDepth": 6,
                "maxBins": 32,
                "maxIter": 100,
                "stepSize": 0.1,
                "subsamplingRate": 0.8,
                "featureSubsetStrategy": "sqrt",
                "seed": 42,
                "test_days": 60,
                "total_features": len(feature_columns),
                "spark_version": spark.version
            }
            
            model, metrics, test_predictions = train_and_evaluate_model(
                ml_pipeline, train_df, val_df, test_df
            )
            
            # 8. Feature importance analysis
            importance_dict = get_feature_importance(model, feature_columns)
            
            # 9. ✅ FIX: Single MLflow logging call with model
            log_to_mlflow(metrics, feature_columns, importance_dict, hyperparams, model)
            
            # 10. Additional metrics (not duplicate logging)
            mlflow.log_metric("total_raw_records", record_count)
            mlflow.log_metric("total_days", day_count)
            mlflow.log_metric("train_days", train_count)
            mlflow.log_metric("val_days", val_count)
            mlflow.log_metric("test_days", test_count)
            mlflow.log_metric("system_aggregation_time", system_time)
            mlflow.log_metric("daily_aggregation_time", daily_time)
            mlflow.log_metric("feature_engineering_time", feature_time)
            
            # 11. Final summary
            total_pipeline_time = time.time() - pipeline_start
            mlflow.log_metric("total_pipeline_time", total_pipeline_time)
            
            # Success logging
            mlflow.log_param("pipeline_status", "SUCCESS")
            
            print("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
            return True
            
        except Exception as e:
            print(f"❌ PIPELINE ERROR: {e}")
            import traceback
            traceback.print_exc()
            
            # Error logging
            mlflow.log_param("pipeline_status", "FAILED")
            mlflow.log_param("error_message", str(e))
            return False
            
        finally:
            if 'spark' in locals():
                spark.stop()

if __name__ == "__main__":
    main()
