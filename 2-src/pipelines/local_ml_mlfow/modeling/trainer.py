#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FULL DATA TRAINING - 25M+ Records!
"""

import sys
import os
import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.metrics import mean_absolute_error, r2_score
from pyspark.sql.functions import col

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'data_processing'))
from config import MODEL_CONFIG
from aggregator import aggregate_electricity_data

def train_full_data_model():
    """
    FULL DATABASE ile model eğitimi
    """
    from data_loader import load_electricity_data
    
    print("🔥 FULL DATA TRAINING STARTED!")
    print("📊 Loading ALL database records...")
    
    # TÜM VERİYİ ÇEK (LIMIT YOK!)
    raw_df, loader = load_electricity_data(
        start_date="2010-01-01",  
        end_date="2030-12-31",    
        limit=None  # LIMIT YOK!
    )
    
    total_records = raw_df.count()
    print(f"🔥 Total raw records: {total_records:,}")
    
    if total_records > 5000000:  # 5M+ 
        print("🚀 HUGE DATASET! Perfect for advanced modeling!")
    
    # AGGREGATE TO HOURLY
    print("📊 Aggregating to hourly...")
    aggregated_df = aggregate_electricity_data(raw_df, loader.spark)
    total_hours = aggregated_df.count()
    
    print(f"📊 Total hours: {total_hours:,} ({total_hours/24:.0f} days)")
    
    if total_hours < 100:
        print("❌ Too few hours for full training")
        loader.close()
        return None
    
    # TIME-BASED SPLIT
    print("📊 Time-based train/validation/test split...")
    
    # Sort by datetime
    sorted_df = aggregated_df.orderBy("datetime")
    
    # Calculate split points
    train_end = int(total_hours * 0.7)
    val_end = int(total_hours * 0.85)
    
    # Add row numbers for splitting
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    window = Window.orderBy("datetime")
    with_row_num = sorted_df.withColumn("row_num", row_number().over(window))
    
    train_df = with_row_num.filter(col("row_num") <= train_end)
    val_df = with_row_num.filter((col("row_num") > train_end) & (col("row_num") <= val_end))
    test_df = with_row_num.filter(col("row_num") > val_end)
    
    train_count = train_df.count()
    val_count = val_df.count() 
    test_count = test_df.count()
    
    print(f"📊 Train: {train_count:,} hours")
    print(f"📊 Validation: {val_count:,} hours")
    print(f"📊 Test: {test_count:,} hours")
    
    # FEATURES (BASİT BAŞLA)
    base_features = [
        "hour", "month", "dayofweek",
        "is_weekend", "is_peak_hour", 
        "active_customers"
    ]
    
    target_col = MODEL_CONFIG["target_variable"]
    columns_needed = [target_col] + base_features
    
    # PANDAS'A ÇEVİR
    print("📊 Converting to pandas...")
    
    train_pandas = train_df.select(*columns_needed).toPandas()
    val_pandas = val_df.select(*columns_needed).toPandas()
    test_pandas = test_df.select(*columns_needed).toPandas()
    
    # X, y SPLIT
    X_train = train_pandas[base_features].values
    y_train = train_pandas[target_col].values
    
    X_val = val_pandas[base_features].values
    y_val = val_pandas[target_col].values
    
    X_test = test_pandas[base_features].values
    y_test = test_pandas[target_col].values
    
    print(f"📊 Train X shape: {X_train.shape}")
    print(f"📊 Val X shape: {X_val.shape}")
    print(f"📊 Test X shape: {X_test.shape}")
    
    # LIGHTGBM TRAINING
    print("🧠 Training LightGBM on FULL DATA...")
    
    params = {
        'objective': 'regression',
        'metric': 'rmse',
        'num_leaves': 100,  # Daha complex model
        'learning_rate': 0.05,
        'feature_fraction': 0.8,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'verbose': -1
    }
    
    train_data = lgb.Dataset(X_train, label=y_train)
    val_data = lgb.Dataset(X_val, label=y_val, reference=train_data)
    
    model = lgb.train(
        params,
        train_data,
        valid_sets=[train_data, val_data],
        valid_names=['train', 'val'],
        num_boost_round=500,
        callbacks=[lgb.early_stopping(50)]
    )
    
    # PREDICTIONS & METRICS
    y_train_pred = model.predict(X_train)
    y_val_pred = model.predict(X_val)
    y_test_pred = model.predict(X_test)
    
    # Calculate metrics
    train_mae = mean_absolute_error(y_train, y_train_pred)
    val_mae = mean_absolute_error(y_val, y_val_pred)
    test_mae = mean_absolute_error(y_test, y_test_pred)
    
    train_r2 = r2_score(y_train, y_train_pred)
    val_r2 = r2_score(y_val, y_val_pred)
    test_r2 = r2_score(y_test, y_test_pred)
    
    print(f"\n🎉 FULL DATA RESULTS:")
    print(f"📊 Train MAE: {train_mae:.2f} MW (R²: {train_r2:.4f})")
    print(f"📊 Val MAE: {val_mae:.2f} MW (R²: {val_r2:.4f})")
    print(f"📊 Test MAE: {test_mae:.2f} MW (R²: {test_r2:.4f})")
    
    loader.close()
    
    return {
        "success": True,
        "model": model,
        "metrics": {
            "train_mae": train_mae, "val_mae": val_mae, "test_mae": test_mae,
            "train_r2": train_r2, "val_r2": val_r2, "test_r2": test_r2
        },
        "data_info": {
            "total_raw_records": total_records,
            "total_hours": total_hours,
            "train_hours": train_count,
            "val_hours": val_count,
            "test_hours": test_count
        }
    }

if __name__ == "__main__":
    result = train_full_data_model()
    
    if result and result["success"]:
        print(f"\n🔥 FULL DATA TRAINING COMPLETE!")
        print(f"📊 Processed {result['data_info']['total_raw_records']:,} raw records")
        print(f"📊 Trained on {result['data_info']['train_hours']:,} hours")
        print(f"📊 Best Test R²: {result['metrics']['test_r2']:.4f}")
    else:
        print("❌ Full data training failed")
