#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GÜNLÜK TOPLAM ENERJİ TAHMİNİ - BÖLÜM 1/3
Tüm sistem için günlük toplam enerji tüketimi tahmini
"""

import sys
import os
import math
import time
import json
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# MLflow imports
import mlflow
import mlflow.spark
from mlflow.models.signature import infer_signature

# Spark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg as spark_avg, max as spark_max, min as spark_min,
    date_format, dayofweek, month, quarter, hour, minute, year,
    when, lit, sin, cos, count, lag, rand, stddev, weekofyear,
    datediff, to_date, date_add, expr
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, DateType
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler, OneHotEncoder, StringIndexer
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor, LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

def setup_mlflow_daily_total():
    """
    Günlük toplam enerji tahmini için MLflow setup
    """
    print("📊 MLflow Günlük Toplam Enerji Tahmini Kurulumu...")
    
    tracking_uri = "http://localhost:5000"  # MLflow server URI
    mlflow.set_tracking_uri(tracking_uri)
    
    experiment_name = "Daily_Total_Energy_Forecasting"
    try:
        experiment_id = mlflow.create_experiment(experiment_name)
        print(f"✅ Yeni deney oluşturuldu: {experiment_name}")
    except:
        experiment = mlflow.get_experiment_by_name(experiment_name)
        experiment_id = experiment.experiment_id
        print(f"✅ Mevcut deney kullanılıyor: {experiment_name}")
    
    mlflow.set_experiment(experiment_name)
    
    print(f"📁 MLflow tracking URI: {tracking_uri}")
    print(f"🧪 Deney: {experiment_name}")
    print(f"🆔 Deney ID: {experiment_id}")
    
    return tracking_uri, experiment_name, experiment_id

def create_spark_session():
    """
    Optimize edilmiş Spark session oluştur
    """
    print("\n🚀 Spark Session Oluşturuluyor...")
    
    spark = SparkSession.builder \
        .appName("Daily_Total_Energy_Prediction") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    print(f"✅ Spark {spark.version} başlatıldı")
    return spark

def load_and_aggregate_data(spark, data_path):
    """
    Veriyi yükle ve günlük toplam enerji tüketimini hesapla
    """
    print("\n📂 Veri Yükleniyor ve Günlük Toplamlar Hesaplanıyor...")
    
    # CSV dosyalarını oku
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("compression", "gzip") \
        .csv(data_path)
    
    # Timestamp kolonunu düzelt
    df = df.withColumn("full_timestamp", col("full_timestamp").cast(TimestampType()))
    
    # Tarih kolonu ekle
    df = df.withColumn("date", to_date(col("full_timestamp")))
    
    # Günlük toplam enerji tüketimini hesapla
    daily_total = df.groupBy("date").agg(
        spark_sum("load_percentage").alias("total_daily_energy"),
        count("*").alias("reading_count"),
        spark_avg("load_percentage").alias("avg_load"),
        spark_max("load_percentage").alias("max_load"),
        spark_min("load_percentage").alias("min_load"),
        stddev("load_percentage").alias("load_std"),
        count(when(col("load_percentage") > 80, 1)).alias("high_load_count"),
        count(when(col("load_percentage") < 20, 1)).alias("low_load_count")
    ).orderBy("date")
    
    # Veri kalitesi kontrolü
    total_days = daily_total.count()
    min_date = daily_total.select(spark_min("date")).collect()[0][0]
    max_date = daily_total.select(spark_max("date")).collect()[0][0]
    
    print(f"✅ Toplam gün sayısı: {total_days}")
    print(f"📅 Tarih aralığı: {min_date} - {max_date}")
    print(f"📊 Ortalama günlük enerji: {daily_total.select(spark_avg('total_daily_energy')).collect()[0][0]:.2f}")
    
    return daily_total

def create_time_features(df):
    """
    Zaman bazlı özellikler oluştur
    """
    print("\n🔧 Zaman Özellikleri Oluşturuluyor...")
    
    # Temel zaman özellikleri
    df = df.withColumn("year", year("date")) \
           .withColumn("month", month("date")) \
           .withColumn("day_of_month", dayofweek("date")) \
           .withColumn("day_of_week", dayofweek("date")) \
           .withColumn("week_of_year", weekofyear("date")) \
           .withColumn("quarter", quarter("date")) \
           .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), 1).otherwise(0)) \
           .withColumn("is_monday", when(col("day_of_week") == 2, 1).otherwise(0)) \
           .withColumn("is_friday", when(col("day_of_week") == 6, 1).otherwise(0))
    
    # Mevsimsel özellikler
    df = df.withColumn("season", 
        when(col("month").isin([12, 1, 2]), 0)  # Kış
        .when(col("month").isin([3, 4, 5]), 1)  # İlkbahar
        .when(col("month").isin([6, 7, 8]), 2)  # Yaz
        .otherwise(3))  # Sonbahar
    
    # Döngüsel özellikler (sin/cos)
    PI = lit(math.pi)
    df = df.withColumn("sin_day_of_week", sin(col("day_of_week") * (2.0 * PI / 7))) \
           .withColumn("cos_day_of_week", cos(col("day_of_week") * (2.0 * PI / 7))) \
           .withColumn("sin_day_of_month", sin(col("day_of_month") * (2.0 * PI / 31))) \
           .withColumn("cos_day_of_month", cos(col("day_of_month") * (2.0 * PI / 31))) \
           .withColumn("sin_month", sin(col("month") * (2.0 * PI / 12))) \
           .withColumn("cos_month", cos(col("month") * (2.0 * PI / 12))) \
           .withColumn("sin_week_of_year", sin(col("week_of_year") * (2.0 * PI / 52))) \
           .withColumn("cos_week_of_year", cos(col("week_of_year") * (2.0 * PI / 52)))
    
    # Tatil günleri (basit yaklaşım - Türkiye için özelleştirilebilir)
    df = df.withColumn("is_holiday", 
        when(
            # Yılbaşı
            ((col("month") == 1) & (col("day_of_month") == 1)) |
            # 23 Nisan
            ((col("month") == 4) & (col("day_of_month") == 23)) |
            # 1 Mayıs
            ((col("month") == 5) & (col("day_of_month") == 1)) |
            # 19 Mayıs
            ((col("month") == 5) & (col("day_of_month") == 19)) |
            # 30 Ağustos
            ((col("month") == 8) & (col("day_of_month") == 30)) |
            # 29 Ekim
            ((col("month") == 10) & (col("day_of_month") == 29)),
            1
        ).otherwise(0))
    
    # Ay başı/sonu özellikleri
    df = df.withColumn("is_month_start", when(col("day_of_month") <= 5, 1).otherwise(0)) \
           .withColumn("is_month_end", when(col("day_of_month") >= 26, 1).otherwise(0))
    
    print("✅ Zaman özellikleri oluşturuldu")
    return df

def create_lag_features(df):
    """
    Lag (gecikmeli) özellikler oluştur - Çok önemli!
    """
    print("\n📈 Lag Özellikleri Oluşturuluyor...")
    
    # Window specification - tarih sırasına göre
    window_spec = Window.orderBy("date")
    
    # Önceki günlerin enerji tüketimi
    for i in [1, 2, 3, 7, 14, 21, 28]:  # 1, 2, 3, 7, 14, 21, 28 gün öncesi
        df = df.withColumn(f"energy_lag_{i}d", lag("total_daily_energy", i).over(window_spec))
    
    # Hareketli ortalamalar
    for window_size in [3, 7, 14, 28]:
        df = df.withColumn(
            f"energy_ma_{window_size}d",
            spark_avg("total_daily_energy").over(
                window_spec.rowsBetween(-window_size, -1)
            )
        )
    
    # Hareketli standart sapma
    for window_size in [7, 14]:
        df = df.withColumn(
            f"energy_std_{window_size}d",
            stddev("total_daily_energy").over(
                window_spec.rowsBetween(-window_size, -1)
            )
        )
    
    # Önceki haftanın aynı günü
    df = df.withColumn("energy_same_day_last_week", lag("total_daily_energy", 7).over(window_spec))
    
    # Önceki ayın aynı günü (yaklaşık)
    df = df.withColumn("energy_same_day_last_month", lag("total_daily_energy", 30).over(window_spec))
    
    # Trend özellikleri
    df = df.withColumn("energy_trend_7d", 
        (col("total_daily_energy") - lag("total_daily_energy", 7).over(window_spec)) / 
        (lag("total_daily_energy", 7).over(window_spec) + 0.001)
    )
    
    df = df.withColumn("energy_trend_28d",
        (col("total_daily_energy") - lag("total_daily_energy", 28).over(window_spec)) /
        (lag("total_daily_energy", 28).over(window_spec) + 0.001)
    )
    
    # Yük profili özellikleri (önceki günlerden)
    for i in [1, 7]:
        df = df.withColumn(f"avg_load_lag_{i}d", lag("avg_load", i).over(window_spec))
        df = df.withColumn(f"max_load_lag_{i}d", lag("max_load", i).over(window_spec))
        df = df.withColumn(f"load_std_lag_{i}d", lag("load_std", i).over(window_spec))
    
    # NULL değerleri doldur
    lag_columns = [col for col in df.columns if 'lag' in col or 'ma_' in col or 'std_' in col or 'trend' in col]
    for col_name in lag_columns:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0).otherwise(col(col_name)))
    
    print(f"✅ {len(lag_columns)} lag özelliği oluşturuldu")
    return df

def create_advanced_features(df):
    """
    İleri düzey özellikler oluştur
    """
    print("\n🔬 İleri Düzey Özellikler Oluşturuluyor...")
    
    # Yılın günü
    df = df.withColumn("day_of_year", datediff(col("date"), 
        to_date(lit(f"{df.select(year('date')).first()[0]}-01-01"), "yyyy-MM-dd")) + 1)
    
    # Özel günler arası mesafe
    df = df.withColumn("days_since_month_start", col("day_of_month") - 1)
    df = df.withColumn("days_until_month_end", 
        datediff(date_add(col("date"), 31 - col("day_of_month")), col("date")))
    
    # Hafta içi ardışık günler
    df = df.withColumn("consecutive_weekdays",
        when(col("is_weekend") == 0, 
            spark_sum(when(col("is_weekend") == 0, 1).otherwise(0)).over(
                Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
            )
        ).otherwise(0)
    )
    
    # Enerji tüketimi kategorileri
    avg_energy = df.select(spark_avg("total_daily_energy")).collect()[0][0]
    std_energy = df.select(stddev("total_daily_energy")).collect()[0][0]
    
    df = df.withColumn("energy_category",
        when(col("total_daily_energy") > avg_energy + 2 * std_energy, "very_high")
        .when(col("total_daily_energy") > avg_energy + std_energy, "high")
        .when(col("total_daily_energy") < avg_energy - std_energy, "low")
        .when(col("total_daily_energy") < avg_energy - 2 * std_energy, "very_low")
        .otherwise("normal")
    )
    
    # Önceki günün kategorisi
    window_spec = Window.orderBy("date")
    df = df.withColumn("prev_energy_category", lag("energy_category", 1).over(window_spec))
    
    # Anomali skoru (basit z-score)
    df = df.withColumn("energy_zscore",
        (col("total_daily_energy") - avg_energy) / std_energy
    )
    
    print("✅ İleri düzey özellikler oluşturuldu")
    return df

def prepare_train_test_split(df, test_days=30):
    """
    Zaman serisi için train/validation/test split
    """
    print(f"\n🔀 Veri Bölünüyor (Son {test_days} gün test için)...")
    
    # Son tarih
    max_date = df.select(spark_max("date")).collect()[0][0]
    
    # Test başlangıç tarihi
    test_start_date = max_date - timedelta(days=test_days)
    val_start_date = test_start_date - timedelta(days=test_days)
    
    # Veriyi böl
    train_df = df.filter(col("date") < val_start_date)
    val_df = df.filter((col("date") >= val_start_date) & (col("date") < test_start_date))
    test_df = df.filter(col("date") >= test_start_date)
    
    # Cache'le
    train_df = train_df.cache()
    val_df = val_df.cache()
    test_df = test_df.cache()
    
    print(f"✅ Train: {train_df.count()} gün")
    print(f"✅ Validation: {val_df.count()} gün")
    print(f"✅ Test: {test_df.count()} gün")
    
    return train_df, val_df, test_df

def build_ml_pipeline(feature_columns, target_column="total_daily_energy"):
    """
    ML pipeline oluştur
    """
    print("\n🏗️ ML Pipeline Oluşturuluyor...")
    
    # Kategorik kolonları encode et
    categorical_cols = ["energy_category", "prev_energy_category"]
    
    # String Indexer
    indexers = []
    for cat_col in categorical_cols:
        if cat_col in feature_columns:
            indexer = StringIndexer(
                inputCol=cat_col,
                outputCol=f"{cat_col}_indexed",
                handleInvalid="keep"
            )
            indexers.append(indexer)
            # Feature listesini güncelle
            feature_columns = [f"{cat_col}_indexed" if col == cat_col else col for col in feature_columns]
    
    # Numeric features
    numeric_features = [col for col in feature_columns if not col.endswith("_indexed")]
    
    # Vector Assembler
    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="features",
        handleInvalid="skip"
    )
    
    # Scaler
    scaler = StandardScaler(
        inputCol="features",
        outputCol="scaled_features",
        withStd=True,
        withMean=True
    )
    
    # Model - Gradient Boosted Trees
    gbt = GBTRegressor(
        featuresCol="scaled_features",
        labelCol=target_column,
        predictionCol="prediction",
        maxIter=100,
        maxDepth=8,
        stepSize=0.1,
        subsamplingRate=0.8,
        featureSubsetStrategy="sqrt",
        minInstancesPerNode=5,
        maxBins=32
    )
    
    # Pipeline
    pipeline = Pipeline(stages=indexers + [assembler, scaler, gbt])
    
    print(f"✅ Pipeline oluşturuldu: {len(feature_columns)} özellik")
    return pipeline

def train_and_evaluate_model(pipeline, train_df, val_df, test_df, target_column="total_daily_energy"):
    """
    Modeli eğit ve değerlendir
    """
    print("\n🎯 Model Eğitiliyor...")
    
    start_time = time.time()
    
    # Model eğitimi
    model = pipeline.fit(train_df)
    
    training_time = time.time() - start_time
    print(f"✅ Model eğitimi tamamlandı: {training_time:.1f} saniye")
    
    # Tahminler
    print("\n📊 Model Değerlendiriliyor...")
    
    train_predictions = model.transform(train_df)
    val_predictions = model.transform(val_df)
    test_predictions = model.transform(test_df)
    
    # Evaluator
    evaluator = RegressionEvaluator(
        labelCol=target_column,
        predictionCol="prediction"
    )
    
    # Metrikleri hesapla
    metrics = {}
    
    for dataset_name, predictions in [("train", train_predictions), 
                                     ("validation", val_predictions), 
                                     ("test", test_predictions)]:
        
        # RMSE
        evaluator.setMetricName("rmse")
        rmse = evaluator.evaluate(predictions)
        
        # R2
        evaluator.setMetricName("r2")
        r2 = evaluator.evaluate(predictions)
        
        # MAE
        evaluator.setMetricName("mae")
        mae = evaluator.evaluate(predictions)
        
        # MAPE (Manual calculation)
        mape_df = predictions.select(
            ((abs(col(target_column) - col("prediction")) / col(target_column)) * 100).alias("ape")
        )
        mape = mape_df.select(spark_avg("ape")).collect()[0][0]
        
        metrics[dataset_name] = {
            "rmse": rmse,
            "r2": r2,
            "mae": mae,
            "mape": mape
        }
        
        print(f"\n{dataset_name.upper()} Metrikleri:")
        print(f"  RMSE: {rmse:,.2f}")
        print(f"  R²: {r2:.4f}")
        print(f"  MAE: {mae:,.2f}")
        print(f"  MAPE: {mape:.2f}%")
    
    return model, metrics, test_predictions

def predict_specific_date(model, spark, date_str, feature_columns, historical_df):
    """
    Belirli bir tarih için tahmin yap
    Örnek: "2016-03-05" için tahmin
    """
    print(f"\n🔮 {date_str} Tarihi İçin Tahmin Yapılıyor...")
    
    # Tarih objesine çevir
    target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    
    # Geçmiş verileri al (feature engineering için)
    historical_data = historical_df.filter(col("date") < target_date).orderBy(col("date").desc())
    
    # Tahmin için DataFrame oluştur
    prediction_df = spark.createDataFrame([(target_date,)], ["date"])
    
    # Zaman özelliklerini ekle
    prediction_df = create_time_features(prediction_df)
    
    # Lag özelliklerini historical_df'den al
    # Son 28 günlük veriyi al
    recent_data = historical_data.limit(28).orderBy("date")
    
    # Lag değerlerini hesapla
    lag_values = {}
    for i in [1, 2, 3, 7, 14, 21, 28]:
        lag_row = recent_data.filter(col("date") == date_add(lit(target_date), -i)).select("total_daily_energy").collect()
        if lag_row:
            lag_values[f"energy_lag_{i}d"] = lag_row[0][0]
        else:
            lag_values[f"energy_lag_{i}d"] = 0
    
    # Hareketli ortalamaları hesapla
    for window_size in [3, 7, 14, 28]:
        ma_data = recent_data.filter(col("date") > date_add(lit(target_date), -window_size)) \
                            .select(spark_avg("total_daily_energy").alias("ma")).collect()
        if ma_data:
            lag_values[f"energy_ma_{window_size}d"] = ma_data[0][0]
        else:
            lag_values[f"energy_ma_{window_size}d"] = 0
    
    # Diğer lag özelliklerini ekle
    for col_name, value in lag_values.items():
        prediction_df = prediction_df.withColumn(col_name, lit(value))
    
    # Eksik kolonları 0 ile doldur
    for col_name in feature_columns:
        if col_name not in prediction_df.columns:
            prediction_df = prediction_df.withColumn(col_name, lit(0))
    
    # Tahmin yap
    prediction = model.transform(prediction_df)
    
    # Sonucu al
    result = prediction.select("date", "prediction").collect()[0]
    predicted_energy = result["prediction"]
    
    print(f"✅ Tahmin Tamamlandı!")
    print(f"📅 Tarih: {date_str}")
    print(f"⚡ Tahmini Toplam Enerji Tüketimi: {predicted_energy:,.2f} birim")
    
    # Geçmiş ortalamalarla karşılaştır
    avg_weekday = historical_data.filter(col("day_of_week") == dayofweek(lit(target_date))) \
                                .select(spark_avg("total_daily_energy")).collect()[0][0]
    
    avg_monthly = historical_data.filter(col("month") == month(lit(target_date))) \
                                .select(spark_avg("total_daily_energy")).collect()[0][0]
    
    print(f"\n📊 Karşılaştırma:")
    print(f"   Aynı gün tipi ortalaması: {avg_weekday:,.2f} birim")
    print(f"   Aynı ay ortalaması: {avg_monthly:,.2f} birim")
    print(f"   Tahmin farkı (gün tipi): {((predicted_energy/avg_weekday - 1) * 100):.1f}%")
    
    return predicted_energy

def save_model_and_artifacts(model, metrics, feature_columns, mlflow_run_id):
    """
    Model ve artifacts'ları kaydet
    """
    print("\n💾 Model ve Artifacts Kaydediliyor...")
    
    # Model özeti
    model_summary = {
        "model_type": "GBTRegressor",
        "feature_count": len(feature_columns),
        "features": feature_columns,
        "metrics": metrics,
        "mlflow_run_id": mlflow_run_id,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    # JSON olarak kaydet
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(model_summary, f, indent=2)
        mlflow.log_artifact(f.name, "model_summary.json")
    
    # Feature importance (eğer varsa)
    if hasattr(model.stages[-1], 'featureImportances'):
        importances = model.stages[-1].featureImportances.toArray()
        feature_importance = list(zip(feature_columns, importances))
        feature_importance.sort(key=lambda x: x[1], reverse=True)
        
        print("\n🏆 En Önemli 10 Özellik:")
        for i, (feature, importance) in enumerate(feature_importance[:10]):
            print(f"   {i+1}. {feature}: {importance:.4f}")
            mlflow.log_metric(f"feature_importance_{feature}", importance)
    
    print("✅ Model ve artifacts kaydedildi")
    
    return model_summary

def main_daily_total_energy_pipeline():
    """
    Ana pipeline - Günlük toplam enerji tahmini
    """
    print("🚀 GÜNLÜK TOPLAM ENERJİ TAHMİNİ PIPELINE")
    print("="*80)
    
    # MLflow setup
    tracking_uri, experiment_name, experiment_id = setup_mlflow_daily_total()
    
    # MLflow run başlat
    with mlflow.start_run(run_name=f"Daily_Total_Energy_{datetime.now().strftime('%Y%m%d_%H%M%S')}") as run:
        
        print(f"🆔 MLflow Run ID: {run.info.run_id}")
        
        try:
            
            spark = create_spark_session()
            mlflow.log_param("spark_version", spark.version)
            
            # Parametreler
            data_path = "/tmp/ultimate_full_year/*.csv*"  # Veri yolu
            test_days = 30  # Test için son 30 gün
            
            mlflow.log_param("data_path", data_path)
            mlflow.log_param("test_days", test_days)
            mlflow.log_param("prediction_type", "daily_total_energy")
            
            # 1. VERİ YÜKLEME VE GÜNLÜK TOPLAM
            print("\n" + "="*50)
            print("ADIM 1: VERİ YÜKLEME VE GÜNLÜK TOPLAMLAR")
            print("="*50)
            
            daily_df = load_and_aggregate_data(spark, data_path)
            
            mlflow.log_metric("total_days", daily_df.count())
            mlflow.log_metric("avg_daily_energy", 
                            daily_df.select(spark_avg("total_daily_energy")).collect()[0][0])
            
            # 2. ÖZELLİK MÜHENDİSLİĞİ
            print("\n" + "="*50)
            print("ADIM 2: ÖZELLİK MÜHENDİSLİĞİ")
            print("="*50)
            
            # Zaman özellikleri
            daily_df = create_time_features(daily_df)
            
            # Lag özellikleri
            daily_df = create_lag_features(daily_df)
            
            # İleri düzey özellikler
            daily_df = create_advanced_features(daily_df)
            
            # Özellik listesi
            feature_columns = [
                # Zaman özellikleri
                "year", "month", "day_of_week", "day_of_month", "week_of_year", "quarter",
                "is_weekend", "is_monday", "is_friday", "season", "is_holiday",
                "is_month_start", "is_month_end",
                
                # Döngüsel özellikler
                "sin_day_of_week", "cos_day_of_week", "sin_day_of_month", "cos_day_of_month",
                "sin_month", "cos_month", "sin_week_of_year", "cos_week_of_year",
                
                # Lag özellikleri
                "energy_lag_1d", "energy_lag_2d", "energy_lag_3d", "energy_lag_7d",
                "energy_lag_14d", "energy_lag_21d", "energy_lag_28d",
                
                # Hareketli ortalamalar
                "energy_ma_3d", "energy_ma_7d", "energy_ma_14d", "energy_ma_28d",
                
                # Hareketli standart sapma
                "energy_std_7d", "energy_std_14d",
                
                # Özel lag'ler
                "energy_same_day_last_week", "energy_same_day_last_month",
                
                # Trend özellikleri
                "energy_trend_7d", "energy_trend_28d",
                
                # Yük profili lag'leri
                "avg_load_lag_1d", "avg_load_lag_7d",
                "max_load_lag_1d", "max_load_lag_7d",
                "load_std_lag_1d", "load_std_lag_7d",
                
                # İleri düzey özellikler
                "day_of_year", "days_since_month_start", "days_until_month_end",
                "consecutive_weekdays", "energy_zscore",
                
                # Günlük istatistikler
                "avg_load", "max_load", "min_load", "load_std",
                "high_load_count", "low_load_count", "reading_count"
            ]
            
            mlflow.log_param("num_features", len(feature_columns))
            mlflow.log_param("features", str(feature_columns))
            
            # 3. TRAIN/VALIDATION/TEST SPLIT
            print("\n" + "="*50)
            print("ADIM 3: VERİ BÖLME")
            print("="*50)
            
            train_df, val_df, test_df = prepare_train_test_split(daily_df, test_days)
            
            mlflow.log_metric("train_days", train_df.count())
            mlflow.log_metric("validation_days", val_df.count())
            mlflow.log_metric("test_days", test_df.count())
            
            # 4. MODEL PIPELINE
            print("\n" + "="*50)
            print("ADIM 4: MODEL EĞİTİMİ")
            print("="*50)
            
            # ML Pipeline oluştur
            pipeline = build_ml_pipeline(feature_columns, target_column="total_daily_energy")
            
            # Model parametrelerini logla
            mlflow.log_param("model_type", "GBTRegressor")
            mlflow.log_param("gbt_max_iter", 100)
            mlflow.log_param("gbt_max_depth", 8)
            mlflow.log_param("gbt_step_size", 0.1)
            mlflow.log_param("gbt_subsampling_rate", 0.8)
            
            # Model eğitimi ve değerlendirme
            model, metrics, test_predictions = train_and_evaluate_model(
                pipeline, train_df, val_df, test_df
            )
            
            # Metrikleri MLflow'a logla
            for dataset_name, dataset_metrics in metrics.items():
                for metric_name, value in dataset_metrics.items():
                    mlflow.log_metric(f"{dataset_name}_{metric_name}", value)
            
            # Test performansı özeti
            print("\n" + "="*50)
            print("TEST PERFORMANSI ÖZETİ")
            print("="*50)
            print(f"📊 Test R²: {metrics['test']['r2']:.4f} ({metrics['test']['r2']*100:.2f}%)")
            print(f"📊 Test RMSE: {metrics['test']['rmse']:,.2f}")
            print(f"📊 Test MAPE: {metrics['test']['mape']:.2f}%")
            
            # Overfitting kontrolü
            overfitting_gap = metrics['train']['r2'] - metrics['test']['r2']
            mlflow.log_metric("overfitting_gap", overfitting_gap)
            print(f"📊 Overfitting Gap: {overfitting_gap:.4f}")
            
            # Bölüm 5'te devam edecek...            
            # Spark session
            
# 5. ÖRNEK TAHMİNLER
            print("\n" + "="*50)
            print("ADIM 5: ÖRNEK TAHMİNLER")
            print("="*50)
            
            # Örnek: 5 Mart 2016 tahmini
            example_date = "2016-03-05"
            predicted_energy = predict_specific_date(
                model, spark, example_date, feature_columns, daily_df
            )
            
            mlflow.log_metric("example_prediction", predicted_energy)
            mlflow.log_param("example_date", example_date)
            
            # Birkaç örnek daha
            print("\n📅 Diğer Örnek Tahminler:")
            for date_str in ["2016-01-15", "2016-06-20", "2016-09-10", "2016-12-25"]:
                try:
                    energy = predict_specific_date(
                        model, spark, date_str, feature_columns, daily_df
                    )
                    print(f"   {date_str}: {energy:,.2f} birim")
                except:
                    print(f"   {date_str}: Tahmin yapılamadı")
            
            # 6. MODEL VE ARTIFACTS KAYDETME
            print("\n" + "="*50)
            print("ADIM 6: MODEL KAYDETME")
            print("="*50)
            
            # Model kaydet
            model_summary = save_model_and_artifacts(
                model, metrics, feature_columns, run.info.run_id
            )
            
            # MLflow model kaydı
            model_info = mlflow.spark.log_model(
                spark_model=model,
                artifact_path="daily_total_energy_model",
                registered_model_name="Daily_Total_Energy_Forecasting_Model"
            )
            
            mlflow.log_param("model_uri", model_info.model_uri)
            
            # Test tahminlerini kaydet
            test_results = test_predictions.select(
                "date", "total_daily_energy", "prediction"
            ).withColumn(
                "error", col("total_daily_energy") - col("prediction")
            ).withColumn(
                "error_percent", 
                (abs(col("error")) / col("total_daily_energy")) * 100
            )
            
            # En iyi ve en kötü tahminler
            print("\n📈 En İyi Tahminler (En Düşük Hata):")
            test_results.orderBy("error_percent").show(5)
            
            print("\n📉 En Kötü Tahminler (En Yüksek Hata):")
            test_results.orderBy(col("error_percent").desc()).show(5)
            
            # Özet
            print("\n" + "="*80)
            print("🎉 GÜNLÜK TOPLAM ENERJİ TAHMİNİ BAŞARIYLA TAMAMLANDI!")
            print("="*80)
            print(f"✅ Model URI: {model_info.model_uri}")
            print(f"✅ MLflow Run ID: {run.info.run_id}")
            print(f"✅ Test R²: {metrics['test']['r2']*100:.2f}%")
            print(f"✅ Test MAPE: {metrics['test']['mape']:.2f}%")
            print(f"✅ Tracking URI: {tracking_uri}")
            
            return {
                "success": True,
                "model_uri": model_info.model_uri,
                "run_id": run.info.run_id,
                "metrics": metrics,
                "model_summary": model_summary
            }
            
        except Exception as e:
            mlflow.log_param("error", str(e))
            print(f"\n❌ HATA: {e}")
            import traceback
            traceback.print_exc()
            raise
            
        finally:
            if 'spark' in locals():
                spark.stop()
                print("\n🧹 Spark session kapatıldı")


# ANA FONKSİYON
if __name__ == "__main__":
    print("🚀 GÜNLÜK TOPLAM ENERJİ TAHMİNİ BAŞLATILIYOR...")
    print("📊 Tüm sistem için günlük toplam enerji tüketimi tahmini")
    print("="*80)
    
    try:
        # Pipeline'ı çalıştır
        result = main_daily_total_energy_pipeline()
        
        if result["success"]:
            print("\n" + "="*80)
            print("✅ TÜM İŞLEMLER BAŞARIYLA TAMAMLANDI!")
            print("="*80)
            print("\n📊 ÖZET:")
            print(f"   - Model URI: {result['model_uri']}")
            print(f"   - Test R²: {result['metrics']['test']['r2']*100:.2f}%")
            print(f"   - Test MAPE: {result['metrics']['test']['mape']:.2f}%")
            print(f"   - MLflow Run ID: {result['run_id']}")
            print("\n💡 Model artık günlük toplam enerji tahminleri yapabilir!")
            print("   Örnek: '5 Mart 2016 tarihinde toplam ne kadar enerji kullanılacak?'")
            
    except Exception as e:
        print(f"\n❌ Pipeline hatası: {e}")
        import traceback
        traceback.print_exc()            
            