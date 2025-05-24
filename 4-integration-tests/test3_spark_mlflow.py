#!/usr/bin/env python3
"""
Test 3 - ML Training: Spark rastgele veri → Linear Regression → MLflow
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, randn, col, round as spark_round
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
import mlflow
import mlflow.spark
from datetime import datetime
import random

# Configuration
MLFLOW_TRACKING_URI = "http://mlflow-server:5000"
EXPERIMENT_NAME = "test-experiments"
MODEL_NAME = "test-linear-regression-v1"

def create_spark_session():
    """Spark Session with MLlib support"""
    return SparkSession.builder \
        .appName("test-ml-training") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def generate_house_data(spark, num_samples=100):
    """Rastgele ev verisi üret (Linear Regression için uygun)"""
    try:
        print("🏠 Rastgele ev verisi oluşturuluyor...")
        
        # Seed ayarla (reproducible results için)
        random.seed(42)
        spark.sparkContext.setLogLevel("WARN")
        
        # Rastgele DataFrame oluştur
        df = spark.range(num_samples).select(
            col("id"),
            # house_size: 50-200 m² arası
            spark_round((rand(seed=42) * 150 + 50), 1).alias("house_size"),
            # location_score: 1-10 arası
            spark_round((rand(seed=123) * 9 + 1), 1).alias("location_score"),
            # age_years: 0-30 yıl arası
            spark_round((rand(seed=456) * 30), 0).alias("age_years"),
            # rooms: 1-6 oda arası
            spark_round((rand(seed=789) * 5 + 1), 0).alias("rooms")
        )
        
        # Price hesapla (realistic linear relationship)
        # Price = base_price + (size * size_factor) + (location * location_factor) - (age * age_factor) + (rooms * room_factor) + noise
        df = df.withColumn(
            "price",
            spark_round(
                (col("house_size") * 2500) +  # 2500 TL per m²
                (col("location_score") * 25000) +  # 25K per location point
                (col("rooms") * 15000) +  # 15K per room
                (col("age_years") * -3000) +  # -3K per year (depreciation)
                (randn(seed=999) * 50000) +  # Noise
                200000,  # Base price
                0
            ).cast("integer")
        )
        
        print(f"✅ {num_samples} ev verisi oluşturuldu")
        print("📊 Örnek veriler:")
        df.show(10)
        
        # İstatistikler
        df.describe().show()
        
        return df
        
    except Exception as e:
        print(f"❌ Veri oluşturma hatası: {e}")
        raise

def prepare_features(df):
    """ML için feature'ları hazırla"""
    try:
        print("🔧 Feature engineering yapılıyor...")
        
        # Feature columns
        feature_cols = ["house_size", "location_score", "age_years", "rooms"]
        
        # VectorAssembler ile features'ları birleştir
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        # Transform
        df_features = assembler.transform(df)
        
        print("✅ Features hazırlandı:")
        print(f"📊 Feature columns: {feature_cols}")
        print(f"📊 Target column: price")
        
        df_features.select("features", "price").show(5, truncate=False)
        
        return df_features, assembler
        
    except Exception as e:
        print(f"❌ Feature hazırlama hatası: {e}")
        raise

def train_linear_regression(df_features):
    """Linear Regression modeli eğit"""
    try:
        print("🧠 Linear Regression modeli eğitiliyor...")
        
        # Train/Test split
        train_df, test_df = df_features.randomSplit([0.8, 0.2], seed=42)
        
        print(f"📊 Training samples: {train_df.count()}")
        print(f"📊 Test samples: {test_df.count()}")
        
        # Linear Regression model
        lr = LinearRegression(
            featuresCol="features",
            labelCol="price",
            maxIter=100,
            regParam=0.1,
            elasticNetParam=0.0
        )
        
        # Model eğit
        print("⏳ Model eğitiliyor...")
        lr_model = lr.fit(train_df)
        
        # Predictions
        train_predictions = lr_model.transform(train_df)
        test_predictions = lr_model.transform(test_df)
        
        # Evaluator
        evaluator = RegressionEvaluator(
            labelCol="price",
            predictionCol="prediction",
            metricName="rmse"
        )
        
        # Metrics
        train_rmse = evaluator.evaluate(train_predictions)
        test_rmse = evaluator.evaluate(test_predictions)
        
        # R² score
        evaluator_r2 = RegressionEvaluator(
            labelCol="price",
            predictionCol="prediction", 
            metricName="r2"
        )
        
        train_r2 = evaluator_r2.evaluate(train_predictions)
        test_r2 = evaluator_r2.evaluate(test_predictions)
        
        print("✅ Model eğitimi tamamlandı!")
        print(f"📊 Train RMSE: {train_rmse:,.2f}")
        print(f"📊 Test RMSE: {test_rmse:,.2f}")
        print(f"📊 Train R²: {train_r2:.4f}")
        print(f"📊 Test R²: {test_r2:.4f}")
        
        # Örnek predictions göster
        print("📊 Örnek tahminler:")
        test_predictions.select("features", "price", "prediction").show(5, truncate=False)
        
        # Model coefficients
        print("🔍 Model Coefficients:")
        print(f"   Intercept: {lr_model.intercept:,.2f}")
        coefficients = lr_model.coefficients
        feature_names = ["house_size", "location_score", "age_years", "rooms"]
        for i, (name, coef) in enumerate(zip(feature_names, coefficients)):
            print(f"   {name}: {coef:,.2f}")
        
        return lr_model, {
            "train_rmse": train_rmse,
            "test_rmse": test_rmse,
            "train_r2": train_r2,
            "test_r2": test_r2,
            "train_samples": train_df.count(),
            "test_samples": test_df.count()
        }
        
    except Exception as e:
        print(f"❌ Model eğitim hatası: {e}")
        raise

def log_to_mlflow(lr_model, assembler, metrics):
    """Modeli ve metrikleri MLflow'a kaydet"""
    try:
        print("📊 MLflow'a model kaydediliyor...")
        
        # MLflow tracking URI
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        
        # Experiment oluştur/bul
        try:
            experiment_id = mlflow.create_experiment(EXPERIMENT_NAME)
        except mlflow.exceptions.MlflowException:
            experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
            experiment_id = experiment.experiment_id
        
        # MLflow run başlat
        with mlflow.start_run(
            experiment_id=experiment_id,
            run_name=f"test-lr-run-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        ):
            # Parameters log et
            mlflow.log_param("model_type", "LinearRegression")
            mlflow.log_param("max_iter", 100)
            mlflow.log_param("reg_param", 0.1)
            mlflow.log_param("elastic_net_param", 0.0)
            mlflow.log_param("train_test_split", "80/20")
            mlflow.log_param("features", "house_size,location_score,age_years,rooms")
            mlflow.log_param("target", "price")
            
            # Metrics log et
            mlflow.log_metric("train_rmse", metrics["train_rmse"])
            mlflow.log_metric("test_rmse", metrics["test_rmse"])
            mlflow.log_metric("train_r2", metrics["train_r2"])
            mlflow.log_metric("test_r2", metrics["test_r2"])
            mlflow.log_metric("train_samples", metrics["train_samples"])
            mlflow.log_metric("test_samples", metrics["test_samples"])
            
            # Model coefficients
            mlflow.log_metric("intercept", lr_model.intercept)
            feature_names = ["house_size", "location_score", "age_years", "rooms"]
            for i, (name, coef) in enumerate(zip(feature_names, lr_model.coefficients)):
                mlflow.log_metric(f"coef_{name}", coef)
            
            # Model kaydet
            mlflow.spark.log_model(
                spark_model=lr_model,
                artifact_path="model",
                registered_model_name=MODEL_NAME
            )
            
            # Assembler kaydet (preprocessing için)
            mlflow.spark.log_model(
                spark_model=assembler,
                artifact_path="preprocessor"
            )
            
            run_id = mlflow.active_run().info.run_id
        
        print("✅ MLflow'a model kaydedildi!")
        print(f"📊 Run ID: {run_id}")
        print(f"📊 Model Name: {MODEL_NAME}")
        print(f"📊 MLflow UI: {MLFLOW_TRACKING_URI}")
        
        return run_id
        
    except Exception as e:
        print(f"❌ MLflow kayıt hatası: {e}")
        return None

def run_ml_training():
    """Ana ML training fonksiyonu"""
    
    spark = None
    
    try:
        print("🔗 TEST 3 - ML TRAINING BAŞLIYOR!")
        print("=" * 50)
        
        # Spark Session oluştur
        spark = create_spark_session()
        print("✅ Spark Session oluşturuldu")
        
        # 1. Rastgele veri üret
        df = generate_house_data(spark, num_samples=200)
        
        # 2. Features hazırla
        df_features, assembler = prepare_features(df)
        
        # 3. Model eğit
        lr_model, metrics = train_linear_regression(df_features)
        
        # 4. MLflow'a kaydet
        run_id = log_to_mlflow(lr_model, assembler, metrics)
        
        print("🎉 TEST 3 TAMAMLANDI!")
        print(f"✅ Linear Regression modeli eğitildi")
        print(f"✅ Model MLflow'a kaydedildi")
        print(f"📊 Test R² Score: {metrics['test_r2']:.4f}")
        print(f"📊 Test RMSE: {metrics['test_rmse']:,.2f} TL")
        
        return True
        
    except Exception as e:
        print(f"❌ ML TRAINING HATASI: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    success = run_ml_training()
    exit(0 if success else 1)