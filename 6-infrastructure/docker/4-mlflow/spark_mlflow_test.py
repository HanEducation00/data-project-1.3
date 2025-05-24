from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
import mlflow
import mlflow.spark
import random
import os

# MLflow sunucusuna bağlan
mlflow_tracking_uri = "http://mlflow-server:5000"
mlflow.set_tracking_uri(mlflow_tracking_uri)

# Spark oturumu oluştur - gerekli bağımlılıkları ekleyerek
spark = SparkSession.builder \
    .appName("Spark MLflow Test") \
    .config("spark.jars.packages", "org.mlflow:mlflow-spark:2.8.0,org.postgresql:postgresql:42.6.0") \
    .config("spark.hadoop.javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver") \
    .master("local[*]") \
    .getOrCreate()

# Basit bir veri seti oluştur
data = [(random.uniform(1.0, 100.0), random.uniform(1.0, 100.0)) for _ in range(100)]
df = spark.createDataFrame(data, ["feature", "label"])

# Özellik vektörü oluştur
assembler = VectorAssembler(inputCols=["feature"], outputCol="features")
df_assembled = assembler.transform(df)

# Veriyi eğitim ve test olarak böl
train_df, test_df = df_assembled.randomSplit([0.8, 0.2], seed=42)

# MLflow deneyini başlat
experiment_name = "spark-mlflow-test"
try:
    experiment_id = mlflow.create_experiment(experiment_name)
except:
    experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id

with mlflow.start_run(experiment_id=experiment_id):
    # Model parametrelerini ayarla
    lr = LinearRegression(featuresCol="features", labelCol="label", maxIter=10, regParam=0.3, elasticNetParam=0.8)

    # Modeli eğit
    lr_model = lr.fit(train_df)

    # Model performansını değerlendir
    predictions = lr_model.transform(test_df)
    evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)

    # Model parametrelerini ve metriklerini MLflow'a kaydet
    mlflow.log_param("maxIter", 10)
    mlflow.log_param("regParam", 0.3)
    mlflow.log_param("elasticNetParam", 0.8)
    mlflow.log_metric("rmse", rmse)

    # Modeli MLflow'a kaydet
    mlflow.spark.log_model(lr_model, "spark-model")

    print(f"Model eğitildi ve MLflow'a kaydedildi. RMSE: {rmse}")