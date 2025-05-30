#!/usr/bin/env python3
"""
Model Performance Monitoring DAG  
Günlük model accuracy kontrolü ve alert sistemi
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.bash import BashOperator
import logging

EMAIL_RECIPIENT = "han.oguz.education@gmail.com"

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 29),
    'email_on_failure': True,
    'email': [EMAIL_RECIPIENT], 
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}

dag = DAG(
    'model_performance_monitoring',
    default_args=default_args,
    description='Daily model performance monitoring and alerting',
    schedule_interval='0 6 * * *',  # Her gün sabah 06:00
    catchup=False,
    tags=['monitoring', 'model', 'alerting']
)

def check_model_accuracy(**context):
    """Son 24 saatte model accuracy'sini kontrol et"""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import avg, count, abs as spark_abs
    
    spark = SparkSession.builder \
        .appName("Model Performance Check") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        # Son 24 saatin tahminlerini al
        query = """
        (SELECT 
            daily_energy,
            predicted_daily_energy,
            ABS(predicted_daily_energy - daily_energy) / daily_energy * 100 as error_percentage,
            prediction_time
         FROM daily_predictions 
         WHERE prediction_time >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
        ) daily_performance
        """
        
        df = spark.read.jdbc(
            url="jdbc:postgresql://postgres:5432/datawarehouse",
            table=query,
            properties={
                "user": "datauser",
                "password": "datapass",
                "driver": "org.postgresql.Driver"
            }
        )
        
        if df.count() == 0:
            raise ValueError("❌ No predictions found in last 24 hours!")
        
        # Performance metrics hesapla
        performance = df.agg(
            avg("error_percentage").alias("avg_error"),
            count("*").alias("prediction_count")
        ).collect()[0]
        
        avg_error = performance['avg_error']
        prediction_count = performance['prediction_count']
        accuracy = 100 - avg_error
        
        logging.info(f"📊 Daily Performance: {accuracy:.2f}% accuracy, {prediction_count} predictions")
        
        # Store metrics
        context['task_instance'].xcom_push(key='accuracy', value=accuracy)
        context['task_instance'].xcom_push(key='prediction_count', value=prediction_count)
        context['task_instance'].xcom_push(key='avg_error', value=avg_error)
        
        # Accuracy threshold check
        ACCURACY_THRESHOLD = 85.0
        
        if accuracy < ACCURACY_THRESHOLD:
            error_msg = f"🚨 Model accuracy dropped to {accuracy:.2f}% (threshold: {ACCURACY_THRESHOLD}%)"
            context['task_instance'].xcom_push(key='alert_needed', value=True)
            context['task_instance'].xcom_push(key='alert_message', value=error_msg)
            logging.error(error_msg)
        else:
            context['task_instance'].xcom_push(key='alert_needed', value=False)
            logging.info(f"✅ Model performance is healthy: {accuracy:.2f}%")
        
        return f"Model accuracy: {accuracy:.2f}%"
        
    except Exception as e:
        logging.error(f"❌ Performance check failed: {e}")
        context['task_instance'].xcom_push(key='alert_needed', value=True)
        context['task_instance'].xcom_push(key='alert_message', value=f"Performance check failed: {str(e)}")
        raise
    finally:
        spark.stop()

def check_prediction_freshness(**context):
    """Tahminlerin güncel olup olmadığını kontrol et"""
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("Prediction Freshness Check") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        # En son tahmin zamanını al
        query = """
        (SELECT MAX(prediction_time) as last_prediction 
         FROM daily_predictions
        ) freshness_check
        """
        
        df = spark.read.jdbc(
            url="jdbc:postgresql://postgres:5432/datawarehouse",
            table=query,
            properties={
                "user": "datauser",
                "password": "datapass",
                "driver": "org.postgresql.Driver"
            }
        )
        
        last_prediction = df.collect()[0]['last_prediction']
        
        if last_prediction is None:
            raise ValueError("❌ No predictions found in database!")
        
        # Time difference check
        time_diff = datetime.now() - last_prediction
        hours_since_last = time_diff.total_seconds() / 3600
        
        context['task_instance'].xcom_push(key='hours_since_last', value=hours_since_last)
        
        # Freshness threshold (2 saat)
        FRESHNESS_THRESHOLD = 2.0
        
        if hours_since_last > FRESHNESS_THRESHOLD:
            alert_msg = f"🚨 No fresh predictions! Last prediction: {hours_since_last:.1f} hours ago"
            context['task_instance'].xcom_push(key='freshness_alert', value=True)
            context['task_instance'].xcom_push(key='freshness_message', value=alert_msg)
            logging.error(alert_msg)
        else:
            context['task_instance'].xcom_push(key='freshness_alert', value=False)
            logging.info(f"✅ Predictions are fresh: {hours_since_last:.1f} hours ago")
        
        return f"Last prediction: {hours_since_last:.1f} hours ago"
        
    except Exception as e:
        logging.error(f"❌ Freshness check failed: {e}")
        context['task_instance'].xcom_push(key='freshness_alert', value=True)
        context['task_instance'].xcom_push(key='freshness_message', value=f"Freshness check failed: {str(e)}")
        raise
    finally:
        spark.stop()

# Task definitions
accuracy_check = PythonOperator(
    task_id='check_model_accuracy',
    python_callable=check_model_accuracy,
    dag=dag
)

freshness_check = PythonOperator(
    task_id='check_prediction_freshness',
    python_callable=check_prediction_freshness,
    dag=dag
)

# Conditional email alerts
def send_alert_if_needed(**context):
    """Gerekirse alert email gönder"""
    accuracy_alert = context['task_instance'].xcom_pull(key='alert_needed')
    freshness_alert = context['task_instance'].xcom_pull(key='freshness_alert')
    
    if accuracy_alert or freshness_alert:
        return 'send_alert_email'
    else:
        return 'send_daily_report'

alert_decision = PythonOperator(
    task_id='alert_decision',
    python_callable=send_alert_if_needed,
    dag=dag
)

# Alert email
alert_email = EmailOperator(
    task_id='send_alert_email',
    to=[EMAIL_RECIPIENT],
    subject='🚨 CRITICAL: Model Performance Alert',
    html_content="""
    <h2>🚨 Model Performance Alert</h2>
    <p><strong>Date:</strong> {{ ds }}</p>
    
    {% if task_instance.xcom_pull(key='alert_needed') %}
    <h3>❌ Accuracy Alert:</h3>
    <p>{{ task_instance.xcom_pull(key='alert_message') }}</p>
    <p><strong>Current Accuracy:</strong> {{ task_instance.xcom_pull(key='accuracy') }}%</p>
    {% endif %}
    
    {% if task_instance.xcom_pull(key='freshness_alert') %}
    <h3>⏰ Freshness Alert:</h3>
    <p>{{ task_instance.xcom_pull(key='freshness_message') }}</p>
    <p><strong>Hours Since Last:</strong> {{ task_instance.xcom_pull(key='hours_since_last') }}</p>
    {% endif %}
    
    <p><em>Please check the ML pipeline immediately!</em></p>
    """,
    dag=dag
)

# Daily report email
daily_report = EmailOperator(
    task_id='send_daily_report',
    to=[EMAIL_RECIPIENT],
    subject='📊 Daily Model Performance Report',
    html_content="""
    <h3>📊 Daily Model Performance Report</h3>
    <p><strong>Date:</strong> {{ ds }}</p>
    <p><strong>✅ Status:</strong> All systems healthy</p>
    <p><strong>Model Accuracy:</strong> {{ task_instance.xcom_pull(key='accuracy') }}%</p>
    <p><strong>Predictions Made:</strong> {{ task_instance.xcom_pull(key='prediction_count') }}</p>
    <p><strong>Avg Error:</strong> {{ task_instance.xcom_pull(key='avg_error') }}%</p>
    <p><strong>Last Prediction:</strong> {{ task_instance.xcom_pull(key='hours_since_last') }} hours ago</p>
    """,
    dag=dag
)

# Dependencies
[accuracy_check, freshness_check] >> alert_decision
alert_decision >> [alert_email, daily_report]
