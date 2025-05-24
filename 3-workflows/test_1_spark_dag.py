from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta
import requests
import json

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# DAG tanımı
dag = DAG(
    'test2_spark_health_check',
    default_args=default_args,
    description='Spark Cluster Health Check DAG',
    schedule_interval='0 */3 * * *',
    catchup=False,
    tags=['test', 'spark', 'health-check']
)

# Başlangıç task'ı
start_health_check = DummyOperator(
    task_id='start_spark_health_check',
    dag=dag
)

# Spark Master UI erişilebilirlik kontrolü
spark_master_ui_check = HttpSensor(
    task_id='check_spark_master_ui',
    http_conn_id='spark_ui',
    endpoint='/',
    response_check=lambda response: response.status_code == 200,
    poke_interval=5,
    timeout=20,
    dag=dag
)

# Spark Worker sayısı ve durumu kontrolü
def check_spark_workers():
    """Spark worker sayısını ve durumunu kontrol eder"""
    try:
        # Spark Master API'ye istek at
        response = requests.get("http://spark-master:8080/json/")
        if response.status_code != 200:
            print(f"❌ Spark Master API erişilemez: {response.status_code}")
            return False
            
        # JSON yanıtını parse et
        data = response.json()
        
        # Worker sayısını kontrol et
        alive_workers = [w for w in data.get('workers', []) if w.get('state') == 'ALIVE']
        worker_count = len(alive_workers)
        
        print(f"📊 Aktif Spark worker sayısı: {worker_count}")
        
        # En az 2 worker olmalı
        if worker_count < 2:
            print("⚠️ Yetersiz Spark worker sayısı! En az 2 worker gerekli.")
            return False
        
        print("✅ Spark cluster sağlıklı çalışıyor!")
        return True
        
    except Exception as e:
        print(f"❌ Spark worker kontrolü başarısız: {str(e)}")
        return False

spark_workers_check = PythonOperator(
    task_id='check_spark_workers',
    python_callable=check_spark_workers,
    dag=dag
)

# Basit bir Spark bağlantı testi
def test_spark_connection():
    """Spark master'a basit bağlantı testi"""
    import socket
    
    try:
        # Spark Master'a socket bağlantısı dene
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect(("spark-master", 7077))
        s.close()
        
        print("✅ Spark Master port 7077 erişilebilir!")
        return True
    except Exception as e:
        print(f"❌ Spark Master port bağlantı hatası: {str(e)}")
        return False

spark_conn_test = PythonOperator(
    task_id='test_spark_connection',
    python_callable=test_spark_connection,
    dag=dag
)

# Bitiş task'ı
end_health_check = DummyOperator(
    task_id='end_spark_health_check',
    dag=dag
)

# Task akışı
start_health_check >> [spark_master_ui_check, spark_workers_check, spark_conn_test] >> end_health_check
