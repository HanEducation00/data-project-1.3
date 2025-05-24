from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta
import json
import requests

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'integration_test_dag_modular',
    default_args=default_args,
    description='Modular Full Stack Integration Test DAG',
    schedule_interval=None,  # Manuel trigger
    catchup=False,
    tags=['integration', 'test', 'modular', 'spark', 'mlflow', 'kafka']
)

# Individual test tasks (paralel çalıştırılabilir)
# 1. Spark testi - SparkSubmitOperator kullanarak
spark_session_test = SparkSubmitOperator(
    task_id='test_spark_session',
    application='/integration-tests/spark_tests/spark_session_test.py',
    conn_id='spark_default',
    verbose=True,
    dag=dag
)

# 2. PostgreSQL testi - PostgresOperator kullanarak
postgresql_connectivity_test = PostgresOperator(
    task_id='test_postgresql_connectivity',
    postgres_conn_id='postgres_default',
    sql="SELECT COUNT(*) FROM integration_test;",
    dag=dag
)

# 3. MLflow testi - HttpSensor kullanarak
mlflow_server_test = HttpSensor(
    task_id='test_mlflow_server',
    http_conn_id='mlflow_conn',  # Airflow'da tanımlanmalı - http://mlflow-server:5000
    endpoint='/health',
    response_check=lambda response: response.status_code == 200,
    poke_interval=5,
    timeout=30,
    dag=dag
)

# 4. Kafka testi - Python Operator kullanarak
def test_kafka_connection():
    from kafka import KafkaAdminClient
    from kafka.errors import KafkaError
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=["kafka1:9092"],
            client_id='airflow-integration-test'
        )
        topics = admin_client.list_topics()
        print(f"Kafka topics: {topics}")
        admin_client.close()
        return True
    except KafkaError as e:
        print(f"Kafka bağlantı hatası: {e}")
        raise e

kafka_cluster_test = PythonOperator(
    task_id='test_kafka_cluster',
    python_callable=test_kafka_connection,
    dag=dag
)

# Ana integration test - SparkSubmitOperator kullanarak
main_integration_test = SparkSubmitOperator(
    task_id='run_main_integration_test',
    application='/integration-tests/main_integration_test.py',
    conn_id='spark_default',
    verbose=True,
    dag=dag
)

# Post-test verification tasks
# MLflow runs kontrolü - PythonOperator kullanarak
def verify_mlflow_experiments():
    response = requests.get("http://mlflow-server:5000/api/2.0/mlflow/runs/search")
    if response.status_code != 200:
        raise Exception(f"MLflow API erişimi başarısız: {response.status_code}")
    
    runs = json.loads(response.text)
    integration_runs = [
        run for run in runs.get('runs', []) 
        if 'Integration_Test' in run.get('info', {}).get('run_name', '')
    ]
    
    print(f"Integration test runs: {json.dumps(integration_runs, indent=2)}")
    return len(integration_runs) > 0

verify_mlflow_runs = PythonOperator(
    task_id='verify_mlflow_runs',
    python_callable=verify_mlflow_experiments,
    dag=dag
)

# Kafka mesaj kontrolü - PythonOperator kullanarak
def verify_kafka_messages_func():
    from kafka import KafkaConsumer
    import json
    
    consumer = KafkaConsumer(
        "integration-test-results",
        bootstrap_servers=["kafka1:9092"],
        auto_offset_reset="earliest",
        group_id="airflow-verification",
        consumer_timeout_ms=10000  # 10 saniye timeout
    )
    
    messages = []
    for message in consumer:
        messages.append(message.value)
        if len(messages) >= 1:
            break
    
    consumer.close()
    
    print(f"Kafka'dan okunan mesaj sayısı: {len(messages)}")
    return len(messages) > 0

verify_kafka_messages = PythonOperator(
    task_id='verify_kafka_messages',
    python_callable=verify_kafka_messages_func,
    dag=dag
)

# Sistem sağlık kontrolü - PythonOperator kullanarak
def check_system_health():
    # Burada HTTP istekleri ile servislerin durumunu kontrol edebilirsiniz
    # Örnek olarak MLflow, Spark UI, PostgreSQL durumunu kontrol edelim
    
    services = {
        "MLflow": "http://mlflow-server:5000/health",
        "Spark Master": "http://spark-master:8080",
        "PostgreSQL": "postgres_default"  # Airflow connection kullanılacak
    }
    
    results = {}
    
    # HTTP servisleri kontrol et
    for name, url in services.items():
        if name != "PostgreSQL":
            try:
                response = requests.get(url, timeout=5)
                results[name] = {
                    "status": "UP" if response.status_code == 200 else "DOWN",
                    "details": f"HTTP Status: {response.status_code}"
                }
            except Exception as e:
                results[name] = {"status": "ERROR", "details": str(e)}
    
    # PostgreSQL kontrolü
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id=services["PostgreSQL"])
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        results["PostgreSQL"] = {"status": "UP", "details": "Connection successful"}
    except Exception as e:
        results["PostgreSQL"] = {"status": "ERROR", "details": str(e)}
    
    print(f"Sistem sağlık durumu: {json.dumps(results, indent=2)}")
    return all(item["status"] == "UP" for item in results.values())

system_health_check = PythonOperator(
    task_id='system_health_check',
    python_callable=check_system_health,
    dag=dag
)

# Task dependencies - Paralel pre-checks, sonra ana test, sonra verification
# Pre-checks (paralel)
pre_checks = [
    spark_session_test,
    postgresql_connectivity_test,
    mlflow_server_test,
    kafka_cluster_test
]

# Ana test
pre_checks >> main_integration_test

# Post-test verifications (paralel)
post_verifications = [
    verify_mlflow_runs,
    verify_kafka_messages,
    system_health_check
]

# Son doğrulamalar
main_integration_test >> post_verifications
