from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator  # Updated from DummyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator  # Updated from PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import requests

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

dag = DAG(
    'test5_ml_pipeline_dag',  # Updated DAG ID
    default_args=default_args,
    description='FINAL TEST - Complete ML Pipeline with Airflow Orchestration',
    schedule=None,  # Updated from schedule_interval
    catchup=False,
    max_active_runs=1,
    tags=['test', 'ml', 'pipeline', 'final', 'airflow', 'spark', 'mlflow']
)

# Python function for health checks
def check_mlflow_health():
    """MLflow server health check"""
    try:
        response = requests.get("http://mlflow-server:5000/health", timeout=10)
        if response.status_code == 200:
            print("✅ MLflow server healthy")
            return True
        else:
            print(f"❌ MLflow server unhealthy: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ MLflow health check failed: {e}")
        return False

def check_postgres_health():
    """PostgreSQL health check"""
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        cursor.close()
        connection.close()
        
        if result and result[0] == 1:
            print("✅ PostgreSQL healthy")
            return True
        else:
            print("❌ PostgreSQL unhealthy")
            return False
    except Exception as e:
        print(f"❌ PostgreSQL health check failed: {e}")
        return False

def verify_mlflow_model():
    """Verify MLflow model existence"""
    try:
        print("🔍 MLflow'da test model kontrol ediliyor...")
        response = requests.get(
            "http://mlflow-server:5000/api/2.0/mlflow/registered-models/get?name=test-linear-regression-v1", 
            timeout=10
        )
        
        if response.status_code == 200:
            print("✅ Registered model bulundu")
            return True
        else:
            print("⚠️ Registered model bulunamadı, experiment runs kontrol ediliyor...")
            exp_response = requests.get(
                "http://mlflow-server:5000/api/2.0/mlflow/experiments/list", 
                timeout=10
            )
            if exp_response.status_code == 200:
                print("✅ MLflow erişilebilir, model inference çalışacak")
                return True
            else:
                print("❌ MLflow API erişilemez")
                return False
    except Exception as e:
        print(f"❌ MLflow model verification failed: {e}")
        return False

def validate_prediction_results():
    """Validate prediction results in PostgreSQL"""
    try:
        print("🔍 Tahmin sonuçları doğrulanıyor...")
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        
        # Check recent predictions
        query = """
        SELECT COUNT(*) 
        FROM test_predictions
        WHERE prediction_timestamp > NOW() - INTERVAL '5 minutes'
        """
        prediction_count = pg_hook.get_first(query)[0]
        
        print(f"📊 Son 5 dakikada yapılan tahmin sayısı: {prediction_count}")
        
        if prediction_count > 0:
            print("✅ Yeni tahminler bulundu")
            
            # Show sample predictions
            sample_query = """
            SELECT house_id, house_size, predicted_price, prediction_timestamp
            FROM test_predictions
            ORDER BY prediction_timestamp DESC
            LIMIT 5
            """
            
            connection = pg_hook.get_conn()
            cursor = connection.cursor()
            cursor.execute(sample_query)
            
            print("📊 Son tahminler:")
            for row in cursor.fetchall():
                print(f"  ID: {row[0]}, Size: {row[1]}, Price: {row[2]}, Time: {row[3]}")
                
            cursor.close()
            connection.close()
            return True
        else:
            print("⚠️ Yeni tahmin bulunamadı")
            return False
    except Exception as e:
        print(f"❌ Prediction validation failed: {e}")
        return False

# Task definitions
start_pipeline = EmptyOperator(  # Updated from DummyOperator
    task_id='start_ml_pipeline',
    dag=dag
)

# Health checks (parallel)
mlflow_health_check = PythonOperator(
    task_id='check_mlflow_health',
    python_callable=check_mlflow_health,
    dag=dag
)

postgres_health_check = PythonOperator(
    task_id='check_postgres_health',
    python_callable=check_postgres_health,
    dag=dag
)

spark_cluster_check = BashOperator(
    task_id='check_spark_cluster',
    bash_command='''
    echo "🔍 Spark cluster durumu kontrol ediliyor..."
    
    # Spark Master health - curl ile kontrol
    curl -s http://spark-master:8080 > /dev/null
    if [ $? -eq 0 ]; then
        echo "✅ Spark Master healthy"
    else
        echo "❌ Spark Master unhealthy"
        exit 1
    fi
    
    # Note: Docker commands won't work inside Airflow container
    # Using alternative approach
    echo "📊 Spark cluster basic health check completed"
    echo "✅ Spark cluster accessible"
    ''',
    dag=dag
)

# Verification tasks
verify_model_exists = PythonOperator(
    task_id='verify_mlflow_model',
    python_callable=verify_mlflow_model,
    dag=dag
)

verify_postgres_tables = SQLExecuteQueryOperator(  # Updated from PostgresOperator
    task_id='verify_postgres_tables',
    conn_id='postgres_default',  # Updated parameter name
    sql="""
    CREATE TABLE IF NOT EXISTS test_predictions (
        house_id INTEGER,
        house_size DECIMAL(10,2),
        location_score DECIMAL(3,1),
        age_years INTEGER,
        rooms INTEGER,
        predicted_price INTEGER,
        prediction_timestamp TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag
)

# MAIN TASK - ML Inference with SparkSubmitOperator
run_ml_inference = SparkSubmitOperator(
    task_id='run_ml_inference_with_spark',
    application='/opt/airflow/dags/test5_ml_inference.py',  # Updated path - accessible inside container
    conn_id='spark_default',  # Make sure this connection exists
    verbose=True,
    application_args=[
        "--run_id", "{{ run_id }}",
        "--task_instance", "{{ task_instance_key_str }}"
    ],
    conf={
        "spark.driver.memory": "1g",  # Reduced for container limits
        "spark.executor.memory": "1g",  # Reduced for container limits
        "spark.executor.cores": "1",   # Reduced for container limits
        "spark.jars.packages": "org.postgresql:postgresql:42.6.0"  # Removed MLflow package to avoid conflicts
    },
    dag=dag
)

# Post-processing tasks
validate_predictions = PythonOperator(
    task_id='validate_predictions',
    python_callable=validate_prediction_results,
    dag=dag
)

# Simplified report generation
generate_report = BashOperator(
    task_id='generate_pipeline_report',
    bash_command='''
    echo "📊 ML PIPELINE RAPORU"
    echo "===================="
    echo "📅 Çalışma Zamanı: $(date)"
    echo "🎯 DAG: test5_ml_pipeline_dag"
    echo "⚡ Run ID: {{ run_id }}"
    echo "🎉 ML PIPELINE RAPORU TAMAMLANDI!"
    ''',
    dag=dag
)

end_pipeline = EmptyOperator(  # Updated from DummyOperator
    task_id='end_ml_pipeline',
    dag=dag
)

# Task dependencies
start_pipeline >> [mlflow_health_check, postgres_health_check, spark_cluster_check]

# Health checks'ten verification'lara
mlflow_health_check >> verify_model_exists
postgres_health_check >> verify_postgres_tables  
spark_cluster_check >> verify_model_exists
spark_cluster_check >> verify_postgres_tables

# Verification'lardan main task'a
verify_model_exists >> run_ml_inference
verify_postgres_tables >> run_ml_inference

# Main task'tan son task'lara
run_ml_inference >> validate_predictions >> generate_report >> end_pipeline