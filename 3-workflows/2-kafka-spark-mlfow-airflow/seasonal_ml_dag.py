#!/usr/bin/env python3
"""
Mevsimsel ML Model Eğitimi DAG'ı
Tarih bazlı veri kontrolü ile if-else mantığı
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
import subprocess
import sys
import os

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

# DAG tanımı
dag = DAG(
    'seasonal_ml_training',
    default_args=default_args,
    description='Mevsimsel ML model eğitimi - tarih bazlı kontrol',
    schedule_interval=None, 
    catchup=False,
    max_active_runs=1
)

def check_spring_data_and_decide():
    """Spring veri kontrolü ve karar verme"""
    try:
        # Spring training modülünü import et ve kontrol et
        sys.path.append('/workspace/pipelines/4-silver_layer')
        from seasonal_models.spring_training import check_data_availability, create_spark_session
        
        spark = create_spark_session("Spring Data Check")
        result = check_data_availability(spark, 2016)
        spark.stop()
        
        if result:
            print("✅ Spring verisi hazır - eğitime başla!")
            return 'spring_training_task'
        else:
            print("⏳ Spring verisi henüz hazır değil - atla")
            return 'spring_skip_task'
        
    except Exception as e:
        print(f"❌ Spring veri kontrolü hatası: {e}")
        return 'spring_skip_task'

def check_summer_data_and_decide():
    """Summer veri kontrolü ve karar verme"""
    try:
        sys.path.append('/workspace/pipelines/4-silver_layer')
        from seasonal_models.summer_training import check_data_availability, create_spark_session
        
        spark = create_spark_session("Summer Data Check")
        result = check_data_availability(spark, 2016)
        spark.stop()
        
        if result:
            print("✅ Summer verisi hazır - eğitime başla!")
            return 'summer_training_task'
        else:
            print("⏳ Summer verisi henüz hazır değil - atla")
            return 'summer_skip_task'
        
    except Exception as e:
        print(f"❌ Summer veri kontrolü hatası: {e}")
        return 'summer_skip_task'

def check_autumn_data_and_decide():
    """Autumn veri kontrolü ve karar verme"""
    try:
        sys.path.append('/workspace/pipelines/4-silver_layer')
        from seasonal_models.autumn_training import check_data_availability, create_spark_session
        
        spark = create_spark_session("Autumn Data Check")
        result = check_data_availability(spark, 2016)
        spark.stop()
        
        if result:
            print("✅ Autumn verisi hazır - eğitime başla!")
            return 'autumn_training_task'
        else:
            print("⏳ Autumn verisi henüz hazır değil - atla")
            return 'autumn_skip_task'
        
    except Exception as e:
        print(f"❌ Autumn veri kontrolü hatası: {e}")
        return 'autumn_skip_task'

# SPRING MODEL TASKS
spring_data_check = BranchPythonOperator(
    task_id='spring_data_check',
    python_callable=check_spring_data_and_decide,
    dag=dag
)

spring_training_task = BashOperator(
    task_id='spring_training_task',
    bash_command='cd /workspace/pipelines/4-silver_layer/seasonal_models && python spring_training.py',
    dag=dag
)

spring_skip_task = DummyOperator(
    task_id='spring_skip_task',
    dag=dag
)

# SUMMER MODEL TASKS  
summer_data_check = BranchPythonOperator(
    task_id='summer_data_check',
    python_callable=check_summer_data_and_decide,
    dag=dag
)

summer_training_task = BashOperator(
    task_id='summer_training_task',
    bash_command='cd /workspace/pipelines/4-silver_layer/seasonal_models && python summer_training.py',
    dag=dag
)

summer_skip_task = DummyOperator(
    task_id='summer_skip_task',
    dag=dag
)

# AUTUMN MODEL TASKS
autumn_data_check = BranchPythonOperator(
    task_id='autumn_data_check',
    python_callable=check_autumn_data_and_decide,
    dag=dag
)

autumn_training_task = BashOperator(
    task_id='autumn_training_task',
    bash_command='cd /workspace/pipelines/4-silver_layer/seasonal_models && python autumn_training.py',
    dag=dag
)

autumn_skip_task = DummyOperator(
    task_id='autumn_skip_task',
    dag=dag
)

# Task Dependencies (Paralel çalışacak)
spring_data_check >> [spring_training_task, spring_skip_task]
summer_data_check >> [summer_training_task, summer_skip_task]  
autumn_data_check >> [autumn_training_task, autumn_skip_task]
