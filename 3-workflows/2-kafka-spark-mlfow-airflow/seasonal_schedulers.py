#!/usr/bin/env python3
"""
Mevsimsel ML Model Trigger DAG'ları
Her modeli bağımsız olarak tetikler
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
import logging

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}

# SPRING TRIGGER DAG
spring_dag = DAG(
    'trigger_spring_training',
    default_args=default_args,
    description='Spring ML model eğitimi tetikleyici',
    schedule_interval='*/2 * * * *',  # Her 2 dakikada bir çalışır
    catchup=False,
    max_active_runs=1,
    tags=['ml', 'spring', 'seasonal']
)

def log_spring_trigger():
    """Spring model tetikleme loglama"""
    logger.info("🌸 Spring model eğitimi tetikleniyor...")
    logger.info("📅 Hedef veri aralığı: 1 Ocak - 31 Mayıs 2016")
    return "spring_trigger_completed"

spring_log_task = PythonOperator(
    task_id='log_spring_trigger',
    python_callable=log_spring_trigger,
    dag=spring_dag
)

spring_trigger_task = TriggerDagRunOperator(
    task_id='trigger_spring_ml',
    trigger_dag_id='seasonal_ml_training',
    conf={'model_type': 'spring', 'target_date': '2016-05-31'},
    wait_for_completion=False,
    poke_interval=30,
    dag=spring_dag
)

spring_log_task >> spring_trigger_task

# SUMMER TRIGGER DAG
summer_dag = DAG(
    'trigger_summer_training',
    default_args=default_args,
    description='Summer ML model eğitimi tetikleyici',
    schedule_interval='*/2 * * * *',  # Her 2 dakikada bir çalışır
    catchup=False,
    max_active_runs=1,
    tags=['ml', 'summer', 'seasonal']
)

def log_summer_trigger():
    """Summer model tetikleme loglama"""
    logger.info("☀️ Summer model eğitimi tetikleniyor...")
    logger.info("📅 Hedef veri aralığı: 1 Haziran - 30 Eylül 2016")
    return "summer_trigger_completed"

summer_log_task = PythonOperator(
    task_id='log_summer_trigger',
    python_callable=log_summer_trigger,
    dag=summer_dag
)

summer_trigger_task = TriggerDagRunOperator(
    task_id='trigger_summer_ml',
    trigger_dag_id='seasonal_ml_training',
    conf={'model_type': 'summer', 'target_date': '2016-09-30'},
    wait_for_completion=False,
    poke_interval=30,
    dag=summer_dag
)

summer_log_task >> summer_trigger_task

# AUTUMN TRIGGER DAG
autumn_dag = DAG(
    'trigger_autumn_training',
    default_args=default_args,
    description='Autumn ML model eğitimi tetikleyici',
    schedule_interval='*/2 * * * *',  # Her 2 dakikada bir çalışır
    catchup=False,
    max_active_runs=1,
    tags=['ml', 'autumn', 'seasonal']
)

def log_autumn_trigger():
    """Autumn model tetikleme loglama"""
    logger.info("🍂 Autumn model eğitimi tetikleniyor...")
    logger.info("📅 Hedef veri aralığı: 1 Ekim - 31 Aralık 2016")
    return "autumn_trigger_completed"

autumn_log_task = PythonOperator(
    task_id='log_autumn_trigger',
    python_callable=log_autumn_trigger,
    dag=autumn_dag
)

autumn_trigger_task = TriggerDagRunOperator(
    task_id='trigger_autumn_ml',
    trigger_dag_id='seasonal_ml_training',
    conf={'model_type': 'autumn', 'target_date': '2016-12-31'},
    wait_for_completion=False,
    poke_interval=30,
    dag=autumn_dag
)

autumn_log_task >> autumn_trigger_task
