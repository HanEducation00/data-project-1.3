#!/usr/bin/env python3
"""
Summer Training - SparkSubmitOperator ile Profesyonel Çözüm
Endüstri Standardı: Airflow → Spark Cluster → Python Script
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def check_spark_cluster_health():
    """Spark Master ve Worker'ların sağlığını kontrol et"""
    import subprocess
    import json
    
    try:
        # Spark Master durumunu kontrol et
        result = subprocess.run(
            ["curl", "-s", "http://development-spark-master:8080/json/"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode != 0:
            raise Exception("❌ Spark Master API'ye erişim başarısız!")
        
        master_info = json.loads(result.stdout)
        alive_workers = master_info.get('aliveworkers', 0)
        total_cores = master_info.get('cores', 0)
        total_memory = master_info.get('memory', 0)
        
        print(f"✅ Spark Master aktif")
        print(f"🔧 Alive Workers: {alive_workers}")
        print(f"💾 Total Cores: {total_cores}")
        print(f"🧠 Total Memory: {total_memory} MB")
        
        if alive_workers < 1:
            raise Exception("❌ Hiç aktif worker yok!")
        
        if total_cores < 1:
            raise Exception("❌ Kullanılabilir core yok!")
            
        return True
        
    except Exception as e:
        print(f"❌ Spark cluster sağlık kontrolü başarısız: {e}")
        raise

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False
}

dag = DAG(
    'summer_training_spark_submit_pro',
    default_args=default_args,
    description='☀️ Summer Training - SparkSubmitOperator (Professional)',
    schedule_interval='*/15 * * * *',  # Her 15 dakikada bir
    catchup=False,
    max_active_runs=1,
    tags=['ml', 'seasonal', 'spark-submit', 'professional']
)

# 1. Spark Cluster sağlık kontrolü
check_spark_health = PythonOperator(
    task_id='check_spark_cluster_health',
    python_callable=check_spark_cluster_health,
    dag=dag
)

# 2. 🚀 SparkSubmitOperator - PROFESYONEL ÇÖZÜM
summer_model_training = SparkSubmitOperator(
    task_id='spark_submit_summer_training',
    
    # 📍 Spark Connection
    conn_id='spark_default',
    
    # 📁 Application Path
    application='/workspace/pipelines/4-silver_layer/seasonal_models/summer_training.py',
    
    # 📦 Dependencies
    packages='org.postgresql:postgresql:42.7.2',
    
    # 🎭 Application Arguments
    application_args=['2016'],
    
    # 📊 Resource Configuration
    executor_cores=1,
    executor_memory='1g',
    driver_memory='2g',
    num_executors=2,
    
    # 🔧 Spark Configuration
    conf={
        'spark.master': 'spark://development-spark-master:7077',
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        'spark.driver.maxResultSize': '2g',
        'spark.network.timeout': '300s',
        'spark.executor.heartbeatInterval': '60s'
    },
    
    # 📝 Naming
    name='Summer_Season_Model_Training_{{ ds }}',
    
    # 🔄 Deploy Mode
    deploy_mode='client',
    
    # 📋 Verbose logging
    verbose=True,
    
    dag=dag
)

# 3. Training sonuçlarını kontrol et
check_training_results = BashOperator(
    task_id='verify_training_completion',
    bash_command="""
    echo "📊 Summer model eğitimi sonuçları kontrol ediliyor..."
    
    # Spark application loglarını kontrol et
    if docker exec development-spark-client bash -c "ls -la /workspace/logs/summer_training_*.log 2>/dev/null"; then
        echo "✅ Log dosyaları bulundu"
        
        # Son log dosyasındaki başarı mesajını ara
        LATEST_LOG=$(docker exec development-spark-client bash -c "ls -t /workspace/logs/summer_training_*.log 2>/dev/null | head -1")
        
        if [ ! -z "$LATEST_LOG" ]; then
            echo "📖 Son log dosyası: $LATEST_LOG"
            
            if docker exec development-spark-client grep -q "Model eğitimi tamamlandı" "$LATEST_LOG"; then
                echo "🎉 Summer model eğitimi başarıyla tamamlandı!"
                
                # Performans metriklerini göster
                echo "📈 Performans Metrikleri:"
                docker exec development-spark-client grep -A 3 "Model performansı:" "$LATEST_LOG" || echo "Metrik bulunamadı"
            else
                echo "⚠️  Summer model eğitimi durumu belirsiz"
            fi
        fi
    else
        echo "📝 Henüz log dosyası oluşmamış"
    fi
    """,
    dag=dag
)

# 4. Spark application geçmişini temizle
cleanup_spark_history = BashOperator(
    task_id='cleanup_spark_applications',
    bash_command="""
    echo "🧹 Eski Summer Spark application'ları temizleniyor..."
    
    echo "🔍 Aktif application sayısı kontrol ediliyor..."
    ACTIVE_APPS=$(curl -s http://development-spark-master:8080/json/ | grep -o '"activeapps":[0-9]*' | cut -d: -f2 || echo "0")
    COMPLETED_APPS=$(curl -s http://development-spark-master:8080/json/ | grep -o '"completedapps":[0-9]*' | cut -d: -f2 || echo "0")
    
    echo "📊 Aktif Applications: $ACTIVE_APPS"
    echo "📊 Tamamlanan Applications: $COMPLETED_APPS"
    
    # Eski log dosyalarını temizle
    docker exec development-spark-client bash -c "
        find /workspace/logs -name 'summer_training_*.log' -mtime +7 -delete 2>/dev/null || true
        echo '✅ 7+ günlük summer log dosyları temizlendi'
    "
    
    echo "✅ Summer Spark geçmişi temizleme tamamlandı"
    """,
    trigger_rule='all_done',  # Başarılı/başarısız fark etmez
    dag=dag
)

# 🔗 Task Dependencies - Profesyonel Flow
check_spark_health >> summer_model_training >> check_training_results >> cleanup_spark_history
