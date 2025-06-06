#!/usr/bin/env python3
"""
Spark Connection Setup for Airflow
SparkSubmitOperator için connection yapılandırması
"""

# 🔧 AIRFLOW CONNECTION SETUP KOMUTU:
"""
Bu komutu Airflow container içinde çalıştırın:

airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'development-spark-master' \
    --conn-port 7077 \
    --conn-extra '{"master": "spark://development-spark-master:7077", "deploy-mode": "client"}'
"""

def setup_spark_connection():
    """Programmatik olarak Spark connection oluştur"""
    from airflow.models import Connection
    from airflow import settings
    
    # Spark connection
    new_conn = Connection(
        conn_id='spark_default',
        conn_type='spark',
        host='development-spark-master',
        port=7077,
        extra='{"master": "spark://development-spark-master:7077", "deploy-mode": "client"}'
    )
    
    session = settings.Session()
    
    # Mevcut connection'ı sil (varsa)
    existing = session.query(Connection).filter(Connection.conn_id == 'spark_default').first()
    if existing:
        session.delete(existing)
    
    # Yeni connection'ı ekle
    session.add(new_conn)
    session.commit()
    session.close()
    
    print("✅ Spark connection başarıyla oluşturuldu!")

if __name__ == "__main__":
    setup_spark_connection()
