### 1- SETUP ENVIRONMENT

- chmod +x scripts/setup/setup_environment.sh

- ./scripts/setup/setup_environment.sh

- conda activate data-platform-1.2

### 2- SETUP DOCKER-COMPOSE


### 3- TEST CONNECTION-SYSTEM TEST 

# Spark
curl http://localhost:8080

# MLflow
curl http://localhost:5000

# Airflow
curl http://localhost:8088/health


# Kafka broker'ların port'larını kontrol et:
nc -zv localhost 9191  # kafka1
nc -zv localhost 9292  # kafka2  
nc -zv localhost 9392  # kafka3

# Topic listesini göster:
docker exec development-kafka1 /kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
# Topic listesini sil:
docker exec development-kafka1 /kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic sensor-data

# Yeni topic oluştur (test için):
docker exec development-kafka1 /kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic sensor-data --partitions 3 --replication-factor 3

# Topic'i doğrula:
docker exec kafka1 /kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic zurna-test

# Producer test (mesaj gönder):
echo "Hello from Zurna Test!" | docker exec -i kafka1 /kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic zurna-test

# Consumer test (mesaj oku):
docker exec kafka1 /kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic zurna-test --from-beginning --max-messages 1

### POSTGRESQL TEST
# PostgreSQL connection test:
psql -h localhost -p 5432 -U datauser -d datawarehouse -c "SELECT version();"
# Ya da:
docker exec postgres pg_isready -U datauser -d datawarehouse
# Ya da:
psql -h localhost -p 5432 -U datauser -d datawarehouse

Username: datauser
Password: datapass
Database: datawarehouse

### Version kontrol
docker exec spark-master /opt/spark/bin/spark-submit --version

docker exec spark-client python3 --version

docker exec spark-master cat /opt/spark/RELEASE | grep -i scala

### TEST KAFKA-SPARK
- test1_kafka_producer.py oluştur.
# Güncellenmiş dosyayı kopyala
docker cp 4-integration-tests/test1/kafka_producer/test1_kafka_producer.py spark-client:/tmp/

# Yeniden çalıştır
docker exec spark-client python3 /tmp/test1_kafka_producer.py

- test2_spark_consumer.py oluştur.

docker exec postgres psql -U datauser -d datawarehouse -c "
DROP TABLE IF EXISTS test_house_data;
CREATE TABLE test_house_data (
    house_id INTEGER,
    house_size DECIMAL(10,2),
    location_score DECIMAL(3,1),
    price INTEGER,
    timestamp VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (house_id)
);"

# Güncellenmiş dosyayı kopyala
docker cp 4-integration-tests/test2/spark_consumer/test2_spark_consumer.py spark-client:/tmp/

# Yeniden çalıştır
docker exec spark-client python3 /tmp/test2_spark_consumer.py

### Linux Komutları
cd ~/projects/data-project-1.2/6-infrastructure/docker

### RUN COMAND
python -m streaming.app


#### spark client
pip install pyspark==3.4.0
apt-get update && apt-get install -y libgomp1
pip install mlflow==2.8.1


docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

🌐 Airflow (Workflow Management): http://localhost:8088
🌐 Spark Master (Cluster UI): http://localhost:8080
🌐 MLflow (ML Tracking): http://localhost:5000
🌐 MinIO (Object Storage): http://localhost:9000

# Development
docker-compose --env-file .env.development up -d

# Staging  
docker-compose --env-file .env.staging up -d

# Production
docker-compose --env-file .env.production up -d

# Kontrol
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep kafka
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep spark
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep airflow
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep mlflow
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep postgres 



-- En son saatlerdeki 5 veri
SELECT 
    customer_id,
    full_timestamp,
    hour,
    minute,
    load_percentage
FROM kafka_raw_data 
WHERE full_timestamp = (SELECT MAX(full_timestamp) FROM kafka_raw_data)
ORDER BY hour DESC, minute DESC, customer_id
LIMIT 5;



-- Hangi günlerde veri var ve en sonuncusu hangisi?
SELECT 
    DATE(full_timestamp) as data_date,
    COUNT(*) as record_count,
    MAX(hour) as last_hour,
    MAX(minute) as last_minute
FROM kafka_raw_data 
GROUP BY DATE(full_timestamp)
ORDER BY data_date DESC
LIMIT 10;
