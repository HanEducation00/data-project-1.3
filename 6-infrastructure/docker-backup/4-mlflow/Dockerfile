FROM python:3.10-slim

# Sistem bağımlılıklarını yükle
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# MLflow ve gerekli paketleri yükle
RUN pip install --no-cache-dir \
    mlflow==2.9.2 \
    psycopg2-binary \
    boto3 \
    pymysql \
    sqlalchemy

# Çalışma dizini oluştur
WORKDIR /mlflow

# Sağlık kontrolü için basit bir script ekle
COPY healthcheck.py /mlflow/healthcheck.py

# Varsayılan port
EXPOSE 5000

# Sağlık kontrolü
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD python /mlflow/healthcheck.py

# Entrypoint
ENTRYPOINT ["mlflow"]
