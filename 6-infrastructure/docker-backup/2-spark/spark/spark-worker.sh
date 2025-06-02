#!/bin/bash
echo "Starting Spark Worker..."

# Master'ı bekle - loop ile
while ! getent hosts spark-master; do
  echo "Waiting for spark-master to be available..."
  sleep 5
done

echo "Master found, starting worker..."
/opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
    spark://spark-master:7077 \
    --webui-port 8081
