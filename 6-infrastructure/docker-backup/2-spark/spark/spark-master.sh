#!/bin/bash

echo "Starting Spark Master..."
echo "$(hostname -i) spark-master" >> /etc/hosts

# Start history server
start-history-server.sh &

# Start master
/opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
    --ip 0.0.0.0 \
    --port 7077 \
    --webui-port 8080
