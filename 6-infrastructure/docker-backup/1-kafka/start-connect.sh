#!/bin/bash

# Wait for Kafka brokers to be ready
echo "Waiting for Kafka to be ready..."
sleep 20

# Check if Kafka cluster is accessible
MAX_RETRIES=30
RETRY_INTERVAL=10
COUNTER=0

while [ $COUNTER -lt $MAX_RETRIES ]; do
    if /kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --list > /dev/null 2>&1; then
        echo "Kafka cluster is accessible. Starting Kafka Connect..."
        break
    else
        echo "Waiting for Kafka cluster to become accessible... ($((COUNTER+1))/$MAX_RETRIES)"
        sleep $RETRY_INTERVAL
        let COUNTER=COUNTER+1
    fi
done

if [ $COUNTER -eq $MAX_RETRIES ]; then
    echo "Timed out waiting for Kafka cluster to become accessible."
    exit 1
fi

# Start Kafka Connect in distributed mode
exec /kafka/bin/connect-distributed.sh /kafka/config/connect-distributed.properties
