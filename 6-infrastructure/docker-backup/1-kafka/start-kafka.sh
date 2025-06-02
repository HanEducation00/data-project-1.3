#!/bin/bash

# KRaft cluster ID
CLUSTER_ID=$(tr -dc 'a-zA-Z0-9' < /dev/urandom | head -c 32)

# Check if meta.properties exists
if [ ! -f /data/kafka/meta.properties ]; then
    echo "Formatting storage directory /data/kafka with cluster ID: $CLUSTER_ID"
    /kafka/bin/kafka-storage.sh format -t $CLUSTER_ID -c /kafka/config/server.properties
fi

# Start Kafka
exec /kafka/bin/kafka-server-start.sh /kafka/config/server.properties
