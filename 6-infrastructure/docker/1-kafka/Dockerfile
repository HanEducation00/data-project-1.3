FROM eclipse-temurin:17-jre

ENV KAFKA_VERSION 3.6.1
ENV SCALA_VERSION 2.13

# Install necessary tools
RUN apt-get update && \
    apt-get install -y curl wget tar && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download and extract Kafka
RUN mkdir /tmp/kafka && \
    wget "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
    -O /tmp/kafka/kafka.tgz && \
    mkdir /kafka && cd /kafka && \
    tar -xzf /tmp/kafka/kafka.tgz --strip 1 && \
    rm -rf /tmp/kafka

# Create data directory
RUN mkdir -p /data/kafka

# Copy startup scripts
COPY start-kafka.sh /usr/bin/
RUN chmod +x /usr/bin/start-kafka.sh

CMD ["start-kafka.sh"]
