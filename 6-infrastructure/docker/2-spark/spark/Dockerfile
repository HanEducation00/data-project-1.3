FROM python:3.9-slim-bullseye

# Install OpenJDK 11
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Install utilities
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    bash curl wget vim nano git \
    iputils-ping telnet openssh-client net-tools \
    unzip netcat-openbsd procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Spark 3.4.1
ENV SPARK_VERSION=3.4.1
ENV HADOOP_VERSION=3
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Add AWS, PostgreSQL and Kafka connectors
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.541/aws-java-sdk-bundle-1.12.541.jar && \
    wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.1/spark-token-provider-kafka-0-10_2.12-3.4.1.jar && \
    wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    mv hadoop-aws-3.3.4.jar aws-java-sdk-bundle-1.12.541.jar postgresql-42.6.0.jar \
       kafka-clients-3.6.1.jar spark-sql-kafka-0-10_2.12-3.4.1.jar \
       spark-token-provider-kafka-0-10_2.12-3.4.1.jar commons-pool2-2.11.1.jar \
       /opt/spark/jars/

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH

# Create directories for history server
RUN mkdir -p /opt/spark/history

# Copy the scripts to the root directory
COPY spark-master.sh /spark-master.sh
COPY spark-worker.sh /spark-worker.sh
RUN chmod +x /spark-master.sh /spark-worker.sh

WORKDIR /opt/spark
