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
    unzip procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Spark 3.4.1
ENV SPARK_VERSION=3.4.1
ENV HADOOP_VERSION=3
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /workspace
