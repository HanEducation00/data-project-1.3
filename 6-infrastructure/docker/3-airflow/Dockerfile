FROM quay.io/astronomer/astro-runtime:12.4.0

USER root

# Install OpenJDK-17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get install -y netcat-openbsd && \
    apt-get clean;

# Set JAVA_HOME (using modern ENV format)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
RUN export JAVA_HOME

USER astro

# Install required Python packages
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt