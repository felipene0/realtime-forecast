FROM apache/airflow:latest
USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk-headless && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN echo "Custom Dockerfile executed" > /tmp/dockerfile_marker.txt
USER airflow
