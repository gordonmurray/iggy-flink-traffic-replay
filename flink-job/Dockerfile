# syntax=docker/dockerfile:1.7
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY connector /build/connector
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -f connector/pom.xml test package
COPY flink-job/pom.xml /build/job/pom.xml
COPY flink-job/src /build/job/src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -f job/pom.xml package

FROM flink:1.20.3-java17 AS flink
ARG ICEBERG_VERSION=1.7.1
ARG HADOOP_VERSION=3.3.4
# Remote ADD defaults to mode 0600. Flink runs as a non-root user.
ADD --chmod=644 https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar /opt/flink/lib/
ADD --chmod=644 https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/flink/lib/
ADD --chmod=644 https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar /opt/flink/lib/
ADD --chmod=644 https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar /opt/flink/lib/
COPY --from=build --chmod=644 /build/connector/target/flink-connector-iggy-0.2.0-SNAPSHOT.jar /opt/flink/lib/flink-connector-iggy.jar

FROM python:3.11-slim-bookworm AS demo
WORKDIR /app
COPY producer/requirements.txt producer/requirements.lock ./
RUN pip install --no-cache-dir --only-binary=:all: -r requirements.txt -c requirements.lock
COPY --from=build --chmod=644 /build/job/target/replay-job-1.0.0.jar /opt/job/replay-job.jar
COPY producer /app
COPY tests /app/tests
RUN chmod -R a+rX /app /opt/job
ENV PYTHONUNBUFFERED=1
CMD ["python", "main.py"]
