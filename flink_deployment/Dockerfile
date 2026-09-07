FROM flink:1.18.1-scala_2.12-java11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev && \
    pip3 install --no-cache-dir apache-flink==1.18.1 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    # `flink run -py` launches the driver with `python` by default
    # (python.client.executable); Debian only ships `python3`.
    ln -sf /usr/bin/python3 /usr/bin/python

# SQL connector / format jars the CDC job needs. The base image ships none of
# these: without flink-sql-connector-kafka a `'connector' = 'kafka'` table
# fails at plan time with "Could not find any factory for identifier 'kafka'".
#
#   flink-sql-connector-kafka 3.2.0-1.18  -> 'kafka' and 'upsert-kafka'
#   flink-sql-avro-confluent-registry     -> 'avro-confluent' (and 'avro',
#                                            'debezium-avro-confluent')
#
# Coordinates verified against repo1.maven.org (HTTP 200 on the jar and
# .sha1); the sha1 is checked so a swapped artifact fails the build.
ARG MAVEN=https://repo1.maven.org/maven2/org/apache/flink
ARG KAFKA_CONNECTOR_VERSION=3.2.0-1.18
ARG KAFKA_CONNECTOR_SHA1=35f0bef78d3f3bd083703c1c6f4ce5545a6bd0a1
ARG AVRO_REGISTRY_VERSION=1.18.1
ARG AVRO_REGISTRY_SHA1=7eb5d69c3e30fe8fd69c4fef61021acb7c9b8681

RUN cd /opt/flink/lib && \
    wget -q -O flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
        ${MAVEN}/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar && \
    echo "${KAFKA_CONNECTOR_SHA1}  flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" | sha1sum -c - && \
    wget -q -O flink-sql-avro-confluent-registry-${AVRO_REGISTRY_VERSION}.jar \
        ${MAVEN}/flink-sql-avro-confluent-registry/${AVRO_REGISTRY_VERSION}/flink-sql-avro-confluent-registry-${AVRO_REGISTRY_VERSION}.jar && \
    echo "${AVRO_REGISTRY_SHA1}  flink-sql-avro-confluent-registry-${AVRO_REGISTRY_VERSION}.jar" | sha1sum -c - && \
    chown flink:flink /opt/flink/lib/*.jar

USER flink
