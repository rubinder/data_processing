# Debezium Connect with the Confluent Avro converter installed.
#
# debezium/connect:2.5 does NOT ship io.confluent.connect.avro.AvroConverter.
# Verified with `docker run --rm --entrypoint sh debezium/connect:2.5 -c
# 'ls /kafka/connect /kafka/external_libs'`: the plugin dir holds only the
# debezium-connector-* plugins and /kafka/external_libs holds the Apicurio
# converters (enabled with ENABLE_APICURIO_CONVERTERS=true), which talk to an
# Apicurio registry, not to Confluent Schema Registry.
#
# The Debezium docs describe two ways to add the Confluent converter: build a
# derived image that downloads the jars (this file), or mount them. We take the
# Confluent Hub package, which bundles the converter plus its runtime deps
# (kafka-schema-registry-client, kafka-avro-serializer, avro, guava, jackson),
# and install it as its own Kafka Connect plugin directory. Kafka Connect 3.6
# (inside this image) loads converters from plugin.path with an isolated
# classloader, so the jars do not have to be copied into the connector's
# folder and do not leak onto the connector's classpath.
FROM debezium/connect:2.5

ARG CONFLUENT_VERSION=7.6.0
ARG AVRO_CONVERTER_ZIP=https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-avro-converter/versions/${CONFLUENT_VERSION}/confluentinc-kafka-connect-avro-converter-${CONFLUENT_VERSION}.zip

# $KAFKA_CONNECT_PLUGINS_DIR (=/kafka/connect) is owned by the kafka user the
# image already runs as, so no USER switch is needed.
RUN mkdir -p /tmp/avro-converter && cd /tmp/avro-converter && \
    curl -fsSL -o converter.zip "${AVRO_CONVERTER_ZIP}" && \
    unzip -q converter.zip && \
    mkdir -p "${KAFKA_CONNECT_PLUGINS_DIR}/confluentinc-kafka-connect-avro-converter" && \
    mv confluentinc-kafka-connect-avro-converter-${CONFLUENT_VERSION}/lib/*.jar \
        "${KAFKA_CONNECT_PLUGINS_DIR}/confluentinc-kafka-connect-avro-converter/" && \
    cd / && rm -rf /tmp/avro-converter && \
    ls "${KAFKA_CONNECT_PLUGINS_DIR}/confluentinc-kafka-connect-avro-converter" | grep -q kafka-connect-avro-converter
