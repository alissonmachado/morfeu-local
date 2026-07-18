# Flink 1.20 com os conectores necessários para o morfeu:
#   - iceberg-flink-runtime       : sink/source Iceberg
#   - iceberg-aws-bundle          : S3FileIO (grava no MinIO/S3)
#   - flink-sql-connector-kafka   : source Kafka
#   - flink-shaded-hadoop-2-uber  : classes org.apache.hadoop.* exigidas
#                                   pelo catálogo Iceberg no Flink
# Base Java 11 (o Hadoop 2 é mais estável no Java 11 do que no 17).
FROM flink:1.20.1-scala_2.12-java11

ENV ICEBERG_VERSION=1.10.1
ENV FLINK_MINOR=1.20
ENV KAFKA_CONNECTOR_VERSION=3.4.0-1.20
ENV HADOOP_UBER_VERSION=2.8.3-10.0
ENV MAVEN=https://repo1.maven.org/maven2

RUN wget -nv -P /opt/flink/lib \
      ${MAVEN}/org/apache/iceberg/iceberg-flink-runtime-${FLINK_MINOR}/${ICEBERG_VERSION}/iceberg-flink-runtime-${FLINK_MINOR}-${ICEBERG_VERSION}.jar && \
    wget -nv -P /opt/flink/lib \
      ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar && \
    wget -nv -P /opt/flink/lib \
      ${MAVEN}/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar && \
    wget -nv -P /opt/flink/lib \
      ${MAVEN}/org/apache/flink/flink-shaded-hadoop-2-uber/${HADOOP_UBER_VERSION}/flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar
