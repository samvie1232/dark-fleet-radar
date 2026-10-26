# Flink con los conectores de Kafka e Iceberg (S3) en el classpath.
FROM flink:1.20.1-scala_2.12-java17

ARG MAVEN=https://repo1.maven.org/maven2
ARG ICEBERG=1.8.1
ARG HADOOP=3.3.6

RUN cd /opt/flink/lib && \
    wget -q ${MAVEN}/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar && \
    wget -q ${MAVEN}/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG}/iceberg-flink-runtime-1.20-${ICEBERG}.jar && \
    wget -q ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG}/iceberg-aws-bundle-${ICEBERG}.jar && \
    wget -q ${MAVEN}/org/apache/hadoop/hadoop-client-api/${HADOOP}/hadoop-client-api-${HADOOP}.jar && \
    wget -q ${MAVEN}/org/apache/hadoop/hadoop-client-runtime/${HADOOP}/hadoop-client-runtime-${HADOOP}.jar

COPY sql/ /opt/sql/
COPY submit.sh /opt/submit.sh
# Quita \r por si el repo se clonó en Windows con saltos de línea CRLF.
RUN sed -i 's/\r$//' /opt/submit.sh /opt/sql/*.sql && chmod +x /opt/submit.sh && mkdir -p /flink-data && chown flink:flink /flink-data
