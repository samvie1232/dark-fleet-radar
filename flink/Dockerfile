# Etapa 1: compila y prueba las UDF de Java (H3, distancias, validación IMO).
FROM eclipse-temurin:17-jdk AS udf
ARG MAVEN=https://repo1.maven.org/maven2
ARG FLINK=1.20.1
ARG H3=4.1.1
WORKDIR /build
RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends wget >/dev/null && \
    wget -q ${MAVEN}/org/apache/flink/flink-table-common/${FLINK}/flink-table-common-${FLINK}.jar && \
    wget -q ${MAVEN}/org/apache/flink/flink-core/${FLINK}/flink-core-${FLINK}.jar && \
    wget -q ${MAVEN}/org/apache/flink/flink-annotations/${FLINK}/flink-annotations-${FLINK}.jar && \
    wget -q ${MAVEN}/com/uber/h3/${H3}/h3-${H3}.jar
COPY udf/src/ src/
RUN CP="flink-table-common-${FLINK}.jar:flink-core-${FLINK}.jar:flink-annotations-${FLINK}.jar:h3-${H3}.jar" && \
    mkdir classes && javac -d classes -cp "$CP" $(find src -name '*.java') && \
    java -cp "classes:$CP" radar.udf.UdfSelfTest && \
    jar cf radar-udf.jar -C classes .

# Etapa 2: Flink con los conectores de Kafka, Iceberg (S3), JDBC (Postgres) y las UDF.
FROM flink:1.20.1-scala_2.12-java17

ARG MAVEN=https://repo1.maven.org/maven2
ARG ICEBERG=1.8.1
ARG HADOOP=3.3.6
ARG H3=4.1.1

RUN cd /opt/flink/lib && \
    wget -q ${MAVEN}/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar && \
    wget -q ${MAVEN}/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG}/iceberg-flink-runtime-1.20-${ICEBERG}.jar && \
    wget -q ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG}/iceberg-aws-bundle-${ICEBERG}.jar && \
    wget -q ${MAVEN}/org/apache/hadoop/hadoop-client-api/${HADOOP}/hadoop-client-api-${HADOOP}.jar && \
    wget -q ${MAVEN}/org/apache/hadoop/hadoop-client-runtime/${HADOOP}/hadoop-client-runtime-${HADOOP}.jar && \
    wget -q ${MAVEN}/org/apache/flink/flink-connector-jdbc/3.3.0-1.20/flink-connector-jdbc-3.3.0-1.20.jar && \
    wget -q ${MAVEN}/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar && \
    wget -q ${MAVEN}/com/uber/h3/${H3}/h3-${H3}.jar

COPY --from=udf /build/radar-udf.jar /opt/flink/lib/radar-udf.jar
COPY sql/ /opt/sql/
COPY submit.sh evaluate.sh /opt/
# Quita \r por si el repo se clonó en Windows con saltos de línea CRLF.
RUN sed -i 's/\r$//' /opt/submit.sh /opt/evaluate.sh /opt/sql/*.sql && chmod +x /opt/submit.sh /opt/evaluate.sh && mkdir -p /flink-data && chown flink:flink /flink-data
