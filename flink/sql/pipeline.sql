-- Dark Fleet Radar · paso 2
-- Redpanda (ais.raw) -> Iceberg: bronze (copia exacta) y silver (posiciones y datos estáticos).
-- Un solo job de Flink con checkpoints exactly-once: Iceberg confirma un commit por checkpoint
-- y los offsets de Kafka viajan dentro del mismo checkpoint, así que un reinicio no pierde ni
-- duplica filas.

SET 'pipeline.name' = 'dark-fleet-radar';
SET 'table.local-time-zone' = 'UTC';
SET 'table.exec.source.idle-timeout' = '30 s';
-- Estado de deduplicación por barco: se olvida después de 1 h sin repetirse.
SET 'table.exec.state.ttl' = '1 h';

CREATE CATALOG lake WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://iceberg-rest:8181',
  'warehouse' = 's3://warehouse/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://s3:8333',
  's3.path-style-access' = 'true',
  's3.access-key-id' = 'radar',
  's3.secret-access-key' = 'radar-local-secret',
  'client.region' = 'us-east-1'
);

CREATE DATABASE IF NOT EXISTS lake.ais;

-- Fuente: el sobre JSON que escribe el productor. `raw` es el mensaje original de aisstream.
CREATE TEMPORARY TABLE kafka_ais (
  mmsi            BIGINT,
  message_type    STRING,
  event_time      TIMESTAMP_LTZ(6),
  ingested_at     TIMESTAMP_LTZ(6),
  `raw`             STRING,
  kafka_partition INT    METADATA FROM 'partition' VIRTUAL,
  kafka_offset    BIGINT METADATA FROM 'offset' VIRTUAL,
  proc_time AS PROCTIME(),
  -- Flink solo acepta watermarks con precisión de milisegundos.
  event_ts AS CAST(event_time AS TIMESTAMP_LTZ(3)),
  -- Tiempo de evento: aceptamos mensajes hasta 1 minuto desordenados (el p100 medido es < 30 s).
  WATERMARK FOR event_ts AS event_ts - INTERVAL '1' MINUTE
) WITH (
  'connector' = 'kafka',
  'topic' = 'ais.raw',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'flink-dark-fleet-radar',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'group-offsets',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'false'
);

-- Bronze: cada registro de Kafka una vez. partition + offset permiten probar exactly-once.
CREATE TABLE IF NOT EXISTS lake.ais.ais_raw (
  kafka_partition INT,
  kafka_offset    BIGINT,
  mmsi            BIGINT,
  message_type    STRING,
  event_time      TIMESTAMP_LTZ(6),
  ingested_at     TIMESTAMP_LTZ(6),
  `raw`             STRING,
  ingest_date     DATE
) PARTITIONED BY (ingest_date) WITH (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd'
);

-- Silver: posiciones tipadas, con banderas de calidad y sin duplicados del mismo reporte.
CREATE TABLE IF NOT EXISTS lake.ais.positions (
  mmsi              BIGINT,
  event_time        TIMESTAMP_LTZ(6),
  message_type      STRING,
  lat               DOUBLE,
  lon               DOUBLE,
  sog               DOUBLE,
  cog               DOUBLE,
  heading           INT,
  nav_status        INT,
  ship_name         STRING,
  pos_unavailable   BOOLEAN,
  pos_out_of_range  BOOLEAN,
  sog_unavailable   BOOLEAN,
  ingested_at       TIMESTAMP_LTZ(6),
  event_date        DATE
) PARTITIONED BY (event_date) WITH (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd'
);

-- Silver: reportes estáticos tal como llegan (la versión SCD2 por IMO se arma en el paso 3).
CREATE TABLE IF NOT EXISTS lake.ais.vessel_static_reports (
  mmsi          BIGINT,
  event_time    TIMESTAMP_LTZ(6),
  message_type  STRING,
  imo           BIGINT,
  name          STRING,
  call_sign     STRING,
  ship_type     INT,
  destination   STRING,
  draught       DOUBLE,
  flag_mid      INT,
  ingested_at   TIMESTAMP_LTZ(6),
  event_date    DATE
) PARTITIONED BY (event_date) WITH (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd'
);

CREATE TEMPORARY VIEW position_reports AS
SELECT
  mmsi,
  event_time,
  message_type,
  CAST(JSON_VALUE(`raw`, '$.MetaData.latitude')  AS DOUBLE) AS lat,
  CAST(JSON_VALUE(`raw`, '$.MetaData.longitude') AS DOUBLE) AS lon,
  CAST(CASE message_type
    WHEN 'PositionReport'               THEN JSON_VALUE(`raw`, '$.Message.PositionReport.Sog')
    WHEN 'StandardClassBPositionReport' THEN JSON_VALUE(`raw`, '$.Message.StandardClassBPositionReport.Sog')
    WHEN 'ExtendedClassBPositionReport' THEN JSON_VALUE(`raw`, '$.Message.ExtendedClassBPositionReport.Sog')
  END AS DOUBLE) AS sog,
  CAST(CASE message_type
    WHEN 'PositionReport'               THEN JSON_VALUE(`raw`, '$.Message.PositionReport.Cog')
    WHEN 'StandardClassBPositionReport' THEN JSON_VALUE(`raw`, '$.Message.StandardClassBPositionReport.Cog')
    WHEN 'ExtendedClassBPositionReport' THEN JSON_VALUE(`raw`, '$.Message.ExtendedClassBPositionReport.Cog')
  END AS DOUBLE) AS cog,
  CAST(CASE message_type
    WHEN 'PositionReport'               THEN JSON_VALUE(`raw`, '$.Message.PositionReport.TrueHeading')
    WHEN 'StandardClassBPositionReport' THEN JSON_VALUE(`raw`, '$.Message.StandardClassBPositionReport.TrueHeading')
    WHEN 'ExtendedClassBPositionReport' THEN JSON_VALUE(`raw`, '$.Message.ExtendedClassBPositionReport.TrueHeading')
  END AS INT) AS heading,
  CAST(JSON_VALUE(`raw`, '$.Message.PositionReport.NavigationalStatus') AS INT) AS nav_status,
  TRIM(JSON_VALUE(`raw`, '$.MetaData.ShipName')) AS ship_name,
  ingested_at,
  proc_time
FROM kafka_ais
WHERE message_type IN (
  'PositionReport', 'StandardClassBPositionReport', 'ExtendedClassBPositionReport'
);

EXECUTE STATEMENT SET
BEGIN

INSERT INTO lake.ais.ais_raw
SELECT kafka_partition, kafka_offset, mmsi, message_type, event_time, ingested_at, `raw`,
       CAST(ingested_at AS DATE)
FROM kafka_ais;

-- Varias antenas reciben el mismo mensaje: nos quedamos con la primera copia.
INSERT INTO lake.ais.positions
SELECT mmsi, event_time, message_type, lat, lon, sog, cog, heading, nav_status, ship_name,
       lat = 91 OR lon = 181,
       NOT (lat = 91 OR lon = 181) AND (ABS(lat) > 90 OR ABS(lon) > 180),
       sog IS NULL OR sog >= 102.3,
       ingested_at,
       CAST(event_time AS DATE)
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY mmsi, event_time, lat, lon ORDER BY proc_time) AS rn
  FROM position_reports
)
WHERE rn = 1;

INSERT INTO lake.ais.vessel_static_reports
SELECT
  mmsi,
  event_time,
  message_type,
  CAST(JSON_VALUE(`raw`, '$.Message.ShipStaticData.ImoNumber') AS BIGINT),
  TRIM(COALESCE(
    JSON_VALUE(`raw`, '$.Message.ShipStaticData.Name'),
    JSON_VALUE(`raw`, '$.Message.StaticDataReport.ReportA.Name'))),
  TRIM(COALESCE(
    JSON_VALUE(`raw`, '$.Message.ShipStaticData.CallSign'),
    JSON_VALUE(`raw`, '$.Message.StaticDataReport.ReportB.CallSign'))),
  CAST(COALESCE(
    JSON_VALUE(`raw`, '$.Message.ShipStaticData.Type'),
    JSON_VALUE(`raw`, '$.Message.StaticDataReport.ReportB.ShipType')) AS INT),
  TRIM(JSON_VALUE(`raw`, '$.Message.ShipStaticData.Destination')),
  CAST(JSON_VALUE(`raw`, '$.Message.ShipStaticData.MaximumStaticDraught') AS DOUBLE),
  CAST(mmsi / 1000000 AS INT),
  ingested_at,
  CAST(event_time AS DATE)
FROM kafka_ais
WHERE message_type IN ('ShipStaticData', 'StaticDataReport');

END;
