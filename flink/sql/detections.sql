-- Dark Fleet Radar · paso 3A
-- Segundo job de Flink: lee ais.raw con su propio consumer group y escribe en la capa gold:
--   gold.coverage_h3  mensajes y barcos por celda H3 (res 7) y hora: el mapa de cobertura
--   gold.alerts       spoofing (saltos imposibles), barcos sancionados y cambios de identidad
-- Va separado del job de ingesta para poder cambiar reglas sin tocar el estado de bronze/silver.

SET 'pipeline.name' = 'dark-fleet-radar-detections';
SET 'parallelism.default' = '1';
SET 'state.checkpoints.dir' = 'file:///flink-data/checkpoints-detections';
SET 'table.local-time-zone' = 'UTC';
SET 'table.exec.source.idle-timeout' = '30 s';
-- El estado por barco (última posición, último reporte por IMO) se olvida tras 30 días.
SET 'table.exec.state.ttl' = '30 d';

CREATE TEMPORARY SYSTEM FUNCTION h3_cell AS 'radar.udf.H3Cell';
CREATE TEMPORARY SYSTEM FUNCTION haversine_km AS 'radar.udf.HaversineKm';
CREATE TEMPORARY SYSTEM FUNCTION implied_speed_kn AS 'radar.udf.ImpliedSpeedKnots';
CREATE TEMPORARY SYSTEM FUNCTION imo_valid AS 'radar.udf.ImoValid';
CREATE TEMPORARY SYSTEM FUNCTION epoch_ms AS 'radar.udf.EpochMillis';

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

CREATE DATABASE IF NOT EXISTS lake.gold;

CREATE TEMPORARY TABLE kafka_ais (
  mmsi         BIGINT,
  message_type STRING,
  event_time   TIMESTAMP_LTZ(6),
  ingested_at  TIMESTAMP_LTZ(6),
  `raw`        STRING,
  proc_time AS PROCTIME(),
  event_ts AS CAST(event_time AS TIMESTAMP_LTZ(3)),
  -- Campos del JSON como columnas calculadas en vez de vistas: en Flink 1.20 el sql-client falla
  -- ("belongs to a different planner") con vistas temporales dentro de un STATEMENT SET.
  lat AS CAST(JSON_VALUE(`raw`, '$.MetaData.latitude') AS DOUBLE),
  lon AS CAST(JSON_VALUE(`raw`, '$.MetaData.longitude') AS DOUBLE),
  imo AS CAST(JSON_VALUE(`raw`, '$.Message.ShipStaticData.ImoNumber') AS BIGINT),
  ship_name AS UPPER(TRIM(COALESCE(
    JSON_VALUE(`raw`, '$.Message.ShipStaticData.Name'),
    JSON_VALUE(`raw`, '$.Message.StaticDataReport.ReportA.Name')))),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '1' MINUTE
) WITH (
  'connector' = 'kafka',
  'topic' = 'ais.raw',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'flink-dark-fleet-radar-detections',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'group-offsets',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'false'
);

-- Tabla de referencia en Postgres (la carga `tools load-sanctions`). Lookup con caché de 1 h:
-- cada barco se consulta como mucho una vez por hora, aunque mande reportes cada pocos minutos.
CREATE TEMPORARY TABLE sanctions (
  os_id    STRING,
  imo      BIGINT,
  mmsi     BIGINT,
  name     STRING,
  flag     STRING,
  risk     STRING,
  datasets STRING,
  url      STRING
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/radar',
  'table-name' = 'sanctioned_vessels',
  'username' = 'radar',
  'password' = 'radar',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.expire-after-write' = '1 h',
  'lookup.partial-cache.max-rows' = '100000',
  'lookup.partial-cache.cache-missing-key' = 'true'
);

CREATE TABLE IF NOT EXISTS lake.gold.coverage_h3 (
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  h3_r7         STRING,
  messages      BIGINT,
  vessels       BIGINT,
  window_date   DATE
) PARTITIONED BY (window_date) WITH (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS lake.gold.alerts (
  alert_id     STRING,
  alert_type   STRING,
  mmsi         BIGINT,
  imo          BIGINT,
  start_time   TIMESTAMP_LTZ(6),
  end_time     TIMESTAMP_LTZ(6),
  lat          DOUBLE,
  lon          DOUBLE,
  h3_r7        STRING,
  score        DOUBLE,
  evidence     STRING,
  detected_at  TIMESTAMP_LTZ(3),
  alert_date   DATE
) PARTITIONED BY (alert_date, alert_type) WITH (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd'
);

EXECUTE STATEMENT SET
BEGIN

-- Mapa de cobertura: cuántos mensajes y barcos hay por celda y hora. Un silencio solo es
-- sospechoso donde normalmente hay señal (lo usa la detección de apagones del paso 3B).
INSERT INTO lake.gold.coverage_h3
SELECT window_start, window_end, h3_r7, COUNT(*), COUNT(DISTINCT mmsi), CAST(window_start AS DATE)
FROM (
  SELECT window_start, window_end, mmsi, h3_cell(lat, lon, 7) AS h3_r7
  FROM TABLE(TUMBLE(TABLE kafka_ais, DESCRIPTOR(event_ts), INTERVAL '1' HOUR))
  -- Posiciones válidas. (0, 0) y lat 91 / lon 181 son valores por defecto, no posiciones reales.
  WHERE message_type IN ('PositionReport', 'StandardClassBPositionReport', 'ExtendedClassBPositionReport')
    AND lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180 AND NOT (lat = 0 AND lon = 0)
)
WHERE h3_r7 IS NOT NULL
GROUP BY window_start, window_end, h3_r7;

-- 1. Spoofing: para llegar a la nueva posición haría falta ir a más de 50 nudos (ningún buque
--    mercante pasa de ~30). Se exige además un salto de más de 2 km para no confundir ruido de
--    GPS en intervalos muy cortos con un salto real.
INSERT INTO lake.gold.alerts
SELECT
  MD5(CONCAT_WS('|', 'spoofing_jump', CAST(mmsi AS STRING), CAST(event_time AS STRING))),
  'spoofing_jump', mmsi, CAST(NULL AS BIGINT), prev_time, event_time, lat, lon, h3_r7,
  LEAST(1.0, speed_kn / 100.0),
  JSON_OBJECT(
    KEY 'implied_speed_kn' VALUE ROUND(speed_kn, 1),
    KEY 'distance_km' VALUE ROUND(dist_km, 2),
    KEY 'seconds' VALUE dt_s,
    KEY 'prev_lat' VALUE prev_lat,
    KEY 'prev_lon' VALUE prev_lon),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)),
  CAST(event_time AS DATE)
FROM (
  SELECT *, implied_speed_kn(dist_km, dt_s) AS speed_kn
  FROM (
    SELECT mmsi, event_time, lat, lon, prev_lat, prev_lon, h3_cell(lat, lon, 7) AS h3_r7,
           TO_TIMESTAMP_LTZ(prev_ms, 3) AS prev_time,
           haversine_km(prev_lat, prev_lon, lat, lon) AS dist_km,
           (epoch_ms(event_time) - prev_ms) / 1000 AS dt_s
    FROM (
      -- Cada posición junto a la anterior del mismo barco (estado por MMSI).
      SELECT mmsi, event_time, lat, lon,
             FIRST_VALUE(epoch_ms(event_time)) OVER w AS prev_ms,
             FIRST_VALUE(lat) OVER w AS prev_lat,
             FIRST_VALUE(lon) OVER w AS prev_lon,
             COUNT(*) OVER w AS n
      FROM (
        SELECT * FROM kafka_ais
        -- Posiciones válidas. (0, 0) y lat 91 / lon 181 son valores por defecto, no posiciones reales.
        WHERE message_type IN ('PositionReport', 'StandardClassBPositionReport', 'ExtendedClassBPositionReport')
          AND lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180 AND NOT (lat = 0 AND lon = 0)
      )
      WINDOW w AS (PARTITION BY mmsi ORDER BY event_ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
    )
    WHERE n = 2
  )
)
WHERE dist_km > 2 AND speed_kn > 50;

-- 2. Barco sancionado: una alerta por barco y entrada de la lista (no una por reporte).
INSERT INTO lake.gold.alerts
SELECT
  MD5(CONCAT_WS('|', 'sanctioned_vessel', CAST(mmsi AS STRING), os_id)),
  'sanctioned_vessel', mmsi, imo, event_time, event_time,
  CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS STRING),
  CASE WHEN risk LIKE 'sanction%' THEN 1.0 ELSE 0.7 END,
  JSON_OBJECT(
    KEY 'matched_on' VALUE matched_on,
    KEY 'opensanctions_id' VALUE os_id,
    KEY 'listed_name' VALUE listed_name,
    KEY 'reported_name' VALUE reported_name,
    KEY 'risk' VALUE risk,
    KEY 'datasets' VALUE datasets,
    KEY 'url' VALUE url),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)),
  CAST(event_time AS DATE)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY mmsi, os_id ORDER BY proc_time) AS rn
  FROM (
    -- Coincidencias por IMO (lo más fiable) o por MMSI (barcos sin IMO).
    SELECT r.mmsi, r.imo, r.event_time, r.proc_time, r.ship_name AS reported_name,
           s.os_id, s.name AS listed_name, s.risk, s.datasets, s.url, 'imo' AS matched_on
    FROM kafka_ais AS r
    JOIN sanctions FOR SYSTEM_TIME AS OF r.proc_time AS s ON r.imo = s.imo
    WHERE r.message_type IN ('ShipStaticData', 'StaticDataReport') AND imo_valid(r.imo)
    UNION ALL
    SELECT r.mmsi, r.imo, r.event_time, r.proc_time, r.ship_name AS reported_name,
           s.os_id, s.name AS listed_name, s.risk, s.datasets, s.url, 'mmsi' AS matched_on
    FROM kafka_ais AS r
    JOIN sanctions FOR SYSTEM_TIME AS OF r.proc_time AS s ON r.mmsi = s.mmsi
    WHERE r.message_type IN ('ShipStaticData', 'StaticDataReport')
  )
)
WHERE rn = 1;

-- 3. Cambio de identidad: mismo IMO con otro nombre o con otro MMSI (otra bandera si cambia el
--    MID, los 3 primeros dígitos) dentro de 30 días.
INSERT INTO lake.gold.alerts
SELECT
  MD5(CONCAT_WS('|', 'identity_change', CAST(imo AS STRING), CAST(event_time AS STRING))),
  'identity_change', mmsi, imo, prev_time, event_time,
  CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS STRING),
  CASE WHEN prev_mmsi / 1000000 <> mmsi / 1000000 THEN 0.9
       WHEN prev_mmsi <> mmsi THEN 0.7
       ELSE 0.5 END,
  JSON_OBJECT(
    KEY 'prev_mmsi' VALUE prev_mmsi,
    KEY 'new_mmsi' VALUE mmsi,
    KEY 'prev_name' VALUE prev_name,
    KEY 'new_name' VALUE name,
    KEY 'flag_mid_changed' VALUE (prev_mmsi / 1000000 <> mmsi / 1000000)),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)),
  CAST(event_time AS DATE)
FROM (
  SELECT imo, mmsi, name, event_time, prev_mmsi, prev_name, n,
         TO_TIMESTAMP_LTZ(prev_ms, 3) AS prev_time
  FROM (
    -- Cada reporte junto al anterior del mismo IMO (estado por IMO). El IMO identifica el casco
    -- y no cambia nunca: si con el mismo IMO cambia el nombre o el MMSI, es un cambio de identidad.
    SELECT imo, mmsi, ship_name AS name, event_time,
           FIRST_VALUE(mmsi) OVER w AS prev_mmsi,
           FIRST_VALUE(ship_name) OVER w AS prev_name,
           FIRST_VALUE(epoch_ms(event_time)) OVER w AS prev_ms,
           COUNT(*) OVER w AS n
    FROM kafka_ais
    WHERE message_type IN ('ShipStaticData', 'StaticDataReport')
      AND imo_valid(imo) AND ship_name IS NOT NULL AND ship_name <> ''
    WINDOW w AS (PARTITION BY imo ORDER BY event_ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
  )
)
WHERE n = 2
  AND (prev_mmsi <> mmsi OR prev_name <> name)
  AND TIMESTAMPDIFF(DAY, prev_time, event_time) <= 30;

END;
