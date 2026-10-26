-- Dark Fleet Radar · pasos 3 y 4
-- Segundo job de Flink: lee ais.raw con su propio consumer group y escribe en la capa gold:
--   gold.coverage_h3  mensajes y barcos por celda H3 (res 7) y hora: el mapa de cobertura
--   gold.alerts       spoofing (saltos imposibles), barcos sancionados, cambios de identidad,
--                     apagones de señal, encuentros en alta mar y merodeo sobre cables
-- Va separado del job de ingesta para poder cambiar reglas sin tocar el estado de bronze/silver.

SET 'pipeline.name' = 'dark-fleet-radar-detections-v5';
SET 'parallelism.default' = '1';
SET 'state.checkpoints.dir' = 'file:///flink-data/checkpoints-detections-v5';
SET 'table.local-time-zone' = 'UTC';
SET 'table.exec.source.idle-timeout' = '30 s';
-- El estado por barco (última posición, último reporte por IMO) se olvida tras 30 días.
SET 'table.exec.state.ttl' = '30 d';

CREATE TEMPORARY SYSTEM FUNCTION h3_cell AS 'radar.udf.H3Cell';
CREATE TEMPORARY SYSTEM FUNCTION haversine_km AS 'radar.udf.HaversineKm';
CREATE TEMPORARY SYSTEM FUNCTION implied_speed_kn AS 'radar.udf.ImpliedSpeedKnots';
CREATE TEMPORARY SYSTEM FUNCTION imo_valid AS 'radar.udf.ImoValid';
CREATE TEMPORARY SYSTEM FUNCTION epoch_ms AS 'radar.udf.EpochMillis';
CREATE TEMPORARY SYSTEM FUNCTION h3_disk AS 'radar.udf.H3Disk';
CREATE TEMPORARY SYSTEM FUNCTION name_key AS 'radar.udf.NameKey';

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
  sog AS CAST(COALESCE(
    JSON_VALUE(`raw`, '$.Message.PositionReport.Sog'),
    JSON_VALUE(`raw`, '$.Message.StandardClassBPositionReport.Sog'),
    JSON_VALUE(`raw`, '$.Message.ExtendedClassBPositionReport.Sog')) AS DOUBLE),
  nav_status AS CAST(JSON_VALUE(`raw`, '$.Message.PositionReport.NavigationalStatus') AS INT),
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
-- Vistas de Postgres con una fila por barco (una por IMO y otra por MMSI).
CREATE TEMPORARY TABLE sanctions_by_imo (
  imo      BIGINT,
  os_ids     STRING,
  names      STRING,
  sanctioned BOOLEAN,
  datasets   STRING,
  url        STRING,
  entries    BIGINT
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/radar',
  'table-name' = 'sanctions_by_imo',
  'username' = 'radar',
  'password' = 'radar',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.expire-after-write' = '1 h',
  'lookup.partial-cache.max-rows' = '100000',
  'lookup.partial-cache.cache-missing-key' = 'true'
);

CREATE TEMPORARY TABLE sanctions_by_mmsi (
  mmsi      BIGINT,
  os_ids     STRING,
  names      STRING,
  sanctioned BOOLEAN,
  datasets   STRING,
  url        STRING,
  entries    BIGINT
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/radar',
  'table-name' = 'sanctions_by_mmsi',
  'username' = 'radar',
  'password' = 'radar',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.expire-after-write' = '1 h',
  'lookup.partial-cache.max-rows' = '100000',
  'lookup.partial-cache.cache-missing-key' = 'true'
);

-- Zonas de puertos y de cables submarinos como celdas H3 (las carga `tools load-reference`).
CREATE TEMPORARY TABLE port_zone (
  h3_r6 STRING,
  port  STRING
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/radar',
  'table-name' = 'port_cells',
  'username' = 'radar',
  'password' = 'radar',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.expire-after-write' = '1 h',
  'lookup.partial-cache.max-rows' = '100000',
  'lookup.partial-cache.cache-missing-key' = 'true'
);

CREATE TEMPORARY TABLE cable_zone (
  h3_r7  STRING,
  cables STRING
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/radar',
  'table-name' = 'cable_cells',
  'username' = 'radar',
  'password' = 'radar',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.expire-after-write' = '1 h',
  'lookup.partial-cache.max-rows' = '100000',
  'lookup.partial-cache.cache-missing-key' = 'true'
);

-- Cobertura por celda y hora también en Postgres: la escribe este job y la consultan los apagones.
-- Sin caché de claves ausentes: una hora que aún no se cerró puede aparecer después.
CREATE TEMPORARY TABLE coverage_pg (
  h3_r7        STRING,
  window_start TIMESTAMP(3),
  messages     BIGINT,
  vessels      BIGINT,
  lat          DOUBLE,
  lon          DOUBLE,
  PRIMARY KEY (h3_r7, window_start) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/radar',
  'table-name' = 'coverage_hourly',
  'username' = 'radar',
  'password' = 'radar',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.expire-after-write' = '1 h',
  'lookup.partial-cache.max-rows' = '100000',
  'lookup.partial-cache.cache-missing-key' = 'false',
  -- Escribe cada hora cerrada de inmediato: los apagones la consultan apenas se cierra.
  'sink.buffer-flush.max-rows' = '1'
);

-- Lo mismo por zona (H3 res 5, unos 250 km²): la usan los apagones. Una celda r7 (5 km²) en mar
-- abierto casi nunca tiene a otro barco en la misma hora, así que la cobertura se mira por zona.
CREATE TEMPORARY TABLE coverage_zone_pg (
  h3_r5        STRING,
  window_start TIMESTAMP(3),
  messages     BIGINT,
  vessels      BIGINT,
  PRIMARY KEY (h3_r5, window_start) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/radar',
  'table-name' = 'coverage_zone_hourly',
  'username' = 'radar',
  'password' = 'radar',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.expire-after-write' = '1 h',
  'lookup.partial-cache.max-rows' = '100000',
  'lookup.partial-cache.cache-missing-key' = 'false',
  'sink.buffer-flush.max-rows' = '1'
);

-- Copia de las alertas en Postgres para Grafana (upsert por alert_id).
CREATE TEMPORARY TABLE alerts_pg (
  alert_id     STRING,
  alert_type   STRING,
  mmsi         BIGINT,
  imo          BIGINT,
  start_time   TIMESTAMP(3),
  end_time     TIMESTAMP(3),
  lat          DOUBLE,
  lon          DOUBLE,
  h3_r7        STRING,
  score        DOUBLE,
  evidence     STRING,
  detected_at  TIMESTAMP(3),
  PRIMARY KEY (alert_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/radar',
  'table-name' = 'alerts',
  'username' = 'radar',
  'password' = 'radar'
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
--    GPS en intervalos muy cortos con un salto real. Una alerta por episodio: un barco con la
--    posición falseada salta una y otra vez, así que solo alerta el primer salto tras 6 h sin saltos.
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
  SELECT *,
         FIRST_VALUE(epoch_ms(event_time)) OVER e AS prev_jump_ms,
         COUNT(*) OVER e AS jumps
  FROM (
    SELECT *
    FROM (
      SELECT *, implied_speed_kn(dist_km, dt_s) AS speed_kn
      FROM (
        SELECT mmsi, event_time, event_ts, lat, lon, prev_lat, prev_lon, h3_cell(lat, lon, 7) AS h3_r7,
               TO_TIMESTAMP_LTZ(prev_ms, 3) AS prev_time,
               haversine_km(prev_lat, prev_lon, lat, lon) AS dist_km,
               (epoch_ms(event_time) - prev_ms) / 1000 AS dt_s
        FROM (
          -- Cada posición junto a la anterior del mismo barco (estado por MMSI).
          SELECT mmsi, event_time, event_ts, lat, lon,
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
    WHERE dist_km > 2 AND speed_kn > 50
  )
  WINDOW e AS (PARTITION BY mmsi ORDER BY event_ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
)
WHERE jumps = 1 OR epoch_ms(event_time) - prev_jump_ms > 6 * 3600 * 1000;

-- 2. Barco sancionado: una sola alerta por barco (MMSI), con todas las listas que lo nombran.
--    Se busca por IMO (lo más fiable, solo si el dígito de control es válido) y por MMSI (barcos
--    sin IMO); si coinciden ambos, manda el IMO.
INSERT INTO lake.gold.alerts
SELECT
  MD5(CONCAT_WS('|', 'sanctioned_vessel', CAST(mmsi AS STRING))),
  'sanctioned_vessel', mmsi, imo, event_time, event_time,
  CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS STRING),
  CASE WHEN sanctioned THEN 1.0 ELSE 0.7 END,
  JSON_OBJECT(
    KEY 'matched_on' VALUE matched_on,
    KEY 'opensanctions_ids' VALUE os_ids,
    KEY 'listed_names' VALUE listed_names,
    KEY 'reported_name' VALUE reported_name,
    KEY 'list_entries' VALUE entries,
    KEY 'datasets' VALUE datasets,
    KEY 'url' VALUE url),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)),
  CAST(event_time AS DATE)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY proc_time) AS rn
  FROM (
    SELECT r.mmsi, r.imo, r.event_time, r.proc_time, r.ship_name AS reported_name,
           CASE WHEN i.imo IS NOT NULL THEN 'imo' ELSE 'mmsi' END AS matched_on,
           COALESCE(i.os_ids, m.os_ids) AS os_ids,
           COALESCE(i.names, m.names) AS listed_names,
           COALESCE(i.sanctioned, m.sanctioned) AS sanctioned,
           COALESCE(i.datasets, m.datasets) AS datasets,
           COALESCE(i.url, m.url) AS url,
           COALESCE(i.entries, m.entries) AS entries
    FROM (
      SELECT mmsi, imo, event_time, proc_time, ship_name,
             CASE WHEN imo_valid(imo) THEN imo END AS imo_key
      FROM kafka_ais
      WHERE message_type IN ('ShipStaticData', 'StaticDataReport')
    ) AS r
    LEFT JOIN sanctions_by_imo FOR SYSTEM_TIME AS OF r.proc_time AS i ON r.imo_key = i.imo
    LEFT JOIN sanctions_by_mmsi FOR SYSTEM_TIME AS OF r.proc_time AS m ON r.mmsi = m.mmsi
    WHERE i.imo IS NOT NULL OR m.mmsi IS NOT NULL
  )
)
WHERE rn = 1;

-- 3. Cambio de identidad: mismo IMO con otro nombre o con otro MMSI (otra bandera si cambia el
--    MID, los 3 primeros dígitos) dentro de 30 días. Solo cuenta una identidad nunca vista para
--    ese IMO: hay barcos que alternan entre dos formas de escribir su nombre en cada reporte.
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
  SELECT imo, mmsi, name, event_time, prev_mmsi, prev_name, nkey, prev_nkey, n,
         TO_TIMESTAMP_LTZ(prev_ms, 3) AS prev_time
  FROM (
    -- Cada reporte junto al anterior del mismo IMO (estado por IMO). El IMO identifica el casco
    -- y no cambia nunca: si con el mismo IMO cambia el nombre o el MMSI, es un cambio de identidad.
    SELECT imo, mmsi, ship_name AS name, event_time, nkey,
           FIRST_VALUE(mmsi) OVER w AS prev_mmsi,
           FIRST_VALUE(ship_name) OVER w AS prev_name,
           FIRST_VALUE(nkey) OVER w AS prev_nkey,
           FIRST_VALUE(epoch_ms(event_time)) OVER w AS prev_ms,
           COUNT(*) OVER w AS n
    FROM (
      -- Primera vez que se ve cada combinación (IMO, MMSI, nombre). El nombre se compara por su
      -- clave (name_key: sin importar orden de palabras, caja ni signos). Va por tiempo de proceso
      -- porque deduplicar por tiempo de evento emite correcciones, y la ventana OVER no las acepta.
      SELECT *
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY imo, mmsi, nkey ORDER BY proc_time) AS rn
        FROM (
          SELECT *, name_key(ship_name) AS nkey
          FROM kafka_ais
          WHERE message_type IN ('ShipStaticData', 'StaticDataReport') AND imo_valid(imo)
        )
        WHERE nkey IS NOT NULL
      )
      WHERE rn = 1
    )
    WINDOW w AS (PARTITION BY imo ORDER BY proc_time ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
  )
)
WHERE n = 2
  AND (prev_mmsi <> mmsi OR prev_nkey <> nkey)
  AND TIMESTAMPDIFF(DAY, prev_time, event_time) <= 30;

-- Misma cobertura en Postgres (upsert por celda y hora) para que la consulten los apagones.
INSERT INTO coverage_pg
SELECT h3_r7, CAST(window_start AS TIMESTAMP(3)), COUNT(*), COUNT(DISTINCT mmsi), AVG(lat), AVG(lon)
FROM (
  SELECT window_start, window_end, mmsi, lat, lon, h3_cell(lat, lon, 7) AS h3_r7
  FROM TABLE(TUMBLE(TABLE kafka_ais, DESCRIPTOR(event_ts), INTERVAL '1' HOUR))
  WHERE message_type IN ('PositionReport', 'StandardClassBPositionReport', 'ExtendedClassBPositionReport')
    AND lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180 AND NOT (lat = 0 AND lon = 0)
)
WHERE h3_r7 IS NOT NULL
GROUP BY window_start, window_end, h3_r7;

INSERT INTO coverage_zone_pg
SELECT h3_r5, CAST(window_start AS TIMESTAMP(3)), COUNT(*), COUNT(DISTINCT mmsi)
FROM (
  SELECT window_start, window_end, mmsi, h3_cell(lat, lon, 5) AS h3_r5
  FROM TABLE(TUMBLE(TABLE kafka_ais, DESCRIPTOR(event_ts), INTERVAL '1' HOUR))
  WHERE message_type IN ('PositionReport', 'StandardClassBPositionReport', 'ExtendedClassBPositionReport')
    AND lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180 AND NOT (lat = 0 AND lon = 0)
)
WHERE h3_r5 IS NOT NULL
GROUP BY window_start, window_end, h3_r5;

-- 4. Apagón de señal: un barco deja de emitir 2 horas o más fuera de puerto, y en la hora central
--    del silencio se oía a otros barcos en la misma zona (H3 res 5). Si no se oía a nadie, el silencio
--    puede ser de cobertura (o de nuestro propio receptor apagado) y no se alerta.
INSERT INTO lake.gold.alerts
SELECT
  MD5(CONCAT_WS('|', 'dark_gap', CAST(g.mmsi AS STRING), CAST(g.event_time AS STRING))),
  'dark_gap', g.mmsi, CAST(NULL AS BIGINT), g.prev_time, g.event_time,
  g.prev_lat, g.prev_lon, g.prev_h3,
  LEAST(1.0, g.gap_h / 12.0),
  JSON_OBJECT(
    KEY 'gap_hours' VALUE ROUND(g.gap_h, 2),
    KEY 'other_vessels_heard' VALUE c.vessels,
    KEY 'messages_heard' VALUE c.messages,
    KEY 'sog_before' VALUE g.prev_sog,
    KEY 'reappeared_lat' VALUE g.lat,
    KEY 'reappeared_lon' VALUE g.lon,
    KEY 'distance_km' VALUE ROUND(haversine_km(g.prev_lat, g.prev_lon, g.lat, g.lon), 2)),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)),
  CAST(g.event_time AS DATE)
FROM (
  SELECT *,
         h3_cell(prev_lat, prev_lon, 7) AS prev_h3,
         h3_cell(prev_lat, prev_lon, 6) AS prev_h3_r6,
         h3_cell(prev_lat, prev_lon, 5) AS prev_h3_r5,
         (epoch_ms(event_time) - prev_ms) / 3600000.0 AS gap_h,
         -- Hora que contiene el punto medio del silencio: con 2 h o más cae entera dentro de él.
         CAST(FLOOR(TO_TIMESTAMP_LTZ((prev_ms + epoch_ms(event_time)) / 2, 3) TO HOUR)
              AS TIMESTAMP(3)) AS mid_hour,
         TO_TIMESTAMP_LTZ(prev_ms, 3) AS prev_time
  FROM (
    SELECT mmsi, event_time, proc_time, lat, lon,
           FIRST_VALUE(epoch_ms(event_time)) OVER w AS prev_ms,
           FIRST_VALUE(lat) OVER w AS prev_lat,
           FIRST_VALUE(lon) OVER w AS prev_lon,
           FIRST_VALUE(sog) OVER w AS prev_sog,
           COUNT(*) OVER w AS n
    FROM kafka_ais
    WHERE message_type IN ('PositionReport', 'StandardClassBPositionReport', 'ExtendedClassBPositionReport')
    AND lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180 AND NOT (lat = 0 AND lon = 0)
    WINDOW w AS (PARTITION BY mmsi ORDER BY event_ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
  )
  WHERE n = 2 AND epoch_ms(event_time) - prev_ms >= 2 * 3600 * 1000
) AS g
LEFT JOIN port_zone FOR SYSTEM_TIME AS OF g.proc_time AS p ON g.prev_h3_r6 = p.h3_r6
JOIN coverage_zone_pg FOR SYSTEM_TIME AS OF g.proc_time AS c
  ON g.prev_h3_r5 = c.h3_r5 AND g.mid_hour = c.window_start
WHERE p.h3_r6 IS NULL AND c.vessels >= 2;

-- 5. Encuentro en alta mar (posible transferencia barco a barco): dos barcos casi quietos
--    (< 2 nudos, sin estar amarrados) fuera de puerto, a menos de 1 km entre sí, durante al menos
--    90 minutos de una ventana de 2 horas. Un trasvase dura horas, así que alerta en cada ventana.
--    Los pares se buscan por celda H3 res 6 (unos 3 km de lado) y sus vecinas: dos barcos a menos
--    de 1 km siempre caen en la misma celda o en celdas contiguas, aunque estén a la deriva.
--    Si en la celda del barco hay más de 4 barcos quietos es un fondeadero o una zona de espera
--    (decenas de barcos fondeados juntos), no un encuentro discreto, y no se alerta.
INSERT INTO lake.gold.alerts
SELECT
  MD5(CONCAT_WS('|', 'sts_encounter', CAST(a.mmsi AS STRING), CAST(b.mmsi AS STRING),
                CAST(a.window_start AS STRING))),
  'sts_encounter', a.mmsi, CAST(NULL AS BIGINT),
  CAST(GREATEST(a.first_ts, b.first_ts) AS TIMESTAMP_LTZ(6)),
  CAST(LEAST(a.last_ts, b.last_ts) AS TIMESTAMP_LTZ(6)),
  (a.lat + b.lat) / 2, (a.lon + b.lon) / 2, h3_cell((a.lat + b.lat) / 2, (a.lon + b.lon) / 2, 7),
  LEAST(1.0, 0.5 + TIMESTAMPDIFF(MINUTE, GREATEST(a.first_ts, b.first_ts),
                                 LEAST(a.last_ts, b.last_ts)) / 240.0),
  JSON_OBJECT(
    KEY 'other_mmsi' VALUE b.mmsi,
    KEY 'distance_km' VALUE ROUND(haversine_km(a.lat, a.lon, b.lat, b.lon), 3),
    KEY 'minutes_together' VALUE TIMESTAMPDIFF(MINUTE, GREATEST(a.first_ts, b.first_ts),
                                               LEAST(a.last_ts, b.last_ts)),
    KEY 'avg_sog' VALUE ROUND(a.avg_sog, 2),
    KEY 'other_avg_sog' VALUE ROUND(b.avg_sog, 2),
    KEY 'slow_vessels_in_cell' VALUE d.slow_vessels),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)),
  CAST(a.window_start AS DATE)
FROM (
  SELECT w.*, d.cell, h3_cell(w.lat, w.lon, 6) AS own_cell
  FROM (
    -- Por barco y ventana de 2 h: posición media y cuánto tiempo estuvo quieto.
    -- (Ventanas de grupo clásicas: la TVF TUMBLE no acepta una subconsulta con lookup join.)
    SELECT mmsi,
           TUMBLE_START(event_ts, INTERVAL '2' HOUR) AS window_start,
           TUMBLE_ROWTIME(event_ts, INTERVAL '2' HOUR) AS w_rt,
           AVG(lat) AS lat, AVG(lon) AS lon, AVG(sog) AS avg_sog,
           MIN(event_ts) AS first_ts, MAX(event_ts) AS last_ts
    FROM (
      SELECT k.mmsi, k.event_ts, k.lat, k.lon, k.sog
      FROM (
        SELECT mmsi, event_ts, proc_time, lat, lon, sog, h3_cell(lat, lon, 6) AS h3_r6
        FROM kafka_ais
        WHERE message_type IN ('PositionReport', 'StandardClassBPositionReport', 'ExtendedClassBPositionReport')
      AND lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180 AND NOT (lat = 0 AND lon = 0)
          AND sog < 2 AND COALESCE(nav_status, -1) <> 5
      ) AS k
      LEFT JOIN port_zone FOR SYSTEM_TIME AS OF k.proc_time AS p ON k.h3_r6 = p.h3_r6
      WHERE p.h3_r6 IS NULL
    )
    GROUP BY mmsi, TUMBLE(event_ts, INTERVAL '2' HOUR)
    HAVING TIMESTAMPDIFF(MINUTE, MIN(event_ts), MAX(event_ts)) >= 90
  ) AS w, LATERAL TABLE(h3_disk(w.lat, w.lon, 6, 1)) AS d(cell)
) AS a
JOIN (
  SELECT w.*, h3_cell(w.lat, w.lon, 6) AS cell
  FROM (
    -- Por barco y ventana de 2 h: posición media y cuánto tiempo estuvo quieto.
    -- (Ventanas de grupo clásicas: la TVF TUMBLE no acepta una subconsulta con lookup join.)
    SELECT mmsi,
           TUMBLE_START(event_ts, INTERVAL '2' HOUR) AS window_start,
           TUMBLE_ROWTIME(event_ts, INTERVAL '2' HOUR) AS w_rt,
           AVG(lat) AS lat, AVG(lon) AS lon, AVG(sog) AS avg_sog,
           MIN(event_ts) AS first_ts, MAX(event_ts) AS last_ts
    FROM (
      SELECT k.mmsi, k.event_ts, k.lat, k.lon, k.sog
      FROM (
        SELECT mmsi, event_ts, proc_time, lat, lon, sog, h3_cell(lat, lon, 6) AS h3_r6
        FROM kafka_ais
        WHERE message_type IN ('PositionReport', 'StandardClassBPositionReport', 'ExtendedClassBPositionReport')
      AND lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180 AND NOT (lat = 0 AND lon = 0)
          AND sog < 2 AND COALESCE(nav_status, -1) <> 5
      ) AS k
      LEFT JOIN port_zone FOR SYSTEM_TIME AS OF k.proc_time AS p ON k.h3_r6 = p.h3_r6
      WHERE p.h3_r6 IS NULL
    )
    GROUP BY mmsi, TUMBLE(event_ts, INTERVAL '2' HOUR)
    HAVING TIMESTAMPDIFF(MINUTE, MIN(event_ts), MAX(event_ts)) >= 90
  ) AS w
) AS b
  -- Misma ventana (join por intervalo sobre el tiempo de la ventana: el estado se limpia solo).
  ON a.cell = b.cell
 AND a.w_rt BETWEEN b.w_rt - INTERVAL '1' SECOND AND b.w_rt + INTERVAL '1' SECOND
JOIN (
  -- Barcos quietos por celda H3 res 6 y ventana de 2 h (para descartar fondeaderos).
  SELECT cell,
         TUMBLE_ROWTIME(w_rt, INTERVAL '2' HOUR) AS d_rt,
         COUNT(*) AS slow_vessels
  FROM (
    SELECT h3_cell(w.lat, w.lon, 6) AS cell, w.w_rt
    FROM (
      -- Por barco y ventana de 2 h: posición media y cuánto tiempo estuvo quieto.
      -- (Ventanas de grupo clásicas: la TVF TUMBLE no acepta una subconsulta con lookup join.)
      SELECT mmsi,
             TUMBLE_START(event_ts, INTERVAL '2' HOUR) AS window_start,
             TUMBLE_ROWTIME(event_ts, INTERVAL '2' HOUR) AS w_rt,
             AVG(lat) AS lat, AVG(lon) AS lon, AVG(sog) AS avg_sog,
             MIN(event_ts) AS first_ts, MAX(event_ts) AS last_ts
      FROM (
        SELECT k.mmsi, k.event_ts, k.lat, k.lon, k.sog
        FROM (
          SELECT mmsi, event_ts, proc_time, lat, lon, sog, h3_cell(lat, lon, 6) AS h3_r6
          FROM kafka_ais
          WHERE message_type IN ('PositionReport', 'StandardClassBPositionReport', 'ExtendedClassBPositionReport')
        AND lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180 AND NOT (lat = 0 AND lon = 0)
            AND sog < 2 AND COALESCE(nav_status, -1) <> 5
        ) AS k
        LEFT JOIN port_zone FOR SYSTEM_TIME AS OF k.proc_time AS p ON k.h3_r6 = p.h3_r6
        WHERE p.h3_r6 IS NULL
      )
      GROUP BY mmsi, TUMBLE(event_ts, INTERVAL '2' HOUR)
      HAVING TIMESTAMPDIFF(MINUTE, MIN(event_ts), MAX(event_ts)) >= 90
    ) AS w
  )
  GROUP BY cell, TUMBLE(w_rt, INTERVAL '2' HOUR)
) AS d
  ON a.own_cell = d.cell
 AND a.w_rt BETWEEN d.d_rt - INTERVAL '1' SECOND AND d.d_rt + INTERVAL '1' SECOND
WHERE a.mmsi < b.mmsi
  AND d.slow_vessels <= 4
  AND haversine_km(a.lat, a.lon, b.lat, b.lon) < 1.0
  AND TIMESTAMPDIFF(MINUTE, GREATEST(a.first_ts, b.first_ts), LEAST(a.last_ts, b.last_ts)) >= 90;

-- 6. Merodeo sobre un cable o ducto submarino: un barco lento (< 3 nudos) o fondeado pasa al menos
--    30 minutos de una hora dentro de la franja de un cable, fuera de puerto. Es el patrón previo
--    a los daños de cables en el Báltico (Balticconnector 2023, Eagle S 2024).
--    Ventanas de 1 h que avanzan cada 30 min: una estadía de 30 min o más siempre deja al menos
--    30 min en alguna ventana, aunque cruce el cambio de hora (con ventanas fijas, 50 min
--    repartidos en 25 + 25 no alertaban).
INSERT INTO lake.gold.alerts
SELECT
  MD5(CONCAT_WS('|', 'cable_loitering', CAST(mmsi AS STRING), CAST(window_start AS STRING))),
  'cable_loitering', mmsi, CAST(NULL AS BIGINT),
  CAST(first_ts AS TIMESTAMP_LTZ(6)), CAST(last_ts AS TIMESTAMP_LTZ(6)), lat, lon, h3_r7,
  LEAST(1.0, 0.4 + TIMESTAMPDIFF(MINUTE, first_ts, last_ts) / 100.0),
  JSON_OBJECT(
    KEY 'cables' VALUE cables,
    KEY 'minutes_in_zone' VALUE TIMESTAMPDIFF(MINUTE, first_ts, last_ts),
    KEY 'avg_sog' VALUE ROUND(avg_sog, 2),
    KEY 'anchored_reports' VALUE anchored,
    KEY 'reports' VALUE reports),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)),
  CAST(window_start AS DATE)
FROM (
  SELECT mmsi, HOP_START(event_ts, INTERVAL '30' MINUTE, INTERVAL '1' HOUR) AS window_start,
         MAX(cables) AS cables, MAX(h3_r7) AS h3_r7, AVG(lat) AS lat, AVG(lon) AS lon,
         AVG(sog) AS avg_sog, MIN(event_ts) AS first_ts, MAX(event_ts) AS last_ts,
         COUNT(*) AS reports, SUM(CASE WHEN nav_status = 1 THEN 1 ELSE 0 END) AS anchored
  FROM (
    SELECT k.mmsi, k.event_ts, k.lat, k.lon, k.sog, k.nav_status, k.h3_r7, z.cables
    FROM (
      SELECT mmsi, event_ts, proc_time, lat, lon, sog, nav_status,
             h3_cell(lat, lon, 7) AS h3_r7, h3_cell(lat, lon, 6) AS h3_r6
      FROM kafka_ais
      WHERE message_type IN ('PositionReport', 'StandardClassBPositionReport', 'ExtendedClassBPositionReport')
    AND lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180 AND NOT (lat = 0 AND lon = 0)
        AND (sog < 3 OR nav_status = 1)
    ) AS k
    JOIN cable_zone FOR SYSTEM_TIME AS OF k.proc_time AS z ON k.h3_r7 = z.h3_r7
    LEFT JOIN port_zone FOR SYSTEM_TIME AS OF k.proc_time AS p ON k.h3_r6 = p.h3_r6
    WHERE p.h3_r6 IS NULL
  )
  GROUP BY mmsi, HOP(event_ts, INTERVAL '30' MINUTE, INTERVAL '1' HOUR)
  HAVING TIMESTAMPDIFF(MINUTE, MIN(event_ts), MAX(event_ts)) >= 30
);

-- Lleva a Postgres todo lo que entra en gold.alerts (lectura continua de la tabla Iceberg: primero
-- lo que ya hay y luego cada commit nuevo). Grafana lee de ahí.
INSERT INTO alerts_pg
SELECT alert_id, alert_type, mmsi, imo,
       CAST(start_time AS TIMESTAMP(3)), CAST(end_time AS TIMESTAMP(3)),
       lat, lon, h3_r7, score, evidence, CAST(detected_at AS TIMESTAMP(3))
FROM lake.gold.alerts /*+ OPTIONS('streaming' = 'true', 'monitor-interval' = '30 s',
                                  'starting-strategy' = 'TABLE_SCAN_THEN_INCREMENTAL') */;

END;
