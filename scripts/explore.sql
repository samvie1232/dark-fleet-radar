-- Exploración rápida de una grabación con DuckDB:
--   duckdb -c ".read scripts/explore.sql"
-- Lee todos los archivos data/raw/ais-*.jsonl.

CREATE OR REPLACE VIEW frames AS
SELECT ingested_at::TIMESTAMPTZ AS ingested_at, frame::JSON AS f
FROM read_json('data/raw/ais-*.jsonl', columns = {ingested_at: 'VARCHAR', frame: 'VARCHAR'});

CREATE OR REPLACE VIEW msgs AS
SELECT
    ingested_at,
    f->>'MessageType'                         AS message_type,
    (f->'MetaData'->>'MMSI')::BIGINT          AS mmsi,
    f->'MetaData'->>'ShipName'                AS ship_name,
    (f->'MetaData'->>'latitude')::DOUBLE      AS lat,
    (f->'MetaData'->>'longitude')::DOUBLE     AS lon
FROM frames
WHERE f->>'MessageType' IS NOT NULL;

-- Volumen total y por minuto
SELECT count(*) AS mensajes,
       count(DISTINCT mmsi) AS barcos,
       round(count(*) / greatest(1, date_diff('minute', min(ingested_at), max(ingested_at))), 1)
           AS mensajes_por_minuto
FROM msgs;

-- Mezcla de tipos de mensaje
SELECT message_type, count(*) AS n FROM msgs GROUP BY ALL ORDER BY n DESC;

-- Señales raras: MMSI que no es de barco o posición "no disponible" (lat 91 / lon 181)
SELECT
    count(*) FILTER (WHERE mmsi NOT BETWEEN 200000000 AND 799999999) AS mmsi_no_barco,
    count(*) FILTER (WHERE lat = 91 OR lon = 181)                    AS posicion_no_disponible,
    round(100.0 * count(*) FILTER (WHERE mmsi NOT BETWEEN 200000000 AND 799999999
                                     OR lat = 91 OR lon = 181) / count(*), 2) AS pct_raro
FROM msgs;

-- Barcos más activos
SELECT mmsi, any_value(ship_name) AS nombre, count(*) AS n
FROM msgs GROUP BY mmsi ORDER BY n DESC LIMIT 10;
