"""Zonas de referencia para las detecciones: puertos y cables/ductos submarinos, como celdas H3.

Flink las consulta en Postgres con un lookup join por celda, igual que la lista de sanciones:
- port_cells (H3 res 6, unos 10 km alrededor del puerto): ahí barcos quietos o juntos son lo
  normal, así que no cuentan para transferencias, merodeo ni apagones.
- cable_cells (H3 res 7, unos 3 km a cada lado del trazado): zona de los cables y ductos.

Los trazados son aproximados (unos pocos puntos por cable, a partir de mapas públicos). Sirven para
la demo; para algo serio habría que cargar el trazado real (por ejemplo de EMODnet).
"""

from __future__ import annotations

import math
from collections import defaultdict

PG_DSN_ENV = "REF_PG_DSN"

PORT_RES = 6
CABLE_RES = 7

# (nombre, lat, lon) de los puertos principales del Mar del Norte y el Báltico.
PORTS: list[tuple[str, float, float]] = [
    ("Rotterdam", 51.95, 4.05),
    ("Rotterdam (ciudad)", 51.90, 4.45),
    ("Amberes", 51.28, 4.33),
    ("Ámsterdam / IJmuiden", 52.46, 4.58),
    ("Zeebrugge", 51.33, 3.20),
    ("Dunkerque", 51.05, 2.35),
    ("Calais", 50.97, 1.86),
    ("Dover", 51.12, 1.33),
    ("Felixstowe", 51.95, 1.31),
    ("London Gateway", 51.50, 0.45),
    ("Immingham", 53.63, -0.19),
    ("Aberdeen", 57.14, -2.08),
    ("Hamburgo", 53.54, 9.97),
    ("Bremerhaven", 53.56, 8.56),
    ("Wilhelmshaven", 53.52, 8.15),
    ("Esbjerg", 55.46, 8.44),
    ("Skagen", 57.72, 10.59),
    ("Aarhus", 56.15, 10.23),
    ("Fredericia", 55.56, 9.75),
    ("Gotemburgo", 57.69, 11.86),
    ("Oslo", 59.90, 10.74),
    ("Bergen", 60.39, 5.32),
    ("Stavanger", 58.97, 5.73),
    ("Copenhague", 55.69, 12.60),
    ("Malmö", 55.61, 12.99),
    ("Kiel", 54.33, 10.15),
    ("Travemünde", 53.96, 10.87),
    ("Rostock", 54.15, 12.10),
    ("Świnoujście", 53.91, 14.25),
    ("Gdańsk", 54.40, 18.67),
    ("Gdynia", 54.53, 18.55),
    ("Baltiysk", 54.64, 19.90),
    ("Kaliningrado", 54.70, 20.45),
    ("Klaipėda", 55.71, 21.12),
    ("Liepāja", 56.52, 21.00),
    ("Ventspils", 57.40, 21.55),
    ("Riga", 57.03, 24.07),
    ("Paldiski", 59.35, 24.05),
    ("Tallin / Muuga", 59.50, 24.95),
    ("Helsinki", 60.15, 24.95),
    ("Inkoo", 60.04, 24.00),
    ("HaminaKotka", 60.47, 26.95),
    ("Primorsk", 60.34, 28.66),
    ("Vysotsk", 60.63, 28.57),
    ("Ust-Luga", 59.68, 28.40),
    ("San Petersburgo", 59.88, 30.20),
    ("Estocolmo", 59.33, 18.10),
    ("Nynäshamn", 58.90, 17.95),
    ("Turku / Naantali", 60.44, 22.10),
]

# Trazados aproximados (lat, lon) de cables eléctricos, de datos y gasoductos submarinos.
CABLES: dict[str, list[tuple[float, float]]] = {
    "Balticconnector (gasoducto FI-EE)": [(59.35, 24.05), (59.70, 24.02), (60.04, 24.00)],
    "Estlink 1 (FI-EE)": [(59.40, 24.55), (59.75, 24.60), (60.12, 24.65)],
    "Estlink 2 (FI-EE)": [(59.53, 26.10), (59.90, 25.95), (60.25, 25.75)],
    "C-Lion1 (FI-DE)": [
        (60.10, 24.90),
        (59.75, 23.00),
        (59.30, 21.50),
        (58.00, 20.40),
        (56.50, 19.40),
        (55.40, 16.00),
        (54.90, 14.00),
        (54.35, 12.50),
        (54.18, 12.08),
    ],
    "Nord Stream (RU-DE)": [
        (60.55, 28.05),
        (60.10, 26.80),
        (59.80, 25.00),
        (59.50, 23.00),
        (58.90, 21.20),
        (57.70, 20.00),
        (56.40, 18.60),
        (55.55, 15.75),
        (54.80, 14.30),
        (54.15, 13.65),
    ],
    "BCS East-West Interlink (LT-SE)": [(56.04, 21.07), (56.80, 19.90), (57.43, 18.85)],
    "NordBalt (LT-SE)": [(55.68, 21.13), (56.00, 19.00), (56.40, 17.30), (56.75, 15.95)],
    "NorNed (NL-NO)": [(53.45, 6.83), (55.00, 6.50), (56.80, 6.60), (58.27, 6.86)],
    "North Sea Link (NO-UK)": [(59.10, 5.60), (58.00, 3.50), (56.50, 1.00), (55.13, -1.50)],
}


def _haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, (*a, *b))
    h = (
        math.sin((lat2 - lat1) / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    )
    return 2 * 6371.0088 * math.asin(math.sqrt(h))


def densify(points: list[tuple[float, float]], step_km: float = 1.0) -> list[tuple[float, float]]:
    """Puntos cada `step_km` a lo largo de la línea, para no saltarse celdas entre vértices."""
    out = [points[0]]
    for a, b in zip(points, points[1:], strict=False):
        n = max(1, math.ceil(_haversine_km(a, b) / step_km))
        out += [
            (a[0] + (b[0] - a[0]) * i / n, a[1] + (b[1] - a[1]) * i / n) for i in range(1, n + 1)
        ]
    return out


def port_cells(ring: int = 1) -> dict[str, str]:
    import h3

    cells: dict[str, str] = {}
    for name, lat, lon in PORTS:
        for cell in h3.grid_disk(h3.latlng_to_cell(lat, lon, PORT_RES), ring):
            cells.setdefault(cell, name)
    return cells


def cable_cells(ring: int = 1) -> dict[str, str]:
    import h3

    names: dict[str, set[str]] = defaultdict(set)
    for name, line in CABLES.items():
        for lat, lon in densify(line):
            for cell in h3.grid_disk(h3.latlng_to_cell(lat, lon, CABLE_RES), ring):
                names[cell].add(name)
    return {cell: "; ".join(sorted(n)) for cell, n in names.items()}


DDL = """
CREATE TABLE IF NOT EXISTS port_cells (h3_r6 TEXT PRIMARY KEY, port TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS cable_cells (h3_r7 TEXT PRIMARY KEY, cables TEXT NOT NULL);
-- La escribe Flink (job de detecciones): mensajes y barcos por celda y hora. Los apagones la
-- consultan para saber si en esa celda y hora se oía a otros barcos.
CREATE TABLE IF NOT EXISTS coverage_hourly (
    h3_r7        TEXT      NOT NULL,
    window_start TIMESTAMP NOT NULL,
    messages     BIGINT    NOT NULL,
    vessels      BIGINT    NOT NULL,
    PRIMARY KEY (h3_r7, window_start)
);
-- Centro aproximado de la celda (posición media de los mensajes), para el mapa de Grafana.
ALTER TABLE coverage_hourly ADD COLUMN IF NOT EXISTS lat DOUBLE PRECISION;
ALTER TABLE coverage_hourly ADD COLUMN IF NOT EXISTS lon DOUBLE PRECISION;
CREATE INDEX IF NOT EXISTS coverage_hourly_time ON coverage_hourly (window_start);
-- Copia de gold.alerts para Grafana (la escribe Flink; upsert por alert_id). Horas en UTC.
CREATE TABLE IF NOT EXISTS alerts (
    alert_id    TEXT PRIMARY KEY,
    alert_type  TEXT NOT NULL,
    mmsi        BIGINT,
    imo         BIGINT,
    start_time  TIMESTAMP,
    end_time    TIMESTAMP,
    lat         DOUBLE PRECISION,
    lon         DOUBLE PRECISION,
    h3_r7       TEXT,
    score       DOUBLE PRECISION,
    evidence    TEXT,
    detected_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS alerts_end_time ON alerts (end_time);
"""


def load(dsn: str) -> tuple[int, int]:
    """Recarga puertos y cables (atómico). Devuelve (celdas de puerto, celdas de cable)."""
    import psycopg

    ports, cables = port_cells(), cable_cells()
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute("TRUNCATE port_cells, cable_cells")
        with cur.copy("COPY port_cells (h3_r6, port) FROM STDIN") as copy:
            for row in ports.items():
                copy.write_row(row)
        with cur.copy("COPY cable_cells (h3_r7, cables) FROM STDIN") as copy:
            for row in cables.items():
                copy.write_row(row)
    return len(ports), len(cables)
