"""Carga la lista de barcos sancionados de OpenSanctions (dataset "maritime") en Postgres.

Flink la consulta con un lookup join (con caché de 1 h), así que basta con refrescar esta tabla
una vez al día. La carga es atómica: TRUNCATE + COPY dentro de una transacción.
"""

from __future__ import annotations

import csv
import io
import json
import os
import urllib.request
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

INDEX_URL = "https://data.opensanctions.org/datasets/latest/maritime/index.json"
PG_DSN = os.environ.get("REF_PG_DSN", "postgresql://radar:radar@postgres:5432/radar")

DDL = """
CREATE TABLE IF NOT EXISTS sanctioned_vessels (
    os_id    TEXT NOT NULL,
    imo      BIGINT,
    mmsi     BIGINT,
    name     TEXT,
    flag     TEXT,
    risk     TEXT,
    datasets TEXT,
    url      TEXT
);
CREATE INDEX IF NOT EXISTS sanctioned_vessels_imo  ON sanctioned_vessels (imo);
CREATE INDEX IF NOT EXISTS sanctioned_vessels_mmsi ON sanctioned_vessels (mmsi);
"""


@dataclass(frozen=True)
class SanctionedVessel:
    os_id: str
    imo: int | None
    mmsi: int | None
    name: str
    flag: str
    risk: str
    datasets: str
    url: str


def _numbers(cell: str | None, digits: int) -> list[int]:
    """Números de `digits` dígitos en una celda que puede traer varios ("IMO1234567;...")."""
    out = []
    for part in (cell or "").replace(",", ";").split(";"):
        num = "".join(ch for ch in part if ch.isdigit())
        if len(num) == digits:
            out.append(int(num))
    return out


def parse_csv(text: str) -> Iterator[SanctionedVessel]:
    """Una fila por cada combinación IMO/MMSI del barco (la tabla se consulta por ambas)."""
    for row in csv.DictReader(io.StringIO(text)):
        row = {(k or "").strip().lower(): (v or "").strip() for k, v in row.items()}
        imos = _numbers(row.get("imo"), 7) or [None]
        mmsis = _numbers(row.get("mmsi"), 9) or [None]
        if imos == [None] and mmsis == [None]:
            continue
        for imo in imos:
            for mmsi in mmsis:
                yield SanctionedVessel(
                    os_id=row.get("id", ""),
                    imo=imo,
                    mmsi=mmsi,
                    name=row.get("caption", ""),
                    flag=row.get("flag", ""),
                    risk=row.get("risk", ""),
                    datasets=row.get("datasets", ""),
                    url=row.get("url", ""),
                )


def resolve_csv_url(index_url: str = INDEX_URL) -> str:
    with urllib.request.urlopen(index_url, timeout=30) as resp:
        index = json.load(resp)
    for res in index.get("resources", []):
        if res.get("name") == "maritime.csv":
            return res["url"]
    raise RuntimeError("El índice de OpenSanctions no trae maritime.csv")


def fetch_csv() -> str:
    local = os.environ.get("SANCTIONS_CSV_FILE")
    if local:
        with open(local, encoding="utf-8") as fh:
            return fh.read()
    url = os.environ.get("SANCTIONS_CSV_URL") or resolve_csv_url()
    with urllib.request.urlopen(url, timeout=120) as resp:
        return resp.read().decode("utf-8")


def store(rows: Iterable[SanctionedVessel], dsn: str = PG_DSN) -> int:
    import psycopg

    rows = list(rows)
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute("TRUNCATE sanctioned_vessels")
        with cur.copy(
            "COPY sanctioned_vessels (os_id, imo, mmsi, name, flag, risk, datasets, url) FROM STDIN"
        ) as copy:
            for r in rows:
                copy.write_row((r.os_id, r.imo, r.mmsi, r.name, r.flag, r.risk, r.datasets, r.url))
    return len(rows)


def ensure_table(dsn: str = PG_DSN) -> None:
    import psycopg

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(DDL)


def load() -> int:
    """Descarga y carga. Si no hay internet, deja la tabla como estaba (o vacía) y avisa."""
    ensure_table()
    try:
        text = fetch_csv()
    except Exception as exc:  # noqa: BLE001 - sin red no debe tumbar el stack
        print(f"AVISO: no se pudo descargar OpenSanctions ({exc}); se mantiene la tabla actual")
        return 0
    n = store(parse_csv(text))
    print(f"{n} filas de barcos sancionados cargadas en Postgres")
    return 0
