"""python -m radar_tools [init-storage | load-sanctions | summary | verify | alerts]"""

from __future__ import annotations

import sys

from . import settings


def summary() -> None:
    catalog = settings.load_catalog()
    for ident in (
        "ais.ais_raw",
        "ais.positions",
        "ais.vessel_static_reports",
        "gold.coverage_h3",
        "gold.alerts",
    ):
        if not catalog.table_exists(ident):
            print(f"{ident:<28} (todavía no existe)")
            continue
        table = catalog.load_table(ident)
        snap = table.current_snapshot()
        if snap is None:
            print(f"{ident:<28} 0 filas")
            continue
        s = snap.summary
        print(
            f"{ident:<28} {s.get('total-records', '?'):>10} filas  "
            f"{s.get('total-data-files', '?'):>5} archivos  "
            f"{len(table.snapshots()):>4} commits"
        )


def verify() -> int:
    from .exactly_once import check, format_report

    catalog = settings.load_catalog()
    ident = f"{settings.NAMESPACE}.ais_raw"
    if not catalog.table_exists(ident):
        print(f"La tabla {ident} todavía no existe: el job de Flink no ha arrancado.")
        print("Revisa: docker compose logs flink-job --tail 40")
        return 1
    table = catalog.load_table(ident)
    offsets = table.scan(selected_fields=("kafka_partition", "kafka_offset")).to_arrow()
    text, ok = format_report(check(offsets))
    print(text)
    return 0 if ok else 1


def alerts(limit: int = 20) -> int:
    """Alertas por tipo y las más recientes."""
    import duckdb

    catalog = settings.load_catalog()
    if not catalog.table_exists("gold.alerts"):
        print("La tabla gold.alerts todavía no existe: el job de detecciones no ha arrancado.")
        print("Revisa: docker compose logs flink-job --tail 40")
        return 1
    rows = catalog.load_table("gold.alerts").scan().to_arrow()  # noqa: F841 (lo lee duckdb)
    con = duckdb.connect()
    print(con.sql(
        "SELECT alert_type, COUNT(*) AS alertas, COUNT(DISTINCT mmsi) AS barcos, "
        "ROUND(AVG(score), 2) AS puntaje_medio FROM rows GROUP BY 1 ORDER BY 2 DESC"
    ))
    print(con.sql(
        "SELECT alert_type, mmsi, imo, end_time, ROUND(score, 2) AS score, evidence "
        f"FROM rows ORDER BY end_time DESC LIMIT {int(limit)}"
    ))
    return 0


def main(argv: list[str]) -> int:
    cmd = argv[0] if argv else "summary"
    if cmd == "init-storage":
        from .storage import ensure_bucket

        ensure_bucket()
        return 0
    if cmd == "load-sanctions":
        from .sanctions import load

        return load()
    if cmd == "summary":
        summary()
        return 0
    if cmd == "verify":
        return verify()
    if cmd == "alerts":
        return alerts()
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
