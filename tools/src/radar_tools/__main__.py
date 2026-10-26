"""python -m radar_tools [init-storage | summary | verify]"""

from __future__ import annotations

import sys

from . import settings


def summary() -> None:
    catalog = settings.load_catalog()
    for name in ("ais_raw", "positions", "vessel_static_reports"):
        ident = f"{settings.NAMESPACE}.{name}"
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
    table = catalog.load_table(f"{settings.NAMESPACE}.ais_raw")
    offsets = table.scan(selected_fields=("kafka_partition", "kafka_offset")).to_arrow()
    text, ok = format_report(check(offsets))
    print(text)
    return 0 if ok else 1


def main(argv: list[str]) -> int:
    cmd = argv[0] if argv else "summary"
    if cmd == "init-storage":
        from .storage import ensure_bucket

        ensure_bucket()
        return 0
    if cmd == "summary":
        summary()
        return 0
    if cmd == "verify":
        return verify()
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
