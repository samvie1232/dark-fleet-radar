"""Verificación de exactly-once sobre la tabla bronze.

Cada registro de Kafka se identifica por (partición, offset). Si el pipeline es exactly-once:
- ninguna pareja aparece dos veces (sin duplicados), y
- en cada partición los offsets van seguidos desde el primero hasta el último (sin huecos).

Redpanda no usa transacciones en este proyecto, así que los offsets de datos son contiguos.
"""

from __future__ import annotations

from dataclasses import dataclass

import duckdb
import pyarrow as pa


@dataclass(frozen=True)
class PartitionReport:
    partition: int
    rows: int
    distinct_offsets: int
    min_offset: int
    max_offset: int

    @property
    def duplicates(self) -> int:
        return self.rows - self.distinct_offsets

    @property
    def gaps(self) -> int:
        return (self.max_offset - self.min_offset + 1) - self.distinct_offsets


def check(offsets: pa.Table) -> list[PartitionReport]:
    """`offsets` debe tener las columnas kafka_partition y kafka_offset."""
    rows = duckdb.sql(
        """
        SELECT kafka_partition, count(*), count(DISTINCT kafka_offset),
               min(kafka_offset), max(kafka_offset)
        FROM offsets
        GROUP BY kafka_partition
        ORDER BY kafka_partition
        """
    ).fetchall()
    return [PartitionReport(*r) for r in rows]


def format_report(reports: list[PartitionReport]) -> tuple[str, bool]:
    lines = [f"{'partición':>9} {'filas':>10} {'offsets':>17} {'duplicados':>10} {'huecos':>7}"]
    for r in reports:
        lines.append(
            f"{r.partition:>9} {r.rows:>10} {f'{r.min_offset}-{r.max_offset}':>17}"
            f" {r.duplicates:>10} {r.gaps:>7}"
        )
    total_rows = sum(r.rows for r in reports)
    total_dup = sum(r.duplicates for r in reports)
    total_gap = sum(r.gaps for r in reports)
    ok = bool(reports) and total_dup == 0 and total_gap == 0
    lines.append(f"{'total':>9} {total_rows:>10} {'':>17} {total_dup:>10} {total_gap:>7}")
    if not reports:
        lines.append("La tabla bronze está vacía todavía (¿pasó el primer checkpoint?).")
    lines.append("EXACTLY-ONCE OK" if ok else "EXACTLY-ONCE FALLÓ")
    return "\n".join(lines), ok
