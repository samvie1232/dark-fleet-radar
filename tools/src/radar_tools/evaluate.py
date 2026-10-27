"""Precision/recall de las detecciones con escenarios inyectados (con etiqueta).

Genera 12 horas simuladas de tráfico AIS con casos positivos (lo que cada regla debe detectar) y
negativos difíciles (lo que se le parece pero no debe alertar), los escribe en el topic `ais.eval`
y deja que el job de evaluación de Flink (el mismo SQL de detecciones, ver flink/evaluate.sh) los
procese. Luego compara las alertas de `gold.alerts_eval` con las etiquetas.

Esto mide si las reglas hacen lo que dicen sobre casos construidos, no la precisión en el mundo
real: en datos reales no hay etiquetas. Las frecuencias de reporte se simplifican a 1 por minuto.
"""

from __future__ import annotations

import asyncio
import json
import math
import random
import time
import urllib.request
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from . import geo

TOPIC = "ais.eval"
GROUP = "flink-dark-fleet-radar-eval"
JOB_NAME = "dark-fleet-radar-eval"
FLINK = "http://flink-jobmanager:8081"
KAFKA = "redpanda:9092"
HOURS = 12
TYPES = (
    "spoofing_jump",
    "dark_gap",
    "sts_encounter",
    "cable_loitering",
    "sanctioned_vessel",
    "identity_change",
)
KM_PER_DEG_LAT = 111.32


# ---------------------------------------------------------------- escenarios


@dataclass
class Ping:
    minute: float
    mmsi: int
    lat: float
    lon: float
    sog: float
    nav: int = 0


@dataclass
class Static:
    minute: float
    mmsi: int
    imo: int
    name: str
    ship_type: int = 80  # código AIS: 80-89 petroleros, 70-79 carga


@dataclass
class Label:
    scenario: str
    alert_type: str
    mmsi: int  # el MMSI con el que debe salir (o no) la alerta
    positive: bool


@dataclass
class Plan:
    pings: list[Ping] = field(default_factory=list)
    statics: list[Static] = field(default_factory=list)
    labels: list[Label] = field(default_factory=list)


def imo_valid(imo: int) -> bool:
    d = [int(c) for c in f"{imo:07d}"]
    return len(d) == 7 and sum(d[i] * (7 - i) for i in range(6)) % 10 == d[6]


def make_imo(rng: random.Random) -> int:
    base = rng.randint(900000, 999999)
    d = [int(c) for c in str(base)]
    return base * 10 + sum(d[i] * (7 - i) for i in range(6)) % 10


def offset(lat: float, lon: float, north_km: float, east_km: float) -> tuple[float, float]:
    dlat = north_km / KM_PER_DEG_LAT
    dlon = east_km / (KM_PER_DEG_LAT * math.cos(math.radians(lat)))
    return lat + dlat, lon + dlon


class Builder:
    """Reparte MMSI, IMO y sitios en mar abierto, lejos de puertos, cables y otros escenarios."""

    def __init__(self, seed: int, listed_imos: list[int]):
        self.rng = random.Random(seed)
        self.plan = Plan()
        self._mmsi = 247_000_000
        self._imos: set[int] = set()
        self.listed = set(listed_imos)
        self.listed_pool = list(listed_imos)
        self.rng.shuffle(self.listed_pool)
        self._sites = self._open_sea_sites()
        self._cable_sites = self._cable_points()

    # --- recursos
    def mmsi(self, mid: int = 247) -> int:
        self._mmsi += 1
        return mid * 1_000_000 + self._mmsi % 1_000_000

    def imo(self) -> int:
        while True:
            imo = make_imo(self.rng)
            if imo not in self.listed and imo not in self._imos:
                self._imos.add(imo)
                return imo

    def site(self) -> tuple[float, float]:
        return self._sites.pop()

    def cable_site(self) -> tuple[float, float]:
        return self._cable_sites.pop()

    def port_site(self) -> tuple[float, float]:
        _, lat, lon = self.rng.choice(geo.PORTS)
        return lat, lon

    def _open_sea_sites(self) -> list[tuple[float, float]]:
        import h3

        ports, cables = geo.port_cells(ring=3), geo.cable_cells(ring=6)
        sites = []
        # Mar del Norte central y Báltico central, cada ~25 km.
        for lat0, lat1, lon0, lon1 in ((54.2, 59.8, 2.6, 6.8), (55.2, 58.4, 15.8, 19.8)):
            lat = lat0
            while lat < lat1:
                lon = lon0
                while lon < lon1:
                    r6 = h3.latlng_to_cell(lat, lon, geo.PORT_RES)
                    r7 = h3.latlng_to_cell(lat, lon, geo.CABLE_RES)
                    if r6 not in ports and r7 not in cables:
                        # Centro de la celda r7: así los barcos de fondo caen en la misma celda.
                        sites.append(h3.cell_to_latlng(r7))
                    lon += 0.4
                lat += 0.22
        self.rng.shuffle(sites)
        return sites

    def _cable_points(self) -> list[tuple[float, float]]:
        import h3

        ports = geo.port_cells(ring=3)
        pts = []
        for line in geo.CABLES.values():
            dense = geo.densify(line, step_km=20.0)
            for lat, lon in dense[1:-1]:
                if h3.latlng_to_cell(lat, lon, geo.PORT_RES) not in ports:
                    pts.append((lat, lon))
        self.rng.shuffle(pts)
        return pts

    # --- primitivas de trayectoria
    def track(
        self,
        mmsi: int,
        start: tuple[float, float],
        t0: float,
        t1: float,
        sog: float,
        heading_deg: float | None = None,
        nav: int = 0,
        jitter_km: float = 0.0,
        skip: tuple[float, float] | None = None,
        every: float = 1.0,
    ) -> tuple[float, float]:
        """Rumbo constante a `sog` nudos de t0 a t1 (minutos). Devuelve la posición final."""
        heading = math.radians(self.rng.uniform(0, 360) if heading_deg is None else heading_deg)
        lat, lon = start
        m = t0
        while m <= t1:
            km = sog * 1.852 * (m - t0) / 60
            p_lat, p_lon = offset(
                start[0], start[1], km * math.cos(heading), km * math.sin(heading)
            )
            if jitter_km:
                p_lat, p_lon = offset(
                    p_lat,
                    p_lon,
                    self.rng.uniform(-jitter_km, jitter_km),
                    self.rng.uniform(-jitter_km, jitter_km),
                )
            if not (skip and skip[0] <= m < skip[1]):
                self.plan.pings.append(Ping(m, mmsi, p_lat, p_lon, sog, nav))
            lat, lon = p_lat, p_lon
            m += every
        return lat, lon

    def patrol(self, mmsi: int, start: tuple[float, float], t0: float, t1: float, sog: float):
        """Va y viene (norte-sur, tramos de 30 min) alrededor de `start`, como un pesquero."""
        m, pos, north = t0, start, True
        while m < t1:
            leg_end = min(m + 30, t1)
            pos = self.track(
                mmsi, start=pos, t0=m, t1=leg_end, sog=sog, heading_deg=0 if north else 180
            )
            m, north = leg_end + 1, not north

    def statics(
        self,
        mmsi: int,
        imo: int,
        name: str,
        t0: float,
        t1: float,
        every: float = 10,
        ship_type: int = 80,
    ):
        m = t0
        while m <= t1:
            self.plan.statics.append(Static(m, mmsi, imo, name, ship_type))
            m += every

    def vessel(self, t0: float, t1: float, **track_kw) -> int:
        mmsi = self.mmsi()
        self.track(mmsi, t0=t0, t1=t1, **track_kw)
        self.statics(mmsi, self.imo(), f"EVAL {mmsi}", t0, t1)
        return mmsi

    def label(self, scenario: str, alert_type: str, mmsi: int, positive: bool):
        self.plan.labels.append(Label(scenario, alert_type, mmsi, positive))


def build_plan(seed: int = 42, n: int = 12, listed_imos: list[int] | None = None) -> Plan:
    """`n` positivos y `n` negativos por tipo (sanciones: tantos positivos como IMO listados)."""
    import h3

    b = Builder(seed, listed_imos or [])
    rng = b.rng
    span = HOURS * 60

    def start(dur: float) -> float:
        return rng.uniform(10, span - dur - 10)

    neg_kind = lambda i, kinds: kinds[i % len(kinds)]  # noqa: E731

    # 1. Spoofing
    for i in range(n):
        t0 = start(240)
        site = b.site()
        mmsi = b.vessel(t0, t0 + 240, start=site, sog=12)
        # Un minuto con la posición desplazada 90-330 km y vuelta: un solo episodio.
        jump_at = t0 + rng.uniform(60, 180)
        b.plan.pings = [p for p in b.plan.pings if not (p.mmsi == mmsi and p.minute == jump_at)]
        lat, lon = offset(*site, rng.uniform(90, 330), 0)
        b.plan.pings.append(Ping(jump_at + 0.5, mmsi, lat, lon, 12))
        b.label(f"spoof-pos-{i}", "spoofing_jump", mmsi, True)
    for i in range(n):
        t0, kind = start(240), neg_kind(i, ("ferry_35kn", "gps_jitter", "late_report"))
        if kind == "ferry_35kn":
            mmsi = b.vessel(t0, t0 + 240, start=b.site(), sog=35)
        elif kind == "gps_jitter":  # ruido de ~1 km entre reportes: < 2 km, no es un salto
            mmsi = b.vessel(t0, t0 + 240, start=b.site(), sog=0.5, jitter_km=0.35)
        else:  # 30 min sin reportar y reaparece donde corresponde
            mmsi = b.vessel(t0, t0 + 240, start=b.site(), sog=12, skip=(t0 + 60, t0 + 90))
        b.label(f"spoof-neg-{kind}-{i}", "spoofing_jump", mmsi, False)

    # 2. Apagón de señal
    def gap_case(
        name: str,
        gap_h: float,
        background: bool,
        where: tuple[float, float],
        pos: bool,
        moved_km: float = 20,
    ):
        t0 = start(gap_h * 60 + 120)
        g0, g1 = t0 + 60, t0 + 60 + gap_h * 60
        mmsi = b.mmsi()
        # Casi quieto (0,2 nudos) en el sitio antes del apagón; reaparece `moved_km` más allá.
        b.track(mmsi, start=where, t0=t0, t1=g0 - 1, sog=0.2, heading_deg=90)
        after = offset(*where, 0, moved_km)
        b.track(mmsi, start=after, t0=g1, t1=g1 + 60, sog=10, heading_deg=90)
        b.statics(mmsi, b.imo(), f"EVAL {mmsi}", t0, g1 + 60)
        if background:  # tres pesqueros yendo y viniendo a pocos km, todo el rato: hay cobertura
            for k in range(3):
                other = b.mmsi()
                b.patrol(other, offset(*where, 0, 2.0 * (k + 1)), t0, g1 + 60, sog=3)
                b.statics(other, b.imo(), f"EVAL {other}", t0, g1 + 60)
        b.label(name, "dark_gap", mmsi, pos)

    for i in range(n):
        gap_case(f"gap-pos-{i}", rng.uniform(2.5, 6), True, b.site(), True)
    for i in range(n):
        kind = neg_kind(i, ("short_gap", "no_coverage", "in_port", "same_spot"))
        if kind == "short_gap":
            gap_case(f"gap-neg-{kind}-{i}", 1.5, True, b.site(), False)
        elif kind == "no_coverage":
            gap_case(f"gap-neg-{kind}-{i}", 4, False, b.site(), False)
        elif kind == "in_port":
            gap_case(f"gap-neg-{kind}-{i}", 4, True, b.port_site(), False)
        else:  # atracado o fondeado: apaga el AIS y vuelve a aparecer en el mismo sitio
            gap_case(f"gap-neg-{kind}-{i}", 4, True, b.site(), False, moved_km=0.3)

    # 3. Encuentro en alta mar
    def pair(
        name: str,
        where,
        dist_km: float,
        minutes: float,
        sog: float,
        nav: int,
        pos: bool,
        ship_type: int = 80,
    ):
        t0 = start(minutes)
        a, c = b.mmsi(), b.mmsi()
        b.track(a, start=where, t0=t0, t1=t0 + minutes, sog=sog, heading_deg=0, nav=nav)
        b.track(
            c,
            start=offset(*where, 0, dist_km),
            t0=t0,
            t1=t0 + minutes,
            sog=sog,
            heading_deg=0,
            nav=nav,
        )
        for m in (a, c):
            b.statics(m, b.imo(), f"EVAL {m}", t0, t0 + minutes, ship_type=ship_type)
        b.label(name, "sts_encounter", min(a, c), pos)

    for i in range(n):
        pair(
            f"sts-pos-{i}",
            b.site(),
            rng.uniform(0.1, 0.8),
            rng.uniform(180, 300),
            rng.uniform(0.2, 1.5),
            1,
            True,
        )
    for i in range(n):
        kind = neg_kind(
            i, ("far_2_5km", "in_port", "only_60min", "convoy_9kn", "anchorage", "no_tanker")
        )
        if kind == "far_2_5km":
            pair(f"sts-neg-{kind}-{i}", b.site(), 2.5, 240, 0.5, 1, False)
        elif kind == "in_port":
            pair(f"sts-neg-{kind}-{i}", b.port_site(), 0.3, 240, 0.3, 1, False)
        elif kind == "only_60min":
            pair(f"sts-neg-{kind}-{i}", b.site(), 0.3, 60, 0.5, 1, False)
        elif kind == "convoy_9kn":
            pair(f"sts-neg-{kind}-{i}", b.site(), 0.2, 240, 9, 0, False)
        elif (
            kind == "no_tanker"
        ):  # dos cargueros (tipo 70) juntos: no es el patrón de la flota fantasma
            pair(f"sts-neg-{kind}-{i}", b.site(), 0.3, 240, 0.5, 1, False, ship_type=70)
        else:  # fondeadero en mar abierto: 9 barcos quietos a 400 m unos de otros
            t0 = start(240)
            center = h3.cell_to_latlng(h3.latlng_to_cell(*b.site(), geo.PORT_RES))
            for r in (-1, 0, 1):
                for c in (-1, 0, 1):
                    m = b.mmsi()
                    b.track(m, start=offset(*center, 0.4 * r, 0.4 * c), t0=t0, t1=t0 + 240,
                            sog=0.1, heading_deg=0, nav=1)  # fmt: skip
                    b.statics(m, b.imo(), f"EVAL {m}", t0, t0 + 240)
                    b.label(f"sts-neg-{kind}-{i}", "sts_encounter", m, False)

    # 4. Merodeo sobre cables
    for i in range(n):
        dur = rng.uniform(45, 120)
        t0 = start(dur)
        mmsi = b.vessel(t0, t0 + dur, start=b.cable_site(), sog=rng.uniform(0.3, 2.5), nav=1)
        b.label(f"cable-pos-{i}", "cable_loitering", mmsi, True)
    for i in range(n):
        kind = neg_kind(i, ("crossing_10kn", "only_20min", "far_from_cable"))
        t0 = start(120)
        if kind == "crossing_10kn":
            mmsi = b.vessel(t0, t0 + 60, start=b.cable_site(), sog=10)
        elif kind == "only_20min":
            mmsi = b.vessel(t0, t0 + 20, start=b.cable_site(), sog=1)
        else:
            mmsi = b.vessel(t0, t0 + 120, start=b.site(), sog=1)
        b.label(f"cable-neg-{kind}-{i}", "cable_loitering", mmsi, False)

    # 5. Barco sancionado (IMO de la lista cargada en Postgres)
    for i in range(min(n, len(b.listed_pool))):
        t0 = start(120)
        mmsi = b.mmsi()
        b.track(mmsi, start=b.site(), t0=t0, t1=t0 + 120, sog=11)
        b.statics(mmsi, b.listed_pool[i], f"EVAL LISTED {i}", t0, t0 + 120)
        b.label(f"sanction-pos-{i}", "sanctioned_vessel", mmsi, True)
    for i in range(n):  # IMO válido que no está en la lista
        t0 = start(120)
        mmsi = b.vessel(t0, t0 + 120, start=b.site(), sog=11)
        b.label(f"sanction-neg-{i}", "sanctioned_vessel", mmsi, False)

    # 6. Cambio de identidad
    for i in range(n):
        t0 = start(240)
        imo, site = b.imo(), b.site()
        old, new = b.mmsi(247), b.mmsi(rng.choice((636, 538, 352)))  # cambia de bandera
        end = b.track(old, start=site, t0=t0, t1=t0 + 120, sog=10, heading_deg=0)
        b.statics(old, imo, f"EVAL OLD {i}", t0, t0 + 120)
        b.track(new, start=end, t0=t0 + 130, t1=t0 + 240, sog=10, heading_deg=0)
        b.statics(new, imo, f"EVAL NEW {i}", t0 + 130, t0 + 240)
        b.label(f"identity-pos-{i}", "identity_change", new, True)
    for i in range(n):
        kind = neg_kind(i, ("same_identity", "name_case_spaces", "name_word_order"))
        t0 = start(240)
        mmsi, imo = b.mmsi(), b.imo()
        b.track(mmsi, start=b.site(), t0=t0, t1=t0 + 240, sog=10)
        if kind == "same_identity":
            b.statics(mmsi, imo, f"EVAL SAME {i}", t0, t0 + 240)
        elif kind == "name_case_spaces":  # otra caja y espacios: se normaliza, no es un cambio
            b.statics(mmsi, imo, f"Eval Same {i}", t0, t0 + 120)
            b.statics(mmsi, imo, f"  EVAL SAME {i} ", t0 + 125, t0 + 240)
        else:  # mismas palabras en otro orden ("VLI-25 CINDY" / "CINDY VLI-25"): tampoco
            b.statics(mmsi, imo, f"EVAL-{i} SAME", t0, t0 + 120)
            b.statics(mmsi, imo, f"SAME EVAL-{i}", t0 + 125, t0 + 240)
        b.label(f"identity-neg-{kind}-{i}", "identity_change", mmsi, False)

    return b.plan


# ---------------------------------------------------------------- mensajes


def _ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f") + "000 +0000 UTC"


def _iso(dt: datetime) -> str:
    return dt.isoformat(timespec="microseconds").replace("+00:00", "Z")


def envelope(t0: datetime, item: Ping | Static) -> tuple[datetime, bytes, bytes]:
    """(hora, key, valor) en el mismo formato que publica el productor."""
    t = t0 + timedelta(minutes=item.minute)
    if isinstance(item, Ping):
        mtype, lat, lon = "PositionReport", item.lat, item.lon
        body = {"Sog": round(item.sog, 1), "Cog": 0.0, "TrueHeading": 0,
                "NavigationalStatus": item.nav}  # fmt: skip
        name = "EVAL"
    else:
        mtype, lat, lon = "ShipStaticData", 0.0, 0.0
        body = {
            "ImoNumber": item.imo,
            "Name": item.name,
            "CallSign": "EVAL",
            "Type": item.ship_type,
        }
        name = item.name
    raw = json.dumps(
        {
            "MessageType": mtype,
            "MetaData": {
                "MMSI": item.mmsi,
                "ShipName": name,
                "latitude": lat,
                "longitude": lon,
                "time_utc": _ts(t),
            },  # fmt: skip
            "Message": {mtype: body},
        }
    )
    value = {
        "mmsi": item.mmsi,
        "message_type": mtype,
        "event_time": _iso(t),
        "ingested_at": _iso(t),
        "raw": raw,
    }
    return t, str(item.mmsi).encode(), json.dumps(value, separators=(",", ":")).encode()


def messages(plan: Plan, t0: datetime) -> Iterator[tuple[datetime, bytes, bytes]]:
    items = sorted([*plan.pings, *plan.statics], key=lambda x: x.minute)
    for item in items:
        yield envelope(t0, item)


# ---------------------------------------------------------------- puntaje


def score(labels: list[Label], alerts: list[tuple[str, int]]) -> dict[str, dict]:
    """Por tipo: TP, FP, FN, precision y recall. `alerts` = [(tipo, mmsi), ...]."""
    by_type: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    for t, mmsi in alerts:
        by_type[t][mmsi] += 1
    out = {}
    for t in TYPES:
        pos = {lb.mmsi for lb in labels if lb.alert_type == t and lb.positive}
        neg = {lb.mmsi for lb in labels if lb.alert_type == t and not lb.positive}
        hit = by_type.get(t, {})
        tp = len(pos & hit.keys())
        fn = len(pos - hit.keys())
        fp = len(hit.keys() - pos)
        fp_hard = len(hit.keys() & neg)
        out[t] = {
            "positives": len(pos),
            "negatives": len(neg),
            "tp": tp,
            "fp": fp,
            "fp_on_hard_negatives": fp_hard,
            "fn": fn,
            "precision": tp / (tp + fp) if tp + fp else None,
            "recall": tp / (tp + fn) if tp + fn else None,
            "alerts_per_detected_case": (
                sum(hit[m] for m in pos & hit.keys()) / tp if tp else None
            ),
            "missed": sorted(lb.scenario for lb in labels
                             if lb.alert_type == t and lb.positive and lb.mmsi not in hit),
            "false_alarms": sorted(lb.scenario for lb in labels
                                   if lb.alert_type == t and not lb.positive and lb.mmsi in hit),
        }  # fmt: skip
    return out


def report_markdown(result: dict[str, dict], meta: dict) -> str:
    def pct(x):
        return "—" if x is None else f"{100 * x:.0f} %"

    lines = [
        "# Evaluación de las detecciones",
        "",
        f"Corrida: {meta['run_at']} · {meta['hours']} h simuladas · {meta['messages']} mensajes · "
        f"semilla {meta['seed']}",
        "",
        "| Alerta | Positivos | Negativos difíciles | Precision | Recall | Alertas por caso |",
        "|---|---|---|---|---|---|",
    ]
    for t, r in result.items():
        per = (
            "—" if r["alerts_per_detected_case"] is None else f"{r['alerts_per_detected_case']:.1f}"
        )
        lines.append(
            f"| `{t}` | {r['positives']} | {r['negatives']} | {pct(r['precision'])} | "
            f"{pct(r['recall'])} | {per} |"
        )
    lines += ["", "## Fallos", ""]
    for t, r in result.items():
        if r["missed"] or r["false_alarms"] or r["fp"] > r["fp_on_hard_negatives"]:
            lines.append(f"- `{t}`: no detectó {r['missed'] or 'ninguno'}; "
                         f"falsas alarmas {r['false_alarms'] or 'ninguna'}"
                         + (f" (+{r['fp'] - r['fp_on_hard_negatives']} en barcos de fondo)"
                            if r["fp"] > r["fp_on_hard_negatives"] else ""))  # fmt: skip
    if lines[-1] == "":
        lines.append("Ninguno.")
    lines += [
        "",
        "Casos construidos a mano con `tools evaluate` (ver `tools/src/radar_tools/evaluate.py`): "
        "miden si cada regla hace lo que dice, no su precisión con datos reales, que no traen "
        "etiquetas.",
    ]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------- orquestación


def _flink_eval_jobs(states=("RUNNING", "CREATED", "INITIALIZING", "RESTARTING")) -> list[str]:
    with urllib.request.urlopen(f"{FLINK}/jobs/overview", timeout=10) as r:
        jobs = json.load(r)["jobs"]
    return [j["jid"] for j in jobs if j["name"] == JOB_NAME and j["state"] in states]


def _cancel_eval_jobs() -> None:
    for jid in _flink_eval_jobs():
        req = urllib.request.Request(f"{FLINK}/jobs/{jid}?mode=cancel", method="PATCH")
        urllib.request.urlopen(req, timeout=10).close()


def reset(dsn: str) -> None:
    """Deja todo limpio para una corrida nueva (no toca nada de producción)."""
    import psycopg
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic

    from . import settings

    _cancel_eval_jobs()

    async def topic():
        admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA)
        await admin.start()
        try:
            if TOPIC in await admin.list_topics():
                await admin.delete_topics([TOPIC])
                await asyncio.sleep(3)
            # Una partición: el orden por tiempo se conserva y el watermark avanza parejo.
            await admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])
            try:
                await admin.delete_consumer_groups([GROUP])
            except Exception:  # noqa: BLE001 - puede no existir aún
                pass
        finally:
            await admin.close()

    asyncio.run(topic())

    catalog = settings.load_catalog()
    for ident in ("gold.alerts_eval", "gold.coverage_h3_eval"):
        if catalog.table_exists(ident):
            catalog.drop_table(ident)

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(geo.DDL)
        for t in ("coverage_hourly", "coverage_zone_hourly", "alerts", "vessel_types"):
            cur.execute(f"DROP TABLE IF EXISTS {t}_eval")
            cur.execute(f"CREATE TABLE {t}_eval (LIKE {t} INCLUDING ALL)")


async def _inject(plan: Plan, t0: datetime, speedup: float) -> int:
    """Envía en orden de tiempo, a `speedup` veces el tiempo real (deja cerrar ventanas a Flink)."""
    from aiokafka import AIOKafkaProducer

    prod = AIOKafkaProducer(bootstrap_servers=KAFKA, acks="all", linger_ms=20)
    await prod.start()
    n = 0
    wall0 = time.monotonic()
    try:
        for t, key, value in messages(plan, t0):
            target = (t - t0).total_seconds() / speedup
            ahead = target - (time.monotonic() - wall0)
            if ahead > 0.05:
                await prod.flush()
                await asyncio.sleep(ahead)
            await prod.send(TOPIC, value, key=key)
            n += 1
        # Un último mensaje 3 h después cierra todas las ventanas.
        last = Ping(HOURS * 60 + 180, 247_999_999, 50.5, 0.5, 5)
        _, key, value = envelope(t0, last)
        await prod.send(TOPIC, value, key=key)
        await prod.flush()
    finally:
        await prod.stop()
    return n + 1


async def _caught_up() -> bool:
    from aiokafka import AIOKafkaConsumer, TopicPartition
    from aiokafka.admin import AIOKafkaAdminClient

    tp = TopicPartition(TOPIC, 0)
    cons = AIOKafkaConsumer(bootstrap_servers=KAFKA)
    admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA)
    await cons.start()
    await admin.start()
    try:
        end = (await cons.end_offsets([tp]))[tp]
        committed = await admin.list_consumer_group_offsets(GROUP)
        done = committed.get(tp)
        return done is not None and done.offset >= end
    finally:
        await cons.stop()
        await admin.close()


def run(dsn: str, seed: int = 42, n: int = 12, speedup: float = 120.0, out_dir: str = "/reports"):
    import psycopg

    from . import settings

    if not _flink_eval_jobs():
        print("No hay job de evaluación corriendo. Primero:")
        print("  docker compose run --rm --entrypoint /docker-entrypoint.sh flink-job \\")
        print("    /opt/evaluate.sh")
        return 1

    with psycopg.connect(dsn) as conn:
        listed = [r[0] for r in conn.execute("SELECT imo FROM sanctions_by_imo ORDER BY imo")]
    plan = build_plan(seed=seed, n=n, listed_imos=listed)
    # Tiempo simulado en el futuro cercano: no se mezcla con nada.
    t0 = (datetime.now(UTC) + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    print(f"Inyectando {HOURS} h simuladas a {speedup:.0f}x "
          f"(~{HOURS * 60 / speedup:.0f} min) ...", flush=True)  # fmt: skip
    sent = asyncio.run(_inject(plan, t0, speedup))
    print(f"{sent} mensajes enviados. Esperando a que Flink termine ...", flush=True)

    deadline = time.monotonic() + 20 * 60
    while not asyncio.run(_caught_up()):
        if time.monotonic() > deadline:
            print("Flink no terminó en 20 minutos; revisa http://localhost:8081")
            return 1
        time.sleep(10)
    time.sleep(15)  # el commit de Iceberg va con el mismo checkpoint que confirma los offsets

    table = settings.load_catalog().load_table("gold.alerts_eval")
    rows = table.scan(selected_fields=("alert_type", "mmsi")).to_arrow().to_pylist()
    result = score(plan.labels, [(r["alert_type"], r["mmsi"]) for r in rows])
    meta = {"run_at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"), "hours": HOURS,
            "messages": sent, "seed": seed}  # fmt: skip
    md = report_markdown(result, meta)
    print(md)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "evaluation.md").write_text(md, encoding="utf-8")
    (out / "evaluation.json").write_text(
        json.dumps({"meta": meta, "result": result}, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"Reporte en {out / 'evaluation.md'}")
    _cancel_eval_jobs()
    return 0
