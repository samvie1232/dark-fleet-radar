"""Genera dashboards/dark-fleet-radar.json. Se edita aquí y se vuelve a correr este script."""

import json
from pathlib import Path

PG = {"type": "grafana-postgresql-datasource", "uid": "radar-pg"}
PROM = {"type": "prometheus", "uid": "radar-prom"}

TYPE_COLORS = {
    "spoofing_jump": "red",
    "sanctioned_vessel": "dark-purple",
    "identity_change": "orange",
    "dark_gap": "blue",
    "sts_encounter": "yellow",
    "cable_loitering": "green",
}

_next_id = 0


def panel(kind, title, x, y, w, h, targets, **extra):
    global _next_id
    _next_id += 1
    p = {
        "id": _next_id,
        "type": kind,
        "title": title,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": targets,
    }
    p.update(extra)
    return p


def sql(query, fmt="table", ref="A"):
    return {
        "refId": ref,
        "datasource": PG,
        "rawQuery": True,
        "editorMode": "code",
        "format": fmt,
        "rawSql": query,
    }


def prom(expr, legend, ref="A"):
    return {"refId": ref, "datasource": PROM, "expr": expr, "legendFormat": legend}


def stat(title, x, y, target, unit="short", decimals=0, w=4):
    return panel(
        "stat",
        title,
        x,
        y,
        w,
        4,
        [target],
        datasource=target["datasource"],
        fieldConfig={
            "defaults": {
                "unit": unit,
                "decimals": decimals,
                "color": {"mode": "thresholds"},
                "thresholds": {"mode": "absolute", "steps": [{"color": "text", "value": None}]},
            },
            "overrides": [],
        },
        options={
            "reduceOptions": {"calcs": ["lastNotNull"], "values": False},
            "colorMode": "value",
            "graphMode": "area",
            "textMode": "value",
        },
    )


type_overrides = [
    {
        "matcher": {"id": "byName", "options": t},
        "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": c}}],
    }
    for t, c in TYPE_COLORS.items()
]

# Color fijo por tipo de alerta en el mapa (mapeo de valores del campo alert_type).
type_color_mapping = {
    "matcher": {"id": "byName", "options": "alert_type"},
    "properties": [
        {
            "id": "mappings",
            "value": [
                {
                    "type": "value",
                    "options": {
                        t: {"color": c, "index": i} for i, (t, c) in enumerate(TYPE_COLORS.items())
                    },
                }
            ],
        }
    ],
}

panels = [
    # Fila 1: salud del pipeline
    stat(
        "Mensajes AIS por minuto",
        0,
        0,
        prom("sum(rate(ais_messages_published_total[$__rate_interval])) * 60", "msg/min"),
    ),
    stat(
        "% a la DLQ (1 h)",
        4,
        0,
        prom(
            "100 * (sum(increase(ais_messages_invalid_total[1h])) or vector(0)) / "
            "clamp_min(sum(increase(ais_messages_received_total[1h])), 1)",
            "DLQ",
        ),
        unit="percent",
        decimals=2,
    ),
    stat(
        "Retraso medio del evento",
        8,
        0,
        prom(
            "sum(rate(ais_event_delay_seconds_sum[$__rate_interval])) / "
            "clamp_min(sum(rate(ais_event_delay_seconds_count[$__rate_interval])), 1e-9)",
            "s",
        ),
        unit="s",
        decimals=1,
    ),
    stat(
        "Checkpoints completados (Flink)",
        12,
        0,
        prom("sum(flink_jobmanager_job_numberOfCompletedCheckpoints)", "ok"),
    ),
    stat(
        "Checkpoints fallidos (Flink)",
        16,
        0,
        prom("sum(flink_jobmanager_job_numberOfFailedCheckpoints)", "fail"),
    ),
    stat(
        "Alertas en el rango",
        20,
        0,
        sql("SELECT count(*) FROM alerts WHERE $__timeFilter(end_time)"),
    ),
    # Fila 2: mapas
    panel(
        "geomap",
        "Alertas en el mapa (color = tipo, tamaño = puntaje)",
        0,
        4,
        14,
        14,
        [
            sql(
                "SELECT lat AS latitude, lon AS longitude, alert_type, mmsi, score, end_time\n"
                "FROM alerts\n"
                "WHERE lat IS NOT NULL AND $__timeFilter(end_time)\n"
                "ORDER BY end_time DESC LIMIT 5000"
            )
        ],
        datasource=PG,
        fieldConfig={"defaults": {}, "overrides": [type_color_mapping]},
        options={
            "view": {"id": "coords", "lat": 57, "lon": 13, "zoom": 4.3},
            "controls": {"showZoom": True, "mouseWheelZoom": True},
            "basemap": {"type": "default", "name": "Mapa base"},
            "layers": [
                {
                    "type": "markers",
                    "name": "Alertas",
                    "location": {
                        "mode": "coords",
                        "latitude": "latitude",
                        "longitude": "longitude",
                    },
                    "config": {
                        "showLegend": False,
                        "style": {
                            "color": {"field": "alert_type", "fixed": "red"},
                            "size": {"field": "score", "min": 4, "max": 12, "fixed": 6},
                            "symbol": {"mode": "fixed", "fixed": "img/icons/marker/circle.svg"},
                            "opacity": 0.8,
                        },
                    },
                    "tooltip": True,
                }
            ],
            "tooltip": {"mode": "details"},
        },
    ),
    panel(
        "geomap",
        "Cobertura AIS: mensajes por celda H3 en la última hora cerrada",
        14,
        4,
        10,
        14,
        [
            sql(
                "SELECT lat AS latitude, lon AS longitude, messages, vessels\n"
                "FROM coverage_hourly\n"
                "WHERE lat IS NOT NULL AND window_start = (\n"
                "  SELECT max(window_start) FROM coverage_hourly\n"
                "  WHERE lat IS NOT NULL AND window_start < now() AT TIME ZONE 'UTC')"
            )
        ],
        datasource=PG,
        options={
            "view": {"id": "coords", "lat": 57, "lon": 13, "zoom": 3.6},
            "controls": {"showZoom": True, "mouseWheelZoom": True},
            "basemap": {"type": "default", "name": "Mapa base"},
            "layers": [
                {
                    "type": "heatmap",
                    "name": "Cobertura",
                    "location": {
                        "mode": "coords",
                        "latitude": "latitude",
                        "longitude": "longitude",
                    },
                    "config": {
                        "weight": {"field": "messages", "min": 0, "max": 1, "fixed": 1},
                        "blur": 12,
                        "radius": 6,
                    },
                }
            ],
        },
    ),
    # Fila 3: alertas en el tiempo y por tipo
    panel(
        "timeseries",
        "Alertas por hora y tipo",
        0,
        18,
        14,
        8,
        [
            sql(
                "SELECT $__timeGroupAlias(end_time, 1h), alert_type AS metric, count(*) AS value\n"
                "FROM alerts WHERE $__timeFilter(end_time)\nGROUP BY 1, 2 ORDER BY 1",
                fmt="time_series",
            )
        ],
        datasource=PG,
        fieldConfig={
            "defaults": {
                "custom": {"drawStyle": "bars", "fillOpacity": 80, "stacking": {"mode": "normal"}}
            },
            "overrides": type_overrides,
        },
    ),
    panel(
        "bargauge",
        "Alertas por tipo (barcos distintos)",
        14,
        18,
        10,
        8,
        [
            sql(
                "SELECT alert_type, count(DISTINCT mmsi) AS barcos\n"
                "FROM alerts WHERE $__timeFilter(end_time)\nGROUP BY 1 ORDER BY 2 DESC"
            )
        ],
        datasource=PG,
        options={
            "reduceOptions": {"values": True, "calcs": [], "fields": "/^barcos$/"},
            "orientation": "horizontal",
            "displayMode": "basic",
            "showUnfilled": True,
        },
        fieldConfig={"defaults": {"color": {"mode": "palette-classic"}}, "overrides": []},
    ),
    # Fila 4: detalle
    panel(
        "table",
        "Últimas alertas",
        0,
        26,
        24,
        10,
        [
            sql(
                "SELECT end_time AS hora_utc, alert_type AS tipo, mmsi, imo,\n"
                "       round(score::numeric, 2) AS puntaje, evidence AS evidencia\n"
                "FROM alerts WHERE $__timeFilter(end_time)\n"
                "ORDER BY end_time DESC LIMIT 200"
            )
        ],
        datasource=PG,
        fieldConfig={
            "defaults": {"custom": {"filterable": True}},
            "overrides": [
                {
                    "matcher": {"id": "byName", "options": "evidencia"},
                    "properties": [{"id": "custom.width", "value": 700}],
                }
            ],
        },
    ),
    # Fila 5: Flink
    panel(
        "timeseries",
        "Duración del último checkpoint por job (Flink)",
        0,
        36,
        12,
        7,
        [prom("max by (job_name) (flink_jobmanager_job_lastCheckpointDuration)", "{{job_name}}")],
        datasource=PROM,
        fieldConfig={"defaults": {"unit": "ms"}, "overrides": []},
    ),
    panel(
        "timeseries",
        "Mensajes publicados por tipo (por segundo)",
        12,
        36,
        12,
        7,
        [prom("sum by (type) (rate(ais_messages_published_total[$__rate_interval]))", "{{type}}")],
        datasource=PROM,
        fieldConfig={"defaults": {"unit": "reqps"}, "overrides": []},
    ),
]

dashboard = {
    "uid": "dark-fleet-radar",
    "title": "Dark Fleet Radar",
    "tags": ["ais", "flink"],
    "timezone": "utc",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "1m",
    "time": {"from": "now-24h", "to": "now"},
    "panels": panels,
}

out = Path(__file__).parent / "dashboards" / "dark-fleet-radar.json"
out.write_text(json.dumps(dashboard, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
print(f"escrito {out}")
