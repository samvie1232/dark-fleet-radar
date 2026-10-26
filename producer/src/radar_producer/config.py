"""Configuración del productor, leída de variables de entorno."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"

# Báltico + Mar del Norte: la región con mejor cobertura terrestre en aisstream.
DEFAULT_BOUNDING_BOXES = [[[50.0, -5.0], [66.0, 31.0]]]
DEFAULT_MESSAGE_TYPES = [
    "PositionReport",
    "StandardClassBPositionReport",
    "ExtendedClassBPositionReport",
    "ShipStaticData",
    "StaticDataReport",
]


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Config:
    api_key: str
    bounding_boxes: list
    message_types: list[str]
    kafka_bootstrap: str
    topic_raw: str
    topic_dlq: str
    record_dir: Path | None
    metrics_port: int
    log_level: str
    url: str = AISSTREAM_URL

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Config:
        env = dict(os.environ if env is None else env)

        api_key = env.get("AISSTREAM_API_KEY", "").strip()
        if not api_key:
            raise ConfigError("Falta AISSTREAM_API_KEY (ponla en tu archivo .env)")

        raw_boxes = env.get("AIS_BOUNDING_BOXES", "").strip()
        try:
            boxes = json.loads(raw_boxes) if raw_boxes else DEFAULT_BOUNDING_BOXES
        except json.JSONDecodeError as exc:
            raise ConfigError(f"AIS_BOUNDING_BOXES no es JSON válido: {exc}") from exc
        _check_boxes(boxes)

        raw_types = env.get("AIS_MESSAGE_TYPES", "").strip()
        types = [t.strip() for t in raw_types.split(",") if t.strip()] or DEFAULT_MESSAGE_TYPES

        record_dir = env.get("AIS_RECORD_DIR", "").strip()

        return cls(
            api_key=api_key,
            bounding_boxes=boxes,
            message_types=types,
            kafka_bootstrap=env.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
            topic_raw=env.get("KAFKA_TOPIC_RAW", "ais.raw"),
            topic_dlq=env.get("KAFKA_TOPIC_DLQ", "ais.dlq"),
            record_dir=Path(record_dir) if record_dir else None,
            metrics_port=int(env.get("METRICS_PORT", "8000")),
            log_level=env.get("LOG_LEVEL", "INFO").upper(),
            # Solo para pruebas o replay contra un servidor WebSocket local.
            url=env.get("AISSTREAM_URL", AISSTREAM_URL),
        )

    def subscription(self) -> dict:
        """Mensaje de suscripción; aisstream exige recibirlo en los primeros 3 s."""
        return {
            "APIKey": self.api_key,
            "BoundingBoxes": self.bounding_boxes,
            "FilterMessageTypes": self.message_types,
        }


def _check_boxes(boxes: object) -> None:
    if not isinstance(boxes, list) or not boxes:
        raise ConfigError("AIS_BOUNDING_BOXES debe ser una lista con al menos una caja")
    for box in boxes:
        try:
            (lat1, lon1), (lat2, lon2) = box
            ok = all(-90 <= v <= 90 for v in (lat1, lat2)) and all(
                -180 <= v <= 180 for v in (lon1, lon2)
            )
        except (TypeError, ValueError):
            ok = False
        if not ok:
            raise ConfigError(f"Caja inválida en AIS_BOUNDING_BOXES: {box!r}")
