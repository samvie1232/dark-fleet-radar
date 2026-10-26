"""Validación de mensajes aisstream y armado del sobre que va a Kafka.

Aquí solo se descarta lo que no sirve para nada aguas abajo (JSON roto, sin
MMSI de barco, sin hora). Las banderas de calidad de posición (lat 91, saltos
imposibles, etc.) se calculan en Flink, en la capa silver.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, ValidationError

# MMSI que aparecen por configuración por defecto o de prueba en los transpondedores.
PLACEHOLDER_MMSI = {
    111111111,
    123456789,
    222222222,
    333333333,
    444444444,
    555555555,
    666666666,
    777777777,
    888888888,
    999999999,
}

# aisstream manda la hora como "2024-05-01 12:34:56.123456789 +0000 UTC" (formato de Go).
_TIME_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})(?:\.(\d+))? ([+-]\d{4})(?: [A-Z]+)?$"
)


class InvalidMessage(Exception):
    def __init__(self, reason: str, detail: str = ""):
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason


class MetaData(BaseModel):
    model_config = ConfigDict(extra="allow")

    MMSI: int
    time_utc: str
    ShipName: str | None = None
    latitude: float | None = None
    longitude: float | None = None


class AisMessage(BaseModel):
    model_config = ConfigDict(extra="allow")

    MessageType: str
    MetaData: MetaData
    Message: dict


@dataclass(frozen=True)
class Envelope:
    mmsi: int
    message_type: str
    event_time: datetime
    ingested_at: datetime
    raw: dict

    @property
    def key(self) -> bytes:
        return str(self.mmsi).encode()

    def to_bytes(self) -> bytes:
        return json.dumps(
            {
                "mmsi": self.mmsi,
                "message_type": self.message_type,
                "event_time": _iso(self.event_time),
                "ingested_at": _iso(self.ingested_at),
                "raw": self.raw,
            },
            separators=(",", ":"),
        ).encode()


def is_ship_mmsi(mmsi: int) -> bool:
    """Estaciones de barco: 9 dígitos con MID que empieza en 2-7 (ITU-R M.585)."""
    return 200_000_000 <= mmsi <= 799_999_999 and mmsi not in PLACEHOLDER_MMSI


def parse_time_utc(value: str) -> datetime:
    m = _TIME_RE.match(value.strip())
    if not m:
        raise InvalidMessage("bad_time", value)
    date, clock, frac, offset = m.groups()
    frac = (frac or "0")[:6].ljust(6, "0")  # Python solo admite microsegundos
    try:
        dt = datetime.strptime(f"{date} {clock}.{frac} {offset}", "%Y-%m-%d %H:%M:%S.%f %z")
    except ValueError as exc:
        raise InvalidMessage("bad_time", value) from exc
    return dt.astimezone(UTC)


def parse_message(frame: str | bytes, ingested_at: datetime) -> Envelope:
    try:
        data = json.loads(frame)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise InvalidMessage("invalid_json", str(exc)) from exc
    if not isinstance(data, dict):
        raise InvalidMessage("invalid_json", "no es un objeto")
    if "error" in data:
        raise InvalidMessage("server_error", str(data["error"]))

    try:
        msg = AisMessage.model_validate(data)
    except ValidationError as exc:
        raise InvalidMessage("missing_fields", exc.errors()[0]["msg"]) from exc

    mmsi = msg.MetaData.MMSI
    if not is_ship_mmsi(mmsi):
        raise InvalidMessage("invalid_mmsi", str(mmsi))

    return Envelope(
        mmsi=mmsi,
        message_type=msg.MessageType,
        event_time=parse_time_utc(msg.MetaData.time_utc),
        ingested_at=ingested_at,
        raw=data,
    )


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
