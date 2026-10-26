"""Graba cada frame crudo en JSONL, un archivo por hora UTC, para replay posterior.

aisstream no permite reproducir datos, así que esta grabación es la única forma
de repetir un día exacto (validación, pruebas de caos, demo).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO


class HourlyJsonlRecorder:
    def __init__(self, directory: Path):
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)
        self._hour: str | None = None
        self._fh: TextIO | None = None

    def path_for(self, ts: datetime) -> Path:
        return self.directory / f"ais-{ts.astimezone(UTC):%Y%m%d-%H}.jsonl"

    def write(self, frame: str, ingested_at: datetime) -> None:
        hour = f"{ingested_at.astimezone(UTC):%Y%m%d%H}"
        if hour != self._hour:
            self.close()
            # buffering=1: escribe por línea, así un corte no pierde más de un mensaje.
            self._fh = self.path_for(ingested_at).open("a", encoding="utf-8", buffering=1)
            self._hour = hour
        line = {"ingested_at": ingested_at.astimezone(UTC).isoformat(), "frame": frame}
        self._fh.write(json.dumps(line, ensure_ascii=False) + "\n")

    def close(self) -> None:
        if self._fh:
            self._fh.close()
            self._fh = None
            self._hour = None
