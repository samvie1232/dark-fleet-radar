"""Lógica por frame: grabar, validar y publicar en Kafka (o en la DLQ)."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Protocol

from . import metrics
from .recorder import HourlyJsonlRecorder
from .validation import InvalidMessage, parse_message

log = logging.getLogger(__name__)


class Sink(Protocol):
    async def send(self, topic: str, value: bytes, key: bytes | None = None) -> None: ...


class Pipeline:
    def __init__(
        self,
        sink: Sink,
        topic_raw: str,
        topic_dlq: str,
        recorder: HourlyJsonlRecorder | None = None,
    ):
        self.sink = sink
        self.topic_raw = topic_raw
        self.topic_dlq = topic_dlq
        self.recorder = recorder

    async def handle(self, frame: str | bytes, ingested_at: datetime | None = None) -> None:
        ingested_at = ingested_at or datetime.now(UTC)
        if isinstance(frame, bytes):
            frame = frame.decode("utf-8", errors="replace")

        metrics.RECEIVED.inc()
        metrics.LAST_MESSAGE.set(ingested_at.timestamp())
        if self.recorder:
            self.recorder.write(frame, ingested_at)

        try:
            env = parse_message(frame, ingested_at)
        except InvalidMessage as exc:
            if exc.reason == "server_error":
                log.error("aisstream respondió con error: %s", exc)
            metrics.INVALID.labels(reason=exc.reason).inc()
            dlq = {
                "reason": exc.reason,
                "error": str(exc),
                "frame": frame,
                "ingested_at": ingested_at.isoformat(),
            }
            await self.sink.send(self.topic_dlq, json.dumps(dlq).encode())
            return

        metrics.EVENT_DELAY.observe(max(0.0, (ingested_at - env.event_time).total_seconds()))
        await self.sink.send(self.topic_raw, env.to_bytes(), key=env.key)
        metrics.PUBLISHED.labels(type=env.message_type).inc()
