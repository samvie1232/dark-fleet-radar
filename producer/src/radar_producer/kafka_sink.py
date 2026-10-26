"""Escritura a Redpanda con productor idempotente (acks=all)."""

from __future__ import annotations

import asyncio
import logging

from aiokafka import AIOKafkaProducer

from . import metrics

log = logging.getLogger(__name__)


class KafkaSink:
    def __init__(self, bootstrap: str):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap,
            enable_idempotence=True,  # sin duplicados por reintentos del productor
            acks="all",
            linger_ms=50,
            compression_type="gzip",
            max_batch_size=256 * 1024,
        )

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()  # vacía lo que quede en el buffer

    async def send(self, topic: str, value: bytes, key: bytes | None = None) -> None:
        # send() solo encola en el buffer interno; no esperamos la confirmación del
        # broker para no frenar la lectura del WebSocket (aisstream descarta si lees lento).
        fut = await self._producer.send(topic, value=value, key=key)
        fut.add_done_callback(_on_delivery)


def _on_delivery(fut: asyncio.Future) -> None:
    if fut.cancelled() or fut.exception() is not None:
        metrics.PUBLISH_ERRORS.inc()
        log.error("Fallo al escribir en Kafka: %s", None if fut.cancelled() else fut.exception())
