"""Cliente WebSocket de aisstream con reconexión indefinida."""

from __future__ import annotations

import asyncio
import json
import logging
import time

from websockets.asyncio.client import connect
from websockets.exceptions import InvalidHandshake, WebSocketException

from . import metrics
from .backoff import backoff_delay
from .config import Config
from .pipeline import Pipeline

log = logging.getLogger(__name__)

# Si la conexión aguanta este tiempo, el siguiente fallo vuelve a empezar el backoff.
STABLE_AFTER_S = 60.0


async def stream_forever(cfg: Config, pipeline: Pipeline, stop: asyncio.Event) -> None:
    attempt = 0
    while not stop.is_set():
        connected_at = None
        try:
            async with connect(
                cfg.url,
                compression="deflate",
                open_timeout=10,
                ping_interval=20,
                ping_timeout=20,
                max_size=2**22,
            ) as ws:
                await ws.send(json.dumps(cfg.subscription()))
                connected_at = time.monotonic()
                metrics.CONNECTED.set(1)
                log.info("Conectado a aisstream; suscripción enviada")
                async for frame in ws:
                    await pipeline.handle(frame)
                    if stop.is_set():
                        break
                log.warning("aisstream cerró la conexión")
        except (OSError, TimeoutError, InvalidHandshake, WebSocketException) as exc:
            log.warning("Conexión caída: %r", exc)
        finally:
            metrics.CONNECTED.set(0)

        if stop.is_set():
            break
        if connected_at and time.monotonic() - connected_at >= STABLE_AFTER_S:
            attempt = 0
        attempt += 1
        delay = backoff_delay(attempt)
        metrics.RECONNECTS.inc()
        log.info("Reintento %d en %.1f s", attempt, delay)
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except TimeoutError:
            pass
