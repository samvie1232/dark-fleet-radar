"""Punto de entrada: python -m radar_producer"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

from prometheus_client import start_http_server

from .config import Config, ConfigError
from .kafka_sink import KafkaSink
from .pipeline import Pipeline
from .recorder import HourlyJsonlRecorder
from .stream import stream_forever

log = logging.getLogger("radar_producer")


async def main() -> None:
    cfg = Config.from_env()
    logging.basicConfig(
        level=cfg.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    start_http_server(cfg.metrics_port)
    log.info("Métricas en :%d/metrics; Kafka en %s", cfg.metrics_port, cfg.kafka_bootstrap)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    sink = KafkaSink(cfg.kafka_bootstrap)
    await sink.start()
    recorder = HourlyJsonlRecorder(cfg.record_dir) if cfg.record_dir else None
    pipeline = Pipeline(sink, cfg.topic_raw, cfg.topic_dlq, recorder)
    try:
        task = asyncio.create_task(stream_forever(cfg, pipeline, stop))
        await stop.wait()
        log.info("Apagando…")
        # Cortamos la lectura del WebSocket aunque no lleguen frames.
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    finally:
        await sink.stop()
        if recorder:
            recorder.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except ConfigError as exc:
        print(f"Error de configuración: {exc}", file=sys.stderr)
        sys.exit(2)
