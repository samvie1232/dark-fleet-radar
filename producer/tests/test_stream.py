"""El cliente contra un servidor WebSocket local que se cae a propósito."""

import asyncio
import dataclasses
import json

from conftest import make_frame
from websockets.asyncio.server import serve

from radar_producer import stream
from radar_producer.config import Config
from radar_producer.pipeline import Pipeline


async def test_reconnects_after_server_drops(sink, monkeypatch):
    monkeypatch.setattr(stream, "backoff_delay", lambda attempt: 0.05)
    subscriptions = []
    connections = 0

    async def handler(ws):
        nonlocal connections
        connections += 1
        subscriptions.append(json.loads(await asyncio.wait_for(ws.recv(), 3)))
        await ws.send(make_frame(mmsi=219000000 + connections))
        await ws.close()  # simula a aisstream cortando la conexión

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        cfg = dataclasses.replace(
            Config.from_env({"AISSTREAM_API_KEY": "k"}), url=f"ws://127.0.0.1:{port}"
        )
        stop = asyncio.Event()
        task = asyncio.create_task(stream.stream_forever(cfg, Pipeline(sink, "r", "d"), stop))
        for _ in range(100):
            if len(sink.sent) >= 3:
                break
            await asyncio.sleep(0.05)
        stop.set()
        await asyncio.wait_for(task, 5)

    assert connections >= 3
    assert all(s["APIKey"] == "k" for s in subscriptions)
    assert [k for _, k, _ in sink.sent[:3]] == [b"219000001", b"219000002", b"219000003"]
