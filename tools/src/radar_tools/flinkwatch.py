"""Sigue el estado de los jobs de Flink durante la prueba de caos (estado y checkpoints)."""

from __future__ import annotations

import json
import time
import urllib.request

FLINK = "http://flink-jobmanager:8081"


def _get(path: str) -> dict:
    with urllib.request.urlopen(f"{FLINK}{path}", timeout=5) as r:
        return json.load(r)


def snapshot() -> list[tuple[str, str, int | None]]:
    """[(nombre, estado, checkpoints completados)] de los jobs que no están terminados."""
    out = []
    for job in _get("/jobs/overview")["jobs"]:
        if job["state"] in ("CANCELED", "FINISHED", "FAILED"):
            continue
        try:
            done = _get(f"/jobs/{job['jid']}/checkpoints")["counts"]["completed"]
        except Exception:
            done = None
        out.append((job["name"], job["state"], done))
    return sorted(out)


def watch(seconds: int = 150, every: int = 10) -> int:
    t0 = time.monotonic()
    while True:
        elapsed = int(time.monotonic() - t0)
        try:
            jobs = snapshot()
            line = "   ".join(
                f"{name.removeprefix('dark-fleet-radar-')}: {state}"
                + ("" if done is None else f" ({done} checkpoints)")
                for name, state, done in jobs
            )
        except Exception as e:  # el JobManager puede tardar en contestar
            line = f"sin respuesta de Flink ({type(e).__name__})"
        print(f"  +{elapsed:>3} s  {line}", flush=True)
        if elapsed >= seconds:
            return 0
        time.sleep(every)
