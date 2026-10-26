import json
from datetime import UTC, datetime

from conftest import make_frame

from radar_producer.pipeline import Pipeline
from radar_producer.recorder import HourlyJsonlRecorder

NOW = datetime(2026, 10, 26, 15, 0, 2, tzinfo=UTC)


async def test_valid_goes_to_raw_topic_keyed_by_mmsi(sink):
    await Pipeline(sink, "ais.raw", "ais.dlq").handle(make_frame(), NOW)
    [(topic, key, value)] = sink.sent
    assert topic == "ais.raw"
    assert key == b"219000001"
    assert value["event_time"] == "2026-10-26T15:00:00.123456Z"
    assert value["ingested_at"] == "2026-10-26T15:00:02.000000Z"


async def test_invalid_goes_to_dlq_with_reason(sink):
    await Pipeline(sink, "ais.raw", "ais.dlq").handle(make_frame(mmsi=0), NOW)
    [(topic, key, value)] = sink.sent
    assert topic == "ais.dlq"
    assert key is None
    assert value["reason"] == "invalid_mmsi"


async def test_bytes_frames_are_decoded(sink):
    await Pipeline(sink, "ais.raw", "ais.dlq").handle(make_frame().encode(), NOW)
    assert sink.sent[0][0] == "ais.raw"


async def test_recorder_keeps_every_frame_and_rotates_hourly(sink, tmp_path):
    rec = HourlyJsonlRecorder(tmp_path)
    p = Pipeline(sink, "ais.raw", "ais.dlq", rec)
    await p.handle(make_frame(), NOW)
    await p.handle("{roto", NOW)  # los inválidos también se graban, para replay fiel
    await p.handle(make_frame(), NOW.replace(hour=16))
    rec.close()

    first = (tmp_path / "ais-20261026-15.jsonl").read_text().splitlines()
    second = (tmp_path / "ais-20261026-16.jsonl").read_text().splitlines()
    assert len(first) == 2 and len(second) == 1
    assert json.loads(first[1])["frame"] == "{roto"
