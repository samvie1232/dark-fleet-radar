from datetime import UTC, datetime

import pytest
from conftest import make_frame

from radar_producer.validation import InvalidMessage, is_ship_mmsi, parse_message, parse_time_utc

NOW = datetime(2026, 10, 26, 15, 0, 2, tzinfo=UTC)


def test_valid_position_report():
    env = parse_message(make_frame(), NOW)
    assert env.mmsi == 219000001
    assert env.key == b"219000001"
    assert env.message_type == "PositionReport"
    assert env.event_time == datetime(2026, 10, 26, 15, 0, 0, 123456, tzinfo=UTC)


def test_envelope_keeps_original_message():
    env = parse_message(make_frame(), NOW)
    assert env.raw["Message"]["PositionReport"]["Sog"] == 11.2


@pytest.mark.parametrize("mmsi", [0, 123456789, 999999999, 12345, 2190000011, 992191234, 111219001])
def test_rejects_non_ship_mmsi(mmsi):
    with pytest.raises(InvalidMessage) as exc:
        parse_message(make_frame(mmsi=mmsi), NOW)
    assert exc.value.reason == "invalid_mmsi"


@pytest.mark.parametrize("mmsi", [200000000, 219000001, 636012345, 799999998])
def test_accepts_ship_mmsi(mmsi):
    assert is_ship_mmsi(mmsi)


@pytest.mark.parametrize(
    "frame,reason",
    [
        ("{no es json", "invalid_json"),
        ("[1, 2]", "invalid_json"),
        ('{"error": "Api Key Is Not Valid"}', "server_error"),
        ('{"MessageType": "PositionReport"}', "missing_fields"),
    ],
)
def test_rejects_bad_frames(frame, reason):
    with pytest.raises(InvalidMessage) as exc:
        parse_message(frame, NOW)
    assert exc.value.reason == reason


def test_rejects_bad_time():
    with pytest.raises(InvalidMessage) as exc:
        parse_message(make_frame(time_utc="ayer a las 3"), NOW)
    assert exc.value.reason == "bad_time"


@pytest.mark.parametrize(
    "value,expected",
    [
        ("2026-10-26 15:00:00 +0000 UTC", datetime(2026, 10, 26, 15, 0, tzinfo=UTC)),
        ("2026-10-26 15:00:00.5 +0000 UTC", datetime(2026, 10, 26, 15, 0, 0, 500000, tzinfo=UTC)),
        ("2026-10-26 10:00:00.000001 -0500 EST", datetime(2026, 10, 26, 15, 0, 0, 1, tzinfo=UTC)),
    ],
)
def test_parse_time_variants(value, expected):
    assert parse_time_utc(value) == expected


def test_default_position_is_not_dropped_here():
    # lat 91 / lon 181 = "posición no disponible": se marca en Flink (silver), no se descarta.
    env = parse_message(make_frame(latitude=91.0, longitude=181.0), NOW)
    assert env.raw["MetaData"]["latitude"] == 91.0
