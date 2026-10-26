import json

import pytest


def make_frame(mmsi=219000001, msg_type="PositionReport", time_utc=None, **meta):
    meta = {
        "MMSI": mmsi,
        "ShipName": "TEST VESSEL",
        "latitude": 55.5,
        "longitude": 12.3,
        "time_utc": time_utc or "2026-10-26 15:00:00.123456789 +0000 UTC",
        **meta,
    }
    return json.dumps(
        {
            "MessageType": msg_type,
            "MetaData": meta,
            "Message": {
                msg_type: {"UserID": mmsi, "Latitude": 55.5, "Longitude": 12.3, "Sog": 11.2}
            },
        }
    )


class FakeSink:
    def __init__(self):
        self.sent = []

    async def send(self, topic, value, key=None):
        self.sent.append((topic, key, json.loads(value)))


@pytest.fixture
def sink():
    return FakeSink()
