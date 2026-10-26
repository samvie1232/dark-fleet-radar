import pytest

from radar_producer.config import DEFAULT_BOUNDING_BOXES, Config, ConfigError


def test_requires_api_key():
    with pytest.raises(ConfigError):
        Config.from_env({})


def test_defaults_and_subscription():
    cfg = Config.from_env({"AISSTREAM_API_KEY": "k"})
    assert cfg.bounding_boxes == DEFAULT_BOUNDING_BOXES
    assert cfg.record_dir is None
    sub = cfg.subscription()
    assert sub["APIKey"] == "k"
    assert "PositionReport" in sub["FilterMessageTypes"]


def test_custom_boxes_and_types():
    cfg = Config.from_env(
        {
            "AISSTREAM_API_KEY": "k",
            "AIS_BOUNDING_BOXES": "[[[8, -80], [13, -74]]]",
            "AIS_MESSAGE_TYPES": "PositionReport, ShipStaticData",
        }
    )
    assert cfg.bounding_boxes == [[[8, -80], [13, -74]]]
    assert cfg.message_types == ["PositionReport", "ShipStaticData"]


@pytest.mark.parametrize("boxes", ["no-json", "[]", "[[[95, 0], [10, 10]]]", "[[1, 2]]"])
def test_rejects_bad_boxes(boxes):
    with pytest.raises(ConfigError):
        Config.from_env({"AISSTREAM_API_KEY": "k", "AIS_BOUNDING_BOXES": boxes})
