from datetime import UTC, datetime

from radar_tools import evaluate as ev


def test_imo_checksum():
    assert ev.imo_valid(9074729)
    assert not ev.imo_valid(9074728)
    import random

    rng = random.Random(1)
    assert all(ev.imo_valid(ev.make_imo(rng)) for _ in range(50))


def test_plan_has_labels_for_every_type_and_unique_vessels():
    plan = ev.build_plan(seed=1, n=4, listed_imos=[9074729, 1234567])
    for t in ev.TYPES:
        pos = [lb for lb in plan.labels if lb.alert_type == t and lb.positive]
        neg = [lb for lb in plan.labels if lb.alert_type == t and not lb.positive]
        assert neg, t
        assert pos, t
    # Cada caso etiquetado usa un MMSI propio.
    mmsis = [lb.mmsi for lb in plan.labels]
    assert len(mmsis) == len(set(mmsis))
    # Todos los mensajes caen dentro de la ventana simulada.
    assert all(0 <= p.minute <= ev.HOURS * 60 for p in plan.pings)


def test_messages_are_time_ordered_and_parseable():
    plan = ev.build_plan(seed=2, n=2, listed_imos=[])
    t0 = datetime(2030, 1, 1, tzinfo=UTC)
    times = [t for t, _, _ in ev.messages(plan, t0)]
    assert times == sorted(times)
    import json

    _, key, value = next(ev.messages(plan, t0))
    env = json.loads(value)
    assert {"mmsi", "message_type", "event_time", "ingested_at", "raw"} <= env.keys()
    assert env["event_time"].endswith("Z") and key == str(env["mmsi"]).encode()
    assert json.loads(env["raw"])["MetaData"]["time_utc"].endswith("+0000 UTC")


def test_score_counts_tp_fp_fn():
    labels = [
        ev.Label("a", "dark_gap", 1, True),
        ev.Label("b", "dark_gap", 2, True),
        ev.Label("c", "dark_gap", 3, False),
    ]
    alerts = [("dark_gap", 1), ("dark_gap", 1), ("dark_gap", 3), ("dark_gap", 99)]
    r = ev.score(labels, alerts)["dark_gap"]
    assert (r["tp"], r["fn"], r["fp"], r["fp_on_hard_negatives"]) == (1, 1, 2, 1)
    assert r["precision"] == 1 / 3 and r["recall"] == 0.5
    assert r["alerts_per_detected_case"] == 2
    assert r["missed"] == ["b"] and r["false_alarms"] == ["c"]
