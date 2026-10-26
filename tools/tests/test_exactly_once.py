import pyarrow as pa

from radar_tools.exactly_once import check, format_report


def offsets(pairs):
    parts, offs = zip(*pairs, strict=True) if pairs else ((), ())
    return pa.table(
        {"kafka_partition": pa.array(parts, pa.int32()), "kafka_offset": pa.array(offs, pa.int64())}
    )


def test_clean_run_passes():
    t = offsets([(0, o) for o in range(100)] + [(1, o) for o in range(50, 80)])
    text, ok = format_report(check(t))
    assert ok, text
    assert "EXACTLY-ONCE OK" in text


def test_detects_duplicates():
    t = offsets([(0, o) for o in range(10)] + [(0, 3), (0, 4)])
    [r] = check(t)
    assert r.duplicates == 2 and r.gaps == 0
    assert not format_report([r])[1]


def test_detects_gaps():
    t = offsets([(0, o) for o in range(10) if o not in (4, 5, 6)])
    [r] = check(t)
    assert r.gaps == 3 and r.duplicates == 0
    assert not format_report([r])[1]


def test_empty_table_is_not_ok():
    text, ok = format_report(check(offsets([])))
    assert not ok
    assert "vacía" in text
