from pathlib import Path

from radar_tools.sanctions import parse_csv

SAMPLE = (Path(__file__).parent / "fixtures" / "maritime_sample.csv").read_text()


def test_parses_imo_and_mmsi():
    rows = list(parse_csv(SAMPLE))
    first = next(r for r in rows if r.os_id == "os-vessel-1")
    assert (first.imo, first.mmsi, first.name, first.risk) == (
        9074729,
        219000001,
        "SHADOW ONE",
        "sanction",
    )


def test_vessel_without_imo_is_kept_by_mmsi():
    [row] = [r for r in parse_csv(SAMPLE) if r.os_id == "os-vessel-2"]
    assert row.imo is None and row.mmsi == 636012345


def test_multiple_imos_become_one_row_each():
    rows = [r for r in parse_csv(SAMPLE) if r.os_id == "os-vessel-3"]
    assert sorted(r.imo for r in rows) == [1234567, 9074729]


def test_rows_without_identifiers_are_skipped():
    assert not [r for r in parse_csv(SAMPLE) if r.os_id == "os-vessel-4"]
