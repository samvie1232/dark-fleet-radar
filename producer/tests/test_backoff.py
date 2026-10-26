import random

import pytest

from radar_producer.backoff import backoff_delay


@pytest.mark.parametrize("attempt,ceiling", [(1, 1), (2, 2), (3, 4), (6, 32), (7, 60), (50, 60)])
def test_delay_within_exponential_ceiling(attempt, ceiling):
    rng = random.Random(42)
    delays = [backoff_delay(attempt, rng=rng) for _ in range(500)]
    assert all(0 <= d <= ceiling for d in delays)
    assert max(delays) > ceiling * 0.9  # el jitter usa todo el rango


def test_no_delay_before_first_attempt():
    assert backoff_delay(0) == 0.0
