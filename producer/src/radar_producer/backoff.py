"""Backoff exponencial con jitter completo para reconectar al WebSocket."""

from __future__ import annotations

import random


def backoff_delay(
    attempt: int,
    base: float = 1.0,
    cap: float = 60.0,
    rng: random.Random | None = None,
) -> float:
    """Espera antes del intento `attempt` (1, 2, ...): uniforme en [0, min(cap, base*2^(n-1))].

    El jitter completo evita que varias instancias reconecten todas a la vez.
    """
    if attempt < 1:
        return 0.0
    ceiling = min(cap, base * 2 ** (attempt - 1))
    return (rng or random).uniform(0, ceiling)
