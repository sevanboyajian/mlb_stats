"""
edge_utils.py
-------------
Small, stable utilities to translate model scores + market odds into an actionable edge
and conservative fractional-Kelly stake sizing.
"""

from __future__ import annotations


def american_to_implied_prob(odds: int | None) -> float | None:
    if odds is None:
        return None
    o = int(odds)
    if o > 0:
        return 100.0 / (o + 100.0)
    return (-o) / ((-o) + 100.0)


def score_to_model_prob(score: int) -> float:
    # Linear + cap; simple and stable
    # 5 → 0.50, 15 → 0.70, 25 → 0.90, 28+ → ~0.95
    s = int(score)
    p = 0.50 + (s - 5) * 0.02
    return max(0.50, min(0.95, p))


def compute_edge(model_p: float, implied_p: float | None) -> float | None:
    if implied_p is None:
        return None
    return float(model_p) - float(implied_p)


def fractional_kelly(model_p: float, odds: int, fraction: float = 0.25) -> float:
    """
    Returns recommended fraction of bankroll (e.g., 0.02 = 2%).
    Uses 1/4 Kelly by default for safety.
    """
    o = int(odds)
    if o > 0:
        b = o / 100.0
    else:
        b = 100.0 / (-o)
    q = 1.0 - float(model_p)
    kelly = (b * float(model_p) - q) / b
    kelly = max(0.0, kelly)  # never negative
    return kelly * float(fraction)


EDGE_MIN = 0.05
EDGE_STRONG = 0.06
EDGE_MAX = 0.15

