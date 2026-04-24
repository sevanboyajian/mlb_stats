"""
edge_utils.py
-------------
Small, stable utilities to translate model scores + market odds into an actionable edge
and conservative fractional-Kelly stake sizing.
"""

from __future__ import annotations

import csv
from functools import lru_cache
from pathlib import Path

def american_to_implied_prob(odds: int | None) -> float | None:
    if odds is None:
        return None
    o = int(odds)
    if o > 0:
        return 100.0 / (o + 100.0)
    return (-o) / ((-o) + 100.0)


def _fallback_prob(score: int) -> float:
    # Conservative until calibration exists:
    # 5 → 0.50, 15 → 0.60, 25 → 0.70, 30 → 0.75 (clamped)
    s = int(score)
    p = 0.50 + (s - 5) * 0.01
    return max(0.50, min(0.75, p))


def _smooth_table(raw: dict[int, float]) -> dict[int, float]:
    out: dict[int, float] = {}
    for s in sorted(raw):
        vals: list[float] = []
        for k in (s - 1, s, s + 1):
            if k in raw:
                vals.append(float(raw[k]))
        out[s] = sum(vals) / len(vals) if vals else float(raw[s])
    return out


def _interp(score: int, table: dict[int, float]) -> float | None:
    if not table:
        return None
    s = int(score)
    if s in table:
        return float(table[s])
    keys = sorted(table)
    lo = max((k for k in keys if k < s), default=None)
    hi = min((k for k in keys if k > s), default=None)
    if lo is None:
        return float(table[hi]) if hi is not None else None
    if hi is None:
        return float(table[lo])
    t = (s - lo) / (hi - lo)
    return float(table[lo] + t * (table[hi] - table[lo]))


@lru_cache(maxsize=1)
def _load_calibration_table() -> dict[int, float]:
    """
    Load data/calibration_log.csv -> {score: smoothed win_rate} for scores with >=5 samples.
    """
    path = Path(__file__).resolve().parents[2] / "data" / "calibration_log.csv"
    if not path.exists():
        return {}
    bets: dict[int, int] = {}
    wins: dict[int, int] = {}
    with path.open("r", newline="", encoding="utf-8") as fh:
        r = csv.DictReader(fh)
        for row in r:
            try:
                s = int(str(row.get("score") or "").strip())
                res = int(str(row.get("result") or "").strip())
            except Exception:
                continue
            if res not in (0, 1):
                continue
            bets[s] = bets.get(s, 0) + 1
            wins[s] = wins.get(s, 0) + res
    raw: dict[int, float] = {}
    for s, n in bets.items():
        if n >= 5:
            raw[s] = float(wins.get(s, 0)) / float(n)
    return _smooth_table(raw)


def score_to_model_prob(score: int) -> float:
    """
    Calibrated score → probability:
    - If calibration exists for the score: use it
    - Else interpolate between nearest calibrated scores
    - Clamp 0.50..0.75
    - If no calibration yet: fall back to 0.50 + (score-5)*0.01 (clamped)
    """
    tbl = _load_calibration_table()
    p = _interp(int(score), tbl) if tbl else None
    if p is None:
        return _fallback_prob(int(score))
    return max(0.50, min(0.75, float(p)))


def compute_edge(model_p: float, implied_p: float | None) -> float | None:
    if implied_p is None:
        return None
    edge = float(model_p) - float(implied_p)
    return min(edge, 0.12)


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


EDGE_MIN = 0.07
EDGE_STRONG = 0.06
EDGE_MAX = 0.15

