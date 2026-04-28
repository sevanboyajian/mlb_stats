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
    Deprecated shim for older call sites.

    Prefer: score_to_model_prob(score, signal_id=...)
    """
    return score_to_model_prob(int(score), None)


# Backtested win rates per signal — grounded in historical data.
# Source: 2025 backtests run April 2026.
# OWM: 116 games, 69.0% win rate
# LHP_FADE Band B [-150 to -203]: 83 games, 54.2% win rate
# MV-B: historical wind-out OVER rate ~62%
# H3b: secondary wind signal ~52%
# S1H2/S1: streak fade signals ~54%/~52%
# MV-F: wind-in ML fade ~53%
_SIGNAL_BASE_WIN_RATES: dict[str, float] = {
    "OWM": 0.690,
    "LHP_FADE": 0.542,
    "LHP_FADE_RL": 0.542,
    "MV-B": 0.620,
    "H3b": 0.522,
    "S1H2": 0.540,
    "S1": 0.520,
    "MV-F": 0.530,
}

# Base scores per signal — must match SIGNAL_BASE_SCORE in score_game.py
_SIGNAL_BASE_SCORES: dict[str, int] = {
    "OWM": 8,
    "LHP_FADE": 7,
    "LHP_FADE_RL": 6,
    "MV-B": 7,
    "H3b": 3,
    "S1H2": 8,
    "S1": 5,
    "MV-F": 8,
}

# Adjustment per score point above/below base score: ±1.5%
_SCORE_ADJ_PER_POINT = 0.015


def score_to_model_prob(score: int, signal_id: str | None = None) -> float:
    """
    Convert a confidence score to a win probability estimate.

    When signal_id is provided and recognized:
      - Uses the backtested win rate as the base probability
      - Adjusts ±1.5% per score point above/below the signal's base score
      - Clamps to 0.50–0.75

    When signal_id is None or unrecognized:
      - Falls back to calibration curve (calibration_log.csv) or linear ramp
      - Behaviour identical to previous implementation

    Examples:
      OWM base score=8, win rate=69.0%
        score=8 → 0.690  (at base)
        score=9 → 0.705  (one booster confirmed)
        score=7 → 0.675  (one booster below base)
      LHP_FADE base score=7, win rate=54.2%
        score=7 → 0.542
        score=8 → 0.557
    """
    if signal_id is not None and signal_id in _SIGNAL_BASE_WIN_RATES:
        base_prob = _SIGNAL_BASE_WIN_RATES[signal_id]
        base_score = _SIGNAL_BASE_SCORES.get(signal_id, int(score))
        delta = (int(score) - base_score) * _SCORE_ADJ_PER_POINT
        p = base_prob + delta
        return max(0.50, min(0.75, round(p, 4)))

    # ── Fallback: original calibration-curve logic ────────────────────────
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


EDGE_MIN = 0.02
EDGE_STRONG = 0.06
EDGE_MAX = 0.15

