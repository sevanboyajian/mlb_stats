from __future__ import annotations

import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


# Shared location used by generate_daily_brief calibration logging
CAL_PATH = Path(__file__).resolve().parents[2] / "data" / "calibration_log.csv"
CURVE_PATH = Path(__file__).resolve().parents[2] / "data" / "calibrated_curve.json"
MIN_SAMPLES = 5


@dataclass(frozen=True)
class ScoreAgg:
    bets: int
    wins: int

    @property
    def win_rate(self) -> float:
        return (self.wins / self.bets) if self.bets else 0.0


def load_calibration_rows(path: Path = CAL_PATH) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as fh:
        r = csv.DictReader(fh)
        return [row for row in r]


def aggregate_by_score(rows: Iterable[dict[str, str]]) -> dict[int, ScoreAgg]:
    bets: dict[int, int] = defaultdict(int)
    wins: dict[int, int] = defaultdict(int)
    for row in rows:
        try:
            score = int(str(row.get("score") or "").strip())
        except Exception:
            continue
        try:
            res = int(str(row.get("result") or "").strip())
        except Exception:
            continue
        if res not in (0, 1):
            continue
        bets[score] += 1
        wins[score] += res
    out: dict[int, ScoreAgg] = {}
    for s, n in bets.items():
        if n >= MIN_SAMPLES:
            out[s] = ScoreAgg(bets=n, wins=int(wins.get(s, 0)))
    return out


def smooth_win_rates(win_rates: dict[int, float]) -> dict[int, float]:
    """Neighbor smoothing: score s = avg(s-1, s, s+1) when present."""
    smoothed: dict[int, float] = {}
    for s in sorted(win_rates):
        vals: list[float] = []
        for k in (s - 1, s, s + 1):
            if k in win_rates:
                vals.append(float(win_rates[k]))
        smoothed[s] = sum(vals) / len(vals) if vals else float(win_rates[s])
    return smoothed


def build_calibration_table(rows: Iterable[dict[str, str]]) -> dict[int, float]:
    """Return {score: smoothed win_rate} for scores with >=MIN_SAMPLES."""
    agg = aggregate_by_score(rows)
    raw = {s: a.win_rate for s, a in agg.items()}
    return smooth_win_rates(raw)


def interpolate_prob(score: int, table: dict[int, float]) -> float | None:
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


def print_score_table(table: dict[int, ScoreAgg]) -> None:
    print("score | bets | wins | win_rate")
    print("--------------------------------")
    for s in sorted(table.keys(), reverse=True):
        a = table[s]
        print(f"{s:<5} | {a.bets:<4} | {a.wins:<4} | {a.win_rate:.2f}")


def main() -> None:
    rows = load_calibration_rows(CAL_PATH)
    agg = aggregate_by_score(rows)
    print_score_table(agg)

    probs = build_calibration_table(rows)
    if probs:
        print("\nSmoothed calibration table (score -> p):")
        for s in sorted(probs.keys(), reverse=True):
            print(f"  {s}: {probs[s]:.3f}")
        # Save curve for model consumption (string keys per requested format)
        CURVE_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {str(k): float(round(v, 4)) for k, v in sorted(probs.items())}
        CURVE_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        print(f"\nSaved: {CURVE_PATH}")
    else:
        print("\n(no calibration data yet — need >=5 samples per score)")


if __name__ == "__main__":
    main()

