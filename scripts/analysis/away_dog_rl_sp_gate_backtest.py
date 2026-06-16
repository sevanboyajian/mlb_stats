#!/usr/bin/env python3
"""
Away Dog RL — home starting pitcher ERA WMA gate backtest.

Segments historical AWAY_DOG_RL staked bets by home SP quality tier and
simulates skipping elite/strong home SP matchups (hsp_era_wma < 3.5).

Usage:
    python scripts/analysis/away_dog_rl_sp_gate_backtest.py
    python scripts/analysis/away_dog_rl_sp_gate_backtest.py --db path/to/mlb_stats.db
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.db.connection import connect as db_connect, get_db_path

ET = ZoneInfo("America/New_York")
OUT_PATH = ROOT / "outputs" / "reports" / "away_dog_rl_sp_gate_backtest.txt"

TIER_ORDER = [
    "Elite (<2.5)",
    "Strong (2.5-3.49)",
    "Average (3.5-4.99)",
    "Weak (5.0+)",
    "Missing",
]

QUERY = """
SELECT
    bl.id,
    bl.game_date,
    bl.game_pk,
    bl.odds_taken,
    bl.stake_units,
    bl.result,
    bl.pnl_units,
    bl.model_version,
    bl.source,
    g.home_team_id,
    g.away_team_id,
    gpp.player_id       AS home_sp_player_id,
    prs.era_wma         AS hsp_era_wma,
    prs.starts_in_window AS hsp_starts_in_window
FROM bet_ledger bl
JOIN games g
    ON g.game_pk = bl.game_pk
LEFT JOIN game_probable_pitchers gpp
    ON gpp.game_pk = bl.game_pk
   AND gpp.team_id = g.home_team_id
LEFT JOIN pitcher_rolling_stats prs
    ON prs.player_id = gpp.player_id
   AND prs.game_pk  = bl.game_pk
WHERE bl.signal_type = 'AWAY_DOG_RL'
  AND bl.result IN ('win', 'loss', 'push')
ORDER BY bl.game_date, bl.id
"""


def _norm_bet_key(row: sqlite3.Row) -> tuple[int, str]:
    return (int(row["game_pk"]), str(row["game_date"]))


def _src_rank(source: str | None) -> int:
    s = (source or "brief").strip().lower()
    if s == "brief":
        return 0
    if s == "brief_late":
        return 1
    if s == "score_today":
        return 2
    return 3


def dedupe_rows(rows: list[sqlite3.Row]) -> list[sqlite3.Row]:
    """One row per game_date+game_pk; prefer brief over score_today."""
    by_key: dict[tuple[int, str], sqlite3.Row] = {}
    for r in rows:
        key = _norm_bet_key(r)
        prev = by_key.get(key)
        if prev is None or _src_rank(r["source"]) < _src_rank(prev["source"]):
            by_key[key] = r
    return list(by_key.values())


def classify_tier(hsp_era_wma: float | None, starts: int | None) -> str:
    if hsp_era_wma is None or starts is None or int(starts) < 3:
        return "Missing"
    era = float(hsp_era_wma)
    if era < 2.5:
        return "Elite (<2.5)"
    if era < 3.5:
        return "Strong (2.5-3.49)"
    if era < 5.0:
        return "Average (3.5-4.99)"
    return "Weak (5.0+)"


def breakeven_cover_pct(american_odds: int | None) -> float | None:
    if american_odds is None:
        return None
    o = int(american_odds)
    if o == 0:
        return None
    if o < 0:
        return abs(o) / (abs(o) + 100.0) * 100.0
    return 100.0 / (o + 100.0) * 100.0


@dataclass
class TierStats:
    tier: str
    n: int = 0
    wins: int = 0
    losses: int = 0
    pushes: int = 0
    pnl: float = 0.0
    stake: float = 0.0
    odds_sum: int = 0
    odds_n: int = 0

    def add(self, row: sqlite3.Row) -> None:
        self.n += 1
        res = (row["result"] or "").lower()
        if res == "win":
            self.wins += 1
        elif res == "loss":
            self.losses += 1
        elif res == "push":
            self.pushes += 1
        self.pnl += float(row["pnl_units"] or 0.0)
        self.stake += float(row["stake_units"] or 0.0)
        if row["odds_taken"] is not None:
            self.odds_sum += int(row["odds_taken"])
            self.odds_n += 1

    @property
    def cover_pct(self) -> float | None:
        denom = self.wins + self.losses
        if denom == 0:
            return None
        return 100.0 * self.wins / denom

    @property
    def roi_pct(self) -> float | None:
        if self.stake <= 0:
            return None
        return 100.0 * self.pnl / self.stake

    @property
    def avg_breakeven_pct(self) -> float | None:
        if self.odds_n == 0:
            return None
        avg_odds = self.odds_sum / self.odds_n
        return breakeven_cover_pct(int(round(avg_odds)))


def aggregate(rows: list[sqlite3.Row], *, model_version: str | None = None) -> dict[str, TierStats]:
    stats = {t: TierStats(tier=t) for t in TIER_ORDER}
    for r in rows:
        if model_version is not None and str(r["model_version"] or "") != model_version:
            continue
        tier = classify_tier(r["hsp_era_wma"], r["hsp_starts_in_window"])
        stats[tier].add(r)
    return stats


def _fmt_pct(val: float | None, width: int = 5) -> str:
    if val is None:
        return "  n/a"
    return f"{val:>{width}.1f}%"


def _fmt_pnl(val: float) -> str:
    return f"{val:+6.2f}"


def format_tier_table(
    stats: dict[str, TierStats],
    *,
    default_be: float = 65.5,
) -> list[str]:
    lines = [
        f"{'Tier':<22}{'N':>4}{'W':>4}{'L':>4}{'Win%':>7}{'P&L':>8}{'ROI%':>8}{'BEvn%':>8}",
    ]
    for tier in TIER_ORDER:
        s = stats[tier]
        be = s.avg_breakeven_pct if s.avg_breakeven_pct is not None else (
            None if tier == "Missing" else default_be
        )
        be_s = _fmt_pct(be, 6) if be is not None else "    n/a"
        win_s = _fmt_pct(s.cover_pct, 6) if s.cover_pct is not None else "    n/a"
        roi_s = _fmt_pct(s.roi_pct, 7) if s.roi_pct is not None else "     n/a"
        lines.append(
            f"{tier:<22}{s.n:>4}{s.wins:>4}{s.losses:>4}{win_s}"
            f"{_fmt_pnl(s.pnl):>8}u{roi_s}{be_s}"
        )
    return lines


def gate_simulation(rows: list[sqlite3.Row]) -> dict[str, float | int]:
    removed = [r for r in rows if classify_tier(r["hsp_era_wma"], r["hsp_starts_in_window"]) in (
        "Elite (<2.5)",
        "Strong (2.5-3.49)",
    )]
    kept = [r for r in rows if r not in removed]

    def _totals(subset: list[sqlite3.Row]) -> tuple[float, float]:
        pnl = sum(float(r["pnl_units"] or 0.0) for r in subset)
        stake = sum(float(r["stake_units"] or 0.0) for r in subset)
        return pnl, stake

    all_pnl, all_stake = _totals(rows)
    gated_pnl, gated_stake = _totals(kept)
    rem_pnl, _ = _totals(removed)

    return {
        "removed_n": len(removed),
        "removed_pnl": rem_pnl,
        "gated_pnl": gated_pnl,
        "gated_stake": gated_stake,
        "gated_roi": (100.0 * gated_pnl / gated_stake) if gated_stake else 0.0,
        "ungated_pnl": all_pnl,
        "ungated_stake": all_stake,
        "ungated_roi": (100.0 * all_pnl / all_stake) if all_stake else 0.0,
    }


def recommendation(stats: dict[str, TierStats], gate: dict[str, float | int]) -> str:
    elite = stats["Elite (<2.5)"]
    strong = stats["Strong (2.5-3.49)"]
    average = stats["Average (3.5-4.99)"]
    weak = stats["Weak (5.0+)"]
    combined_n = elite.wins + elite.losses + strong.wins + strong.losses
    combined_w = elite.wins + strong.wins
    be = 65.5
    pnl_improve = float(gate["gated_pnl"]) - float(gate["ungated_pnl"])

    lines: list[str] = []

    if combined_n < 5:
        lines.append(
            "GATE NOT SUPPORTED: Elite+Strong sample too small "
            f"(N={combined_n} graded, need >= 5) - insufficient data for SP gate"
        )
        return "\n  ".join(lines)

    combined_cover = 100.0 * combined_w / combined_n
    avg_cover = average.cover_pct
    weak_cover = weak.cover_pct

    if combined_cover < 58.0 and pnl_improve >= 0.25:
        lines.append(
            f"GATE CONFIRMED: Elite+Strong cover rate {combined_cover:.1f}% below "
            f"breakeven {be:.1f}% (N={combined_n}) - recommend adding "
            "hsp_era_wma >= 3.5 gate to Away Dog RL signal"
        )
    elif combined_cover < be and pnl_improve >= 1.0:
        lines.append(
            f"GATE CONFIRMED: Elite+Strong cover {combined_cover:.1f}% below breakeven "
            f"{be:.1f}% and gate simulation improves P&L by {pnl_improve:+.2f}u - "
            "recommend hsp_era_wma >= 3.5 gate"
        )
    else:
        lines.append(
            "GATE NOT SUPPORTED: cover rates within normal variance across tiers - "
            "no SP gate warranted at this sample size"
        )

    # Supplemental findings (always printed)
    if avg_cover is not None and weak_cover is not None:
        if weak_cover > 65.0 and (avg_cover or 0) < 55.0:
            lines.append(
                f"NOTE: Weak home SP tier ({weak_cover:.1f}% cover, N={weak.n}) "
                f"outperforms Average ({avg_cover:.1f}%, N={average.n}) - "
                "edge may concentrate vs weak SP, not vs average SP"
            )
    if pnl_improve > 0:
        lines.append(
            f"Gate simulation: skipping hsp_era_wma < 3.5 improves P&L by "
            f"{pnl_improve:+.2f}u ({gate['ungated_pnl']:+.2f} -> {gate['gated_pnl']:+.2f})"
        )
    return "\n  ".join(lines)


def build_report(rows: list[sqlite3.Row], raw_count: int) -> str:
    ts = datetime.now(tz=ET).strftime("%Y-%m-%d %H:%M %Z")
    stats = aggregate(rows)
    v2_stats = aggregate(rows, model_version="v2")
    gate = gate_simulation(rows)

    sp_joined = sum(
        1 for r in rows
        if r["hsp_era_wma"] is not None and (r["hsp_starts_in_window"] or 0) >= 3
    )
    join_pct = (100.0 * sp_joined / len(rows)) if rows else 0.0

    lines = [
        "------------------------------------------------------------",
        "AWAY DOG RL - HOME SP QUALITY GATE BACKTEST",
        f"Generated: {ts}",
        f"Total Away Dog RL bets in sample: {len(rows)} "
        f"(raw graded rows: {raw_count}, deduped by game_pk+date)",
        f"SP data joined (era_wma + starts>=3): {sp_joined}/{len(rows)} ({join_pct:.1f}%)",
        "------------------------------------------------------------",
        "SP TIER BREAKDOWN",
        *format_tier_table(stats),
        "------------------------------------------------------------",
        "GATE SIMULATION (exclude hsp_era_wma < 3.5)",
        f"Bets removed: {gate['removed_n']}  |  P&L removed: {gate['removed_pnl']:+.2f} u",
        (
            f"Gated P&L: {gate['gated_pnl']:+.2f} u  |  Gated ROI: {gate['gated_roi']:+.1f}%"
        ),
        (
            f"vs. Ungated P&L: {gate['ungated_pnl']:+.2f} u  |  "
            f"Ungated ROI: {gate['ungated_roi']:+.1f}%"
        ),
        "------------------------------------------------------------",
        "V2 MODEL BREAKDOWN BY TIER",
        *format_tier_table(v2_stats),
        "------------------------------------------------------------",
        "RECOMMENDATION:",
        *[f"  {ln}" for ln in recommendation(stats, gate).splitlines()],
        "------------------------------------------------------------",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Away Dog RL home SP gate backtest")
    parser.add_argument("--db", default=get_db_path())
    parser.add_argument(
        "--no-dedupe",
        action="store_true",
        help="Use all bet_ledger rows (including brief+score_today duplicates)",
    )
    args = parser.parse_args()

    con = db_connect(args.db)
    con.row_factory = sqlite3.Row
    try:
        raw = con.execute(QUERY).fetchall()
    finally:
        con.close()

    rows = list(raw) if args.no_dedupe else dedupe_rows(list(raw))
    report = build_report(rows, raw_count=len(raw))

    print(report)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(report + "\n", encoding="utf-8")
    print(f"\n[away_dog_rl_sp_gate] Report saved to {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
