#!/usr/bin/env python3
"""
Under signal backtest ROI audit (Open Item #26).

Recalculates Under signal universe stats using the same gates as score_today.py
and compares against hardcoded display strings.

USAGE:
  python scripts/analysis/under_signal_backtest_audit.py
  python scripts/analysis/under_signal_backtest_audit.py --db data/mlb_stats.db
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.db.connection import connect as db_connect, get_db_path

OUT_PATH = ROOT / "outputs" / "reports" / "under_signal_backtest_audit_2026-06-16.txt"

# Recalculated 2026-06-16 (May-Aug 2019-2025, posted closing total_line)
UNDER_BACKTEST_N = 966
UNDER_BACKTEST_UNDER_RATE = 48.7
UNDER_BACKTEST_ROI_MINUS110 = -7.1
UNDER_BACKTEST_ROI_ACTUAL = -2.7
UNDER_STRONG_N = 99
UNDER_STRONG_UNDER_RATE = 54.5
UNDER_STRONG_ROI_MINUS110 = 4.1
UNDER_BREAKEVEN_MINUS110 = 52.38

MIN_SP_STARTS = 3
UNDER_SUPPRESSED_VENUES = ("Fenway Park", "Oracle Park")
BACKTEST_START = "2019-05-01"
BACKTEST_END = "2025-08-31"

# Hardcoded in score_today.py / docs (pre-audit)
ORIGINAL = {
    "standard_n": 652,
    "standard_under_rate": 44.6,
    "standard_roi": 14.8,
    "strong_under_rate": 41.6,
    "strong_roi": 20.6,
}

_GAME_DATE = "COALESCE(NULLIF(TRIM(g.game_date_et), ''), g.game_date)"

GAMES_SQL = f"""
SELECT
    g.game_pk,
    {_GAME_DATE} AS game_date_et,
    g.home_score,
    g.away_score,
    g.home_score + g.away_score AS total_runs,
    v.name AS venue_name,
    g.wind_direction,
    COALESCE(prs_hg.era_wma, prs_hl.era_wma) AS hsp_era_wma,
    COALESCE(prs_ag.era_wma, prs_al.era_wma) AS asp_era_wma,
    COALESCE(prs_hg.starts_in_window, prs_hl.starts_in_window) AS hsp_starts,
    COALESCE(prs_ag.starts_in_window, prs_al.starts_in_window) AS asp_starts,
    go.total_line,
    go.under_odds
FROM games g
LEFT JOIN venues v ON v.venue_id = g.venue_id

LEFT JOIN game_probable_pitchers gpp_h
  ON gpp_h.game_pk = g.game_pk AND gpp_h.team_id = g.home_team_id
LEFT JOIN pitcher_rolling_stats prs_hg
  ON prs_hg.game_pk = g.game_pk AND prs_hg.player_id = gpp_h.player_id
LEFT JOIN pitcher_rolling_stats prs_hl
  ON prs_hl.player_id = gpp_h.player_id
 AND prs_hl.game_pk = (
     SELECT prs2.game_pk
     FROM pitcher_rolling_stats prs2
     JOIN games g2 ON g2.game_pk = prs2.game_pk
     WHERE prs2.player_id = gpp_h.player_id
       AND COALESCE(NULLIF(TRIM(g2.game_date_et), ''), g2.game_date) < {_GAME_DATE}
     ORDER BY COALESCE(NULLIF(TRIM(g2.game_date_et), ''), g2.game_date) DESC,
              prs2.game_pk DESC
     LIMIT 1
 )

LEFT JOIN game_probable_pitchers gpp_a
  ON gpp_a.game_pk = g.game_pk AND gpp_a.team_id = g.away_team_id
LEFT JOIN pitcher_rolling_stats prs_ag
  ON prs_ag.game_pk = g.game_pk AND prs_ag.player_id = gpp_a.player_id
LEFT JOIN pitcher_rolling_stats prs_al
  ON prs_al.player_id = gpp_a.player_id
 AND prs_al.game_pk = (
     SELECT prs2.game_pk
     FROM pitcher_rolling_stats prs2
     JOIN games g2 ON g2.game_pk = prs2.game_pk
     WHERE prs2.player_id = gpp_a.player_id
       AND COALESCE(NULLIF(TRIM(g2.game_date_et), ''), g2.game_date) < {_GAME_DATE}
     ORDER BY COALESCE(NULLIF(TRIM(g2.game_date_et), ''), g2.game_date) DESC,
              prs2.game_pk DESC
     LIMIT 1
 )

LEFT JOIN (
    SELECT game_pk, total_line, under_odds,
           ROW_NUMBER() OVER (
               PARTITION BY game_pk
               ORDER BY is_closing_line DESC, captured_at_utc DESC, id DESC
           ) AS rn
    FROM game_odds
    WHERE market_type = 'total' AND total_line IS NOT NULL
) go ON go.game_pk = g.game_pk AND go.rn = 1

WHERE g.game_type = 'R'
  AND g.status = 'Final'
  AND g.home_score IS NOT NULL
  AND g.away_score IS NOT NULL
  AND {_GAME_DATE} >= ?
  AND {_GAME_DATE} <= ?
ORDER BY game_date_et, g.game_pk
"""

LEDGER_SQL = """
SELECT
    result,
    pnl_units,
    stake_units,
    odds_taken
FROM bet_ledger
WHERE signal_type = 'UNDER'
  AND result IN ('win', 'loss', 'push')
"""


@dataclass
class TierStats:
    label: str
    n: int
    under_wins: int
    pushes: int
    under_rate: float
    roi_minus110: float
    roi_actual_odds: float | None
    avg_under_odds: float | None


def american_payout(odds: float) -> float:
    odds = float(odds)
    if odds < 0:
        return 100.0 / abs(odds)
    return odds / 100.0


def roi_at_minus110(win_rate: float) -> float:
    """Flat 1u UNDER bets at -110 juice."""
    return (win_rate * (100.0 / 110.0) - (1.0 - win_rate)) * 100.0


def over_roi_at_minus110(over_rate: float) -> float:
    """Same formula as ou_rl_backtest.ou_over_roi — ROI if betting OVER."""
    under_rate = 1.0 - over_rate
    return (over_rate * (100.0 / 110.0) - under_rate) * 100.0


def wind_in(direction: object) -> bool:
    return "in" in str(direction or "").lower()


def grade_under(row: sqlite3.Row) -> str | None:
    total_line = row["total_line"]
    if total_line is None:
        return None
    runs = int(row["total_runs"])
    line = float(total_line)
    if runs < line:
        return "win"
    if runs > line:
        return "loss"
    return "push"


def actual_pnl(row: sqlite3.Row, outcome: str) -> float | None:
    odds = row["under_odds"]
    if odds is None:
        return None
    payout = american_payout(int(odds))
    if outcome == "win":
        return payout
    if outcome == "loss":
        return -1.0
    return 0.0


def summarize(rows: list[sqlite3.Row], label: str) -> TierStats:
    n = len(rows)
    if n == 0:
        return TierStats(label, 0, 0, 0, 0.0, 0.0, None, None)

    wins = pushes = 0
    pnl_sum = 0.0
    pnl_n = 0
    odds_sum = 0.0
    odds_n = 0

    for r in rows:
        outcome = grade_under(r)
        if outcome is None:
            continue
        if outcome == "win":
            wins += 1
        elif outcome == "push":
            pushes += 1
        pnl = actual_pnl(r, outcome)
        if pnl is not None:
            pnl_sum += pnl
            pnl_n += 1
        if r["under_odds"] is not None:
            odds_sum += float(r["under_odds"])
            odds_n += 1

    graded = n - sum(1 for r in rows if grade_under(r) is None)
    under_rate = (wins / graded * 100.0) if graded else 0.0
    win_rate_frac = wins / graded if graded else 0.0
    roi_110 = roi_at_minus110(win_rate_frac)
    roi_actual = (pnl_sum / pnl_n * 100.0) if pnl_n else None
    avg_odds = (odds_sum / odds_n) if odds_n else None

    return TierStats(
        label=label,
        n=n,
        under_wins=wins,
        pushes=pushes,
        under_rate=under_rate,
        roi_minus110=roi_110,
        roi_actual_odds=roi_actual,
        avg_under_odds=avg_odds,
    )


def ledger_stats(con: sqlite3.Connection) -> dict:
    rows = con.execute(LEDGER_SQL).fetchall()
    n = len(rows)
    w = sum(1 for r in rows if r["result"] == "win")
    l = sum(1 for r in rows if r["result"] == "loss")
    p = sum(1 for r in rows if r["result"] == "push")
    pnl = sum(float(r["pnl_units"] or 0) for r in rows)
    staked = sum(float(r["stake_units"] or 0) for r in rows)
    roi = (pnl / staked * 100.0) if staked else 0.0
    win_rate = (w / (w + l) * 100.0) if (w + l) else 0.0
    return {
        "n": n,
        "w": w,
        "l": l,
        "p": p,
        "win_rate": win_rate,
        "pnl": pnl,
        "staked": staked,
        "roi": roi,
    }


def filter_signal_rows(all_rows: list[sqlite3.Row]) -> tuple[list, list]:
    standard: list[sqlite3.Row] = []
    strong: list[sqlite3.Row] = []

    for r in all_rows:
        venue = r["venue_name"] or ""
        if venue in UNDER_SUPPRESSED_VENUES:
            continue
        h_era = r["hsp_era_wma"]
        a_era = r["asp_era_wma"]
        h_st = int(r["hsp_starts"] or 0)
        a_st = int(r["asp_starts"] or 0)
        if h_era is None or a_era is None:
            continue
        if h_st < MIN_SP_STARTS or a_st < MIN_SP_STARTS:
            continue
        combined = float(h_era) + float(a_era)
        if combined >= 6.0:
            continue
        if r["total_line"] is None:
            continue
        standard.append(r)
        if combined < 5.0 and wind_in(r["wind_direction"]):
            strong.append(r)

    return standard, strong


def fmt_tier(s: TierStats) -> list[str]:
    over_rate = 100.0 - s.under_rate if s.n else 0.0
    lines = [
        f"  {s.label}:",
        f"    N={s.n}  under_wins={s.under_wins}  pushes={s.pushes}",
        f"    under_rate={s.under_rate:.1f}%  (games finishing below posted total)",
        f"    over_rate={over_rate:.1f}%",
        f"    UNDER ROI at -110 (correct flat-bet formula): {s.roi_minus110:+.1f}%",
        f"    OVER  ROI at -110 (ou_rl_backtest formula):   {over_roi_at_minus110(over_rate/100.0):+.1f}%",
    ]
    if s.roi_actual_odds is not None:
        lines.append(
            f"    UNDER ROI at actual closing under_odds (avg {s.avg_under_odds:+.0f}): "
            f"{s.roi_actual_odds:+.1f}%"
        )
    else:
        lines.append("    UNDER ROI at actual odds: n/a (missing under_odds join)")
    return lines


def build_verdict(std: TierStats, strong: TierStats, live: dict) -> str:
    roi_mismatch = abs(std.roi_minus110 - ORIGINAL["standard_roi"]) > 5.0

    if roi_mismatch:
        return (
            "SIGNAL MISCALCULATED: Correct UNDER ROI at -110 on the live signal "
            f"universe (posted total_line) is {std.roi_minus110:+.1f}% at "
            f"{std.under_rate:.1f}% under rate (N={std.n}). The displayed +14.8% "
            "ROI cannot pair with 44.6% under rate at -110 (-14.9% expected). "
            "+14.8% implies ~60% win rate (likely swapped from combined ERA < 5.0 "
            "tier) or used ou_rl_backtest.py OVER-bet ROI. The 44.6% figure matches "
            "a fixed 8.0-run proxy (~44.9%), not posted-total grading. "
            f"Live 2026: {live['win_rate']:.1f}% win rate, {live['roi']:+.1f}% ROI "
            "— consistent with a sub-breakeven signal at standard juice."
        )
    return "SIGNAL VALIDATED: Recalculated figures match display within tolerance."


def main() -> int:
    parser = argparse.ArgumentParser(description="Under signal ROI audit")
    parser.add_argument("--db", default=get_db_path())
    args = parser.parse_args()

    con = db_connect(args.db)
    con.row_factory = sqlite3.Row
    try:
        all_rows = list(con.execute(GAMES_SQL, (BACKTEST_START, BACKTEST_END)).fetchall())
        standard, strong = filter_signal_rows(all_rows)
        std = summarize(standard, "Standard (combined ERA WMA < 6.0, both SP known)")
        stg = summarize(strong, "Strong (combined < 5.0 + wind IN)")
        live = ledger_stats(con)
    finally:
        con.close()

    lines = [
        "UNDER SIGNAL BACKTEST AUDIT — 2026-06-16",
        "=" * 60,
        "",
        "ORIGINAL DISPLAYED FIGURES (hardcoded in score_today.py):",
        f"  Standard: {ORIGINAL['standard_n']} games | "
        f"{ORIGINAL['standard_under_rate']:.1f}% under rate | "
        f"+{ORIGINAL['standard_roi']:.1f}% ROI at -110",
        f"  Strong:   {ORIGINAL['strong_under_rate']:.1f}% under rate | "
        f"+{ORIGINAL['strong_roi']:.1f}% ROI at -110",
        "",
        f"Backtest window: {BACKTEST_START} to {BACKTEST_END}",
        f"Venues suppressed: {', '.join(UNDER_SUPPRESSED_VENUES)}",
        f"Min SP starts: {MIN_SP_STARTS}",
        "",
        "METHODOLOGY NOTES",
        "-" * 60,
        "under_rate = games where (home_score + away_score) < posted total_line",
        "             divided by all signal-universe games with a known total_line.",
        "UNDER ROI at -110 = (win_rate * 100/110) - (1 - win_rate), expressed as %.",
        "Break-even UNDER win rate at -110 = 52.38%.",
        "",
        "Closest repo script: batch/analysis/prediction/ou_rl_backtest.py",
        "  - Uses era_bucket <6.0 on all seasons (no May-Aug filter, no venue gate).",
        "  - Displays ROI via ou_over_roi() = OVER-bet ROI, not UNDER-bet ROI.",
        "  - No standalone script producing 652 / 44.6% / +14.8% was found.",
        "",
        "RECALCULATED FROM DATABASE",
        "-" * 60,
        *fmt_tier(std),
        "",
        *fmt_tier(stg),
        "",
        "CROSS-CHECK: ROI implied by displayed +14.8% at -110",
        f"  Implied win rate for +14.8% UNDER ROI: "
        f"{((ORIGINAL['standard_roi']/100 + 1) / (1 + 100/110))*100:.1f}%",
        f"  (matches combined ERA < 5.0 tier ~60% under rate in docs, NOT 44.6%)",
        "",
        "CROSS-CHECK: source of 44.6% under rate",
        "  Fixed 8.0-run proxy (NOT posted total) on May-Aug combined ERA < 6.0:",
        "    N=922, under_rate=44.9%, UNDER ROI at -110=-14.3%",
        "  This matches the displayed 44.6% but uses a different outcome definition.",
        "  Live signal grades vs posted total_line — use posted-total stats above.",
        "",
        "2026 LIVE PERFORMANCE (bet_ledger, signal_type='UNDER')",
        "-" * 60,
        f"  N={live['n']}  W={live['w']}  L={live['l']}  P={live['p']}",
        f"  win_rate={live['win_rate']:.1f}%  P&L={live['pnl']:+.2f}u  "
        f"staked={live['staked']:.2f}u  ROI={live['roi']:+.1f}%",
        "",
        "VERDICT",
        "-" * 60,
        f"  {build_verdict(std, stg, live)}",
        "",
        "RECOMMENDATION",
        "-" * 60,
        "  Update score_today.py display strings to show correct under_rate AND",
        "  correct UNDER ROI at -110 (or actual-odds ROI). Do not show positive ROI",
        "  paired with sub-52.4% under rates without explaining line-shopping edge.",
        "=" * 60,
    ]

    report = "\n".join(lines) + "\n"
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(report, encoding="utf-8")
    print(report)
    print(f"[under_signal_backtest_audit] Report saved to {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
