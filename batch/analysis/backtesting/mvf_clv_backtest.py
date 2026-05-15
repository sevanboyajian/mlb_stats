"""
mvf_clv_backtest.py
===================
MV-F CLV gate backtest (prompts 1–2 of 4).

Rehydrates MV-F candidate games from game_odds / games / venues and grades
gate-on vs gate-off performance at flat 1 unit on session-time away ML.

USAGE
-----
    python batch/analysis/backtesting/mvf_clv_backtest.py

    MLB_DB_PATH=C:\\path\\to\\mlb_stats.db python batch/analysis/backtesting/mvf_clv_backtest.py
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path
from statistics import mean
from typing import Any

# Repo root on sys.path so `python batch/analysis/backtesting/mvf_clv_backtest.py` works.
_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import connect as db_connect
from core.db.connection import get_db_path

MVF_CANDIDATE_SQL = """
SELECT
    g.game_pk,
    g.game_date_et                          AS game_date,
    g.home_team_id,
    g.away_team_id,
    g.home_score,
    g.away_score,
    g.wind_mph,
    g.wind_direction,
    g.temp_f,
    v.wind_effect,
    v.name                                  AS venue_name,

    go_open.away_ml                         AS open_away_ml,
    go_open.home_ml                         AS open_home_ml,
    go_open.captured_at_utc                 AS open_captured_utc,

    go_session.away_ml                      AS session_away_ml,
    go_session.home_ml                      AS session_home_ml,
    go_session.captured_at_utc              AS session_captured_utc,
    go_session.hours_before_game            AS session_hours_before

FROM games g
JOIN venues v ON v.venue_id = g.venue_id

JOIN game_odds go_open
    ON  go_open.game_pk       = g.game_pk
    AND go_open.market_type   = 'moneyline'
    AND go_open.is_opening_line = 1
    AND go_open.bookmaker     = (
            SELECT go2.bookmaker FROM game_odds go2
            WHERE  go2.game_pk = g.game_pk
              AND  go2.market_type = 'moneyline'
              AND  go2.is_opening_line = 1
              AND  go2.bookmaker IN (
                     'draftkings','fanduel','betmgm',
                     'betonlineag','sbro','oddswarehouse')
            ORDER BY CASE go2.bookmaker
                WHEN 'draftkings'    THEN 1
                WHEN 'fanduel'       THEN 2
                WHEN 'betmgm'        THEN 3
                WHEN 'betonlineag'   THEN 4
                WHEN 'sbro'          THEN 5
                WHEN 'oddswarehouse' THEN 6
                ELSE 7 END
            LIMIT 1)

JOIN game_odds go_session
    ON  go_session.game_pk     = g.game_pk
    AND go_session.market_type = 'moneyline'
    AND go_session.bookmaker   = go_open.bookmaker
    AND go_session.away_ml     IS NOT NULL
    AND go_session.hours_before_game BETWEEN 3.5 AND 8.0
    AND go_session.id = (
            SELECT go3.id FROM game_odds go3
            WHERE  go3.game_pk     = g.game_pk
              AND  go3.market_type = 'moneyline'
              AND  go3.bookmaker   = go_open.bookmaker
              AND  go3.away_ml     IS NOT NULL
              AND  go3.hours_before_game BETWEEN 3.5 AND 8.0
            ORDER BY go3.hours_before_game DESC
            LIMIT 1)

WHERE g.season         = ?
  AND g.game_type      = 'R'
  AND g.status         = 'Final'
  AND g.wind_direction LIKE '%IN%'
  AND g.wind_mph       >= 10
  AND v.wind_effect    IN ('HIGH', 'MODERATE')
  AND go_open.home_ml  BETWEEN -170 AND -130

ORDER BY g.game_date_et, g.game_pk
"""


def american_to_implied(ml: float | int | None) -> float | None:
    """American ML → implied win probability (0–1)."""
    if ml is None:
        return None
    x = float(ml)
    if x > 0:
        return 100.0 / (100.0 + abs(x))
    return abs(x) / (abs(x) + 100.0)


def enrich_mvf_row(row: dict[str, Any]) -> dict[str, Any]:
    """Add implied probs, CLV delta, gate flag, and outcome fields."""
    out = dict(row)
    open_imp = american_to_implied(out.get("open_away_ml"))
    session_imp = american_to_implied(out.get("session_away_ml"))
    out["open_away_implied"] = open_imp
    out["session_away_implied"] = session_imp

    if open_imp is not None and session_imp is not None:
        clv_delta_pp = (session_imp - open_imp) * 100.0
    else:
        clv_delta_pp = None

    out["clv_delta_pp"] = clv_delta_pp
    out["clv_gate_passed"] = (
        1 if (clv_delta_pp is not None and clv_delta_pp >= 0.5) else 0
    )

    home_score = out.get("home_score")
    away_score = out.get("away_score")
    if home_score is not None and away_score is not None:
        out["away_won"] = 1 if int(away_score) > int(home_score) else 0
        out["total_runs"] = int(home_score) + int(away_score)
    else:
        out["away_won"] = None
        out["total_runs"] = None

    return out


def build_mvf_candidate_universe(
    db_path: str,
    season: int = 2026,
) -> list[dict[str, Any]]:
    """
    Load MV-F candidate games (gate passed or not) for one season.

    Candidates match wind / venue / home-fav band filters; CLV gate is computed
    in Python from opening vs session-time away ML.
    """
    conn = db_connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(MVF_CANDIDATE_SQL, (int(season),))
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    return [enrich_mvf_row(r) for r in rows]


def pnl_units_for_candidate(candidate: dict[str, Any]) -> float:
    """Flat 1-unit P&L for away ML at session-time odds."""
    if candidate.get("away_won") != 1:
        return -1.0
    ml = candidate.get("session_away_ml")
    if ml is None:
        return -1.0
    x = float(ml)
    if x > 0:
        return x / 100.0
    return 100.0 / abs(x)


def _summarize_mvf_results(results: list[dict[str, Any]], label: str) -> dict[str, Any]:
    n_bets = len(results)
    if n_bets == 0:
        return {
            "label": label,
            "n_bets": 0,
            "n_wins": 0,
            "n_losses": 0,
            "win_rate": 0.0,
            "total_pnl": 0.0,
            "roi_pct": 0.0,
            "avg_odds": None,
            "avg_clv_delta": None,
        }

    n_wins = sum(int(r.get("away_won") or 0) for r in results)
    n_losses = n_bets - n_wins
    pnls = [float(r["pnl_units"]) for r in results]
    total_pnl = sum(pnls)
    odds_vals = [float(r["session_away_ml"]) for r in results if r.get("session_away_ml") is not None]
    clv_vals = [float(r["clv_delta_pp"]) for r in results if r.get("clv_delta_pp") is not None]

    return {
        "label": label,
        "n_bets": n_bets,
        "n_wins": n_wins,
        "n_losses": n_losses,
        "win_rate": round(n_wins / n_bets, 4),
        "total_pnl": round(total_pnl, 3),
        "roi_pct": round(total_pnl / n_bets * 100.0, 2),
        "avg_odds": round(mean(odds_vals), 1) if odds_vals else None,
        "avg_clv_delta": round(mean(clv_vals), 3) if clv_vals else None,
    }


def grade_mvf_candidates(
    candidates: list[dict[str, Any]],
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    """
    Grade MV-F away-ML bets at session odds; compare gate-on vs gate-off.

    Returns:
        gate_on_summary, gate_off_summary, gate_suppressed_summary,
        gate_on_results, gate_off_results
    """
    graded: list[dict[str, Any]] = []
    for c in candidates:
        row = dict(c)
        row["pnl_units"] = pnl_units_for_candidate(row)
        graded.append(row)

    gate_on_results = [r for r in graded if r.get("clv_gate_passed") == 1]
    gate_off_results = list(graded)
    gate_suppressed_results = [r for r in graded if r.get("clv_gate_passed") == 0]

    gate_on_summary = _summarize_mvf_results(gate_on_results, "Gate ON")
    gate_off_summary = _summarize_mvf_results(
        gate_off_results, "Gate OFF (all candidates)"
    )
    gate_suppressed_summary = _summarize_mvf_results(
        gate_suppressed_results, "Gate suppressed only (clv_gate_passed=0)"
    )

    return (
        gate_on_summary,
        gate_off_summary,
        gate_suppressed_summary,
        gate_on_results,
        gate_off_results,
    )


def _print_summary_table(summaries: list[dict[str, Any]]) -> None:
    cols = (
        ("label", 42),
        ("n_bets", 8),
        ("n_wins", 8),
        ("n_losses", 9),
        ("win_rate", 10),
        ("total_pnl", 11),
        ("roi_pct", 9),
        ("avg_odds", 10),
        ("avg_clv_delta", 14),
    )
    header = "".join(h.rjust(w) for h, w in cols)
    print(header)
    print("-" * len(header))
    for s in summaries:
        avg_odds = s["avg_odds"]
        avg_clv = s["avg_clv_delta"]
        line = (
            f"{s['label'][:42]:42}"
            f"{s['n_bets']:8d}"
            f"{s['n_wins']:8d}"
            f"{s['n_losses']:9d}"
            f"{s['win_rate']:10.4f}"
            f"{s['total_pnl']:11.3f}"
            f"{s['roi_pct']:9.2f}"
            f"{(f'{avg_odds:.1f}' if avg_odds is not None else 'N/A'):>10}"
            f"{(f'{avg_clv:+.3f}' if avg_clv is not None else 'N/A'):>14}"
        )
        print(line)


def _resolve_db_path() -> str:
    env = (os.getenv("MLB_DB_PATH") or "").strip()
    if env:
        p = Path(env)
        if not p.is_absolute():
            p = (_REPO_ROOT / p).resolve()
        return str(p)
    home_db = Path.home() / "mlb_data.db"
    if home_db.is_file():
        return str(home_db)
    return get_db_path()


def main() -> int:
    db_path = _resolve_db_path()
    if not Path(db_path).is_file():
        print(f"Database not found: {db_path}")
        return 1

    candidates = build_mvf_candidate_universe(db_path, season=2026)
    print(f"MV-F candidate universe: {len(candidates)} row(s)  (db={db_path})")

    if not candidates:
        print("No candidates — check Prompt 1 filters / data.")
        return 1

    n_pass = sum(1 for r in candidates if r.get("clv_gate_passed") == 1)
    print(f"  clv_gate_passed=1: {n_pass}  |  clv_gate_passed=0: {len(candidates) - n_pass}")

    (
        gate_on_summary,
        gate_off_summary,
        gate_suppressed_summary,
        _gate_on_results,
        _gate_off_results,
    ) = grade_mvf_candidates(candidates)

    print("\nMV-F CLV gate grading (flat 1u, session away ML):\n")
    _print_summary_table([
        gate_on_summary,
        gate_off_summary,
        gate_suppressed_summary,
    ])

    if os.getenv("MVF_CLV_DEBUG"):
        print("\nFirst 3 candidates (debug):")
        print(json.dumps(candidates[:3], indent=2, default=str))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
