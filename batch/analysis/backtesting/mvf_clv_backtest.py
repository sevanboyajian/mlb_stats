"""
mvf_clv_backtest.py
===================
MV-F CLV gate backtest (prompts 1–4 of 4).

Rehydrates MV-F candidate games from game_odds / games / venues and grades
gate-on vs gate-off performance at flat 1 unit on session-time away ML.

USAGE
-----
    python batch/analysis/backtesting/mvf_clv_backtest.py

    python batch/analysis/backtesting/mvf_clv_backtest.py
    python batch/analysis/backtesting/mvf_clv_backtest.py --report

    MLB_DB_PATH=C:\\path\\to\\mlb_stats.db python batch/analysis/backtesting/mvf_clv_backtest.py --report
"""

from __future__ import annotations

import argparse
import datetime as dt
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

# Effective hours before first pitch for a game_odds row: prefer stored column;
# if NULL (common on older backfills), derive from first pitch - snapshot time.
# ``game_alias`` must reference the games row for that game_pk (e.g. ``g`` or ``gx``).
_SESSION_EFF_HOURS_TMPL = """
COALESCE(
    {odds_alias}.hours_before_game,
    (
        julianday(replace(trim(replace({game_alias}.game_start_utc, 'Z', '')), 'T', ' '))
        - julianday(replace(trim(replace({odds_alias}.captured_at_utc, 'Z', '')), 'T', ' '))
    ) * 24.0
)
""".replace("\n", " ").strip()


def _session_eff_hours(odds_alias: str, game_alias: str = "g") -> str:
    return _SESSION_EFF_HOURS_TMPL.format(odds_alias=odds_alias, game_alias=game_alias)


_SESSION_JOIN_COND = _session_eff_hours("go_session", "g")
_SESSION_SUBQUERY = _session_eff_hours("go3", "gx")

MVF_CANDIDATE_SQL = f"""
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
    ({_SESSION_JOIN_COND})                  AS session_hours_before

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
    AND g.game_start_utc       IS NOT NULL
    AND ({_SESSION_JOIN_COND}) BETWEEN 3.5 AND 8.0
    AND go_session.id = (
            SELECT go3.id FROM game_odds go3
            INNER JOIN games gx ON gx.game_pk = go3.game_pk
            WHERE  go3.game_pk     = g.game_pk
              AND  go3.market_type = 'moneyline'
              AND  go3.bookmaker   = go_open.bookmaker
              AND  go3.away_ml     IS NOT NULL
              AND ({_SESSION_SUBQUERY}) BETWEEN 3.5 AND 8.0
            ORDER BY ({_SESSION_SUBQUERY}) DESC
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


def _clv_delta(r: dict[str, Any]) -> float | None:
    v = r.get("clv_delta_pp")
    if v is None:
        return None
    return float(v)


def _in_clv_negative(r: dict[str, Any]) -> bool:
    d = _clv_delta(r)
    return d is not None and d < 0


def _in_clv_zero_to_half(r: dict[str, Any]) -> bool:
    d = _clv_delta(r)
    return d is not None and 0 <= d < 0.5


def _in_clv_half_to_two(r: dict[str, Any]) -> bool:
    d = _clv_delta(r)
    return d is not None and 0.5 <= d < 2.0


def _in_clv_two_plus(r: dict[str, Any]) -> bool:
    d = _clv_delta(r)
    return d is not None and d >= 2.0


def _wind_mph(r: dict[str, Any]) -> float | None:
    v = r.get("wind_mph")
    if v is None:
        return None
    return float(v)


def _in_wind_10_to_14(r: dict[str, Any]) -> bool:
    w = _wind_mph(r)
    return w is not None and 10 <= w <= 14


def _in_wind_15_to_19(r: dict[str, Any]) -> bool:
    w = _wind_mph(r)
    return w is not None and 15 <= w <= 19


def _in_wind_20_plus(r: dict[str, Any]) -> bool:
    w = _wind_mph(r)
    return w is not None and w >= 20


def _venue_effect(r: dict[str, Any]) -> str:
    return str(r.get("wind_effect") or "").strip().upper()


def _open_home_ml(r: dict[str, Any]) -> int | None:
    v = r.get("open_home_ml")
    if v is None:
        return None
    return int(v)


# (segment_key, display_label, predicate)
MVF_SEGMENT_SPECS: list[tuple[str, str, Any]] = [
    # CLV gate buckets
    ("clv_negative", "clv_negative (clv_delta_pp < 0)", _in_clv_negative),
    ("clv_zero_to_half", "clv_zero_to_half (0 <= clv < 0.5)", _in_clv_zero_to_half),
    ("clv_half_to_two", "clv_half_to_two (0.5 <= clv < 2.0)", _in_clv_half_to_two),
    ("clv_two_plus", "clv_two_plus (clv >= 2.0)", _in_clv_two_plus),
    # Wind speed
    ("wind_10_to_14", "wind_10_to_14 (10-14 mph)", _in_wind_10_to_14),
    ("wind_15_to_19", "wind_15_to_19 (15-19 mph)", _in_wind_15_to_19),
    ("wind_20_plus", "wind_20_plus (>= 20 mph)", _in_wind_20_plus),
    # Venue sensitivity
    ("venue_HIGH", "venue_HIGH", lambda r: _venue_effect(r) == "HIGH"),
    ("venue_MODERATE", "venue_MODERATE", lambda r: _venue_effect(r) == "MODERATE"),
    # Home fav sub-buckets (open line)
    (
        "fav_130_to_145",
        "fav_130_to_145 (open home -145..-130)",
        lambda r: (h := _open_home_ml(r)) is not None and -145 <= h <= -130,
    ),
    (
        "fav_146_to_160",
        "fav_146_to_160 (open home -160..-146)",
        lambda r: (h := _open_home_ml(r)) is not None and -160 <= h <= -146,
    ),
    (
        "fav_161_to_170",
        "fav_161_to_170 (open home -170..-161)",
        lambda r: (h := _open_home_ml(r)) is not None and -170 <= h <= -161,
    ),
]

MVF_SEGMENT_GROUP_ORDER: list[tuple[str, list[str]]] = [
    ("By CLV gate bucket", ["clv_negative", "clv_zero_to_half", "clv_half_to_two", "clv_two_plus"]),
    ("By wind speed", ["wind_10_to_14", "wind_15_to_19", "wind_20_plus"]),
    ("By venue wind sensitivity", ["venue_HIGH", "venue_MODERATE"]),
    (
        "By home favorite strength (open line)",
        ["fav_130_to_145", "fav_146_to_160", "fav_161_to_170"],
    ),
]

_SEGMENT_LABEL_BY_KEY = {k: lbl for k, lbl, _ in MVF_SEGMENT_SPECS}
_SEGMENT_PRED_BY_KEY = {k: pred for k, _, pred in MVF_SEGMENT_SPECS}


def segment_mvf_results(
    gate_off_results: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """
    Break Gate OFF candidates into sub-groups; each value is a summary dict
    (same shape as grade_mvf_candidates summaries).
    """
    out: dict[str, dict[str, Any]] = {}
    for key, label, pred in MVF_SEGMENT_SPECS:
        bucket = [r for r in gate_off_results if pred(r)]
        out[key] = _summarize_mvf_results(bucket, label)
    return out


def _print_segment_blocks(segments: dict[str, dict[str, Any]], *, min_n: int = 3) -> None:
    """Print segmented summaries; flag buckets with n_bets < min_n."""
    for group_title, keys in MVF_SEGMENT_GROUP_ORDER:
        print(f"\n{group_title}")
        print("-" * len(group_title))
        for key in keys:
            s = segments.get(key)
            if s is None:
                continue
            label = _SEGMENT_LABEL_BY_KEY.get(key, key)
            skip = s["n_bets"] < min_n
            suffix = "  (n<3 - skip)" if skip else ""
            print(
                f"  {label}{suffix}\n"
                f"    n_bets={s['n_bets']}  win_rate={s['win_rate']:.4f}  "
                f"total_pnl={s['total_pnl']:+.3f}  roi_pct={s['roi_pct']:+.2f}%"
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


REPORTS_DIR = _REPO_ROOT / "reports"


def _compact_summary_table_lines(summaries: list[dict[str, Any]]) -> list[str]:
    """Fixed-width: Group | N | W | L | Win% | P&L | ROI%"""
    hdr = f"{'Group':<28} {'N':>4} {'W':>4} {'L':>4} {'Win%':>7} {'P&L':>8} {'ROI%':>8}"
    sep = "-" * len(hdr)
    lines = [hdr, sep]
    for s in summaries:
        short = s["label"][:28]
        lines.append(
            f"{short:<28} {s['n_bets']:4d} {s['n_wins']:4d} {s['n_losses']:4d} "
            f"{s['win_rate'] * 100:6.1f}% {s['total_pnl']:+8.3f} {s['roi_pct']:+7.2f}%"
        )
    return lines


def _format_verdict_block(
    gate_on_summary: dict[str, Any],
    gate_off_summary: dict[str, Any],
    gate_suppressed_summary: dict[str, Any],
) -> list[str]:
    on_roi = float(gate_on_summary["roi_pct"])
    off_roi = float(gate_off_summary["roi_pct"])
    lines: list[str] = []
    if off_roi > on_roi:
        lines.append(
            "VERDICT: CLV gate HURTS performance. Gate OFF ROI "
            f"({off_roi:.2f}%) exceeds Gate ON ROI ({on_roi:.2f}%). "
            "Recommend removing or weakening the CLV gate."
        )
    else:
        lines.append(
            "VERDICT: CLV gate HELPS performance. Gate ON ROI "
            f"({on_roi:.2f}%) exceeds Gate OFF ROI ({off_roi:.2f}%). "
            "Recommend retaining the CLV gate."
        )
    n_supp = int(gate_suppressed_summary["n_bets"])
    n_off = int(gate_off_summary["n_bets"])
    pct = (100.0 * n_supp / n_off) if n_off else 0.0
    supp_roi = float(gate_suppressed_summary["roi_pct"])
    lines.append(
        f"Gate suppresses {n_supp} of {n_off} candidates ({pct:.1f}%). "
        f"Suppressed group ROI: {supp_roi:.2f}%"
    )
    return lines


def generate_report(
    gate_on_summary: dict[str, Any],
    gate_off_summary: dict[str, Any],
    gate_suppressed_summary: dict[str, Any],
    segments: dict[str, dict[str, Any]],
    gate_off_results: list[dict[str, Any]],
    run_date: str,
    *,
    season: int = 2026,
) -> str:
    """Build full plain-text MV-F CLV gate backtest report."""
    out: list[str] = []

    # Section 1 — header
    out.append("MV-F CLV GATE BACKTEST REPORT")
    out.append(f"Run date: {run_date}")
    out.append(f"Season: {season}   Data source: MLB DB (game_odds + games + venues)")
    out.append(
        "MV-F criteria: wind IN >= 10 mph, wind-sensitive venue "
        "(HIGH/MODERATE), home fav -130 to -170"
    )
    out.append("")

    # Section 2 — verdict
    out.append("=" * 72)
    out.append("TOP-LINE VERDICT")
    out.append("=" * 72)
    out.extend(_format_verdict_block(gate_on_summary, gate_off_summary, gate_suppressed_summary))
    out.append("")

    # Section 3 — summary table
    out.append("=" * 72)
    out.append("SUMMARY TABLE")
    out.append("=" * 72)
    out.extend(
        _compact_summary_table_lines([
            gate_on_summary,
            gate_off_summary,
            gate_suppressed_summary,
        ])
    )
    out.append("")

    # Section 4 — segmentation
    out.append("=" * 72)
    out.append("SEGMENTATION (Gate OFF candidates)")
    out.append("=" * 72)
    for group_title, keys in MVF_SEGMENT_GROUP_ORDER:
        out.append("")
        out.append(group_title)
        out.append("-" * len(group_title))
        bucket_summaries = [segments[k] for k in keys if k in segments]
        out.extend(_compact_summary_table_lines(bucket_summaries))

    # Section 5 — game-level detail
    out.append("")
    out.append("=" * 72)
    out.append("GAME-LEVEL DETAIL")
    out.append("=" * 72)
    detail_hdr = (
        f"{'Tag':<6} {'Date':<12} {'Venue':<28} {'Wind':>6} "
        f"{'HmML':>6} {'AwML':>6} {'CLVpp':>7} {'Gate':>5} {'Won':>4} {'P&L':>7}"
    )
    out.append(detail_hdr)
    out.append("-" * len(detail_hdr))

    sorted_rows = sorted(
        gate_off_results,
        key=lambda r: (str(r.get("game_date") or ""), int(r.get("game_pk") or 0)),
    )
    for r in sorted_rows:
        tag = "[PASS]" if r.get("clv_gate_passed") == 1 else "[SUPP]"
        venue = str(r.get("venue_name") or "")[:28]
        wind = r.get("wind_mph")
        wind_s = f"{int(wind)}" if wind is not None else "?"
        hm = r.get("open_home_ml")
        aw = r.get("session_away_ml")
        clv = r.get("clv_delta_pp")
        clv_s = f"{float(clv):+.2f}" if clv is not None else "N/A"
        gate_s = "PASS" if r.get("clv_gate_passed") == 1 else "SUPP"
        won = "Y" if r.get("away_won") == 1 else "N"
        pnl = float(r.get("pnl_units", 0))
        out.append(
            f"{tag:<6} {str(r.get('game_date') or ''):<12} {venue:<28} {wind_s:>6} "
            f"{hm:>6} {aw:>6} {clv_s:>7} {gate_s:>5} {won:>4} {pnl:+7.3f}"
        )

    # Section 6 — methodology
    out.append("")
    out.append("=" * 72)
    out.append("METHODOLOGY NOTE")
    out.append("=" * 72)
    out.append(
        "Signal validity (model score, edge) was not available for suppressed "
        "signals in the DB. This backtest uses venue + wind + odds criteria only "
        "to identify MV-F candidates, matching the live pipeline's entry conditions. "
        "CLV delta computed as: session_away_implied_prob - open_away_implied_prob "
        "(in pp). Gate threshold: +0.5pp. P&L graded at flat 1 unit, session-time "
        "away ML odds. Session pregame hours use game_odds.hours_before_game when "
        "set; if NULL, hours are derived from games.game_start_utc minus "
        "captured_at_utc (same formula as load_odds). Run "
        "batch/jobs/backfill_game_odds_hours.py --dry-run to audit rows; omit "
        "--dry-run to persist hours for other queries."
    )
    out.append("")

    return "\n".join(out)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="MV-F CLV gate backtest: rehydrate candidates, grade gate on/off, segment, report.",
    )
    p.add_argument(
        "--report",
        action="store_true",
        help="Write full report to reports/mvf_clv_backtest_YYYY-MM-DD.txt",
    )
    p.add_argument(
        "--season",
        type=int,
        default=2026,
        help="Season to backtest (default: 2026)",
    )
    return p.parse_args()


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
    args = parse_args()
    run_date = dt.date.today().isoformat()
    season = int(args.season)

    db_path = _resolve_db_path()
    if not Path(db_path).is_file():
        print(f"Database not found: {db_path}")
        return 1

    candidates = build_mvf_candidate_universe(db_path, season=season)
    print(f"MV-F candidate universe: {len(candidates)} row(s)  (db={db_path}, season={season})")

    if not candidates:
        print("No candidates — check filters / data.")
        return 1

    n_pass = sum(1 for r in candidates if r.get("clv_gate_passed") == 1)
    print(f"  clv_gate_passed=1: {n_pass}  |  clv_gate_passed=0: {len(candidates) - n_pass}")

    (
        gate_on_summary,
        gate_off_summary,
        gate_suppressed_summary,
        _gate_on_results,
        gate_off_results,
    ) = grade_mvf_candidates(candidates)

    segments = segment_mvf_results(gate_off_results)

    # Always: top-line verdict + compact summary table
    print("\n" + "=" * 72)
    print("TOP-LINE VERDICT")
    print("=" * 72)
    for line in _format_verdict_block(
        gate_on_summary, gate_off_summary, gate_suppressed_summary
    ):
        print(line)

    print("\n" + "=" * 72)
    print("SUMMARY TABLE")
    print("=" * 72)
    for line in _compact_summary_table_lines([
        gate_on_summary,
        gate_off_summary,
        gate_suppressed_summary,
    ]):
        print(line)

    print("\n" + "=" * 72)
    print("MV-F segmentation (Gate OFF candidates)")
    print("=" * 72)
    _print_segment_blocks(segments)

    clv_neg = segments["clv_negative"]["n_bets"]
    clv_zfh = segments["clv_zero_to_half"]["n_bets"]
    suppressed_n = gate_suppressed_summary["n_bets"]
    if clv_neg + clv_zfh != suppressed_n:
        print(
            f"\n  Note: clv_negative + clv_zero_to_half = {clv_neg + clv_zfh} "
            f"(gate_suppressed n_bets = {suppressed_n}; "
            "null CLV rows excluded from CLV buckets)"
        )

    if args.report:
        report_text = generate_report(
            gate_on_summary,
            gate_off_summary,
            gate_suppressed_summary,
            segments,
            gate_off_results,
            run_date,
            season=season,
        )
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        report_path = REPORTS_DIR / f"mvf_clv_backtest_{run_date}.txt"
        report_path.write_text(report_text, encoding="utf-8")
        print(f"\nReport written: {report_path}")

    if os.getenv("MVF_CLV_DEBUG"):
        print("\nFirst 3 candidates (debug):")
        print(json.dumps(candidates[:3], indent=2, default=str))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
