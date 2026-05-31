#!/usr/bin/env python3
"""
series_momentum_backtest.py
───────────────────────────
Backtest series-level hypotheses on completed regular-season games.

Hypothesis A — Rubber-game trailing-team edge (avoid-sweep finale).
Hypothesis B — Hangover Under after high-scoring series games.

USAGE:
  python batch/analysis/prediction/series_momentum_backtest.py
  python batch/analysis/prediction/series_momentum_backtest.py \\
      --start-year 2022 --end-year 2025 --output-csv
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import warnings
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import connect as db_connect, get_db_path

DEFAULT_OUTPUT_DIR = "outputs/reports"
EXCLUDE_SEASONS = {2020}
_GAME_DATE = "COALESCE(NULLIF(TRIM(g.game_date_et), ''), g.game_date)"


def resolve_path(path: str) -> Path:
    p = Path(path)
    return p if p.is_absolute() else _REPO_ROOT / p


def american_to_implied(ml: float) -> float:
    ml = float(ml)
    if ml < 0:
        return abs(ml) / (abs(ml) + 100.0)
    return 100.0 / (ml + 100.0)


def signal_verdict(win_rate: float, n: int) -> str:
    if n > 50 and win_rate > 0.55:
        return "VIABLE"
    if n < 50 or (0.50 <= win_rate <= 0.55):
        return "BORDERLINE"
    return "NOT SUPPORTED"


def load_games(con: sqlite3.Connection, start_year: int, end_year: int) -> pd.DataFrame:
    seasons = [y for y in range(start_year, end_year + 1) if y not in EXCLUDE_SEASONS]
    if not seasons:
        return pd.DataFrame()
    ph = ",".join("?" * len(seasons))
    sql = f"""
    SELECT
        g.game_pk,
        g.season,
        {_GAME_DATE} AS game_date_et,
        g.home_team_id,
        g.away_team_id,
        ht.abbreviation AS home_team,
        at.abbreviation AS away_team,
        g.home_score,
        g.away_score,
        g.series_game_number,
        g.double_header,
        ml.home_ml,
        ml.away_ml,
        tot.total_line,
        tot.over_odds,
        tot.under_odds
    FROM games g
    JOIN teams ht ON ht.team_id = g.home_team_id
    JOIN teams at ON at.team_id = g.away_team_id
    LEFT JOIN (
        SELECT game_pk, home_ml, away_ml,
               ROW_NUMBER() OVER (
                   PARTITION BY game_pk
                   ORDER BY is_closing_line DESC, captured_at_utc DESC, id DESC
               ) AS rn
        FROM game_odds
        WHERE market_type = 'moneyline'
          AND home_ml IS NOT NULL
          AND away_ml IS NOT NULL
    ) ml ON ml.game_pk = g.game_pk AND ml.rn = 1
    LEFT JOIN (
        SELECT game_pk, total_line, over_odds, under_odds,
               ROW_NUMBER() OVER (
                   PARTITION BY game_pk
                   ORDER BY is_closing_line DESC, captured_at_utc DESC, id DESC
               ) AS rn
        FROM game_odds
        WHERE market_type = 'total'
          AND total_line IS NOT NULL
    ) tot ON tot.game_pk = g.game_pk AND tot.rn = 1
    WHERE g.game_type = 'R'
      AND g.status = 'Final'
      AND g.home_score IS NOT NULL
      AND g.away_score IS NOT NULL
      AND g.series_game_number IS NOT NULL
      AND g.series_game_number <= 4
      AND CAST(strftime('%m', {_GAME_DATE}) AS INTEGER) BETWEEN 5 AND 8
      AND g.season IN ({ph})
      AND (g.double_header IS NULL OR g.double_header NOT IN ('Y', 'S'))
    ORDER BY g.season, g.home_team_id, g.away_team_id, {_GAME_DATE}, g.game_pk
    """
    return pd.read_sql_query(sql, con, params=seasons)


def winner_team_id(row: pd.Series) -> int | None:
    if row["home_score"] == row["away_score"]:
        return None
    return int(row["home_team_id"] if row["home_score"] > row["away_score"] else row["away_team_id"])


def build_series_blocks(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Assign series_id within (team_a, team_b, season) using series_game_number resets."""
    warnings_out: list[str] = []
    if df.empty:
        return df, warnings_out

    out = df.copy()
    out["team_a_id"] = out[["home_team_id", "away_team_id"]].min(axis=1).astype(int)
    out["team_b_id"] = out[["home_team_id", "away_team_id"]].max(axis=1).astype(int)
    out["combined_runs"] = out["home_score"] + out["away_score"]
    out["winner_team_id"] = out.apply(winner_team_id, axis=1)

    out = out.sort_values(
        ["team_a_id", "team_b_id", "season", "game_date_et", "series_game_number", "game_pk"]
    ).reset_index(drop=True)

    series_ids: list[int] = []
    current_series = 0
    prev_key: tuple[int, int, int] | None = None
    prev_sgn = 0

    for _, row in out.iterrows():
        key = (int(row["team_a_id"]), int(row["team_b_id"]), int(row["season"]))
        sgn = int(row["series_game_number"])
        if key != prev_key:
            current_series += 1
        elif sgn == 1 and prev_sgn >= 1:
            # Reset to game 1 after prior series game, or back-to-back series openers.
            current_series += 1
        series_ids.append(current_series)
        prev_key = key
        prev_sgn = sgn

    out["series_id"] = series_ids

    # Validate sequential numbering within each block.
    bad_blocks: set[int] = set()
    for sid, grp in out.groupby("series_id"):
        sgns = grp.sort_values("game_date_et")["series_game_number"].astype(int).tolist()
        if len(sgns) <= 1:
            continue
        for i in range(1, len(sgns)):
            if sgns[i] != sgns[i - 1] + 1:
                bad_blocks.add(int(sid))
                warnings_out.append(
                    f"series_id={sid}: non-sequential game numbers {sgns} — block skipped"
                )
                break

    if bad_blocks:
        out = out[~out["series_id"].isin(bad_blocks)].copy()

    return out, warnings_out


@dataclass
class HypothesisAResult:
    rows: list[dict] = field(default_factory=list)
    debug_blocks: list[dict] = field(default_factory=list)


def analyze_hypothesis_a(df: pd.DataFrame) -> HypothesisAResult:
    result = HypothesisAResult()
    if df.empty:
        return result

    for sid, block in df.groupby("series_id"):
        block = block.sort_values("game_date_et")
        by_sgn = {int(r.series_game_number): r for r in block.itertuples()}
        max_sgn = max(by_sgn)

        if max_sgn == 3 and all(n in by_sgn for n in (1, 2, 3)):
            prior = [by_sgn[1], by_sgn[2]]
            finale = by_sgn[3]
            series_type = "3-game"
            prior_count = 2
        elif max_sgn >= 4 and all(n in by_sgn for n in (1, 2, 3, 4)):
            prior = [by_sgn[1], by_sgn[2], by_sgn[3]]
            finale = by_sgn[4]
            series_type = "4-game"
            prior_count = 3
        else:
            continue

        winners = [p.winner_team_id for p in prior]
        if any(w is None for w in winners):
            continue
        if len(set(winners)) != 1:
            continue  # split — not avoid-sweep scenario

        sweeping_team = int(winners[0])
        team_a = int(block.iloc[0]["team_a_id"])
        team_b = int(block.iloc[0]["team_b_id"])
        trailing_team = team_b if sweeping_team == team_a else team_a

        trailing_won = int(finale.winner_team_id == trailing_team)
        trailing_side = "home" if int(finale.home_team_id) == trailing_team else "away"
        trailing_ml = finale.home_ml if trailing_side == "home" else finale.away_ml
        has_odds = pd.notna(trailing_ml)
        implied = american_to_implied(trailing_ml) if has_odds else np.nan

        row = {
            "series_id": int(sid),
            "game_pk": int(finale.game_pk),
            "series_game_number": int(finale.series_game_number),
            "hypothesis": "A",
            "series_type": series_type,
            "trailing_team_side": trailing_side,
            "trailing_team_won": trailing_won,
            "combined_runs": int(finale.combined_runs),
            "total_line": finale.total_line,
            "under_result": "",
            "has_odds": int(has_odds),
            "trailing_implied": implied,
            "prior_games_won_by": sweeping_team,
        }
        result.rows.append(row)

        if len(result.debug_blocks) < 5:
            result.debug_blocks.append(
                {
                    "series_id": sid,
                    "series_type": series_type,
                    "dates": block["game_date_et"].tolist(),
                    "sgns": block["series_game_number"].tolist(),
                    "sweeping_team_id": sweeping_team,
                    "trailing_team_id": trailing_team,
                    "trailing_won_finale": trailing_won,
                }
            )

    return result


@dataclass
class HypothesisBResult:
    hangover_rows: list[dict] = field(default_factory=list)
    baseline_under_rate: float = 0.0
    baseline_avg_runs: float = 0.0
    baseline_n: int = 0
    baseline_under_n: int = 0


def analyze_hypothesis_b(df: pd.DataFrame, threshold: int) -> HypothesisBResult:
    result = HypothesisBResult()
    if df.empty:
        return result

    hangover_pks: set[int] = set()
    trigger_runs: dict[int, int] = {}

    for _, block in df.groupby("series_id"):
        block = block.sort_values("game_date_et")
        rows = list(block.itertuples())
        for i, row in enumerate(rows[:-1]):
            if int(row.combined_runs) >= threshold:
                nxt = rows[i + 1]
                if int(nxt.series_game_number) == int(row.series_game_number) + 1:
                    hangover_pks.add(int(nxt.game_pk))
                    trigger_runs[int(nxt.game_pk)] = int(row.combined_runs)

    for _, row in df.iterrows():
        pk = int(row["game_pk"])
        is_hangover = pk in hangover_pks
        total = row["total_line"]
        has_total = pd.notna(total)
        combined = int(row["combined_runs"])
        under_result = ""
        if has_total:
            if combined < float(total):
                under_result = "WIN"
            elif combined > float(total):
                under_result = "LOSS"
            else:
                under_result = "PUSH"

        if is_hangover:
            trig = trigger_runs.get(pk, np.nan)
            result.hangover_rows.append(
                {
                    "series_id": int(row["series_id"]),
                    "game_pk": pk,
                    "series_game_number": int(row["series_game_number"]),
                    "hypothesis": "B",
                    "series_type": "",
                    "trailing_team_side": "",
                    "trailing_team_won": "",
                    "combined_runs": combined,
                    "total_line": total,
                    "under_result": under_result,
                    "has_odds": int(has_total),
                    "trigger_combined_runs": trig,
                    "scored_below_trigger": int(combined < trig) if pd.notna(trig) else "",
                }
            )

    # Baseline: non-hangover games with total line.
    baseline = df[~df["game_pk"].isin(hangover_pks)].copy()
    baseline_with_total = baseline[baseline["total_line"].notna()].copy()
    result.baseline_n = len(baseline)
    result.baseline_avg_runs = float(baseline["combined_runs"].mean()) if len(baseline) else 0.0
    result.baseline_under_n = len(baseline_with_total)
    if len(baseline_with_total):
        wins = (baseline_with_total["combined_runs"] < baseline_with_total["total_line"]).sum()
        result.baseline_under_rate = float(wins / len(baseline_with_total))

    return result


def _rate_summary(rows: list[dict], key: str | None = None, val=None) -> tuple[int, float]:
    subset = rows if key is None else [r for r in rows if r.get(key) == val]
    n = len(subset)
    if n == 0:
        return 0, 0.0
    wins = sum(int(r["trailing_team_won"]) for r in subset)
    return n, wins / n


def _odds_edge_summary(rows: list[dict]) -> tuple[int, float, float, float]:
    with_odds = [r for r in rows if r.get("has_odds")]
    n = len(with_odds)
    if n == 0:
        return 0, 0.0, 0.0, 0.0
    actual = sum(int(r["trailing_team_won"]) for r in with_odds) / n
    implied = float(np.mean([r["trailing_implied"] for r in with_odds]))
    return n, actual, implied, actual - implied


def write_report(
    path: Path,
    *,
    df: pd.DataFrame,
    warnings_out: list[str],
    start_year: int,
    end_year: int,
    threshold: int,
    hyp_a: HypothesisAResult,
    hyp_b: HypothesisBResult,
) -> None:
    lines: list[str] = []
    lines.append("SERIES MOMENTUM BACKTEST")
    lines.append("=" * 60)
    lines.append(f"Generated: {datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"Seasons: {start_year}–{end_year} (excludes {sorted(EXCLUDE_SEASONS)})")
    lines.append("Months: May–August  |  series_game_number <= 4  |  no DH (Y/S)")
    lines.append(f"Total games loaded: {len(df)}")
    lines.append(f"Series blocks: {df['series_id'].nunique() if not df.empty else 0}")
    lines.append(f"High-run threshold (Hypothesis B): {threshold}")
    if warnings_out:
        lines.append("")
        lines.append(f"Warnings ({len(warnings_out)}):")
        for w in warnings_out[:20]:
            lines.append(f"  - {w}")
        if len(warnings_out) > 20:
            lines.append(f"  ... and {len(warnings_out) - 20} more")

    lines.append("")
    lines.append("HYPOTHESIS A — RUBBER GAME TRAILING-TEAM EDGE")
    lines.append("-" * 60)
    lines.append(
        "Scenario: same team won all prior games in series; trailing team "
        "faces avoid-sweep finale."
    )

    rows_a = hyp_a.rows
    n_all, rate_all = _rate_summary(rows_a)
    lines.append(
        f"Overall trailing-team win rate in finale: n={n_all}, "
        f"win rate={rate_all:.1%} (vs 50% baseline)"
    )

    for label, key, val in (
        ("3-game series finale", "series_type", "3-game"),
        ("4-game series finale", "series_type", "4-game"),
        ("Trailing team = HOME", "trailing_team_side", "home"),
        ("Trailing team = AWAY", "trailing_team_side", "away"),
    ):
        n, rate = _rate_summary(rows_a, key, val)
        lines.append(f"  {label}: n={n}, win rate={rate:.1%}")

    n_odds, actual, implied, edge = _odds_edge_summary(rows_a)
    no_odds = n_all - n_odds
    lines.append(
        f"With closing ML (finale): n={n_odds}, actual={actual:.1%}, "
        f"implied={implied:.1%}, edge={edge:+.1%}"
    )
    lines.append(f"Finale games without ML odds: n={no_odds}")

    lines.append(f"SIGNAL VERDICT: {signal_verdict(rate_all, n_all)}")

    if n_all and abs(rate_all - 0.5) < 0.001:
        lines.append("")
        lines.append("DEBUG — sample series blocks (exact 50% check):")
        for blk in hyp_a.debug_blocks:
            lines.append(f"  {blk}")

    lines.append("")
    lines.append("HYPOTHESIS B — HANGOVER UNDER")
    lines.append("-" * 60)
    lines.append(
        f"Trigger: combined runs >= {threshold}; next game in same series block."
    )

    hang = hyp_b.hangover_rows
    n_h = len(hang)
    avg_h = float(np.mean([r["combined_runs"] for r in hang])) if n_h else 0.0
    lines.append(f"Hangover games: n={n_h}, avg combined runs={avg_h:.2f}")
    lines.append(
        f"Baseline (non-hangover): n={hyp_b.baseline_n}, "
        f"avg combined runs={hyp_b.baseline_avg_runs:.2f}"
    )

    with_total = [r for r in hang if r.get("has_odds")]
    n_t = len(with_total)
    if n_t:
        under_wins = sum(1 for r in with_total if r["under_result"] == "WIN")
        under_rate = under_wins / n_t
        lines.append(
            f"Under win rate vs closing total (hangover): n={n_t}, "
            f"rate={under_rate:.1%}"
        )
        lines.append(
            f"Baseline Under win rate (non-hangover, with total): "
            f"n={hyp_b.baseline_under_n}, rate={hyp_b.baseline_under_rate:.1%}"
        )
    else:
        under_rate = 0.0
        lines.append("Under win rate vs closing total (hangover): n=0")

    below_trig = [r for r in hang if r.get("scored_below_trigger") == 1]
    if n_h:
        pct_below = len(below_trig) / n_h
        lines.append(
            f"Hangover games scoring below triggering game total: "
            f"{len(below_trig)}/{n_h} ({pct_below:.1%})"
        )

    lines.append(f"SIGNAL VERDICT: {signal_verdict(under_rate if n_t else 0.5, n_t)}")

    lines.append("")
    lines.append("INTEGRATION NOTE")
    lines.append("-" * 60)
    if signal_verdict(rate_all, n_all) == "VIABLE":
        lines.append(
            "Hypothesis A shows a trailing-team avoid-sweep edge that may "
            "warrant a series-finale ML signal layer."
        )
    elif signal_verdict(rate_all, n_all) == "BORDERLINE":
        lines.append(
            "Hypothesis A is inconclusive — monitor avoid-sweep spots but do "
            "not auto-fire until sample/edge improves."
        )
    else:
        lines.append(
            "Hypothesis A does not support a trailing-team avoid-sweep ML signal."
        )

    if signal_verdict(under_rate if n_t else 0.5, n_t) == "VIABLE":
        lines.append(
            "Hypothesis B supports layering a Hangover Under flag after "
            f"high-scoring (≥{threshold}) series games."
        )
    elif signal_verdict(under_rate if n_t else 0.5, n_t) == "BORDERLINE":
        lines.append(
            "Hypothesis B is borderline — consider as a soft Under lean only "
            "when total line is available."
        )
    else:
        lines.append(
            "Hypothesis B does not support a systematic Hangover Under signal."
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_csv(path: Path, hyp_a: HypothesisAResult, hyp_b: HypothesisBResult) -> None:
    rows = hyp_a.rows + hyp_b.hangover_rows
    if not rows:
        return
    df = pd.DataFrame(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Series momentum backtest (Hypotheses A & B).")
    p.add_argument("--db", default=get_db_path(), help="SQLite database path")
    p.add_argument("--start-year", type=int, default=2022)
    p.add_argument("--end-year", type=int, default=2025)
    p.add_argument("--high-run-threshold", type=int, default=14)
    p.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Report output directory",
    )
    p.add_argument(
        "--output-csv",
        action="store_true",
        help="Write outputs/reports/series_momentum_detail.csv",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = resolve_path(args.output_dir)
    report_path = out_dir / "series_momentum_backtest.txt"
    csv_path = out_dir / "series_momentum_detail.csv"

    con = db_connect(args.db)
    try:
        df = load_games(con, args.start_year, args.end_year)
    finally:
        con.close()

    df, warnings_out = build_series_blocks(df)
    hyp_a = analyze_hypothesis_a(df)
    hyp_b = analyze_hypothesis_b(df, args.high_run_threshold)

    write_report(
        report_path,
        df=df,
        warnings_out=warnings_out,
        start_year=args.start_year,
        end_year=args.end_year,
        threshold=args.high_run_threshold,
        hyp_a=hyp_a,
        hyp_b=hyp_b,
    )

    if args.output_csv:
        write_csv(csv_path, hyp_a, hyp_b)

    print(f"[series_momentum] Report -> {report_path}")
    print(f"[series_momentum] Games={len(df)} blocks={df['series_id'].nunique() if not df.empty else 0}")
    print(f"[series_momentum] Hypothesis A finale spots n={len(hyp_a.rows)}")
    print(f"[series_momentum] Hypothesis B hangover games n={len(hyp_b.hangover_rows)}")
    if args.output_csv:
        print(f"[series_momentum] Detail CSV -> {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
