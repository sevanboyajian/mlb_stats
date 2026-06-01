#!/usr/bin/env python3
"""
owm_veto_backtest.py
────────────────────
Backtest OWM signal plus away-offense veto and Net Offensive Matchup (NOM).

Primary: when OWM fires (home hot offense + weak away SP), does win rate drop
when away offense is also hot?

Secondary: does a four-factor NOM score predict home wins better than OWM alone?

USAGE:
  python batch/analysis/prediction/owm_veto_backtest.py
  python batch/analysis/prediction/owm_veto_backtest.py \\
      --start-year 2022 --end-year 2025 --output-csv
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
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
MIN_SP_STARTS = 3
NOM_ERA_SCALAR = 0.05
_GAME_DATE = "COALESCE(NULLIF(TRIM(g.game_date_et), ''), g.game_date)"


def resolve_path(path: str) -> Path:
    p = Path(path)
    return p if p.is_absolute() else _REPO_ROOT / p


def american_to_implied(ml: float) -> float:
    ml = float(ml)
    if ml < 0:
        return abs(ml) / (abs(ml) + 100.0)
    return 100.0 / (ml + 100.0)


def flat_home_roi(home_wins: pd.Series, home_ml: pd.Series) -> tuple[float, int]:
    """Flat 1-unit bet on home ML; returns (roi, n_with_odds)."""
    mask = home_ml.notna()
    if not mask.any():
        return 0.0, 0
    total = 0.0
    n = int(mask.sum())
    for won, ml in zip(home_wins[mask], home_ml[mask], strict=True):
        if won:
            ml = float(ml)
            total += 100.0 / abs(ml) if ml < 0 else ml / 100.0
        else:
            total -= 1.0
    return total / n, n


def bucket_stats(df: pd.DataFrame, label_col: str) -> list[dict]:
    rows: list[dict] = []
    if df.empty:
        return rows
    for label, grp in df.groupby(label_col, sort=False):
        n = len(grp)
        wins = int(grp["home_won"].sum())
        win_rate = wins / n if n else 0.0
        roi, n_odds = flat_home_roi(grp["home_won"], grp["home_ml"])
        rows.append(
            {
                "label": str(label),
                "n": n,
                "wins": wins,
                "win_rate": win_rate,
                "roi": roi,
                "n_with_odds": n_odds,
            }
        )
    return rows


def away_offense_tier(ops: float) -> str:
    if ops >= 0.80:
        return "Away Hot"
    if ops >= 0.70:
        return "Away Average"
    return "Away Cold"


def home_sp_tier(era: float) -> str:
    if era < 4.0:
        return "Home SP Strong"
    if era < 5.5:
        return "Home SP Average"
    return "Home SP Weak"


def query_rolling_stats_coverage(
    con: sqlite3.Connection, start_year: int, end_year: int
) -> dict[str, list[int]]:
    """Seasons with rolling-stats rows inside the requested year window."""
    team_rows = con.execute(
        """
        SELECT season, COUNT(*)
        FROM team_rolling_stats
        WHERE season BETWEEN ? AND ?
          AND season != 2020
        GROUP BY season
        ORDER BY season
        """,
        (start_year, end_year),
    ).fetchall()
    pitcher_rows = con.execute(
        """
        SELECT season, COUNT(*)
        FROM pitcher_rolling_stats
        WHERE season BETWEEN ? AND ?
          AND season != 2020
        GROUP BY season
        ORDER BY season
        """,
        (start_year, end_year),
    ).fetchall()
    return {
        "team_rolling": [int(r[0]) for r in team_rows],
        "pitcher_rolling": [int(r[0]) for r in pitcher_rows],
    }


def missing_rolling_seasons(
    start_year: int, end_year: int, available: list[int]
) -> list[int]:
    wanted = [y for y in range(start_year, end_year + 1) if y not in EXCLUDE_SEASONS]
    have = set(available)
    return [y for y in wanted if y not in have]


def load_games(con: sqlite3.Connection, start_year: int, end_year: int) -> pd.DataFrame:
    if start_year > end_year:
        return pd.DataFrame()

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
        COALESCE(th.rolling_ops_wma, th.rolling_ops) AS home_ops_wma,
        COALESCE(ta.rolling_ops_wma, ta.rolling_ops) AS away_ops_wma,
        prs_h.era_wma AS home_sp_era_wma,
        prs_a.era_wma AS away_sp_era_wma,
        prs_h.starts_in_window AS home_sp_starts,
        prs_a.starts_in_window AS away_sp_starts,
        ml.home_ml,
        ml.away_ml
    FROM games g
    JOIN teams ht ON ht.team_id = g.home_team_id
    JOIN teams at ON at.team_id = g.away_team_id
    JOIN team_rolling_stats th
         ON th.game_pk = g.game_pk AND th.team_id = g.home_team_id
    JOIN team_rolling_stats ta
         ON ta.game_pk = g.game_pk AND ta.team_id = g.away_team_id
    JOIN game_probable_pitchers gpp_h
         ON gpp_h.game_pk = g.game_pk AND gpp_h.team_id = g.home_team_id
    JOIN game_probable_pitchers gpp_a
         ON gpp_a.game_pk = g.game_pk AND gpp_a.team_id = g.away_team_id
    JOIN pitcher_rolling_stats prs_h
         ON prs_h.game_pk = g.game_pk AND prs_h.player_id = gpp_h.player_id
    JOIN pitcher_rolling_stats prs_a
         ON prs_a.game_pk = g.game_pk AND prs_a.player_id = gpp_a.player_id
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
    WHERE g.game_type = 'R'
      AND g.status = 'Final'
      AND g.home_score IS NOT NULL
      AND g.away_score IS NOT NULL
      AND g.season BETWEEN ? AND ?
      AND g.season != 2020
      AND CAST(strftime('%m', {_GAME_DATE}) AS INTEGER) BETWEEN 5 AND 8
      AND COALESCE(th.rolling_ops_wma, th.rolling_ops) IS NOT NULL
      AND COALESCE(ta.rolling_ops_wma, ta.rolling_ops) IS NOT NULL
      AND prs_h.era_wma IS NOT NULL
      AND prs_a.era_wma IS NOT NULL
    ORDER BY g.season, {_GAME_DATE}, g.game_pk
    """
    df = pd.read_sql_query(sql, con, params=(start_year, end_year))
    if df.empty:
        return df

    for col in (
        "home_ops_wma",
        "away_ops_wma",
        "home_sp_era_wma",
        "away_sp_era_wma",
        "home_sp_starts",
        "away_sp_starts",
    ):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["home_won"] = df["home_score"] > df["away_score"]
    df["sp_starts_ok"] = (
        df["home_sp_starts"].fillna(0) >= MIN_SP_STARTS
    ) & (df["away_sp_starts"].fillna(0) >= MIN_SP_STARTS)

    df["nom"] = (
        (df["home_ops_wma"] - df["away_ops_wma"])
        + (df["away_sp_era_wma"] - df["home_sp_era_wma"]) * NOM_ERA_SCALAR
    )
    return df


def flag_owm(
    df: pd.DataFrame,
    ops_threshold: float,
    era_threshold: float,
) -> pd.DataFrame:
    out = df.copy()
    out["owm_fires"] = (
        out["sp_starts_ok"]
        & (out["home_ops_wma"] >= ops_threshold)
        & (out["away_sp_era_wma"] >= era_threshold)
    )
    return out


def add_owm_labels(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    owm = out[out["owm_fires"]].copy()
    if owm.empty:
        out["away_offense_tier"] = pd.NA
        out["home_sp_tier"] = pd.NA
        return out

    tier_map = owm["away_ops_wma"].apply(away_offense_tier)
    sp_map = owm["home_sp_era_wma"].apply(home_sp_tier)
    out.loc[out["owm_fires"], "away_offense_tier"] = tier_map.values
    out.loc[out["owm_fires"], "home_sp_tier"] = sp_map.values
    return out


def nom_quintiles(df: pd.DataFrame) -> pd.DataFrame:
    out = df.dropna(subset=["nom"]).copy()
    if out.empty:
        out["nom_quintile"] = pd.NA
        return out
    try:
        out["nom_quintile"] = pd.qcut(
            out["nom"],
            q=5,
            labels=["Q1 (lowest)", "Q2", "Q3", "Q4", "Q5 (highest)"],
            duplicates="drop",
        )
    except ValueError:
        out["nom_quintile"] = pd.NA
    return out


def format_stats_row(row: dict) -> str:
    odds_note = f", odds n={row['n_with_odds']}" if row["n_with_odds"] else ""
    return (
        f"  {row['label']:<22} n={row['n']:>4}  wins={row['wins']:>4}  "
        f"win rate={row['win_rate']:>6.1%}  ROI={row['roi']:>+7.1%}{odds_note}"
    )


def recommend_rule_change(
    owm_stats: dict,
    away_tiers: list[dict],
    home_sp_tiers: list[dict],
    nom_quintiles: list[dict],
    nom_owm_split: dict,
    *,
    min_n: int = 30,
    min_gap: float = 0.05,
) -> str:
    """Pick one recommendation based on backtest evidence."""
    baseline_wr = owm_stats.get("win_rate", 0.0)
    baseline_n = owm_stats.get("n", 0)

    tier_by_label = {r["label"]: r for r in away_tiers}
    hot = tier_by_label.get("Away Hot")
    cold = tier_by_label.get("Away Cold")

    if hot and cold and hot["n"] >= min_n and cold["n"] >= min_n:
        gap_hot_vs_cold = cold["win_rate"] - hot["win_rate"]
        gap_hot_vs_base = baseline_wr - hot["win_rate"]
        if gap_hot_vs_cold >= min_gap or gap_hot_vs_base >= min_gap:
            return (
                "VETO CONFIRMED: add away_ops_wma < 0.80 as a required OWM condition "
                f"(Away Hot win rate {hot['win_rate']:.1%} vs Away Cold "
                f"{cold['win_rate']:.1%}, gap {gap_hot_vs_cold:+.1%})."
            )

    sp_by_label = {r["label"]: r for r in home_sp_tiers}
    weak = sp_by_label.get("Home SP Weak")
    strong = sp_by_label.get("Home SP Strong")
    if weak and strong and weak["n"] >= min_n and strong["n"] >= min_n:
        gap = strong["win_rate"] - weak["win_rate"]
        if gap >= min_gap:
            return (
                "HOME SP GATE CONFIRMED: add home_sp_era_wma < 5.5 as a required "
                f"condition (Weak {weak['win_rate']:.1%} vs Strong "
                f"{strong['win_rate']:.1%}, gap {gap:+.1%})."
            )

    q_by_label = {r["label"]: r for r in nom_quintiles}
    q45 = [q_by_label[k] for k in ("Q4", "Q5 (highest)") if k in q_by_label]
    if q45 and baseline_n >= min_n:
        nom_top_n = sum(r["n"] for r in q45)
        nom_top_wins = sum(r["wins"] for r in q45)
        if nom_top_n >= min_n:
            nom_top_wr = nom_top_wins / nom_top_n
            if nom_top_wr - baseline_wr >= min_gap:
                return (
                    "NOM REPLACES OWM: NOM quintiles 4-5 outperform OWM baseline "
                    f"({nom_top_wr:.1%} vs {baseline_wr:.1%}) — recommend migration "
                    "path to NOM-filtered home ML signal."
                )

    pos = nom_owm_split.get("positive")
    neg = nom_owm_split.get("non_positive")
    if pos and neg and pos["n"] >= 15 and neg["n"] >= 15:
        if pos["win_rate"] - neg["win_rate"] >= min_gap:
            return (
                "VETO CONFIRMED (NOM layer): among OWM games, require NOM > 0 "
                f"(NOM>0 {pos['win_rate']:.1%} vs NOM<=0 {neg['win_rate']:.1%})."
            )

    if baseline_n < min_n:
        return (
            f"NO CHANGE SUPPORTED: OWM sample too small (n={baseline_n} < {min_n})."
        )
    return (
        "NO CHANGE SUPPORTED: tier differences not significant enough "
        f"(need n>={min_n} and win rate gap >= {min_gap:.0%})."
    )


def write_report(
    path: Path,
    *,
    df: pd.DataFrame,
    owm_df: pd.DataFrame,
    start_year: int,
    end_year: int,
    ops_threshold: float,
    era_threshold: float,
    nom_df: pd.DataFrame,
    coverage: dict[str, list[int]],
    missing_team_seasons: list[int],
) -> None:
    lines: list[str] = []
    lines.append("OWM VETO BACKTEST")
    lines.append("=" * 70)
    lines.append(f"Generated: {datetime.now().isoformat(timespec='seconds')}")
    lines.append(
        f"Seasons: {start_year}–{end_year} (excludes {sorted(EXCLUDE_SEASONS)})"
    )
    lines.append("Months: May–August  |  MIN_SP_STARTS = 3")
    lines.append(
        f"OWM thresholds: home OPS WMA >= {ops_threshold:.2f}, "
        f"away SP ERA WMA >= {era_threshold:.1f}"
    )
    lines.append(
        "Stats: COALESCE(rolling_ops_wma, rolling_ops); SP ERA from "
        "pitcher_rolling_stats (production OWM alignment)."
    )
    lines.append(
        "Note: team_rolling_stats rows are pre-game builder snapshots on game_pk; "
        "no computed_at vs game_start filter applied (consistent with score_today)."
    )
    lines.append(
        f"Rolling-stats DB coverage: team={coverage['team_rolling'] or 'none'}, "
        f"pitcher={coverage['pitcher_rolling'] or 'none'}"
    )
    if missing_team_seasons:
        lines.append(
            "DATA GAP: no team_rolling_stats for "
            f"{missing_team_seasons} — INNER JOIN limits games to covered seasons."
        )
    if not df.empty:
        by_season = df.groupby("season").size().sort_index()
        lines.append(
            "Games loaded by season: "
            + ", ".join(f"{int(s)}={int(n)}" for s, n in by_season.items())
        )
    lines.append(f"Total games loaded: {len(df)}")
    lines.append(f"OWM fires: {len(owm_df)}")

    # Section 1 — baseline
    lines.append("")
    lines.append("SECTION 1 — OWM BASELINE")
    lines.append("-" * 70)
    owm_stats_list = bucket_stats(owm_df, "owm_fires")
    if owm_stats_list:
        s = owm_stats_list[0]
        owm_stats = s
        lines.append(format_stats_row({**s, "label": "OWM (all fires)"}))
    else:
        owm_stats = {"n": 0, "wins": 0, "win_rate": 0.0, "roi": 0.0, "n_with_odds": 0}
        lines.append("  No OWM-fired games in sample.")

    # Section 2 — away offense veto
    lines.append("")
    lines.append("SECTION 2 — AWAY OFFENSE VETO ANALYSIS (within OWM games)")
    lines.append("-" * 70)
    lines.append("  Tiers: Hot >= 0.80 | Average 0.70–0.80 | Cold < 0.70")
    tier_order = ["Away Hot", "Away Average", "Away Cold"]
    away_rows = bucket_stats(owm_df.dropna(subset=["away_offense_tier"]), "away_offense_tier")
    away_by_label = {r["label"]: r for r in away_rows}
    for label in tier_order:
        if label in away_by_label:
            lines.append(format_stats_row(away_by_label[label]))
        else:
            lines.append(f"  {label:<22} n=   0  (no games)")

    hot_row = away_by_label.get("Away Hot")
    cold_row = away_by_label.get("Away Cold")
    if hot_row and cold_row and hot_row["n"] >= 10:
        gap = cold_row["win_rate"] - hot_row["win_rate"]
        flag = " *** UNDERPERFORMS" if gap >= 0.05 else ""
        lines.append(
            f"  Away Hot vs Cold gap: {gap:+.1%} (positive = Cold outperforms Hot){flag}"
        )

    # Section 3 — home SP cross-cut
    lines.append("")
    lines.append("SECTION 3 — HOME SP CROSS-CUT (within OWM games)")
    lines.append("-" * 70)
    lines.append("  Tiers: Strong < 4.0 | Average 4.0–5.5 | Weak >= 5.5")
    sp_order = ["Home SP Strong", "Home SP Average", "Home SP Weak"]
    sp_rows = bucket_stats(owm_df.dropna(subset=["home_sp_tier"]), "home_sp_tier")
    sp_by_label = {r["label"]: r for r in sp_rows}
    for label in sp_order:
        if label in sp_by_label:
            lines.append(format_stats_row(sp_by_label[label]))
        else:
            lines.append(f"  {label:<22} n=   0  (no games)")

    # Section 4 — NOM
    lines.append("")
    lines.append("SECTION 4 — NOM SCORE ANALYSIS")
    lines.append("-" * 70)
    lines.append(
        f"  NOM = (home_ops - away_ops) + (away_sp_era - home_sp_era) * {NOM_ERA_SCALAR}"
    )

    nom_quint_rows = bucket_stats(nom_df.dropna(subset=["nom_quintile"]), "nom_quintile")
    if nom_quint_rows:
        lines.append("  All games — NOM quintiles:")
        q_order = {"Q1 (lowest)": 1, "Q2": 2, "Q3": 3, "Q4": 4, "Q5 (highest)": 5}
        for row in sorted(nom_quint_rows, key=lambda r: q_order.get(r["label"], 99)):
            lines.append(format_stats_row(row))
    else:
        lines.append("  Quintile breakdown unavailable (insufficient NOM spread).")

    if owm_stats["n"]:
        lines.append("")
        lines.append("  OWM baseline comparison:")
        lines.append(format_stats_row({**owm_stats, "label": "OWM fires"}))

    q5 = next((r for r in nom_quint_rows if "Q5" in r["label"]), None)
    if q5 and owm_stats["n"]:
        diff = q5["win_rate"] - owm_stats["win_rate"]
        lines.append(
            f"  Q5 vs OWM win rate delta: {diff:+.1%}"
        )

    lines.append("")
    lines.append("  NOM veto within OWM games:")
    owm_nom = owm_df.copy()
    owm_nom["nom_sign"] = np.where(owm_nom["nom"] > 0, "NOM > 0", "NOM <= 0")
    nom_split_rows = bucket_stats(owm_nom, "nom_sign")
    nom_owm_split: dict[str, dict] = {}
    for row in nom_split_rows:
        key = "positive" if ">" in row["label"] else "non_positive"
        nom_owm_split[key] = row
        lines.append(format_stats_row(row))

    # Section 5 — recommendation
    lines.append("")
    lines.append("SECTION 5 — RECOMMENDED RULE CHANGE")
    lines.append("-" * 70)
    rec = recommend_rule_change(
        owm_stats,
        away_rows,
        sp_rows,
        nom_quint_rows,
        nom_owm_split,
    )
    lines.append(f"  {rec}")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_detail_csv(path: Path, owm_df: pd.DataFrame) -> None:
    if owm_df.empty:
        return
    cols = [
        "game_pk",
        "season",
        "game_date_et",
        "home_team",
        "away_team",
        "home_ops_wma",
        "away_ops_wma",
        "home_sp_era_wma",
        "away_sp_era_wma",
        "home_sp_starts",
        "away_sp_starts",
        "nom",
        "away_offense_tier",
        "home_sp_tier",
        "home_won",
        "home_ml",
        "away_ml",
        "home_score",
        "away_score",
    ]
    out = owm_df[cols].copy()
    out["bet_side"] = "home_ml"
    out["pnl_units"] = out.apply(_row_pnl, axis=1)
    path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(path, index=False)


def _row_pnl(row: pd.Series) -> float:
    if pd.isna(row["home_ml"]):
        return np.nan
    ml = float(row["home_ml"])
    if row["home_won"]:
        return 100.0 / abs(ml) if ml < 0 else ml / 100.0
    return -1.0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="OWM veto + NOM backtest (May–Aug regular season)."
    )
    p.add_argument("--db", default=get_db_path(), help="SQLite database path")
    p.add_argument("--start-year", type=int, default=2022)
    p.add_argument("--end-year", type=int, default=2025)
    p.add_argument(
        "--owm-ops-threshold",
        type=float,
        default=0.8,
        help="Home OPS WMA minimum for OWM",
    )
    p.add_argument(
        "--owm-era-threshold",
        type=float,
        default=5.0,
        help="Away SP ERA WMA minimum for OWM",
    )
    p.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Report output directory",
    )
    p.add_argument(
        "--output-csv",
        action="store_true",
        help="Write outputs/reports/owm_veto_detail.csv",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = resolve_path(args.output_dir)
    report_path = out_dir / "owm_veto_backtest.txt"
    csv_path = out_dir / "owm_veto_detail.csv"

    print(
        f"[owm_veto] Season filter: {args.start_year}-{args.end_year} "
        f"(excludes {sorted(EXCLUDE_SEASONS)})"
    )

    con = db_connect(args.db)
    try:
        coverage = query_rolling_stats_coverage(con, args.start_year, args.end_year)
        missing_team = missing_rolling_seasons(
            args.start_year, args.end_year, coverage["team_rolling"]
        )
        print(
            f"[owm_veto] team_rolling_stats seasons in range: "
            f"{coverage['team_rolling'] or '(none)'}"
        )
        print(
            f"[owm_veto] pitcher_rolling_stats seasons in range: "
            f"{coverage['pitcher_rolling'] or '(none)'}"
        )
        if missing_team:
            print(
                f"[owm_veto] WARNING: no team_rolling_stats for {missing_team} - "
                "JOIN caps effective window"
            )
        df = load_games(con, args.start_year, args.end_year)
    finally:
        con.close()

    df = flag_owm(df, args.owm_ops_threshold, args.owm_era_threshold)
    df = add_owm_labels(df)
    owm_df = df[df["owm_fires"]].copy()
    nom_df = nom_quintiles(df[df["sp_starts_ok"]].copy())

    write_report(
        report_path,
        df=df,
        owm_df=owm_df,
        start_year=args.start_year,
        end_year=args.end_year,
        ops_threshold=args.owm_ops_threshold,
        era_threshold=args.owm_era_threshold,
        nom_df=nom_df,
        coverage=coverage,
        missing_team_seasons=missing_team,
    )

    if args.output_csv:
        write_detail_csv(csv_path, owm_df)

    print(f"[owm_veto] Report -> {report_path}")
    print(f"[owm_veto] Games={len(df)} OWM fires={len(owm_df)}")
    if not owm_df.empty:
        wr = owm_df["home_won"].mean()
        roi, _ = flat_home_roi(owm_df["home_won"], owm_df["home_ml"])
        print(f"[owm_veto] OWM baseline: win rate={wr:.1%} ROI={roi:+.1%}")
    if args.output_csv:
        print(f"[owm_veto] Detail CSV -> {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
