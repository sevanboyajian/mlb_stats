#!/usr/bin/env python3
"""
rl_margin_backtest.py
─────────────────────
Analyze MLB game margin distributions and run line cover rates by context.

Hypothesis A — margin distribution (1-run league, scoring environment).
Hypothesis B — underdog +1.5 cover rates by segmentation.
Hypothesis C — favorite -1.5 cover rates by segmentation.

USAGE:
  python batch/analysis/prediction/rl_margin_backtest.py
  python batch/analysis/prediction/rl_margin_backtest.py \\
      --start-year 2019 --end-year 2025 --output-csv
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
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

MARGIN_BINS = [1, 2, 3, 4, 5, 6, 7]
RUNS_SCORED_BUCKETS = [
    ("Low (≤7)", lambda r: r <= 7),
    ("Medium (8–10)", lambda r: 8 <= r <= 10),
    ("High (11–14)", lambda r: 11 <= r <= 14),
    ("Very High (15+)", lambda r: r >= 15),
]

TOTAL_LINE_BUCKETS = [
    ("Total ≤7.0", lambda t: t <= 7.0),
    ("Total 7.5–8.5", lambda t: 7.0 < t <= 8.5),
    ("Total 9.0–10.0", lambda t: 8.5 < t <= 10.0),
    ("Total 10.5+", lambda t: t > 10.5),
]

DOG_ML_BUCKETS = [
    ("Dog +101–130", 101, 130),
    ("Dog +131–160", 131, 160),
    ("Dog +161–200", 161, 200),
    ("Dog +201+", 201, 9999),
]


def resolve_path(path: str) -> Path:
    p = Path(path)
    return p if p.is_absolute() else _REPO_ROOT / p


def american_to_implied(ml: float) -> float:
    ml = float(ml)
    if ml < 0:
        return abs(ml) / (abs(ml) + 100.0)
    return 100.0 / (ml + 100.0)


def breakeven_juice(cover_rate: float) -> str:
    """American juice (negative) at which cover_rate breaks even."""
    if cover_rate <= 0.5 or cover_rate >= 1.0:
        return "n/a"
    juice = int(round(100.0 * cover_rate / (1.0 - cover_rate)))
    return f"-{juice}"


def signal_verdict(cover_rate: float, n: int) -> str:
    if n < 50:
        return "NOT SUPPORTED"
    if cover_rate > 0.55:
        return "VIABLE"
    if cover_rate >= 0.52:
        return "BORDERLINE"
    return "NOT SUPPORTED"


def query_rolling_stats_coverage(
    con: sqlite3.Connection, start_year: int, end_year: int
) -> dict[str, list[int]]:
    out: dict[str, list[int]] = {}
    for table, col in (
        ("team_rolling_stats", "season"),
        ("pitcher_rolling_stats", "season"),
    ):
        rows = con.execute(
            f"""
            SELECT {col}, COUNT(*)
            FROM {table}
            WHERE {col} BETWEEN ? AND ?
              AND {col} != 2020
            GROUP BY {col}
            ORDER BY {col}
            """,
            (start_year, end_year),
        ).fetchall()
        out[table] = [int(r[0]) for r in rows]
    return out


def load_games(
    con: sqlite3.Connection,
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
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
        g.home_score - g.away_score AS margin,
        ABS(g.home_score - g.away_score) AS abs_margin,
        g.home_score + g.away_score AS total_runs,
        ml.home_ml,
        ml.away_ml,
        rl.home_rl_odds,
        rl.away_rl_odds,
        rl.home_rl_line,
        rl.away_rl_line,
        tot.total_line,
        COALESCE(th.rolling_ops_wma, th.rolling_ops) AS home_ops_wma,
        COALESCE(ta.rolling_ops_wma, ta.rolling_ops) AS away_ops_wma,
        prs_h.era_wma AS home_sp_era_wma,
        prs_a.era_wma AS away_sp_era_wma,
        th.sp_starts_in_window AS home_sp_starts,
        ta.sp_starts_in_window AS away_sp_starts
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
        SELECT game_pk, home_rl_odds, away_rl_odds, home_rl_line, away_rl_line,
               ROW_NUMBER() OVER (
                   PARTITION BY game_pk
                   ORDER BY is_closing_line DESC, captured_at_utc DESC, id DESC
               ) AS rn
        FROM game_odds
        WHERE market_type = 'runline'
          AND home_rl_odds IS NOT NULL
          AND away_rl_odds IS NOT NULL
    ) rl ON rl.game_pk = g.game_pk AND rl.rn = 1
    LEFT JOIN (
        SELECT game_pk, total_line,
               ROW_NUMBER() OVER (
                   PARTITION BY game_pk
                   ORDER BY is_closing_line DESC, captured_at_utc DESC, id DESC
               ) AS rn
        FROM game_odds
        WHERE market_type = 'total'
          AND total_line IS NOT NULL
    ) tot ON tot.game_pk = g.game_pk AND tot.rn = 1
    LEFT JOIN team_rolling_stats th
         ON th.game_pk = g.game_pk AND th.team_id = g.home_team_id
    LEFT JOIN team_rolling_stats ta
         ON ta.game_pk = g.game_pk AND ta.team_id = g.away_team_id
    LEFT JOIN game_probable_pitchers gpp_h
         ON gpp_h.game_pk = g.game_pk AND gpp_h.team_id = g.home_team_id
    LEFT JOIN pitcher_rolling_stats prs_h
         ON prs_h.game_pk = g.game_pk AND prs_h.player_id = gpp_h.player_id
    LEFT JOIN game_probable_pitchers gpp_a
         ON gpp_a.game_pk = g.game_pk AND gpp_a.team_id = g.away_team_id
    LEFT JOIN pitcher_rolling_stats prs_a
         ON prs_a.game_pk = g.game_pk AND prs_a.player_id = gpp_a.player_id
    WHERE g.game_type = 'R'
      AND g.status = 'Final'
      AND g.home_score IS NOT NULL
      AND g.away_score IS NOT NULL
      AND g.season BETWEEN ? AND ?
      AND g.season != 2020
      AND CAST(strftime('%m', {_GAME_DATE}) AS INTEGER) BETWEEN 5 AND 8
      AND g.home_score != g.away_score
    ORDER BY g.season, {_GAME_DATE}, g.game_pk
    """
    return pd.read_sql_query(sql, con, params=(start_year, end_year))


def engineer_features(df: pd.DataFrame, min_sp_starts: int) -> pd.DataFrame:
    out = df.copy()
    for col in (
        "home_ml",
        "away_ml",
        "home_rl_odds",
        "away_rl_odds",
        "total_line",
        "home_ops_wma",
        "away_ops_wma",
        "home_sp_era_wma",
        "away_sp_era_wma",
        "home_sp_starts",
        "away_sp_starts",
    ):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    # Favorite: lower ML (more negative favorite, or smaller positive)
    out["home_is_favorite"] = (
        out["home_ml"].notna()
        & out["away_ml"].notna()
        & (out["home_ml"] < out["away_ml"])
    )
    out["favorite_is_home"] = out["home_is_favorite"]
    out["underdog_ml"] = np.where(
        out["favorite_is_home"],
        out["away_ml"],
        out["home_ml"],
    )
    out["favorite_ml"] = np.where(
        out["favorite_is_home"],
        out["home_ml"],
        out["away_ml"],
    )
    out["underdog_rl_odds"] = np.where(
        out["favorite_is_home"],
        out["away_rl_odds"],
        out["home_rl_odds"],
    )
    out["favorite_rl_odds"] = np.where(
        out["favorite_is_home"],
        out["home_rl_odds"],
        out["away_rl_odds"],
    )

    margin = out["margin"].astype(float)
    out["favorite_covers_rl"] = np.where(
        out["favorite_is_home"],
        margin >= 2,
        margin <= -2,
    ).astype(int)
    out["underdog_covers_rl"] = 1 - out["favorite_covers_rl"]
    out["rl_competitive"] = (out["abs_margin"] <= 2).astype(int)

    out["runs_scored_bucket"] = pd.cut(
        out["total_runs"],
        bins=[-1, 7, 10, 14, 999],
        labels=[b[0] for b in RUNS_SCORED_BUCKETS],
    )

    def total_line_bucket(tl: float) -> str:
        if pd.isna(tl):
            return "No total line"
        for label, fn in TOTAL_LINE_BUCKETS:
            if fn(float(tl)):
                return label
        return "No total line"

    out["total_line_bucket"] = out["total_line"].apply(total_line_bucket)

    def dog_ml_bucket(ml: float) -> str:
        if pd.isna(ml) or ml <= 100:
            return "Not underdog (+ML)"
        for label, lo, hi in DOG_ML_BUCKETS:
            if lo <= ml <= hi:
                return label
        return "Not underdog (+ML)"

    out["underdog_ml_bucket"] = out["underdog_ml"].apply(dog_ml_bucket)
    out["underdog_side"] = np.where(
        out["favorite_is_home"],
        "Away underdog",
        "Home underdog",
    )

    sp_ok = (
        out["home_sp_era_wma"].notna()
        & out["away_sp_era_wma"].notna()
        & (out["home_sp_starts"].fillna(0) >= min_sp_starts)
        & (out["away_sp_starts"].fillna(0) >= min_sp_starts)
    )
    out["sp_data_ok"] = sp_ok.astype(int)
    out["sp_duel"] = (
        sp_ok
        & (out["home_sp_era_wma"] < 4.0)
        & (out["away_sp_era_wma"] < 4.0)
    ).astype(int)

    home_weak = out["home_sp_era_wma"] < 3.5
    home_strong_bad = out["home_sp_era_wma"] > 5.0
    away_weak = out["away_sp_era_wma"] < 3.5
    away_strong_bad = out["away_sp_era_wma"] > 5.0
    out["sp_mismatch"] = (
        sp_ok
        & (
            (home_weak & away_strong_bad)
            | (away_weak & home_strong_bad)
        )
    ).astype(int)
    out["sp_mismatch_home_weak"] = (sp_ok & home_weak & away_strong_bad).astype(int)
    out["sp_mismatch_away_weak"] = (sp_ok & away_weak & home_strong_bad).astype(int)

    out["close_game_archetype"] = (
        (out["underdog_ml"] >= 101)
        & (out["underdog_ml"] <= 130)
        & out["total_line"].notna()
        & (out["total_line"] <= 8.5)
    ).astype(int)

    out["underdog_implied"] = out["underdog_rl_odds"].map(
        lambda x: american_to_implied(x) if pd.notna(x) else np.nan
    )
    out["favorite_implied"] = out["favorite_rl_odds"].map(
        lambda x: american_to_implied(x) if pd.notna(x) else np.nan
    )

    return out


@dataclass
class BucketResult:
    label: str
    n: int
    dog_cover: float
    fav_cover: float
    dog_implied: float
    fav_implied: float
    dog_edge: float
    fav_edge: float
    dog_n_odds: int
    fav_n_odds: int
    dog_verdict: str
    fav_verdict: str


def summarize_rl_bucket(df: pd.DataFrame, label: str) -> BucketResult:
    n = len(df)
    if n == 0:
        return BucketResult(
            label=label,
            n=0,
            dog_cover=0.0,
            fav_cover=0.0,
            dog_implied=np.nan,
            fav_implied=np.nan,
            dog_edge=np.nan,
            fav_edge=np.nan,
            dog_n_odds=0,
            fav_n_odds=0,
            dog_verdict="NOT SUPPORTED",
            fav_verdict="NOT SUPPORTED",
        )

    dog_cover = float(df["underdog_covers_rl"].mean())
    fav_cover = float(df["favorite_covers_rl"].mean())

    dog_odds_mask = df["underdog_rl_odds"].notna()
    fav_odds_mask = df["favorite_rl_odds"].notna()
    dog_n_odds = int(dog_odds_mask.sum())
    fav_n_odds = int(fav_odds_mask.sum())

    dog_implied = (
        float(df.loc[dog_odds_mask, "underdog_implied"].mean())
        if dog_n_odds
        else np.nan
    )
    fav_implied = (
        float(df.loc[fav_odds_mask, "favorite_implied"].mean())
        if fav_n_odds
        else np.nan
    )
    dog_edge = dog_cover - dog_implied if dog_n_odds else np.nan
    fav_edge = fav_cover - fav_implied if fav_n_odds else np.nan

    return BucketResult(
        label=label,
        n=n,
        dog_cover=dog_cover,
        fav_cover=fav_cover,
        dog_implied=dog_implied,
        fav_implied=fav_implied,
        dog_edge=dog_edge,
        fav_edge=fav_edge,
        dog_n_odds=dog_n_odds,
        fav_n_odds=fav_n_odds,
        dog_verdict=signal_verdict(dog_cover, n),
        fav_verdict=signal_verdict(fav_cover, n),
    )


def format_bucket_row(r: BucketResult, *, side: str) -> str:
    if side == "dog":
        rate, implied, edge, n_odds, verdict = (
            r.dog_cover,
            r.dog_implied,
            r.dog_edge,
            r.dog_n_odds,
            r.dog_verdict,
        )
    else:
        rate, implied, edge, n_odds, verdict = (
            r.fav_cover,
            r.fav_implied,
            r.fav_edge,
            r.fav_n_odds,
            r.fav_verdict,
        )
    impl_s = f"{implied:.1%}" if not np.isnan(implied) else "n/a"
    edge_s = f"{edge:+.1%}" if not np.isnan(edge) else "n/a"
    be = breakeven_juice(rate)
    return (
        f"  {r.label:<32} n={r.n:>5}  cover={rate:>6.1%}  "
        f"implied={impl_s:>6}  edge={edge_s:>7}  "
        f"breakeven={be:>5}  odds_n={n_odds:>4}  [{verdict}]"
    )


def margin_distribution(df: pd.DataFrame) -> tuple[list[dict], list[dict]]:
    """Overall and by runs_scored_bucket margin counts."""
    overall: list[dict] = []
    n_all = len(df)
    for m in MARGIN_BINS:
        cnt = int((df["abs_margin"] == m).sum())
        overall.append(
            {
                "margin": str(m),
                "n": cnt,
                "pct": cnt / n_all if n_all else 0.0,
            }
        )
    cnt_8p = int((df["abs_margin"] >= 8).sum())
    overall.append(
        {
            "margin": "8+",
            "n": cnt_8p,
            "pct": cnt_8p / n_all if n_all else 0.0,
        }
    )

    by_bucket: list[dict] = []
    for label, _ in RUNS_SCORED_BUCKETS:
        sub = df[df["runs_scored_bucket"] == label]
        n_sub = len(sub)
        if n_sub == 0:
            continue
        one_run = int((sub["abs_margin"] == 1).sum())
        two_run = int((sub["abs_margin"] == 2).sum())
        le2 = int((sub["abs_margin"] <= 2).sum())
        by_bucket.append(
            {
                "bucket": label,
                "n": n_sub,
                "one_run_pct": one_run / n_sub,
                "two_run_pct": two_run / n_sub,
                "le2_pct": le2 / n_sub,
            }
        )
    return overall, by_bucket


def collect_viable(results: list[BucketResult], *, side: str) -> list[str]:
    lines: list[str] = []
    for r in results:
        verdict = r.dog_verdict if side == "dog" else r.fav_verdict
        if verdict in ("VIABLE", "BORDERLINE"):
            rate = r.dog_cover if side == "dog" else r.fav_cover
            lines.append(
                f"  [{verdict}] {r.label}: "
                f"{'underdog' if side == 'dog' else 'favorite'} RL cover "
                f"{rate:.1%} (n={r.n}) — breakeven juice {breakeven_juice(rate)}"
            )
    return lines


def write_report(
    path: Path,
    *,
    df: pd.DataFrame,
    start_year: int,
    end_year: int,
    min_sp_starts: int,
    coverage: dict[str, list[int]],
    margin_overall: list[dict],
    margin_by_runs: list[dict],
    dog_results: list[BucketResult],
    fav_results: list[BucketResult],
    combined_results: list[BucketResult],
) -> None:
    lines: list[str] = []
    lines.append("RL MARGIN BACKTEST")
    lines.append("=" * 72)
    lines.append(f"Generated: {datetime.now().isoformat(timespec='seconds')}")
    lines.append(
        f"Seasons: {start_year}–{end_year} (excludes {sorted(EXCLUDE_SEASONS)})"
    )
    lines.append("Months: May–August  |  ties excluded")
    lines.append(f"Min SP starts (ERA segmentation): {min_sp_starts}")
    lines.append(
        f"Rolling-stats coverage: team={coverage.get('team_rolling_stats') or 'none'}, "
        f"pitcher={coverage.get('pitcher_rolling_stats') or 'none'}"
    )
    lines.append(
        "SP duel / mismatch buckets require probable-pitcher ERA WMA — "
        "effective window often 2022–2025 only."
    )
    lines.append(f"Total games: {len(df)}")
    with_ml = df["home_ml"].notna() & df["away_ml"].notna()
    with_rl = df["underdog_rl_odds"].notna()
    lines.append(f"Games with closing ML: {int(with_ml.sum())}")
    lines.append(f"Games with closing RL odds: {int(with_rl.sum())}")

    # Section 1
    lines.append("")
    lines.append("SECTION 1 — MARGIN DISTRIBUTION (Hypothesis A)")
    lines.append("-" * 72)
    n_all = len(df)
    one_run = int((df["abs_margin"] == 1).sum())
    le2 = int((df["abs_margin"] <= 2).sum())
    lines.append(
        f"Overall: {one_run / n_all:.1%} decided by exactly 1 run "
        f"({one_run}/{n_all}); {le2 / n_all:.1%} by ≤2 runs (RL-competitive)."
    )
    lines.append("")
    lines.append(f"  {'Margin':<8} {'Count':>8} {'Pct':>8}")
    lines.append(f"  {'-'*8} {'-'*8} {'-'*8}")
    for row in margin_overall:
        lines.append(
            f"  {row['margin']:<8} {row['n']:>8} {row['pct']:>7.1%}"
        )
    lines.append("")
    lines.append("  By total runs scored (actual):")
    lines.append(
        f"  {'Bucket':<18} {'N':>6} {'1-run':>8} {'2-run':>8} {'≤2-run':>8}"
    )
    for row in margin_by_runs:
        lines.append(
            f"  {row['bucket']:<18} {row['n']:>6} "
            f"{row['one_run_pct']:>7.1%} {row['two_run_pct']:>7.1%} "
            f"{row['le2_pct']:>7.1%}"
        )

    # Section 2
    lines.append("")
    lines.append("SECTION 2 — UNDERDOG RL COVER RATES (Hypothesis B, +1.5)")
    lines.append("-" * 72)
    lines.append(
        "Cover = underdog wins outright OR favorite wins by exactly 1 run."
    )
    lines.append(
        "Breakeven juice: American odds at which cover rate = break-even "
        "(e.g. 55% → -122, 57% → -133)."
    )
    for r in dog_results:
        lines.append(format_bucket_row(r, side="dog"))

    # Section 3
    lines.append("")
    lines.append("SECTION 3 — FAVORITE RL COVER RATES (Hypothesis C, -1.5)")
    lines.append("-" * 72)
    lines.append("Cover = favorite wins by 2+ runs.")
    for r in fav_results:
        lines.append(format_bucket_row(r, side="fav"))

    # Section 4
    lines.append("")
    lines.append("SECTION 4 — COMBINED CONDITION BUCKETS")
    lines.append("-" * 72)
    for r in combined_results:
        lines.append(format_bucket_row(r, side="dog"))
        lines.append(format_bucket_row(r, side="fav"))

    # Section 5
    lines.append("")
    lines.append("SECTION 5 — SIGNAL VIABILITY SUMMARY")
    lines.append("-" * 72)
    dog_viable = collect_viable(dog_results + combined_results, side="dog")
    fav_viable = collect_viable(fav_results + combined_results, side="fav")
    if dog_viable:
        lines.append("Underdog +1.5 candidates:")
        lines.extend(dog_viable)
    else:
        lines.append(
            "No VIABLE/BORDERLINE underdog RL buckets (need n≥50 and cover >52%)."
        )
    if fav_viable:
        lines.append("")
        lines.append("Favorite -1.5 candidates:")
        lines.extend(fav_viable)
    else:
        lines.append(
            "No VIABLE/BORDERLINE favorite RL buckets (need n≥50 and cover >52%)."
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_detail_csv(path: Path, df: pd.DataFrame) -> None:
    cols = [
        "game_pk",
        "season",
        "game_date_et",
        "home_team",
        "away_team",
        "home_score",
        "away_score",
        "margin",
        "abs_margin",
        "total_runs",
        "runs_scored_bucket",
        "total_line",
        "total_line_bucket",
        "home_ml",
        "away_ml",
        "favorite_ml",
        "underdog_ml",
        "underdog_ml_bucket",
        "underdog_side",
        "home_rl_odds",
        "away_rl_odds",
        "underdog_rl_odds",
        "favorite_rl_odds",
        "underdog_implied",
        "favorite_implied",
        "favorite_covers_rl",
        "underdog_covers_rl",
        "rl_competitive",
        "sp_data_ok",
        "sp_duel",
        "sp_mismatch",
        "sp_mismatch_home_weak",
        "sp_mismatch_away_weak",
        "close_game_archetype",
        "home_sp_era_wma",
        "away_sp_era_wma",
    ]
    existing = [c for c in cols if c in df.columns]
    path.parent.mkdir(parents=True, exist_ok=True)
    df[existing].to_csv(path, index=False)


def build_bucket_results(df: pd.DataFrame) -> tuple[
    list[BucketResult],
    list[BucketResult],
    list[BucketResult],
]:
    """Dog section, fav section (same buckets, fav perspective), combined."""
    rl_base = df[df["home_ml"].notna() & df["away_ml"].notna()].copy()

    dog_specs: list[tuple[str, pd.Series]] = [
        ("All games (ML known)", pd.Series(True, index=rl_base.index)),
    ]
    for label, lo, hi in DOG_ML_BUCKETS:
        mask = (rl_base["underdog_ml"] >= lo) & (rl_base["underdog_ml"] <= hi)
        dog_specs.append((label, mask))
    for label, _ in TOTAL_LINE_BUCKETS:
        mask = rl_base["total_line_bucket"] == label
        dog_specs.append((f"Total line — {label}", mask))
    dog_specs.extend(
        [
            ("Home underdog", rl_base["underdog_side"] == "Home underdog"),
            ("Away underdog", rl_base["underdog_side"] == "Away underdog"),
            (
                "SP duel (both ERA < 4.0)",
                rl_base["sp_duel"] == 1,
            ),
            (
                "SP mismatch (any)",
                rl_base["sp_mismatch"] == 1,
            ),
            (
                "Mismatch — home SP weak / away SP bad",
                rl_base["sp_mismatch_home_weak"] == 1,
            ),
            (
                "Mismatch — away SP weak / home SP bad",
                rl_base["sp_mismatch_away_weak"] == 1,
            ),
        ]
    )

    dog_results = [
        summarize_rl_bucket(rl_base.loc[mask[mask].index], label)
        for label, mask in dog_specs
    ]

    fav_results = [
        summarize_rl_bucket(rl_base.loc[mask[mask].index], label)
        for label, mask in dog_specs
    ]

    combined_specs: list[tuple[str, pd.Series]] = [
        (
            "Close game: dog +101–130 & total ≤8.5",
            rl_base["close_game_archetype"] == 1,
        ),
        (
            "Dog +101–130 & SP duel",
            (rl_base["underdog_ml"] >= 101)
            & (rl_base["underdog_ml"] <= 130)
            & (rl_base["sp_duel"] == 1),
        ),
        (
            "Dog +101–130 & total ≤9.0 (sensitivity)",
            (rl_base["underdog_ml"] >= 101)
            & (rl_base["underdog_ml"] <= 130)
            & rl_base["total_line"].notna()
            & (rl_base["total_line"] <= 9.0),
        ),
        (
            "Home dog +101–130 & total ≤8.5",
            (rl_base["underdog_side"] == "Home underdog")
            & (rl_base["underdog_ml"] >= 101)
            & (rl_base["underdog_ml"] <= 130)
            & rl_base["total_line"].notna()
            & (rl_base["total_line"] <= 8.5),
        ),
        (
            "Away dog +101–130 & total ≤8.5",
            (rl_base["underdog_side"] == "Away underdog")
            & (rl_base["underdog_ml"] >= 101)
            & (rl_base["underdog_ml"] <= 130)
            & rl_base["total_line"].notna()
            & (rl_base["total_line"] <= 8.5),
        ),
    ]
    combined_results = [
        summarize_rl_bucket(rl_base.loc[mask[mask].index], label)
        for label, mask in combined_specs
    ]

    return dog_results, fav_results, combined_results


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="RL margin distribution and run line cover backtest."
    )
    p.add_argument("--db", default=get_db_path(), help="SQLite database path")
    p.add_argument("--start-year", type=int, default=2019)
    p.add_argument("--end-year", type=int, default=2025)
    p.add_argument(
        "--min-sp-starts",
        type=int,
        default=3,
        help="Min starts for SP ERA segmentation buckets",
    )
    p.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Report output directory",
    )
    p.add_argument(
        "--output-csv",
        action="store_true",
        help="Write outputs/reports/rl_margin_detail.csv",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = resolve_path(args.output_dir)
    report_path = out_dir / "rl_margin_backtest.txt"
    csv_path = out_dir / "rl_margin_detail.csv"

    print(
        f"[rl_margin] Season filter: {args.start_year}-{args.end_year} "
        f"(excludes {sorted(EXCLUDE_SEASONS)})"
    )

    con = db_connect(args.db)
    try:
        coverage = query_rolling_stats_coverage(
            con, args.start_year, args.end_year
        )
        df = load_games(con, args.start_year, args.end_year)
    finally:
        con.close()

    if df.empty:
        print("[rl_margin] No games loaded.")
        return 1

    df = engineer_features(df, args.min_sp_starts)
    margin_overall, margin_by_runs = margin_distribution(df)
    dog_results, fav_results, combined_results = build_bucket_results(df)

    write_report(
        report_path,
        df=df,
        start_year=args.start_year,
        end_year=args.end_year,
        min_sp_starts=args.min_sp_starts,
        coverage=coverage,
        margin_overall=margin_overall,
        margin_by_runs=margin_by_runs,
        dog_results=dog_results,
        fav_results=fav_results,
        combined_results=combined_results,
    )

    if args.output_csv:
        write_detail_csv(csv_path, df)

    n = len(df)
    one_run_pct = (df["abs_margin"] == 1).mean()
    print(f"[rl_margin] Report -> {report_path}")
    print(f"[rl_margin] Games={n}  1-run rate={one_run_pct:.1%}")
    viable_dog = sum(1 for r in dog_results if r.dog_verdict == "VIABLE")
    print(f"[rl_margin] VIABLE underdog buckets: {viable_dog}")
    if args.output_csv:
        print(f"[rl_margin] Detail CSV -> {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
