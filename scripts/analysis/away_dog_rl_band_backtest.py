#!/usr/bin/env python3
"""
Away Dog RL ML band backtest — control (+101/+130) vs extension (+131/+160).

Open Item #10b: segment historical Away Dog RL setups by away ML band before
widening the live signal band.

Usage:
    python scripts/analysis/away_dog_rl_band_backtest.py
    python scripts/analysis/away_dog_rl_band_backtest.py --db data/mlb_stats.db \\
        --seasons 2019 2020 2021 2022 2023 2024 2025
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from batch.analysis.prediction.ou_rl_backtest import (
    american_payout,
    build_games_query,
    load_closing_odds,
    resolve_path,
)
from core.db.connection import connect as db_connect, get_db_path

ET = ZoneInfo("America/New_York")
REPORT_PATH = ROOT / "outputs" / "reports" / "away_dog_rl_band_backtest.txt"
CSV_PATH = ROOT / "outputs" / "reports" / "away_dog_rl_band_backtest_detail.csv"

DEFAULT_SEASONS = [2019, 2020, 2021, 2022, 2023, 2024, 2025]
MIN_GAMES = 20
MAY_AUG_MONTHS = {5, 6, 7, 8}

# Gates (match live Away Dog RL signal)
ML_BAND_A = (101, 130)
ML_BAND_B = (131, 160)
ML_BAND_COMBINED = (101, 160)
ML_BAND_B_LOW = (131, 145)
ML_BAND_B_HIGH = (146, 160)
TOTAL_MAX = 8.5
RL_JUICE_MIN = -190
HOME_SP_ERA_MIN = 5.0

# Known control benchmark (May–Aug 2019–2025, pre–#10b reference)
CONTROL_COVER_PCT = 66.1
CONTROL_N = 1059

MONTH_NAMES = {5: "May", 6: "Jun", 7: "Jul", 8: "Aug"}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Away Dog RL ML band backtest (#10b)")
    p.add_argument("--db", default=get_db_path())
    p.add_argument("--seasons", type=int, nargs="+", default=DEFAULT_SEASONS)
    p.add_argument("--min-games", type=int, default=MIN_GAMES)
    return p.parse_args()


def _month_from_date(val: object) -> int | None:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    s = str(val).strip()
    if len(s) >= 7 and s[4] == "-":
        try:
            return int(s[5:7])
        except ValueError:
            pass
    try:
        return int(pd.Timestamp(val).month)
    except (TypeError, ValueError):
        return None


def is_dome_venue(venue_name: object) -> bool:
    v = str(venue_name or "")
    return "Tropicana" in v or "Rogers Centre" in v


def away_dog_rl_cover(home_score: float, away_score: float) -> tuple[bool | None, bool]:
    """Away +1.5 cover: home margin <= 1. Integer +1.5 lines do not push."""
    margin = float(home_score) - float(away_score)
    if margin <= 1:
        return True, False
    return False, False


def roi_at_odds(cover_rate: float, avg_odds: float | None) -> float:
    payout = american_payout(avg_odds) if avg_odds is not None and not pd.isna(avg_odds) else 100.0 / 130.0
    return cover_rate * payout - (1.0 - cover_rate)


def in_ml_band(away_ml: float, lo: int, hi: int) -> bool:
    return lo <= int(round(float(away_ml))) <= hi


@dataclass
class SliceStats:
    label: str
    n: int = 0
    wins: int = 0
    losses: int = 0
    pushes: int = 0
    avg_rl_odds: float | None = None
    median_rl_odds: float | None = None

    @property
    def cover_rate(self) -> float | None:
        d = self.wins + self.losses
        return self.wins / d if d else None

    @property
    def cover_pct(self) -> float | None:
        cr = self.cover_rate
        return cr * 100.0 if cr is not None else None

    @property
    def roi_avg(self) -> float | None:
        cr = self.cover_rate
        if cr is None:
            return None
        return roi_at_odds(cr, self.median_rl_odds) * 100.0

    @property
    def roi_flat_130(self) -> float | None:
        cr = self.cover_rate
        if cr is None:
            return None
        return roi_at_odds(cr, -130.0) * 100.0


def summarize_slice(df: pd.DataFrame, label: str) -> SliceStats:
    if df.empty:
        return SliceStats(label=label)

    covers = df["cover"].astype("boolean")
    wins = int(covers.sum())
    losses = int((~covers).sum())
    pushes = int(df["push"].sum()) if "push" in df.columns else 0
    odds = pd.to_numeric(df["away_rl_odds"], errors="coerce").dropna()

    return SliceStats(
        label=label,
        n=len(df),
        wins=wins,
        losses=losses,
        pushes=pushes,
        avg_rl_odds=float(odds.mean()) if len(odds) else None,
        median_rl_odds=float(odds.median()) if len(odds) else None,
    )


def fmt_pct(val: float | None, width: int = 6) -> str:
    if val is None:
        return "   n/a"
    return f"{val:>{width}.1f}%"


def fmt_roi(val: float | None) -> str:
    if val is None:
        return "  n/a"
    return f"{val:+6.1f}%"


def fmt_odds(val: float | None) -> str:
    if val is None:
        return "n/a"
    return f"{int(round(val)):+d}"


def format_slice_block(stats: SliceStats) -> list[str]:
    cr = stats.cover_pct
    lines = [
        f"  N={stats.n}  |  Cover={fmt_pct(cr).strip()}  "
        f"(W={stats.wins} L={stats.losses} Push={stats.pushes})",
        f"  Avg away RL odds: {fmt_odds(stats.avg_rl_odds)}  "
        f"|  Median: {fmt_odds(stats.median_rl_odds)}",
        f"  ROI @ median RL: {fmt_roi(stats.roi_avg).strip()}  "
        f"|  ROI @ -130 flat: {fmt_roi(stats.roi_flat_130).strip()}",
    ]
    return lines


def season_breakdown(df: pd.DataFrame) -> list[str]:
    lines = ["  Season breakdown:"]
    for season in sorted(df["season"].unique()):
        s = summarize_slice(df[df["season"] == season], str(season))
        lines.append(
            f"    {season}: N={s.n:4d}  Cover={fmt_pct(s.cover_pct).strip()}  "
            f"ROI@med={fmt_roi(s.roi_avg).strip()}"
        )
    return lines


def month_breakdown(df: pd.DataFrame) -> list[str]:
    lines = ["  Monthly breakdown (May–Aug window):"]
    for m in sorted(MAY_AUG_MONTHS):
        sub = df[df["month"] == m]
        if sub.empty:
            continue
        s = summarize_slice(sub, MONTH_NAMES[m])
        lines.append(
            f"    {MONTH_NAMES[m]:>3}: N={s.n:4d}  Cover={fmt_pct(s.cover_pct).strip()}  "
            f"ROI@med={fmt_roi(s.roi_avg).strip()}"
        )
    return lines


def apply_base_gates(
    games: pd.DataFrame,
    odds: pd.DataFrame,
    min_games: int,
    *,
    require_sp_gate: bool = True,
    require_rl_juice: bool = True,
) -> pd.DataFrame:
    df = games.merge(odds, on="game_pk", how="inner")

    for col in (
        "home_ml", "away_ml", "total_line", "away_rl_odds", "hsp_era",
        "home_score", "away_score", "h_games_played", "a_games_played",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["month"] = df["game_date_et"].map(_month_from_date)
    df = df[df["month"].isin(MAY_AUG_MONTHS)].copy()

    mask = (
        (df["h_games_played"] >= min_games)
        & (df["a_games_played"] >= min_games)
        & df["away_ml"].notna()
        & df["home_ml"].notna()
        & (df["away_ml"] > 0)
        & (df["away_ml"] > df["home_ml"])
        & df["total_line"].notna()
        & (df["total_line"] <= TOTAL_MAX)
    )
    if require_rl_juice:
        mask = mask & df["away_rl_odds"].notna() & (df["away_rl_odds"] >= RL_JUICE_MIN)
    if require_sp_gate:
        mask = mask & df["hsp_era"].notna() & (df["hsp_era"] >= HOME_SP_ERA_MIN)

    df = df[mask].copy()

    covers: list[bool] = []
    pushes: list[bool] = []
    for _, row in df.iterrows():
        c, p = away_dog_rl_cover(row["home_score"], row["away_score"])
        covers.append(bool(c))
        pushes.append(bool(p))
    df["cover"] = covers
    df["push"] = pushes
    df["hsp_era_wma"] = df["hsp_era"]
    df["is_dome"] = df["venue_name"].map(is_dome_venue)
    return df


def assign_ml_band(away_ml: float) -> str | None:
    if in_ml_band(away_ml, *ML_BAND_A):
        return "A"
    if in_ml_band(away_ml, *ML_BAND_B):
        return "B"
    return None


def band_filter(df: pd.DataFrame, lo: int, hi: int) -> pd.DataFrame:
    mask = df["away_ml"].apply(lambda x: in_ml_band(x, lo, hi))
    return df[mask].copy()


def gate_funnel(games: pd.DataFrame, odds: pd.DataFrame, min_games: int) -> list[str]:
    """Document how many games survive each cumulative filter."""
    df = games.merge(odds, on="game_pk", how="inner")
    for col in (
        "home_ml", "away_ml", "total_line", "away_rl_odds", "hsp_era",
        "h_games_played", "a_games_played",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["month"] = df["game_date_et"].map(_month_from_date)

    steps: list[tuple[str, pd.Series]] = [
        ("May-Aug with closing odds", df["month"].isin(MAY_AUG_MONTHS)),
        (f"Both teams >= {min_games} GP", (df["h_games_played"] >= min_games) & (df["a_games_played"] >= min_games)),
        ("Away underdog (away ML > home ML)", (df["away_ml"] > 0) & (df["away_ml"] > df["home_ml"])),
        (f"Total line <= {TOTAL_MAX}", df["total_line"] <= TOTAL_MAX),
        (f"Away RL juice >= {RL_JUICE_MIN}", df["away_rl_odds"] >= RL_JUICE_MIN),
        (f"Home SP ERA WMA >= {HOME_SP_ERA_MIN}", df["hsp_era"] >= HOME_SP_ERA_MIN),
    ]
    lines = ["GATE FUNNEL (cumulative):"]
    pool = df.copy()
    for label, mask in steps:
        pool = pool[mask.reindex(pool.index).fillna(False)]
        lines.append(f"  {label:<40} N={len(pool):5d}")
    lines.append(
        "  NOTE: pitcher_rolling_stats SP gate only populated 2022+ in this DB; "
        "2019-2021 drop at SP gate."
    )
    return lines


def pipeline_validation(games: pd.DataFrame, odds: pd.DataFrame, min_games: int) -> list[str]:
    """Reproduce headline benchmark slice (no SP / optional juice gates)."""
    headline = apply_base_gates(
        games, odds, min_games, require_sp_gate=False, require_rl_juice=False,
    )
    with_juice = apply_base_gates(
        games, odds, min_games, require_sp_gate=False, require_rl_juice=True,
    )
    a_head = summarize_slice(band_filter(headline, *ML_BAND_A), "headline A")
    a_juice = summarize_slice(band_filter(with_juice, *ML_BAND_A), "A+juice")
    b_juice = summarize_slice(band_filter(with_juice, *ML_BAND_B), "B+juice")

    lines = [
        "PIPELINE VALIDATION (headline benchmark reproduction):",
        (
            f"  Band A, no SP/juice gates: N={a_head.n}  "
            f"Cover={fmt_pct(a_head.cover_pct).strip()}  "
            f"(target ~{CONTROL_N:,} / {CONTROL_COVER_PCT:.1f}%)"
        ),
        (
            f"  Band A, juice gate only:   N={a_juice.n}  "
            f"Cover={fmt_pct(a_juice.cover_pct).strip()}"
        ),
        (
            f"  Band B, juice gate only:   N={b_juice.n}  "
            f"Cover={fmt_pct(b_juice.cover_pct).strip()}  "
            "(no SP gate — historical reference only)"
        ),
        (
            "  Live-signal sections A-D below apply ALL gates "
            f"(SP >= {HOME_SP_ERA_MIN}, RL >= {RL_JUICE_MIN})."
        ),
    ]
    return lines


def control_check(stats: SliceStats) -> list[str]:
    lines = ["  CONTROL CHECK (Band A full live gates vs headline benchmark):"]
    if stats.cover_pct is None:
        lines.append("  WARN: Band A has no qualifying games -- pipeline issue.")
        return lines

    cover_ok = abs(stats.cover_pct - CONTROL_COVER_PCT) <= 5.0

    lines.append(
        f"  Full gates: N={stats.n}  Cover={stats.cover_pct:.1f}%  "
        f"(headline ref {CONTROL_COVER_PCT:.1f}%, n={CONTROL_N:,} without SP gate)"
    )
    if cover_ok:
        lines.append(
            f"  OK: Cover within 5pp of headline ({stats.cover_pct - CONTROL_COVER_PCT:+.1f}pp)."
        )
    else:
        lines.append(
            f"  WARN: Cover diverges by {stats.cover_pct - CONTROL_COVER_PCT:+.1f}pp "
            "-- verify joins if >5pp."
        )
    lines.append(
        "  N is lower than headline because home SP >= 5.0 gate + RL juice gate "
        "and pitcher_rolling_stats only covers 2022+."
    )
    return lines


def band_b_verdict(stats: SliceStats) -> list[str]:
    lines = ["  BAND B INTERPRETATION:"]
    if stats.n < 100:
        lines.append(
            f"  N={stats.n} < 100 — insufficient historical sample under all gates; "
            "DEFER band extension until more data accumulates."
        )
        return lines
    if stats.n < 200:
        lines.append(
            f"  N={stats.n} < 200 — below statistically meaningful threshold; "
            "treat results as directional only."
        )

    cr = stats.cover_pct
    if cr is None:
        lines.append("  No cover rate available.")
        return lines

    if cr >= 62.0 and stats.n >= 200:
        lines.append(
            f"  STRONG CASE TO EXTEND: cover {cr:.1f}% with N={stats.n} "
            f"(>= 62% threshold, N >= 200)."
        )
    elif cr >= 58.0:
        lines.append(
            f"  MARGINAL: cover {cr:.1f}% — examine sub-slices and season breakdown "
            "before going live."
        )
    else:
        lines.append(
            f"  DO NOT EXTEND: cover {cr:.1f}% < 58% — wider band dilutes signal."
        )
    return lines


def open_item_recommendation(stats_b: SliceStats) -> str:
    if stats_b.cover_pct is not None and stats_b.n >= 200 and stats_b.cover_pct >= 62.0:
        return (
            "OPEN ITEM #10b: EXTEND -- update score_today.py / generate_daily_brief.py "
            f"away ML band to +101-+160 (Band B cover {stats_b.cover_pct:.1f}%, N={stats_b.n})."
        )
    if stats_b.n < 200 or (stats_b.cover_pct is not None and stats_b.cover_pct < 62.0):
        return (
            "OPEN ITEM #10b: DEFER -- keep +101-+130 band "
            f"(Band B cover {fmt_pct(stats_b.cover_pct).strip()}, N={stats_b.n})."
        )
    return "OPEN ITEM #10b: REVIEW — inconclusive; manual review required."


def build_report(
    band_a: SliceStats,
    band_b: SliceStats,
    band_combined: SliceStats,
    band_b_no_dome: SliceStats,
    band_b_dome_excl: SliceStats,
    band_b_low: SliceStats,
    band_b_high: SliceStats,
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    df_combined: pd.DataFrame,
    df_b_all: pd.DataFrame,
    seasons: list[int],
    funnel_lines: list[str],
    validation_lines: list[str],
) -> str:
    ts = datetime.now(tz=ET).strftime("%Y-%m-%d %I:%M %p %Z")
    lines = [
        "=" * 60,
        "AWAY DOG RL ML BAND BACKTEST -- Open Item #10b",
        f"Generated: {ts}",
        f"Seasons: {seasons}  |  Window: May-Aug  |  Min GP: {MIN_GAMES}",
        "Gates: away underdog | total <= 8.5 | away RL >= -190 | home SP ERA WMA >= 5.0",
        "=" * 60,
        "",
        *funnel_lines,
        "",
        *validation_lines,
        "",
        "SECTION A -- Band A: away ML +101 to +130 (control)",
        *format_slice_block(band_a),
        *season_breakdown(df_a),
        *month_breakdown(df_a),
        *control_check(band_a),
        "",
        "SECTION B -- Band B: away ML +131 to +160 (proposed extension)",
        *format_slice_block(band_b),
        *season_breakdown(df_b),
        *month_breakdown(df_b),
        *band_b_verdict(band_b),
        "",
        "  Band B sub-slices:",
        f"    +131-+145: N={band_b_low.n}  Cover={fmt_pct(band_b_low.cover_pct).strip()}  "
        f"ROI@med={fmt_roi(band_b_low.roi_avg).strip()}",
        f"    +146-+160: N={band_b_high.n}  Cover={fmt_pct(band_b_high.cover_pct).strip()}  "
        f"ROI@med={fmt_roi(band_b_high.roi_avg).strip()}",
        "",
        "SECTION C -- Combined: away ML +101 to +160",
        *format_slice_block(band_combined),
        *season_breakdown(df_combined),
        *month_breakdown(df_combined),
        "",
        "SECTION D -- Band B dome sensitivity",
        "  With domes included:",
        *format_slice_block(band_b_no_dome),
        "  With domes excluded (Tropicana / Rogers Centre):",
        *format_slice_block(band_b_dome_excl),
        f"  Dome games in Band B: {int(df_b_all['is_dome'].sum())} of {len(df_b_all)}",
        "",
        "RECOMMENDATION:",
        f"  {open_item_recommendation(band_b)}",
        "=" * 60,
    ]
    return "\n".join(lines)


def build_csv(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["ml_band"] = out["away_ml"].map(
        lambda x: assign_ml_band(x) if pd.notna(x) else None
    )
    cols = [
        "game_pk", "game_date_et", "season", "month", "home_team", "away_team",
        "venue_name", "away_ml", "total_line", "away_rl_odds", "hsp_era_wma",
        "cover", "push", "ml_band",
    ]
    existing = [c for c in cols if c in out.columns]
    return out[existing].sort_values(["season", "game_date_et", "game_pk"])


def main() -> int:
    args = parse_args()
    seasons = sorted(set(args.seasons))

    con = db_connect(args.db)
    try:
        games = pd.read_sql_query(build_games_query(seasons), con, params=seasons)
        odds = load_closing_odds(con, games["game_pk"].tolist())
    finally:
        con.close()

    print(f"[away_dog_rl_band] Loaded {len(games)} completed games")

    base = apply_base_gates(games, odds, args.min_games)
    print(f"[away_dog_rl_band] After base gates: {len(base)} games")

    df_a = band_filter(base, *ML_BAND_A)
    df_b = band_filter(base, *ML_BAND_B)
    df_combined = band_filter(base, *ML_BAND_COMBINED)
    df_b_low = band_filter(base, *ML_BAND_B_LOW)
    df_b_high = band_filter(base, *ML_BAND_B_HIGH)
    df_b_no_dome = df_b.copy()
    df_b_dome_excl = df_b[~df_b["is_dome"]].copy()

    band_a = summarize_slice(df_a, "Band A")
    band_b = summarize_slice(df_b, "Band B")
    band_combined = summarize_slice(df_combined, "Combined")
    band_b_no_dome_s = summarize_slice(df_b_no_dome, "Band B (domes incl)")
    band_b_dome_excl_s = summarize_slice(df_b_dome_excl, "Band B (domes excl)")
    band_b_low_s = summarize_slice(df_b_low, "Band B +131–145")
    band_b_high_s = summarize_slice(df_b_high, "Band B +146–160")

    report = build_report(
        band_a, band_b, band_combined,
        band_b_no_dome_s, band_b_dome_excl_s,
        band_b_low_s, band_b_high_s,
        df_a, df_b, df_combined, df_b,
        seasons,
        gate_funnel(games, odds, args.min_games),
        pipeline_validation(games, odds, args.min_games),
    )

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(report + "\n", encoding="utf-8")

    csv_df = build_csv(pd.concat([df_a, df_b], ignore_index=True).drop_duplicates("game_pk"))
    csv_df.to_csv(CSV_PATH, index=False)

    sys.stdout.buffer.write((report + "\n\n").encode("utf-8", errors="replace"))
    sys.stdout.buffer.flush()
    print(f"\n[away_dog_rl_band] Report: {REPORT_PATH}")
    print(f"[away_dog_rl_band] CSV:    {CSV_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
