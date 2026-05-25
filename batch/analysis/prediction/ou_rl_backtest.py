#!/usr/bin/env python3
"""
ou_rl_backtest.py
─────────────────
Over/Under and run line (-1.5) backtesting on completed games 2024-2026.
Discovers available odds columns first, then outcome + market-relative analysis.

USAGE:
  python batch/analysis/prediction/ou_rl_backtest.py --db data/mlb_stats.db
  python batch/analysis/prediction/ou_rl_backtest.py --seasons 2024 2025 2026
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import connect as db_connect, get_db_path

DEFAULT_SEASONS = [2024, 2025, 2026]
DEFAULT_LOGREG = "outputs/reports/outcome_model_predictions_logreg.csv"
DEFAULT_OUTPUT_DIR = "outputs/reports"
DEFAULT_MIN_GAMES = 20

_GAME_DATE = "COALESCE(NULLIF(TRIM(g.game_date_et), ''), g.game_date)"


def resolve_path(path: str) -> Path:
    p = Path(path)
    return p if p.is_absolute() else _REPO_ROOT / p


def american_payout(odds: float) -> float:
    odds = float(odds)
    if odds < 0:
        return 100.0 / abs(odds)
    return odds / 100.0


class OddsDiscovery:
    def __init__(
        self,
        has_total_line: bool,
        has_rl_odds: bool,
        total_col: str | None,
        rl_home_col: str | None,
        rl_away_col: str | None,
        ml_home_col: str | None,
        ml_away_col: str | None,
        tables: list[str],
        line_tables: list[str],
    ):
        self.has_total_line = has_total_line
        self.has_rl_odds = has_rl_odds
        self.total_col = total_col
        self.rl_home_col = rl_home_col
        self.rl_away_col = rl_away_col
        self.ml_home_col = ml_home_col
        self.ml_away_col = ml_away_col
        self.tables = tables
        self.line_tables = line_tables


def discover_odds(con: sqlite3.Connection) -> tuple[OddsDiscovery, list[str]]:
    lines: list[str] = []
    lines.append("ODDS TABLE DISCOVERY")
    lines.append("=" * 54)

    tables = [
        r[0]
        for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
    ]
    lines.append(f"Tables in database: {len(tables)}")

    if "game_odds" in tables:
        cols = [r[1] for r in con.execute("PRAGMA table_info(game_odds)").fetchall()]
        lines.append(f"game_odds columns: {', '.join(cols)}")
        sample = con.execute("SELECT * FROM game_odds LIMIT 3").fetchall()
        lines.append(f"game_odds sample rows: {len(sample)}")
    else:
        cols = []

    line_tables = [
        r[0]
        for r in con.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table'
              AND (name LIKE '%total%'
                OR name LIKE '%over%'
                OR name LIKE '%runline%'
                OR name LIKE '%spread%'
                OR name LIKE '%line%')
            ORDER BY name
            """
        ).fetchall()
    ]
    lines.append(f"Line-related tables: {line_tables or '(none)'}")

    has_total = "total_line" in cols
    has_rl = "home_rl_line" in cols and "home_rl_odds" in cols
    discovery = OddsDiscovery(
        has_total_line=has_total,
        has_rl_odds=has_rl,
        total_col="total_line" if has_total else None,
        rl_home_col="home_rl_odds" if has_rl else None,
        rl_away_col="away_rl_odds" if has_rl else None,
        ml_home_col="home_ml" if "home_ml" in cols else None,
        ml_away_col="away_ml" if "away_ml" in cols else None,
        tables=tables,
        line_tables=line_tables,
    )

    lines.extend([
        "",
        f"HAS_TOTAL_LINE = {discovery.has_total_line}",
        f"HAS_RL_ODDS    = {discovery.has_rl_odds}",
        f"TOTAL_COL      = {discovery.total_col}",
        f"RL_HOME_COL    = {discovery.rl_home_col}",
        f"RL_AWAY_COL    = {discovery.rl_away_col}",
        "",
    ])
    return discovery, lines


def build_games_query(seasons: list[int]) -> str:
    ph = ",".join("?" * len(seasons))
    return f"""
SELECT
    g.game_pk,
    {_GAME_DATE} AS game_date_et,
    g.season,
    g.home_team_id,
    g.away_team_id,
    ht.abbreviation AS home_team,
    at.abbreviation AS away_team,
    g.home_score,
    g.away_score,
    g.home_score + g.away_score AS total_runs,
    g.home_score - g.away_score AS run_diff,
    CASE WHEN g.home_score > g.away_score THEN g.home_team_id ELSE g.away_team_id END AS winner_id,
    CASE WHEN g.home_score > g.away_score THEN ht.abbreviation ELSE at.abbreviation END AS winner,

    v.venue_id,
    v.name AS venue_name,
    v.park_factor_runs,
    v.elevation_ft,

    g.temp_f AS temperature,
    g.wind_mph AS wind_speed,
    g.wind_direction,

    trs_h.rolling_ops AS h_ops,
    trs_h.rolling_runs_scored_pg AS h_runs_pg,
    trs_h.rolling_runs_allowed_pg AS h_ra_pg,

    trs_a.rolling_ops AS a_ops,
    trs_a.rolling_runs_scored_pg AS a_runs_pg,

    prs_h.era_wma AS hsp_era,
    prs_h.k_per_9_wma AS hsp_k9,

    prs_a.era_wma AS asp_era,
    prs_a.k_per_9_wma AS asp_k9,

    (SELECT COUNT(*)
     FROM games g2
     WHERE g2.season = g.season
       AND g2.game_type = 'R'
       AND g2.status = 'Final'
       AND g2.home_score IS NOT NULL
       AND COALESCE(NULLIF(TRIM(g2.game_date_et), ''), g2.game_date) < {_GAME_DATE}
       AND (g2.home_team_id = g.home_team_id OR g2.away_team_id = g.home_team_id)
    ) AS h_games_played,

    (SELECT COUNT(*)
     FROM games g3
     WHERE g3.season = g.season
       AND g3.game_type = 'R'
       AND g3.status = 'Final'
       AND g3.home_score IS NOT NULL
       AND COALESCE(NULLIF(TRIM(g3.game_date_et), ''), g3.game_date) < {_GAME_DATE}
       AND (g3.home_team_id = g.away_team_id OR g3.away_team_id = g.away_team_id)
    ) AS a_games_played

FROM games g
JOIN teams ht ON ht.team_id = g.home_team_id
JOIN teams at ON at.team_id = g.away_team_id
LEFT JOIN venues v ON v.venue_id = g.venue_id

LEFT JOIN team_rolling_stats trs_h
  ON trs_h.game_pk = g.game_pk AND trs_h.team_id = g.home_team_id
LEFT JOIN team_rolling_stats trs_a
  ON trs_a.game_pk = g.game_pk AND trs_a.team_id = g.away_team_id

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
  AND g.season IN ({ph})
ORDER BY game_date_et, g.game_pk
"""


def load_closing_odds(con: sqlite3.Connection, game_pks: list[int]) -> pd.DataFrame:
    if not game_pks:
        return pd.DataFrame()

    ph = ",".join("?" * len(game_pks))
    sql = f"""
    WITH totals AS (
        SELECT game_pk, total_line, over_odds, under_odds, captured_at_utc,
               ROW_NUMBER() OVER (
                   PARTITION BY game_pk
                   ORDER BY is_closing_line DESC, captured_at_utc DESC, id DESC
               ) AS rn
        FROM game_odds
        WHERE game_pk IN ({ph})
          AND market_type = 'total'
          AND total_line IS NOT NULL
    ),
    runlines AS (
        SELECT game_pk, home_rl_line, home_rl_odds, away_rl_line, away_rl_odds,
               captured_at_utc AS rl_captured_at_utc,
               ROW_NUMBER() OVER (
                   PARTITION BY game_pk
                   ORDER BY is_closing_line DESC, captured_at_utc DESC, id DESC
               ) AS rn
        FROM game_odds
        WHERE game_pk IN ({ph})
          AND market_type = 'runline'
          AND home_rl_line IS NOT NULL
    ),
    moneylines AS (
        SELECT game_pk, home_ml, away_ml,
               ROW_NUMBER() OVER (
                   PARTITION BY game_pk
                   ORDER BY is_closing_line DESC, captured_at_utc DESC, id DESC
               ) AS rn
        FROM game_odds
        WHERE game_pk IN ({ph})
          AND market_type = 'moneyline'
          AND home_ml IS NOT NULL
          AND away_ml IS NOT NULL
    )
    SELECT
        g.game_pk,
        t.total_line,
        t.over_odds,
        t.under_odds,
        r.home_rl_line,
        r.home_rl_odds,
        r.away_rl_line,
        r.away_rl_odds,
        m.home_ml,
        m.away_ml
    FROM (SELECT DISTINCT game_pk FROM game_odds WHERE game_pk IN ({ph})) g
    LEFT JOIN totals t ON t.game_pk = g.game_pk AND t.rn = 1
    LEFT JOIN runlines r ON r.game_pk = g.game_pk AND r.rn = 1
    LEFT JOIN moneylines m ON m.game_pk = g.game_pk AND m.rn = 1
    """
    params = game_pks * 4
    return pd.read_sql_query(sql, con, params=params)


def parse_wind_speed(row: pd.Series) -> float | None:
    if pd.notna(row.get("wind_speed")):
        try:
            return float(row["wind_speed"])
        except (TypeError, ValueError):
            pass
    text = str(row.get("wind_direction") or "")
    if " mph" in text.lower():
        try:
            return float(text.lower().split("mph")[0].strip().split()[-1])
        except (IndexError, ValueError):
            return None
    return None


def wind_bucket(speed: float | None, direction: object) -> str:
    if speed is None or pd.isna(speed) or speed < 5:
        return "calm"
    d = str(direction or "").lower()
    if "out" in d:
        return "out"
    if "in" in d:
        return "in"
    return "cross"


def assign_favorite(row: pd.Series) -> pd.Series:
    home_ml = row.get("home_ml")
    away_ml = row.get("away_ml")
    if pd.isna(home_ml) or pd.isna(away_ml):
        return pd.Series({"favorite_team": None, "favorite_ml": None, "underdog_ml": None, "favorite_is_home": None})

    home_ml = int(home_ml)
    away_ml = int(away_ml)

    if home_ml < away_ml:
        fav_home = True
        fav_ml, dog_ml = home_ml, away_ml
    elif away_ml < home_ml:
        fav_home = False
        fav_ml, dog_ml = away_ml, home_ml
    else:
        fav_home = True
        fav_ml, dog_ml = home_ml, away_ml

    fav_team = row["home_team"] if fav_home else row["away_team"]
    return pd.Series({
        "favorite_team": fav_team,
        "favorite_ml": fav_ml,
        "underdog_ml": dog_ml,
        "favorite_is_home": fav_home,
    })


def engineer_features(df: pd.DataFrame, discovery: OddsDiscovery) -> pd.DataFrame:
    out = df.copy()
    out["combined_ops"] = out["h_ops"] + out["a_ops"]
    out["combined_era"] = out["hsp_era"] + out["asp_era"]
    out["combined_k9"] = out["hsp_k9"] + out["asp_k9"]
    out["combined_runs_pg"] = out["h_runs_pg"] + out["a_runs_pg"]

    out["parsed_wind_speed"] = out.apply(parse_wind_speed, axis=1)
    out["wind_bucket"] = out.apply(
        lambda r: wind_bucket(r["parsed_wind_speed"], r["wind_direction"]),
        axis=1,
    )

    if out["temperature"].notna().any():
        out["temp_bucket"] = pd.cut(
            out["temperature"],
            bins=[-100, 50, 60, 70, 80, 200],
            labels=["<50F", "50-60F", "60-70F", "70-80F", "80F+"],
            right=False,
        )
    else:
        out["temp_bucket"] = "unknown"

    if out["park_factor_runs"].notna().any():
        out["park_tier"] = pd.cut(
            out["park_factor_runs"],
            bins=[0, 95, 100, 105, 200],
            labels=["pitcher_park", "neutral", "hitter_park", "extreme"],
            right=False,
        )
    else:
        out["park_tier"] = "unknown"

    out["is_coors"] = out["venue_name"].fillna("").str.contains("Coors", case=False)

    fav = out.apply(assign_favorite, axis=1)
    out = pd.concat([out, fav], axis=1)

    out["over_8"] = (out["total_runs"] > 8).astype(int)
    out["over_75"] = (out["total_runs"] > 7.5).astype(int)

    if discovery.has_total_line and "total_line" in out.columns:
        out["push_ou"] = (out["total_runs"] == out["total_line"]).astype(int)
        out["over_result"] = np.where(
            out["total_line"].isna(),
            np.nan,
            np.where(
                out["total_runs"] > out["total_line"],
                1,
                np.where(out["total_runs"] < out["total_line"], 0, np.nan),
            ),
        )
    else:
        out["push_ou"] = 0
        out["over_result"] = np.nan

    def fav_cover_row(r: pd.Series) -> float | None:
        if pd.isna(r.get("favorite_is_home")):
            return np.nan
        if r["favorite_is_home"]:
            return float(r["run_diff"] >= 2)
        return float(r["run_diff"] <= -2)

    out["fav_cover"] = out.apply(fav_cover_row, axis=1)

    out["abs_cover"] = (out["run_diff"].abs() >= 2).astype(int)

    if discovery.has_rl_odds:
        out["favorite_rl_odds"] = np.where(
            out["favorite_is_home"].fillna(False),
            out["home_rl_odds"],
            out["away_rl_odds"],
        )
    else:
        out["favorite_rl_odds"] = np.nan

    out["era_bucket"] = pd.cut(
        out["combined_era"],
        bins=[0, 6, 8, 10, 100],
        labels=["<6.0", "6.0-8.0", "8.0-10.0", "10.0+"],
        right=False,
    )
    out["ops_bucket"] = pd.cut(
        out["combined_ops"],
        bins=[0, 1.30, 1.50, 1.70, 10],
        labels=["<1.30", "1.30-1.50", "1.50-1.70", "1.70+"],
        right=False,
    )

    return out


def ou_over_roi(over_pct: float, under_pct: float) -> float:
    return over_pct * (100.0 / 110.0) - under_pct * 1.0


def rl_roi(cover_pct: float, rl_odds: float | None) -> float:
    payout = american_payout(rl_odds) if rl_odds is not None and not pd.isna(rl_odds) else 100.0 / 130.0
    return cover_pct * payout - (1.0 - cover_pct)


def summarize_ou(group: pd.DataFrame, use_market: bool) -> dict:
    n = len(group)
    if n == 0:
        return {"n": 0, "over_pct": np.nan, "under_pct": np.nan, "push_pct": np.nan, "roi": np.nan}

    if use_market and group["over_result"].notna().any():
        valid = group[group["over_result"].notna()]
        n = len(valid)
        if n == 0:
            return {"n": 0, "over_pct": np.nan, "under_pct": np.nan, "push_pct": np.nan, "roi": np.nan}
        over_pct = float(valid["over_result"].mean())
        under_pct = 1.0 - over_pct
        push_pct = float(group["push_ou"].mean()) if "push_ou" in group else 0.0
    else:
        over_pct = float(group["over_8"].mean())
        under_pct = 1.0 - over_pct
        push_pct = 0.0

    return {
        "n": n,
        "over_pct": over_pct * 100.0,
        "under_pct": under_pct * 100.0,
        "push_pct": push_pct * 100.0,
        "roi": ou_over_roi(over_pct, under_pct) * 100.0,
    }


def summarize_rl(group: pd.DataFrame) -> dict:
    valid = group[group["fav_cover"].notna()]
    n = len(valid)
    if n == 0:
        return {"n": 0, "cover_pct": np.nan, "roi": np.nan}
    cover_pct = float(valid["fav_cover"].mean())
    avg_odds = valid["favorite_rl_odds"].dropna().median() if valid["favorite_rl_odds"].notna().any() else None
    return {
        "n": n,
        "cover_pct": cover_pct * 100.0,
        "roi": rl_roi(cover_pct, avg_odds) * 100.0,
    }


def format_ou_row(label: str, stats: dict) -> str:
    if stats["n"] == 0:
        return f"  {label:<28} N={0:4d}  (no data)"
    return (
        f"  {label:<28} N={stats['n']:4d}  "
        f"Over={stats['over_pct']:5.1f}%  Under={stats['under_pct']:5.1f}%  "
        f"Push={stats['push_pct']:4.1f}%  ROI={stats['roi']:+5.1f}%"
    )


def format_rl_row(label: str, stats: dict) -> str:
    if stats["n"] == 0:
        return f"  {label:<28} N={0:4d}  (no data)"
    return (
        f"  {label:<28} N={stats['n']:4d}  "
        f"Cover={stats['cover_pct']:5.1f}%  ROI={stats['roi']:+5.1f}%"
    )


def ou_section(df: pd.DataFrame, discovery: OddsDiscovery) -> list[str]:
    use_market = discovery.has_total_line and df["over_result"].notna().any()
    lines = [
        "",
        "OVER / UNDER ANALYSIS",
        "=" * 54,
    ]
    if not use_market:
        lines.append(
            "No market O/U line found — using outcome rates only (over 8 runs proxy)."
        )
    else:
        lines.append("Using closing total_line from game_odds (market_type='total').")

    lines.extend(["", "A) BASELINE"])
    for season in sorted(df["season"].unique()):
        stats = summarize_ou(df[df["season"] == season], use_market)
        lines.append(format_ou_row(str(season), stats))
    stats = summarize_ou(df, use_market)
    lines.append(format_ou_row("Combined", stats))

    df_month = df.copy()
    df_month["month"] = df_month["game_date_et"].astype(str).str.slice(0, 7)
    lines.append("  Monthly (combined seasons):")
    for month, grp in df_month.groupby("month", sort=True):
        lines.append(format_ou_row(f"  {month}", summarize_ou(grp, use_market)))

    lines.extend(["", "B) BY PARK TIER"])
    for tier in ["pitcher_park", "neutral", "hitter_park", "extreme", "unknown"]:
        grp = df[df["park_tier"].astype(str) == tier]
        if len(grp):
            lines.append(format_ou_row(tier, summarize_ou(grp, use_market)))
    coors = df[df["is_coors"]]
    lines.append(format_ou_row("Coors Field", summarize_ou(coors, use_market)))

    lines.extend(["", "C) BY TEMPERATURE"])
    for tb in ["<50F", "50-60F", "60-70F", "70-80F", "80F+", "unknown"]:
        grp = df[df["temp_bucket"].astype(str) == tb]
        if len(grp):
            lines.append(format_ou_row(tb, summarize_ou(grp, use_market)))

    lines.extend(["", "D) BY WIND BUCKET"])
    for wb in ["calm", "out", "in", "cross"]:
        grp = df[df["wind_bucket"] == wb]
        if len(grp):
            lines.append(format_ou_row(wb, summarize_ou(grp, use_market)))
    wind_out10 = df[(df["wind_bucket"] == "out") & (df["parsed_wind_speed"] >= 10)]
    wind_in10 = df[(df["wind_bucket"] == "in") & (df["parsed_wind_speed"] >= 10)]
    lines.append(format_ou_row("Wind out >=10mph", summarize_ou(wind_out10, use_market)))
    lines.append(format_ou_row("Wind in >=10mph", summarize_ou(wind_in10, use_market)))

    lines.extend(["", "E) BY COMBINED SP ERA"])
    for eb in ["<6.0", "6.0-8.0", "8.0-10.0", "10.0+"]:
        grp = df[df["era_bucket"].astype(str) == eb]
        if len(grp):
            lines.append(format_ou_row(eb, summarize_ou(grp, use_market)))

    lines.extend(["", "F) BY COMBINED TEAM OPS"])
    for ob in ["<1.30", "1.30-1.50", "1.50-1.70", "1.70+"]:
        grp = df[df["ops_bucket"].astype(str) == ob]
        if len(grp):
            lines.append(format_ou_row(ob, summarize_ou(grp, use_market)))

    lines.extend(["", "G) INTERACTION: Wind out + high OPS"])
    inter = df[(df["wind_bucket"] == "out") & (df["combined_ops"] >= 1.60)]
    lines.append(format_ou_row("wind out + ops>=1.60", summarize_ou(inter, use_market)))

    lines.extend(["", "H) INTERACTION: High ERA + hitter park"])
    inter2 = df[(df["combined_era"] >= 8.0) & (df["park_factor_runs"] >= 105)]
    lines.append(format_ou_row("era>=8 + pf>=105", summarize_ou(inter2, use_market)))

    return lines


def ml_tier(ml: float) -> str:
    ml = int(ml)
    if ml >= -120:
        return "-101 to -120"
    if ml >= -150:
        return "-121 to -150"
    if ml >= -200:
        return "-151 to -200"
    if ml >= -250:
        return "-201 to -250"
    if ml >= -300:
        return "-251 to -300"
    return "-301+"


def rl_section(df: pd.DataFrame, discovery: OddsDiscovery) -> list[str]:
    fav = df[df["favorite_ml"].notna()].copy()
    lines = [
        "",
        "RUN LINE (-1.5) ANALYSIS — MARKET FAVORITES",
        "=" * 54,
    ]
    if fav.empty:
        lines.append("No moneyline odds available for favorite identification.")
        return lines

    lines.extend(["", "A) BASELINE COVER RATE"])
    stats = summarize_rl(fav)
    lines.append(format_rl_row("Overall", stats))
    for season in sorted(fav["season"].unique()):
        lines.append(format_rl_row(str(season), summarize_rl(fav[fav["season"] == season])))
    fav_m = fav.copy()
    fav_m["month"] = fav_m["game_date_et"].astype(str).str.slice(0, 7)
    lines.append("  By month:")
    for month, grp in fav_m.groupby("month", sort=True):
        lines.append(format_rl_row(f"  {month}", summarize_rl(grp)))

    lines.extend(["", "B) BY FAVORITE ML ODDS TIER"])
    for tier in ["-101 to -120", "-121 to -150", "-151 to -200", "-201 to -250", "-251 to -300", "-301+"]:
        grp = fav[fav["favorite_ml"].apply(ml_tier) == tier]
        if len(grp):
            lines.append(format_rl_row(tier, summarize_rl(grp)))

    lines.extend(["", "C) BY WIN PROBABILITY MODEL (LogReg >=67%)"])
    if "model_confidence" in fav.columns and fav["model_confidence"].notna().any():
        hi = fav[fav["model_confidence"] >= 0.67]
        agree = hi[hi["model_agrees_fav"] == True]
        disagree = hi[hi["model_agrees_fav"] == False]
        lines.append(format_rl_row("Model conf>=67% (all)", summarize_rl(hi)))
        lines.append(format_rl_row("  agrees with favorite", summarize_rl(agree)))
        lines.append(format_rl_row("  disagrees with favorite", summarize_rl(disagree)))
    else:
        lines.append("  LogReg predictions not joined — skip model cover analysis.")

    lines.extend(["", "D) BY COMBINED SP ERA"])
    for eb in ["<6.0", "6.0-8.0", "8.0-10.0", "10.0+"]:
        grp = fav[fav["era_bucket"].astype(str) == eb]
        if len(grp):
            lines.append(format_rl_row(eb, summarize_rl(grp)))

    lines.extend(["", "E) INTERACTION: Strong fav + model conf>=67%"])
    inter = fav[(fav["favorite_ml"] <= -200) & (fav["model_confidence"].fillna(0) >= 0.67)]
    lines.append(format_rl_row("ML<=-200 & conf>=67%", summarize_rl(inter)))

    if not discovery.has_rl_odds:
        lines.append("")
        lines.append("Note: Run line odds columns present but join may be sparse; ROI uses -130 if missing.")

    return lines


def join_logreg(df: pd.DataFrame, logreg_path: Path) -> pd.DataFrame:
    if not logreg_path.is_file():
        return df
    preds = pd.read_csv(logreg_path)
    preds["game_pk"] = preds["game_pk"].astype(int)
    keep = preds[["game_pk", "home_win_prob", "predicted_winner"]].rename(
        columns={"home_win_prob": "home_win_prob", "predicted_winner": "model_pick"}
    )
    out = df.merge(keep, on="game_pk", how="left")
    if "home_win_prob" in out.columns:
        out["model_confidence"] = np.maximum(
            out["home_win_prob"].astype(float),
            1.0 - out["home_win_prob"].astype(float),
        )
        out["model_agrees_fav"] = out["model_pick"] == out["favorite_team"]
    return out


def build_ou_csv(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "game_pk", "game_date_et", "season", "home_team", "away_team",
        "total_runs", "total_line", "over_result", "over_8", "over_75",
        "wind_bucket", "temp_bucket", "park_tier", "venue_name",
        "combined_ops", "combined_era", "combined_k9", "combined_runs_pg",
        "is_coors",
    ]
    existing = [c for c in cols if c in df.columns]
    return df[existing].copy()


def build_rl_csv(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "game_pk", "game_date_et", "season", "home_team", "away_team",
        "favorite_team", "favorite_ml", "favorite_rl_odds", "run_diff",
        "fav_cover", "home_win_prob", "model_confidence", "model_agrees_fav",
    ]
    existing = [c for c in cols if c in df.columns]
    return df[existing].copy()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="O/U and run line backtest analysis.")
    p.add_argument("--db", default=get_db_path())
    p.add_argument("--seasons", type=int, nargs="+", default=DEFAULT_SEASONS)
    p.add_argument("--min-games", type=int, default=DEFAULT_MIN_GAMES)
    p.add_argument("--logreg", default=DEFAULT_LOGREG)
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    seasons = sorted(set(args.seasons))
    output_dir = resolve_path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    logreg_path = resolve_path(args.logreg)

    con = db_connect(args.db)
    con.row_factory = sqlite3.Row
    try:
        discovery, discovery_lines = discover_odds(con)

        games = pd.read_sql_query(build_games_query(seasons), con, params=seasons)
        print(f"[ou_rl_backtest] Loaded {len(games)} completed games")

        pre_n = len(games)
        games = games[
            (games["h_games_played"] >= args.min_games)
            & (games["a_games_played"] >= args.min_games)
        ].copy()
        print(
            f"[ou_rl_backtest] Early-season filter (>={args.min_games} GP): "
            f"dropped {pre_n - len(games)}, {len(games)} remaining"
        )

        odds = load_closing_odds(con, games["game_pk"].tolist())
        df = games.merge(odds, on="game_pk", how="left")
    finally:
        con.close()

    df = engineer_features(df, discovery)
    df = join_logreg(df, logreg_path)

    report_lines = discovery_lines + [
        f"Games analyzed: {len(df)}  (seasons {seasons}, min GP {args.min_games})",
        f"Games with total_line: {int(df['total_line'].notna().sum())}",
        f"Games with RL odds: {int(df['home_rl_odds'].notna().sum())}",
        f"Games with ML odds: {int(df['home_ml'].notna().sum())}",
    ]
    report_lines.extend(ou_section(df, discovery))
    report_lines.extend(rl_section(df, discovery))

    report = "\n".join(report_lines)
    report_path = output_dir / "ou_rl_backtest.txt"
    report_path.write_text(report + "\n", encoding="utf-8")

    build_ou_csv(df).to_csv(output_dir / "ou_backtest_detail.csv", index=False)
    build_rl_csv(df).to_csv(output_dir / "rl_backtest_detail.csv", index=False)

    sys.stdout.buffer.write((report + "\n\n").encode("utf-8", errors="replace"))
    sys.stdout.buffer.flush()
    print(f"[ou_rl_backtest] Report saved to {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
