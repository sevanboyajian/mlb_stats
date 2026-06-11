#!/usr/bin/env python3
"""
score_today.py
──────────────
Daily decision script: load saved LogReg artifact, score unplayed games,
join current moneyline odds, apply decision rules, output ranked picks.

Standalone — does not import outcome_model.py.

USAGE:
  python batch/analysis/prediction/score_today.py --db data/mlb_stats.db
  python batch/analysis/prediction/score_today.py --date 2026-05-24
  python batch/analysis/prediction/score_today.py --threshold 0.65 --min-edge 0.0
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import joblib
import numpy as np
import pandas as pd

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import connect as db_connect, get_db_path

DEFAULT_MODELS_DIR = "outputs/models"
DEFAULT_OUTPUT_DIR = "outputs/reports"
MIN_SP_STARTS = 3  # minimum starts for reliable SP ERA WMA (Under signal)
OWM_OPS_THRESHOLD = 0.80
OWM_ERA_THRESHOLD = 5.0
OWM_HOME_SP_ERA_MAX = 4.0  # Strong SP only — backtest confirmed 66.7% win rate

# Away Dog RL signal (Tier 1 — backtest confirmed 66.1% cover, n=1059)
AWAY_DOG_RL_ML_MIN = 101
AWAY_DOG_RL_ML_MAX = 130
AWAY_DOG_RL_TOTAL_MAX = 8.5
AWAY_DOG_RL_ML_NEXT_TIER_MIN = 131
AWAY_DOG_RL_ML_NEXT_TIER_MAX = 160
# Strictly better than -190: -190 passes, -191 blocks.
AWAY_DOG_RL_MAX_JUICE = -190
AWAY_DOG_RL_DAILY_CAP = 4

# Venues where Under signals are suppressed regardless of ERA.
# These parks structurally override pitcher quality for totals.
# Oracle Park: architecture neutralises wind (existing rule)
# Fenway Park: short left field wall, pull-heavy lineups —
#   3 blown Under signals 05-26 to 05-28 with ERA < 5.0
UNDER_SUPPRESSED_VENUES = {
    "Fenway Park",
    "Oracle Park",  # add explicit Under gate alongside wind logic
}

ET = ZoneInfo("America/New_York")

# Match generate_daily_brief.py: repo-root .env wins over config/.env for SMTP.
try:
    import os

    from dotenv import load_dotenv

    load_dotenv(_REPO_ROOT / "config" / ".env", override=False)
    load_dotenv(_REPO_ROOT / ".env", override=True)
    load_dotenv(override=False)
except ImportError:
    pass

_GAME_DATE_EXPR = "COALESCE(NULLIF(TRIM(g.game_date_et), ''), g.game_date)"

FILL_DEFAULTS: dict[str, float] = {
    "elevation_ft": 0.0,
    "park_factor_runs": 100.0,
}


def resolve_path(path: str) -> Path:
    p = Path(path)
    return p if p.is_absolute() else _REPO_ROOT / p


def today_et() -> str:
    return datetime.now(ET).date().isoformat()


def american_to_implied(ml: float) -> float:
    ml = float(ml)
    if ml < 0:
        return abs(ml) / (abs(ml) + 100.0)
    return 100.0 / (ml + 100.0)


def american_payout(ml: float) -> float:
    ml = float(ml)
    if ml < 0:
        return 100.0 / abs(ml)
    return ml / 100.0


def winner_confidence(home_prob: float) -> float:
    return max(float(home_prob), 1.0 - float(home_prob))


def passes_odds_tier(odds: float | None) -> bool:
    """Rule 3 — odds tier filter (backtest: -150/-199 and -300+ outperform)."""
    if odds is None or pd.isna(odds):
        return False
    odds = float(odds)
    if -199 <= odds <= -150:
        return True
    if odds <= -300:
        return True
    return False


def load_artifacts(models_dir: Path) -> tuple[object, dict]:
    meta_path = models_dir / "outcome_model_meta.json"
    pipeline_path = models_dir / "outcome_model_logreg.joblib"
    if not meta_path.is_file():
        raise FileNotFoundError(f"Missing model metadata: {meta_path}")
    if not pipeline_path.is_file():
        raise FileNotFoundError(f"Missing LogReg pipeline: {pipeline_path}")

    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    pipeline = joblib.load(pipeline_path)
    return pipeline, meta


def build_today_games_query() -> str:
    return f"""
SELECT
    g.game_pk,
    {_GAME_DATE_EXPR} AS game_date_et,
    g.season,
    g.home_team_id,
    g.away_team_id,
    g.game_start_utc,
    g.status,
    th.abbreviation AS home_team,
    ta.abbreviation AS away_team,
    g.venue_id,

    (SELECT COUNT(*)
     FROM games gp
     WHERE gp.season = g.season
       AND gp.game_type = 'R'
       AND gp.status = 'Final'
       AND gp.home_score IS NOT NULL
       AND gp.away_score IS NOT NULL
       AND COALESCE(NULLIF(TRIM(gp.game_date_et), ''), gp.game_date) < {_GAME_DATE_EXPR}
       AND (gp.home_team_id = g.home_team_id OR gp.away_team_id = g.home_team_id)
    ) AS h_season_games_played,

    (SELECT COUNT(*)
     FROM games gp
     WHERE gp.season = g.season
       AND gp.game_type = 'R'
       AND gp.status = 'Final'
       AND gp.home_score IS NOT NULL
       AND gp.away_score IS NOT NULL
       AND COALESCE(NULLIF(TRIM(gp.game_date_et), ''), gp.game_date) < {_GAME_DATE_EXPR}
       AND (gp.home_team_id = g.away_team_id OR gp.away_team_id = g.away_team_id)
    ) AS a_season_games_played,

    COALESCE(trs_hg.rolling_ops, trs_hl.rolling_ops)              AS h_rolling_ops,
    COALESCE(
        trs_hg.rolling_ops_wma, trs_hl.rolling_ops_wma,
        trs_hg.rolling_ops, trs_hl.rolling_ops
    )                                                              AS h_rolling_ops_wma,
    COALESCE(trs_hg.rolling_runs_scored_pg, trs_hl.rolling_runs_scored_pg) AS h_rolling_runs_scored_pg,
    COALESCE(trs_hg.rolling_runs_allowed_pg, trs_hl.rolling_runs_allowed_pg) AS h_rolling_runs_allowed_pg,
    COALESCE(trs_hg.rolling_run_diff_pg, trs_hl.rolling_run_diff_pg) AS h_rolling_run_diff_pg,
    COALESCE(trs_hg.rolling_sp_era, trs_hl.rolling_sp_era)        AS h_rolling_sp_era,
    COALESCE(trs_hg.rolling_obp, trs_hl.rolling_obp)              AS h_rolling_obp,
    COALESCE(trs_hg.rolling_slg, trs_hl.rolling_slg)              AS h_rolling_slg,
    COALESCE(trs_hg.rolling_iso, trs_hl.rolling_iso)              AS h_rolling_iso,
    COALESCE(trs_hg.rolling_k_pct, trs_hl.rolling_k_pct)          AS h_rolling_k_pct,
    COALESCE(trs_hg.rolling_bb_pct, trs_hl.rolling_bb_pct)        AS h_rolling_bb_pct,
    COALESCE(trs_hg.rolling_hr_pg, trs_hl.rolling_hr_pg)          AS h_rolling_hr_pg,
    COALESCE(trs_hg.rolling_ops_home, trs_hl.rolling_ops_home)    AS h_rolling_ops_home,

    COALESCE(trs_ag.rolling_ops, trs_al.rolling_ops)              AS a_rolling_ops,
    COALESCE(trs_ag.rolling_run_diff_pg, trs_al.rolling_run_diff_pg) AS a_rolling_run_diff_pg,
    COALESCE(trs_ag.rolling_k_pct, trs_al.rolling_k_pct)          AS a_rolling_k_pct,
    COALESCE(trs_ag.rolling_bb_pct, trs_al.rolling_bb_pct)        AS a_rolling_bb_pct,
    COALESCE(trs_ag.rolling_ops_road, trs_al.rolling_ops_road)    AS a_rolling_ops_road,

    COALESCE(prs_hg.era_wma, prs_hl.era_wma)                    AS hsp_era_wma,
    COALESCE(prs_hg.k_per_9_wma, prs_hl.k_per_9_wma)            AS hsp_k_per_9_wma,
    COALESCE(prs_hg.whip_wma, prs_hl.whip_wma)                  AS hsp_whip_wma,
    COALESCE(prs_hg.starts_in_window, prs_hl.starts_in_window)  AS hsp_starts_in_window,

    COALESCE(prs_ag.era_wma, prs_al.era_wma)                    AS asp_era_wma,
    COALESCE(prs_ag.k_per_9_wma, prs_al.k_per_9_wma)            AS asp_k_per_9_wma,
    COALESCE(prs_ag.whip_wma, prs_al.whip_wma)                  AS asp_whip_wma,
    COALESCE(prs_ag.starts_in_window, prs_al.starts_in_window)  AS asp_starts_in_window,

    hs.win_pct                 AS h_win_pct,
    hs.pythag_win_pct          AS h_pythag_win_pct,
    hs.run_diff                AS h_run_diff,

    ast.win_pct                AS a_win_pct,
    ast.pythag_win_pct         AS a_pythag_win_pct,
    ast.run_diff               AS a_run_diff,

    v.park_factor_runs,
    v.park_factor_hr,
    v.elevation_ft,
    v.name                       AS venue_name,

    g.wind_direction

FROM games g
JOIN teams th ON th.team_id = g.home_team_id
JOIN teams ta ON ta.team_id = g.away_team_id

LEFT JOIN team_rolling_stats trs_hg
  ON trs_hg.game_pk = g.game_pk AND trs_hg.team_id = g.home_team_id
LEFT JOIN team_rolling_stats trs_ag
  ON trs_ag.game_pk = g.game_pk AND trs_ag.team_id = g.away_team_id

LEFT JOIN team_rolling_stats trs_hl
  ON trs_hl.team_id = g.home_team_id
 AND trs_hl.season = g.season
 AND trs_hl.game_pk = (
     SELECT trs2.game_pk
     FROM team_rolling_stats trs2
     JOIN games g2 ON g2.game_pk = trs2.game_pk
     WHERE trs2.team_id = g.home_team_id
       AND trs2.season = g.season
       AND COALESCE(NULLIF(TRIM(g2.game_date_et), ''), g2.game_date) < {_GAME_DATE_EXPR}
     ORDER BY COALESCE(NULLIF(TRIM(g2.game_date_et), ''), g2.game_date) DESC,
              trs2.game_pk DESC
     LIMIT 1
 )
LEFT JOIN team_rolling_stats trs_al
  ON trs_al.team_id = g.away_team_id
 AND trs_al.season = g.season
 AND trs_al.game_pk = (
     SELECT trs2.game_pk
     FROM team_rolling_stats trs2
     JOIN games g2 ON g2.game_pk = trs2.game_pk
     WHERE trs2.team_id = g.away_team_id
       AND trs2.season = g.season
       AND COALESCE(NULLIF(TRIM(g2.game_date_et), ''), g2.game_date) < {_GAME_DATE_EXPR}
     ORDER BY COALESCE(NULLIF(TRIM(g2.game_date_et), ''), g2.game_date) DESC,
              trs2.game_pk DESC
     LIMIT 1
 )

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
       AND COALESCE(NULLIF(TRIM(g2.game_date_et), ''), g2.game_date) < {_GAME_DATE_EXPR}
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
       AND COALESCE(NULLIF(TRIM(g2.game_date_et), ''), g2.game_date) < {_GAME_DATE_EXPR}
     ORDER BY COALESCE(NULLIF(TRIM(g2.game_date_et), ''), g2.game_date) DESC,
              prs2.game_pk DESC
     LIMIT 1
 )

LEFT JOIN standings hs
  ON hs.team_id = g.home_team_id
 AND hs.season = g.season
 AND hs.snapshot_date = (
     SELECT MAX(s.snapshot_date)
     FROM standings s
     WHERE s.team_id = g.home_team_id
       AND s.season = g.season
       AND s.snapshot_date <= date({_GAME_DATE_EXPR}, '-1 day')
 )
LEFT JOIN standings ast
  ON ast.team_id = g.away_team_id
 AND ast.season = g.season
 AND ast.snapshot_date = (
     SELECT MAX(s.snapshot_date)
     FROM standings s
     WHERE s.team_id = g.away_team_id
       AND s.season = g.season
       AND s.snapshot_date <= date({_GAME_DATE_EXPR}, '-1 day')
 )

LEFT JOIN venues v ON v.venue_id = g.venue_id

WHERE g.game_type = 'R'
  AND {_GAME_DATE_EXPR} = ?
  AND (
        g.status IN ('Scheduled', 'Pre-Game')
        OR (g.game_start_utc IS NOT NULL AND g.game_start_utc > datetime('now'))
      )
  AND g.status NOT IN ('Final', 'In Progress', 'Cancelled', 'Postponed')
ORDER BY g.game_start_utc, g.game_pk
"""


def load_today_games(con: sqlite3.Connection, score_date: str) -> pd.DataFrame:
    sql = build_today_games_query()
    return pd.read_sql_query(sql, con, params=[score_date])


def load_current_odds(con: sqlite3.Connection, game_pks: list[int]) -> pd.DataFrame:
    """Latest closing ML, total, and run line odds per game."""
    if not game_pks:
        return pd.DataFrame()

    placeholders = ",".join("?" * len(game_pks))
    sql = f"""
    WITH moneylines AS (
        SELECT
            go.game_pk,
            go.home_ml,
            go.away_ml,
            go.captured_at_utc,
            go.bookmaker,
            ROW_NUMBER() OVER (
                PARTITION BY go.game_pk
                ORDER BY go.is_closing_line DESC,
                         go.captured_at_utc DESC,
                         go.id DESC
            ) AS rn
        FROM game_odds go
        WHERE go.game_pk IN ({placeholders})
          AND go.market_type = 'moneyline'
          AND go.home_ml IS NOT NULL
          AND go.away_ml IS NOT NULL
    ),
    totals AS (
        SELECT
            go.game_pk,
            go.total_line,
            go.over_odds,
            go.under_odds,
            ROW_NUMBER() OVER (
                PARTITION BY go.game_pk
                ORDER BY go.is_closing_line DESC,
                         go.captured_at_utc DESC,
                         go.id DESC
            ) AS rn
        FROM game_odds go
        WHERE go.game_pk IN ({placeholders})
          AND go.market_type = 'total'
          AND go.total_line IS NOT NULL
    ),
    runlines AS (
        SELECT
            go.game_pk,
            go.home_rl_line,
            go.home_rl_odds,
            go.away_rl_line,
            go.away_rl_odds,
            ROW_NUMBER() OVER (
                PARTITION BY go.game_pk
                ORDER BY go.is_closing_line DESC,
                         go.captured_at_utc DESC,
                         CASE WHEN go.home_rl_line = -1.5 THEN 0 ELSE 1 END,
                         go.id DESC
            ) AS rn
        FROM game_odds go
        WHERE go.game_pk IN ({placeholders})
          AND go.market_type = 'runline'
          AND go.home_rl_line IS NOT NULL
    )
    SELECT
        g.game_pk,
        m.home_ml,
        m.away_ml,
        m.captured_at_utc,
        m.bookmaker,
        t.total_line,
        t.over_odds,
        t.under_odds,
        r.home_rl_line,
        r.home_rl_odds,
        r.away_rl_line,
        r.away_rl_odds
    FROM (SELECT DISTINCT game_pk FROM game_odds WHERE game_pk IN ({placeholders})) g
    LEFT JOIN moneylines m ON m.game_pk = g.game_pk AND m.rn = 1
    LEFT JOIN totals t ON t.game_pk = g.game_pk AND t.rn = 1
    LEFT JOIN runlines r ON r.game_pk = g.game_pk AND r.rn = 1
    """
    return pd.read_sql_query(sql, con, params=game_pks * 4)


def compute_sp_data_missing(df: pd.DataFrame) -> pd.Series:
    h_missing = df["hsp_starts_in_window"].isna() | (df["hsp_starts_in_window"] == 0)
    a_missing = df["asp_starts_in_window"].isna() | (df["asp_starts_in_window"] == 0)
    return (h_missing | a_missing).astype(int)


def get_favorite_info(row: pd.Series) -> tuple[object, object, object, object]:
    hml = row.get("home_ml")
    aml = row.get("away_ml")
    try:
        hml = float(hml) if hml not in (None, "") else None
        aml = float(aml) if aml not in (None, "") else None
    except (TypeError, ValueError):
        return None, None, None, None
    if hml is None or aml is None:
        return None, None, None, None
    if hml < 0 and (aml >= 0 or hml <= aml):
        return row["home_team"], hml, "home", row.get("home_rl_odds")
    if aml < 0 and (hml >= 0 or aml < hml):
        return row["away_team"], aml, "away", row.get("away_rl_odds")
    return None, None, None, None


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["ops_diff"] = out["h_rolling_ops"] - out["a_rolling_ops"]
    out["sp_era_diff"] = out["asp_era_wma"] - out["hsp_era_wma"]
    out["sp_whip_diff"] = out["asp_whip_wma"] - out["hsp_whip_wma"]
    out["sp_k9_diff"] = out["hsp_k_per_9_wma"] - out["asp_k_per_9_wma"]
    out["win_pct_diff"] = out["h_win_pct"] - out["a_win_pct"]
    out["pythag_diff"] = out["h_pythag_win_pct"] - out["a_pythag_win_pct"]
    out["home_split_ops"] = out["h_rolling_ops_home"] - out["a_rolling_ops_road"]
    out["home_field"] = 1
    out["min_games_played"] = out[["h_season_games_played", "a_season_games_played"]].min(axis=1)
    return out


def compute_ou_rl_signals(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["combined_era"] = (
        out["hsp_era_wma"].fillna(999)
        + out["asp_era_wma"].fillna(999)
    )
    out["both_sp_known"] = (
        out["hsp_era_wma"].notna()
        & out["asp_era_wma"].notna()
        & (out["hsp_starts_in_window"].fillna(0) >= MIN_SP_STARTS)
        & (out["asp_starts_in_window"].fillna(0) >= MIN_SP_STARTS)
    )

    out["under_signal"] = (
        out["both_sp_known"]
        & (out["combined_era"] < 6.0)
    )

    if "wind_direction" in out.columns:
        out["wind_in"] = out["wind_direction"].astype(str).str.lower().str.contains("in", na=False)
    else:
        out["wind_in"] = False

    out["under_signal_strong"] = (
        out["both_sp_known"]
        & (out["combined_era"] < 5.0)
        & out["wind_in"]
    )

    if "venue_name" in out.columns:
        out["under_venue_suppressed"] = out["venue_name"].isin(UNDER_SUPPRESSED_VENUES)
    else:
        out["under_venue_suppressed"] = False

    out["under_signal"] = out["under_signal"] & ~out["under_venue_suppressed"]
    out["under_signal_strong"] = (
        out["under_signal_strong"] & ~out["under_venue_suppressed"]
    )

    fav_cols = out.apply(
        lambda r: pd.Series(
            get_favorite_info(r),
            index=["favorite_team", "favorite_ml", "favorite_side", "fav_rl_odds"],
        ),
        axis=1,
    )
    out = pd.concat([out, fav_cols], axis=1)

    out["rl_signal"] = out["favorite_ml"].notna() & (
        out["favorite_ml"].astype(float) <= -301
    )

    out = _apply_owm_signal(out)
    out = _apply_away_dog_rl_signal(out)
    out = _finalize_away_dog_rl_slate(out)

    return out


def _apply_owm_signal(df: pd.DataFrame) -> pd.DataFrame:
    """OWM — home hot offense vs struggling away SP, with strong home SP gate."""
    out = df.copy()

    home_ops = pd.to_numeric(out.get("h_rolling_ops_wma"), errors="coerce")
    away_era = pd.to_numeric(out.get("asp_era_wma"), errors="coerce")
    home_era = pd.to_numeric(out.get("hsp_era_wma"), errors="coerce")
    away_starts = pd.to_numeric(out.get("asp_starts_in_window"), errors="coerce").fillna(0)
    home_starts = pd.to_numeric(out.get("hsp_starts_in_window"), errors="coerce").fillna(0)

    away_sp_ok = away_era.notna() & (away_starts >= MIN_SP_STARTS)
    home_sp_ok = home_era.notna() & (home_starts >= MIN_SP_STARTS)
    ops_ok = home_ops.notna() & (home_ops >= OWM_OPS_THRESHOLD)
    away_era_ok = away_sp_ok & (away_era >= OWM_ERA_THRESHOLD)
    home_sp_strong = home_sp_ok & (home_era < OWM_HOME_SP_ERA_MAX)

    out["owm_signal"] = ops_ok & away_era_ok & home_sp_strong

    core_match = ops_ok & away_era_ok
    blocked = core_match & home_sp_ok & ~home_sp_strong
    block_reasons: list[str] = []
    for i in out.index:
        if not bool(blocked.loc[i]):
            block_reasons.append("")
            continue
        era_val = float(home_era.loc[i])
        block_reasons.append(
            f"OWM blocked — home SP ERA WMA {era_val:.2f} >= {OWM_HOME_SP_ERA_MAX:.1f} "
            f"(need Strong SP < {OWM_HOME_SP_ERA_MAX:.1f})"
        )
    out["owm_block_reason"] = block_reasons

    return out


def _apply_away_dog_rl_signal(df: pd.DataFrame) -> pd.DataFrame:
    """Standalone Away Dog +1.5 when away ML +101–+130 and total ≤ 8.5."""
    out = df.copy()
    away_ml = pd.to_numeric(out.get("away_ml"), errors="coerce")
    home_ml = pd.to_numeric(out.get("home_ml"), errors="coerce")
    total_line = pd.to_numeric(out.get("total_line"), errors="coerce")

    away_dog = away_ml.notna() & home_ml.notna() & (away_ml > home_ml)
    band_ok = away_dog & (away_ml >= AWAY_DOG_RL_ML_MIN) & (away_ml <= AWAY_DOG_RL_ML_MAX)
    total_ok = total_line.notna() & (total_line <= AWAY_DOG_RL_TOTAL_MAX)
    out["away_dog_rl_signal"] = band_ok & total_ok
    out["away_dog_rl_fires"] = out["away_dog_rl_signal"]

    block_reasons: list[str] = []
    for i in out.index:
        if bool(out.loc[i, "away_dog_rl_signal"]):
            block_reasons.append("")
            continue
        aml = away_ml.loc[i]
        hml = home_ml.loc[i]
        tot = total_line.loc[i]
        if pd.isna(aml) or pd.isna(hml):
            block_reasons.append("")
            continue
        if int(aml) <= int(hml):
            block_reasons.append("")
            continue
        if AWAY_DOG_RL_ML_MIN <= int(aml) <= AWAY_DOG_RL_ML_MAX:
            if pd.notna(tot) and float(tot) > AWAY_DOG_RL_TOTAL_MAX:
                block_reasons.append(
                    f"[Away Dog RL — total {float(tot):g} above "
                    f"{AWAY_DOG_RL_TOTAL_MAX:g} gate (need ≤ {AWAY_DOG_RL_TOTAL_MAX:g})]"
                )
            else:
                block_reasons.append("")
        elif AWAY_DOG_RL_ML_NEXT_TIER_MIN <= int(aml) <= AWAY_DOG_RL_ML_NEXT_TIER_MAX:
            block_reasons.append(
                f"[Away Dog RL — away ML +{int(aml)} outside +101–+130 band "
                f"(not yet implemented tier)]"
            )
        else:
            block_reasons.append("")
    out["away_dog_rl_block_reason"] = block_reasons
    return out


def _finalize_away_dog_rl_slate(df: pd.DataFrame) -> pd.DataFrame:
    """Juice gate per row, then daily cap (best RL juice first) across the slate."""
    out = df.copy()
    for col, default in (
        ("away_dog_rl_actionable", False),
        ("away_dog_rl_juice_blocked", False),
        ("away_dog_rl_cap_blocked", False),
        ("away_dog_rl_rank", pd.NA),
        ("away_dog_rl_stake", 0.0),
    ):
        if col not in out.columns:
            out[col] = default

    rl_odds = pd.to_numeric(out.get("away_rl_odds"), errors="coerce")
    fires = out["away_dog_rl_fires"].fillna(False).astype(bool)
    out.loc[fires, "away_dog_rl_juice_blocked"] = (
        fires & rl_odds.notna() & (rl_odds < AWAY_DOG_RL_MAX_JUICE)
    )
    for i in out.index[fires & out["away_dog_rl_juice_blocked"]]:
        o = int(rl_odds.loc[i])
        away = out.loc[i, "away_team"]
        home = out.loc[i, "home_team"]
        out.loc[i, "away_dog_rl_block_reason"] = (
            f"[Away Dog RL — {away}@{home} RL odds {o:+d} worse than -190 juice gate]"
        )
        out.loc[i, "away_dog_rl_actionable"] = False
        out.loc[i, "away_dog_rl_stake"] = 0.0

    eligible = out.index[
        fires & ~out["away_dog_rl_juice_blocked"].astype(bool)
    ].tolist()
    sort_key = out.loc[eligible, "away_rl_odds"].map(
        lambda x: float(x) if pd.notna(x) else -10_000.0
    )
    order = sorted(eligible, key=lambda i: float(sort_key.loc[i]), reverse=True)
    total_qual = len(order)

    for rank, idx in enumerate(order, start=1):
        out.at[idx, "away_dog_rl_rank"] = rank
        if rank <= AWAY_DOG_RL_DAILY_CAP:
            out.at[idx, "away_dog_rl_actionable"] = True
            out.at[idx, "away_dog_rl_stake"] = 0.10
            out.at[idx, "away_dog_rl_cap_blocked"] = False
        else:
            out.at[idx, "away_dog_rl_actionable"] = False
            out.at[idx, "away_dog_rl_stake"] = 0.0
            out.at[idx, "away_dog_rl_cap_blocked"] = True
            out.at[idx, "away_dog_rl_block_reason"] = (
                f"[Away Dog RL — daily cap reached "
                f"({AWAY_DOG_RL_DAILY_CAP}/{AWAY_DOG_RL_DAILY_CAP})]"
            )

    return out


def impute_features(df: pd.DataFrame, features: list[str], medians: dict[str, float]) -> pd.DataFrame:
    out = df.copy()
    for col in features:
        if col in {"sp_data_missing", "home_field"}:
            continue
        fill_val = medians.get(col, FILL_DEFAULTS.get(col, 0.0))
        if col in out.columns:
            out[col] = out[col].fillna(fill_val)
    return out


def score_games(
    df: pd.DataFrame,
    pipeline,
    features: list[str],
    medians: dict[str, float],
) -> pd.DataFrame:
    if df.empty:
        return df

    scored = df.copy()
    scored["sp_data_missing"] = compute_sp_data_missing(scored)
    scored = engineer_features(scored)
    scored = impute_features(scored, features, medians)

    x = scored[features].astype(float).values
    home_prob = pipeline.predict_proba(x)[:, 1]
    scored["home_win_prob"] = home_prob
    scored["away_win_prob"] = 1.0 - home_prob
    scored["predicted_winner"] = np.where(
        home_prob >= 0.5,
        scored["home_team"],
        scored["away_team"],
    )
    scored["confidence"] = np.maximum(home_prob, 1.0 - home_prob)
    return scored


def attach_odds_metrics(scored: pd.DataFrame) -> pd.DataFrame:
    out = scored.copy()
    if "home_ml" not in out.columns:
        out["home_ml"] = np.nan
        out["away_ml"] = np.nan

    raw_home = out["home_ml"].map(american_to_implied)
    raw_away = out["away_ml"].map(american_to_implied)
    vig = raw_home + raw_away
    out["implied_home"] = raw_home / vig
    out["implied_away"] = raw_away / vig

    pick_home = out["predicted_winner"] == out["home_team"]
    out["model_prob"] = np.where(pick_home, out["home_win_prob"], out["away_win_prob"])
    out["market_prob"] = np.where(pick_home, out["implied_home"], out["implied_away"])
    out["odds_used"] = np.where(pick_home, out["home_ml"], out["away_ml"])
    out["edge"] = out["model_prob"] - out["market_prob"]
    out["ev"] = out["model_prob"] * out["odds_used"].map(american_payout) - (1.0 - out["model_prob"])
    return out


def apply_decision_rules(
    df: pd.DataFrame,
    *,
    min_games: int,
    confidence_threshold: float,
) -> pd.DataFrame:
    out = df.copy()
    out["rule_min_games"] = (
        (out["h_season_games_played"] >= min_games)
        & (out["a_season_games_played"] >= min_games)
    )
    out["rule_confidence"] = out["confidence"] >= confidence_threshold
    out["rule_odds_tier"] = out["odds_used"].apply(passes_odds_tier)
    out["has_odds"] = out["home_ml"].notna() & out["away_ml"].notna()
    out["actionable"] = (
        out["rule_min_games"]
        & out["rule_confidence"]
        & out["rule_odds_tier"]
        & out["has_odds"]
    )

    reasons: list[str] = []
    for _, row in out.iterrows():
        if row["actionable"]:
            reasons.append("PASS")
            continue
        fails: list[str] = []
        if not row["rule_min_games"]:
            fails.append(f"GP<{min_games}")
        if not row["rule_confidence"]:
            fails.append(f"conf<{confidence_threshold:.0%}")
        if not row["has_odds"]:
            fails.append("no_odds")
        elif not row["rule_odds_tier"]:
            fails.append("odds_tier")
        reasons.append(",".join(fails) if fails else "SKIP")
    out["skip_reason"] = reasons
    return out


def _wind_label(row: pd.Series) -> str:
    if row.get("wind_in"):
        return "IN"
    direction = row.get("wind_direction")
    if pd.isna(direction) or direction is None:
        return "unknown"
    return str(direction)


def print_sp_starts_diagnostic(scored: pd.DataFrame) -> None:
    """Print SP starts-in-window table for Under signal debugging."""
    print(
        f"[score_today] SP starts diagnostic (min starts for Under: {MIN_SP_STARTS})"
    )
    print(
        f"  {'game':<12} {'h_era':>6} {'h_st':>5} {'a_era':>6} {'a_st':>5} "
        f"{'sp_ok':>5} {'under':>5}"
    )
    print(f"  {'-'*12} {'-'*6} {'-'*5} {'-'*6} {'-'*5} {'-'*5} {'-'*5}")
    for _, row in scored.sort_values("game_start_utc").iterrows():
        game = f"{row['away_team']}@{row['home_team']}"
        h_era = row.get("hsp_era_wma")
        a_era = row.get("asp_era_wma")
        h_st = row.get("hsp_starts_in_window")
        a_st = row.get("asp_starts_in_window")
        h_era_s = f"{float(h_era):.2f}" if pd.notna(h_era) else "n/a"
        a_era_s = f"{float(a_era):.2f}" if pd.notna(a_era) else "n/a"
        h_st_s = str(int(h_st)) if pd.notna(h_st) else "0"
        a_st_s = str(int(a_st)) if pd.notna(a_st) else "0"
        sp_ok = int(bool(row.get("both_sp_known")))
        under = int(bool(row.get("under_signal")))
        print(
            f"  {game:<12} {h_era_s:>6} {h_st_s:>5} {a_era_s:>6} {a_st_s:>5} "
            f"{sp_ok:>5} {under:>5}"
        )


def build_report(
    scored: pd.DataFrame,
    *,
    score_date: str,
    min_games: int,
    confidence_threshold: float,
    trained_on_season: int,
) -> str:
    actionable = scored[scored["actionable"]].sort_values(
        ["confidence", "edge"], ascending=[False, False]
    )
    skipped = scored[~scored["actionable"]]
    eligible = scored[scored["rule_min_games"]]
    under_hits = scored[scored["under_signal"]]
    rl_hits = scored[scored["rl_signal"]]
    owm_hits = scored[scored["owm_signal"]] if "owm_signal" in scored.columns else scored.iloc[0:0]
    away_dog_hits = (
        scored[scored["away_dog_rl_signal"]]
        if "away_dog_rl_signal" in scored.columns
        else scored.iloc[0:0]
    )
    ad_fired_n = int(away_dog_hits.shape[0]) if not away_dog_hits.empty else 0
    ad_staked_n = (
        int(away_dog_hits["away_dog_rl_actionable"].sum())
        if ad_fired_n and "away_dog_rl_actionable" in away_dog_hits.columns
        else 0
    )
    ad_juice_n = (
        int(away_dog_hits["away_dog_rl_juice_blocked"].sum())
        if ad_fired_n and "away_dog_rl_juice_blocked" in away_dog_hits.columns
        else 0
    )

    lines = [
        f"SCORE TODAY — {score_date}",
        "═" * 54,
        f"Model trained on:           {trained_on_season}",
        "Decision rules:",
        f"  1. Both teams >={min_games} GP this season",
        f"  2. Model confidence >={confidence_threshold:.0%}",
        "  3. Odds tier: -150 to -199 OR -300 or worse",
        "     (edge calc shown for context only — not a gate)",
        "",
        f"Slated games loaded:        {len(scored)}",
        f"Eligible (>={min_games} GP):       {len(eligible)}",
        f"── ML picks:                {len(actionable)}  "
        f"(>={confidence_threshold:.0%} conf, fav, tier -150/-199 or -300+)",
        f"── Under signals:           {len(under_hits)}  (combined SP ERA WMA < 6.0)",
        f"── Run line signals:        {len(rl_hits)}  (ML favorite <= -301)",
        f"── OWM signals:             {len(owm_hits)}  "
        f"(home OPS WMA >= {OWM_OPS_THRESHOLD}, away SP ERA >= {OWM_ERA_THRESHOLD}, "
        f"home SP ERA < {OWM_HOME_SP_ERA_MAX})",
        f"── Away Dog RL signals:     {ad_fired_n} fired → {ad_staked_n} staked "
        f"(juice blocked {ad_juice_n}, cap max {AWAY_DOG_RL_DAILY_CAP})",
        "",
    ]

    if actionable.empty:
        lines.append("No actionable ML picks today.")
    else:
        lines.extend([
            "ML PICKS (confidence descending):",
            f"  {'#':>2}  {'matchup':<13} {'pick':<4} {'odds':>5}  "
            f"{'model%':>6} {'mkt%':>6} {'edge':>6} {'conf':>6}  ev",
            "  " + "-" * 62,
        ])
        for i, (_, row) in enumerate(actionable.iterrows(), start=1):
            matchup = f"{row['away_team']}@{row['home_team']}"
            lines.append(
                f"  {i:2d}  ✅ GO  {matchup:<13} {row['predicted_winner']:<4} "
                f"{int(row['odds_used']):+5d}  "
                f"{row['model_prob']*100:5.1f}% {row['market_prob']*100:5.1f}% "
                f"{row['edge']*100:+5.1f}% {row['confidence']*100:5.1f}%  "
                f"{row['ev']:+.3f}u"
            )

    if not skipped.empty:
        lines.extend(["", "SKIPPED GAMES:"])
        for _, row in skipped.sort_values("game_start_utc").iterrows():
            matchup = f"{row['away_team']}@{row['home_team']}"
            conf = f"{row['confidence']*100:.1f}%" if pd.notna(row.get("confidence")) else "n/a"
            edge = (
                f"{row['edge']*100:+.1f}%"
                if pd.notna(row.get("edge")) and row.get("has_odds")
                else "n/a"
            )
            lines.append(
                f"  {matchup:<13} conf={conf:<6} edge={edge:<7} "
                f"hGP={int(row['h_season_games_played'])} "
                f"aGP={int(row['a_season_games_played'])}  "
                f"[{row['skip_reason']}]"
            )

    lines.extend([
        "",
        "── UNDER SIGNAL ──────────────────────────────────────────────",
        f"Both SP ERA WMA combined < 6.0  |  Min starts: {MIN_SP_STARTS}  |",
        "Backtest: 652 games  |  Under rate: 44.6%  |  ROI: +14.8% at -110",
        "Strong (combined <5.0 + wind in): Under rate 41.6%  |  ROI: +20.6%",
        "Suppressed venues: Fenway Park, Oracle Park",
        "──────────────────────────────────────────────────────────────",
    ])
    if under_hits.empty:
        lines.extend([
            "  No Under signal today.",
            "  (Fires when combined SP ERA WMA < 6.0 — typically 1-2x per week)",
        ])
    else:
        for _, row in under_hits.sort_values("combined_era").iterrows():
            matchup = f"{row['away_team']}@{row['home_team']}"
            sp_block = (
                f"      Home SP: {row['home_team']} {row['hsp_era_wma']:.2f} ERA  |  "
                f"Away SP: {row['away_team']} {row['asp_era_wma']:.2f} ERA\n"
                f"      Combined ERA: {row['combined_era']:.2f}  |  "
                f"Wind: {_wind_label(row)}"
            )
            if pd.isna(row.get("total_line")):
                lines.append(f"  ⚠ SIGNAL — LINE NOT YET POSTED  [{matchup}]")
                lines.append(sp_block)
            elif row.get("under_signal_strong"):
                under_odds = int(row["under_odds"]) if pd.notna(row.get("under_odds")) else "n/a"
                lines.append(
                    f"  ✅ GO — STRONG  [{matchup}]  →  UNDER {row['total_line']:.1f}\n"
                    f"{sp_block}\n"
                    f"      Under odds: {under_odds}  |  Line: {row['total_line']:.1f}"
                )
            else:
                under_odds = int(row["under_odds"]) if pd.notna(row.get("under_odds")) else "n/a"
                lines.append(
                    f"  ✅ GO  [{matchup}]  →  UNDER {row['total_line']:.1f}\n"
                    f"{sp_block}\n"
                    f"      Under odds: {under_odds}  |  Line: {row['total_line']:.1f}"
                )

    lines.extend([
        "",
        "── OWM SIGNAL ────────────────────────────────────────────────",
        f"Home OPS WMA >= {OWM_OPS_THRESHOLD}  |  Away SP ERA WMA >= {OWM_ERA_THRESHOLD}  |",
        f"Home SP ERA WMA < {OWM_HOME_SP_ERA_MAX} (Strong SP gate)  |  Min starts: {MIN_SP_STARTS}",
        "Backtest 2019-2025: Strong home SP 66.7% win / +7.6% ROI",
        "──────────────────────────────────────────────────────────────",
    ])
    if owm_hits.empty:
        lines.extend([
            "  No OWM signal today.",
            f"  (Blocked when home SP ERA WMA >= {OWM_HOME_SP_ERA_MAX})",
        ])
    else:
        for _, row in owm_hits.sort_values("h_rolling_ops_wma", ascending=False).iterrows():
            matchup = f"{row['away_team']}@{row['home_team']}"
            home_ml_raw = row.get("home_ml")
            if pd.notna(home_ml_raw):
                home_ml_s = f"{int(home_ml_raw):+d}"
            else:
                home_ml_s = "n/a"
            lines.append(
                f"  ✅ GO  [{matchup}]  →  {row['home_team']} ML {home_ml_s}\n"
                f"      Home offense OPS WMA: {row['h_rolling_ops_wma']:.3f}\n"
                f"      Away SP ERA WMA: {row['asp_era_wma']:.2f}\n"
                f"      DATA: home SP ERA WMA {row['hsp_era_wma']:.2f} "
                f"(gate < {OWM_HOME_SP_ERA_MAX:.1f} — Strong)"
            )

    blocked_owm = scored[
        scored.get("owm_block_reason", pd.Series("", index=scored.index)).astype(str).str.len() > 0
    ] if "owm_block_reason" in scored.columns else scored.iloc[0:0]
    if not blocked_owm.empty:
        lines.extend(["", "  OWM blocked (home SP gate):"])
        for _, row in blocked_owm.sort_values("game_start_utc").iterrows():
            matchup = f"{row['away_team']}@{row['home_team']}"
            lines.append(f"  [{matchup}]  {row['owm_block_reason']}")

    lines.extend([
        "",
        "── AWAY DOG RL SIGNAL ─────────────────────────────────────────",
        f"Away ML +{AWAY_DOG_RL_ML_MIN}–+{AWAY_DOG_RL_ML_MAX} (inclusive)  |  "
        f"Closing total ≤ {AWAY_DOG_RL_TOTAL_MAX}  |  Away team underdog",
        "Backtest May–Aug 2019–2025: 66.1% cover, +2.2% edge vs implied, n=1,059",
        f"Cap: {AWAY_DOG_RL_DAILY_CAP} per day  |  "
        f"Juice gate: RL odds must be -190 or better (-190 passes)",
        f"Today: {ad_staked_n} staked / {ad_fired_n} qualified / {ad_juice_n} juice-blocked",
        "──────────────────────────────────────────────────────────────",
    ])
    if away_dog_hits.empty:
        lines.extend([
            "  No Away Dog RL signal today.",
            f"  (Near-miss: away ML +{AWAY_DOG_RL_ML_MIN}–+{AWAY_DOG_RL_ML_MAX} but total > {AWAY_DOG_RL_TOTAL_MAX})",
        ])
    else:
        sort_col = "away_rl_odds"
        ad_sorted = away_dog_hits.sort_values(
            sort_col,
            ascending=False,
            na_position="last",
        )
        total_qual = int((~ad_sorted["away_dog_rl_juice_blocked"].fillna(False)).sum())
        for _, row in ad_sorted.iterrows():
            matchup = f"{row['away_team']}@{row['home_team']}"
            aml = int(row["away_ml"])
            tot = float(row["total_line"])
            if bool(row.get("away_dog_rl_juice_blocked")):
                continue
            rank = row.get("away_dog_rl_rank")
            try:
                rank_i = int(rank)
            except (TypeError, ValueError):
                rank_i = None
            rl_odds = row.get("away_rl_odds")
            rl_s = f"{int(rl_odds):+d}" if pd.notna(rl_odds) else "n/a"
            rank_lbl = (
                f"({rank_i}/{total_qual})"
                if rank_i is not None and total_qual
                else ""
            )
            if bool(row.get("away_dog_rl_actionable")):
                prefix = f"  ✅ GO  {rank_lbl}".strip()
                stake_l = "      STAKE: 0.10u ← PLAY THIS"
            else:
                prefix = f"  ⛔ NO BET {rank_lbl}".strip()
                stake_l = "      STAKE: 0.00u — NO BET (daily cap reached)"
            lines.append(
                f"{prefix}  [{matchup}]  →  {row['away_team']} +1.5\n"
                f"      Away ML: +{aml}  |  Total: {tot:g}  |  RL odds: {rl_s}\n"
                f"      SIGNAL: Away Dog RL (standalone)\n"
                f"      DATA: away ML +{aml} (band +101–+130)\n"
                f"      DATA: total line {tot:g} (gate ≤ {AWAY_DOG_RL_TOTAL_MAX:g})\n"
                f"      DATA: away RL odds {rl_s}\n"
                f"      DATA: backtest cover rate 66.1% (n=1,059, May–Aug 2019–2025)\n"
                f"{stake_l}"
            )

    juice_blocked = (
        away_dog_hits[away_dog_hits["away_dog_rl_juice_blocked"].fillna(False)]
        if ad_fired_n and "away_dog_rl_juice_blocked" in away_dog_hits.columns
        else away_dog_hits.iloc[0:0]
    )
    if not juice_blocked.empty:
        lines.extend(["", "  Away Dog RL juice-blocked:"])
        for _, row in juice_blocked.sort_values("game_start_utc").iterrows():
            matchup = f"{row['away_team']}@{row['home_team']}"
            reason = (row.get("away_dog_rl_block_reason") or "").strip()
            lines.append(f"  [{matchup}]  {reason}")

    near_miss = scored[
        scored.get("away_dog_rl_block_reason", pd.Series("", index=scored.index))
        .astype(str)
        .str.len()
        > 0
    ] if "away_dog_rl_block_reason" in scored.columns else scored.iloc[0:0]
    near_miss = near_miss[~near_miss.index.isin(away_dog_hits.index)]
    if not near_miss.empty:
        lines.extend(["", "  Away Dog RL near-miss:"])
        for _, row in near_miss.sort_values("game_start_utc").iterrows():
            matchup = f"{row['away_team']}@{row['home_team']}"
            lines.append(f"  [{matchup}]  {row['away_dog_rl_block_reason']}")

    lines.extend([
        "",
        "── RUN LINE SIGNAL ───────────────────────────────────────────",
        "ML Favorite <= -301  |  Backtest: 57 games 2024-2025  |",
        "Cover rate: 63.2%  |  ROI: +21.1% at avg RL odds -116",
        "Note: 2026 YTD only 3 games — treat with caution",
        "──────────────────────────────────────────────────────────────",
    ])
    if rl_hits.empty:
        lines.extend([
            "  No Run Line signal today.",
            "  (Fires when ML favorite is -301 or worse)",
        ])
    else:
        for _, row in rl_hits.sort_values("favorite_ml").iterrows():
            matchup = f"{row['away_team']}@{row['home_team']}"
            if pd.isna(row.get("fav_rl_odds")):
                lines.append(
                    f"  ✅ GO  [{matchup}]  →  {row['favorite_team']} -1.5\n"
                    f"      Favorite ML: {int(row['favorite_ml']):+d}  |  "
                    f"RL odds: n/a\n"
                    f"      ⚠ RL odds not yet posted — check DraftKings before first pitch"
                )
            else:
                lines.append(
                    f"  ✅ GO  [{matchup}]  →  {row['favorite_team']} -1.5\n"
                    f"      Favorite ML: {int(row['favorite_ml']):+d}  |  "
                    f"RL odds: {int(row['fav_rl_odds']):+d}"
                )

    return "\n".join(lines)


def build_output_csv(scored: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "game_pk",
        "game_date_et",
        "game_start_utc",
        "season",
        "home_team",
        "away_team",
        "predicted_winner",
        "home_win_prob",
        "away_win_prob",
        "confidence",
        "h_season_games_played",
        "a_season_games_played",
        "min_games_played",
        "sp_data_missing",
        "home_ml",
        "away_ml",
        "odds_used",
        "model_prob",
        "market_prob",
        "edge",
        "ev",
        "actionable",
        "skip_reason",
        "bookmaker",
        "captured_at_utc",
        "hsp_era_wma",
        "asp_era_wma",
        "h_rolling_ops_wma",
        "hsp_starts_in_window",
        "asp_starts_in_window",
        "combined_era",
        "both_sp_known",
        "venue_name",
        "under_venue_suppressed",
        "under_signal",
        "under_signal_strong",
        "total_line",
        "over_odds",
        "under_odds",
        "favorite_team",
        "favorite_ml",
        "favorite_side",
        "fav_rl_odds",
        "home_rl_odds",
        "away_rl_odds",
        "rl_signal",
        "owm_signal",
        "owm_block_reason",
        "away_dog_rl_signal",
        "away_dog_rl_fires",
        "away_dog_rl_actionable",
        "away_dog_rl_rank",
        "away_dog_rl_juice_blocked",
        "away_dog_rl_cap_blocked",
        "away_dog_rl_stake",
        "away_dog_rl_block_reason",
        "wind_direction",
    ]
    existing = [c for c in cols if c in scored.columns]
    out = scored[existing].copy()
    if "actionable" in out.columns:
        out["actionable"] = out["actionable"].astype(int)
    for flag_col in (
        "under_signal",
        "under_signal_strong",
        "rl_signal",
        "owm_signal",
        "away_dog_rl_signal",
        "both_sp_known",
        "under_venue_suppressed",
    ):
        if flag_col in out.columns:
            out[flag_col] = out[flag_col].astype(int)
    return out


def print_odds_tier_debug(scored: pd.DataFrame) -> None:
    """Print per-game odds tier inputs (use with --debug)."""
    print("[score_today] Odds tier debug (odds_used = predicted winner ML):")
    print(
        f"  {'game':<12} {'pick':<4} {'home_ml':>7} {'away_ml':>7} "
        f"{'odds_used':>8} {'tier_ok':>7} {'skip'}"
    )
    for _, row in scored.sort_values("game_start_utc").iterrows():
        game = f"{row['away_team']}@{row['home_team']}"
        pick = row.get("predicted_winner", "?")
        ou = row.get("odds_used")
        tier_ok = passes_odds_tier(ou)
        hm = row.get("home_ml")
        am = row.get("away_ml")
        hm_s = f"{int(hm):+d}" if pd.notna(hm) else "n/a"
        am_s = f"{int(am):+d}" if pd.notna(am) else "n/a"
        ou_s = f"{int(ou):+d}" if pd.notna(ou) else "n/a"
        skip = row.get("skip_reason", "")
        print(
            f"  {game:<12} {pick:<4} {hm_s:>7} {am_s:>7} {ou_s:>8} "
            f"{int(bool(tier_ok)):>7} {skip}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Score today's unplayed games and output ranked ML picks."
    )
    parser.add_argument("--db", default=get_db_path(), help="SQLite database path")
    parser.add_argument(
        "--date",
        default=None,
        help="Score date in ET (YYYY-MM-DD). Default: today ET.",
    )
    parser.add_argument(
        "--models-dir",
        default=DEFAULT_MODELS_DIR,
        help="Directory containing outcome_model_*.joblib and meta.json",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for score_today report and CSV",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=None,
        help="Confidence threshold (default: from meta or 0.65)",
    )
    parser.add_argument(
        "--min-edge",
        type=float,
        default=0.0,
        help="Deprecated — edge is display-only; not used as a decision gate.",
    )
    parser.add_argument(
        "--min-games",
        type=int,
        default=None,
        help="Override min games played filter (default: from meta or 20)",
    )
    parser.add_argument(
        "--no-email",
        action="store_true",
        help="Skip email delivery (still saves formatted report locally)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print odds tier diagnostic table per game",
    )
    parser.add_argument(
        "--verify-ledger",
        action="store_true",
        help="After run, print bet_ledger rows for --date (dedupe diagnostic)",
    )
    return parser.parse_args()


def _resolve_score_today_recipients() -> list[str]:
    """
    Recipients for score_today / prediction_engine emails.

    Priority: SCORE_TODAY_EMAIL_TO → DB score_today subscription → group_brief
    → BRIEF_EMAIL_TO / SMTP_TO (same order as run_pipeline._resolve_score_today_recipients).
    """
    import os

    explicit = (os.getenv("SCORE_TODAY_EMAIL_TO") or "").strip()
    if explicit:
        return [p.strip() for p in explicit.replace(";", ",").split(",") if p.strip()]
    try:
        from delivery.recipient_resolver import get_recipients

        rec = get_recipients("score_today") or get_recipients("group_brief")
        if rec:
            return rec
    except ImportError:
        pass
    fallback = (os.getenv("BRIEF_EMAIL_TO") or os.getenv("SMTP_TO") or "").strip()
    if fallback:
        return [p.strip() for p in fallback.replace(";", ",").split(",") if p.strip()]
    return []


def _maybe_email_prediction_report(
    *,
    prediction_engine_path: Path,
    score_today_path: Path,
    csv_path: Path,
    subject: str,
    body: str,
    score_date: str,
) -> tuple[bool, str]:
    """Email prediction engine report on creation; attach score_today txt + csv."""
    try:
        from delivery.email_sender import send_report_email

        recipients = _resolve_score_today_recipients()
        if not recipients:
            return False, (
                "no recipients (SCORE_TODAY_EMAIL_TO / score_today / "
                "group_brief / BRIEF_EMAIL_TO)"
            )
        print(f"[score_today] Email recipients={recipients}")

        extra: list[Path] = []
        if score_today_path.is_file():
            extra.append(score_today_path)
        if csv_path.is_file():
            extra.append(csv_path)

        ok, msg = send_report_email(
            str(prediction_engine_path) if prediction_engine_path.is_file() else None,
            subject,
            recipients,
            body=body,
            extra_attachment_paths=extra,
        )
        return ok, msg
    except Exception as exc:
        return False, str(exc)


def main() -> int:
    args = parse_args()
    score_date = args.date or today_et()
    models_dir = resolve_path(args.models_dir)
    output_dir = resolve_path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    pipeline, meta = load_artifacts(models_dir)
    features = meta["feature_list"]
    min_games = args.min_games if args.min_games is not None else int(meta.get("min_games_filter", 20))
    confidence_threshold = (
        args.threshold
        if args.threshold is not None
        else float(meta.get("confidence_threshold", 0.65))
    )
    medians = meta.get("feature_medians", {})

    train_label = meta.get("trained_on_seasons") or [meta.get("trained_on_season")]
    print(f"[score_today] Model trained on: {train_label}")
    print(f"[score_today] Features: {len(features)}")
    print(f"[score_today] Min games filter: {min_games}")
    print(f"[score_today] Confidence threshold: {confidence_threshold:.0%}")
    print(f"[score_today] Scoring date: {score_date}")
    if not medians:
        print("[score_today] WARNING: feature_medians missing from meta — using fill defaults")

    con = db_connect(args.db)
    con.row_factory = sqlite3.Row
    try:
        games = load_today_games(con, score_date)
        if games.empty:
            print(f"[score_today] No unplayed games found for {score_date}")
            if args.verify_ledger:
                from batch.analysis.prediction.bet_ledger_writes import (
                    verify_bet_ledger_for_date,
                )

                verify_bet_ledger_for_date(con, score_date)
            return 0

        odds = load_current_odds(con, games["game_pk"].tolist())
        merged = games.merge(odds, on="game_pk", how="left")
    finally:
        con.close()

    scored = score_games(merged, pipeline, features, medians)
    scored = attach_odds_metrics(scored)
    scored = compute_ou_rl_signals(scored)
    print_sp_starts_diagnostic(scored)
    scored = apply_decision_rules(
        scored,
        min_games=min_games,
        confidence_threshold=confidence_threshold,
    )
    if args.debug:
        print_odds_tier_debug(scored)

    try:
        from batch.analysis.prediction.bet_ledger_writes import (
            collect_score_today_picks,
            verify_bet_ledger_for_date,
            write_picks_to_bet_ledger,
        )

        con_ledger = db_connect(args.db)
        try:
            picks = collect_score_today_picks(scored, score_date)
            stats = write_picks_to_bet_ledger(
                con_ledger, picks, score_date=score_date,
            )
            err_note = f", {stats['errors']} error(s)" if stats.get("errors") else ""
            print(
                f"[score_today] bet_ledger: {stats['written']} picks written, "
                f"{stats['skipped']} skipped (already in ledger){err_note}"
            )
            if args.verify_ledger:
                verify_bet_ledger_for_date(con_ledger, score_date)
        finally:
            con_ledger.close()
    except Exception as exc:
        print(f"[score_today] bet_ledger write failed (non-fatal): {exc}")

    report = build_report(
        scored,
        score_date=score_date,
        min_games=min_games,
        confidence_threshold=confidence_threshold,
        trained_on_season=int(meta.get("trained_on_season", 0)),
    )

    report_path = output_dir / f"score_today_{score_date}.txt"
    csv_path = output_dir / f"score_today_{score_date}.csv"
    report_path.write_text(report + "\n", encoding="utf-8")
    build_output_csv(scored).to_csv(csv_path, index=False)

    from batch.analysis.prediction.format_report import format_prediction_report

    trained_on = int(meta.get("trained_on_season", 2024))
    subject, formatted_body = format_prediction_report(
        csv_path,
        score_date,
        trained_on_season=trained_on,
        min_games=min_games,
    )
    formatted_path = output_dir / f"prediction_engine_{score_date}.txt"
    formatted_path.write_text(formatted_body + "\n", encoding="utf-8")

    sys.stdout.buffer.write((formatted_body + "\n\n").encode("utf-8", errors="replace"))
    sys.stdout.buffer.flush()
    print(f"[score_today] Report saved to {report_path}")
    print(f"[score_today] Prediction engine report saved to {formatted_path}")
    print(f"[score_today] CSV saved to {csv_path}")

    if not args.no_email:
        try:
            ok, msg = _maybe_email_prediction_report(
                prediction_engine_path=formatted_path,
                score_today_path=report_path,
                csv_path=csv_path,
                subject=subject,
                body=formatted_body,
                score_date=score_date,
            )
            if ok:
                print(f"[score_today] Report emailed: {subject}")
                print(f"[score_today] Email sent: {msg}")
            else:
                print(f"[score_today] Email failed (non-fatal): {msg}")
                print("[score_today] Report saved locally - check outputs/")
        except Exception as exc:
            print(f"[score_today] Email failed (non-fatal): {exc}")
            print("[score_today] Report saved locally - check outputs/")
    else:
        print("[score_today] Email skipped (--no-email)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
