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
ET = ZoneInfo("America/New_York")

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
    v.elevation_ft

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
    if not game_pks:
        return pd.DataFrame()

    placeholders = ",".join("?" * len(game_pks))
    sql = f"""
    WITH ranked AS (
        SELECT
            go.game_pk,
            go.home_ml,
            go.away_ml,
            go.captured_at_utc,
            go.bookmaker,
            ROW_NUMBER() OVER (
                PARTITION BY go.game_pk
                ORDER BY go.captured_at_utc DESC, go.id DESC
            ) AS rn
        FROM game_odds go
        WHERE go.game_pk IN ({placeholders})
          AND go.market_type = 'moneyline'
          AND go.home_ml IS NOT NULL
          AND go.away_ml IS NOT NULL
    )
    SELECT game_pk, home_ml, away_ml, captured_at_utc, bookmaker
    FROM ranked
    WHERE rn = 1
    """
    return pd.read_sql_query(sql, con, params=game_pks)


def compute_sp_data_missing(df: pd.DataFrame) -> pd.Series:
    h_missing = df["hsp_starts_in_window"].isna() | (df["hsp_starts_in_window"] == 0)
    a_missing = df["asp_starts_in_window"].isna() | (df["asp_starts_in_window"] == 0)
    return (h_missing | a_missing).astype(int)


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
    min_edge: float,
) -> pd.DataFrame:
    out = df.copy()
    out["rule_min_games"] = (
        (out["h_season_games_played"] >= min_games)
        & (out["a_season_games_played"] >= min_games)
    )
    out["rule_confidence"] = out["confidence"] >= confidence_threshold
    out["rule_edge"] = out["edge"] > min_edge
    out["has_odds"] = out["home_ml"].notna() & out["away_ml"].notna()
    out["actionable"] = (
        out["rule_min_games"]
        & out["rule_confidence"]
        & out["rule_edge"]
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
        elif not row["rule_edge"]:
            fails.append(f"edge<={min_edge:.0%}")
        reasons.append(",".join(fails) if fails else "SKIP")
    out["skip_reason"] = reasons
    return out


def build_report(
    scored: pd.DataFrame,
    *,
    score_date: str,
    min_games: int,
    confidence_threshold: float,
    min_edge: float,
    trained_on_season: int,
) -> str:
    actionable = scored[scored["actionable"]].sort_values(
        ["edge", "confidence"], ascending=[False, False]
    )
    skipped = scored[~scored["actionable"]]

    lines = [
        f"SCORE TODAY — {score_date}",
        "═" * 54,
        f"Model trained on:           {trained_on_season}",
        f"Decision rules:",
        f"  1. Both teams >={min_games} GP this season",
        f"  2. Model confidence >={confidence_threshold:.0%}",
        f"  3. Model edge > {min_edge:.0%} vs market (vig-removed)",
        "",
        f"Slated games loaded:        {len(scored)}",
        f"Actionable picks:           {len(actionable)}",
        f"Skipped / no bet:           {len(skipped)}",
        "",
    ]

    if actionable.empty:
        lines.append("No actionable picks today.")
    else:
        lines.extend([
            "RANKED PICKS (edge descending):",
            f"  {'#':>2}  {'matchup':<13} {'pick':<4} {'odds':>5}  "
            f"{'model%':>6} {'mkt%':>6} {'edge':>6} {'conf':>6}  ev",
            "  " + "-" * 62,
        ])
        for i, (_, row) in enumerate(actionable.iterrows(), start=1):
            matchup = f"{row['away_team']}@{row['home_team']}"
            lines.append(
                f"  {i:2d}  {matchup:<13} {row['predicted_winner']:<4} "
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
    ]
    existing = [c for c in cols if c in scored.columns]
    out = scored[existing].copy()
    if "actionable" in out.columns:
        out["actionable"] = out["actionable"].astype(int)
    return out


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
        help="Minimum model-minus-market edge to qualify (default: 0.0)",
    )
    parser.add_argument(
        "--min-games",
        type=int,
        default=None,
        help="Override min games played filter (default: from meta or 20)",
    )
    return parser.parse_args()


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

    print(f"[score_today] Model trained on: {meta.get('trained_on_season')}")
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
            return 0

        odds = load_current_odds(con, games["game_pk"].tolist())
        merged = games.merge(odds, on="game_pk", how="left")
    finally:
        con.close()

    scored = score_games(merged, pipeline, features, medians)
    scored = attach_odds_metrics(scored)
    scored = apply_decision_rules(
        scored,
        min_games=min_games,
        confidence_threshold=confidence_threshold,
        min_edge=args.min_edge,
    )

    report = build_report(
        scored,
        score_date=score_date,
        min_games=min_games,
        confidence_threshold=confidence_threshold,
        min_edge=args.min_edge,
        trained_on_season=int(meta.get("trained_on_season", 0)),
    )

    report_path = output_dir / f"score_today_{score_date}.txt"
    csv_path = output_dir / f"score_today_{score_date}.csv"
    report_path.write_text(report + "\n", encoding="utf-8")
    build_output_csv(scored).to_csv(csv_path, index=False)

    sys.stdout.buffer.write((report + "\n\n").encode("utf-8", errors="replace"))
    sys.stdout.buffer.flush()
    print(f"[score_today] Report saved to {report_path}")
    print(f"[score_today] CSV saved to {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
