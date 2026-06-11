#!/usr/bin/env python3
"""
outcome_model.py
────────────────
Pure outcome prediction model: home team win probability from pre-game
matchup features only (no odds / market lines).

Train: 2024+2025 regular season (combined)  |  Test: 2026 YTD

USAGE:
  python batch/analysis/prediction/outcome_model.py --db data/mlb_stats.db
  python batch/analysis/prediction/outcome_model.py --seasons 2024 2025 2026
  python batch/analysis/prediction/outcome_model.py --seasons 2024 2025 --min-games 20
  python batch/analysis/prediction/outcome_model.py --output-dir outputs/reports
  python batch/analysis/prediction/outcome_model.py --min-games 20
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import connect as db_connect, get_db_path

DEFAULT_SEASONS = [2024, 2025, 2026]
TRAIN_SEASONS = [2024, 2025]
TEST_SEASONS = [2026]
TRAIN_SEASON = TRAIN_SEASONS[0]  # legacy alias for score_today display

FEATURES = [
    "ops_diff",
    "sp_whip_diff",
    "sp_k9_diff",
    "sp_era_diff",
    "home_split_ops",
    "park_factor_runs",
    "elevation_ft",
    "home_field",
    "h_rolling_k_pct",
    "a_rolling_k_pct",
    "h_rolling_bb_pct",
    "a_rolling_bb_pct",
    "sp_data_missing",
    "pythag_diff",
    "win_pct_diff",
    "min_games_played",
]

SP_COLS = [
    "hsp_era_wma",
    "hsp_k_per_9_wma",
    "hsp_whip_wma",
    "hsp_starts_in_window",
    "asp_era_wma",
    "asp_k_per_9_wma",
    "asp_whip_wma",
    "asp_starts_in_window",
]

MODELS = {
    "LogReg_C10": {
        "slug": "logreg",
        "estimator": LogisticRegression(
            C=10.0,
            class_weight="balanced",
            max_iter=1000,
            solver="lbfgs",
        ),
    },
    "GradBoost": {
        "slug": "gradboost",
        "estimator": CalibratedClassifierCV(
            GradientBoostingClassifier(
                n_estimators=300,
                max_depth=3,
                learning_rate=0.05,
                subsample=0.8,
                min_samples_leaf=20,
                random_state=42,
            ),
            method="isotonic",
            cv=5,
        ),
    },
}

_GAME_DATE_EXPR = "COALESCE(NULLIF(TRIM(g.game_date_et), ''), g.game_date)"


def build_feature_query(seasons: list[int]) -> str:
    placeholders = ",".join("?" * len(seasons))
    return f"""
SELECT
    g.game_pk,
    COALESCE(NULLIF(TRIM(g.game_date_et), ''), g.game_date) AS game_date_et,
    g.season,
    g.home_team_id,
    g.away_team_id,
    g.venue_id,
    CASE WHEN g.home_score > g.away_score THEN 1 ELSE 0 END AS home_win,
    th.abbreviation AS home_team,
    ta.abbreviation AS away_team,

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

    h.rolling_ops              AS h_rolling_ops,
    h.rolling_runs_scored_pg   AS h_rolling_runs_scored_pg,
    h.rolling_runs_allowed_pg  AS h_rolling_runs_allowed_pg,
    h.rolling_run_diff_pg      AS h_rolling_run_diff_pg,
    h.rolling_sp_era           AS h_rolling_sp_era,
    h.rolling_sp_whip          AS h_rolling_sp_whip,
    h.rolling_sp_k9            AS h_rolling_sp_k9,
    h.rolling_obp              AS h_rolling_obp,
    h.rolling_slg              AS h_rolling_slg,
    h.rolling_iso              AS h_rolling_iso,
    h.rolling_k_pct            AS h_rolling_k_pct,
    h.rolling_bb_pct           AS h_rolling_bb_pct,
    h.rolling_hr_pg            AS h_rolling_hr_pg,
    h.rolling_ops_home         AS h_rolling_ops_home,

    a.rolling_ops              AS a_rolling_ops,
    a.rolling_runs_scored_pg   AS a_rolling_runs_scored_pg,
    a.rolling_runs_allowed_pg  AS a_rolling_runs_allowed_pg,
    a.rolling_run_diff_pg      AS a_rolling_run_diff_pg,
    a.rolling_sp_era           AS a_rolling_sp_era,
    a.rolling_sp_whip          AS a_rolling_sp_whip,
    a.rolling_sp_k9            AS a_rolling_sp_k9,
    a.rolling_obp              AS a_rolling_obp,
    a.rolling_slg              AS a_rolling_slg,
    a.rolling_iso              AS a_rolling_iso,
    a.rolling_k_pct            AS a_rolling_k_pct,
    a.rolling_bb_pct           AS a_rolling_bb_pct,
    a.rolling_hr_pg            AS a_rolling_hr_pg,
    a.rolling_ops_road         AS a_rolling_ops_road,

    hsp.era_wma                AS hsp_era_wma,
    hsp.k_per_9_wma            AS hsp_k_per_9_wma,
    hsp.whip_wma               AS hsp_whip_wma,
    hsp.starts_in_window       AS hsp_starts_in_window,

    asp.era_wma                AS asp_era_wma,
    asp.k_per_9_wma            AS asp_k_per_9_wma,
    asp.whip_wma               AS asp_whip_wma,
    asp.starts_in_window       AS asp_starts_in_window,

    hs.win_pct                 AS h_win_pct,
    hs.pythag_win_pct          AS h_pythag_win_pct,
    hs.run_diff                AS h_run_diff,
    hs.streak_length           AS h_streak_length,
    hs.streak_type             AS h_streak_type,
    hs.home_wins               AS h_home_wins,
    hs.home_losses             AS h_home_losses,

    ast.win_pct                AS a_win_pct,
    ast.pythag_win_pct         AS a_pythag_win_pct,
    ast.run_diff               AS a_run_diff,
    ast.streak_length          AS a_streak_length,
    ast.streak_type            AS a_streak_type,
    ast.away_wins              AS a_away_wins,
    ast.away_losses            AS a_away_losses,

    v.park_factor_runs,
    v.park_factor_hr,
    v.elevation_ft

FROM games g
JOIN teams th ON th.team_id = g.home_team_id
JOIN teams ta ON ta.team_id = g.away_team_id

JOIN team_rolling_stats h
  ON h.game_pk = g.game_pk AND h.team_id = g.home_team_id
JOIN team_rolling_stats a
  ON a.game_pk = g.game_pk AND a.team_id = g.away_team_id

JOIN game_probable_pitchers gpp_h
  ON gpp_h.game_pk = g.game_pk AND gpp_h.team_id = g.home_team_id
JOIN pitcher_rolling_stats hsp
  ON hsp.game_pk = g.game_pk AND hsp.player_id = gpp_h.player_id

JOIN game_probable_pitchers gpp_a
  ON gpp_a.game_pk = g.game_pk AND gpp_a.team_id = g.away_team_id
JOIN pitcher_rolling_stats asp
  ON asp.game_pk = g.game_pk AND asp.player_id = gpp_a.player_id

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
  AND g.status = 'Final'
  AND g.home_score IS NOT NULL
  AND g.away_score IS NOT NULL
  AND g.season IN ({placeholders})
ORDER BY game_date_et, g.game_pk
"""


def load_games(con: sqlite3.Connection, seasons: list[int]) -> pd.DataFrame:
    sql = build_feature_query(seasons)
    return pd.read_sql_query(sql, con, params=seasons)


def apply_early_season_filter(df: pd.DataFrame, min_games: int) -> tuple[pd.DataFrame, dict[str, int]]:
    stats = {
        "loaded_n": len(df),
        "dropped_n": 0,
        "retained_n": len(df),
    }
    if min_games <= 0:
        return df.copy(), stats

    pre_filter_n = len(df)
    filtered = df[
        (df["h_season_games_played"] >= min_games)
        & (df["a_season_games_played"] >= min_games)
    ].copy()
    post_filter_n = len(filtered)
    stats["dropped_n"] = pre_filter_n - post_filter_n
    stats["retained_n"] = post_filter_n

    print(
        f"[outcome_model] Early-season filter (min {min_games} GP): "
        f"dropped {stats['dropped_n']} games, {post_filter_n} remaining"
    )
    return filtered, stats


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


FILL_DEFAULTS: dict[str, float] = {
    "elevation_ft": 0.0,
    "park_factor_runs": 100.0,
}


def impute_train_medians(
    train: pd.DataFrame,
    test: pd.DataFrame,
    columns: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    impute_cols = [c for c in columns if c not in {"sp_data_missing", "min_games_played"}]
    medians = train[impute_cols].median(numeric_only=True)
    train_filled = train.copy()
    test_filled = test.copy()
    for col in impute_cols:
        fill_val = medians[col] if col in medians.index and pd.notna(medians[col]) else FILL_DEFAULTS.get(col, 0.0)
        train_filled[col] = train_filled[col].fillna(fill_val)
        test_filled[col] = test_filled[col].fillna(fill_val)
    return train_filled, test_filled


def winner_confidence(home_prob: np.ndarray) -> np.ndarray:
    return np.maximum(home_prob, 1.0 - home_prob)


def predicted_correct(home_prob: np.ndarray, y_true: np.ndarray) -> np.ndarray:
    pred_home = home_prob >= 0.5
    return (pred_home & (y_true == 1)) | (~pred_home & (y_true == 0))


def calibration_lines(confidence: np.ndarray, correct: np.ndarray) -> list[str]:
    buckets = [
        ("50-55%", 0.50, 0.55),
        ("55-60%", 0.55, 0.60),
        ("60-65%", 0.60, 0.65),
        ("65-70%", 0.65, 0.70),
        ("70-75%", 0.70, 0.75),
        ("75%+  ", 0.75, 1.01),
    ]
    lines: list[str] = []
    for label, lo, hi in buckets:
        if hi > 1.0:
            mask = confidence >= lo
        else:
            mask = (confidence >= lo) & (confidence < hi)
        n = int(mask.sum())
        if n == 0:
            lines.append(f"  Bucket {label}:  N={n:4d}  Actual win rate:   n/a")
        else:
            actual = float(correct[mask].mean()) * 100.0
            lines.append(f"  Bucket {label}:  N={n:4d}  Actual win rate: {actual:5.1f}%")
    return lines


def prob_distribution_lines(confidence: np.ndarray) -> list[str]:
    return [
        f"  Median: {np.median(confidence):.3f}",
        f"  p75:    {np.percentile(confidence, 75):.3f}",
        f"  p90:    {np.percentile(confidence, 90):.3f}",
        f"  p95:    {np.percentile(confidence, 95):.3f}",
        f"  Max:    {np.max(confidence):.3f}",
    ]


def high_confidence_lines(
    confidence: np.ndarray,
    correct: np.ndarray,
    test_df: pd.DataFrame,
    n_total: int,
) -> list[str]:
    col_laa = test_df["home_team"].isin(["COL", "LAA"]) | test_df["away_team"].isin(["COL", "LAA"])
    lines: list[str] = []
    for threshold in (0.60, 0.65, 0.68):
        mask = confidence >= threshold
        n = int(mask.sum())
        pct = (n / n_total * 100.0) if n_total else 0.0
        acc = float(correct[mask].mean()) * 100.0 if n else float("nan")
        lines.append(
            f"  >={threshold * 100:.0f}% confidence: N={n:4d} ({pct:4.1f}% of games)  "
            f"Accuracy: {acc:5.1f}%"
        )

    mask65_excl = (confidence >= 0.65) & ~col_laa.to_numpy()
    n_excl = int(mask65_excl.sum())
    acc_excl = float(correct[mask65_excl].mean()) * 100.0 if n_excl else float("nan")
    lines.append(f"  >=65% excl COL/LAA: N={n_excl:4d}  Accuracy: {acc_excl:5.1f}%")
    return lines


def monthly_accuracy_lines(test: pd.DataFrame, y_test: np.ndarray, preds: np.ndarray) -> list[str]:
    tmp = test.copy()
    tmp["month"] = tmp["game_date_et"].astype(str).str.slice(0, 7)
    tmp["y"] = y_test
    tmp["pred"] = preds

    lines = [
        "  MONTHLY ACCURACY (test set, post-filter):",
        "    Month       N    Accuracy",
    ]
    for month, group in tmp.groupby("month", sort=True):
        acc = accuracy_score(group["y"], group["pred"]) * 100.0
        lines.append(f"    {month:<9} {len(group):4d}     {acc:5.1f}%")
    return lines


def feature_importance_lines(model_name: str, model, features: list[str]) -> list[str]:
    if model_name == "LogReg_C10":
        pairs = sorted(
            zip(features, model.coef_[0]),
            key=lambda x: abs(x[1]),
            reverse=True,
        )
        return [f"  {name + ':':<18} {coef:+.3f}" for name, coef in pairs]

    base = model.calibrated_classifiers_[0].estimator
    pairs = sorted(
        zip(features, base.feature_importances_),
        key=lambda x: abs(x[1]),
        reverse=True,
    )
    return [f"  {name + ':':<18} {imp:.3f}" for name, imp in pairs]


def build_predictions_csv(test_df: pd.DataFrame, probs: np.ndarray) -> pd.DataFrame:
    home_prob = probs
    away_prob = 1.0 - home_prob
    predicted_winner = np.where(home_prob >= 0.5, test_df["home_team"], test_df["away_team"])
    actual_winner = np.where(test_df["home_win"].astype(int) == 1, test_df["home_team"], test_df["away_team"])
    correct = predicted_winner == actual_winner

    return pd.DataFrame(
        {
            "game_pk": test_df["game_pk"].values,
            "game_date_et": test_df["game_date_et"].values,
            "season": test_df["season"].values,
            "home_team": test_df["home_team"].values,
            "away_team": test_df["away_team"].values,
            "home_win_prob": np.round(home_prob, 4),
            "away_win_prob": np.round(away_prob, 4),
            "predicted_winner": predicted_winner,
            "actual_winner": actual_winner,
            "correct": correct.astype(int),
            "sp_data_missing": test_df["sp_data_missing"].astype(int).values,
            "h_season_games_played": test_df["h_season_games_played"].astype(int).values,
            "a_season_games_played": test_df["a_season_games_played"].astype(int).values,
            "min_games_played": test_df["min_games_played"].astype(int).values,
        }
    )


def resolve_train_test_splits(
    seasons_present: list[int],
) -> tuple[list[int], list[int]]:
    """
    Train on 2024+2025 combined when both are available; hold out 2026 for test.
    Falls back to train=2024 / test=2025 when 2026 is not in the DB.
    """
    present = sorted(set(int(s) for s in seasons_present))
    train_pool = [s for s in TRAIN_SEASONS if s in present]
    if not train_pool:
        raise ValueError(f"No training seasons from {TRAIN_SEASONS} in loaded data: {present}")

    if 2026 in present and len(train_pool) >= 2:
        return train_pool, [2026]
    if 2026 in present:
        return train_pool, [2026]
    if 2025 in present and 2024 in present:
        return [2024], [2025]
    if len(present) >= 2:
        return present[:-1], [present[-1]]
    raise ValueError(f"Need at least one train and one test season; got {present}")


def filter_summary_lines(
    filter_stats: dict[str, int],
    min_games: int,
    train_n: int,
    test_n: int,
    *,
    train_seasons: list[int],
    test_seasons: list[int],
) -> list[str]:
    loaded = filter_stats["loaded_n"]
    dropped = filter_stats["dropped_n"]
    retained = filter_stats["retained_n"]
    pct = (dropped / loaded * 100.0) if loaded else 0.0
    train_label = "+".join(str(s) for s in train_seasons)
    test_label = "+".join(str(s) for s in test_seasons)
    return [
        "EARLY-SEASON FILTER",
        "══════════════════════════════════════════════════════",
        f"Min games played threshold: {min_games} (per team)" if min_games > 0 else "Min games played threshold: disabled",
        f"Total games loaded:         {loaded}",
        f"Games dropped (early szn):  {dropped} ({pct:.1f}%)",
        f"Games retained:             {retained}",
        f"Train ({train_label}, filtered): {train_n} games",
        f"Test  ({test_label}, filtered):  {test_n} games",
        "",
    ]


def format_model_report(
    model_name: str,
    *,
    train: pd.DataFrame,
    test: pd.DataFrame,
    train_seasons: list[int],
    test_seasons: list[int],
    y_test: np.ndarray,
    probs: np.ndarray,
    model,
    baseline_acc: float,
) -> list[str]:
    preds = (probs >= 0.5).astype(int)
    confidence = winner_confidence(probs)
    correct = predicted_correct(probs, y_test)

    train_complete = int((train["sp_data_missing"] == 0).sum())
    train_imputed = int((train["sp_data_missing"] == 1).sum())
    test_complete = int((test["sp_data_missing"] == 0).sum())
    test_imputed = int((test["sp_data_missing"] == 1).sum())

    season_lines: list[str] = []
    for season in sorted(test["season"].unique()):
        mask = test["season"].values == season
        if not mask.any():
            continue
        acc = accuracy_score(y_test[mask], preds[mask]) * 100.0
        label = f"{season} YTD" if season == max(test_seasons) else str(season)
        season_lines.append(f"  {label + ':':<10} N={mask.sum():4d}  Accuracy: {acc:5.1f}%")

    return [
        "══════════════════════════════════════════════",
        f"MODEL: {model_name}",
        "══════════════════════════════════════════════",
        f"Train: {'+'.join(str(s) for s in train_seasons)}  |  N={len(train)} games "
        f"({train_complete} with complete SP, {train_imputed} imputed)",
        f"Test:  {'+'.join(str(s) for s in test_seasons)}  |  N={len(test)} games "
        f"({test_complete} with complete SP, {test_imputed} imputed)",
        "",
        "BASELINE (always predict home win):",
        f"  Accuracy: {baseline_acc:5.1f}%",
        "",
        "MODEL PERFORMANCE (test set):",
        f"  Accuracy:    {accuracy_score(y_test, preds) * 100.0:5.1f}%",
        f"  Brier Score: {brier_score_loss(y_test, probs):.3f}",
        f"  Log Loss:    {log_loss(y_test, probs):.3f}",
        "",
        "PROBABILITY DISTRIBUTION (predicted winner confidence):",
        *prob_distribution_lines(confidence),
        "",
        "CALIBRATION:",
        *calibration_lines(confidence, correct),
        "",
        "HIGH-CONFIDENCE SUBSETS:",
        *high_confidence_lines(confidence, correct, test, len(test)),
        "",
        "FEATURE COEFFICIENTS (sorted by absolute value):",
        *feature_importance_lines(model_name, model, FEATURES),
        "",
        "SEASON SPLITS:",
        *season_lines,
        *monthly_accuracy_lines(test, y_test, preds),
        "",
    ]


def save_model_artifacts(
    *,
    output_dir: Path,
    scaler: StandardScaler,
    fitted_models: dict[str, object],
    meta: dict,
) -> Path:
    model_dir = (output_dir / ".." / "models").resolve()
    model_dir.mkdir(parents=True, exist_ok=True)

    for slug, model in fitted_models.items():
        pipeline = Pipeline([
            ("scaler", scaler),
            ("model", model),
        ])
        joblib.dump(pipeline, model_dir / f"outcome_model_{slug}.joblib")

    meta_path = model_dir / "outcome_model_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"[outcome_model] Models saved to {model_dir}")
    return model_dir


def run_backtest(
    df: pd.DataFrame,
    *,
    train_seasons: list[int] | None = None,
    test_seasons: list[int] | None = None,
    min_games: int = 20,
    filter_stats: dict[str, int] | None = None,
) -> tuple[str, dict[str, pd.DataFrame], dict]:
    if filter_stats is None:
        filter_stats = {"loaded_n": len(df), "dropped_n": 0, "retained_n": len(df)}

    df = df.copy()
    if train_seasons is None or test_seasons is None:
        resolved_train, resolved_test = resolve_train_test_splits(
            sorted(df["season"].unique().tolist())
        )
        train_seasons = train_seasons or resolved_train
        test_seasons = test_seasons or resolved_test

    df["sp_data_missing"] = compute_sp_data_missing(df)
    imputed_count = int(df["sp_data_missing"].sum())
    complete_count = int(len(df) - imputed_count)
    print(
        f"[outcome_model] SP data: {complete_count} complete, "
        f"{imputed_count} imputed (not dropped)"
    )

    df = engineer_features(df)

    train = df[df["season"].isin(train_seasons)].copy()
    test = df[df["season"].isin(test_seasons)].copy()

    if train.empty:
        raise ValueError(f"No training games for seasons {train_seasons}")
    if test.empty:
        raise ValueError(f"No test games for seasons {test_seasons}")

    impute_cols = list(set(FEATURES + SP_COLS))
    raw_medians = train[impute_cols].median(numeric_only=True)
    feature_medians = {
        col: float(val)
        for col, val in raw_medians.items()
        if pd.notna(val)
    }
    train, test = impute_train_medians(train, test, impute_cols)

    x_train = train[FEATURES].astype(float).values
    y_train = train["home_win"].astype(int).values
    x_test = test[FEATURES].astype(float).values
    y_test = test["home_win"].astype(int).values

    scaler = StandardScaler()
    x_train_scaled = scaler.fit_transform(x_train)
    x_test_scaled = scaler.transform(x_test)

    baseline_acc = float(y_test.mean()) * 100.0

    report_sections: list[str] = [
        "OUTCOME PREDICTION MODEL — BACKTEST REPORT",
        "==========================================",
        "",
        *filter_summary_lines(
            filter_stats,
            min_games,
            len(train),
            len(test),
            train_seasons=train_seasons,
            test_seasons=test_seasons,
        ),
    ]
    predictions_by_slug: dict[str, pd.DataFrame] = {}
    fitted_models: dict[str, object] = {}
    metrics: dict[str, float | int] = {}

    for model_name, spec in MODELS.items():
        print(f"[outcome_model] Fitting {model_name}…")
        model = spec["estimator"]
        model.fit(x_train_scaled, y_train)
        fitted_models[spec["slug"]] = model
        if model_name == "LogReg_C10":
            print(f"[outcome_model] LogReg C={model.C}")

        train_probs = model.predict_proba(x_train_scaled)[:, 1]
        probs = model.predict_proba(x_test_scaled)[:, 1]
        preds = (probs >= 0.5).astype(int)

        slug = spec["slug"]
        metrics[f"{slug}_train_accuracy"] = float(accuracy_score(y_train, (train_probs >= 0.5).astype(int)))
        metrics[f"{slug}_test_accuracy"] = float(accuracy_score(y_test, preds))

        if slug == "logreg":
            conf = winner_confidence(probs)
            correct = predicted_correct(probs, y_test)
            ge65 = conf >= 0.65
            metrics["logreg_ge65_n"] = int(ge65.sum())
            metrics["logreg_ge65_accuracy"] = (
                float(correct[ge65].mean()) if ge65.any() else float("nan")
            )

        report_sections.extend(
            format_model_report(
                model_name,
                train=train,
                test=test,
                train_seasons=train_seasons,
                test_seasons=test_seasons,
                y_test=y_test,
                probs=probs,
                model=model,
                baseline_acc=baseline_acc,
            )
        )
        predictions_by_slug[slug] = build_predictions_csv(test, probs)

    artifact_meta = {
        "trained_on_season": train_seasons[0],
        "trained_on_seasons": train_seasons,
        "min_games_filter": min_games,
        "confidence_threshold": 0.65,
        "feature_list": FEATURES,
        "feature_medians": feature_medians,
        "train_n": len(train),
        "train_accuracy": metrics.get("logreg_train_accuracy"),
        "test_seasons": test_seasons,
        "test_n": len(test),
        "logreg_test_accuracy": metrics.get("logreg_test_accuracy"),
        "gradboost_test_accuracy": metrics.get("gradboost_test_accuracy"),
        "logreg_ge65_accuracy": metrics.get("logreg_ge65_accuracy"),
        "logreg_ge65_n": metrics.get("logreg_ge65_n"),
        "saved_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }

    return "\n".join(report_sections), predictions_by_slug, {
        "scaler": scaler,
        "fitted_models": fitted_models,
        "meta": artifact_meta,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pure outcome prediction model backtest (no odds features)."
    )
    parser.add_argument(
        "--db",
        default=get_db_path(),
        help="Path to SQLite database (default: MLB_DB_PATH / data/mlb_stats.db)",
    )
    parser.add_argument(
        "--seasons",
        type=int,
        nargs="+",
        default=DEFAULT_SEASONS,
        help="Seasons to include (default: 2024 2025 2026)",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs/reports",
        help="Directory for report and predictions CSV",
    )
    parser.add_argument(
        "--min-games",
        type=int,
        default=20,
        help="Minimum games played by each team to include a game (default: 20). Use 0 to disable.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    seasons = sorted(set(args.seasons))
    # Ensure 2026 is loaded for holdout when training on 2024+2025
    if any(s in seasons for s in TRAIN_SEASONS) and 2026 not in seasons:
        seasons.append(2026)
        seasons = sorted(set(seasons))

    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = _REPO_ROOT / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    report_path = output_dir / "outcome_model_backtest.txt"

    con = db_connect(args.db)
    con.row_factory = sqlite3.Row
    try:
        print(f"[outcome_model] Loading games for seasons {seasons}…")
        df = load_games(con, seasons)
        print(f"[outcome_model] Loaded {len(df)} completed regular-season games")
    finally:
        con.close()

    if df.empty:
        print("[outcome_model] ERROR: no games returned — check seasons and joins")
        return 1

    df, filter_stats = apply_early_season_filter(df, args.min_games)

    train_seasons, test_seasons = resolve_train_test_splits(
        sorted(df["season"].unique().tolist())
    )
    print(
        f"[outcome_model] Split: train={train_seasons} test={test_seasons}"
    )

    report, predictions_by_slug, artifacts = run_backtest(
        df,
        train_seasons=train_seasons,
        test_seasons=test_seasons,
        min_games=args.min_games,
        filter_stats=filter_stats,
    )

    save_model_artifacts(
        output_dir=output_dir,
        scaler=artifacts["scaler"],
        fitted_models=artifacts["fitted_models"],
        meta=artifacts["meta"],
    )

    report_path.write_text(report + "\n", encoding="utf-8")
    csv_paths: dict[str, Path] = {}
    for slug, pred_df in predictions_by_slug.items():
        csv_path = output_dir / f"outcome_model_predictions_{slug}.csv"
        pred_df.to_csv(csv_path, index=False)
        csv_paths[slug] = csv_path

    sys.stdout.buffer.write((report + "\n\n").encode("utf-8", errors="replace"))
    sys.stdout.buffer.flush()
    print(f"[outcome_model] Report saved to {report_path}")
    for slug, path in csv_paths.items():
        print(f"[outcome_model] Predictions saved to {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
