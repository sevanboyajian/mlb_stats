#!/usr/bin/env python3
"""
outcome_model.py
────────────────
Pure outcome prediction model: home team win probability from pre-game
matchup features only (no odds / market lines).

Train: 2024 regular season  |  Test: 2025 + 2026 YTD

USAGE:
  python batch/analysis/prediction/outcome_model.py --db data/mlb_stats.db
  python batch/analysis/prediction/outcome_model.py --seasons 2024 2025 2026
  python batch/analysis/prediction/outcome_model.py --output-dir outputs/reports
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss
from sklearn.preprocessing import StandardScaler

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import connect as db_connect, get_db_path

DEFAULT_SEASONS = [2024, 2025, 2026]
TRAIN_SEASON = 2024
TEST_SEASONS = [2025, 2026]

FEATURES = [
    "ops_diff",
    "run_diff_diff",
    "sp_era_diff",
    "sp_whip_diff",
    "sp_k9_diff",
    "win_pct_diff",
    "pythag_diff",
    "home_split_ops",
    "park_factor_runs",
    "elevation_ft",
    "home_field",
    "h_rolling_k_pct",
    "a_rolling_k_pct",
    "h_rolling_bb_pct",
    "a_rolling_bb_pct",
]

SP_COLS = [
    "hsp_era_wma",
    "hsp_k_per_9_wma",
    "hsp_whip_wma",
    "asp_era_wma",
    "asp_k_per_9_wma",
    "asp_whip_wma",
]


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
       AND s.snapshot_date <= date(COALESCE(NULLIF(TRIM(g.game_date_et), ''), g.game_date), '-1 day')
 )
LEFT JOIN standings ast
  ON ast.team_id = g.away_team_id
 AND ast.season = g.season
 AND ast.snapshot_date = (
     SELECT MAX(s.snapshot_date)
     FROM standings s
     WHERE s.team_id = g.away_team_id
       AND s.season = g.season
       AND s.snapshot_date <= date(COALESCE(NULLIF(TRIM(g.game_date_et), ''), g.game_date), '-1 day')
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


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["ops_diff"] = out["h_rolling_ops"] - out["a_rolling_ops"]
    out["run_diff_diff"] = out["h_rolling_run_diff_pg"] - out["a_rolling_run_diff_pg"]
    out["sp_era_diff"] = out["asp_era_wma"] - out["hsp_era_wma"]
    out["sp_whip_diff"] = out["asp_whip_wma"] - out["hsp_whip_wma"]
    out["sp_k9_diff"] = out["hsp_k_per_9_wma"] - out["asp_k_per_9_wma"]
    out["win_pct_diff"] = out["h_win_pct"] - out["a_win_pct"]
    out["pythag_diff"] = out["h_pythag_win_pct"] - out["a_pythag_win_pct"]
    out["home_split_ops"] = out["h_rolling_ops_home"] - out["a_rolling_ops_road"]
    out["home_field"] = 1

    return out


def drop_sp_nulls(df: pd.DataFrame) -> pd.DataFrame:
    mask = df[SP_COLS].notna().all(axis=1)
    dropped = (~mask).sum()
    if dropped:
        print(f"[outcome_model] Dropped {dropped} rows with missing SP features")
    return df.loc[mask].copy()


FILL_DEFAULTS: dict[str, float] = {
    "elevation_ft": 0.0,
    "park_factor_runs": 100.0,
}


def impute_train_medians(train: pd.DataFrame, test: pd.DataFrame, columns: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    medians = train[columns].median(numeric_only=True)
    train_filled = train.copy()
    test_filled = test.copy()
    for col in columns:
        fill_val = medians[col] if col in medians.index and pd.notna(medians[col]) else FILL_DEFAULTS.get(col, 0.0)
        train_filled[col] = train_filled[col].fillna(fill_val)
        test_filled[col] = test_filled[col].fillna(fill_val)
    return train_filled, test_filled


def calibration_lines(y_true: np.ndarray, y_prob: np.ndarray) -> list[str]:
    buckets = [
        ("50-55%", 0.50, 0.55),
        ("55-60%", 0.55, 0.60),
        ("60-65%", 0.60, 0.65),
        ("65-70%", 0.65, 0.70),
        ("70%+  ", 0.70, 1.01),
    ]
    lines: list[str] = []
    for label, lo, hi in buckets:
        if hi > 1.0:
            mask = y_prob >= lo
        else:
            mask = (y_prob >= lo) & (y_prob < hi)
        n = int(mask.sum())
        if n == 0:
            actual = float("nan")
            lines.append(f"  Bucket {label}:  N={n:4d}  Actual win rate:   n/a")
        else:
            actual = float(y_true[mask].mean()) * 100.0
            lines.append(f"  Bucket {label}:  N={n:4d}  Actual win rate: {actual:5.1f}%")
    return lines


def format_top_predictions(test_df: pd.DataFrame, probs: np.ndarray, n: int = 20) -> list[str]:
    tmp = test_df.copy()
    tmp["home_win_prob"] = probs
    tmp["confidence"] = np.abs(tmp["home_win_prob"] - 0.5)
    tmp = tmp.sort_values(["confidence", "game_date_et"], ascending=[False, True]).head(n)

    lines = [
        f"  {'date':<10} {'home':<4} {'away':<4} {'pred_prob':>9}  actual",
    ]
    for _, row in tmp.iterrows():
        actual = "WIN" if int(row["home_win"]) == 1 else "LOSS"
        lines.append(
            f"  {str(row['game_date_et']):<10} {row['home_team']:<4} {row['away_team']:<4} "
            f"{row['home_win_prob']:9.2f}  {actual}"
        )
    return lines


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
        }
    )


def run_backtest(
    df: pd.DataFrame,
    train_season: int = TRAIN_SEASON,
    test_seasons: list[int] | None = None,
) -> tuple[str, pd.DataFrame]:
    if test_seasons is None:
        test_seasons = TEST_SEASONS

    df = engineer_features(df)
    df = drop_sp_nulls(df)

    train = df[df["season"] == train_season].copy()
    test = df[df["season"].isin(test_seasons)].copy()

    if train.empty:
        raise ValueError(f"No training games for season {train_season}")
    if test.empty:
        raise ValueError(f"No test games for seasons {test_seasons}")

    train, test = impute_train_medians(train, test, FEATURES)

    x_train = train[FEATURES].astype(float).values
    y_train = train["home_win"].astype(int).values
    x_test = test[FEATURES].astype(float).values
    y_test = test["home_win"].astype(int).values

    scaler = StandardScaler()
    x_train_scaled = scaler.fit_transform(x_train)
    x_test_scaled = scaler.transform(x_test)

    model = LogisticRegression(
        class_weight="balanced",
        C=1.0,
        max_iter=1000,
        solver="lbfgs",
    )
    model.fit(x_train_scaled, y_train)

    probs = model.predict_proba(x_test_scaled)[:, 1]
    preds = (probs >= 0.5).astype(int)

    baseline_acc = float(y_test.mean()) * 100.0
    model_acc = accuracy_score(y_test, preds) * 100.0
    brier = brier_score_loss(y_test, probs)
    ll = log_loss(y_test, probs)

    coef_pairs = sorted(
        zip(FEATURES, model.coef_[0]),
        key=lambda x: abs(x[1]),
        reverse=True,
    )
    coef_lines = [f"  {name + ':':<18} {coef:+.3f}" for name, coef in coef_pairs]

    season_lines: list[str] = []
    for season in sorted(test["season"].unique()):
        mask = test["season"].values == season
        if not mask.any():
            continue
        acc = accuracy_score(y_test[mask], preds[mask]) * 100.0
        label = f"{season} YTD" if season == max(test_seasons) else str(season)
        season_lines.append(f"  {label + ':':<10} N={mask.sum():4d}  Accuracy: {acc:5.1f}%")

    lines = [
        "OUTCOME PREDICTION MODEL — BACKTEST REPORT",
        "==========================================",
        f"Train: {train_season}  |  N={len(train)} games",
        f"Test:  {'+'.join(str(s) for s in test_seasons)}  |  N={len(test)} games",
        "",
        "BASELINE (always predict home win):",
        f"  Accuracy: {baseline_acc:5.1f}%  (historical home win rate)",
        "",
        "MODEL PERFORMANCE (test set):",
        f"  Accuracy:    {model_acc:5.1f}%",
        f"  Brier Score: {brier:.3f}  (lower = better; 0.25 = no skill)",
        f"  Log Loss:    {ll:.3f}",
        "",
        "CALIBRATION (does 60% confidence mean 60% win rate?):",
        *calibration_lines(y_test, probs),
        "",
        "FEATURE COEFFICIENTS (sorted by absolute value):",
        *coef_lines,
        "",
        "TOP 20 HIGHEST-CONFIDENCE TEST PREDICTIONS:",
        *format_top_predictions(test, probs),
        "",
        "SEASON SPLITS:",
        *season_lines,
    ]

    report = "\n".join(lines)
    predictions = build_predictions_csv(test, probs)
    return report, predictions


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
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    seasons = sorted(set(args.seasons))

    train_season = TRAIN_SEASON if TRAIN_SEASON in seasons else seasons[0]
    test_seasons = [s for s in seasons if s != train_season]
    if TRAIN_SEASON in seasons:
        test_seasons = [s for s in TEST_SEASONS if s in seasons]

    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = _REPO_ROOT / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    report_path = output_dir / "outcome_model_backtest.txt"
    csv_path = output_dir / "outcome_model_predictions.csv"

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

    report, predictions = run_backtest(df, train_season=train_season, test_seasons=test_seasons)

    report_path.write_text(report + "\n", encoding="utf-8")
    predictions.to_csv(csv_path, index=False)

    print(report)
    print()
    print(f"[outcome_model] Report saved to {report_path}")
    print(f"[outcome_model] Predictions saved to {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
