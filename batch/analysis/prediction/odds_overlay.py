#!/usr/bin/env python3
"""
odds_overlay.py
───────────────
Join model-agreement predictions to closing moneyline odds and evaluate
whether model accuracy translates to positive EV vs the market.

USAGE:
  python batch/analysis/prediction/odds_overlay.py --db data/mlb_stats.db
  python batch/analysis/prediction/odds_overlay.py --threshold 0.68
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import connect as db_connect, get_db_path

DEFAULT_LOGREG = "outputs/reports/outcome_model_predictions_logreg.csv"
DEFAULT_GRADBOOST = "outputs/reports/outcome_model_predictions_gradboost.csv"
DEFAULT_OUTPUT_DIR = "outputs/reports"

ODDS_KEYWORDS = ("ml", "moneyline", "odds", "line", "price", "american", "closing")
ODDS_CANDIDATES = (
    "game_odds",
    "closing_odds",
    "odds",
    "bet_snapshots",
    "lines",
    "market_odds",
    "game_odds_f5",
)


def resolve_path(path: str) -> Path:
    p = Path(path)
    return p if p.is_absolute() else _REPO_ROOT / p


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


def discover_odds_table(con: sqlite3.Connection, verbose: bool = False) -> str | None:
    tables = [
        row[0]
        for row in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
    ]
    if verbose:
        print("[odds_overlay] Tables in database:")
        for name in tables:
            print(f"  - {name}")

    candidates: list[tuple[str, int]] = []
    for table in tables:
        if table not in ODDS_CANDIDATES and "odds" not in table.lower():
            continue
        cols = [row[1].lower() for row in con.execute(f"PRAGMA table_info({table})").fetchall()]
        score = sum(1 for c in cols if any(k in c for k in ODDS_KEYWORDS))
        if "game_pk" in cols and any(c in cols for c in ("home_ml", "home_f5_ml")):
            score += 5
        if score:
            candidates.append((table, score))
            if verbose:
                print(f"[odds_overlay] Candidate {table} (score={score})")
                print(f"  columns: {cols[:12]}{'...' if len(cols) > 12 else ''}")
                sample = con.execute(f"SELECT * FROM {table} LIMIT 3").fetchall()
                print(f"  sample rows: {len(sample)}")

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[0][0]


def load_agreement_subset(
    logreg_path: Path,
    gradboost_path: Path,
    threshold: float,
) -> pd.DataFrame:
    logreg = pd.read_csv(logreg_path)
    gradboost = pd.read_csv(gradboost_path)

    merged = logreg.merge(gradboost, on="game_pk", suffixes=("_lr", "_gb"))
    merged["logreg_confidence"] = merged[["home_win_prob_lr", "away_win_prob_lr"]].max(axis=1)

    agree = merged[
        (merged["predicted_winner_lr"] == merged["predicted_winner_gb"])
        & (merged["logreg_confidence"] >= threshold)
    ].copy()

    agree = agree.rename(
        columns={
            "home_win_prob_lr": "logreg_home_win_prob",
            "home_win_prob_gb": "gradboost_home_win_prob",
            "predicted_winner_lr": "predicted_winner",
            "actual_winner_lr": "actual_winner",
            "correct_lr": "correct",
            "game_date_et_lr": "game_date_et",
            "season_lr": "season",
            "home_team_lr": "home_team",
            "away_team_lr": "away_team",
        }
    )

    cols = [
        "game_pk",
        "game_date_et",
        "season",
        "home_team",
        "away_team",
        "logreg_home_win_prob",
        "gradboost_home_win_prob",
        "predicted_winner",
        "actual_winner",
        "correct",
        "logreg_confidence",
    ]
    return agree[cols].reset_index(drop=True)


def load_closing_odds(con: sqlite3.Connection, game_pks: list[int]) -> pd.DataFrame:
    if not game_pks:
        return pd.DataFrame()

    table = discover_odds_table(con)
    if table is None:
        all_tables = [
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
        ]
        print("NO ODDS TABLE FOUND — tables available:", all_tables)
        print(
            "Create or load odds into game_odds (game_pk, home_ml, away_ml, "
            "market_type='moneyline', is_closing_line, captured_at_utc)."
        )
        sys.exit(0)

    if table != "game_odds":
        print(f"[odds_overlay] Using odds table: {table}")

    placeholders = ",".join("?" * len(game_pks))
    sql = f"""
    WITH ranked AS (
        SELECT
            go.game_pk,
            go.home_ml,
            go.away_ml,
            go.captured_at_utc,
            go.is_closing_line,
            go.bookmaker,
            go.data_source,
            ROW_NUMBER() OVER (
                PARTITION BY go.game_pk
                ORDER BY go.is_closing_line DESC,
                         go.captured_at_utc DESC,
                         go.id DESC
            ) AS rn
        FROM {table} go
        WHERE go.game_pk IN ({placeholders})
          AND go.market_type = 'moneyline'
          AND go.home_ml IS NOT NULL
          AND go.away_ml IS NOT NULL
    )
    SELECT game_pk, home_ml, away_ml, captured_at_utc, is_closing_line, bookmaker, data_source
    FROM ranked
    WHERE rn = 1
    """
    odds = pd.read_sql_query(sql, con, params=game_pks)
    if odds.empty:
        return odds

    missing_pks = set(game_pks) - set(odds["game_pk"].tolist())
    if missing_pks:
        # Fallback: team + date match for games without game_pk odds
        fallback = _load_odds_by_team_date(con, table, missing_pks)
        if not fallback.empty:
            odds = pd.concat([odds, fallback], ignore_index=True).drop_duplicates("game_pk")

    return odds


def _load_odds_by_team_date(
    con: sqlite3.Connection,
    table: str,
    game_pks: set[int],
) -> pd.DataFrame:
    if not game_pks:
        return pd.DataFrame()

    placeholders = ",".join("?" * len(game_pks))
    games = pd.read_sql_query(
        f"""
        SELECT g.game_pk,
               COALESCE(NULLIF(TRIM(g.game_date_et), ''), g.game_date) AS game_date_et,
               th.abbreviation AS home_team,
               ta.abbreviation AS away_team
        FROM games g
        JOIN teams th ON th.team_id = g.home_team_id
        JOIN teams ta ON ta.team_id = g.away_team_id
        WHERE g.game_pk IN ({placeholders})
        """,
        con,
        params=list(game_pks),
    )
    if games.empty:
        return pd.DataFrame()

    rows: list[dict] = []
    for _, g in games.iterrows():
        row = con.execute(
            f"""
            SELECT go.game_pk, go.home_ml, go.away_ml, go.captured_at_utc,
                   go.is_closing_line, go.bookmaker, go.data_source
            FROM {table} go
            JOIN games gx ON gx.game_pk = go.game_pk
            JOIN teams th ON th.team_id = gx.home_team_id
            JOIN teams ta ON ta.team_id = gx.away_team_id
            WHERE go.market_type = 'moneyline'
              AND go.home_ml IS NOT NULL
              AND go.away_ml IS NOT NULL
              AND th.abbreviation = ?
              AND ta.abbreviation = ?
              AND COALESCE(NULLIF(TRIM(gx.game_date_et), ''), gx.game_date) = ?
            ORDER BY go.is_closing_line DESC, go.captured_at_utc DESC, go.id DESC
            LIMIT 1
            """,
            (g["home_team"], g["away_team"], g["game_date_et"]),
        ).fetchone()
        if row:
            rows.append(
                {
                    "game_pk": int(g["game_pk"]),
                    "home_ml": row[1],
                    "away_ml": row[2],
                    "captured_at_utc": row[3],
                    "is_closing_line": row[4],
                    "bookmaker": row[5],
                    "data_source": row[6],
                }
            )

    return pd.DataFrame(rows)


def enrich_with_odds_metrics(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    raw_home = out["home_ml"].map(american_to_implied)
    raw_away = out["away_ml"].map(american_to_implied)
    vig = raw_home + raw_away
    out["implied_home"] = raw_home / vig
    out["implied_away"] = raw_away / vig

    pick_home = out["predicted_winner"] == out["home_team"]
    out["model_prob"] = np.where(
        pick_home,
        out["logreg_home_win_prob"],
        1.0 - out["logreg_home_win_prob"],
    )
    out["market_prob"] = np.where(pick_home, out["implied_home"], out["implied_away"])
    out["odds_used"] = np.where(pick_home, out["home_ml"], out["away_ml"])
    out["edge"] = out["model_prob"] - out["market_prob"]
    out["payout"] = out["odds_used"].map(american_payout)
    out["ev"] = out["model_prob"] * out["payout"] - (1.0 - out["model_prob"])
    out["profit"] = np.where(out["correct"] == 1, out["payout"], -1.0)

    out = out.sort_values(["game_date_et", "game_pk"]).reset_index(drop=True)
    out["cum_pl"] = out["profit"].cumsum()
    return out


def roi_for_games(games: pd.DataFrame) -> float:
    if games.empty:
        return float("nan")
    return float(games["profit"].sum() / len(games) * 100.0)


def accuracy_pct(games: pd.DataFrame) -> float:
    if games.empty:
        return float("nan")
    return float(games["correct"].mean() * 100.0)


def favorite_bucket_label(odds_used: float) -> str | None:
    if odds_used >= 0:
        return None
    if -149 <= odds_used <= -100:
        return "-100 to -149"
    if -199 <= odds_used <= -150:
        return "-150 to -199"
    if -249 <= odds_used <= -200:
        return "-200 to -249"
    if odds_used <= -250:
        return "-250+"
    return None


def build_report(
    agreement: pd.DataFrame,
    with_odds: pd.DataFrame,
    threshold: float,
) -> str:
    n_agree = len(agreement)
    n_odds = len(with_odds)
    n_missing = n_agree - n_odds

    lines = [
        f"ODDS OVERLAY — MODEL AGREEMENT SUBSET (LogReg >={threshold:.0%}, Both Agree)",
        "================================================================",
        f"Agreement games found:      {n_agree}",
        f"Games with odds data:       {n_odds}",
        f"Games missing odds:         {n_missing}",
        "",
    ]

    if with_odds.empty:
        lines.append("No games with closing odds — cannot compute edge or ROI.")
        return "\n".join(lines)

    avg_model = with_odds["model_prob"].mean() * 100.0
    avg_market = with_odds["market_prob"].mean() * 100.0
    avg_edge = with_odds["edge"].mean() * 100.0
    avg_odds = with_odds["odds_used"].mean()
    n_correct = int(with_odds["correct"].sum())
    acc = accuracy_pct(with_odds)
    total_staked = float(n_odds)
    total_return = float(with_odds["profit"].sum() + total_staked)
    net_pl = float(with_odds["profit"].sum())
    roi = net_pl / total_staked * 100.0

    lines.extend(
        [
            "PREDICTED WINNER PROFILE:",
            f"  Avg model probability:    {avg_model:5.1f}%",
            f"  Avg market implied prob:  {avg_market:5.1f}%  (vig-removed)",
            f"  Avg edge (model - market): {avg_edge:+5.1f}%",
            f"  Avg closing odds:         {avg_odds:+.0f} (American)",
            "",
            "ACTUAL RESULTS:",
            f"  Correct predictions:      {n_correct} / {n_odds} ({acc:.1f}%)",
            f"  True accuracy:            {acc:.1f}%",
            "",
            "FLAT-UNIT SIMULATION (1u per game at closing odds):",
            f"  Total staked:             {total_staked:.0f}u",
            f"  Total return:             {total_return:.2f}u",
            f"  Net P&L:                  {net_pl:+.2f}u",
            f"  ROI:                      {roi:+.1f}%",
            "",
            "EDGE DISTRIBUTION:",
        ]
    )

    for edge_min, label in [
        (0.0, "> 0%"),
        (0.03, "> 3%"),
        (0.05, "> 5%"),
    ]:
        subset = with_odds[with_odds["edge"] > edge_min]
        n = len(subset)
        pct = n / n_odds * 100.0 if n_odds else 0.0
        acc_sub = accuracy_pct(subset)
        lines.append(
            f"  Games with model edge {label:>4}:  {n:3d} ({pct:4.1f}%)  "
            f"Accuracy: {acc_sub:5.1f}%"
        )

    pos = with_odds[with_odds["edge"] > 0]
    neg = with_odds[with_odds["edge"] <= 0]
    lines.append(f"  Games with model edge > 0%, correct: {int(pos['correct'].sum())}")
    lines.append(
        f"  Games with model edge <= 0%:  {len(neg):3d} "
        f"({len(neg)/n_odds*100.0:4.1f}%)  Accuracy: {accuracy_pct(neg):5.1f}%"
    )
    lines.append("")
    lines.append("ODDS RANGE BREAKDOWN (predicted-side favorite only):")

    bucket_defs = [
        ("Favorite -100 to -149", "-100 to -149"),
        ("Favorite -150 to -199", "-150 to -199"),
        ("Favorite -200 to -249", "-200 to -249"),
        ("Favorite -250+       ", "-250+"),
    ]
    fav = with_odds[with_odds["odds_used"] < 0].copy()
    fav["bucket"] = fav["odds_used"].map(favorite_bucket_label)
    underdog_n = int((with_odds["odds_used"] >= 0).sum())
    if underdog_n:
        dog = with_odds[with_odds["odds_used"] >= 0]
        lines.append(
            f"  Underdog (+ odds)      : N={underdog_n:2d}  "
            f"Model acc: {accuracy_pct(dog):5.1f}%  ROI: {roi_for_games(dog):+5.1f}%"
        )

    for label, key in bucket_defs:
        subset = fav[fav["bucket"] == key]
        n = len(subset)
        lines.append(
            f"  {label}: N={n:2d}  "
            f"Model acc: {accuracy_pct(subset):5.1f}%  ROI: {roi_for_games(subset):+5.1f}%"
        )

    lines.extend(["", "SEASON BREAKDOWN:"])
    for season in sorted(with_odds["season"].unique()):
        subset = with_odds[with_odds["season"] == season]
        label = f"{season} YTD" if season == with_odds["season"].max() else str(season)
        lines.append(
            f"  {label}: N={len(subset):2d}  "
            f"Accuracy: {accuracy_pct(subset):5.1f}%  ROI: {roi_for_games(subset):+5.1f}%"
        )

    lines.extend(
        [
            "",
            "CUMULATIVE P&L BY DATE (chronological):",
            "  date       home  away  pred  odds  model%  mkt%  edge   result  cumPL",
        ]
    )
    for _, row in with_odds.iterrows():
        result = "WIN" if int(row["correct"]) == 1 else "LOSS"
        lines.append(
            f"  {row['game_date_et']:<10} {row['home_team']:<4} {row['away_team']:<4} "
            f"{row['predicted_winner']:<4} {int(row['odds_used']):+4d}  "
            f"{row['model_prob']*100:5.1f}% {row['market_prob']*100:5.1f}% "
            f"{row['edge']*100:+5.1f}%  {result:<4} {row['cum_pl']:+.2f}u"
        )

    return "\n".join(lines)


def build_detail_csv(with_odds: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "game_pk",
        "game_date_et",
        "season",
        "home_team",
        "away_team",
        "predicted_winner",
        "actual_winner",
        "correct",
        "logreg_home_win_prob",
        "gradboost_home_win_prob",
        "logreg_confidence",
        "home_ml",
        "away_ml",
        "odds_used",
        "captured_at_utc",
        "is_closing_line",
        "bookmaker",
        "data_source",
        "model_prob",
        "market_prob",
        "edge",
        "ev",
        "payout",
        "profit",
        "cum_pl",
    ]
    existing = [c for c in cols if c in with_odds.columns]
    detail = with_odds[existing].copy()
    for col in ("model_prob", "market_prob", "edge", "ev", "logreg_confidence"):
        if col in detail.columns:
            detail[col] = detail[col].round(4)
    return detail


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Overlay closing odds on model-agreement prediction subset."
    )
    parser.add_argument("--db", default=get_db_path(), help="SQLite database path")
    parser.add_argument("--logreg", default=DEFAULT_LOGREG, help="LogReg predictions CSV")
    parser.add_argument(
        "--gradboost",
        default=DEFAULT_GRADBOOST,
        help="GradBoost predictions CSV",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.65,
        help="LogReg confidence cutoff (default 0.65)",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Output directory for report and detail CSV",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print odds table discovery diagnostics",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    logreg_path = resolve_path(args.logreg)
    gradboost_path = resolve_path(args.gradboost)
    output_dir = resolve_path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not logreg_path.is_file():
        print(f"[odds_overlay] ERROR: logreg CSV not found: {logreg_path}")
        return 1
    if not gradboost_path.is_file():
        print(f"[odds_overlay] ERROR: gradboost CSV not found: {gradboost_path}")
        return 1

    agreement = load_agreement_subset(logreg_path, gradboost_path, args.threshold)
    print(f"Agreement subset: N={len(agreement)} games")

    if agreement.empty:
        lr = pd.read_csv(logreg_path)
        gb = pd.read_csv(gradboost_path)
        print("[odds_overlay] logreg columns:", list(lr.columns))
        print("[odds_overlay] gradboost columns:", list(gb.columns))
        return 1

    con = db_connect(args.db)
    con.row_factory = sqlite3.Row
    try:
        if args.verbose:
            discover_odds_table(con, verbose=True)
        odds = load_closing_odds(con, agreement["game_pk"].tolist())
    finally:
        con.close()

    merged = agreement.merge(odds, on="game_pk", how="left")
    with_odds = enrich_with_odds_metrics(merged.dropna(subset=["home_ml", "away_ml"]).copy())

    report = build_report(agreement, with_odds, args.threshold)
    report_path = output_dir / "odds_overlay_report.txt"
    detail_path = output_dir / "odds_overlay_detail.csv"

    report_path.write_text(report + "\n", encoding="utf-8")
    build_detail_csv(with_odds).to_csv(detail_path, index=False)

    sys.stdout.buffer.write((report + "\n\n").encode("utf-8", errors="replace"))
    sys.stdout.buffer.flush()
    print(f"[odds_overlay] Report saved to {report_path}")
    print(f"[odds_overlay] Detail CSV saved to {detail_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
