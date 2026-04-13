#!/usr/bin/env python3
"""
backtest_analysis.py
MLB Backtesting Analysis - supports SBRO (2015-2021) and OddsWarehouse (2022-2025)

Usage:
    python backtest_analysis.py                            # SBRO 2015-2021 (default, unchanged)
    python backtest_analysis.py --bookmaker oddswarehouse  # OW 2022-2025
    python backtest_analysis.py --bookmaker all            # combined 2015-2025
    python backtest_analysis.py --season 2024              # single season
    python backtest_analysis.py --h4                       # rolling ERA report only
    python backtest_analysis.py --h4 --bookmaker oddswarehouse  # rolling ERA for OW

Runs six hypothesis tests against closing moneyline and totals data.
Outputs a markdown report to the reports/ directory.
"""

import argparse
import os
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from core.db.connection import connect as db_connect

DEFAULT_DB  = r"C:\Users\sevan\OneDrive\Documents\Python\mlb_stats\mlb_stats.db"
REPORTS_DIR = r"C:\Users\sevan\OneDrive\Documents\Python\mlb_stats\reports"
DEFAULT_OUT = r"C:\Users\sevan\OneDrive\Documents\Python\mlb_stats\reports\backtest_report.md"

# Bookmaker config  -  maps CLI arg to DB value(s) and display labels
BOOKMAKER_CONFIG = {
    "sbro": {
        "db_values":     ["sbro"],
        "label":         "SBRO (Sharp Offshore)",
        "seasons_label": "2015-2021",
        "h4_early":      [2015, 2016, 2017],
        "h4_late":       [2019, 2020, 2021],
        "h4_early_lbl":  "2015-2017",
        "h4_late_lbl":   "2019-2021",
    },
    "oddswarehouse": {
        "db_values":     ["oddswarehouse"],
        "label":         "Odds Warehouse",
        "seasons_label": "2022-2025",
        "h4_early":      [2022, 2023],
        "h4_late":       [2024, 2025],
        "h4_early_lbl":  "2022-2023",
        "h4_late_lbl":   "2024-2025",
    },
    "all": {
        "db_values":     ["sbro", "oddswarehouse"],
        "label":         "SBRO + Odds Warehouse (combined)",
        "seasons_label": "2015-2025",
        "h4_early":      [2015, 2016, 2017],
        "h4_late":       [2022, 2023, 2024, 2025],
        "h4_early_lbl":  "2015-2017",
        "h4_late_lbl":   "2022-2025",
    },
}

# -- Helpers -------------------------------------------------------------------

def connect(db_path):
    con = db_connect(db_path)
    con.row_factory = sqlite3.Row
    return con

def q(con, sql, params=()):
    return pd.read_sql_query(sql, con, params=params)

def american_to_implied(odds):
    return np.where(
        odds < 0,
        (-odds) / (-odds + 100),
        100  / (odds   + 100)
    )

def american_to_payout(odds):
    return np.where(
        odds < 0,
        100 / (-odds),
        odds / 100.0
    )

def roi(wins, payouts):
    total_bet = len(wins)
    if total_bet == 0:
        return 0.0
    total_return = (wins * (1 + payouts)).sum()
    return (total_return - total_bet) / total_bet

def bm_in_clause(db_values):
    """Return SQL IN clause for bookmaker filter."""
    return "IN (" + ",".join(f"'{v}'" for v in db_values) + ")"

# -- Core dataset --------------------------------------------------------------

def build_core_sql(db_values):
    bm = bm_in_clause(db_values)
    return f"""
SELECT
    g.game_pk,
    g.season,
    g.game_date,
    g.home_score,
    g.away_score,
    g.innings_played,
    g.extra_innings,
    g.temp_f,
    g.wind_mph,
    g.wind_direction,
    g.attendance,
    g.double_header,
    th.abbreviation  AS home_team,
    ta.abbreviation  AS away_team,
    ml.home_ml,
    ml.away_ml,
    rl.home_rl_line,
    rl.home_rl_odds,
    rl.away_rl_line,
    rl.away_rl_odds,
    tot.total_line,
    tot.over_odds,
    tot.under_odds
FROM games g
JOIN teams th ON th.team_id = g.home_team_id
JOIN teams ta ON ta.team_id = g.away_team_id
LEFT JOIN game_odds ml
    ON  ml.game_pk        = g.game_pk
    AND ml.bookmaker       {bm}
    AND ml.market_type    = 'moneyline'
    AND ml.is_closing_line = 1
LEFT JOIN game_odds rl
    ON  rl.game_pk        = g.game_pk
    AND rl.bookmaker       {bm}
    AND rl.market_type    = 'runline'
    AND rl.is_closing_line = 1
LEFT JOIN game_odds tot
    ON  tot.game_pk       = g.game_pk
    AND tot.bookmaker      {bm}
    AND tot.market_type   = 'total'
    AND tot.is_closing_line = 1
WHERE g.game_type = 'R'
AND   g.status    = 'Final'
AND   g.home_score IS NOT NULL
AND   ml.home_ml  IS NOT NULL
ORDER BY g.game_date
"""

# -- Analysis functions --------------------------------------------------------

def hypothesis_1_favourite_bias(df):
    d = df.dropna(subset=["home_ml","away_ml","home_score","away_score"]).copy()
    d["home_win"]     = (d["home_score"] > d["away_score"]).astype(int)
    d["away_win"]     = (d["away_score"] > d["home_score"]).astype(int)
    d["home_implied"] = american_to_implied(d["home_ml"])
    d["away_implied"] = american_to_implied(d["away_ml"])
    d["home_payout"]  = american_to_payout(d["home_ml"])
    d["away_payout"]  = american_to_payout(d["away_ml"])
    d["home_is_fav"]  = d["home_ml"] < 0
    home_dog = d[~d["home_is_fav"]].copy()
    away_dog = d[ d["home_is_fav"]].copy()

    bins   = [0, 0.35, 0.40, 0.45, 0.50]
    labels = ["<35%","35-40%","40-45%","45-50%"]
    d["home_implied_bucket"] = pd.cut(d["home_implied"], bins=bins, labels=labels)

    bucket_results = (
        d.groupby("home_implied_bucket", observed=True)
         .apply(lambda x: pd.Series({
             "games":         len(x),
             "home_wins":     x["home_win"].sum(),
             "home_win_rate": x["home_win"].mean(),
             "roi_home_bet":  roi(x["home_win"], x["home_payout"]),
         }))
         .reset_index()
    )

    home_fav = d[d["home_is_fav"]].copy()
    away_fav = d[~d["home_is_fav"]].copy()

    return {
        "n_games":               len(d),
        "home_dog_n":            len(home_dog),
        "away_dog_n":            len(away_dog),
        "roi_home_dog":          roi(home_dog["home_win"], home_dog["home_payout"]),
        "roi_away_dog":          roi(away_dog["away_win"], away_dog["away_payout"]),
        "roi_home_fav":          roi(home_fav["home_win"], home_fav["home_payout"]),
        "roi_away_fav":          roi(away_fav["away_win"], away_fav["away_payout"]),
        "bucket_table":          bucket_results,
        "overall_home_win_rate": d["home_win"].mean(),
    }

def hypothesis_2_home_field(df):
    d = df.dropna(subset=["home_ml","home_score","away_score"]).copy()
    d["home_win"]     = (d["home_score"] > d["away_score"]).astype(int)
    d["home_implied"] = american_to_implied(d["home_ml"])

    by_season = (
        d.groupby("season")
         .apply(lambda x: pd.Series({
             "games":             len(x),
             "actual_home_win%":  round(x["home_win"].mean() * 100, 1),
             "implied_home_win%": round(x["home_implied"].mean() * 100, 1),
             "edge%":             round((x["home_win"].mean() - x["home_implied"].mean()) * 100, 2),
         }))
         .reset_index()
    )
    return {
        "n_games":         len(d),
        "overall_actual":  d["home_win"].mean(),
        "overall_implied": d["home_implied"].mean(),
        "by_season":       by_season,
    }

def hypothesis_3_weather_totals(df):
    d = df.dropna(subset=["total_line","home_score","away_score"]).copy()
    d["total_runs"]   = d["home_score"] + d["away_score"]
    d["over_result"]  = (d["total_runs"] > d["total_line"]).astype(int)
    d["push"]         = (d["total_runs"] == d["total_line"]).astype(int)
    d["over_payout"]  = american_to_payout(d["over_odds"].fillna(-110))
    d["under_payout"] = american_to_payout(d["under_odds"].fillna(-110))

    has_temp = d.dropna(subset=["temp_f"])
    results  = {}

    if len(has_temp) > 100:
        temp_bins   = [0, 45, 55, 65, 75, 200]
        temp_labels = ["<45?F","45-55?F","55-65?F","65-75?F",">75?F"]
        has_temp = has_temp.copy()
        has_temp["temp_bucket"] = pd.cut(has_temp["temp_f"],
                                         bins=temp_bins, labels=temp_labels)
        temp_table = (
            has_temp.groupby("temp_bucket", observed=True)
            .apply(lambda x: pd.Series({
                "games":        len(x),
                "avg_runs":     round(x["total_runs"].mean(), 2),
                "avg_line":     round(x["total_line"].mean(), 2),
                "runs_vs_line": round((x["total_runs"] - x["total_line"]).mean(), 2),
                "over_rate":    round(x[x["push"]==0]["over_result"].mean()*100, 1)
                                if len(x[x["push"]==0]) > 0 else None,
            }))
            .reset_index()
        )
        results["temp_table"] = temp_table
        results["temp_n"]     = len(has_temp)
    else:
        results["temp_table"] = None
        results["temp_n"]     = len(has_temp)

    has_wind = d.dropna(subset=["wind_direction","wind_mph"])
    if len(has_wind) > 100:
        has_wind = has_wind.copy()
        has_wind["wind_in"]  = has_wind["wind_direction"].str.lower().str.contains(
            "in|center|centerfield", na=False).astype(int)
        has_wind["wind_out"] = has_wind["wind_direction"].str.lower().str.contains(
            "out|leftfield|rightfield", na=False).astype(int)
        has_wind_strong = has_wind[has_wind["wind_mph"] >= 10]

        wind_table = []
        for label, mask in [
            ("Wind In  (>=10mph)", has_wind_strong["wind_in"]==1),
            ("Wind Out (>=10mph)", has_wind_strong["wind_out"]==1),
            ("Calm (<10mph)",      has_wind["wind_mph"]<10),
        ]:
            subset = has_wind_strong[mask] if "Calm" not in label else has_wind[mask]
            if len(subset) > 10:
                no_push = subset[subset["push"]==0]
                wind_table.append({
                    "condition": label,
                    "games":     len(subset),
                    "avg_runs":  round(subset["total_runs"].mean(), 2),
                    "avg_line":  round(subset["total_line"].mean(), 2),
                    "over_rate": round(no_push["over_result"].mean()*100, 1)
                                 if len(no_push) > 0 else None,
                })
        results["wind_table"] = pd.DataFrame(wind_table) if wind_table else None
        results["wind_n"]     = len(has_wind)
    else:
        results["wind_table"] = None
        results["wind_n"]     = len(has_wind)

    results["overall_over_rate"] = (
        d[d["push"]==0]["over_result"].mean() if len(d[d["push"]==0]) > 0 else None
    )
    results["n_with_total"] = len(d)
    return results

def hypothesis_4_sp_era_vs_market(df, con):
    """
    H4 (same-game ERA version  -  has look-ahead bias, kept for continuity).
    Returns a merged DataFrame so the caller can apply whatever period slicing it wants.
    Use --h4 flag for the rolling pre-game ERA version (no look-ahead bias).
    """
    era_sql = """
    SELECT
        pgs.game_pk,
        pgs.team_id,
        SUM(pgs.innings_pitched) AS total_ip,
        SUM(pgs.earned_runs)     AS total_er
    FROM player_game_stats pgs
    WHERE pgs.player_role = 'pitcher'
    AND   pgs.innings_pitched > 0
    GROUP BY pgs.game_pk, pgs.team_id
    """
    era_df = q(con, era_sql)
    if era_df.empty:
        return {"available": False, "reason": "No pitcher stats loaded yet"}

    gt = q(con, """
        SELECT g.game_pk, g.home_team_id, g.away_team_id, g.season
        FROM   games g
        WHERE  g.game_type = 'R' AND g.status = 'Final'
    """)

    home_era = era_df.rename(columns={"team_id":"home_team_id",
                                       "total_ip":"home_ip","total_er":"home_er"})
    away_era = era_df.rename(columns={"team_id":"away_team_id",
                                       "total_ip":"away_ip","total_er":"away_er"})

    merged = (gt
              .merge(home_era, on=["game_pk","home_team_id"], how="left")
              .merge(away_era, on=["game_pk","away_team_id"], how="left"))

    merged = merged.merge(
        df[["game_pk","home_ml","away_ml","home_score","away_score"]].dropna(),
        on="game_pk", how="inner"
    )

    merged["home_game_era"] = np.where(
        merged["home_ip"] > 0, 9 * merged["home_er"] / merged["home_ip"], np.nan)
    merged["away_game_era"] = np.where(
        merged["away_ip"] > 0, 9 * merged["away_er"] / merged["away_ip"], np.nan)
    merged = merged.dropna(subset=["home_game_era","away_game_era"])

    if len(merged) < 100:
        return {"available": False, "reason": f"Insufficient merged rows ({len(merged)})"}

    merged["home_win"]      = (merged["home_score"] > merged["away_score"]).astype(int)
    merged["home_era_edge"] = merged["away_game_era"] - merged["home_game_era"]
    merged["home_implied"]  = american_to_implied(merged["home_ml"])
    merged["home_payout"]   = american_to_payout(merged["home_ml"])
    merged["away_payout"]   = american_to_payout(merged["away_ml"])
    return merged   # DataFrame  -  caller slices by period

def hypothesis_5_extra_innings_totals(df):
    d = df.dropna(subset=["home_ml","away_ml","total_line",
                           "home_score","away_score"]).copy()
    d["total_runs"]   = d["home_score"] + d["away_score"]
    d["push"]         = (d["total_runs"] == d["total_line"]).astype(int)
    d["over_result"]  = (d["total_runs"] > d["total_line"]).astype(int)
    d["home_implied"] = american_to_implied(d["home_ml"])

    close    = d[(d["home_implied"] >= 0.45) & (d["home_implied"] <= 0.55)]
    lopsided = d[(d["home_implied"] < 0.38)  | (d["home_implied"] > 0.62)]

    result = {}
    for label, subset in [("Close games (45-55% implied)", close),
                           ("Lopsided (>62% or <38%)",     lopsided),
                           ("All games",                    d)]:
        no_push = subset[subset["push"]==0]
        result[label] = {
            "games":             len(subset),
            "extra_inning_rate": round(subset["extra_innings"].mean()*100, 1),
            "over_rate":         round(no_push["over_result"].mean()*100, 1)
                                 if len(no_push) > 0 else None,
            "avg_total_runs":    round(subset["total_runs"].mean(), 2),
            "avg_line":          round(subset["total_line"].mean(), 2),
        }
    return result

def hypothesis_6_outlier_check(df, cfg):
    d = df.dropna(subset=["home_ml","home_score","away_score"]).copy()
    d["home_win"]     = (d["home_score"] > d["away_score"]).astype(int)
    d["home_implied"] = american_to_implied(d["home_ml"])
    d["total_runs"]   = d["home_score"] + d["away_score"]

    by_season = (
        d.groupby("season")
         .apply(lambda x: pd.Series({
             "games":          len(x),
             "home_win_rate":  round(x["home_win"].mean()*100, 1),
             "avg_implied":    round(x["home_implied"].mean()*100, 1),
             "avg_total_runs": round(x["total_runs"].mean(), 2),
             "extra_inn_rate": round(x["extra_innings"].mean()*100, 1),
         }))
         .reset_index()
    )
    return {"by_season": by_season}


# -- Report writer -------------------------------------------------------------

def write_report(results, out_path, cfg):
    lines = []
    W = lines.append

    h4_early = cfg["h4_early_lbl"]
    h4_late  = cfg["h4_late_lbl"]

    W("# MLB Backtesting Analysis Report")
    W(f"## {cfg['label']}  |  Closing Lines {cfg['seasons_label']}")
    W("")
    W(f"> **Source:** {cfg['label']} closing lines.")
    W("> Closing lines represent the market's sharpest consensus just before first pitch.")
    W("> No vig removal has been applied  -  ROI figures include the full juice.")
    W("")

    # H1
    W("---")
    W("## Hypothesis 1: Favourite Bias")
    W("*Prediction: Underdogs return slightly positive EV, favourites slightly negative.*")
    W("")
    h1 = results["h1"]
    W(f"**Sample:** {h1['n_games']:,} games with closing moneyline data")
    W(f"**Overall home win rate:** {h1['overall_home_win_rate']*100:.1f}%")
    W("")
    W("| Bet | Games | ROI |")
    W("|-----|-------|-----|")
    W(f"| Bet every home underdog  | {h1['home_dog_n']:,} | {h1['roi_home_dog']*100:+.2f}% |")
    W(f"| Bet every away underdog  | {h1['away_dog_n']:,} | {h1['roi_away_dog']*100:+.2f}% |")
    W(f"| Bet every home favourite | {h1['n_games']-h1['home_dog_n']:,} | {h1['roi_home_fav']*100:+.2f}% |")
    W(f"| Bet every away favourite | {h1['n_games']-h1['away_dog_n']:,} | {h1['roi_away_fav']*100:+.2f}% |")
    W("")
    if h1["bucket_table"] is not None:
        W("**Home team ROI by implied probability bucket:**")
        W("")
        W("| Implied Prob | Games | Actual Win% | ROI |")
        W("|-------------|-------|-------------|-----|")
        for _, row in h1["bucket_table"].iterrows():
            if row["games"] > 0:
                W(f"| {row['home_implied_bucket']} | {int(row['games']):,} | "
                  f"{row['home_win_rate']*100:.1f}% | {row['roi_home_bet']*100:+.2f}% |")
    W("")

    # H2
    W("---")
    W("## Hypothesis 2: Home Field Advantage Pricing")
    W("*Prediction: Market correctly prices home advantage.*")
    W("")
    h2 = results["h2"]
    W(f"**Overall actual home win rate:** {h2['overall_actual']*100:.1f}%")
    W(f"**Overall implied home win rate:** {h2['overall_implied']*100:.1f}%")
    W(f"**Edge (actual - implied):** {(h2['overall_actual']-h2['overall_implied'])*100:+.2f}%")
    W("")
    W("| Season | Games | Actual Win% | Implied Win% | Edge% |")
    W("|--------|-------|-------------|-------------|-------|")
    for _, row in h2["by_season"].iterrows():
        W(f"| {int(row['season'])} | {int(row['games']):,} | "
          f"{row['actual_home_win%']}% | {row['implied_home_win%']}% | "
          f"{row['edge%']:+.2f}% |")
    W("")

    # H3
    W("---")
    W("## Hypothesis 3: Weather Effects on Totals")
    W("*Prediction: Cold and wind-in suppress runs; market may underadjust for wind.*")
    W("")
    h3 = results["h3"]
    W(f"**Games with total line:** {h3['n_with_total']:,}")
    if h3["overall_over_rate"]:
        W(f"**Overall over rate (excl. pushes):** {h3['overall_over_rate']*100:.1f}%")
    W(f"**Games with temperature data:** {h3['temp_n']:,}")
    W("")
    if h3["temp_table"] is not None:
        W("**Runs vs Line by Temperature:**")
        W("")
        W("| Temp | Games | Avg Runs | Avg Line | Runs vs Line | Over Rate% |")
        W("|------|-------|----------|----------|--------------|-----------|")
        for _, row in h3["temp_table"].iterrows():
            over = f"{row['over_rate']}%" if row["over_rate"] else "N/A"
            W(f"| {row['temp_bucket']} | {int(row['games']):,} | "
              f"{row['avg_runs']} | {row['avg_line']} | "
              f"{row['runs_vs_line']:+.2f} | {over} |")
    else:
        W(f"*Temperature data insufficient ({h3['temp_n']} games)  -  skip*")
    W("")
    if h3["wind_table"] is not None:
        W("**Over Rate by Wind Direction (>=10mph):**")
        W("")
        W("| Condition | Games | Avg Runs | Avg Line | Over Rate% |")
        W("|-----------|-------|----------|----------|-----------|")
        for _, row in h3["wind_table"].iterrows():
            over = f"{row['over_rate']}%" if row["over_rate"] else "N/A"
            W(f"| {row['condition']} | {int(row['games']):,} | "
              f"{row['avg_runs']} | {row['avg_line']} | {over} |")
    else:
        W(f"*Wind data insufficient ({h3.get('wind_n', 0)} games)  -  skip*")
    W("")

    # H4
    W("---")
    W("## Hypothesis 4: Starting Pitcher ERA vs Market Efficiency")
    W(f"*Prediction: ERA advantage was a stronger predictor in {h4_early} vs {h4_late}.*")
    W("")
    h4 = results["h4"]
    if not isinstance(h4, pd.DataFrame):
        W(f"*Not available: {h4.get('reason', 'unknown')}*")
        W("*Load pitcher stats with load_mlb_stats.py --season YYYY to enable this.*")
        W("*Note: Use --h4 flag for the rolling pre-game ERA version (no look-ahead bias).*")
    else:
        early = h4[h4["season"].isin(cfg["h4_early"])]
        late  = h4[h4["season"].isin(cfg["h4_late"])]
        W(f"**Total games with pitcher stats:** {len(h4):,}")
        W("*Strategy: Bet home when home ERA advantage > 1.5 runs*")
        W("*Note: This version uses same-game ERA (look-ahead bias). Run --h4 for the*")
        W("*methodologically correct rolling pre-game ERA version.*")
        W("")
        W("| Period | ERA-Edge Games | Home Win Rate | ROI |")
        W("|--------|---------------|---------------|-----|")
        for period_lbl, subset in [(h4_early, early), (h4_late, late)]:
            strong = subset[subset["home_era_edge"] > 1.5]
            if len(strong) > 0:
                win_rt   = round(strong["home_win"].mean()*100, 1)
                edge_roi = roi(strong["home_win"], strong["home_payout"])
                W(f"| {period_lbl} | {len(strong):,} | {win_rt}% | {edge_roi*100:+.2f}% |")
            else:
                W(f"| {period_lbl} | 0 | N/A | N/A |")
    W("")

    # H5
    W("---")
    W("## Hypothesis 5: Close Games and Over Bias")
    W("*Prediction: Close games (45-55% implied) have higher over rate due to extra innings.*")
    W("")
    h5 = results["h5"]
    W("| Category | Games | Extra Inn% | Over Rate% | Avg Runs | Avg Line |")
    W("|----------|-------|-----------|-----------|----------|----------|")
    for label, vals in h5.items():
        over = f"{vals['over_rate']}%" if vals["over_rate"] else "N/A"
        W(f"| {label} | {vals['games']:,} | "
          f"{vals['extra_inning_rate']}% | {over} | "
          f"{vals['avg_total_runs']} | {vals['avg_line']} |")
    W("")

    # H6
    W("---")
    W("## Hypothesis 6: Season-by-Season Structural Check")
    W("*Flags anomalous seasons that should be excluded from or weighted differently in models.*")
    W("")
    h6 = results["h6"]
    W("| Season | Games | Home Win% | Avg Implied% | Avg Total Runs | Extra Inn% |")
    W("|--------|-------|-----------|-------------|----------------|-----------|")
    for _, row in h6["by_season"].iterrows():
        s = int(row["season"])
        flag = ""
        if s == 2020: flag = " <- COVID shortened"
        if s == 2023: flag = " <- pitch clock introduced"
        W(f"| {s}{flag} | {int(row['games']):,} | "
          f"{row['home_win_rate']}% | {row['avg_implied']}% | "
          f"{row['avg_total_runs']} | {row['extra_inn_rate']}% |")
    W("")

    # Honest appraisal
    W("---")
    W("## Honest Appraisal: Predictions vs Findings")
    W("")
    W("*(Fill this in after reviewing the numbers above.)*")
    W("")
    W("**What to look for:**")
    W("")
    W("- **Favourite bias:** If home dog ROI is positive, the bias exists in this data.")
    W("  +2% over a large sample is marginal. +5%+ over 5,000+ games is meaningful.")
    W("  Check the <35% implied bucket specifically  -  that is where the overlay is largest.")
    W("")
    W("- **Home field pricing:** Edge% consistently positive = market underprices home teams.")
    W("  Near zero = efficient. A persistent +1-2% across seasons is the expected finding.")
    W("")
    W("- **Weather:** Over rate in <45F games below 48% = temperature suppresses scoring")
    W("  AND the market does not fully adjust. At 50% the market has priced it in.")
    W("")
    W(f"- **ERA vs market ({h4_early} vs {h4_late}):** Positive ROI early / negative late")
    W("  supports the hypothesis that analytics adoption eroded the simple ERA edge.")
    W("")
    W("- **Extra innings / close games:** Over rate above 52% in close games supports H5.")
    W("")
    W("- **2020/2023:** If their numbers deviate more than 3% from surrounding seasons,")
    W("  treat them as separate model regimes or exclude from combined training sets.")
    W("")

    os.makedirs(str(Path(out_path).parent), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"Report written to: {out_path}")


# -- Main ----------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        description="MLB Backtesting Analysis  -  SBRO 2015-2021, OddsWarehouse 2022-2025, or combined")
    p.add_argument("--db",        default=DEFAULT_DB)
    p.add_argument("--out",       default=DEFAULT_OUT)
    p.add_argument("--season",    type=int, help="Restrict to single season")
    p.add_argument("--bookmaker", default="sbro",
                   choices=["sbro","oddswarehouse","all"],
                   help="sbro (default, 2015-2021) | oddswarehouse (2022-2025) | all (2015-2025)")
    args = p.parse_args()

    cfg = BOOKMAKER_CONFIG[args.bookmaker]

    # Auto-name output by bookmaker when not using default
    if args.out == DEFAULT_OUT and args.bookmaker != "sbro":
        args.out = str(Path(REPORTS_DIR) / f"backtest_report_{args.bookmaker}.md")

    os.makedirs(str(Path(args.out).parent), exist_ok=True)

    print("Connecting to database...")
    con = connect(args.db)

    print(f"Loading core dataset ({cfg['label']}, {cfg['seasons_label']})...")
    df = pd.read_sql_query(build_core_sql(cfg["db_values"]), con)
    if args.season:
        df = df[df["season"] == args.season]

    print(f"  {len(df):,} games loaded")
    if len(df) == 0:
        print("ERROR: No games found. Verify odds are loaded for this bookmaker.")
        sys.exit(1)

    print("Running hypothesis tests...")
    results = {}
    print("  H1: Favourite bias...")
    results["h1"] = hypothesis_1_favourite_bias(df)
    print("  H2: Home field pricing...")
    results["h2"] = hypothesis_2_home_field(df)
    print("  H3: Weather and totals...")
    results["h3"] = hypothesis_3_weather_totals(df)
    print("  H4: SP ERA vs market efficiency...")
    results["h4"] = hypothesis_4_sp_era_vs_market(df, con)
    print("  H5: Close games / extra innings...")
    results["h5"] = hypothesis_5_extra_innings_totals(df)
    print("  H6: Season structural check...")
    results["h6"] = hypothesis_6_outlier_check(df, cfg)

    print("Writing report...")
    write_report(results, args.out, cfg)

    print("\n" + "="*60)
    print(f"  QUICK SUMMARY  [{cfg['label']}  {cfg['seasons_label']}]")
    print("="*60)
    h1 = results["h1"]
    print(f"  Home underdog ROI:  {h1['roi_home_dog']*100:+.2f}%  (n={h1['home_dog_n']:,})")
    print(f"  Away underdog ROI:  {h1['roi_away_dog']*100:+.2f}%  (n={h1['away_dog_n']:,})")
    h2 = results["h2"]
    print(f"  Home win rate:      {h2['overall_actual']*100:.1f}%  (implied: {h2['overall_implied']*100:.1f}%)")
    h3 = results["h3"]
    if h3["overall_over_rate"]:
        print(f"  Overall over rate:  {h3['overall_over_rate']*100:.1f}%")
    print(f"  Full report:        {args.out}")
    print("="*60)
    con.close()


# ??????????????????????????????????????????????????????????????????????????????
# H4 REBUILD  -  Rolling Pre-Game ERA (methodologically correct, no look-ahead)
# ??????????????????????????????????????????????????????????????????????????????

def hypothesis_4_rolling_era(con, db_values, cfg):
    """
    Rolling ERA computed in Python (not SQL) to avoid a quadratic self-join
    that causes SQLite to hang on large datasets.

    Approach:
      1. Pull all per-game team pitching totals in one flat query (~2s).
      2. Sort by season + date and use cumsum per team to build
         season-to-date IP and ER entering each game.
      3. Join onto the games+odds table.

    ERA entering game N = 9 * cumulative_ER_through_game_(N-1)
                              / cumulative_IP_through_game_(N-1)
    Minimum 15 IP before the game is required (same threshold as before).
    """
    bm = bm_in_clause(db_values)

    # Step 1: all team pitching lines, one row per team per game
    pitching_sql = """
    SELECT
        pgs.game_pk,
        g.season,
        g.game_date,
        pgs.team_id,
        SUM(pgs.innings_pitched) AS ip,
        SUM(pgs.earned_runs)     AS er
    FROM player_game_stats pgs
    JOIN games g ON g.game_pk = pgs.game_pk
    WHERE pgs.player_role     = 'pitcher'
    AND   pgs.innings_pitched > 0
    AND   g.game_type         = 'R'
    AND   g.status            = 'Final'
    GROUP BY pgs.game_pk, g.season, g.game_date, pgs.team_id
    ORDER BY g.season, g.game_date, pgs.game_pk
    """

    # Step 2: games + odds
    games_sql = f"""
    SELECT
        g.game_pk,
        g.season,
        g.game_date,
        g.home_score,
        g.away_score,
        g.home_team_id,
        g.away_team_id,
        ml.home_ml,
        ml.away_ml
    FROM games g
    LEFT JOIN game_odds ml
        ON  ml.game_pk        = g.game_pk
        AND ml.bookmaker       {bm}
        AND ml.market_type    = 'moneyline'
        AND ml.is_closing_line = 1
    WHERE g.game_type = 'R'
    AND   g.status    = 'Final'
    AND   ml.home_ml  IS NOT NULL
    ORDER BY g.season, g.game_date
    """

    print("    Loading pitching lines...")
    pit = pd.read_sql_query(pitching_sql, con)
    print("    Loading games + odds...")
    gms = pd.read_sql_query(games_sql, con)

    if pit.empty:
        return {"available": False, "reason": "No pitcher stats loaded yet"}

    # Step 3: build season-to-date cumulative ERA entering each game (Python)
    # Sort: season -> team -> date -> game_pk (tie-break for DH same date)
    pit = pit.sort_values(["season", "team_id", "game_date", "game_pk"])

    # cumsum gives totals THROUGH each game; shift(1) gives totals BEFORE each game
    pit["cum_ip"] = pit.groupby(["season", "team_id"])["ip"].cumsum()
    pit["cum_er"] = pit.groupby(["season", "team_id"])["er"].cumsum()
    pit["pre_ip"] = pit.groupby(["season", "team_id"])["cum_ip"].shift(1).fillna(0)
    pit["pre_er"] = pit.groupby(["season", "team_id"])["cum_er"].shift(1).fillna(0)

    # Only keep rows where team has >= 15 IP entering the game
    pit = pit[pit["pre_ip"] >= 15].copy()
    pit["rolling_era"] = 9.0 * pit["pre_er"] / pit["pre_ip"]

    era_lookup = pit.set_index(["game_pk", "team_id"])["rolling_era"]

    # Step 4: attach home and away rolling ERA to each game
    print("    Attaching rolling ERA to games...")
    gms["home_rolling_era"] = gms.apply(
        lambda r: era_lookup.get((r["game_pk"], r["home_team_id"]), np.nan), axis=1)
    gms["away_rolling_era"] = gms.apply(
        lambda r: era_lookup.get((r["game_pk"], r["away_team_id"]), np.nan), axis=1)

    df = gms.dropna(subset=["home_rolling_era", "away_rolling_era"]).copy()

    if df.empty or len(df) < 100:
        return {"available": False, "reason": f"Insufficient data ({len(df)} rows)"}

    df["home_win"]      = (df["home_score"] > df["away_score"]).astype(int)
    df["era_advantage"] = df["away_rolling_era"] - df["home_rolling_era"]
    df["home_payout"]   = american_to_payout(df["home_ml"])
    df["away_payout"]   = american_to_payout(df["away_ml"])

    early = df[df["season"].isin(cfg["h4_early"])]
    late  = df[df["season"].isin(cfg["h4_late"])]

    results = {"available": True, "total_games": len(df)}
    for label, subset in [
        (cfg["h4_early_lbl"], early),
        (cfg["h4_late_lbl"],  late),
        ("All seasons",       df),
    ]:
        edge = subset[subset["era_advantage"] > 1.0]
        results[label] = {
            "total_games":       len(subset),
            "era_edge_games":    len(edge),
            "era_edge_win_rate": round(edge["home_win"].mean()*100, 1) if len(edge) > 0 else None,
            "era_edge_roi":      roi(edge["home_win"], edge["home_payout"]) if len(edge) > 0 else None,
            "baseline_win_rate": round(subset["home_win"].mean()*100, 1),
            "baseline_roi":      roi(subset["home_win"], subset["home_payout"]),
        }

    df["era_bucket"] = pd.cut(df["era_advantage"],
                              bins=[-20, -2, -1, 0, 1, 2, 20],
                              labels=["<<-2","-2to-1","-1to0","0to+1","+1to+2",">>+2"])
    results["bucket_table"] = (
        df.groupby("era_bucket", observed=True)
          .apply(lambda x: pd.Series({
              "games":         len(x),
              "home_win_rate": round(x["home_win"].mean()*100, 1),
              "roi":           round(roi(x["home_win"], x["home_payout"])*100, 2),
          }))
          .reset_index()
    )
    return results


def run_h4_standalone():
    p = argparse.ArgumentParser()
    p.add_argument("--db",        default=DEFAULT_DB)
    p.add_argument("--out",       default=str(Path(REPORTS_DIR) / "h4_rolling_era_report.md"))
    p.add_argument("--bookmaker", default="sbro",
                   choices=["sbro","oddswarehouse","all"])
    args = p.parse_args()

    cfg = BOOKMAKER_CONFIG[args.bookmaker]
    if args.bookmaker != "sbro" and "h4_rolling_era_report.md" in args.out:
        args.out = str(Path(REPORTS_DIR) / f"h4_rolling_era_report_{args.bookmaker}.md")

    os.makedirs(str(Path(args.out).parent), exist_ok=True)
    con = db_connect(args.db)
    con.row_factory = sqlite3.Row

    print(f"Running H4 rolling ERA [{cfg['label']} {cfg['seasons_label']}]...")
    result = hypothesis_4_rolling_era(con, cfg["db_values"], cfg)
    con.close()

    lines = []
    lines.append("# H4 Rebuild: Rolling Pre-Game ERA vs Closing Moneyline")
    lines.append(f"## Source: {cfg['label']}  |  {cfg['seasons_label']}")
    lines.append("## Strategy: Bet home team when season-to-date ERA advantage > 1.0 run")
    lines.append("")
    lines.append("> ERA computed from all prior games in same season (minimum 15 IP).")
    lines.append("> This is a genuine pre-game predictor with no look-ahead bias.")
    lines.append("")

    if not result.get("available"):
        lines.append(f"**Not available:** {result.get('reason')}")
    else:
        lines.append(f"**Total games with valid rolling ERA:** {result['total_games']:,}")
        lines.append("")
        lines.append("| Period | All Games | ERA-Edge Games | Edge Win Rate | Edge ROI | Baseline ROI |")
        lines.append("|--------|-----------|---------------|--------------|----------|-------------|")
        for period in [cfg["h4_early_lbl"], cfg["h4_late_lbl"], "All seasons"]:
            r = result[period]
            if r["era_edge_roi"] is not None:
                lines.append(
                    f"| {period} | {r['total_games']:,} | {r['era_edge_games']:,} | "
                    f"{r['era_edge_win_rate']}% | {r['era_edge_roi']*100:+.2f}% | "
                    f"{r['baseline_roi']*100:+.2f}% |"
                )
            else:
                lines.append(
                    f"| {period} | {r['total_games']:,} | 0 | N/A | N/A | "
                    f"{r['baseline_roi']*100:+.2f}% |"
                )
        lines.append("")
        lines.append("**ERA advantage bucket breakdown:**")
        lines.append("")
        lines.append("| ERA Advantage | Games | Home Win% | ROI |")
        lines.append("|--------------|-------|-----------|-----|")
        for _, row in result["bucket_table"].iterrows():
            if row["games"] > 0:
                lines.append(f"| {row['era_bucket']} | {int(row['games']):,} | "
                              f"{row['home_win_rate']}% | {row['roi']:+.2f}% |")
        lines.append("")
        lines.append("## Interpretation")
        lines.append("")
        lines.append("If ERA advantage correlates with win rate across buckets, the signal is real.")
        lines.append("If ERA advantage ROI exceeds baseline ROI, it is predictive beyond random.")
        lines.append(f"If {cfg['h4_early_lbl']} ROI exceeds {cfg['h4_late_lbl']} ROI, "
                     "the edge has eroded  -  consistent with analytics adoption.")

    with open(args.out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"Report written to {args.out}")
    if result.get("available"):
        for period in [cfg["h4_early_lbl"], cfg["h4_late_lbl"], "All seasons"]:
            r = result[period]
            if r["era_edge_roi"] is not None:
                print(f"  {period}: {r['era_edge_games']} ERA-edge games, "
                      f"ROI {r['era_edge_roi']*100:+.2f}%")
            else:
                print(f"  {period}: no data")


# ==============================================================================
# MULTIVARIATE COMBINATION ANALYSIS  (added previous turn)
# ==============================================================================

def multivariate_analysis(df):
    d = df.dropna(subset=["home_ml","away_ml","total_line",
                           "home_score","away_score",
                           "wind_direction","wind_mph"]).copy()
    d["home_win"]     = (d["home_score"] > d["away_score"]).astype(int)
    d["total_runs"]   = d["home_score"] + d["away_score"]
    d["push"]         = (d["total_runs"] == d["total_line"]).astype(int)
    d["over_result"]  = (d["total_runs"] > d["total_line"]).astype(int)
    d["home_implied"] = american_to_implied(d["home_ml"])
    d["home_payout"]  = american_to_payout(d["home_ml"])
    d["away_payout"]  = american_to_payout(d["away_ml"])
    wdir = d["wind_direction"].str.lower()
    d["wind_out_flag"] = (wdir.str.contains("out|leftfield|rightfield", na=False) &
                          (d["wind_mph"] >= 10)).astype(int)
    d["wind_in_flag"]  = (wdir.str.contains("in|center|centerfield", na=False) &
                          (d["wind_mph"] >= 10)).astype(int)
    d["calm_flag"]     = (d["wind_mph"] < 10).astype(int)
    bins   = [0, 0.35, 0.40, 0.45, 0.50, 1.01]
    labels = ["<35%","35-40%","40-45%","45-50%",">50% (fav)"]
    d["home_bucket"] = pd.cut(d["home_implied"], bins=bins, labels=labels)
    d["cold_flag"]   = (d["temp_f"] < 45).astype(int) if "temp_f" in d.columns else 0
    d["fav_mid_flag"] = ((d["home_ml"] >= -160) & (d["home_ml"] <= -130)).astype(int)
    results = {}
    matrix_rows = []
    for wind_label, wind_col in [("Wind out >=10mph","wind_out_flag"),
                                  ("Wind in  >=10mph","wind_in_flag"),
                                  ("Calm <10mph","calm_flag")]:
        wind_df = d[d[wind_col] == 1]
        for bucket in labels:
            sub = wind_df[wind_df["home_bucket"] == bucket]
            if len(sub) < 15: continue
            no_push = sub[sub["push"] == 0]
            ml_roi  = roi(sub["home_win"], sub["home_payout"])
            over_rt = no_push["over_result"].mean()*100 if len(no_push) > 0 else None
            matrix_rows.append({"wind_condition": wind_label,"implied_bucket": bucket,
                "games": len(sub),"home_win_rate": round(sub["home_win"].mean()*100,1),
                "ml_roi": round(ml_roi*100,2),
                "over_rate": round(over_rt,1) if over_rt is not None else None})
    results["matrix"] = pd.DataFrame(matrix_rows)
    mv_b = d[(d["wind_out_flag"]==1) & (d["home_implied"] < 0.50)]
    mv_b_no_push = mv_b[mv_b["push"]==0]
    results["mv_b"] = {"label":"Wind out >=10mph + home dog (<50% implied)","games":len(mv_b),
        "home_win_rate": round(mv_b["home_win"].mean()*100,1) if len(mv_b)>0 else None,
        "ml_roi": round(roi(mv_b["home_win"],mv_b["home_payout"])*100,2) if len(mv_b)>0 else None,
        "over_rate": round(mv_b_no_push["over_result"].mean()*100,1) if len(mv_b_no_push)>0 else None,
        "baseline_over": round(d[d["push"]==0]["over_result"].mean()*100,1)}
    mv_b_rows = []
    for bucket in labels[:-1]:
        sub = d[(d["wind_out_flag"]==1) & (d["home_bucket"]==bucket)]
        if len(sub) < 10: continue
        no_push = sub[sub["push"]==0]
        mv_b_rows.append({"bucket":bucket,"games":len(sub),
            "ml_roi":round(roi(sub["home_win"],sub["home_payout"])*100,2),
            "over_rate":round(no_push["over_result"].mean()*100,1) if len(no_push)>0 else None,
            "home_win_rate":round(sub["home_win"].mean()*100,1)})
    results["mv_b_detail"] = pd.DataFrame(mv_b_rows)
    cold_df = d.dropna(subset=["temp_f"])
    mv_c = cold_df[(cold_df["wind_out_flag"]==1) & (cold_df["temp_f"]<45)]
    mv_c_no_push = mv_c[mv_c["push"]==0]
    cold_only = cold_df[cold_df["temp_f"]<45]
    cold_no_push = cold_only[cold_only["push"]==0]
    results["mv_c"] = {"label":"Wind out >=10mph + cold (<45F)","games":len(mv_c),
        "over_rate":round(mv_c_no_push["over_result"].mean()*100,1) if len(mv_c_no_push)>0 else None,
        "avg_runs":round(mv_c["total_runs"].mean(),2) if len(mv_c)>0 else None,
        "avg_line":round(mv_c["total_line"].mean(),2) if len(mv_c)>0 else None,
        "runs_vs_line":round((mv_c["total_runs"]-mv_c["total_line"]).mean(),2) if len(mv_c)>0 else None,
        "cold_baseline_over":round(cold_no_push["over_result"].mean()*100,1) if len(cold_no_push)>0 else None,
        "cold_baseline_games":len(cold_only),
        "wind_out_baseline_over":round(d[(d["wind_out_flag"]==1)&(d["push"]==0)]["over_result"].mean()*100,1)
            if len(d[d["wind_out_flag"]==1])>0 else None}
    mv_f = d[(d["wind_in_flag"]==1) & (d["fav_mid_flag"]==1)]
    mv_f_no_push = mv_f[mv_f["push"]==0]
    results["mv_f"] = {"label":"Wind in >=10mph + home fav (-130 to -160)","games":len(mv_f),
        "home_win_rate":round(mv_f["home_win"].mean()*100,1) if len(mv_f)>0 else None,
        "ml_roi":round(roi(mv_f["home_win"],mv_f["home_payout"])*100,2) if len(mv_f)>0 else None,
        "over_rate":round(mv_f_no_push["over_result"].mean()*100,1) if len(mv_f_no_push)>0 else None}
    mv_g = cold_df[(cold_df["temp_f"]<45) & (cold_df["home_implied"]<0.40)]
    results["mv_g"] = {"label":"Cold (<45F) + home dog (<40% implied)","games":len(mv_g),
        "home_win_rate":round(mv_g["home_win"].mean()*100,1) if len(mv_g)>0 else None,
        "ml_roi":round(roi(mv_g["home_win"],mv_g["home_payout"])*100,2) if len(mv_g)>0 else None,
        "over_rate":round(mv_g[mv_g["push"]==0]["over_result"].mean()*100,1) if len(mv_g)>0 else None}
    mv_h = cold_df[(cold_df["wind_out_flag"]==1)&(cold_df["temp_f"]<45)&(cold_df["home_implied"]<0.40)]
    mv_h_no_push = mv_h[mv_h["push"]==0]
    results["mv_h"] = {"label":"3-way: wind out + cold (<45F) + home dog (<40% implied)","games":len(mv_h),
        "home_win_rate":round(mv_h["home_win"].mean()*100,1) if len(mv_h)>0 else None,
        "ml_roi":round(roi(mv_h["home_win"],mv_h["home_payout"])*100,2) if len(mv_h)>0 else None,
        "over_rate":round(mv_h_no_push["over_result"].mean()*100,1) if len(mv_h_no_push)>0 else None}
    all_no_push = d[d["push"]==0]
    results["baseline"] = {"total_games":len(d),
        "overall_over":round(all_no_push["over_result"].mean()*100,1),
        "overall_home_win":round(d["home_win"].mean()*100,1),
        "overall_ml_roi":round(roi(d["home_win"],d["home_payout"])*100,2)}
    return results


def write_multivariate_report(mv, out_path, cfg):
    lines = []
    W = lines.append
    W("# MLB Multivariate Combination Analysis")
    W(f"## {cfg['label']}  |  {cfg['seasons_label']}")
    W("")
    W("> Signal combinations using weather x pricing interactions.")
    W("> All ROI figures include full vig. Threshold for net profitability: ~+4.5% ROI.")
    W("")
    base = mv["baseline"]
    W(f"**Total games in sample:** {base['total_games']:,}")
    W(f"**Baseline overall over rate:** {base['overall_over']}%")
    W(f"**Baseline home win rate:** {base['overall_home_win']}%")
    W(f"**Baseline home ML ROI:** {base['overall_ml_roi']:+.2f}%")
    W("")
    W("---")
    W("## Section 1: Wind Condition x Implied Probability Matrix (MV-A/D/E)")
    W("| Wind Condition | Implied Bucket | Games | Home Win% | ML ROI | Over Rate% |")
    W("|----------------|---------------|-------|-----------|--------|-----------|")
    for _, row in mv["matrix"].iterrows():
        over = f"{row['over_rate']}%" if row["over_rate"] is not None else "N/A"
        W(f"| {row['wind_condition']} | {row['implied_bucket']} | {int(row['games']):,} | "
          f"{row['home_win_rate']}% | {row['ml_roi']:+.2f}% | {over} |")
    W("")
    W("---")
    W("## Section 2: Wind Out x Home Dog (MV-B)")
    b = mv["mv_b"]
    W(f"**{b['label']}**")
    W(f"- Games: {b['games']:,}")
    if b["games"] > 0:
        W(f"- Home win rate: {b['home_win_rate']}%  |  ML ROI: {b['ml_roi']:+.2f}%  |  Over rate: {b['over_rate']}%  (baseline: {b['baseline_over']}%)")
    if not mv["mv_b_detail"].empty:
        W("")
        W("| Bucket | Games | Home Win% | ML ROI | Over Rate% |")
        W("|--------|-------|-----------|--------|-----------|")
        for _, row in mv["mv_b_detail"].iterrows():
            over = f"{row['over_rate']}%" if row["over_rate"] is not None else "N/A"
            W(f"| {row['bucket']} | {int(row['games']):,} | {row['home_win_rate']}% | {row['ml_roi']:+.2f}% | {over} |")
    W("")
    for key, title in [("mv_c","Section 3: Wind Out x Cold (MV-C)"),
                        ("mv_f","Section 4: Wind In x Home Fav -130/-160 (MV-F)"),
                        ("mv_g","Section 5: Cold x Home Dog (MV-G)"),
                        ("mv_h","Section 6: 3-Way Wind Out + Cold + Dog (MV-H)")]:
        W("---")
        W(f"## {title}")
        r = mv[key]
        W(f"**{r['label']}**  |  Games: {r['games']:,}")
        if r["games"] > 0:
            W(f"- Home win rate: {r.get('home_win_rate','N/A')}%  |  ML ROI: {r.get('ml_roi', 'N/A')}  |  Over rate: {r.get('over_rate','N/A')}%")
        W("")
    os.makedirs(str(Path(out_path).parent), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"Report written to {out_path}")


def run_multivariate_standalone():
    p = argparse.ArgumentParser()
    p.add_argument("--db",        default=DEFAULT_DB)
    p.add_argument("--bookmaker", default="sbro", choices=["sbro","oddswarehouse","all"])
    args = p.parse_args()
    cfg = BOOKMAKER_CONFIG[args.bookmaker]
    suffix = "" if args.bookmaker == "sbro" else f"_{args.bookmaker}"
    out_path = str(Path(REPORTS_DIR) / f"backtest_report_multivariate{suffix}.md")
    print(f"Running multivariate analysis [{cfg['label']} {cfg['seasons_label']}]...")
    con = connect(args.db)
    df = pd.read_sql_query(build_core_sql(cfg["db_values"]), con)
    con.close()
    print(f"  {len(df):,} games loaded")
    if len(df) == 0:
        print("ERROR: No games found.")
        sys.exit(1)
    mv = multivariate_analysis(df)
    write_multivariate_report(mv, out_path, cfg)
    base = mv["baseline"]
    print(f"\n{'='*60}")
    print(f"  MULTIVARIATE QUICK SUMMARY  [{cfg['label']}]")
    print(f"{'='*60}")
    print(f"  Baseline over rate:   {base['overall_over']}%")
    print(f"  Baseline home ML ROI: {base['overall_ml_roi']:+.2f}%")
    b = mv["mv_b"]
    if b["games"] > 0:
        print(f"  MV-B (wind out + home dog): {b['games']} games | over {b['over_rate']}% | ML ROI {b['ml_roi']:+.2f}%")
    print(f"{'='*60}")


# ==============================================================================
# STREAK ANALYSIS  (S1 + S2: team win/loss streaks vs market pricing)
# ==============================================================================

def build_team_streaks(con, db_values):
    """
    Build a per-game streak table for every team.
    Returns a DataFrame with columns:
        game_pk, team_id, game_date, season,
        streak_len   (positive = win streak, negative = loss streak)
        is_home      (1 if this team is the home team)
    No look-ahead bias: streak entering game N is built from games 1..N-1.
    """
    bm = bm_in_clause(db_values)
    games_sql = f"""
    SELECT
        g.game_pk,
        g.season,
        g.game_date,
        g.home_team_id,
        g.away_team_id,
        g.home_score,
        g.away_score,
        ml.home_ml,
        ml.away_ml,
        tot.total_line,
        tot.over_odds,
        tot.under_odds,
        g.temp_f,
        g.wind_mph,
        g.wind_direction
    FROM games g
    LEFT JOIN game_odds ml
        ON  ml.game_pk = g.game_pk
        AND ml.bookmaker {bm}
        AND ml.market_type = 'moneyline'
        AND ml.is_closing_line = 1
    LEFT JOIN game_odds tot
        ON  tot.game_pk = g.game_pk
        AND tot.bookmaker {bm}
        AND tot.market_type = 'total'
        AND tot.is_closing_line = 1
    WHERE g.game_type = 'R'
    AND   g.status    = 'Final'
    AND   g.home_score IS NOT NULL
    AND   ml.home_ml   IS NOT NULL
    ORDER BY g.game_date, g.game_pk
    """
    gms = pd.read_sql_query(games_sql, con)

    # FIX: The LEFT JOINs on game_odds can return multiple rows per game_pk
    # when multiple closing-line records exist (e.g. line moves, data quirks).
    # Deduplicate to one row per game_pk immediately after loading, keeping
    # the first occurrence. This brings counts back to ~1 row per game.
    gms = gms.drop_duplicates(subset=["game_pk"]).reset_index(drop=True)

    # Build per-team game list (one row per team per game)
    # We only need game_pk, season, game_date, team_id, and scores for the
    # streak calculation — keep it lean to avoid carrying duplicate cols.
    home_rows = gms[["game_pk","season","game_date","home_team_id",
                      "home_score","away_score"]].copy()
    home_rows.columns = ["game_pk","season","game_date","team_id","team_score","opp_score"]
    home_rows["is_home"] = 1

    away_rows = gms[["game_pk","season","game_date","away_team_id",
                      "away_score","home_score"]].copy()
    away_rows.columns = ["game_pk","season","game_date","team_id","team_score","opp_score"]
    away_rows["is_home"] = 0

    team_games = pd.concat([home_rows, away_rows]).sort_values(
        ["team_id","game_date","game_pk"]).reset_index(drop=True)

    team_games["win"] = (team_games["team_score"] > team_games["opp_score"]).astype(int)

    # Streak entering each game: cumulative W/L sequence before this game
    def calc_streak(group):
        wins = group["win"].values
        streaks = np.zeros(len(wins), dtype=int)
        for i in range(1, len(wins)):
            if wins[i-1] == 1:
                streaks[i] = max(streaks[i-1], 0) + 1
            else:
                streaks[i] = min(streaks[i-1], 0) - 1
        group = group.copy()
        group["streak_entering"] = streaks
        return group

    team_games = team_games.groupby("team_id", group_keys=False).apply(calc_streak)

    # FIX: Deduplicate streak lookup tables before merging back.
    # The concat of home+away produces 2 rows per game_pk per team;
    # after groupby-apply some duplicates may persist. Keep first per game_pk.
    home_streaks = (team_games[team_games["is_home"] == 1]
                    [["game_pk","streak_entering"]]
                    .drop_duplicates(subset=["game_pk"])
                    .rename(columns={"streak_entering":"home_streak"}))
    away_streaks = (team_games[team_games["is_home"] == 0]
                    [["game_pk","streak_entering"]]
                    .drop_duplicates(subset=["game_pk"])
                    .rename(columns={"streak_entering":"away_streak"}))

    gms = gms.merge(home_streaks, on="game_pk", how="left")
    gms = gms.merge(away_streaks, on="game_pk", how="left")

    return gms


def streak_analysis(con, db_values, cfg):
    """
    S1: Home team win streak over-reaction
    S2: Home team loss streak over-reaction
    S3: Away team loss streak (home playing vs struggling opponent)
    S4: Combined — home on win streak vs away on loss streak
    S5: Implied probability movement on streak games vs baseline
    """
    print("    Building team streak table...")
    gms = build_team_streaks(con, db_values)
    gms = gms.dropna(subset=["home_ml","away_ml"]).copy()

    gms["home_win"]     = (gms["home_score"] > gms["away_score"]).astype(int)
    gms["home_implied"] = american_to_implied(gms["home_ml"])
    gms["home_payout"]  = american_to_payout(gms["home_ml"])
    gms["away_payout"]  = american_to_payout(gms["away_ml"])
    gms["total_runs"]   = gms["home_score"] + gms["away_score"]

    has_total = gms.dropna(subset=["total_line","home_score","away_score"]).copy()
    has_total["over_result"] = (has_total["total_runs"] > has_total["total_line"]).astype(int)
    has_total["push"]        = (has_total["total_runs"] == has_total["total_line"]).astype(int)
    has_total["over_payout"] = american_to_payout(has_total["over_odds"].fillna(-110))

    results = {}

    # ── Baseline ──────────────────────────────────────────────────────────────
    results["baseline"] = {
        "games":            len(gms),
        "home_win_rate":    round(gms["home_win"].mean()*100, 1),
        "overall_ml_roi":   round(roi(gms["home_win"], gms["home_payout"])*100, 2),
        "avg_implied":      round(gms["home_implied"].mean()*100, 1),
    }

    # ── S1/S2: streak buckets ─────────────────────────────────────────────────
    streak_buckets = {
        "W3+":  gms[gms["home_streak"] >= 3],
        "W5+":  gms[gms["home_streak"] >= 5],
        "W7+":  gms[gms["home_streak"] >= 7],
        "W10+": gms[gms["home_streak"] >= 10],
        "L3+":  gms[gms["home_streak"] <= -3],
        "L5+":  gms[gms["home_streak"] <= -5],
        "L7+":  gms[gms["home_streak"] <= -7],
        "W1-2": gms[(gms["home_streak"] >= 1) & (gms["home_streak"] <= 2)],
        "L1-2": gms[(gms["home_streak"] >= -2) & (gms["home_streak"] <= -1)],
        "None": gms[gms["home_streak"] == 0],
    }

    streak_rows = []
    for label, sub in streak_buckets.items():
        if len(sub) < 20:
            continue
        streak_rows.append({
            "streak":         label,
            "games":          len(sub),
            "home_win_rate":  round(sub["home_win"].mean()*100, 1),
            "ml_roi":         round(roi(sub["home_win"], sub["home_payout"])*100, 2),
            "avg_implied":    round(sub["home_implied"].mean()*100, 1),
            "implied_delta":  round((sub["home_implied"].mean() - gms["home_implied"].mean())*100, 2),
        })
    results["s1_s2_table"] = pd.DataFrame(streak_rows)

    # ── S3: away team loss streak (home team facing a struggling opponent) ────
    away_streak_rows = []
    for label, thresh in [("Opp L3+", -3), ("Opp L5+", -5), ("Opp L7+", -7)]:
        sub = gms[gms["away_streak"] <= thresh]
        if len(sub) < 20:
            continue
        away_streak_rows.append({
            "condition":     label,
            "games":         len(sub),
            "home_win_rate": round(sub["home_win"].mean()*100, 1),
            "ml_roi":        round(roi(sub["home_win"], sub["home_payout"])*100, 2),
            "avg_implied":   round(sub["home_implied"].mean()*100, 1),
        })
    results["s3_table"] = pd.DataFrame(away_streak_rows)

    # ── S4: combined — home on win streak vs away on loss streak ─────────────
    combined_rows = []
    for hw, al in [(3, -3), (5, -5), (7, -5), (5, -7)]:
        sub = gms[(gms["home_streak"] >= hw) & (gms["away_streak"] <= al)]
        if len(sub) < 10:
            continue
        label = f"H W{hw}+ vs A L{abs(al)}+"
        combined_rows.append({
            "condition":     label,
            "games":         len(sub),
            "home_win_rate": round(sub["home_win"].mean()*100, 1),
            "ml_roi":        round(roi(sub["home_win"], sub["home_payout"])*100, 2),
            "avg_implied":   round(sub["home_implied"].mean()*100, 1),
        })
    results["s4_table"] = pd.DataFrame(combined_rows)

    # ── S5: totals behaviour on home team streak games ────────────────────────
    total_rows = []
    has_total_copy = has_total.copy()
    for label, sub_mask in [
        ("W3+",  has_total_copy["home_streak"] >= 3),
        ("W5+",  has_total_copy["home_streak"] >= 5),
        ("L3+",  has_total_copy["home_streak"] <= -3),
        ("L5+",  has_total_copy["home_streak"] <= -5),
        ("All",  pd.Series([True]*len(has_total_copy), index=has_total_copy.index)),
    ]:
        sub = has_total_copy[sub_mask]
        if len(sub) < 20:
            continue
        no_push = sub[sub["push"] == 0]
        total_rows.append({
            "streak":        label,
            "games":         len(sub),
            "avg_runs":      round(sub["total_runs"].mean(), 2),
            "avg_line":      round(sub["total_line"].mean(), 2),
            "runs_vs_line":  round((sub["total_runs"] - sub["total_line"]).mean(), 2),
            "over_rate":     round(no_push["over_result"].mean()*100, 1) if len(no_push) > 0 else None,
        })
    results["s5_totals"] = pd.DataFrame(total_rows)

    # ── Season-by-season stability check on W5+ ───────────────────────────────
    w5 = gms[gms["home_streak"] >= 5]
    if len(w5) > 0:
        season_rows = []
        for season, grp in w5.groupby("season"):
            if len(grp) < 5:
                continue
            season_rows.append({
                "season":        int(season),
                "games":         len(grp),
                "home_win_rate": round(grp["home_win"].mean()*100, 1),
                "ml_roi":        round(roi(grp["home_win"], grp["home_payout"])*100, 2),
                "avg_implied":   round(grp["home_implied"].mean()*100, 1),
            })
        results["s1_by_season"] = pd.DataFrame(season_rows)
    else:
        results["s1_by_season"] = pd.DataFrame()

    return results


def write_streak_report(sr, out_path, cfg):
    lines = []
    W = lines.append

    W("# MLB Streak Analysis Report")
    W(f"## {cfg['label']}  |  {cfg['seasons_label']}")
    W("")
    W("> Tests whether the betting market over-reacts or under-reacts to team win/loss streaks.")
    W("> Streak entering game N = consecutive W or L results through game N-1.")
    W("> Positive streak = win streak length. Negative = loss streak length.")
    W("> All ROI figures include full bookmaker vig. Profitability threshold: ~+4.5%.")
    W("")

    base = sr["baseline"]
    W(f"**Total games in sample:** {base['games']:,}")
    W(f"**Baseline home win rate:** {base['home_win_rate']}%")
    W(f"**Baseline home ML ROI:** {base['overall_ml_roi']:+.2f}%")
    W(f"**Baseline avg home implied probability:** {base['avg_implied']}%")
    W("")

    # S1/S2
    W("---")
    W("## S1 + S2: Home Team Win and Loss Streak vs Baseline")
    W("*Strategy: bet home team on win streak (S1) or bet against home team on loss streak (S2).*")
    W("*Implied delta = how much the market moved the line vs the overall average.*")
    W("*A positive implied delta means the market priced the hot team higher than usual.*")
    W("")
    W("| Streak Entering Game | Games | Home Win% | Home ML ROI | Avg Implied% | Implied Delta |")
    W("|---------------------|-------|-----------|-------------|-------------|--------------|")
    if not sr["s1_s2_table"].empty:
        for _, row in sr["s1_s2_table"].iterrows():
            W(f"| {row['streak']} | {int(row['games']):,} | {row['home_win_rate']}% | "
              f"{row['ml_roi']:+.2f}% | {row['avg_implied']}% | {row['implied_delta']:+.2f}pp |")
    W("")
    W("> **Interpretation guide:**")
    W("> - If W5+ ROI is significantly NEGATIVE: market over-reacts to win streaks (overprices hot teams).")
    W("> - If W5+ ROI is significantly POSITIVE: market under-reacts (momentum is real and unprice).")
    W("> - If implied delta is positive but ROI is negative: market moved line but moved it too far.")
    W("> - L streak ROI: negative = market over-discounts cold teams; positive = regression to mean real.")
    W("")

    # S3
    W("---")
    W("## S3: Home Team Facing a Struggling Opponent (Away on Loss Streak)")
    W("*Does the home team ML ROI improve when the visiting team is on a losing streak?*")
    W("*If the market under-discounts the away team's struggles, home ML value emerges.*")
    W("")
    W("| Condition | Games | Home Win% | Home ML ROI | Avg Implied% |")
    W("|-----------|-------|-----------|-------------|-------------|")
    if not sr["s3_table"].empty:
        for _, row in sr["s3_table"].iterrows():
            W(f"| {row['condition']} | {int(row['games']):,} | {row['home_win_rate']}% | "
              f"{row['ml_roi']:+.2f}% | {row['avg_implied']}% |")
    W("")

    # S4
    W("---")
    W("## S4: Combined — Home on Win Streak vs Away on Loss Streak")
    W("*Both signals stacking simultaneously. Tests whether combination amplifies any edge.*")
    W("")
    W("| Condition | Games | Home Win% | Home ML ROI | Avg Implied% |")
    W("|-----------|-------|-----------|-------------|-------------|")
    if not sr["s4_table"].empty:
        for _, row in sr["s4_table"].iterrows():
            W(f"| {row['condition']} | {int(row['games']):,} | {row['home_win_rate']}% | "
              f"{row['ml_roi']:+.2f}% | {row['avg_implied']}% |")
    W("")

    # S5
    W("---")
    W("## S5: Totals Behaviour on Home Team Streak Games")
    W("*Does scoring volume change when home teams are on a streak?*")
    W("*A positive runs-vs-line means actual scoring exceeded the set total.*")
    W("")
    W("| Streak | Games | Avg Runs | Avg Line | Runs vs Line | Over Rate% |")
    W("|--------|-------|----------|----------|-------------|-----------|")
    if not sr["s5_totals"].empty:
        for _, row in sr["s5_totals"].iterrows():
            over = f"{row['over_rate']}%" if row["over_rate"] is not None else "N/A"
            W(f"| {row['streak']} | {int(row['games']):,} | {row['avg_runs']} | "
              f"{row['avg_line']} | {row['runs_vs_line']:+.2f} | {over} |")
    W("")

    # Season stability
    W("---")
    W("## Season-by-Season Stability: Home W5+ ML ROI")
    W("*Checks whether the streak signal is consistent across seasons or driven by a few anomalous years.*")
    W("")
    W("| Season | Games | Home Win% | ML ROI | Avg Implied% |")
    W("|--------|-------|-----------|--------|-------------|")
    if not sr["s1_by_season"].empty:
        for _, row in sr["s1_by_season"].iterrows():
            W(f"| {int(row['season'])} | {int(row['games']):,} | {row['home_win_rate']}% | "
              f"{row['ml_roi']:+.2f}% | {row['avg_implied']}% |")
    W("")

    W("---")
    W("## Interpretation Framework")
    W("")
    W("| Result | Meaning | Findings Matrix Verdict |")
    W("|--------|---------|------------------------|")
    W("| W5+ ML ROI < -4% consistently | Market strongly over-reacts to win streaks | WRONG (contrarian: fade hot home teams) |")
    W("| W5+ ML ROI +2% to +5% | Market under-reacts, momentum is real | CONFIRMED (bet with streaks) |")
    W("| W5+ ML ROI near 0% | Market efficiently prices streaks | PARTIAL (no edge, signal priced in) |")
    W("| L5+ ML ROI > +4.5% | Market over-discounts cold home teams | CONFIRMED (buy the dip) |")
    W("| Implied delta positive + ROI negative | Market moved line too far | Over-reaction confirmed |")
    W("| S4 combined ROI > S1 alone | Stacking amplifies signal | Combination signal worth testing |")
    W("")
    W("**Phase 2 (requires --h4 pitcher stats):** pitcher win/loss streak and K-rate trend hypotheses")
    W("(S5-pitcher, S6) will be added once 2022-2025 pitcher game logs are loaded.")

    os.makedirs(str(Path(out_path).parent), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"Report written to {out_path}")


def run_streaks_standalone():
    p = argparse.ArgumentParser()
    p.add_argument("--db",        default=DEFAULT_DB)
    p.add_argument("--bookmaker", default="sbro", choices=["sbro","oddswarehouse","all"])
    args = p.parse_args()

    cfg = BOOKMAKER_CONFIG[args.bookmaker]
    suffix = "" if args.bookmaker == "sbro" else f"_{args.bookmaker}"
    out_path = str(Path(REPORTS_DIR) / f"backtest_report_streaks{suffix}.md")

    print(f"Running streak analysis [{cfg['label']} {cfg['seasons_label']}]...")
    con = connect(args.db)
    sr = streak_analysis(con, cfg["db_values"], cfg)
    con.close()

    write_streak_report(sr, out_path, cfg)

    base = sr["baseline"]
    print(f"\n{'='*60}")
    print(f"  STREAK QUICK SUMMARY  [{cfg['label']}]")
    print(f"{'='*60}")
    print(f"  Baseline home ML ROI:  {base['overall_ml_roi']:+.2f}%")
    if not sr["s1_s2_table"].empty:
        for _, row in sr["s1_s2_table"].iterrows():
            if row["streak"] in ["W5+","L5+"]:
                print(f"  {row['streak']:6s}: {row['games']:4d} games | "
                      f"win {row['home_win_rate']}% | ROI {row['ml_roi']:+.2f}% | "
                      f"implied delta {row['implied_delta']:+.2f}pp")
    print(f"{'='*60}")


# ==============================================================================
# ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    if "--h4" in sys.argv:
        sys.argv.remove("--h4")
        run_h4_standalone()
    elif "--multivariate" in sys.argv:
        sys.argv.remove("--multivariate")
        run_multivariate_standalone()
    elif "--streaks" in sys.argv:
        sys.argv.remove("--streaks")
        run_streaks_standalone()
    else:
        main()
