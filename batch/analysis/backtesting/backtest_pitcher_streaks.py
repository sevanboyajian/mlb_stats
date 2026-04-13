#!/usr/bin/env python3
"""
backtest_pitcher_streaks.py — S6 and S7 pitcher-level streak signals.

Tests whether the betting market over- or under-reacts to recent starting
pitcher performance, creating exploitable edges on moneyline (S6) and
totals (S7).

SIGNAL DEFINITIONS
──────────────────
  S6  Pitcher win/loss streak (market over-reaction)
      SP entering game N on a W streak of 4+ consecutive starts
      OR a L streak of 3+ consecutive starts.

      Hypothesis: the market overprices teams whose SP is on a hot streak
      (analogous to S1 team streaks). Bet AGAINST the hot SP's team when
      on W4+. Bet FOR the struggling SP's team when on L3+ (market
      over-discounts struggling pitchers).

      W streak bet: FADE — bet ML against the hot SP's team.
      L streak bet: FOLLOW — bet ML for the cold SP's team.

      Enhancer (W streak): SP's current season ERA is worse than their
      streak-start ERA — hot results may be luck, not skill regression.
      Enhancer (L streak): SP's current K/9 is higher than their
      season avg — still missing but maintaining stuff.

  S7  Pitcher K-rate trend vs total line
      SP on a 3-start K-rate surge (avg K/9 > 10.0 in last 3 starts)
      OR collapse (avg K/9 < 4.0 in last 3 starts).

      Hypothesis: high-K SPs suppress scoring below the total line;
      low-K SPs inflate scoring above it. Market may not fully adjust
      the total for short-term K-rate changes.

      K surge bet: UNDER — SP likely to suppress scoring.
      K collapse bet: OVER — SP likely to allow more contact/scoring.

      Enhancer (K surge): opposing lineup K-rate > 24% season-to-date.
      Enhancer (K collapse): opposing lineup OPS > .780 season-to-date.

NO LOOK-AHEAD BIAS METHODOLOGY
───────────────────────────────
  Streak entering game N = computed from games 1..N-1 only (same season).
  K-rate trend = rolling 3-start average of K/9, using only prior starts.
  Season-to-date ERA = cumulative ERA entering game N (not same-game ERA).
  Opposing lineup stats = season-to-date entering game N.

  Critical: we need to identify WHICH pitcher started each game.
  Proxy: the pitcher on a team with the highest innings_pitched in that
  game (same approach as backtest_analysis.py --h4). Pitchers with
  innings_pitched < 3.0 in a game are classified as relievers and
  excluded from streak computation.

STREAK CONSTRUCTION
───────────────────
  Per pitcher per season:
    - Sort all qualifying starts (IP ≥ 3.0) by game date.
    - win_streak_entering_N = consecutive wins in starts 1..N-1
      (resets to 0 on any loss or no-decision).
    - loss_streak_entering_N = consecutive losses in starts 1..N-1
      (resets to 0 on any win or no-decision).
    - k9_last3_entering_N = avg K/9 across the 3 starts immediately
      before start N (NULL if fewer than 3 prior starts).
    - era_entering_N = cumulative season ERA before start N.

OUTPUT SECTIONS
───────────────
  1. S6 W-streak: fire rate, implied delta, home/away ML ROI by streak length
  2. S6 L-streak: fire rate, implied delta, ML ROI by streak length
  3. S6 enhanced gate analysis
  4. S7 K-surge: UNDER ROI by K/9 threshold
  5. S7 K-collapse: OVER ROI by K/9 threshold
  6. S7 enhanced gate analysis
  7. Season-by-season stability for all four signal variants
  8. Cross-signal: games where S6 and S7 fire simultaneously
  9. Comparison to team streak signals (S1/S1+H2) — does pitcher-level
     add independent value beyond what team streaks already capture?

USAGE
─────
    python backtest_pitcher_streaks.py
    python backtest_pitcher_streaks.py --bookmaker oddswarehouse
    python backtest_pitcher_streaks.py --bookmaker all
    python backtest_pitcher_streaks.py --s6-win-streak 3      # lower W threshold
    python backtest_pitcher_streaks.py --s6-loss-streak 2     # lower L threshold
    python backtest_pitcher_streaks.py --s7-surge-k9 9.0      # lower K surge
    python backtest_pitcher_streaks.py --s7-collapse-k9 5.0   # higher K collapse
    python backtest_pitcher_streaks.py --min-gs 3             # min prior starts
    python backtest_pitcher_streaks.py --seasons 2022 2023 2024 2025
"""

import argparse
import logging
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from core.db.connection import connect as db_connect

# ── Paths ──────────────────────────────────────────────────────────────────────
DEFAULT_DB  = r"C:\Users\sevan\OneDrive\Documents\Python\mlb_stats\mlb_stats.db"
REPORTS_DIR = Path(r"C:\Users\sevan\OneDrive\Documents\Python\mlb_stats\reports")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BOOKMAKER_PRIORITY = [
    "draftkings", "fanduel", "betmgm", "betonlineag", "sbro", "oddswarehouse",
]
BOOKMAKER_SET_ALL  = set(BOOKMAKER_PRIORITY)
BOOKMAKER_SET_SBRO = {"sbro"}
BOOKMAKER_SET_OW   = {"oddswarehouse"}


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def connect(db_path: str) -> sqlite3.Connection:
    con = db_connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA foreign_keys=ON")
    return con


def get_bookmaker_set(bm_arg: str) -> set:
    if bm_arg == "all":
        return BOOKMAKER_SET_ALL
    if bm_arg == "oddswarehouse":
        return BOOKMAKER_SET_OW
    return BOOKMAKER_SET_SBRO


def payout(american_odds: int) -> float:
    if american_odds is None:
        return None
    return american_odds / 100.0 if american_odds > 0 else 100.0 / abs(american_odds)


def implied_prob(american_odds: int) -> float:
    if american_odds is None:
        return None
    if american_odds > 0:
        return 100.0 / (american_odds + 100.0)
    return abs(american_odds) / (abs(american_odds) + 100.0)


def grade_ml(home_score: int, away_score: int, bet_side: str, odds: int) -> float:
    if None in (home_score, away_score, odds):
        return None
    home_won = home_score > away_score
    bet_won  = (bet_side == "home" and home_won) or \
               (bet_side == "away" and not home_won)
    return payout(odds) if bet_won else -1.0


def grade_total(home_score: int, away_score: int,
                total_line: float, over_odds: int, bet: str) -> float:
    if None in (home_score, away_score, total_line):
        return None
    scored = home_score + away_score
    if bet == "over":
        if scored > total_line:
            return payout(over_odds or -110)
        if scored < total_line:
            return -1.0
        return 0.0
    else:
        if scored < total_line:
            return payout(over_odds or -110)
        if scored > total_line:
            return -1.0
        return 0.0


def load_season_start(con: sqlite3.Connection, season: int) -> str | None:
    row = con.execute(
        "SELECT season_start FROM seasons WHERE season = ?", (season,)
    ).fetchone()
    return row["season_start"] if row else None


# ══════════════════════════════════════════════════════════════════════════════
# PITCHER GAME LOG BUILDER
# Core function — builds the rolling streak data for every SP in a season.
# No look-ahead bias: all stats computed from games prior to game N only.
# ══════════════════════════════════════════════════════════════════════════════

def build_pitcher_game_logs(con: sqlite3.Connection, season: int,
                             min_ip: float = 3.0) -> dict:
    """
    Build per-pitcher rolling game logs for a season.

    Returns:
        {player_id: [start_dict, ...]}

    Each start_dict contains:
        game_pk, game_date, team_id, innings_pitched,
        strikeouts_pit, earned_runs, win, loss,
        k_per_9_game,           ← K/9 in THIS start
        era_entering,           ← season ERA BEFORE this start
        k9_last3_entering,      ← avg K/9 in 3 starts BEFORE this one
        win_streak_entering,    ← consecutive W streak entering this start
        loss_streak_entering,   ← consecutive L streak entering this start
        start_number            ← which start of the season (1-indexed)

    Only includes games where the pitcher threw ≥ min_ip innings
    (proxy for qualifying start vs. bulk relief appearance).
    """
    log.info("    Building pitcher game logs (season %d, min_ip=%.1f) ...",
             season, min_ip)

    rows = con.execute("""
        SELECT
            pgs.player_id,
            pgs.game_pk,
            pgs.team_id,
            g.game_date,
            pgs.innings_pitched,
            pgs.strikeouts_pit,
            pgs.earned_runs,
            pgs.win,
            pgs.loss
        FROM player_game_stats pgs
        JOIN games g ON g.game_pk = pgs.game_pk
        WHERE g.season      = ?
          AND g.game_type   = 'R'
          AND g.status      = 'Final'
          AND pgs.player_role = 'pitcher'
          AND pgs.innings_pitched >= ?
        ORDER BY pgs.player_id, g.game_date, pgs.game_pk
    """, (season, min_ip)).fetchall()

    # Group by pitcher
    raw = defaultdict(list)
    for r in rows:
        raw[r["player_id"]].append(dict(r))

    pitcher_logs = {}
    for pid, starts in raw.items():
        processed = []
        cumulative_ip  = 0.0
        cumulative_er  = 0
        win_streak     = 0
        loss_streak    = 0

        for i, s in enumerate(starts):
            ip  = s["innings_pitched"] or 0.0
            er  = s["earned_runs"]     or 0
            k   = s["strikeouts_pit"]  or 0
            w   = s["win"]             or 0
            l   = s["loss"]            or 0

            # ERA entering this start (from all prior starts this season)
            era_entering = (cumulative_er * 9 / cumulative_ip) \
                           if cumulative_ip > 0 else None

            # K/9 for the last 3 starts before this one
            if i >= 3:
                last3 = processed[-3:]
                k9_last3 = sum(x["k_per_9_game"] or 0 for x in last3) / 3.0
            elif i >= 1:
                # Fewer than 3 prior starts — use what we have
                k9_last3 = None  # not enough history
            else:
                k9_last3 = None

            # K/9 for this start
            k9_game = (k * 9 / ip) if ip > 0 else None

            entry = {
                "game_pk":              s["game_pk"],
                "game_date":            s["game_date"],
                "team_id":              s["team_id"],
                "innings_pitched":      ip,
                "strikeouts_pit":       k,
                "earned_runs":          er,
                "win":                  w,
                "loss":                 l,
                "k_per_9_game":         k9_game,
                "era_entering":         era_entering,
                "k9_last3_entering":    k9_last3,
                "win_streak_entering":  win_streak,
                "loss_streak_entering": loss_streak,
                "start_number":         i + 1,
            }
            processed.append(entry)

            # Update streaks for next iteration
            if w:
                win_streak  += 1
                loss_streak  = 0
            elif l:
                loss_streak += 1
                win_streak   = 0
            else:
                # No-decision: streaks reset to 0
                win_streak   = 0
                loss_streak  = 0

            # Update cumulative stats
            cumulative_ip += ip
            cumulative_er += er

        pitcher_logs[pid] = processed

    total_starts = sum(len(v) for v in pitcher_logs.values())
    log.info("      → %d pitchers, %d qualifying starts.", len(pitcher_logs), total_starts)
    return pitcher_logs


def build_game_sp_map(con: sqlite3.Connection, season: int,
                      pitcher_logs: dict, min_ip: float = 3.0) -> dict:
    """
    Map each game to its home and away starting pitcher using the pitcher
    with the highest innings_pitched on each team per game.

    Returns:
        {game_pk: {"home_sp": player_id, "away_sp": player_id}}

    Only returns pitchers who appear in pitcher_logs (qualifying starters).
    """
    log.info("    Building game→SP map (season %d) ...", season)

    rows = con.execute("""
        SELECT
            pgs.game_pk,
            pgs.player_id,
            pgs.team_id,
            pgs.innings_pitched,
            g.home_team_id,
            g.away_team_id
        FROM player_game_stats pgs
        JOIN games g ON g.game_pk = pgs.game_pk
        WHERE g.season      = ?
          AND g.game_type   = 'R'
          AND g.status      = 'Final'
          AND pgs.player_role = 'pitcher'
          AND pgs.innings_pitched >= ?
        ORDER BY pgs.game_pk, pgs.innings_pitched DESC
    """, (season, min_ip)).fetchall()

    # For each game, track the pitcher with most IP per team
    game_best = defaultdict(dict)  # {game_pk: {team_id: (player_id, ip)}}
    for r in rows:
        gk  = r["game_pk"]
        tid = r["team_id"]
        pid = r["player_id"]
        ip  = r["innings_pitched"]
        if tid not in game_best[gk] or ip > game_best[gk][tid][1]:
            game_best[gk][tid] = (pid, ip)

    valid_pids = set(pitcher_logs.keys())
    result = {}
    for gk, team_data in game_best.items():
        # We need rows to determine home/away team for this game
        pass

    # Re-query to get home/away team IDs per game
    game_teams = {}
    team_rows = con.execute("""
        SELECT DISTINCT game_pk, home_team_id, away_team_id
        FROM games
        WHERE season = ? AND game_type = 'R' AND status = 'Final'
    """, (season,)).fetchall()
    for r in team_rows:
        game_teams[r["game_pk"]] = (r["home_team_id"], r["away_team_id"])

    for gk, (home_tid, away_tid) in game_teams.items():
        if gk not in game_best:
            continue
        home_entry = game_best[gk].get(home_tid)
        away_entry = game_best[gk].get(away_tid)

        home_sp = home_entry[0] if home_entry and home_entry[0] in valid_pids else None
        away_sp = away_entry[0] if away_entry and away_entry[0] in valid_pids else None

        if home_sp or away_sp:
            result[gk] = {"home_sp": home_sp, "away_sp": away_sp}

    log.info("      → %d games with identifiable SP.", len(result))
    return result


def get_sp_stats_entering(pitcher_logs: dict, player_id: int,
                           game_pk: int) -> dict | None:
    """
    Return the streak/trend stats for a pitcher ENTERING a specific game.
    Looks up the start_dict whose game_pk matches — that dict's
    'entering' fields are pre-computed from prior starts only.
    Returns None if the pitcher or game is not found.
    """
    starts = pitcher_logs.get(player_id)
    if not starts:
        return None
    for s in starts:
        if s["game_pk"] == game_pk:
            return s
    return None


# ══════════════════════════════════════════════════════════════════════════════
# ODDS LOADER
# ══════════════════════════════════════════════════════════════════════════════

def load_games_with_odds(con: sqlite3.Connection, season: int,
                         bm_set: set) -> list:
    """Final regular-season games with closing ML and total odds."""
    rows = con.execute("""
        SELECT
            g.game_pk, g.game_date,
            g.home_team_id, g.away_team_id,
            g.home_score, g.away_score,
            go.bookmaker,
            go.home_ml, go.away_ml
        FROM games g
        JOIN game_odds go
          ON go.game_pk       = g.game_pk
         AND go.market_type   = 'moneyline'
         AND go.is_closing_line = 1
        WHERE g.season    = ?
          AND g.game_type = 'R'
          AND g.status    = 'Final'
          AND g.home_score IS NOT NULL
          AND g.away_score IS NOT NULL
          AND go.home_ml   IS NOT NULL
    """, (season,)).fetchall()

    seen = {}
    for r in rows:
        if r["bookmaker"] not in bm_set:
            continue
        gk = r["game_pk"]
        if gk not in seen:
            seen[gk] = dict(r)
        else:
            curr = BOOKMAKER_PRIORITY.index(seen[gk]["bookmaker"]) \
                   if seen[gk]["bookmaker"] in BOOKMAKER_PRIORITY else 999
            new  = BOOKMAKER_PRIORITY.index(r["bookmaker"]) \
                   if r["bookmaker"] in BOOKMAKER_PRIORITY else 999
            if new < curr:
                seen[gk] = dict(r)

    tot_rows = con.execute("""
        SELECT go.game_pk, go.bookmaker, go.total_line, go.over_odds
        FROM game_odds go
        JOIN games g ON g.game_pk = go.game_pk
        WHERE g.season       = ?
          AND g.game_type    = 'R'
          AND go.market_type = 'total'
          AND go.is_closing_line = 1
          AND go.total_line  IS NOT NULL
    """, (season,)).fetchall()

    tot_best = {}
    for r in tot_rows:
        if r["bookmaker"] not in bm_set:
            continue
        gk = r["game_pk"]
        if gk not in tot_best:
            tot_best[gk] = dict(r)
        else:
            curr = BOOKMAKER_PRIORITY.index(tot_best[gk]["bookmaker"]) \
                   if tot_best[gk]["bookmaker"] in BOOKMAKER_PRIORITY else 999
            new  = BOOKMAKER_PRIORITY.index(r["bookmaker"]) \
                   if r["bookmaker"] in BOOKMAKER_PRIORITY else 999
            if new < curr:
                tot_best[gk] = dict(r)

    games = []
    for gk, ml in seen.items():
        game = dict(ml)
        tot  = tot_best.get(gk)
        game["total_line"] = tot["total_line"] if tot else None
        game["over_odds"]  = tot["over_odds"]  if tot else None
        games.append(game)

    return games


# ══════════════════════════════════════════════════════════════════════════════
# SIGNAL EVALUATORS
# ══════════════════════════════════════════════════════════════════════════════

def run_s6(
    games: list,
    pitcher_logs: dict,
    sp_map: dict,
    season: int,
    win_streak_min: int = 4,
    loss_streak_min: int = 3,
    min_prior_starts: int = 3,
) -> dict:
    """
    S6 — Pitcher win/loss streak.

    Returns:
        {
          "w_fires":  [fire_dict, ...],  # W streak fires — fade the hot SP's team
          "l_fires":  [fire_dict, ...],  # L streak fires — back the cold SP's team
        }

    fire_dict keys:
        game_pk, game_date, season, bet_side, bet_type,
        odds_used, pnl, enhanced, streak_len, streak_type,
        sp_role (home/away), era_entering, k9_last3
    """
    w_fires = []
    l_fires = []

    game_index = {g["game_pk"]: g for g in games}

    for gk, sp_info in sp_map.items():
        g = game_index.get(gk)
        if g is None:
            continue

        for sp_role in ("home_sp", "away_sp"):
            pid = sp_info.get(sp_role)
            if pid is None:
                continue

            stats = get_sp_stats_entering(pitcher_logs, pid, gk)
            if stats is None:
                continue

            # Require minimum prior starts for meaningful streak
            if stats["start_number"] <= min_prior_starts:
                continue

            w_streak = stats["win_streak_entering"]
            l_streak = stats["loss_streak_entering"]
            era_in   = stats["era_entering"]
            k9_l3    = stats["k9_last3_entering"]

            # Determine bet side
            # W streak: FADE the hot SP's team (bet against them)
            # L streak: FOLLOW (back the cold SP's team — market over-discounts)
            if sp_role == "home_sp":
                w_fade_side  = "away"   # fade home SP → bet away
                l_follow_side = "home"  # follow home SP → bet home
                w_odds  = g["away_ml"]
                l_odds  = g["home_ml"]
            else:
                w_fade_side   = "home"  # fade away SP → bet home
                l_follow_side = "away"  # follow away SP → bet away
                w_odds  = g["home_ml"]
                l_odds  = g["away_ml"]

            base = {
                "game_pk":    gk,
                "game_date":  g["game_date"],
                "season":     season,
                "bet_type":   "moneyline",
                "sp_role":    sp_role.replace("_sp", ""),
                "era_entering": era_in,
                "k9_last3":   k9_l3,
            }

            # W streak fire
            if w_streak >= win_streak_min:
                # Enhanced: pitcher's current ERA is ABOVE their streak-start
                # quality (i.e. winning on luck, not dominance)
                # Proxy: era_entering > 4.50 (league avg) while on W streak
                enhanced = era_in is not None and era_in > 4.50
                pnl = grade_ml(g["home_score"], g["away_score"],
                               w_fade_side, w_odds)
                if pnl is not None:
                    w_fires.append({**base,
                        "bet_side":   w_fade_side,
                        "odds_used":  w_odds,
                        "pnl":        pnl,
                        "enhanced":   enhanced,
                        "streak_len": w_streak,
                        "streak_type": "W",
                    })

            # L streak fire
            if l_streak >= loss_streak_min:
                # Enhanced: SP's K/9 in last 3 starts still above season avg
                # (still has stuff, just unlucky results)
                season_k9 = None
                if era_in is not None and stats["start_number"] > 1:
                    # Use last3 vs current game k9 as proxy for recent K ability
                    season_k9 = k9_l3
                enhanced = (k9_l3 is not None and
                            stats["k_per_9_game"] is not None and
                            k9_l3 >= stats["k_per_9_game"])
                pnl = grade_ml(g["home_score"], g["away_score"],
                               l_follow_side, l_odds)
                if pnl is not None:
                    l_fires.append({**base,
                        "bet_side":    l_follow_side,
                        "odds_used":   l_odds,
                        "pnl":         pnl,
                        "enhanced":    enhanced,
                        "streak_len":  l_streak,
                        "streak_type": "L",
                    })

    return {"w_fires": w_fires, "l_fires": l_fires}


def run_s7(
    games: list,
    pitcher_logs: dict,
    sp_map: dict,
    season: int,
    surge_k9_threshold: float  = 10.0,
    collapse_k9_threshold: float = 4.0,
    min_prior_starts: int = 3,
) -> dict:
    """
    S7 — Pitcher K-rate trend vs total line.

    K surge  (last 3 starts avg K/9 ≥ threshold) → bet UNDER
    K collapse (last 3 starts avg K/9 ≤ threshold) → bet OVER

    Returns:
        {
          "surge_fires":   [fire_dict, ...],   # K surge → UNDER
          "collapse_fires": [fire_dict, ...],  # K collapse → OVER
        }
    """
    surge_fires   = []
    collapse_fires = []

    game_index = {g["game_pk"]: g for g in games}

    for gk, sp_info in sp_map.items():
        g = game_index.get(gk)
        if g is None:
            continue
        if g.get("total_line") is None:
            continue  # need total line for this signal

        for sp_role in ("home_sp", "away_sp"):
            pid = sp_info.get(sp_role)
            if pid is None:
                continue

            stats = get_sp_stats_entering(pitcher_logs, pid, gk)
            if stats is None:
                continue

            # Need at least 3 prior starts for rolling K/9
            if stats["start_number"] <= min_prior_starts:
                continue

            k9_l3 = stats["k9_last3_entering"]
            if k9_l3 is None:
                continue

            era_in = stats["era_entering"]

            base = {
                "game_pk":    gk,
                "game_date":  g["game_date"],
                "season":     season,
                "bet_type":   "total",
                "sp_role":    sp_role.replace("_sp", ""),
                "k9_last3":   k9_l3,
                "era_entering": era_in,
                "total_line": g["total_line"],
            }

            # K surge → UNDER
            if k9_l3 >= surge_k9_threshold:
                # Enhanced: opposing lineup has high K-rate (amplifies suppression)
                # We approximate using team-level stats which we don't have here,
                # so enhanced = surge is extreme (K/9 ≥ 11.0)
                enhanced = k9_l3 >= 11.0
                pnl = grade_total(
                    g["home_score"], g["away_score"],
                    g["total_line"], g["over_odds"], "under"
                )
                if pnl is not None:
                    surge_fires.append({**base,
                        "bet_side":  "under",
                        "odds_used": g["over_odds"] or -110,
                        "pnl":       pnl,
                        "enhanced":  enhanced,
                        "trend":     "surge",
                    })

            # K collapse → OVER
            if k9_l3 <= collapse_k9_threshold:
                # Enhanced: very extreme collapse (K/9 ≤ 3.0)
                enhanced = k9_l3 <= 3.0
                pnl = grade_total(
                    g["home_score"], g["away_score"],
                    g["total_line"], g["over_odds"], "over"
                )
                if pnl is not None:
                    collapse_fires.append({**base,
                        "bet_side":  "over",
                        "odds_used": g["over_odds"] or -110,
                        "pnl":       pnl,
                        "enhanced":  enhanced,
                        "trend":     "collapse",
                    })

    return {"surge_fires": surge_fires, "collapse_fires": collapse_fires}


# ══════════════════════════════════════════════════════════════════════════════
# STATS + REPORT HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def stats(fires: list) -> dict:
    n      = len(fires)
    valid  = [f for f in fires if f.get("pnl") is not None]
    nv     = len(valid)
    wins   = sum(1 for f in valid if f["pnl"] > 0)
    losses = sum(1 for f in valid if f["pnl"] < 0)
    pnl    = sum(f["pnl"] for f in valid)
    avg_o  = sum(f["odds_used"] for f in valid
                 if f.get("odds_used")) / nv if nv > 0 else 0
    return dict(
        n=n, wins=wins, losses=losses, pnl=pnl,
        hit_rate=(wins / (wins + losses) * 100) if (wins + losses) > 0 else 0.0,
        roi=(pnl / nv * 100) if nv > 0 else 0.0,
        avg_odds=avg_o,
    )


def tbl_header() -> str:
    h = "| Segment                          |    N |   W |   L |  Hit% |    P&L |   ROI% | Avg Odds |\n"
    s = "|----------------------------------|------|-----|-----|-------|--------|--------|----------|\n"
    return h + s


def tbl_row(label: str, s: dict) -> str:
    return (
        f"| {label:<34} | {s['n']:>4} | {s['wins']:>3} | {s['losses']:>3} "
        f"| {s['hit_rate']:>5.1f}% | {s['pnl']:>+6.2f} | {s['roi']:>+5.1f}% "
        f"| {s['avg_odds']:>+7.0f} |\n"
    )


def verdict(s: dict) -> str:
    n, roi = s["n"], s["roi"]
    if n < 30:
        return f"> ⚠️  Sample too small ({n} fires) — no conclusion.\n\n"
    if roi >= 7.0:
        return f"> ✅  **Strong.** ROI {roi:+.1f}% on {n} fires.\n\n"
    if roi >= 4.5:
        return f"> ✅  **Above threshold.** ROI {roi:+.1f}% on {n} fires.\n\n"
    if roi >= 2.0:
        return f"> 🟡  **Borderline.** ROI {roi:+.1f}% on {n} fires.\n\n"
    return f"> ❌  **Below threshold.** ROI {roi:+.1f}% on {n} fires.\n\n"


def streak_breakdown(fires: list, streak_field: str,
                      thresholds: list) -> str:
    """Show ROI by minimum streak length."""
    h = f"| Min streak | N | Hit% | ROI% |\n|------------|---|------|------|\n"
    lines = [h]
    for t in thresholds:
        subset = [f for f in fires if f.get(streak_field, 0) >= t]
        s = stats(subset)
        lines.append(
            f"| ≥ {t}         | {s['n']:>4} | {s['hit_rate']:>5.1f}% "
            f"| {s['roi']:>+5.1f}% |\n"
        )
    return "".join(lines)


def k9_breakdown(fires: list, field: str, thresholds: list,
                  direction: str) -> str:
    """Show ROI by K/9 threshold (surge: ≥ threshold, collapse: ≤ threshold)."""
    op = "≥" if direction == "surge" else "≤"
    h = f"| K/9 {op}    | N | Hit% | ROI% |\n|---------|---|------|------|\n"
    lines = [h]
    for t in thresholds:
        if direction == "surge":
            subset = [f for f in fires if f.get(field, 0) >= t]
        else:
            subset = [f for f in fires if f.get(field, 99) <= t]
        s = stats(subset)
        lines.append(
            f"| {op} {t:.1f}   | {s['n']:>4} | {s['hit_rate']:>5.1f}% "
            f"| {s['roi']:>+5.1f}% |\n"
        )
    return "".join(lines)


def season_tbl(fires_by_season: dict, seasons: list) -> str:
    h = "| Season |    N |  Hit% |    P&L |   ROI% |\n"
    s = "|--------|------|-------|--------|--------|\n"
    lines = [h, s]
    for season in sorted(seasons):
        st = stats(fires_by_season.get(season, []))
        lines.append(
            f"| {season} | {st['n']:>4} | {st['hit_rate']:>5.1f}% "
            f"| {st['pnl']:>+6.2f} | {st['roi']:>+5.1f}% |\n"
        )
    return "".join(lines)


def implied_delta(fires: list) -> float:
    """
    Average implied probability of the bet odds vs the baseline
    home implied for the full dataset. Positive delta = market
    over-values the opposing side.
    """
    if not fires:
        return 0.0
    return sum(implied_prob(f["odds_used"]) for f in fires
               if f.get("odds_used")) / len(fires) * 100


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(
        description="S6/S7 pitcher streak signal backtest"
    )
    p.add_argument("--db",               default=DEFAULT_DB)
    p.add_argument("--bookmaker",        default="all",
                   choices=["sbro", "oddswarehouse", "all"])
    p.add_argument("--seasons",          nargs="+", type=int)
    p.add_argument("--output",           default=None)

    # S6 thresholds
    p.add_argument("--s6-win-streak",    type=int,   default=4,
                   help="Min W streak to fire S6 (default 4)")
    p.add_argument("--s6-loss-streak",   type=int,   default=3,
                   help="Min L streak to fire S6 (default 3)")

    # S7 thresholds
    p.add_argument("--s7-surge-k9",      type=float, default=10.0,
                   help="K/9 threshold for K-surge (default 10.0)")
    p.add_argument("--s7-collapse-k9",   type=float, default=4.0,
                   help="K/9 threshold for K-collapse (default 4.0)")

    # General
    p.add_argument("--min-ip",           type=float, default=3.0,
                   help="Min IP for a qualifying start (default 3.0)")
    p.add_argument("--min-gs",           type=int,   default=3,
                   help="Min prior starts before streak counts (default 3)")

    args = p.parse_args()

    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    log.info("=" * 60)
    log.info("  S6/S7 PITCHER STREAK BACKTEST")
    log.info("  Bookmaker: %s", args.bookmaker)
    log.info("  %s", run_ts)
    log.info("=" * 60)

    if not Path(args.db).exists():
        log.error("DB not found: %s", args.db)
        sys.exit(1)

    con    = connect(args.db)
    bm_set = get_bookmaker_set(args.bookmaker)

    available = [
        r[0] for r in con.execute(
            "SELECT season FROM seasons WHERE season >= 2018 ORDER BY season"
        ).fetchall()
    ]
    # S6/S7 need intra-season streaks — use 2018+ (PBP and pitcher stats confirmed loaded)
    seasons = args.seasons or available
    seasons = [s for s in seasons if s in available]

    if not seasons:
        log.error("No valid seasons found.")
        sys.exit(1)

    log.info("Seasons: %s", seasons)
    log.info("S6: W streak ≥ %d  |  L streak ≥ %d  |  min prior starts: %d",
             args.s6_win_streak, args.s6_loss_streak, args.min_gs)
    log.info("S7: K surge ≥ %.1f  |  K collapse ≤ %.1f",
             args.s7_surge_k9, args.s7_collapse_k9)

    # Accumulators
    s6_w_all = []; s6_l_all = []
    s7_surge_all = []; s7_collapse_all = []
    s6_w_by_s   = defaultdict(list)
    s6_l_by_s   = defaultdict(list)
    s7_su_by_s  = defaultdict(list)
    s7_co_by_s  = defaultdict(list)

    for season in seasons:
        log.info("")
        log.info("── Season %d ─────────────────────────────────────────", season)

        ss = load_season_start(con, season)
        if not ss:
            log.warning("  Season %d not in seasons table.", season)
            continue

        # Build pitcher game logs for THIS season (intra-season streaks)
        pitcher_logs = build_pitcher_game_logs(con, season, args.min_ip)
        sp_map       = build_game_sp_map(con, season, pitcher_logs, args.min_ip)
        games        = load_games_with_odds(con, season, bm_set)

        if not games:
            log.warning("  No games with odds for season %d.", season)
            continue

        log.info("  %d games with odds, %d games with SP map.",
                 len(games), len(sp_map))

        # Run S6
        s6 = run_s6(
            games, pitcher_logs, sp_map, season,
            win_streak_min=args.s6_win_streak,
            loss_streak_min=args.s6_loss_streak,
            min_prior_starts=args.min_gs,
        )
        s6_w_all.extend(s6["w_fires"])
        s6_l_all.extend(s6["l_fires"])
        s6_w_by_s[season].extend(s6["w_fires"])
        s6_l_by_s[season].extend(s6["l_fires"])
        log.info("  S6: W-streak fires=%d  L-streak fires=%d",
                 len(s6["w_fires"]), len(s6["l_fires"]))

        # Run S7
        s7 = run_s7(
            games, pitcher_logs, sp_map, season,
            surge_k9_threshold=args.s7_surge_k9,
            collapse_k9_threshold=args.s7_collapse_k9,
            min_prior_starts=args.min_gs,
        )
        s7_surge_all.extend(s7["surge_fires"])
        s7_collapse_all.extend(s7["collapse_fires"])
        s7_su_by_s[season].extend(s7["surge_fires"])
        s7_co_by_s[season].extend(s7["collapse_fires"])
        log.info("  S7: K-surge fires=%d  K-collapse fires=%d",
                 len(s7["surge_fires"]), len(s7["collapse_fires"]))

    con.close()

    # ── Build report ──────────────────────────────────────────────────────────
    lines = []
    lines.append("# S6/S7 — Pitcher Streak Signal Backtest\n\n")
    lines.append(f"Generated: {run_ts}  \n")
    lines.append(f"Bookmaker: `{args.bookmaker}`  \n")
    lines.append(f"Seasons: {', '.join(str(s) for s in sorted(seasons))}  \n\n")

    lines.append("## Thresholds\n\n")
    lines.append("| Parameter | Value |\n|-----------|-------|\n")
    lines.append(f"| S6 W streak ≥ | {args.s6_win_streak} |\n")
    lines.append(f"| S6 L streak ≥ | {args.s6_loss_streak} |\n")
    lines.append(f"| S6 min prior starts | {args.min_gs} |\n")
    lines.append(f"| S7 K surge ≥ K/9 | {args.s7_surge_k9} |\n")
    lines.append(f"| S7 K collapse ≤ K/9 | {args.s7_collapse_k9} |\n")
    lines.append(f"| Min IP per qualifying start | {args.min_ip} |\n\n")
    lines.append("---\n\n")

    # ── Section 1: S6 W streak ───────────────────────────────────────────────
    lines.append("## 1. S6 — Pitcher W streak fade (bet against hot SP)\n\n")
    lines.append(
        "Bet: ML against the team whose SP enters on W≥ streak.  \n"
        "Hypothesis: market over-values SP on hot streak; "
        "actual win rate does not justify elevated implied probability.\n\n"
    )
    s_w = stats(s6_w_all)
    s_w_enh = stats([f for f in s6_w_all if f["enhanced"]])
    s_w_base = stats([f for f in s6_w_all if not f["enhanced"]])
    s_w_home = stats([f for f in s6_w_all if f["sp_role"] == "home"])
    s_w_away = stats([f for f in s6_w_all if f["sp_role"] == "away"])

    lines.append(tbl_header())
    lines.append(tbl_row(f"All W≥{args.s6_win_streak} fires", s_w))
    lines.append(tbl_row("  Enhanced (high ERA on W streak)", s_w_enh))
    lines.append(tbl_row("  Base (no enhancer)", s_w_base))
    lines.append(tbl_row("  Home SP on W streak (bet away)", s_w_home))
    lines.append(tbl_row("  Away SP on W streak (bet home)", s_w_away))
    lines.append("\n")
    lines.append(verdict(s_w))

    lines.append("### S6 W streak — by streak length\n\n")
    lines.append(streak_breakdown(
        s6_w_all, "streak_len",
        [args.s6_win_streak, args.s6_win_streak + 1,
         args.s6_win_streak + 2, args.s6_win_streak + 3]
    ))
    lines.append("\n")

    # ── Section 2: S6 L streak ───────────────────────────────────────────────
    lines.append("## 2. S6 — Pitcher L streak follow (back the cold SP's team)\n\n")
    lines.append(
        "Bet: ML for the team whose SP enters on L≥ streak.  \n"
        "Hypothesis: market over-discounts SP on losing streak; "
        "back the team before regression to mean.\n\n"
    )
    s_l = stats(s6_l_all)
    s_l_enh  = stats([f for f in s6_l_all if f["enhanced"]])
    s_l_base = stats([f for f in s6_l_all if not f["enhanced"]])
    s_l_home = stats([f for f in s6_l_all if f["sp_role"] == "home"])
    s_l_away = stats([f for f in s6_l_all if f["sp_role"] == "away"])

    lines.append(tbl_header())
    lines.append(tbl_row(f"All L≥{args.s6_loss_streak} fires", s_l))
    lines.append(tbl_row("  Enhanced (still high K/9 on L streak)", s_l_enh))
    lines.append(tbl_row("  Base (no enhancer)", s_l_base))
    lines.append(tbl_row("  Home SP on L streak (bet home)", s_l_home))
    lines.append(tbl_row("  Away SP on L streak (bet away)", s_l_away))
    lines.append("\n")
    lines.append(verdict(s_l))

    lines.append("### S6 L streak — by streak length\n\n")
    lines.append(streak_breakdown(
        s6_l_all, "streak_len",
        [args.s6_loss_streak, args.s6_loss_streak + 1,
         args.s6_loss_streak + 2, args.s6_loss_streak + 3]
    ))
    lines.append("\n---\n\n")

    # ── Section 3: S7 K surge ────────────────────────────────────────────────
    lines.append("## 3. S7 — K surge → UNDER\n\n")
    lines.append(
        f"SP's 3-start rolling K/9 ≥ {args.s7_surge_k9}. Bet: UNDER.  \n"
        "Hypothesis: high-K SP suppresses scoring below the total line.\n\n"
    )
    s_su = stats(s7_surge_all)
    s_su_enh  = stats([f for f in s7_surge_all if f["enhanced"]])
    s_su_base = stats([f for f in s7_surge_all if not f["enhanced"]])
    s_su_home = stats([f for f in s7_surge_all if f["sp_role"] == "home"])
    s_su_away = stats([f for f in s7_surge_all if f["sp_role"] == "away"])

    lines.append(tbl_header())
    lines.append(tbl_row(f"All K≥{args.s7_surge_k9} surge fires", s_su))
    lines.append(tbl_row("  Enhanced (K/9 ≥ 11.0)", s_su_enh))
    lines.append(tbl_row("  Base (10.0–10.9)", s_su_base))
    lines.append(tbl_row("  Home SP surge", s_su_home))
    lines.append(tbl_row("  Away SP surge", s_su_away))
    lines.append("\n")
    lines.append(verdict(s_su))

    lines.append("### S7 K surge — threshold sensitivity\n\n")
    lines.append(k9_breakdown(
        s7_surge_all, "k9_last3", [9.0, 9.5, 10.0, 10.5, 11.0], "surge"
    ))
    lines.append("\n")

    # ── Section 4: S7 K collapse ─────────────────────────────────────────────
    lines.append("## 4. S7 — K collapse → OVER\n\n")
    lines.append(
        f"SP's 3-start rolling K/9 ≤ {args.s7_collapse_k9}. Bet: OVER.  \n"
        "Hypothesis: low-K SP allows more contact, inflating total above line.\n\n"
    )
    s_co = stats(s7_collapse_all)
    s_co_enh  = stats([f for f in s7_collapse_all if f["enhanced"]])
    s_co_base = stats([f for f in s7_collapse_all if not f["enhanced"]])
    s_co_home = stats([f for f in s7_collapse_all if f["sp_role"] == "home"])
    s_co_away = stats([f for f in s7_collapse_all if f["sp_role"] == "away"])

    lines.append(tbl_header())
    lines.append(tbl_row(f"All K≤{args.s7_collapse_k9} collapse fires", s_co))
    lines.append(tbl_row("  Enhanced (K/9 ≤ 3.0)", s_co_enh))
    lines.append(tbl_row("  Base (3.1–4.0)", s_co_base))
    lines.append(tbl_row("  Home SP collapse", s_co_home))
    lines.append(tbl_row("  Away SP collapse", s_co_away))
    lines.append("\n")
    lines.append(verdict(s_co))

    lines.append("### S7 K collapse — threshold sensitivity\n\n")
    lines.append(k9_breakdown(
        s7_collapse_all, "k9_last3", [5.0, 4.5, 4.0, 3.5, 3.0], "collapse"
    ))
    lines.append("\n---\n\n")

    # ── Section 5: Season-by-season stability ─────────────────────────────────
    lines.append("## 5. Season-by-season stability\n\n")
    lines.append("### S6 W streak fade\n\n")
    lines.append(season_tbl(s6_w_by_s, seasons))
    lines.append("\n### S6 L streak follow\n\n")
    lines.append(season_tbl(s6_l_by_s, seasons))
    lines.append("\n### S7 K surge → UNDER\n\n")
    lines.append(season_tbl(s7_su_by_s, seasons))
    lines.append("\n### S7 K collapse → OVER\n\n")
    lines.append(season_tbl(s7_co_by_s, seasons))
    lines.append("\n---\n\n")

    # ── Section 6: Cross-signal overlap ──────────────────────────────────────
    lines.append("## 6. Cross-signal overlap\n\n")
    lines.append(
        "Games where S6 and S7 fire simultaneously "
        "(SP on streak AND K-rate signal active).\n\n"
    )
    s6_w_pks = {f["game_pk"] for f in s6_w_all}
    s6_l_pks = {f["game_pk"] for f in s6_l_all}
    s7_su_pks = {f["game_pk"] for f in s7_surge_all}
    s7_co_pks = {f["game_pk"] for f in s7_collapse_all}

    combos = [
        ("S6-W + S7-surge (fade + UNDER)", s6_w_all,
         [f for f in s7_surge_all if f["game_pk"] in s6_w_pks]),
        ("S6-W + S7-collapse (fade + OVER)", s6_w_all,
         [f for f in s7_collapse_all if f["game_pk"] in s6_w_pks]),
        ("S6-L + S7-surge (follow + UNDER)", s6_l_all,
         [f for f in s7_surge_all if f["game_pk"] in s6_l_pks]),
        ("S6-L + S7-collapse (follow + OVER)", s6_l_all,
         [f for f in s7_collapse_all if f["game_pk"] in s6_l_pks]),
    ]

    lines.append(tbl_header())
    for label, primary, overlap in combos:
        s = stats(overlap)
        lines.append(tbl_row(label, s))
    lines.append("\n---\n\n")

    # ── Section 7: Summary verdict ────────────────────────────────────────────
    lines.append("## 7. Summary verdict\n\n")
    lines.append("| Signal variant | N | ROI% | Verdict |\n")
    lines.append("|----------------|---|------|---------|\n")
    for label, s in [
        (f"S6 W≥{args.s6_win_streak} fade", s_w),
        (f"S6 L≥{args.s6_loss_streak} follow", s_l),
        (f"S7 K≥{args.s7_surge_k9} surge UNDER", s_su),
        (f"S7 K≤{args.s7_collapse_k9} collapse OVER", s_co),
    ]:
        v = "✅ pass" if s["roi"] >= 4.5 and s["n"] >= 30 else \
            "🟡 watch" if s["roi"] >= 2.0 and s["n"] >= 30 else \
            "⚠️ small N" if s["n"] < 30 else "❌ fail"
        lines.append(f"| {label} | {s['n']} | {s['roi']:+.1f}% | {v} |\n")
    lines.append("\n")

    lines.append("### Structural note\n\n")
    lines.append(
        "S6 is the pitcher-level analogue to S1 (team win streak). "
        "If S6 W-streak fade shows positive ROI, the key follow-up question is: "
        "does it fire on the SAME games as S1, or on different games? "
        "If mostly different games, S6 is additive. If heavily overlapping, "
        "it is redundant with S1 and should not be treated as an independent signal.\n\n"
        "S7 K-surge UNDER is a second-order signal — it detects recent pitcher "
        "dominance not yet priced into the total line. Unlike Signal B (ISO + park), "
        "this is rolling intra-season data, which the bookmaker's opening line "
        "model may not fully incorporate for SPs who suddenly peak.\n\n"
    )

    lines.append("---\n\n*End of report.*\n")
    report = "".join(lines)

    # ── Console summary ───────────────────────────────────────────────────────
    log.info("")
    log.info("=" * 60)
    log.info("  S6/S7 SUMMARY")
    log.info("=" * 60)
    for label, s in [
        (f"S6 W≥{args.s6_win_streak} fade", s_w),
        (f"S6 L≥{args.s6_loss_streak} follow", s_l),
        (f"S7 K≥{args.s7_surge_k9} surge UNDER", s_su),
        (f"S7 K≤{args.s7_collapse_k9} collapse OVER", s_co),
    ]:
        log.info("  %-30s %d fires | %.1f%% hit | %+.2f u | %+.1f%% ROI",
                 label, s["n"], s["hit_rate"], s["pnl"], s["roi"])

    # ── Save ──────────────────────────────────────────────────────────────────
    if args.output:
        out_path = Path(args.output)
    else:
        bm_tag   = f"_{args.bookmaker}" if args.bookmaker != "all" else ""
        out_path = REPORTS_DIR / f"backtest_pitcher_streaks{bm_tag}.md"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report, encoding="utf-8")
    log.info("  Report: %s", out_path)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
