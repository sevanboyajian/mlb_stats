#!/usr/bin/env python3
"""
backtest_early_season.py — Regression test for early-season signals.

Tests four signals that fire from prior-year player stats and are designed
to provide Top Tip / Top 5 picks in the first 14 days of the season,
before streak-based signals (H3b, S1, S1+H2) accumulate enough data.

These signals may also remain valid as reinforcers throughout the full season
— the backtest covers both early-season windows AND the full season so we
can compare performance across phases.

SIGNALS TESTED
──────────────
  Signal A  SP ERA edge
            Prior-season ERA ≤ 3.50 (starter) vs opposing team OPS ≤ .710
            Bet: game_winner (same side as pitcher's team)
            Enhancer: SP K/9 ≥ 9.0 + opponent K-rate ≥ 24%

  Signal B  Team ISO + park factor OVER lean
            Batting team prior-season ISO ≥ .180 + venue park_factor_runs > 105
            Bet: total (over) — both teams checked, fire if either qualifies
            Enhancer: opposing SP prior-season HR/9 ≥ 1.4

  Signal C  Thin starter suppressor
            SP had < 10 GS prior season OR avg IP/GS < 4.5
            Effect: reduces confidence on ML for that team by one tier
            Tested as a standalone fade (bet against the thin-starter team)

  Signal D  Home/away batting split edge
            Top-4 hitters in a team's prior-season lineup:
            avg home OPS - avg away OPS ≥ .080 (strong home OPS advantage)
            Bet: home team (game_winner home)

METHODOLOGY
───────────
  - Prior-year stats computed from player_game_stats for season N-1
  - Applied to season N games (the "new season" being predicted)
  - No look-ahead bias: only stats from games COMPLETED in season N-1 are used
  - Day-of-season gate: each signal is tested across ALL games, then separately
    for Days 1-7, Days 8-14, Days 15+
  - Bookmaker priority: same ladder as v_closing_game_odds view
    (draftkings → fanduel → betmgm → betonlineag → sbro → oddswarehouse)
  - Seasons covered: configurable via --seasons (default: all available)

USAGE
─────
    python backtest_early_season.py
    python backtest_early_season.py --seasons 2023 2024 2025
    python backtest_early_season.py --signal A
    python backtest_early_season.py --signal A --min-era 3.75 --min-ops 0.720
    python backtest_early_season.py --early-only          # Days 1-14 only
    python backtest_early_season.py --bookmaker all
    python backtest_early_season.py --output reports/early_season_signals.md

OUTPUT
──────
  Markdown report saved to reports/ folder.
  Console summary printed after each signal.
  Report includes:
    - Per-signal fire count, hit rate, ROI, avg odds
    - Day-of-season breakdown (Days 1-7 / 8-14 / 15+)
    - Season-by-season stability check
    - Head-to-head: signals alone vs signal + enhancer
    - Cross-signal overlap: how many games fired 2+ signals
"""

import argparse
import logging
import math
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from core.db.connection import connect as db_connect

# ── Paths ──────────────────────────────────────────────────────────────────────
DEFAULT_DB = r"C:\Users\sevan\OneDrive\Documents\Python\mlb_stats\mlb_stats.db"
REPORTS_DIR = Path(r"C:\Users\sevan\OneDrive\Documents\Python\mlb_stats\reports")

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# BOOKMAKER PRIORITY
# Same ladder as v_closing_game_odds view — keeps results consistent
# ══════════════════════════════════════════════════════════════════════════════

BOOKMAKER_PRIORITY = [
    "draftkings",
    "fanduel",
    "betmgm",
    "betonlineag",
    "sbro",
    "oddswarehouse",
]

BOOKMAKER_SET_ALL  = set(BOOKMAKER_PRIORITY)
BOOKMAKER_SET_SBRO = {"sbro"}
BOOKMAKER_SET_OW   = {"oddswarehouse"}


def get_bookmaker_filter(bookmaker_arg: str) -> set:
    """Return the set of accepted bookmakers for the chosen --bookmaker arg."""
    if bookmaker_arg == "all":
        return BOOKMAKER_SET_ALL
    if bookmaker_arg == "oddswarehouse":
        return BOOKMAKER_SET_OW
    return BOOKMAKER_SET_SBRO  # default


# ══════════════════════════════════════════════════════════════════════════════
# DB HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def connect(db_path: str) -> sqlite3.Connection:
    con = db_connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA foreign_keys=ON")
    return con


def implied_prob(american_odds: int) -> float:
    """Convert American odds to implied probability (0-1)."""
    if american_odds is None:
        return None
    if american_odds > 0:
        return 100.0 / (american_odds + 100.0)
    return abs(american_odds) / (abs(american_odds) + 100.0)


def payout_multiplier(american_odds: int) -> float:
    """
    Return profit per 1 unit staked.
    e.g. -150 → 0.667   +130 → 1.30
    """
    if american_odds is None:
        return None
    if american_odds > 0:
        return american_odds / 100.0
    return 100.0 / abs(american_odds)


def day_of_season(game_date: str, season_start: str) -> int:
    """Return how many days into the season game_date falls (Day 1 = opening day)."""
    gd = datetime.strptime(game_date, "%Y-%m-%d").date()
    ss = datetime.strptime(season_start, "%Y-%m-%d").date()
    return (gd - ss).days + 1


# ══════════════════════════════════════════════════════════════════════════════
# PRIOR-YEAR STAT BUILDERS
# All return dicts keyed by team_id or player_id for fast lookup.
# All use only completed games (status='Final') from season N-1.
# ══════════════════════════════════════════════════════════════════════════════

def build_team_batting_stats(con: sqlite3.Connection, season: int) -> dict:
    """
    Compute team-level prior-year batting stats from player_game_stats.
    Returns {team_id: {ops, iso, k_rate, home_ops, away_ops}}

    ISO = SLG - BA  (Isolated Power — measures raw power, strips singles)
    K_rate = strikeouts / plate_appearances

    Home/away split:
      is_home = 1 when team_id == games.home_team_id for that game
    """
    log.info("  Computing team batting stats for season %d ...", season)

    rows = con.execute("""
        SELECT
            pgs.team_id,
            -- is_home flag: 1 if this team is the home team in this game
            CASE WHEN g.home_team_id = pgs.team_id THEN 1 ELSE 0 END AS is_home,
            SUM(pgs.plate_appearances)  AS pa,
            SUM(pgs.at_bats)            AS ab,
            SUM(pgs.hits)               AS h,
            SUM(pgs.home_runs)          AS hr,
            SUM(pgs.doubles)            AS dbl,
            SUM(pgs.triples)            AS trp,
            SUM(pgs.strikeouts_bat)     AS k,
            SUM(pgs.walks)              AS bb
        FROM player_game_stats pgs
        JOIN games g ON g.game_pk = pgs.game_pk
        WHERE g.season    = ?
          AND g.game_type = 'R'
          AND g.status    = 'Final'
          AND pgs.player_role = 'batter'
          AND pgs.plate_appearances > 0
        GROUP BY pgs.team_id, is_home
    """, (season,)).fetchall()

    # Aggregate: home, away, overall per team
    team_data = defaultdict(lambda: {
        "home_pa": 0, "home_ab": 0, "home_h": 0, "home_hr": 0,
        "home_dbl": 0, "home_trp": 0, "home_k": 0, "home_bb": 0,
        "away_pa": 0, "away_ab": 0, "away_h": 0, "away_hr": 0,
        "away_dbl": 0, "away_trp": 0, "away_k": 0, "away_bb": 0,
    })

    for r in rows:
        d = team_data[r["team_id"]]
        prefix = "home_" if r["is_home"] else "away_"
        d[prefix + "pa"]  += r["pa"]  or 0
        d[prefix + "ab"]  += r["ab"]  or 0
        d[prefix + "h"]   += r["h"]   or 0
        d[prefix + "hr"]  += r["hr"]  or 0
        d[prefix + "dbl"] += r["dbl"] or 0
        d[prefix + "trp"] += r["trp"] or 0
        d[prefix + "k"]   += r["k"]   or 0
        d[prefix + "bb"]  += r["bb"]  or 0

    # Compute derived stats
    result = {}
    for team_id, d in team_data.items():
        total_ab  = d["home_ab"]  + d["away_ab"]
        total_h   = d["home_h"]   + d["away_h"]
        total_hr  = d["home_hr"]  + d["away_hr"]
        total_dbl = d["home_dbl"] + d["away_dbl"]
        total_trp = d["home_trp"] + d["away_trp"]
        total_pa  = d["home_pa"]  + d["away_pa"]
        total_k   = d["home_k"]   + d["away_k"]

        def _ops(ab, h, hr, dbl, trp, bb):
            if ab <= 0:
                return None
            ba  = h / ab
            obp = (h + bb) / (ab + bb) if (ab + bb) > 0 else 0
            slg = (h + dbl + 2*trp + 3*hr) / ab
            return obp + slg

        def _iso(ab, h, hr, dbl, trp):
            if ab <= 0:
                return None
            ba  = h / ab
            slg = (h + dbl + 2*trp + 3*hr) / ab
            return slg - ba

        overall_ops = _ops(
            total_ab, total_h, total_hr, total_dbl, total_trp,
            d["home_bb"] + d["away_bb"]
        )
        home_ops = _ops(
            d["home_ab"], d["home_h"], d["home_hr"], d["home_dbl"],
            d["home_trp"], d["home_bb"]
        )
        away_ops = _ops(
            d["away_ab"], d["away_h"], d["away_hr"], d["away_dbl"],
            d["away_trp"], d["away_bb"]
        )
        overall_iso = _iso(total_ab, total_h, total_hr, total_dbl, total_trp)
        k_rate = (total_k / total_pa) if total_pa > 0 else None

        result[team_id] = {
            "ops":      overall_ops,
            "home_ops": home_ops,
            "away_ops": away_ops,
            "iso":      overall_iso,
            "k_rate":   k_rate,
            "ab":       total_ab,
        }

    log.info("    → %d teams computed.", len(result))
    return result


def build_sp_stats(con: sqlite3.Connection, season: int) -> dict:
    """
    Compute per-pitcher prior-year starting pitcher stats.
    Returns {player_id: {era, k_per_9, hr_per_9, gs, ip_per_gs, whip}}

    'Starting pitcher' = games where innings_pitched >= 1.0 AND batting_order IS NULL
    (proxy: pitchers who logged meaningful innings in a game without batting)
    We use GS count from games where innings_pitched >= 3.0 as proxy for starts.
    """
    log.info("  Computing SP stats for season %d ...", season)

    rows = con.execute("""
        SELECT
            pgs.player_id,
            pgs.team_id,
            COUNT(*)                    AS appearances,
            SUM(CASE WHEN pgs.innings_pitched >= 3.0 THEN 1 ELSE 0 END) AS gs_proxy,
            SUM(pgs.innings_pitched)    AS total_ip,
            SUM(pgs.earned_runs)        AS total_er,
            SUM(pgs.strikeouts_pit)     AS total_k,
            SUM(pgs.hr_allowed)         AS total_hr,
            SUM(pgs.hits_allowed)       AS total_h,
            SUM(pgs.walks_allowed)      AS total_bb,
            SUM(pgs.quality_start)      AS total_qs
        FROM player_game_stats pgs
        JOIN games g ON g.game_pk = pgs.game_pk
        WHERE g.season    = ?
          AND g.game_type = 'R'
          AND g.status    = 'Final'
          AND pgs.player_role   = 'pitcher'
          AND pgs.innings_pitched >= 1.0   -- exclude pure relievers
        GROUP BY pgs.player_id, pgs.team_id
        HAVING gs_proxy >= 3               -- require at least 3 meaningful outings
    """, (season,)).fetchall()

    result = {}
    for r in rows:
        ip      = r["total_ip"] or 0.0
        er      = r["total_er"] or 0
        k       = r["total_k"]  or 0
        hr      = r["total_hr"] or 0
        h       = r["total_h"]  or 0
        bb      = r["total_bb"] or 0
        gs      = r["gs_proxy"] or 0
        ip_per_gs = (ip / gs) if gs > 0 else 0.0
        era    = (er * 9 / ip)  if ip > 0 else None
        k9     = (k  * 9 / ip)  if ip > 0 else None
        hr9    = (hr * 9 / ip)  if ip > 0 else None
        whip   = ((h + bb) / ip) if ip > 0 else None

        result[r["player_id"]] = {
            "team_id":   r["team_id"],
            "era":       era,
            "k_per_9":   k9,
            "hr_per_9":  hr9,
            "whip":      whip,
            "gs":        gs,
            "ip_per_gs": ip_per_gs,
            "total_ip":  ip,
        }

    log.info("    → %d pitchers computed.", len(result))
    return result


def build_sp_team_map(con: sqlite3.Connection, season: int) -> dict:
    """
    Return {team_id: [{player_id, gs, avg_ip}]} for ALL pitchers with at least
    one meaningful outing (gs_proxy >= 1) for each team in the given season.

    NOTE: This intentionally does NOT pre-filter to reliable starters.
    Signal C's is_thin_starter_team() applies the gs/ip thresholds itself.
    Pre-filtering here caused all teams to appear reliable, producing zero fires.
    """
    rows = con.execute("""
        SELECT
            pgs.team_id,
            pgs.player_id,
            SUM(CASE WHEN pgs.innings_pitched >= 3.0 THEN 1 ELSE 0 END) AS gs_proxy,
            AVG(CASE WHEN pgs.innings_pitched >= 3.0
                     THEN pgs.innings_pitched ELSE NULL END)             AS avg_ip
        FROM player_game_stats pgs
        JOIN games g ON g.game_pk = pgs.game_pk
        WHERE g.season    = ?
          AND g.game_type = 'R'
          AND g.status    = 'Final'
          AND pgs.player_role = 'pitcher'
          AND pgs.innings_pitched >= 1.0
        GROUP BY pgs.team_id, pgs.player_id
        HAVING gs_proxy >= 1  -- include all pitchers with any start-length outing
    """, (season,)).fetchall()

    team_sp_map = defaultdict(list)
    for r in rows:
        team_sp_map[r["team_id"]].append({
            "player_id": r["player_id"],
            "gs":        r["gs_proxy"] or 0,
            "avg_ip":    r["avg_ip"]   or 0.0,
        })

    return dict(team_sp_map)


def build_venue_park_factors(con: sqlite3.Connection) -> dict:
    """Return {venue_id: {park_factor_runs, park_factor_hr}} from venues table."""
    rows = con.execute(
        "SELECT venue_id, park_factor_runs, park_factor_hr FROM venues"
    ).fetchall()
    return {
        r["venue_id"]: {
            "pf_runs": r["park_factor_runs"] or 100,
            "pf_hr":   r["park_factor_hr"]   or 100,
        }
        for r in rows
    }


def load_season_start(con: sqlite3.Connection, season: int) -> str:
    """Return the season_start date string for a given season."""
    row = con.execute(
        "SELECT season_start FROM seasons WHERE season = ?", (season,)
    ).fetchone()
    return row["season_start"] if row else None


def load_games_with_odds(
    con: sqlite3.Connection,
    season: int,
    bookmaker_set: set,
) -> list:
    """
    Return all Final regular-season games for the given season that have
    closing moneyline odds from an accepted bookmaker.

    Returns list of dicts with game context + closing odds.
    Bookmaker priority applied inline (same as v_closing_game_odds view).
    """
    log.info("  Loading games + closing odds for season %d ...", season)

    # Fetch all closing moneyline rows for this season
    rows = con.execute("""
        SELECT
            g.game_pk,
            g.game_date,
            g.home_team_id,
            g.away_team_id,
            g.venue_id,
            g.home_score,
            g.away_score,
            go.bookmaker,
            go.home_ml,
            go.away_ml,
            go.total_line,
            go.over_odds,
            go.under_odds
        FROM games g
        JOIN game_odds go
          ON go.game_pk     = g.game_pk
         AND go.market_type = 'moneyline'
         AND go.is_closing_line = 1
        WHERE g.season    = ?
          AND g.game_type = 'R'
          AND g.status    = 'Final'
          AND g.home_score IS NOT NULL
          AND g.away_score IS NOT NULL
          AND go.home_ml IS NOT NULL
          AND go.away_ml IS NOT NULL
        ORDER BY g.game_date, g.game_pk
    """, (season,)).fetchall()

    # Apply bookmaker priority: keep only the highest-priority bookmaker per game
    seen    = {}
    ordered = []
    for r in rows:
        bk = r["bookmaker"]
        if bk not in bookmaker_set:
            continue
        gk = r["game_pk"]
        if gk not in seen:
            seen[gk] = r
        else:
            # Replace if this bookmaker has higher priority
            curr_pri = BOOKMAKER_PRIORITY.index(seen[gk]["bookmaker"]) \
                       if seen[gk]["bookmaker"] in BOOKMAKER_PRIORITY else 999
            new_pri  = BOOKMAKER_PRIORITY.index(bk) \
                       if bk in BOOKMAKER_PRIORITY else 999
            if new_pri < curr_pri:
                seen[gk] = r

    games = [dict(r) for r in seen.values()]
    log.info("    → %d games with closing odds.", len(games))
    return games


def load_closing_totals(con: sqlite3.Connection, season: int, bookmaker_set: set) -> dict:
    """
    Return {game_pk: {total_line, over_odds, under_odds}} for closing total lines.
    """
    rows = con.execute("""
        SELECT
            go.game_pk,
            go.bookmaker,
            go.total_line,
            go.over_odds,
            go.under_odds
        FROM game_odds go
        JOIN games g ON g.game_pk = go.game_pk
        WHERE g.season       = ?
          AND g.game_type    = 'R'
          AND go.market_type = 'total'
          AND go.is_closing_line = 1
          AND go.total_line  IS NOT NULL
    """, (season,)).fetchall()

    seen = {}
    for r in rows:
        if r["bookmaker"] not in bookmaker_set:
            continue
        gk = r["game_pk"]
        if gk not in seen:
            seen[gk] = dict(r)
        else:
            curr_pri = BOOKMAKER_PRIORITY.index(seen[gk]["bookmaker"]) \
                       if seen[gk]["bookmaker"] in BOOKMAKER_PRIORITY else 999
            new_pri  = BOOKMAKER_PRIORITY.index(r["bookmaker"]) \
                       if r["bookmaker"] in BOOKMAKER_PRIORITY else 999
            if new_pri < curr_pri:
                seen[gk] = dict(r)

    return seen


# ══════════════════════════════════════════════════════════════════════════════
# RESULT GRADING
# ══════════════════════════════════════════════════════════════════════════════

def grade_ml(game: dict, bet_side: str, odds_used: int) -> float:
    """
    Grade a moneyline bet.
    bet_side: 'home' or 'away'
    Returns profit/loss in units (1 unit staked).
    """
    home_won = game["home_score"] > game["away_score"]
    bet_won  = (bet_side == "home" and home_won) or \
               (bet_side == "away" and not home_won)
    if bet_won:
        return payout_multiplier(odds_used)
    return -1.0


def grade_total(game: dict, total_odds: dict, bet_side: str) -> float:
    """
    Grade an over/under bet.
    bet_side: 'over' or 'under'
    Returns profit/loss in units (1 unit staked), or 0.0 on push.
    """
    if total_odds is None:
        return None
    total_scored = (game["home_score"] or 0) + (game["away_score"] or 0)
    line         = total_odds.get("total_line")
    if line is None:
        return None

    if bet_side == "over":
        if total_scored > line:
            return payout_multiplier(total_odds.get("over_odds", -110))
        elif total_scored < line:
            return -1.0
        return 0.0  # push
    else:  # under
        if total_scored < line:
            return payout_multiplier(total_odds.get("under_odds", -110))
        elif total_scored > line:
            return -1.0
        return 0.0


# ══════════════════════════════════════════════════════════════════════════════
# SIGNAL EVALUATORS
# Each returns a list of "fire" dicts for a single season.
# fire dict keys:
#   game_pk, game_date, day_of_season, bet_side, bet_type,
#   odds_used, pnl, enhanced (bool), season
# ══════════════════════════════════════════════════════════════════════════════

def run_signal_a(
    games: list,
    sp_stats: dict,       # prior-year SP stats {player_id: {...}}
    team_batting: dict,   # prior-year team batting {team_id: {...}}
    sp_team_starters: dict,  # {team_id: [player_id, ...]} for prior-year primary SPs
    season_start: str,
    season: int,
    era_threshold: float  = 3.50,
    ops_threshold: float  = 0.710,
    k9_boost_threshold: float = 9.0,
    k_rate_boost: float   = 0.24,
) -> list:
    """
    Signal A — SP ERA edge.
    Fire when the home team's prior-year best SP ERA ≤ era_threshold
    AND the opposing (away) team's prior-year OPS ≤ ops_threshold.
    Also fires mirror case: away SP edge vs home team weak OPS.
    Enhanced = SP K/9 ≥ k9_boost_threshold AND opp K-rate ≥ k_rate_boost.
    """
    fires = []

    for g in games:
        home_id = g["home_team_id"]
        away_id = g["away_team_id"]
        dos     = day_of_season(g["game_date"], season_start)

        # Get best SP ERA for home and away from prior-year starters
        home_sp_list = sp_team_starters.get(home_id, [])
        away_sp_list = sp_team_starters.get(away_id, [])

        def best_sp(sp_list, role_stats):
            """Return (era, k9) for the best qualifying SP on a team."""
            best_era, best_k9 = None, None
            for sp in sp_list:
                pid  = sp["player_id"]
                stat = role_stats.get(pid)
                if stat is None or stat["era"] is None:
                    continue
                if stat["gs"] < 5 or stat["ip_per_gs"] < 4.5:
                    continue  # thin starter — exclude from Signal A
                if best_era is None or stat["era"] < best_era:
                    best_era = stat["era"]
                    best_k9  = stat["k_per_9"]
            return best_era, best_k9

        home_era, home_k9 = best_sp(home_sp_list, sp_stats)
        away_era, away_k9 = best_sp(away_sp_list, sp_stats)

        home_bat = team_batting.get(home_id, {})
        away_bat = team_batting.get(away_id, {})

        # Case 1: home SP dominates away offense
        if (home_era is not None and home_era <= era_threshold and
                away_bat.get("ops") is not None and
                away_bat["ops"] <= ops_threshold):
            enhanced = (
                home_k9 is not None and home_k9 >= k9_boost_threshold and
                away_bat.get("k_rate") is not None and
                away_bat["k_rate"] >= k_rate_boost
            )
            pnl = grade_ml(g, "home", g["home_ml"])
            fires.append({
                "game_pk":      g["game_pk"],
                "game_date":    g["game_date"],
                "day_of_season": dos,
                "bet_side":     "home",
                "bet_type":     "moneyline",
                "odds_used":    g["home_ml"],
                "pnl":          pnl,
                "enhanced":     enhanced,
                "season":       season,
            })

        # Case 2: away SP dominates home offense
        if (away_era is not None and away_era <= era_threshold and
                home_bat.get("ops") is not None and
                home_bat["ops"] <= ops_threshold):
            enhanced = (
                away_k9 is not None and away_k9 >= k9_boost_threshold and
                home_bat.get("k_rate") is not None and
                home_bat["k_rate"] >= k_rate_boost
            )
            pnl = grade_ml(g, "away", g["away_ml"])
            fires.append({
                "game_pk":      g["game_pk"],
                "game_date":    g["game_date"],
                "day_of_season": dos,
                "bet_side":     "away",
                "bet_type":     "moneyline",
                "odds_used":    g["away_ml"],
                "pnl":          pnl,
                "enhanced":     enhanced,
                "season":       season,
            })

    return fires


def run_signal_b(
    games: list,
    team_batting: dict,
    sp_stats: dict,
    sp_team_starters: dict,
    venue_pf: dict,
    closing_totals: dict,
    season_start: str,
    season: int,
    iso_threshold: float  = 0.180,
    pf_threshold: int     = 105,
    hr9_threshold: float  = 1.40,
) -> list:
    """
    Signal B — Team ISO + park factor OVER lean.
    Fire when either team's prior-year ISO ≥ iso_threshold
    AND venue park_factor_runs > pf_threshold.
    Bet: OVER.
    Enhanced = opposing SP prior-year HR/9 ≥ hr9_threshold.
    """
    fires = []

    for g in games:
        total_data = closing_totals.get(g["game_pk"])
        if total_data is None:
            continue

        home_id  = g["home_team_id"]
        away_id  = g["away_team_id"]
        venue_id = g["venue_id"]
        dos      = day_of_season(g["game_date"], season_start)

        pf_data  = venue_pf.get(venue_id, {})
        pf_runs  = pf_data.get("pf_runs", 100)

        home_bat = team_batting.get(home_id, {})
        away_bat = team_batting.get(away_id, {})

        if pf_runs <= pf_threshold:
            continue  # park doesn't qualify

        # Check either team's ISO
        home_qualifies = home_bat.get("iso") is not None and home_bat["iso"] >= iso_threshold
        away_qualifies = away_bat.get("iso") is not None and away_bat["iso"] >= iso_threshold

        if not (home_qualifies or away_qualifies):
            continue

        # Enhanced: check opposing SP's HR/9
        home_sp_list = sp_team_starters.get(home_id, [])
        away_sp_list = sp_team_starters.get(away_id, [])

        def worst_sp_hr9(sp_list):
            """Return the highest HR/9 among the team's prior-year SPs."""
            worst = None
            for sp in sp_list:
                stat = sp_stats.get(sp["player_id"])
                if stat and stat.get("hr_per_9") is not None:
                    if worst is None or stat["hr_per_9"] > worst:
                        worst = stat["hr_per_9"]
            return worst

        home_sp_hr9 = worst_sp_hr9(home_sp_list)
        away_sp_hr9 = worst_sp_hr9(away_sp_list)

        enhanced = (
            (home_sp_hr9 is not None and home_sp_hr9 >= hr9_threshold) or
            (away_sp_hr9 is not None and away_sp_hr9 >= hr9_threshold)
        )

        pnl = grade_total(g, total_data, "over")
        if pnl is None:
            continue

        fires.append({
            "game_pk":       g["game_pk"],
            "game_date":     g["game_date"],
            "day_of_season": dos,
            "bet_side":      "over",
            "bet_type":      "total",
            "odds_used":     total_data.get("over_odds", -110),
            "pnl":           pnl,
            "enhanced":      enhanced,
            "season":        season,
        })

    return fires


def run_signal_c(
    games: list,
    sp_team_starters: dict,
    sp_stats: dict,
    season_start: str,
    season: int,
    max_gs: int         = 10,
    min_ip_per_gs: float = 4.5,
) -> list:
    """
    Signal C — Thin starter flag (bet against).
    Fire when the home OR away team's best prior-year SP had < max_gs
    OR avg_ip < min_ip_per_gs. Bet against that team (fade).
    If both teams have thin starters, skip — no edge.
    """
    fires = []

    for g in games:
        home_id = g["home_team_id"]
        away_id = g["away_team_id"]
        dos     = day_of_season(g["game_date"], season_start)

        def is_thin_starter_team(team_id):
            """Return True if this team lacks a reliable prior-year SP."""
            sp_list = sp_team_starters.get(team_id, [])
            # Check if any SP on the team has ≥ max_gs AND ≥ min_ip_per_gs
            has_reliable = any(
                sp["gs"] >= max_gs and sp["avg_ip"] >= min_ip_per_gs
                for sp in sp_list
            )
            return not has_reliable  # thin = no reliable starter found

        home_thin = is_thin_starter_team(home_id)
        away_thin = is_thin_starter_team(away_id)

        # Both thin = no edge signal
        if home_thin and away_thin:
            continue
        # Neither thin = no signal
        if not home_thin and not away_thin:
            continue

        # Fade the thin-starter team
        if home_thin:
            bet_side  = "away"
            odds_used = g["away_ml"]
        else:
            bet_side  = "home"
            odds_used = g["home_ml"]

        pnl = grade_ml(g, bet_side, odds_used)
        fires.append({
            "game_pk":       g["game_pk"],
            "game_date":     g["game_date"],
            "day_of_season": dos,
            "bet_side":      bet_side,
            "bet_type":      "moneyline",
            "odds_used":     odds_used,
            "pnl":           pnl,
            "enhanced":      False,  # no enhancer for Signal C
            "season":        season,
        })

    return fires


def run_signal_d(
    games: list,
    team_batting: dict,
    season_start: str,
    season: int,
    split_threshold: float = 0.080,
) -> list:
    """
    Signal D — Home/away batting split edge.
    Fire when the home team's prior-year (home_OPS - away_OPS) ≥ split_threshold.
    Bet: home team (game_winner home).
    Enhanced: away team has opposite pattern (away_OPS > home_OPS).
    """
    fires = []

    for g in games:
        home_id = g["home_team_id"]
        away_id = g["away_team_id"]
        dos     = day_of_season(g["game_date"], season_start)

        home_bat = team_batting.get(home_id, {})
        away_bat = team_batting.get(away_id, {})

        home_ops_h = home_bat.get("home_ops")
        home_ops_a = home_bat.get("away_ops")

        if home_ops_h is None or home_ops_a is None:
            continue

        home_split = home_ops_h - home_ops_a
        if home_split < split_threshold:
            continue

        # Enhanced: away team performs worse at home (positive away OPS bias)
        away_ops_h = away_bat.get("home_ops")
        away_ops_a = away_bat.get("away_ops")
        enhanced   = (
            away_ops_h is not None and away_ops_a is not None and
            (away_ops_a - away_ops_h) >= 0.030  # away team hits better on road
        )

        pnl = grade_ml(g, "home", g["home_ml"])
        fires.append({
            "game_pk":       g["game_pk"],
            "game_date":     g["game_date"],
            "day_of_season": dos,
            "bet_side":      "home",
            "bet_type":      "moneyline",
            "odds_used":     g["home_ml"],
            "pnl":           pnl,
            "enhanced":      enhanced,
            "season":        season,
        })

    return fires


# ══════════════════════════════════════════════════════════════════════════════
# REPORT GENERATION
# ══════════════════════════════════════════════════════════════════════════════

def summarise(fires: list, label: str, day_filter=None) -> dict:
    """
    Compute summary stats for a list of fire dicts.
    day_filter: None = all games, (lo, hi) = inclusive day-of-season range.
    Returns dict with keys: n, wins, losses, pushes, pnl, hit_rate, roi, avg_odds.
    """
    subset = fires
    if day_filter:
        lo, hi = day_filter
        subset = [f for f in fires if lo <= f["day_of_season"] <= hi]

    n      = len(subset)
    wins   = sum(1 for f in subset if f["pnl"] and f["pnl"] > 0)
    losses = sum(1 for f in subset if f["pnl"] and f["pnl"] < 0)
    pushes = sum(1 for f in subset if f["pnl"] == 0)
    pnl    = sum(f["pnl"] for f in subset if f["pnl"] is not None)
    avg_odds = (sum(f["odds_used"] for f in subset if f["odds_used"] is not None) / n) if n > 0 else 0

    hit_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0.0
    roi      = (pnl / n * 100) if n > 0 else 0.0

    return {
        "label":    label,
        "n":        n,
        "wins":     wins,
        "losses":   losses,
        "pushes":   pushes,
        "pnl":      pnl,
        "hit_rate": hit_rate,
        "roi":      roi,
        "avg_odds": avg_odds,
    }


def format_summary_table(rows: list) -> str:
    """Format a list of summary dicts as a markdown table."""
    header = "| Window | N | W | L | Hit% | P&L (units) | ROI% | Avg Odds |\n"
    sep    = "|--------|---|---|---|------|-------------|------|----------|\n"
    lines  = [header, sep]
    for r in rows:
        lines.append(
            f"| {r['label']:<22} | {r['n']:>4} | {r['wins']:>3} | {r['losses']:>3} "
            f"| {r['hit_rate']:>5.1f}% | {r['pnl']:>+10.2f} | {r['roi']:>+5.1f}% "
            f"| {r['avg_odds']:>+7.0f} |\n"
        )
    return "".join(lines)


def season_by_season(fires: list, seasons: list) -> str:
    """Return a markdown table of per-season stats."""
    header = "| Season | N | Hit% | P&L | ROI% |\n"
    sep    = "|--------|---|------|-----|------|\n"
    lines  = [header, sep]
    for s in sorted(seasons):
        subset = [f for f in fires if f["season"] == s]
        stats  = summarise(subset, str(s))
        lines.append(
            f"| {s} | {stats['n']:>4} | {stats['hit_rate']:>5.1f}% "
            f"| {stats['pnl']:>+6.2f} | {stats['roi']:>+5.1f}% |\n"
        )
    return "".join(lines)


def build_report(
    signal_results: dict,
    seasons_run: list,
    args,
    run_ts: str,
) -> str:
    """Build the full markdown report."""
    lines = []
    lines.append(f"# Early Season Signal Backtest Report\n")
    lines.append(f"Generated: {run_ts}  \n")
    lines.append(f"Bookmaker: `{args.bookmaker}`  \n")
    lines.append(f"Seasons: {', '.join(str(s) for s in sorted(seasons_run))}  \n")
    lines.append(f"Signals tested: {', '.join(args.signal if args.signal else ['A','B','C','D'])}  \n\n")
    lines.append("---\n\n")

    lines.append("## Thresholds used\n\n")
    lines.append(f"| Parameter | Value |\n|-----------|-------|\n")
    lines.append(f"| Signal A: ERA ≤ | {args.min_era} |\n")
    lines.append(f"| Signal A: Opp OPS ≤ | {args.max_ops} |\n")
    lines.append(f"| Signal A: SP K/9 ≥ (enhanced) | {args.k9_boost} |\n")
    lines.append(f"| Signal B: ISO ≥ | {args.min_iso} |\n")
    lines.append(f"| Signal B: Park factor runs > | {args.min_pf} |\n")
    lines.append(f"| Signal B: SP HR/9 ≥ (enhanced) | {args.hr9_boost} |\n")
    lines.append(f"| Signal C: Max GS (thin) | {args.max_gs} |\n")
    lines.append(f"| Signal C: Min IP/GS (thin) | {args.min_ip_gs} |\n")
    lines.append(f"| Signal D: Home OPS split ≥ | {args.min_split} |\n\n")
    lines.append("---\n\n")

    signal_labels = {
        "A": "Signal A — SP ERA edge (moneyline)",
        "B": "Signal B — Team ISO + park factor (OVER)",
        "C": "Signal C — Thin starter fade (moneyline)",
        "D": "Signal D — Home/away bat split (moneyline home)",
    }

    for sig_key in ["A", "B", "C", "D"]:
        if sig_key not in signal_results:
            continue
        fires = signal_results[sig_key]
        lines.append(f"## {signal_labels[sig_key]}\n\n")

        # Overall
        all_fires      = summarise(fires, "All games (all seasons)")
        enhanced_fires = summarise([f for f in fires if f["enhanced"]], "Enhanced only")
        base_fires     = summarise([f for f in fires if not f["enhanced"]], "Base only (no enhancer)")

        # Day windows
        days_1_7   = summarise(fires, "Days 1–7   (Week 1)", (1, 7))
        days_8_14  = summarise(fires, "Days 8–14  (Week 2)", (8, 14))
        days_15_30 = summarise(fires, "Days 15–30 (rest of April)", (15, 30))
        days_31p   = summarise(fires, "Days 31+   (post-April)", (31, 999))

        lines.append(format_summary_table([
            all_fires,
            enhanced_fires,
            base_fires,
            days_1_7,
            days_8_14,
            days_15_30,
            days_31p,
        ]))
        lines.append("\n")

        # Season-by-season
        lines.append("### Season-by-season stability\n\n")
        lines.append(season_by_season(fires, seasons_run))
        lines.append("\n")

        # Verdict
        roi_all  = all_fires["roi"]
        roi_e    = enhanced_fires["roi"]
        n        = all_fires["n"]
        lines.append("### Verdict\n\n")
        if n < 30:
            lines.append(f"> ⚠️  Sample too small ({n} fires). Do not draw conclusions.\n\n")
        elif roi_all >= 5.0:
            lines.append(f"> ✅  ROI {roi_all:+.1f}% on {n} fires — above +4.5% vig threshold. Candidate for live model.\n\n")
        elif roi_all >= 2.0:
            lines.append(f"> 🟡  ROI {roi_all:+.1f}% on {n} fires — borderline. Watch with more data.\n\n")
        else:
            lines.append(f"> ❌  ROI {roi_all:+.1f}% on {n} fires — below threshold. Do not add to model.\n\n")
        if enhanced_fires["n"] >= 20 and roi_e > roi_all + 3:
            lines.append(f"> 💡  Enhanced version ROI {roi_e:+.1f}% on {enhanced_fires['n']} fires — consider enhanced-only gate.\n\n")

        lines.append("---\n\n")

    # Cross-signal overlap
    lines.append("## Cross-signal overlap\n\n")
    lines.append("Games that fired two or more signals simultaneously:\n\n")
    all_game_pks = defaultdict(list)
    for sig_key, fires in signal_results.items():
        for f in fires:
            all_game_pks[f["game_pk"]].append(sig_key)

    multi_fire = {pk: sigs for pk, sigs in all_game_pks.items() if len(sigs) >= 2}
    lines.append(f"Total overlap games: **{len(multi_fire)}**  \n")

    if multi_fire:
        combo_counts = defaultdict(int)
        combo_pnl    = defaultdict(float)
        for pk, sigs in multi_fire.items():
            combo = "+".join(sorted(set(sigs)))
            combo_counts[combo] += 1
            # P&L: use first fire's pnl (same game, assume same outcome direction)
            for sig_key, fires in signal_results.items():
                for f in fires:
                    if f["game_pk"] == pk:
                        combo_pnl[combo] += f["pnl"] or 0
                        break

        lines.append("\n| Combination | Fires | P&L |\n|-------------|-------|-----|\n")
        for combo in sorted(combo_counts.keys()):
            lines.append(f"| {combo} | {combo_counts[combo]} | {combo_pnl[combo]:+.2f} |\n")

    lines.append("\n---\n\n")
    lines.append("*End of report.*\n")
    return "".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(
        description="Backtest early-season prior-year roster signals"
    )
    p.add_argument("--db",         default=DEFAULT_DB,   help="Path to mlb_stats.db")
    p.add_argument("--bookmaker",  default="sbro",
                   choices=["sbro", "oddswarehouse", "all"],
                   help="Bookmaker source (default: sbro)")
    p.add_argument("--seasons",    nargs="+", type=int,
                   help="Seasons to test e.g. 2022 2023 2024 (default: all available)")
    p.add_argument("--signal",     nargs="+", choices=["A","B","C","D"],
                   help="Specific signal(s) to run (default: all four)")
    p.add_argument("--early-only", action="store_true",
                   help="Restrict output to Days 1-14 only")
    p.add_argument("--output",     default=None,
                   help="Output file path (default: reports/backtest_early_season[_bookmaker].md)")

    # Threshold overrides
    p.add_argument("--min-era",    type=float, default=3.50,  help="Signal A: SP ERA ≤ (default 3.50)")
    p.add_argument("--max-ops",    type=float, default=0.710, help="Signal A: Opp OPS ≤ (default 0.710)")
    p.add_argument("--k9-boost",   type=float, default=9.0,   help="Signal A: K/9 enhanced gate (default 9.0)")
    p.add_argument("--min-iso",    type=float, default=0.180, help="Signal B: ISO ≥ (default 0.180)")
    p.add_argument("--min-pf",     type=int,   default=105,   help="Signal B: Park factor runs > (default 105)")
    p.add_argument("--hr9-boost",  type=float, default=1.40,  help="Signal B: SP HR/9 ≥ enhanced gate (default 1.40)")
    p.add_argument("--max-gs",     type=int,   default=10,    help="Signal C: Max GS before 'thin' (default 10)")
    p.add_argument("--min-ip-gs",  type=float, default=4.5,   help="Signal C: Min IP/GS for 'reliable' (default 4.5)")
    p.add_argument("--min-split",  type=float, default=0.080, help="Signal D: Home/away OPS split ≥ (default 0.080)")

    args = p.parse_args()

    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    log.info("=" * 60)
    log.info("  EARLY SEASON SIGNAL BACKTEST")
    log.info("  %s", run_ts)
    log.info("=" * 60)

    # Connect
    if not Path(args.db).exists():
        log.error("DB not found: %s", args.db)
        sys.exit(1)
    con = connect(args.db)

    # Determine seasons
    available = [
        r[0] for r in con.execute(
            "SELECT season FROM seasons WHERE season >= 2016 ORDER BY season"
        ).fetchall()
    ]
    # We need N-1 stats so minimum is 2016 (uses 2015 prior year)
    seasons_to_test = args.seasons if args.seasons else available
    seasons_to_test = [s for s in seasons_to_test if s in available and s >= 2016]

    if not seasons_to_test:
        log.error("No valid seasons found. Need season ≥ 2016 (requires prior-year data).")
        sys.exit(1)

    log.info("Seasons to test: %s", seasons_to_test)
    log.info("Bookmaker: %s", args.bookmaker)
    sigs_to_run = args.signal or ["A", "B", "C", "D"]
    log.info("Signals: %s", sigs_to_run)

    bookmaker_set = get_bookmaker_filter(args.bookmaker)
    venue_pf      = build_venue_park_factors(con)

    signal_results = {}

    for season in seasons_to_test:
        prior = season - 1
        log.info("")
        log.info("── Season %d (prior year stats from %d) ──────────────", season, prior)

        season_start = load_season_start(con, season)
        if not season_start:
            log.warning("  Season %d not in seasons table — skipping.", season)
            continue

        # Load prior-year stats
        team_batting   = build_team_batting_stats(con, prior)
        sp_stats       = build_sp_stats(con, prior)
        sp_team_map    = build_sp_team_map(con, prior)

        # Load current-season games + odds
        games           = load_games_with_odds(con, season, bookmaker_set)
        closing_totals  = load_closing_totals(con, season, bookmaker_set)

        if not games:
            log.warning("  No games with odds found for season %d — skipping.", season)
            continue

        # Build sp_team_starters compatible format for signal functions
        # Convert sp_team_map to {team_id: [{player_id, gs, avg_ip}]}
        sp_team_starters = sp_team_map  # already in correct format

        # Run signals
        if "A" in sigs_to_run:
            fires = run_signal_a(
                games, sp_stats, team_batting, sp_team_starters,
                season_start, season,
                era_threshold=args.min_era,
                ops_threshold=args.max_ops,
                k9_boost_threshold=args.k9_boost,
            )
            signal_results.setdefault("A", []).extend(fires)
            log.info("  Signal A: %d fires this season.", len(fires))

        if "B" in sigs_to_run:
            fires = run_signal_b(
                games, team_batting, sp_stats, sp_team_starters,
                venue_pf, closing_totals,
                season_start, season,
                iso_threshold=args.min_iso,
                pf_threshold=args.min_pf,
                hr9_threshold=args.hr9_boost,
            )
            signal_results.setdefault("B", []).extend(fires)
            log.info("  Signal B: %d fires this season.", len(fires))

        if "C" in sigs_to_run:
            fires = run_signal_c(
                games, sp_team_starters, sp_stats,
                season_start, season,
                max_gs=args.max_gs,
                min_ip_per_gs=args.min_ip_gs,
            )
            signal_results.setdefault("C", []).extend(fires)
            log.info("  Signal C: %d fires this season.", len(fires))

        if "D" in sigs_to_run:
            fires = run_signal_d(
                games, team_batting,
                season_start, season,
                split_threshold=args.min_split,
            )
            signal_results.setdefault("D", []).extend(fires)
            log.info("  Signal D: %d fires this season.", len(fires))

    con.close()

    # Console quick summary
    log.info("")
    log.info("=" * 60)
    log.info("  QUICK SUMMARY")
    log.info("=" * 60)
    for sig_key in ["A", "B", "C", "D"]:
        if sig_key not in signal_results:
            continue
        fires = signal_results[sig_key]
        s = summarise(fires, f"Signal {sig_key}")
        log.info(
            "  Signal %s: %d fires | %.1f%% hit | %+.2f units | %+.1f%% ROI",
            sig_key, s["n"], s["hit_rate"], s["pnl"], s["roi"]
        )
        if args.early_only:
            early = summarise(fires, "Days 1-14", (1, 14))
            log.info(
                "    Days 1-14: %d fires | %.1f%% hit | %+.2f units | %+.1f%% ROI",
                early["n"], early["hit_rate"], early["pnl"], early["roi"]
            )

    # Build and save report
    report = build_report(signal_results, seasons_to_test, args, run_ts)

    if args.output:
        out_path = Path(args.output)
    else:
        suffix   = f"_{args.bookmaker}" if args.bookmaker != "sbro" else ""
        out_path = REPORTS_DIR / f"backtest_early_season{suffix}.md"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report, encoding="utf-8")
    log.info("")
    log.info("  Report saved: %s", out_path)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
