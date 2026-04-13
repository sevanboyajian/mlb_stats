#!/usr/bin/env python3
"""
backtest_signal_b.py — Signal B full backtest with lineup power comparison.

Tests three versions of Signal B side-by-side to answer the core question:
does comparing HOME lineup power vs AWAY lineup power produce a stronger
signal than treating them identically?

SIGNAL B VERSIONS
─────────────────
  B-either   Original design: EITHER team ISO ≥ threshold in hitter park
             → OVER lean. Weakest filter — fires most often.

  B-both     BOTH teams ISO ≥ threshold in hitter park
             → OVER lean. Maximum power matchup — both lineups can score.
             Hypothesis: mutual high-ISO matchup inflates totals more than
             one-sided power.

  B-home     Home team ISO significantly EXCEEDS away team ISO in hitter park
             (home_iso − away_iso ≥ iso_edge_threshold, default .020)
             → HOME ML lean (not OVER — home team has the lineup advantage).
             Hypothesis: books price park factor into totals efficiently but
             may underweight relative lineup power for ML pricing.

ENHANCER (shared across all three versions)
───────────────────────────────────────────
  Either starting pitcher's prior-year HR/9 ≥ hr9_threshold (default 1.40)
  → Amplifies OVER lean for B-either and B-both
  → Amplifies home ML lean for B-home (home lineup vs HR-prone away SP)

METHODOLOGY
───────────
  - Prior-year ISO computed from player_game_stats (season N-1)
    ISO = SLG − BA, from raw counting stats (not stored columns)
  - Park factors from venues table (park_factor_runs)
  - Closing odds from game_odds via bookmaker priority ladder
  - No look-ahead bias: all stats from fully completed prior season
  - 2025 full season: primary target (OddsWarehouse)
  - All seasons available: for stability comparison

OUTPUT SECTIONS
───────────────
  1. Three-way comparison table (B-either / B-both / B-home)
  2. Enhanced gate analysis per version
  3. Day-of-season breakdown per version (Days 1-7 / 8-14 / 15-30 / 31+)
  4. Season-by-season stability per version
  5. Park factor sensitivity (pf > 100 / 102 / 105 / 108)
  6. ISO edge sensitivity for B-home (.010 / .020 / .030 / .040)
  7. 2025 season deep-dive (standalone section)

USAGE
─────
    # All seasons, all bookmakers (recommended)
    python backtest_signal_b.py --bookmaker all

    # 2025 only (OW)
    python backtest_signal_b.py --seasons 2025 --bookmaker oddswarehouse

    # All seasons, SBRO then OW separately
    python backtest_signal_b.py --bookmaker sbro
    python backtest_signal_b.py --bookmaker oddswarehouse

    # Threshold sensitivity
    python backtest_signal_b.py --min-iso 0.160 --min-pf 103
    python backtest_signal_b.py --iso-edge 0.030 --bookmaker all
"""

import argparse
import logging
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
DEFAULT_DB  = r"C:\Users\sevan\OneDrive\Documents\Python\mlb_stats\mlb_stats.db"
REPORTS_DIR = Path(r"C:\Users\sevan\OneDrive\Documents\Python\mlb_stats\reports")

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Bookmaker priority ─────────────────────────────────────────────────────────
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
    con = sqlite3.connect(db_path)
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
    """Profit per 1 unit staked."""
    if american_odds is None:
        return None
    return american_odds / 100.0 if american_odds > 0 else 100.0 / abs(american_odds)


def grade_total(home_score: int, away_score: int,
                total_line: float, over_odds: int, bet: str) -> float:
    """Grade an OVER or UNDER bet. Returns P&L in units, 0 on push, None if data missing."""
    if None in (home_score, away_score, total_line):
        return None
    scored = home_score + away_score
    if bet == "over":
        if scored > total_line:
            return payout(over_odds or -110)
        if scored < total_line:
            return -1.0
        return 0.0  # push
    else:
        under_payout = payout(over_odds or -110)  # most books price both sides equally
        if scored < total_line:
            return under_payout
        if scored > total_line:
            return -1.0
        return 0.0


def grade_ml(home_score: int, away_score: int, bet_side: str, odds: int) -> float:
    """Grade a moneyline bet. Returns P&L in units."""
    if None in (home_score, away_score, odds):
        return None
    home_won = home_score > away_score
    bet_won  = (bet_side == "home" and home_won) or (bet_side == "away" and not home_won)
    return payout(odds) if bet_won else -1.0


def day_of_season(game_date: str, season_start: str) -> int:
    from datetime import datetime as dt
    return (dt.strptime(game_date, "%Y-%m-%d").date()
            - dt.strptime(season_start, "%Y-%m-%d").date()).days + 1


def load_season_start(con: sqlite3.Connection, season: int) -> str | None:
    row = con.execute(
        "SELECT season_start FROM seasons WHERE season = ?", (season,)
    ).fetchone()
    return row["season_start"] if row else None


# ══════════════════════════════════════════════════════════════════════════════
# PRIOR-YEAR STAT BUILDERS
# ══════════════════════════════════════════════════════════════════════════════

def build_team_iso(con: sqlite3.Connection, season: int) -> dict:
    """
    Compute prior-year ISO per team from raw counting stats.
    ISO = SLG − BA = (TB − H) / AB
    where TB = H + 2B + 2×3B + 3×HR

    Returns {team_id: iso_value}

    Why from counting stats and not stored columns:
      stored batting_avg / slg are per-game snapshots that may be
      season-to-date values at game time, not full-season totals.
      Summing counting stats gives the true full-season ISO.
    """
    log.info("    Building team ISO (season %d) ...", season)
    rows = con.execute("""
        SELECT
            pgs.team_id,
            SUM(pgs.at_bats)    AS ab,
            SUM(pgs.hits)       AS h,
            SUM(pgs.home_runs)  AS hr,
            SUM(pgs.doubles)    AS dbl,
            SUM(pgs.triples)    AS trp
        FROM player_game_stats pgs
        JOIN games g ON g.game_pk = pgs.game_pk
        WHERE g.season    = ?
          AND g.game_type = 'R'
          AND g.status    = 'Final'
          AND pgs.player_role = 'batter'
          AND pgs.at_bats > 0
        GROUP BY pgs.team_id
    """, (season,)).fetchall()

    result = {}
    for r in rows:
        ab  = r["ab"]  or 0
        h   = r["h"]   or 0
        hr  = r["hr"]  or 0
        dbl = r["dbl"] or 0
        trp = r["trp"] or 0
        if ab <= 0:
            continue
        tb  = h + dbl + 2 * trp + 3 * hr
        ba  = h  / ab
        slg = tb / ab
        result[r["team_id"]] = round(slg - ba, 4)

    log.info("      → %d teams with ISO.", len(result))
    return result


def build_sp_hr9(con: sqlite3.Connection, season: int) -> dict:
    """
    Compute prior-year HR/9 per team (using best/worst SP on roster).
    Returns {team_id: {"best_hr9": float, "worst_hr9": float}}
    best_hr9  = lowest HR/9 (most HR-suppressing SP on staff)
    worst_hr9 = highest HR/9 (most HR-prone SP on staff)

    For the enhancer we want to know if EITHER team's SP is HR-prone,
    so we expose both for flexibility.
    """
    log.info("    Building SP HR/9 (season %d) ...", season)
    rows = con.execute("""
        SELECT
            pgs.team_id,
            pgs.player_id,
            SUM(pgs.innings_pitched) AS ip,
            SUM(pgs.hr_allowed)      AS hr,
            SUM(CASE WHEN pgs.innings_pitched >= 3.0 THEN 1 ELSE 0 END) AS gs
        FROM player_game_stats pgs
        JOIN games g ON g.game_pk = pgs.game_pk
        WHERE g.season    = ?
          AND g.game_type = 'R'
          AND g.status    = 'Final'
          AND pgs.player_role     = 'pitcher'
          AND pgs.innings_pitched >= 1.0
        GROUP BY pgs.team_id, pgs.player_id
        HAVING gs >= 5 AND ip >= 20
    """, (season,)).fetchall()

    # Compute HR/9 per pitcher then aggregate per team
    team_hr9 = defaultdict(list)
    for r in rows:
        ip = r["ip"] or 0.0
        if ip <= 0:
            continue
        hr9 = (r["hr"] * 9) / ip
        team_hr9[r["team_id"]].append(hr9)

    result = {}
    for team_id, hr9_list in team_hr9.items():
        result[team_id] = {
            "best_hr9":  min(hr9_list),   # most suppressing SP
            "worst_hr9": max(hr9_list),   # most HR-prone SP
        }

    log.info("      → %d teams with SP HR/9.", len(result))
    return result


def build_venue_pf(con: sqlite3.Connection) -> dict:
    """Return {venue_id: park_factor_runs} from venues table."""
    rows = con.execute(
        "SELECT venue_id, park_factor_runs FROM venues"
    ).fetchall()
    return {r["venue_id"]: (r["park_factor_runs"] or 100) for r in rows}


def load_games_with_odds(
    con: sqlite3.Connection, season: int, bm_set: set
) -> list:
    """
    Load Final regular-season games with closing ML and total odds.
    Returns list of dicts — one per game, best bookmaker by priority.
    """
    log.info("    Loading games + odds (season %d) ...", season)

    # Moneyline closing rows
    ml_rows = con.execute("""
        SELECT
            g.game_pk, g.game_date,
            g.home_team_id, g.away_team_id, g.venue_id,
            g.home_score, g.away_score,
            go.bookmaker, go.home_ml, go.away_ml
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

    # Pick best bookmaker per game for ML
    ml_best = {}
    for r in ml_rows:
        if r["bookmaker"] not in bm_set:
            continue
        gk = r["game_pk"]
        if gk not in ml_best:
            ml_best[gk] = dict(r)
        else:
            curr_pri = BOOKMAKER_PRIORITY.index(ml_best[gk]["bookmaker"]) \
                       if ml_best[gk]["bookmaker"] in BOOKMAKER_PRIORITY else 999
            new_pri  = BOOKMAKER_PRIORITY.index(r["bookmaker"]) \
                       if r["bookmaker"] in BOOKMAKER_PRIORITY else 999
            if new_pri < curr_pri:
                ml_best[gk] = dict(r)

    # Total closing rows
    tot_rows = con.execute("""
        SELECT
            go.game_pk, go.bookmaker,
            go.total_line, go.over_odds, go.under_odds
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
            curr_pri = BOOKMAKER_PRIORITY.index(tot_best[gk]["bookmaker"]) \
                       if tot_best[gk]["bookmaker"] in BOOKMAKER_PRIORITY else 999
            new_pri  = BOOKMAKER_PRIORITY.index(r["bookmaker"]) \
                       if r["bookmaker"] in BOOKMAKER_PRIORITY else 999
            if new_pri < curr_pri:
                tot_best[gk] = dict(r)

    # Merge: game must have both ML and total
    games = []
    for gk, ml in ml_best.items():
        tot = tot_best.get(gk)
        if tot is None:
            continue  # skip games without total line
        game = dict(ml)
        game["total_line"]  = tot["total_line"]
        game["over_odds"]   = tot["over_odds"]
        game["under_odds"]  = tot["under_odds"]
        games.append(game)

    log.info("      → %d games with ML + total odds.", len(games))
    return games


# ══════════════════════════════════════════════════════════════════════════════
# SIGNAL B EVALUATORS — THREE VERSIONS
# Each returns a list of fire dicts.
# fire dict keys: game_pk, game_date, dos, bet_side, bet_type,
#                 odds_used, pnl, enhanced, season,
#                 home_iso, away_iso, pf_runs, iso_edge (B-home only)
# ══════════════════════════════════════════════════════════════════════════════

def _sp_is_hr_prone(team_id: int, sp_hr9: dict, threshold: float) -> bool:
    """True if the team's worst (most HR-prone) prior-year SP exceeds threshold."""
    d = sp_hr9.get(team_id)
    return d is not None and d["worst_hr9"] >= threshold


def run_b_either(
    games: list,
    team_iso: dict,
    sp_hr9: dict,
    venue_pf: dict,
    season_start: str,
    season: int,
    iso_threshold: float,
    pf_threshold: int,
    hr9_threshold: float,
) -> list:
    """
    B-either: EITHER team ISO ≥ threshold + park factor > threshold → OVER.
    Original design — broadest filter.
    """
    fires = []
    for g in games:
        home_id  = g["home_team_id"]
        away_id  = g["away_team_id"]
        pf_runs  = venue_pf.get(g["venue_id"], 100)
        if pf_runs <= pf_threshold:
            continue

        home_iso = team_iso.get(home_id)
        away_iso = team_iso.get(away_id)
        if home_iso is None or away_iso is None:
            continue

        home_q = home_iso >= iso_threshold
        away_q = away_iso >= iso_threshold
        if not (home_q or away_q):
            continue

        enhanced = (
            _sp_is_hr_prone(home_id, sp_hr9, hr9_threshold) or
            _sp_is_hr_prone(away_id, sp_hr9, hr9_threshold)
        )

        pnl = grade_total(
            g["home_score"], g["away_score"],
            g["total_line"], g["over_odds"], "over"
        )
        if pnl is None:
            continue

        fires.append({
            "game_pk":   g["game_pk"],
            "game_date": g["game_date"],
            "dos":       day_of_season(g["game_date"], season_start),
            "bet_side":  "over",
            "bet_type":  "total",
            "odds_used": g["over_odds"] or -110,
            "pnl":       pnl,
            "enhanced":  enhanced,
            "season":    season,
            "home_iso":  home_iso,
            "away_iso":  away_iso,
            "pf_runs":   pf_runs,
            "version":   "either",
        })
    return fires


def run_b_both(
    games: list,
    team_iso: dict,
    sp_hr9: dict,
    venue_pf: dict,
    season_start: str,
    season: int,
    iso_threshold: float,
    pf_threshold: int,
    hr9_threshold: float,
) -> list:
    """
    B-both: BOTH teams ISO ≥ threshold + park factor > threshold → OVER.
    Mutual power matchup — maximum scoring environment hypothesis.
    """
    fires = []
    for g in games:
        home_id  = g["home_team_id"]
        away_id  = g["away_team_id"]
        pf_runs  = venue_pf.get(g["venue_id"], 100)
        if pf_runs <= pf_threshold:
            continue

        home_iso = team_iso.get(home_id)
        away_iso = team_iso.get(away_id)
        if home_iso is None or away_iso is None:
            continue

        # BOTH must qualify — tighter than B-either
        if home_iso < iso_threshold or away_iso < iso_threshold:
            continue

        enhanced = (
            _sp_is_hr_prone(home_id, sp_hr9, hr9_threshold) or
            _sp_is_hr_prone(away_id, sp_hr9, hr9_threshold)
        )

        pnl = grade_total(
            g["home_score"], g["away_score"],
            g["total_line"], g["over_odds"], "over"
        )
        if pnl is None:
            continue

        fires.append({
            "game_pk":   g["game_pk"],
            "game_date": g["game_date"],
            "dos":       day_of_season(g["game_date"], season_start),
            "bet_side":  "over",
            "bet_type":  "total",
            "odds_used": g["over_odds"] or -110,
            "pnl":       pnl,
            "enhanced":  enhanced,
            "season":    season,
            "home_iso":  home_iso,
            "away_iso":  away_iso,
            "pf_runs":   pf_runs,
            "version":   "both",
        })
    return fires


def run_b_home(
    games: list,
    team_iso: dict,
    sp_hr9: dict,
    venue_pf: dict,
    season_start: str,
    season: int,
    iso_threshold: float,
    pf_threshold: int,
    hr9_threshold: float,
    iso_edge: float,
) -> list:
    """
    B-home: Home team ISO EXCEEDS away ISO by ≥ iso_edge in hitter park
            AND home ISO ≥ iso_threshold → HOME ML bet.

    Rationale: when the home lineup is meaningfully more powerful than the
    visitor's in a run-friendly park, the home team has a structural offensive
    advantage that may not be fully priced into the moneyline.

    Enhanced: away SP is HR-prone (hr9 ≥ threshold) — compounds home power edge.
    """
    fires = []
    for g in games:
        home_id  = g["home_team_id"]
        away_id  = g["away_team_id"]
        pf_runs  = venue_pf.get(g["venue_id"], 100)
        if pf_runs <= pf_threshold:
            continue

        home_iso = team_iso.get(home_id)
        away_iso = team_iso.get(away_id)
        if home_iso is None or away_iso is None:
            continue

        # Home ISO must clear minimum AND exceed away ISO by the edge threshold
        if home_iso < iso_threshold:
            continue
        if (home_iso - away_iso) < iso_edge:
            continue

        # Enhanced: away SP is HR-prone (amplifies home power advantage)
        enhanced = _sp_is_hr_prone(away_id, sp_hr9, hr9_threshold)

        pnl = grade_ml(g["home_score"], g["away_score"], "home", g["home_ml"])
        if pnl is None:
            continue

        fires.append({
            "game_pk":   g["game_pk"],
            "game_date": g["game_date"],
            "dos":       day_of_season(g["game_date"], season_start),
            "bet_side":  "home",
            "bet_type":  "moneyline",
            "odds_used": g["home_ml"],
            "pnl":       pnl,
            "enhanced":  enhanced,
            "season":    season,
            "home_iso":  home_iso,
            "away_iso":  away_iso,
            "iso_edge":  round(home_iso - away_iso, 4),
            "pf_runs":   pf_runs,
            "version":   "home",
        })
    return fires


# ══════════════════════════════════════════════════════════════════════════════
# STATS + REPORT HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def stats(fires: list, lo: int = 1, hi: int = 9999) -> dict:
    subset = [f for f in fires if lo <= f["dos"] <= hi]
    n      = len(subset)
    valid  = [f for f in subset if f["pnl"] is not None]
    nv     = len(valid)
    wins   = sum(1 for f in valid if f["pnl"] > 0)
    losses = sum(1 for f in valid if f["pnl"] < 0)
    pushes = sum(1 for f in valid if f["pnl"] == 0)
    pnl    = sum(f["pnl"] for f in valid)
    avg_o  = sum(f["odds_used"] for f in valid if f["odds_used"]) / nv if nv > 0 else 0
    return dict(
        n=n, wins=wins, losses=losses, pushes=pushes, pnl=pnl,
        hit_rate=(wins / (wins + losses) * 100) if (wins + losses) > 0 else 0.0,
        roi=(pnl / nv * 100) if nv > 0 else 0.0,
        avg_odds=avg_o,
    )


def tbl_header() -> str:
    h = "| Window                        |    N |   W |   L |  Hit% |    P&L |   ROI% | Avg Odds |\n"
    s = "|-------------------------------|------|-----|-----|-------|--------|--------|----------|\n"
    return h + s


def tbl_row(label: str, s: dict) -> str:
    return (
        f"| {label:<31} | {s['n']:>4} | {s['wins']:>3} | {s['losses']:>3} "
        f"| {s['hit_rate']:>5.1f}% | {s['pnl']:>+6.2f} | {s['roi']:>+5.1f}% "
        f"| {s['avg_odds']:>+7.0f} |\n"
    )


def verdict(s: dict) -> str:
    n, roi = s["n"], s["roi"]
    if n < 30:
        return f"> ⚠️  Sample too small ({n} fires).\n\n"
    if roi >= 7.0:
        return f"> ✅  **Strong.** ROI {roi:+.1f}% on {n} fires.\n\n"
    if roi >= 4.5:
        return f"> ✅  **Above threshold.** ROI {roi:+.1f}% on {n} fires.\n\n"
    if roi >= 2.0:
        return f"> 🟡  **Borderline.** ROI {roi:+.1f}% on {n} fires.\n\n"
    return f"> ❌  **Below threshold.** ROI {roi:+.1f}% on {n} fires.\n\n"


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


def pf_sensitivity(fires_all: list, versions: dict, thresholds: list) -> str:
    """
    Show how each version performs across different park factor cutoffs.
    fires_all is unused — we use the versions dict {label: fires}.
    """
    lines = []
    lines.append("| PF cutoff | Version | N | ROI% |\n")
    lines.append("|-----------|---------|---|------|\n")
    for pf in thresholds:
        for label, fires in versions.items():
            subset = [f for f in fires if f.get("pf_runs", 0) > pf]
            s = stats(subset)
            lines.append(f"| > {pf:3d} | {label:<8} | {s['n']:>4} | {s['roi']:>+5.1f}% |\n")
        lines.append("|           |         |   |      |\n")  # spacer
    return "".join(lines)


def iso_edge_sensitivity(fires_home: list, thresholds: list) -> str:
    """Show B-home performance at different ISO edge thresholds."""
    lines = []
    lines.append("| ISO edge ≥ | N | Hit% | ROI% |\n")
    lines.append("|------------|---|------|------|\n")
    for edge in thresholds:
        subset = [f for f in fires_home if f.get("iso_edge", 0) >= edge]
        s = stats(subset)
        lines.append(
            f"| ≥ {edge:.3f}    | {s['n']:>4} | {s['hit_rate']:>5.1f}% | {s['roi']:>+5.1f}% |\n"
        )
    return "".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(description="Signal B lineup power backtest — 3 versions")
    p.add_argument("--db",         default=DEFAULT_DB)
    p.add_argument("--bookmaker",  default="all",
                   choices=["sbro", "oddswarehouse", "all"])
    p.add_argument("--seasons",    nargs="+", type=int)
    p.add_argument("--output",     default=None)

    # Shared thresholds
    p.add_argument("--min-iso",    type=float, default=0.160,
                   help="Minimum ISO to qualify (default 0.160 — broader than original 0.180)")
    p.add_argument("--min-pf",     type=int,   default=102,
                   help="Park factor runs > threshold (default 102 — broader than original 105)")
    p.add_argument("--hr9-boost",  type=float, default=1.40,
                   help="SP HR/9 ≥ for enhanced gate (default 1.40)")

    # B-home specific
    p.add_argument("--iso-edge",   type=float, default=0.020,
                   help="B-home: home ISO must exceed away ISO by ≥ this (default 0.020)")

    args = p.parse_args()

    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    log.info("=" * 60)
    log.info("  SIGNAL B — THREE-VERSION BACKTEST")
    log.info("  Bookmaker: %s", args.bookmaker)
    log.info("  %s", run_ts)
    log.info("=" * 60)

    if not Path(args.db).exists():
        log.error("DB not found: %s", args.db)
        sys.exit(1)

    con     = connect(args.db)
    bm_set  = get_bookmaker_set(args.bookmaker)
    venue_pf = build_venue_pf(con)

    available = [
        r[0] for r in con.execute(
            "SELECT season FROM seasons WHERE season >= 2016 ORDER BY season"
        ).fetchall()
    ]
    seasons = args.seasons or available
    seasons = [s for s in seasons if s in available and s >= 2016]

    if not seasons:
        log.error("No valid seasons (need ≥ 2016).")
        sys.exit(1)

    log.info("Seasons: %s", seasons)
    log.info("ISO threshold: %.3f  |  PF threshold: %d  |  ISO edge (B-home): %.3f",
             args.min_iso, args.min_pf, args.iso_edge)

    # Accumulators
    fires_either = []
    fires_both   = []
    fires_home   = []

    either_by_s  = defaultdict(list)
    both_by_s    = defaultdict(list)
    home_by_s    = defaultdict(list)

    for season in seasons:
        prior = season - 1
        log.info("")
        log.info("── Season %d (prior: %d) ──────────────────────────────", season, prior)

        ss = load_season_start(con, season)
        if not ss:
            log.warning("  Season %d not in seasons table.", season)
            continue

        team_iso  = build_team_iso(con, prior)
        sp_hr9    = build_sp_hr9(con, prior)
        games     = load_games_with_odds(con, season, bm_set)

        if not games:
            log.warning("  No games with odds for season %d.", season)
            continue

        kw = dict(
            games=games,
            team_iso=team_iso, sp_hr9=sp_hr9, venue_pf=venue_pf,
            season_start=ss, season=season,
            iso_threshold=args.min_iso,
            pf_threshold=args.min_pf,
            hr9_threshold=args.hr9_boost,
        )

        fe = run_b_either(**kw)
        fb = run_b_both(**kw)
        fh = run_b_home(**kw, iso_edge=args.iso_edge)

        fires_either.extend(fe);  either_by_s[season].extend(fe)
        fires_both.extend(fb);    both_by_s[season].extend(fb)
        fires_home.extend(fh);    home_by_s[season].extend(fh)

        log.info("  B-either: %d  |  B-both: %d  |  B-home: %d",
                 len(fe), len(fb), len(fh))

    con.close()

    # ── Build report ──────────────────────────────────────────────────────────
    lines = []
    lines.append("# Signal B — Lineup Power Backtest (Three Versions)\n\n")
    lines.append(f"Generated: {run_ts}  \n")
    lines.append(f"Bookmaker: `{args.bookmaker}`  \n")
    lines.append(f"Seasons: {', '.join(str(s) for s in sorted(seasons))}  \n\n")

    lines.append("## Thresholds\n\n")
    lines.append("| Parameter | Value |\n|-----------|-------|\n")
    lines.append(f"| ISO threshold (all versions) | {args.min_iso} |\n")
    lines.append(f"| Park factor runs > | {args.min_pf} |\n")
    lines.append(f"| SP HR/9 enhanced gate | {args.hr9_boost} |\n")
    lines.append(f"| B-home ISO edge (home − away ≥) | {args.iso_edge} |\n\n")

    lines.append("## Version definitions\n\n")
    lines.append(
        "| Version | Filter | Bet type | Logic |\n"
        "|---------|--------|----------|-------|\n"
        f"| B-either | Either team ISO ≥ {args.min_iso} + PF > {args.min_pf} | OVER | Original design — broadest |\n"
        f"| B-both   | Both teams ISO ≥ {args.min_iso} + PF > {args.min_pf} | OVER | Mutual power matchup |\n"
        f"| B-home   | Home ISO ≥ {args.min_iso} + home exceeds away by ≥ {args.iso_edge} + PF > {args.min_pf} | Home ML | Relative lineup power edge |\n\n"
    )
    lines.append("---\n\n")

    # ── Section 1: Three-way comparison ──────────────────────────────────────
    lines.append("## 1. Three-way comparison — all games, all seasons\n\n")

    s_either     = stats(fires_either)
    s_either_enh = stats([f for f in fires_either if f["enhanced"]])
    s_either_base= stats([f for f in fires_either if not f["enhanced"]])
    s_both       = stats(fires_both)
    s_both_enh   = stats([f for f in fires_both   if f["enhanced"]])
    s_both_base  = stats([f for f in fires_both   if not f["enhanced"]])
    s_home       = stats(fires_home)
    s_home_enh   = stats([f for f in fires_home   if f["enhanced"]])
    s_home_base  = stats([f for f in fires_home   if not f["enhanced"]])

    # Games in B-both but not B-either (shouldn't happen — both is subset of either)
    either_only = [f for f in fires_either
                   if f["game_pk"] not in {x["game_pk"] for x in fires_both}]
    s_either_only = stats(either_only)

    lines.append(tbl_header())
    lines.append(tbl_row("B-either  (any team qualifies)", s_either))
    lines.append(tbl_row("  → enhanced only",              s_either_enh))
    lines.append(tbl_row("  → base only",                  s_either_base))
    lines.append(tbl_row("  → either-only (B-both misses)", s_either_only))
    lines.append(tbl_row("B-both    (both teams qualify)",  s_both))
    lines.append(tbl_row("  → enhanced only",              s_both_enh))
    lines.append(tbl_row("  → base only",                  s_both_base))
    lines.append(tbl_row("B-home    (home ISO edge, ML)",   s_home))
    lines.append(tbl_row("  → enhanced only",              s_home_enh))
    lines.append(tbl_row("  → base only",                  s_home_base))
    lines.append("\n")

    overlap_rate = len(fires_both) / len(fires_either) * 100 if fires_either else 0
    lines.append(
        f"B-both is a subset of B-either: {len(fires_both)} of {len(fires_either)} "
        f"B-either fires ({overlap_rate:.1f}%) had both teams qualify.  \n"
        f"B-home is an independent track ({len(fires_home)} fires) — different bet type (ML vs OVER).\n\n"
    )

    lines.append("### Verdicts\n\n")
    for label, s in [("B-either", s_either), ("B-both", s_both), ("B-home", s_home)]:
        lines.append(f"**{label}:** {verdict(s)}")

    lines.append("---\n\n")

    # ── Section 2: Day-of-season breakdown ────────────────────────────────────
    lines.append("## 2. Day-of-season breakdown\n\n")
    day_windows = [
        ("Days 1–7   (Week 1)",       1,   7),
        ("Days 8–14  (Week 2)",       8,  14),
        ("Days 1–14  (Early season)", 1,  14),
        ("Days 15–30 (Rest of April)",15,  30),
        ("Days 31+   (Post-April)",   31, 999),
        ("Full season",                1, 999),
    ]

    for label, fires_set in [
        ("B-either", fires_either),
        ("B-both",   fires_both),
        ("B-home",   fires_home),
    ]:
        lines.append(f"### {label}\n\n")
        lines.append(tbl_header())
        for wlabel, lo, hi in day_windows:
            lines.append(tbl_row(wlabel, stats(fires_set, lo, hi)))
        lines.append("\n")

    lines.append("---\n\n")

    # ── Section 3: Season-by-season stability ─────────────────────────────────
    lines.append("## 3. Season-by-season stability\n\n")
    lines.append("### B-either\n\n")
    lines.append(season_tbl(either_by_s, seasons))
    lines.append("\n### B-both\n\n")
    lines.append(season_tbl(both_by_s, seasons))
    lines.append("\n### B-home\n\n")
    lines.append(season_tbl(home_by_s, seasons))
    lines.append("\n---\n\n")

    # ── Section 4: Park factor sensitivity ────────────────────────────────────
    lines.append("## 4. Park factor sensitivity\n\n")
    lines.append(
        "How does each version perform as we raise the park factor cutoff?  \n"
        "Higher cutoff = fewer games but more extreme hitter parks.\n\n"
    )
    lines.append(pf_sensitivity(
        [],
        {"B-either": fires_either, "B-both": fires_both, "B-home": fires_home},
        [100, 102, 105, 108],
    ))
    lines.append("\n---\n\n")

    # ── Section 5: ISO edge sensitivity (B-home only) ─────────────────────────
    lines.append("## 5. ISO edge sensitivity — B-home only\n\n")
    lines.append(
        "How does B-home perform as we require a larger gap "
        "between home ISO and away ISO?\n\n"
    )
    lines.append(iso_edge_sensitivity(fires_home, [0.010, 0.020, 0.030, 0.040, 0.050]))
    lines.append("\n---\n\n")

    # ── Section 6: 2025 deep-dive ─────────────────────────────────────────────
    lines.append("## 6. 2025 season deep-dive\n\n")
    for label, fires_set in [
        ("B-either", fires_either),
        ("B-both",   fires_both),
        ("B-home",   fires_home),
    ]:
        f25  = [f for f in fires_set if f["season"] == 2025]
        s_25 = stats(f25)
        s_e  = stats([f for f in f25 if f["enhanced"]])
        s_b  = stats([f for f in f25 if not f["enhanced"]])
        lines.append(f"### {label} — 2025\n\n")
        lines.append(tbl_header())
        lines.append(tbl_row("Full 2025 season", s_25))
        lines.append(tbl_row("  Enhanced only",  s_e))
        lines.append(tbl_row("  Base only",      s_b))
        for wlabel, lo, hi in day_windows[:-1]:  # skip 'Full season' row
            lines.append(tbl_row(wlabel, stats(f25, lo, hi)))
        lines.append("\n")
        lines.append(verdict(s_25))

    lines.append("---\n\n")

    # ── Section 7: Key question answered ──────────────────────────────────────
    lines.append("## 7. Does comparing lineup power add value?\n\n")

    roi_either = s_either["roi"]
    roi_both   = s_both["roi"]
    roi_home   = s_home["roi"]

    lines.append("| Comparison | ROI difference | Interpretation |\n")
    lines.append("|------------|---------------|----------------|\n")
    lines.append(
        f"| B-both vs B-either | {roi_both - roi_either:+.1f}pp | "
        f"{'Mutual power adds value' if roi_both > roi_either + 1 else 'Minimal additive value'} |\n"
    )
    lines.append(
        f"| B-home vs baseline | {roi_home:+.1f}% ROI | "
        f"{'Home ISO edge is a real ML signal' if roi_home >= 4.5 else 'No meaningful ML edge from ISO gap'} |\n"
    )
    lines.append("\n")

    if roi_both > roi_either + 2 and s_both["n"] >= 50:
        lines.append(
            "> **Conclusion:** Requiring both lineups to qualify (B-both) produces "
            f"meaningfully better OVER results (+{roi_both - roi_either:.1f}pp vs B-either). "
            "Use B-both as the primary version.\n\n"
        )
    elif roi_home >= 4.5 and s_home["n"] >= 50:
        lines.append(
            "> **Conclusion:** B-home (relative ISO edge → home ML) is the strongest version "
            f"at {roi_home:+.1f}% ROI on {s_home['n']} fires. The lineup comparison adds "
            "value on the ML side rather than totals.\n\n"
        )
    else:
        lines.append(
            "> **Conclusion:** No version clears the +4.5% threshold at current thresholds. "
            "Review Section 4 (park factor sensitivity) and Section 5 (ISO edge sensitivity) "
            "to identify if tighter gates improve results.\n\n"
        )

    lines.append("---\n\n*End of report.*\n")

    report = "".join(lines)

    # ── Console summary ───────────────────────────────────────────────────────
    log.info("")
    log.info("=" * 60)
    log.info("  SIGNAL B SUMMARY")
    log.info("=" * 60)
    for label, s in [
        ("B-either", s_either),
        ("B-both",   s_both),
        ("B-home",   s_home),
    ]:
        log.info("  %-10s %d fires | %.1f%% hit | %+.2f units | %+.1f%% ROI",
                 label, s["n"], s["hit_rate"], s["pnl"], s["roi"])

    # ── Save ──────────────────────────────────────────────────────────────────
    if args.output:
        out_path = Path(args.output)
    else:
        bm_tag   = f"_{args.bookmaker}" if args.bookmaker != "all" else ""
        out_path = REPORTS_DIR / f"backtest_signal_b{bm_tag}.md"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report, encoding="utf-8")
    log.info("  Report: %s", out_path)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
