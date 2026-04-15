#!/usr/bin/env python3
"""
backtest_overlap_AD.py — Signal A ∩ Signal D overlap analysis.

Finds games where Signal A (SP ERA edge) AND Signal D (home/away batting
split) both fire on the SAME side, then compares three tracks side-by-side:

  Track 1  Signal A alone  (all A fires, regardless of D)
  Track 2  Signal D alone  (all D fires, regardless of A)
  Track 3  A ∩ D           (games where both fire on same side — the overlap)

The core question: does the overlap produce a stronger signal than either
alone? If A∩D ROI >> A-alone and D-alone, it qualifies as a standalone
combined signal for the model.

SIGNAL DEFINITIONS (matches backtest_early_season.py exactly)
─────────────────────────────────────────────────────────────
  Signal A  — SP ERA edge
    Prior-year best SP ERA ≤ 3.50 AND opposing team OPS ≤ .710
    Bet: ML toward the strong-pitcher team (home or away)
    Enhanced: SP K/9 ≥ 9.0 AND opp K-rate ≥ 24%

  Signal D  — Home/away batting split
    Home team prior-year (home_OPS − away_OPS) ≥ .080
    Bet: home ML always (Signal D only fires for home)
    Enhanced: away team has road-better OPS split ≥ .030

OVERLAP LOGIC
─────────────
  A∩D fires when:
    - Signal A fires pointing HOME (home pitcher has ERA edge)
    - Signal D fires (home team has strong home batting split)
    Both signals agree: bet HOME ML.

  Note: Signal A can fire pointing AWAY (away pitcher ERA edge).
  Those games are tracked in a separate "A-away-only" bucket and
  excluded from the A∩D overlap (D never fires away).

OUTPUT SECTIONS
───────────────
  1. Three-way comparison table (A-alone / D-alone / A∩D)
  2. Day-of-season breakdown per track (Days 1-7 / 8-14 / 15-30 / 31+)
  3. Season-by-season stability for A∩D
  4. Odds distribution: avg home ML when both fire vs each alone
  5. Enhanced gate: A∩D with both enhancers active
  6. Directional note: A-away fires excluded from overlap

USAGE
─────
    python backtest_overlap_AD.py
    python backtest_overlap_AD.py --bookmaker oddswarehouse
    python backtest_overlap_AD.py --bookmaker all
    python backtest_overlap_AD.py --seasons 2022 2023 2024 2025
    python backtest_overlap_AD.py --min-era 3.25 --min-split 0.090
    python backtest_overlap_AD.py --output reports/overlap_AD_tight.md
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

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Bookmaker priority (matches v_closing_game_odds) ──────────────────────────
BOOKMAKER_PRIORITY = [
    "draftkings", "fanduel", "betmgm", "betonlineag", "sbro", "oddswarehouse",
]
BOOKMAKER_SET_ALL  = set(BOOKMAKER_PRIORITY)
BOOKMAKER_SET_SBRO = {"sbro"}
BOOKMAKER_SET_OW   = {"oddswarehouse"}


# ══════════════════════════════════════════════════════════════════════════════
# DB + MATH HELPERS
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
    """Profit per 1 unit staked."""
    if american_odds is None:
        return None
    if american_odds > 0:
        return american_odds / 100.0
    return 100.0 / abs(american_odds)


def grade_ml(home_score: int, away_score: int, bet_side: str, odds: int) -> float:
    """Return P&L in units for a moneyline bet. None if data missing."""
    if home_score is None or away_score is None or odds is None:
        return None
    home_won = home_score > away_score
    bet_won  = (bet_side == "home" and home_won) or \
               (bet_side == "away" and not home_won)
    return payout(odds) if bet_won else -1.0


def day_of_season(game_date: str, season_start: str) -> int:
    from datetime import datetime as dt
    gd = dt.strptime(game_date, "%Y-%m-%d").date()
    ss = dt.strptime(season_start, "%Y-%m-%d").date()
    return (gd - ss).days + 1


# ══════════════════════════════════════════════════════════════════════════════
# PRIOR-YEAR STAT BUILDERS
# (identical logic to backtest_early_season.py — kept self-contained so
#  this script can be run independently without importing the other file)
# ══════════════════════════════════════════════════════════════════════════════

def build_team_batting(con: sqlite3.Connection, season: int) -> dict:
    """
    {team_id: {ops, home_ops, away_ops, iso, k_rate}}
    Computed from player_game_stats for the given season (prior year).
    """
    log.info("    Building team batting stats (season %d) ...", season)
    rows = con.execute("""
        SELECT
            pgs.team_id,
            CASE WHEN g.home_team_id = pgs.team_id THEN 1 ELSE 0 END AS is_home,
            SUM(pgs.plate_appearances) AS pa,
            SUM(pgs.at_bats)           AS ab,
            SUM(pgs.hits)              AS h,
            SUM(pgs.home_runs)         AS hr,
            SUM(pgs.doubles)           AS dbl,
            SUM(pgs.triples)           AS trp,
            SUM(pgs.strikeouts_bat)    AS k,
            SUM(pgs.walks)             AS bb
        FROM player_game_stats pgs
        JOIN games g ON g.game_pk = pgs.game_pk
        WHERE g.season    = ?
          AND g.game_type = 'R'
          AND g.status    = 'Final'
          AND pgs.player_role = 'batter'
          AND pgs.plate_appearances > 0
        GROUP BY pgs.team_id, is_home
    """, (season,)).fetchall()

    raw = defaultdict(lambda: {k: 0 for k in [
        "home_pa","home_ab","home_h","home_hr","home_dbl","home_trp","home_k","home_bb",
        "away_pa","away_ab","away_h","away_hr","away_dbl","away_trp","away_k","away_bb",
    ]})
    for r in rows:
        p = "home_" if r["is_home"] else "away_"
        d = raw[r["team_id"]]
        for col in ["pa","ab","h","hr","dbl","trp","k","bb"]:
            d[p+col] += r[col] or 0

    def _ops(ab, h, hr, dbl, trp, bb):
        if ab <= 0:
            return None
        slg = (h + dbl + 2*trp + 3*hr) / ab
        obp = (h + bb) / (ab + bb) if (ab + bb) > 0 else 0.0
        return obp + slg

    def _iso(ab, h, hr, dbl, trp):
        if ab <= 0:
            return None
        return ((h + dbl + 2*trp + 3*hr) / ab) - (h / ab)

    result = {}
    for tid, d in raw.items():
        tab  = d["home_ab"]  + d["away_ab"]
        th   = d["home_h"]   + d["away_h"]
        thr  = d["home_hr"]  + d["away_hr"]
        tdbl = d["home_dbl"] + d["away_dbl"]
        ttrp = d["home_trp"] + d["away_trp"]
        tpa  = d["home_pa"]  + d["away_pa"]
        tk   = d["home_k"]   + d["away_k"]
        tbb  = d["home_bb"]  + d["away_bb"]

        result[tid] = {
            "ops":      _ops(tab, th, thr, tdbl, ttrp, tbb),
            "home_ops": _ops(d["home_ab"], d["home_h"], d["home_hr"],
                             d["home_dbl"], d["home_trp"], d["home_bb"]),
            "away_ops": _ops(d["away_ab"], d["away_h"], d["away_hr"],
                             d["away_dbl"], d["away_trp"], d["away_bb"]),
            "iso":      _iso(tab, th, thr, tdbl, ttrp),
            "k_rate":   (tk / tpa) if tpa > 0 else None,
            "ab":       tab,
        }
    log.info("      → %d teams.", len(result))
    return result


def build_sp_stats(con: sqlite3.Connection, season: int) -> dict:
    """
    {player_id: {team_id, era, k_per_9, hr_per_9, gs, ip_per_gs}}
    Only pitchers with gs_proxy >= 3 (innings_pitched >= 3.0 in ≥3 games).
    """
    log.info("    Building SP stats (season %d) ...", season)
    rows = con.execute("""
        SELECT
            pgs.player_id,
            pgs.team_id,
            SUM(CASE WHEN pgs.innings_pitched >= 3.0 THEN 1 ELSE 0 END) AS gs_proxy,
            SUM(pgs.innings_pitched) AS ip,
            SUM(pgs.earned_runs)     AS er,
            SUM(pgs.strikeouts_pit)  AS k,
            SUM(pgs.hr_allowed)      AS hr,
            SUM(pgs.hits_allowed)    AS h,
            SUM(pgs.walks_allowed)   AS bb
        FROM player_game_stats pgs
        JOIN games g ON g.game_pk = pgs.game_pk
        WHERE g.season    = ?
          AND g.game_type = 'R'
          AND g.status    = 'Final'
          AND pgs.player_role      = 'pitcher'
          AND pgs.innings_pitched  >= 1.0
        GROUP BY pgs.player_id, pgs.team_id
        HAVING gs_proxy >= 3
    """, (season,)).fetchall()

    result = {}
    for r in rows:
        ip  = r["ip"] or 0.0
        gs  = r["gs_proxy"] or 0
        result[r["player_id"]] = {
            "team_id":   r["team_id"],
            "era":       (r["er"] * 9 / ip) if ip > 0 else None,
            "k_per_9":   (r["k"]  * 9 / ip) if ip > 0 else None,
            "hr_per_9":  (r["hr"] * 9 / ip) if ip > 0 else None,
            "whip":      ((r["h"] + r["bb"]) / ip) if ip > 0 else None,
            "gs":        gs,
            "ip_per_gs": (ip / gs) if gs > 0 else 0.0,
        }
    log.info("      → %d pitchers.", len(result))
    return result


def build_sp_team_map(con: sqlite3.Connection, season: int) -> dict:
    """
    {team_id: [{player_id, gs, avg_ip}]}
    All pitchers who appeared in that season with gs_proxy >= 1.
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
          AND pgs.player_role     = 'pitcher'
          AND pgs.innings_pitched >= 1.0
        GROUP BY pgs.team_id, pgs.player_id
    """, (season,)).fetchall()

    result = defaultdict(list)
    for r in rows:
        result[r["team_id"]].append({
            "player_id": r["player_id"],
            "gs":        r["gs_proxy"] or 0,
            "avg_ip":    r["avg_ip"]   or 0.0,
        })
    return dict(result)


def load_games_with_odds(
    con: sqlite3.Connection, season: int, bm_set: set
) -> list:
    """Final regular-season games + best closing ML per bookmaker priority."""
    log.info("    Loading games + odds (season %d) ...", season)
    rows = con.execute("""
        SELECT
            g.game_pk, g.game_date_et AS game_date,
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
          AND go.away_ml   IS NOT NULL
        ORDER BY g.game_date_et, g.game_pk
    """, (season,)).fetchall()

    seen = {}
    for r in rows:
        bk = r["bookmaker"]
        if bk not in bm_set:
            continue
        gk = r["game_pk"]
        if gk not in seen:
            seen[gk] = dict(r)
        else:
            curr = BOOKMAKER_PRIORITY.index(seen[gk]["bookmaker"]) \
                   if seen[gk]["bookmaker"] in BOOKMAKER_PRIORITY else 999
            new  = BOOKMAKER_PRIORITY.index(bk) \
                   if bk in BOOKMAKER_PRIORITY else 999
            if new < curr:
                seen[gk] = dict(r)

    result = list(seen.values())
    log.info("      → %d games with odds.", len(result))
    return result


def load_season_start(con: sqlite3.Connection, season: int) -> str | None:
    row = con.execute(
        "SELECT season_start FROM seasons WHERE season = ?", (season,)
    ).fetchone()
    return row["season_start"] if row else None


# ══════════════════════════════════════════════════════════════════════════════
# SIGNAL EVALUATORS
# Each returns {game_pk: fire_dict} for easy overlap intersection.
# ══════════════════════════════════════════════════════════════════════════════

def eval_signal_a(
    games: list,
    sp_stats: dict,
    team_batting: dict,
    sp_team_map: dict,
    season_start: str,
    season: int,
    era_threshold: float,
    ops_threshold: float,
    k9_boost: float,
    k_rate_boost: float,
) -> dict:
    """
    Returns {game_pk: fire_dict} for all Signal A fires.
    fire_dict keys: game_pk, game_date, dos, bet_side, odds_used,
                    pnl, enhanced, season, home_ml, away_ml,
                    home_score, away_score,
                    sp_era, opp_ops  (for report detail)
    """
    fires = {}

    def best_sp_for_team(team_id):
        """Best qualifying SP (lowest ERA) for a team. Returns (era, k9) or (None, None)."""
        best_era, best_k9 = None, None
        for sp in sp_team_map.get(team_id, []):
            stat = sp_stats.get(sp["player_id"])
            if stat is None or stat["era"] is None:
                continue
            if sp["gs"] < 5 or sp["avg_ip"] < 4.5:
                continue  # thin starter excluded from Signal A
            if best_era is None or stat["era"] < best_era:
                best_era = stat["era"]
                best_k9  = stat["k_per_9"]
        return best_era, best_k9

    for g in games:
        home_id = g["home_team_id"]
        away_id = g["away_team_id"]
        dos     = day_of_season(g["game_date"], season_start)
        gk      = g["game_pk"]

        home_era, home_k9 = best_sp_for_team(home_id)
        away_era, away_k9 = best_sp_for_team(away_id)
        home_bat = team_batting.get(home_id, {})
        away_bat = team_batting.get(away_id, {})

        # Case 1: home SP edge
        if (home_era is not None and home_era <= era_threshold and
                away_bat.get("ops") is not None and
                away_bat["ops"] <= ops_threshold):

            enhanced = bool(
                home_k9 and home_k9 >= k9_boost and
                away_bat.get("k_rate") and away_bat["k_rate"] >= k_rate_boost
            )
            pnl = grade_ml(g["home_score"], g["away_score"], "home", g["home_ml"])
            # If game already has an away-SP fire, keep the stronger one (lower ERA).
            if gk not in fires or home_era < fires[gk].get("sp_era", 99):
                fires[gk] = {
                    "game_pk":    gk,
                    "game_date":  g["game_date"],
                    "dos":        dos,
                    "bet_side":   "home",
                    "odds_used":  g["home_ml"],
                    "home_ml":    g["home_ml"],
                    "away_ml":    g["away_ml"],
                    "home_score": g["home_score"],
                    "away_score": g["away_score"],
                    "pnl":        pnl,
                    "enhanced":   enhanced,
                    "season":     season,
                    "sp_era":     home_era,
                    "opp_ops":    away_bat.get("ops"),
                    "direction":  "home",
                }

        # Case 2: away SP edge (stored separately — cannot overlap with D)
        if (away_era is not None and away_era <= era_threshold and
                home_bat.get("ops") is not None and
                home_bat["ops"] <= ops_threshold):

            enhanced = bool(
                away_k9 and away_k9 >= k9_boost and
                home_bat.get("k_rate") and home_bat["k_rate"] >= k_rate_boost
            )
            pnl = grade_ml(g["home_score"], g["away_score"], "away", g["away_ml"])
            # Only add away fire if no home fire already registered for this game
            if gk not in fires:
                fires[gk] = {
                    "game_pk":    gk,
                    "game_date":  g["game_date"],
                    "dos":        dos,
                    "bet_side":   "away",
                    "odds_used":  g["away_ml"],
                    "home_ml":    g["home_ml"],
                    "away_ml":    g["away_ml"],
                    "home_score": g["home_score"],
                    "away_score": g["away_score"],
                    "pnl":        pnl,
                    "enhanced":   enhanced,
                    "season":     season,
                    "sp_era":     away_era,
                    "opp_ops":    home_bat.get("ops"),
                    "direction":  "away",
                }

    return fires


def eval_signal_d(
    games: list,
    team_batting: dict,
    season_start: str,
    season: int,
    split_threshold: float,
    away_reverse_threshold: float = 0.030,
) -> dict:
    """
    Returns {game_pk: fire_dict} for all Signal D fires.
    Always bets HOME. Only fires home-side.
    """
    fires = {}

    for g in games:
        home_id = g["home_team_id"]
        away_id = g["away_team_id"]
        dos     = day_of_season(g["game_date"], season_start)

        home_bat = team_batting.get(home_id, {})
        away_bat = team_batting.get(away_id, {})

        h_home = home_bat.get("home_ops")
        h_away = home_bat.get("away_ops")
        if h_home is None or h_away is None:
            continue

        split = h_home - h_away
        if split < split_threshold:
            continue

        # Enhanced: away team actually hits better on the road
        a_home = away_bat.get("home_ops")
        a_away = away_bat.get("away_ops")
        enhanced = (
            a_home is not None and a_away is not None and
            (a_away - a_home) >= away_reverse_threshold
        )

        pnl = grade_ml(g["home_score"], g["away_score"], "home", g["home_ml"])
        fires[g["game_pk"]] = {
            "game_pk":    g["game_pk"],
            "game_date":  g["game_date"],
            "dos":        dos,
            "bet_side":   "home",
            "odds_used":  g["home_ml"],
            "home_ml":    g["home_ml"],
            "away_ml":    g["away_ml"],
            "home_score": g["home_score"],
            "away_score": g["away_score"],
            "pnl":        pnl,
            "enhanced":   enhanced,
            "season":     season,
            "home_split": split,
        }

    return fires


# ══════════════════════════════════════════════════════════════════════════════
# STATS + REPORT HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def stats(fires: list, day_lo: int = 1, day_hi: int = 9999) -> dict:
    """Summarise a list of fire dicts for a given day-of-season window."""
    subset = [f for f in fires if day_lo <= f["dos"] <= day_hi]
    n      = len(subset)
    valid  = [f for f in subset if f["pnl"] is not None]
    wins   = sum(1 for f in valid if f["pnl"] > 0)
    losses = sum(1 for f in valid if f["pnl"] < 0)
    pushes = sum(1 for f in valid if f["pnl"] == 0)
    pnl    = sum(f["pnl"] for f in valid)
    n_v    = len(valid)
    avg_odds = (
        sum(f["odds_used"] for f in valid if f["odds_used"]) / n_v
        if n_v > 0 else 0
    )
    hit_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0.0
    roi      = (pnl / n_v * 100) if n_v > 0 else 0.0
    return dict(
        n=n, wins=wins, losses=losses, pushes=pushes,
        pnl=pnl, hit_rate=hit_rate, roi=roi, avg_odds=avg_odds
    )


def fmt_row(label: str, s: dict) -> str:
    return (
        f"| {label:<28} | {s['n']:>4} | {s['wins']:>3} | {s['losses']:>3} "
        f"| {s['hit_rate']:>5.1f}% | {s['pnl']:>+9.2f} "
        f"| {s['roi']:>+6.1f}% | {s['avg_odds']:>+7.0f} |\n"
    )


def section_header(label: str) -> str:
    return f"\n## {label}\n\n"


def table_header() -> str:
    h = "| Window                       |    N |   W |   L |  Hit% |   P&L (u) |   ROI% | Avg Odds |\n"
    s = "|------------------------------|------|-----|-----|-------|-----------|--------|----------|\n"
    return h + s


def verdict(s: dict, label: str) -> str:
    n   = s["n"]
    roi = s["roi"]
    if n < 30:
        return f"> ⚠️  Sample too small ({n} fires) — no conclusion.\n\n"
    if roi >= 7.0:
        return f"> ✅  **Strong signal.** ROI {roi:+.1f}% on {n} fires — well above +4.5% vig threshold.\n\n"
    if roi >= 4.5:
        return f"> ✅  **Above threshold.** ROI {roi:+.1f}% on {n} fires — candidate for model.\n\n"
    if roi >= 2.0:
        return f"> 🟡  **Borderline.** ROI {roi:+.1f}% on {n} fires — watch with more data.\n\n"
    return f"> ❌  **Below threshold.** ROI {roi:+.1f}% on {n} fires — do not add to model.\n\n"


def season_table(fires_by_season: dict, seasons: list) -> str:
    h = "| Season |    N |  Hit% |    P&L |   ROI% |\n"
    s = "|--------|------|-------|--------|--------|\n"
    lines = [h, s]
    for season in sorted(seasons):
        fs = fires_by_season.get(season, [])
        st = stats(fs)
        lines.append(
            f"| {season} | {st['n']:>4} | {st['hit_rate']:>5.1f}% "
            f"| {st['pnl']:>+6.2f} | {st['roi']:>+5.1f}% |\n"
        )
    return "".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(
        description="Signal A ∩ Signal D overlap analysis"
    )
    p.add_argument("--db",        default=DEFAULT_DB)
    p.add_argument("--bookmaker", default="all",
                   choices=["sbro", "oddswarehouse", "all"])
    p.add_argument("--seasons",   nargs="+", type=int)
    p.add_argument("--output",    default=None)

    # Signal A thresholds
    p.add_argument("--min-era",    type=float, default=3.50)
    p.add_argument("--max-ops",    type=float, default=0.710)
    p.add_argument("--k9-boost",   type=float, default=9.0)
    p.add_argument("--k-rate",     type=float, default=0.24)

    # Signal D thresholds
    p.add_argument("--min-split",  type=float, default=0.080)
    p.add_argument("--away-rev",   type=float, default=0.030,
                   help="Away team road-OPS advantage for D enhanced gate")

    args = p.parse_args()

    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    log.info("=" * 60)
    log.info("  SIGNAL A ∩ D OVERLAP ANALYSIS")
    log.info("  Bookmaker: %s", args.bookmaker)
    log.info("  %s", run_ts)
    log.info("=" * 60)

    if not Path(args.db).exists():
        log.error("DB not found: %s", args.db)
        sys.exit(1)

    con = connect(args.db)
    bm_set = get_bookmaker_set(args.bookmaker)

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

    # Accumulate across all seasons
    a_all  = []   # all A fires (home + away direction)
    a_home = []   # A fires pointing home only (eligible for overlap)
    a_away = []   # A fires pointing away (excluded from A∩D)
    d_all  = []   # all D fires
    ad     = []   # A∩D overlap fires (A-home ∩ D, same game_pk)

    # For season-by-season tables
    a_by_season  = defaultdict(list)
    d_by_season  = defaultdict(list)
    ad_by_season = defaultdict(list)

    for season in seasons:
        prior = season - 1
        log.info("")
        log.info("── Season %d (prior: %d) ──────────────────────────────", season, prior)

        ss = load_season_start(con, season)
        if not ss:
            log.warning("  Season %d missing from seasons table — skipping.", season)
            continue

        team_bat   = build_team_batting(con, prior)
        sp_stats   = build_sp_stats(con, prior)
        sp_map     = build_sp_team_map(con, prior)
        games      = load_games_with_odds(con, season, bm_set)

        if not games:
            log.warning("  No games with odds for season %d.", season)
            continue

        # Evaluate signals — returns {game_pk: fire_dict}
        a_fires = eval_signal_a(
            games, sp_stats, team_bat, sp_map, ss, season,
            era_threshold=args.min_era,
            ops_threshold=args.max_ops,
            k9_boost=args.k9_boost,
            k_rate_boost=args.k_rate,
        )
        d_fires = eval_signal_d(
            games, team_bat, ss, season,
            split_threshold=args.min_split,
            away_reverse_threshold=args.away_rev,
        )

        # Split A into home vs away direction
        a_h = {pk: f for pk, f in a_fires.items() if f["direction"] == "home"}
        a_v = {pk: f for pk, f in a_fires.items() if f["direction"] == "away"}

        # Overlap: game in both A-home and D, same direction (both home)
        overlap_pks = set(a_h.keys()) & set(d_fires.keys())

        log.info("  A fires: %d total (%d home, %d away)  D fires: %d  Overlap: %d",
                 len(a_fires), len(a_h), len(a_v), len(d_fires), len(overlap_pks))

        # Accumulate lists
        for f in a_fires.values():
            a_all.append(f)
            a_by_season[season].append(f)
        for f in a_h.values():
            a_home.append(f)
        for f in a_v.values():
            a_away.append(f)
        for f in d_fires.values():
            d_all.append(f)
            d_by_season[season].append(f)

        for pk in overlap_pks:
            # Use Signal A's fire dict as base (it has sp_era, opp_ops)
            # Attach Signal D's split info
            base = dict(a_h[pk])
            base["home_split"] = d_fires[pk].get("home_split")
            base["d_enhanced"] = d_fires[pk].get("enhanced", False)
            # Both-enhanced gate
            base["both_enhanced"] = base["enhanced"] and base["d_enhanced"]
            ad.append(base)
            ad_by_season[season].append(base)

    con.close()

    # ── Build report ──────────────────────────────────────────────────────────
    lines = []
    lines.append("# Signal A ∩ Signal D — Overlap Analysis\n\n")
    lines.append(f"Generated: {run_ts}  \n")
    lines.append(f"Bookmaker: `{args.bookmaker}`  \n")
    lines.append(f"Seasons: {', '.join(str(s) for s in sorted(seasons))}  \n\n")

    lines.append("## Thresholds\n\n")
    lines.append("| Parameter | Value |\n|-----------|-------|\n")
    lines.append(f"| Signal A: SP ERA ≤ | {args.min_era} |\n")
    lines.append(f"| Signal A: Opp OPS ≤ | {args.max_ops} |\n")
    lines.append(f"| Signal A: K/9 enhanced gate | {args.k9_boost} |\n")
    lines.append(f"| Signal D: Home OPS split ≥ | {args.min_split} |\n")
    lines.append(f"| Signal D: Away-road enhanced gate | {args.away_rev} |\n\n")
    lines.append("---\n")

    # ── Section 1: Three-way comparison (all games, all seasons) ─────────────
    lines.append(section_header("1. Three-way comparison — all games, all seasons"))
    lines.append(
        "Tracks compared on **home-side bets only** for apples-to-apples comparison.  \n"
        "Signal A-away fires are tracked separately in Section 6.\n\n"
    )

    s_a_home_all  = stats(a_home)
    s_d_all       = stats(d_all)
    s_ad_all      = stats(ad)
    s_ad_enh      = stats([f for f in ad if f.get("both_enhanced")])
    s_a_noD       = stats([f for f in a_home if f["game_pk"] not in {x["game_pk"] for x in ad}])
    s_d_noA       = stats([f for f in d_all  if f["game_pk"] not in {x["game_pk"] for x in ad}])

    lines.append(table_header())
    lines.append(fmt_row("Signal A — home fires only", s_a_home_all))
    lines.append(fmt_row("Signal D — all fires (home only)", s_d_all))
    lines.append(fmt_row("A ∩ D — overlap (both agree home)", s_ad_all))
    lines.append(fmt_row("A ∩ D — both enhancers active", s_ad_enh))
    lines.append(fmt_row("A only (no D)", s_a_noD))
    lines.append(fmt_row("D only (no A)", s_d_noA))
    lines.append("\n")

    # Overlap rate
    if s_a_home_all["n"] > 0:
        overlap_rate_a = s_ad_all["n"] / s_a_home_all["n"] * 100
        lines.append(f"**Overlap rate:** {s_ad_all['n']} of {s_a_home_all['n']} A-home fires "
                     f"also had D active ({overlap_rate_a:.1f}%).  \n")
    if s_d_all["n"] > 0:
        overlap_rate_d = s_ad_all["n"] / s_d_all["n"] * 100
        lines.append(f"{s_ad_all['n']} of {s_d_all['n']} D fires also had A active "
                     f"({overlap_rate_d:.1f}%).\n\n")

    lines.append(verdict(s_ad_all, "A∩D"))

    # ── Section 2: Day-of-season breakdown ────────────────────────────────────
    lines.append(section_header("2. Day-of-season breakdown"))
    lines.append(
        "Key question: is the overlap stronger in the first 14 days "
        "(early-season design goal) or consistent all season?\n\n"
    )

    day_windows = [
        ("Days 1–7   (Week 1)",          1,   7),
        ("Days 8–14  (Week 2)",          8,  14),
        ("Days 1–14  (Early season)",    1,  14),
        ("Days 15–30 (Rest of April)",  15,  30),
        ("Days 31+   (Post-April)",     31, 999),
        ("Full season",                  1, 999),
    ]

    lines.append("### Signal A (home fires)\n\n")
    lines.append(table_header())
    for label, lo, hi in day_windows:
        lines.append(fmt_row(label, stats(a_home, lo, hi)))
    lines.append("\n")

    lines.append("### Signal D\n\n")
    lines.append(table_header())
    for label, lo, hi in day_windows:
        lines.append(fmt_row(label, stats(d_all, lo, hi)))
    lines.append("\n")

    lines.append("### A ∩ D overlap\n\n")
    lines.append(table_header())
    for label, lo, hi in day_windows:
        lines.append(fmt_row(label, stats(ad, lo, hi)))
    lines.append("\n")

    # ── Section 3: Season-by-season stability ─────────────────────────────────
    lines.append(section_header("3. Season-by-season stability — A ∩ D"))
    lines.append(
        "A single anomalous year should not drive the result. "
        "Look for consistent positive ROI across ≥3 seasons.\n\n"
    )
    lines.append(season_table(ad_by_season, seasons))
    lines.append("\n")

    # Compare to A and D standalone per season
    lines.append("### Signal A home fires — season-by-season\n\n")
    lines.append(season_table(a_by_season, seasons))
    lines.append("\n")

    lines.append("### Signal D — season-by-season\n\n")
    lines.append(season_table(d_by_season, seasons))
    lines.append("\n")

    # ── Section 4: Odds profile ────────────────────────────────────────────────
    lines.append(section_header("4. Odds profile — home ML distribution"))
    lines.append(
        "When both signals agree on the home team, are we getting "
        "better or worse odds than either signal alone?\n\n"
    )

    def odds_buckets(fires: list) -> str:
        buckets = {
            "Heavy fav (≤ −200)":         0,
            "Fav (−150 to −199)":         0,
            "Soft fav (−110 to −149)":    0,
            "Pick'em (−109 to +109)":     0,
            "Dog (+110 to +149)":         0,
            "Heavy dog (≥ +150)":         0,
        }
        for f in fires:
            o = f.get("home_ml") or f.get("odds_used")
            if o is None:
                continue
            if o <= -200:
                buckets["Heavy fav (≤ −200)"] += 1
            elif o <= -150:
                buckets["Fav (−150 to −199)"] += 1
            elif o <= -110:
                buckets["Soft fav (−110 to −149)"] += 1
            elif o <= 109:
                buckets["Pick'em (−109 to +109)"] += 1
            elif o <= 149:
                buckets["Dog (+110 to +149)"] += 1
            else:
                buckets["Heavy dog (≥ +150)"] += 1
        total = sum(buckets.values()) or 1
        rows  = ["| Odds bucket | N | % |\n|-------------|---|---|\n"]
        for label, cnt in buckets.items():
            rows.append(f"| {label} | {cnt} | {cnt/total*100:.0f}% |\n")
        return "".join(rows)

    lines.append("**A home fires:**\n\n")
    lines.append(odds_buckets(a_home))
    lines.append("\n**D fires:**\n\n")
    lines.append(odds_buckets(d_all))
    lines.append("\n**A ∩ D overlap:**\n\n")
    lines.append(odds_buckets(ad))
    lines.append("\n")

    # ── Section 5: Enhanced gate analysis ─────────────────────────────────────
    lines.append(section_header("5. Enhanced gate — both enhancers active"))
    lines.append(
        "A-enhanced = SP K/9 ≥ 9.0 AND opp K-rate ≥ 24%.  \n"
        "D-enhanced = away team hits better on road (road OPS − home OPS ≥ .030).\n\n"
    )

    a_enh_only = [f for f in ad if f.get("enhanced") and not f.get("d_enhanced")]
    d_enh_only = [f for f in ad if not f.get("enhanced") and f.get("d_enhanced")]
    both_enh   = [f for f in ad if f.get("both_enhanced")]
    neither    = [f for f in ad if not f.get("enhanced") and not f.get("d_enhanced")]

    lines.append(table_header())
    lines.append(fmt_row("Neither enhanced",        stats(neither)))
    lines.append(fmt_row("A enhanced only",         stats(a_enh_only)))
    lines.append(fmt_row("D enhanced only",         stats(d_enh_only)))
    lines.append(fmt_row("Both enhanced (A ∩ D ++)", stats(both_enh)))
    lines.append("\n")

    if len(both_enh) >= 10:
        lines.append(verdict(stats(both_enh), "A∩D both-enhanced"))
    else:
        lines.append(f"> ⚠️  Both-enhanced sample only {len(both_enh)} fires — too small to grade.\n\n")

    # ── Section 6: A-away fires (excluded from overlap) ───────────────────────
    lines.append(section_header("6. Signal A — away fires (excluded from overlap)"))
    lines.append(
        "Signal A fires pointing **away** cannot overlap with Signal D "
        "(D always bets home). Tracked here for completeness.\n\n"
    )
    s_a_away = stats(a_away)
    lines.append(table_header())
    lines.append(fmt_row("Signal A — away fires", s_a_away))
    lines.append("\n")
    lines.append(verdict(s_a_away, "A-away"))

    # ── Section 7: Interpretation guide ───────────────────────────────────────
    lines.append(section_header("7. Interpretation guide"))
    lines.append("""
**How to read this report:**

| Scenario | Conclusion |
|----------|------------|
| A∩D ROI >> A-alone AND D-alone | Overlap is a genuine combined signal — add as A∩D to model |
| A∩D ROI ≈ A-alone or D-alone | Signals are not additive — use whichever standalone is stronger |
| A∩D ROI < either standalone | Overlap may select unfavourable sub-population — investigate odds |
| A∩D Days 1-14 >> Days 31+ | Genuine early-season signal — apply seasonal gate |
| A∩D consistent all season | Full-season reinforcer — no date gate needed |
| Both-enhanced >> base overlap | Use enhancers as required gates, not optional boosters |

**Pass criteria for model inclusion:**
- ROI ≥ +4.5% on N ≥ 50 fires (SBRO)
- ROI ≥ +4.5% OddsWarehouse (OOS validation)
- No single season driving > 60% of total P&L
- Day-of-season pattern consistent with intended use (early or full-season)
""")

    lines.append("\n---\n\n*End of report.*\n")

    report = "".join(lines)

    # ── Save ──────────────────────────────────────────────────────────────────
    if args.output:
        out_path = Path(args.output)
    else:
        bm_tag   = f"_{args.bookmaker}" if args.bookmaker != "all" else ""
        out_path = REPORTS_DIR / f"backtest_overlap_AD{bm_tag}.md"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report, encoding="utf-8")

    # Console summary
    log.info("")
    log.info("=" * 60)
    log.info("  OVERLAP SUMMARY")
    log.info("=" * 60)
    log.info("  A home fires : %d | %.1f%% | %+.2f units | %+.1f%% ROI",
             s_a_home_all["n"], s_a_home_all["hit_rate"],
             s_a_home_all["pnl"], s_a_home_all["roi"])
    log.info("  D fires      : %d | %.1f%% | %+.2f units | %+.1f%% ROI",
             s_d_all["n"], s_d_all["hit_rate"],
             s_d_all["pnl"], s_d_all["roi"])
    log.info("  A ∩ D        : %d | %.1f%% | %+.2f units | %+.1f%% ROI",
             s_ad_all["n"], s_ad_all["hit_rate"],
             s_ad_all["pnl"], s_ad_all["roi"])
    log.info("  Report: %s", out_path)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
