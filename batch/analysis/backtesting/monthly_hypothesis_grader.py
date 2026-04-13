"""
monthly_hypothesis_grader.py
=============================
MLB Scout · Game-by-Game Hypothesis Grader

For every game in a chosen month, evaluates every testable hypothesis
from the findings matrix, records a PREDICTION and RESULT for each,
then scores each hypothesis at the end.

USAGE
-----
    python monthly_hypothesis_grader.py --month 2025-09
    python monthly_hypothesis_grader.py --month 2024-07 --output reports\\july2024.txt
    python monthly_hypothesis_grader.py --month 2025-09 --hyp H3b MV-B S1
    python monthly_hypothesis_grader.py --month 2025-09 --no-detail
    python monthly_hypothesis_grader.py --list-months

WHAT IT TESTS (14 hypotheses)
------------------------------
  H1a   Home Underdog Overlay    (home dog ML — positive ROI expected)
  H1b   Favourite ROI            (all home favs — near-vig loss expected)
  H2    Home Field Overpricing   (market implied vs actual home win%)
  H3a   Cold Weather Over        (temp <45°F games — OVER expected)
  H3b   Wind-Out Over            (>=10 mph OUT, HIGH venue — OVER expected)
  H4    SP ERA vs Market         (ERA advantage team — conditional on stats loaded)
  H5    Close Game Over Bias     (near pick-em ML — slight over tendency)
  H6    COVID Outlier            (2020 season home win rate anomaly)
  NF1   Road Fav Overpricing     (road favs — negative ROI expected)
  NF2   2018 Home Collapse       (season-specific home win rate check)
  NF3   Wind-In Under            (>=10 mph IN, HIGH venue — UNDER expected)
  MV-B  Multivariate Wind-Out    (wind OUT + home dog 35-45% → OVER)
  MV-F  Multivariate Wind-In     (wind IN + home fav -130/-160 → fade ML)
  S1    Win Streak Fade          (W5+ home team → bet away ML)

OUTPUT FORMAT (per game)
-------------------------
  DATE  HOME(h)  vs  AWAY  |  SCORE  |  [H] PRED → RESULT ✓/✗
  ...repeated for every hypothesis that fires on that game...

SCORING (per hypothesis, end of report)
-----------------------------------------
  N games applicable  |  wins  |  losses  |  hit%  |  ROI  |  benchmark  |  GRADE
"""

import argparse
import datetime
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent / "mlb_stats.db"

# ── Import signal helpers from the brief generator ────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))
try:
    import generate_daily_brief as gdb
except ImportError:
    print("✗  generate_daily_brief.py not found. Run from the mlb_stats folder.")
    sys.exit(1)

# ── Findings matrix benchmarks ────────────────────────────────────────────────
BENCHMARKS = {
    "H1a":  {"desc": "Home Underdog Overlay",        "direction": "home_dog_ml",    "sbro": 0.0270,  "ow": 0.0038,  "threshold": 0.045,  "note": "+2.70% SBRO / +0.38% OW — sub-threshold OOS"},
    "H1b":  {"desc": "Favourite ROI (home favs)",    "direction": "home_fav_ml",    "sbro": -0.0376, "ow": None,    "threshold": -0.045, "note": "-3.76% SBRO home fav ROI"},
    "H2":   {"desc": "Home Field Overpricing",       "direction": "home_ml_all",    "sbro": -0.0118, "ow": -0.027,  "threshold": None,   "note": "Actual home win% < implied by 1-2.7pp"},
    "H3a":  {"desc": "Cold Weather Over (<45°F)",    "direction": "total_over",     "sbro": 0.516,   "ow": None,    "threshold": 0.50,   "note": "51.6% SBRO over rate — market sets lines TOO LOW in cold"},
    "H3b":  {"desc": "Wind-Out Over (>=10 mph OUT)", "direction": "total_over",     "sbro": 0.522,   "ow": None,    "threshold": 0.50,   "note": "52.2% SBRO over rate (z=2.99, p=0.003)"},
    "H4":   {"desc": "SP ERA Advantage vs Market",   "direction": "era_edge_ml",    "sbro": -0.0856, "ow": -0.0016, "threshold": None,   "note": "Contrarian — betting ERA-advantaged team LOSES money"},
    "H5":   {"desc": "Close Game Over Bias",         "direction": "total_over",     "sbro": 0.510,   "ow": None,    "threshold": 0.50,   "note": "51.0% over rate in pick-em games — weak signal"},
    "H6":   {"desc": "2020 COVID Home Anomaly",      "direction": "home_win_rate",  "sbro": 0.586,   "ow": None,    "threshold": None,   "note": "2020: 58.6% home wins vs 53.3% average"},
    "NF1":  {"desc": "Road Fav Overpricing",         "direction": "road_fav_ml",    "sbro": -0.0581, "ow": None,    "threshold": -0.045, "note": "-5.81% SBRO — worst category in dataset"},
    "NF2":  {"desc": "2018 Home Team Collapse",      "direction": "home_win_rate",  "sbro": 0.502,   "ow": None,    "threshold": None,   "note": "2018: 50.2% home wins vs 55.0% implied"},
    "NF3":  {"desc": "Wind-In Under (>=10 mph IN)",  "direction": "total_under",    "sbro": 0.485,   "ow": 0.485,   "threshold": 0.50,   "note": "48.5% over rate (under wins 51.5%)"},
    "MV-B": {"desc": "Wind-Out + Home Dog → OVER",   "direction": "total_over",     "sbro": 0.587,   "ow": 0.623,   "threshold": 0.50,   "note": "58.7% SBRO / 62.3% OW over rate"},
    "MV-F": {"desc": "Wind-In + Home Fav → Fade ML", "direction": "away_ml_fade",   "sbro": 0.1131,  "ow": 0.0847,  "threshold": 0.045,  "note": "-11.31% SBRO / -8.47% OW home ML ROI (fade side wins)"},
    "S1":   {"desc": "W5+ Home Streak → Bet Away",   "direction": "away_ml_fade",   "sbro": 0.0750,  "ow": 0.0899,  "threshold": 0.045,  "note": "+7.50% SBRO / +8.99% OW away ROI"},
}

HYPOTHESES_ORDERED = ["H1a","H1b","H2","H3a","H3b","H4","H5","H6",
                       "NF1","NF2","NF3","MV-B","MV-F","S1"]

# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def open_db():
    if not DB_PATH.exists():
        print(f"✗  Database not found: {DB_PATH}")
        sys.exit(1)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def implied_prob(ml):
    if ml is None: return None
    return abs(ml) / (abs(ml) + 100) if ml < 0 else 100 / (ml + 100)


def pnl(ml, won):
    """P&L units for flat $1 bet."""
    if ml is None: return 0.0
    return (100 / abs(ml) if ml < 0 else ml / 100) if won else -1.0


def fmt_ml(ml):
    if ml is None: return "N/A"
    return f"+{ml}" if ml > 0 else str(ml)


def fmt_pct(v):
    if v is None: return "N/A"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v*100:.1f}%"


def fmt_roi(v):
    if v is None: return "N/A"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v*100:.2f}%"


# ─────────────────────────────────────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────────────────────────────────────

def load_month_games(conn, year, month):
    """Load all Final regular-season games in the month with odds + venue."""
    start = f"{year}-{month:02d}-01"
    # Last day of month
    import calendar
    last = calendar.monthrange(year, month)[1]
    end   = f"{year}-{month:02d}-{last:02d}"

    rows = conn.execute("""
        SELECT
            g.game_pk, g.game_date, g.season,
            g.home_score, g.away_score,
            g.temp_f, g.wind_mph, g.wind_direction, g.sky_condition,
            g.innings_played, g.extra_innings,
            th.team_id   AS home_team_id,
            th.name      AS home_name,
            th.abbreviation AS home_abbr,
            ta.team_id   AS away_team_id,
            ta.name      AS away_name,
            ta.abbreviation AS away_abbr,
            v.name       AS venue_name,
            v.wind_effect,
            ml.home_ml, ml.away_ml,
            tot.total_line, tot.over_odds, tot.under_odds
        FROM   games g
        JOIN   teams th  ON th.team_id  = g.home_team_id
        JOIN   teams ta  ON ta.team_id  = g.away_team_id
        LEFT JOIN venues v ON v.venue_id = g.venue_id
        LEFT JOIN game_odds ml  ON ml.game_pk   = g.game_pk
                                AND ml.market_type = 'moneyline'
                                AND ml.is_closing_line = 1
        LEFT JOIN game_odds tot ON tot.game_pk  = g.game_pk
                                AND tot.market_type = 'total'
                                AND tot.is_closing_line = 1
        WHERE  g.game_type  = 'R'
          AND  g.status     = 'Final'
          AND  g.game_date BETWEEN ? AND ?
          AND  g.home_score IS NOT NULL
        ORDER  BY g.game_date, g.game_start_utc
    """, (start, end)).fetchall()
    return [dict(r) for r in rows]


def compute_streaks_for_month(conn, year, month, team_ids):
    """
    For each game, compute the home team's win/loss streak entering that game.
    Returns dict: {game_pk: home_streak_int}
    """
    import calendar
    last  = calendar.monthrange(year, month)[1]
    start = f"{year}-{month:02d}-01"
    end   = f"{year}-{month:02d}-{last:02d}"

    # Get all games in the month to know dates
    month_games = conn.execute(
        "SELECT game_pk, game_date, home_team_id FROM games "
        "WHERE game_type='R' AND status='Final' "
        "AND game_date BETWEEN ? AND ? ORDER BY game_date",
        (start, end)
    ).fetchall()

    streaks = {}
    for mg in month_games:
        home_id   = mg["home_team_id"]
        game_date = mg["game_date"]
        prior = conn.execute("""
            SELECT CASE WHEN home_team_id=? THEN
                       CASE WHEN home_score>away_score THEN 'W' ELSE 'L' END
                   ELSE CASE WHEN away_score>home_score THEN 'W' ELSE 'L' END
                   END result
            FROM games
            WHERE (home_team_id=? OR away_team_id=?)
              AND status='Final' AND game_type='R'
              AND season=?
              AND game_date < ?
            ORDER BY game_date DESC, game_pk DESC
            LIMIT 12
        """, (home_id, home_id, home_id, year, game_date)).fetchall()

        if not prior:
            streaks[mg["game_pk"]] = 0
            continue
        cur = prior[0][0]
        count = 0
        for p in prior:
            if p[0] == cur: count += 1
            else: break
        streaks[mg["game_pk"]] = count if cur == 'W' else -count

    return streaks


def load_sp_era_data(conn, year, month):
    """
    For each game, find the starting pitcher for each team and their
    season-to-date ERA entering that game (no look-ahead).
    Returns dict: {game_pk: {home_era, away_era}} or None if stats unavailable.
    """
    import calendar
    last  = calendar.monthrange(year, month)[1]
    start = f"{year}-{month:02d}-01"
    end   = f"{year}-{month:02d}-{last:02d}"

    # Check if pitcher stats exist
    count = conn.execute(
        "SELECT COUNT(*) FROM player_game_stats pgs "
        "JOIN games g ON g.game_pk = pgs.game_pk "
        "WHERE pgs.player_role='pitcher' AND g.season=? LIMIT 1",
        (year,)
    ).fetchone()[0]
    if not count:
        return None

    # Get all pitcher rows for the season up to end of month
    pitcher_rows = conn.execute("""
        SELECT
            pgs.game_pk, pgs.player_id, pgs.team_id,
            pgs.innings_pitched, pgs.earned_runs,
            g.game_date, g.home_team_id, g.away_team_id
        FROM   player_game_stats pgs
        JOIN   games g ON g.game_pk = pgs.game_pk
        WHERE  pgs.player_role = 'pitcher'
          AND  g.game_type = 'R'
          AND  g.status = 'Final'
          AND  g.season = ?
          AND  g.game_date <= ?
        ORDER  BY g.game_date, pgs.game_pk
    """, (year, end)).fetchall()

    # Build season-to-date ERA for each pitcher entering each game
    # Accumulate IP and ER per pitcher per date
    from collections import defaultdict
    pitcher_cumulative = defaultdict(lambda: {"ip": 0.0, "er": 0})
    game_sp_era = {}

    # For each game in month, identify likely starter (most IP in that game)
    # and compute their ERA entering that game
    month_game_pks = set(
        r[0] for r in conn.execute(
            "SELECT game_pk FROM games WHERE game_type='R' AND status='Final' "
            "AND game_date BETWEEN ? AND ?", (start, end)
        ).fetchall()
    )

    # Group pitcher rows by game
    from collections import defaultdict
    by_game = defaultdict(list)
    all_pitcher_stats = []
    for r in pitcher_rows:
        by_game[r["game_pk"]].append(dict(r))
        all_pitcher_stats.append(dict(r))

    # Process chronologically
    processed_games = set()
    era_by_game = {}
    # running totals: {player_id: {ip, er}}
    running = defaultdict(lambda: {"ip": 0.0, "er": 0})

    # Sort all games chronologically
    all_game_dates = conn.execute(
        "SELECT DISTINCT game_pk, game_date FROM games "
        "WHERE game_type='R' AND status='Final' AND season=? "
        "AND game_date <= ? ORDER BY game_date, game_pk",
        (year, end)
    ).fetchall()

    for gd in all_game_dates:
        gpk  = gd["game_pk"]
        rows_for_game = by_game.get(gpk, [])
        if not rows_for_game:
            continue

        home_tid = rows_for_game[0]["home_team_id"]
        away_tid = rows_for_game[0]["away_team_id"]

        # Find likely starters (max IP per team in this game)
        home_pitchers = [r for r in rows_for_game if r["team_id"] == home_tid]
        away_pitchers = [r for r in rows_for_game if r["team_id"] == away_tid]

        home_sp = max(home_pitchers, key=lambda x: x["innings_pitched"] or 0) if home_pitchers else None
        away_sp = max(away_pitchers, key=lambda x: x["innings_pitched"] or 0) if away_pitchers else None

        def era_entering(sp):
            if not sp: return None
            pid = sp["player_id"]
            prior_ip = running[pid]["ip"]
            prior_er = running[pid]["er"]
            if prior_ip < 1.0: return None  # too few innings — unreliable
            return (prior_er / prior_ip) * 9

        if gpk in month_game_pks:
            era_by_game[gpk] = {
                "home_era": era_entering(home_sp),
                "away_era": era_entering(away_sp),
                "home_sp": home_sp["player_id"] if home_sp else None,
                "away_sp": away_sp["player_id"] if away_sp else None,
            }

        # Now update running totals with this game's results
        for r in rows_for_game:
            running[r["player_id"]]["ip"] += r["innings_pitched"] or 0
            running[r["player_id"]]["er"] += r["earned_runs"] or 0

    return era_by_game


# ─────────────────────────────────────────────────────────────────────────────
# Hypothesis evaluators — one per hypothesis
# Each returns: (applicable: bool, prediction: str, won: bool|None, detail: str)
# won=None means push or not applicable
# ─────────────────────────────────────────────────────────────────────────────

def eval_h1a(g):
    """Home Underdog Overlay — grade every game where home team is a dog."""
    if g["home_ml"] is None or g["away_ml"] is None:
        return False, "", None, "no odds"
    if g["home_ml"] <= 0:  # home team is favourite
        return False, "", None, "home is fav"
    home_impl = implied_prob(g["home_ml"])
    runs_home_won = g["home_score"] > g["away_score"]
    won = runs_home_won
    bet_ml = g["home_ml"]
    return (True, f"BET home {fmt_ml(bet_ml)} ({home_impl:.0%} impl)",
            won, f"{'WIN' if won else 'LOSS'} {g['away_abbr']} {g['away_score']} @ {g['home_abbr']} {g['home_score']}")


def eval_h1b(g):
    """Home Fav ROI — grade every home favourite."""
    if g["home_ml"] is None:
        return False, "", None, "no odds"
    if g["home_ml"] >= 0:
        return False, "", None, "home is dog"
    home_won = g["home_score"] > g["away_score"]
    return (True, f"BET home fav {fmt_ml(g['home_ml'])}",
            home_won, f"{'WIN' if home_won else 'LOSS'}")


def eval_h2(g):
    """Home Field Overpricing — every game, compare implied to actual."""
    if g["home_ml"] is None:
        return False, "", None, "no odds"
    home_impl = implied_prob(g["home_ml"])
    home_won  = g["home_score"] > g["away_score"]
    delta = (1 if home_won else 0) - home_impl
    return (True,
            f"Home implied {home_impl:.1%}",
            home_won,
            f"{'WON' if home_won else 'LOST'} (impl delta {delta:+.1%})")


def eval_h3a(g):
    """Cold Weather Over — <45°F games."""
    if g["temp_f"] is None:
        return False, "", None, "no temp data"
    if g["temp_f"] >= 45:
        return False, "", None, f"temp {g['temp_f']}°F ≥45"
    if g["total_line"] is None:
        return False, "", None, "no total"
    if g["home_score"] is None or g["away_score"] is None:
        return False, "", None, "no score"
    runs = g["home_score"] + g["away_score"]
    over = runs > g["total_line"]
    push = runs == g["total_line"]
    if push:
        return True, f"BET OVER {g['total_line']} ({g['temp_f']}°F)", None, f"PUSH ({runs} runs)"
    return (True, f"BET OVER {g['total_line']} ({g['temp_f']}°F)",
            over, f"{'OVER' if over else 'UNDER'} — {runs} runs vs {g['total_line']}")


def eval_h3b(g):
    """Wind-Out Over — >=10mph OUT, HIGH venue."""
    if g.get("wind_effect") != "HIGH":
        return False, "", None, f"venue not HIGH ({g.get('wind_effect','?')})"
    if g["wind_mph"] is None or g["wind_mph"] < gdb.WIND_OUT_MIN_MPH:
        return False, "", None, f"wind {g['wind_mph']} mph < 10"
    wind_dir = gdb.wind_direction_label(g.get("wind_direction") or "")
    if wind_dir != "OUT":
        return False, "", None, f"wind direction {wind_dir}"
    if g["total_line"] is None:
        return False, "", None, "no total"
    runs = (g["home_score"] or 0) + (g["away_score"] or 0)
    over = runs > g["total_line"]
    push = runs == g["total_line"]
    if push:
        return True, f"BET OVER {g['total_line']} ({g['wind_mph']}mph OUT)", None, "PUSH"
    return (True,
            f"BET OVER {g['total_line']} ({g['wind_mph']}mph OUT @ {g.get('venue_name','?')})",
            over, f"{'OVER ✓' if over else 'UNDER ✗'} — {runs} runs")


def eval_h4(g, era_data):
    """SP ERA vs Market — ERA-advantaged team is contrarian (bet AGAINST them)."""
    if era_data is None:
        return False, "", None, "pitcher stats not loaded"
    gpk = g["game_pk"]
    if gpk not in era_data:
        return False, "", None, "no ERA data for this game"
    home_era = era_data[gpk]["home_era"]
    away_era = era_data[gpk]["away_era"]
    if home_era is None or away_era is None:
        return False, "", None, f"insufficient ERA data (H:{home_era} A:{away_era})"
    era_diff = home_era - away_era   # positive = home SP worse
    if abs(era_diff) < 1.0:
        return False, "", None, f"ERA diff {era_diff:+.2f} < 1.0 threshold"
    if g["home_ml"] is None:
        return False, "", None, "no odds"
    # ERA advantage goes to the team with LOWER ERA
    # H4 finding: betting that team LOSES money — so bet AGAINST them (contrarian)
    home_has_era_edge = home_era < away_era   # lower ERA = better pitcher
    if home_has_era_edge:
        # ERA says home pitcher is better → market over-prices home → bet AWAY (contrarian)
        pred = f"CONTRARIAN BET AWAY {fmt_ml(g['away_ml'])} (home ERA {home_era:.2f} < away {away_era:.2f})"
        won = g["away_score"] > g["home_score"]
    else:
        # ERA says away pitcher is better → bet HOME (contrarian fade of ERA-advantaged away)
        pred = f"CONTRARIAN BET HOME {fmt_ml(g['home_ml'])} (away ERA {away_era:.2f} < home {home_era:.2f})"
        won = g["home_score"] > g["away_score"]
    return (True, pred, won,
            f"{'WIN ✓' if won else 'LOSS ✗'} ERA diff {era_diff:+.2f}")


def eval_h5(g):
    """Close Game Over Bias — near pick-em games."""
    if g["home_ml"] is None or g["away_ml"] is None:
        return False, "", None, "no odds"
    home_impl = implied_prob(g["home_ml"])
    away_impl = implied_prob(g["away_ml"])
    if abs(home_impl - away_impl) > 0.10:
        return False, "", None, f"not close ({home_impl:.0%}/{away_impl:.0%})"
    if g["total_line"] is None:
        return False, "", None, "no total"
    runs = (g["home_score"] or 0) + (g["away_score"] or 0)
    over = runs > g["total_line"]
    push = runs == g["total_line"]
    if push:
        return True, f"BET OVER {g['total_line']} (close game)", None, "PUSH"
    return (True, f"BET OVER {g['total_line']} (close: {home_impl:.0%}/{away_impl:.0%})",
            over, f"{'OVER ✓' if over else 'UNDER ✗'} — {runs} runs")


def eval_h6(g):
    """2020 COVID Outlier — track home win rate vs implied for season."""
    # Every game contributes to seasonal home win rate tracking
    home_won = g["home_score"] > g["away_score"]
    return (True, f"Track home win rate (season {g['season']})", home_won,
            f"Home {'WON' if home_won else 'LOST'}")


def eval_nf1(g):
    """Road Fav Overpricing — bet on every road favourite."""
    if g["away_ml"] is None:
        return False, "", None, "no odds"
    if g["away_ml"] >= 0:
        return False, "", None, "away is dog"
    away_won = g["away_score"] > g["home_score"]
    return (True, f"BET road fav {fmt_ml(g['away_ml'])}",
            away_won, f"{'WIN ✓' if away_won else 'LOSS ✗'}")


def eval_nf2(g):
    """2018 Home Collapse — track home win rate (most meaningful for 2018 season)."""
    home_won = g["home_score"] > g["away_score"]
    home_impl = implied_prob(g["home_ml"]) if g["home_ml"] else None
    impl_str = f"implied {home_impl:.1%}" if home_impl else "no odds"
    return (True, f"Track home win ({impl_str})", home_won,
            f"Home {'WON' if home_won else 'LOST'} in {g['season']}")


def eval_nf3(g):
    """Wind-In Under — >=10mph IN, HIGH venue."""
    if g.get("wind_effect") != "HIGH":
        return False, "", None, f"venue not HIGH ({g.get('wind_effect','?')})"
    if g["wind_mph"] is None or g["wind_mph"] < gdb.WIND_IN_MIN_MPH:
        return False, "", None, f"wind {g['wind_mph']} mph < 10"
    wind_dir = gdb.wind_direction_label(g.get("wind_direction") or "")
    if wind_dir != "IN":
        return False, "", None, f"wind direction {wind_dir}"
    if g["total_line"] is None:
        return False, "", None, "no total"
    runs = (g["home_score"] or 0) + (g["away_score"] or 0)
    under = runs < g["total_line"]
    push  = runs == g["total_line"]
    if push:
        return True, f"BET UNDER {g['total_line']} ({g['wind_mph']}mph IN)", None, "PUSH"
    return (True,
            f"BET UNDER {g['total_line']} ({g['wind_mph']}mph IN @ {g.get('venue_name','?')})",
            under, f"{'UNDER ✓' if under else 'OVER ✗'} — {runs} runs")


def eval_mvb(g):
    """MV-B: Wind-Out + Home Dog 35-45% → OVER."""
    if g.get("wind_effect") != "HIGH":
        return False, "", None, f"venue not HIGH"
    if g["wind_mph"] is None or g["wind_mph"] < gdb.WIND_OUT_MIN_MPH:
        return False, "", None, f"wind {g['wind_mph']} < 10"
    wind_dir = gdb.wind_direction_label(g.get("wind_direction") or "")
    if wind_dir != "OUT":
        return False, "", None, f"wind {wind_dir}"
    if g["home_ml"] is None:
        return False, "", None, "no ML"
    home_impl = implied_prob(g["home_ml"])
    if not (gdb.DOG_IMPL_LOW <= home_impl <= gdb.DOG_IMPL_HIGH):
        return False, "", None, f"home impl {home_impl:.0%} outside 35-45%"
    if g["total_line"] is None:
        return False, "", None, "no total"
    runs = (g["home_score"] or 0) + (g["away_score"] or 0)
    over = runs > g["total_line"]
    push = runs == g["total_line"]
    if push:
        return True, f"BET OVER {g['total_line']} (wind {g['wind_mph']}mph OUT, home {fmt_ml(g['home_ml'])})", None, "PUSH"
    return (True,
            f"BET OVER {g['total_line']} (wind {g['wind_mph']}mph OUT, {g['home_abbr']} {fmt_ml(g['home_ml'])} = {home_impl:.0%})",
            over, f"{'OVER ✓' if over else 'UNDER ✗'} — {runs} runs")


def eval_mvf(g):
    """MV-F: Wind-In + Home Fav -130/-160 → Fade ML (bet away)."""
    if g.get("wind_effect") != "HIGH":
        return False, "", None, f"venue not HIGH"
    if g["wind_mph"] is None or g["wind_mph"] < gdb.WIND_IN_MIN_MPH:
        return False, "", None, f"wind {g['wind_mph']} < 10"
    wind_dir = gdb.wind_direction_label(g.get("wind_direction") or "")
    if wind_dir != "IN":
        return False, "", None, f"wind {wind_dir}"
    if g["home_ml"] is None:
        return False, "", None, "no ML"
    if not (gdb.HOME_FAV_MV_F_HIGH <= g["home_ml"] <= gdb.HOME_FAV_MV_F_LOW):
        return False, "", None, f"home ML {fmt_ml(g['home_ml'])} not in -130/-160 zone"
    away_won = (g["away_score"] or 0) > (g["home_score"] or 0)
    away_ml  = g["away_ml"]
    return (True,
            f"BET AWAY {g['away_abbr']} {fmt_ml(away_ml)} (fade {g['home_abbr']} {fmt_ml(g['home_ml'])}, wind IN {g['wind_mph']}mph)",
            away_won, f"{'WIN ✓' if away_won else 'LOSS ✗'} — {g['away_abbr']} {g['away_score']} vs {g['home_abbr']} {g['home_score']}")


def eval_s1(g, streak):
    """S1: Home team W5+ → bet away ML."""
    if streak < gdb.STREAK_THRESHOLD:
        return False, "", None, f"home streak W{streak} < W{gdb.STREAK_THRESHOLD}"
    if g["away_ml"] is None:
        return False, "", None, "no away ML"
    away_won = (g["away_score"] or 0) > (g["home_score"] or 0)
    return (True,
            f"BET AWAY {g['away_abbr']} {fmt_ml(g['away_ml'])} (home {g['home_abbr']} on W{streak})",
            away_won, f"{'WIN ✓' if away_won else 'LOSS ✗'} — {g['away_abbr']} {g['away_score']} vs {g['home_abbr']} {g['home_score']}")


# ─────────────────────────────────────────────────────────────────────────────
# Scoring
# ─────────────────────────────────────────────────────────────────────────────

class HypothesisScore:
    def __init__(self, hyp_id):
        self.hyp_id = hyp_id
        self.bm     = BENCHMARKS[hyp_id]
        self.games  = []   # list of (won: bool|None, ml: int|None)

    def add(self, won, ml=None):
        self.games.append((won, ml))

    @property
    def n(self):
        return sum(1 for w, _ in self.games if w is not None)

    @property
    def wins(self):
        return sum(1 for w, _ in self.games if w is True)

    @property
    def losses(self):
        return sum(1 for w, _ in self.games if w is False)

    @property
    def pushes(self):
        return sum(1 for w, _ in self.games if w is None)

    @property
    def hit_rate(self):
        return self.wins / self.n if self.n > 0 else None

    @property
    def roi(self):
        if self.n == 0: return None
        total = 0.0
        for won, ml in self.games:
            if won is None: continue
            odds = ml if ml else -110
            total += pnl(odds, won)
        return total / self.n

    def grade_line(self):
        bm = self.bm
        direction = bm["direction"]
        hr   = self.hit_rate
        roi_ = self.roi
        n    = self.n

        if n == 0:
            return f"  {'─'*68}\n  NO DATA — no applicable games found in this month\n"

        lines = []
        lines.append(f"  {'─'*68}")
        lines.append(f"  {self.hyp_id:<6}  {bm['desc']}")
        lines.append(f"  Sample  : {n} applicable games  ({self.wins}W / {self.losses}L / {self.pushes} push)")

        if hr is not None:
            lines.append(f"  Hit rate: {hr*100:.1f}%")
        if roi_ is not None:
            sign = "+" if roi_ >= 0 else ""
            lines.append(f"  ROI     : {sign}{roi_*100:.2f}%")

        # Benchmark comparison
        bench_str = bm["note"]
        lines.append(f"  Bench   : {bench_str}")

        # Grade
        threshold = bm.get("threshold")
        grade = None

        if direction in ("total_over", "total_under", "home_dog_ml", "away_ml_fade", "road_fav_ml"):
            if roi_ is not None and threshold is not None:
                if direction in ("road_fav_ml",):
                    # For NF1 — expect negative: good if roi_ < threshold (threshold is negative)
                    grade = "✓ CONFIRMED" if roi_ <= threshold else ("~ PARTIAL" if roi_ < 0 else "✗ WRONG")
                else:
                    grade = "✓ CONFIRMED" if roi_ >= threshold else ("~ PARTIAL" if roi_ >= 0 else "✗ WRONG")

        elif direction in ("home_ml_all", "home_fav_ml", "era_edge_ml"):
            if roi_ is not None:
                grade = "✓ CONFIRMED" if roi_ < -0.03 else ("~ PARTIAL" if roi_ < 0 else "✗ WRONG")

        elif direction == "home_win_rate":
            if hr is not None:
                sbro = bm["sbro"]
                grade = "✓ CONFIRMED" if abs(hr - sbro) <= 0.04 else f"~ {hr*100:.1f}% vs expected {sbro*100:.1f}%"

        if grade:
            lines.append(f"  GRADE   : {grade}")

        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Main runner
# ─────────────────────────────────────────────────────────────────────────────

def run_month(conn, year, month, hyp_filter, show_detail, output_lines):

    def out(line=""):
        output_lines.append(line)

    month_name = datetime.date(year, month, 1).strftime("%B %Y")
    out(f"\n{'═'*72}")
    out(f"  GAME-BY-GAME HYPOTHESIS GRADER  ·  {month_name}")
    out(f"  Hypotheses: {', '.join(hyp_filter)}")
    out(f"{'═'*72}")

    # Load data
    out(f"\n  Loading games for {month_name}...")
    games = load_month_games(conn, year, month)
    out(f"  {len(games)} completed regular-season games found.")

    if not games:
        out(f"\n  ✗  No Final games found for {month_name}.")
        out(f"     Check that load_mlb_stats.py has been run for this period.")
        return

    out(f"\n  Computing home team streaks...")
    team_ids = list({g["home_team_id"] for g in games})
    streaks  = compute_streaks_for_month(conn, year, month, team_ids)

    out(f"  Loading SP ERA data (if available)...")
    era_data = load_sp_era_data(conn, year, month) if "H4" in hyp_filter else None
    if "H4" in hyp_filter:
        if era_data:
            out(f"  SP ERA data available — H4 will be graded.")
        else:
            out(f"  SP ERA data NOT available — H4 will be skipped.")

    # Initialise scorecards
    scores = {h: HypothesisScore(h) for h in hyp_filter}

    # ── Day-by-day game loop ──────────────────────────────────────────────────
    current_date = None
    day_count    = 0

    for g in games:
        if g["game_date"] != current_date:
            current_date = g["game_date"]
            day_count   += 1
            dow = datetime.date.fromisoformat(current_date).strftime("%a")
            if show_detail:
                out(f"\n  ── {current_date} ({dow}) {'─'*50}")

        home_streak = streaks.get(g["game_pk"], 0)
        score_str   = f"{g['away_abbr']} {g['away_score']}  @  {g['home_abbr']} (h) {g['home_score']}"
        ml_str      = f"ML {fmt_ml(g['home_ml'])}/{fmt_ml(g['away_ml'])}  O/U {g['total_line']}"
        wind_str    = f"wind {g['wind_mph']}mph {gdb.wind_direction_label(g.get('wind_direction') or '')}" if g.get("wind_mph") else "no wind data"

        if show_detail:
            out(f"\n  {score_str:<32}  {ml_str:<28}  {wind_str}")

        # Evaluate each hypothesis
        evaluators = {
            "H1a":  lambda g=g: eval_h1a(g),
            "H1b":  lambda g=g: eval_h1b(g),
            "H2":   lambda g=g: eval_h2(g),
            "H3a":  lambda g=g: eval_h3a(g),
            "H3b":  lambda g=g: eval_h3b(g),
            "H4":   lambda g=g: eval_h4(g, era_data),
            "H5":   lambda g=g: eval_h5(g),
            "H6":   lambda g=g: eval_h6(g),
            "NF1":  lambda g=g: eval_nf1(g),
            "NF2":  lambda g=g: eval_nf2(g),
            "NF3":  lambda g=g: eval_nf3(g),
            "MV-B": lambda g=g: eval_mvb(g),
            "MV-F": lambda g=g: eval_mvf(g),
            "S1":   lambda g=g: eval_s1(g, home_streak),
        }

        for h in hyp_filter:
            applicable, prediction, won, detail = evaluators[h]()
            if not applicable:
                continue

            # Score it
            ml_for_scoring = None
            if h in ("H1a",):       ml_for_scoring = g["home_ml"]
            elif h in ("H1b","H2"): ml_for_scoring = g["home_ml"]
            elif h in ("NF1","MV-F","S1"): ml_for_scoring = g["away_ml"]
            else:                   ml_for_scoring = g.get("over_odds") or g.get("under_odds") or -110

            scores[h].add(won, ml_for_scoring)

            if show_detail:
                mark = "✓" if won is True else ("✗" if won is False else "~")
                out(f"    [{h}] {prediction}")
                out(f"         → {detail}  {mark}")

    # ── Scorecards ────────────────────────────────────────────────────────────
    out(f"\n\n{'═'*72}")
    out(f"  HYPOTHESIS SCORECARD  ·  {month_name}  ·  {len(games)} games graded")
    out(f"{'═'*72}")

    for h in hyp_filter:
        out(scores[h].grade_line())

    # ── Summary table ─────────────────────────────────────────────────────────
    out(f"\n{'─'*72}")
    out(f"  SUMMARY TABLE")
    out(f"{'─'*72}")
    out(f"  {'HYP':<8} {'N':>5}  {'WIN':>5}  {'LOSS':>5}  {'PUSH':>5}  {'HIT%':>7}  {'ROI':>8}  GRADE")
    out(f"  {'─'*70}")

    for h in hyp_filter:
        sc = scores[h]
        n  = sc.n
        if n == 0:
            out(f"  {h:<8} {'—':>5}  {'—':>5}  {'—':>5}  {'—':>5}  {'—':>7}  {'—':>8}  NO DATA")
            continue
        hr_s  = f"{sc.hit_rate*100:.1f}%" if sc.hit_rate is not None else "—"
        roi_s = f"{sc.roi*100:+.2f}%"    if sc.roi      is not None else "—"
        bm    = BENCHMARKS[h]
        thr   = bm.get("threshold")
        dir_  = bm["direction"]
        roi_  = sc.roi
        if thr is not None and roi_ is not None:
            if dir_ == "road_fav_ml":
                g_str = "✓" if roi_ <= thr else ("~" if roi_ < 0 else "✗")
            else:
                g_str = "✓" if roi_ >= thr else ("~" if roi_ >= 0 else "✗")
        elif dir_ in ("home_ml_all","home_fav_ml","era_edge_ml") and roi_ is not None:
            g_str = "✓" if roi_ < -0.03 else ("~" if roi_ < 0 else "✗")
        elif dir_ == "home_win_rate" and sc.hit_rate is not None:
            g_str = "✓" if abs(sc.hit_rate - bm["sbro"]) <= 0.04 else "~"
        else:
            g_str = "—"

        out(f"  {h:<8} {n:>5}  {sc.wins:>5}  {sc.losses:>5}  {sc.pushes:>5}  {hr_s:>7}  {roi_s:>8}  {g_str}")

    out(f"\n  GRADE KEY: ✓ = confirmed (clears threshold)  "
        f"~ = partial/directional  ✗ = wrong  — = no data")
    out(f"\n  Run on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    out(f"{'═'*72}\n")


# ─────────────────────────────────────────────────────────────────────────────
# Available months helper
# ─────────────────────────────────────────────────────────────────────────────

def list_available_months(conn):
    rows = conn.execute("""
        SELECT substr(game_date,1,7) ym, COUNT(*) n
        FROM games
        WHERE game_type='R' AND status='Final'
        GROUP BY ym ORDER BY ym
    """).fetchall()
    print(f"\n  Available months with Final regular-season games:\n")
    for r in rows:
        print(f"    {r[0]}   ({r[1]} games)")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="MLB Scout — Monthly Game-by-Game Hypothesis Grader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "EXAMPLES\n"
            "  python monthly_hypothesis_grader.py --month 2025-09\n"
            "  python monthly_hypothesis_grader.py --month 2024-07 --output reports\\jul24.txt\n"
            "  python monthly_hypothesis_grader.py --month 2025-09 --hyp H3b MV-B S1\n"
            "  python monthly_hypothesis_grader.py --month 2025-09 --no-detail\n"
            "  python monthly_hypothesis_grader.py --list-months\n"
        ),
    )
    p.add_argument("--month",       default=None,
                   help="Month to grade: YYYY-MM (e.g. 2025-09)")
    p.add_argument("--hyp",         nargs="+", default=None,
                   choices=HYPOTHESES_ORDERED, metavar="HYP",
                   help="Grade specific hypotheses only (default: all)")
    p.add_argument("--no-detail",   action="store_true",
                   help="Suppress per-game rows — show scorecard only")
    p.add_argument("--output",      default=None,
                   help="Save full report to this file path")
    p.add_argument("--list-months", action="store_true",
                   help="List available months in the DB and exit")
    args = p.parse_args()

    conn = open_db()

    if args.list_months:
        list_available_months(conn)
        conn.close()
        return

    if not args.month:
        p.error("--month is required (or use --list-months to see available dates)")

    try:
        dt    = datetime.datetime.strptime(args.month, "%Y-%m")
        year  = dt.year
        month = dt.month
    except ValueError:
        p.error(f"Invalid --month format: '{args.month}'. Use YYYY-MM.")

    hyp_filter  = args.hyp or HYPOTHESES_ORDERED
    show_detail = not args.no_detail

    output_lines = []
    run_month(conn, year, month, hyp_filter, show_detail, output_lines)
    conn.close()

    full_output = "\n".join(output_lines)
    print(full_output)

    # Save if requested
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(full_output)
        print(f"\n  Report saved: {args.output}")
    else:
        # Auto-save to reports folder
        reports_dir = Path(__file__).parent / "reports"
        reports_dir.mkdir(exist_ok=True)
        fname = reports_dir / f"hypothesis_grade_{args.month}.txt"
        with open(fname, "w", encoding="utf-8") as f:
            f.write(full_output)
        print(f"\n  Report auto-saved: {fname}")


if __name__ == "__main__":
    main()
