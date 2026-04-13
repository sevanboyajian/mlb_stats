"""
backtest_top_pick.py
====================
MLB Betting Model · Top Pick Backtester

Simulates what the Primary Brief's Top Pick would have been on every game
day in a date range, grades it against actual results, and produces a
financial summary using flat-bet sizing.

The signal engine is imported directly from generate_daily_brief.py, so
this backtester uses the exact same logic as the live brief — no drift.

USAGE
-----
    # Default: $10 flat bet, September 2025
    python backtest_top_pick.py --month 2025-09

    # Custom date range and stake
    python backtest_top_pick.py --from 2025-04-01 --to 2025-09-30 --stake 25

    # Save results to file
    python backtest_top_pick.py --month 2025-09 --output reports/backtest_sep25.txt

    # Show every game day detail (not just picks)
    python backtest_top_pick.py --month 2025-09 --verbose

    # Filter to a specific signal
    python backtest_top_pick.py --month 2025-09 --signal MV-B

    # See which months have data
    python backtest_top_pick.py --list-months

HOW THE TOP PICK IS DETERMINED
-------------------------------
For each game day the backtester:
  1. Loads all that day's Final games with closing-line odds
  2. Runs evaluate_signals() on every game (same as the Primary Brief)
  3. Picks the highest-priority signal across the slate
  4. Grades it WIN / LOSS / PUSH against the actual score
  5. Applies flat-bet ROI math at the closing line

If no signal fires on a given day, the day is skipped (no bet placed).
This matches real-world discipline — the brief says "no bet today" and
you don't bet.

ROI MATH
--------
  Bet favourite -150 → WIN:  +$6.67 on $10 bet (100/150 × $10)
  Bet favourite -150 → LOSS: -$10.00
  Bet underdog  +140 → WIN:  +$14.00
  Bet underdog  +140 → LOSS: -$10.00
  PUSH:  $0.00
"""

import argparse
import datetime
import importlib.util
import os
import sqlite3
import sys
import textwrap
from pathlib import Path

# ── Locate generate_daily_brief.py in the same folder ──────────────────────
_SCRIPT_DIR = Path(__file__).parent
_BRIEF_PATH = _SCRIPT_DIR / "generate_daily_brief.py"

DB_PATH = _SCRIPT_DIR / "mlb_stats.db"


def _load_brief_module():
    """Import generate_daily_brief as a module to reuse its signal engine."""
    if not _BRIEF_PATH.exists():
        print(f"\n✗  generate_daily_brief.py not found in {_SCRIPT_DIR}")
        print("   Both scripts must live in the same directory.")
        sys.exit(1)
    spec = importlib.util.spec_from_file_location("generate_daily_brief", _BRIEF_PATH)
    mod  = importlib.util.module_from_spec(spec)
    # Suppress argparse processing during import
    _orig = sys.argv
    sys.argv = sys.argv[:1]
    try:
        spec.loader.exec_module(mod)
    except SystemExit:
        pass
    sys.argv = _orig
    return mod


# ═══════════════════════════════════════════════════════════════════════════
# Database helpers
# ═══════════════════════════════════════════════════════════════════════════

def open_db() -> sqlite3.Connection:
    if not DB_PATH.exists():
        print(f"\n✗  Database not found: {DB_PATH}")
        sys.exit(1)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_game_dates(conn, date_from: str, date_to: str) -> list:
    """Return sorted list of dates that have at least one Final regular-season game."""
    cur = conn.execute(
        """
        SELECT DISTINCT game_date
        FROM   games
        WHERE  game_date BETWEEN ? AND ?
          AND  status    = 'Final'
          AND  game_type = 'R'
        ORDER  BY game_date
        """,
        (date_from, date_to),
    )
    return [r["game_date"] for r in cur.fetchall()]


def list_available_months(conn):
    """Print available months and exit."""
    cur = conn.execute(
        """
        SELECT SUBSTR(game_date,1,7) AS month,
               COUNT(DISTINCT game_date) AS game_days,
               COUNT(*) AS total_games
        FROM   games
        WHERE  status    = 'Final'
          AND  game_type = 'R'
        GROUP  BY month
        ORDER  BY month
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("\n  No Final regular-season games found in DB.\n")
        return
    print(f"\n  {'MONTH':<10}  {'GAME DAYS':>10}  {'TOTAL GAMES':>12}")
    print(f"  {'─'*10}  {'─'*10}  {'─'*12}")
    for r in rows:
        print(f"  {r['month']:<10}  {r['game_days']:>10}  {r['total_games']:>12}")
    print()


def load_games_for_date(conn, game_date: str, mod) -> list:
    """
    Load Final games for a given date with closing odds.
    Mirrors the load_games() query in generate_daily_brief.py but filters
    on status = 'Final' instead of != 'Final'.
    """
    cur = conn.execute(
        """
        SELECT
            g.game_pk,
            g.game_date,
            g.game_start_utc,
            v.name          AS venue_name,
            g.temp_f,
            g.wind_mph,
            g.wind_direction,
            g.sky_condition,
            v.wind_effect,
            v.wind_note,
            v.roof_type,
            v.elevation_ft,
            v.park_factor_runs,
            v.park_factor_hr,
            v.orientation_hp,
            th.team_id      AS home_team_id,
            th.abbreviation AS home_abbr,
            th.name         AS home_name,
            ta.team_id      AS away_team_id,
            ta.abbreviation AS away_abbr,
            ta.name         AS away_name,
            ml.home_ml,
            ml.away_ml,
            tot.total_line,
            tot.over_odds,
            tot.under_odds,
            rl.home_rl_line AS home_rl,
            rl.away_rl_line AS away_rl,
            rl.home_rl_odds,
            rl.away_rl_odds,
            g.home_score,
            g.away_score
        FROM   games g
        JOIN   teams  th ON th.team_id  = g.home_team_id
        JOIN   teams  ta ON ta.team_id  = g.away_team_id
        LEFT JOIN venues v  ON v.venue_id   = g.venue_id
        LEFT JOIN v_closing_game_odds ml  ON ml.game_pk  = g.game_pk
                                         AND ml.market_type = 'moneyline'
        LEFT JOIN v_closing_game_odds tot ON tot.game_pk = g.game_pk
                                         AND tot.market_type = 'total'
        LEFT JOIN v_closing_game_odds rl  ON rl.game_pk  = g.game_pk
                                         AND rl.market_type = 'runline'
        WHERE  g.game_date = ?
          AND  g.status    = 'Final'
          AND  g.game_type = 'R'
        ORDER  BY g.game_start_utc
        """,
        (game_date,),
    )
    return [dict(r) for r in cur.fetchall()]


# ═══════════════════════════════════════════════════════════════════════════
# Grading helpers
# ═══════════════════════════════════════════════════════════════════════════

def grade_pick(game: dict, pick: dict) -> str:
    """
    Grade a pick as WIN / LOSS / PUSH / NO_RESULT.
    pick["market"] is 'ML' or 'TOTAL'.
    pick["bet"] is e.g. 'STL ML' or 'OVER 8.5'.
    """
    hs = game.get("home_score")
    as_ = game.get("away_score")
    if hs is None or as_ is None:
        return "NO_RESULT"

    market = pick.get("market", "ML").upper()

    if market == "ML":
        # Determine which side was bet
        bet_str = pick.get("bet", "").upper()
        home_abbr = (game.get("home_abbr") or "").upper()
        away_abbr = (game.get("away_abbr") or "").upper()
        if home_abbr in bet_str:
            winner = "HOME"
        elif away_abbr in bet_str:
            winner = "AWAY"
        else:
            # Fallback: if bet contains no recognisable abbr, can't grade
            return "NO_RESULT"

        if hs > as_:
            actual = "HOME"
        elif as_ > hs:
            actual = "AWAY"
        else:
            return "PUSH"

        return "WIN" if winner == actual else "LOSS"

    elif market == "TOTAL":
        total = game.get("total_line")
        if total is None:
            return "NO_RESULT"
        runs = hs + as_
        bet_str = pick.get("bet", "").upper()
        is_over = "OVER" in bet_str
        if runs > total:
            return "WIN" if is_over else "LOSS"
        elif runs < total:
            return "LOSS" if is_over else "WIN"
        else:
            return "PUSH"

    return "NO_RESULT"


def calc_pnl(pick: dict, game: dict, result: str, stake: float) -> float:
    """
    Calculate profit/loss for a flat-bet wager.
    For ML bets, uses the actual line from the pick odds string.
    For total bets, uses -110 unless over_odds/under_odds is available.
    """
    if result in ("NO_RESULT",):
        return 0.0
    if result == "PUSH":
        return 0.0
    if result == "LOSS":
        return -stake

    # WIN — calculate return based on odds
    market = pick.get("market", "ML").upper()
    odds_str = pick.get("odds", "-110")

    # Parse odds string (e.g. "+145", "-165", "-110")
    try:
        odds_str_clean = str(odds_str).replace(" ", "")
        if odds_str_clean.lstrip("+-").isdigit():
            odds_int = int(odds_str_clean)
        else:
            odds_int = -110  # fallback
    except (ValueError, TypeError):
        odds_int = -110

    if odds_int > 0:
        return stake * (odds_int / 100.0)
    elif odds_int < 0:
        return stake * (100.0 / abs(odds_int))
    else:
        return 0.0


# ═══════════════════════════════════════════════════════════════════════════
# Core backtest loop
# ═══════════════════════════════════════════════════════════════════════════

def _recent_run_diff(conn, team_id: int, game_date: str, n: int = 5) -> float | None:
    """
    Return the average run differential per game for team_id over the
    last n Final games before game_date.
    Positive = team outscored opponents on average; negative = got outscored.
    Returns None if fewer than 3 games found (insufficient sample).
    """
    try:
        cur = conn.execute(
            """
            SELECT
                CASE WHEN home_team_id = ? THEN home_score - away_score
                     ELSE away_score - home_score END AS diff
            FROM   games
            WHERE  (home_team_id = ? OR away_team_id = ?)
              AND  status    = 'Final'
              AND  game_date < ?
              AND  home_score IS NOT NULL
            ORDER  BY game_date DESC, game_start_utc DESC
            LIMIT  ?
            """,
            (team_id, team_id, team_id, game_date, n),
        )
        diffs = [r["diff"] for r in cur.fetchall()]
        if len(diffs) < 3:
            return None
        return sum(diffs) / len(diffs)
    except Exception:
        return None


def _best_value_pick(games: list, conn=None, game_date: str = "") -> dict | None:
    """
    On days with no model signal, select the best home underdog for
    educational publishing. Four filters applied (derived from Sep 2025
    backtesting analysis):

    Filter 1 — Home underdog only (+odds on home team).
        H1a research is specifically a home underdog finding. Away underdog
        picks showed 29% hit rate vs 36% for home underdogs in Sep 2025.

    Filter 2 — Odds band +115 to +145.
        The +120/+139 bucket produced 67% hit rate and +$31 P&L in Sep 2025.
        Near-pick-em (+100/+114) went 1-6 (−$49). Extreme dogs (+145+) have
        high variance and narrow sample. This band maps to ~41–46% implied,
        aligning with the H1a heavy-dog sub-segment.

    Filter 3 — Implied probability 37%–46%.
        Directly encodes the H1a overlay zone. Below 37% = too long, too
        volatile. Above 46% = market has priced out the overlay.

    Filter 4 — Blowout guard: away team avg run diff ≤ +2.0 over last 5.
        The three worst losses (Sep 7 −16, Sep 6 −6, Sep 1 −5) all involved
        visiting teams in dominant recent form. If the away team is averaging
        +2 or more runs per game over their last 5, skip this game — the home
        underdog price does not reflect the actual mismatch.

    If no game passes all four filters, falls back progressively:
        → relax blowout guard to +3.0
        → relax odds band to +110–+155
        → any home underdog with positive odds
        → any game (original least-vig selector)

    This pick is NEVER included in ROI / P&L calculations.
    """

    def _impl(ml: int) -> float:
        """American odds → implied probability."""
        if ml > 0:
            return 100 / (ml + 100)
        return abs(ml) / (abs(ml) + 100)

    def _try_filters(games, odds_lo, odds_hi, impl_lo, impl_hi,
                     blowout_limit, conn, game_date):
        """Attempt selection with given filter parameters. Returns (game, reason) or None."""
        candidates = []
        for g in games:
            hml = g.get("home_ml")
            aml = g.get("away_ml")
            if hml is None or aml is None:
                continue

            # Filter 1: home team must be underdog (+odds)
            if hml <= 0:
                continue

            # Filter 2: odds band
            if not (odds_lo <= hml <= odds_hi):
                continue

            # Filter 3: implied probability band
            home_impl = _impl(hml)
            if not (impl_lo <= home_impl <= impl_hi):
                continue

            # Filter 4: blowout guard — check away team's recent form
            if conn and game_date and blowout_limit is not None:
                away_id  = g.get("away_team_id")
                away_diff = _recent_run_diff(conn, away_id, game_date)
                if away_diff is not None and away_diff > blowout_limit:
                    continue   # visiting team in too dominant recent form

            # Score by implied probability proximity to 42% (centre of target band)
            score = abs(home_impl - 0.42)
            candidates.append((score, g))

        if not candidates:
            return None
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]

    # Attempt 1: primary H1a band +100–+139, implied 37–51%, blowout guard.
    # Data shows +100/+119 is the BEST performing home dog bucket (ROI +5.36%,
    # n=707) and +120/+139 is second best (+4.05%, n=416). Upper cap moved from
    # +145 down to +139 because +140/+159 shows NEGATIVE ROI (−3.92%) in
    # 2024-2025 data — the gap between implied and win% turns negative there.
    g = _try_filters(games, 100, 139, 0.37, 0.51, 2.0, conn, game_date)
    filter_desc = "H1a overlay zone (+100/+139, home dog, blowout guard)"

    # Attempt 2: relax blowout guard
    if g is None:
        g = _try_filters(games, 100, 139, 0.37, 0.51, 3.0, conn, game_date)
        filter_desc = "H1a overlay zone (+100/+139, home dog, relaxed blowout guard)"

    # Attempt 3: relax odds band (widen both ends)
    if g is None:
        g = _try_filters(games, 100, 155, 0.34, 0.51, None, conn, game_date)
        filter_desc = "relaxed odds band (+100/+155, home dog)"

    # Attempt 4: any home underdog
    if g is None:
        g = _try_filters(games, 100, 999, 0.0, 1.0, None, conn, game_date)
        filter_desc = "any home underdog (no qualifying game in target band)"

    # Fallback: original least-vig selector (no home-dog requirement)
    if g is None:
        best = None
        best_score = float("inf")
        for game in games:
            hml = game.get("home_ml")
            aml = game.get("away_ml")
            if hml is None or aml is None:
                continue
            impl_sum = _impl(hml) + _impl(aml)
            if impl_sum < best_score:
                best_score = impl_sum
                best = game
        g = best
        filter_desc = "least-vig fallback (no home underdog available)"

    if g is None:
        return None

    home  = g.get("home_abbr", "HOME")
    away  = g.get("away_abbr", "AWAY")
    hml   = g.get("home_ml")
    total = g.get("total_line")
    hs    = g.get("home_score")
    as_   = g.get("away_score")

    # Always bet the home team (all filters above require home underdog)
    if hml is not None and hml > 0:
        bet_side = f"{home} ML"
        odds_str = f"+{int(hml)}"
    else:
        # Fallback path only — pick whichever side is the underdog
        aml = g.get("away_ml")
        if aml is not None and aml > 0:
            bet_side = f"{away} ML"
            odds_str = f"+{int(aml)}"
        elif hml is not None:
            bet_side = f"{home} ML"
            odds_str = str(int(hml))
        else:
            bet_side = f"{away} ML"
            odds_str = "N/A"

    return {
        "game":        g,
        "matchup":     f"{away} @ {home} (h)",
        "score":       f"{away} {as_}  –  {home} {hs}" if hs is not None else "N/A",
        "bet":         bet_side,
        "odds":        odds_str,
        "total":       f"O/U {total}" if total else "",
        "signals":     "NO SIGNAL",
        "filter_desc": filter_desc,
        "result":      "N/A  (educational only — not graded)",
        "pnl":         None,
    }


def load_opening_odds(conn, game_pks: list) -> dict:
    """
    Load the earliest (opening) odds for a set of game_pks from game_odds.
    Returns {game_pk: {home_ml_open, away_ml_open, total_open}} or empty dict
    if the game_odds table is not available or has no pregame rows.
    CLV = closing odds − opening odds (in cents on implied probability).
    """
    if not game_pks:
        return {}
    placeholders = ",".join("?" * len(game_pks))
    try:
        cur = conn.execute(
            f"""
            SELECT   game_pk,
                     market_type,
                     home_price,
                     away_price,
                     line_value,
                     captured_at_utc
            FROM     game_odds
            WHERE    game_pk IN ({placeholders})
              AND    market_type IN ('moneyline', 'total')
            ORDER BY game_pk, market_type, captured_at_utc ASC
            """,
            game_pks,
        )
        rows = cur.fetchall()
    except Exception:
        return {}   # table missing or schema mismatch — CLV unavailable

    opening = {}
    for r in rows:
        gpk  = r["game_pk"]
        mkt  = r["market_type"]
        if gpk not in opening:
            opening[gpk] = {}
        # Only store the FIRST (earliest) row per game+market
        if mkt == "moneyline" and "home_ml_open" not in opening[gpk]:
            opening[gpk]["home_ml_open"] = r["home_price"]
            opening[gpk]["away_ml_open"] = r["away_price"]
        elif mkt == "total" and "total_open" not in opening[gpk]:
            opening[gpk]["total_open"] = r["line_value"]
    return opening


def calc_clv(pick: dict, game: dict, opening: dict) -> str | None:
    """
    Calculate Closing Line Value for a pick.
    CLV > 0 means you got a better price than the close (positive indicator).
    CLV < 0 means you got a worse price (sharp money moved against you).

    For ML picks: CLV = closing implied − opening implied (in pp).
    For TOTAL picks: CLV = closing line − opening line (in runs).

    Returns a formatted string or None if data is unavailable.
    """
    gpk     = game.get("game_pk")
    mkt     = pick.get("market", "ML").upper()
    opening_game = opening.get(gpk, {})
    if not opening_game:
        return None

    def _impl(ml):
        if ml is None: return None
        ml = float(ml)
        return abs(ml) / (abs(ml) + 100) if ml < 0 else 100 / (ml + 100)

    if mkt == "ML":
        bet_str   = pick.get("bet", "").upper()
        home_abbr = (game.get("home_abbr") or "").upper()
        away_abbr = (game.get("away_abbr") or "").upper()

        # Determine which side was bet
        if home_abbr in bet_str:
            close_ml = game.get("home_ml")
            open_ml  = opening_game.get("home_ml_open")
        elif away_abbr in bet_str:
            close_ml = game.get("away_ml")
            open_ml  = opening_game.get("away_ml_open")
        else:
            return None

        impl_close = _impl(close_ml)
        impl_open  = _impl(open_ml)
        if impl_close is None or impl_open is None:
            return None

        # CLV in implied probability pp (positive = your price was better)
        clv_pp = (impl_open - impl_close) * 100
        arrow  = "+" if clv_pp >= 0 else ""
        return f"{arrow}{clv_pp:.1f}pp impl"

    elif mkt == "TOTAL":
        close_total = game.get("total_line")
        open_total  = opening_game.get("total_open")
        if close_total is None or open_total is None:
            return None

        bet_str  = pick.get("bet", "").upper()
        is_over  = "OVER" in bet_str
        # For OVER bets: CLV > 0 if closing total is higher (you got lower number = better)
        # For UNDER bets: CLV > 0 if closing total is lower (you got higher number = better)
        if is_over:
            clv = close_total - open_total   # positive = close moved up = bad for OVER
            clv = -clv                        # invert: positive CLV = close higher = good entry
        else:
            clv = open_total - close_total
        arrow = "+" if clv >= 0 else ""
        return f"{arrow}{clv:.1f} runs"

    return None


def run_backtest(conn, mod, date_from: str, date_to: str,
                 stake: float, signal_filter: str | None,
                 verbose: bool,
                 late_season_stake: float | None = None) -> dict:
    """
    Main backtest loop. Returns a results dict.
    late_season_stake: if set, bets in Aug (month 8) and Sep (month 9) use
    this stake instead of the default stake. Models the 3-year finding that
    Aug/Sep ROI averages −13% vs +12% Apr–Jun.
    """
    # Months considered late-season for stake reduction
    LATE_SEASON_MONTHS = {8, 9}

    def stake_for_date(date_str: str) -> float:
        """Return the appropriate stake based on calendar month."""
        if late_season_stake is None:
            return stake
        try:
            month = int(date_str[5:7])
        except (ValueError, IndexError):
            return stake
        return late_season_stake if month in LATE_SEASON_MONTHS else stake
    game_dates = get_game_dates(conn, date_from, date_to)

    if not game_dates:
        print(f"\n  ⚠  No Final regular-season games found between {date_from} and {date_to}.")
        print("     Check --from / --to dates, or run: python backtest_top_pick.py --list-months\n")
        sys.exit(0)

    results = {
        "date_from":    date_from,
        "date_to":      date_to,
        "stake":        stake,
        "late_season_stake": late_season_stake,
        "signal_filter": signal_filter,
        "game_days_in_range": len(game_dates),
        "days_with_pick": 0,
        "days_no_signal": 0,
        "days_no_odds":   0,
        "bets":          [],   # graded signal picks
        "no_signal_bets": [],  # educational best-value picks (not graded)
        "total_staked":  0.0,
        "total_pnl":     0.0,
        "wins":   0,
        "losses": 0,
        "pushes": 0,
        "no_results": 0,
    }

    for game_date in game_dates:
        games = load_games_for_date(conn, game_date, mod)
        if not games:
            continue

        # Load streaks entering this date
        team_ids = list({g["home_team_id"] for g in games}
                      | {g["away_team_id"] for g in games})
        streaks = mod.load_streaks(conn, game_date, team_ids, False)

        # Evaluate signals on every game
        picks_today = []
        for game in games:
            if game.get("home_ml") is None:
                continue   # no odds — skip this game
            sigs = mod.evaluate_signals(game, streaks, "primary")
            if not sigs["picks"]:
                continue
            for pick in sigs["picks"]:
                # Apply signal filter if requested
                if signal_filter:
                    if signal_filter not in sigs["signals"]:
                        continue
                picks_today.append({
                    "game":     game,
                    "pick":     pick,
                    "signals":  sigs["signals"],
                    "priority": pick["priority"],
                })

        if not picks_today:
            # Check if games existed but had no odds (data gap)
            has_odds = any(g.get("home_ml") is not None for g in games)
            if not has_odds:
                results["days_no_odds"] += 1
            else:
                results["days_no_signal"] += 1
                # Capture best-value pick for educational publishing
                fallback = _best_value_pick(games, conn=conn, game_date=game_date)
                if fallback:
                    fallback["date"] = game_date
                    results["no_signal_bets"].append(fallback)
            if verbose:
                status = "NO ODDS" if not has_odds else "NO SIGNAL"
                print(f"  {game_date}  {status}  ({len(games)} games)")
            continue

        # Top pick = lowest priority number (1 = highest priority)
        picks_today.sort(key=lambda x: x["priority"])
        top = picks_today[0]
        game = top["game"]
        pick = top["pick"]

        result = grade_pick(game, pick)
        day_stake = stake_for_date(game_date)
        pnl    = calc_pnl(pick, game, result, day_stake)

        # CLV — load opening odds for this game only (lazy, one query per signal day)
        opening = load_opening_odds(conn, [game["game_pk"]])
        clv_str = calc_clv(pick, game, opening)

        home = game.get("home_abbr", "HOME")
        away = game.get("away_abbr", "AWAY")
        hs   = game.get("home_score")
        as_  = game.get("away_score")

        bet_record = {
            "date":     game_date,
            "matchup":  f"{away} @ {home} (h)",
            "score":    f"{away} {as_}  –  {home} {hs}" if hs is not None else "N/A",
            "bet":      pick["bet"],
            "odds":     pick["odds"],
            "signals":  ", ".join(top["signals"]),
            "result":   result,
            "pnl":      pnl,
            "stake":    day_stake,
            "clv":      clv_str,
            "reason":   pick["reason"][:80],
        }
        results["bets"].append(bet_record)
        results["days_with_pick"] += 1

        if result == "WIN":
            results["wins"]  += 1
            results["total_pnl"]    += pnl
            results["total_staked"] += day_stake
        elif result == "LOSS":
            results["losses"] += 1
            results["total_pnl"]    += pnl
            results["total_staked"] += day_stake
        elif result == "PUSH":
            results["pushes"] += 1
            results["total_staked"] += day_stake
        else:
            results["no_results"] += 1

    return results


# ═══════════════════════════════════════════════════════════════════════════
# Report formatting
# ═══════════════════════════════════════════════════════════════════════════

def format_report(results: dict) -> str:
    W  = 72
    SEP = "  " + "─" * (W - 2)   # underbar separator between each day block

    lines = []

    def bar(ch="═"):
        return ch * W

    stake      = results["stake"]
    bets       = results["bets"]
    wins       = results["wins"]
    losses     = results["losses"]
    pushes     = results["pushes"]
    total_bets = wins + losses + pushes
    hit_rate   = (wins / total_bets * 100) if total_bets > 0 else 0
    total_pnl  = results["total_pnl"]
    staked     = results["total_staked"]
    roi        = (total_pnl / staked * 100) if staked > 0 else 0
    net_str    = f"+${total_pnl:.2f}" if total_pnl >= 0 else f"-${abs(total_pnl):.2f}"
    sig_label  = f"  Filter: {results['signal_filter']}" if results["signal_filter"] else ""

    lines.append(f"\n{bar()}")
    lines.append(f"  MLB SCOUT · TOP PICK BACKTESTER")
    lines.append(f"  {results['date_from']}  →  {results['date_to']}"
                 + f"     Stake: ${stake:.2f}/day")
    if sig_label:
        lines.append(sig_label)
    lines.append(bar())

    # ── Build unified chronological day list ─────────────────────────────
    all_days = []
    for b in bets:
        all_days.append(("SIGNAL", b))
    for b in results.get("no_signal_bets", []):
        all_days.append(("NO_SIGNAL", b))
    all_days.sort(key=lambda x: x[1]["date"])

    lines.append("")

    for i, (kind, b) in enumerate(all_days):

        if kind == "SIGNAL":
            result_str = b["result"]
            if result_str == "WIN":
                result_disp = "✓ WIN"
            elif result_str == "LOSS":
                result_disp = "✗ LOSS"
            elif result_str == "PUSH":
                result_disp = "~ PUSH"
            else:
                result_disp = "? N/A"
            pnl = b["pnl"]
            pnl_str = (f"+${pnl:.2f}" if pnl > 0
                       else (f"-${abs(pnl):.2f}" if pnl < 0 else "$0.00"))
            clv     = b.get("clv")
            clv_str = f"   CLV: {clv}" if clv else "   CLV: n/a"
            # Show stake only when variable (late-season reduction active)
            bet_stake = b.get("stake", stake)
            stake_note = f"   stake: ${bet_stake:.2f}" if results.get("late_season_stake") else ""

            lines.append(f"  {b['date']}   {b['matchup']}")
            lines.append(f"  Bet     : {b['bet']}  {b['odds']}   Signal: {b['signals']}{stake_note}")
            lines.append(f"  Score   : {b['score']}")
            lines.append(f"  Result  : {result_disp}   P&L: {pnl_str}{clv_str}")

        else:
            # ── Educational pick — grade it for reference ─────────────────
            game = b.get("game", {})
            hs   = game.get("home_score")
            as_  = game.get("away_score")

            if hs is not None and as_ is not None:
                # Determine which team was bet
                home_abbr = game.get("home_abbr", "")
                away_abbr = game.get("away_abbr", "")
                bet_str   = b.get("bet", "").upper()
                if home_abbr.upper() in bet_str:
                    edu_won = hs > as_
                elif away_abbr.upper() in bet_str:
                    edu_won = as_ > hs
                else:
                    edu_won = None

                if edu_won is True:
                    edu_result = "✓ WIN  (educational — not staked)"
                elif edu_won is False:
                    edu_result = "✗ LOSS  (educational — not staked)"
                else:
                    edu_result = "? N/A  (educational — not staked)"
            else:
                edu_result = "? N/A  (educational — not staked)"

            total_str = f"   {b.get('total', '')}" if b.get("total") else ""
            lines.append(f"  {b['date']}   {b['matchup']}   [no model signal]")
            lines.append(f"  Bet     : {b['bet']}  {b['odds']}{total_str}")
            lines.append(f"  Score   : {b['score']}")
            lines.append(f"  Result  : {edu_result}")

        # Separator after every day block except the last
        if i < len(all_days) - 1:
            lines.append(SEP)

    lines.append(f"\n{bar()}")

    if results.get("days_no_odds", 0) > 0:
        lines.append(f"  Note: {results['days_no_odds']} day(s) had no odds in DB and are omitted above.")

    # ── Summary scorecard ────────────────────────────────────────────────
    lines.append(f"  SUMMARY  (signal days only — no-signal days not staked)")
    lines.append(f"  {'─'*W}")
    if results.get("late_season_stake"):
        lines.append(f"  Stake structure     : ${stake:.2f} Apr–Jul  /  "
                     f"${results['late_season_stake']:.2f} Aug–Sep (late-season reduction)")
    lines.append(f"  Game days in range  : {results['game_days_in_range']}")
    lines.append(f"  Days a bet was made : {results['days_with_pick']}")
    lines.append(f"  Days no signal fired: {results['days_no_signal']}  "
                 f"(educational pick shown above for reference)")
    lines.append(f"  Total bets graded   : {total_bets}  "
                 f"(W:{wins}  L:{losses}  P:{pushes})")
    if results["no_results"] > 0:
        lines.append(f"  Ungraded (no score) : {results['no_results']}")
    lines.append(f"  Hit rate            : {hit_rate:.1f}%")
    lines.append(f"  Stake per bet       : ${stake:.2f}")
    lines.append(f"  Total staked        : ${staked:.2f}")
    lines.append(f"  Net P&L             : {net_str}")
    lines.append(f"  ROI                 : {roi:+.2f}%")
    lines.append("")

    # ── Signal breakdown ──────────────────────────────────────────────────
    sig_counts: dict = {}
    sig_wins:   dict = {}
    sig_pnl:    dict = {}
    for b in bets:
        for sig in b["signals"].split(", "):
            sig = sig.strip()
            if not sig:
                continue
            sig_counts[sig] = sig_counts.get(sig, 0) + 1
            sig_wins[sig]   = sig_wins.get(sig, 0)   + (1 if b["result"] == "WIN" else 0)
            sig_pnl[sig]    = sig_pnl.get(sig, 0.0)  + b["pnl"]

    if sig_counts:
        lines.append(f"  {'─'*W}")
        lines.append(f"  SIGNAL BREAKDOWN  (stacked signals count in each component)")
        lines.append(f"  {'─'*W}")
        lines.append(f"  {'SIGNAL':<12}  {'FIRED':>6}  {'WINS':>6}  {'HIT%':>7}  {'NET P&L':>10}")
        lines.append(f"  {'─'*12}  {'─'*6}  {'─'*6}  {'─'*7}  {'─'*10}")
        for sig, cnt in sorted(sig_counts.items(), key=lambda x: -x[1]):
            w   = sig_wins.get(sig, 0)
            pnl = sig_pnl.get(sig, 0.0)
            hr  = w / cnt * 100 if cnt > 0 else 0
            ps  = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
            lines.append(f"  {sig:<12}  {cnt:>6}  {w:>6}  {hr:>6.1f}%  {ps:>10}")
        lines.append("")

    # ── Combined wins vs losses summary ─────────────────────────────────
    edu_wins   = 0
    edu_losses = 0
    edu_na     = 0
    for b in results.get("no_signal_bets", []):
        game      = b.get("game", {})
        hs        = game.get("home_score")
        as_       = game.get("away_score")
        if hs is None or as_ is None:
            edu_na += 1
            continue
        home_abbr = game.get("home_abbr", "")
        away_abbr = game.get("away_abbr", "")
        bet_str   = b.get("bet", "").upper()
        if home_abbr.upper() in bet_str:
            won = hs > as_
        elif away_abbr.upper() in bet_str:
            won = as_ > hs
        else:
            edu_na += 1
            continue
        if won:
            edu_wins += 1
        else:
            edu_losses += 1

    edu_total    = edu_wins + edu_losses
    edu_hit      = (edu_wins / edu_total * 100) if edu_total > 0 else 0
    sig_total    = wins + losses + pushes
    sig_hit      = (wins / sig_total * 100) if sig_total > 0 else 0
    combined_w   = wins + edu_wins
    combined_l   = losses + edu_losses
    combined_tot = combined_w + combined_l
    combined_hit = (combined_w / combined_tot * 100) if combined_tot > 0 else 0

    lines.append(f"  {'─'*W}")
    lines.append(f"  WINS VS LOSSES — SIGNAL vs EDUCATIONAL COMPARISON")
    lines.append(f"  {'─'*W}")
    lines.append(f"  {'Category':<26}  {'Bets':>5}  {'W':>5}  {'L':>5}  {'Hit%':>7}  {'Note'}")
    lines.append(f"  {'─'*26}  {'─'*5}  {'─'*5}  {'─'*5}  {'─'*7}  {'─'*18}")
    lines.append(
        f"  {'Signal picks (staked)':<26}  {sig_total:>5}  {wins:>5}  {losses:>5}  "
        f"{sig_hit:>6.1f}%  ROI {roi:+.2f}%"
    )
    lines.append(
        f"  {'Educational (not staked)':<26}  {edu_total:>5}  {edu_wins:>5}  "
        f"{edu_losses:>5}  {edu_hit:>6.1f}%  reference only"
    )
    lines.append(f"  {'─'*26}  {'─'*5}  {'─'*5}  {'─'*5}  {'─'*7}")
    lines.append(
        f"  {'Combined (all days)':<26}  {combined_tot:>5}  {combined_w:>5}  "
        f"{combined_l:>5}  {combined_hit:>6.1f}%"
    )
    if edu_na > 0:
        lines.append(f"  ({edu_na} educational pick(s) had no score data and are excluded above.)")
    lines.append("")

    # ── CLV summary ───────────────────────────────────────────────────────
    clv_available = [b for b in bets if b.get("clv") and b["clv"] != "n/a"]
    if clv_available:
        lines.append(f"  {'─'*W}")
        lines.append(f"  CLOSING LINE VALUE (CLV)  — did we beat the close?")
        lines.append(f"  {'─'*W}")
        lines.append(f"  CLV > 0 = your entry price was better than closing line (sharp side).")
        lines.append(f"  CLV < 0 = sharp money moved against you after entry.")
        lines.append(f"  Consistent positive CLV is the strongest indicator of real edge.")
        lines.append(f"  {'─'*W}")
        pos_clv = [b for b in clv_available if not b["clv"].startswith("-")]
        neg_clv = [b for b in clv_available if b["clv"].startswith("-")]
        lines.append(f"  Picks with CLV data : {len(clv_available)} of {total_bets}")
        lines.append(f"  Positive CLV        : {len(pos_clv)}")
        lines.append(f"  Negative CLV        : {len(neg_clv)}")
        lines.append(f"\n  {'DATE':<12}  {'BET':<22}  {'RESULT':<8}  {'CLV'}")
        lines.append(f"  {'─'*12}  {'─'*22}  {'─'*8}  {'─'*16}")
        for b in clv_available:
            rdisp = "✓ WIN" if b["result"]=="WIN" else ("✗ LOSS" if b["result"]=="LOSS" else b["result"])
            lines.append(f"  {b['date']:<12}  {b['bet']:<22}  {rdisp:<8}  {b['clv']}")
        lines.append("")
    else:
        lines.append(f"  {'─'*W}")
        lines.append(f"  CLV: opening odds not available in DB for this period.")
        lines.append(f"  To enable CLV tracking, ensure game_odds table contains pregame rows.")
        lines.append("")

    # ── Context note ─────────────────────────────────────────────────────
    lines.append(f"  {'─'*W}")
    lines.append(f"  NOTES")
    lines.append(f"  {'─'*W}")
    lines.append(f"  · Signal days: Top Pick = highest-priority signal on the slate.")
    lines.append(f"    Only these days are staked and included in ROI.")
    lines.append(f"  · No-signal days: best-value home underdog shown for reference.")
    lines.append(f"    Win/loss outcome shown but NOT graded and NOT staked.")
    lines.append(f"  · All odds are closing-line prices from your mlb_stats.db.")
    lines.append(f"  · ROI is after-vig (no vig removal applied).")
    lines.append(f"  · +4.5% ROI threshold needed to clear vig on flat -110 structure.")
    lines.append(f"  · Run monthly_hypothesis_grader.py for per-hypothesis breakdown.")
    lines.append(f"  {'─'*W}\n")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(
        description="MLB Betting Model — Top Pick Backtester",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            EXAMPLES
            --------
            September 2025, $10 flat bet (default):
              python backtest_top_pick.py --month 2025-09

            Full 2025 season, $25 stake:
              python backtest_top_pick.py --from 2025-04-01 --to 2025-09-28 --stake 25

            Specific signal only:
              python backtest_top_pick.py --month 2025-09 --signal S1

            Save output to file:
              python backtest_top_pick.py --month 2025-09 --output reports/bt_sep25.txt

            See what months have data:
              python backtest_top_pick.py --list-months
        """),
    )
    p.add_argument("--month",  default=None,
                   help="Month to backtest as YYYY-MM (e.g. 2025-09). "
                        "Sets --from to first of month and --to to last day.")
    p.add_argument("--from",   dest="date_from", default=None,
                   help="Start date YYYY-MM-DD (inclusive).")
    p.add_argument("--to",     dest="date_to",   default=None,
                   help="End date YYYY-MM-DD (inclusive).")
    p.add_argument("--stake",  type=float, default=10.0,
                   help="Flat bet stake per pick in dollars (default: 10).")
    p.add_argument("--late-season-stake", type=float, default=None,
                   dest="late_season_stake",
                   help="Reduced stake for Aug–Sep bets (e.g. 5.0 for half-stake). "
                        "3-year data shows Aug/Sep ROI averages -13%% vs +12%% Apr–Jun. "
                        "Omit to use flat --stake all season.")
    p.add_argument("--signal", default=None,
                   choices=["MV-F", "S1", "MV-B", "S1+H2", "H3b"],
                   help="Restrict backtest to one signal type only.")
    p.add_argument("--output", default=None,
                   help="Save the report to this file path.")
    p.add_argument("--verbose", action="store_true",
                   help="Print every game date, including no-signal days.")
    p.add_argument("--list-months", action="store_true",
                   help="List available months in DB and exit.")
    return p.parse_args()


def main():
    args = parse_args()

    # Load the signal engine
    mod = _load_brief_module()

    conn = open_db()

    if args.list_months:
        list_available_months(conn)
        conn.close()
        return

    # Resolve date range
    if args.month:
        try:
            y, m   = int(args.month[:4]), int(args.month[5:7])
            d_from = f"{y:04d}-{m:02d}-01"
            # Last day of month
            if m == 12:
                d_to = f"{y+1:04d}-01-01"
            else:
                d_to = f"{y:04d}-{m+1:02d}-01"
            d_to = (datetime.date.fromisoformat(d_to)
                    - datetime.timedelta(days=1)).isoformat()
        except (ValueError, IndexError):
            print(f"✗  Invalid --month value '{args.month}'. Use YYYY-MM.")
            sys.exit(1)
    elif args.date_from and args.date_to:
        d_from = args.date_from
        d_to   = args.date_to
    else:
        print("✗  Specify either --month YYYY-MM  or both --from and --to.")
        sys.exit(1)

    # Validate dates
    try:
        datetime.date.fromisoformat(d_from)
        datetime.date.fromisoformat(d_to)
    except ValueError as e:
        print(f"✗  Invalid date: {e}")
        sys.exit(1)

    print(f"\n{'═'*72}")
    print(f"  MLB Scout · Top Pick Backtester")
    print(f"  Range: {d_from}  →  {d_to}   Stake: ${args.stake:.2f}/day")
    if args.late_season_stake:
        print(f"  Late-season stake (Aug–Sep): ${args.late_season_stake:.2f}/day")
    if args.signal:
        print(f"  Signal filter: {args.signal}")
    print(f"{'═'*72}")
    print(f"  Loading signal engine from generate_daily_brief.py ...")
    print(f"  Running backtest ...\n")

    results = run_backtest(
        conn              = conn,
        mod               = mod,
        date_from         = d_from,
        date_to           = d_to,
        stake             = args.stake,
        signal_filter     = args.signal,
        verbose           = args.verbose,
        late_season_stake = args.late_season_stake,
    )

    report = format_report(results)
    print(report)

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as fh:
            fh.write(report)
        print(f"  ✓ Report saved to: {args.output}\n")

    conn.close()


if __name__ == "__main__":
    main()
