#!/usr/bin/env python3
"""
check_odds_ready.py — Pre-brief odds readiness check.

Run this AFTER load_odds.py and BEFORE generate_daily_brief.py.
Ensures every scheduled game for today has closing-line moneyline
odds in the DB before the brief attempts to evaluate signals.

Handles two distinct failure modes:
  1. Odds rows exist but is_closing_line = 0 (Opening Week / early pull timing)
     → Auto-promotes today's latest odds rows to is_closing_line = 1
     → Brief will then find them normally via v_closing_game_odds

  2. Odds rows genuinely missing (books haven't posted lines yet)
     → Reports which games are missing and exits with a clear message
     → Re-run load_odds.py and try again in 30–60 minutes

USAGE
─────
    python check_odds_ready.py                    # check today
    python check_odds_ready.py --date 2026-03-27  # check specific date
    python check_odds_ready.py --fix              # auto-fix closing flag if possible
    python check_odds_ready.py --warn             # warn on missing, don't exit
    python check_odds_ready.py --preview          # show signal preview after odds check
    python check_odds_ready.py --fix --preview    # fix + preview (recommended pre-brief)

EXIT CODES
──────────
    0  All games have closing-line odds — safe to run brief
    1  Some games missing odds, not auto-fixable — re-run load_odds.py
    2  DB not found or query error
"""

# CHANGE LOG (latest first)
# -------------------------
# 2026-04-13 16:24 ET  Refactor: route sqlite3.connect() calls through core.db.connection.connect().

import argparse
import os
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone

from core.db.connection import connect as db_connect

DEFAULT_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mlb_stats.db")


def get_connection(db_path: str) -> sqlite3.Connection:
    if not os.path.exists(db_path):
        print(f"\n  ✗  Database not found: {db_path}")
        print("     Run from your mlb_stats folder.")
        sys.exit(2)
    con = db_connect(db_path)
    con.row_factory = sqlite3.Row
    return con


def _et_time(utc_str) -> str:
    """Return game start in ET (e.g. '7:10 PM ET'), or '' on failure."""
    if not utc_str:
        return ""
    try:
        dt = datetime.fromisoformat(utc_str.rstrip("Z")).replace(tzinfo=timezone.utc)
        et = dt + timedelta(hours=-4)   # EDT (UTC-4) — correct for MLB season
        # Use %I (zero-padded) then strip leading zero — works on Windows and Linux
        return et.strftime("%I:%M %p ET").lstrip("0")
    except Exception:
        return ""


def check_odds_ready(game_date: str, auto_fix: bool, warn_only: bool, db_path: str = None) -> int:
    """
    Main check. Returns 0 if all games have odds, 1 if some are missing.
    """
    con = get_connection(db_path or DEFAULT_DB)

    # ── Step 1: get all scheduled regular-season games for today ─────────
    games = con.execute("""
        SELECT
            g.game_pk,
            ta.abbreviation || '@' || th.abbreviation AS matchup,
            g.game_start_utc,
            g.status
        FROM games g
        JOIN teams th ON th.team_id = g.home_team_id
        JOIN teams ta ON ta.team_id = g.away_team_id
        WHERE g.game_date = ?
          AND g.game_type = 'R'
          AND g.status NOT IN ('Final', 'Cancelled', 'Postponed')
        ORDER BY g.game_start_utc
    """, (game_date,)).fetchall()

    if not games:
        print(f"\n  ⚠  No scheduled games found for {game_date}.")
        print("     Run: python load_mlb_stats.py (or load_today.py) first.")
        return 1

    print(f"\n  Checking odds readiness for {game_date} ({len(games)} games)...")

    # ── Step 2: check which games have closing-line ML odds ───────────────
    closing_pks = set(
        r[0] for r in con.execute("""
            SELECT DISTINCT go.game_pk
            FROM game_odds go
            WHERE go.game_pk IN ({})
              AND go.market_type = 'moneyline'
              AND go.is_closing_line = 1
              AND go.home_ml IS NOT NULL
        """.format(",".join("?" * len(games))),
        [g["game_pk"] for g in games]
        ).fetchall()
    )

    # ── Step 3: for games without closing lines, check if non-closing ─────
    #            rows exist (is_closing_line = 0) — fixable situation
    missing_pks = [g["game_pk"] for g in games if g["game_pk"] not in closing_pks]

    fixable = {}    # {game_pk: max captured_at_utc} — has rows, wrong flag
    no_rows  = []   # game_pks with zero odds rows at all

    if missing_pks:
        for pk in missing_pks:
            row = con.execute("""
                SELECT COUNT(*) AS cnt, MAX(captured_at_utc) AS latest
                FROM game_odds
                WHERE game_pk = ?
                  AND market_type = 'moneyline'
                  AND home_ml IS NOT NULL
            """, (pk,)).fetchone()

            if row and row["cnt"] > 0:
                fixable[pk] = row["latest"]
            else:
                no_rows.append(pk)

    # ── Step 4: report ────────────────────────────────────────────────────
    all_ok = len(missing_pks) == 0

    # Games with closing odds — always show
    ok_games = [g for g in games if g["game_pk"] in closing_pks]
    for g in ok_games:
        t = _et_time(g["game_start_utc"])
        print(f"  ✓  {g['matchup']:<14}  {t:<13}  closing ML odds present")

    # Fixable games (rows exist, wrong flag)
    for g in games:
        if g["game_pk"] in fixable:
            t = _et_time(g["game_start_utc"])
            latest = fixable[g["game_pk"]]
            print(f"  ~  {g['matchup']:<14}  {t:<13}  odds rows exist (is_closing_line=0, latest: {latest})")

    # Genuinely missing
    for g in games:
        if g["game_pk"] in no_rows:
            t = _et_time(g["game_start_utc"])
            print(f"  ✗  {g['matchup']:<14}  {t:<13}  NO ODDS IN DB — books may not have posted yet")

    print()

    # ── Step 5: auto-fix if requested and possible ────────────────────────
    if fixable and auto_fix:
        print(f"  ── Auto-fixing {len(fixable)} game(s) with is_closing_line=0 ──")

        for pk, latest_ts in fixable.items():
            # Promote the most recent snapshot to closing line
            # Use the latest captured_at_utc as the "closing" row
            updated = con.execute("""
                UPDATE game_odds
                SET is_closing_line = 1
                WHERE game_pk = ?
                  AND market_type IN ('moneyline', 'total', 'runline')
                  AND captured_at_utc = (
                      SELECT MAX(captured_at_utc)
                      FROM game_odds
                      WHERE game_pk = ?
                        AND market_type = 'moneyline'
                        AND home_ml IS NOT NULL
                  )
                  AND home_ml IS NOT NULL
            """, (pk, pk)).rowcount
            con.commit()

            matchup = next(g["matchup"] for g in games if g["game_pk"] == pk)
            print(f"  ✓  {matchup:<14}  promoted {updated} row(s) to is_closing_line=1")

        # Re-check after fix
        closing_pks_after = set(
            r[0] for r in con.execute("""
                SELECT DISTINCT go.game_pk
                FROM game_odds go
                WHERE go.game_pk IN ({})
                  AND go.market_type = 'moneyline'
                  AND go.is_closing_line = 1
                  AND go.home_ml IS NOT NULL
            """.format(",".join("?" * len(games))),
            [g["game_pk"] for g in games]
            ).fetchall()
        )

        still_missing = [
            g["matchup"] for g in games
            if g["game_pk"] not in closing_pks_after
        ]

        if still_missing:
            print(f"\n  ⚠  Still missing after fix: {', '.join(still_missing)}")
        else:
            print(f"\n  ✓  All games now have closing-line odds. Safe to run brief.\n")
            con.close()
            return 0

    # ── Step 6: final verdict ─────────────────────────────────────────────
    con.close()

    if no_rows:
        games_missing = [g["matchup"] for g in games if g["game_pk"] in no_rows]
        print(f"  ✗  {len(no_rows)} game(s) have no odds: {', '.join(games_missing)}")
        print()
        print("  Options:")
        print("    1. Re-run load_odds.py and wait 30–60 min for books to post lines")
        print("    2. Run generate_daily_brief.py --warn-missing to proceed anyway")
        print("       (those games will show N/A odds and skip signal evaluation)")
        print()

        if warn_only:
            return 0   # caller asked for warn-not-exit
        return 1

    if fixable and not auto_fix:
        games_fixable = [g["matchup"] for g in games if g["game_pk"] in fixable]
        print(f"  ~  {len(fixable)} game(s) have odds but is_closing_line=0:")
        print(f"     {', '.join(games_fixable)}")
        print()
        print("  Fix options:")
        print("    A. Re-run:  python load_odds.py --pregame --markets game")
        print("       (subsequent pull will set is_closing_line=1 automatically)")
        print()
        print("    B. Auto-fix now:")
        print("       python check_odds_ready.py --fix")
        print()
        print("    C. Proceed anyway:")
        print("       python generate_daily_brief.py --warn-missing")
        print()

        if warn_only:
            return 0
        return 1

    if all_ok:
        print(f"  ✓  All {len(games)} games have closing-line odds. Safe to run brief.\n")
        return 0

    return 1


def _american_to_implied(ml):
    """Convert American moneyline to implied probability."""
    if ml is None:
        return None
    ml = float(ml)
    return abs(ml) / (abs(ml) + 100) if ml < 0 else 100 / (ml + 100)


def show_signal_preview(con: sqlite3.Connection, game_date: str) -> None:
    """
    Show a pre-brief signal context table for games that have not yet started.
    Displays: matchup, start time ET, wind, ML odds, total, streak info,
    and a quick signal eligibility note.

    Run before any brief session to sanity-check the data pipeline:
      - Wind populated? (source: forecast vs actual)
      - Odds loaded?
      - Any obvious signal candidates?
    """
    now_utc = datetime.now(timezone.utc)

    rows = con.execute("""
        SELECT
            ta.abbreviation || '@' || th.abbreviation AS matchup,
            g.game_start_utc,
            g.status,
            g.wind_mph,
            g.wind_direction,
            g.wind_source,
            g.temp_f,
            v.wind_effect,
            v.park_factor_runs,
            COALESCE(v.name, '') AS venue_name,
            go_ml.home_ml,
            go_ml.away_ml,
            go_tot.total_line,
            -- Home streak from standings or streaks table if available
            NULL AS home_streak
        FROM games g
        JOIN teams th ON th.team_id = g.home_team_id
        JOIN teams ta ON ta.team_id = g.away_team_id
        LEFT JOIN venues v ON v.venue_id = g.venue_id
        LEFT JOIN v_closing_game_odds go_ml
            ON go_ml.game_pk = g.game_pk AND go_ml.market_type = 'moneyline'
        LEFT JOIN v_closing_game_odds go_tot
            ON go_tot.game_pk = g.game_pk AND go_tot.market_type = 'total'
        WHERE g.game_date = ?
          AND g.game_type = 'R'
          AND g.status NOT IN ('Final', 'Cancelled', 'Postponed')
        ORDER BY g.game_start_utc
    """, (game_date,)).fetchall()

    if not rows:
        print("  No games to preview.")
        return

    print()
    print("  ─────────────────────────────────────────────────────────────────────")
    print("  📋  SIGNAL PREVIEW  —  Pre-brief data check")
    print("  ─────────────────────────────────────────────────────────────────────")

    in_game_warned = False

    for r in rows:
        # Parse game start
        try:
            start_utc = datetime.fromisoformat(
                r["game_start_utc"].rstrip("Z")
            ).replace(tzinfo=timezone.utc)
            # ET = UTC-4 (EDT) or UTC-5 (EST) — use UTC-4 for season
            from datetime import timedelta
            et_offset = timedelta(hours=-4)
            start_et  = start_utc + et_offset
            start_str = start_et.strftime("%I:%M %p ET").lstrip("0") if hasattr(start_et, "strftime") else ""
            started   = now_utc >= start_utc
        except Exception:
            start_str = ""
            started   = False

        if started and not in_game_warned:
            print()
            print("  ⚠  WARNING: Some games below have already started.")
            print("     Odds flagged as closing may be IN-GAME lines, not pre-game prices.")
            print("     Do NOT run load_odds.py --pregame for games already in progress.")
            print()
            in_game_warned = True

        # Wind status
        wind_mph = r["wind_mph"]
        wind_dir = r["wind_direction"] or ""
        wind_src = r["wind_source"] or ""
        effect   = (r["wind_effect"] or "HIGH").upper()
        temp     = r["temp_f"]

        if wind_mph is not None:
            wind_str = f"{wind_mph:.0f} mph {wind_dir}"
            if wind_src == "forecast":
                wind_str += " (fcst)"
            elif wind_src == "actual":
                wind_str += " (actual)"
        else:
            wind_str = "no wind data ⚠"

        if temp is not None:
            wind_str = f"{temp:.0f}°F  {wind_str}"

        if effect == "SUPPRESSED":
            wind_str = "SUPPRESSED venue"

        # Odds
        hml  = r["home_ml"]
        aml  = r["away_ml"]
        tot  = r["total_line"]
        himp = _american_to_implied(hml)

        if hml is not None:
            odds_str = (f"H {hml:+d} / A {aml:+d}" if aml is not None
                       else f"H {hml:+d}")
        else:
            odds_str = "no odds ⚠"

        tot_str = f"O/U {tot}" if tot else "no total ⚠"

        # Quick signal eligibility check (informational only)
        signals = []
        if effect != "SUPPRESSED" and wind_mph is not None:
            is_in  = wind_dir and "IN" in wind_dir.upper() and "R To" not in wind_dir and "L To" not in wind_dir
            is_out = wind_dir and "OUT" in wind_dir.upper()
            pf     = r["park_factor_runs"] or 100
            venue  = r["venue_name"]

            # H3b: wind OUT >= 10, PF >= 98 (whitelist check omitted here — just flag)
            if is_out and wind_mph >= 10 and pf >= 98:
                signals.append("H3b?")
            # MV-F: wind IN >= 10, home fav in -130/-160 range
            if is_in and wind_mph >= 10 and hml is not None and -160 <= hml <= -130:
                signals.append("MV-F?")
            # MV-B: wind OUT >= 15, home dog 35-42% implied
            if is_out and wind_mph >= 15 and himp is not None and 0.35 <= himp <= 0.42:
                signals.append("MV-B?")
            # S1/S1+H2 can't be checked without streak data — omit here

        sig_str = "  ⚑ " + "+".join(signals) if signals else ""
        started_flag = "  [IN PROGRESS]" if started else ""

        print(f"  {r['matchup']:<10}  {start_str:<13}  {wind_str:<28}  "
              f"{odds_str:<20}  {tot_str:<10}{sig_str}{started_flag}")

    print("  ─────────────────────────────────────────────────────────────────────")
    print("  ⚑ = potential signal candidate (verify in full brief)")
    print("  (fcst) = Open-Meteo forecast  |  (actual) = post-game MLB Stats API")
    print()

    # ── Starter check ────────────────────────────────────────────────────────
    print("  ─────────────────────────────────────────────────────────────────────")
    print("  🔰  STARTING PITCHERS")
    print("  ─────────────────────────────────────────────────────────────────────")

    try:
        starter_rows = con.execute("""
            SELECT
                ta.abbreviation || '@' || th.abbreviation AS matchup,
                g.game_start_utc,
                ph.full_name   AS home_name,
                ph.era_season  AS home_era,
                pa.full_name   AS away_name,
                pa.era_season  AS away_era
            FROM games g
            JOIN teams th ON th.team_id = g.home_team_id
            JOIN teams ta ON ta.team_id = g.away_team_id
            LEFT JOIN game_probable_pitchers gp_h
                ON gp_h.game_pk = g.game_pk AND gp_h.team_id = g.home_team_id
            LEFT JOIN game_probable_pitchers gp_a
                ON gp_a.game_pk = g.game_pk AND gp_a.team_id = g.away_team_id
            LEFT JOIN players ph ON ph.player_id = gp_h.player_id
            LEFT JOIN players pa ON pa.player_id = gp_a.player_id
            WHERE g.game_date = ?
              AND g.game_type = 'R'
              AND g.status NOT IN ('Final', 'Cancelled', 'Postponed')
            ORDER BY g.game_start_utc
        """, (game_date,)).fetchall()

        any_confirmed = False
        for r in starter_rows:
            home_sp = (f"{r['home_name']} (ERA {r['home_era']:.2f})"
                       if r['home_name'] and r['home_era'] is not None
                       else (r['home_name'] or "TBD"))
            away_sp = (f"{r['away_name']} (ERA {r['away_era']:.2f})"
                       if r['away_name'] and r['away_era'] is not None
                       else (r['away_name'] or "TBD"))
            status = "✓" if r['home_name'] and r['away_name'] else "~"
            if r['home_name'] or r['away_name']:
                any_confirmed = True
            # Parse ET start time
            try:
                from datetime import timedelta
                su = datetime.fromisoformat(
                    r["game_start_utc"].rstrip("Z")).replace(tzinfo=timezone.utc)
                et_str = (su + timedelta(hours=-4)).strftime("%I:%M %p ET").lstrip("0")
            except Exception:
                et_str = ""
            print(f"  {status}  {r['matchup']:<10}  {et_str:<13}"
                  f"  H: {home_sp:<35}  A: {away_sp}")

        if not any_confirmed:
            print("  ⚠  No starters confirmed for any game.")
            print("     Run: python load_weather.py  (refreshes starters from MLB API)")
            print("     Or run: python load_mlb_stats.py if load_weather.py not yet deployed")
        else:
            tbd = sum(1 for r in starter_rows
                      if not r['home_name'] or not r['away_name'])
            confirmed = len(starter_rows) - tbd
            print(f"  {confirmed} game(s) fully confirmed  |  "
                  f"{tbd} game(s) TBD (check back closer to game time)")

    except Exception as e:
        print(f"  ⚠  Starter data unavailable: {e}")
        print("     game_probable_pitchers table may not exist yet.")
        print("     Deploy updated load_mlb_stats.py and run it to create the table.")

    print("  ─────────────────────────────────────────────────────────────────────")
    print()


def main():
    p = argparse.ArgumentParser(description="Pre-brief odds readiness check")
    p.add_argument("--date",  default=date.today().isoformat(),
                   help="Game date to check (default: today)")
    p.add_argument("--fix",   action="store_true",
                   help="Auto-promote latest odds rows to is_closing_line=1 if possible")
    p.add_argument("--warn",  action="store_true",
                   help="Warn on missing odds but exit 0 (don't block the brief)")
    p.add_argument("--preview", action="store_true",
                   help="Show signal preview table after odds check (recommended pre-brief)")
    p.add_argument("--db", default=DEFAULT_DB,
                   help=f"Database path (default: {DEFAULT_DB})")
    args = p.parse_args()

    rc = check_odds_ready(args.date, args.fix, args.warn, args.db)

    # Show signal preview if requested (or if odds check passed)
    if args.preview or rc == 0:
        con = get_connection(args.db or DEFAULT_DB)
        show_signal_preview(con, args.date)
        con.close()

    sys.exit(rc)


if __name__ == "__main__":
    main()
