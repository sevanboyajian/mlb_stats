"""
load_today.py
=============
Loads today's scheduled games into mlb_stats.db.

Run at 6:05 AM daily, immediately after load_mlb_stats.py.
Registers today's game_pks with correct game_type and status
so the brief can find them.

USAGE
-----
    python load_today.py

    # Dry-run (no DB writes):
    python load_today.py --dry-run

    # Verbose output:
    python load_today.py --verbose

    # Override date (for recovery):
    python load_today.py --date 2026-03-27

No API key or quota required. Reads from the MLB Stats API (free).
Takes ~5-15 seconds.
"""

# CHANGE LOG (latest first)
# -------------------------
# 2026-04-13 22:15 ET  Default DB from get_db_path(); repo root on sys.path for core.* imports.
# 2026-04-13 16:24 ET  Refactor: route sqlite3.connect() calls through core.db.connection.connect().

import argparse
import os
import sqlite3
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from core.db.connection import connect as db_connect, get_db_path


def main():
    # ── Console encoding guard (Windows cp1252) ───────────────────────────
    # Prevent crashes when printing unicode glyphs (✓, box drawing, arrows).
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    parser = argparse.ArgumentParser(
        description="Load today's scheduled games into mlb_stats.db"
    )
    parser.add_argument(
        "--date", default=None,
        help="Target date YYYY-MM-DD (default: today)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Pass --dry-run to load_mlb_stats.py (no DB writes)"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Pass --verbose to load_mlb_stats.py"
    )
    args = parser.parse_args()

    target = args.date or date.today().isoformat()

    # Validate date format
    try:
        target_dt = date.fromisoformat(target)
    except ValueError:
        print(f"✗  Invalid date: '{target}'. Use YYYY-MM-DD format.")
        sys.exit(1)

    # Query target date AND the next calendar day. The MLB Stats API schedules
    # endpoint uses officialDate (local US time), so most games appear under
    # target. However, late West Coast games (HOU@SEA 9:40 PM PT, TEX@LAD
    # 10:10 PM PT) that cross UTC midnight are sometimes filed by the API under
    # the next UTC date. Querying endDate=target+1 ensures we catch all of them.
    # load_mlb_stats.py stores each game under its officialDate regardless of
    # what end date we pass, so this never double-loads any game.
    from datetime import timedelta
    end_date = (target_dt + timedelta(days=1)).isoformat()

    # Use repo-root-relative path (avoid "same directory" assumptions).
    script = Path(_REPO_ROOT) / "batch" / "ingestion" / "load_mlb_stats.py"
    if not script.exists():
        print(f"✗  load_mlb_stats.py not found: {script}")
        sys.exit(1)

    cmd = [
        sys.executable, str(script),
        "--start", target,
        "--end",   end_date,  # +1 day catches late West Coast UTC-midnight crossovers
        "--no-pbp",           # skip play-by-play — irrelevant for unplayed games
        "--load-players",     # always refresh rosters (trades, call-ups, DL moves)
    ]
    if args.dry_run:
        cmd.append("--dry-run")
    if args.verbose:
        cmd.append("--verbose")

    print(f"\n  Loading schedule for {target} (querying through {end_date}) ...")
    result = subprocess.run(cmd, capture_output=True, text=True)

    # Pass through any output from load_mlb_stats.py
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)

    if result.returncode != 0:
        print(f"\n  ✗  load_mlb_stats.py exited with code {result.returncode}")
        sys.exit(result.returncode)

    # ── Query DB and display game summary ─────────────────────────────────
    db_path = Path(get_db_path())
    if not db_path.exists():
        print(f"  ✗  Database not found — cannot show game summary: {db_path}")
        sys.exit(1)

    con = db_connect(str(db_path))
    con.row_factory = sqlite3.Row

    games = con.execute("""
        SELECT
            g.game_pk,
            g.game_date_et AS game_date,
            g.game_start_utc,
            g.status,
            ta.abbreviation AS away_abbr,
            ta.name         AS away_name,
            th.abbreviation AS home_abbr,
            th.name         AS home_name,
            v.name          AS venue_name
        FROM   games g
        JOIN   teams th ON th.team_id = g.home_team_id
        JOIN   teams ta ON ta.team_id = g.away_team_id
        LEFT JOIN venues v ON v.venue_id = g.venue_id
        WHERE  g.game_date_et = ?
          AND  g.game_type = 'R'
        ORDER  BY g.game_start_utc
    """, (target,)).fetchall()

    con.close()

    if not games:
        print(f"\n  ⚠  No regular-season games found in DB for {target}.")
        print("     Check that load_mlb_stats.py ran successfully.")
        sys.exit(1)

    # Determine ET offset — EDT (UTC-4) during MLB season
    et_offset = timedelta(hours=-4)

    print()
    print(f"  ✓  {len(games)} game(s) loaded for {target}")
    print()
    print(f"  {'#':<3}  {'AWAY':<22}  {'HOME':<22}  {'START (UTC)':<20}  {'START (ET)':<13}  {'STATUS'}")
    print("  " + "─" * 98)

    for i, g in enumerate(games, 1):
        # Parse UTC start time
        utc_str = g["game_start_utc"] or ""
        if utc_str:
            try:
                dt_utc = datetime.fromisoformat(utc_str.rstrip("Z")).replace(tzinfo=timezone.utc)
                dt_et  = dt_utc + et_offset
                utc_display = dt_utc.strftime("%Y-%m-%d %H:%M")
                et_display  = dt_et.strftime("%I:%M %p ET").lstrip("0")
            except Exception:
                utc_display = utc_str[:16]
                et_display  = "?"
        else:
            utc_display = "TBD"
            et_display  = "TBD"

        away = f"{g['away_abbr']} ({g['away_name'].split()[-1]})"
        home = f"{g['home_abbr']} ({g['home_name'].split()[-1]})"

        print(f"  {i:<3}  {away:<22}  {home:<22}  {utc_display:<20}  {et_display:<13}  {g['status']}")

    print("  " + "─" * 98)
    print(f"  {len(games)} game(s)  ·  {len(games) * 2} team rows written  ·  game_date = {target}")
    print()


if __name__ == "__main__":
    main()
