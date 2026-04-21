"""
backfill_runline_by_year.py
───────────────────────────
Backfill MLB runline (spreads) odds by season/year using The Odds API historical endpoint.

Why this exists:
  - load_odds.py supports historical backfill, but pulls *all* game markets (h2h, spreads, totals).
  - This script is a narrow tool that pulls *only* runline/spreads data for a full season.
  - It uses the same 3 bookmakers as your current pregame pulls:
      DraftKings + FanDuel + BetMGM

Writes:
  - Inserts into game_odds with market_type='runline' (via load_odds.parse_game_markets).
  - Uses load_odds.upsert_game_odds_row so opening/closing flags are managed consistently.

Usage:
  python backfill_runline_by_year.py --season 2023
  python backfill_runline_by_year.py --season 2023 --dry-run
  python backfill_runline_by_year.py --season 2023 --start 2023-04-01 --end 2023-10-01

Requirements:
  pip install requests python-dotenv

API key:
  THE_ODDS_API_KEY in config/.env or environment.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from datetime import date as _date
from datetime import datetime, timedelta


_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

# Optional .env support (same pattern as load_odds.py)
try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(_REPO_ROOT, "config", ".env"), override=False)
    load_dotenv(os.path.join(_REPO_ROOT, ".env"), override=False)
    load_dotenv(override=False)
except ImportError:
    pass


def _iso(s: str) -> str:
    return _date.fromisoformat(s).isoformat()


def _season_date_bounds(con: sqlite3.Connection, season: int) -> tuple[str, str]:
    """
    Prefer bounds from games table (regular season only) so we don’t hit the API
    on off-days when there are no MLB games.
    """
    row = con.execute(
        """
        SELECT
            MIN(game_date_et) AS min_date,
            MAX(game_date_et) AS max_date
        FROM games
        WHERE season = ?
          AND game_type = 'R'
          AND game_date_et IS NOT NULL
        """,
        (season,),
    ).fetchone()
    if row and row[0] and row[1]:
        return _iso(row[0]), _iso(row[1])
    # Fallback: season table
    row2 = con.execute(
        "SELECT season_start, season_end FROM seasons WHERE season = ?",
        (season,),
    ).fetchone()
    if row2 and row2[0] and row2[1]:
        return _iso(row2[0]), _iso(row2[1])
    # Last resort: typical MLB window (still safe; we skip days with no matches)
    return f"{season}-03-01", f"{season}-11-30"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Backfill MLB runline odds by season (Odds API historical endpoint)."
    )
    p.add_argument("--season", type=int, required=True, help="Season year (YYYY), e.g. 2023")
    p.add_argument("--start", default=None, help="Optional start date YYYY-MM-DD (overrides season min)")
    p.add_argument("--end", default=None, help="Optional end date YYYY-MM-DD (overrides season max)")
    p.add_argument("--db", default=None, help="DB path override (defaults to core.db.connection.get_db_path())")
    p.add_argument("--dry-run", action="store_true", help="Do not write rows; just report counts.")
    p.add_argument("--verbose", "-v", action="store_true", help="Verbose API logging.")
    p.add_argument(
        "--pause",
        type=float,
        default=None,
        help="Seconds to pause between daily API requests (default: from load_odds.REQUEST_PAUSE).",
    )
    return p.parse_args()


def main() -> int:
    # Reuse the existing implementation details from load_odds.py
    from batch.ingestion import load_odds as lo

    lo.configure_logging(verbose=False)
    args = parse_args()

    # Honor caller verbosity in the shared logger.
    if args.verbose:
        lo.configure_logging(verbose=True)

    db_path = args.db or lo.DEFAULT_DB
    con = lo.get_connection(db_path)
    api_key = lo.get_api_key()

    start_default, end_default = _season_date_bounds(con, args.season)
    start_date = args.start or start_default
    end_date = args.end or end_default
    # Validate ISO dates
    start_date = _iso(start_date)
    end_date = _iso(end_date)

    pause = args.pause if args.pause is not None else lo.REQUEST_PAUSE

    lo.log.info("Mode: runline-only historical backfill (season=%s)", args.season)
    lo.log.info("Date range: %s → %s", start_date, end_date)
    lo.log.info("Bookmakers: %s", lo.BOOKMAKERS)
    lo.log.info("Market: spreads (runline)")
    if args.dry_run:
        lo.log.info("DRY RUN: no DB writes")

    total_rows = 0
    total_api = 0
    api_rem = 0
    days = 0

    current = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    url = f"{lo.API_BASE}/historical/sports/{lo.SPORT}/odds"

    while current <= end:
        date_str = current.isoformat()
        days += 1

        # Snapshot timestamp: noon ET-ish. Keep same convention as load_odds.pull_historical.
        snapshot_ts = f"{date_str}T17:00:00Z"
        captured_utc = snapshot_ts.replace("Z", "")

        # Build lookup for games on that local date (plus next day to match UTC commence_time).
        game_lookup = lo.build_game_lookup(con, date_str)

        params = {
            "apiKey": api_key,
            "markets": "spreads",
            "oddsFormat": lo.ODDS_FORMAT,
            "date": snapshot_ts,
            # Critical: limit to the same 3 books you already use.
            "bookmakers": lo.BOOKMAKERS,
        }

        events, headers = lo.api_get(url, params, verbose=args.verbose)
        quota = lo.quota_from_headers(headers or {})
        total_api += quota["requests_used"]
        api_rem = quota["requests_remaining"]

        # Odds API historical endpoint sometimes returns dict {"data":[...]}.
        if isinstance(events, dict):
            events = events.get("data", [])
        if not isinstance(events, list):
            events = []

        inserted_today = 0
        matched_games = 0

        if events:
            for event in events:
                event_date = (event.get("commence_time", "") or "")[:10] or date_str
                game_row = lo.match_event_to_game(event, event_date, game_lookup)
                if not game_row:
                    continue
                matched_games += 1
                game_pk = game_row["game_pk"]
                for bm in event.get("bookmakers", []):
                    bm["_home_team"] = event.get("home_team", "")
                    bm["_away_team"] = event.get("away_team", "")
                    for row in lo.parse_game_markets(bm, game_pk, captured_utc, None):
                        # parse_game_markets returns rows for *any* market keys present.
                        # Since we requested only spreads, this should be runline only, but keep a guard.
                        if row.get("market_type") != "runline":
                            continue
                        if args.dry_run:
                            inserted_today += 1
                        else:
                            inserted_today += lo.upsert_game_odds_row(con, row)

            if not args.dry_run:
                con.commit()

        total_rows += inserted_today
        lo.log.info(
            "  %s: matched_games=%d  inserted_runline_rows=%d  api_used=%d  remaining=%d",
            date_str,
            matched_games,
            inserted_today,
            total_api,
            api_rem,
        )

        current += timedelta(days=1)
        time.sleep(pause)

    lo.log.info(
        "Done. season=%s  days=%d  total_runline_rows=%d  api_used=%d  remaining=%d",
        args.season,
        days,
        total_rows,
        total_api,
        api_rem,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

