"""
backfill_starters.py
====================
One-time backfill of probable/actual starting pitchers for all 2026
regular-season games already in mlb_stats.db.

Uses the MLB Stats API (free, no key required).  Processes dates in
chronological order and writes to game_probable_pitchers, the same table
used by load_weather.py's refresh_probable_starters().

USAGE
-----
    # Dry run — shows what would be written, no DB changes:
    python backfill_starters.py --dry-run

    # Full backfill for 2026 season to date:
    python backfill_starters.py

    # Specific date range only:
    python backfill_starters.py --start 2026-03-25 --end 2026-04-07

    # Verbose — show every starter found:
    python backfill_starters.py --verbose

NOTES
-----
  · For completed games the API returns the actual starting pitcher
    (from the boxscore hydration), not just the probable.  This gives
    accurate data for grading purposes.
  · For unplayed games it returns the probable pitcher (same as
    load_weather.py).
  · Uses INSERT OR IGNORE so existing rows are never overwritten —
    run safely multiple times.
  · Rate-limited to 1 request per 0.5 seconds to be a good citizen.
  · Completed games use hydrate=boxscore to get actual starters.
    Unplayed games use hydrate=probablePitchers.
"""

# CHANGE LOG (latest first)
# -------------------------
# 2026-04-13 16:24 ET  Refactor: route sqlite3.connect() calls through core.db.connection.connect().

import argparse
import json
import sqlite3
import sys
import time
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

from core.db.connection import connect as db_connect

try:
    from zoneinfo import ZoneInfo as _ZI
    _ET = _ZI("America/New_York")
except Exception:
    import datetime as _dt
    _ET = _dt.timezone(datetime.timedelta(hours=-4))

MLB_API   = "https://statsapi.mlb.com/api/v1"
SPORT_ID  = 1
DEFAULT_DB = Path(__file__).parent / "mlb_stats.db"
SEASON     = 2026
RATE_LIMIT = 0.5   # seconds between API calls


def get_connection(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        print(f"✗  Database not found: {db_path}")
        sys.exit(1)
    con = db_connect(str(db_path), timeout=30)
    con.row_factory = sqlite3.Row
    return con


def ensure_tables(con: sqlite3.Connection) -> None:
    """Create game_probable_pitchers and era_season column if absent."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS game_probable_pitchers (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            game_pk     INTEGER NOT NULL,
            team_id     INTEGER NOT NULL,
            player_id   INTEGER NOT NULL,
            fetched_at  TEXT    NOT NULL,
            UNIQUE (game_pk, team_id)
        )
    """)
    try:
        con.execute("ALTER TABLE players ADD COLUMN era_season REAL")
    except Exception:
        pass   # already exists
    con.commit()


def load_game_dates(con: sqlite3.Connection,
                    start: str, end: str) -> list[str]:
    """Return sorted list of distinct game_dates in range with regular season games."""
    rows = con.execute("""
        SELECT DISTINCT game_date
        FROM   games
        WHERE  game_date >= ? AND game_date <= ?
          AND  game_type = 'R'
        ORDER  BY game_date
    """, (start, end)).fetchall()
    return [r["game_date"] for r in rows]


def fetch_starters_for_date(game_date: str,
                             verbose: bool = False) -> list[dict]:
    """
    Fetch starting pitchers for all games on game_date.

    Strategy:
      - For Final games: use hydrate=boxscore to get actual starters
        (the pitcher who threw the first pitch).
      - For non-Final games: use hydrate=probablePitchers.

    Returns list of dicts: {game_pk, team_id, player_id, full_name, source}
    """
    results = []

    # ── Pull schedule for this date ───────────────────────────────────────────
    sched_url = (f"{MLB_API}/schedule?sportId={SPORT_ID}"
                 f"&startDate={game_date}&endDate={game_date}"
                 f"&gameType=R"
                 f"&hydrate=probablePitchers")
    try:
        req = urllib.request.Request(sched_url,
                                     headers={"User-Agent": "MLB-Scout/2.5"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            sched = json.loads(resp.read())
    except Exception as e:
        print(f"  ⚠  Schedule fetch failed for {game_date}: {e}")
        return results

    time.sleep(RATE_LIMIT)

    for date_obj in sched.get("dates", []):
        for g in date_obj.get("games", []):
            game_pk    = g.get("gamePk")
            status     = g.get("status", {}).get("abstractGameState", "")
            teams_node = g.get("teams", {})

            if not game_pk:
                continue

            # ── Final games: fetch boxscore for actual starters ───────────────
            if status in ("Final", "Live"):
                box_url = f"{MLB_API}/game/{game_pk}/boxscore"
                try:
                    req2 = urllib.request.Request(
                        box_url, headers={"User-Agent": "MLB-Scout/2.5"})
                    with urllib.request.urlopen(req2, timeout=15) as resp2:
                        box = json.loads(resp2.read())
                    time.sleep(RATE_LIMIT)

                    for side in ("home", "away"):
                        team_node  = box.get("teams", {}).get(side, {})
                        team_id    = team_node.get("team", {}).get("id")
                        pitchers   = team_node.get("pitchers", [])
                        if not pitchers or not team_id:
                            continue
                        starter_id = pitchers[0]   # first pitcher = starter
                        # Look up name from players node in boxscore
                        players_node = team_node.get("players", {})
                        player_key   = f"ID{starter_id}"
                        player_data  = players_node.get(player_key, {})
                        full_name    = (player_data.get("person", {})
                                        .get("fullName", f"Player {starter_id}"))
                        results.append({
                            "game_pk":   game_pk,
                            "team_id":   team_id,
                            "player_id": starter_id,
                            "full_name": full_name,
                            "source":    "boxscore",
                        })
                        if verbose:
                            print(f"    {game_date} gm={game_pk} {side:4} "
                                  f"starter={full_name} (id={starter_id}) [boxscore]")

                except Exception as e:
                    if verbose:
                        print(f"    ⚠  Boxscore fetch failed game {game_pk}: {e}")
                    # Fall through to probable pitcher from schedule
                    _extract_probables(teams_node, game_pk, results,
                                       game_date, verbose, source="probable-fallback")

            else:
                # ── Unplayed: use probable from schedule response ──────────────
                _extract_probables(teams_node, game_pk, results,
                                   game_date, verbose, source="probable")

    return results


def _extract_probables(teams_node: dict, game_pk: int,
                        results: list, game_date: str,
                        verbose: bool, source: str) -> None:
    for side in ("home", "away"):
        prob = teams_node.get(side, {}).get("probablePitcher")
        if not prob:
            continue
        player_id = prob.get("id")
        team_id   = teams_node.get(side, {}).get("team", {}).get("id")
        full_name = prob.get("fullName", f"Player {player_id}")
        if not player_id or not team_id:
            continue
        results.append({
            "game_pk":   game_pk,
            "team_id":   team_id,
            "player_id": player_id,
            "full_name": full_name,
            "source":    source,
        })
        if verbose:
            print(f"    {game_date} gm={game_pk} {side:4} "
                  f"starter={full_name} (id={player_id}) [{source}]")


def write_starters(con: sqlite3.Connection,
                   starters: list[dict],
                   dry_run: bool) -> int:
    """Write starter rows. Uses INSERT OR IGNORE — never overwrites existing."""
    fetched_at = datetime.now(tz=_ET).strftime("%Y-%m-%d %H:%M ET")
    written = 0
    for s in starters:
        # Ensure player row exists
        if not dry_run:
            con.execute("""
                INSERT INTO players (player_id, full_name, last_name, active)
                VALUES (?, ?, ?, 1)
                ON CONFLICT(player_id) DO UPDATE SET
                    full_name = CASE
                        WHEN full_name LIKE 'Player %' THEN excluded.full_name
                        ELSE full_name
                    END
            """, (s["player_id"], s["full_name"],
                  s["full_name"].split()[-1] if s["full_name"] else ""))

            cur = con.execute("""
                INSERT OR IGNORE INTO game_probable_pitchers
                    (game_pk, team_id, player_id, fetched_at)
                VALUES (?, ?, ?, ?)
            """, (s["game_pk"], s["team_id"], s["player_id"], fetched_at))
            if cur.rowcount:
                written += 1
        else:
            written += 1   # count as would-be written in dry-run

    if not dry_run:
        con.commit()
    return written


def main():
    p = argparse.ArgumentParser(
        description="Backfill starting pitchers for 2026 season games",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  python backfill_starters.py --dry-run
      Show what would be written without touching the DB.

  python backfill_starters.py
      Backfill all 2026 regular season games to date.

  python backfill_starters.py --start 2026-03-25 --end 2026-04-01
      Backfill a specific date range only.

  python backfill_starters.py --verbose
      Show each starter found during the backfill.
""")
    p.add_argument("--start",    default=f"{SEASON}-03-01",
                   help=f"Start date (default: {SEASON}-03-01)")
    p.add_argument("--end",      default=date.today().isoformat(),
                   help="End date (default: today)")
    p.add_argument("--db",       default=str(DEFAULT_DB))
    p.add_argument("--dry-run",  action="store_true",
                   help="Print what would be written without DB changes")
    p.add_argument("--verbose", "-v", action="store_true",
                   help="Show each starter found")
    args = p.parse_args()

    con = get_connection(Path(args.db))
    ensure_tables(con)

    dates = load_game_dates(con, args.start, args.end)
    if not dates:
        print(f"  No regular season games found between {args.start} and {args.end}.")
        con.close()
        return

    print(f"\n  MLB Starter Backfill  ·  {args.start} → {args.end}")
    print(f"  {'DRY RUN — no DB writes' if args.dry_run else 'Writing to game_probable_pitchers'}")
    print(f"  Dates to process: {len(dates)}")
    print(f"  Rate limit: {RATE_LIMIT}s between API calls\n")

    total_written  = 0
    total_found    = 0
    total_dates    = 0

    for game_date in dates:
        starters = fetch_starters_for_date(game_date, args.verbose)
        total_found += len(starters)

        if starters:
            written = write_starters(con, starters, args.dry_run)
            total_written += written
            sources = {}
            for s in starters:
                sources[s["source"]] = sources.get(s["source"], 0) + 1
            src_str = "  ".join(f"{v} {k}" for k, v in sources.items())
            flag = " [DRY RUN]" if args.dry_run else ""
            print(f"  {game_date}  {len(starters):2} starters found  "
                  f"{written:2} new rows{flag}  ({src_str})")
        else:
            print(f"  {game_date}  no starters found")

        total_dates += 1

    print(f"\n  ══  SUMMARY  ══")
    print(f"  Dates processed : {total_dates}")
    print(f"  Starters found  : {total_found}")
    print(f"  Rows written    : {total_written}"
          + (" [DRY RUN — nothing written]" if args.dry_run else ""))
    print()

    con.close()


if __name__ == "__main__":
    main()
