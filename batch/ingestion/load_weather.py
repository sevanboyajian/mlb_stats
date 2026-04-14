"""
load_weather.py
───────────────
Loads forecast wind conditions for today's games from Open-Meteo
(https://open-meteo.com — free, no API key, no registration required).

Writes forecast wind_mph, wind_direction, and temp_f into the games
table for every scheduled game that hasn't started yet.  The brief
then has real wind data when it runs at 9:30 AM (morning) and 5:30 PM
(primary), enabling MV-F, MV-B, and H3b signal evaluation on live games.

Wind direction is converted from meteorological degrees → MLB-style
string (IN / OUT / L To R / R To L / CALM) using each venue's
cf_direction column, which records the compass bearing from home plate
toward center field.  Wind blowing FROM cf_direction → "IN".
Wind blowing TOWARD cf_direction → "OUT".  Perpendicular → "L To R"
or "R To L".  Below 3 mph → "CALM".

DATA SOURCE
───────────
Open-Meteo free tier:
  · No API key or registration
  · Hourly wind speed (mph) + direction (degrees) + gusts
  · 7-day forecast horizon
  · Updates every hour
  · US and global coverage
  · Non-commercial personal use: unlimited requests

USAGE
─────
    python load_weather.py                     # today's games
    python load_weather.py --date 2026-04-15   # specific date
    python load_weather.py --dry-run           # print without writing
    python load_weather.py --verbose           # show per-venue details
    python load_weather.py --no-starters       # skip starter refresh

DAILY SCHEDULE
──────────────
Run at 8:45 AM, 12:00 PM, and 5:00 PM (alongside odds pulls).
Each run refreshes both wind forecasts AND probable starters.
Re-running is always safe and free — Open-Meteo has no rate limits.

WIND SOURCE FLAG
────────────────
games.wind_source = 'forecast'  written by this script
games.wind_source = 'actual'    written by load_mlb_stats.py after
                                game is Final

The brief shows "(forecast)" next to wind conditions so you know the
source.  Actual post-game wind from MLB Stats API overwrites forecasts
once games complete (load_mlb_stats.py runs at 6 AM next morning).
"""

# CHANGE LOG (latest first)
# -------------------------
# 2026-04-13 22:15 ET  Default DB from get_db_path(); repo root on sys.path for core.* imports.
# 2026-04-13 16:24 ET  Refactor: route sqlite3.connect() calls through core.db.connection.connect().

import argparse
import math
import os
import sqlite3
import sys
import time
import urllib.request
import json
from datetime import date, datetime, timezone, timedelta
from typing import Optional

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from core.db.connection import connect as db_connect, get_db_path

# ── Optional .env support ─────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    # Load in a stable order; avoids relying on "current working directory".
    load_dotenv(os.path.join(_REPO_ROOT, "config", ".env"), override=False)
    load_dotenv(os.path.join(_REPO_ROOT, ".env"), override=False)
    load_dotenv(override=False)  # fallback: cwd / parent-chain
except ImportError:
    pass

DEFAULT_DB = get_db_path()

# ── Pause between API calls (polite rate limiting) ────────────────────────────
REQUEST_PAUSE = 0.3   # seconds

# ── Wind classification thresholds ───────────────────────────────────────────
CALM_MPH       = 3    # below this → CALM regardless of direction
IN_OUT_ARC     = 45   # degrees either side of direct in/out → IN or OUT
CROSS_ARC      = 45   # remainder → L To R or R To L

# ── MLB Stats API (for probable pitcher refresh) ────────────────────────────
MLB_API = "https://statsapi.mlb.com/api/v1"
SPORT_ID = 1


def _write_starter(con, game_pk, team_id, player_id, full_name, fetched_at):
    """Write one starter row to players + game_probable_pitchers."""
    con.execute("""
        INSERT INTO players (player_id, full_name, last_name, active)
        VALUES (?, ?, ?, 1)
        ON CONFLICT(player_id) DO UPDATE SET
            full_name = CASE
                WHEN full_name LIKE 'Player %' THEN excluded.full_name
                ELSE full_name
            END
    """, (player_id, full_name, full_name.split()[-1] if full_name else ""))
    con.execute("""
        INSERT INTO game_probable_pitchers
            (game_pk, team_id, player_id, fetched_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(game_pk, team_id) DO UPDATE SET
            player_id  = excluded.player_id,
            fetched_at = excluded.fetched_at
    """, (game_pk, team_id, player_id, fetched_at))


def _fetch_starter_from_boxscore(game_pk: int,
                                  verbose: bool = False) -> list:
    """Fetch actual starters from a Live or Final game's boxscore.
    Returns list of (team_id, player_id, full_name) tuples.
    pitchers[0] is always the actual starter.
    """
    url = f"{MLB_API}/game/{game_pk}/boxscore"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "MLB-Scout/2.5"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            box = json.loads(resp.read())
    except Exception as e:
        if verbose:
            print(f"    ⚠  Boxscore fetch failed game {game_pk}: {e}")
        return []

    results = []
    for side in ("home", "away"):
        team_node  = box.get("teams", {}).get(side, {})
        team_id    = team_node.get("team", {}).get("id")
        pitchers   = team_node.get("pitchers", [])
        if not pitchers or not team_id:
            continue
        starter_id   = pitchers[0]
        players_node = team_node.get("players", {})
        player_data  = players_node.get(f"ID{starter_id}", {})
        full_name    = (player_data.get("person", {})
                        .get("fullName", f"Player {starter_id}"))
        results.append((team_id, starter_id, full_name))
        if verbose:
            print(f"    boxscore: game {game_pk} {side} → "
                  f"{full_name} (id={starter_id})")
    return results


def refresh_probable_starters(con: sqlite3.Connection,
                              game_date: str,
                              verbose: bool = False) -> int:
    """Fetch probable/actual starting pitchers from the MLB Stats API.

    Three-pass strategy (proven by check_mlb_dates.py diagnostics):

    Pass 1 — Bulk schedule scan (startDate=today, endDate=today+2):
      Identifies all game_pks in the window and their current status.
      Bulk probablePitchers hydration is UNRELIABLE — returns TBD even
      when MLB has filed the probable. Used only to build the game list
      and route each game to the right pass.

    Pass 2 — Per-game endpoint for Scheduled/Preview games:
      GET /schedule?gamePk=X&hydrate=probablePitcher(note)
      This is what MLB.com uses. Returns accurate starters 2+ days ahead.
      Rate-limited to 0.2s between calls.

    Pass 3 — Boxscore for Live/Final games:
      GET /game/X/boxscore  →  pitchers[0] is the actual starter.
      Only used when a game has already started and no probable was filed.

    Uses INSERT OR REPLACE so later runs overwrite earlier values.
    Returns number of starter rows written.
    """
    # ── Ensure table exists ───────────────────────────────────────────
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
        con.commit()
    except Exception:
        pass

    from datetime import date as _date, timedelta as _td
    import time as _time

    try:
        end_date = (_date.fromisoformat(game_date) + _td(days=2)).isoformat()
    except ValueError:
        end_date = game_date

    fetched_at = datetime.now(_ET).strftime("%Y-%m-%d %H:%M ET")
    written    = 0

    # ── Pass 1: Bulk scan — build game list ───────────────────────────
    bulk_url = (f"{MLB_API}/schedule?sportId={SPORT_ID}"
                f"&startDate={game_date}&endDate={end_date}"
                f"&gameType=R&hydrate=probablePitchers")
    try:
        req = urllib.request.Request(bulk_url, headers={"User-Agent": "MLB-Scout/2.5"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            bulk_data = json.loads(resp.read())
    except Exception as e:
        print(f"  ⚠  Starter refresh failed (bulk API error): {e}")
        return 0

    per_game_queue = []   # (game_pk, home_team_id, away_team_id) for scheduled games
    boxscore_queue = []   # game_pk for Live/Final with no probable

    for date_obj in bulk_data.get("dates", []):
        for g in date_obj.get("games", []):
            game_pk = g.get("gamePk")
            if not game_pk:
                continue
            status   = g.get("status", {}).get("abstractGameState", "")
            teams    = g.get("teams", {})
            home_id  = teams.get("home", {}).get("team", {}).get("id")
            away_id  = teams.get("away", {}).get("team", {}).get("id")

            # Check if bulk hydration happened to populate probables
            has_probable = any(
                teams.get(side, {}).get("probablePitcher", {}).get("id")
                for side in ("home", "away")
            )

            if has_probable:
                # Lucky — bulk returned data, write it directly
                for side in ("home", "away"):
                    prob      = teams.get(side, {}).get("probablePitcher", {})
                    player_id = prob.get("id")
                    team_id   = teams.get(side, {}).get("team", {}).get("id")
                    full_name = prob.get("fullName", f"Player {player_id}")
                    if not player_id or not team_id:
                        continue
                    _write_starter(con, game_pk, team_id, player_id, full_name, fetched_at)
                    written += 1
                    if verbose:
                        print(f"    bulk: {game_pk} {side} → {full_name}")
            elif status in ("Live", "Final"):
                boxscore_queue.append(game_pk)
            else:
                # Scheduled/Preview — must use per-game endpoint
                per_game_queue.append((game_pk, home_id, away_id))

    con.commit()

    # ── Pass 2: Per-game endpoint for all Scheduled/Preview games ─────
    # probablePitcher(note) (singular) is what MLB.com uses and reliably
    # returns starters 2+ days ahead when the bulk endpoint returns TBD.
    for (game_pk, home_id, away_id) in per_game_queue:
        per_url = (f"{MLB_API}/schedule?sportId={SPORT_ID}"
                   f"&gamePk={game_pk}&hydrate=probablePitcher(note)")
        try:
            req2 = urllib.request.Request(per_url, headers={"User-Agent": "MLB-Scout/2.5"})
            with urllib.request.urlopen(req2, timeout=15) as resp2:
                per_data = json.loads(resp2.read())
            _time.sleep(0.2)
        except Exception as e:
            if verbose:
                print(f"    ⚠  Per-game fetch failed {game_pk}: {e}")
            continue

        for pd in per_data.get("dates", []):
            for pg in pd.get("games", []):
                pt = pg.get("teams", {})
                for side, tid in [("home", home_id), ("away", away_id)]:
                    prob      = pt.get(side, {}).get("probablePitcher", {})
                    player_id = prob.get("id")
                    team_id   = pt.get(side, {}).get("team", {}).get("id") or tid
                    full_name = prob.get("fullName", f"Player {player_id}")
                    if not player_id or not team_id:
                        continue
                    _write_starter(con, game_pk, team_id, player_id, full_name, fetched_at)
                    written += 1
                    if verbose:
                        print(f"    per-game: {game_pk} {side} → {full_name}")

    con.commit()

    # ── Pass 3: Boxscore for Live/Final with no probable filed ────────
    for game_pk in boxscore_queue:
        starters = _fetch_starter_from_boxscore(game_pk, verbose)
        _time.sleep(0.3)
        for (team_id, player_id, full_name) in starters:
            _write_starter(con, game_pk, team_id, player_id, full_name, fetched_at)
            written += 1

    con.commit()
    return written


# ── Venue coordinate + orientation table ─────────────────────────────────────
# cf_direction: compass bearing FROM home plate TOWARD center field
# wind blowing FROM that direction = IN;  blowing TOWARD it = OUT
# Matches venues.cf_direction column in DB (populated by add_stadium_data.py)

CF_BEARING = {
    "N":   0,   "NNE":  22,  "NE":  45,  "ENE":  67,
    "E":  90,   "ESE": 112,  "SE": 135,  "SSE": 157,
    "S":  180,  "SSW": 202,  "SW": 225,  "WSW": 247,
    "W":  270,  "WNW": 292,  "NW": 315,  "NNW": 337,
}


# ── ET helper ─────────────────────────────────────────────────────────────────
# Fixed UTC-4 offset (Eastern Daylight Time, correct for MLB season Apr-Oct).
# Avoids zoneinfo/tzdata dependency which requires a separate install on Windows.
_ET = timezone(timedelta(hours=-4))


def _now_et():
    return datetime.now(tz=_ET)


# ── DB ────────────────────────────────────────────────────────────────────────

def get_connection(db_path: str) -> sqlite3.Connection:
    if not os.path.exists(db_path):
        print(f"\n  ✗  Database not found: {db_path}")
        sys.exit(2)
    con = db_connect(db_path, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode = WAL")
    return con


def ensure_wind_source_column(con: sqlite3.Connection) -> None:
    """Add wind_source column to games table if it doesn't exist."""
    cols = [r[1] for r in con.execute("PRAGMA table_info(games)").fetchall()]
    if "wind_source" not in cols:
        con.execute("ALTER TABLE games ADD COLUMN wind_source TEXT")
        con.commit()
        print("  ✓  Added wind_source column to games table")


def load_today_games(con: sqlite3.Connection, game_date: str) -> list:
    """Load today's games with venue lat/lon and orientation."""
    return [dict(r) for r in con.execute("""
        SELECT
            g.game_pk,
            g.game_start_utc,
            g.wind_mph,
            g.wind_direction,
            g.wind_source,
            g.status,
            th.abbreviation AS home_abbr,
            ta.abbreviation AS away_abbr,
            v.name          AS venue_name,
            v.latitude,
            v.longitude,
            v.cf_direction,
            v.wind_effect
        FROM   games g
        JOIN   teams  th ON th.team_id = g.home_team_id
        JOIN   teams  ta ON ta.team_id = g.away_team_id
        LEFT JOIN venues v ON v.venue_id = g.venue_id
        WHERE  g.game_date_et = ?
          AND  g.game_type = 'R'
          AND  g.status NOT IN ('Final', 'Cancelled', 'Postponed')
        ORDER  BY g.game_start_utc
    """, (game_date,)).fetchall()]


# ── Wind direction conversion ─────────────────────────────────────────────────

def degrees_to_mlb_direction(wind_deg: float, cf_dir: str) -> str:
    """
    Convert meteorological wind direction (degrees, where wind is FROM)
    to MLB-style label relative to the ballpark orientation.

    wind_deg : meteorological degrees the wind is blowing FROM
    cf_dir   : compass label of center field from home plate
               e.g. 'SW' means CF is to the southwest of home plate

    Returns: 'IN', 'OUT', 'L To R', 'R To L', or 'CROSS'
    """
    cf_bearing = CF_BEARING.get(cf_dir.upper() if cf_dir else "", None)
    if cf_bearing is None:
        return "CROSS"   # unknown orientation → conservative label

    # Wind is blowing FROM wind_deg degrees.
    # Wind blowing FROM the CF direction = wind travelling TOWARD home = IN
    # Wind blowing TOWARD CF = wind from behind home plate = OUT

    # Angular difference between wind source and CF bearing
    diff = (wind_deg - cf_bearing) % 360
    if diff > 180:
        diff = 360 - diff   # normalise to 0–180

    # diff ≈ 0   → wind FROM CF direction → IN
    # diff ≈ 180 → wind FROM behind home  → OUT
    # diff ≈ 90  → crosswind

    if diff <= IN_OUT_ARC:
        return "IN"
    elif diff >= (180 - IN_OUT_ARC):
        return "OUT"
    else:
        # Crosswind — determine left vs right from batter's perspective
        # Batter faces CF (cf_bearing).  Left of batter = cf_bearing - 90.
        # Wind coming from LEFT of batter blows L→R across the field.
        left_bearing = (cf_bearing - 90) % 360
        diff_left = (wind_deg - left_bearing) % 360
        if diff_left > 180:
            diff_left = 360 - diff_left
        if diff_left <= 90:
            return "L To R"
        else:
            return "R To L"


def format_wind_direction(wind_deg: float, wind_mph: float,
                          cf_dir: Optional[str]) -> str:
    """Return full MLB-style direction string."""
    if wind_mph < CALM_MPH:
        return "CALM"
    if not cf_dir:
        return f"{wind_deg:.0f}°"
    return degrees_to_mlb_direction(wind_deg, cf_dir)


# ── Open-Meteo API ────────────────────────────────────────────────────────────

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_forecast(lat: float, lon: float,
                   game_start_utc: str,
                   verbose: bool = False) -> Optional[dict]:
    """
    Fetch hourly wind forecast from Open-Meteo for the hour containing
    the game start time.

    Returns dict with keys: wind_mph, wind_deg, wind_gust_mph, temp_f
    Returns None on network error.
    """
    # Build URL
    params = (
        f"?latitude={lat:.4f}&longitude={lon:.4f}"
        f"&hourly=wind_speed_10m,wind_direction_10m,wind_gusts_10m,temperature_2m"
        f"&wind_speed_unit=mph"
        f"&temperature_unit=fahrenheit"
        f"&forecast_days=2"
        f"&timezone=UTC"
    )
    url = OPEN_METEO_URL + params

    if verbose:
        print(f"    GET {url[:80]}...")

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "MLB-Scout/2.4"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        print(f"    ⚠  Open-Meteo request failed: {e}")
        return None

    # Find the hour index matching the game start UTC
    if not game_start_utc:
        return None

    try:
        # game_start_utc is like '2026-04-01T23:40:00' or with Z
        game_dt = datetime.fromisoformat(
            game_start_utc.rstrip("Z")).replace(tzinfo=timezone.utc)
        # Round down to hour
        target_hour = game_dt.strftime("%Y-%m-%dT%H:00")
    except ValueError:
        return None

    hours      = data["hourly"]["time"]
    speeds     = data["hourly"]["wind_speed_10m"]
    directions = data["hourly"]["wind_direction_10m"]
    gusts      = data["hourly"]["wind_gusts_10m"]
    temps      = data["hourly"]["temperature_2m"]

    # Find matching hour
    idx = None
    for i, h in enumerate(hours):
        if h == target_hour:
            idx = i
            break

    if idx is None:
        # Try the closest hour within ±1
        for offset in (1, -1, 2, -2):
            try:
                candidate = (game_dt + timedelta(hours=offset)).strftime("%Y-%m-%dT%H:00")
                if candidate in hours:
                    idx = hours.index(candidate)
                    break
            except Exception:
                pass

    if idx is None:
        print(f"    ⚠  Game start hour not found in forecast ({target_hour})")
        return None

    return {
        "wind_mph":      round(speeds[idx],     1),
        "wind_deg":      round(directions[idx], 1),
        "wind_gust_mph": round(gusts[idx],      1),
        "temp_f":        round(temps[idx],      1),
    }


# ── DB write ──────────────────────────────────────────────────────────────────

def write_forecast(con: sqlite3.Connection, game_pk: int,
                   wind_mph: float, wind_direction: str,
                   temp_f: float, dry_run: bool = False) -> None:
    """Write forecast conditions to games table."""
    if dry_run:
        return
    con.execute("""
        UPDATE games
        SET    wind_mph       = ?,
               wind_direction = ?,
               temp_f         = ?,
               wind_source    = 'forecast'
        WHERE  game_pk = ?
          AND  (wind_source IS NULL OR wind_source = 'forecast')
    """, (wind_mph, wind_direction, temp_f, game_pk))
    # Never overwrite 'actual' wind from completed games


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="Load forecast wind conditions for today's games from Open-Meteo"
    )
    p.add_argument("--date",    default=date.today().isoformat(),
                   help="Game date (default: today)")
    p.add_argument("--db",      default=DEFAULT_DB)
    p.add_argument("--dry-run", action="store_true",
                   help="Print forecasts without writing to DB")
    p.add_argument("--verbose", "-v", action="store_true",
                   help="Show API request details")
    p.add_argument("--force",   action="store_true",
                   help="Overwrite even if forecast already loaded today")
    p.add_argument("--no-starters", action="store_true",
                   help="Skip probable starter refresh (weather only)")
    p.add_argument("--no-weather",  action="store_true",
                   help="Skip weather fetch — update starters only. "
                        "Runs starter refresh then exits.")
    args = p.parse_args()

    con = get_connection(args.db)
    ensure_wind_source_column(con)

    games = load_today_games(con, args.date)

    if not games:
        print(f"\n  ⚠  No scheduled games found for {args.date}.")
        print("     Run load_mlb_stats.py and load_today.py first.\n")
        con.close()
        sys.exit(0)

    run_ts = _now_et().strftime("%Y-%m-%d %H:%M ET")
    print(f"\n  ══  Weather + Starter Refresh  ·  {args.date}  ·  {run_ts}  ══")

    # ── Probable starter refresh (runs before weather loop) ───────────
    if not args.no_starters and not args.dry_run:
        print("  Refreshing probable starters from MLB Stats API...")
        written = refresh_probable_starters(con, args.date, args.verbose)
        if written > 0:
            print(f"  ✓  {written} starter row(s) written to game_probable_pitchers")
        else:
            print("  —  No new starters announced yet (will retry on next run)")
        print()

    if getattr(args, "no_weather", False):
        print("  --no-weather: skipping forecast fetch.")
        con.close()
        sys.exit(0)

    print(f"  Source: Open-Meteo (free, no API key)")
    print(f"  Games:  {len(games)}")
    if args.dry_run:
        print("  Mode:   DRY RUN — no DB writes\n")
    else:
        print()

    # Group by venue to avoid duplicate API calls for doubleheaders
    seen_venues: dict = {}   # venue_name → forecast result
    loaded = 0
    skipped = 0
    failed = 0

    for g in games:
        home    = g["home_abbr"]
        away    = g["away_abbr"]
        venue   = g["venue_name"] or "Unknown venue"
        lat     = g["latitude"]
        lon     = g["longitude"]
        cf_dir  = g["cf_direction"]
        effect  = (g["wind_effect"] or "HIGH").upper()

        # Skip suppressed venues — wind signals never fire there
        if effect == "SUPPRESSED":
            print(f"  —  {away}@{home:<4}  [{venue}]  SUPPRESSED venue — skipping")
            skipped += 1
            continue

        # Skip if already has actual wind (from completed game stats)
        if g["wind_source"] == "actual" and not args.force:
            print(f"  —  {away}@{home:<4}  [{venue}]  actual wind already present — skipping")
            skipped += 1
            continue

        # Skip if already has forecast and not --force
        if g["wind_source"] == "forecast" and g["wind_mph"] is not None and not args.force:
            dir_label = g["wind_direction"] or "?"
            print(f"  ~  {away}@{home:<4}  [{venue}]  forecast already loaded "
                  f"({g['wind_mph']} mph {dir_label}) — use --force to refresh")
            skipped += 1
            continue

        if lat is None or lon is None:
            print(f"  ✗  {away}@{home:<4}  [{venue}]  no coordinates in DB — skipping")
            failed += 1
            continue

        # Use cached result for same venue (doubleheader)
        if venue in seen_venues:
            forecast = seen_venues[venue]
            cache_note = " (cached)"
        else:
            time.sleep(REQUEST_PAUSE)
            forecast = fetch_forecast(lat, lon, g["game_start_utc"], args.verbose)
            seen_venues[venue] = forecast
            cache_note = ""

        if forecast is None:
            print(f"  ✗  {away}@{home:<4}  [{venue}]  forecast unavailable")
            failed += 1
            continue

        mph      = forecast["wind_mph"]
        deg      = forecast["wind_deg"]
        gust     = forecast["wind_gust_mph"]
        temp     = forecast["temp_f"]
        dir_lbl  = format_wind_direction(deg, mph, cf_dir)

        # Signal eligibility quick check
        is_wind = mph >= 10 and effect == "HIGH"
        signal_note = " ⚑ WIND SIGNAL" if is_wind else ""

        print(f"  ✓  {away}@{home:<4}  [{venue[:28]}]  "
              f"{temp:.0f}°F  {mph:.0f} mph {dir_lbl}"
              f"  (gust {gust:.0f}){cache_note}{signal_note}")

        if args.verbose:
            print(f"       Raw: {deg:.0f}° from  CF={cf_dir}  → {dir_lbl}")

        write_forecast(con, g["game_pk"], mph, dir_lbl, temp, args.dry_run)
        loaded += 1

    if not args.dry_run:
        con.commit()

    print()
    print(f"  {'DRY RUN — ' if args.dry_run else ''}Results:")
    print(f"    Loaded  : {loaded}")
    print(f"    Skipped : {skipped}  (suppressed / already actual / already forecast)")
    print(f"    Failed  : {failed}  (missing coords or API error)")
    print()

    if loaded > 0 and not args.dry_run:
        print("  ✓  Wind forecast written to games table.")
        print("     Brief will use forecast wind for signal evaluation.")
        print("     Actual post-game wind loaded by load_mlb_stats.py next morning.")
    print()

    con.close()


if __name__ == "__main__":
    main()
