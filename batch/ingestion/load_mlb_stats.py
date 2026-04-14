"""
load_mlb_stats.py
────────────────────────────────────────────────────────────────────────────
MLB Stats API ingestion script.

Pulls data from the free, public MLB Stats API (statsapi.mlb.com) and
loads it into mlb_stats.db.  No API key required.

WHAT IT LOADS (in order):
  1. Teams + Venues       → teams, venues tables
  2. Players (roster)     → players table
  3. Schedule             → games table (status, score, timing)
  4. Box scores           → player_game_stats table
  5. Play-by-play         → play_by_play table
  6. Standings            → standings table
  7. Ingest log           → ingest_log (tracks every game pull)

DAILY USAGE (add to cron/Task Scheduler to run every morning):
  python load_mlb_stats.py

BACKFILL USAGE (load a full season, or a date range):
  python load_mlb_stats.py --start 2024-03-20 --end 2024-09-29
  python load_mlb_stats.py --season 2025
  python load_mlb_stats.py --season 2015 --no-pbp   # skip play-by-play to go faster

RETRY FAILED GAMES:
  python load_mlb_stats.py --retry-errors

FLAGS:
  --start DATE      Start date (YYYY-MM-DD). Default: yesterday.
  --end DATE        End date   (YYYY-MM-DD). Default: yesterday.
  --season YEAR     Load entire season (overrides --start/--end).
  --no-pbp          Skip play-by-play (much faster, use for initial catch-up).
  --retry-errors    Re-attempt games with status='error' in ingest_log.
  --db PATH         Path to database file. Default: get_db_path() (env / config/.env / cwd).
  --dry-run         Print what would be loaded, don't write anything.
  --verbose         Extra logging (shows each API call).

RATE LIMITING:
  The MLB Stats API is public and has no documented rate limit, but we pause
  0.3 s between game-level requests to be a good citizen.

DEPENDENCIES:
  Python 3.8+ standard library only.  No pip installs required.
────────────────────────────────────────────────────────────────────────────
"""

# CHANGE LOG (latest first)
# -------------------------
# 2026-04-13 21:00 ET  Default --db uses core.db.connection.get_db_path() (matches config/.env).
# 2026-04-13 16:24 ET  Refactor: route sqlite3.connect() calls through core.db.connection.connect().

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

# Repo root on sys.path so `core.*` imports work when run from batch/ingestion/
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from core.db.connection import connect as db_connect, get_db_path
from core.utils.log_manager import rotate_logs

# ── Constants ─────────────────────────────────────────────────────────────────
MLB_API      = "https://statsapi.mlb.com/api/v1"
MLB_API_V11  = "https://statsapi.mlb.com/api/v1.1"
SPORT_ID     = 1          # 1 = MLB
LEAGUE_IDS   = [103, 104] # AL = 103, NL = 104
REQUEST_PAUSE = 0.3       # seconds between game-level API calls
MAX_RETRIES   = 3         # HTTP retries per request
RETRY_BACKOFF = 2.0       # seconds, doubles each retry

SCRIPT_DIR = _SCRIPT_DIR
DEFAULT_DB = get_db_path()

# ── Logging setup ─────────────────────────────────────────────────────────────
log = logging.getLogger("mlb_loader")


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )


# ── HTTP helpers ───────────────────────────────────────────────────────────────
def fetch_json(url: str, verbose: bool = False) -> Optional[dict]:
    """
    GET a URL and return parsed JSON.  Retries on transient failures.
    Returns None on permanent failure (404, etc.).
    """
    if verbose:
        log.debug("GET %s", url)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "mlb-backtest-loader/1.0"}
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))

        except urllib.error.HTTPError as e:
            if e.code == 404:
                log.warning("404 Not Found: %s", url)
                return None
            if e.code in (429, 503) and attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF * attempt
                log.warning("HTTP %d — waiting %.1fs before retry %d/%d",
                            e.code, wait, attempt, MAX_RETRIES)
                time.sleep(wait)
            else:
                log.error("HTTP %d on %s", e.code, url)
                return None

        except (urllib.error.URLError, TimeoutError) as e:
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF * attempt
                log.warning("Network error (%s) — retry %d/%d in %.1fs",
                            e, attempt, MAX_RETRIES, wait)
                time.sleep(wait)
            else:
                log.error("Network error after %d retries: %s", MAX_RETRIES, e)
                return None

    return None


# ── Database helpers ───────────────────────────────────────────────────────────
def get_connection(db_path: str) -> sqlite3.Connection:
    """Open the database and apply performance PRAGMAs."""
    if not os.path.exists(db_path):
        log.error("Database not found: %s", db_path)
        log.error("Run create_db.py first to create the database.")
        sys.exit(1)

    con = db_connect(db_path, timeout=30)  # wait up to 30s if Scout holds a lock
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = OFF")   # OFF during bulk load — re-enabled in verify step
    con.execute("PRAGMA journal_mode = WAL")
    con.execute("PRAGMA synchronous  = NORMAL")
    con.execute("PRAGMA cache_size    = -64000")  # ~64 MB cache
    return con


def safe_int(value) -> Optional[int]:
    """Convert API value to int, returning None on failure."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def safe_float(value) -> Optional[float]:
    """Convert API value to float, returning None on failure."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def ip_to_float(ip_str) -> Optional[float]:
    """
    Convert innings pitched string to decimal float.
    '6.1' → 6.333, '7.2' → 7.667, '9.0' → 9.0
    The MLB API sometimes uses the confusing '6.1 means 6 and 1/3' format.
    """
    if ip_str is None:
        return None
    try:
        val = float(ip_str)
        whole = int(val)
        thirds = round(val - whole, 1)
        if thirds == 0.1:
            return whole + 1/3
        elif thirds == 0.2:
            return whole + 2/3
        else:
            return val
    except (TypeError, ValueError):
        return None


# ── Conversion helpers ─────────────────────────────────────────────────────────
def normalize_game_type(raw: str) -> str:
    """
    Map MLB API game type codes to the values accepted by our schema CHECK.
    Schema allows: R, S, P, A, E, D
    """
    mapping = {
        "R": "R",  # Regular
        "S": "S",  # Spring Training
        "P": "P",  # Postseason (generic)
        "A": "A",  # All-Star
        "E": "E",  # Exhibition
        "D": "D",  # Division Series
        "W": "P",  # World Series → P
        "L": "P",  # League Championship → P
        "F": "P",  # Wild Card → P
        "C": "P",  # ALCS/NLCS → P
    }
    return mapping.get(raw, "R")


def normalize_status(raw: str) -> str:
    """Map MLB API abstract game state to our schema values."""
    s = raw.lower()
    if "final" in s or "game over" in s or "completed" in s:
        return "Final"
    if "progress" in s or "live" in s:
        return "In Progress"
    if "postponed" in s:
        return "Postponed"
    if "suspended" in s:
        return "Suspended"
    if "cancelled" in s or "canceled" in s:
        return "Cancelled"
    if "pre-game" in s or "warmup" in s:
        return "Pre-Game"
    return "Scheduled"


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1: TEAMS + VENUES
# ══════════════════════════════════════════════════════════════════════════════

def load_teams_and_venues(con: sqlite3.Connection, verbose: bool = False) -> None:
    """
    Pull all 30 MLB teams and their home venues.
    Runs ONCE on first setup; safe to re-run (uses INSERT OR REPLACE).
    """
    log.info("Loading teams and venues …")

    url = f"{MLB_API}/teams?sportId={SPORT_ID}&fields=teams,id,name,teamName,shortName,abbreviation,league,id,name,division,id,name,venue,id,name,active,firstYearOfPlay"
    data = fetch_json(url, verbose)
    if not data:
        log.error("Could not fetch teams.")
        return

    teams_loaded = 0
    venues_seen  = set()

    for team in data.get("teams", []):
        # ── Venue ──────────────────────────────────────────────────────────
        venue_obj = team.get("venue", {})
        venue_id  = safe_int(venue_obj.get("id"))

        if venue_id and venue_id not in venues_seen:
            venues_seen.add(venue_id)
            # Fetch full venue detail for dimensions etc.
            vurl  = f"{MLB_API}/venues/{venue_id}?hydrate=location,fieldInfo"
            vdata = fetch_json(vurl, verbose)
            venue_detail = {}
            if vdata and vdata.get("venues"):
                venue_detail = vdata["venues"][0]

            field   = venue_detail.get("fieldInfo", {})
            loc     = venue_detail.get("location", {})
            coords  = loc.get("defaultCoordinates", {})

            surface_raw = field.get("turfType", "")
            surface_map = {"Grass": "Grass", "Artificial Turf": "Turf",
                           "Mixed": "Mixed"}
            surface = surface_map.get(surface_raw)

            roof_raw = field.get("roofType", "")
            roof_map = {"Open": "Open", "Dome": "Dome",
                        "Retractable Roof": "Retractable"}
            roof = roof_map.get(roof_raw)

            # ── Two-step upsert: preserves wind/park-factor columns ───────
            # INSERT OR REPLACE would DELETE + re-INSERT the row, wiping any
            # columns not named here (wind_effect, wind_note, etc.).
            # INSERT OR IGNORE (no-op if exists) + targeted UPDATE keeps them.
            _venue_vals = (
                venue_id,
                venue_detail.get("name", venue_obj.get("name", "")),
                loc.get("city", ""),
                loc.get("stateAbbrev", loc.get("state", "")),
                loc.get("country", "USA"),
                safe_int(field.get("capacity")),
                surface, roof,
                safe_int(field.get("leftLine")),
                safe_int(field.get("leftCenter")),
                safe_int(field.get("center")),
                safe_int(field.get("rightCenter")),
                safe_int(field.get("rightLine")),
                safe_float(coords.get("latitude")),
                safe_float(coords.get("longitude")),
            )
            con.execute("""
                INSERT OR IGNORE INTO venues
                    (venue_id, name, city, state, country,
                     capacity, surface, roof_type,
                     left_line_ft, left_center_ft, center_ft,
                     right_center_ft, right_line_ft,
                     latitude, longitude)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, _venue_vals)
            con.execute("""
                UPDATE venues SET
                    name            = ?,
                    city            = ?,
                    state           = ?,
                    country         = ?,
                    capacity        = ?,
                    surface         = ?,
                    roof_type       = ?,
                    left_line_ft    = ?,
                    left_center_ft  = ?,
                    center_ft       = ?,
                    right_center_ft = ?,
                    right_line_ft   = ?,
                    latitude        = ?,
                    longitude       = ?
                WHERE venue_id = ?
            """, _venue_vals[1:] + (_venue_vals[0],))

        # ── Team ───────────────────────────────────────────────────────────
        team_id  = safe_int(team.get("id"))
        league   = team.get("league", {}).get("name", "")
        division = team.get("division", {}).get("name", "")

        # Normalize league / division to schema values
        league_code   = "AL" if "american" in league.lower()  else "NL"
        division_code = "East" if "east"  in division.lower() else \
                        "West" if "west"  in division.lower() else "Central"

        if team_id:
            con.execute("""
                INSERT OR REPLACE INTO teams
                    (team_id, name, abbreviation, short_name,
                     league, division, venue_id, first_year, active)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                team_id,
                team.get("name", ""),
                team.get("abbreviation", ""),
                team.get("teamName", ""),
                league_code, division_code,
                venue_id,
                safe_int(team.get("firstYearOfPlay")),
                1 if team.get("active", True) else 0,
            ))
            teams_loaded += 1

    con.commit()
    log.info("  → %d teams, %d venues loaded.", teams_loaded, len(venues_seen))


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2: PLAYERS
# ══════════════════════════════════════════════════════════════════════════════

def load_players_for_season(con: sqlite3.Connection, season: int,
                             verbose: bool = False) -> int:
    """
    Pull every player who appeared on any roster in a given season.
    Uses the sports_players endpoint which returns all active players.
    Returns number of players upserted.
    """
    log.info("Loading players for season %d …", season)

    url  = f"{MLB_API}/sports/{SPORT_ID}/players?season={season}&fields=people,id,fullName,firstName,lastName,birthDate,birthCity,birthCountry,height,weight,batSide,pitchHand,primaryPosition,code,mlbDebutDate,active"
    data = fetch_json(url, verbose)
    if not data:
        log.warning("  No player data for season %d.", season)
        return 0

    count = 0
    now   = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

    for p in data.get("people", []):
        pid = safe_int(p.get("id"))
        if not pid:
            continue

        # Height arrives as '6'' 3"' — convert to total inches
        height_str = p.get("height", "")
        height_in  = None
        if "'" in height_str:
            try:
                parts    = height_str.replace('"', '').split("'")
                height_in = int(parts[0].strip()) * 12 + int(parts[1].strip())
            except (ValueError, IndexError):
                pass

        con.execute("""
            INSERT INTO players
                (player_id, full_name, first_name, last_name,
                 birth_date, birth_city, birth_country,
                 height_inches, weight_lbs,
                 bats, throws, primary_position,
                 debut_date, active, last_updated)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(player_id) DO UPDATE SET
                full_name        = excluded.full_name,
                active           = excluded.active,
                last_updated     = excluded.last_updated
        """, (
            pid,
            p.get("fullName", ""),
            p.get("firstName", ""),
            p.get("lastName", ""),
            p.get("birthDate"),
            p.get("birthCity"),
            p.get("birthCountry"),
            height_in,
            safe_int(p.get("weight")),
            p.get("batSide", {}).get("code"),
            p.get("pitchHand", {}).get("code"),
            p.get("primaryPosition", {}).get("code"),
            p.get("mlbDebutDate"),
            1 if p.get("active", False) else 0,
            now,
        ))
        count += 1

    con.commit()
    log.info("  → %d players upserted.", count)
    return count


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3: SCHEDULE (games rows)
# ══════════════════════════════════════════════════════════════════════════════

# In-process cache: team IDs we've already confirmed exist in the DB this run
_team_cache: set = set()


def ensure_team(con: sqlite3.Connection, team_id: int,
                verbose: bool = False) -> None:
    """
    Guarantee a team row exists before we insert a game that references it.
    Fetches full detail from the API so league/division CHECK constraints
    are always satisfied.  Uses an in-process cache to avoid repeat calls.
    """
    global _team_cache
    if team_id in _team_cache:
        return

    # Already in DB — no fetch needed
    row = con.execute("SELECT 1 FROM teams WHERE team_id = ?",
                      (team_id,)).fetchone()
    if row:
        _team_cache.add(team_id)
        return

    log.debug("Fetching team %d from API …", team_id)
    url  = (f"{MLB_API}/teams/{team_id}"
            f"?fields=teams,id,name,teamName,shortName,abbreviation,"
            f"league,id,name,division,id,name,venue,id,active,firstYearOfPlay")
    data = fetch_json(url, verbose)

    if not data or not data.get("teams"):
        # API unavailable — insert a safe placeholder that satisfies CHECK
        log.warning("Could not fetch team %d — inserting placeholder.", team_id)
        con.execute("""
            INSERT OR IGNORE INTO teams
                (team_id, name, abbreviation, league, division)
            VALUES (?,?,?,?,?)
        """, (team_id, f"Team {team_id}", "???", "AL", "East"))
        return

    team = data["teams"][0]
    league_name   = team.get("league",   {}).get("name", "")
    division_name = team.get("division", {}).get("name", "")
    venue_id      = safe_int(team.get("venue", {}).get("id"))

    league_code   = "AL" if "american" in league_name.lower() else "NL"
    division_code = ("East"    if "east"    in division_name.lower() else
                     "West"    if "west"    in division_name.lower() else
                     "Central")

    # Insert a venue stub so the team FK is satisfied (full data loaded later)
    if venue_id:
        con.execute("""
            INSERT OR IGNORE INTO venues (venue_id, name, city)
            VALUES (?, ?, ?)
        """, (venue_id, team.get("venue", {}).get("name", ""), ""))

    con.execute("""
        INSERT OR REPLACE INTO teams
            (team_id, name, abbreviation, short_name,
             league, division, venue_id, first_year, active)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        team_id,
        team.get("name", f"Team {team_id}"),
        team.get("abbreviation", "???"),
        team.get("teamName", ""),
        league_code, division_code,
        venue_id,
        safe_int(team.get("firstYearOfPlay")),
        1 if team.get("active", True) else 0,
    ))
    con.commit()
    _team_cache.add(team_id)



def ensure_venue(con: sqlite3.Connection, venue_id: int,
                 name: str = "", verbose: bool = False) -> None:
    """
    Guarantee a venue row exists before we insert a game that references it.
    Inserts a minimal stub if we can't fetch full details — enough to satisfy
    the FK constraint.  Full venue data is populated by load_teams_and_venues.
    """
    if venue_id is None:
        return
    row = con.execute("SELECT 1 FROM venues WHERE venue_id = ?",
                      (venue_id,)).fetchone()
    if row:
        return

    log.debug("Inserting venue stub for venue_id=%d (%s)", venue_id, name or "?")
    con.execute("""
        INSERT OR IGNORE INTO venues (venue_id, name, city)
        VALUES (?, ?, ?)
    """, (venue_id, name or f"Venue {venue_id}", ""))
    con.commit()


def load_schedule(con: sqlite3.Connection, start_date: str, end_date: str,
                  verbose: bool = False) -> list[dict]:
    """
    Pull the game schedule for a date range and upsert into the games table.
    Returns a list of game dicts that are 'Final' and not yet fully ingested.
    """
    log.info("Loading schedule %s → %s …", start_date, end_date)

    url = (f"{MLB_API}/schedule?sportId={SPORT_ID}"
           f"&startDate={start_date}&endDate={end_date}"
           f"&gameType=R,P,S,A"
           f"&hydrate=decisions,linescore,weather,venue,probablePitchers")

    data = fetch_json(url, verbose)
    if not data:
        log.warning("  No schedule data returned.")
        return []

    games_to_ingest = []
    total_upserted  = 0

    for date_obj in data.get("dates", []):
        for g in date_obj.get("games", []):
            game_pk = safe_int(g.get("gamePk"))
            if not game_pk:
                continue

            status_raw  = g.get("status", {}).get("abstractGameState", "")
            detail_raw  = g.get("status", {}).get("detailedState", "")
            status      = normalize_status(detail_raw or status_raw)

            game_type_raw = g.get("gameType", "R")
            game_type     = normalize_game_type(game_type_raw)

            season = safe_int(g.get("season"))
            if not season:
                season = int(start_date[:4])

            home_team = g.get("teams", {}).get("home",  {}).get("team", {})
            away_team = g.get("teams", {}).get("away",  {}).get("team", {})
            home_id   = safe_int(home_team.get("id"))
            away_id   = safe_int(away_team.get("id"))
            venue_id  = safe_int(g.get("venue", {}).get("id"))

            # Score (only present when Final or In Progress)
            home_score = safe_int(g.get("teams", {}).get("home", {}).get("score"))
            away_score = safe_int(g.get("teams", {}).get("away", {}).get("score"))

            # Linescore for innings count
            linescore  = g.get("linescore", {})
            innings    = safe_int(linescore.get("currentInning"))
            extra      = 1 if innings and innings > 9 else 0

            # Game datetime
            # IMPORTANT: gameDate from MLB API is UTC tip-off time.
            # Slicing [:10] gives the UTC date, which is WRONG for late
            # West Coast games (e.g. 10:10 PM PT = 01:10 UTC next day).
            # officialDate is the correct local calendar date (e.g. '2026-03-26')
            # regardless of when UTC midnight falls relative to first pitch.
            # Always prefer officialDate; fall back to date_obj["date"] (the
            # schedule query date bucket), then gameDate[:10] as last resort.
            game_date_str  = g.get("gameDate", "")   # UTC: '2026-03-27T02:10:00Z'
            game_start_utc = game_date_str.replace("Z", "") if game_date_str else None
            game_date_only = (
                g.get("officialDate")                  # correct local date always
                or date_obj.get("date", "")            # schedule query date bucket
                or (game_date_str[:10] if game_date_str else "")  # UTC fallback
            )

            # Duration (only on Final games)
            duration_min = None
            if g.get("gameData", {}).get("datetime", {}).get("amPm"):
                pass  # will be filled from feed/live if needed

            # Weather
            weather = g.get("weather", {})

            # Series info
            series_desc   = g.get("seriesDescription", "")
            series_gamenum = safe_int(g.get("seriesGameNumber"))
            double_header = g.get("doubleHeader", "N")
            game_number   = safe_int(g.get("gameNumber")) or 1

            # Postpone reason
            postpone_reason = None
            if status == "Postponed":
                postpone_reason = detail_raw

            # Ensure season exists in seasons table (add it if missing)
            con.execute("""
                INSERT OR IGNORE INTO seasons (season, season_start, season_end)
                VALUES (?, ?, ?)
            """, (season, f"{season}-03-01", f"{season}-11-30"))

            # Ensure teams + venue exist before inserting the game row
            if home_id:
                ensure_team(con, home_id, verbose)
            if away_id:
                ensure_team(con, away_id, verbose)
            if venue_id:
                venue_name = g.get("venue", {}).get("name", "")
                ensure_venue(con, venue_id, venue_name, verbose)

            con.execute("""
                INSERT INTO games
                    (game_pk, season, game_date, game_type,
                     series_description, series_game_number,
                     home_team_id, away_team_id, venue_id,
                     game_start_utc,
                     home_score, away_score, innings_played, extra_innings,
                     status, postpone_reason,
                     temp_f, wind_mph, wind_direction, sky_condition,
                     double_header, game_number)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(game_pk) DO UPDATE SET
                    status          = excluded.status,
                    home_score      = COALESCE(excluded.home_score, home_score),
                    away_score      = COALESCE(excluded.away_score, away_score),
                    innings_played  = COALESCE(excluded.innings_played, innings_played),
                    extra_innings   = excluded.extra_innings,
                    temp_f          = COALESCE(excluded.temp_f, temp_f),
                    wind_mph        = COALESCE(excluded.wind_mph, wind_mph),
                    wind_direction  = COALESCE(excluded.wind_direction, wind_direction),
                    sky_condition   = COALESCE(excluded.sky_condition, sky_condition)
            """, (
                game_pk, season, game_date_only, game_type,
                series_desc, series_gamenum,
                home_id, away_id, venue_id,
                game_start_utc,
                home_score, away_score, innings, extra,
                status, postpone_reason,
                safe_int(weather.get("temp")),
                safe_int(weather.get("wind", "").split(" ")[0]) if weather.get("wind") else None,
                weather.get("wind"),
                weather.get("condition"),
                double_header if double_header in ("N", "Y", "S") else "N",
                game_number,
            ))
            total_upserted += 1

            # ── Probable pitchers (announced starters) ──────────────────
            # probablePitchers is in g["teams"]["home"/"away"] when announced.
            # Only present for upcoming/scheduled games — not for Final games
            # (those use actual box score starters from player_game_stats).
            # INSERT OR REPLACE so re-runs with updated starters overwrite stale.
            home_prob = safe_int(
                g.get("teams", {}).get("home", {})
                 .get("probablePitcher", {}).get("id")
            )
            away_prob = safe_int(
                g.get("teams", {}).get("away", {})
                 .get("probablePitcher", {}).get("id")
            )
            if (home_prob or away_prob) and home_id and away_id:
                # Ensure player rows exist before the FK write
                for pid in filter(None, [home_prob, away_prob]):
                    con.execute("""
                        INSERT OR IGNORE INTO players
                            (player_id, full_name, last_name, active)
                        VALUES (?, ?, ?, 1)
                    """, (pid, f"Player {pid}", f"P{pid}"))
                upsert_probable_pitchers(
                    con, game_pk, home_id, away_id,
                    home_prob, away_prob,
                    datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M")
                )

            # Queue Final games that haven't been fully ingested
            if status == "Final":
                games_to_ingest.append({
                    "game_pk": game_pk,
                    "game_date": game_date_only,
                    "season": season,
                })

    con.commit()
    log.info("  → %d games upserted, %d Final and ready for box score pull.",
             total_upserted, len(games_to_ingest))

    # Update players.era_season from the most recent season pitching row.
    # Uses the era column in player_game_stats which carries season-to-date ERA
    # (from seasonStats in the box score API). Only updates if data is present.
    try:
        season_year = int(start_date[:4])
        con.execute("""
            UPDATE players
            SET era_season = (
                SELECT pgs.era
                FROM   player_game_stats pgs
                JOIN   games g ON g.game_pk = pgs.game_pk
                WHERE  pgs.player_id = players.player_id
                  AND  pgs.player_role = 'pitcher'
                  AND  g.season = ?
                  AND  pgs.era IS NOT NULL
                ORDER  BY g.game_date_et DESC, g.game_pk DESC
                LIMIT  1
            )
            WHERE EXISTS (
                SELECT 1 FROM player_game_stats pgs
                JOIN   games g ON g.game_pk = pgs.game_pk
                WHERE  pgs.player_id = players.player_id
                  AND  pgs.player_role = 'pitcher'
                  AND  g.season = ?
                  AND  pgs.era IS NOT NULL
            )
        """, (season_year, season_year))
        con.commit()
    except Exception as e:
        log.debug("  era_season update skipped: %s", e)

    return games_to_ingest


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3b: PROBABLE PITCHERS
# ══════════════════════════════════════════════════════════════════════════════

def ensure_probable_pitchers_table(con: sqlite3.Connection) -> None:
    """Create game_probable_pitchers table if it does not exist.
    Also ensures players.era_season column exists (added Apr 2026).
    Called once at startup. Safe to call on every run.
    """
    # Add era_season to players if missing (one-time migration, idempotent)
    try:
        con.execute("ALTER TABLE players ADD COLUMN era_season REAL")
        con.commit()
        log.info("  Added era_season column to players table.")
    except Exception:
        pass  # Column already exists — normal on subsequent runs
    con.execute("""
        CREATE TABLE IF NOT EXISTS game_probable_pitchers (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            game_pk     INTEGER NOT NULL REFERENCES games (game_pk),
            team_id     INTEGER NOT NULL,
            player_id   INTEGER NOT NULL,
            fetched_at  TEXT    NOT NULL,
            UNIQUE (game_pk, team_id)
        )
    """)
    con.commit()


def upsert_probable_pitchers(con: sqlite3.Connection,
                              game_pk: int,
                              home_team_id: int,
                              away_team_id: int,
                              home_pitcher_id: int | None,
                              away_pitcher_id: int | None,
                              fetched_at: str) -> int:
    """Write probable pitcher rows for a game. Returns number of rows upserted.
    Uses INSERT OR REPLACE so re-running with updated starters overwrites stale data.
    """
    count = 0
    for team_id, player_id in [
        (home_team_id, home_pitcher_id),
        (away_team_id, away_pitcher_id),
    ]:
        if player_id is None:
            continue
        con.execute("""
            INSERT INTO game_probable_pitchers
                (game_pk, team_id, player_id, fetched_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(game_pk, team_id) DO UPDATE SET
                player_id  = excluded.player_id,
                fetched_at = excluded.fetched_at
        """, (game_pk, team_id, player_id, fetched_at))
        count += 1
    return count


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4: BOX SCORES (player_game_stats)
# ══════════════════════════════════════════════════════════════════════════════

def load_boxscore(con: sqlite3.Connection, game_pk: int,
                  verbose: bool = False) -> int:
    """
    Pull the box score for one game and insert batting + pitching lines.
    Returns number of player-stat rows inserted/updated.
    """
    url  = f"{MLB_API}/game/{game_pk}/boxscore"
    data = fetch_json(url, verbose)
    if not data:
        return 0

    rows_inserted = 0
    teams         = data.get("teams", {})

    for side in ("home", "away"):
        team_obj    = teams.get(side, {})
        team_info   = team_obj.get("team", {})
        team_id     = safe_int(team_info.get("id"))
        players_obj = team_obj.get("players", {})

        for player_key, player_data in players_obj.items():
            person   = player_data.get("person", {})
            player_id = safe_int(person.get("id"))
            if not player_id:
                continue

            # Ensure player exists in players table
            con.execute("""
                INSERT OR IGNORE INTO players
                    (player_id, full_name, last_name, active)
                VALUES (?,?,?,1)
            """, (player_id,
                  person.get("fullName", f"Player {player_id}"),
                  person.get("fullName", "").split()[-1]))

            batting_order = safe_int(player_data.get("battingOrder"))
            position      = player_data.get("position", {}).get("abbreviation")
            stats         = player_data.get("stats", {})

            # ── BATTING ─────────────────────────────────────────────────
            bat_s = stats.get("batting", {})
            if bat_s:
                ip_raw  = bat_s.get("inningsPitched")
                runs    = safe_int(bat_s.get("runs"))
                hits    = safe_int(bat_s.get("hits"))

                # Only write a batting row if the player actually batted
                # (has plate appearances or at-bats)
                pa  = safe_int(bat_s.get("plateAppearances"))
                ab  = safe_int(bat_s.get("atBats"))
                if pa or ab:
                    con.execute("""
                        INSERT INTO player_game_stats
                            (game_pk, player_id, team_id, player_role,
                             batting_order, position,
                             at_bats, plate_appearances, runs, hits,
                             doubles, triples, home_runs, rbi,
                             walks, intentional_walks, strikeouts_bat,
                             stolen_bases, caught_stealing,
                             hit_by_pitch, sac_flies, sac_bunts,
                             left_on_base, ground_into_dp,
                             batting_avg, obp, slg, ops)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(game_pk, player_id, player_role) DO UPDATE SET
                            runs     = excluded.runs,
                            hits     = excluded.hits,
                            home_runs = excluded.home_runs,
                            rbi       = excluded.rbi,
                            walks     = excluded.walks,
                            strikeouts_bat = excluded.strikeouts_bat
                    """, (
                        game_pk, player_id, team_id, "batter",
                        batting_order, position,
                        ab, pa, runs, hits,
                        safe_int(bat_s.get("doubles")),
                        safe_int(bat_s.get("triples")),
                        safe_int(bat_s.get("homeRuns")),
                        safe_int(bat_s.get("rbi")),
                        safe_int(bat_s.get("baseOnBalls")),
                        safe_int(bat_s.get("intentionalWalks")),
                        safe_int(bat_s.get("strikeOuts")),
                        safe_int(bat_s.get("stolenBases")),
                        safe_int(bat_s.get("caughtStealing")),
                        safe_int(bat_s.get("hitByPitch")),
                        safe_int(bat_s.get("sacFlies")),
                        safe_int(bat_s.get("sacBunts")),
                        safe_int(bat_s.get("leftOnBase")),
                        safe_int(bat_s.get("groundIntoDoublePlay")),
                        safe_float(bat_s.get("avg")),
                        safe_float(bat_s.get("obp")),
                        safe_float(bat_s.get("slg")),
                        safe_float(bat_s.get("ops")),
                    ))
                    rows_inserted += 1

            # ── PITCHING ────────────────────────────────────────────────
            pit_s = stats.get("pitching", {})
            if pit_s:
                ip_raw  = pit_s.get("inningsPitched")
                ip_dec  = ip_to_float(ip_raw)

                # Only write a pitching row if the player actually pitched
                if ip_dec is not None and ip_dec > 0:
                    season_stats = player_data.get("seasonStats", {}).get("pitching", {})

                    # Quality start: 6+ IP and 3 or fewer earned runs
                    er    = safe_int(pit_s.get("earnedRuns"))
                    qs    = 1 if (ip_dec is not None and ip_dec >= 6.0
                                  and er is not None and er <= 3) else 0

                    con.execute("""
                        INSERT INTO player_game_stats
                            (game_pk, player_id, team_id, player_role,
                             position,
                             innings_pitched, pitches_thrown, strikes_thrown,
                             earned_runs, runs_allowed, hits_allowed,
                             doubles_allowed, triples_allowed, hr_allowed,
                             walks_allowed, ibb_allowed, strikeouts_pit,
                             hit_batters, wild_pitches, balks,
                             ground_outs, air_outs,
                             win, loss, save, blown_save, hold,
                             complete_game, shutout, quality_start,
                             era, whip, k_per_9, bb_per_9)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(game_pk, player_id, player_role) DO UPDATE SET
                            innings_pitched  = excluded.innings_pitched,
                            earned_runs      = excluded.earned_runs,
                            strikeouts_pit   = excluded.strikeouts_pit,
                            win              = excluded.win,
                            loss             = excluded.loss,
                            save             = excluded.save,
                            era              = excluded.era
                    """, (
                        game_pk, player_id, team_id, "pitcher",
                        position,
                        ip_dec,
                        safe_int(pit_s.get("pitchesThrown") or pit_s.get("numberOfPitches")),
                        safe_int(pit_s.get("strikes")),
                        er,
                        safe_int(pit_s.get("runs")),
                        safe_int(pit_s.get("hits")),
                        safe_int(pit_s.get("doubles")),
                        safe_int(pit_s.get("triples")),
                        safe_int(pit_s.get("homeRuns")),
                        safe_int(pit_s.get("baseOnBalls")),
                        safe_int(pit_s.get("intentionalWalks")),
                        safe_int(pit_s.get("strikeOuts")),
                        safe_int(pit_s.get("hitBatsmen")),
                        safe_int(pit_s.get("wildPitches")),
                        safe_int(pit_s.get("balks")),
                        safe_int(pit_s.get("groundOuts")),
                        safe_int(pit_s.get("airOuts")),
                        1 if pit_s.get("wins")      else 0,
                        1 if pit_s.get("losses")    else 0,
                        1 if pit_s.get("saves")     else 0,
                        1 if pit_s.get("blownSaves") else 0,
                        1 if pit_s.get("holds")     else 0,
                        1 if pit_s.get("completeGames") else 0,
                        1 if pit_s.get("shutouts")  else 0,
                        qs,
                        safe_float(season_stats.get("era") or pit_s.get("era")),
                        safe_float(season_stats.get("whip") or pit_s.get("whip")),
                        safe_float(season_stats.get("strikeoutsPer9Inn")),
                        safe_float(season_stats.get("walksPer9Inn")),
                    ))
                    rows_inserted += 1

    return rows_inserted


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5: PLAY-BY-PLAY
# ══════════════════════════════════════════════════════════════════════════════

def load_play_by_play(con: sqlite3.Connection, game_pk: int,
                      verbose: bool = False) -> int:
    """
    Pull full play-by-play for one game from the live feed endpoint.
    Returns number of play rows inserted.
    """
    # v1.1 live feed has the fullest play-by-play data including Statcast
    url  = f"{MLB_API_V11}/game/{game_pk}/feed/live"
    data = fetch_json(url, verbose)
    if not data:
        return 0

    all_plays = data.get("liveData", {}).get("plays", {}).get("allPlays", [])
    rows_inserted = 0

    for play in all_plays:
        about    = play.get("about", {})
        matchup  = play.get("matchup", {})
        result   = play.get("result",  {})
        count    = play.get("count",   {})

        ab_index  = safe_int(about.get("atBatIndex"))
        inning    = safe_int(about.get("inning"))
        half      = "top" if about.get("halfInning") == "top" else "bottom"
        batter_id = safe_int(matchup.get("batter",  {}).get("id"))
        pitcher_id= safe_int(matchup.get("pitcher", {}).get("id"))

        event_type = result.get("eventType", result.get("event", "")).lower().replace(" ", "_")
        description= result.get("description", "")
        is_scoring = 1 if result.get("isScoringPlay") else 0
        runs       = safe_int(result.get("runs")) or 0
        rbi        = safe_int(result.get("rbi"))  or 0

        # Runners on base
        offense    = play.get("offense", {})
        on_first   = safe_int(offense.get("first",  {}).get("id"))
        on_second  = safe_int(offense.get("second", {}).get("id"))
        on_third   = safe_int(offense.get("third",  {}).get("id"))

        # Fielders involved
        runners    = play.get("runners", [])
        fielder_ids = list({
            safe_int(r.get("credits", [{}])[0].get("player", {}).get("id"))
            for r in runners if r.get("credits")
        } - {None})

        play_events = play.get("playEvents", [])

        for event in play_events:
            play_idx   = safe_int(event.get("index"))
            is_pitch   = event.get("isPitch", False)

            details    = event.get("details",   {})
            pitch_data = event.get("pitchData",  {})
            hit_data   = event.get("hitData",    {})
            ev_count   = event.get("count",      {})

            pitch_type      = None
            pitch_type_desc = None
            pitch_speed     = None
            pitch_zone      = None

            if is_pitch:
                ptype           = details.get("type", {})
                pitch_type      = ptype.get("code")
                pitch_type_desc = ptype.get("description")
                pitch_speed     = safe_float(pitch_data.get("startSpeed"))
                pitch_zone      = safe_int(pitch_data.get("zone"))

            # Hit / Statcast data
            trajectory     = hit_data.get("trajectory")
            exit_velocity  = safe_float(hit_data.get("launchSpeed"))
            launch_angle   = safe_float(hit_data.get("launchAngle"))
            hit_distance   = safe_float(hit_data.get("totalDistance"))
            hit_coord_x    = safe_float(hit_data.get("coordinates", {}).get("coordX"))
            hit_coord_y    = safe_float(hit_data.get("coordinates", {}).get("coordY"))

            # Barrel: exit velo ≥ 98 and launch angle 26–30
            is_barrel = None
            if exit_velocity is not None and launch_angle is not None:
                is_barrel = 1 if (exit_velocity >= 98.0
                                  and 26.0 <= launch_angle <= 30.0) else 0

            # For the final play-event in the at-bat, use play-level data
            is_last_event = (event == play_events[-1])
            final_event   = event_type if is_last_event else None
            final_desc    = description if is_last_event else details.get("description")

            try:
                con.execute("""
                    INSERT OR IGNORE INTO play_by_play
                        (game_pk, inning, inning_half,
                         at_bat_index, play_index,
                         batter_id, pitcher_id, fielder_ids,
                         event_type, event_code, description,
                         is_scoring_play, runs_scored, rbi_on_play,
                         outs_before, balls_before, strikes_before,
                         on_first, on_second, on_third,
                         pitch_type, pitch_type_desc, pitch_speed_mph, pitch_zone,
                         hit_trajectory, exit_velocity, launch_angle,
                         hit_distance_ft, hit_coord_x, hit_coord_y, is_barrel)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    game_pk, inning, half,
                    ab_index, play_idx,
                    batter_id, pitcher_id,
                    json.dumps(fielder_ids) if fielder_ids else None,
                    final_event,
                    details.get("code"),
                    final_desc,
                    is_scoring, runs, rbi,
                    safe_int(ev_count.get("outs")),
                    safe_int(ev_count.get("balls")),
                    safe_int(ev_count.get("strikes")),
                    on_first, on_second, on_third,
                    pitch_type, pitch_type_desc, pitch_speed, pitch_zone,
                    trajectory, exit_velocity, launch_angle,
                    hit_distance, hit_coord_x, hit_coord_y, is_barrel,
                ))
                rows_inserted += 1
            except sqlite3.IntegrityError:
                pass  # duplicate play — skip

    return rows_inserted


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6: STANDINGS
# ══════════════════════════════════════════════════════════════════════════════

def load_standings(con: sqlite3.Connection, snapshot_date: str,
                   season: int, verbose: bool = False) -> int:
    """
    Pull standings for both leagues for a given date.
    Returns number of rows inserted/replaced.
    """
    rows = 0
    for league_id in LEAGUE_IDS:
        url  = (f"{MLB_API}/standings?leagueId={league_id}"
                f"&season={season}&date={snapshot_date}"
                f"&standingsType=regularSeason"
                f"&hydrate=team,record,streak,division,sport,league")
        data = fetch_json(url, verbose)
        if not data:
            continue

        for record in data.get("records", []):
            for tr in record.get("teamRecords", []):
                team_id = safe_int(tr.get("team", {}).get("id"))
                if not team_id:
                    continue

                w   = safe_int(tr.get("wins",   0)) or 0
                l   = safe_int(tr.get("losses", 0)) or 0
                pct = safe_float(tr.get("winningPercentage")) or (w / (w + l) if (w + l) else 0.0)

                streak_obj = tr.get("streak", {})
                streak_str = streak_obj.get("streakCode", "")
                streak_type   = streak_str[0] if streak_str else None
                streak_length = safe_int(streak_str[1:]) if len(streak_str) > 1 else None

                # Last 10
                split10 = tr.get("records", {}).get("splitRecords", [])
                last10  = next((s for s in split10 if s.get("type") == "lastTen"), {})
                l10w    = safe_int(last10.get("wins"))
                l10l    = safe_int(last10.get("losses"))

                # Splits for home/away
                splits  = tr.get("records", {}).get("splitRecords", [])
                home_rec = next((s for s in splits if s.get("type") == "home"),  {})
                away_rec = next((s for s in splits if s.get("type") == "away"),  {})

                # Runs
                rs  = safe_int(tr.get("runsScored"))
                ra  = safe_int(tr.get("runsAllowed"))
                rd  = (rs - ra) if (rs is not None and ra is not None) else None

                # Pythagorean expected win %
                pythag = None
                if rs and ra and ra > 0:
                    pythag = round(rs**2 / (rs**2 + ra**2), 4)

                con.execute("""
                    INSERT OR REPLACE INTO standings
                        (snapshot_date, team_id, season,
                         wins, losses, win_pct,
                         games_back, wild_card_gb,
                         division_rank, league_rank, wild_card_rank,
                         last_10_wins, last_10_losses,
                         streak, streak_type, streak_length,
                         runs_scored, runs_allowed, run_diff, pythag_win_pct,
                         home_wins, home_losses, away_wins, away_losses)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    snapshot_date, team_id, season,
                    w, l, pct,
                    safe_float(tr.get("gamesBack"))           or 0.0,
                    safe_float(tr.get("wildCardGamesBack")),
                    safe_int(tr.get("divisionRank")),
                    safe_int(tr.get("leagueRank")),
                    safe_int(tr.get("wildCardRank")),
                    l10w, l10l,
                    streak_str, streak_type, streak_length,
                    rs, ra, rd, pythag,
                    safe_int(home_rec.get("wins")),
                    safe_int(home_rec.get("losses")),
                    safe_int(away_rec.get("wins")),
                    safe_int(away_rec.get("losses")),
                ))
                rows += 1

    con.commit()
    return rows


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 7: INGEST LOG
# ══════════════════════════════════════════════════════════════════════════════

def get_already_ingested(con: sqlite3.Connection) -> set[int]:
    """Return set of game_pks already successfully ingested."""
    rows = con.execute(
        "SELECT game_pk FROM ingest_log WHERE status = 'success'"
    ).fetchall()
    return {r[0] for r in rows}


def get_pbp_pending(con: sqlite3.Connection,
                    season: int = None) -> list[dict]:
    """
    Return games that have box scores (status='success', boxscore_rows>0)
    but no play-by-play yet (pbp_rows=0).
    Optionally filter by season.
    """
    season_clause = "AND g.season = :season" if season else ""
    rows = con.execute(f"""
        SELECT il.game_pk, g.game_date_et AS game_date, g.season
        FROM   ingest_log il
        JOIN   games g ON g.game_pk = il.game_pk
        WHERE  il.status        = 'success'
          AND  il.boxscore_rows > 0
          AND  il.pbp_rows      = 0
          AND  g.game_type      = 'R'
          {season_clause}
        ORDER  BY g.game_date_et
    """, {"season": season} if season else {}).fetchall()
    return [{"game_pk": r[0], "game_date": r[1], "season": r[2]} for r in rows]


def get_error_games(con: sqlite3.Connection) -> list[dict]:
    """Return games with status='error' for retry."""
    rows = con.execute("""
        SELECT il.game_pk, g.game_date_et AS game_date, g.season
        FROM ingest_log il
        JOIN games g ON g.game_pk = il.game_pk
        WHERE il.status = 'error'
        ORDER BY g.game_date_et
    """).fetchall()
    return [{"game_pk": r[0], "game_date": r[1], "season": r[2]} for r in rows]


def log_ingest_attempt(con: sqlite3.Connection, game_pk: int,
                       status: str, boxscore_rows: int = 0,
                       pbp_rows: int = 0, error_msg: str = None) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    con.execute("""
        INSERT INTO ingest_log
            (game_pk, first_attempted_utc, last_attempted_utc,
             status, attempts, boxscore_rows, pbp_rows, error_message)
        VALUES (?,?,?,?,1,?,?,?)
        ON CONFLICT(game_pk) DO UPDATE SET
            last_attempted_utc = excluded.last_attempted_utc,
            status             = excluded.status,
            attempts           = attempts + 1,
            boxscore_rows      = excluded.boxscore_rows,
            pbp_rows           = excluded.pbp_rows,
            error_message      = excluded.error_message
    """, (game_pk, now, now, status, boxscore_rows, pbp_rows, error_msg))
    con.commit()


# ══════════════════════════════════════════════════════════════════════════════
# MAIN INGESTION LOOP
# ══════════════════════════════════════════════════════════════════════════════

def ingest_games(con: sqlite3.Connection, games: list[dict],
                 load_pbp: bool = True, dry_run: bool = False,
                 verbose: bool = False) -> dict:
    """
    For each Final game: pull box score + play-by-play, write to DB.
    Skips games already successfully ingested.
    Returns summary dict.
    """
    already_done = get_already_ingested(con)
    pending      = [g for g in games if g["game_pk"] not in already_done]

    log.info("Games to ingest: %d  (skipping %d already done)",
             len(pending), len(games) - len(pending))

    if dry_run:
        for g in pending:
            log.info("  [dry-run] Would ingest game_pk=%d  %s",
                     g["game_pk"], g["game_date"])
        return {"pending": len(pending), "done": 0, "errors": 0}

    done   = 0
    errors = 0

    for i, g in enumerate(pending, 1):
        game_pk    = g["game_pk"]
        game_date  = g["game_date"]

        log.info("[%d/%d]  game_pk=%-8d  %s",
                 i, len(pending), game_pk, game_date)

        try:
            # Box score
            bs_rows = load_boxscore(con, game_pk, verbose)
            con.commit()

            # Play-by-play
            pbp_rows = 0
            if load_pbp:
                pbp_rows = load_play_by_play(con, game_pk, verbose)
                con.commit()

            log_ingest_attempt(con, game_pk, "success", bs_rows, pbp_rows)
            done += 1

            log.info("         ✓  boxscore=%d rows, pbp=%d rows",
                     bs_rows, pbp_rows)

        except Exception as e:
            con.rollback()
            log_ingest_attempt(con, game_pk, "error", error_msg=str(e))
            errors += 1
            log.error("         ✗  %s", e)

        # Be polite to the API
        if i < len(pending):
            time.sleep(REQUEST_PAUSE)

    return {"pending": len(pending), "done": done, "errors": errors}


# ══════════════════════════════════════════════════════════════════════════════
# INTEGRITY CHECK + ROW COUNTS
# ══════════════════════════════════════════════════════════════════════════════

def verify_integrity(con: sqlite3.Connection) -> None:
    """
    Re-enable FK enforcement and run a full integrity check.
    Call this after a backfill to confirm the DB is clean.
    Usage:  python load_mlb_stats.py --check
    """
    log.info("Running integrity check …")
    con.execute("PRAGMA foreign_keys = ON")

    result = con.execute("PRAGMA foreign_key_check").fetchall()
    if result:
        log.error("FK violations found:")
        for row in result:
            log.error("  table=%s rowid=%s parent=%s fkid=%s",
                      row[0], row[1], row[2], row[3])
    else:
        log.info("  ✓  No foreign key violations.")

    result2 = con.execute("PRAGMA integrity_check").fetchall()
    if result2 and result2[0][0] != "ok":
        log.error("Integrity issues:")
        for row in result2:
            log.error("  %s", row[0])
    else:
        log.info("  ✓  integrity_check passed.")


def show_row_counts(con: sqlite3.Connection) -> None:
    """Print row counts for every table — quick health check."""
    tables = [
        "seasons", "venues", "teams", "players",
        "games", "player_game_stats", "play_by_play", "standings",
        "game_odds", "player_props", "line_movement",
        "model_predictions", "backtest_results",
        "ingest_log", "odds_ingest_log",
    ]
    log.info("")
    log.info("%-30s %12s", "TABLE", "ROWS")
    log.info("-" * 44)
    for t in tables:
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            flag = "  ← empty" if n == 0 else ""
            log.info("  %-28s %10d%s", t, n, flag)
        except Exception as e:
            log.info("  %-28s  ERROR: %s", t, e)
    log.info("")



def show_season_stats(con, season: int) -> None:
    """Print season-to-date stats — quick progress snapshot."""
    log.info("")
    log.info("%-30s %12s", f"SEASON {season} TO DATE", "")
    log.info("-" * 44)

    def q(sql, *params):
        try:
            row = con.execute(sql, params).fetchone()
            return row[0] if row else 0
        except Exception:
            return 0

    games_final  = q("SELECT COUNT(*) FROM games WHERE season=? AND game_type='R' AND status='Final'", season)
    games_sched  = q("SELECT COUNT(*) FROM games WHERE season=? AND game_type='R' AND status='Scheduled'", season)
    games_total  = q("SELECT COUNT(*) FROM games WHERE season=? AND game_type='R'", season)
    with_wind    = q("SELECT COUNT(*) FROM games WHERE season=? AND game_type='R' AND status='Final' AND wind_mph IS NOT NULL", season)
    with_odds    = q("""SELECT COUNT(DISTINCT g.game_pk) FROM games g
                        JOIN game_odds go ON go.game_pk=g.game_pk
                         AND go.is_closing_line=1 AND go.market_type='moneyline'
                        WHERE g.season=? AND g.game_type='R' AND g.status='Final'""", season)
    with_starts  = q("""SELECT COUNT(DISTINCT gpp.game_pk) FROM game_probable_pitchers gpp
                        JOIN games g ON g.game_pk=gpp.game_pk
                        WHERE g.season=? AND g.game_type='R'""", season)
    bat_rows     = q("SELECT COUNT(*) FROM player_game_stats pgs JOIN games g ON g.game_pk=pgs.game_pk WHERE g.season=? AND pgs.player_role='batter'", season)
    pit_rows     = q("SELECT COUNT(*) FROM player_game_stats pgs JOIN games g ON g.game_pk=pgs.game_pk WHERE g.season=? AND pgs.player_role='pitcher'", season)
    odds_rows    = q("SELECT COUNT(*) FROM game_odds go JOIN games g ON g.game_pk=go.game_pk WHERE g.season=?", season)
    brief_picks  = q("""SELECT COUNT(*) FROM brief_picks bp
                        JOIN seasons s ON s.season=?
                        WHERE bp.game_date >= s.season_start
                          AND bp.game_date <= date('now')""", season)
    bank_delta   = q("""SELECT SUM(dp.pnl_dollars) FROM daily_pnl dp
                        JOIN games g ON g.game_pk=dp.game_pk WHERE g.season=?""", season) or 0.0
    pw           = q("""SELECT COUNT(*) FROM daily_pnl dp
                        JOIN games g ON g.game_pk=dp.game_pk
                        WHERE g.season=? AND dp.result='WIN'""", season)
    pl           = q("""SELECT COUNT(*) FROM daily_pnl dp
                        JOIN games g ON g.game_pk=dp.game_pk
                        WHERE g.season=? AND dp.result='LOSS'""", season)

    pct = lambda n, d: f"{n/d*100:.0f}%" if d else "—"

    log.info("  %-28s %10s", "Games Final",          f"{games_final} / {games_total}")
    log.info("  %-28s %10s", "Games Remaining",      str(games_sched))
    log.info("  %-28s %10s", "Wind data coverage",   f"{with_wind} ({pct(with_wind, games_final)})")
    log.info("  %-28s %10s", "Closing odds coverage",f"{with_odds} ({pct(with_odds, games_final)})")
    log.info("  %-28s %10s", "Starters on file",     f"{with_starts} ({pct(with_starts, games_total)})")
    log.info("  %-28s %10s", "Batting rows",         str(bat_rows))
    log.info("  %-28s %10s", "Pitching rows",        str(pit_rows))
    log.info("  %-28s %10s", "Odds rows",            str(odds_rows))
    log.info("  %-28s %10s", "Confirmed picks",      str(brief_picks))
    log.info("  %-28s %10s", "Paper bank",           f"${500 + bank_delta:.2f}  (W:{pw} L:{pl})")
    log.info("")


# ══════════════════════════════════════════════════════════════════════════════
# CLI ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Load MLB Stats API data into mlb_stats.db",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  python load_mlb_stats.py
      Load yesterday's games (default daily run).

  python load_mlb_stats.py --season 2025
      Backfill the full 2025 season (or any year back to 2015).

  python load_mlb_stats.py --start 2024-07-01 --end 2024-07-31
      Load a specific month.

  python load_mlb_stats.py --season 2023 --no-pbp
      Load box scores only, no play-by-play (much faster).

  python load_mlb_stats.py --retry-errors
      Re-try any games that previously failed.
""")
    p.add_argument("--start",         help="Start date YYYY-MM-DD")
    p.add_argument("--end",           help="End date YYYY-MM-DD")
    p.add_argument("--season",        type=int, help="Load full season")
    p.add_argument("--no-pbp",        action="store_true",
                                      help="Skip play-by-play (faster)")
    p.add_argument("--pbp-only",      action="store_true",
                                      help="Load play-by-play ONLY for games that have "
                                           "box scores but pbp_rows=0. Safe to run after "
                                           "a --no-pbp backfill.")
    p.add_argument("--retry-errors",  action="store_true",
                                      help="Retry previously failed games")
    p.add_argument("--load-teams",    action="store_true",
                                      help="Re-load teams/venues (run on first setup)")
    p.add_argument("--load-players",  action="store_true",
                                      help="Re-load player roster")
    p.add_argument("--no-players",    action="store_true",
                                      help="Skip player roster load (faster; players table may be incomplete)")
    p.add_argument("--db",            default=DEFAULT_DB,
                                      help=f"Database path (default: {DEFAULT_DB})")
    p.add_argument("--dry-run",       action="store_true",
                                      help="Print plan, don't write anything")
    p.add_argument("--check",         action="store_true",
                                      help="Show row counts and run FK integrity check, then exit")
    p.add_argument("--verbose", "-v", action="store_true",
                                      help="Show each API call")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    # Rotate load_mlb_stats logs — keep last 7 days
    import datetime as _dt2
    from pathlib import Path as _Path
    _today = _dt2.date.today().isoformat()
    rotate_logs("load_mlb_stats", date_str=_today,
                script_path=_Path(__file__))
    configure_logging(args.verbose)

    log.info("=" * 60)
    log.info("  MLB Stats Loader")
    log.info("=" * 60)

    con = get_connection(args.db)

    # --check: just show counts and integrity, then exit
    if args.check:
        show_row_counts(con)
        verify_integrity(con)
        con.close()
        return

    # ── Determine date range ───────────────────────────────────────────────
    if args.pbp_only:
        # ── PBP-only mode ─────────────────────────────────────────────────
        pbp_season = args.season if args.season else None
        if pbp_season:
            log.info("Mode: Play-by-play only — season %d", pbp_season)
        else:
            log.info("Mode: Play-by-play only — all seasons with missing pbp")
        games = get_pbp_pending(con, pbp_season)
        if not games:
            log.info("No games with missing play-by-play found.")
            show_row_counts(con)
            con.close()
            return
        log.info("Found %d games with box scores but no play-by-play.", len(games))
        # Force load_pbp=True; override ingest skip logic by temporarily
        # resetting pbp_rows=0 games to allow re-entry — we use a
        # dedicated path that doesn't call get_already_ingested()
        done = 0; errors = 0
        for i, g in enumerate(games, 1):
            game_pk   = g["game_pk"]
            game_date = g["game_date"]
            log.info("[%d/%d]  game_pk=%-8d  %s", i, len(games), game_pk, game_date)
            try:
                pbp_rows = load_play_by_play(con, game_pk, args.verbose)
                con.commit()
                # Update existing ingest_log row — mark pbp loaded
                con.execute("""
                    UPDATE ingest_log
                    SET pbp_rows           = ?,
                        last_attempted_utc = ?
                    WHERE game_pk = ?
                """, (pbp_rows, datetime.now(timezone.utc).replace(tzinfo=None).isoformat(), game_pk))
                con.commit()
                done += 1
                log.info("         ✓  pbp=%d rows", pbp_rows)
            except Exception as e:
                con.rollback()
                log.error("         ✗  %s", e)
                errors += 1
            if i < len(games):
                time.sleep(REQUEST_PAUSE)
        result = {"pending": len(games), "done": done, "errors": errors}

    elif args.retry_errors:
        log.info("Mode: Retry errors")
        games = get_error_games(con)
        if not games:
            log.info("No failed games to retry.")
            con.close()
            return
        log.info("Found %d games with errors to retry.", len(games))
        result = ingest_games(con, games,
                              load_pbp=not args.no_pbp,
                              dry_run=args.dry_run,
                              verbose=args.verbose)

    else:
        # Determine start/end dates
        if args.season:
            season = args.season
            # Fetch season dates from the seasons table or use defaults
            row = con.execute(
                "SELECT season_start, season_end FROM seasons WHERE season=?",
                (season,)
            ).fetchone()
            if row:
                start_date = row["season_start"]
                end_date   = row["season_end"]
            else:
                start_date = f"{season}-03-20"
                end_date   = f"{season}-11-30"
        elif args.start and args.end:
            start_date = args.start
            end_date   = args.end
            season     = int(start_date[:4])
        else:
            # Default: yesterday
            yesterday  = date.today() - timedelta(days=1)
            start_date = yesterday.isoformat()
            end_date   = yesterday.isoformat()
            season     = yesterday.year

        log.info("Date range: %s → %s  (season %d)", start_date, end_date, season)

        # ── Ensure new tables/columns exist (idempotent migrations) ─────
        ensure_probable_pitchers_table(con)

        # ── Bootstrap: always load teams on first run ───────────────────
        # If the teams table is empty, load_schedule will fail FK checks.
        # We auto-run load_teams_and_venues when teams aren't loaded yet,
        # so the user never has to remember to pass --load-teams.
        team_count = con.execute("SELECT COUNT(*) FROM teams").fetchone()[0]
        if team_count == 0 or args.load_teams:
            if team_count == 0:
                log.info("Teams table is empty — auto-loading teams and venues first.")
            load_teams_and_venues(con, args.verbose)

        if (args.load_players or args.season) and not args.no_players:
            load_players_for_season(con, season, args.verbose)
        elif args.no_players:
            log.info("Skipping player roster load (--no-players).")

        # ── Load schedule ─────────────────────────────────────────────────
        games = load_schedule(con, start_date, end_date, args.verbose)

        # ── Load standings (once per date, for the last date in range) ────
        if games and not args.dry_run:
            standings_date = end_date
            rows = load_standings(con, standings_date, season, args.verbose)
            log.info("Standings: %d rows for %s", rows, standings_date)

        # ── Ingest box scores + play-by-play ─────────────────────────────
        result = ingest_games(con, games,
                              load_pbp=not args.no_pbp,
                              dry_run=args.dry_run,
                              verbose=args.verbose)

    # ── Summary ───────────────────────────────────────────────────────────────
    log.info("")
    log.info("=" * 60)
    log.info("  SUMMARY")
    log.info("=" * 60)
    log.info("  Games pending   : %d", result["pending"])
    log.info("  Games ingested  : %d", result["done"])
    log.info("  Errors          : %d", result["errors"])
    if result["errors"]:
        log.info("  → Run with --retry-errors to retry failed games.")

    # Season-to-date snapshot then all-time table counts
    import datetime as _dt
    _season = int(args.start[:4]) if getattr(args, "start", None) else _dt.date.today().year
    show_season_stats(con, _season)
    show_row_counts(con)

    con.close()


if __name__ == "__main__":
    main()
