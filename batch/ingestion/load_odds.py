"""
load_odds.py
────────────
Loads Vegas odds from The Odds API into mlb_stats.db.

Markets pulled:
  game_odds table    : h2h (moneyline), spreads (run line), totals
  game_odds_f5 table : h2h_1st_5_innings, spreads_1st_5_innings,
                       totals_1st_5_innings
  player_props table : pitcher_strikeouts, batter_hits,
                       batter_home_runs, batter_total_bases,
                       batter_rbis, batter_runs_scored,
                       batter_stolen_bases

Modes:
  --pregame              Pull today's upcoming games (run 4x/day during season)
  --historical           Backfill historical odds for a date range or season
  --compute-movement     Compute line_movement for completed games
  --check                Show odds_ingest_log summary and row counts

Usage:
  python load_odds.py --pregame
  python load_odds.py --pregame --markets game f5 props
  python load_odds.py --historical --season 2023
  python load_odds.py --historical --start 2024-04-01 --end 2024-04-30
  python load_odds.py --compute-movement
  python load_odds.py --check

Requirements:
  pip install requests python-dotenv

API key:
  Set THE_ODDS_API_KEY in `config/.env` (recommended), repo root `.env`,
  or as an environment variable. Get a key at https://the-odds-api.com
"""

# CHANGE LOG (latest first)
# -------------------------
# 2026-04-13 22:15 ET  Default DB from get_db_path(); repo root on sys.path for core.* imports.
# 2026-04-13 16:24 ET  Refactor: route sqlite3.connect() calls through core.db.connection.connect().

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Optional

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import requests
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

# ── Configuration ──────────────────────────────────────────────────────────────
DEFAULT_DB   = get_db_path()
API_BASE     = "https://api.the-odds-api.com/v4"
SPORT        = "baseball_mlb"
# Use specific bookmakers instead of region to control exactly which books
# are pulled. DraftKings + FanDuel + BetMGM = 3 books = 1 quota unit.
# This is the same quota cost as regions=us but stores 73% fewer rows.
BOOKMAKERS   = "draftkings,fanduel,betmgm"
REGIONS      = "us"          # kept as fallback — not used in pregame pull
ODDS_FORMAT  = "american"

# Markets pulled per endpoint type
GAME_MARKETS    = ["h2h", "spreads", "totals"]
F5_MARKETS      = ["h2h_1st_5_innings", "spreads_1st_5_innings",
                   "totals_1st_5_innings"]
PROP_MARKETS    = [
    "pitcher_strikeouts",
    "batter_hits",
    "batter_home_runs",
    "batter_total_bases",
    "batter_rbis",
    "batter_runs_scored",
    "batter_stolen_bases",
]

# Polite pause between per-event API calls (props/f5 per-event endpoint)
REQUEST_PAUSE = 0.5

# Steam move threshold: if line moves more than this many cents within
# STEAM_WINDOW_HOURS, flag as steam move
STEAM_CENTS_THRESHOLD = 10
STEAM_WINDOW_HOURS    = 2

log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════

def configure_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════════════════════

def get_connection(db_path: str) -> sqlite3.Connection:
    if not os.path.exists(db_path):
        log.error("Database not found: %s", db_path)
        log.error("To initialize the DB, apply `core/db/schema.sql` and then run:")
        log.error("  python batch/ingestion/add_f5_table.py --db \"%s\"", db_path)
        sys.exit(1)
    # timeout=30: wait up to 30 seconds for any write lock to clear before failing.
    # This allows load_odds.py to run while Scout (Streamlit) is open, since WAL
    # mode permits one writer + multiple readers simultaneously.
    con = db_connect(db_path, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode = WAL")
    con.execute("PRAGMA foreign_keys = OFF")   # off during bulk inserts
    con.execute("PRAGMA cache_size   = -64000")
    return con


def show_odds_counts(con: sqlite3.Connection):
    tables = [
        "game_odds", "game_odds_f5", "player_props",
        "line_movement", "odds_ingest_log",
    ]
    log.info("")
    log.info("  %-28s  %s", "TABLE", "ROWS")
    log.info("  " + "-" * 44)
    for t in tables:
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            flag = "  ← empty" if n == 0 else ""
            log.info("  %-28s  %d%s", t, n, flag)
        except Exception as e:
            log.info("  %-28s  ERROR: %s", t, e)
    log.info("")

    # Odds ingest log summary
    rows = con.execute("""
        SELECT pull_type,
               COUNT(*) AS pulls,
               SUM(odds_rows_inserted) AS odds_rows,
               MIN(pulled_at_utc) AS first_pull,
               MAX(pulled_at_utc) AS last_pull
        FROM odds_ingest_log
        GROUP BY pull_type
        ORDER BY last_pull DESC
    """).fetchall()
    if rows:
        log.info("  Ingest log by pull type:")
        for r in rows:
            log.info("    %-22s  pulls=%-4d  odds_rows=%-8s  last=%s",
                     r["pull_type"], r["pulls"],
                     r["odds_rows"] or 0, r["last_pull"])
        log.info("")


# ══════════════════════════════════════════════════════════════════════════════
# API HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def get_api_key() -> str:
    key = os.environ.get("THE_ODDS_API_KEY", "").strip()
    if not key:
        log.error("THE_ODDS_API_KEY not set.")
        log.error("Add it to `config/.env` (recommended) or repo root `.env`:")
        log.error("  THE_ODDS_API_KEY=your_key_here")
        log.error("Or set it as an environment variable.")
        sys.exit(1)
    return key


def api_get(url: str, params: dict, verbose: bool = False) -> Optional[dict]:
    """GET from The Odds API.  Returns (data, headers) or (None, {})."""
    if verbose:
        safe_params = {k: v for k, v in params.items() if k != "apiKey"}
        log.debug("GET %s params=%s", url, safe_params)
    try:
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 401:
            log.error("API key invalid or expired (401).")
            return None, {}
        if resp.status_code == 422:
            try:
                detail = resp.json()
            except Exception:
                detail = resp.text[:200]
            log.warning("Unprocessable request (422) — params may be invalid: %s | detail: %s",
                        url, detail)
            return None, resp.headers
        if resp.status_code == 429:
            log.warning("Rate limited (429) — quota exceeded or too many requests.")
            return None, resp.headers
        resp.raise_for_status()
        return resp.json(), resp.headers
    except requests.RequestException as e:
        log.error("Request failed: %s", e)
        return None, {}


def quota_from_headers(headers: dict) -> dict:
    """Extract quota info from Odds API response headers.

    x-requests-used:      cumulative total used since quota reset (NOT per-call cost)
    x-requests-last:      cost of THIS specific API call
    x-requests-remaining: quota remaining after this call
    """
    return {
        "requests_used":      int(headers.get("x-requests-last",      0)),  # per-call cost
        "requests_remaining": int(headers.get("x-requests-remaining", 0)),
    }


# ══════════════════════════════════════════════════════════════════════════════
# GAME MATCHING — link Odds API events to DB game_pk
# ══════════════════════════════════════════════════════════════════════════════

def build_game_lookup(con: sqlite3.Connection,
                      game_date: str) -> dict:
    """
    Return a lookup dict mapping Odds API events to DB game rows.

    KEY DESIGN (revised):
    The original date-agnostic (home_abbr, away_abbr) key caused wrong-game
    matches when the same teams play on consecutive days (e.g. CLE hosts KC on
    April 7 AND April 8). The +1 day query fetches both games, and the last
    one written wins — routing April 7 Odds API events to April 8 game_pks.

    Fix: key on (home_abbr, away_abbr, utc_date) where utc_date is the DATE
    portion of game_start_utc. The Odds API commence_time is in UTC, and
    game_start_utc is also in UTC — so they share the same date reference.
    Late West Coast games (e.g. PHI@SF starting 01:45 UTC = Apr 8 UTC) are
    stored with game_start_utc = 2026-04-08T01:45:00 and game_date = 2026-04-07.
    The Odds API will show commence_time date = 2026-04-08 for these games.
    Keying on utc_date handles this correctly without the overwrite problem.

    Keys returned:
      (home_abbr, away_abbr, utc_date)  → game row dict  [primary]
      (word, word2, game_date)           → game row dict  [fuzzy fallback]
    """
    from datetime import date as _date, timedelta
    try:
        d = _date.fromisoformat(game_date)
        next_day = (d + timedelta(days=1)).isoformat()
    except ValueError:
        next_day = game_date

    rows = con.execute("""
        SELECT g.game_pk, g.game_date,
               th.abbreviation AS home_abbr, th.name AS home_name,
               ta.abbreviation AS away_abbr, ta.name AS away_name,
               g.game_start_utc
        FROM   games g
        JOIN   teams th ON th.team_id = g.home_team_id
        JOIN   teams ta ON ta.team_id = g.away_team_id
        WHERE  g.game_date IN (?, ?)
          AND  g.game_type = 'R'
    """, (game_date, next_day)).fetchall()

    # Split into today-games and next-day-games.
    # Process today first so that cross-midnight games (game_date=today,
    # game_start_utc=tomorrow) are registered under their UTC key BEFORE
    # tomorrow's genuine games are processed.
    # This prevents tomorrow's BOS@STL from overwriting tonight's BOS@STL
    # when both produce the same (home, away, utc_date) key.
    today_rows    = [r for r in rows if r["game_date"] == game_date]
    tomorrow_rows = [r for r in rows if r["game_date"] != game_date]

    lookup = {}

    def _add_row(r, allow_overwrite):
        start_utc = r["game_start_utc"] or ""
        utc_date  = start_utc[:10] if start_utc else r["game_date"]

        # PRIMARY key: team pair + full game_start_utc (most precise).
        # This uniquely identifies each game even when two games between
        # the same teams share the same UTC date (e.g. BOS@STL 00:15 UTC
        # and BOS@STL 23:15 UTC both fall on 2026-04-11).
        key_full  = (r["home_abbr"].upper(), r["away_abbr"].upper(), start_utc)
        if allow_overwrite or key_full not in lookup:
            lookup[key_full] = dict(r)

        # SECONDARY key: team pair + utc_date (matches Odds API event_utc_date).
        # Only set if not already claimed — today's games take priority.
        # For a doubleheader on the same UTC date, whichever game runs first
        # in today_rows claims this key; the second game is only reachable
        # via the full key above. The Odds API includes commence_time which
        # lets match_event_to_game use the full key.
        key_utc = (r["home_abbr"].upper(), r["away_abbr"].upper(), utc_date)
        if allow_overwrite or key_utc not in lookup:
            lookup[key_utc] = dict(r)

        # TERTIARY key: team pair + game_date (local calendar date).
        # For cross-midnight games so brief/check can find by local date.
        if utc_date != r["game_date"]:
            key_local = (r["home_abbr"].upper(), r["away_abbr"].upper(), r["game_date"])
            if allow_overwrite or key_local not in lookup:
                lookup[key_local] = dict(r)

        # Fuzzy name fallback keyed on game_date
        for word in r["home_name"].split():
            for word2 in r["away_name"].split():
                k = (word.lower(), word2.lower(), r["game_date"])
                if k not in lookup:
                    lookup[k] = dict(r)

    # Today's games first — they take priority and cannot be overwritten
    for r in today_rows:
        _add_row(r, allow_overwrite=True)

    # Tomorrow's games second — only fill in keys not already set by today
    for r in tomorrow_rows:
        _add_row(r, allow_overwrite=False)

    return lookup


# Team name fragments from The Odds API → MLB abbreviations
# The Odds API uses city+nickname format e.g. "New York Yankees"
ODDS_API_TEAM_MAP = {
    "yankees": "NYY", "red sox": "BOS", "blue jays": "TOR",
    "rays": "TB",   "orioles": "BAL",
    "white sox": "CWS", "guardians": "CLE", "tigers": "DET",
    "royals": "KC",  "twins": "MIN",
    "astros": "HOU", "athletics": "ATH", "a's": "ATH",
    "rangers": "TEX", "angels": "LAA", "mariners": "SEA",
    "braves": "ATL", "marlins": "MIA", "mets": "NYM",
    "phillies": "PHI", "nationals": "WSH",
    "cubs": "CHC",   "reds": "CIN",   "brewers": "MIL",
    "pirates": "PIT", "cardinals": "STL",
    "diamondbacks": "ARI", "rockies": "COL",
    "dodgers": "LAD", "padres": "SD",  "giants": "SF",
}

def resolve_team_abbr(odds_team_name: str) -> Optional[str]:
    """Map an Odds API team name to our MLB abbreviation."""
    lower = odds_team_name.lower()
    for fragment, abbr in ODDS_API_TEAM_MAP.items():
        if fragment in lower:
            return abbr
    return None


def match_event_to_game(event: dict, game_date: str,
                        lookup: dict) -> Optional[dict]:
    """
    Try to find the DB game row matching an Odds API event.
    Returns the game row dict or None.

    Uses the event commence_time UTC date as the primary key component.
    This correctly handles both:
      - Same teams playing consecutive days (prevents wrong-game match)
      - West Coast games where commence_time UTC date != local game_date
    """
    home_name = event.get("home_team", "")
    away_name = event.get("away_team", "")
    home_abbr = resolve_team_abbr(home_name)
    away_abbr = resolve_team_abbr(away_name)

    # Extract UTC date from the event's commence_time
    commence_time = event.get("commence_time", "")
    event_utc_date = commence_time[:10] if commence_time else game_date

    if home_abbr and away_abbr:
        # Most precise: match on team pair + full commence_time string.
        # This uniquely identifies a game even when two games between the
        # same teams share the same UTC date (e.g. BOS@STL 00:15 UTC and
        # BOS@STL 23:15 UTC). The Odds API commence_time maps directly to
        # game_start_utc stored in the DB.
        key_full = (home_abbr, away_abbr, commence_time[:19] if commence_time else "")
        if key_full in lookup:
            return lookup[key_full]

        # Secondary: team pair + UTC date (handles most cases)
        key_utc = (home_abbr, away_abbr, event_utc_date)
        if key_utc in lookup:
            return lookup[key_utc]

        # Tertiary: team pair + local game_date
        key_local = (home_abbr, away_abbr, game_date)
        if key_local in lookup:
            return lookup[key_local]

    # Last resort: fuzzy name match on game_date
    for hw in home_name.split():
        for aw in away_name.split():
            key = (hw.lower(), aw.lower(), game_date)
            if key in lookup:
                return lookup[key]

    log.warning("  Could not match event to DB game: %s vs %s on %s",
                home_name, away_name, game_date)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# AMERICAN ODDS HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def implied_prob(american_odds: int) -> float:
    """Convert American odds to implied probability (0–1)."""
    if american_odds is None:
        return 0.0
    if american_odds > 0:
        return 100 / (american_odds + 100)
    else:
        return abs(american_odds) / (abs(american_odds) + 100)


def ml_move_cents(open_odds: int, close_odds: int) -> Optional[int]:
    """
    Net movement in implied probability cents toward the home team.
    Positive = market moved toward home (home became more favoured).
    """
    if open_odds is None or close_odds is None:
        return None
    open_prob  = implied_prob(open_odds)
    close_prob = implied_prob(close_odds)
    return round((close_prob - open_prob) * 100)


# ══════════════════════════════════════════════════════════════════════════════
# PARSE ODDS API RESPONSES
# ══════════════════════════════════════════════════════════════════════════════

def parse_game_markets(bookmaker: dict, game_pk: int,
                       captured_utc: str,
                       hours_before: Optional[float]) -> list[dict]:
    """
    Parse one bookmaker's markets from an event response.
    Returns list of dicts ready to INSERT into game_odds.
    """
    rows = []
    for market in bookmaker.get("markets", []):
        key = market.get("key", "")
        outcomes = {o["name"]: o for o in market.get("outcomes", [])}

        row = {
            "game_pk":          game_pk,
            "bookmaker":        bookmaker["key"],
            "captured_at_utc":  captured_utc,
            "hours_before_game": hours_before,
            "home_ml": None, "away_ml": None,
            "home_rl_line": None, "home_rl_odds": None,
            "away_rl_line": None, "away_rl_odds": None,
            "total_line": None, "over_odds": None, "under_odds": None,
            "is_opening_line": 0, "is_closing_line": 0,
        }

        if key == "h2h":
            row["market_type"] = "moneyline"
            # Match outcomes by team name against the event home_team/away_team.
            # The Odds API returns outcomes alphabetically — NOT home-first.
            # Relying on position causes home_ml/away_ml to be swapped whenever
            # the away team name sorts before the home team name (e.g. BAL@CWS).
            home_team_name = bookmaker.get("_home_team", "")
            away_team_name = bookmaker.get("_away_team", "")
            for o in market.get("outcomes", []):
                name  = o.get("name", "")
                price = o.get("price")
                if name == home_team_name:
                    row["home_ml"] = price
                elif name == away_team_name:
                    row["away_ml"] = price
            # Fallback: if names didn't match (name format mismatch),
            # use position — but log a warning so we can fix the mapping.
            if row["home_ml"] is None and row["away_ml"] is None:
                outcome_list = market.get("outcomes", [])
                if len(outcome_list) >= 2:
                    log.debug("  h2h name-match fallback for game_pk=%s "
                              "(home=%s away=%s outcomes=%s)",
                              game_pk, home_team_name, away_team_name,
                              [o.get("name") for o in outcome_list])
                    row["home_ml"] = outcome_list[0].get("price")
                    row["away_ml"] = outcome_list[1].get("price")

        elif key == "spreads":
            row["market_type"] = "runline"
            for o in market.get("outcomes", []):
                point = o.get("point")
                price = o.get("price")
                if point is not None and point < 0:
                    row["home_rl_line"] = point
                    row["home_rl_odds"] = price
                elif point is not None and point > 0:
                    row["away_rl_line"] = point
                    row["away_rl_odds"] = price

        elif key == "totals":
            row["market_type"] = "total"
            for o in market.get("outcomes", []):
                name  = o.get("name", "").lower()
                price = o.get("price")
                point = o.get("point")
                if name == "over":
                    row["total_line"] = point
                    row["over_odds"]  = price
                elif name == "under":
                    row["under_odds"] = price

        else:
            continue  # skip unknown market keys

        rows.append(row)

    return rows


def parse_f5_markets(bookmaker: dict, game_pk: int,
                     captured_utc: str,
                     hours_before: Optional[float]) -> Optional[dict]:
    """
    Parse F5 markets for one bookmaker.
    Returns a single dict ready to INSERT into game_odds_f5, or None.
    """
    row = {
        "game_pk": game_pk, "bookmaker": bookmaker["key"],
        "captured_at_utc": captured_utc, "hours_before_game": hours_before,
        "home_f5_ml": None, "away_f5_ml": None,
        "home_f5_rl_line": None, "home_f5_rl_odds": None,
        "away_f5_rl_line": None, "away_f5_rl_odds": None,
        "f5_total_line": None, "f5_over_odds": None, "f5_under_odds": None,
        "is_opening_line": 0, "is_closing_line": 0,
    }
    found = False

    for market in bookmaker.get("markets", []):
        key = market.get("key", "")
        outcomes = market.get("outcomes", [])

        if key == "h2h_1st_5_innings":
            found = True
            if len(outcomes) >= 2:
                row["home_f5_ml"] = outcomes[0].get("price")
                row["away_f5_ml"] = outcomes[1].get("price")

        elif key == "spreads_1st_5_innings":
            found = True
            for o in outcomes:
                point = o.get("point")
                price = o.get("price")
                if point is not None and point < 0:
                    row["home_f5_rl_line"] = point
                    row["home_f5_rl_odds"] = price
                elif point is not None and point > 0:
                    row["away_f5_rl_line"] = point
                    row["away_f5_rl_odds"] = price

        elif key == "totals_1st_5_innings":
            found = True
            for o in outcomes:
                name  = o.get("name", "").lower()
                price = o.get("price")
                point = o.get("point")
                if name == "over":
                    row["f5_total_line"] = point
                    row["f5_over_odds"]  = price
                elif name == "under":
                    row["f5_under_odds"] = price

    return row if found else None


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE WRITES
# ══════════════════════════════════════════════════════════════════════════════

def upsert_game_odds_row(con: sqlite3.Connection, row: dict) -> int:
    """
    Insert one game_odds row.
    Sets is_opening_line=1 if this is the first snapshot for game+book+market.
    Clears is_closing_line on all prior rows BEFORE inserting, then sets it on
    the new row. Clearing first ensures only one closing-line row exists even if
    a prior session's commit left stale flags.
    Returns 1 if inserted, 0 if duplicate (already exists).

    IN-GAME GUARD: If hours_before_game is negative (game has already started),
    the row is NOT written. The API returns live in-game lines for active games
    which would overwrite the true pre-game closing line and corrupt CLV.
    The existing is_closing_line=1 row (from the last pre-game pull) is preserved.
    """
    # ── In-game guard: reject odds pulled after game start ────────────────
    hours = row.get("hours_before_game")
    if hours is not None and hours < 0:
        # Game has already started — preserve the last pre-game closing line
        log.info("  In-game guard: skipping game_pk=%s market=%s (%s, %.2f hrs past start)",
                 row.get("game_pk"), row.get("market_type"),
                 row.get("bookmaker"), abs(hours))
        return 0

    # Is this the first snapshot for this game+book+market?
    existing = con.execute("""
        SELECT COUNT(*) FROM game_odds
        WHERE game_pk = ? AND bookmaker = ? AND market_type = ?
    """, (row["game_pk"], row["bookmaker"], row["market_type"])).fetchone()[0]

    is_opening = 1 if existing == 0 else 0

    # Clear closing flag on ALL existing rows for this game+book+market BEFORE
    # inserting the new row. This guarantees only one is_closing_line=1 row exists
    # regardless of how many prior pulls ran.
    con.execute("""
        UPDATE game_odds SET is_closing_line = 0
        WHERE game_pk = ? AND bookmaker = ? AND market_type = ?
    """, (row["game_pk"], row["bookmaker"], row["market_type"]))

    try:
        con.execute("""
            INSERT OR IGNORE INTO game_odds
                (game_pk, bookmaker, data_source, captured_at_utc, hours_before_game,
                 market_type, home_ml, away_ml,
                 home_rl_line, home_rl_odds, away_rl_line, away_rl_odds,
                 total_line, over_odds, under_odds,
                 is_opening_line, is_closing_line)
            VALUES
                (:game_pk, :bookmaker, 'the-odds-api', :captured_at_utc, :hours_before_game,
                 :market_type, :home_ml, :away_ml,
                 :home_rl_line, :home_rl_odds, :away_rl_line, :away_rl_odds,
                 :total_line, :over_odds, :under_odds,
                 :is_opening, 1)
        """, {**row, "is_opening": is_opening})

        if con.execute("SELECT changes()").fetchone()[0] == 0:
            # Duplicate — row already exists for this exact timestamp.
            # Still need to re-set closing flag on it since we cleared all above.
            con.execute("""
                UPDATE game_odds SET is_closing_line = 1
                WHERE game_pk = ? AND bookmaker = ? AND market_type = ?
                  AND captured_at_utc = ?
            """, (row["game_pk"], row["bookmaker"],
                  row["market_type"], row["captured_at_utc"]))
            return 0

        if is_opening:
            con.execute("""
                UPDATE game_odds SET is_opening_line = 1
                WHERE game_pk = ? AND bookmaker = ? AND market_type = ?
                  AND captured_at_utc = ?
            """, (row["game_pk"], row["bookmaker"],
                  row["market_type"], row["captured_at_utc"]))

        return 1

    except sqlite3.IntegrityError:
        return 0


def upsert_f5_row(con: sqlite3.Connection, row: dict) -> int:
    """
    Insert one game_odds_f5 row with opening/closing flag management.
    Clears is_closing_line on all prior rows BEFORE inserting.
    """
    existing = con.execute("""
        SELECT COUNT(*) FROM game_odds_f5
        WHERE game_pk = ? AND bookmaker = ?
    """, (row["game_pk"], row["bookmaker"])).fetchone()[0]

    is_opening = 1 if existing == 0 else 0

    # Clear closing flag before insert to guarantee single closing-line row
    con.execute("""
        UPDATE game_odds_f5 SET is_closing_line = 0
        WHERE game_pk = ? AND bookmaker = ?
    """, (row["game_pk"], row["bookmaker"]))

    try:
        con.execute("""
            INSERT OR IGNORE INTO game_odds_f5
                (game_pk, bookmaker, data_source, captured_at_utc, hours_before_game,
                 home_f5_ml, away_f5_ml,
                 home_f5_rl_line, home_f5_rl_odds,
                 away_f5_rl_line, away_f5_rl_odds,
                 f5_total_line, f5_over_odds, f5_under_odds,
                 is_opening_line, is_closing_line)
            VALUES
                (:game_pk, :bookmaker, 'the-odds-api', :captured_at_utc, :hours_before_game,
                 :home_f5_ml, :away_f5_ml,
                 :home_f5_rl_line, :home_f5_rl_odds,
                 :away_f5_rl_line, :away_f5_rl_odds,
                 :f5_total_line, :f5_over_odds, :f5_under_odds,
                 :is_opening, 1)
        """, {**row, "is_opening": is_opening})

        if con.execute("SELECT changes()").fetchone()[0] == 0:
            # Duplicate timestamp — re-set closing flag on the existing row
            con.execute("""
                UPDATE game_odds_f5 SET is_closing_line = 1
                WHERE game_pk = ? AND bookmaker = ? AND captured_at_utc = ?
            """, (row["game_pk"], row["bookmaker"], row["captured_at_utc"]))
            return 0

        return 1

    except sqlite3.IntegrityError:
        return 0


def upsert_player_prop(con: sqlite3.Connection,
                       game_pk: int, player_id: int,
                       bookmaker: str, prop_type: str,
                       line: float, over_odds: Optional[int],
                       under_odds: Optional[int],
                       captured_utc: str,
                       hours_before: Optional[float]) -> int:
    """Insert a player prop snapshot with closing line management."""
    existing = con.execute("""
        SELECT COUNT(*) FROM player_props
        WHERE game_pk = ? AND player_id = ? AND bookmaker = ? AND prop_type = ?
    """, (game_pk, player_id, bookmaker, prop_type)).fetchone()[0]

    try:
        con.execute("""
            INSERT OR IGNORE INTO player_props
                (game_pk, player_id, bookmaker, data_source,
                 captured_at_utc, hours_before_game,
                 prop_type, line, over_odds, under_odds, is_closing_line)
            VALUES (?,?,?,'the-odds-api',?,?,?,?,?,?,1)
        """, (game_pk, player_id, bookmaker, captured_utc, hours_before,
              prop_type, line, over_odds, under_odds))

        if con.execute("SELECT changes()").fetchone()[0] == 0:
            return 0

        # Clear prior closing flags
        con.execute("""
            UPDATE player_props SET is_closing_line = 0
            WHERE game_pk = ? AND player_id = ? AND bookmaker = ?
              AND prop_type = ? AND captured_at_utc != ?
        """, (game_pk, player_id, bookmaker, prop_type, captured_utc))

        return 1

    except sqlite3.IntegrityError:
        return 0


def log_odds_ingest(con: sqlite3.Connection, pull_type: str,
                    markets: list, games_covered: int,
                    odds_rows: int, props_rows: int,
                    api_used: int, api_remaining: int,
                    status: str, error: str = None):
    con.execute("""
        INSERT INTO odds_ingest_log
            (pulled_at_utc, pull_type, sport, markets_pulled,
             games_covered, odds_rows_inserted, props_rows_inserted,
             api_requests_used, api_quota_remaining, status, error_message)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (datetime.now(timezone.utc).replace(tzinfo=None).isoformat(), pull_type, SPORT,
          ",".join(markets), games_covered, odds_rows, props_rows,
          api_used, api_remaining, status, error))
    con.commit()


# ══════════════════════════════════════════════════════════════════════════════
# PLAYER LOOKUP
# ══════════════════════════════════════════════════════════════════════════════

_player_cache: dict = {}  # full_name → player_id

def lookup_player(con: sqlite3.Connection,
                  full_name: str) -> Optional[int]:
    """Find player_id by full name.  Caches results in memory."""
    if full_name in _player_cache:
        return _player_cache[full_name]
    row = con.execute(
        "SELECT player_id FROM players WHERE full_name = ? LIMIT 1",
        (full_name,)
    ).fetchone()
    if row:
        pid = row["player_id"]
        _player_cache[full_name] = pid
        return pid
    # Try last name only
    last = full_name.split()[-1] if full_name else ""
    row = con.execute(
        "SELECT player_id FROM players WHERE last_name = ? LIMIT 1",
        (last,)
    ).fetchone()
    if row:
        pid = row["player_id"]
        _player_cache[full_name] = pid
        return pid
    log.debug("  Player not found in DB: %s", full_name)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# PREGAME PULL  (run multiple times per day during season)
# ══════════════════════════════════════════════════════════════════════════════

def pull_pregame(con: sqlite3.Connection, api_key: str,
                 markets_flag: list, target_date: str,
                 verbose: bool = False) -> dict:
    """
    Pull odds for today's upcoming games.
    markets_flag: subset of ['game','f5','props'] or all three.
    """
    log.info("Mode: Pregame pull for %s", target_date)
    log.info("Markets: %s", markets_flag)

    captured_utc = datetime.now(timezone.utc).replace(tzinfo=None).replace(microsecond=0).isoformat()
    total_odds_rows  = 0
    total_props_rows = 0
    total_api_used   = 0
    api_remaining    = 0
    games_covered    = 0

    # ── Step 1: Pull game lines + F5 from the bulk odds endpoint ──────────
    if "game" in markets_flag or "f5" in markets_flag:
        all_markets = []
        if "game" in markets_flag:
            all_markets += GAME_MARKETS
        if "f5" in markets_flag:
            all_markets += F5_MARKETS

        url = f"{API_BASE}/sports/{SPORT}/odds"
        params = {
            "apiKey":      api_key,
            "bookmakers":  BOOKMAKERS,   # DraftKings, FanDuel, BetMGM only
            "markets":     ",".join(all_markets),
            "oddsFormat":  ODDS_FORMAT,
            "dateFormat":  "iso",
        }
        data, headers = api_get(url, params, verbose)
        quota = quota_from_headers(headers)
        total_api_used   += quota["requests_used"]
        api_remaining     = quota["requests_remaining"]

        if data:
            # Build lookup once for target_date (also includes +1 day internally)
            game_lookup = build_game_lookup(con, target_date)
            log.info("  API returned %d events total", len(data))
            # Show what dates appeared so we can diagnose filter issues
            date_counts = {}
            for e in data:
                d = e.get("commence_time", "")[:10]
                date_counts[d] = date_counts.get(d, 0) + 1
            for d, n in sorted(date_counts.items()):
                log.info("    %s: %d event(s)", d, n)

            for event in data:
                event_utc_date = event.get("commence_time", "")[:10]

                # Accept events whose UTC date is target_date OR target_date+1.
                # Late West Coast games (e.g. 10:10 PM PT = 02:10 UTC next day)
                # appear with a UTC date of tomorrow in the API but belong to
                # today in the DB (stored via officialDate). We rely on
                # match_event_to_game to confirm the game is actually in our DB
                # for target_date — if it's not, match returns None and we skip.
                from datetime import date as _d, timedelta as _td
                try:
                    next_utc_date = (_d.fromisoformat(target_date)
                                     + _td(days=1)).isoformat()
                except ValueError:
                    next_utc_date = target_date

                if event_utc_date not in (target_date, next_utc_date):
                    continue

                game_row = match_event_to_game(event, target_date, game_lookup)
                if not game_row:
                    continue

                game_pk     = game_row["game_pk"]
                start_utc   = game_row.get("game_start_utc")
                hours_before = None
                if start_utc:
                    try:
                        start_dt = datetime.fromisoformat(start_utc.replace("Z",""))
                        hours_before = round(
                            (start_dt - datetime.now(timezone.utc).replace(tzinfo=None)).total_seconds() / 3600, 2
                        )
                    except Exception:
                        pass

                games_covered += 1
                log.info("  Matched: %s vs %s → game_pk=%s  hours_before=%.2f",
                         event.get("away_team","?"), event.get("home_team","?"),
                         game_pk, hours_before if hours_before is not None else 0)

                for bm in event.get("bookmakers", []):
                    # Inject home/away team names so parse_game_markets
                    # can match outcomes by name instead of by position.
                    bm["_home_team"] = event.get("home_team", "")
                    bm["_away_team"] = event.get("away_team", "")
                    # Game lines
                    if "game" in markets_flag:
                        for row in parse_game_markets(bm, game_pk,
                                                      captured_utc, hours_before):
                            total_odds_rows += upsert_game_odds_row(con, row)

                    # F5 lines
                    if "f5" in markets_flag:
                        f5_row = parse_f5_markets(bm, game_pk,
                                                   captured_utc, hours_before)
                        if f5_row:
                            total_odds_rows += upsert_f5_row(con, f5_row)

            con.commit()
            log.info("Game/F5 lines: %d rows inserted for %d games",
                     total_odds_rows, games_covered)

    # ── Step 2: Props — one request per event ─────────────────────────────
    if "props" in markets_flag:
        # Get event IDs from the events endpoint
        url = f"{API_BASE}/sports/{SPORT}/events"
        params = {
            "apiKey":     api_key,
            "dateFormat": "iso",
        }
        events_data, headers = api_get(url, params, verbose)
        quota = quota_from_headers(headers)
        total_api_used += quota["requests_used"]

        if events_data:
            from datetime import date as _d2, timedelta as _td2
            try:
                next_utc_date2 = (_d2.fromisoformat(target_date)
                                  + _td2(days=1)).isoformat()
            except ValueError:
                next_utc_date2 = target_date

            today_events = [e for e in events_data
                            if e.get("commence_time","")[:10]
                            in (target_date, next_utc_date2)]
            log.info("Pulling props for %d events ...", len(today_events))

            game_lookup_props = build_game_lookup(con, target_date)
            for i, event in enumerate(today_events, 1):
                event_id   = event["id"]
                game_row    = match_event_to_game(event, target_date, game_lookup_props)
                if not game_row:
                    continue

                game_pk = game_row["game_pk"]
                start_utc = game_row.get("game_start_utc")
                hours_before = None
                if start_utc:
                    try:
                        start_dt = datetime.fromisoformat(start_utc.replace("Z",""))
                        hours_before = round(
                            (start_dt - datetime.now(timezone.utc).replace(tzinfo=None)).total_seconds() / 3600, 2
                        )
                    except Exception:
                        pass

                url2 = f"{API_BASE}/sports/{SPORT}/events/{event_id}/odds"
                params2 = {
                    "apiKey":     api_key,
                    "regions":    REGIONS,
                    "markets":    ",".join(PROP_MARKETS),
                    "oddsFormat": ODDS_FORMAT,
                }
                prop_data, headers2 = api_get(url2, params2, verbose)
                quota2 = quota_from_headers(headers2)
                total_api_used  += quota2["requests_used"]
                api_remaining    = quota2["requests_remaining"]

                if prop_data:
                    for bm in prop_data.get("bookmakers", []):
                        for market in bm.get("markets", []):
                            prop_type = market.get("key","")
                            if prop_type not in PROP_MARKETS:
                                continue
                            for outcome in market.get("outcomes", []):
                                player_name = outcome.get("description","")
                                player_id   = lookup_player(con, player_name)
                                if not player_id:
                                    continue
                                line       = outcome.get("point")
                                name_lower = outcome.get("name","").lower()
                                over_odds  = outcome.get("price") if name_lower=="over"  else None
                                under_odds = outcome.get("price") if name_lower=="under" else None
                                if line is None:
                                    continue
                                total_props_rows += upsert_player_prop(
                                    con, game_pk, player_id,
                                    bm["key"], prop_type,
                                    line, over_odds, under_odds,
                                    captured_utc, hours_before
                                )

                if i < len(today_events):
                    time.sleep(REQUEST_PAUSE)

            con.commit()
            log.info("Props: %d rows inserted", total_props_rows)

    log.info("API requests this pull: %d  |  Remaining: %d",
             total_api_used, api_remaining)

    return {
        "odds_rows":    total_odds_rows,
        "props_rows":   total_props_rows,
        "games":        games_covered,
        "api_used":     total_api_used,
        "api_remaining": api_remaining,
    }



# ══════════════════════════════════════════════════════════════════════════════
# MISSING ODDS RECOVERY  (run after pregame pull to catch games already started)
# ══════════════════════════════════════════════════════════════════════════════

def get_games_missing_odds(con: sqlite3.Connection, target_date: str) -> list:
    """
    Return list of game rows for target_date that have no closing-line
    moneyline odds in game_odds. Includes games regardless of status
    (scheduled, in-progress, or final) so we can recover odds for games
    that started before the pregame pull caught them.
    """
    rows = con.execute("""
        SELECT
            g.game_pk,
            g.game_start_utc,
            g.status,
            th.abbreviation AS home_abbr,
            ta.abbreviation AS away_abbr
        FROM games g
        JOIN teams th ON th.team_id = g.home_team_id
        JOIN teams ta ON ta.team_id = g.away_team_id
        WHERE g.game_date = ?
          AND g.game_type = 'R'
          AND g.status NOT IN ('Cancelled', 'Postponed')
          AND g.game_pk NOT IN (
              SELECT DISTINCT game_pk
              FROM game_odds
              WHERE market_type = 'moneyline'
                AND is_closing_line = 1
                AND home_ml IS NOT NULL
          )
        ORDER BY g.game_start_utc
    """, (target_date,)).fetchall()
    return [dict(r) for r in rows]


def pull_missing(con: sqlite3.Connection, api_key: str,
                 target_date: str, markets_flag: list,
                 verbose: bool = False) -> dict:
    """
    Recover odds for any games on target_date that are missing from game_odds.

    Strategy:
      1. Query which games have no closing-line ML odds yet.
      2. For each missing game, use the historical endpoint with a snapshot
         timestamp 5 minutes before game start — captures the last pre-game
         odds regardless of whether the game has since started or finished.
      3. If game_start_utc is unknown, fall back to noon ET (17:00 UTC).
      4. Promote recovered rows to is_closing_line=1 so the brief finds them.

    Cost: 1 historical API request per missing game per market group.
    """
    missing_games = get_games_missing_odds(con, target_date)

    if not missing_games:
        log.info("Missing odds check: all games have closing-line odds. Nothing to recover.")
        return {"odds_rows": 0, "props_rows": 0, "games": 0,
                "api_used": 0, "api_remaining": 0}

    log.info("Missing odds recovery: %d game(s) need odds — using historical endpoint.",
             len(missing_games))
    for g in missing_games:
        log.info("  Missing: %s@%s  start=%s  status=%s",
                 g["away_abbr"], g["home_abbr"],
                 g["game_start_utc"] or "unknown", g["status"])

    captured_utc    = datetime.now(timezone.utc).replace(tzinfo=None).replace(microsecond=0).isoformat()
    total_odds_rows = 0
    total_api_used  = 0
    api_remaining   = 0
    games_recovered = 0
    game_lookup     = build_game_lookup(con, target_date)

    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)

    for game in missing_games:
        game_pk   = game["game_pk"]
        start_utc = game["game_start_utc"]

        # FUTURE GAME GUARD: skip historical recovery for games not yet started
        if start_utc:
            try:
                start_dt = datetime.fromisoformat(start_utc.replace("Z", ""))
                if start_dt > now_utc:
                    log.debug("  Skipping recovery for future game %s@%s",
                              game["away_abbr"], game["home_abbr"])
                    continue
            except Exception:
                pass

        # Snapshot = 5 minutes before first pitch (or noon ET if unknown)
        if start_utc:
            try:
                start_dt    = datetime.fromisoformat(start_utc.replace("Z", ""))
                snapshot_dt = start_dt - timedelta(minutes=5)
                snapshot_ts = snapshot_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                snapshot_ts = f"{target_date}T17:00:00Z"
        else:
            snapshot_ts = f"{target_date}T17:00:00Z"

        log.info("  Recovering %s@%s — snapshot: %s",
                 game["away_abbr"], game["home_abbr"], snapshot_ts)

        if "game" in markets_flag:
            url    = f"{API_BASE}/historical/sports/{SPORT}/odds"
            params = {
                "apiKey":     api_key,
                "regions":    REGIONS,
                "markets":    ",".join(GAME_MARKETS),
                "oddsFormat": ODDS_FORMAT,
                "date":       snapshot_ts,
            }
            data, headers = api_get(url, params, verbose)
            quota          = quota_from_headers(headers)
            total_api_used += quota["requests_used"]
            api_remaining   = quota["requests_remaining"]

            if data:
                events = data.get("data", data) if isinstance(data, dict) else data
                if not isinstance(events, list):
                    events = []

                for event in events:
                    game_row = match_event_to_game(event, target_date, game_lookup)
                    if not game_row or game_row["game_pk"] != game_pk:
                        continue
                    rows_inserted = 0
                    for bm in event.get("bookmakers", []):
                        bm["_home_team"] = event.get("home_team", "")
                        bm["_away_team"] = event.get("away_team", "")
                        for row in parse_game_markets(bm, game_pk, captured_utc, None):
                            row["is_closing_line"] = 1
                            rows_inserted += upsert_game_odds_row(con, row)
                    total_odds_rows += rows_inserted
                    if rows_inserted > 0:
                        games_recovered += 1
                        log.info("    ✓ %s@%s: %d rows recovered",
                                 game["away_abbr"], game["home_abbr"], rows_inserted)
                    else:
                        log.warning("    ⚠ %s@%s found in API but no bookmaker rows parsed.",
                                    game["away_abbr"], game["home_abbr"])
                    break
                else:
                    log.warning("    ✗ %s@%s not found in historical snapshot at %s",
                                game["away_abbr"], game["home_abbr"], snapshot_ts)

        time.sleep(REQUEST_PAUSE)

    con.commit()
    log.info("Missing odds recovery: %d game(s) recovered, %d rows inserted, "
             "%d API requests used.",
             games_recovered, total_odds_rows, total_api_used)

    return {
        "odds_rows":    total_odds_rows,
        "props_rows":   0,
        "games":        games_recovered,
        "api_used":     total_api_used,
        "api_remaining": api_remaining,
    }

# ══════════════════════════════════════════════════════════════════════════════
# HISTORICAL BACKFILL  (The Odds API historical endpoint)
# ══════════════════════════════════════════════════════════════════════════════

def pull_historical(con: sqlite3.Connection, api_key: str,
                    start_date: str, end_date: str,
                    markets_flag: list,
                    verbose: bool = False) -> dict:
    """
    Backfill historical odds for a date range.
    Uses the /historical/sports/{sport}/odds endpoint.
    NOTE: Historical endpoint costs more quota — check your plan.
    """
    log.info("Mode: Historical backfill %s → %s", start_date, end_date)
    log.info("Markets: %s", markets_flag)
    log.warning("Historical odds consume more API quota per request. "
                "Check odds_ingest_log.api_quota_remaining after each run.")

    total_odds  = 0
    total_props = 0
    total_api   = 0
    api_rem     = 0
    games_cov   = 0

    # Iterate day by day
    current = datetime.strptime(start_date, "%Y-%m-%d").date()
    end     = datetime.strptime(end_date,   "%Y-%m-%d").date()

    while current <= end:
        date_str = current.isoformat()
        log.info("  Backfilling %s ...", date_str)

        # The historical endpoint takes a snapshot timestamp
        # We use noon UTC on each date as the "closing line" snapshot
        snapshot_ts = f"{date_str}T17:00:00Z"  # ~noon ET = 17:00 UTC

        captured_utc = snapshot_ts.replace("Z", "")
        url = f"{API_BASE}/historical/sports/{SPORT}/odds"

        def _hist_fetch(mkts: list) -> tuple:
            """Fetch one market group from the historical endpoint.
            Returns (events_list, headers) or (None, headers) on failure."""
            p = {
                "apiKey":     api_key,
                "regions":    REGIONS,
                "markets":    ",".join(mkts),
                "oddsFormat": ODDS_FORMAT,
                "date":       snapshot_ts,
            }
            d, h = api_get(url, p, verbose)
            if d is None:
                return None, h
            events = d.get("data", d) if isinstance(d, dict) else d
            return (events if isinstance(events, list) else []), h

        game_lookup = build_game_lookup(con, date_str)

        # ── Pull game markets (h2h, spreads, totals) ──────────────────────
        if "game" in markets_flag:
            events, headers = _hist_fetch(GAME_MARKETS)
            quota = quota_from_headers(headers)
            total_api += quota["requests_used"]
            api_rem    = quota["requests_remaining"]

            if events:
                for event in events:
                    event_date = event.get("commence_time", "")[:10]
                    game_row   = match_event_to_game(event, event_date, game_lookup)
                    if not game_row:
                        continue
                    game_pk = game_row["game_pk"]
                    games_cov += 1
                    for bm in event.get("bookmakers", []):
                        bm["_home_team"] = event.get("home_team", "")
                        bm["_away_team"] = event.get("away_team", "")
                        for row in parse_game_markets(bm, game_pk, captured_utc, None):
                            total_odds += upsert_game_odds_row(con, row)
                con.commit()

        # ── Pull F5 markets (separate request — 422-safe) ─────────────────
        # The historical endpoint sometimes lacks F5 data for early-season
        # dates.  A 422 here is skipped gracefully; it does not abort the run.
        if "f5" in markets_flag:
            events_f5, headers_f5 = _hist_fetch(F5_MARKETS)
            quota_f5 = quota_from_headers(headers_f5)
            total_api += quota_f5["requests_used"]
            api_rem    = quota_f5["requests_remaining"]

            if events_f5:
                for event in events_f5:
                    event_date = event.get("commence_time", "")[:10]
                    game_row   = match_event_to_game(event, event_date, game_lookup)
                    if not game_row:
                        continue
                    game_pk = game_row["game_pk"]
                    for bm in event.get("bookmakers", []):
                        f5_row = parse_f5_markets(bm, game_pk, captured_utc, None)
                        if f5_row:
                            total_odds += upsert_f5_row(con, f5_row)
                con.commit()

        log.info("    %s: odds=%d  api_used=%d  remaining=%d",
                 date_str, total_odds, total_api, api_rem)

        current += timedelta(days=1)
        time.sleep(REQUEST_PAUSE)

    return {
        "odds_rows": total_odds, "props_rows": total_props,
        "games": games_cov, "api_used": total_api,
        "api_remaining": api_rem,
    }


# ══════════════════════════════════════════════════════════════════════════════
# COMPUTE LINE MOVEMENT  (run nightly after games complete)
# ══════════════════════════════════════════════════════════════════════════════

def compute_movement(con: sqlite3.Connection,
                     target_date: str,
                     verbose: bool = False) -> int:
    """
    For each game on target_date, compute line_movement rows from game_odds.
    Detects steam moves and reverse line moves.
    Returns number of rows upserted.
    """
    log.info("Computing line movement for %s ...", target_date)

    games = con.execute("""
        SELECT g.game_pk, g.game_start_utc
        FROM   games g
        WHERE  g.game_date = ? AND g.game_type = 'R'
    """, (target_date,)).fetchall()

    upserted = 0

    for game in games:
        game_pk = game["game_pk"]

        # Get all bookmakers that have data for this game
        bookmakers = [r[0] for r in con.execute("""
            SELECT DISTINCT bookmaker FROM game_odds WHERE game_pk = ?
        """, (game_pk,)).fetchall()]

        for bookmaker in bookmakers:
            for market_type in ("moneyline", "runline", "total"):
                rows = con.execute("""
                    SELECT home_ml, away_ml, total_line,
                           home_rl_odds,
                           is_opening_line, is_closing_line,
                           captured_at_utc
                    FROM   game_odds
                    WHERE  game_pk = ? AND bookmaker = ? AND market_type = ?
                    ORDER  BY captured_at_utc
                """, (game_pk, bookmaker, market_type)).fetchall()

                if len(rows) < 1:
                    continue

                open_row  = next((r for r in rows if r["is_opening_line"]), rows[0])
                close_row = next((r for r in rows if r["is_closing_line"]), rows[-1])

                # Compute movement
                move_cents = ml_move_cents(open_row["home_ml"], close_row["home_ml"])
                total_mv   = None
                if open_row["total_line"] and close_row["total_line"]:
                    total_mv = round(close_row["total_line"] - open_row["total_line"], 2)

                # Direction
                if move_cents and abs(move_cents) > 2:
                    direction = "home" if move_cents > 0 else "away"
                elif total_mv and abs(total_mv) > 0.1:
                    direction = "over" if total_mv > 0 else "under"
                else:
                    direction = "none"

                # Steam move: check for rapid cross-book movement
                # Simplified: flag if move_cents > threshold
                steam = 1 if (move_cents and abs(move_cents) >= STEAM_CENTS_THRESHOLD) else 0

                # Reverse line move: requires public ticket % data
                # We can't determine this from Odds API alone — set 0
                reverse_lm = 0

                con.execute("""
                    INSERT INTO line_movement
                        (game_pk, bookmaker, market_type,
                         open_home_ml, open_away_ml, open_total,
                         open_rl_home_odds, open_captured_utc,
                         close_home_ml, close_away_ml, close_total,
                         close_rl_home_odds, close_captured_utc,
                         ml_move_cents, total_move, move_direction,
                         steam_move, reverse_line_move)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(game_pk, bookmaker, market_type) DO UPDATE SET
                        close_home_ml      = excluded.close_home_ml,
                        close_away_ml      = excluded.close_away_ml,
                        close_total        = excluded.close_total,
                        close_captured_utc = excluded.close_captured_utc,
                        ml_move_cents      = excluded.ml_move_cents,
                        total_move         = excluded.total_move,
                        move_direction     = excluded.move_direction,
                        steam_move         = excluded.steam_move
                """, (
                    game_pk, bookmaker, market_type,
                    open_row["home_ml"], open_row["away_ml"],
                    open_row["total_line"], open_row["home_rl_odds"],
                    open_row["captured_at_utc"],
                    close_row["home_ml"], close_row["away_ml"],
                    close_row["total_line"], close_row["home_rl_odds"],
                    close_row["captured_at_utc"],
                    move_cents, total_mv, direction, steam, reverse_lm
                ))
                upserted += 1

    con.commit()
    log.info("line_movement: %d rows upserted for %s", upserted, target_date)
    return upserted


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(
        description="Load Vegas odds from The Odds API into mlb_stats.db",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  python load_odds.py --pregame
      Pull today's game lines only (default — safe for daily use).

  python load_odds.py --pregame --markets game f5
      Pull game lines and F5. Note: F5 may return 422 on some API plans.

  python load_odds.py --pregame --markets game f5 props
      Pull all markets (props costs ~15 requests per pull).

  python load_odds.py --pregame --date 2026-04-01 --force
      Pull odds for a specific past date. --force required for non-today dates.

  python load_odds.py --historical --season 2023
      Backfill full 2023 game lines.  Requires historical plan.

  python load_odds.py --historical --start 2024-04-01 --end 2024-04-30
      Backfill a specific date range.

  python load_odds.py --compute-movement
      Compute line_movement for yesterday's games (default).

  python load_odds.py --compute-movement --date 2025-04-15
      Compute line_movement for a specific date.

  python load_odds.py --check
      Show odds row counts and ingest log summary.
""")
    p.add_argument("--pregame",           action="store_true",
                   help="Pull pre-game odds. Default date: today. "
                        "Use --date + --force to pull for a different date.")
    p.add_argument("--late-games",         action="store_true",
                   help="Pull odds for unstarted games only (~8 PM ET). "
                        "Safe after 6:30 PM closing pull. In-game guard skips "
                        "started games automatically. Use to catch West Coast "
                        "lines (SEA, HOU, ATL etc.) posted late. "
                        "Logged as pull_type='late_games'.")
    p.add_argument("--historical",        action="store_true",
                   help="Backfill historical odds")
    p.add_argument("--compute-movement",  action="store_true",
                   help="Compute line_movement table for completed games")
    p.add_argument("--check",             action="store_true",
                   help="Show row counts and ingest log, then exit")
    p.add_argument("--markets",           nargs="+",
                   choices=["game","f5","props"],
                   default=["game"],
                   help="Which market groups to pull (default: game). "
                        "f5 may return 422 on some API plans. "
                        "props costs ~15 requests per pull.")
    p.add_argument("--season",            type=int,
                   help="Season year for historical backfill")
    p.add_argument("--start",
                   help="Start date for historical backfill (YYYY-MM-DD)")
    p.add_argument("--end",
                   help="End date for historical backfill (YYYY-MM-DD)")
    p.add_argument("--date",
                   help="Target date override (YYYY-MM-DD). "
                        "For --pregame: requires --force to prevent accidental re-pulls. "
                        "For --compute-movement: defaults to yesterday.")
    p.add_argument("--recover",           action="store_true",
                   help="Use historical endpoint to recover odds for any games "
                        "missing lines (runs automatically after --pregame).")
    p.add_argument("--no-recover",        action="store_true",
                   help="Skip automatic missing-odds recovery after --pregame. "
                        "Use to save API quota when all games are upcoming.")
    p.add_argument("--force",             action="store_true",
                   help="Allow --pregame to target a non-today date via --date. "
                        "Required safety gate to prevent accidental closing-line corruption.")
    p.add_argument("--db",                default=DEFAULT_DB,
                   help=f"Database path (default: {DEFAULT_DB})")
    p.add_argument("--verbose","-v",      action="store_true",
                   help="Show each API call")
    return p.parse_args()


def _migrate_odds_ingest_log(con: sqlite3.Connection) -> None:
    """
    Widen the pull_type CHECK constraint on odds_ingest_log to include
    'late_games'. SQLite cannot ALTER a CHECK constraint in-place, so we
    use rename-recreate-copy-drop. Safe to run on every startup — no-op
    if 'late_games' is already in the constraint.

    Strategy: always recreate the table WITH error_message. Copy only
    the columns that exist in the old table, leaving error_message NULL
    for historical rows. This handles both old (11-col) and new (12-col)
    source tables safely.
    """
    row = con.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='odds_ingest_log'"
    ).fetchone()
    if row is None or "late_games" in (row[0] or ""):
        return  # absent or already widened

    # Columns present in the old table (excluding auto-increment id)
    old_cols = [r[1] for r in con.execute("PRAGMA table_info(odds_ingest_log)").fetchall()
                if r[1] != "id"]
    copy_cols = ", ".join(old_cols)

    log.info("Migrating odds_ingest_log: widening pull_type CHECK constraint ...")
    con.execute("PRAGMA foreign_keys = OFF")
    con.execute("ALTER TABLE odds_ingest_log RENAME TO odds_ingest_log_old")
    con.execute("""
        CREATE TABLE odds_ingest_log (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            pulled_at_utc       TEXT    NOT NULL,
            pull_type           TEXT    NOT NULL CHECK(pull_type IN (
                                    'historical_backfill',
                                    'daily_pregame',
                                    'late_games',
                                    'live_update',
                                    'props_update'
                                )),
            sport               TEXT    NOT NULL DEFAULT 'baseball_mlb',
            markets_pulled      TEXT,
            games_covered       INTEGER,
            odds_rows_inserted  INTEGER,
            props_rows_inserted INTEGER DEFAULT 0,
            api_requests_used   INTEGER,
            api_quota_remaining INTEGER,
            status              TEXT    NOT NULL DEFAULT 'success',
            error_message       TEXT
        )
    """)
    # Copy only the columns that existed in the old table — error_message
    # defaults to NULL for old rows if it was absent.
    con.execute(
        f"INSERT INTO odds_ingest_log ({copy_cols}) "
        f"SELECT {copy_cols} FROM odds_ingest_log_old"
    )
    con.execute("DROP TABLE odds_ingest_log_old")
    con.execute("PRAGMA foreign_keys = ON")
    con.commit()
    log.info("  Migration complete — pull_type now accepts 'late_games'.")


def main():
    args = parse_args()
    configure_logging(args.verbose)

    log.info("=" * 60)
    log.info("  MLB Odds Loader  —  The Odds API")
    log.info("=" * 60)

    con = get_connection(args.db)
    _migrate_odds_ingest_log(con)  # widen pull_type CHECK if needed

    # ── Check mode ────────────────────────────────────────────────────────
    if args.check:
        show_odds_counts(con)
        con.close()
        return

    # ── Validate at least one mode selected ───────────────────────────────
    if not any([args.pregame, getattr(args, "late_games", False), args.historical, args.compute_movement, args.recover]):
        log.error("Specify a mode: --pregame, --historical, or --compute-movement")
        log.error("Run with --help for examples.")
        con.close()
        sys.exit(1)

    api_key = get_api_key()
    pull_type = "daily_pregame"

    # ── Compute movement mode ─────────────────────────────────────────────
    if args.compute_movement:
        target = args.date or (date.today() - timedelta(days=1)).isoformat()
        compute_movement(con, target, args.verbose)
        show_odds_counts(con)
        con.close()
        return

    # ── Recover missing odds (standalone mode) ───────────────────────────
    if args.recover and not args.pregame:
        target_date = args.date or date.today().isoformat()
        result = pull_missing(con, api_key, target_date, args.markets, args.verbose)
        log.info("")
        log.info("=" * 60)
        log.info("  RECOVERY SUMMARY")
        log.info("=" * 60)
        log.info("  Games recovered     : %d", result["games"])
        log.info("  Odds rows inserted  : %d", result["odds_rows"])
        log.info("  API requests used   : %d", result["api_used"])
        log.info("  API quota remaining : %d", result.get("api_remaining", 0))
        log_odds_ingest(
            con, "daily_pregame", args.markets,
            result["games"], result["odds_rows"], result["props_rows"],
            result["api_used"], result.get("api_remaining", 0), "success"
        )
        show_odds_counts(con)
        con.close()
        return

    # ── Historical backfill ───────────────────────────────────────────────
    if args.historical:
        pull_type = "historical_backfill"
        if args.season:
            row = con.execute(
                "SELECT season_start, season_end FROM seasons WHERE season=?",
                (args.season,)
            ).fetchone()
            if row:
                start_date = row["season_start"]
                end_date   = row["season_end"]
            else:
                start_date = f"{args.season}-04-01"
                end_date   = f"{args.season}-10-01"
        elif args.start and args.end:
            start_date = args.start
            end_date   = args.end
        else:
            log.error("--historical requires --season YYYY or --start/--end dates")
            con.close()
            sys.exit(1)

        result = pull_historical(con, api_key, start_date, end_date,
                                 args.markets, args.verbose)

    # ── Pregame pull ──────────────────────────────────────────────────────
    elif args.pregame:
        today = date.today().isoformat()

        if args.date and args.date != today:
            # Non-today date requested — require --force as safety gate
            if not args.force:
                log.error(
                    "✗  --pregame --date %s requires --force to pull for a date other than today.",
                    args.date
                )
                log.error(
                    "   Re-pulling odds for a past date overwrites is_closing_line flags "
                    "and corrupts CLV data for that date."
                )
                log.error(
                    "   If you are sure, re-run with --force added."
                )
                con.close()
                sys.exit(1)
            target_date = args.date
            log.warning("⚠  Pulling odds for %s (not today). --force acknowledged.", target_date)
        else:
            target_date = today
            if args.date:
                log.info("--date %s matches today — proceeding normally.", args.date)

        result = pull_pregame(con, api_key, args.markets,
                              target_date, args.verbose)

        # Auto-recover any games still missing odds after pregame pull.
        # Games that started before lines appeared in the live feed are
        # recovered via the historical endpoint (1 request per game).
        if not getattr(args, "no_recover", False):
            recovery = pull_missing(con, api_key, target_date,
                                    args.markets, args.verbose)
            result["odds_rows"] += recovery["odds_rows"]
            result["games"]     += recovery["games"]
            result["api_used"]  += recovery["api_used"]
            if recovery.get("api_remaining"):
                result["api_remaining"] = recovery["api_remaining"]

    # ── Late-games pull (~8 PM) ──────────────────────────────────────────
    # Identical to --pregame in logic. The in-game guard in
    # upsert_game_odds_row() skips any game where hours_before_game < 0,
    # preserving its is_closing_line=1 row untouched. Only unstarted West
    # Coast games receive new rows. No auto-recovery on this pull.
    elif getattr(args, "late_games", False):
        pull_type   = "late_games"
        target_date = date.today().isoformat()
        log.info("Mode: Late-games pull for %s (~8 PM West Coast catch-up)", target_date)
        log.info("  In-game guard active — started games skipped automatically.")
        log.info("  Only unstarted games will receive new odds rows.")
        result = pull_pregame(con, api_key, args.markets,
                              target_date, args.verbose)
        # No auto-recovery — late pull is purely additive for unstarted games.

    # ── Summary ───────────────────────────────────────────────────────────
    log.info("")
    log.info("=" * 60)
    log.info("  SUMMARY")
    log.info("=" * 60)
    log.info("  Games covered       : %d", result["games"])
    log.info("  Odds rows inserted  : %d", result["odds_rows"])
    log.info("  Props rows inserted : %d", result["props_rows"])
    log.info("  API requests used   : %d", result["api_used"])
    log.info("  API quota remaining : %d", result.get("api_remaining",0))

    log_odds_ingest(
        con, pull_type, args.markets,
        result["games"], result["odds_rows"], result["props_rows"],
        result["api_used"], result.get("api_remaining",0),
        "success"
    )

    show_odds_counts(con)
    con.close()


if __name__ == "__main__":
    main()
