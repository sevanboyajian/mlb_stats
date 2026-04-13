"""
add_stadium_data.py
===================
MLB Betting Model · Stadium Data Migration
Extends the EXISTING venues table in mlb_stats.db with wind, park-factor,
orientation, and metadata columns for all 30 MLB parks.

SAFE TO RUN ON A POPULATED DATABASE
-------------------------------------
  • Never drops, truncates, or deletes any table or row
  • Uses ALTER TABLE … ADD COLUMN pattern (checked via PRAGMA table_info
    before each ALTER — SQLite lacks IF NOT EXISTS on ALTER TABLE)
  • Updates only the new columns — all existing data (name, city, dimensions,
    lat/lon, surface, roof_type) is untouched
  • Matching is done by venue name (venues.name) with a team-abbr fallback
  • Rows with no match are logged, never silently skipped

COLUMN NAMING — matches existing schema exactly:
  venues.name          (not venue_name)
  venues.state         (not state_province)
  venues.elevation_ft  (not altitude_ft)
  roof_type values     'Open' / 'Retractable' / 'Dome'  (schema CHECK values)

USAGE
-----
    python add_stadium_data.py              # migrate + seed all 30 venues
    python add_stadium_data.py --check      # verify data, show summary
    python add_stadium_data.py --update     # re-seed (safe to rerun)
    python add_stadium_data.py --verbose    # show every row after seeding
    python add_stadium_data.py --dry-run    # show what would change, no writes

DATA SOURCES (all verified March 2025)
---------------------------------------
  Roof/surface  : BetMGM, Baseball Bible, MLB.com
  Altitude      : BallparkPal.com, Pittsburgh Post-Gazette
  Orientation   : Baseball Almanac NL/AL charts, Hardball Times, DNA of Sports
  Wind effect   : BallparkPal.com park pages, Fox Weather retractable roof study
  Oracle note   : BallparkPal.com — architecture neutralises wind effect
  Park factors  : ESPN Fantasy Baseball 2025, Yahoo Sports, BettorEdge
  Capacity/year : MLB.com

WIND EFFECT KEY
---------------
  HIGH       — open air, meaningfully wind-exposed (Wrigley, Fenway, Coors)
  MODERATE   — open air but sheltered or low avg wind (PNC, Camden, Dodger)
  LOW        — retractable; wind applies only when roof is confirmed open
  SUPPRESSED — architecture neutralises wind OR fixed dome
               (Oracle Park, Tropicana, loanDepot park)
               DO NOT apply MV-B / MV-F signals at SUPPRESSED venues
"""

# CHANGE LOG (latest first)
# -------------------------
# 2026-04-13 16:24 ET  Refactor: route sqlite3.connect() calls through core.db.connection.connect().

import argparse
import datetime
import sqlite3
import sys
from pathlib import Path

from core.db.connection import connect as db_connect

DB_PATH = Path(__file__).parent / "mlb_stats.db"

# ── New columns this script manages ───────────────────────────────────────────
# These are the ONLY columns touched. Existing columns are never modified.
NEW_COLUMNS = {
    "wind_effect":      "TEXT",
    "wind_note":        "TEXT",
    "orientation_hp":   "TEXT",
    "cf_direction":     "TEXT",
    "park_factor_runs": "INTEGER DEFAULT 100",
    "park_factor_hr":   "INTEGER DEFAULT 100",
    "altitude_note":    "TEXT",
    "opened_year":      "INTEGER",
    "last_updated":     "TEXT",
}

# ═══════════════════════════════════════════════════════════════════════════════
# VENUE DATA — all 30 MLB parks, 2025 season
#
# "name" must match venues.name exactly as loaded by load_mlb_stats.py.
# "team_abbr" is used only as a fallback match key — not stored.
# ═══════════════════════════════════════════════════════════════════════════════

VENUES = [
    # ── American League East ─────────────────────────────────────────────────
    {
        "name":             "Fenway Park",
        "team_abbr":        "BOS",
        "wind_effect":      "HIGH",
        "wind_note":        "Open air. Green Monster creates wind eddies in LF. "
                            "SE wind (off harbor) blows in; NW blows out. "
                            "Fully open — no roof of any kind.",
        "orientation_hp":   "SE",
        "cf_direction":     "NW",
        "park_factor_runs": 105,
        "park_factor_hr":   104,
        "altitude_note":    None,
        "opened_year":      1912,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "Yankee Stadium",
        "team_abbr":        "NYY",
        "wind_effect":      "MODERATE",
        "wind_note":        "Open air but bowl design moderates wind. "
                            "Short RF porch (314 ft) heavily favors LH power "
                            "regardless of wind direction.",
        "orientation_hp":   "NE",
        "cf_direction":     "SW",
        "park_factor_runs": 106,
        "park_factor_hr":   110,
        "altitude_note":    None,
        "opened_year":      2009,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "Rogers Centre",
        "team_abbr":        "TOR",
        "wind_effect":      "LOW",
        "wind_note":        "Retractable roof. Closed ~1/3 of games. "
                            "When open, wind can affect play. "
                            "When closed, fully controlled — wind signals suppressed. "
                            "Check roof status before applying signals.",
        "orientation_hp":   "NE",
        "cf_direction":     "SW",
        "park_factor_runs": 103,
        "park_factor_hr":   101,
        "altitude_note":    None,
        "opened_year":      1989,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "Oriole Park at Camden Yards",
        "team_abbr":        "BAL",
        "wind_effect":      "MODERATE",
        "wind_note":        "Open air. Warehouse in RF creates wind eddies. "
                            "Generally moderate wind exposure.",
        "orientation_hp":   "SE",
        "cf_direction":     "NW",
        "park_factor_runs": 98,
        "park_factor_hr":   97,
        "altitude_note":    None,
        "opened_year":      1992,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "Tropicana Field",
        "team_abbr":        "TB",
        "wind_effect":      "SUPPRESSED",
        "wind_note":        "Only fixed dome in MLB. Wind signals never apply. "
                            "Catwalks create unique ground-rule situations.",
        "orientation_hp":   "NE",
        "cf_direction":     "SW",
        "park_factor_runs": 95,
        "park_factor_hr":   93,
        "altitude_note":    None,
        "opened_year":      1990,
        "last_updated":     "2025-03-01",
    },
    # ── American League Central ──────────────────────────────────────────────
    {
        "name":             "Guaranteed Rate Field",
        "team_abbr":        "CWS",
        "wind_effect":      "HIGH",
        "wind_note":        "Open air South Side. Chicago wind can be significant. "
                            "Oriented SE — south wind blows out to CF.",
        "orientation_hp":   "SE",
        "cf_direction":     "NW",
        "park_factor_runs": 102,
        "park_factor_hr":   104,
        "altitude_note":    None,
        "opened_year":      1991,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "Progressive Field",
        "team_abbr":        "CLE",
        "wind_effect":      "MODERATE",
        "wind_note":        "Open air, lower bowl is sheltered. "
                            "Wind off Lake Erie can impact play, especially April–May.",
        "orientation_hp":   "NE",
        "cf_direction":     "SW",
        "park_factor_runs": 96,
        "park_factor_hr":   94,
        "altitude_note":    None,
        "opened_year":      1994,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "Comerica Park",
        "team_abbr":        "DET",
        "wind_effect":      "MODERATE",
        "wind_note":        "Open air. Deep dimensions suppress HR. "
                            "Stadium walls provide shelter — moderate wind effect.",
        "orientation_hp":   "SE",
        "cf_direction":     "NW",
        "park_factor_runs": 94,
        "park_factor_hr":   89,
        "altitude_note":    None,
        "opened_year":      2000,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "Kauffman Stadium",
        "team_abbr":        "KC",
        "wind_effect":      "HIGH",
        "wind_note":        "Open air, exposed location. Kansas City wind is notable. "
                            "2025: outfield fences moved in and lowered — more HR-friendly.",
        "orientation_hp":   "NE",
        "cf_direction":     "SW",
        "park_factor_runs": 103,
        "park_factor_hr":   98,
        "altitude_note":    "909 ft elevation adds modest carry to fly balls.",
        "opened_year":      1973,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "Target Field",
        "team_abbr":        "MIN",
        "wind_effect":      "HIGH",
        "wind_note":        "Open air, exposed downtown location. "
                            "Cold April/May wind from NW blows in — suppresses scoring "
                            "early in season.",
        "orientation_hp":   "NW",
        "cf_direction":     "SE",
        "park_factor_runs": 99,
        "park_factor_hr":   97,
        "altitude_note":    None,
        "opened_year":      2010,
        "last_updated":     "2025-03-01",
    },
    # ── American League West ─────────────────────────────────────────────────
    {
        "name":             "Globe Life Field",
        "team_abbr":        "TEX",
        "wind_effect":      "LOW",
        "wind_note":        "Retractable roof. Texas summer heat means roof closed "
                            "most games. Wind signals rarely apply in summer. "
                            "Check roof status — April/September more likely open.",
        "orientation_hp":   "NE",
        "cf_direction":     "SW",
        "park_factor_runs": 100,
        "park_factor_hr":   99,
        "altitude_note":    None,
        "opened_year":      2020,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "Daikin Park",
        "team_abbr":        "HOU",
        "wind_effect":      "LOW",
        "wind_note":        "Retractable roof. Typically open in April and a few "
                            "September games only. Houston heat/humidity means roof "
                            "closed most of season. Wind signals apply only when "
                            "roof confirmed open.",
        "orientation_hp":   "NW",
        "cf_direction":     "SE",
        "park_factor_runs": 98,
        "park_factor_hr":   97,
        "altitude_note":    None,
        "opened_year":      2000,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "Angel Stadium",
        "team_abbr":        "LAA",
        "wind_effect":      "MODERATE",
        "wind_note":        "Open air. Marine influence from Pacific moderates temps. "
                            "Wind generally mild — moderate effect on totals.",
        "orientation_hp":   "NW",
        "cf_direction":     "SE",
        "park_factor_runs": 97,
        "park_factor_hr":   98,
        "altitude_note":    None,
        "opened_year":      1966,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "T-Mobile Park",
        "team_abbr":        "SEA",
        "wind_effect":      "MODERATE",
        "wind_note":        "Retractable roof used ~17–18 games/year — one of the "
                            "least-used roofs in MLB. Usually open. Cool Pacific NW "
                            "climate suppresses offense regardless of wind.",
        "orientation_hp":   "NE",
        "cf_direction":     "SW",
        "park_factor_runs": 91,
        "park_factor_hr":   88,
        "altitude_note":    None,
        "opened_year":      1999,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "Sutter Health Park",
        "team_abbr":        "OAK",
        "wind_effect":      "HIGH",
        "wind_note":        "Open air, single-deck minor league park (A's temp home "
                            "2025–27). Delta breeze blows OUT to RF/CF. Single-deck "
                            "structure more exposed to wind than multi-deck MLB venues "
                            "— wind signals amplified vs typical MLB park.",
        "orientation_hp":   "NE",
        "cf_direction":     "SW",
        "park_factor_runs": 110,
        "park_factor_hr":   105,
        "altitude_note":    "Delta breeze consistently blows out — favorable for OVER.",
        "opened_year":      2025,
        "last_updated":     "2025-03-01",
    },
    # ── National League East ─────────────────────────────────────────────────
    {
        "name":             "Nationals Park",
        "team_abbr":        "WSH",
        "wind_effect":      "HIGH",
        "wind_note":        "Open air. Potomac River location — wind off river can "
                            "be significant. Early April cold front winds suppress "
                            "scoring.",
        "orientation_hp":   "E",
        "cf_direction":     "W",
        "park_factor_runs": 99,
        "park_factor_hr":   98,
        "altitude_note":    None,
        "opened_year":      2008,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "Citi Field",
        "team_abbr":        "NYM",
        "wind_effect":      "MODERATE",
        "wind_note":        "Open air. Location near Flushing Bay provides some "
                            "marine influence. Relatively moderate wind effect.",
        "orientation_hp":   "W",
        "cf_direction":     "E",
        "park_factor_runs": 97,
        "park_factor_hr":   95,
        "altitude_note":    None,
        "opened_year":      2009,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "Citizens Bank Park",
        "team_abbr":        "PHI",
        "wind_effect":      "HIGH",
        "wind_note":        "Open air, exposed location. One of the more wind-variable "
                            "parks. SW wind blows out to RF — favorable for OVER.",
        "orientation_hp":   "NE",
        "cf_direction":     "SW",
        "park_factor_runs": 104,
        "park_factor_hr":   107,
        "altitude_note":    None,
        "opened_year":      2004,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "Truist Park",
        "team_abbr":        "ATL",
        "wind_effect":      "MODERATE",
        "wind_note":        "Open air, suburban Atlanta. Not a dome. "
                            "Elevated altitude (1050 ft) adds modest carry. "
                            "Warm summer temps support scoring.",
        "orientation_hp":   "SE",
        "cf_direction":     "NW",
        "park_factor_runs": 102,
        "park_factor_hr":   101,
        "altitude_note":    "1050 ft provides modest carry boost for fly balls.",
        "opened_year":      2017,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "loanDepot park",
        "team_abbr":        "MIA",
        "wind_effect":      "SUPPRESSED",
        "wind_note":        "Retractable roof almost always closed — Miami opened "
                            "roof just 5 times combined 2021–22. Daily thunderstorms "
                            "mean wind signals essentially never apply here.",
        "orientation_hp":   "W",
        "cf_direction":     "E",
        "park_factor_runs": 95,
        "park_factor_hr":   93,
        "altitude_note":    None,
        "opened_year":      2012,
        "last_updated":     "2025-03-01",
    },
    # ── National League Central ──────────────────────────────────────────────
    {
        "name":             "Wrigley Field",
        "team_abbr":        "CHC",
        "wind_effect":      "HIGH",
        "wind_note":        "Open air, highly wind-sensitive. No upper-deck overhang "
                            "in OF — wind fully exposed. Core signal park for MV-B "
                            "(wind out) and MV-F (wind in). Lake Michigan wind is the "
                            "primary totals driver at this venue.",
        "orientation_hp":   "W",
        "cf_direction":     "E",
        "park_factor_runs": 103,
        "park_factor_hr":   105,
        "altitude_note":    None,
        "opened_year":      1914,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "American Family Field",
        "team_abbr":        "MIL",
        "wind_effect":      "LOW",
        "wind_note":        "Fan-shaped retractable roof, closes in under 10 min. "
                            "Wind signals apply only when roof confirmed open. "
                            "Check roof status before applying wind signals.",
        "orientation_hp":   "E",
        "cf_direction":     "W",
        "park_factor_runs": 99,
        "park_factor_hr":   98,
        "altitude_note":    None,
        "opened_year":      2001,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "Busch Stadium",
        "team_abbr":        "STL",
        "wind_effect":      "MODERATE",
        "wind_note":        "Open air. Downtown St. Louis — moderate wind exposure. "
                            "Hot summer conditions boost scoring.",
        "orientation_hp":   "NE",
        "cf_direction":     "SW",
        "park_factor_runs": 98,
        "park_factor_hr":   95,
        "altitude_note":    None,
        "opened_year":      2006,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "PNC Park",
        "team_abbr":        "PIT",
        "wind_effect":      "MODERATE",
        "wind_note":        "Open air, riverfront location. Wind rarely exceeds 12 mph. "
                            "Blows out 53% of games. Altitude (743 ft, 6th highest in "
                            "MLB) adds modest carry. Not particularly wind-reactive.",
        "orientation_hp":   "NE",
        "cf_direction":     "SW",
        "park_factor_runs": 98,
        "park_factor_hr":   96,
        "altitude_note":    "743 ft — 6th highest in MLB. Mild carry boost.",
        "opened_year":      2001,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "Great American Ball Park",
        "team_abbr":        "CIN",
        "wind_effect":      "HIGH",
        "wind_note":        "Open air, Ohio River location. Most HR-friendly park in "
                            "MLB (+42% over league avg per 2025 data). Wind from NW "
                            "blows out to CF. Even modest wind meaningfully boosts "
                            "scoring here.",
        "orientation_hp":   "SE",
        "cf_direction":     "NW",
        "park_factor_runs": 112,
        "park_factor_hr":   130,
        "altitude_note":    None,
        "opened_year":      2003,
        "last_updated":     "2025-03-01",
    },
    # ── National League West ─────────────────────────────────────────────────
    {
        "name":             "Dodger Stadium",
        "team_abbr":        "LAD",
        "wind_effect":      "MODERATE",
        "wind_note":        "Open air, hillside setting. Generally calm winds. "
                            "Boosted HR for RHB +26% over 2022–24. Mild marine "
                            "climate. Wind rarely a factor.",
        "orientation_hp":   "NE",
        "cf_direction":     "SW",
        "park_factor_runs": 101,
        "park_factor_hr":   108,
        "altitude_note":    None,
        "opened_year":      1962,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "Oracle Park",
        "team_abbr":        "SF",
        "wind_effect":      "SUPPRESSED",
        "wind_note":        "Open air BUT architecture neutralises wind — BallparkPal "
                            "explicitly notes wind can be ignored at Oracle Park. "
                            "Marine layer and cool bay temps consistently suppress "
                            "scoring. RHB HR suppressed 21% over 3 seasons. "
                            "DO NOT apply MV-B or MV-F wind signals here.",
        "orientation_hp":   "E",
        "cf_direction":     "W",
        "park_factor_runs": 93,
        "park_factor_hr":   82,
        "altitude_note":    None,
        "opened_year":      2000,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "Petco Park",
        "team_abbr":        "SD",
        "wind_effect":      "MODERATE",
        "wind_note":        "Open air. Ocean breeze typically blows IN from SW — "
                            "suppresses scoring. Large outfield, marine climate = "
                            "pitcher-friendly. Wind-in under signal supported.",
        "orientation_hp":   "NW",
        "cf_direction":     "SE",
        "park_factor_runs": 93,
        "park_factor_hr":   88,
        "altitude_note":    None,
        "opened_year":      2004,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "Chase Field",
        "team_abbr":        "ARI",
        "wind_effect":      "LOW",
        "wind_note":        "Retractable roof with AC. Opens more than other AZ/TX "
                            "parks (~22 games/yr). High altitude (1082 ft) when open "
                            "provides significant carry boost. Check roof status — "
                            "altitude note applies only when roof is open.",
        "orientation_hp":   "NE",
        "cf_direction":     "SW",
        "park_factor_runs": 101,
        "park_factor_hr":   102,
        "altitude_note":    "1082 ft — when roof open, altitude meaningfully boosts carry.",
        "opened_year":      1998,
        "last_updated":     "2025-03-01",
    },
    {
        "name":             "Coors Field",
        "team_abbr":        "COL",
        "wind_effect":      "HIGH",
        "wind_note":        "Open air. Mile High altitude (5200 ft) is the dominant "
                            "scoring factor — thin air adds significant carry. Humidor "
                            "regulates ball moisture but altitude still dominates. "
                            "OVER signal is extremely strong here regardless of wind "
                            "direction.",
        "orientation_hp":   "NE",
        "cf_direction":     "SW",
        "park_factor_runs": 125,
        "park_factor_hr":   104,
        "altitude_note":    "5200 ft — most extreme altitude in MLB. Thin air adds "
                            "significant carry. Dominant factor over all wind signals.",
        "opened_year":      1995,
        "last_updated":     "2025-03-01",
    },
]


# ═══════════════════════════════════════════════════════════════════════════════
# Migration helpers
# ═══════════════════════════════════════════════════════════════════════════════

def open_db(path: Path) -> sqlite3.Connection:
    if not path.exists():
        print(f"\n✗  Database not found: {path}")
        print("   Run from the mlb_stats folder.")
        sys.exit(1)
    conn = db_connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def existing_columns(conn: sqlite3.Connection) -> set:
    """Return the set of column names currently in the venues table."""
    rows = conn.execute("PRAGMA table_info(venues)").fetchall()
    return {r["name"] for r in rows}


def add_missing_columns(conn: sqlite3.Connection, dry_run: bool) -> list:
    """
    Add any NEW_COLUMNS not yet in venues using ALTER TABLE.
    SQLite lacks IF NOT EXISTS on ALTER TABLE — we check first.
    Safe to call multiple times.
    Returns list of column names actually added.
    """
    have  = existing_columns(conn)
    added = []
    for col, col_type in NEW_COLUMNS.items():
        if col not in have:
            sql = f"ALTER TABLE venues ADD COLUMN {col} {col_type}"
            if dry_run:
                print(f"  [dry-run] Would run: {sql}")
            else:
                conn.execute(sql)
                added.append(col)
                print(f"  ✓ Added column: {col}  ({col_type})")
        else:
            print(f"  — Already exists: {col}")
    if not dry_run and added:
        conn.commit()
    return added


def build_name_to_venue_id(conn: sqlite3.Connection) -> dict:
    """Return {lowercase_stripped_name: venue_id} for all venues rows."""
    rows = conn.execute("SELECT venue_id, name FROM venues").fetchall()
    return {r["name"].lower().strip(): r["venue_id"] for r in rows}


def build_abbr_to_venue_id(conn: sqlite3.Connection) -> dict:
    """Fallback: team abbreviation → venue_id via teams table."""
    rows = conn.execute(
        "SELECT t.abbreviation, t.venue_id "
        "FROM teams t WHERE t.venue_id IS NOT NULL"
    ).fetchall()
    return {r["abbreviation"].upper(): r["venue_id"] for r in rows}


def seed_venues(conn: sqlite3.Connection, dry_run: bool, verbose: bool) -> dict:
    """
    UPDATE new columns for each venue. Matches by name first,
    falls back to team abbreviation. Never inserts or deletes rows.
    """
    name_map = build_name_to_venue_id(conn)
    abbr_map = build_abbr_to_venue_id(conn)

    updated  = 0
    no_match = []

    UPDATE_SQL = """
        UPDATE venues SET
            wind_effect      = :wind_effect,
            wind_note        = :wind_note,
            orientation_hp   = :orientation_hp,
            cf_direction     = :cf_direction,
            park_factor_runs = :park_factor_runs,
            park_factor_hr   = :park_factor_hr,
            altitude_note    = :altitude_note,
            opened_year      = :opened_year,
            last_updated     = :last_updated
        WHERE venue_id = :venue_id
    """

    for v in VENUES:
        # Primary match: name (case-insensitive)
        venue_id = name_map.get(v["name"].lower().strip())

        # Fallback: team abbreviation → venue FK in teams table
        if venue_id is None:
            venue_id = abbr_map.get(v["team_abbr"].upper())
            if venue_id is not None:
                print(f"  ↳ Name miss — matched via team abbr: "
                      f"{v['name']} ({v['team_abbr']}) → venue_id={venue_id}")

        if venue_id is None:
            no_match.append(v["name"])
            print(f"  ⚠  No DB match: {v['name']} ({v['team_abbr']}) — skipped")
            continue

        params = {k: v[k] for k in NEW_COLUMNS}
        params["venue_id"] = venue_id

        if dry_run:
            if verbose:
                print(f"  [dry-run] venue_id={venue_id}  {v['name']}  "
                      f"wind_effect={v['wind_effect']}")
        else:
            conn.execute(UPDATE_SQL, params)
            updated += 1
            if verbose:
                print(f"  ✓ venue_id={venue_id}  {v['name']:<35}  "
                      f"[{v['wind_effect']}]")

    if not dry_run:
        conn.commit()

    return {"updated": updated, "no_match": no_match, "total": len(VENUES)}


def run_check(conn: sqlite3.Connection) -> None:
    """Print a summary of migration state."""
    have        = existing_columns(conn)
    new_present = [c for c in NEW_COLUMNS if c in have]
    new_missing = [c for c in NEW_COLUMNS if c not in have]

    print("\n  ── venues migration check ──────────────────────────────────────")

    if new_missing:
        print(f"  ✗  Columns not yet added: {new_missing}")
        print("     Run without --check to apply the migration.")
    else:
        print(f"  ✓  All {len(NEW_COLUMNS)} new columns present")

    total  = conn.execute("SELECT COUNT(*) FROM venues").fetchone()[0]
    seeded = conn.execute(
        "SELECT COUNT(*) FROM venues WHERE wind_effect IS NOT NULL"
    ).fetchone()[0]
    print(f"\n  Total venue rows : {total}")
    print(f"  Rows seeded      : {seeded}  (wind_effect populated)")

    if seeded > 0:
        by_effect = conn.execute(
            "SELECT wind_effect, COUNT(*) n FROM venues "
            "WHERE wind_effect IS NOT NULL "
            "GROUP BY wind_effect ORDER BY n DESC"
        ).fetchall()
        print(f"\n  Wind effect distribution:")
        for r in by_effect:
            print(f"    {(r['wind_effect'] or 'NULL'):<12}  {r['n']}")

        suppressed = conn.execute(
            "SELECT v.name, t.abbreviation FROM venues v "
            "LEFT JOIN teams t ON t.venue_id = v.venue_id "
            "WHERE v.wind_effect = 'SUPPRESSED'"
        ).fetchall()
        print(f"\n  SUPPRESSED venues (wind signals DO NOT apply):")
        for r in suppressed:
            print(f"    {(r['abbreviation'] or '?'):<6}  {r['name']}")

        high_alt = conn.execute(
            "SELECT v.name, v.elevation_ft, t.abbreviation "
            "FROM venues v "
            "LEFT JOIN teams t ON t.venue_id = v.venue_id "
            "WHERE v.elevation_ft > 500 "
            "ORDER BY v.elevation_ft DESC"
        ).fetchall()
        if high_alt:
            print(f"\n  Parks above 500 ft:")
            for r in high_alt:
                print(f"    {(r['abbreviation'] or '?'):<6}  "
                      f"{r['name']:<36}  {r['elevation_ft']} ft")
    print()



def seed_2025_venues(conn: sqlite3.Connection, dry_run: bool, verbose: bool) -> dict:
    """
    Seed any unseeded venue that hosted at least one game in 2025.
    These are typically spring training facilities, neutral-site parks,
    or alternate venues — open-air but not relevant to live betting signals.

    Default values applied:
      wind_effect      = MODERATE  — open air but not a primary signal park
      wind_note        = auto-generated from venue name + city
      orientation_hp   = NULL      — unknown without research
      cf_direction     = NULL
      park_factor_runs = 100       — league average (no data for these parks)
      park_factor_hr   = 100
      altitude_note    = NULL
      opened_year      = NULL
      last_updated     = today

    These venues will not affect live brief operations — generate_daily_brief.py
    only queries games with status != 'Final' for today's date, and spring
    training / alternate venues do not appear in the regular season schedule.
    The seeding removes the check_db.py ⚠ warning and provides a safe fallback
    if an unexpected game does reference one of these venues.
    """
    today = datetime.date.today().isoformat()

    # Find unseeded venues that have at least one 2025 game
    rows = conn.execute("""
        SELECT DISTINCT v.venue_id, v.name, v.city, v.state, v.roof_type,
               COUNT(g.game_pk) AS game_count
        FROM   venues v
        JOIN   games  g ON g.venue_id = v.venue_id
        WHERE  v.wind_effect IS NULL
          AND  g.game_date >= '2025-01-01'
        GROUP  BY v.venue_id, v.name, v.city, v.state, v.roof_type
        ORDER  BY v.name
    """).fetchall()

    if not rows:
        print("  No unseeded 2025 venues found — nothing to do.")
        return {"seeded": 0, "total": 0}

    print(f"  Found {len(rows)} unseeded venue(s) with 2025 games:")

    UPDATE_SQL = """
        UPDATE venues SET
            wind_effect      = :wind_effect,
            wind_note        = :wind_note,
            orientation_hp   = :orientation_hp,
            cf_direction     = :cf_direction,
            park_factor_runs = :park_factor_runs,
            park_factor_hr   = :park_factor_hr,
            altitude_note    = :altitude_note,
            opened_year      = :opened_year,
            last_updated     = :last_updated
        WHERE venue_id = :venue_id
    """

    seeded = 0
    for r in rows:
        venue_id  = r["venue_id"]
        name      = r["name"] or f"Venue {venue_id}"
        city      = r["city"] or ""
        roof      = r["roof_type"] or "Open"
        games     = r["game_count"]

        # Determine wind_effect from roof type
        # Fixed dome → SUPPRESSED; retractable → LOW; open → MODERATE
        if roof and roof.lower() in ("dome", "fixed dome"):
            wind_effect = "SUPPRESSED"
            note = (f"{name} — fixed dome. Wind signals suppressed. "
                    f"Not a primary MLB venue (spring training / alternate site).")
        elif roof and "retractable" in roof.lower():
            wind_effect = "LOW"
            note = (f"{name} ({city}) — retractable roof. "
                    f"Spring training / alternate venue. Check roof status.")
        else:
            wind_effect = "MODERATE"
            note = (f"{name} ({city}) — open air spring training or alternate venue. "
                    f"Not a primary MLB signal park. {games} game(s) in 2025.")

        params = {
            "venue_id":        venue_id,
            "wind_effect":     wind_effect,
            "wind_note":       note,
            "orientation_hp":  None,
            "cf_direction":    None,
            "park_factor_runs": 100,
            "park_factor_hr":   100,
            "altitude_note":   None,
            "opened_year":     None,
            "last_updated":    today,
        }

        status = f"[{wind_effect}]"
        print(f"    venue_id={venue_id:<6} {name:<45} {status}")

        if not dry_run:
            conn.execute(UPDATE_SQL, params)
            seeded += 1

    if not dry_run:
        conn.commit()

    return {"seeded": seeded, "total": len(rows)}

def main():
    p = argparse.ArgumentParser(
        description="MLB Stadium Data Migration — safe for populated databases",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "EXAMPLES\n"
            "  python add_stadium_data.py              # run migration\n"
            "  python add_stadium_data.py --check      # verify state\n"
            "  python add_stadium_data.py --dry-run    # preview changes\n"
            "  python add_stadium_data.py --update      # re-seed (safe)\n"
            "  python add_stadium_data.py --verbose    # show every row\n"
            "  python add_stadium_data.py --seed-2025  # seed spring training / alt venues\n"
        ),
    )
    p.add_argument("--check",   action="store_true",
                   help="Verify migration state only — no writes")
    p.add_argument("--update",  action="store_true",
                   help="Re-seed all venue rows (safe to rerun)")
    p.add_argument("--dry-run", action="store_true",
                   help="Show what would change without writing anything")
    p.add_argument("--verbose", action="store_true",
                   help="Show every row update")
    p.add_argument("--seed-2025", action="store_true",
                   help="Seed any unseeded venue that hosted a 2025 game "
                        "(spring training / alternate sites). Safe default values applied.")
    p.add_argument("--db",      default=None,
                   help="Override DB path (default: mlb_stats.db in script folder)")
    args = p.parse_args()

    db_path = Path(args.db) if args.db else DB_PATH

    print(f"\n{'═'*64}")
    print(f"  MLB Stadium Data Migration")
    print(f"  {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    if args.dry_run:
        print(f"  MODE: DRY RUN — no changes will be written")
    print(f"{'═'*64}\n")

    conn = open_db(db_path)

    if args.check:
        run_check(conn)
        conn.close()
        return

    # --seed-2025 standalone: skip full migration, just seed 2025 venues
    if args.seed_2025 and not args.update:
        # Columns must already exist
        missing = [c for c in NEW_COLUMNS if c not in existing_columns(conn)]
        if missing:
            print(f"  ✗  Migration not yet run — missing columns: {missing}")
            print(f"     Run: python add_stadium_data.py   (full migration first)")
            conn.close()
            sys.exit(1)
        print(f"  Seeding unseeded 2025 venues (standalone):")
        s2025 = seed_2025_venues(conn, args.dry_run, args.verbose)
        if s2025["total"] > 0:
            print(f"  Result: {s2025['seeded']} / {s2025['total']} seeded")
        else:
            print("  No unseeded 2025 venues — all clear.")
        if not args.dry_run:
            run_check(conn)
        conn.close()
        mode = "Dry run complete." if args.dry_run else "Done."
        print(f"  {mode}\n")
        return

    # Step 1: Add missing columns via ALTER TABLE
    print("  Step 1 — Adding new columns to venues:")
    add_missing_columns(conn, args.dry_run)

    # Step 2: Populate new columns via UPDATE
    print(f"\n  Step 2 — Seeding {len(VENUES)} venue rows:")
    summary = seed_venues(conn, args.dry_run, args.verbose)

    print(f"\n  Result: {summary['updated']} / {summary['total']} venues updated")
    if summary["no_match"]:
        print(f"  ⚠  Unmatched venues: {summary['no_match']}")
        print(f"     Ensure load_teams_and_venues() has run at least once.")

    # Step 3 (optional): seed any unseeded 2025 venues
    if args.seed_2025 or getattr(args, "seed_2025", False):
        print(f"\n  Step 3 — Seeding unseeded 2025 venues:")
        s2025 = seed_2025_venues(conn, args.dry_run, args.verbose)
        if s2025["total"] > 0:
            print(f"  Result: {s2025['seeded']} / {s2025['total']} seeded")

    if not args.dry_run:
        run_check(conn)

    conn.close()
    mode = "Dry run complete — nothing written." if args.dry_run else "Migration complete."
    print(f"  {mode}\n")


if __name__ == "__main__":
    main()
