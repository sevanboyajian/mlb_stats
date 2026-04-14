#!/usr/bin/env python3
"""
load_sbro.py — Import SportsBookReviewsOnline (SBRO) historical MLB odds
               into the game_odds table.

Source:  https://www.sportsbookreviewsonline.com/scoresoddsarchives/mlb/mlboddsarchives.htm
Files:   mlb-odds-YYYY.xlsx  (one file per season, 2015-2021)
Folder:  C:\\Users\\sevan\\OneDrive\\Documents\\Python\\mlb_stats\\OddsData\\

Usage:
    # Recommended: clear existing odds rows first (see --clear flag)
    python load_sbro.py --clear

    # Import all XLSX files found in OddsData folder
    python load_sbro.py

    # Import a specific season only
    python load_sbro.py --season 2019

    # Dry run — shows what would be loaded without touching the DB
    python load_sbro.py --dry-run

    # Full wipe + import in one command
    python load_sbro.py --clear --season 2021

SBRO format (one row per team per game, two rows per game):
    Date    MMDD integer (e.g. 401 = April 1)
    Rot     Daily rotation number (visitor=odd, home=even; resets each day)
    VH      V=visitor/away, H=home
    Team    3-letter abbreviation (SBRO-specific — mapped to MLB Stats API below)
    Pitcher Starting pitcher name
    1st-9th Inning-by-inning scores ('x' for incomplete innings)
    Final   Final score
    Open    Opening moneyline (American odds)
    Close   Closing moneyline
    RunLine Run line (always ±1.5); next unnamed col is the RL odds
    OpenOU  Opening over/under line; next unnamed col is the over odds
    CloseOU Closing over/under line; next unnamed col is the over odds

Notes:
    - Single bookmaker source (sharp offshore, historically 5Dimes/Pinnacle-era)
    - bookmaker stored as 'sbro' in game_odds
    - is_opening_line=1 and is_closing_line=1 both set (open and close are
      separate columns in the same row, not separate snapshots)
    - No F5 data in SBRO files — game_odds_f5 is unaffected
    - 2022-2024 not available from SBRO; use Odds Warehouse for those years
"""

# CHANGE LOG (latest first)
# -------------------------
# 2026-04-13 22:15 ET  Default DB from get_db_path(); repo root on sys.path for core.* imports.
# 2026-04-13 16:24 ET  Refactor: route sqlite3.connect() calls through core.db.connection.connect().

import argparse
import logging
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import openpyxl
from core.db.connection import connect as db_connect, get_db_path

# ── Paths ─────────────────────────────────────────────────────────────────────
DEFAULT_DB   = get_db_path()
ODDS_DIR     = r"C:\Users\sevan\OneDrive\Documents\Python\mlb_stats\OddsData"
BOOKMAKER    = "sbro"          # stored in game_odds.bookmaker for all SBRO rows
DATA_SOURCE  = "sbro-xlsx"     # stored in game_odds.data_source

# ── SBRO → MLB Stats API team abbreviation map ────────────────────────────────
# SBRO uses non-standard abbreviations for 6 teams.
# All others match the MLB Stats API abbreviations already in your teams table.
TEAM_MAP = {
    "KAN": "KC",    # Kansas City Royals
    "SDG": "SD",    # San Diego Padres
    "SFO": "SF",    # San Francisco Giants
    "TAM": "TB",    # Tampa Bay Rays
    "WAS": "WSH",   # Washington Nationals
    "CUB": "CHC",   # Chicago Cubs
    "ARI": "AZ",    # Arizona Diamondbacks (DB uses AZ not ARI)
    "OAK": "ATH",   # Oakland/Sacramento Athletics (DB uses ATH not OAK)
    "LOS": "LAD",   # Los Angeles Dodgers in 2015-2017 (SBRO used LOS, switched to LAD ~2018)
}

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# TEAM LOOKUP
# ══════════════════════════════════════════════════════════════════════════════

def build_team_lookup(con: sqlite3.Connection) -> dict:
    """Return {abbreviation: team_id} from the teams table."""
    rows = con.execute("SELECT abbreviation, team_id FROM teams").fetchall()
    return {r["abbreviation"]: r["team_id"] for r in rows}


def resolve_team(sbro_abbr: str, team_lookup: dict) -> int | None:
    """Map a SBRO team abbreviation to a DB team_id. Returns None if unresolved."""
    abbr = TEAM_MAP.get(sbro_abbr, sbro_abbr)
    return team_lookup.get(abbr)


# ══════════════════════════════════════════════════════════════════════════════
# GAME MATCHING
# ══════════════════════════════════════════════════════════════════════════════

def build_game_lookup(con: sqlite3.Connection, season: int) -> dict:
    """
    Return a lookup dict for matching SBRO rows to DB game_pks.

    Key: (game_date_str, away_team_id, home_team_id)
    Value: list of game_pk (list because doubleheaders share the same key)
    """
    rows = con.execute("""
        SELECT game_pk, game_date, home_team_id, away_team_id
        FROM   games
        WHERE  season    = ?
        AND    game_type = 'R'
    """, (season,)).fetchall()

    lookup = {}
    for r in rows:
        key = (r["game_date"], r["away_team_id"], r["home_team_id"])
        lookup.setdefault(key, [])
        lookup[key].append(r["game_pk"])
    return lookup


# ══════════════════════════════════════════════════════════════════════════════
# SBRO DATE DECODING
# ══════════════════════════════════════════════════════════════════════════════

def decode_date(mmdd: int, season: int) -> str:
    """
    Convert SBRO MMDD integer to ISO date string.
    e.g. 401 + season 2021 → '2021-04-01'
         1003 + season 2021 → '2021-10-03'
    """
    mmdd_str = str(mmdd).zfill(4)
    month = int(mmdd_str[:2])
    day   = int(mmdd_str[2:])
    return f"{season}-{month:02d}-{day:02d}"


# ══════════════════════════════════════════════════════════════════════════════
# UPSERT
# ══════════════════════════════════════════════════════════════════════════════

def upsert_odds_row(con: sqlite3.Connection, row: dict, dry_run: bool) -> int:
    """
    Insert one game_odds row. Skips if an identical SBRO row already exists
    for this game_pk + bookmaker + market_type.
    Returns 1 if inserted, 0 if skipped.
    """
    if dry_run:
        return 1

    existing = con.execute("""
        SELECT id FROM game_odds
        WHERE  game_pk        = ?
        AND    bookmaker       = ?
        AND    market_type     = ?
        AND    data_source     = ?
        AND    captured_at_utc = ?
    """, (row["game_pk"], BOOKMAKER, row["market_type"],
          DATA_SOURCE, row["captured_at_utc"])).fetchone()

    if existing:
        return 0

    con.execute("""
        INSERT INTO game_odds (
            game_pk, bookmaker, data_source, captured_at_utc,
            hours_before_game, market_type,
            home_ml, away_ml,
            home_rl_line, home_rl_odds, away_rl_line, away_rl_odds,
            total_line, over_odds, under_odds,
            is_opening_line, is_closing_line
        ) VALUES (
            :game_pk, :bookmaker, :data_source, :captured_at_utc,
            :hours_before_game, :market_type,
            :home_ml, :away_ml,
            :home_rl_line, :home_rl_odds, :away_rl_line, :away_rl_odds,
            :total_line, :over_odds, :under_odds,
            :is_opening_line, :is_closing_line
        )
    """, row)
    return 1


# ══════════════════════════════════════════════════════════════════════════════
# PARSE ONE XLSX FILE
# ══════════════════════════════════════════════════════════════════════════════

def parse_file(
    xlsx_path: Path,
    season: int,
    con: sqlite3.Connection,
    dry_run: bool,
) -> dict:
    """
    Parse one SBRO Excel file and load into game_odds.
    Returns summary dict.
    """
    log.info("Parsing %s (season %d) ...", xlsx_path.name, season)

    team_lookup = build_team_lookup(con)
    game_lookup = build_game_lookup(con, season)

    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb.active
    raw_rows = list(ws.iter_rows(min_row=2, values_only=True))
    wb.close()

    # SBRO rows come in V/H pairs — process two at a time
    total_games   = 0
    matched       = 0
    unmatched     = 0
    rows_inserted = 0
    rows_skipped  = 0
    team_errors   = 0

    # Track doubleheader assignment: (date_str, away_id, home_id) → index into game_pk list
    dh_counter: dict[tuple, int] = {}

    i = 0
    while i < len(raw_rows) - 1:
        v_raw = raw_rows[i]
        h_raw = raw_rows[i + 1]
        i += 2

        # Validate pairing
        if v_raw[2] != "V" or h_raw[2] != "H":
            log.warning("  Unexpected V/H pairing at row %d — skipping pair", i)
            unmatched += 1
            continue

        total_games += 1

        # Decode date
        date_str = decode_date(v_raw[0], season)

        # Resolve teams
        away_id = resolve_team(v_raw[3], team_lookup)
        home_id = resolve_team(h_raw[3], team_lookup)

        if not away_id or not home_id:
            log.warning("  Unknown team: %s or %s on %s", v_raw[3], h_raw[3], date_str)
            team_errors += 1
            unmatched += 1
            continue

        # Match to DB game — handle doubleheaders
        # Primary key: SBRO local date
        key = (date_str, away_id, home_id)
        game_pks = game_lookup.get(key, [])

        # UTC date-shift fallback: west coast night games start at 7pm PDT = 02:00 UTC
        # next day, so the MLB Stats API stores them under the following calendar date.
        # If the primary key misses, try date + 1 day before giving up.
        if not game_pks:
            from datetime import date as _date, timedelta
            next_date = (
                _date.fromisoformat(date_str) + timedelta(days=1)
            ).isoformat()
            key_next = (next_date, away_id, home_id)
            game_pks = game_lookup.get(key_next, [])
            if game_pks:
                log.debug("  UTC shift match: %s vs %s  SBRO=%s → DB=%s",
                          v_raw[3], h_raw[3], date_str, next_date)

        if not game_pks:
            log.debug("  No DB game found: %s vs %s on %s", v_raw[3], h_raw[3], date_str)
            unmatched += 1
            continue

        # For doubleheaders, assign game 1 first, game 2 second
        dh_idx = dh_counter.get(key, 0)
        if dh_idx < len(game_pks):
            game_pk = game_pks[dh_idx]
            dh_counter[key] = dh_idx + 1
        else:
            log.debug("  Extra game beyond DB count: %s vs %s on %s", v_raw[3], h_raw[3], date_str)
            unmatched += 1
            continue

        matched += 1
        captured_utc = f"{date_str}T17:00:00"  # noon ET proxy, same as historical endpoint

        # ── Extract odds fields ───────────────────────────────────────────
        # V row = away team  |  H row = home team
        # Moneyline: positive = underdog, negative = favorite (American odds)
        # SBRO stores away ML on V row, home ML on H row

        open_away_ml  = v_raw[15]   # Open ML for away team
        open_home_ml  = h_raw[15]   # Open ML for home team
        close_away_ml = v_raw[16]
        close_home_ml = h_raw[16]

        rl_away_line  = v_raw[17]   # always +1.5 for away (underdog by convention)
        rl_away_odds  = v_raw[18]
        rl_home_line  = h_raw[17]   # always -1.5 for home
        rl_home_odds  = h_raw[18]

        open_ou_line  = v_raw[19]   # same for both rows
        open_ou_odds  = v_raw[20]
        close_ou_line = v_raw[21]
        close_ou_odds = v_raw[22]

        base = {
            "game_pk":          game_pk,
            "bookmaker":        BOOKMAKER,
            "data_source":      DATA_SOURCE,
            "captured_at_utc":  captured_utc,
            "hours_before_game": None,   # not available in SBRO
            "home_ml":          None,
            "away_ml":          None,
            "home_rl_line":     None,
            "home_rl_odds":     None,
            "away_rl_line":     None,
            "away_rl_odds":     None,
            "total_line":       None,
            "over_odds":        None,
            "under_odds":       None,
            "is_opening_line":  1,
            "is_closing_line":  1,
        }

        # ── Opening moneyline row ─────────────────────────────────────────
        ml_open = {**base,
            "market_type": "moneyline",
            "captured_at_utc": f"{date_str}T13:00:00",  # tag opening ~9am ET
            "home_ml":  open_home_ml,
            "away_ml":  open_away_ml,
            "is_opening_line": 1,
            "is_closing_line": 0,
        }
        rows_inserted += upsert_odds_row(con, ml_open, dry_run)

        # ── Closing moneyline row ─────────────────────────────────────────
        ml_close = {**base,
            "market_type": "moneyline",
            "captured_at_utc": f"{date_str}T17:00:00",  # tag closing ~1pm ET
            "home_ml":  close_home_ml,
            "away_ml":  close_away_ml,
            "is_opening_line": 0,
            "is_closing_line": 1,
        }
        rows_inserted += upsert_odds_row(con, ml_close, dry_run)

        # ── Run line (closing only — SBRO has one RL snapshot) ───────────
        rl_row = {**base,
            "market_type":  "runline",
            "home_rl_line": rl_home_line,
            "home_rl_odds": rl_home_odds,
            "away_rl_line": rl_away_line,
            "away_rl_odds": rl_away_odds,
        }
        rows_inserted += upsert_odds_row(con, rl_row, dry_run)

        # ── Opening total row ─────────────────────────────────────────────
        total_open = {**base,
            "market_type":   "total",
            "captured_at_utc": f"{date_str}T13:00:00",
            "total_line":    open_ou_line,
            "over_odds":     open_ou_odds,
            "under_odds":    open_ou_odds,   # SBRO stores one odds value for both
            "is_opening_line": 1,
            "is_closing_line": 0,
        }
        rows_inserted += upsert_odds_row(con, total_open, dry_run)

        # ── Closing total row ─────────────────────────────────────────────
        total_close = {**base,
            "market_type":   "total",
            "captured_at_utc": f"{date_str}T17:00:00",
            "total_line":    close_ou_line,
            "over_odds":     close_ou_odds,
            "under_odds":    close_ou_odds,
            "is_opening_line": 0,
            "is_closing_line": 1,
        }
        rows_inserted += upsert_odds_row(con, total_close, dry_run)

        rows_skipped += (5 - (rows_inserted - (rows_inserted - rows_skipped)))

    if not dry_run:
        con.commit()

    return {
        "season":         season,
        "total_games":    total_games,
        "matched":        matched,
        "unmatched":      unmatched,
        "team_errors":    team_errors,
        "rows_inserted":  rows_inserted,
    }


# ══════════════════════════════════════════════════════════════════════════════
# CLEAR ODDS TABLES
# ══════════════════════════════════════════════════════════════════════════════

def clear_odds_tables(con: sqlite3.Connection, dry_run: bool):
    """Delete all rows from game_odds only. Leaves game_odds_f5 and player_props intact."""
    count = con.execute("SELECT COUNT(*) FROM game_odds").fetchone()[0]
    log.info("Clearing game_odds: %d rows will be deleted ...", count)
    if not dry_run:
        con.execute("DELETE FROM game_odds")
        con.execute("DELETE FROM odds_ingest_log WHERE pull_type = 'historical_backfill'")
        con.commit()
        log.info("game_odds cleared.")
    else:
        log.info("DRY RUN — no rows deleted.")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(description="Load SBRO historical MLB odds into game_odds")
    p.add_argument("--db",      default=DEFAULT_DB,  help="Path to mlb_stats.db")
    p.add_argument("--dir",     default=ODDS_DIR,    help="Folder containing SBRO xlsx files")
    p.add_argument("--season",  type=int,            help="Load a single season only (e.g. 2019)")
    p.add_argument("--clear",   action="store_true", help="Clear game_odds before importing")
    p.add_argument("--dry-run", action="store_true", help="Parse files without writing to DB")
    args = p.parse_args()

    odds_dir = Path(args.dir)
    if not odds_dir.exists():
        log.error("OddsData folder not found: %s", odds_dir)
        sys.exit(1)

    # Find XLSX files
    if args.season:
        files = sorted(odds_dir.glob(f"mlb-odds-{args.season}.xlsx"))
    else:
        files = sorted(odds_dir.glob("mlb-odds-????.xlsx"))

    if not files:
        log.error("No mlb-odds-YYYY.xlsx files found in %s", odds_dir)
        sys.exit(1)

    log.info("Files to process: %s", [f.name for f in files])

    # Connect
    con = db_connect(args.db)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA foreign_keys=ON")

    # Optionally clear
    if args.clear:
        clear_odds_tables(con, args.dry_run)

    # Process each file
    all_results = []
    for xlsx_path in files:
        # Infer season from filename: mlb-odds-2021.xlsx → 2021
        try:
            season = int(xlsx_path.stem.split("-")[-1])
        except ValueError:
            log.warning("Cannot parse season from filename: %s — skipping", xlsx_path.name)
            continue

        result = parse_file(xlsx_path, season, con, args.dry_run)
        all_results.append(result)

        log.info(
            "  %d: %d games found, %d matched, %d unmatched, %d rows inserted",
            result["season"], result["total_games"],
            result["matched"], result["unmatched"], result["rows_inserted"]
        )

    # Summary
    log.info("")
    log.info("=" * 55)
    log.info("  SUMMARY")
    log.info("=" * 55)
    log.info("  %-6s  %10s  %8s  %9s  %13s",
             "Season", "Games Found", "Matched", "Unmatched", "Rows Inserted")
    log.info("  " + "-" * 52)
    total_inserted = 0
    for r in all_results:
        log.info("  %-6d  %10d  %8d  %9d  %13d",
                 r["season"], r["total_games"],
                 r["matched"], r["unmatched"], r["rows_inserted"])
        total_inserted += r["rows_inserted"]
    log.info("  " + "-" * 52)
    log.info("  Total rows inserted: %d", total_inserted)
    if args.dry_run:
        log.info("  (DRY RUN — nothing written to database)")
    log.info("=" * 55)

    # Write a single summary entry to odds_ingest_log so load_odds.py --check
    # reflects the SBRO import in the ingest history
    if not args.dry_run and total_inserted > 0:
        seasons_loaded = [str(r["season"]) for r in all_results if r["rows_inserted"] > 0]
        total_matched  = sum(r["matched"]  for r in all_results)
        try:
            con2 = db_connect(args.db)
            con2.execute("""
                INSERT INTO odds_ingest_log
                    (pulled_at_utc, pull_type, sport, markets_pulled,
                     games_covered, odds_rows_inserted, props_rows_inserted,
                     api_requests_used, api_quota_remaining, status, error_message)
                VALUES (?, 'historical_backfill', 'baseball_mlb', 'sbro:moneyline,runline,total',
                        ?, ?, 0, 0, 0, 'success', NULL)
            """, (
                __import__("datetime").datetime.utcnow().isoformat(),
                total_matched,
                total_inserted,
            ))
            con2.commit()
            con2.close()
            log.info("  Ingest log updated.")
        except Exception as e:
            log.warning("  Could not write ingest log: %s", e)

    con.close()


if __name__ == "__main__":
    main()
