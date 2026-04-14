#!/usr/bin/env python3
"""
load_oddswarehouse.py
Import Odds Warehouse MLB_Basic.xlsx into game_odds for seasons 2022-2025.

Source format (one row per game):
  game id | date(YYYYMMDD) | away team | away score | away ml open | away ml close |
  over open | over open odds | over close | over close odds |
  home team | home score | home ml open | home ml close |
  under open | under open odds | under close | under close odds

Usage:
    python load_oddswarehouse.py
    python load_oddswarehouse.py --season 2024
    python load_oddswarehouse.py --clear
    python load_oddswarehouse.py --dry-run

Data quality issues handled (confirmed by inspection):
  1. Trailing spaces on ALL 30 team names starting mid-April 2024 — stripped
  2. CHW throughout  — map to CWS (DB uses CWS)
  3. OAK (2022-2024) — map to ATH (DB uses ATH for Oakland/Sacramento)
  4. ARI (2022-early 2024) — map to AZ (DB uses AZ; ARI last used 2024-04-12)
  5. AL / NL — All-Star game rows; skip entirely
  6. CIN - (one row, 2024-06-26) — strip dash, treat as CIN
  7. Duplicate game_ids (~91 cases) — two different games with same id reused
     across different dates; use (date + home_team + away_team) as match key
  8. No run line column — insert None for all RL fields
  9. Date format: YYYYMMDD integer -> ISO YYYY-MM-DD string
  10. Totals edge cases: 4.0 and 20.5 are valid (confirmed real games)
"""

# CHANGE LOG (latest first)
# -------------------------
# 2026-04-13 22:15 ET  Default DB from get_db_path(); repo root on sys.path for core.* imports.
# 2026-04-13 16:24 ET  Refactor: route sqlite3.connect() calls through core.db.connection.connect().

import argparse
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from core.db.connection import connect as db_connect, get_db_path

try:
    import openpyxl
except ImportError:
    print("ERROR: openpyxl not installed. Run: pip install openpyxl")
    sys.exit(1)

# ── Paths ─────────────────────────────────────────────────────────────────────
DEFAULT_DB  = get_db_path()
DEFAULT_XLS = r"C:\Users\sevan\OneDrive\Documents\Python\mlb_stats\OddsData\MLB_Basic.xlsx"

BOOKMAKER   = "oddswarehouse"
DATA_SOURCE = "oddswarehouse-xlsx"
TARGET_SEASONS = {2022, 2023, 2024, 2025}

# ── Team abbreviation map ─────────────────────────────────────────────────────
# Maps OddsWarehouse abbreviations → DB abbreviations.
# Strip trailing spaces first, then apply this map.
TEAM_MAP = {
    "CHW": "CWS",   # Chicago White Sox (DB uses CWS throughout)
    "OAK": "ATH",   # Oakland/Sacramento Athletics (DB uses ATH)
    "ARI": "AZ",    # Arizona Diamondbacks (OW used ARI through 2024-04-12, DB uses AZ)
}

# Skip these — All-Star game entries
SKIP_TEAMS = {"AL", "NL"}


def normalize_team(raw: str) -> str | None:
    """Strip whitespace, handle CIN-, apply team map. Return None to skip row."""
    if raw is None:
        return None
    t = raw.strip()
    # Handle CIN- edge case (one row confirmed: 2024-06-26)
    if t.startswith("CIN") and "-" in t:
        t = "CIN"
    if t in SKIP_TEAMS:
        return None          # signal: skip this game
    return TEAM_MAP.get(t, t)


def date_to_iso(yyyymmdd: int) -> str:
    """Convert 20220415 -> '2022-04-15'."""
    s = str(yyyymmdd)
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"


def upsert_odds_row(con: sqlite3.Connection, row: dict, dry_run: bool) -> int:
    """
    Insert one game_odds row.
    Duplicate check: game_pk + bookmaker + market_type + data_source + captured_at_utc.
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


def build_game_lookup(con: sqlite3.Connection) -> dict:
    """
    Build a lookup of (game_date, home_abbr, away_abbr) -> game_pk.
    Used as primary match key because OddsWarehouse game_ids are unreliable
    (confirmed 91 duplicate IDs in 2022-2025 mapping to different games).
    """
    rows = con.execute("""
        SELECT g.game_pk, g.game_date,
               th.abbreviation AS home_abbr,
               ta.abbreviation AS away_abbr
        FROM   games g
        JOIN   teams th ON th.team_id = g.home_team_id
        JOIN   teams ta ON ta.team_id = g.away_team_id
        WHERE  g.game_type = 'R'
        AND    g.season    IN (2022,2023,2024,2025)
    """).fetchall()

    lookup = {}
    for r in rows:
        key = (r["game_date"], r["home_abbr"], r["away_abbr"])
        lookup[key] = r["game_pk"]
    return lookup


def process_file(xlsx_path: Path, season_filter,  # int or None
                 con: sqlite3.Connection, dry_run: bool) -> dict:
    """
    Read MLB_Basic.xlsx, filter to target seasons, match to DB, insert odds.
    Returns result dict with counts.
    """
    print(f"  Reading {xlsx_path.name} ...")
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb.active

    print("  Building game lookup from DB ...")
    lookup = build_game_lookup(con)
    print(f"    {len(lookup):,} games in lookup (2022-2025 regular season)")

    stats = {
        "total_rows":     0,
        "skipped_season": 0,
        "skipped_allstar": 0,
        "skipped_no_match": 0,
        "skipped_dupe_row": 0,
        "inserted":       0,
        "by_season":      {},
    }

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")

    # Base row template — fields that don't vary per snapshot
    base_template = {
        "bookmaker":      BOOKMAKER,
        "data_source":    DATA_SOURCE,
        "hours_before_game": None,    # not available in this source
        "home_rl_line":   None,       # not in OddsWarehouse MLB_Basic
        "home_rl_odds":   None,
        "away_rl_line":   None,
        "away_rl_odds":   None,
    }

    for raw_row in ws.iter_rows(min_row=2, values_only=True):
        # Unpack columns (18 total)
        (ow_game_id, date_int, away_raw, away_score,
         away_ml_open, away_ml_close,
         over_open, over_open_odds, over_close, over_close_odds,
         home_raw, home_score,
         home_ml_open, home_ml_close,
         under_open, under_open_odds, under_close, under_close_odds) = raw_row

        # Skip header-like or null rows
        if not date_int or not isinstance(date_int, (int, float)):
            continue

        stats["total_rows"] += 1
        season = int(str(int(date_int))[:4])

        # Season filter
        if season not in TARGET_SEASONS:
            stats["skipped_season"] += 1
            continue
        if season_filter and season != season_filter:
            stats["skipped_season"] += 1
            continue

        # Normalize team names
        home_abbr = normalize_team(home_raw)
        away_abbr = normalize_team(away_raw)

        # Skip All-Star games
        if home_abbr is None or away_abbr is None:
            stats["skipped_allstar"] += 1
            continue

        # Convert date
        game_date = date_to_iso(int(date_int))

        # Match to DB game_pk using (date, home, away)
        game_pk = lookup.get((game_date, home_abbr, away_abbr))

        # Try UTC date+1 fallback for west-coast night games
        if game_pk is None:
            from datetime import date, timedelta
            try:
                d = date.fromisoformat(game_date)
                alt_date = (d + timedelta(days=1)).isoformat()
                game_pk = lookup.get((alt_date, home_abbr, away_abbr))
            except Exception:
                pass

        if game_pk is None:
            stats["skipped_no_match"] += 1
            continue

        # Update per-season stats
        if season not in stats["by_season"]:
            stats["by_season"][season] = {"matched": 0, "inserted": 0}
        stats["by_season"][season]["matched"] += 1

        base = {
            **base_template,
            "game_pk": game_pk,
        }

        rows_this_game = 0

        # ── 1. Opening moneyline ─────────────────────────────────────────────
        if home_ml_open is not None and away_ml_open is not None:
            ml_open = {
                **base,
                "captured_at_utc": f"{game_date}T13:00:00",   # ~hours before game nominal
                "market_type":     "moneyline",
                "home_ml":         home_ml_open,
                "away_ml":         away_ml_open,
                "total_line":      None,
                "over_odds":       None,
                "under_odds":      None,
                "is_opening_line": 1,
                "is_closing_line": 0,
            }
            rows_this_game += upsert_odds_row(con, ml_open, dry_run)

        # ── 2. Closing moneyline ─────────────────────────────────────────────
        if home_ml_close is not None and away_ml_close is not None:
            ml_close = {
                **base,
                "captured_at_utc": f"{game_date}T17:00:00",
                "market_type":     "moneyline",
                "home_ml":         home_ml_close,
                "away_ml":         away_ml_close,
                "total_line":      None,
                "over_odds":       None,
                "under_odds":      None,
                "is_opening_line": 0,
                "is_closing_line": 1,
            }
            rows_this_game += upsert_odds_row(con, ml_close, dry_run)

        # ── 3. Opening total ─────────────────────────────────────────────────
        if over_open is not None:
            tot_open = {
                **base,
                "captured_at_utc": f"{game_date}T13:00:00",
                "market_type":     "total",
                "home_ml":         None,
                "away_ml":         None,
                "total_line":      over_open,
                "over_odds":       over_open_odds,
                "under_odds":      under_open_odds,
                "is_opening_line": 1,
                "is_closing_line": 0,
            }
            rows_this_game += upsert_odds_row(con, tot_open, dry_run)

        # ── 4. Closing total ─────────────────────────────────────────────────
        if over_close is not None:
            tot_close = {
                **base,
                "captured_at_utc": f"{game_date}T17:00:00",
                "market_type":     "total",
                "home_ml":         None,
                "away_ml":         None,
                "total_line":      over_close,
                "over_odds":       over_close_odds,
                "under_odds":      under_close_odds,
                "is_opening_line": 0,
                "is_closing_line": 1,
            }
            rows_this_game += upsert_odds_row(con, tot_close, dry_run)

        stats["inserted"]                       += rows_this_game
        stats["by_season"][season]["inserted"]  += rows_this_game

    if not dry_run:
        con.commit()

    return stats


def main():
    p = argparse.ArgumentParser(
        description="Import Odds Warehouse MLB_Basic.xlsx into game_odds (2022-2025)")
    p.add_argument("--db",      default=DEFAULT_DB,  help="Path to mlb_stats.db")
    p.add_argument("--input",   default=DEFAULT_XLS, help="Path to MLB_Basic.xlsx")
    p.add_argument("--season",  type=int,            help="Load single season only (2022-2025)")
    p.add_argument("--clear",   action="store_true", help="Clear oddswarehouse rows before import")
    p.add_argument("--dry-run", action="store_true", help="Parse without writing to DB")
    args = p.parse_args()

    xlsx_path = Path(args.input)
    if not xlsx_path.exists():
        print(f"ERROR: File not found: {xlsx_path}")
        sys.exit(1)

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"ERROR: Database not found: {db_path}")
        sys.exit(1)

    print("=" * 60)
    print("  Odds Warehouse MLB Importer  —  2022-2025")
    print("=" * 60)

    con = db_connect(args.db)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA foreign_keys=ON")

    # Optional clear
    if args.clear and not args.dry_run:
        count = con.execute(
            "SELECT COUNT(*) FROM game_odds WHERE data_source = ?",
            (DATA_SOURCE,)).fetchone()[0]
        print(f"  Clearing {count:,} existing oddswarehouse rows ...")
        con.execute("DELETE FROM game_odds WHERE data_source = ?", (DATA_SOURCE,))
        con.execute(
            "DELETE FROM odds_ingest_log WHERE markets_pulled LIKE 'oddswarehouse%'")
        con.commit()
        print("  Cleared.")

    if args.dry_run:
        print("  DRY RUN — no data will be written")

    # Run import
    result = process_file(xlsx_path, args.season, con, args.dry_run)

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    print(f"  {'Season':<8}  {'Matched':>10}  {'Rows Inserted':>13}")
    print("  " + "-" * 35)
    for season in sorted(result["by_season"]):
        s = result["by_season"][season]
        print(f"  {season:<8}  {s['matched']:>10,}  {s['inserted']:>13,}")
    print("  " + "-" * 35)
    print(f"  {'TOTAL':<8}  {'':>10}  {result['inserted']:>13,}")
    print()
    print(f"  Rows skipped — wrong season : {result['skipped_season']:,}")
    print(f"  Rows skipped — all-star     : {result['skipped_allstar']:,}")
    print(f"  Rows skipped — no DB match  : {result['skipped_no_match']:,}")
    if args.dry_run:
        print("  (DRY RUN — nothing written)")
    print("=" * 60)

    # ── Write ingest log ──────────────────────────────────────────────────────
    if not args.dry_run and result["inserted"] > 0:
        total_matched = sum(s["matched"] for s in result["by_season"].values())
        try:
            con.execute("""
                INSERT INTO odds_ingest_log
                    (pulled_at_utc, pull_type, sport, markets_pulled,
                     games_covered, odds_rows_inserted, props_rows_inserted,
                     api_requests_used, api_quota_remaining, status, error_message)
                VALUES (?, 'historical_backfill', 'baseball_mlb',
                        'oddswarehouse:moneyline,total',
                        ?, ?, 0, 0, 0, 'success', NULL)
            """, (
                datetime.now(timezone.utc).isoformat(),
                total_matched,
                result["inserted"],
            ))
            con.commit()
            print("  Ingest log updated.")
        except Exception as e:
            print(f"  WARNING: Could not write ingest log: {e}")

    con.close()


if __name__ == "__main__":
    main()
