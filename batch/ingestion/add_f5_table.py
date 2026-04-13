"""
add_f5_table.py
───────────────
Safe, non-destructive migration that adds the game_odds_f5 table,
its indexes, and the v_closing_f5_odds view to an existing mlb_stats.db.

Nothing is dropped or modified.  Existing data is untouched.
Safe to run multiple times — every statement uses IF NOT EXISTS.

Usage:
    python add_f5_table.py
    python add_f5_table.py --db C:/path/to/mlb_stats.db
    python add_f5_table.py --check   (verify state only, no changes)
"""

# CHANGE LOG (latest first)
# -------------------------
# 2026-04-13 16:24 ET  Refactor: route sqlite3.connect() calls through core.db.connection.connect().

import argparse
import os
import sqlite3
import sys
from datetime import datetime

from core.db.connection import connect as db_connect

# ── Default DB location: same folder as this script ───────────────────────────
DEFAULT_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mlb_stats.db")

# ── DDL statements ─────────────────────────────────────────────────────────────

TABLE_DDL = """
-- ------------------------------------------------------------
-- game_odds_f5
-- First-5-innings lines captured per bookmaker per game.
-- Isolates starting pitcher performance — the line closes when
-- the starter exits or 5 innings are complete.
--
-- API market keys pulled from The Odds API:
--   h2h_1st_5_innings        → F5 moneyline
--   spreads_1st_5_innings    → F5 run line
--   totals_1st_5_innings     → F5 total
--
-- Same backtest timestamp rule as game_odds:
--   captured_at_utc < games.game_start_utc
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS game_odds_f5 (
    id                  INTEGER PRIMARY KEY,
    game_pk             INTEGER NOT NULL REFERENCES games (game_pk),
    bookmaker           TEXT    NOT NULL,
    data_source         TEXT    NOT NULL DEFAULT 'the-odds-api',
    captured_at_utc     DATETIME NOT NULL,
    hours_before_game   REAL,              -- computed on insert

    -- ── F5 MONEYLINE ─────────────────────────────────────────
    -- Which team leads after 5 innings?
    home_f5_ml          INTEGER,           -- American odds e.g. -130
    away_f5_ml          INTEGER,

    -- ── F5 RUN LINE ──────────────────────────────────────────
    -- Usually ±0.5 (no-vig even) or ±1.5; varies by book
    home_f5_rl_line     REAL,
    home_f5_rl_odds     INTEGER,
    away_f5_rl_line     REAL,
    away_f5_rl_odds     INTEGER,

    -- ── F5 TOTAL ─────────────────────────────────────────────
    -- Over/under on combined runs through 5 innings
    f5_total_line       REAL,              -- e.g. 4.5
    f5_over_odds        INTEGER,
    f5_under_odds       INTEGER,

    -- ── LINE FLAGS ───────────────────────────────────────────
    is_opening_line     INTEGER NOT NULL DEFAULT 0,
    is_closing_line     INTEGER NOT NULL DEFAULT 0,

    UNIQUE (game_pk, bookmaker, captured_at_utc)
);
"""

INDEX_DDL = [
    "CREATE INDEX IF NOT EXISTS idx_f5_game    ON game_odds_f5 (game_pk);",
    "CREATE INDEX IF NOT EXISTS idx_f5_book    ON game_odds_f5 (bookmaker);",
    "CREATE INDEX IF NOT EXISTS idx_f5_closing ON game_odds_f5 (game_pk, is_closing_line);",
    "CREATE INDEX IF NOT EXISTS idx_f5_captured ON game_odds_f5 (captured_at_utc);",
]

VIEW_DDL = """
-- ------------------------------------------------------------
-- v_closing_f5_odds
-- The last F5 snapshot before first pitch per game per book.
-- Joins to games so you can filter by date or season.
-- Use this view for backtesting — never the raw table.
-- ------------------------------------------------------------
CREATE VIEW IF NOT EXISTS v_closing_f5_odds AS
SELECT
    f5.game_pk,
    g.game_date,
    g.season,
    f5.bookmaker,
    f5.home_f5_ml,
    f5.away_f5_ml,
    f5.home_f5_rl_line,
    f5.home_f5_rl_odds,
    f5.away_f5_rl_line,
    f5.away_f5_rl_odds,
    f5.f5_total_line,
    f5.f5_over_odds,
    f5.f5_under_odds,
    f5.captured_at_utc,
    f5.hours_before_game
FROM  game_odds_f5 f5
JOIN  games g ON g.game_pk = f5.game_pk
WHERE f5.is_closing_line = 1;
"""

# ── Schema for odds_ingest_log — update pull_type CHECK to include f5 types ───
# SQLite doesn't support ALTER TABLE to modify CHECK constraints, so we note
# this in the log but leave the constraint as-is.  The load_odds.py script
# will use existing pull_type values ('daily_pregame','historical_backfill').

# ── Helpers ────────────────────────────────────────────────────────────────────

def separator(char="─", width=60):
    print(char * width)

def check_state(con: sqlite3.Connection) -> dict:
    """Return current state of f5-related objects in the DB."""
    tables = {r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    views = {r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='view'"
    ).fetchall()}
    indexes = {r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='index'"
    ).fetchall()}

    f5_exists   = "game_odds_f5"     in tables
    view_exists = "v_closing_f5_odds" in views
    idx_count   = sum(1 for i in ["idx_f5_game","idx_f5_book",
                                   "idx_f5_closing","idx_f5_captured"]
                      if i in indexes)

    row_count = 0
    if f5_exists:
        row_count = con.execute("SELECT COUNT(*) FROM game_odds_f5").fetchone()[0]

    return {
        "f5_table":   f5_exists,
        "f5_view":    view_exists,
        "idx_count":  idx_count,
        "row_count":  row_count,
        "all_tables": tables,
    }


def print_state(state: dict):
    separator()
    print("  game_odds_f5 table   :", "✓ exists" if state["f5_table"] else "✗ missing")
    print("  v_closing_f5_odds    :", "✓ exists" if state["f5_view"]  else "✗ missing")
    print("  Indexes (4 expected) :", f"{state['idx_count']} / 4")
    if state["f5_table"]:
        print("  Rows in game_odds_f5 :", state["row_count"])
    separator()


def run_migration(con: sqlite3.Connection, verbose: bool = False):
    """Apply all DDL statements.  Safe to re-run."""
    steps = [
        ("game_odds_f5 table",    TABLE_DDL),
        ("v_closing_f5_odds view", VIEW_DDL),
    ]
    for (name, ddl) in steps:
        if verbose:
            print(f"  Applying: {name}")
        con.executescript(ddl)

    for idx_ddl in INDEX_DDL:
        name = idx_ddl.split("idx_")[1].split(" ")[0]
        if verbose:
            print(f"  Index: idx_{name}")
        con.executescript(idx_ddl)

    con.commit()


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Add game_odds_f5 table to mlb_stats.db (non-destructive).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  python add_f5_table.py
      Apply migration to mlb_stats.db in the same folder.

  python add_f5_table.py --db C:/Users/sevan/Database/mlb_stats.db
      Specify explicit path.

  python add_f5_table.py --check
      Show current state of f5 objects without making any changes.
""")
    parser.add_argument("--db",    default=DEFAULT_DB,
                        help=f"Path to database (default: {DEFAULT_DB})")
    parser.add_argument("--check", action="store_true",
                        help="Show state only — no changes made")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Show each DDL step")
    args = parser.parse_args()

    separator("═")
    print("  MLB Stats DB — F5 Table Migration")
    print(f"  Database : {args.db}")
    print(f"  Run at   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    separator("═")

    # ── Validate DB exists ─────────────────────────────────────────────────
    if not os.path.exists(args.db):
        print(f"\n  ERROR: Database not found: {args.db}")
        print("  Run create_db.py first to initialise the database.")
        sys.exit(1)

    con = db_connect(args.db)
    con.row_factory = sqlite3.Row

    # ── Verify this is the right DB (check for games table) ───────────────
    tables = {r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    if "games" not in tables:
        print("\n  ERROR: This does not look like mlb_stats.db — games table missing.")
        print("  Are you pointing at the correct file?")
        con.close()
        sys.exit(1)

    # ── Show pre-run state ─────────────────────────────────────────────────
    print("\n  State BEFORE migration:")
    before = check_state(con)
    print_state(before)

    if args.check:
        print("  --check mode: no changes made.")
        con.close()
        return

    # ── Already done? ──────────────────────────────────────────────────────
    if before["f5_table"] and before["f5_view"] and before["idx_count"] == 4:
        print("  All F5 objects already present — nothing to do.")
        con.close()
        return

    # ── Apply migration ────────────────────────────────────────────────────
    print("\n  Applying migration...")
    try:
        run_migration(con, args.verbose)
    except Exception as e:
        print(f"\n  ERROR during migration: {e}")
        con.rollback()
        con.close()
        sys.exit(1)

    # ── Show post-run state ────────────────────────────────────────────────
    print("\n  State AFTER migration:")
    after = check_state(con)
    print_state(after)

    # ── Final verdict ──────────────────────────────────────────────────────
    if after["f5_table"] and after["f5_view"] and after["idx_count"] == 4:
        print("  SUCCESS — F5 table, indexes, and view are ready.")
        print("  Existing data was not modified.")
        print("  load_odds.py will populate game_odds_f5 when --f5 is passed.")
    else:
        print("  WARNING — migration ran but some objects may be missing.")
        print("  Run with --check to inspect.")

    con.close()


if __name__ == "__main__":
    main()
