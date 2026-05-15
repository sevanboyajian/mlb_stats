#!/usr/bin/env python3
"""
backfill_game_odds_hours.py
---------------------------
Populate ``game_odds.hours_before_game`` when NULL by deriving hours from
``games.game_start_utc`` and ``game_odds.captured_at_utc``.

Same formula as ``mvf_clv_backtest.py`` (julianday delta * 24). Use this so
historical odds rows and other queries see consistent pregame hours without
re-ingesting from the API.

Usage (from repo root)::
    python batch/jobs/backfill_game_odds_hours.py --dry-run
    python batch/jobs/backfill_game_odds_hours.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import connect, get_db_path

_EFF_HOURS_SET = """
round(
    (
        julianday(replace(trim(replace(g.game_start_utc, 'Z', '')), 'T', ' '))
        - julianday(replace(trim(replace(go.captured_at_utc, 'Z', '')), 'T', ' '))
    ) * 24.0,
    4
)
""".replace("\n", " ").strip()


def main() -> int:
    p = argparse.ArgumentParser(description="Backfill game_odds.hours_before_game from timestamps.")
    p.add_argument("--db", default=None, help="SQLite path (default: MLB_DB_PATH / get_db_path())")
    p.add_argument("--dry-run", action="store_true", help="Show how many rows would update; no writes.")
    args = p.parse_args()

    db_path = args.db or get_db_path()
    print(f"db={db_path}")

    con = connect(db_path)
    try:
        cur = con.execute(
            f"""
            SELECT COUNT(*)
            FROM game_odds AS go
            JOIN games AS g ON g.game_pk = go.game_pk
            WHERE go.hours_before_game IS NULL
              AND g.game_start_utc IS NOT NULL
              AND go.captured_at_utc IS NOT NULL
              AND julianday(replace(trim(replace(g.game_start_utc, 'Z', '')), 'T', ' '))
                  > julianday(replace(trim(replace(go.captured_at_utc, 'Z', '')), 'T', ' '))
            """
        )
        n = int(cur.fetchone()[0])
        print(f"rows with NULL hours_before_game (pregame only): {n}")

        if args.dry_run or n == 0:
            if args.dry_run:
                print("dry-run: no UPDATE executed.")
            return 0

        cur_up = con.execute(
            f"""
            UPDATE game_odds AS go
            SET hours_before_game = ({_EFF_HOURS_SET})
            FROM games AS g
            WHERE go.game_pk = g.game_pk
              AND go.hours_before_game IS NULL
              AND g.game_start_utc IS NOT NULL
              AND go.captured_at_utc IS NOT NULL
              AND julianday(replace(trim(replace(g.game_start_utc, 'Z', '')), 'T', ' '))
                  > julianday(replace(trim(replace(go.captured_at_utc, 'Z', '')), 'T', ' '))
            """
        )
        con.commit()
        print(f"updated {cur_up.rowcount} row(s).")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
