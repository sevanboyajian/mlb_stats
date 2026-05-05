#!/usr/bin/env python3
"""
One-time cleanup: remove signal_state TOP/NEXT rows that can regenerate phantom bet_ledger bets.

Why:
- The prior report calls backfill_bet_ledger_from_signal_state(), which materializes bets from
  signal_state into bet_ledger.
- If you delete phantom rows from bet_ledger only, the next prior run can recreate them if the
  underlying signal_state rows still exist.

This script deletes *only* signal_state rows where:
  - signal_type IN ('top','next')
  - AND there is no matching brief_picks row for (game_date, game_pk, market, bet)

It does NOT touch signal_state 'avoid' rows.

Run from repo root:
  python scripts/remove_phantom_signal_state.py              # dry run (counts only)
  python scripts/remove_phantom_signal_state.py --execute    # DELETE + COMMIT

Optional:
  python scripts/remove_phantom_signal_state.py --db path/to/mlb_stats.db --execute
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

# Repo root on path for core.*
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import get_db_path  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Remove phantom signal_state TOP/NEXT rows not backed by brief_picks."
    )
    ap.add_argument("--db", type=str, default=None, help="SQLite DB path (default: get_db_path())")
    ap.add_argument(
        "--execute",
        action="store_true",
        help="Run DELETE and COMMIT. Without this flag, only print candidate counts.",
    )
    args = ap.parse_args()

    db_path = Path(args.db).resolve() if args.db else Path(get_db_path()).resolve()
    if not db_path.is_file():
        print(f"ERROR: database file not found: {db_path}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        has_ss = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='signal_state'"
        ).fetchone()
        has_bp = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='brief_picks'"
        ).fetchone()
        if not has_ss or not has_bp:
            print("Nothing to do: missing signal_state or brief_picks table.")
            return 0

        # market mapping: signal_state.market_type -> brief_picks.market
        # signal_state uses values like 'moneyline','total','spread' while brief_picks uses 'ML','TOTAL','RL'.
        join_market = """
            bp.market = (
                CASE trim(lower(COALESCE(ss.market_type, '')))
                    WHEN 'moneyline' THEN 'ML'
                    WHEN 'total' THEN 'TOTAL'
                    WHEN 'spread' THEN 'RL'
                    WHEN 'runline' THEN 'RL'
                    ELSE trim(COALESCE(ss.market_type, ''))
                END
            )
        """.strip()

        # Candidate = top/next signal_state rows with no matching brief pick (same bet string too).
        n = conn.execute(
            f"""
            SELECT COUNT(*) AS n
            FROM signal_state ss
            LEFT JOIN brief_picks bp
                   ON bp.game_date = ss.game_date
                  AND bp.game_pk   = ss.game_pk
                  AND ({join_market})
                  AND TRIM(COALESCE(bp.bet,'')) = TRIM(COALESCE(ss.bet,''))
            WHERE ss.signal_type IN ('top','next')
              AND bp.id IS NULL
            """
        ).fetchone()[0]

        print(f"Database: {db_path}")
        print("Candidate signal_state rows to delete:")
        print(f"  - TOP/NEXT with no matching brief_picks (game_date+game_pk+market+bet): {int(n)}")

        if not args.execute:
            print("\nDry run only. Re-run with --execute to DELETE and COMMIT.")
            return 0

        cur = conn.execute(
            f"""
            DELETE FROM signal_state
            WHERE id IN (
                SELECT ss.id
                FROM signal_state ss
                LEFT JOIN brief_picks bp
                       ON bp.game_date = ss.game_date
                      AND bp.game_pk   = ss.game_pk
                      AND ({join_market})
                      AND TRIM(COALESCE(bp.bet,'')) = TRIM(COALESCE(ss.bet,''))
                WHERE ss.signal_type IN ('top','next')
                  AND bp.id IS NULL
            )
            """
        )
        print(f"Rows deleted: {cur.rowcount}")
        conn.commit()
        print("COMMIT OK.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

