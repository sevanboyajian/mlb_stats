#!/usr/bin/env python3
"""
One-time cleanup: remove bet_ledger rows that are not backed by a real brief pick.

Deletes staked rows (stake_units > 0) when:
  1) No brief_picks row matches (game_date, game_pk, market), after normalizing
     bet_ledger.market_type → brief_picks.market (moneyline→ML, total→TOTAL, etc.).
  2) A matching brief_picks row exists AND brief_picks.stake_multiplier = 0,
     if that column exists on the table (otherwise this branch is skipped).

Run from repo root:

  python scripts/remove_phantom_bet_ledger.py              # counts only (dry run)
  python scripts/remove_phantom_bet_ledger.py --execute    # apply DELETE + COMMIT

Optional:

  python scripts/remove_phantom_bet_ledger.py --db path/to/mlb_stats.db --execute
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

# bet_ledger.market_type uses values like 'moneyline','total','spread' (from signal_state);
# brief_picks.market uses 'ML','TOTAL','RL'.
_MARKET_JOIN_SQL = """
    bp.game_date = bl.game_date
    AND bp.game_pk = bl.game_pk
    AND bp.market = (
        CASE trim(lower(COALESCE(bl.market_type, '')))
            WHEN 'moneyline' THEN 'ML'
            WHEN 'total' THEN 'TOTAL'
            WHEN 'spread' THEN 'RL'
            WHEN 'runline' THEN 'RL'
            ELSE trim(COALESCE(bl.market_type, ''))
        END
    )
"""


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def main() -> int:
    ap = argparse.ArgumentParser(description="Remove phantom bet_ledger rows vs brief_picks.")
    ap.add_argument(
        "--db",
        type=str,
        default=None,
        help="SQLite DB path (default: get_db_path())",
    )
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
        bp_cols = _table_columns(conn, "brief_picks")
        if "stake_multiplier" not in bp_cols:
            print(
                "Note: brief_picks has no stake_multiplier column - "
                "only 'no matching brief_picks' deletions apply.\n"
                "      Add: ALTER TABLE brief_picks ADD COLUMN stake_multiplier REAL;\n"
                "      if you need zero-stake matches removed.\n"
            )
        has_sm = "stake_multiplier" in bp_cols

        j = _MARKET_JOIN_SQL.strip()

        # --- Orphan staked rows: no brief_picks match ---
        n_orphan = conn.execute(
            f"""
            SELECT COUNT(*) FROM bet_ledger bl
            LEFT JOIN brief_picks bp ON ({j})
            WHERE COALESCE(bl.stake_units, 0) > 0
              AND bp.id IS NULL
            """
        ).fetchone()[0]

        # --- Joined rows with stake_multiplier = 0 ---
        n_zero_bp = 0
        if has_sm:
            n_zero_bp = conn.execute(
                f"""
                SELECT COUNT(*) FROM bet_ledger bl
                INNER JOIN brief_picks bp ON ({j})
                WHERE COALESCE(bl.stake_units, 0) > 0
                  AND bp.stake_multiplier = 0
                """
            ).fetchone()[0]

        # Distinct IDs (UNION dedupes overlap between the two rules)
        if has_sm:
            n_total = conn.execute(
                f"""
                SELECT COUNT(*) FROM (
                    SELECT bl.id FROM bet_ledger bl
                    LEFT JOIN brief_picks bp ON ({j})
                    WHERE COALESCE(bl.stake_units, 0) > 0 AND bp.id IS NULL
                    UNION
                    SELECT bl.id FROM bet_ledger bl
                    INNER JOIN brief_picks bp ON ({j})
                    WHERE COALESCE(bl.stake_units, 0) > 0 AND bp.stake_multiplier = 0
                ) AS phantom_ids
                """
            ).fetchone()[0]
        else:
            n_total = int(n_orphan)

        print(f"Database: {db_path}")
        print(f"Candidate rows (stake_units > 0):")
        print(f"  - No matching brief_picks row (game_date + game_pk + market): {n_orphan}")
        if has_sm:
            print(f"  - Matching brief_picks with stake_multiplier = 0:         {n_zero_bp}")
        print(f"  TOTAL distinct bet_ledger IDs to delete: {n_total}")

        if not args.execute:
            print("\nDry run only. Re-run with --execute to DELETE and COMMIT.")
            return 0

        cur = conn.cursor()
        if has_sm:
            cur.execute(
                f"""
                DELETE FROM bet_ledger
                WHERE id IN (
                    SELECT id FROM (
                        SELECT bl.id FROM bet_ledger bl
                        LEFT JOIN brief_picks bp ON ({j})
                        WHERE COALESCE(bl.stake_units, 0) > 0 AND bp.id IS NULL
                        UNION
                        SELECT bl.id FROM bet_ledger bl
                        INNER JOIN brief_picks bp ON ({j})
                        WHERE COALESCE(bl.stake_units, 0) > 0 AND bp.stake_multiplier = 0
                    ) AS phantom_ids
                )
                """
            )
        else:
            cur.execute(
                f"""
                DELETE FROM bet_ledger
                WHERE id IN (
                    SELECT bl.id FROM bet_ledger bl
                    LEFT JOIN brief_picks bp ON ({j})
                    WHERE COALESCE(bl.stake_units, 0) > 0 AND bp.id IS NULL
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
