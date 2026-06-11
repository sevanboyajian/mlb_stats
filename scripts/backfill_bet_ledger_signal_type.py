#!/usr/bin/env python3
"""Targeted backfill of bet_ledger signal_type / pick_side for NULL rows."""

from __future__ import annotations

import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.db.connection import connect, get_db_path

ET = ZoneInfo("America/New_York")
OUT_DIR = ROOT / "outputs" / "regression"

STEPS = [
    (
        "step1_legacy",
        """
        UPDATE bet_ledger
        SET signal_type = 'LEGACY'
        WHERE signal_type IS NULL
          AND model_version = 'legacy'
        """,
    ),
    (
        "step2_under",
        """
        UPDATE bet_ledger
        SET signal_type = 'UNDER',
            pick_side   = 'under'
        WHERE signal_type IS NULL
          AND market_type IN ('total', 'TOTAL')
        """,
    ),
    (
        "step3_away_dog_rl",
        """
        UPDATE bet_ledger
        SET signal_type = 'AWAY_DOG_RL',
            pick_side   = 'away_rl'
        WHERE signal_type IS NULL
          AND market_type IN ('spread', 'runline', 'RL')
          AND stake_units = 0.10
          AND game_date >= '2026-06-02'
        """,
    ),
    (
        "step4_rl_favorite",
        """
        UPDATE bet_ledger
        SET signal_type = 'RL',
            pick_side   = 'home_rl'
        WHERE signal_type IS NULL
          AND market_type IN ('spread', 'runline', 'RL')
          AND stake_units = 1.00
        """,
    ),
    (
        "step5a_ml_logreg",
        """
        UPDATE bet_ledger
        SET signal_type = 'ML'
        WHERE signal_type IS NULL
          AND market_type IN ('moneyline', 'ML')
          AND model_version = 'v2'
          AND (
              (odds_taken BETWEEN -199 AND -150)
              OR odds_taken <= -300
          )
        """,
    ),
    (
        "step5b_owm",
        """
        UPDATE bet_ledger
        SET signal_type = 'OWM',
            pick_side   = 'home_ml'
        WHERE signal_type IS NULL
          AND market_type IN ('moneyline', 'ML')
          AND model_version = 'v2'
          AND game_date >= '2026-05-25'
        """,
    ),
    (
        "step6_mv_b",
        """
        UPDATE bet_ledger
        SET signal_type = 'MV-B'
        WHERE signal_type IS NULL
          AND market_type IN ('moneyline', 'ML')
          AND model_version = 'v2'
        """,
    ),
    (
        "step7_unclassified",
        """
        UPDATE bet_ledger
        SET signal_type = 'UNCLASSIFIED'
        WHERE signal_type IS NULL
        """,
    ),
]

VERIFY_SQL = """
SELECT
    signal_type,
    COUNT(*) AS n,
    SUM(CASE WHEN result='win' THEN 1 ELSE 0 END) AS wins,
    ROUND(100.0 * SUM(CASE WHEN result='win' THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*),0), 1) AS win_pct,
    ROUND(SUM(COALESCE(pnl_units,0)), 2) AS pnl_units,
    ROUND(100.0 * SUM(COALESCE(pnl_units,0))
          / NULLIF(SUM(stake_units),0), 1) AS roi_pct
FROM bet_ledger
WHERE result IS NOT NULL
GROUP BY signal_type
ORDER BY n DESC
"""

REMAINING_NULL_SQL = """
SELECT COUNT(*) AS remaining_null
FROM bet_ledger
WHERE signal_type IS NULL OR TRIM(COALESCE(signal_type, '')) = ''
"""


def _count_null(conn: sqlite3.Connection) -> int:
    return int(conn.execute(REMAINING_NULL_SQL).fetchone()[0])


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Backfill bet_ledger signal_type")
    parser.add_argument("--db", default=get_db_path())
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    con = connect(args.db)
    con.row_factory = sqlite3.Row
    lines: list[str] = []
    stamp = datetime.now(tz=ET).strftime("%Y%m%d_%H%M%S_ET")

    before = _count_null(con)
    lines.append(f"Before backfill: {before} rows with NULL/empty signal_type")

    step_counts: list[tuple[str, int]] = []
    for name, sql in STEPS:
        if args.dry_run:
            # Count rows that would match (approximate via subquery pattern)
            count_sql = f"SELECT COUNT(*) FROM bet_ledger WHERE rowid IN (SELECT rowid FROM ({sql.replace('UPDATE bet_ledger', 'SELECT rowid FROM bet_ledger')}))"  # noqa: E501
            # Simpler: run SELECT version of WHERE clause
            where = sql.split("WHERE", 1)[1].strip().rstrip(";")
            n = con.execute(f"SELECT COUNT(*) FROM bet_ledger WHERE {where}").fetchone()[0]
            step_counts.append((name, int(n)))
            lines.append(f"[dry-run] {name}: would update {n} rows")
        else:
            cur = con.execute(sql)
            n = int(getattr(cur, "rowcount", 0) or 0)
            step_counts.append((name, n))
            lines.append(f"{name}: updated {n} rows")

    if not args.dry_run:
        con.commit()

    after = _count_null(con)
    lines.append(f"After backfill: {after} rows with NULL/empty signal_type")
    lines.append("")
    lines.append("VERIFICATION (graded rows by signal_type)")
    lines.append("-" * 72)

    rows = con.execute(VERIFY_SQL).fetchall()
    header = f"{'signal_type':<16} {'n':>5} {'wins':>5} {'win%':>7} {'pnl':>8} {'roi%':>7}"
    lines.append(header)
    for r in rows:
        lines.append(
            f"{str(r['signal_type'] or 'NULL'):<16} {int(r['n']):>5} "
            f"{int(r['wins'] or 0):>5} {float(r['win_pct'] or 0):>7.1f} "
            f"{float(r['pnl_units'] or 0):>8.2f} {float(r['roi_pct'] or 0):>7.1f}"
        )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / f"bet_ledger_backfill_{stamp}.txt"
    latest = OUT_DIR / "bet_ledger_backfill_latest.txt"
    text = "\n".join(lines) + "\n"
    out_path.write_text(text, encoding="utf-8")
    latest.write_text(text, encoding="utf-8")

    print(text)
    print(f"Report: {out_path}")
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
