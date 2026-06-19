"""pitcher_rolling_stats schema migrations (home/away split WMA columns)."""

from __future__ import annotations

import sqlite3


def ensure_pitcher_rolling_splits(conn: sqlite3.Connection) -> None:
    """
    Add home/away split WMA columns to pitcher_rolling_stats if missing.
    Idempotent — safe from build_pitcher_wma and pipeline jobs.
    """
    cols = {
        r[1] for r in conn.execute("PRAGMA table_info(pitcher_rolling_stats)").fetchall()
    }
    for col, ddl in (
        ("era_wma_home", "ALTER TABLE pitcher_rolling_stats ADD COLUMN era_wma_home REAL"),
        ("era_wma_away", "ALTER TABLE pitcher_rolling_stats ADD COLUMN era_wma_away REAL"),
        ("k_per_9_wma_home", "ALTER TABLE pitcher_rolling_stats ADD COLUMN k_per_9_wma_home REAL"),
        ("k_per_9_wma_away", "ALTER TABLE pitcher_rolling_stats ADD COLUMN k_per_9_wma_away REAL"),
        ("whip_wma_home", "ALTER TABLE pitcher_rolling_stats ADD COLUMN whip_wma_home REAL"),
        ("whip_wma_away", "ALTER TABLE pitcher_rolling_stats ADD COLUMN whip_wma_away REAL"),
        (
            "starts_in_window_home",
            "ALTER TABLE pitcher_rolling_stats ADD COLUMN starts_in_window_home "
            "INTEGER NOT NULL DEFAULT 0",
        ),
        (
            "starts_in_window_away",
            "ALTER TABLE pitcher_rolling_stats ADD COLUMN starts_in_window_away "
            "INTEGER NOT NULL DEFAULT 0",
        ),
    ):
        if col not in cols:
            try:
                conn.execute(ddl)
            except sqlite3.OperationalError:
                pass
    conn.commit()
