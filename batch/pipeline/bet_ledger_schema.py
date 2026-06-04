"""bet_ledger schema migrations shared by brief and score_today."""

from __future__ import annotations

import sqlite3


def ensure_bet_ledger_extended(conn: sqlite3.Connection) -> None:
    """
    Ensure bet_ledger exists and has source / signal_type / pick_side columns.
    Idempotent — safe from score_today and generate_daily_brief.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bet_ledger (
            id              INTEGER PRIMARY KEY,
            game_date       TEXT,
            game_pk         INTEGER,
            market_type     TEXT,
            bet             TEXT,
            odds_taken      INTEGER,
            stake_units     REAL,
            signal_at_time  TEXT,
            session         TEXT,
            placed_at       TEXT,
            total_line_at_bet REAL,
            late_signal     INTEGER NOT NULL DEFAULT 0,
            model_version   TEXT NOT NULL DEFAULT 'legacy',
            result          TEXT,
            pnl_units       REAL
        )
        """
    )
    try:
        conn.execute("DROP INDEX IF EXISTS idx_bet_ledger_game_market")
    except sqlite3.OperationalError:
        pass
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_bet_ledger_game_market_signal
        ON bet_ledger (game_pk, market_type, IFNULL(signal_at_time, ''))
        """
    )
    cols = {r[1] for r in conn.execute("PRAGMA table_info(bet_ledger)").fetchall()}
    for col, ddl in (
        ("total_line_at_bet", "ALTER TABLE bet_ledger ADD COLUMN total_line_at_bet REAL"),
        ("late_signal", "ALTER TABLE bet_ledger ADD COLUMN late_signal INTEGER NOT NULL DEFAULT 0"),
        ("model_version", "ALTER TABLE bet_ledger ADD COLUMN model_version TEXT DEFAULT 'legacy'"),
        ("source", "ALTER TABLE bet_ledger ADD COLUMN source TEXT DEFAULT 'brief'"),
        ("signal_type", "ALTER TABLE bet_ledger ADD COLUMN signal_type TEXT"),
        ("pick_side", "ALTER TABLE bet_ledger ADD COLUMN pick_side TEXT"),
    ):
        if col not in cols:
            try:
                conn.execute(ddl)
            except sqlite3.OperationalError:
                pass
    try:
        conn.execute(
            "UPDATE bet_ledger SET source = 'brief' WHERE source IS NULL OR TRIM(source) = ''"
        )
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_bet_ledger_score_today_signal
            ON bet_ledger (game_date, game_pk, IFNULL(signal_type, ''))
            WHERE source = 'score_today'
            """
        )
    except sqlite3.OperationalError:
        pass
    conn.commit()
