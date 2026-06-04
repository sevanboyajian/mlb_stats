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
    repair_away_dog_rl_stake_units(conn)
    conn.commit()


def repair_away_dog_rl_stake_units(conn: sqlite3.Connection) -> int:
    """
    Correct brief rows staked at 1.00u that should be Away Dog RL (0.10u).
    Scales graded pnl_units when stake was overstated.
    """
    from batch.pipeline.score_game import AWAY_DOG_RL_STAKE

    try:
        cur = conn.execute(
            """
            UPDATE bet_ledger
            SET stake_units = ?,
                signal_type = COALESCE(NULLIF(TRIM(signal_type), ''), 'AWAY_DOG_RL'),
                pnl_units = CASE
                    WHEN result IS NOT NULL AND pnl_units IS NOT NULL AND stake_units > ?
                    THEN ROUND(pnl_units * (? / stake_units), 4)
                    ELSE pnl_units
                END
            WHERE market_type IN ('spread', 'runline')
              AND stake_units > ?
              AND upper(trim(bet)) LIKE '%+1.5%'
              AND (
                COALESCE(signal_type, '') = 'AWAY_DOG_RL'
                OR COALESCE(signal_at_time, '') LIKE 'score_today:AWAY_DOG_RL'
                OR id IN (
                    SELECT bl.id
                    FROM bet_ledger bl
                    INNER JOIN bet_snapshots bs
                        ON bs.game_date = bl.game_date
                       AND bs.game_pk = bl.game_pk
                       AND bs.market_type = 'RL'
                    WHERE upper(COALESCE(bs.signals_used, '')) LIKE '%AWAY_DOG_RL%'
                      AND bl.market_type IN ('spread', 'runline')
                      AND upper(trim(bl.bet)) LIKE '%+1.5%'
                )
              )
            """,
            (
                AWAY_DOG_RL_STAKE,
                AWAY_DOG_RL_STAKE,
                AWAY_DOG_RL_STAKE,
                AWAY_DOG_RL_STAKE,
            ),
        )
        return int(getattr(cur, "rowcount", 0) or 0)
    except sqlite3.OperationalError:
        return 0
