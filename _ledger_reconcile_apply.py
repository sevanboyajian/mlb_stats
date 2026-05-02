"""Apply bet_ledger baseline reconciliation (run once)."""
import sqlite3

from core.db.connection import get_db_path

CONN = sqlite3.connect(get_db_path())
CONN.row_factory = sqlite3.Row


def fix1_delete_bal_phantom(conn):
    rows = conn.execute(
        """
        SELECT id FROM bet_ledger
        WHERE game_pk = 823557
          AND game_date = '2026-05-01'
          AND lower(trim(market_type)) IN ('ml', 'moneyline')
        """
    ).fetchall()
    if not rows:
        print("Fix1: no BAL phantom row (823557) — skipped")
        return
    conn.execute(
        """
        DELETE FROM bet_ledger
        WHERE game_pk = 823557
          AND game_date = '2026-05-01'
          AND lower(trim(market_type)) IN ('ml', 'moneyline')
        """
    )
    print(f"Fix1: deleted phantom BAL ML row(s) ids={[r['id'] for r in rows]}")


def fix2_insert_apr6_mv_f(conn):
    ex = conn.execute(
        "SELECT id FROM bet_ledger WHERE game_pk = 824619"
    ).fetchone()
    if ex:
        print(f"Fix2: game_pk 824619 already in bet_ledger id={ex['id']} — skipped")
        return
    # Slate: BAL @ CWS; away BAL won. brief_picks: BAL ML +120 MV-F WIN.
    conn.execute(
        """
        INSERT INTO bet_ledger (
            game_date, game_pk, market_type, bet,
            odds_taken, stake_units, signal_at_time,
            session, placed_at, total_line_at_bet,
            late_signal, model_version, result, pnl_units
        ) VALUES (
            '2026-04-06', 824619, 'moneyline', 'BAL ML',
            120, 1.0, 'top',
            'afternoon', '2026-04-06 15:16 ET', NULL,
            0, 'legacy', 'win', 1.20
        )
        """
    )
    print("Fix2: inserted 824619 BAL ML (away) MV-F win +1.20u")


def fix3_second_legacy_loss(conn):
    """Insert TOR ML LOSS 2026-04-22 (matched brief_picks id 67; away TOR lost vs LAA)."""
    ex = conn.execute(
        """
        SELECT id FROM bet_ledger
        WHERE game_pk = 824044 AND game_date = '2026-04-22'
          AND lower(trim(market_type)) IN ('moneyline','ml')
        """
    ).fetchone()
    if ex:
        print(f"Fix3: 824044 already ledger id={ex['id']} — skipped")
        return
    conn.execute(
        """
        INSERT INTO bet_ledger (
            game_date, game_pk, market_type, bet,
            odds_taken, stake_units, signal_at_time,
            session, placed_at, total_line_at_bet,
            late_signal, model_version, result, pnl_units
        ) VALUES (
            '2026-04-22', 824044, 'moneyline', 'TOR ML',
            124, 1.0, 'top',
            'early', '2026-04-22 11:40 ET', NULL,
            0, 'legacy', 'loss', -1.0
        )
        """
    )
    print("Fix3: inserted 824044 TOR ML loss −1u (legacy)")


def fix4_adjust_sea_min_total(conn):
    row = conn.execute(
        """
        SELECT id, odds_taken, pnl_units, total_line_at_bet
        FROM bet_ledger
        WHERE game_pk = 823716
          AND lower(trim(market_type)) IN ('total', 'totals')
        """
    ).fetchone()
    if not row:
        print("Fix4: no TOTAL row for 823716 — skipped")
        return
    conn.execute(
        """
        UPDATE bet_ledger
        SET odds_taken = -118,
            total_line_at_bet = 7.5,
            pnl_units = ROUND(100.0 / 118.0, 2)
        WHERE game_pk = 823716
          AND lower(trim(market_type)) IN ('total', 'totals')
        """
    )
    print(
        f"Fix4: updated 823716 OVER (was id={row['id']} odds={row['odds_taken']} pnl={row['pnl_units']}) "
        f"→ odds=-118 line=7.5 pnl=0.85"
    )


def print_verify(conn):
    q = """
    SELECT model_version,
           SUM(CASE WHEN result IN ('win','loss','push') THEN 1 ELSE 0 END) AS graded_bets,
           SUM(CASE WHEN result='win' THEN 1 ELSE 0 END) AS wins,
           SUM(CASE WHEN result='loss' THEN 1 ELSE 0 END) AS losses,
           ROUND(SUM(CASE WHEN result IN ('win','loss','push')
                         THEN COALESCE(pnl_units, 0) ELSE 0 END), 2) AS units
    FROM bet_ledger
    WHERE COALESCE(lower(trim(signal_at_time)), '') NOT IN ('avoid', '')
           OR signal_at_time IS NOT NULL
    """
    q = """
    SELECT model_version,
           SUM(CASE WHEN result IN ('win','loss','push') THEN 1 ELSE 0 END) AS graded_bets,
           SUM(CASE WHEN result='win' THEN 1 ELSE 0 END) AS wins,
           SUM(CASE WHEN result='loss' THEN 1 ELSE 0 END) AS losses,
           ROUND(SUM(CASE WHEN result IN ('win','loss','push')
                         THEN COALESCE(pnl_units, 0) ELSE 0 END), 2) AS units
    FROM bet_ledger
    WHERE lower(trim(COALESCE(signal_at_time,''))) <> 'avoid'
    GROUP BY model_version
    ORDER BY model_version
    """
    print("\n=== By model_version (graded non-avoid) ===")
    for r in conn.execute(q):
        print(dict(r))

    q2 = """
    SELECT SUM(CASE WHEN result IN ('win','loss','push') THEN 1 ELSE 0 END) AS total_bets,
           SUM(CASE WHEN result='win' THEN 1 ELSE 0 END) AS wins,
           SUM(CASE WHEN result='loss' THEN 1 ELSE 0 END) AS losses,
           ROUND(SUM(CASE WHEN result IN ('win','loss','push')
                         THEN COALESCE(pnl_units, 0) ELSE 0 END), 2) AS total_units
    FROM bet_ledger
    WHERE lower(trim(COALESCE(signal_at_time,''))) <> 'avoid'
    """
    print("\n=== Overall graded staked ===")
    print(dict(conn.execute(q2).fetchone()))


def main():
    fix1_delete_bal_phantom(CONN)
    fix2_insert_apr6_mv_f(CONN)
    fix3_second_legacy_loss(CONN)
    fix4_adjust_sea_min_total(CONN)
    CONN.commit()
    print_verify(CONN)
    CONN.close()


if __name__ == "__main__":
    main()
