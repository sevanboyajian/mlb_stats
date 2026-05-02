"""One-shot bet_ledger reconciliation — run once then delete."""
import sqlite3

from core.db.connection import get_db_path

DB = get_db_path()


def q(conn, sql, params=()):
    return conn.execute(sql, params).fetchall()


def main():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    print("=== Fix 1: BAL phantom 823557 2026-05-01 ML ===")
    rows = q(
        conn,
        """
        SELECT id, game_date, game_pk, market_type, bet, odds_taken, stake_units,
               signal_at_time, session, placed_at, result, pnl_units, model_version
        FROM bet_ledger
        WHERE game_pk = 823557
          AND game_date = '2026-05-01'
          AND upper(trim(market_type)) IN ('ML', 'MONEYLINE')
        """,
    )
    for r in rows:
        print(dict(r))
    if rows:
        cur = conn.execute(
            """
            DELETE FROM bet_ledger
            WHERE game_pk = 823557
              AND game_date = '2026-05-01'
              AND upper(trim(market_type)) IN ('ML', 'MONEYLINE')
            """,
        )
        print(f"Deleted {cur.rowcount} row(s)")
        conn.commit()

    print("\n=== Fix 2: Apr 6 MV-F 824619 ===")
    ex = q(conn, "SELECT * FROM bet_ledger WHERE game_pk = 824619", ())
    print(f"existing rows for 824619: {len(ex)}")
    if ex:
        for r in ex:
            print(dict(r))
    if not ex:
        conn.execute(
            """
            INSERT INTO bet_ledger (
                game_date, game_pk, market_type, bet,
                odds_taken, stake_units, signal_at_time,
                session, placed_at, total_line_at_bet,
                late_signal, model_version, result, pnl_units
            ) VALUES (
                '2026-04-06', 824619, 'ML', 'CWS ML (away)',
                120, 1.0, 'top',
                'afternoon', '2026-04-06 15:16 ET', NULL,
                0, 'legacy', 'win', 1.20
            )
            """
        )
        print("Inserted 824619 legacy win")
        conn.commit()

    print("\n=== Fix 3: Unmatched legacy brief_picks ===")
    # brief_picks.market is ML/TOTAL/etc; ledger may store moneyline/total — join both semantics
    join_sql = """
    SELECT bp.game_date, bp.game_pk, bp.market, bp.signal,
           bp.odds, bp.model_version, bp.recorded_at, bp.bet, bp.total_line, bp.pick_rank
    FROM brief_picks bp
    LEFT JOIN bet_ledger bl
      ON bl.game_pk = bp.game_pk
     AND bl.game_date = bp.game_date
     AND (
          (upper(trim(bl.market_type)) IN ('ML', 'MONEYLINE') AND upper(trim(bp.market)) = 'ML')
       OR (upper(trim(bl.market_type)) IN ('TOTAL', 'TOTALS') AND upper(trim(bp.market)) = 'TOTAL')
       OR (upper(trim(bl.market_type)) IN ('RL', 'RUNLINE', 'SPREAD') AND upper(trim(bp.market)) = 'RL')
         )
    WHERE bl.game_pk IS NULL
      AND bp.market NOT IN ('OTHER', 'ENV')
      AND COALESCE(bp.model_version, 'legacy') = 'legacy'
    ORDER BY bp.game_date, bp.recorded_at
    """
    unmatched = q(conn, join_sql)
    print(f"Unmatched legacy brief_picks: {len(unmatched)}")
    for r in unmatched:
        print(dict(r))

    if len(unmatched) == 1:
        bp = unmatched[0]
        gpk = int(bp["game_pk"])
        gd = bp["game_date"]
        odds = int(bp["odds"]) if bp["odds"] is not None else None

        gg = conn.execute(
            """
            SELECT g.home_team_id, g.away_team_id, g.home_score, g.away_score,
                   th.abbreviation AS home_abbr, ta.abbreviation AS away_abbr
            FROM games g
            JOIN teams th ON th.team_id = g.home_team_id
            JOIN teams ta ON ta.team_id = g.away_team_id
            WHERE g.game_pk = ?
            """,
            (gpk,),
        ).fetchone()
        if gg is None or gg["home_score"] is None:
            raise SystemExit(f"Cannot grade game_pk {gpk}: missing Final scores")

        hs, aws = int(gg["home_score"]), int(gg["away_score"])
        home_abbr = (gg["home_abbr"] or "").strip().upper()
        away_abbr = (gg["away_abbr"] or "").strip().upper()
        market = (bp["market"] or "").strip().upper()
        bet_text = (bp["bet"] or "").strip()

        result = None
        pnl = 0.0
        tlb = None

        def pnl_win(o: int | None) -> float:
            if o is None or o == 0:
                return 1.0
            return (o / 100.0) if o > 0 else (100.0 / abs(o))

        if market == "ML":
            mt_store = "ML"
            bt = bet_text.upper()
            team_abbr = bt.split()[0].strip().upper() if bt else ""
            away_won = aws > hs
            home_won = hs > aws
            if hs == aws:
                result, pnl = "push", 0.0
            elif team_abbr == away_abbr:
                won = away_won
                result = "win" if won else "loss"
                pnl = pnl_win(odds) if won else -1.0
            elif team_abbr == home_abbr:
                won = home_won
                result = "win" if won else "loss"
                pnl = pnl_win(odds) if won else -1.0
            else:
                raise SystemExit(f"Cannot parse ML team from bet={bet_text!r}")
        elif market == "TOTAL":
            mt_store = "TOTAL"
            runs = hs + aws
            total_line_at_bet = bp["total_line"]
            if total_line_at_bet is None:
                raise SystemExit(f"TOTAL pick missing total_line for {gpk}")
            tlb = float(total_line_at_bet)
            bu = bet_text.upper()
            if "OVER" in bu:
                if runs > tlb:
                    result, pnl = "win", pnl_win(odds)
                elif runs < tlb:
                    result, pnl = "loss", -1.0
                else:
                    result, pnl = "push", 0.0
            elif "UNDER" in bu:
                if runs < tlb:
                    result, pnl = "win", pnl_win(odds)
                elif runs > tlb:
                    result, pnl = "loss", -1.0
                else:
                    result, pnl = "push", 0.0
            else:
                raise SystemExit(f"Cannot parse total side from {bet_text!r}")
        else:
            mt_store = market
            raise SystemExit(f"Unhandled market {market} for automated insert")

        conn.execute(
            """
            INSERT INTO bet_ledger (
                game_date, game_pk, market_type, bet,
                odds_taken, stake_units, signal_at_time,
                session, placed_at, total_line_at_bet,
                late_signal, model_version, result, pnl_units
            ) VALUES (
                ?, ?, ?, ?,
                ?, 1.0, 'top',
                'primary', ?, ?,
                0, 'legacy', ?, ?
            )
            """,
            (
                gd,
                gpk,
                mt_store,
                bet_text or bp["bet"],
                odds,
                bp["recorded_at"] or f"{gd} 12:00 ET",
                tlb,
                result,
                round(pnl, 2),
            ),
        )
        print(f"\nInserted missing legacy bet game_pk={gpk} market={mt_store} result={result} pnl={pnl:.2f}")
        conn.commit()
    elif len(unmatched) != 1 and len(unmatched) != 0:
        print("Multiple unmatched — manual review required; no auto-insert.")

    print("\n=== Fix 4: SEA@MIN OVER 823716 ===")
    r823 = q(
        conn,
        """
        SELECT * FROM bet_ledger
        WHERE game_pk = 823716 AND upper(trim(market_type)) IN ('TOTAL', 'TOTALS')
        """,
    )
    if r823:
        print(dict(r823[0]))

    conn.close()
    print("\nDone. Run verification queries separately.")


if __name__ == "__main__":
    main()
