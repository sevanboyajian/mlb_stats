#!/usr/bin/env python3
"""
diagnostics/debug_phantom_bets.py
--------------------------------
Read-only inspection script to help locate "phantom" bets/P&L rows and identify
which tables/views are contributing to prior reports.

Usage:
  python diagnostics/debug_phantom_bets.py --date 2026-05-03
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import get_db_path


def _table_exists(con: sqlite3.Connection, name: str) -> bool:
    r = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = ?",
        (name,),
    ).fetchone()
    return bool(r)


def _cols(con: sqlite3.Connection, table: str) -> list[str]:
    try:
        info = con.execute(f"PRAGMA table_info({table})").fetchall()
        return [str(r[1]) for r in info]
    except Exception:
        return []


def _dump_query(con: sqlite3.Connection, title: str, sql: str, params: tuple) -> None:
    print("\n" + "=" * 88)
    print(title)
    print("=" * 88)
    try:
        rows = con.execute(sql, params).fetchall()
    except Exception as exc:
        print(f"QUERY ERROR: {exc!r}")
        return
    print(f"rows: {len(rows)}")
    for r in rows[:200]:
        # sqlite3.Row -> dict
        try:
            d = dict(r)
        except Exception:
            d = {f"c{i}": r[i] for i in range(len(r))}
        print(d)
    if len(rows) > 200:
        print(f"... truncated ({len(rows) - 200} more row(s))")


def main() -> None:
    p = argparse.ArgumentParser(description="Inspect DB for phantom bets/P&L rows (read-only).")
    p.add_argument("--date", required=True, help="Game date ET (YYYY-MM-DD), e.g. 2026-05-03")
    p.add_argument(
        "--search-bet",
        default=None,
        help="Optional substring to search in bet fields across ledger-ish tables (e.g. 'HOU ML').",
    )
    p.add_argument(
        "--search-pnl",
        type=float,
        default=None,
        help="Optional pnl_units value to search for across bet_ledger/daily_pnl (no date filter).",
    )
    args = p.parse_args()
    game_date = str(args.date).strip()
    search_bet = (str(args.search_bet).strip() if args.search_bet is not None else "") or None
    search_pnl = float(args.search_pnl) if args.search_pnl is not None else None

    # Windows consoles can default to cp1252; avoid crashing on unicode.
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    db = get_db_path()
    print(f"db: {db}")

    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row

    # Quick inventory of the likely contributors.
    targets = [
        "bet_ledger",
        "daily_pnl",
        "brief_picks",
        "brief_log",
        "bet_snapshots",
        "signal_state",
        "pipeline_jobs",
        "pipeline_job_runs",
        "v_closing_game_odds",
    ]
    print("\nobjects:")
    for t in targets:
        print(f"  {t:<20} exists={_table_exists(con, t)} cols={_cols(con, t)[:12]}")

    # daily_pnl is often the "season P&L" source.
    if _table_exists(con, "daily_pnl"):
        _dump_query(
            con,
            f"daily_pnl rows for {game_date}",
            "SELECT * FROM daily_pnl WHERE game_date = ? ORDER BY id",
            (game_date,),
        )

    # bet_ledger can contain placed bets that later roll into daily_pnl.
    if _table_exists(con, "bet_ledger"):
        cols = set(_cols(con, "bet_ledger"))
        date_col = "game_date" if "game_date" in cols else ("game_date_et" if "game_date_et" in cols else None)
        if date_col:
            _dump_query(
                con,
                f"bet_ledger rows for {game_date} (by {date_col})",
                f"SELECT * FROM bet_ledger WHERE {date_col} = ? ORDER BY id",
                (game_date,),
            )

    # signal_state can repopulate bet_ledger (especially during prior runs).
    if _table_exists(con, "signal_state"):
        cols = set(_cols(con, "signal_state"))
        if "game_date" in cols:
            _dump_query(
                con,
                f"signal_state rows for {game_date}",
                "SELECT * FROM signal_state WHERE game_date = ? ORDER BY game_pk, recorded_at, id LIMIT 200",
                (game_date,),
            )

    # brief_picks can also represent "bets" depending on how reports are built.
    if _table_exists(con, "brief_picks"):
        cols = set(_cols(con, "brief_picks"))
        date_col = "game_date" if "game_date" in cols else ("game_date_et" if "game_date_et" in cols else None)
        if date_col:
            _dump_query(
                con,
                f"brief_picks rows for {game_date} (by {date_col})",
                f"SELECT * FROM brief_picks WHERE {date_col} = ? ORDER BY id",
                (game_date,),
            )

    # Also scan for team abbreviations in free text for quick spotting of HOU/CWS phantoms.
    # (We keep it best-effort and limited.)
    for t in ("daily_pnl", "bet_ledger", "brief_picks"):
        if not _table_exists(con, t):
            continue
        cols = _cols(con, t)
        text_cols = [c for c in cols if c.lower() in ("bet", "notes", "signal", "team", "pick", "market")]
        if not text_cols:
            continue
        where = " OR ".join([f"{c} LIKE ?" for c in text_cols])
        params = tuple(["%HOU%"] * len(text_cols))
        _dump_query(con, f"{t}: rows containing 'HOU' in {text_cols}", f"SELECT * FROM {t} WHERE {where} LIMIT 200", params)
        params = tuple(["%CWS%"] * len(text_cols))
        _dump_query(con, f"{t}: rows containing 'CWS' in {text_cols}", f"SELECT * FROM {t} WHERE {where} LIMIT 200", params)

    if search_bet:
        for t in ("bet_ledger", "brief_picks", "daily_pnl", "signal_state"):
            if not _table_exists(con, t):
                continue
            cols = set(_cols(con, t))
            if "bet" not in cols:
                continue
            # No date filter here; this is for global find.
            _dump_query(
                con,
                f"{t}: rows where bet LIKE '%{search_bet}%' (no date filter)",
                f"SELECT * FROM {t} WHERE bet LIKE ? ORDER BY 1 DESC LIMIT 200",
                (f"%{search_bet}%",),
            )

    if search_pnl is not None:
        for t in ("bet_ledger", "daily_pnl"):
            if not _table_exists(con, t):
                continue
            cols = set(_cols(con, t))
            if "pnl_units" not in cols:
                continue
            _dump_query(
                con,
                f"{t}: rows where pnl_units ~= {search_pnl} (no date filter)",
                f"""
                SELECT *
                FROM {t}
                WHERE pnl_units IS NOT NULL
                  AND ABS(CAST(pnl_units AS REAL) - ?) < 0.0005
                ORDER BY 1 DESC
                LIMIT 200
                """,
                (float(search_pnl),),
            )

    con.close()


if __name__ == "__main__":
    main()

