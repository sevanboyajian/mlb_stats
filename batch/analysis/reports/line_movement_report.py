#!/usr/bin/env python3
"""
line_movement_report.py
──────────────────────
Report: opening -> closing line movement summary for a season (default 2026).

Reads:
  - line_movement (computed by load_odds.py --compute-movement)
  - games / teams for matchup context

Outputs:
  - CSV (default: outputs/analysis/line_movement/season_<year>/line_movement_<year>_<today>.csv)
  - Console summary (counts + avg/median movement + steam/RLM rates)
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, "..", "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from core.db.connection import connect as db_connect, get_db_path


def _median(xs: List[float]) -> Optional[float]:
    xs2 = sorted(x for x in xs if x is not None)
    n = len(xs2)
    if n == 0:
        return None
    mid = n // 2
    if n % 2 == 1:
        return float(xs2[mid])
    return (float(xs2[mid - 1]) + float(xs2[mid])) / 2.0


def _default_out_path(season: int) -> Path:
    d = Path(_REPO_ROOT) / "outputs" / "analysis" / "line_movement" / f"season_{season}"
    d.mkdir(parents=True, exist_ok=True)
    return d / f"line_movement_{season}_{dt.date.today().isoformat()}.csv"


def fetch_line_movement(con: sqlite3.Connection, season: int) -> List[Dict[str, Any]]:
    cur = con.execute(
        """
        SELECT
            g.season,
            g.game_date_et,
            g.game_pk,
            g.game_start_utc,
            ta.abbreviation AS away_abbr,
            th.abbreviation AS home_abbr,
            lm.bookmaker,
            lm.market_type,
            lm.open_home_ml,
            lm.open_away_ml,
            lm.open_total,
            lm.open_captured_utc,
            lm.close_home_ml,
            lm.close_away_ml,
            lm.close_total,
            lm.close_captured_utc,
            lm.ml_move_cents,
            lm.total_move,
            lm.move_direction,
            lm.steam_move,
            lm.reverse_line_move
        FROM line_movement lm
        JOIN games g ON g.game_pk = lm.game_pk
        JOIN teams th ON th.team_id = g.home_team_id
        JOIN teams ta ON ta.team_id = g.away_team_id
        WHERE g.season = ?
          AND g.game_type = 'R'
        ORDER BY g.game_date_et, g.game_start_utc, g.game_pk, lm.bookmaker, lm.market_type
        """,
        (int(season),),
    )
    return [dict(r) for r in cur.fetchall()]


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as f:
            f.write("")
        return
    cols = list(rows[0].keys())
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


def summarize(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        print("No line_movement rows found for season.")
        return

    total_moves = [r.get("total_move") for r in rows if r.get("total_move") is not None]
    ml_moves = [float(r.get("ml_move_cents")) for r in rows if r.get("ml_move_cents") is not None]

    steam = sum(1 for r in rows if int(r.get("steam_move") or 0) == 1)
    rlm = sum(1 for r in rows if int(r.get("reverse_line_move") or 0) == 1)

    def _avg(xs: List[float]) -> Optional[float]:
        if not xs:
            return None
        return sum(xs) / len(xs)

    print(f"Rows: {len(rows)}")
    if total_moves:
        abs_tot = [abs(float(x)) for x in total_moves]
        print(f"Total move: avg(abs)={_avg(abs_tot):.3f}  median(abs)={_median(abs_tot):.3f}  max(abs)={max(abs_tot):.3f}")
    else:
        print("Total move: (none)")

    if ml_moves:
        abs_ml = [abs(float(x)) for x in ml_moves]
        print(f"ML move (cents): avg(abs)={_avg(abs_ml):.2f}  median(abs)={_median(abs_ml):.2f}  max(abs)={max(abs_ml):.2f}")
    else:
        print("ML move (cents): (none)")

    print(f"Steam move flagged: {steam}  ({steam/len(rows):.1%})")
    print(f"Reverse line move flagged: {rlm}  ({rlm/len(rows):.1%})")

    # Per bookmaker/market counts + avg abs movement
    by: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for r in rows:
        key = (str(r.get("bookmaker") or ""), str(r.get("market_type") or ""))
        by.setdefault(key, []).append(r)

    print("\nBy bookmaker / market_type:")
    for (bm, mt), lst in sorted(by.items(), key=lambda x: (x[0][0], x[0][1])):
        tot2 = [abs(float(r["total_move"])) for r in lst if r.get("total_move") is not None]
        ml2 = [abs(float(r["ml_move_cents"])) for r in lst if r.get("ml_move_cents") is not None]
        parts = [f"n={len(lst)}"]
        if tot2:
            parts.append(f"avg|total|={_avg(tot2):.3f}")
        if ml2:
            parts.append(f"avg|ml_cents|={_avg(ml2):.2f}")
        print(f"  {bm:<12} {mt:<10} " + "  ".join(parts))


def main() -> None:
    p = argparse.ArgumentParser(description="Report: opening->closing line movement for a season.")
    p.add_argument("--season", type=int, default=2026, help="Season year (default 2026)")
    p.add_argument("--out", default=None, help="Output CSV path (optional)")
    args = p.parse_args()

    db_path = get_db_path()
    con = db_connect(db_path, timeout=30)
    con.row_factory = sqlite3.Row

    rows = fetch_line_movement(con, int(args.season))

    out_path = Path(args.out) if args.out else _default_out_path(int(args.season))
    write_csv(out_path, rows)

    print(f"Wrote CSV -> {out_path}")
    summarize(rows)
    con.close()


if __name__ == "__main__":
    main()

