#!/usr/bin/env python3
"""
dress_games.py
──────────────
CLI: load games for a date and print Fully Dressed Game v1 (identifiers + environment).

Runs in parallel with generate_daily_brief — read-only DB access.

Example:
  python -m batch.pipeline.dress_games --date 2026-04-17
  python -m batch.pipeline.dress_games --date 2026-04-17 --json
  python -m batch.pipeline.dress_games --date 2026-04-17 --full --json
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import connect as db_connect, get_db_path

from batch.pipeline.dressed_game_blocks import dress_full_game_row, fully_dressed_to_json
from batch.pipeline.fully_dressed_game import (
    GameEnvironment,
    GameIdentifiers,
    dress_game_row,
)


def _table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({table})").fetchall()
        return {str(r[1]) for r in rows}
    except Exception:
        return set()


def load_game_rows(con: sqlite3.Connection, game_date: str) -> list[dict]:
    """Minimal join for dressing: identities + venue + conditions."""
    cols = _table_columns(con, "games")
    wind_src_sql = "COALESCE(g.wind_source, 'actual') AS wind_source" if "wind_source" in cols else "'actual' AS wind_source"
    cur = con.execute(
        f"""
        SELECT
            g.game_pk,
            g.game_date_et AS game_date_et,
            g.game_start_utc,
            g.season,
            g.status,
            g.venue_id,
            g.wind_mph,
            g.wind_direction,
            g.temp_f,
            {wind_src_sql},
            v.name AS venue_name,
            v.roof_type,
            v.wind_effect,
            v.park_factor_runs,
            v.park_factor_hr,
            v.orientation_hp,
            th.team_id AS home_team_id,
            th.abbreviation AS home_abbr,
            th.name AS home_name,
            ta.team_id AS away_team_id,
            ta.abbreviation AS away_abbr,
            ta.name AS away_name
        FROM games g
        JOIN teams th ON th.team_id = g.home_team_id
        JOIN teams ta ON ta.team_id = g.away_team_id
        LEFT JOIN venues v ON v.venue_id = g.venue_id
        WHERE g.game_date_et = ?
          AND g.game_type = 'R'
        ORDER BY g.game_start_utc, g.game_pk
        """,
        (game_date,),
    )
    return [dict(r) for r in cur.fetchall()]


def _as_json(obj: GameIdentifiers | GameEnvironment) -> dict:
    if isinstance(obj, GameIdentifiers):
        return {
            "game_pk": obj.game_pk,
            "game_date_et": obj.game_date_et,
            "game_start_et": obj.game_start_et,
            "season": obj.season,
            "home_team_id": obj.home_team_id,
            "home_team_abbr": obj.home_team_abbr,
            "home_team_name": obj.home_team_name,
            "away_team_id": obj.away_team_id,
            "away_team_abbr": obj.away_team_abbr,
            "away_team_name": obj.away_team_name,
            "venue_id": obj.venue_id,
            "venue_name": obj.venue_name,
        }
    return {
        "roof_type": obj.roof_type,
        "wind_effect": obj.wind_effect,
        "park_factor_runs": obj.park_factor_runs,
        "park_factor_hr": obj.park_factor_hr,
        "orientation_hp": obj.orientation_hp,
        "wind_mph": obj.wind_mph,
        "wind_direction": obj.wind_direction,
        "wind_dir_label": obj.wind_dir_label,
        "wind_in": obj.wind_in,
        "wind_out": obj.wind_out,
        "temp_f": obj.temp_f,
        "wind_source": obj.wind_source,
        "is_wind_suppressed": obj.is_wind_suppressed,
        "is_retractable": obj.is_retractable,
        "roof_status_known": obj.roof_status_known,
        "env_ceiling": obj.env_ceiling,
        "h3b_eligible": obj.h3b_eligible,
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Fully Dressed Game v1 — print enrichment for a slate date.")
    p.add_argument("--date", required=True, help="game_date_et YYYY-MM-DD")
    p.add_argument("--db", default=None, help="Path to mlb_stats.db (default: get_db_path())")
    p.add_argument("--json", action="store_true", help="Print JSON array instead of text")
    p.add_argument(
        "--full",
        action="store_true",
        help="Include matchup + market + completeness (blocks 3–6); requires DB for the full row pass",
    )
    p.add_argument("--skip-errors", action="store_true", help="Skip rows that fail dressing; print count to stderr")
    args = p.parse_args()

    db_path = str(Path(args.db).resolve()) if args.db else str(Path(get_db_path()).resolve())
    con = db_connect(db_path, timeout=30)
    con.row_factory = sqlite3.Row

    rows = load_game_rows(con, args.date.strip())

    out: list[dict] = []
    errors = 0
    for row in rows:
        try:
            if args.full:
                fd = dress_full_game_row(con, row)
                out.append(fully_dressed_to_json(fd))
            else:
                ids, env = dress_game_row(row)
                out.append({"identifiers": _as_json(ids), "environment": _as_json(env)})
        except Exception as e:
            errors += 1
            if not args.skip_errors:
                print(f"✗  game_pk={row.get('game_pk')}: {e}", file=sys.stderr)
                sys.exit(1)

    con.close()

    if args.json:
        print(json.dumps(out, indent=2))
    else:
        for item in out:
            if args.full:
                i = item["identifiers"]
                e = item["environment"]
                m = item["market"]
                c = item["completeness"]
                print(
                    f"{i['away_team_abbr']} @ {i['home_team_abbr']}  pk={i['game_pk']}  "
                    f"{i['game_start_et']}  {i['venue_name']}\n"
                    f"  env_ceiling={e['env_ceiling']}  wind_effect={e['wind_effect']}  "
                    f"wind={e['wind_mph']} mph {e['wind_dir_label']}  "
                    f"src={e['wind_source']}  h3b_eligible={e['h3b_eligible']}\n"
                    f"  market: book={m['odds_source']}  home_ml={m['home_ml_current']}  "
                    f"conf={m['market_confidence']}\n"
                    f"  completeness={c['completeness_tier']}  platoon_disc="
                    f"{item['matchup']['home_platoon_disadvantage']}"
                )
            else:
                i = item["identifiers"]
                e = item["environment"]
                print(
                    f"{i['away_team_abbr']} @ {i['home_team_abbr']}  pk={i['game_pk']}  "
                    f"{i['game_start_et']}  {i['venue_name']}\n"
                    f"  env_ceiling={e['env_ceiling']}  wind_effect={e['wind_effect']}  "
                    f"wind={e['wind_mph']} mph {e['wind_dir_label']}  "
                    f"src={e['wind_source']}  h3b_eligible={e['h3b_eligible']}"
                )
    if args.skip_errors and errors:
        print(f"[dress_games] skipped {errors} row(s) with errors", file=sys.stderr)


if __name__ == "__main__":
    main()
