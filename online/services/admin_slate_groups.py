"""Slate by start-time group (ET) for MLB Scout Admin — matches ``schedule_pipeline_day`` game query + grouping."""

from __future__ import annotations

import datetime as dt
import sqlite3
from typing import Any

import pandas as pd

from core.utils.game_start_grouping import group_games_by_start_time

try:
    from zoneinfo import ZoneInfo

    _ET = ZoneInfo("America/New_York")
except Exception:
    _ET = None


def fetch_games_for_slate(
    con: sqlite3.Connection, game_date_et: str
) -> list[dict[str, Any]]:
    cur = con.execute(
        """
        SELECT
            g.game_pk,
            g.game_start_utc,
            ta.abbreviation AS away_abbr,
            th.abbreviation AS home_abbr
        FROM games g
        LEFT JOIN teams ta ON ta.team_id = g.away_team_id
        LEFT JOIN teams th ON th.team_id = g.home_team_id
        WHERE g.game_date_et = ?
          AND g.game_type = 'R'
        ORDER BY g.game_start_utc, g.game_pk
        """,
        (game_date_et,),
    )
    return [dict(r) for r in cur.fetchall()]


def _first_pitch_clock_et(game_start_utc: str) -> str:
    raw = (game_start_utc or "").strip()
    if "T" not in raw:
        return "?"
    try:
        d_utc = dt.datetime.fromisoformat(raw.rstrip("Z")).replace(
            tzinfo=dt.timezone.utc
        )
    except Exception:
        return "?"
    if _ET is not None:
        t = d_utc.astimezone(_ET)
    else:
        t = d_utc
    h12 = t.strftime("%I").lstrip("0") or "12"
    return f"{h12}:{t.strftime('%M')} {t.strftime('%p')}"


def build_slate_by_group_dataframe(
    con: sqlite3.Connection,
    game_date_et: str,
    *,
    group_window_min: int = 30,
) -> pd.DataFrame:
    games = fetch_games_for_slate(con, game_date_et)
    if not games:
        return pd.DataFrame(
            columns=["group", "time_et", "matchup", "game_pk", "time_sort"]
        )
    groups = group_games_by_start_time(
        games, window_minutes=int(group_window_min)
    )
    pk_map: dict[int, dict[str, Any]] = {}
    for g in games:
        try:
            pk_map[int(g["game_pk"])] = g
        except Exception:
            continue
    rows: list[dict[str, Any]] = []
    for grp in groups:
        gid = int(grp["group_id"])
        for pk in grp.get("game_pks") or []:
            row = pk_map.get(int(pk))
            if not row:
                continue
            away = row.get("away_abbr") or "?"
            home = row.get("home_abbr") or "?"
            raw = str(row.get("game_start_utc") or "")
            t_et = _first_pitch_clock_et(raw)
            sort_key: dt.datetime | None = None
            try:
                d_utc = dt.datetime.fromisoformat(raw.rstrip("Z"))
                if _ET is not None:
                    sort_key = d_utc.replace(tzinfo=dt.timezone.utc).astimezone(
                        _ET
                    )
                else:
                    sort_key = d_utc
            except Exception:
                sort_key = None
            rows.append(
                {
                    "group": f"{gid:02d}",
                    "time_et": t_et,
                    "matchup": f"{away} @ {home}",
                    "game_pk": int(pk),
                    "time_sort": sort_key,
                }
            )
    if not rows:
        return pd.DataFrame(
            columns=["group", "time_et", "matchup", "game_pk", "time_sort"]
        )
    df = pd.DataFrame(rows)
    return df.sort_values(
        by=["time_sort", "group", "game_pk"],
        na_position="last",
    ).drop(columns=["time_sort"])


__all__ = [
    "build_slate_by_group_dataframe",
    "fetch_games_for_slate",
]
