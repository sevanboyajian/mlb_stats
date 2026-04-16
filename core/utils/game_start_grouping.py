from __future__ import annotations

import datetime as dt
from typing import Any


def group_games_by_start_time(
    games: list[dict[str, Any]],
    window_minutes: int = 30,
) -> list[dict[str, Any]]:
    """
    Group games into deterministic clusters by scheduled start time (UTC).

    Requirements:
      - Input: games with game_start_utc (ISO string like '2026-04-15T22:40:00Z' or without Z)
      - Group games within a `window_minutes` window (default 30 minutes)
      - Assign group_id to each cluster (1..N, deterministic)

    Clustering rule:
      - Sort by (parsed_start_time, game_pk) for determinism
      - Start a new group when next game's start_time is > window_minutes AFTER
        the group's anchor start_time (the first game in that group)

    Output:
      [
        {"group_id": 1, "start_time": "<UTC iso>Z", "game_pks": [..]},
        ...
      ]
    """
    if not games:
        return []

    window = dt.timedelta(minutes=int(window_minutes))

    def _parse_start_utc_iso(game: dict[str, Any]) -> dt.datetime:
        raw = str(game.get("game_start_utc") or "").strip()
        if "T" not in raw:
            raise ValueError("All games must have a parseable game_start_utc")
        try:
            # Accept either "...Z" or naive ISO; treat as UTC-naive consistently.
            return dt.datetime.fromisoformat(raw.rstrip("Z"))
        except Exception as e:
            raise ValueError(f"Unparseable game_start_utc: {raw!r}") from e

    def _game_pk(game: dict[str, Any]) -> int:
        try:
            return int(game["game_pk"])
        except Exception as e:
            raise ValueError("All games must have an integer game_pk") from e

    ordered = sorted(games, key=lambda g: (_parse_start_utc_iso(g), _game_pk(g)))

    out: list[dict[str, Any]] = []
    group_id = 0

    cur_anchor: dt.datetime | None = None
    cur_pks: list[int] = []

    def _flush() -> None:
        nonlocal group_id, cur_anchor, cur_pks
        if cur_anchor is None:
            return
        group_id += 1
        start_iso = cur_anchor.replace(microsecond=0).isoformat() + "Z"
        out.append({"group_id": group_id, "start_time": start_iso, "game_pks": cur_pks})
        cur_anchor = None
        cur_pks = []

    for g in ordered:
        start = _parse_start_utc_iso(g)
        pk = _game_pk(g)

        if cur_anchor is None:
            cur_anchor = start
            cur_pks = [pk]
            continue

        if start - cur_anchor <= window:
            cur_pks.append(pk)
        else:
            _flush()
            cur_anchor = start
            cur_pks = [pk]

    _flush()
    return out

