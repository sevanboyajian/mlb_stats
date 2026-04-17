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


def ensure_pipeline_jobs_table(con: Any) -> None:
    """
    Create pipeline_jobs table + indexes if they do not exist.
    Uses duck-typed DB connection (sqlite3.Connection-like).
    """
    # New schema (ET-first, with optional windows). If the table already exists,
    # we also attempt a lightweight migration by adding missing columns.
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_jobs (
            job_id            INTEGER PRIMARY KEY AUTOINCREMENT,
            job_type          TEXT    NOT NULL,
            job_date_et       TEXT    NOT NULL,
            scheduled_time_et TEXT    NOT NULL,
            scheduled_time_utc DATETIME,
            window_start_et   TEXT,
            window_end_et     TEXT,
            status            TEXT    NOT NULL DEFAULT 'pending'
                                  CHECK (status IN ('pending','running','complete','failed','timeout')),
            game_group_id     INTEGER,
            started_at        DATETIME,
            completed_at      DATETIME,
            error_message     TEXT,
            retry_count       INTEGER NOT NULL DEFAULT 0,
            retries           INTEGER NOT NULL DEFAULT 0,
            created_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    # Best-effort migration for older table versions.
    try:
        cols = {r[1] for r in con.execute("PRAGMA table_info(pipeline_jobs)").fetchall()}
    except Exception:
        cols = set()

    def _add(col_sql: str) -> None:
        try:
            con.execute(f"ALTER TABLE pipeline_jobs ADD COLUMN {col_sql}")
        except Exception:
            pass

    if "job_date_et" not in cols:
        _add("job_date_et TEXT")
    if "scheduled_time_et" not in cols:
        _add("scheduled_time_et TEXT")
    if "scheduled_time_utc" not in cols:
        _add("scheduled_time_utc DATETIME")
    if "window_start_et" not in cols:
        _add("window_start_et TEXT")
    if "window_end_et" not in cols:
        _add("window_end_et TEXT")
    if "started_at" not in cols:
        _add("started_at DATETIME")
    if "completed_at" not in cols:
        _add("completed_at DATETIME")
    if "error_message" not in cols:
        _add("error_message TEXT")
    if "retry_count" not in cols:
        _add("retry_count INTEGER NOT NULL DEFAULT 0")
    if "retries" not in cols:
        _add("retries INTEGER NOT NULL DEFAULT 0")

    # Indexes (ET-based)
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_jobs_unique
            ON pipeline_jobs (job_type, scheduled_time_et, game_group_id)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_status_time
            ON pipeline_jobs (status, scheduled_time_et)
        """
    )
    try:
        con.commit()
    except Exception:
        pass


def schedule_pipeline_jobs_for_game_groups(
    con: Any,
    groups: list[dict[str, Any]],
    *,
    job_type: str,
    scheduled_time_key: str = "scheduled_time_et",
    scheduled_time_utc_key: str = "scheduled_time_utc",
    status: str = "pending",
) -> int:
    """
    Insert one pipeline_jobs row per game group (idempotent), ET-first.

    - `scheduled_time_et` is taken from group[scheduled_time_key] (expected 'YYYY-MM-DD HH:MM ET')
    - UTC is taken from group[scheduled_time_utc_key] when available (ISO string).
    - `job_date_et` is derived from scheduled_time_et (YYYY-MM-DD)
    - `game_group_id` is taken from group['group_id'].
    - Returns number of rows inserted when rowcount is available; otherwise 0.
    """
    if not groups:
        return 0

    ensure_pipeline_jobs_table(con)

    inserted = 0
    try:
        cols = {r[1] for r in con.execute("PRAGMA table_info(pipeline_jobs)").fetchall()}
    except Exception:
        cols = set()

    for g in groups:
        gid = g.get("group_id")
        sched_et = g.get(scheduled_time_key)
        sched_utc = g.get(scheduled_time_utc_key)
        if gid is None or not sched_et:
            continue
        try:
            # Back-compat: older table versions may still have scheduled_time (UTC) as NOT NULL.
            if "scheduled_time" in cols:
                cur = con.execute(
                    """
                    INSERT OR IGNORE INTO pipeline_jobs
                        (job_type, scheduled_time, job_date_et, scheduled_time_et, scheduled_time_utc,
                         window_start_et, window_end_et, status, game_group_id)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        str(job_type),
                        str(sched_utc or ""),
                        str(sched_et)[:10],
                        str(sched_et),
                        (str(sched_utc) if sched_utc else None),
                        g.get("window_start_et"),
                        g.get("window_end_et"),
                        str(status),
                        int(gid),
                    ),
                )
            else:
                cur = con.execute(
                    """
                    INSERT OR IGNORE INTO pipeline_jobs
                        (job_type, job_date_et, scheduled_time_et, scheduled_time_utc,
                         window_start_et, window_end_et, status, game_group_id)
                    VALUES (?,?,?,?,?,?,?,?)
                    """,
                    (
                        str(job_type),
                        str(sched_et)[:10],
                        str(sched_et),
                        (str(sched_utc) if sched_utc else None),
                        g.get("window_start_et"),
                        g.get("window_end_et"),
                        str(status),
                        int(gid),
                    ),
                )
            if getattr(cur, "rowcount", 0) == 1:
                inserted += 1
        except Exception:
            continue

    try:
        con.commit()
    except Exception:
        pass
    return inserted


def group_games_and_schedule_jobs(
    con: Any,
    games: list[dict[str, Any]],
    *,
    job_type: str,
    window_minutes: int = 30,
    scheduled_time_key: str = "start_time",
    status: str = "pending",
) -> list[dict[str, Any]]:
    """
    Default "program" behavior: group games, then persist one job per group.

    - Deterministic grouping via `group_games_by_start_time`
    - Idempotent DB insert via `schedule_pipeline_jobs_for_game_groups`

    Returns the computed groups (regardless of whether rows already existed).
    """
    groups = group_games_by_start_time(games, window_minutes=window_minutes)
    schedule_pipeline_jobs_for_game_groups(
        con,
        groups,
        job_type=job_type,
        scheduled_time_key=scheduled_time_key,
        status=status,
    )
    return groups

