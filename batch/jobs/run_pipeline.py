#!/usr/bin/env python3
"""
run_pipeline.py
───────────────
Execution engine for scheduled pipeline jobs.

Polls pipeline_jobs and runs due jobs (single-threaded) in scheduled_time order.

CHANGE LOG (latest first)
────────────────────────
2026-04-16  schedule_next_day_globals → schedule_pipeline_day.py --globals-only --date-et
            (next calendar day after job_date_et); day_setup → --groups-only --date-et.
            Helpers _next_calendar_date_et, _default_tomorrow_date_et.
2026-04-16  Align pipeline_jobs extras with schema: started_at/completed_at as DATETIME;
            matches core/db/schema.sql and ensure_pipeline_jobs_table().
2026-04-16  Read-only --status: print pending / running / failed jobs + last 10 runs.
2026-04-16  Duplicate execution guard: claim via UPDATE … WHERE status='pending'
            (rowcount check); skip if another worker already claimed the job.
2026-04-16  Running timeout: jobs left in running > N minutes → status timeout + run row
            (status=timeout); no OS kill. DB without timeout in CHECK falls back to failed.
2026-04-16  pipeline_job_runs is source of truth: after each execution insert full run row,
            then sync pipeline_jobs (status, started_at, completed_at, error_message).
2026-04-16  pipeline_job_runs: one row per execution; duration_seconds (REAL) set on finish
            from finished_at_utc - started_at_utc (existing run rows never backfilled).
2026-04-16  Failure retries: retry_count column (default 0); on failure re-queue pending
            up to 2 retries (retry_count < 2 before increment); next attempt next poll loop.
2026-04-16  Hardened state: commit running+started_at before exec; try/except around
            subprocess; terminal states set completed_at (or ended_at fallback);
            stale running jobs (>N min) reset to pending for retry with retries++.
2026-04-16  Failure safety: capture stderr on failed jobs; continue pipeline.
            Optional retries column (default 0) added best-effort; no retries yet.
2026-04-16  Add hardcoded dependency checks: skip pending jobs until required
            upstream job_type rows are complete (do not fail on unmet deps).
2026-04-16  Add --ghost mode to print due jobs/commands without executing or
            updating DB state. Useful to validate schedule completeness safely.
2026-04-16  Initial version: single-threaded pipeline runner for pipeline_jobs.
2026-04-16  Compatibility: pipeline_jobs is job_type-driven (no command column);
            runner derives commands from job_type and uses scheduled_time_et when present.

Rules:
- Only runs jobs with status='pending' and scheduled_time <= now
- Marks jobs: pending -> running -> complete/failed/timeout
- Does not modify job definitions
- Does not run jobs in parallel
- Command lines for day_setup and schedule_next_day_globals are built in _build_command();
  they must stay aligned with schedule_pipeline_day.py CLI flags.
"""

from __future__ import annotations

import argparse
import datetime as dt
import sqlite3
import subprocess
import sys
import time
import traceback
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import connect as db_connect, get_db_path

# Max automatic re-runs after a failed attempt (retry_count 0→1→2 then terminal fail).
_MAX_FAILURE_RETRIES = 2


def _utc_now_iso_z() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_started_at(s: str | None) -> dt.datetime | None:
    if not s:
        return None
    raw = str(s).strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        t = dt.datetime.fromisoformat(raw)
        if t.tzinfo is not None:
            t = t.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return t
    except Exception:
        return None

def _next_calendar_date_et(job_date_et: str) -> str:
    """
    Return YYYY-MM-DD one calendar day after job_date_et (slate date on the job row).

    Used when building schedule_next_day_globals → schedule_pipeline_day --globals-only
    so the evening job pre-seeds the *following* calendar day's group-0 globals.
    """
    d = dt.date.fromisoformat(str(job_date_et).strip())
    return (d + dt.timedelta(days=1)).isoformat()


def _default_tomorrow_date_et() -> str:
    """
    Next calendar day in America/New_York (fallback: local today + 1).

    Used only if job_date_et is missing when building schedule_next_day_globals command.
    """
    try:
        from zoneinfo import ZoneInfo

        et = ZoneInfo("America/New_York")
        return (dt.datetime.now(tz=et).date() + dt.timedelta(days=1)).isoformat()
    except Exception:
        return (dt.date.today() + dt.timedelta(days=1)).isoformat()


def _et_now_str() -> str:
    """
    Return a lexicographically sortable ET timestamp string matching pipeline_jobs.scheduled_time_et:
    'YYYY-MM-DD HH:MM ET'
    """
    try:
        from zoneinfo import ZoneInfo

        et = ZoneInfo("America/New_York")
        now_et = dt.datetime.now(tz=et)
    except Exception:
        # Fallback: assume ET; good enough for local usage.
        now_et = dt.datetime.now()
    return now_et.replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M ET")


def _table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({table})").fetchall()
        return {str(r[1]) for r in rows}  # (cid, name, type, notnull, dflt, pk)
    except Exception:
        return set()

def _build_command(job: dict) -> str:
    """
    Translate a pipeline_jobs row into an executable command string.
    pipeline_jobs definitions are job_type-driven (no 'command' column).

    Scheduling jobs (see batch/jobs/schedule_pipeline_day.py):
    - day_setup: morning pass — only per-group jobs for this slate date (--groups-only).
    - schedule_next_day_globals: evening pass — only next day's group-0 globals
      (--globals-only --date-et job_date_et + 1 calendar day).
    """
    job_type = str(job.get("job_type") or "").strip()
    job_date = str(job.get("job_date_et") or "").strip()
    group_id = job.get("game_group_id")
    win_start = str(job.get("window_start_et") or "").strip()
    win_end = str(job.get("window_end_et") or "").strip()

    # Note: commands intentionally simple; no parallelism.
    # If a job requires additional parameters later, extend this mapping only.
    # day_setup / schedule_next_day_globals: must match schedule_pipeline_day.py modes.
    mapping: dict[str, str] = {
        "stats_pull": "python batch/ingestion/load_mlb_stats.py",
        "load_today": f"python batch/ingestion/load_today.py --date {job_date}" if job_date else "python batch/ingestion/load_today.py",
        "day_setup": (
            f"python batch/jobs/schedule_pipeline_day.py --groups-only --date-et {job_date}"
            if job_date
            else "python batch/jobs/schedule_pipeline_day.py --groups-only"
        ),
        "prior_report": f"python batch/pipeline/generate_daily_brief.py --session prior --date {job_date}" if job_date else "python batch/pipeline/generate_daily_brief.py --session prior",
        "early_peek": f"python batch/pipeline/generate_daily_brief.py --session morning --date {job_date}" if job_date else "python batch/pipeline/generate_daily_brief.py --session morning",
        # Group jobs (windows are informational; scripts should filter by unplayed games / session rules)
        "odds_pull": "python batch/ingestion/load_odds.py --markets game",
        "odds_check": "python diagnostics/check_odds_ready.py",
        "weather": "python batch/ingestion/load_weather.py",
        # Group brief generation (time-windowing is embedded in generate_daily_brief session logic)
        "group_brief": f"python batch/pipeline/generate_daily_brief.py --session primary --date {job_date}" if job_date else "python batch/pipeline/generate_daily_brief.py --session primary",
        "ledger_snapshot": "python batch/pipeline/daily_results_report.py",
        "schedule_next_day_globals": (
            f"python batch/jobs/schedule_pipeline_day.py --globals-only --date-et {_next_calendar_date_et(job_date)}"
            if job_date
            else f"python batch/jobs/schedule_pipeline_day.py --globals-only --date-et {_default_tomorrow_date_et()}"
        ),
    }

    cmd = mapping.get(job_type, "")
    if cmd and group_id not in (None, "", 0):
        # Emit context only; do not assume scripts accept group_id flags.
        cmd = f"{cmd}  # group_id={group_id} window=[{win_start or '?'} -> {win_end or '?'}]"
    return cmd


def _dependency_rules() -> dict[str, list[str]]:
    """
    Hardcoded dependencies (simple + readable, no new tables).
    If a job_type has dependencies, it should not run until those job_types are complete.
    """
    return {
        # load_today must complete before any game-based jobs
        "odds_pull": ["load_today"],
        "odds_check": ["load_today", "odds_pull"],
        "weather": ["load_today"],
        # odds pulls must complete before brief generation
        "group_brief": ["load_today", "odds_pull"],
        "ledger_snapshot": ["load_today", "odds_pull"],
    }


def _deps_complete(con: sqlite3.Connection, job: dict) -> tuple[bool, str]:
    """
    Return (ok, message). If dependencies are not met, ok=False and message explains why.
    On unmet deps the caller should skip the job (leave pending), not fail it.
    """
    job_type = str(job.get("job_type") or "").strip()
    job_date = str(job.get("job_date_et") or "").strip()
    deps = _dependency_rules().get(job_type, [])
    if not deps:
        return True, ""

    date_clause = ""
    params: list[object] = []
    if job_date:
        date_clause = " AND job_date_et = ?"
        params.append(job_date)

    missing: list[str] = []
    for dep in deps:
        cur = con.execute(
            f"""
            SELECT COUNT(*) AS n
            FROM pipeline_jobs
            WHERE job_type = ?
              AND status = 'complete'
              {date_clause}
            """,
            (dep, *params),
        )
        n = int(cur.fetchone()[0] or 0)
        if n <= 0:
            missing.append(dep)

    if missing:
        scope = f"job_date_et={job_date}" if job_date else "all dates"
        return False, f"deps not complete ({scope}): {', '.join(missing)}"
    return True, ""


def _duration_seconds_utc(started_at_utc: str, finished_at_utc: str) -> float | None:
    """Wall-clock seconds between two UTC ISO timestamps (e.g. ...Z)."""
    a = _parse_started_at(started_at_utc)
    b = _parse_started_at(finished_at_utc)
    if a is None or b is None:
        return None
    return float((b - a).total_seconds())


def _ensure_pipeline_job_runs(con: sqlite3.Connection) -> set[str]:
    """
    Ensure pipeline_job_runs exists (matches core/db/schema.sql).
    Adds duration_seconds via ALTER if an older table lacked it.
    """
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_job_runs (
                run_id              INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id              INTEGER NOT NULL,
                job_type            TEXT,
                job_date_et         TEXT,
                started_at_utc      TEXT    NOT NULL,
                finished_at_utc     TEXT,
                duration_seconds    REAL,
                status              TEXT,
                error_message       TEXT
            )
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pipeline_job_runs_job_id
                ON pipeline_job_runs (job_id)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pipeline_job_runs_started
                ON pipeline_job_runs (started_at_utc)
            """
        )
        con.commit()
    except Exception:
        try:
            con.rollback()
        except Exception:
            pass
        return set()

    cols = _table_columns(con, "pipeline_job_runs")
    if "duration_seconds" not in cols:
        try:
            con.execute("ALTER TABLE pipeline_job_runs ADD COLUMN duration_seconds REAL")
            con.commit()
        except Exception:
            pass
        cols = _table_columns(con, "pipeline_job_runs")
    return cols


def _insert_pipeline_job_run_full(
    con: sqlite3.Connection,
    run_cols: set[str],
    *,
    job_id: int,
    job_type: str,
    job_date_et: str,
    started_at_utc: str,
    finished_at_utc: str,
    run_status: str,
    error_message: str | None,
) -> int | None:
    """
    Insert one complete pipeline_job_runs row (source of truth for this execution).
    """
    if not run_cols or "started_at_utc" not in run_cols:
        return None
    dur: float | None = None
    if "duration_seconds" in run_cols:
        dur = _duration_seconds_utc(started_at_utc, finished_at_utc)
    try:
        cur = con.execute(
            """
            INSERT INTO pipeline_job_runs (
                job_id, job_type, job_date_et, started_at_utc, finished_at_utc, duration_seconds, status, error_message
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(job_id),
                job_type or None,
                job_date_et or None,
                started_at_utc,
                finished_at_utc,
                dur,
                run_status,
                error_message if error_message is not None else "",
            ),
        )
        con.commit()
        return int(cur.lastrowid) if cur.lastrowid else None
    except Exception:
        try:
            con.rollback()
        except Exception:
            pass
        return None


def _sync_pipeline_jobs_from_run(
    con: sqlite3.Connection,
    cols: set[str],
    *,
    job_id: int,
    job_status: str,
    started_at_utc: str | None,
    finished_at_utc: str | None,
    job_error_message: str | None,
    retry_count_value: int | None = None,
) -> None:
    """
    Mirror the latest execution into pipeline_jobs (runs table already inserted).
    started_at / completed_at on jobs match run timestamps when provided; cleared when None.
    """
    parts: list[str] = ["status = ?"]
    params: list[object] = [job_status]

    if "started_at" in cols:
        if started_at_utc is not None:
            parts.append("started_at = ?")
            params.append(started_at_utc)
        else:
            parts.append("started_at = NULL")
    elif "start_time" in cols:
        if started_at_utc is not None:
            parts.append("start_time = ?")
            params.append(started_at_utc)
        else:
            parts.append("start_time = NULL")

    if finished_at_utc is not None:
        if "completed_at" in cols:
            parts.append("completed_at = ?")
            params.append(finished_at_utc)
        elif "ended_at" in cols:
            parts.append("ended_at = ?")
            params.append(finished_at_utc)
        elif "end_time" in cols:
            parts.append("end_time = ?")
            params.append(finished_at_utc)
    else:
        if "completed_at" in cols:
            parts.append("completed_at = NULL")
        elif "ended_at" in cols:
            parts.append("ended_at = NULL")
        elif "end_time" in cols:
            parts.append("end_time = NULL")

    if "error_message" in cols:
        parts.append("error_message = ?")
        params.append(job_error_message if job_error_message is not None else "")

    if retry_count_value is not None and "retry_count" in cols:
        parts.append("retry_count = ?")
        params.append(int(retry_count_value))

    params.append(int(job_id))
    con.execute(
        f"UPDATE pipeline_jobs SET {', '.join(parts)} WHERE job_id = ?",
        params,
    )
    con.commit()


def _set_completion_timestamp(fields: list[str], params: list[object], cols: set[str], ts: str) -> None:
    """Set completed_at if present, else ended_at / end_time for backward compatibility."""
    if "completed_at" in cols:
        fields.append("completed_at = ?")
        params.append(ts)
    elif "ended_at" in cols:
        fields.append("ended_at = ?")
        params.append(ts)
    elif "end_time" in cols:
        fields.append("end_time = ?")
        params.append(ts)


def _update_job_status(
    con: sqlite3.Connection,
    *,
    job_id: int,
    status: str,
    error_message: str | None = None,
    started_at: str | None = None,
    ended_at: str | None = None,
    completed_at: str | None = None,
    retries: int | None = None,
    retry_count: int | None = None,
    cols: set[str],
) -> None:
    fields: list[str] = ["status = ?"]
    params: list[object] = [status]

    if error_message is not None and "error_message" in cols:
        fields.append("error_message = ?")
        params.append(error_message)

    if retries is not None and "retries" in cols:
        fields.append("retries = ?")
        params.append(int(retries))

    if retry_count is not None and "retry_count" in cols:
        fields.append("retry_count = ?")
        params.append(int(retry_count))

    if started_at is not None:
        if "started_at" in cols:
            fields.append("started_at = ?")
            params.append(started_at)
        elif "start_time" in cols:
            fields.append("start_time = ?")
            params.append(started_at)

    # Terminal completion time: prefer explicit completed_at kwarg, else ended_at for compat.
    ts = completed_at if completed_at is not None else ended_at
    if ts is not None:
        _set_completion_timestamp(fields, params, cols, ts)

    sql = f"UPDATE pipeline_jobs SET {', '.join(fields)} WHERE job_id = ?"
    params.append(int(job_id))
    con.execute(sql, params)
    con.commit()


def _claim_pending_job(
    con: sqlite3.Connection,
    cols: set[str],
    *,
    job_id: int,
    started_at: str,
) -> bool:
    """
    Atomically transition pending -> running. Returns True only if exactly one row
    was updated (still pending). Other workers racing on the same job get rowcount 0.
    """
    parts: list[str] = ["status = ?"]
    params: list[object] = ["running"]

    if "started_at" in cols:
        parts.append("started_at = ?")
        params.append(started_at)
    elif "start_time" in cols:
        parts.append("start_time = ?")
        params.append(started_at)

    if "completed_at" in cols:
        parts.append("completed_at = NULL")
    elif "ended_at" in cols:
        parts.append("ended_at = NULL")
    elif "end_time" in cols:
        parts.append("end_time = NULL")

    params.append(int(job_id))
    cur = con.execute(
        f"UPDATE pipeline_jobs SET {', '.join(parts)} WHERE job_id = ? AND status = 'pending'",
        params,
    )
    con.commit()
    return int(getattr(cur, "rowcount", 0) or 0) == 1


def _ensure_pipeline_jobs_extras(con: sqlite3.Connection, cols: set[str]) -> set[str]:
    """Best-effort columns for retries / completion tracking."""
    if "retries" not in cols:
        try:
            con.execute("ALTER TABLE pipeline_jobs ADD COLUMN retries INTEGER NOT NULL DEFAULT 0")
            con.commit()
        except Exception:
            pass
        cols = _table_columns(con, "pipeline_jobs")
    if "completed_at" not in cols:
        try:
            con.execute("ALTER TABLE pipeline_jobs ADD COLUMN completed_at DATETIME")
            con.commit()
        except Exception:
            pass
        cols = _table_columns(con, "pipeline_jobs")
    if "error_message" not in cols:
        try:
            con.execute("ALTER TABLE pipeline_jobs ADD COLUMN error_message TEXT")
            con.commit()
        except Exception:
            pass
        cols = _table_columns(con, "pipeline_jobs")
    if "started_at" not in cols:
        try:
            con.execute("ALTER TABLE pipeline_jobs ADD COLUMN started_at DATETIME")
            con.commit()
        except Exception:
            pass
        cols = _table_columns(con, "pipeline_jobs")
    if "retry_count" not in cols:
        try:
            con.execute("ALTER TABLE pipeline_jobs ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0")
            con.commit()
        except Exception:
            pass
        cols = _table_columns(con, "pipeline_jobs")
    return cols


def _handle_job_failure(
    con: sqlite3.Connection,
    cols: set[str],
    *,
    job_id: int,
    job_type: str,
    job_date_et: str,
    retry_count_before: int,
    error_message: str,
    completed_ts: str,
    run_cols: set[str],
    started_iso: str,
) -> None:
    """
    On failure: record run (source of truth), then sync pipeline_jobs.
    If retry_count_before < _MAX_FAILURE_RETRIES, re-queue job as pending (next poll loop).
    """
    _insert_pipeline_job_run_full(
        con,
        run_cols,
        job_id=job_id,
        job_type=job_type,
        job_date_et=job_date_et,
        started_at_utc=started_iso,
        finished_at_utc=completed_ts,
        run_status="failed",
        error_message=error_message,
    )

    rc_col = "retry_count" in cols
    next_count = int(retry_count_before) + 1

    if int(retry_count_before) < _MAX_FAILURE_RETRIES and rc_col:
        _sync_pipeline_jobs_from_run(
            con,
            cols,
            job_id=job_id,
            job_status="pending",
            started_at_utc=None,
            finished_at_utc=None,
            job_error_message=error_message,
            retry_count_value=next_count,
        )
        print(
            f"[job] RETRY — job_id={job_id} retry_count={next_count}/{_MAX_FAILURE_RETRIES} "
            f"(will run after next poll loop)"
        )
        return

    _sync_pipeline_jobs_from_run(
        con,
        cols,
        job_id=job_id,
        job_status="failed",
        started_at_utc=started_iso,
        finished_at_utc=completed_ts,
        job_error_message=error_message,
        retry_count_value=next_count if rc_col else None,
    )


def _reset_stale_running_jobs(
    con: sqlite3.Connection,
    cols: set[str],
    *,
    stale_minutes: int,
) -> int:
    """
    Jobs stuck in 'running' longer than stale_minutes are reset to 'pending'
    so they can be retried. Increments retries when column exists.
    """
    if stale_minutes <= 0:
        return 0
    now_utc = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    cutoff = now_utc - dt.timedelta(minutes=int(stale_minutes))

    start_col = "started_at" if "started_at" in cols else None
    if not start_col and "start_time" in cols:
        start_col = "start_time"
    if not start_col:
        return 0

    try:
        cur = con.execute(
            f"""
            SELECT job_id, {start_col}
            FROM pipeline_jobs
            WHERE status = 'running'
            """
        )
        rows = cur.fetchall()
    except Exception:
        return 0

    reset_n = 0
    for row in rows:
        jid = int(row[0])
        st_raw = row[1]
        t = _parse_started_at(st_raw)
        if t is None or t >= cutoff:
            continue

        note = f"stale-running reset after {stale_minutes}m (was running since {st_raw!r})"
        try:
            set_parts = ["status = 'pending'"]
            params: list[object] = []
            if "error_message" in cols:
                set_parts.append("error_message = ?")
                params.append(note)
            if "started_at" in cols:
                set_parts.append("started_at = NULL")
            if "completed_at" in cols:
                set_parts.append("completed_at = NULL")
            elif "ended_at" in cols:
                set_parts.append("ended_at = NULL")
            elif "end_time" in cols:
                set_parts.append("end_time = NULL")

            if "retries" in cols:
                set_parts.append("retries = COALESCE(retries, 0) + 1")

            params.append(jid)
            con.execute(
                f"UPDATE pipeline_jobs SET {', '.join(set_parts)} WHERE job_id = ?",
                params,
            )
            con.commit()
            reset_n += 1
            print(f"[run_pipeline] STALE running job_id={jid} reset to pending — {note}")
        except Exception:
            try:
                con.rollback()
            except Exception:
                pass

    return reset_n


def _mark_running_timed_out(
    con: sqlite3.Connection,
    cols: set[str],
    run_cols: set[str],
    *,
    timeout_minutes: int,
) -> int:
    """
    Jobs stuck in running longer than timeout_minutes: insert pipeline_job_runs (timeout)
    and set pipeline_jobs status to timeout. Does not kill subprocesses.
    If the DB CHECK disallows status=timeout, sync as failed with an explanatory message.
    """
    if timeout_minutes <= 0:
        return 0
    now_utc = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    cutoff = now_utc - dt.timedelta(minutes=int(timeout_minutes))

    start_col = "started_at" if "started_at" in cols else None
    if not start_col and "start_time" in cols:
        start_col = "start_time"
    if not start_col:
        return 0

    try:
        cur = con.execute(
            f"""
            SELECT job_id, job_type, job_date_et, {start_col}
            FROM pipeline_jobs
            WHERE status = 'running'
            """
        )
        rows = cur.fetchall()
    except Exception:
        return 0

    marked = 0
    msg_base = (
        f"timeout: running longer than {timeout_minutes} minutes "
        f"(runner-side; process not killed)"
    )

    for row in rows:
        jid = int(row[0])
        jt = str(row[1] or "")
        jde = str(row[2] or "")
        st_raw = row[3]
        t = _parse_started_at(str(st_raw) if st_raw is not None else "")
        if t is None or t >= cutoff:
            continue

        finished_iso = _utc_now_iso_z()
        started_str = str(st_raw) if st_raw is not None else ""

        _insert_pipeline_job_run_full(
            con,
            run_cols,
            job_id=jid,
            job_type=jt,
            job_date_et=jde,
            started_at_utc=started_str,
            finished_at_utc=finished_iso,
            run_status="timeout",
            error_message=msg_base,
        )

        try:
            _sync_pipeline_jobs_from_run(
                con,
                cols,
                job_id=jid,
                job_status="timeout",
                started_at_utc=started_str,
                finished_at_utc=finished_iso,
                job_error_message=msg_base,
                retry_count_value=None,
            )
        except (sqlite3.OperationalError, sqlite3.IntegrityError):
            try:
                con.rollback()
            except Exception:
                pass
            _sync_pipeline_jobs_from_run(
                con,
                cols,
                job_id=jid,
                job_status="failed",
                started_at_utc=started_str,
                finished_at_utc=finished_iso,
                job_error_message=(
                    f"{msg_base} (stored as failed: pipeline_jobs CHECK has no 'timeout')"
                ),
                retry_count_value=None,
            )

        marked += 1
        print(f"[run_pipeline] TIMEOUT job_id={jid} after >{timeout_minutes}m in running")

    return marked


def _fetch_due_jobs(con: sqlite3.Connection, now_iso_z: str, cols: set[str]) -> list[dict]:
    if not cols:
        return []

    retry_sel = ", retry_count" if "retry_count" in cols else ""

    # Prefer ET-based schedule when present (most current schema).
    if "scheduled_time_et" in cols:
        now_et = _et_now_str()
        cur = con.execute(
            f"""
            SELECT job_id, job_type, job_date_et, scheduled_time_et, window_start_et, window_end_et, status, game_group_id
                   {retry_sel}
            FROM pipeline_jobs
            WHERE status = 'pending'
              AND scheduled_time_et <= ?
            ORDER BY scheduled_time_et, job_id
            """,
            (now_et,),
        )
        return [dict(r) for r in cur.fetchall()]

    # Legacy fallback: UTC scheduled_time column.
    if "scheduled_time" in cols:
        cur = con.execute(
            f"""
            SELECT job_id, job_type, job_date_et, scheduled_time, status, game_group_id
                   {retry_sel}
            FROM pipeline_jobs
            WHERE status = 'pending'
              AND scheduled_time <= ?
            ORDER BY scheduled_time, job_id
            """,
            (now_iso_z,),
        )
        return [dict(r) for r in cur.fetchall()]

    return []


def _run_command(command: str) -> tuple[int, str, str]:
    p = subprocess.run(
        command,
        shell=True,
        text=True,
        capture_output=True,
    )
    return int(p.returncode), (p.stdout or ""), (p.stderr or "")


def run_loop(
    *,
    db_path: str,
    once: bool,
    poll_seconds: int,
    ghost: bool,
    stale_minutes: int,
    timeout_minutes: int,
) -> None:
    con = db_connect(db_path, timeout=30)
    con.row_factory = sqlite3.Row

    cols = _table_columns(con, "pipeline_jobs")
    if not cols:
        raise RuntimeError("pipeline_jobs table not found or unreadable")

    cols = _ensure_pipeline_jobs_extras(con, cols)
    run_cols = _ensure_pipeline_job_runs(con)

    print(f"[run_pipeline] db={db_path}")
    print(
        f"[run_pipeline] mode={'once' if once else 'loop'} poll_seconds={poll_seconds} "
        f"ghost={ghost} stale_minutes={stale_minutes} timeout_minutes={timeout_minutes}"
    )

    while True:
        _mark_running_timed_out(con, cols, run_cols, timeout_minutes=timeout_minutes)
        _reset_stale_running_jobs(con, cols, stale_minutes=stale_minutes)

        now_iso = _utc_now_iso_z()
        due = _fetch_due_jobs(con, now_iso, cols)

        if not due:
            if once:
                print(f"[run_pipeline] {now_iso} no due jobs; exiting (--once).")
                break
            time.sleep(max(1, int(poll_seconds)))
            continue

        for job in due:
            job_id = int(job["job_id"])
            job_type = str(job.get("job_type") or "")
            scheduled_time = str(job.get("scheduled_time_et") or job.get("scheduled_time") or "")
            retry_count_before = int(job.get("retry_count") or 0)
            command = _build_command(job).strip()

            start_iso = _utc_now_iso_z()
            print(f"\n[job] id={job_id} type={job_type} scheduled_time={scheduled_time}")
            print(f"[job] start={start_iso}")
            print(f"[job] command={command!r}")

            ok, dep_msg = _deps_complete(con, job)
            if not ok:
                print(f"[job] SKIP — {dep_msg}")
                continue

            if ghost:
                print("[job] GHOST MODE — would set status=running, execute command, then set complete/failed")
                continue

            # Claim job: single UPDATE … AND status='pending' (SQLite-atomic; avoids duplicate execution).
            if not _claim_pending_job(con, cols, job_id=job_id, started_at=start_iso):
                print(f"[job] SKIP — job_id={job_id} not pending (already claimed or state changed)")
                continue

            job_date_et = str(job.get("job_date_et") or "")

            if not command:
                end_iso = _utc_now_iso_z()
                msg = f"no command mapping for job_type={job_type!r}"
                print(f"[job] end={end_iso} failure (no command) — {msg}")
                _handle_job_failure(
                    con,
                    cols,
                    job_id=job_id,
                    job_type=job_type,
                    job_date_et=job_date_et,
                    retry_count_before=retry_count_before,
                    error_message=msg,
                    completed_ts=end_iso,
                    run_cols=run_cols,
                    started_iso=start_iso,
                )
                continue

            try:
                rc, out, err = _run_command(command)
            except Exception as exc:
                end_iso = _utc_now_iso_z()
                tb = traceback.format_exc()
                msg = f"exception: {exc!s}\n{tb}"
                if len(msg) > 8000:
                    msg = msg[-8000:]
                print(f"[job] end={end_iso} failure exception={exc!r}")
                _handle_job_failure(
                    con,
                    cols,
                    job_id=job_id,
                    job_type=job_type,
                    job_date_et=job_date_et,
                    retry_count_before=retry_count_before,
                    error_message=msg,
                    completed_ts=end_iso,
                    run_cols=run_cols,
                    started_iso=start_iso,
                )
                continue

            end_iso = _utc_now_iso_z()

            if rc == 0:
                print(f"[job] end={end_iso} status=complete rc=0")
                _insert_pipeline_job_run_full(
                    con,
                    run_cols,
                    job_id=job_id,
                    job_type=job_type,
                    job_date_et=job_date_et,
                    started_at_utc=start_iso,
                    finished_at_utc=end_iso,
                    run_status="complete",
                    error_message="",
                )
                _sync_pipeline_jobs_from_run(
                    con,
                    cols,
                    job_id=job_id,
                    job_status="complete",
                    started_at_utc=start_iso,
                    finished_at_utc=end_iso,
                    job_error_message="",
                    retry_count_value=0 if "retry_count" in cols else None,
                )
            else:
                # Capture stderr (preferred) for error_message; fall back to stdout.
                raw_err = (err or "").strip()
                raw_out = (out or "").strip()
                tail = raw_err if raw_err else raw_out
                if len(tail) > 4000:
                    tail = tail[-4000:]
                msg = f"rc={rc} {tail}".strip() if tail else f"rc={rc}"
                print(f"[job] end={end_iso} failure rc={rc}")
                if tail:
                    print(f"[job] error_tail={tail}")
                _handle_job_failure(
                    con,
                    cols,
                    job_id=job_id,
                    job_type=job_type,
                    job_date_et=job_date_et,
                    retry_count_before=retry_count_before,
                    error_message=msg,
                    completed_ts=end_iso,
                    run_cols=run_cols,
                    started_iso=start_iso,
                )
                # Do NOT stop the pipeline on failure — continue to next job.

        if once:
            print(f"\n[run_pipeline] {now_iso} processed due jobs; exiting (--once).")
            break

    con.close()


def _fmt_row(widths: list[int], cells: list[str]) -> str:
    parts = []
    for w, c in zip(widths, cells):
        parts.append((c or "")[: w].ljust(w))
    return "  ".join(parts)


def _rule_line_for_widths(widths: list[int]) -> str:
    total = sum(widths) + 2 * (len(widths) - 1)
    return "  " + ("-" * total)


def print_pipeline_status(db_path: str) -> None:
    """
    Read-only snapshot: pending / running / failed jobs and last 10 pipeline_job_runs.
    """
    p = Path(db_path)
    if not p.is_file():
        print(f"Error: database file not found:\n  {p.resolve()}", file=sys.stderr)
        print(
            "  Omit --db to use the default from config/env, or pass the full path to mlb_stats.db.",
            file=sys.stderr,
        )
        sys.exit(1)
    try:
        con = db_connect(db_path, timeout=30)
    except sqlite3.OperationalError as exc:
        print(f"Error: could not open database:\n  {db_path}\n  {exc}", file=sys.stderr)
        sys.exit(1)
    con.row_factory = sqlite3.Row

    pj = _table_columns(con, "pipeline_jobs")
    pr = _table_columns(con, "pipeline_job_runs")

    print()
    print("═" * 76)
    print("  PIPELINE STATUS  (read-only)")
    print("═" * 76)
    print(f"  Database: {db_path}")
    print()

    if not pj:
        print("  pipeline_jobs: (table missing)")
        con.close()
        return

    sched = "scheduled_time_et" if "scheduled_time_et" in pj else "scheduled_time" if "scheduled_time" in pj else None
    sched_sel = f", {sched}" if sched else ""

    def _print_job_block(title: str, where_sql: str, params: tuple = ()) -> None:
        try:
            cur = con.execute(
                f"""
                SELECT job_id, job_type, job_date_et, status, game_group_id
                       {sched_sel}
                FROM pipeline_jobs
                WHERE {where_sql}
                ORDER BY job_id
                """,
                params,
            )
            rows = [dict(r) for r in cur.fetchall()]
        except Exception as exc:
            print(f"  [{title}] query error: {exc}")
            print()
            return

        print(f"── {title}  ({len(rows)}) " + "─" * max(0, 60 - len(title)))
        if not rows:
            print("  (none)")
            print()
            return

        headers = ["job_id", "job_type", "job_date_et", "status", "group", "scheduled"]
        sc = sched or ""
        # job_type must fit longest names (e.g. schedule_next_day_globals)
        widths = [8, 30, 12, 10, 6, 22]
        print(_fmt_row(widths, headers))
        print(_rule_line_for_widths(widths))
        for r in rows:
            line = [
                str(r.get("job_id", "")),
                str(r.get("job_type", "")),
                str(r.get("job_date_et", "")),
                str(r.get("status", "")),
                str(r.get("game_group_id", "")),
                str(r.get(sc, "")) if sc else "",
            ]
            print(_fmt_row(widths, line))
        print()

    _print_job_block("Pending jobs", "status = 'pending'")
    _print_job_block("Running jobs", "status = 'running'")
    _print_job_block("Failed & timeout jobs", "status IN ('failed','timeout')")

    # Last 10 runs
    print("── Last 10 job runs " + "─" * 52)
    if not pr or "run_id" not in pr:
        print("  pipeline_job_runs: (table missing or empty schema)")
        print()
        con.close()
        return

    order_by = "run_id DESC"
    try:
        cur = con.execute(
            f"""
            SELECT run_id, job_id, job_type, status, started_at_utc, finished_at_utc,
                   duration_seconds, substr(COALESCE(error_message,''),1,48) AS err48
            FROM pipeline_job_runs
            ORDER BY {order_by}
            LIMIT 10
            """
        )
        rrows = cur.fetchall()
    except Exception as exc:
        print(f"  query error: {exc}")
        print()
        con.close()
        return

    if not rrows:
        print("  (none)")
        print()
        con.close()
        return

    hdr = ["run_id", "job_id", "type", "status", "started_utc", "finished_utc", "sec", "error (trunc)"]
    w = [7, 8, 30, 10, 20, 20, 6, 44]
    print(_fmt_row(w, hdr))
    print(_rule_line_for_widths(w))
    for r in rrows:
        d = dict(r)
        print(
            _fmt_row(
                w,
                [
                    str(d.get("run_id", "")),
                    str(d.get("job_id", "")),
                    str(d.get("job_type", "")),
                    str(d.get("status", "")),
                    str(d.get("started_at_utc", "")),
                    str(d.get("finished_at_utc", "")),
                    "" if d.get("duration_seconds") is None else f"{float(d['duration_seconds']):.1f}",
                    str(d.get("err48", "")),
                ],
            )
        )
    print()
    print("═" * 76)
    print()

    con.close()


def main() -> None:
    p = argparse.ArgumentParser(description="Run due pipeline_jobs in scheduled order (single-threaded).")
    p.add_argument(
        "--status",
        action="store_true",
        help="Print pending/running/failed jobs and last 10 runs, then exit (read-only)",
    )
    p.add_argument(
        "--db",
        default=None,
        help="Path to mlb_stats.db (defaults to core.db.connection.get_db_path()); must exist for --status",
    )
    p.add_argument("--once", action="store_true", help="Run one polling pass then exit")
    p.add_argument("--ghost", action="store_true", help="Print what would run; do not execute or update DB")
    p.add_argument("--poll-seconds", type=int, default=60, help="Polling interval when looping (default 60)")
    p.add_argument(
        "--stale-minutes",
        type=int,
        default=30,
        help="Reset status=running jobs older than this to pending (retry). 0 disables (default 30)",
    )
    p.add_argument(
        "--timeout-minutes",
        type=int,
        default=30,
        help="Mark status=running jobs older than this as timeout + pipeline_job_runs row. 0 disables (default 30)",
    )
    args = p.parse_args()

    db_path = str(Path(args.db).resolve()) if args.db else str(Path(get_db_path()).resolve())
    if args.status:
        print_pipeline_status(db_path)
        return

    run_loop(
        db_path=db_path,
        once=bool(args.once),
        poll_seconds=int(args.poll_seconds),
        ghost=bool(args.ghost),
        stale_minutes=max(0, int(args.stale_minutes)),
        timeout_minutes=max(0, int(args.timeout_minutes)),
    )


if __name__ == "__main__":
    main()

