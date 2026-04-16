#!/usr/bin/env python3
"""
run_pipeline.py
───────────────
Execution engine for scheduled pipeline jobs.

Polls pipeline_jobs and runs due jobs (single-threaded) in scheduled_time order.

CHANGE LOG (latest first)
────────────────────────
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
- Marks jobs: pending -> running -> complete/failed
- Does not modify job definitions
- Does not run jobs in parallel
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
    """
    job_type = str(job.get("job_type") or "").strip()
    job_date = str(job.get("job_date_et") or "").strip()
    group_id = job.get("game_group_id")
    win_start = str(job.get("window_start_et") or "").strip()
    win_end = str(job.get("window_end_et") or "").strip()

    # Note: commands intentionally simple; no parallelism.
    # If a job requires additional parameters later, extend this mapping only.
    mapping: dict[str, str] = {
        "stats_pull": "python batch/ingestion/load_mlb_stats.py",
        "load_today": f"python batch/ingestion/load_today.py --date {job_date}" if job_date else "python batch/ingestion/load_today.py",
        "day_setup": f"python batch/jobs/schedule_pipeline_day.py --date-et {job_date}" if job_date else "python batch/jobs/schedule_pipeline_day.py",
        "prior_report": f"python batch/pipeline/generate_daily_brief.py --session prior --date {job_date}" if job_date else "python batch/pipeline/generate_daily_brief.py --session prior",
        "early_peek": f"python batch/pipeline/generate_daily_brief.py --session morning --date {job_date}" if job_date else "python batch/pipeline/generate_daily_brief.py --session morning",
        # Group jobs (windows are informational; scripts should filter by unplayed games / session rules)
        "odds_pull": "python batch/ingestion/load_odds.py --markets game",
        "odds_check": "python diagnostics/check_odds_ready.py",
        "weather": "python batch/ingestion/load_weather.py",
        # Group brief generation (time-windowing is embedded in generate_daily_brief session logic)
        "group_brief": f"python batch/pipeline/generate_daily_brief.py --session primary --date {job_date}" if job_date else "python batch/pipeline/generate_daily_brief.py --session primary",
        "ledger_snapshot": "python batch/pipeline/daily_results_report.py",
        "schedule_next_day_globals": "python batch/jobs/schedule_pipeline_day.py",
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
            con.execute("ALTER TABLE pipeline_jobs ADD COLUMN completed_at TEXT")
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
            con.execute("ALTER TABLE pipeline_jobs ADD COLUMN started_at TEXT")
            con.commit()
        except Exception:
            pass
        cols = _table_columns(con, "pipeline_jobs")
    return cols


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


def _fetch_due_jobs(con: sqlite3.Connection, now_iso_z: str) -> list[dict]:
    cols = _table_columns(con, "pipeline_jobs")
    if not cols:
        return []

    # Prefer ET-based schedule when present (most current schema).
    if "scheduled_time_et" in cols:
        now_et = _et_now_str()
        cur = con.execute(
            """
            SELECT job_id, job_type, job_date_et, scheduled_time_et, window_start_et, window_end_et, status, game_group_id
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
            """
            SELECT job_id, job_type, job_date_et, scheduled_time, status, game_group_id
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
) -> None:
    con = db_connect(db_path, timeout=30)
    con.row_factory = sqlite3.Row

    cols = _table_columns(con, "pipeline_jobs")
    if not cols:
        raise RuntimeError("pipeline_jobs table not found or unreadable")

    cols = _ensure_pipeline_jobs_extras(con, cols)

    print(f"[run_pipeline] db={db_path}")
    print(
        f"[run_pipeline] mode={'once' if once else 'loop'} poll_seconds={poll_seconds} "
        f"ghost={ghost} stale_minutes={stale_minutes}"
    )

    while True:
        _reset_stale_running_jobs(con, cols, stale_minutes=stale_minutes)

        now_iso = _utc_now_iso_z()
        due = _fetch_due_jobs(con, now_iso)

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

            # Claim job: running + started_at, commit before any subprocess (survives crash mid-run).
            _update_job_status(
                con,
                job_id=job_id,
                status="running",
                error_message=None,
                started_at=start_iso,
                ended_at=None,
                completed_at=None,
                retries=None,
                cols=cols,
            )

            if not command:
                end_iso = _utc_now_iso_z()
                msg = f"no command mapping for job_type={job_type!r}"
                print(f"[job] end={end_iso} status=failed error={msg}")
                _update_job_status(
                    con,
                    job_id=job_id,
                    status="failed",
                    error_message=msg,
                    started_at=None,
                    ended_at=None,
                    completed_at=end_iso,
                    cols=cols,
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
                print(f"[job] end={end_iso} status=failed exception={exc!r}")
                _update_job_status(
                    con,
                    job_id=job_id,
                    status="failed",
                    error_message=msg,
                    started_at=None,
                    ended_at=None,
                    completed_at=end_iso,
                    retries=None,
                    cols=cols,
                )
                continue

            end_iso = _utc_now_iso_z()

            if rc == 0:
                print(f"[job] end={end_iso} status=complete rc=0")
                _update_job_status(
                    con,
                    job_id=job_id,
                    status="complete",
                    error_message="",
                    started_at=None,
                    ended_at=None,
                    completed_at=end_iso,
                    retries=None,
                    cols=cols,
                )
            else:
                # Capture stderr (preferred) for error_message; fall back to stdout.
                raw_err = (err or "").strip()
                raw_out = (out or "").strip()
                tail = raw_err if raw_err else raw_out
                if len(tail) > 4000:
                    tail = tail[-4000:]
                msg = f"rc={rc} {tail}".strip() if tail else f"rc={rc}"
                print(f"[job] end={end_iso} status=failed rc={rc}")
                if tail:
                    print(f"[job] error_tail={tail}")
                _update_job_status(
                    con,
                    job_id=job_id,
                    status="failed",
                    error_message=msg,
                    started_at=None,
                    ended_at=None,
                    completed_at=end_iso,
                    retries=None,
                    cols=cols,
                )
                # Do NOT stop the pipeline on failure — continue to next job.

        if once:
            print(f"\n[run_pipeline] {now_iso} processed due jobs; exiting (--once).")
            break

    con.close()


def main() -> None:
    p = argparse.ArgumentParser(description="Run due pipeline_jobs in scheduled order (single-threaded).")
    p.add_argument("--db", default=None, help="Path to mlb_stats.db (defaults to core.db.connection.get_db_path())")
    p.add_argument("--once", action="store_true", help="Run one polling pass then exit")
    p.add_argument("--ghost", action="store_true", help="Print what would run; do not execute or update DB")
    p.add_argument("--poll-seconds", type=int, default=20, help="Polling interval when looping (default 20)")
    p.add_argument(
        "--stale-minutes",
        type=int,
        default=30,
        help="Reset status=running jobs older than this to pending (retry). 0 disables (default 30)",
    )
    args = p.parse_args()

    db_path = str(Path(args.db).resolve()) if args.db else str(Path(get_db_path()).resolve())
    run_loop(
        db_path=db_path,
        once=bool(args.once),
        poll_seconds=int(args.poll_seconds),
        ghost=bool(args.ghost),
        stale_minutes=max(0, int(args.stale_minutes)),
    )


if __name__ == "__main__":
    main()

