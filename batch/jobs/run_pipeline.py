#!/usr/bin/env python3
"""
run_pipeline.py
───────────────
Execution engine for scheduled pipeline jobs.

Polls pipeline_jobs and runs due jobs (single-threaded) in scheduled_time order.

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
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import connect as db_connect, get_db_path


def _utc_now_iso_z() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({table})").fetchall()
        return {str(r[1]) for r in rows}  # (cid, name, type, notnull, dflt, pk)
    except Exception:
        return set()


def _update_job_status(
    con: sqlite3.Connection,
    *,
    job_id: int,
    status: str,
    error_message: str | None = None,
    started_at: str | None = None,
    ended_at: str | None = None,
    cols: set[str],
) -> None:
    fields: list[str] = ["status = ?"]
    params: list[object] = [status]

    if error_message is not None and "error_message" in cols:
        fields.append("error_message = ?")
        params.append(error_message)

    if started_at is not None:
        if "started_at" in cols:
            fields.append("started_at = ?")
            params.append(started_at)
        elif "start_time" in cols:
            fields.append("start_time = ?")
            params.append(started_at)

    if ended_at is not None:
        if "ended_at" in cols:
            fields.append("ended_at = ?")
            params.append(ended_at)
        elif "end_time" in cols:
            fields.append("end_time = ?")
            params.append(ended_at)

    sql = f"UPDATE pipeline_jobs SET {', '.join(fields)} WHERE job_id = ?"
    params.append(int(job_id))
    con.execute(sql, params)
    con.commit()


def _fetch_due_jobs(con: sqlite3.Connection, now_iso_z: str) -> list[dict]:
    cur = con.execute(
        """
        SELECT job_id, job_type, scheduled_time, command
        FROM pipeline_jobs
        WHERE status = 'pending'
          AND scheduled_time <= ?
        ORDER BY scheduled_time, job_id
        """,
        (now_iso_z,),
    )
    return [dict(r) for r in cur.fetchall()]


def _run_command(command: str) -> tuple[int, str, str]:
    p = subprocess.run(
        command,
        shell=True,
        text=True,
        capture_output=True,
    )
    return int(p.returncode), (p.stdout or ""), (p.stderr or "")


def run_loop(*, db_path: str, once: bool, poll_seconds: int, ghost: bool) -> None:
    con = db_connect(db_path, timeout=30)
    con.row_factory = sqlite3.Row

    cols = _table_columns(con, "pipeline_jobs")
    if not cols:
        raise RuntimeError("pipeline_jobs table not found or unreadable")

    print(f"[run_pipeline] db={db_path}")
    print(f"[run_pipeline] mode={'once' if once else 'loop'} poll_seconds={poll_seconds} ghost={ghost}")

    while True:
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
            command = str(job.get("command") or "").strip()
            scheduled_time = str(job.get("scheduled_time") or "")

            start_iso = _utc_now_iso_z()
            print(f"\n[job] id={job_id} type={job_type} scheduled_time={scheduled_time}")
            print(f"[job] start={start_iso}")
            print(f"[job] command={command!r}")

            if ghost:
                print("[job] GHOST MODE — would set status=running, execute command, then set complete/failed")
                continue

            _update_job_status(
                con,
                job_id=job_id,
                status="running",
                error_message=None,
                started_at=start_iso,
                ended_at=None,
                cols=cols,
            )

            if not command:
                end_iso = _utc_now_iso_z()
                msg = "empty command"
                print(f"[job] end={end_iso} status=failed error={msg}")
                _update_job_status(
                    con,
                    job_id=job_id,
                    status="failed",
                    error_message=msg,
                    started_at=None,
                    ended_at=end_iso,
                    cols=cols,
                )
                continue

            rc, out, err = _run_command(command)
            end_iso = _utc_now_iso_z()

            if rc == 0:
                print(f"[job] end={end_iso} status=complete rc=0")
                _update_job_status(
                    con,
                    job_id=job_id,
                    status="complete",
                    error_message="",
                    started_at=None,
                    ended_at=end_iso,
                    cols=cols,
                )
            else:
                tail = (err or out or "").strip()
                if len(tail) > 2000:
                    tail = tail[-2000:]
                msg = f"rc={rc} {tail}".strip()
                print(f"[job] end={end_iso} status=failed rc={rc}")
                if tail:
                    print(f"[job] error_tail={tail}")
                _update_job_status(
                    con,
                    job_id=job_id,
                    status="failed",
                    error_message=msg,
                    started_at=None,
                    ended_at=end_iso,
                    cols=cols,
                )

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
    args = p.parse_args()

    db_path = str(Path(args.db).resolve()) if args.db else str(Path(get_db_path()).resolve())
    run_loop(
        db_path=db_path,
        once=bool(args.once),
        poll_seconds=int(args.poll_seconds),
        ghost=bool(args.ghost),
    )


if __name__ == "__main__":
    main()

