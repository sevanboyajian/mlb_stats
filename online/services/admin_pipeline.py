"""Read-only pipeline + runner_lock queries for MLB Scout Admin (import-safe)."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd

from core.db.connection import connect as db_connect, get_db_path


def _table_columns(con: sqlite3.Connection, table: str) -> list[str]:
    try:
        cur = con.execute(f"PRAGMA table_info({table})")
        return [str(r[1]) for r in cur.fetchall()]
    except Exception:
        return []


def _sched_col(cols: list[str]) -> str | None:
    if "scheduled_time_et" in cols:
        return "scheduled_time_et"
    if "scheduled_time" in cols:
        return "scheduled_time"
    return None


def open_db(db_path: str | None = None) -> tuple[sqlite3.Connection, str]:
    raw = (db_path or "").strip()
    path = str(Path(raw).resolve()) if raw else str(Path(get_db_path()).resolve())
    con = db_connect(path, timeout=30)
    con.row_factory = sqlite3.Row
    return con, path


def fetch_runner_lock(con: sqlite3.Connection) -> dict[str, Any] | None:
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS runner_lock (
                lock_id INTEGER PRIMARY KEY CHECK (lock_id = 1),
                acquired_at_utc TEXT NOT NULL,
                pid INTEGER,
                host TEXT
            )
            """
        )
        con.commit()
    except Exception:
        return None
    try:
        r = con.execute(
            "SELECT lock_id, acquired_at_utc, pid, host FROM runner_lock WHERE lock_id = 1"
        ).fetchone()
        return dict(r) if r else None
    except Exception:
        return None


def clear_runner_lock(con: sqlite3.Connection) -> bool:
    try:
        con.execute("DELETE FROM runner_lock WHERE lock_id = 1")
        con.commit()
        return True
    except Exception:
        try:
            con.rollback()
        except Exception:
            pass
        return False


def fetch_pipeline_jobs(
    con: sqlite3.Connection,
    *,
    status: str,
    job_date_et: str | None = None,
) -> pd.DataFrame:
    cols = _table_columns(con, "pipeline_jobs")
    if not cols:
        return pd.DataFrame()
    sched = _sched_col(cols)
    sel_sched = f", {sched}" if sched else ""
    if job_date_et:
        sql = f"""
            SELECT job_id, job_type, job_date_et, status, game_group_id{sel_sched}
            FROM pipeline_jobs
            WHERE status = ? AND job_date_et = ?
            ORDER BY job_id
        """
        params: tuple = (status, job_date_et)
    else:
        sql = f"""
            SELECT job_id, job_type, job_date_et, status, game_group_id{sel_sched}
            FROM pipeline_jobs
            WHERE status = ?
            ORDER BY job_id
        """
        params = (status,)
    try:
        return pd.read_sql_query(sql, con, params=params)
    except Exception:
        return pd.DataFrame()


def fetch_pipeline_jobs_multi_status(
    con: sqlite3.Connection,
    statuses: tuple[str, ...],
    *,
    job_date_et: str | None = None,
) -> pd.DataFrame:
    cols = _table_columns(con, "pipeline_jobs")
    if not cols:
        return pd.DataFrame()
    sched = _sched_col(cols)
    sel_sched = f", {sched}" if sched else ""
    placeholders = ",".join("?" * len(statuses))
    if job_date_et:
        sql = f"""
            SELECT job_id, job_type, job_date_et, status, game_group_id{sel_sched}
            FROM pipeline_jobs
            WHERE status IN ({placeholders}) AND job_date_et = ?
            ORDER BY job_id
        """
        params = tuple(statuses) + (job_date_et,)
    else:
        sql = f"""
            SELECT job_id, job_type, job_date_et, status, game_group_id{sel_sched}
            FROM pipeline_jobs
            WHERE status IN ({placeholders})
            ORDER BY job_id
        """
        params = tuple(statuses)
    try:
        return pd.read_sql_query(sql, con, params=params)
    except Exception:
        return pd.DataFrame()


def count_pending(con: sqlite3.Connection, job_date_et: str | None) -> int:
    try:
        if job_date_et:
            cur = con.execute(
                "SELECT COUNT(*) AS n FROM pipeline_jobs WHERE status = 'pending' AND job_date_et = ?",
                (job_date_et,),
            )
        else:
            cur = con.execute("SELECT COUNT(*) AS n FROM pipeline_jobs WHERE status = 'pending'")
        r = cur.fetchone()
        return int(r[0]) if r else 0
    except Exception:
        return 0


def fetch_last_job_runs(con: sqlite3.Connection, limit: int = 15) -> pd.DataFrame:
    cols = _table_columns(con, "pipeline_job_runs")
    if not cols or "run_id" not in cols:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            f"""
            SELECT run_id, job_id, job_type, status, started_at_utc, finished_at_utc,
                   duration_seconds,
                   substr(COALESCE(error_message,''),1,120) AS error_preview
            FROM pipeline_job_runs
            ORDER BY run_id DESC
            LIMIT {int(limit)}
            """,
            con,
        )
    except Exception:
        return pd.DataFrame()


def fetch_brief_log_recent(con: sqlite3.Connection, limit: int = 25) -> pd.DataFrame:
    try:
        return pd.read_sql_query(
            f"""
            SELECT game_date, session, games_covered, picks_count, output_file, generated_at
            FROM brief_log
            ORDER BY generated_at DESC
            LIMIT {int(limit)}
            """,
            con,
        )
    except Exception:
        return pd.DataFrame()


_ALLOWED_JOB_STATUS: tuple[str, ...] = (
    "pending",
    "running",
    "complete",
    "failed",
    "timeout",
    "skipped",
)


def fetch_pipeline_job_by_id(
    con: sqlite3.Connection, job_id: int
) -> dict[str, Any] | None:
    try:
        r = con.execute(
            "SELECT * FROM pipeline_jobs WHERE job_id = ?",
            (int(job_id),),
        ).fetchone()
        return dict(r) if r else None
    except Exception:
        return None


def fetch_pipeline_jobs_all_columns(
    con: sqlite3.Connection,
    *,
    job_date_et: str | None = None,
    limit: int = 800,
) -> pd.DataFrame:
    if job_date_et:
        try:
            return pd.read_sql_query(
                f"""
                SELECT * FROM pipeline_jobs
                WHERE job_date_et = ?
                ORDER BY scheduled_time_et, job_id
                LIMIT {int(limit)}
                """,
                con,
                params=(job_date_et,),
            )
        except Exception:
            return pd.DataFrame()
    try:
        return pd.read_sql_query(
            f"""
            SELECT * FROM pipeline_jobs
            ORDER BY job_id DESC
            LIMIT {int(limit)}
            """,
            con,
        )
    except Exception:
        return pd.DataFrame()


def update_pipeline_job_row(
    con: sqlite3.Connection,
    *,
    job_id: int,
    status: str,
    error_message: str | None = None,
    retry_count: int | None = None,
    clear_timestamps_for_retry: bool = False,
) -> str | None:
    """
    Update one ``pipeline_jobs`` row (operator tool). Returns None on success, else error string.
    """
    st = (status or "").strip()
    if st not in _ALLOWED_JOB_STATUS:
        return f"Invalid status: {st!r}"

    set_clauses: list[str] = ["status = ?", "error_message = ?"]
    pvals2: list[Any] = [st, (error_message if error_message is not None else None)]
    if retry_count is not None:
        set_clauses.append("retry_count = ?")
        pvals2.append(int(retry_count))
    if clear_timestamps_for_retry and st == "pending":
        set_clauses.extend(["started_at = NULL", "completed_at = NULL"])
    if clear_timestamps_for_retry and st == "running":
        set_clauses.append("completed_at = NULL")

    sql = f"UPDATE pipeline_jobs SET {', '.join(set_clauses)} WHERE job_id = ?"
    pvals2.append(int(job_id))
    try:
        con.execute(sql, pvals2)
        con.commit()
    except Exception as exc:
        try:
            con.rollback()
        except Exception:
            pass
        return str(exc)
    return None
