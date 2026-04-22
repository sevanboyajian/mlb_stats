#!/usr/bin/env python3
"""
run_pipeline.py
───────────────
Execution engine for scheduled pipeline jobs.

Polls pipeline_jobs and runs due jobs (single-threaded) in scheduled_time order.

CHANGE LOG (latest first)
────────────────────────
2026-04-22  ``_group_brief_cli_suffix``: pass ``--game-group-id`` for ``group_brief`` so
            ``brief_log`` duplicate checks and bet_ledger materialization are not skipped after
            the first same-session group run.
2026-04-22  ``_gdb_as_of_suffix``: pass America/New_York wall time when the job is dequeued
            (not ``pipeline_jobs.scheduled_time_et``) so ``generate_daily_brief`` timestamps
            and hybrid session match actual execution; still satisfies ``--as-of`` for rc≠2.
2026-04-17  ``_build_command``: ``odds_pull`` includes ``--pregame`` (required by load_odds.py);
            ``--date job_date_et`` and ``--force`` when slate date is not local today;
            ``odds_check`` / ``ledger_snapshot`` pass ``--date`` so jobs match the slate.
2026-04-17  Dependency satisfaction: ``failed`` and ``timeout`` upstream rows count as resolved
            (with ``complete``/``skipped``) so terminal failures do not block the whole day; only
            ``pending``/``running`` block. Missing rows for a job_type still block.
2026-04-17  ``--explain-deps YYYY-MM-DD``: read-only diagnostic listing upstream job_types for
            the slate (counts by status, which rows satisfy ``complete``/``skipped``) and each
            job row's dependency result — distinguishes ``SKIP — deps`` from terminal ``skipped``.
2026-04-19  After ``_MAX_FAILURE_RETRIES``, terminal status is ``skipped`` (not ``failed``);
            ``pipeline_job_runs`` records ``skipped``; dependency checks treat ``skipped`` like
            ``complete`` so downstream jobs are not blocked. SQLite CHECK + optional table rebuild
            adds ``skipped``. ``--status`` lists skipped jobs separately.
2026-04-19  Dependency check: treat ``job_date_et`` match OR (NULL/blank ``job_date_et`` on the
            dep row with ``scheduled_time_et`` starting with the slate YYYY-MM-DD). Fixes false
            ``SKIP — deps`` when globals completed but legacy rows lacked ``job_date_et``.
2026-04-19  Stale-running reset now uses ``_handle_job_failure`` (increments ``retry_count``,
            respects ``_MAX_FAILURE_RETRIES``). Previously reset-to-pending left ``retry_count``
            unchanged, so stuck ``running`` jobs could retry without bound.
2026-04-19  Failure retries: ``_MAX_FAILURE_RETRIES`` raised from 2 to 5 — after 5 failed
            attempts the job is marked ``failed`` and the runner moves on (terminal alert).
2026-04-19  Fix: do not append ``# group_id=…`` to subprocess command strings. On Windows
            ``cmd.exe`` (``shell=True``) ``#`` is not a comment, so Python received ``#`` as an
            argv token (e.g. ``load_odds --markets game # …`` → invalid ``--markets`` choice).
            Group context is logged separately.
2026-04-19  job_type ``load_weather`` → ``load_weather.py --date`` (morning global: wind +
            probable starters after ``load_today``). Deps: ``day_setup`` / ``prior_report`` /
            ``early_peek`` / ``group_brief`` wait on ``load_weather`` where applicable.
2026-04-19  --sleep-until-due: when idle (or all due rows skipped on deps), sleep until the
            next pending ``scheduled_time_et`` instead of fixed polling; optional ``--job-date-et``
            scopes next-wake MIN query; ``--exit-when-no-pending`` exits when nothing pending.
            Fixes busy-spin when every due job hits SKIP deps. Logs include ``game_group_id``.
2026-04-17  Fix: due-job execution body was outside the ``for job in due`` loop, so only the
            last due row ran per poll; indent so every pending due job runs in order.
2026-04-17  job_type bet_ledger_sync → generate_daily_brief.py --sync-bet-ledger-only (deps: load_today).
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
            up to N retries (``retry_count < _MAX_FAILURE_RETRIES``); next attempt next poll loop.
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
            runner derives commands from job_type; brief jobs get --as-of from wall-clock ET at run.

Rules:
- Only runs jobs with status='pending' and scheduled_time <= now
- Optional ``--sleep-until-due``: no fixed poll interval when idle; sleep until the next
  pending ``scheduled_time_et`` (table-driven wake). Overlapping game groups still interleave
  by ``scheduled_time_et`` order; merged odds blocks keep a single ``odds_pull`` on the rep
  ``game_group_id``—do not batch strictly by group without respecting that ordering.
- Marks jobs: pending -> running -> complete / skipped (max retries) / failed / timeout
- Does not modify job definitions
- Does not run jobs in parallel
- Command lines for day_setup and schedule_next_day_globals are built in _build_command();
  they must stay aligned with schedule_pipeline_day.py CLI flags.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import connect as db_connect, get_db_path

# Max automatic re-runs after a failed attempt (retry_count 0…N−1 re-queue; at N terminal fail).
_MAX_FAILURE_RETRIES = 5

# Upstream rows in these statuses count as "resolved" for dependency checks so one stuck or
# failed job does not block the entire slate. ``failed``/``timeout`` are terminal (not pending);
# downstream may run with degraded/missing upstream data — use alerts and --explain-deps.
_DEPS_UPSTREAM_RESOLVED_STATUSES: tuple[str, ...] = (
    "complete",
    "skipped",
    "failed",
    "timeout",
)

class _TeeTextIO:
    """Write-through tee for stdout/stderr (keeps console output)."""

    def __init__(self, a, b):
        self._a = a
        self._b = b

    def write(self, s):
        try:
            self._a.write(s)
        except Exception:
            pass
        try:
            self._b.write(s)
        except Exception:
            pass
        return len(s)

    def flush(self):
        try:
            self._a.flush()
        except Exception:
            pass
        try:
            self._b.flush()
        except Exception:
            pass

    def isatty(self):
        try:
            return bool(self._a.isatty())
        except Exception:
            return False


def _utc_now_iso_z() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ensure_runner_lock_table(con: sqlite3.Connection) -> None:
    """
    Create a single-row lock table used to prevent concurrent runner execution.
    SQLite-compatible, no OS locks.
    """
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


def _try_acquire_runner_lock(con: sqlite3.Connection) -> bool:
    """
    Attempt to acquire the runner lock.
    Returns True if acquired by this process; False if another runner holds it.
    """
    try:
        _ensure_runner_lock_table(con)
    except Exception:
        # If we can't ensure the table, do not risk concurrent execution.
        return False

    acquired_at = _utc_now_iso_z()
    try:
        pid: int | None = int(os.getpid())
    except Exception:
        pid = None
    host = (os.getenv("COMPUTERNAME") or os.getenv("HOSTNAME") or "").strip() or None

    try:
        # Acquire in a transaction so two runners don't both think they succeeded.
        con.execute("BEGIN IMMEDIATE")
        cur = con.execute(
            """
            INSERT OR IGNORE INTO runner_lock (lock_id, acquired_at_utc, pid, host)
            VALUES (1, ?, ?, ?)
            """,
            (acquired_at, pid, host),
        )
        con.commit()
        return int(getattr(cur, "rowcount", 0) or 0) == 1
    except Exception:
        try:
            con.rollback()
        except Exception:
            pass
        return False


def _release_runner_lock(con: sqlite3.Connection) -> None:
    """
    Best-effort lock release. Non-fatal.
    """
    try:
        con.execute("DELETE FROM runner_lock WHERE lock_id = 1")
        con.commit()
    except Exception:
        try:
            con.rollback()
        except Exception:
            pass


def _read_runner_lock_row(con: sqlite3.Connection) -> dict | None:
    """
    Return current runner_lock row as dict, or None if unlocked/unavailable.
    """
    try:
        _ensure_runner_lock_table(con)
        con.row_factory = sqlite3.Row
        r = con.execute(
            "SELECT lock_id, acquired_at_utc, pid, host FROM runner_lock WHERE lock_id = 1"
        ).fetchone()
        return dict(r) if r else None
    except Exception:
        return None


def _force_clear_runner_lock(*, con: sqlite3.Connection) -> bool:
    """
    Force-clear the runner lock row (best-effort). Returns True if cleared.
    Safe only when you are sure no other runner is actually active.
    """
    try:
        _ensure_runner_lock_table(con)
    except Exception:
        return False
    try:
        cur = con.execute("DELETE FROM runner_lock WHERE lock_id = 1")
        con.commit()
        return int(getattr(cur, "rowcount", 0) or 0) >= 0
    except Exception:
        try:
            con.rollback()
        except Exception:
            pass
        return False


def _repo_root_path() -> Path:
    # Keep consistent with _REPO_ROOT but return as Path.
    try:
        return Path(_REPO_ROOT)
    except Exception:
        return Path.cwd()


def _alert_log_path() -> Path:
    return _repo_root_path() / "logs" / "alerts.log"


def _print_failure_alert(*, payload: dict[str, Any]) -> None:
    """
    Non-fatal alert to console for terminal job failures.
    Must never raise.
    """
    try:
        print("\n" + "!" * 76)
        kind = str(payload.get("terminal_kind") or "failed")
        title = (
            "! PIPELINE ALERT: JOB SKIPPED (terminal — max retries; deps unblocked)"
            if kind == "skipped"
            else "! PIPELINE ALERT: JOB FAILED (terminal)"
        )
        print(title)
        print("!" * 76)
        print(f"  time_utc:     {payload.get('time_utc', '')}")
        print(f"  job_id:       {payload.get('job_id', '')}")
        print(f"  job_type:     {payload.get('job_type', '')}")
        print(f"  job_date_et:  {payload.get('job_date_et', '')}")
        print(f"  started_utc:  {payload.get('started_at_utc', '')}")
        print(f"  finished_utc: {payload.get('finished_at_utc', '')}")
        err = str(payload.get("error_message", "") or "")
        if err:
            if len(err) > 1200:
                err = err[-1200:]
            print("  error_tail:")
            for line in err.splitlines()[-30:]:
                print(f"    {line}")
        print("!" * 76 + "\n")
    except Exception:
        # Never allow alerting to break pipeline output
        pass


def _append_alert_log(*, payload: dict[str, Any]) -> None:
    """
    Best-effort append-only alert log.
    Must never raise and should be quick.
    """
    try:
        p = _alert_log_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        # One line per alert (easy to tail/grep)
        err_one_line = str(payload.get("error_message", "") or "").replace("\n", " ")[:2000]
        kind = str(payload.get("terminal_kind") or "failed")
        tag = "SKIPPED" if kind == "skipped" else "FAILED"
        line = (
            f"{payload.get('time_utc','')}\t{tag}\t"
            f"job_id={payload.get('job_id','')}\t"
            f"type={payload.get('job_type','')}\t"
            f"date_et={payload.get('job_date_et','')}\t"
            f"started_utc={payload.get('started_at_utc','')}\t"
            f"finished_utc={payload.get('finished_at_utc','')}\t"
            f"err={err_one_line}"
            "\n"
        )
        p.open("a", encoding="utf-8").write(line)
    except Exception:
        pass


def _send_smtp_email_alert(*, payload: dict[str, Any]) -> None:
    """
    Optional SMTP email alert (best-effort). Runs in background thread.
    Enabled only when env vars are present.
    Must never raise.
    """
    try:
        host = (os.getenv("PIPELINE_SMTP_HOST") or "").strip()
        port = int(os.getenv("PIPELINE_SMTP_PORT") or "587")
        user = (os.getenv("PIPELINE_SMTP_USER") or "").strip()
        password = (os.getenv("PIPELINE_SMTP_PASS") or "").strip()
        mail_from = (os.getenv("PIPELINE_ALERT_FROM") or user or "").strip()
        mail_to = (os.getenv("PIPELINE_ALERT_TO") or "").strip()
        if not host or not mail_to or not mail_from:
            return

        import smtplib
        from email.message import EmailMessage

        msg = EmailMessage()
        tk = str(payload.get("terminal_kind") or "failed")
        subj = "SKIPPED" if tk == "skipped" else "FAILED"
        msg["Subject"] = (
            f"[mlb_stats] PIPELINE {subj} job_id={payload.get('job_id','')} "
            f"type={payload.get('job_type','')}"
        )
        msg["From"] = mail_from
        msg["To"] = mail_to
        tk2 = str(payload.get("terminal_kind") or "failed")
        head = (
            "PIPELINE ALERT: JOB SKIPPED (terminal — max retries)"
            if tk2 == "skipped"
            else "PIPELINE ALERT: JOB FAILED (terminal)"
        )
        body = "\n".join(
            [
                head,
                f"time_utc:     {payload.get('time_utc','')}",
                f"job_id:       {payload.get('job_id','')}",
                f"job_type:     {payload.get('job_type','')}",
                f"job_date_et:  {payload.get('job_date_et','')}",
                f"started_utc:  {payload.get('started_at_utc','')}",
                f"finished_utc: {payload.get('finished_at_utc','')}",
                "",
                "error_message (tail):",
                str(payload.get("error_message", "") or "")[-4000:],
                "",
                f"log_file: {str(_alert_log_path())}",
            ]
        )
        msg.set_content(body)

        with smtplib.SMTP(host, port, timeout=5) as s:
            try:
                s.starttls()
            except Exception:
                pass
            if user and password:
                s.login(user, password)
            s.send_message(msg)
    except Exception:
        pass


def _alert_job_failed_terminal(
    *,
    job_id: int,
    job_type: str,
    job_date_et: str,
    started_at_utc: str,
    finished_at_utc: str,
    error_message: str,
    terminal_kind: str = "failed",
) -> None:
    """
    Non-blocking, non-fatal alert fanout for terminal outcomes (failed or skipped).
    """
    payload: dict[str, Any] = {
        "time_utc": _utc_now_iso_z(),
        "job_id": int(job_id),
        "job_type": str(job_type),
        "job_date_et": str(job_date_et),
        "started_at_utc": str(started_at_utc),
        "finished_at_utc": str(finished_at_utc),
        "error_message": str(error_message or ""),
        "terminal_kind": str(terminal_kind or "failed"),
    }
    _print_failure_alert(payload=payload)
    _append_alert_log(payload=payload)

    # Optional SMTP alert: send in background so pipeline never blocks.
    t = threading.Thread(target=_send_smtp_email_alert, kwargs={"payload": payload}, daemon=True)
    t.start()


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


def _et_now_dt() -> dt.datetime:
    """Current instant in America/New_York (minute-truncated), for sleep-until-due."""
    try:
        from zoneinfo import ZoneInfo

        et = ZoneInfo("America/New_York")
        return dt.datetime.now(tz=et).replace(second=0, microsecond=0)
    except Exception:
        return dt.datetime.now().replace(second=0, microsecond=0)


def _parse_scheduled_time_et(raw: str | None) -> dt.datetime | None:
    """Parse pipeline_jobs.scheduled_time_et values like '2026-04-19 14:05 ET'."""
    if not raw or not isinstance(raw, str):
        return None
    s = raw.strip()
    if not s.endswith("ET"):
        return None
    base = s[:-2].strip()
    try:
        from zoneinfo import ZoneInfo

        zi = ZoneInfo("America/New_York")
        naive = dt.datetime.strptime(base, "%Y-%m-%d %H:%M")
        return naive.replace(tzinfo=zi)
    except Exception:
        return None


def _count_pending_jobs(con: sqlite3.Connection, *, job_date_et: str | None) -> int:
    try:
        if job_date_et:
            row = con.execute(
                "SELECT COUNT(*) AS n FROM pipeline_jobs WHERE status = 'pending' AND job_date_et = ?",
                (str(job_date_et).strip(),),
            ).fetchone()
        else:
            row = con.execute(
                "SELECT COUNT(*) AS n FROM pipeline_jobs WHERE status = 'pending'"
            ).fetchone()
        return int(row[0] or 0) if row else 0
    except Exception:
        return 0


def _next_pending_scheduled_time_et(
    con: sqlite3.Connection,
    cols: set[str],
    *,
    job_date_et: str | None,
) -> str | None:
    if "scheduled_time_et" not in cols:
        return None
    try:
        if job_date_et and "job_date_et" in cols:
            row = con.execute(
                """
                SELECT MIN(scheduled_time_et) AS m
                FROM pipeline_jobs
                WHERE status = 'pending'
                  AND scheduled_time_et IS NOT NULL
                  AND TRIM(scheduled_time_et) != ''
                  AND job_date_et = ?
                """,
                (str(job_date_et).strip(),),
            ).fetchone()
        else:
            row = con.execute(
                """
                SELECT MIN(scheduled_time_et) AS m
                FROM pipeline_jobs
                WHERE status = 'pending'
                  AND scheduled_time_et IS NOT NULL
                  AND TRIM(scheduled_time_et) != ''
                """
            ).fetchone()
        m = row[0] if row else None
        return str(m).strip() if m else None
    except Exception:
        return None


def _sleep_until_next_pending_or_poll(
    *,
    con: sqlite3.Connection,
    cols: set[str],
    sleep_until_due: bool,
    job_date_et: str | None,
    poll_seconds: int,
    exit_when_no_pending: bool,
    max_sleep_seconds: int,
) -> bool:
    """
    Idle wait between waves. Returns True if the outer run loop should exit.
    """
    pending_n = _count_pending_jobs(con, job_date_et=job_date_et)
    if pending_n == 0:
        if exit_when_no_pending:
            print("[run_pipeline] no pending pipeline_jobs; exiting (--exit-when-no-pending).")
            return True
        if not sleep_until_due:
            time.sleep(max(1, int(poll_seconds)))
            return False
        print(f"[run_pipeline] sleep-until-due: no pending jobs{f' for {job_date_et}' if job_date_et else ''}; sleeping {poll_seconds}s")
        time.sleep(max(1, int(poll_seconds)))
        return False

    if not sleep_until_due:
        time.sleep(max(1, int(poll_seconds)))
        return False

    now_et = _et_now_dt()
    nxt = _next_pending_scheduled_time_et(con, cols, job_date_et=job_date_et)
    if not nxt:
        time.sleep(max(1, int(poll_seconds)))
        return False

    wake = _parse_scheduled_time_et(nxt)
    if wake is None:
        time.sleep(max(1, int(poll_seconds)))
        return False

    delta = (wake - now_et).total_seconds()
    if delta > float(max_sleep_seconds):
        print(
            f"[run_pipeline] sleep-until-due: next {nxt} is {delta:.0f}s away (> cap {max_sleep_seconds}s); "
            f"sleeping {max_sleep_seconds}s then recomputing."
        )
        time.sleep(float(max_sleep_seconds))
        return False

    if delta <= 0:
        time.sleep(0.25)
        return False

    print(f"[run_pipeline] sleep-until-due: next pending at {nxt} (~{delta:.0f}s)")
    time.sleep(delta)
    return False


def _table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({table})").fetchall()
        return {str(r[1]) for r in rows}  # (cid, name, type, notnull, dflt, pk)
    except Exception:
        return set()

def _gdb_as_of_suffix(_job: dict) -> str:
    """
    Append --as-of for generate_daily_brief (hybrid session + run timestamps).

    Uses **America/New_York wall time at the moment the command is built** (when a due job
    is dequeued in the run loop), not the row's ``scheduled_time_et``. That way
    ``brief_log.generated_at``, default filenames, and email bodies align with when the
    process actually runs; slate filtering uses the same clock.

    The ``_job`` argument is unused but kept for call-site stability. For reproducible
    backtests, run generate_daily_brief from the shell with an explicit ``--as-of`` instead
    of the pipeline.
    """
    try:
        from zoneinfo import ZoneInfo

        w = dt.datetime.now(ZoneInfo("America/New_York")).replace(microsecond=0)
    except Exception:
        w = dt.datetime.now()
    return f' --as-of "{w.strftime("%Y-%m-%d %H:%M")}"'


def _build_command(job: dict) -> str:
    """
    Translate a pipeline_jobs row into an executable command string.
    pipeline_jobs definitions are job_type-driven (no 'command' column).

    Scheduling jobs (see batch/jobs/schedule_pipeline_day.py):
    - load_weather: morning global — Open-Meteo wind + MLB probable starters (after load_today).
    - day_setup: morning pass — only per-group jobs for this slate date (--groups-only).
    - schedule_next_day_globals: evening pass — only next day's group-0 globals
      (--globals-only --date-et job_date_et + 1 calendar day).
    """
    job_type = str(job.get("job_type") or "").strip()
    job_date = str(job.get("job_date_et") or "").strip()
    today_iso = dt.date.today().isoformat()

    # load_odds.py requires a mode (--pregame, etc.); pipeline runs pregame pulls for the slate.
    _odds_pull = "python batch/ingestion/load_odds.py --pregame --markets game"
    if job_date:
        _odds_pull += f" --date {job_date}"
        if job_date != today_iso:
            _odds_pull += " --force"

    # Note: commands intentionally simple; no parallelism.
    # If a job requires additional parameters later, extend this mapping only.
    # day_setup / schedule_next_day_globals: must match schedule_pipeline_day.py modes.
    mapping: dict[str, str] = {
        "stats_pull": "python batch/ingestion/load_mlb_stats.py",
        "load_today": f"python batch/ingestion/load_today.py --date {job_date}" if job_date else "python batch/ingestion/load_today.py",
        "load_weather": (
            f"python batch/ingestion/load_weather.py --date {job_date}"
            if job_date
            else "python batch/ingestion/load_weather.py"
        ),
        "day_setup": (
            f"python batch/jobs/schedule_pipeline_day.py --groups-only --date-et {job_date}"
            if job_date
            else "python batch/jobs/schedule_pipeline_day.py --groups-only"
        ),
        "prior_report": f"python batch/pipeline/generate_daily_brief.py --session prior --date {job_date}" if job_date else "python batch/pipeline/generate_daily_brief.py --session prior",
        # ``morning`` = Today's Slate only (no model signals); job_type name is ``early_peek``.
        "early_peek": f"python batch/pipeline/generate_daily_brief.py --session morning --date {job_date}" if job_date else "python batch/pipeline/generate_daily_brief.py --session morning",
        # Group jobs (windows are informational; scripts should filter by unplayed games / session rules)
        "odds_pull": _odds_pull,
        "odds_check": (
            f"python diagnostics/check_odds_ready.py --date {job_date}"
            if job_date
            else "python diagnostics/check_odds_ready.py"
        ),
        "weather": (
            f"python batch/ingestion/load_weather.py --date {job_date}"
            if job_date
            else "python batch/ingestion/load_weather.py"
        ),
        # Group brief: --session primary; ET clock can remap to early/afternoon/primary/late (not closing).
        "group_brief": f"python batch/pipeline/generate_daily_brief.py --session primary --date {job_date}" if job_date else "python batch/pipeline/generate_daily_brief.py --session primary",
        # Materialize bet_ledger inside T−30 pregame window; schedule every N minutes on game days if needed.
        "bet_ledger_sync": (
            f"python batch/pipeline/generate_daily_brief.py --sync-bet-ledger-only --date {job_date}"
            if job_date
            else "python batch/pipeline/generate_daily_brief.py --sync-bet-ledger-only"
        ),
        "ledger_snapshot": (
            f"python batch/pipeline/daily_results_report.py --date {job_date}"
            if job_date
            else "python batch/pipeline/daily_results_report.py"
        ),
        "schedule_next_day_globals": (
            f"python batch/jobs/schedule_pipeline_day.py --globals-only --date-et {_next_calendar_date_et(job_date)}"
            if job_date
            else f"python batch/jobs/schedule_pipeline_day.py --globals-only --date-et {_default_tomorrow_date_et()}"
        ),
        "daily_backup": "python batch/utils/daily_backup.py",
        "email_runlog_morning": (
            f"python batch/jobs/email_run_log.py --date {job_date} --kind morning"
            if job_date
            else "python batch/jobs/email_run_log.py --kind morning"
        ),
        "email_runlog_eod": (
            f"python batch/jobs/email_run_log.py --date {job_date} --kind eod"
            if job_date
            else "python batch/jobs/email_run_log.py --kind eod"
        ),
    }

    # Never append ``# …`` for human context: Windows cmd.exe (shell=True) does not treat
    # ``#`` as a comment, so the rest of the line becomes argv and breaks argparse.
    base = mapping.get(job_type, "")
    if not base:
        return ""
    # Hybrid session CLI: generate_daily_brief needs a clock for non-prior sessions.
    # Append --as-of using wall-clock ET at execution (see _gdb_as_of_suffix) so rc≠2.
    if job_type in ("early_peek", "group_brief"):
        cmd = base + _gdb_as_of_suffix(job)
        if job_type == "group_brief":
            cmd += _group_brief_cli_suffix(job)
        return cmd
    if job_type in ("prior_report", "bet_ledger_sync"):
        return base + _gdb_as_of_suffix(job)
    return base


def _group_brief_cli_suffix(job: dict) -> str:
    """So duplicate guard and filenames are per pipeline game group, not per session slot."""
    gid = job.get("game_group_id")
    if gid in (None, "", 0, "0"):
        return ""
    try:
        n = int(gid)
    except (TypeError, ValueError):
        return ""
    if n <= 0:
        return ""
    return f" --game-group-id {n}"


def _job_group_context(job: dict) -> str:
    """Non-executable annotation for logs (pipeline_jobs row)."""
    group_id = job.get("game_group_id")
    if group_id in (None, "", 0):
        return ""
    ws = str(job.get("window_start_et") or "").strip()
    we = str(job.get("window_end_et") or "").strip()
    return f"group_id={group_id} window=[{ws or '?'} -> {we or '?'}]"


def _dependency_rules() -> dict[str, list[str]]:
    """
    Hardcoded dependencies (simple + readable, no new tables).
    If a job_type has dependencies, it should not run until those job_types are resolved
    (see ``_DEPS_UPSTREAM_RESOLVED_STATUSES``).
    """
    return {
        # load_today must complete before any game-based jobs
        "load_weather": ["load_today"],
        "day_setup": ["load_weather"],
        "odds_pull": ["load_today"],
        "odds_check": ["load_today", "odds_pull"],
        "weather": ["load_today"],
        # odds pulls must complete before brief generation; morning load_weather fills wind + starters
        "prior_report": ["load_weather"],
        "early_peek": ["load_weather"],
        "group_brief": ["load_today", "odds_pull", "load_weather"],
        "bet_ledger_sync": ["load_today"],
        "ledger_snapshot": ["load_today", "odds_pull"],
        # Run backup only after next-day globals have been scheduled.
        "daily_backup": ["schedule_next_day_globals"],
        # Email the runner log after group-0 phases complete.
        "email_runlog_morning": ["early_peek"],
        "email_runlog_eod": ["daily_backup"],
    }


def _dependency_complete_for_slate(
    con: sqlite3.Connection,
    *,
    dep_job_type: str,
    job_date_et: str,
) -> bool:
    """
    True if some pipeline_jobs row shows ``dep_job_type`` satisfied for this slate date.

    Resolved statuses include ``complete``, ``skipped``, ``failed``, and ``timeout`` so a
    terminal upstream failure does not deadlock the slate; only ``pending``/``running`` wait.

    Matches ``job_date_et`` when set. Rows with NULL/blank ``job_date_et`` (legacy) still
    count if ``scheduled_time_et`` begins with YYYY-MM-DD (first 10 chars), so morning globals
    satisfy deps for same-calendar-day group jobs.
    """
    st_in = ", ".join(f"'{s}'" for s in _DEPS_UPSTREAM_RESOLVED_STATUSES)
    jd = str(job_date_et or "").strip()
    if not jd:
        cur = con.execute(
            f"""
            SELECT COUNT(*) AS n
            FROM pipeline_jobs
            WHERE job_type = ?
              AND status IN ({st_in})
            """,
            (str(dep_job_type),),
        )
        return int(cur.fetchone()[0] or 0) > 0

    cur = con.execute(
        f"""
        SELECT COUNT(*) AS n
        FROM pipeline_jobs
        WHERE job_type = ?
          AND status IN ({st_in})
          AND (
            TRIM(COALESCE(job_date_et, '')) = ?
            OR (
              TRIM(COALESCE(job_date_et, '')) = ''
              AND LENGTH(?) = 10
              AND SUBSTR(TRIM(COALESCE(scheduled_time_et, '')), 1, 10) = ?
            )
          )
        """,
        (str(dep_job_type), jd, jd, jd),
    )
    return int(cur.fetchone()[0] or 0) > 0


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

    missing: list[str] = []
    for dep in deps:
        if not _dependency_complete_for_slate(con, dep_job_type=dep, job_date_et=job_date):
            missing.append(dep)

    if missing:
        scope = f"job_date_et={job_date}" if job_date else "all dates"
        return False, f"deps not complete ({scope}): {', '.join(missing)}"
    return True, ""


def _dep_rows_for_slate(
    con: sqlite3.Connection,
    *,
    dep_job_type: str,
    job_date_et: str,
) -> list[dict[str, Any]]:
    """
    Rows matching the same slate filter as ``_dependency_complete_for_slate`` (for diagnostics).
    """
    jd = str(job_date_et or "").strip()
    if not jd:
        cur = con.execute(
            """
            SELECT job_id, job_type, game_group_id, status, retry_count,
                   substr(COALESCE(error_message,''),1,160) AS err_tail
            FROM pipeline_jobs
            WHERE job_type = ?
            ORDER BY job_id
            """,
            (str(dep_job_type),),
        )
        return [dict(r) for r in cur.fetchall()]

    cur = con.execute(
        """
        SELECT job_id, job_type, game_group_id, status, retry_count,
               substr(COALESCE(error_message,''),1,160) AS err_tail
        FROM pipeline_jobs
        WHERE job_type = ?
          AND (
            TRIM(COALESCE(job_date_et, '')) = ?
            OR (
              TRIM(COALESCE(job_date_et, '')) = ''
              AND LENGTH(?) = 10
              AND SUBSTR(TRIM(COALESCE(scheduled_time_et, '')), 1, 10) = ?
            )
          )
        ORDER BY job_id
        """,
        (str(dep_job_type), jd, jd, jd),
    )
    return [dict(r) for r in cur.fetchall()]


def _format_dep_slate_explain(rows: list[dict[str, Any]], *, dep_job_type: str) -> tuple[bool, str]:
    """Return (satisfied, one-line explanation). Satisfied if any row is in resolved-status set."""
    if not rows:
        return False, f"{dep_job_type}: no pipeline_jobs rows match this slate (insert schedule or fix job_date_et)"
    ok_ids: list[str] = []
    terminal_ids: list[str] = []
    by_st: dict[str, int] = {}
    blockers: list[str] = []
    for r in rows:
        st = str(r.get("status") or "")
        by_st[st] = by_st.get(st, 0) + 1
        jid = int(r.get("job_id") or 0)
        gid = r.get("game_group_id")
        if st in ("complete", "skipped"):
            ok_ids.append(f"{jid}(gid={gid})")
        elif st in ("failed", "timeout"):
            terminal_ids.append(f"{jid}(gid={gid})")
        elif st == "pending":
            blockers.append(f"job_id={jid} gid={gid} status=pending")
        elif st == "running":
            blockers.append(f"job_id={jid} gid={gid} status=running")

    satisfied = bool(ok_ids or terminal_ids)
    parts = [f"{dep_job_type}: {len(rows)} row(s) on slate"]
    if ok_ids:
        parts.append(f"OK <- {', '.join(ok_ids)}")
    if terminal_ids:
        parts.append(
            f"resolved (terminal failure unblocks deps) <- {', '.join(terminal_ids)}"
        )
    if not satisfied:
        summary = ", ".join(f"{k}={v}" for k, v in sorted(by_st.items()))
        parts.append(
            "blocked - need a row in resolved status "
            f"({', '.join(_DEPS_UPSTREAM_RESOLVED_STATUSES)}); counts: {summary}"
        )
        if blockers[:5]:
            parts.append("examples: " + "; ".join(blockers[:5]))
    return satisfied, " | ".join(parts)


def print_pipeline_jobs_explain_deps(db_path: str, job_date_et: str) -> None:
    """
    Read-only: for one Eastern ``job_date_et``, show slate-wide dependency satisfaction and each job.
    """
    jd = str(job_date_et or "").strip()
    if len(jd) != 10:
        print(f"Error: --explain-deps expects YYYY-MM-DD, got {job_date_et!r}", file=sys.stderr)
        sys.exit(1)

    p = Path(db_path)
    if not p.is_file():
        print(f"Error: database not found:\n  {p.resolve()}", file=sys.stderr)
        sys.exit(1)

    try:
        con = db_connect(db_path, timeout=30)
    except sqlite3.OperationalError as exc:
        print(f"Error: could not open database:\n  {db_path}\n  {exc}", file=sys.stderr)
        sys.exit(1)

    con.row_factory = sqlite3.Row
    print()
    print("=" * 76)
    print(f"  PIPELINE DEPS EXPLAIN  job_date_et={jd}")
    print("=" * 76)
    print(f"  Database: {db_path}")
    print()
    print(
        "  Rules: each dependency is satisfied if at least one matching row has a resolved "
        "status: complete, skipped, failed, or timeout. Only pending/running block. "
        "Matching is slate-wide (no game_group_id filter): one rep odds_pull can cover a block."
    )
    print()

    dep_types: set[str] = set()
    for deps in _dependency_rules().values():
        dep_types.update(deps)

    print("-- Upstream job_types for this slate " + "-" * 36)
    for dep in sorted(dep_types):
        rows = _dep_rows_for_slate(con, dep_job_type=dep, job_date_et=jd)
        ok, line = _format_dep_slate_explain(rows, dep_job_type=dep)
        tag = "OK" if ok else "NO"
        print(f"  [{tag}] {line}")
    print()

    try:
        cur = con.execute(
            """
            SELECT job_id, job_type, job_date_et, game_group_id, status, scheduled_time_et, retry_count,
                   substr(COALESCE(error_message,''),1,120) AS err_tail
            FROM pipeline_jobs
            WHERE TRIM(COALESCE(job_date_et, '')) = ?
            ORDER BY game_group_id, scheduled_time_et, job_id
            """,
            (jd,),
        )
        jobs = [dict(r) for r in cur.fetchall()]
    except Exception as exc:
        print(f"  Error listing jobs: {exc}")
        con.close()
        return

    print(f"-- Each job on slate ({len(jobs)} row(s)) " + "-" * max(0, 40 - len(str(len(jobs)))))
    hdr = ["job_id", "type", "gid", "status", "deps_ok", "detail"]
    w = [7, 22, 4, 9, 8, 72]
    print(_fmt_row(w, hdr))
    print(_rule_line_for_widths(w))
    for job in jobs:
        ok, msg = _deps_complete(con, job)
        jt = str(job.get("job_type") or "")
        st = str(job.get("status") or "")
        deps_cell = "yes" if ok else "no"
        detail = (msg or "-") if not ok else "ready (deps)"
        if st == "skipped" and ok:
            detail = "deps OK - status=skipped means terminal max retries on this row"
        d = detail if len(detail) <= 72 else (detail[:69] + "...")
        line = [
            str(job.get("job_id", "")),
            jt[:22],
            str(job.get("game_group_id", "")),
            st[:9],
            deps_cell,
            d,
        ]
        print(_fmt_row(w, line))
    print()
    print("=" * 76)
    print()
    con.close()


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
    _migrate_pipeline_jobs_allow_skipped_status(con)
    return cols


def _migrate_pipeline_jobs_allow_skipped_status(con: sqlite3.Connection) -> None:
    """
    SQLite CHECK on ``pipeline_jobs.status`` may omit ``skipped`` (older DBs).
    If a test insert with status=skipped fails, rebuild the table via rename/copy.
    """
    try:
        con.execute("BEGIN IMMEDIATE")
        con.execute(
            """
            INSERT INTO pipeline_jobs (job_type, job_date_et, scheduled_time_et, status, game_group_id)
            VALUES ('__skip_chk', '2099-01-01', '2099-01-01 12:00 ET', 'skipped', 0)
            """
        )
        con.execute("DELETE FROM pipeline_jobs WHERE job_type = '__skip_chk'")
        con.commit()
        return
    except Exception:
        try:
            con.rollback()
        except Exception:
            pass

    try:
        from core.utils.game_start_grouping import ensure_pipeline_jobs_table
    except ImportError:
        return

    try:
        con.execute("ALTER TABLE pipeline_jobs RENAME TO pipeline_jobs_old")
    except Exception:
        return

    try:
        ensure_pipeline_jobs_table(con)
        old_info = con.execute("PRAGMA table_info(pipeline_jobs_old)").fetchall()
        new_info = con.execute("PRAGMA table_info(pipeline_jobs)").fetchall()
        old_cols = {r[1] for r in old_info}
        new_cols = {r[1] for r in new_info}
        shared = sorted(old_cols & new_cols)
        if shared:
            cs = ", ".join(shared)
            con.execute(f"INSERT INTO pipeline_jobs ({cs}) SELECT {cs} FROM pipeline_jobs_old")
        con.execute("DROP TABLE pipeline_jobs_old")
        con.commit()
        print("[run_pipeline] migrated pipeline_jobs: CHECK now allows status='skipped'")
    except Exception:
        try:
            con.rollback()
        except Exception:
            pass
        try:
            con.execute("ALTER TABLE pipeline_jobs_old RENAME TO pipeline_jobs")
            con.commit()
        except Exception:
            pass


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
    Otherwise set status ``skipped`` (or ``failed`` if no retry_count column) and alert;
    downstream deps treat ``skipped`` like ``complete`` for scheduling purposes.
    """
    rc_col = "retry_count" in cols
    next_count = int(retry_count_before) + 1

    if int(retry_count_before) < _MAX_FAILURE_RETRIES and rc_col:
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
            f"[job] RETRY — job_id={job_id} attempt={next_count}/{_MAX_FAILURE_RETRIES} "
            f"(will run after next poll loop)"
        )
        return

    term_msg = (
        f"{error_message} | terminal=skipped after {_MAX_FAILURE_RETRIES} attempts "
        f"(deps unblocked — assume manual follow-up)"
    )
    terminal = "skipped" if rc_col else "failed"
    run_st = "skipped" if rc_col else "failed"
    job_st = "skipped" if rc_col else "failed"

    _insert_pipeline_job_run_full(
        con,
        run_cols,
        job_id=job_id,
        job_type=job_type,
        job_date_et=job_date_et,
        started_at_utc=started_iso,
        finished_at_utc=completed_ts,
        run_status=run_st,
        error_message=term_msg if rc_col else error_message,
    )
    _sync_pipeline_jobs_from_run(
        con,
        cols,
        job_id=job_id,
        job_status=job_st,
        started_at_utc=started_iso,
        finished_at_utc=completed_ts,
        job_error_message=term_msg if rc_col else error_message,
        retry_count_value=next_count if rc_col else None,
    )

    if job_st == "skipped":
        print(
            f"[job] SKIPPED (terminal, max retries) job_id={job_id} type={job_type!s} "
            f"— status=skipped; downstream deps will proceed"
        )
    _alert_job_failed_terminal(
        job_id=job_id,
        job_type=job_type,
        job_date_et=job_date_et,
        started_at_utc=started_iso,
        finished_at_utc=completed_ts,
        error_message=term_msg if rc_col else error_message,
        terminal_kind=terminal,
    )


def _reset_stale_running_jobs(
    con: sqlite3.Connection,
    cols: set[str],
    run_cols: set[str],
    *,
    stale_minutes: int,
) -> int:
    """
    Jobs stuck in 'running' longer than stale_minutes are treated like a failed run:
    ``_handle_job_failure`` records a run row, increments ``retry_count``, and either
    re-queues as ``pending`` (under cap) or marks ``failed`` (terminal). Without this,
    reset-to-pending left ``retry_count`` unchanged and the same job could retry without bound.
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

        try:
            jcur = con.execute("SELECT * FROM pipeline_jobs WHERE job_id = ?", (jid,))
            job_row = jcur.fetchone()
            if not job_row:
                continue
            job = dict(job_row)
            retry_before = int(job.get("retry_count") or 0)
            started_iso = str(job.get("started_at") or job.get("start_time") or "").strip() or _utc_now_iso_z()
            finished_iso = _utc_now_iso_z()
            note = (
                f"stale-running after {stale_minutes}m in status=running "
                f"(started_at={st_raw!r}; runner reset — counts as failed attempt)"
            )
            print(f"[run_pipeline] STALE running job_id={jid} — applying retry cap via failure handler")
            _handle_job_failure(
                con,
                cols,
                job_id=jid,
                job_type=str(job.get("job_type") or ""),
                job_date_et=str(job.get("job_date_et") or ""),
                retry_count_before=retry_before,
                error_message=note,
                completed_ts=finished_iso,
                run_cols=run_cols,
                started_iso=started_iso,
            )
            reset_n += 1
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
        encoding="utf-8",
        errors="replace",
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
    sleep_until_due: bool = False,
    scope_job_date_et: str | None = None,
    exit_when_no_pending: bool = False,
    max_sleep_seconds: int = 86400,
) -> None:
    con = db_connect(db_path, timeout=30)
    con.row_factory = sqlite3.Row
    lock_acquired = False
    try:
        lock_acquired = _try_acquire_runner_lock(con)
        if not lock_acquired:
            print("[run_pipeline] runner lock already held; exiting safely.")
            return

        cols = _table_columns(con, "pipeline_jobs")
        if not cols:
            raise RuntimeError("pipeline_jobs table not found or unreadable")

        cols = _ensure_pipeline_jobs_extras(con, cols)
        run_cols = _ensure_pipeline_job_runs(con)

        print(f"[run_pipeline] db={db_path}")
        print(
            f"[run_pipeline] mode={'once' if once else 'loop'} poll_seconds={poll_seconds} "
            f"ghost={ghost} stale_minutes={stale_minutes} timeout_minutes={timeout_minutes} "
            f"sleep_until_due={sleep_until_due} scope_job_date_et={scope_job_date_et!r} "
            f"exit_when_no_pending={exit_when_no_pending}"
        )

        while True:
            _mark_running_timed_out(con, cols, run_cols, timeout_minutes=timeout_minutes)
            _reset_stale_running_jobs(con, cols, run_cols, stale_minutes=stale_minutes)

            now_iso = _utc_now_iso_z()
            due = _fetch_due_jobs(con, now_iso, cols)

            if not due:
                if once:
                    print(f"[run_pipeline] {now_iso} no due jobs; exiting (--once).")
                    break
                if exit_when_no_pending and _count_pending_jobs(con, job_date_et=scope_job_date_et) == 0:
                    print("[run_pipeline] no pending pipeline_jobs; exiting (--exit-when-no-pending).")
                    break
                if _sleep_until_next_pending_or_poll(
                    con=con,
                    cols=cols,
                    sleep_until_due=sleep_until_due,
                    job_date_et=scope_job_date_et,
                    poll_seconds=poll_seconds,
                    exit_when_no_pending=exit_when_no_pending,
                    max_sleep_seconds=max_sleep_seconds,
                ):
                    break
                continue

            progressed = False
            for job in due:
                job_id = int(job["job_id"])
                job_type = str(job.get("job_type") or "")
                scheduled_time = str(job.get("scheduled_time_et") or job.get("scheduled_time") or "")
                retry_count_before = int(job.get("retry_count") or 0)
                command = _build_command(job).strip()
                gid = job.get("game_group_id")

                start_iso = _utc_now_iso_z()
                print(
                    f"\n[job] id={job_id} type={job_type} job_date_et={job.get('job_date_et')!s} "
                    f"game_group_id={gid!s} scheduled_time={scheduled_time}"
                )
                print(f"[job] start={start_iso}")
                print(f"[job] command={command!r}")
                ctx = _job_group_context(job)
                if ctx:
                    print(f"[job] context: {ctx}")
    
                ok, dep_msg = _deps_complete(con, job)
                if not ok:
                    print(f"[job] SKIP — {dep_msg}")
                    continue
    
                if ghost:
                    progressed = True
                    print("[job] GHOST MODE — would set status=running, execute command, then set complete/failed")
                    continue
    
                # Claim job: single UPDATE … AND status='pending' (SQLite-atomic; avoids duplicate execution).
                if not _claim_pending_job(con, cols, job_id=job_id, started_at=start_iso):
                    print(f"[job] SKIP — job_id={job_id} not pending (already claimed or state changed)")
                    continue
    
                progressed = True
                jd_et = str(job.get("job_date_et") or "")
    
                if not command:
                    end_iso = _utc_now_iso_z()
                    msg = f"no command mapping for job_type={job_type!r}"
                    print(f"[job] end={end_iso} failure (no command) — {msg}")
                    _handle_job_failure(
                        con,
                        cols,
                        job_id=job_id,
                        job_type=job_type,
                        job_date_et=jd_et,
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
                        job_date_et=jd_et,
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
                        job_date_et=jd_et,
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
                        job_date_et=jd_et,
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

            if not progressed:
                print(
                    "[run_pipeline] no job progressed this wave (all SKIP deps or empty); "
                    "backing off to avoid a busy loop."
                )
                if _sleep_until_next_pending_or_poll(
                    con=con,
                    cols=cols,
                    sleep_until_due=sleep_until_due,
                    job_date_et=scope_job_date_et,
                    poll_seconds=max(15, int(poll_seconds)),
                    exit_when_no_pending=exit_when_no_pending,
                    max_sleep_seconds=max_sleep_seconds,
                ):
                    break
    finally:
        if lock_acquired:
            _release_runner_lock(con)
        try:
            con.close()
        except Exception:
            pass


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
    _print_job_block("Skipped jobs (max retries — deps unblocked)", "status = 'skipped'")
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
        "--explain-deps",
        metavar="YYYY-MM-DD",
        default=None,
        help="Read-only: show slate-wide dependency satisfaction and per-job deps for this job_date_et",
    )
    p.add_argument(
        "--status",
        action="store_true",
        help="Print pending/running/failed jobs and last 10 runs, then exit (read-only)",
    )
    p.add_argument(
        "--db",
        default=None,
        help="Path to mlb_stats.db (defaults to core.db.connection.get_db_path()); must exist for --status / --explain-deps",
    )
    p.add_argument(
        "--force-unlock",
        action="store_true",
        help="Force-clear runner_lock before starting (use only if a previous run crashed and no runner is active).",
    )
    p.add_argument("--once", action="store_true", help="Run one polling pass then exit")
    p.add_argument("--ghost", action="store_true", help="Print what would run; do not execute or update DB")
    p.add_argument(
        "--log-file",
        default=None,
        metavar="PATH",
        help="Append all runner stdout/stderr to this UTF-8 log file while still printing to console. "
        "If omitted and --job-date-et is set, defaults to logs/run_pipeline_YYYY-MM-DD.txt",
    )
    p.add_argument("--poll-seconds", type=int, default=60, help="Polling interval when looping (default 60)")
    p.add_argument(
        "--sleep-until-due",
        action="store_true",
        help="When looping with no due jobs (or all due rows skipped on deps), sleep until the "
        "earliest pending scheduled_time_et instead of polling every --poll-seconds. "
        "Optional --job-date-et scopes the next-wake query to one slate day.",
    )
    p.add_argument(
        "--job-date-et",
        default=None,
        metavar="YYYY-MM-DD",
        help="Eastern slate date: limit sleep-until-due / exit-when-no-pending to this job_date_et.",
    )
    p.add_argument(
        "--exit-when-no-pending",
        action="store_true",
        help="Exit the loop when there are zero pending rows (after optional --job-date-et filter). "
        "Checked whenever there are no due jobs; works with or without --sleep-until-due.",
    )
    p.add_argument(
        "--max-sleep-hours",
        type=float,
        default=24.0,
        metavar="H",
        help="Cap each sleep-until-due wait (default 24 hours) before recomputing MIN(scheduled_time_et).",
    )
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
    # Tee runner output to a log file (still prints live to console).
    log_path = str(args.log_file).strip() if args.log_file else ""
    if not log_path and args.job_date_et:
        log_path = str((_REPO_ROOT / "logs" / f"run_pipeline_{str(args.job_date_et).strip()}.txt").resolve())
    _log_fh = None
    if log_path:
        try:
            Path(log_path).parent.mkdir(parents=True, exist_ok=True)
            _log_fh = open(log_path, "a", encoding="utf-8", errors="replace")
            sys.stdout = _TeeTextIO(sys.stdout, _log_fh)
            sys.stderr = _TeeTextIO(sys.stderr, _log_fh)
        except Exception as exc:
            print(f"[run_pipeline] WARNING: could not open --log-file {log_path!r}: {exc}")
    if args.force_unlock:
        try:
            con = db_connect(db_path, timeout=30)
            row = _read_runner_lock_row(con)
            if row:
                print(
                    "[run_pipeline] force-unlock: clearing existing runner_lock "
                    f"(acquired_at_utc={row.get('acquired_at_utc')!s} pid={row.get('pid')!s} host={row.get('host')!s})"
                )
            else:
                print("[run_pipeline] force-unlock: runner_lock not present (nothing to clear)")
            ok = _force_clear_runner_lock(con=con)
            if not ok:
                print("[run_pipeline] force-unlock: failed to clear runner_lock; refusing to start.")
                return
        finally:
            try:
                con.close()
            except Exception:
                pass
    if args.explain_deps:
        print_pipeline_jobs_explain_deps(db_path, str(args.explain_deps).strip())
        return
    if args.status:
        print_pipeline_status(db_path)
        return

    max_sleep = max(60.0, float(args.max_sleep_hours) * 3600.0)

    run_loop(
        db_path=db_path,
        once=bool(args.once),
        poll_seconds=int(args.poll_seconds),
        ghost=bool(args.ghost),
        stale_minutes=max(0, int(args.stale_minutes)),
        timeout_minutes=max(0, int(args.timeout_minutes)),
        sleep_until_due=bool(args.sleep_until_due),
        scope_job_date_et=(str(args.job_date_et).strip() if args.job_date_et else None),
        exit_when_no_pending=bool(args.exit_when_no_pending),
        max_sleep_seconds=int(max_sleep),
    )

    try:
        if _log_fh is not None:
            _log_fh.flush()
            _log_fh.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()

