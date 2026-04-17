#!/usr/bin/env python3
"""
schedule_pipeline_day.py
────────────────────────
Create/update today's dynamic pipeline schedule in pipeline_jobs based on the
current games inserted by load_today.py.

Console output is intentionally verbose so you can see exactly what it is doing.

CHANGE LOG (latest first)
────────────────────────
2026-04-16  Split scheduling: --globals-only (evening pre-seed next calendar day),
            --groups-only (morning intraday groups/jobs), default = full slate +
            schedule_next_day_globals hook. Shared helper _insert_global_daily_setup_jobs.

Design:
  - game grouping window: 30 minutes (group sessions)
  - odds pull efficiency threshold: 90 minutes (one pull per group at T0-90m)
  - bet logging window: <30 minutes before T0 (ledger_snapshot at T0-29m)

  Modes:
  - --globals-only --date-et TARGET: insert only group-0 daily globals for TARGET (evening
    pre-seed for the next calendar day). No games required.
  - --groups-only: insert only per-group jobs (odds/weather/brief/ledger) for --date-et;
    use after globals were pre-seeded (e.g. morning day_setup).
  - default (neither flag): full day — globals + schedule_next_day_globals + per-group jobs.

  Invocation (run_pipeline.py):
  - job_type schedule_next_day_globals → --globals-only --date-et (job_date_et + 1 day)
  - job_type day_setup → --groups-only --date-et job_date_et

This script is idempotent: it uses INSERT OR IGNORE via a unique index on
(job_type, scheduled_time, game_group_id).
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import connect as db_connect, get_db_path
from core.utils.game_start_grouping import (
    ensure_pipeline_jobs_table,
    group_games_by_start_time,
    schedule_pipeline_jobs_for_game_groups,
)

try:
    from zoneinfo import ZoneInfo

    _ET = ZoneInfo("America/New_York")
except Exception:
    _ET = dt.timezone(dt.timedelta(hours=-4))


def _iso_z(d: dt.datetime) -> str:
    # Store as UTC ISO with Z (consistent with game_start_grouping groups)
    d2 = d.replace(microsecond=0)
    return d2.isoformat() + "Z"


def _parse_iso_z(s: str) -> dt.datetime:
    raw = (s or "").strip()
    if not raw:
        raise ValueError("empty datetime")
    return dt.datetime.fromisoformat(raw.rstrip("Z"))

def _fmt_et(d_utc_naive: dt.datetime) -> str:
    """Naive UTC datetime -> 'YYYY-MM-DD HH:MM ET'."""
    d_utc = d_utc_naive.replace(tzinfo=dt.timezone.utc)
    d_et = d_utc.astimezone(_ET)
    return d_et.strftime("%Y-%m-%d %H:%M ET")

def _et_to_utc_iso_z(et_str: str) -> str:
    """'YYYY-MM-DD HH:MM ET' -> 'YYYY-MM-DDTHH:MM:SSZ'."""
    s = (et_str or "").strip().replace("  ", " ")
    if not s.endswith("ET"):
        raise ValueError(f"expected ET string, got: {et_str!r}")
    base = s[:-2].strip()
    d_local = dt.datetime.strptime(base, "%Y-%m-%d %H:%M").replace(tzinfo=_ET)
    d_utc = d_local.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return _iso_z(d_utc)


def _fetch_games_for_date(con: sqlite3.Connection, game_date_et: str) -> List[Dict[str, Any]]:
    cur = con.execute(
        """
        SELECT game_pk, game_start_utc
        FROM games
        WHERE game_date_et = ?
          AND game_type = 'R'
        ORDER BY game_start_utc, game_pk
        """,
        (game_date_et,),
    )
    return [dict(r) for r in cur.fetchall()]


def _print_groups(groups: List[Dict[str, Any]], max_groups: int = 50) -> None:
    if not groups:
        print("  [groups] none")
        return
    print(f"  [groups] {len(groups)} group(s):")
    for g in groups[:max_groups]:
        print(
            f"    - group_id={g['group_id']:02d}  start_time_utc={g['start_time']}  "
            f"n_games={len(g.get('game_pks') or [])}  game_pks={g.get('game_pks')}"
        )
    if len(groups) > max_groups:
        print(f"    ... ({len(groups) - max_groups} more)")


def _schedule_job_type(
    con: sqlite3.Connection,
    groups: List[Dict[str, Any]],
    *,
    job_type: str,
    offset_minutes: int,
    status: str = "pending",
    window_start_offset_min: int | None = None,
    window_end_offset_min: int | None = None,
) -> int:
    """
    Schedule one job per group at (group_start_time - offset_minutes).
    offset_minutes can be negative to schedule after T0.
    """
    sched_groups = []
    for g in groups:
        t0 = _parse_iso_z(str(g["start_time"]))
        sched = t0 - dt.timedelta(minutes=int(offset_minutes))
        row = {
            "group_id": g["group_id"],
            "scheduled_time_et": _fmt_et(sched),
            "scheduled_time_utc": _iso_z(sched),
        }
        # Optional ET window for human readability
        if window_start_offset_min is not None:
            row["window_start_et"] = _fmt_et(t0 - dt.timedelta(minutes=int(window_start_offset_min)))
        if window_end_offset_min is not None:
            row["window_end_et"] = _fmt_et(t0 - dt.timedelta(minutes=int(window_end_offset_min)))
        sched_groups.append(row)

    return schedule_pipeline_jobs_for_game_groups(
        con,
        sched_groups,
        job_type=job_type,
        scheduled_time_key="scheduled_time_et",
        status=status,
    )

def _insert_global_job(
    con: sqlite3.Connection,
    *,
    job_date_et: str,
    job_type: str,
    scheduled_time_et: str,
    status: str = "pending",
    window_start_et: str | None = None,
    window_end_et: str | None = None,
) -> int:
    ensure_pipeline_jobs_table(con)
    try:
        sched_utc = _et_to_utc_iso_z(str(scheduled_time_et))
        cur = con.execute(
            """
            INSERT OR IGNORE INTO pipeline_jobs
                (job_type, scheduled_time, job_date_et, scheduled_time_et, scheduled_time_utc,
                 window_start_et, window_end_et, status, game_group_id)
            VALUES (?,?,?,?,?,?,?,?,0)
            """,
            (
                str(job_type),
                sched_utc,
                str(job_date_et),
                str(scheduled_time_et),
                sched_utc,
                window_start_et,
                window_end_et,
                str(status),
            ),
        )
        con.commit()
        return 1 if getattr(cur, "rowcount", 0) == 1 else 0
    except Exception:
        return 0


def _normalize_and_dedupe_globals(con: sqlite3.Connection, *, job_date_et: str) -> None:
    """
    SQLite UNIQUE indexes treat NULLs as distinct; use group_id=0 for global jobs.
    Also dedupe any accidental duplicates for the date (keep lowest job_id).
    """
    try:
        # Hard cleanup: drop legacy global rows that used NULL group ids for this date.
        con.execute(
            """
            DELETE FROM pipeline_jobs
            WHERE game_group_id IS NULL
              AND scheduled_time_et IS NOT NULL
              AND substr(scheduled_time_et, 1, 10) = ?
              AND job_type IN ('stats_pull','load_today','day_setup','early_peek','prior_report')
            """,
            (job_date_et,),
        )

        # Some older rows may not have job_date_et populated yet; key off scheduled_time_et prefix too.
        con.execute(
            """
            UPDATE pipeline_jobs
            SET game_group_id = 0
            WHERE game_group_id IS NULL
              AND scheduled_time_et IS NOT NULL
              AND substr(scheduled_time_et, 1, 10) = ?
            """,
            (job_date_et,),
        )
        con.execute(
            """
            DELETE FROM pipeline_jobs
            WHERE IFNULL(game_group_id, 0) = 0
              AND scheduled_time_et IS NOT NULL
              AND substr(scheduled_time_et, 1, 10) = ?
              AND job_id NOT IN (
                SELECT MIN(job_id)
                FROM pipeline_jobs
                WHERE IFNULL(game_group_id, 0) = 0
                  AND scheduled_time_et IS NOT NULL
                  AND substr(scheduled_time_et, 1, 10) = ?
                GROUP BY job_type, scheduled_time_et, IFNULL(game_group_id, 0)
              )
            """,
            (job_date_et, job_date_et),
        )
        con.commit()
    except Exception:
        pass


def _insert_global_daily_setup_jobs(con: sqlite3.Connection, *, job_date_et: str) -> int:
    """Insert the fixed morning global jobs (group_id=0) for job_date_et."""
    g_ins = 0
    g_ins += _insert_global_job(con, job_date_et=job_date_et, job_type="stats_pull", scheduled_time_et=f"{job_date_et} 06:00 ET")
    g_ins += _insert_global_job(con, job_date_et=job_date_et, job_type="load_today", scheduled_time_et=f"{job_date_et} 06:05 ET")
    g_ins += _insert_global_job(con, job_date_et=job_date_et, job_type="day_setup", scheduled_time_et=f"{job_date_et} 06:10 ET")
    g_ins += _insert_global_job(con, job_date_et=job_date_et, job_type="prior_report", scheduled_time_et=f"{job_date_et} 06:15 ET")
    g_ins += _insert_global_job(con, job_date_et=job_date_et, job_type="early_peek", scheduled_time_et=f"{job_date_et} 06:20 ET")
    return int(g_ins)


def _print_jobs_for_date(con: sqlite3.Connection, game_date_et: str, limit: int = 200) -> None:
    cur = con.execute(
        """
        SELECT job_id, job_type, scheduled_time_et, window_start_et, window_end_et, status, game_group_id, created_at
        FROM pipeline_jobs
        WHERE job_date_et = ?
        ORDER BY scheduled_time_et, job_type, game_group_id
        LIMIT ?
        """,
        (game_date_et, int(limit)),
    )
    rows = cur.fetchall()
    print(f"\n  [pipeline_jobs] showing up to {limit} row(s) for {game_date_et}: {len(rows)} found")
    for r in rows:
        d = dict(r)
        print(
            f"    job_id={d.get('job_id')}  type={d.get('job_type'):<15}  "
            f"time={d.get('scheduled_time_et')}  "
            f"win=[{d.get('window_start_et') or ''} -> {d.get('window_end_et') or ''}]  "
            f"status={d.get('status'):<8}  group={d.get('game_group_id')}  created_at={d.get('created_at')}"
        )


def _print_job_counts(con: sqlite3.Connection, game_date_et: str) -> None:
    try:
        cur = con.execute(
            """
            SELECT job_type, COUNT(*) AS n
            FROM pipeline_jobs
            WHERE job_date_et = ?
            GROUP BY job_type
            ORDER BY job_type
            """,
            (game_date_et,),
        )
        rows = cur.fetchall()
    except Exception:
        rows = []
    print("\n  [counts] jobs by type:")
    if not rows:
        print("    (none)")
        return
    for r in rows:
        d = dict(r)
        print(f"    {d.get('job_type'):<15} {d.get('n')}")


def _backfill_et_fields_for_existing_rows(
    con: sqlite3.Connection,
    *,
    game_date_et: str,
    group_t0_by_id: dict[int, dt.datetime],
    brief_min: int,
    ledger_min: int,
) -> int:
    """
    Backfill ET fields/windows for legacy rows that were inserted before ET columns existed.
    Targets this date and the core job types (odds_pull, odds_check, weather, group_brief, ledger_snapshot).
    """
    try:
        cur = con.execute(
            """
            SELECT job_id, job_type, scheduled_time, game_group_id, scheduled_time_et
            FROM pipeline_jobs
            WHERE job_type IN ('odds_pull','odds_check','weather','group_brief','ledger_snapshot')
              AND (job_date_et IS NULL OR job_date_et = ?)
            """,
            (game_date_et,),
        )
        rows = [dict(r) for r in cur.fetchall()]
    except Exception:
        return 0

    updated = 0
    for r in rows:
        if r.get("scheduled_time_et"):
            continue
        jid = r.get("job_id")
        st_utc = r.get("scheduled_time")
        gid = r.get("game_group_id")
        if jid is None or not st_utc:
            continue
        try:
            sched_dt = _parse_iso_z(str(st_utc))
            sched_et = _fmt_et(sched_dt)
        except Exception:
            continue

        # Optional windows based on group anchor (T0)
        win_start = None
        win_end = None
        try:
            if gid is not None:
                t0 = group_t0_by_id.get(int(gid))
            else:
                t0 = None
        except Exception:
            t0 = None

        if t0 is not None and r.get("job_type") == "group_brief":
            win_start = _fmt_et(t0 - dt.timedelta(minutes=int(brief_min)))
            win_end = _fmt_et(t0 - dt.timedelta(minutes=int(ledger_min)))
        elif t0 is not None and r.get("job_type") == "ledger_snapshot":
            win_start = _fmt_et(t0 - dt.timedelta(minutes=30))
            win_end = _fmt_et(t0)

        try:
            con.execute(
                """
                UPDATE pipeline_jobs
                SET job_date_et = ?,
                    scheduled_time_et = ?,
                    scheduled_time_utc = COALESCE(scheduled_time_utc, ?),
                    window_start_et = COALESCE(window_start_et, ?),
                    window_end_et = COALESCE(window_end_et, ?)
                WHERE job_id = ?
                """,
                (game_date_et, sched_et, str(st_utc), win_start, win_end, int(jid)),
            )
            updated += 1
        except Exception:
            continue

    try:
        con.commit()
    except Exception:
        pass
    return updated


def main() -> None:
    p = argparse.ArgumentParser(description="Schedule today's pipeline jobs into pipeline_jobs.")
    p.add_argument("--date-et", default=None, help="Eastern date YYYY-MM-DD (default: today)")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument(
        "--globals-only",
        action="store_true",
        help="Insert only group-0 daily globals for --date-et (requires --date-et). No games needed.",
    )
    mode.add_argument(
        "--groups-only",
        action="store_true",
        help="Insert only per-group jobs for --date-et; skip globals and schedule_next_day_globals.",
    )
    p.add_argument("--group-window-min", type=int, default=30, help="Start-time grouping window (minutes, default 30)")
    p.add_argument("--odds-threshold-min", type=int, default=90, help="Odds pull threshold (minutes before group start, default 90)")
    p.add_argument("--odds-block-min", type=int, default=90, help="Merge adjacent groups into one odds block when within N minutes (default 90)")
    p.add_argument("--weather-min", type=int, default=45, help="Weather refresh scheduled at T0 - N minutes (default 45)")
    p.add_argument("--brief-min", type=int, default=32, help="Brief scheduled at T0 - N minutes (default 32)")
    p.add_argument("--ledger-min", type=int, default=29, help="Ledger snapshot at T0 - N minutes (default 29)")
    p.add_argument("--dry-run", action="store_true", help="Compute + print, but do not write pipeline_jobs")
    args = p.parse_args()

    if args.globals_only and not args.date_et:
        print("error: --globals-only requires --date-et (target calendar day YYYY-MM-DD)")
        sys.exit(2)

    game_date_et = args.date_et or dt.date.today().isoformat()
    print(
        f"[schedule_pipeline_day] date_et={game_date_et} "
        f"globals_only={bool(args.globals_only)} groups_only={bool(args.groups_only)} dry_run={bool(args.dry_run)}"
    )

    db_path = Path(get_db_path())
    print(f"[db] {db_path}")
    con = db_connect(str(db_path), timeout=30)
    con.row_factory = sqlite3.Row

    if args.globals_only:
        if args.dry_run:
            print("\n[dry-run] would insert global daily setup jobs only; not writing to pipeline_jobs.")
            con.close()
            return
        print("\n[db] ensuring pipeline_jobs table exists")
        ensure_pipeline_jobs_table(con)
        _print_job_counts(con, game_date_et)
        print("\n[schedule] inserting global daily setup jobs (globals-only)")
        g_ins = _insert_global_daily_setup_jobs(con, job_date_et=game_date_et)
        print(f"[schedule] inserted global jobs: {g_ins}")
        _normalize_and_dedupe_globals(con, job_date_et=game_date_et)
        _print_job_counts(con, game_date_et)
        _print_jobs_for_date(con, game_date_et)
        con.close()
        print("\nDone.")
        return

    games = _fetch_games_for_date(con, game_date_et)
    print(f"[games] regular-season games found: {len(games)}")
    if not games:
        print("  Nothing to schedule. (Did load_today.py run?)")
        con.close()
        return

    groups = group_games_by_start_time(games, window_minutes=int(args.group_window_min))
    _print_groups(groups)

    if args.dry_run:
        print("\n[dry-run] not writing to pipeline_jobs.")
        con.close()
        return

    print("\n[db] ensuring pipeline_jobs table exists")
    ensure_pipeline_jobs_table(con)

    group_t0_by_id = {int(g["group_id"]): _parse_iso_z(str(g["start_time"])) for g in groups}
    _print_job_counts(con, game_date_et)

    g_ins = 0
    if not args.groups_only:
        # Minimal agreed-to schedule (dynamic per game_group_id) follows below.
        # Also insert global daily setup jobs (group_id = 0) and the next-evening pre-seed job.
        print("\n[schedule] inserting global daily setup jobs")
        g_ins = _insert_global_daily_setup_jobs(con, job_date_et=game_date_et)
        print(f"[schedule] inserted global jobs: {g_ins}")
        _normalize_and_dedupe_globals(con, job_date_et=game_date_et)

        # Insert a single "schedule_next_day_globals" job near the end of the last game group.
        # Runner invokes --globals-only --date-et <next calendar day>.
        try:
            last_t0_utc = max(group_t0_by_id.values())
            last_t0_et = last_t0_utc.replace(tzinfo=dt.timezone.utc).astimezone(_ET)
            sched_next_et = last_t0_et + dt.timedelta(minutes=5)
            sched_next_et_str = sched_next_et.strftime("%Y-%m-%d %H:%M ET")
            inserted_next = _insert_global_job(
                con,
                job_date_et=game_date_et,
                job_type="schedule_next_day_globals",
                scheduled_time_et=sched_next_et_str,
            )
            print(f"[schedule] inserted schedule_next_day_globals: {inserted_next} at {sched_next_et_str}")
        except Exception as exc:
            print(f"[schedule] failed to insert schedule_next_day_globals: {exc}")
    else:
        print("\n[schedule] groups-only: skipping global daily jobs and schedule_next_day_globals")

    # Build odds blocks (merge adjacent groups within args.odds_block_min minutes)
    sorted_groups = sorted(groups, key=lambda g: (_parse_iso_z(str(g["start_time"])), int(g["group_id"])))
    odds_blocks: list[list[dict[str, Any]]] = []
    cur: list[dict[str, Any]] = []
    anchor_t0: dt.datetime | None = None
    for g in sorted_groups:
        t0 = _parse_iso_z(str(g["start_time"]))
        if not cur:
            cur = [g]
            anchor_t0 = t0
            continue
        if anchor_t0 is not None and (t0 - anchor_t0) <= dt.timedelta(minutes=int(args.odds_block_min)):
            cur.append(g)
        else:
            odds_blocks.append(cur)
            cur = [g]
            anchor_t0 = t0
    if cur:
        odds_blocks.append(cur)

    rep_groups: list[dict[str, Any]] = []
    print(f"[odds_blocks] {len(odds_blocks)} block(s) (threshold={int(args.odds_block_min)}m)")
    for b in odds_blocks:
        rep = b[0]
        rep_groups.append(rep)
        gids = [int(x["group_id"]) for x in b]
        print(f"  - rep_group_id={int(rep['group_id'])} covers groups={gids}")

    # Cleanup: remove legacy odds_pull rows for non-representative groups on this date.
    try:
        rep_ids = {int(g["group_id"]) for g in rep_groups}
        rep_ids_sql = ",".join(str(x) for x in sorted(rep_ids)) or "-1"
        con.execute(
            f"""
            DELETE FROM pipeline_jobs
            WHERE job_type = 'odds_pull'
              AND job_date_et = ?
              AND game_group_id IS NOT NULL
              AND game_group_id != 0
              AND game_group_id NOT IN ({rep_ids_sql})
            """,
            (game_date_et,),
        )
        con.commit()
    except Exception:
        pass

    inserted_odds = _schedule_job_type(
        con,
        rep_groups,
        job_type="odds_pull",
        offset_minutes=int(args.odds_threshold_min),
        status="pending",
    )
    print(f"[schedule] inserted odds_pull (by block): {inserted_odds}")

    inserted_odds_check = _schedule_job_type(
        con,
        rep_groups,
        job_type="odds_check",
        offset_minutes=max(0, int(args.odds_threshold_min) - 5),
        status="pending",
    )
    print(f"[schedule] inserted odds_check (by block): {inserted_odds_check}")

    inserted_weather = _schedule_job_type(
        con,
        groups,
        job_type="weather",
        offset_minutes=int(args.weather_min),
        status="pending",
    )
    print(f"[schedule] inserted weather: {inserted_weather}")

    inserted_brief = _schedule_job_type(
        con,
        groups,
        job_type="group_brief",
        offset_minutes=int(args.brief_min),
        status="pending",
        window_start_offset_min=int(args.brief_min),
        window_end_offset_min=int(args.ledger_min),
    )
    print(f"[schedule] inserted group_brief: {inserted_brief}")

    inserted_ledger = _schedule_job_type(
        con,
        groups,
        job_type="ledger_snapshot",
        offset_minutes=int(args.ledger_min),
        status="pending",
        window_start_offset_min=30,
        window_end_offset_min=0,
    )
    print(f"[schedule] inserted ledger_snapshot: {inserted_ledger}")

    total_inserted = (
        int(g_ins)
        + int(inserted_odds)
        + int(inserted_odds_check)
        + int(inserted_weather)
        + int(inserted_brief)
        + int(inserted_ledger)
    )
    print(f"[schedule] total inserted rows: {total_inserted}")

    # Backfill ET fields for legacy rows created before ET columns existed.
    backfilled = _backfill_et_fields_for_existing_rows(
        con,
        game_date_et=game_date_et,
        group_t0_by_id=group_t0_by_id,
        brief_min=int(args.brief_min),
        ledger_min=int(args.ledger_min),
    )
    if backfilled:
        print(f"[migrate] backfilled ET/window fields on {backfilled} existing row(s)")

    _print_job_counts(con, game_date_et)
    _print_jobs_for_date(con, game_date_et)
    con.close()
    print("\nDone.")


if __name__ == "__main__":
    main()

