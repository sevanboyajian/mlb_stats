#!/usr/bin/env python3
"""
schedule_pipeline_day.py
────────────────────────
Create/update today's dynamic pipeline schedule in pipeline_jobs based on the
current games inserted by load_today.py.

Console output is intentionally verbose so you can see exactly what it is doing.

CHANGE LOG (latest first)
────────────────────────
2026-04-20  Word brief email delivery moved to ``generate_daily_brief.py`` via ``delivery.email_sender``
            (env ``BRIEF_EMAIL_TO`` + SMTP); pipeline no longer schedules ``email_job``.
2026-04-17  ``_insert_global_job``: ET-first DBs have no ``scheduled_time`` column; branch like
            ``schedule_pipeline_jobs_for_game_groups`` (legacy INSERT vs modern columns). Fixes
            ``--globals-only`` silently inserting 0 rows (exceptions were swallowed).
2026-04-19  Slate/group report: matchups (away @ home), first pitch ET per game, anchor ET per
            group; optional ``--group-report PATH`` writes UTF-8 file. ``_fetch_games_for_date``
            joins ``teams`` for abbreviations.
2026-04-19  Intraday ``weather`` jobs use the same ``odds_blocks`` / ``rep_groups`` as
            ``odds_pull`` / ``odds_check`` (one refresh per merged block, not per raw group);
            stale ``weather`` rows for non-representative groups are deleted like ``odds_pull``.
2026-04-19  Global morning job ``load_weather`` (06:07 ET): runs ``load_weather.py`` after
            ``load_today`` so wind direction + probable starters exist before ``day_setup``
            and prior/morning briefs.
2026-04-19  group_brief: default --brief-min 30 / --ledger-min 28 (inside 30m pregame bet-ledger window);
            --brief-extra-minutes default 15,5 for extra primary briefs per group; validate brief_min > ledger_min.
2026-04-19  pipeline_jobs windows: odds_pull / odds_check / weather now get window_start_et &
            window_end_et (T0-relative); shared _window_offsets_for_odds_weather; backfill fills
            NULL windows on existing rows for that slate date.
2026-04-19  schedule_next_day_globals: insert after per-group jobs for --groups-only too
            (day_setup had been skipping the evening hook). Shared helper
            _insert_schedule_next_day_globals_job; --groups-only help text updated.
2026-04-16  Split scheduling: --globals-only (evening pre-seed next calendar day),
            --groups-only (morning intraday groups/jobs), default = full slate +
            schedule_next_day_globals hook. Shared helper _insert_global_daily_setup_jobs.

Design:
  - game grouping window: 30 minutes (group sessions)
  - odds pull efficiency threshold: 90 minutes (one pull per merged odds block at T0-90m)
  - weather refresh: same merged blocks as odds (one ``load_weather.py`` per block at T0−N)
  - bet-ledger window: [T0−30m, T0); group_brief default T0−30m (ledger T0−28m); optional extra briefs
    at T0−15m / T0−5m for overlapping slates (see --brief-extra-minutes).

  Modes:
  - --globals-only --date-et TARGET: insert only group-0 daily globals for TARGET (evening
    pre-seed for the next calendar day). No games required. Safe to re-run on a slate date
    that already has per-group jobs: uses INSERT OR IGNORE (adds e.g. missing ``load_weather``
    without deleting odds/weather/brief rows).
  - --groups-only: per-group jobs (odds/weather/brief/ledger) + schedule_next_day_globals for
    --date-et; use after globals were pre-seeded (e.g. morning day_setup). Does not insert the
    morning global jobs (stats_pull … early_peek).
  - default (neither flag): full day — globals + per-group jobs + schedule_next_day_globals.

  Invocation (run_pipeline.py):
  - job_type schedule_next_day_globals → --globals-only --date-et (job_date_et + 1 day)
  - job_type day_setup → --groups-only --date-et job_date_et

This script is idempotent: it uses INSERT OR IGNORE via a unique index on
(job_type, scheduled_time_et, game_group_id).
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


def _reconfigure_stdio_utf8() -> None:
    """
    Avoid UnicodeEncodeError on Windows consoles (cp1252) when printing box-drawing chars.
    """
    for stream in (sys.stdout, sys.stderr):
        try:
            if hasattr(stream, "reconfigure"):
                stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


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


def _parse_brief_extra_minutes(s: str, *, max_min: int = 30) -> list[int]:
    """
    Comma-separated integers: minutes before T0 for extra group_brief rows.
    Returns unique values in (0, max_min], descending (earlier wall times first).
    """
    raw = (s or "").strip()
    if not raw or raw.lower() in ("none", "no", "-"):
        return []
    out: list[int] = []
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        try:
            v = int(p)
        except ValueError:
            continue
        if 0 < v <= int(max_min):
            out.append(v)
    return sorted(set(out), reverse=True)


def _group_brief_window_offsets(
    brief_offset_min: int, ledger_offset_min: int
) -> tuple[int, int]:
    """ET-chronological window: larger offset = earlier instant before T0."""
    a, b = int(brief_offset_min), int(ledger_offset_min)
    return max(a, b), min(a, b)

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


def _game_first_pitch_et(game: Dict[str, Any]) -> str:
    raw = str(game.get("game_start_utc") or "").strip()
    if "T" not in raw:
        return "?"
    try:
        return _fmt_et(dt.datetime.fromisoformat(raw.rstrip("Z")))
    except Exception:
        return "?"


def _format_group_slate_report(
    groups: List[Dict[str, Any]],
    games: List[Dict[str, Any]],
    *,
    game_date_et: str,
    group_window_min: int,
) -> str:
    """Human-readable slate: groups, anchor times (ET), each game matchup + first pitch ET."""
    pk_map: Dict[int, Dict[str, Any]] = {}
    for g in games:
        try:
            pk_map[int(g["game_pk"])] = g
        except Exception:
            continue

    lines: list[str] = []
    lines.append(f"game_date_et={game_date_et}  group_window_min={group_window_min}")
    lines.append(f"games_on_slate={len(games)}  n_groups={len(groups)}")
    lines.append("")

    for grp in groups:
        gid = grp.get("group_id")
        try:
            anchor_et = _fmt_et(_parse_iso_z(str(grp["start_time"])))
        except Exception:
            anchor_et = "?"
        lines.append(
            f"Group {int(gid):02d}  anchor_first_pitch={anchor_et} ET  (UTC {grp.get('start_time', '')})"
        )
        for pk in grp.get("game_pks") or []:
            row = pk_map.get(int(pk))
            if not row:
                lines.append(f"    game_pk={pk}  (not found in slate query)")
                continue
            away = row.get("away_abbr") or "?"
            home = row.get("home_abbr") or "?"
            gfp = _game_first_pitch_et(row)
            lines.append(f"    {away} @ {home}   first_pitch={gfp} ET   game_pk={pk}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _emit_group_slate_report(
    groups: List[Dict[str, Any]],
    games: List[Dict[str, Any]],
    *,
    game_date_et: str,
    group_window_min: int,
    report_file: str | None,
) -> None:
    text = _format_group_slate_report(
        groups,
        games,
        game_date_et=game_date_et,
        group_window_min=group_window_min,
    )
    print("\n" + "═" * 72)
    print("  SLATE / GROUP REPORT  (matchups + first pitch ET)")
    print("═" * 72)
    print(text, end="")
    if report_file:
        p = Path(report_file)
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(text, encoding="utf-8")
            print(f"[report] wrote {p.resolve()}")
        except Exception as e:
            print(f"[report] failed to write {report_file!r}: {e}", file=sys.stderr)


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
        cols = {r[1] for r in con.execute("PRAGMA table_info(pipeline_jobs)").fetchall()}
    except Exception:
        cols = set()
    try:
        sched_utc = _et_to_utc_iso_z(str(scheduled_time_et))
        if "scheduled_time" in cols:
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
        else:
            cur = con.execute(
                """
                INSERT OR IGNORE INTO pipeline_jobs
                    (job_type, job_date_et, scheduled_time_et, scheduled_time_utc,
                     window_start_et, window_end_et, status, game_group_id)
                VALUES (?,?,?,?,?,?,?,0)
                """,
                (
                    str(job_type),
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
    except Exception as exc:
        print(f"[schedule] WARNING _insert_global_job({job_type!s}): {exc}", file=sys.stderr)
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
              AND job_type IN ('stats_pull','load_today','load_weather','day_setup','early_peek','prior_report')
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
    g_ins += _insert_global_job(con, job_date_et=job_date_et, job_type="load_weather", scheduled_time_et=f"{job_date_et} 06:07 ET")
    g_ins += _insert_global_job(con, job_date_et=job_date_et, job_type="day_setup", scheduled_time_et=f"{job_date_et} 06:10 ET")
    g_ins += _insert_global_job(con, job_date_et=job_date_et, job_type="prior_report", scheduled_time_et=f"{job_date_et} 06:15 ET")
    g_ins += _insert_global_job(con, job_date_et=job_date_et, job_type="early_peek", scheduled_time_et=f"{job_date_et} 06:20 ET")
    return int(g_ins)


def _insert_schedule_next_day_globals_job(
    con: sqlite3.Connection, *, job_date_et: str, groups: List[Dict[str, Any]]
) -> tuple[int, int, str, str]:
    """
    One row at last group T0 + 5 min ET. Runner runs --globals-only for the *next* calendar day.
    Needed after both full-day scheduling and --groups-only (day_setup) so the evening hook
    is never omitted when globals were not inserted in the same run.
    """
    if not groups:
        return 0, 0, "", ""
    try:
        group_t0_by_id = {int(g["group_id"]): _parse_iso_z(str(g["start_time"])) for g in groups}
        last_t0_utc = max(group_t0_by_id.values())
        last_t0_et = last_t0_utc.replace(tzinfo=dt.timezone.utc).astimezone(_ET)
        sched_next_et = last_t0_et + dt.timedelta(minutes=5)
        sched_next_et_str = sched_next_et.strftime("%Y-%m-%d %H:%M ET")
        n_next = _insert_global_job(
            con,
            job_date_et=job_date_et,
            job_type="schedule_next_day_globals",
            scheduled_time_et=sched_next_et_str,
        )
        # Daily DB backup: run immediately after schedule_next_day_globals completes.
        sched_backup_et = sched_next_et + dt.timedelta(minutes=1)
        sched_backup_et_str = sched_backup_et.strftime("%Y-%m-%d %H:%M ET")
        n_backup = _insert_global_job(
            con,
            job_date_et=job_date_et,
            job_type="daily_backup",
            scheduled_time_et=sched_backup_et_str,
        )
        return int(n_next), int(n_backup), sched_next_et_str, sched_backup_et_str
    except Exception:
        return 0, 0, "", ""


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


def _window_offsets_for_odds_weather(
    job_type: str,
    *,
    odds_threshold_min: int,
    weather_min: int,
    brief_min: int,
    ledger_min: int,
) -> tuple[int | None, int | None]:
    """Return (window_start_offset_min, window_end_offset_min) from first pitch T0, or (None, None)."""
    th = int(odds_threshold_min)
    check_off = max(0, th - 5)
    w = int(weather_min)
    if job_type == "odds_pull":
        return th + 5, check_off
    if job_type == "odds_check":
        return th + 5, max(0, check_off - 5)
    if job_type == "weather":
        return w + 15, max(0, w - 15)
    if job_type == "group_brief":
        return _group_brief_window_offsets(int(brief_min), int(ledger_min))
    if job_type == "ledger_snapshot":
        return 30, 0
    return None, None


def _backfill_et_fields_for_existing_rows(
    con: sqlite3.Connection,
    *,
    game_date_et: str,
    group_t0_by_id: dict[int, dt.datetime],
    brief_min: int,
    ledger_min: int,
    odds_threshold_min: int = 90,
    weather_min: int = 45,
) -> int:
    """
    Backfill ET fields/windows for legacy rows that were inserted before ET columns existed,
    or fill missing window_start_et / window_end_et on existing rows (INSERT OR IGNORE leaves
    old NULL windows).
    Targets this date and the core job types (odds_pull, odds_check, weather, group_brief,
    ledger_snapshot).
    """
    try:
        cur = con.execute(
            """
            SELECT job_id, job_type, scheduled_time, game_group_id, scheduled_time_et,
                   window_start_et, window_end_et
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
        jid = r.get("job_id")
        gid = r.get("game_group_id")
        jt = str(r.get("job_type") or "")
        if jid is None:
            continue

        try:
            if gid is not None:
                t0 = group_t0_by_id.get(int(gid))
            else:
                t0 = None
        except Exception:
            t0 = None

        st_utc = r.get("scheduled_time")
        sched_et_existing = (r.get("scheduled_time_et") or "").strip()
        has_ws = bool((r.get("window_start_et") or "").strip())
        has_we = bool((r.get("window_end_et") or "").strip())

        # Nothing to backfill for this row
        if sched_et_existing and has_ws and has_we:
            continue

        win_start = None
        win_end = None
        if t0 is not None and jt in (
            "odds_pull",
            "odds_check",
            "weather",
            "group_brief",
            "ledger_snapshot",
        ):
            ws: int | None
            we: int | None
            if jt == "group_brief" and sched_et_existing:
                inferred_ok = False
                try:
                    sched_utc = _parse_iso_z(_et_to_utc_iso_z(sched_et_existing))
                    inferred = int(round((t0 - sched_utc).total_seconds() / 60.0))
                    if 0 < inferred <= 120:
                        ws, we = _group_brief_window_offsets(inferred, int(ledger_min))
                        inferred_ok = True
                except Exception:
                    pass
                if not inferred_ok:
                    ws, we = _window_offsets_for_odds_weather(
                        jt,
                        odds_threshold_min=odds_threshold_min,
                        weather_min=weather_min,
                        brief_min=brief_min,
                        ledger_min=ledger_min,
                    )
            else:
                ws, we = _window_offsets_for_odds_weather(
                    jt,
                    odds_threshold_min=odds_threshold_min,
                    weather_min=weather_min,
                    brief_min=brief_min,
                    ledger_min=ledger_min,
                )
            if ws is not None and we is not None:
                win_start = _fmt_et(t0 - dt.timedelta(minutes=int(ws)))
                win_end = _fmt_et(t0 - dt.timedelta(minutes=int(we)))

        try:
            # Rows that already have ET: only fill missing windows (INSERT OR IGNORE left old NULLs).
            if sched_et_existing and win_start and win_end and (not has_ws or not has_we):
                con.execute(
                    """
                    UPDATE pipeline_jobs
                    SET window_start_et = ?, window_end_et = ?
                    WHERE job_id = ?
                    """,
                    (win_start, win_end, int(jid)),
                )
                updated += 1
                continue

            # Legacy: derive ET from UTC scheduled_time
            if not sched_et_existing and st_utc:
                try:
                    sched_dt = _parse_iso_z(str(st_utc))
                    sched_et = _fmt_et(sched_dt)
                except Exception:
                    continue
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
                    (
                        game_date_et,
                        sched_et,
                        str(st_utc),
                        win_start,
                        win_end,
                        int(jid),
                    ),
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
    _reconfigure_stdio_utf8()
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
        help="Insert only per-group jobs for --date-et; skip morning globals (still inserts schedule_next_day_globals).",
    )
    p.add_argument("--group-window-min", type=int, default=30, help="Start-time grouping window (minutes, default 30)")
    p.add_argument("--odds-threshold-min", type=int, default=90, help="Odds pull threshold (minutes before group start, default 90)")
    p.add_argument("--odds-block-min", type=int, default=90, help="Merge adjacent groups into one odds block when within N minutes (default 90)")
    p.add_argument("--weather-min", type=int, default=45, help="Weather refresh scheduled at T0 - N minutes (default 45)")
    p.add_argument(
        "--brief-min",
        type=int,
        default=30,
        help="Primary group_brief at T0 - N minutes (default 30; must be > --ledger-min, typically <= 30 for bet window)",
    )
    p.add_argument(
        "--ledger-min",
        type=int,
        default=28,
        help="ledger_snapshot at T0 - N minutes (default 28; materialize bets in [T0-30m, T0) with run_pipeline timing)",
    )
    p.add_argument(
        "--brief-extra-minutes",
        default="15,5",
        help="Extra group_brief rows: comma-separated minutes before T0 (default 15,5). "
        "Use 'none' to disable. Each must be <= 30; duplicates vs --brief-min are skipped.",
    )
    p.add_argument("--dry-run", action="store_true", help="Compute + print, but do not write pipeline_jobs")
    p.add_argument(
        "--group-report",
        metavar="PATH",
        default=None,
        help="Write the slate/group report (matchups + first pitch ET) to this UTF-8 file; also printed to stdout.",
    )
    args = p.parse_args()

    bm = int(args.brief_min)
    lm = int(args.ledger_min)
    if bm <= lm:
        print(
            "error: --brief-min must be greater than --ledger-min "
            "(primary brief must run before ledger_snapshot in wall-clock order).",
            file=sys.stderr,
        )
        sys.exit(2)
    if bm > 30:
        print(
            f"[schedule] warning: --brief-min={bm} schedules primary group_brief more than 30 minutes before first pitch."
        )
    if lm > 30:
        print(
            f"[schedule] warning: --ledger-min={lm} is unusually early; bet materialization is normally within 30 minutes of T0."
        )

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
    _emit_group_slate_report(
        groups,
        games,
        game_date_et=game_date_et,
        group_window_min=int(args.group_window_min),
        report_file=str(args.group_report) if args.group_report else None,
    )

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
    else:
        print("\n[schedule] groups-only: skipping global daily jobs (stats_pull … early_peek)")

    # Prereq job dedupe: merge odds/weather/check jobs whose anchor first pitches are within 30 minutes.
    # window_start_et / window_end_et remain metadata only (not drivers of job creation).
    PREREQ_MERGE_MIN = 30
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
        if anchor_t0 is not None and (t0 - anchor_t0) <= dt.timedelta(minutes=PREREQ_MERGE_MIN):
            cur.append(g)
        else:
            odds_blocks.append(cur)
            cur = [g]
            anchor_t0 = t0
    if cur:
        odds_blocks.append(cur)

    rep_groups: list[dict[str, Any]] = []
    print(f"[odds_blocks] {len(odds_blocks)} block(s) (threshold={int(PREREQ_MERGE_MIN)}m)")
    for b in odds_blocks:
        rep = dict(b[0])
        rep["covered_group_ids"] = ",".join(str(int(x["group_id"])) for x in b)
        rep_groups.append(rep)
        gids = [int(x["group_id"]) for x in b]
        print(f"  - rep_group_id={int(rep['group_id'])} covers groups={gids}")

    # Cleanup: remove legacy odds_pull / weather rows for non-representative groups on this date.
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
        con.execute(
            f"""
            DELETE FROM pipeline_jobs
            WHERE job_type = 'weather'
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

    th = int(args.odds_threshold_min)
    check_off = max(0, th - 5)
    w_weather = int(args.weather_min)

    inserted_odds = _schedule_job_type(
        con,
        rep_groups,
        job_type="odds_pull",
        offset_minutes=th,
        status="pending",
        window_start_offset_min=th + 5,
        window_end_offset_min=check_off,
    )
    print(f"[schedule] inserted odds_pull (by block): {inserted_odds}")

    inserted_odds_check = _schedule_job_type(
        con,
        rep_groups,
        job_type="odds_check",
        offset_minutes=check_off,
        status="pending",
        window_start_offset_min=th + 5,
        window_end_offset_min=max(0, check_off - 5),
    )
    print(f"[schedule] inserted odds_check (by block): {inserted_odds_check}")

    inserted_weather = _schedule_job_type(
        con,
        rep_groups,
        job_type="weather",
        offset_minutes=w_weather,
        status="pending",
        window_start_offset_min=w_weather + 15,
        window_end_offset_min=max(0, w_weather - 15),
    )
    print(f"[schedule] inserted weather (by block): {inserted_weather}")

    # group_brief: EXACTLY one job per group at (anchor_first_pitch - 30 minutes).
    # window_start_et / window_end_et remain metadata only (not drivers of job creation).
    PRIMARY_BRIEF_MIN = 30
    b_ws, b_we = _group_brief_window_offsets(PRIMARY_BRIEF_MIN, int(args.ledger_min))
    inserted_brief = _schedule_job_type(
        con,
        groups,
        job_type="group_brief",
        offset_minutes=PRIMARY_BRIEF_MIN,
        status="pending",
        window_start_offset_min=b_ws,
        window_end_offset_min=b_we,
    )
    print(f"[schedule] inserted group_brief (T0-{PRIMARY_BRIEF_MIN}m): {inserted_brief}")

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

    # Pre-seed next calendar day's group-0 globals (evening). Same row whether we came from full day or --groups-only.
    try:
        inserted_next, inserted_backup, sched_next_et_str, sched_backup_et_str = _insert_schedule_next_day_globals_job(
            con, job_date_et=game_date_et, groups=groups
        )
        print(
            f"[schedule] inserted schedule_next_day_globals: {inserted_next}"
            + (f" at {sched_next_et_str}" if sched_next_et_str else "")
        )
        print(
            f"[schedule] inserted daily_backup: {inserted_backup}"
            + (f" at {sched_backup_et_str}" if sched_backup_et_str else "")
        )
    except Exception as exc:
        print(f"[schedule] failed to insert schedule_next_day_globals: {exc}")

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
        odds_threshold_min=int(args.odds_threshold_min),
        weather_min=int(args.weather_min),
    )
    if backfilled:
        print(f"[migrate] backfilled ET/window fields on {backfilled} existing row(s)")

    _print_job_counts(con, game_date_et)
    _print_jobs_for_date(con, game_date_et)
    con.close()
    print("\nDone.")


if __name__ == "__main__":
    main()

