#!/usr/bin/env python3
"""
Remove duplicate rows from pipeline_jobs that share the same logical job key.

Why duplicates happen
─────────────────────
- The unique index is (job_type, scheduled_time_et, game_group_id). In SQLite,
  each NULL ``game_group_id`` counts as distinct from another NULL, so legacy
  rows without ``game_group_id=0`` can duplicate globals.
- Re-scheduling before the index existed, or subtle ``scheduled_time_et`` drift.

Keeper policy
─────────────
For each duplicate group, keep one row: highest status priority
(complete > failed/timeout > running > pending), then lowest ``job_id``.
Optional: delete orphan ``pipeline_job_runs`` rows for removed ``job_id``s.

Usage
-----
  python diagnostics/dedupe_pipeline_jobs.py --dry-run
  python diagnostics/dedupe_pipeline_jobs.py --execute
  python diagnostics/dedupe_pipeline_jobs.py --execute --date-et 2026-04-19
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import connect as db_connect, get_db_path


def _norm(s: object) -> str:
    return " ".join(str(s or "").strip().split())


def _status_rank(status: object) -> int:
    s = str(status or "").strip().lower()
    return {
        "complete": 50,
        "failed": 40,
        "timeout": 40,
        "running": 20,
        "pending": 10,
    }.get(s, 0)


def _dedupe_key(row: sqlite3.Row) -> tuple:
    gid = row["game_group_id"]
    try:
        g = int(gid) if gid is not None else None
    except (TypeError, ValueError):
        g = None
    return (
        _norm(row["job_type"]),
        _norm(row["job_date_et"]),
        _norm(row["scheduled_time_et"]),
        -1 if g is None else g,
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Deduplicate pipeline_jobs rows.")
    p.add_argument("--dry-run", action="store_true", help="Print plan only (default if no --execute)")
    p.add_argument("--execute", action="store_true", help="Apply deletes")
    p.add_argument("--date-et", default=None, help="Only rows with this job_date_et (YYYY-MM-DD)")
    p.add_argument(
        "--purge-orphan-runs",
        action="store_true",
        help="Delete pipeline_job_runs whose job_id was removed",
    )
    args = p.parse_args()
    dry = not args.execute

    db = get_db_path()
    con = db_connect(db, timeout=30)
    con.row_factory = sqlite3.Row

    try:
        con.execute("SELECT 1 FROM pipeline_jobs LIMIT 1")
    except Exception as e:
        print(f"error: pipeline_jobs: {e}", file=sys.stderr)
        sys.exit(2)

    where = "1=1"
    params: list[object] = []
    if args.date_et:
        where = "job_date_et = ?"
        params.append(str(args.date_et).strip())

    cur = con.execute(
        f"SELECT * FROM pipeline_jobs WHERE {where} ORDER BY job_id",
        params,
    )
    rows = cur.fetchall()

    buckets: dict[tuple, list[sqlite3.Row]] = defaultdict(list)
    for r in rows:
        buckets[_dedupe_key(r)].append(r)

    dup_sets = {k: v for k, v in buckets.items() if len(v) > 1}
    if not dup_sets:
        print(f"[dedupe] no duplicate keys found ({len(rows)} row(s) scanned). db={db}")
        con.close()
        return

    to_delete: list[int] = []
    for key, group in sorted(dup_sets.items(), key=lambda x: x[0]):
        ranked = sorted(
            group,
            key=lambda r: (-_status_rank(r["status"]), int(r["job_id"])),
        )
        keeper = ranked[0]
        victims = ranked[1:]
        print(
            f"\n[key] {key}  — {len(group)} rows; "
            f"keep job_id={keeper['job_id']} status={keeper['status']!r}"
        )
        for r in victims:
            print(
                f"    DELETE job_id={r['job_id']} status={r['status']!r} "
                f"type={r['job_type']!r} time={r['scheduled_time_et']!r}"
            )
            to_delete.append(int(r["job_id"]))

    print(f"\n[dedupe] summary: {len(to_delete)} row(s) to delete, db={db}")

    if dry:
        print("[dedupe] dry-run — no changes. Pass --execute to apply.")
        con.close()
        return

    try:
        con.execute("BEGIN IMMEDIATE")
        for jid in to_delete:
            con.execute("DELETE FROM pipeline_jobs WHERE job_id = ?", (jid,))
        if args.purge_orphan_runs and to_delete:
            con.executemany(
                "DELETE FROM pipeline_job_runs WHERE job_id = ?",
                [(jid,) for jid in to_delete],
            )
        con.commit()
        print(f"[dedupe] deleted {len(to_delete)} pipeline_jobs row(s).")
        if args.purge_orphan_runs:
            print("[dedupe] purged pipeline_job_runs for those job_ids.")
    except Exception as e:
        con.rollback()
        print(f"[dedupe] error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        con.close()


if __name__ == "__main__":
    main()
