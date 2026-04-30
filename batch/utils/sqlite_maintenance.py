#!/usr/bin/env python3
"""
sqlite_maintenance.py
──────────────────────
Low-risk SQLite housekeeping for ``mlb_stats.db``.

What it does
────────────
- ``PRAGMA optimize`` — recommended periodic maintenance (updates internals; cheap).
- Optional ``VACUUM`` — rewrites the DB file to reclaim space from freed pages.
  Use sparingly on large WAL DBs (can take minutes and needs brief exclusive access).

Typical weekly run (recommended)
────────────────────────────────
    python batch/utils/sqlite_maintenance.py

Weekly run with conditional vacuum (only if freelist waste is large)
────────────────────────────────────────────────────────────────────
    python batch/utils/sqlite_maintenance.py --vacuum-if-needed --min-free-mb 128

Scheduling (Windows Task Scheduler example)
───────────────────────────────────────────
Create a weekly task pointing at your repo root, e.g.::

    python C:\\Users\\you\\path\\mlb_stats\\batch\\utils\\sqlite_maintenance.py

Run off-peak and avoid overlapping ``run_pipeline``, Streamlit Scout, or backup jobs.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.db.connection import connect as db_connect, get_db_path
from core.utils.base_dir import get_base_dir

LOG_PATH = get_base_dir() / "logs" / "sqlite_maintenance.log"


def _reconfigure_stdio_utf8() -> None:
    for stream in (sys.stdout, sys.stderr):
        try:
            if hasattr(stream, "reconfigure"):
                stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def _ts_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(msg: str) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(f"{_ts_utc()} {msg}\n")


def _db_stats(con: sqlite3.Connection) -> tuple[int, int, int, float]:
    """page_count, freelist_count, page_size, free_bytes."""
    pc = int(con.execute("PRAGMA page_count").fetchone()[0])
    fc = int(con.execute("PRAGMA freelist_count").fetchone()[0])
    ps = int(con.execute("PRAGMA page_size").fetchone()[0])
    free_b = float(fc * ps)
    return pc, fc, ps, free_b


def main() -> int:
    _reconfigure_stdio_utf8()

    p = argparse.ArgumentParser(
        description="SQLite maintenance: PRAGMA optimize and optional VACUUM.",
    )
    p.add_argument(
        "--db",
        default=None,
        help=f"Database path (default: get_db_path() → typically data/mlb_stats.db)",
    )
    p.add_argument(
        "--optimize",
        dest="optimize",
        action="store_true",
        default=True,
        help="Run PRAGMA optimize (default: on)",
    )
    p.add_argument(
        "--no-optimize",
        dest="optimize",
        action="store_false",
        help="Skip PRAGMA optimize",
    )
    p.add_argument(
        "--vacuum",
        action="store_true",
        help="Always run VACUUM after optimize (heavy; use when you know you need it)",
    )
    p.add_argument(
        "--vacuum-if-needed",
        action="store_true",
        help="Run VACUUM only when freelist waste exceeds thresholds",
    )
    p.add_argument(
        "--min-free-mb",
        type=float,
        default=0.0,
        metavar="MB",
        help="With --vacuum-if-needed: vacuum if freelist_bytes >= MB (0 = ignore)",
    )
    p.add_argument(
        "--min-free-pct",
        type=float,
        default=0.0,
        metavar="PCT",
        help="With --vacuum-if-needed: vacuum if freelist/pages >= PCT/100 (0 = ignore)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would happen; do not write or optimize",
    )
    args = p.parse_args()

    db_path = Path(args.db).resolve() if args.db else Path(get_db_path()).resolve()
    if not db_path.is_file():
        print(f"ERROR: database not found: {db_path}", file=sys.stderr)
        return 2

    if args.vacuum and args.vacuum_if_needed:
        print("ERROR: use only one of --vacuum or --vacuum-if-needed", file=sys.stderr)
        return 2

    if args.vacuum_if_needed and args.min_free_mb <= 0 and args.min_free_pct <= 0:
        print(
            "ERROR: --vacuum-if-needed requires --min-free-mb > 0 and/or --min-free-pct > 0",
            file=sys.stderr,
        )
        return 2

    print(f"[sqlite_maintenance] db={db_path}")
    _log(f"start db={db_path} optimize={args.optimize} vacuum={args.vacuum} vacuum_if_needed={args.vacuum_if_needed}")

    if args.dry_run:
        print("[sqlite_maintenance] dry-run — no changes")
        _log("dry-run exit")
        return 0

    t0 = time.perf_counter()
    con = db_connect(str(db_path), timeout=60)
    try:
        pc, fc, ps, free_b = _db_stats(con)
        free_mb = free_b / (1024.0 * 1024.0)
        pct = (100.0 * fc / pc) if pc else 0.0
        print(
            f"[sqlite_maintenance] pages={pc} freelist_pages={fc} page_size={ps} "
            f"freelist≈{free_mb:.1f} MiB ({pct:.2f}% of pages)"
        )
        _log(f"stats pages={pc} freelist={fc} free_mb={free_mb:.2f} pct={pct:.3f}")

        if args.optimize:
            con.execute("PRAGMA optimize")
            con.commit()
            print("[sqlite_maintenance] PRAGMA optimize — done")
            _log("PRAGMA optimize ok")

        do_vacuum = False
        if args.vacuum:
            do_vacuum = True
        elif args.vacuum_if_needed:
            if args.min_free_mb > 0 and free_mb >= args.min_free_mb:
                do_vacuum = True
                print(
                    f"[sqlite_maintenance] vacuum trigger: freelist >= {args.min_free_mb} MiB "
                    f"(actual {free_mb:.1f})"
                )
            if args.min_free_pct > 0 and pct >= args.min_free_pct:
                do_vacuum = True
                print(
                    f"[sqlite_maintenance] vacuum trigger: freelist >= {args.min_free_pct}% of pages "
                    f"(actual {pct:.2f}%)"
                )
            if not do_vacuum:
                print("[sqlite_maintenance] VACUUM skipped — below thresholds")

        if do_vacuum:
            print("[sqlite_maintenance] VACUUM — starting (may take several minutes)...")
            _log("VACUUM start")
            t_v = time.perf_counter()
            con.execute("VACUUM")
            con.commit()
            print(f"[sqlite_maintenance] VACUUM — done in {time.perf_counter() - t_v:.1f}s")
            _log(f"VACUUM ok elapsed_s={time.perf_counter() - t_v:.1f}")
            pc2, fc2, ps2, free_b2 = _db_stats(con)
            print(
                f"[sqlite_maintenance] after VACUUM: pages={pc2} freelist_pages={fc2} "
                f"freelist≈{free_b2 / (1024*1024):.1f} MiB"
            )
            _log(f"after_vacuum pages={pc2} freelist={fc2}")
    finally:
        con.close()

    elapsed = time.perf_counter() - t0
    print(f"[sqlite_maintenance] finished in {elapsed:.1f}s")
    _log(f"finish elapsed_s={elapsed:.1f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
