#!/usr/bin/env python3
"""Simulate a full day of generate_daily_brief runs using fixed ET --as-of times."""

import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

BATCH_DIR = Path(__file__).resolve().parent.parent
GENERATE = BATCH_DIR / "pipeline" / "generate_daily_brief.py"

# (session, HH:MM) — wall clock in America/New_York; combined with --date for --as-of
SCHEDULE = [
    ("morning", "09:30"),
    ("early", "12:15"),
    ("afternoon", "15:45"),
    ("primary", "17:30"),
    ("closing", "18:45"),
    ("late", "20:15"),
]


def main() -> None:
    if len(sys.argv) >= 2:
        day = sys.argv[1].strip()
    else:
        day = date.today().isoformat()
    try:
        datetime.strptime(day, "%Y-%m-%d")
    except ValueError:
        print(f"Usage: {sys.argv[0]} [YYYY-MM-DD]", file=sys.stderr)
        sys.exit(2)

    if not GENERATE.is_file():
        print(f"Cannot find {GENERATE}", file=sys.stderr)
        sys.exit(2)

    results: list[tuple[str, str, bool]] = []

    for session, hm in SCHEDULE:
        as_of = f"{day} {hm}"
        cmd = [
            sys.executable,
            str(GENERATE),
            "--session",
            session,
            "--date",
            day,
            "--as-of",
            as_of,
            "--force",
        ]
        print(f"\n=== Running session={session!r}  --as-of {as_of!r} ===\n", flush=True)
        proc = subprocess.run(cmd, cwd=str(BATCH_DIR / "pipeline"))
        ok = proc.returncode == 0
        results.append((session, as_of, ok))
        label = "success" if ok else f"failure (exit {proc.returncode})"
        print(f"--- {session}: {label} ---\n", flush=True)

    print("Summary:")
    for session, as_of, ok in results:
        mark = "OK " if ok else "FAIL"
        print(f"  [{mark}] {session:12}  {as_of}")

    if not all(ok for _, _, ok in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
