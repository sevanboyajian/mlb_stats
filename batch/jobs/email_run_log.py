#!/usr/bin/env python3
"""
email_run_log.py
────────────────
Email the run_pipeline log file as a text attachment.

This is intentionally dumb: no pipeline business logic. It just attaches the log.

Env:
  BRIEF_EMAIL_TO (or REPORT_EMAIL_TO): recipients (comma-separated). If unset, exits 0 (no-op).
  SMTP_* as used by delivery.email_sender (Gmail App Password recommended).
"""

from __future__ import annotations

import argparse
import os
import sys
import shutil
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(_REPO_ROOT / "config" / ".env", override=False)
    load_dotenv(_REPO_ROOT / ".env", override=False)
    load_dotenv(override=False)
except ImportError:
    pass

from delivery.email_sender import send_report_email


def main() -> int:
    p = argparse.ArgumentParser(description="Email run_pipeline log file as attachment.")
    p.add_argument("--date", default=None, metavar="YYYY-MM-DD", help="Slate date used in log filename.")
    p.add_argument("--kind", required=True, choices=["morning", "eod"], help="Which phase label to use in subject.")
    args = p.parse_args()

    to_raw = (os.getenv("BRIEF_EMAIL_TO") or os.getenv("REPORT_EMAIL_TO") or "").strip()
    if not to_raw:
        # No-op by default (safe for automation)
        return 0

    jd = (str(args.date).strip() if args.date else "")
    if not jd:
        # Best-effort: today in local date; for automation we expect --date.
        import datetime as dt

        jd = dt.date.today().isoformat()

    base_log_path = (_REPO_ROOT / "logs" / f"run_pipeline_{jd}.txt").resolve()
    if not base_log_path.is_file():
        ok, msg = send_report_email(
            None,
            f"MLB pipeline run log ({args.kind}) — {jd} (missing)",
            to_raw,
            body=f"Expected log file not found:\n{base_log_path}\n",
        )
        # Still non-fatal for the pipeline job; return success even if email fails.
        return 0 if ok else 0

    # Attach a phase-specific snapshot so filenames clearly differentiate morning vs eod.
    # Keep the snapshot file on disk (useful for debugging when an email is missing).
    snap_log_path = (_REPO_ROOT / "logs" / f"run_pipeline_{jd}_{args.kind}.txt").resolve()
    try:
        shutil.copyfile(str(base_log_path), str(snap_log_path))
    except Exception:
        # If copy fails (file locked, permissions), fall back to attaching the base log.
        snap_log_path = base_log_path

    subject = f"MLB pipeline run log ({args.kind}) — {jd}"
    body = f"Attached: {snap_log_path.name}\nPath: {snap_log_path}\n"
    ok, msg = send_report_email(str(snap_log_path), subject, to_raw, body=body)
    print(f"[email_run_log] {'OK' if ok else 'WARN'} {msg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

