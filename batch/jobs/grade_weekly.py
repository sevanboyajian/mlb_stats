#!/usr/bin/env python3
"""
grade_weekly.py
───────────────
Monday AM weekly signal performance summary email.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
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

from core.db.connection import connect as db_connect, get_db_path
from batch.pipeline.grade_ledger import build_weekly_signal_report
from batch.jobs.grade_daily import send_grading_email


def run(
    as_of_date: str,
    *,
    db_path: str | None = None,
    send_email: bool = True,
) -> dict:
    conn = db_connect(db_path or get_db_path(), timeout=30)
    try:
        body = build_weekly_signal_report(conn, as_of_date)
    finally:
        conn.close()

    out_dir = _REPO_ROOT / "outputs" / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / f"grading-weekly-{as_of_date}.txt"
    report_path.write_text(body, encoding="utf-8")

    subject = f"MLB Scout · Weekly Signal Summary · through {as_of_date}"
    result = {
        "as_of_date": as_of_date,
        "report_path": str(report_path),
        "email_sent": False,
        "email_message": "skipped",
    }

    if send_email:
        ok, msg = send_grading_email(subject, body, report_path=report_path)
        result["email_sent"] = ok
        result["email_message"] = msg
        if ok:
            logging.info("[grade_weekly] email sent: %s", msg)
        else:
            logging.warning("[grade_weekly] email failed (non-fatal): %s", msg)

    result["email_subject"] = subject
    result["email_body"] = body
    return result


def main() -> int:
    p = argparse.ArgumentParser(description="Weekly signal performance summary.")
    p.add_argument(
        "--date",
        default=None,
        metavar="YYYY-MM-DD",
        help="As-of date (default: yesterday ET).",
    )
    p.add_argument("--no-email", action="store_true", help="Build report only; do not email.")
    args = p.parse_args()

    if args.date:
        as_of = args.date
    else:
        try:
            from zoneinfo import ZoneInfo

            as_of = (date.today() - timedelta(days=1)).isoformat()
            _ = ZoneInfo("America/New_York")
        except Exception:
            as_of = (date.today() - timedelta(days=1)).isoformat()

    summary = run(as_of, send_email=not args.no_email)
    print(f"[grade_weekly] through {summary['as_of_date']}  report={summary['report_path']}")
    if summary.get("email_sent"):
        print(f"[grade_weekly] email: {summary['email_message']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
