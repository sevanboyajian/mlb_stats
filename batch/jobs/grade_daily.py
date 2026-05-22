#!/usr/bin/env python3
"""
grade_daily.py
──────────────
Standalone daily grading job: grade bet_ledger, write grading_log, email report.
"""

from __future__ import annotations

import argparse
import logging
import sys
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
from batch.pipeline.grade_ledger import (
    format_email_report,
    run_daily_grading,
    write_grading_report_file,
)


def send_grading_email(
    subject: str,
    body: str,
    *,
    report_path: Path | None = None,
    subscription_type: str = "group_brief",
) -> tuple[bool, str]:
    """Send grading report via the same mechanism as brief emails. Non-fatal."""
    try:
        from delivery.recipient_resolver import get_recipients
        from delivery.email_sender import send_report_email

        recipients = get_recipients(subscription_type)
        if not recipients:
            return False, f"no recipients for subscription_type={subscription_type!r}"
        attach = str(report_path) if report_path and report_path.is_file() else None
        return send_report_email(attach, subject, recipients, body=body)
    except Exception as exc:
        return False, str(exc)


def run(
    date: str,
    *,
    db_path: str | None = None,
    trigger: str = "scheduled_job",
    sync_odds: bool = True,
    retry_ungraded: bool = True,
    send_email: bool = False,
    dry_run: bool = False,
) -> dict:
    """
    Programmatic entry point for pipeline runner and tests.

    ``date`` is the slate ``game_date_et`` to grade (typically yesterday relative to
    the pipeline ``job_date_et`` row).
    """
    conn = db_connect(db_path or get_db_path(), timeout=30)
    try:
        summary = run_daily_grading(
            conn,
            date,
            trigger=trigger,
            sync_odds=sync_odds,
            retry_ungraded=retry_ungraded,
        )
    finally:
        conn.close()

    signal_performance = summary.get("signal_performance") or {}
    subject, body = format_email_report(summary, signal_performance, date)

    if not dry_run:
        report_path = write_grading_report_file(date, body, _REPO_ROOT)
        summary["report_path"] = str(report_path)
    else:
        report_path = None
        summary["report_path"] = None

    if send_email and not dry_run:
        ok, msg = send_grading_email(subject, body, report_path=report_path)
        summary["email_sent"] = ok
        summary["email_message"] = msg
        if ok:
            logging.info("[grade_daily] email sent: %s", msg)
        else:
            logging.warning("[grade_daily] email failed (non-fatal): %s", msg)
    else:
        summary["email_sent"] = False
        summary["email_message"] = "skipped"

    summary["email_subject"] = subject
    summary["email_body"] = body
    return summary


def _print_summary(summary: dict) -> None:
    err = summary.get("error_message")
    if err:
        print(f"[grade_daily] ERROR: {err}", file=sys.stderr)
    print(
        f"[grade_daily] {summary['game_date']}  graded={summary['rows_graded']}  "
        f"attempted={summary['rows_attempted']}  "
        f"W-L-P={summary['wins']}-{summary['losses']}-{summary['pushes']}  "
        f"pnl={summary['pnl_units']:+.4f}u  "
        f"alerts={summary.get('alert_count', 0)}  "
        f"ungraded_after={summary['ungraded_after']}  "
        f"log_id={summary['grading_log_id']}"
    )
    if summary.get("report_path"):
        print(f"[grade_daily] report: {summary['report_path']}")
    if summary.get("email_sent"):
        print(f"[grade_daily] email: {summary.get('email_message')}")
    elif summary.get("send_email_requested"):
        print(f"[grade_daily] email skipped/failed: {summary.get('email_message')}")


def main() -> int:
    p = argparse.ArgumentParser(description="Grade bet_ledger for a slate date.")
    p.add_argument(
        "--date",
        required=True,
        metavar="YYYY-MM-DD",
        help="Slate date (game_date_et) to grade.",
    )
    p.add_argument(
        "--trigger",
        default="cli",
        help="grading_log trigger label (default: cli). Pipeline uses scheduled_job.",
    )
    p.add_argument(
        "--no-sync-odds",
        action="store_true",
        help="Skip bet_ledger odds sync from first-fire bet_snapshots.",
    )
    p.add_argument(
        "--no-retry",
        action="store_true",
        help="Do not retry grading when Final games remain ungraded.",
    )
    p.add_argument(
        "--no-email",
        action="store_true",
        help="Do not send the grading report email.",
    )
    p.add_argument(
        "--email",
        action="store_true",
        help="Force email on manual runs (pipeline scheduled_job sends by default).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Grade and build report context but do not write report file or email.",
    )
    args = p.parse_args()

    trigger = str(args.trigger)
    should_email = False
    if not args.dry_run:
        if args.email:
            should_email = True
        elif trigger == "scheduled_job" and not args.no_email:
            should_email = True

    summary = run(
        args.date,
        trigger=trigger,
        sync_odds=not args.no_sync_odds,
        retry_ungraded=not args.no_retry,
        send_email=should_email,
        dry_run=args.dry_run,
    )
    summary["send_email_requested"] = should_email
    _print_summary(summary)
    return 1 if summary.get("error_message") else 0


if __name__ == "__main__":
    raise SystemExit(main())
