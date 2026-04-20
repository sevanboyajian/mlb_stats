"""
SMTP email helper for reports. No business / picks logic.

Environment (with fallbacks for existing BRIEF_* names):
  SMTP_HOST, SMTP_PORT (default 587)
  SMTP_USER, SMTP_PASSWORD
  SMTP_FROM (optional; defaults to SMTP_USER)

  Fallbacks if SMTP_* unset: BRIEF_SMTP_HOST, BRIEF_SMTP_PORT, BRIEF_SMTP_USER,
  BRIEF_SMTP_PASSWORD, BRIEF_EMAIL_FROM
"""

from __future__ import annotations

import os
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Sequence


def _env(name: str, fallback: str = "") -> str:
    v = os.getenv(name)
    if v is not None and str(v).strip():
        return str(v).strip()
    return fallback


def _smtp_settings() -> tuple[str, int, str, str, str]:
    host = _env("SMTP_HOST") or _env("BRIEF_SMTP_HOST", "smtp.gmail.com")
    port_s = _env("SMTP_PORT") or _env("BRIEF_SMTP_PORT", "587")
    try:
        port = int(port_s)
    except ValueError:
        port = 587
    user = _env("SMTP_USER") or _env("BRIEF_SMTP_USER", "mlb.stats.sender@gmail.com")
    password = _env("SMTP_PASSWORD") or _env("BRIEF_SMTP_PASSWORD", "")
    mail_from = _env("SMTP_FROM") or _env("BRIEF_EMAIL_FROM") or user
    return host, port, user, password, mail_from


def _normalize_recipients(recipients: str | Sequence[str]) -> list[str]:
    if isinstance(recipients, str):
        return [p.strip() for p in recipients.replace(";", ",").split(",") if p.strip()]
    return [str(r).strip() for r in recipients if str(r).strip()]


def send_report_email(
    report_path: str | Path | None,
    subject: str,
    recipients: str | Sequence[str],
    *,
    body: str | None = None,
    body_html: str | None = None,
    attachment_path: str | Path | None = None,
) -> tuple[bool, str]:
    """
    Send an email with optional plain and/or HTML body and optional attachment.

    ``report_path`` is attached when it points to an existing file. If
    ``attachment_path`` is set, it is attached instead (or in addition is not
    supported — prefer one attachment). If only ``attachment_path`` is set, it
    is used as the attachment.

    Returns (success, human-readable message). Does not raise on SMTP errors.
    """
    to_list = _normalize_recipients(recipients)
    if not to_list:
        return False, "no recipients"

    host, port, user, password, mail_from = _smtp_settings()
    if not password:
        return False, "SMTP_PASSWORD (or BRIEF_SMTP_PASSWORD) not set"

    attach_p: Path | None = None
    if attachment_path:
        attach_p = Path(attachment_path)
    elif report_path:
        rp = Path(report_path)
        if rp.is_file():
            attach_p = rp
    has_attach = attach_p is not None and attach_p.is_file()

    plain = (body or "").strip() or None
    html = (body_html or "").strip() or None
    if plain is None and html is None:
        plain = "See attached report."

    msg: MIMEMultipart
    if html and plain:
        msg = MIMEMultipart("mixed")
        alt = MIMEMultipart("alternative")
        alt.attach(MIMEText(plain, "plain", "utf-8"))
        alt.attach(MIMEText(html, "html", "utf-8"))
        msg.attach(alt)
    elif html:
        msg = MIMEMultipart("mixed")
        msg.attach(MIMEText(html, "html", "utf-8"))
    else:
        msg = MIMEMultipart("mixed")
        msg.attach(MIMEText(plain or "", "plain", "utf-8"))

    msg["Subject"] = str(subject or "Report")
    msg["From"] = mail_from
    msg["To"] = ", ".join(to_list)

    if has_attach and attach_p is not None:
        data = attach_p.read_bytes()
        part = MIMEApplication(data, _subtype="octet-stream")
        part.add_header(
            "Content-Type",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )
        part.add_header("Content-Disposition", "attachment", filename=attach_p.name)
        msg.attach(part)

    try:
        if port == 465:
            with smtplib.SMTP_SSL(host, port, timeout=120) as smtp:
                smtp.login(user, password)
                smtp.send_message(msg)
        else:
            with smtplib.SMTP(host, port, timeout=120) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.ehlo()
                smtp.login(user, password)
                smtp.send_message(msg)
    except Exception as exc:
        return False, f"send failed: {exc!s}"

    att = attach_p.name if has_attach else "no attachment"
    return True, f"sent to {', '.join(to_list)} ({att})"
