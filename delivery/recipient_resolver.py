"""
recipient_resolver.py
─────────────────────
Resolve email recipients from SQLite (users + user_subscriptions).

Rules:
- Always include active admins (role='admin', is_active=1) even if not subscribed.
- Also include active users who are enabled for the given subscription_type.
- Never return duplicates (case-insensitive).

No email sending logic lives here — this module only returns recipient addresses.
"""

from __future__ import annotations

import sqlite3

from core.db.connection import get_db_path


def _dedupe_emails(emails: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in emails:
        e = str(raw or "").strip()
        if not e:
            continue
        k = e.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(e)
    return out


def get_recipients(subscription_type: str, *, db_path: str | None = None) -> list[str]:
    """
    Return a list of recipient email addresses for a subscription type.

    Logic:
    - Always include: users.role='admin' AND users.is_active=1
    - Also include: users.is_active=1 joined to user_subscriptions where:
        - subscription_type matches
        - is_enabled=1
    """
    st = (subscription_type or "").strip()
    if not st:
        return []

    con = sqlite3.connect(db_path or get_db_path())
    try:
        con.row_factory = sqlite3.Row

        admin_rows = con.execute(
            "SELECT email FROM users WHERE is_active=1 AND role='admin' ORDER BY email"
        ).fetchall()
        admins = [r["email"] for r in admin_rows if r and r["email"]]

        sub_rows = con.execute(
            """
            SELECT u.email
            FROM users u
            JOIN user_subscriptions s ON s.user_id = u.user_id
            WHERE u.is_active = 1
              AND s.is_enabled = 1
              AND s.subscription_type = ?
            ORDER BY u.email
            """,
            (st,),
        ).fetchall()
        subs = [r["email"] for r in sub_rows if r and r["email"]]

        return _dedupe_emails(admins + subs)
    finally:
        con.close()

