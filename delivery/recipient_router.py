"""
recipient_router.py
───────────────────
Resolve email recipients from the SQLite DB (users + user_subscriptions).

This lets us control email delivery by subscription_type instead of hardcoding
BRIEF_EMAIL_TO everywhere.

Rules:
- only users.is_active=1
- only user_subscriptions.is_enabled=1
"""

from __future__ import annotations

import sqlite3
from typing import Iterable

from core.db.connection import get_db_path


def _tables_exist(con: sqlite3.Connection) -> bool:
    rows = con.execute(
        "SELECT name FROM sqlite_master WHERE type=? AND name IN (?,?)",
        ("table", "users", "user_subscriptions"),
    ).fetchall()
    have = {r[0] for r in rows}
    return "users" in have and "user_subscriptions" in have


def recipients_for_subscription(subscription_type: str, *, db_path: str | None = None) -> list[str]:
    """
    Return recipient email addresses for a subscription type.

    If tables are missing or empty, returns [].
    """
    st = (subscription_type or "").strip()
    if not st:
        return []

    con = sqlite3.connect(db_path or get_db_path())
    try:
        con.row_factory = sqlite3.Row
        try:
            con.execute("PRAGMA foreign_keys = ON")
        except Exception:
            pass

        if not _tables_exist(con):
            return []

        rows = con.execute(
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
        return [str(r["email"]).strip() for r in rows if str(r["email"]).strip()]
    finally:
        con.close()


def recipients_csv(subscription_type: str, *, db_path: str | None = None) -> str:
    """Convenience: return recipients as comma-separated string."""
    return ", ".join(recipients_for_subscription(subscription_type, db_path=db_path))


def merge_recipients(*groups: Iterable[str]) -> list[str]:
    """Unique, stable-order merge of multiple recipient lists."""
    out: list[str] = []
    seen: set[str] = set()
    for g in groups:
        for raw in g:
            e = str(raw or "").strip()
            if not e:
                continue
            k = e.lower()
            if k in seen:
                continue
            seen.add(k)
            out.append(e)
    return out

