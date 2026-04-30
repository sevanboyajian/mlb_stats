#!/usr/bin/env python3
"""
seed_users.py
─────────────
Idempotently seed initial users + email subscriptions into SQLite.

Seeds:
- Admin user (defaults to BRIEF_EMAIL_TO from config/.env; role='admin')
- Test user (default email configurable; role='user')

Subscriptions:
- admin → group_brief, system_alert, admin_report
- user  → group_brief

Safe to run multiple times (no duplicate users; no duplicate subscriptions).
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.db.connection import get_db_path


def _load_env_var_from_file(path: Path, key: str) -> str | None:
    """Minimal dotenv reader (no external deps)."""
    if not path.is_file():
        return None
    try:
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if k.strip() != key:
                continue
            return v.strip().strip('"').strip("'")
    except Exception:
        return None
    return None


def _default_admin_email() -> str | None:
    # Prefer process env, fall back to config/.env, then repo .env.
    env_val = (os.getenv("BRIEF_EMAIL_TO") or "").strip()
    if env_val:
        return env_val
    for p in (REPO_ROOT / "config" / ".env", REPO_ROOT / ".env"):
        v = _load_env_var_from_file(p, "BRIEF_EMAIL_TO")
        if v:
            return v
    return None


def _ensure_tables_exist(con: sqlite3.Connection) -> None:
    needed = {"users", "user_subscriptions"}
    rows = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name IN (?,?)",
        ("users", "user_subscriptions"),
    ).fetchall()
    have = {r[0] for r in rows}
    missing = sorted(needed - have)
    if missing:
        raise RuntimeError(
            "Missing required table(s): "
            + ", ".join(missing)
            + ". Apply core/db/schema.sql first."
        )


def _ensure_user_name_columns(con: sqlite3.Connection) -> None:
    """
    Best-effort, idempotent migration for older DBs: add first_name/last_name columns.
    """
    try:
        cols = {r[1] for r in con.execute("PRAGMA table_info(users)").fetchall()}
    except Exception:
        return
    if "first_name" not in cols:
        try:
            con.execute("ALTER TABLE users ADD COLUMN first_name TEXT")
        except Exception:
            pass
    if "last_name" not in cols:
        try:
            con.execute("ALTER TABLE users ADD COLUMN last_name TEXT")
        except Exception:
            pass


def _upsert_user(
    con: sqlite3.Connection,
    *,
    email: str,
    role: str,
    first_name: str | None = None,
    last_name: str | None = None,
) -> int:
    email_n = email.strip().lower()
    if not email_n:
        raise ValueError("email is required")
    if role not in ("admin", "user", "fe"):
        raise ValueError("role must be one of: admin, user, fe")

    # Insert new users with optional names if those columns exist.
    cols = {r[1] for r in con.execute("PRAGMA table_info(users)").fetchall()}
    if "first_name" in cols and "last_name" in cols:
        con.execute(
            """
            INSERT OR IGNORE INTO users (email, first_name, last_name, role, is_active)
            VALUES (?, ?, ?, ?, 1)
            """,
            ((email_n, (first_name or "").strip() or None, (last_name or "").strip() or None, role)),
        )
    else:
        con.execute(
            """
            INSERT OR IGNORE INTO users (email, role, is_active)
            VALUES (?, ?, 1)
            """,
            (email_n, role),
        )

    # If user existed, optionally upgrade role (admin should win).
    con.execute(
        """
        UPDATE users
        SET role = ?
        WHERE email = ?
        """,
        (role, email_n),
    )
    # Backfill names if provided and columns exist.
    if ("first_name" in cols and "last_name" in cols) and (first_name or last_name):
        con.execute(
            """
            UPDATE users
            SET first_name = COALESCE(first_name, ?),
                last_name  = COALESCE(last_name, ?)
            WHERE email = ?
            """,
            ((first_name or "").strip() or None, (last_name or "").strip() or None, email_n),
        )
    row = con.execute("SELECT user_id FROM users WHERE email = ?", (email_n,)).fetchone()
    if not row:
        raise RuntimeError(f"Failed to create or fetch user_id for {email_n}")
    return int(row[0])


def _ensure_subscription(
    con: sqlite3.Connection,
    *,
    user_id: int,
    subscription_type: str,
    is_enabled: int = 1,
) -> None:
    st = subscription_type.strip()
    if not st:
        raise ValueError("subscription_type is required")
    exists = con.execute(
        """
        SELECT 1
        FROM user_subscriptions
        WHERE user_id = ? AND subscription_type = ?
        """,
        (int(user_id), st),
    ).fetchone()
    if exists:
        # Keep it enabled if requested; don't disable existing rows implicitly.
        if int(is_enabled) == 1:
            con.execute(
                """
                UPDATE user_subscriptions
                SET is_enabled = 1
                WHERE user_id = ? AND subscription_type = ?
                """,
                (int(user_id), st),
            )
        return
    con.execute(
        """
        INSERT INTO user_subscriptions (user_id, subscription_type, is_enabled)
        VALUES (?, ?, ?)
        """,
        (int(user_id), st, 1 if int(is_enabled) else 0),
    )


def main() -> int:
    p = argparse.ArgumentParser(description="Seed initial users and subscriptions (idempotent).")
    p.add_argument("--db", default=None, help="Path to SQLite DB (default: from config/env)")
    p.add_argument(
        "--admin-email",
        default=None,
        help="Admin email (default: BRIEF_EMAIL_TO from env/config/.env)",
    )
    p.add_argument(
        "--test-email",
        default="test.user@example.com",
        help="Test user email (default: test.user@example.com)",
    )
    p.add_argument("--admin-first", default=None, help="Admin first name (optional)")
    p.add_argument("--admin-last", default=None, help="Admin last name (optional)")
    p.add_argument("--test-first", default=None, help="Test user first name (optional)")
    p.add_argument("--test-last", default=None, help="Test user last name (optional)")
    args = p.parse_args()

    db_path = Path(args.db).resolve() if args.db else Path(get_db_path()).resolve()
    if not db_path.is_file():
        print(f"ERROR: database not found: {db_path}", file=sys.stderr)
        return 2

    admin_email = (args.admin_email or _default_admin_email() or "").strip()
    if not admin_email:
        print(
            "ERROR: admin email not provided and BRIEF_EMAIL_TO not found in env/config/.env",
            file=sys.stderr,
        )
        return 2

    con = sqlite3.connect(str(db_path), timeout=30)
    try:
        con.execute("PRAGMA foreign_keys = ON")
        _ensure_tables_exist(con)
        _ensure_user_name_columns(con)

        admin_id = _upsert_user(
            con,
            email=admin_email,
            role="admin",
            first_name=args.admin_first,
            last_name=args.admin_last,
        )
        user_id = _upsert_user(
            con,
            email=args.test_email,
            role="user",
            first_name=args.test_first,
            last_name=args.test_last,
        )

        for sub in ("group_brief", "system_alert", "admin_report"):
            _ensure_subscription(con, user_id=admin_id, subscription_type=sub, is_enabled=1)
        _ensure_subscription(con, user_id=user_id, subscription_type="group_brief", is_enabled=1)

        con.commit()
        print(f"[seed_users] db={db_path}")
        print(f"[seed_users] admin: {admin_email} (user_id={admin_id})")
        print(f"[seed_users] user : {args.test_email} (user_id={user_id})")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())

