#!/usr/bin/env python3
"""
daily_backup.py
───────────────
Daily SQLite backup routine:
  1) PRAGMA integrity_check on main DB (exit if not 'ok')
  2) Safe backup via sqlite3 CLI: .backup
  3) PRAGMA integrity_check on backup (delete + exit if not 'ok')
  4) Retention: keep last 2 backups in backups/daily/
  5) Logging: logs/backup.log (timestamp, filename, size, success/failure)

Notes:
  - Uses SQLite online backup (WAL-safe) via `.backup`.
  - Avoids long write locks; integrity_check is read-only but can take time on large DBs.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from core.db.connection import get_db_path

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKUP_DIR = REPO_ROOT / "backups" / "daily"
LOG_PATH = REPO_ROOT / "logs" / "backup.log"


def _ts_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(line: str) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as fh:
        fh.write(f"{_ts_utc()} {line}\n")


def _integrity_check(db_path: Path) -> tuple[bool, str]:
    try:
        con = sqlite3.connect(str(db_path), timeout=5)
        try:
            cur = con.execute("PRAGMA integrity_check;")
            row = cur.fetchone()
        finally:
            con.close()
        msg = str(row[0]) if row and row[0] is not None else ""
        return (msg.strip().lower() == "ok"), msg.strip()
    except Exception as exc:
        return False, f"exception: {exc!s}"


def _resolve_sqlite3_exe() -> str | None:
    # 1) explicit
    p = (os.getenv("SQLITE3_PATH") or "").strip()
    if p and Path(p).exists():
        return p
    # 2) PATH
    which = shutil.which("sqlite3")
    if which:
        return which
    # 3) repo-bundled (if present)
    bundled = REPO_ROOT / "sqlite-tools" / "sqlite3.exe"
    if bundled.exists():
        return str(bundled)
    return None


def _sqlite_cli_backup(sqlite3_exe: str, src_db: Path, dst_db: Path) -> tuple[bool, str]:
    """
    Run: sqlite3 SRC ".backup DST"
    """
    dst_db.parent.mkdir(parents=True, exist_ok=True)
    cmd = [sqlite3_exe, str(src_db), f".backup {dst_db}"]
    try:
        p = subprocess.run(
            cmd,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=300,
        )
        if p.returncode != 0:
            tail = (p.stderr or p.stdout or "").strip()
            return False, f"rc={p.returncode} {tail}".strip()
        return True, "ok"
    except Exception as exc:
        return False, f"exception: {exc!s}"


def _retention_keep_last_2() -> None:
    try:
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        backups = sorted(BACKUP_DIR.glob("mlb_backup_*.db"), key=lambda p: p.name, reverse=True)
        for old in backups[2:]:
            try:
                old.unlink(missing_ok=True)
                _log(f"retention deleted={old.name}")
            except Exception as exc:
                _log(f"retention failed_delete={old.name} err={exc!s}")
    except Exception as exc:
        _log(f"retention exception={exc!s}")


def main() -> int:
    src = Path(get_db_path()).resolve()
    if not src.exists():
        _log(f"FAIL db_missing={src}")
        print(f"✗ DB not found: {src}")
        return 2

    ok, msg = _integrity_check(src)
    if not ok:
        _log(f"FAIL integrity_main db={src.name} detail={msg}")
        print(f"✗ integrity_check failed on main DB: {msg}")
        return 1

    sqlite3_exe = _resolve_sqlite3_exe()
    if not sqlite3_exe:
        _log("FAIL sqlite3_missing (set SQLITE3_PATH or install sqlite3 in PATH)")
        print("✗ sqlite3 CLI not found. Install sqlite3 or set SQLITE3_PATH.")
        return 3

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d")
    dst = (BACKUP_DIR / f"mlb_backup_{stamp}.db").resolve()

    ok, detail = _sqlite_cli_backup(sqlite3_exe, src, dst)
    if not ok:
        _log(f"FAIL backup_write dst={dst.name} detail={detail}")
        print(f"✗ backup failed: {detail}")
        return 1

    ok, msg = _integrity_check(dst)
    if not ok:
        try:
            dst.unlink(missing_ok=True)
        except Exception:
            pass
        _log(f"FAIL integrity_backup dst={dst.name} detail={msg}")
        print(f"✗ integrity_check failed on backup (deleted): {msg}")
        return 1

    size = 0
    try:
        size = dst.stat().st_size
    except Exception:
        pass
    _log(f"OK backup={dst.name} size_bytes={size}")
    print(f"✓ backup ok: {dst} ({size} bytes)")

    _retention_keep_last_2()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
