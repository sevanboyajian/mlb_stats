#  core/db/connection.py
#
# CHANGE LOG (latest first)
# -------------------------
# 2026-04-13 21:00 ET  Resolve relative MLB_DB_PATH against repo root (stable cwd).
# 2026-04-13 20:30 ET  Resolve .env from repo root and config/.env (not cwd-only);
#                      cwd .env still supported as last resort.
# 2026-04-13 19:54 ET 
# Database connection abstraction layer.
# Eliminates hardcoded paths and centralizes configuration.
#
# Priority:
# - ENV (MLB_DB_PATH) for deployment
# - .env for developer overrides
# - Default local file for backward compatibility
#
# This design supports migration, testing, and cloud deployment
# without requiring code changes in dependent modules.
# 2026-04-13 16:24 ET  Refactor: add shared connect() helper for sqlite connections.

import sqlite3
import os
from pathlib import Path

# core/db/connection.py -> parents[2] == repository root
_REPO_ROOT = Path(__file__).resolve().parents[2]


def _resolve_db_path(raw: str) -> str:
    """Absolute paths unchanged; relative paths are anchored to the repository root."""
    p = Path(raw)
    if p.is_absolute():
        return str(p)
    return str((_REPO_ROOT / p).resolve())


def _mlb_db_path_from_dotenv(path: Path) -> str | None:
    if not path.is_file():
        return None
    with open(path, encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if line.startswith("MLB_DB_PATH="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


def get_db_path():
    raw: str | None = None
    # 1. ENV variable (highest priority)
    env_path = os.getenv("MLB_DB_PATH")
    if env_path:
        raw = env_path
    else:
        # 2. .env files: not cwd-only — running from batch/ingestion would miss repo/config .env
        for env_file in (
            _REPO_ROOT / "config" / ".env",
            _REPO_ROOT / ".env",
            Path.cwd() / ".env",
        ):
            parsed = _mlb_db_path_from_dotenv(env_file)
            if parsed:
                raw = parsed
                break

    if raw is not None:
        return _resolve_db_path(raw)

    # 3. Safe fallback (current behavior)
    return str(Path.cwd() / "mlb_stats.db")


def connect(db_path: str | None = None, **kwargs):
    """sqlite3.connect wrapper; kwargs e.g. timeout=30, check_same_thread=False."""
    return sqlite3.connect(db_path or get_db_path(), **kwargs)


def get_connection():
    return connect()