#  core/db/connection.py
#
# CHANGE LOG (latest first)
# -------------------------
# 2026-04-13 16:24 ET  Refactor: add shared connect() helper for sqlite connections.

import sqlite3
import os

def get_db_path():
    # Option 1: environment variable
    path = os.getenv("MLB_DB_PATH")

    if path:
        return path

    # fallback (current behavior)
    return os.path.join(os.getcwd(), "mlb_stats.db")


def connect(db_path: str | None = None, **kwargs):
    """
    Shared sqlite connection helper.

    - db_path: override path (preserves scripts that pick their own DB)
    - kwargs: passed through to sqlite3.connect (timeout, check_same_thread, etc.)
    """
    return sqlite3.connect(db_path or get_db_path(), **kwargs)


def get_connection():
    return connect()