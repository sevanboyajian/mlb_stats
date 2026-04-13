#  core/db/connection.py
#
import sqlite3
import os

def get_db_path():
    # Option 1: environment variable
    path = os.getenv("MLB_DB_PATH")

    if path:
        return path

    # fallback (current behavior)
    return os.path.join(os.getcwd(), "mlb_stats.db")


def get_connection():
    return sqlite3.connect(get_db_path())