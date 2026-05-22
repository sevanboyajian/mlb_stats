import sqlite3
from core.db.connection import get_db_path

con = sqlite3.connect(get_db_path())
con.row_factory = sqlite3.Row

for label, sql in [
    ("signal_state", "SELECT * FROM signal_state WHERE game_date='2026-05-21' ORDER BY recorded_at"),
    ("bet_ledger", "SELECT * FROM bet_ledger WHERE game_date='2026-05-21'"),
    ("games COL@AZ", "SELECT game_pk, status, home_score, away_score FROM games WHERE game_date_et='2026-05-21' AND game_pk=825083"),
]:
    print(f"\n=== {label} ===")
    try:
        rows = con.execute(sql).fetchall()
        for r in rows:
            print(dict(r))
        print("count", len(rows))
    except Exception as e:
        print("ERR", e)
