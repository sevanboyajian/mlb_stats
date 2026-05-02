import sqlite3
from core.db.connection import get_db_path

c = sqlite3.connect(get_db_path())
c.row_factory = sqlite3.Row
r = c.execute(
    "SELECT id FROM bet_ledger WHERE game_pk = 824044 AND game_date = '2026-04-22'"
).fetchone()
if r:
    c.execute("DELETE FROM bet_ledger WHERE id = ?", (r["id"],))
    c.commit()
    print("Deleted TOR row", dict(r))
