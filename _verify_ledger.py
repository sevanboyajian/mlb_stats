import sqlite3

from core.db.connection import get_db_path

CONN = sqlite3.connect(get_db_path())
CONN.row_factory = sqlite3.Row
q = """
SELECT model_version,
       SUM(CASE WHEN result IN ('win','loss','push') THEN 1 ELSE 0 END) AS graded_bets,
       SUM(CASE WHEN result='win' THEN 1 ELSE 0 END) AS wins,
       SUM(CASE WHEN result='loss' THEN 1 ELSE 0 END) AS losses,
       ROUND(SUM(CASE WHEN result IN ('win','loss','push')
                     THEN COALESCE(pnl_units, 0) ELSE 0 END), 2) AS units
FROM bet_ledger
WHERE lower(trim(COALESCE(signal_at_time,''))) <> 'avoid'
GROUP BY model_version
ORDER BY model_version
"""
for r in CONN.execute(q):
    print(dict(r))
tot = """
SELECT SUM(CASE WHEN result IN ('win','loss','push') THEN 1 ELSE 0 END),
       SUM(CASE WHEN result='win' THEN 1 ELSE 0 END),
       SUM(CASE WHEN result='loss' THEN 1 ELSE 0 END),
       ROUND(SUM(CASE WHEN result IN ('win','loss','push')
                     THEN COALESCE(pnl_units, 0) ELSE 0 END), 2)
FROM bet_ledger
WHERE lower(trim(COALESCE(signal_at_time,''))) <> 'avoid'
"""
print("TOTAL", CONN.execute(tot).fetchone())
