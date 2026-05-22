import datetime
from core.db.connection import connect, get_db_path
from batch.pipeline import generate_daily_brief as gdb

game_date = "2026-05-21"
conn = connect(get_db_path())
conn.row_factory = __import__("sqlite3").Row
now = gdb._now_et()

n = gdb.backfill_bet_ledger_from_signal_state(conn, game_date, now=now)
print("backfill inserted", n)

rows = conn.execute(
    "SELECT * FROM bet_ledger WHERE game_date=?",
    (game_date,),
).fetchall()
print("ledger rows", len(rows))
for r in rows:
    print(dict(r))

# trace latest keys
sig_rows = conn.execute(
    """
    SELECT game_pk, market_type, signal_type, bet, odds, session, recorded_at
    FROM signal_state WHERE game_date=?
    """,
    (game_date,),
).fetchall()
game_pks = sorted({r["game_pk"] for r in sig_rows})
ph = ",".join("?" * len(game_pks))
g_rows = conn.execute(
    f"SELECT game_pk, game_start_utc FROM games WHERE game_pk IN ({ph})",
    tuple(game_pks),
).fetchall()
start_by_pk = {}
for r in g_rows:
    raw = r["game_start_utc"] or ""
    if "T" not in raw:
        print("no T in start", r)
        continue
    utc_dt = datetime.datetime.fromisoformat(raw.rstrip("Z")).replace(tzinfo=datetime.timezone.utc)
    start_by_pk[int(r["game_pk"])] = utc_dt.astimezone(gdb._ET)
print("start_by_pk", start_by_pk)

latest = gdb._ledger_latest_from_signal_rows(
    sig_rows, start_by_pk, now, pregame_window_only=False,
)
print("latest keys", list(latest.keys()))
for k, (dt, r) in latest.items():
    act = gdb._ledger_signal_row_is_actionable(conn, game_date, r)
    print(k, r["recorded_at"], r["odds"], "actionable", act)
