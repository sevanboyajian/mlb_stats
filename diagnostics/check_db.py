"""
check_db.py
────────────────────────────────────────────────────────────────
Quick health check for mlb_stats.db.

Usage (run from repo root or anywhere):
    python diagnostics/check_db.py

What it shows:
  1. Whether the database file exists and its size
  2. All tables and their row counts
  3. Season coverage — which years are loaded in games and stats
  4. Venues migration — wind_effect columns populated, suppressed parks
  5. A sample row from key tables to confirm data looks right
  6. ingest_log summary — successes vs errors
────────────────────────────────────────────────────────────────
"""

# CHANGE LOG (latest first)
# -------------------------
# 2026-04-13 22:15 ET  Use get_db_path() (env / config/.env); repo root on sys.path for core.*.
# 2026-04-13 16:24 ET  Refactor: route sqlite3.connect() calls through core.db.connection.connect().

import os
import sqlite3
import sys
from datetime import date, datetime, timezone, timedelta

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from core.db.connection import connect as db_connect, get_db_path

# ── Console encoding guard (Windows cp1252) ─────────────────────
# Prevent crashes when printing unicode glyphs (⚠, arrows, etc.).
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass
try:
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# ── Locate the database ───────────────────────────────────────
db_path = get_db_path()

if not os.path.exists(db_path):
    print("ERROR: Database file not found.")
    print(f"  Resolved path: {db_path}")
    print("  Set MLB_DB_PATH or add MLB_DB_PATH=... to config/.env")
    sys.exit(1)

size_mb = os.path.getsize(db_path) / (1024 * 1024)
print("=" * 56)
print("  MLB Stats DB — Health Check")
print("=" * 56)
print(f"  File : {db_path}")
print(f"  Size : {size_mb:.1f} MB")
print()

con = db_connect(db_path)
con.row_factory = sqlite3.Row

def _table_exists(c: sqlite3.Connection, name: str) -> bool:
    try:
        r = c.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (name,),
        ).fetchone()
        return bool(r)
    except Exception:
        return False


def _table_columns(c: sqlite3.Connection, name: str) -> set[str]:
    try:
        return {r[1] for r in c.execute(f"PRAGMA table_info({name})").fetchall()}
    except Exception:
        return set()


def _safe_date_range(c: sqlite3.Connection, table: str, date_col: str) -> tuple[str | None, str | None]:
    """Return (min_date, max_date) as strings if available; else (None, None)."""
    try:
        row = c.execute(
            f"SELECT MIN({date_col}) AS mn, MAX({date_col}) AS mx FROM {table}"
        ).fetchone()
        if not row:
            return None, None
        return (row["mn"] if isinstance(row, sqlite3.Row) else row[0],
                row["mx"] if isinstance(row, sqlite3.Row) else row[1])
    except Exception:
        return None, None


def _is_stale_iso_date(d: str | None, *, days: int = 14) -> bool:
    """True if ISO YYYY-MM-DD is older than N days from today."""
    if not d:
        return True
    try:
        dt0 = date.fromisoformat(str(d)[:10])
        return (date.today() - dt0).days > int(days)
    except Exception:
        return True


# ── 1. Table list and row counts ──────────────────────────────
TABLES = [
    "seasons", "venues", "teams", "players",
    "games", "player_game_stats", "play_by_play", "standings",
    "game_odds", "player_props", "line_movement",
    "model_predictions", "backtest_results",
    "ingest_log", "odds_ingest_log",
    # Ops / reporting tables
    "brief_log", "brief_picks", "daily_pnl",
    # Intra-day signal tracking + real betting ledger
    "signal_state", "bet_ledger", "team_rolling_stats",
]

print(f"  {'TABLE':<28} {'ROWS':>10}  STATUS")
print("  " + "-" * 50)

all_tables_exist = True
for t in TABLES:
    try:
        n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        if n == 0:
            status = "<-- empty"
        elif t in ("player_game_stats", "play_by_play") and n > 0:
            status = "OK"
        elif t in ("seasons", "teams") and n > 0:
            status = "OK"
        else:
            status = ""
        print(f"  {t:<28} {n:>10}  {status}")
    except sqlite3.OperationalError:
        print(f"  {t:<28}       MISSING  <-- initialize DB schema (core/db/schema.sql)")
        all_tables_exist = False

print()

if not all_tables_exist:
    print("  Some tables are missing.")
    print("  Initialize the DB by applying the schema, e.g.:")
    print('    sqlite3 "<path-to-mlb_stats.db>" < core/db/schema.sql')
    print("  Then rerun the loaders (from repo root), e.g.:")
    print("    python batch/ingestion/load_mlb_stats.py --start 2024-03-20 --end 2024-03-21")
    print()
    con.close()
    sys.exit(1)

# ── 2. Season coverage ────────────────────────────────────────
print("  SEASON COVERAGE")
print("  " + "-" * 50)

# Games per season
print(f"  {'Season':<10} {'Games':>8}  {'Stat rows':>12}  {'Status'}")
print(f"  {'-'*6:<10} {'-'*5:>8}  {'-'*9:>12}  {'-'*6}")

seasons_in_games = con.execute(
    "SELECT season, COUNT(*) as n FROM games "
    "WHERE game_type = 'R' GROUP BY season ORDER BY season"
).fetchall()

if not seasons_in_games:
    print("  (no games loaded yet)")
else:
    for row in seasons_in_games:
        season = row[0]
        game_count = row[1]
        stat_count = con.execute(
            "SELECT COUNT(*) FROM player_game_stats pgs "
            "JOIN games g ON g.game_pk = pgs.game_pk "
            "WHERE g.season = ?", (season,)
        ).fetchone()[0]

        if game_count >= 2400:
            status = "Full"
        elif game_count >= 1000:
            status = "Partial"
        elif game_count > 0:
            status = "Sparse"
        else:
            status = "No games"

        if stat_count == 0 and game_count > 0:
            status += " / no stats"

        print(f"  {season:<10} {game_count:>8}  {stat_count:>12}  {status}")

# Check for missing seasons in range 2015-2025
loaded_seasons = {row[0] for row in seasons_in_games}
expected = set(range(2015, 2026))
missing = sorted(expected - loaded_seasons)
if missing:
    print()
    print(f"  Missing seasons: {missing}")
    print("  To backfill, run:")
    for y in missing:
        print(f"    python batch/ingestion/load_mlb_stats.py --season {y} --no-pbp")

print()

# ── 3. Play-by-play coverage ──────────────────────────────────
pbp_count = con.execute("SELECT COUNT(*) FROM play_by_play").fetchone()[0]
if pbp_count == 0:
    print("  PLAY-BY-PLAY: empty (expected — load with: python batch/ingestion/load_mlb_stats.py --season YYYY)")
else:
    pbp_seasons = con.execute(
        "SELECT g.season, COUNT(*) FROM play_by_play pbp "
        "JOIN games g ON g.game_pk = pbp.game_pk "
        "GROUP BY g.season ORDER BY g.season"
    ).fetchall()
    print(f"  PLAY-BY-PLAY: {pbp_count:,} rows across seasons: "
          f"{[r[0] for r in pbp_seasons]}")
print()

# ── 4. Venues migration health check ─────────────────────────
print("  VENUES MIGRATION")
print("  " + "-" * 50)

# Check whether add_stadium_data.py has been run
venue_cols = {r[1] for r in con.execute("PRAGMA table_info(venues)").fetchall()}
new_cols   = {"wind_effect", "wind_note", "orientation_hp", "cf_direction",
              "park_factor_runs", "park_factor_hr", "altitude_note",
              "opened_year", "last_updated"}
missing_cols = new_cols - venue_cols

if missing_cols:
    print(f"  ⚠  Migration not yet run — missing columns: {sorted(missing_cols)}")
    print("     Run: python batch/ingestion/add_stadium_data.py")
else:
    total_v  = con.execute("SELECT COUNT(*) FROM venues").fetchone()[0]
    seeded   = con.execute(
        "SELECT COUNT(*) FROM venues WHERE wind_effect IS NOT NULL"
    ).fetchone()[0]
    unseeded = total_v - seeded

    print(f"  Venue rows      : {total_v}")
    print(f"  Seeded (wind)   : {seeded}")
    if unseeded:
        print(f"  ⚠  Unseeded     : {unseeded}  — run: python batch/ingestion/add_stadium_data.py --update")

    if seeded > 0:
        # Wind effect distribution
        dist = con.execute(
            "SELECT wind_effect, COUNT(*) n FROM venues "
            "WHERE wind_effect IS NOT NULL "
            "GROUP BY wind_effect ORDER BY n DESC"
        ).fetchall()
        effect_str = "  ".join(f"{r[0]}:{r[1]}" for r in dist)
        print(f"  Wind effects    : {effect_str}")

        # Suppressed venues — wind signals never apply
        suppressed = con.execute(
            "SELECT v.name, t.abbreviation FROM venues v "
            "LEFT JOIN teams t ON t.venue_id = v.venue_id "
            "WHERE v.wind_effect = 'SUPPRESSED' "
            "ORDER BY t.abbreviation"
        ).fetchall()
        if suppressed:
            names = ", ".join(
                f"{r['abbreviation'] or '?'} ({r['name']})" for r in suppressed
            )
            print(f"  Suppressed      : {names}")

        # HIGH-altitude parks
        high_alt = con.execute(
            "SELECT v.name, v.elevation_ft, t.abbreviation "
            "FROM venues v "
            "LEFT JOIN teams t ON t.venue_id = v.venue_id "
            "WHERE v.elevation_ft > 500 "
            "ORDER BY v.elevation_ft DESC"
        ).fetchall()
        if high_alt:
            alt_str = "  ".join(
                f"{r['abbreviation'] or '?'}:{r['elevation_ft']}ft" for r in high_alt
            )
            print(f"  Alt > 500 ft    : {alt_str}")

print()

# ── 5. Sample data spot-checks ────────────────────────────────
print("  SPOT CHECKS")
print("  " + "-" * 50)

# Most recent game
row = con.execute(
    "SELECT g.game_date_et AS game_date, t1.abbreviation, g.away_score, "
    "t2.abbreviation, g.home_score, g.status "
    "FROM games g "
    "JOIN teams t1 ON t1.team_id = g.away_team_id "
    "JOIN teams t2 ON t2.team_id = g.home_team_id "
    "WHERE g.status = 'Final' AND g.game_type = 'R' "
    "ORDER BY g.game_date_et DESC LIMIT 1"
).fetchone()
if row:
    print(f"  Most recent game : {row[0]}  "
          f"{row[1]} {row[2]} @ {row[3]} {row[4]}")
else:
    print("  Most recent game : (none loaded)")

# Player count
player_count = con.execute("SELECT COUNT(*) FROM players").fetchone()[0]
print(f"  Players loaded   : {player_count:,}")

# Team count (use active teams if available)
try:
    team_cols = _table_columns(con, "teams")
    if "active" in team_cols:
        active_team_count = con.execute("SELECT COUNT(*) FROM teams WHERE active=1").fetchone()[0]
        total_team_count = con.execute("SELECT COUNT(*) FROM teams").fetchone()[0]
        note = "" if active_team_count == 30 else "  <-- unexpected"
        print(f"  Teams loaded     : {active_team_count} active / {total_team_count} total  (active should be 30){note}")
    else:
        team_count = con.execute("SELECT COUNT(*) FROM teams").fetchone()[0]
        note = "" if team_count == 30 else "  <-- unexpected"
        print(f"  Teams loaded     : {team_count}  (should be 30){note}")
except Exception:
    team_count = con.execute("SELECT COUNT(*) FROM teams").fetchone()[0]
    print(f"  Teams loaded     : {team_count}  (should be 30)")

# ── 6. Ingest log summary ─────────────────────────────────────
total  = con.execute("SELECT COUNT(*) FROM ingest_log").fetchone()[0]
ok     = con.execute("SELECT COUNT(*) FROM ingest_log WHERE status='success'").fetchone()[0]
errors = con.execute("SELECT COUNT(*) FROM ingest_log WHERE status='error'").fetchone()[0]

print()
print("  INGEST LOG")
print("  " + "-" * 50)
print(f"  Total attempts : {total:,}")
print(f"  Successful     : {ok:,}")
print(f"  Errors         : {errors:,}")
if errors > 0:
    print("  --> Run: python batch/ingestion/load_mlb_stats.py --retry-errors")

# Show recent errors if any
if errors > 0:
    print()
    print("  Recent errors:")
    err_rows = con.execute(
        "SELECT il.game_pk, g.game_date_et AS game_date, il.error_message "
        "FROM ingest_log il "
        "JOIN games g ON g.game_pk = il.game_pk "
        "WHERE il.status = 'error' "
        "ORDER BY g.game_date_et DESC LIMIT 5"
    ).fetchall()
    for r in err_rows:
        msg = (r[2] or "")[:60]
        print(f"    game_pk={r[0]}  {r[1]}  {msg}")


# ── 7. TODAY — Current state at time of running ───────────────
from datetime import date, datetime, timezone, timedelta

today_date = date.today().isoformat()

# ET offset — UTC-4 (EDT, correct for MLB season Apr-Oct)
now_et = datetime.now(timezone.utc) + timedelta(hours=-4)
now_et_str = now_et.strftime("%Y-%m-%d %H:%M ET")

print("  TODAY — " + today_date + "  (as of " + now_et_str + ")")
print("  " + "-" * 50)

# ── 7a. Games ─────────────────────────────────────────────────
game_rows = con.execute("""
    SELECT
        ta.abbreviation || '@' || th.abbreviation AS matchup,
        g.game_start_utc,
        g.status,
        g.home_score,
        g.away_score
    FROM games g
    JOIN teams th ON th.team_id = g.home_team_id
    JOIN teams ta ON ta.team_id = g.away_team_id
    WHERE g.game_date_et = ?
      AND g.game_type = 'R'
    ORDER BY g.game_start_utc
""", (today_date,)).fetchall()

total_today = len(game_rows)
final_today = sum(1 for r in game_rows if r["status"] == "Final")
sched_today = sum(1 for r in game_rows if r["status"] == "Scheduled")
live_today  = total_today - final_today - sched_today

print(f"  Games today      : {total_today}  "
      f"({final_today} Final  {sched_today} Scheduled"
      + (f"  {live_today} Live/Other" if live_today else "") + ")")

# ── 7b. Odds ──────────────────────────────────────────────────
if total_today > 0:
    odds_rows = con.execute("""
        SELECT
            ta.abbreviation || '@' || th.abbreviation AS matchup,
            COUNT(go.id) AS total_rows,
            SUM(CASE WHEN go.is_closing_line=1 AND go.home_ml IS NOT NULL
                     THEN 1 ELSE 0 END) AS has_closing_ml
        FROM games g
        JOIN teams th ON th.team_id = g.home_team_id
        JOIN teams ta ON ta.team_id = g.away_team_id
        LEFT JOIN game_odds go ON go.game_pk = g.game_pk
        WHERE g.game_date_et = ? AND g.game_type = 'R'
        GROUP BY g.game_pk
        ORDER BY g.game_start_utc
    """, (today_date,)).fetchall()

    with_odds    = sum(1 for r in odds_rows if r["has_closing_ml"] and r["has_closing_ml"] > 0)
    without_odds = total_today - with_odds
    missing_list = [r["matchup"] for r in odds_rows
                    if not r["has_closing_ml"] or r["has_closing_ml"] == 0]

    # Last odds pull time
    last_pull = con.execute("""
        SELECT pulled_at_utc, api_quota_remaining, api_requests_used
        FROM odds_ingest_log
        WHERE pull_type = 'daily_pregame'
        ORDER BY pulled_at_utc DESC LIMIT 1
    """).fetchone()

    print(f"  Odds loaded      : {with_odds}/{total_today} games have closing-line ML odds")
    if last_pull:
        # Convert UTC to ET for display
        try:
            lp_dt = datetime.fromisoformat(last_pull["pulled_at_utc"].split(".")[0])
            lp_et = lp_dt + timedelta(hours=-4)
            lp_str = lp_et.strftime("%I:%M %p ET").lstrip("0")
        except Exception:
            lp_str = last_pull["pulled_at_utc"]
        print(f"  Last odds pull   : {lp_str}  "
              f"(cost: {last_pull['api_requests_used']} req  "
              f"quota remaining: {last_pull['api_quota_remaining']:,})")
    else:
        print("  Last odds pull   : no pull recorded today")

    if missing_list:
        print(f"  Missing odds     : {', '.join(missing_list)}")
        print("                     --> python batch/ingestion/load_odds.py --pregame --markets game")

    # ── 7c. Weather ───────────────────────────────────────────
    weather_rows = con.execute("""
        SELECT
            ta.abbreviation || '@' || th.abbreviation AS matchup,
            g.wind_mph,
            g.wind_direction,
            g.temp_f,
            g.wind_source
        FROM games g
        JOIN teams th ON th.team_id = g.home_team_id
        JOIN teams ta ON ta.team_id = g.away_team_id
        WHERE g.game_date_et = ? AND g.game_type = 'R'
        ORDER BY g.game_start_utc
    """, (today_date,)).fetchall()

    with_wind    = sum(1 for r in weather_rows if r["wind_mph"] is not None)
    forecast_ct  = sum(1 for r in weather_rows if r["wind_source"] == "forecast")
    actual_ct    = sum(1 for r in weather_rows if r["wind_source"] == "actual")

    wind_status = f"{with_wind}/{total_today} games"
    if forecast_ct: wind_status += f"  ({forecast_ct} forecast"
    if actual_ct:   wind_status += f"  {actual_ct} actual"
    if forecast_ct or actual_ct: wind_status += ")"

    print(f"  Wind loaded      : {wind_status}")
    if with_wind < total_today:
        no_wind = [r["matchup"] for r in weather_rows if r["wind_mph"] is None]
        print(f"  No wind data     : {', '.join(no_wind)}")
        print("                     --> python batch/ingestion/load_weather.py")

    # ── 7d. Starters ─────────────────────────────────────────
    try:
        starter_rows = con.execute("""
            SELECT
                ta.abbreviation || '@' || th.abbreviation AS matchup,
                COUNT(gp.player_id) AS starters_filed,
                MAX(gp.fetched_at)  AS last_fetched
            FROM games g
            JOIN teams th ON th.team_id = g.home_team_id
            JOIN teams ta ON ta.team_id = g.away_team_id
            LEFT JOIN game_probable_pitchers gp ON gp.game_pk = g.game_pk
            WHERE g.game_date_et = ? AND g.game_type = 'R'
            GROUP BY g.game_pk
            ORDER BY g.game_start_utc
        """, (today_date,)).fetchall()

        both_confirmed = sum(1 for r in starter_rows if r["starters_filed"] >= 2)
        one_confirmed  = sum(1 for r in starter_rows if r["starters_filed"] == 1)
        none_confirmed = sum(1 for r in starter_rows if r["starters_filed"] == 0)
        last_fetched   = max((r["last_fetched"] for r in starter_rows
                              if r["last_fetched"]), default=None)

        starter_status = f"{both_confirmed}/{total_today} both confirmed"
        if one_confirmed: starter_status += f"  {one_confirmed} partial"
        if none_confirmed: starter_status += f"  {none_confirmed} TBD"
        if last_fetched:
            try:
                lf_dt = datetime.strptime(last_fetched, "%Y-%m-%d %H:%M ET")
                starter_status += f"  (last fetched {last_fetched})"
            except Exception:
                starter_status += f"  (last fetched {last_fetched})"

        print(f"  Starters         : {starter_status}")
    except Exception:
        print(f"  Starters         : game_probable_pitchers table not available")

    # ── 7e. Briefs run today ──────────────────────────────────
    try:
        brief_rows = con.execute("""
            SELECT session, generated_at, picks_count
            FROM brief_log
            WHERE game_date = ?
            ORDER BY generated_at
        """, (today_date,)).fetchall()

        if brief_rows:
            sessions_run = [r["session"].upper() for r in brief_rows]
            print(f"  Briefs run       : {', '.join(sessions_run)}")
            for r in brief_rows:
                picks_note = f"  {r['picks_count']} pick(s)" if r["picks_count"] else ""
                print(f"    {r['session'].upper():<12} {r['generated_at']}{picks_note}")
        else:
            print(f"  Briefs run       : none yet today")
    except Exception:
        print(f"  Briefs run       : brief_log table not available")

    # ── 7f. Confirmed picks today ─────────────────────────────
    try:
        pick_rows = con.execute("""
            SELECT session, signal, bet, odds, pick_rank, recorded_at
            FROM brief_picks
            WHERE game_date = ?
            ORDER BY pick_rank, recorded_at
        """, (today_date,)).fetchall()

        if pick_rows:
            print(f"  Picks logged     : {len(pick_rows)} pick(s) in brief_picks")
            sessions_seen = set()
            for r in pick_rows:
                sess = r["session"].upper()
                if sess not in sessions_seen:
                    sessions_seen.add(sess)
                print(f"    [{sess}] Rank {r['pick_rank']}  {r['signal']:<8}  "
                      f"{r['bet']:<12}  {r['odds']:+d}  @ {r['recorded_at']}")
        else:
            print(f"  Picks logged     : none yet (no action session run)")
    except Exception:
        print(f"  Picks logged     : brief_picks table not available")

# ── Ops / ledger recency (avoid “old assets” confusion) ────────────────
print()
print("  OPS DATA RECENCY")
print("  " + "-" * 50)

try:
    # brief_log
    if _table_exists(con, "brief_log"):
        cols = _table_columns(con, "brief_log")
        # Prefer generated_at (has timestamp), fall back to game_date
        if "generated_at" in cols:
            row = con.execute("SELECT MAX(generated_at) AS mx FROM brief_log").fetchone()
            mx = (row["mx"] if row else None)
            tag = "STALE" if _is_stale_iso_date(str(mx)[:10] if mx else None, days=14) else "OK"
            print(f"  brief_log         : last={mx or '—'}  [{tag}]")
        elif "game_date" in cols:
            mn, mx = _safe_date_range(con, "brief_log", "game_date")
            tag = "STALE" if _is_stale_iso_date(mx, days=14) else "OK"
            print(f"  brief_log         : {mn or '—'} → {mx or '—'}  [{tag}]")
        else:
            print("  brief_log         : (date columns not found)")
    else:
        print("  brief_log         : (missing)")

    # brief_picks
    if _table_exists(con, "brief_picks"):
        cols = _table_columns(con, "brief_picks")
        if "recorded_at" in cols:
            row = con.execute("SELECT MAX(recorded_at) AS mx FROM brief_picks").fetchone()
            mx = (row["mx"] if row else None)
            tag = "STALE" if _is_stale_iso_date(str(mx)[:10] if mx else None, days=14) else "OK"
            print(f"  brief_picks       : last={mx or '—'}  [{tag}]")
        elif "game_date" in cols:
            mn, mx = _safe_date_range(con, "brief_picks", "game_date")
            tag = "STALE" if _is_stale_iso_date(mx, days=14) else "OK"
            print(f"  brief_picks       : {mn or '—'} → {mx or '—'}  [{tag}]")
        else:
            print("  brief_picks       : (date columns not found)")
    else:
        print("  brief_picks       : (missing)")

    # daily_pnl (legacy / deprecated)
    if _table_exists(con, "daily_pnl"):
        cols = _table_columns(con, "daily_pnl")
        date_col = "game_date" if "game_date" in cols else None
        if date_col:
            mn, mx = _safe_date_range(con, "daily_pnl", date_col)
            tag = "LEGACY"
            print(f"  daily_pnl         : {mn or '—'} → {mx or '—'}  [{tag}]")
        else:
            print("  daily_pnl         : (legacy; date column not found)")
    else:
        print("  daily_pnl         : (missing; legacy)")

    if _table_exists(con, "bet_ledger"):
        cols = _table_columns(con, "bet_ledger")
        # Prefer placed_at (when bet was taken), fall back to game_date.
        if "placed_at" in cols:
            row = con.execute("SELECT MAX(placed_at) AS mx FROM bet_ledger").fetchone()
            mx = (row["mx"] if row else None)
            tag = "STALE" if _is_stale_iso_date(str(mx)[:10] if mx else None, days=21) else "OK"
            print(f"  bet_ledger        : last={mx or '—'}  [{tag}]")
        elif "game_date" in cols:
            mn, mx = _safe_date_range(con, "bet_ledger", "game_date")
            tag = "STALE" if _is_stale_iso_date(mx, days=21) else "OK"
            print(f"  bet_ledger        : {mn or '—'} → {mx or '—'}  [{tag}]")
        else:
            print("  bet_ledger        : (date columns not found)")
    else:
        print("  bet_ledger        : (missing)")
except Exception as _e:
    print(f"  (recency check unavailable: {_e})")

# ── 7g. Paper account season balance ─────────────────────────
try:
    season_year = date.today().year
    season_row = con.execute(
        "SELECT season_start, postseason_start FROM seasons WHERE season=?",
        (season_year,)
    ).fetchone()
    if season_row:
        season_start = season_row["season_start"]
        season_end   = season_row["postseason_start"] or f"{season_year}-10-01"
        if _table_exists(con, "bet_ledger"):
            # bet_ledger is the source of truth. Only include settled bets (result present).
            # pnl_units is in "units" (1.0 ~= $100 in reports); show both units and dollars.
            ledg_cols = _table_columns(con, "bet_ledger")
            if "pnl_units" in ledg_cols and "result" in ledg_cols and "game_date" in ledg_cols:
                row = con.execute(
                    """
                    SELECT
                        SUM(CASE WHEN result IS NOT NULL THEN pnl_units ELSE 0 END) AS pnl_units,
                        SUM(CASE WHEN lower(result)='win'  THEN 1 ELSE 0 END) AS wins,
                        SUM(CASE WHEN lower(result)='loss' THEN 1 ELSE 0 END) AS losses,
                        SUM(CASE WHEN lower(result)='push' THEN 1 ELSE 0 END) AS pushes,
                        SUM(CASE WHEN result IS NOT NULL THEN 1 ELSE 0 END) AS settled_bets,
                        SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) AS open_bets
                    FROM bet_ledger
                    WHERE game_date >= ? AND game_date < ?
                    """,
                    (season_start, season_end),
                ).fetchone()

                settled = int(row["settled_bets"] or 0) if row else 0
                open_bets = int(row["open_bets"] or 0) if row else 0
                pnl_units = float(row["pnl_units"] or 0.0) if row else 0.0
                wins = int(row["wins"] or 0) if row else 0
                losses = int(row["losses"] or 0) if row else 0
                pushes = int(row["pushes"] or 0) if row else 0

                base_bank_dollars = 500.0
                unit_dollars = 100.0
                bank = base_bank_dollars + pnl_units * unit_dollars
                sign_units = "+" if pnl_units >= 0 else ""
                sign_dol = "+" if (pnl_units * unit_dollars) >= 0 else ""

                if settled or open_bets:
                    extra = f"  (open:{open_bets})" if open_bets else ""
                    print(
                        f"  Paper account    : Bank ${bank:.2f}  "
                        f"({sign_dol}${pnl_units * unit_dollars:.2f} season; "
                        f"{sign_units}{pnl_units:.2f}u  W:{wins} L:{losses} P:{pushes} settled:{settled}){extra}"
                    )
                else:
                    print("  Paper account    : $500.00  (no bet_ledger rows this season)")
            else:
                print("  Paper account    : bet_ledger missing required columns (game_date/result/pnl_units)")
        else:
            print("  Paper account    : bet_ledger table not available")
    else:
        print(f"  Paper account    : seasons table has no entry for {season_year}")
except Exception as e:
    print(f"  Paper account    : not available ({e})")

print()
print("=" * 56)
print("  Check complete.")
print("=" * 56)

con.close()

