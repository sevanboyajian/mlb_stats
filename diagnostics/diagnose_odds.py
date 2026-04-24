"""
diagnose_odds.py  —  v2
Queries DB directly for actual game_pks, then shows what the Odds API
returns and whether each event correctly maps to today's game_pks.

Run:
  python diagnostics/diagnose_odds.py
"""

# CHANGE LOG (latest first)
# -------------------------
# 2026-04-13 22:15 ET  DB from get_db_path(); repo root on sys.path for core.* imports.
# 2026-04-13 16:24 ET  Refactor: route sqlite3.connect() calls through core.db.connection.connect().

import json
import os
import sqlite3
import sys
import urllib.parse
import urllib.request
from datetime import date, timedelta, datetime, timezone

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from core.db.connection import connect as db_connect, get_db_path

API_KEY = os.getenv("THE_ODDS_API_KEY")
DB_PATH = get_db_path()
TARGET  = date.today().isoformat()
NEXT    = (date.today() + timedelta(days=1)).isoformat()

if not API_KEY:
    print("ERROR: THE_ODDS_API_KEY is not set.")
    print("  Set it in your environment or in config/.env")
    sys.exit(2)

con = db_connect(DB_PATH)
con.row_factory = sqlite3.Row

# ── Pull ALL games for today + tomorrow directly ──────────────────────────────
rows = con.execute("""
    SELECT g.game_pk, g.game_date_et AS game_date, g.game_start_utc,
           th.abbreviation AS home_abbr, th.name AS home_name,
           ta.abbreviation AS away_abbr, ta.name AS away_name,
           (SELECT COUNT(*) FROM game_odds WHERE game_pk = g.game_pk) AS odds_rows
    FROM   games g
    JOIN   teams th ON th.team_id = g.home_team_id
    JOIN   teams ta ON ta.team_id = g.away_team_id
    WHERE  g.game_date_et IN (?, ?) AND g.game_type = 'R'
    ORDER  BY g.game_start_utc
""", (TARGET, NEXT)).fetchall()

print(f"\nDB games for {TARGET} (+{NEXT}):")
# Build lookup keyed by ACTUAL game_pk — one entry per game_pk
# For UTC key collisions, prefer target_date game (the fix)
lookup_by_utc  = {}   # (home, away, utc_date) → game row
lookup_by_pk   = {}   # game_pk → game row

for r in rows:
    d = dict(r)
    lookup_by_pk[r["game_pk"]] = d
    utc_date = (r["game_start_utc"] or "")[:10] or r["game_date"]
    key = (r["home_abbr"].upper(), r["away_abbr"].upper(), utc_date)
    # Prefer target_date game on collision (the fix)
    if key not in lookup_by_utc or r["game_date"] == TARGET:
        lookup_by_utc[key] = d

for r in rows:
    utc_date = (r["game_start_utc"] or "")[:10]
    flag = " ← MISSING" if r["odds_rows"] == 0 and r["game_date"] == TARGET else ""
    print(f"  {r['away_abbr']:4}@{r['home_abbr']:4}  game_pk={r['game_pk']}  "
          f"game_date={r['game_date']}  start_utc={r['game_start_utc']}  "
          f"odds_rows={r['odds_rows']}{flag}")

TEAM_MAP = {
    "yankees":"NYY","red sox":"BOS","blue jays":"TOR","rays":"TB","orioles":"BAL",
    "white sox":"CWS","guardians":"CLE","tigers":"DET","royals":"KC","twins":"MIN",
    "astros":"HOU","athletics":"ATH","a's":"ATH","rangers":"TEX","angels":"LAA",
    "mariners":"SEA","braves":"ATL","marlins":"MIA","mets":"NYM","phillies":"PHI",
    "nationals":"WSH","cubs":"CHC","reds":"CIN","brewers":"MIL","pirates":"PIT",
    "cardinals":"STL","diamondbacks":"AZ","rockies":"COL","dodgers":"LAD",
    "padres":"SD","giants":"SF",
}

def resolve(name):
    lo = name.lower()
    for frag, abbr in TEAM_MAP.items():
        if frag in lo:
            return abbr
    return None

# ── Pull from Odds API (standard library; no requests dependency) ─────────────
params = {
    "apiKey": API_KEY,
    "bookmakers": "draftkings",
    "markets": "h2h",
    "oddsFormat": "american",
    "dateFormat": "iso",
}
url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds?" + urllib.parse.urlencode(params)
try:
    req = urllib.request.Request(url, headers={"User-Agent": "mlb_stats/diagnose_odds"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        events = json.loads(body) if body else []
except Exception as e:
    print(f"\nERROR: Could not fetch Odds API events: {e}")
    con.close()
    sys.exit(2)

print(f"\nOdds API events ({len(events)} total) — today/tomorrow only:")
print(f"  {'MATCHUP':<35} {'CT (UTC)':<25} {'ABBR':<12} {'RESULT'}")
print(f"  {'-'*35} {'-'*25} {'-'*12} {'-'*40}")

for ev in sorted(events, key=lambda e: e.get("commence_time","")):
    ct = ev.get("commence_time","")
    utc_date = ct[:10]
    if utc_date not in (TARGET, NEXT):
        continue
    home = ev.get("home_team","")
    away = ev.get("away_team","")
    ha = resolve(home)
    aa = resolve(away)
    abbr_str = f"{aa}@{ha}" if ha and aa else "UNRESOLVED"
    key = (ha, aa, utc_date) if ha and aa else None
    matched = lookup_by_utc.get(key) if key else None

    if matched:
        gd    = matched["game_date"]
        gk    = matched["game_pk"]
        today = "TODAY" if gd == TARGET else "TOMORROW"
        result = f"→ game_pk={gk} ({today}, {matched['odds_rows']} rows)"
    else:
        result = "→ NO MATCH"
        # Show what DB has for these teams
        candidates = {k: v for k, v in lookup_by_utc.items()
                      if k[0] in (ha,"") or k[1] in (aa,"")}

    print(f"  {away[:34]:<35} {ct:<25} {abbr_str:<12} {result}")
    if not matched and ha and aa:
        for k, v in lookup_by_pk.items():
            if v["away_abbr"] == aa or v["home_abbr"] == ha:
                print(f"    DB has: game_pk={k} game_date={v['game_date']} "
                      f"{v['away_abbr']}@{v['home_abbr']} start_utc={v['game_start_utc']}")

con.close()
