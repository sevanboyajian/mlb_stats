"""
boxscore_app.py
===============
Streamlit box score viewer — renders full game reports in-browser
with no file output.  Runs alongside scout.py on a different port.

USAGE
-----
    streamlit run boxscore_app.py
    streamlit run boxscore_app.py --server.port 8502

Controls
--------
  · Date picker  — choose any game date
  · Game picker  — "All games" or a single matchup
  · Auto-refresh checkbox — re-queries DB every 60 s (useful for live games)

Shares the scout.py colour palette and font system via .streamlit/config.toml.
No files are written; every render is built from a fresh DB query.
"""

# CHANGE LOG (latest first)
# -------------------------
# 2026-04-13 16:24 ET  Refactor: route sqlite3.connect() calls through core.db.connection.connect().

import re
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components
from core.db.connection import connect as db_connect

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="MLB Box Score",
    page_icon="⚾",
    layout="wide",
    initial_sidebar_state="collapsed",
)

DB_PATH = Path(__file__).parent / "mlb_stats.db"

# ── CSS — matches scout.py palette exactly ────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:ital,wght@0,300;0,400;0,600;1,300&display=swap');

:root {
  --bg:#0d0f14; --surface:#14171f; --border:#1e2330;
  --accent:#e8a020; --accent2:#3b82f6; --accent3:#10b981;
  --danger:#ef4444; --text:#e2e8f0; --muted:#64748b;
}

html,body,[data-testid="stAppViewContainer"],[data-testid="stMain"],
.main,.block-container {
  background:var(--bg) !important; color:var(--text) !important;
  font-family:'IBM Plex Sans',sans-serif !important;
  padding:0 !important; margin:0 !important; max-width:100% !important;
}
[data-testid="stHeader"],[data-testid="stToolbar"],
[data-testid="collapsedControl"],footer { display:none !important; }

.topbar {
  position:fixed; top:0; left:0; right:0; z-index:1000;
  height:52px; background:var(--surface); border-bottom:1px solid var(--border);
  display:flex; align-items:center; padding:0 20px; gap:16px;
}
.topbar-logo { font-family:'Bebas Neue',sans-serif; font-size:22px;
  letter-spacing:2px; color:var(--accent); }
.topbar-logo span { color:var(--text); }
.topbar-div  { width:1px; height:28px; background:var(--border); }
.topbar-sub  { font-size:11px; color:var(--muted); font-family:'IBM Plex Mono',monospace;
  letter-spacing:1px; text-transform:uppercase; }
.topbar-right { margin-left:auto; display:flex; align-items:center; gap:10px; }
.badge { font-family:'IBM Plex Mono',monospace; font-size:10px; padding:3px 8px;
  border-radius:2px; letter-spacing:.5px; text-transform:uppercase; }
.bg { background:rgba(16,185,129,.15); color:var(--accent3); border:1px solid rgba(16,185,129,.3); }
.bb { background:rgba(59,130,246,.15); color:var(--accent2); border:1px solid rgba(59,130,246,.3); }
.main-wrap { margin-top:52px; padding:20px 28px; }
[data-testid="stSidebar"] { display:none !important; }

/* ── control bar ── */
.ctrl-bar {
  display:flex; align-items:flex-end; gap:16px; flex-wrap:wrap;
  background:var(--surface); border:1px solid var(--border); border-radius:5px;
  padding:14px 18px; margin-bottom:20px;
}

/* ── game card ── */
.bs-card {
  background:var(--surface); border:1px solid var(--border);
  border-radius:6px; margin-bottom:24px; overflow:hidden;
}
.bs-header {
  background:#1f3864; padding:14px 18px; position:relative;
}
.bs-score {
  font-family:'Bebas Neue',sans-serif; font-size:34px;
  letter-spacing:1.5px; color:#fff; line-height:1;
}
.bs-winner { color:#7ec8e3; }
.bs-meta { font-size:11px; color:rgba(255,255,255,.65); margin-top:4px; }
.bs-info {
  display:flex; flex-wrap:wrap; gap:.5rem 2rem;
  padding:10px 18px; background:#0f1420;
  border-bottom:1px solid var(--border); font-size:12px;
}
.bs-info strong { color:var(--accent2); }
.bs-info span   { color:#94a3b8; }

.signal-bar {
  background:rgba(245,158,11,.08); border-bottom:2px solid #f59e0b;
  padding:8px 18px; font-size:12px; font-weight:600; color:#fbbf24;
  font-family:'IBM Plex Mono',monospace;
}
.nosig-bar {
  background:rgba(16,185,129,.05); border-bottom:1px solid rgba(16,185,129,.2);
  padding:8px 18px; font-size:12px; color:#4ade80;
  font-family:'IBM Plex Mono',monospace;
}
.odds-bar {
  display:flex; flex-wrap:wrap; gap:.5rem 2rem;
  padding:8px 18px; background:var(--surface);
  border-bottom:1px solid var(--border); font-size:12px;
}
.odds-bar .lbl { color:var(--muted); margin-right:4px; }

/* ── section label ── */
.sec-lbl {
  font-family:'IBM Plex Mono',monospace; font-size:9px; font-weight:700;
  letter-spacing:.12em; text-transform:uppercase; color:var(--muted);
  padding:7px 18px 4px; background:#0f1420;
  border-bottom:1px solid var(--border); border-top:1px solid var(--border);
}

/* ── tables ── */
.bs-table { width:100%; border-collapse:collapse; font-size:12px;
  font-family:'IBM Plex Mono',monospace; }
.bs-table th {
  background:#0a0c10; color:var(--muted); font-weight:500; font-size:9px;
  text-transform:uppercase; letter-spacing:1px; padding:6px 10px;
  border-bottom:1px solid var(--border); text-align:right;
}
.bs-table th:first-child,.bs-table th:nth-child(2) { text-align:left; }
.bs-table td {
  padding:5px 10px; border-bottom:1px solid rgba(30,35,48,.7);
  color:var(--text); text-align:right;
}
.bs-table td:first-child { text-align:center; color:var(--muted); font-size:10px; width:26px; }
.bs-table td:nth-child(2) { text-align:left; font-weight:600; }
.bs-table tr:last-child td { border-bottom:none; }
.bs-table tr.totals td { background:rgba(30,35,48,.5); font-weight:700; border-top:1px solid var(--border); }
.hl { color:#60a5fa; font-weight:700; }
.dec { display:inline-block; font-size:9px; font-weight:700;
  padding:1px 5px; border-radius:2px; margin-left:4px; vertical-align:middle; }
.dec-w { background:rgba(16,185,129,.25); color:#34d399; }
.dec-l { background:rgba(239,68,68,.25); color:#f87171; }
.dec-s { background:rgba(59,130,246,.25); color:#60a5fa; }
.dec-h { background:rgba(100,116,139,.25); color:#94a3b8; }
.dec-bs{ background:rgba(239,68,68,.15); color:#fca5a5; }

/* ── preview / no-data notices ── */
.preview-note {
  padding:24px 18px; font-size:13px; color:var(--muted);
  font-family:'IBM Plex Mono',monospace;
}

/* ── st widget overrides ── */
[data-baseweb="select"] > div,[data-baseweb="input"] > div {
  background:var(--surface) !important; border-color:var(--border) !important;
  color:var(--text) !important;
}
[data-baseweb="select"] svg { color:var(--muted) !important; }
.stSelectbox label,.stDateInput label,.stCheckbox label {
  font-family:'IBM Plex Mono',monospace !important; font-size:10px !important;
  text-transform:uppercase; letter-spacing:1px; color:var(--muted) !important;
}
button[data-testid="baseButton-primary"] {
  background:#10b981 !important; color:#000 !important;
  border:none !important; border-radius:3px !important;
  font-family:'IBM Plex Mono',monospace !important;
  font-size:11px !important; font-weight:700 !important;
}
button[data-testid="baseButton-secondary"] {
  background:#1e2330 !important; color:#e2e8f0 !important;
  border:1px solid #334155 !important; border-radius:3px !important;
  font-family:'IBM Plex Mono',monospace !important; font-size:11px !important;
}
::-webkit-scrollbar{width:4px;height:4px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px}
</style>
""", unsafe_allow_html=True)


# ── DB helpers ────────────────────────────────────────────────────────────────

@st.cache_resource
def get_conn():
    if not DB_PATH.exists():
        return None
    con = db_connect(str(DB_PATH), check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def q(sql, params=()):
    con = get_conn()
    if con is None:
        return []
    try:
        return con.execute(sql, params).fetchall()
    except Exception:
        return []


# ── Domain helpers ────────────────────────────────────────────────────────────

try:
    from zoneinfo import ZoneInfo
    _ET = ZoneInfo("America/New_York")
except Exception:
    import datetime as _dtt
    _ET = _dtt.timezone(_dtt.timedelta(hours=-4))


def fmt_odds(v) -> str:
    if v is None:
        return "N/A"
    return f"+{v}" if int(v) > 0 else str(int(v))


def fmt_ip(v) -> str:
    if v is None:
        return "N/A"
    v = float(v)
    w = int(v); f = v - w
    out = 0 if f < 0.17 else 1 if f < 0.50 else 2 if f < 0.83 else 0
    if f >= 0.83:
        w += 1; out = 0
    return f"{w}.{out}"


def fmt_avg(v) -> str:
    if v is None:
        return ".---"
    s = f"{float(v):.3f}".lstrip("0")
    return s or ".000"


def wind_label(direction: str) -> str:
    if not direction:
        return "N/A"
    cleaned = re.sub(r"^\d+\s*mph,?\s*", "", direction.strip(), flags=re.IGNORECASE)
    d = cleaned.upper()
    if d in ("NONE", ""):
        return "CALM"
    if any(k in d for k in ("OUT", "BLOWING OUT")):
        return "OUT"
    if any(k in d for k in ("IN", "BLOWING IN")):
        return "IN"
    if d in ("L TO R", "R TO L"):
        return "CROSS"
    if any(k in d for k in ("CROSS", "LEFT", "RIGHT")):
        return "CROSS"
    if any(k in d for k in ("CALM", "STILL")):
        return "CALM"
    return cleaned


def game_time_et(utc_str) -> str:
    if not utc_str:
        return ""
    try:
        from datetime import timezone
        dt = datetime.fromisoformat(str(utc_str).rstrip("Z")).replace(tzinfo=timezone.utc)
        et = dt.astimezone(_ET)
        h = et.strftime("%I").lstrip("0") or "12"
        return f"{h}:{et.strftime('%M %p ET')}"
    except Exception:
        return ""


def get_streak(team_id: int, game_date: str) -> int:
    rows = q("""
        SELECT CASE WHEN home_team_id=? THEN
                 CASE WHEN home_score>away_score THEN 'W' ELSE 'L' END
               ELSE
                 CASE WHEN away_score>home_score THEN 'W' ELSE 'L' END
               END AS result
        FROM   games
        WHERE  (home_team_id=? OR away_team_id=?)
          AND  status='Final' AND game_date < ?
        ORDER  BY game_date DESC, game_start_utc DESC LIMIT 15
    """, (team_id, team_id, team_id, game_date))
    if not rows:
        return 0
    cur = rows[0]["result"]
    count = 0
    for r in rows:
        if r["result"] == cur:
            count += 1
        else:
            break
    return count if cur == "W" else -count


def streak_label(s: int) -> str:
    if s == 0: return "—"
    return f"W{s}" if s > 0 else f"L{abs(s)}"


# ── Signal evaluation (mirrors generate_daily_brief.py thresholds) ────────────

WIND_IN_MIN   = 10;  WIND_OUT_MIN  = 10;  WIND_OUT_MVB  = 15
FAV_LOW = -130;      FAV_HIGH = -160
DOG_LOW = 0.35;      DOG_HIGH = 0.42
STREAK_T = 5;        S1_MIN = 6
S1_PL = -105;        S1_PH = -170
H3B_PF  = 98
H3B_PARKS = {
    "Wrigley Field","Coors Field","Kauffman Stadium","Globe Life Field",
    "Progressive Field","PNC Park","Comerica Park","American Family Field",
    "Great American Ball Park","Nationals Park","Camden Yards","Fenway Park",
    "Yankee Stadium","Citi Field","Oakland Coliseum",
}


def impl(odds):
    if odds is None: return None
    odds = int(odds)
    return 100/(odds+100) if odds > 0 else abs(odds)/(abs(odds)+100)


def evaluate_signals(game, home_streak):
    sigs = []
    hml = game["home_ml"]; aml = game["away_ml"]; tot = game["total_line"]
    mph = game["wind_mph"] or 0
    wdir = wind_label(game["wind_direction"] or "")
    eff  = (game["wind_effect"] or "HIGH").upper()
    venue = game["venue_name"] or ""
    pf   = game["park_factor_runs"] or 100
    ok   = eff == "HIGH"; sup = eff == "SUPPRESSED"
    ai   = impl(aml)

    if ok and not sup and wdir=="IN" and mph>=WIND_IN_MIN and hml is not None and FAV_HIGH<=hml<=FAV_LOW:
        sigs.append(f"MV-F  Wind IN {mph} mph  →  AWAY ML {fmt_odds(aml)}")
    if ok and not sup and wdir=="OUT" and mph>=WIND_OUT_MVB and ai is not None and DOG_LOW<=ai<=DOG_HIGH:
        sigs.append(f"MV-B  Wind OUT {mph} mph  →  OVER {tot}")
    if home_streak >= STREAK_T and hml is not None and FAV_HIGH<=hml<=FAV_LOW:
        sigs.append(f"S1+H2  Home W{home_streak}  →  AWAY ML {fmt_odds(aml)}")
    elif home_streak >= S1_MIN and hml is not None and S1_PH<=hml<=S1_PL:
        sigs.append(f"S1  Home W{home_streak}  →  AWAY ML {fmt_odds(aml)}")
    if ok and not sup and wdir=="OUT" and mph>=WIND_OUT_MIN and tot and venue in H3B_PARKS and pf>=H3B_PF:
        sigs.append(f"H3b  Wind OUT {mph} mph  {venue}  PF {pf}  →  OVER {tot}")
    return sigs


# ── HTML builder ──────────────────────────────────────────────────────────────

def _hl(v, threshold=0):
    """Wrap value in highlight span if > threshold."""
    if v and int(v) > threshold:
        return f"<span class='hl'>{v}</span>"
    return str(v) if v is not None else "0"


def build_batting_table(rows, home_tid):
    if not rows:
        return ""
    html = ["<table class='bs-table'><thead><tr>",
            "<th>#</th><th>Player</th><th>Pos</th>",
            "<th>AB</th><th>R</th><th>H</th><th>RBI</th>",
            "<th>HR</th><th>2B</th><th>BB</th><th>SO</th>",
            "<th>SB</th><th>AVG</th>",
            "</tr></thead><tbody>"]

    tot = {k: 0 for k in ["ab","r","h","rbi","hr","db","bb","so","sb"]}
    for r in rows:
        base = (r["batting_order"] or 0) // 100
        is_sub = (r["batting_order"] or 0) % 100 != 0
        order_cell = "↳" if is_sub else (str(base) if base else "")
        ab  = r["at_bats"] or 0;  run = r["runs"] or 0
        h   = r["hits"] or 0;     rbi = r["rbi"] or 0
        hr  = r["home_runs"] or 0; db = r["doubles"] or 0
        bb  = r["walks"] or 0;     so = r["so"] or 0
        sb  = r["stolen_bases"] or 0
        tot["ab"]+=ab; tot["r"]+=run; tot["h"]+=h; tot["rbi"]+=rbi
        tot["hr"]+=hr; tot["db"]+=db; tot["bb"]+=bb; tot["so"]+=so; tot["sb"]+=sb
        html.append(
            f"<tr><td>{order_cell}</td><td>{r['full_name'] or ''}</td>"
            f"<td>{r['position'] or ''}</td>"
            f"<td>{ab}</td><td>{_hl(run)}</td><td>{_hl(h)}</td>"
            f"<td>{_hl(rbi)}</td><td>{_hl(hr)}</td><td>{_hl(db)}</td>"
            f"<td>{_hl(bb)}</td><td>{so}</td><td>{_hl(sb)}</td>"
            f"<td>{fmt_avg(r['batting_avg'])}</td></tr>"
        )
    html.append(
        f"<tr class='totals'><td></td><td>Totals</td><td></td>"
        f"<td>{tot['ab']}</td><td>{tot['r']}</td><td>{tot['h']}</td>"
        f"<td>{tot['rbi']}</td><td>{tot['hr']}</td><td>{tot['db']}</td>"
        f"<td>{tot['bb']}</td><td>{tot['so']}</td><td>{tot['sb']}</td>"
        f"<td></td></tr>"
    )
    html.append("</tbody></table>")
    return "".join(html)


def build_pitching_table(rows):
    if not rows:
        return ""
    html = ["<table class='bs-table'><thead><tr>",
            "<th></th><th>Pitcher</th><th>IP</th>",
            "<th>H</th><th>R</th><th>ER</th>",
            "<th>BB</th><th>SO</th><th>HR</th>",
            "<th>Pitches</th><th>S%</th><th>ERA</th>",
            "</tr></thead><tbody>"]

    for i, r in enumerate(rows):
        dec_html = ""
        if r["win"]:      dec_html = "<span class='dec dec-w'>W</span>"
        elif r["loss"]:   dec_html = "<span class='dec dec-l'>L</span>"
        elif r["save"]:   dec_html = "<span class='dec dec-s'>S</span>"
        elif r["hold"]:   dec_html = "<span class='dec dec-h'>H</span>"
        elif r["blown_save"]: dec_html = "<span class='dec dec-bs'>BS</span>"
        role = "SP" if i == 0 else "RP"
        pit  = r["pitches_thrown"] or 0
        strk = r["strikes_thrown"] or 0
        spct = f"{strk/pit*100:.0f}%" if pit else "—"
        era  = f"{float(r['era']):.2f}" if r["era"] is not None else "—"
        so   = r["so"] or 0
        so_cell = f"<span class='hl'>{so}</span>" if so >= 7 else str(so)
        html.append(
            f"<tr><td>{role}</td>"
            f"<td>{r['full_name'] or ''}{dec_html}</td>"
            f"<td>{fmt_ip(r['innings_pitched'])}</td>"
            f"<td>{r['hits_allowed'] or 0}</td>"
            f"<td>{r['runs_allowed'] or 0}</td>"
            f"<td>{r['earned_runs'] or 0}</td>"
            f"<td>{r['walks_allowed'] or 0}</td>"
            f"<td>{so_cell}</td>"
            f"<td>{r['hr_allowed'] or 0}</td>"
            f"<td>{pit}</td><td>{spct}</td><td>{era}</td></tr>"
        )
    html.append("</tbody></table>")
    return "".join(html)


def build_game_card(game_pk: int, game_date: str) -> str:
    """Build complete HTML card for one game. Returns empty string on error."""
    # ── Game header ───────────────────────────────────────────────────────────
    game_rows = q("""
        SELECT g.game_pk, g.game_date, g.game_start_utc, g.status,
               g.home_score, g.away_score, g.innings_played,
               g.wind_mph, g.wind_direction, g.temp_f, g.wind_source,
               th.team_id AS home_tid, th.abbreviation AS home_abbr,
               ta.team_id AS away_tid, ta.abbreviation AS away_abbr,
               v.name AS venue_name, v.wind_effect, v.park_factor_runs,
               ml.home_ml, ml.away_ml,
               tot.total_line, tot.over_odds, tot.under_odds
        FROM   games g
        JOIN   teams th ON th.team_id = g.home_team_id
        JOIN   teams ta ON ta.team_id = g.away_team_id
        LEFT JOIN venues v ON v.venue_id = g.venue_id
        LEFT JOIN v_closing_game_odds ml  ON ml.game_pk=g.game_pk AND ml.market_type='moneyline'
        LEFT JOIN v_closing_game_odds tot ON tot.game_pk=g.game_pk AND tot.market_type='total'
        WHERE  g.game_pk = ?
    """, (game_pk,))
    if not game_rows:
        return ""
    g = dict(game_rows[0])
    is_final = g["status"] == "Final"
    hs = g["home_score"]; aws = g["away_score"]
    home_abbr = g["home_abbr"]; away_abbr = g["away_abbr"]
    home_tid  = g["home_tid"];  away_tid  = g["away_tid"]

    # Streak
    home_streak = get_streak(home_tid, game_date)
    away_streak = get_streak(away_tid, game_date)

    # Starters
    starters = {}
    for r in q("""
        SELECT gpp.team_id, p.full_name, p.era_season
        FROM   game_probable_pitchers gpp
        JOIN   players p ON p.player_id = gpp.player_id
        WHERE  gpp.game_pk = ?
    """, (game_pk,)):
        starters[r["team_id"]] = {"name": r["full_name"], "era": r["era_season"]}

    def sp_str(tid, abbr):
        d = starters.get(tid, {})
        if not d:
            return f"{abbr}: TBD"
        era = f" <span style='color:#475569'>(ERA {d['era']:.2f})</span>" if d.get("era") is not None else ""
        return f"{abbr}: {d['name']}{era}"

    # Opening line
    open_row = q("""
        SELECT home_ml, away_ml, total_line FROM game_odds
        WHERE  game_pk=? AND market_type='moneyline' AND is_opening_line=1
        ORDER  BY captured_at_utc ASC LIMIT 1
    """, (game_pk,))
    opening = dict(open_row[0]) if open_row else None

    # Signals
    signals = evaluate_signals(g, home_streak)

    # Wind display
    mph    = g["wind_mph"] or 0
    wdir   = wind_label(g["wind_direction"] or "")
    wdisp  = g["wind_direction"] or ""
    wsrc   = " (forecast)" if g.get("wind_source") == "forecast" else " (actual)"
    temp   = f"{int(g['temp_f'])}°F  " if g["temp_f"] else ""
    wind_str = f"{temp}{mph} mph {wdir}{wsrc}" if mph else "Wind N/A"

    # Score display
    if is_final:
        h_win = hs is not None and aws is not None and hs > aws
        a_win = hs is not None and aws is not None and aws > hs
        away_cls = "bs-winner" if a_win else ""
        home_cls = "bs-winner" if h_win else ""
        score_html = (f"<span class='{away_cls}'>{away_abbr} {aws}</span>"
                      f"  —  "
                      f"<span class='{home_cls}'>{home_abbr} {hs}</span>"
                      f"  <span style='font-size:15px;opacity:.5'>Final</span>")
    else:
        score_html = (f"{away_abbr} @ {home_abbr}"
                      f"  <span style='font-size:15px;opacity:.5'>{g['status']}</span>")

    # Odds bar
    hml = g["home_ml"]; aml = g["away_ml"]; total = g["total_line"]
    ml_str  = f"{home_abbr} {fmt_odds(hml)} / {away_abbr} {fmt_odds(aml)}" if hml else "N/A"
    tot_str = f"O/U {total}" if total else "N/A"
    clv_str = ""
    if opening and hml and aml:
        ch = int(hml) - int(opening["home_ml"] or hml)
        ca = int(aml) - int(opening["away_ml"] or aml)
        open_str = f"{home_abbr} {fmt_odds(opening['home_ml'])} / {away_abbr} {fmt_odds(opening['away_ml'])}"
        clv_str  = (f"<span><span class='lbl'>Opening</span>{open_str}</span>"
                    f"<span><span class='lbl'>CLV</span>{home_abbr} {ch:+d}  {away_abbr} {ca:+d}</span>")

    sig_bar = ""
    if signals:
        sig_bar = "<div class='signal-bar'>⚑  " + "  |  ".join(signals) + "</div>"
    else:
        sig_bar = "<div class='nosig-bar'>—  No model signals fired</div>"

    # ── Batting & pitching data ───────────────────────────────────────────────
    batting_rows = q("""
        SELECT pgs.team_id, pgs.batting_order, pgs.position,
               p.full_name, pgs.at_bats, pgs.runs, pgs.hits, pgs.rbi,
               pgs.home_runs, pgs.doubles, pgs.triples,
               pgs.walks AS bb, pgs.strikeouts_bat AS so,
               pgs.stolen_bases, pgs.batting_avg
        FROM   player_game_stats pgs
        JOIN   players p ON p.player_id = pgs.player_id
        WHERE  pgs.game_pk=? AND pgs.player_role='batter'
        ORDER  BY pgs.team_id, pgs.batting_order
    """, (game_pk,))

    pitching_rows = q("""
        SELECT pgs.team_id, p.full_name, pgs.innings_pitched,
               pgs.pitches_thrown, pgs.strikes_thrown,
               pgs.earned_runs, pgs.runs_allowed, pgs.hits_allowed,
               pgs.hr_allowed, pgs.walks_allowed, pgs.strikeouts_pit AS so,
               pgs.win, pgs.loss, pgs.save, pgs.hold, pgs.blown_save, pgs.era
        FROM   player_game_stats pgs
        JOIN   players p ON p.player_id = pgs.player_id
        WHERE  pgs.game_pk=? AND pgs.player_role='pitcher'
        ORDER  BY pgs.team_id, pgs.innings_pitched DESC
    """, (game_pk,))

    # Split by team
    away_bat  = [r for r in batting_rows  if r["team_id"] == away_tid]
    home_bat  = [r for r in batting_rows  if r["team_id"] == home_tid]
    away_pit  = [r for r in pitching_rows if r["team_id"] == away_tid]
    home_pit  = [r for r in pitching_rows if r["team_id"] == home_tid]
    has_data  = bool(away_bat or home_bat or away_pit or home_pit)

    box_html = ""
    if has_data:
        box_html = (
            f"<div class='sec-lbl'>Batting — {away_abbr}</div>"
            + build_batting_table(away_bat, home_tid)
            + f"<div class='sec-lbl'>Batting — {home_abbr} (h)</div>"
            + build_batting_table(home_bat, home_tid)
            + f"<div class='sec-lbl'>Pitching — {away_abbr}</div>"
            + build_pitching_table(away_pit)
            + f"<div class='sec-lbl'>Pitching — {home_abbr} (h)</div>"
            + build_pitching_table(home_pit)
        )
    else:
        box_html = (
            "<div class='preview-note'>"
            "⚑  Box score not yet available — game hasn't started or stats haven't loaded."
            "</div>"
        )

    return f"""
<div class='bs-card'>
  <div class='bs-header'>
    <div class='bs-score'>{score_html}</div>
    <div class='bs-meta'>{g['game_date']}  ·  {game_time_et(g['game_start_utc'])}  ·  {g['venue_name'] or ''}</div>
  </div>
  <div class='bs-info'>
    <span><strong>Wind</strong> {wind_str}</span>
    <span><strong>SP {away_abbr}</strong> {sp_str(away_tid, away_abbr)}</span>
    <span><strong>SP {home_abbr}</strong> {sp_str(home_tid, home_abbr)}</span>
    <span><strong>Streaks</strong> {away_abbr} {streak_label(away_streak)}  |  {home_abbr} {streak_label(home_streak)}</span>
  </div>
  <div class='odds-bar'>
    <span><span class='lbl'>Closing ML</span>{ml_str}</span>
    <span><span class='lbl'>Total</span>{tot_str}</span>
    {clv_str}
  </div>
  {sig_bar}
  {box_html}
</div>
"""


# ── Top bar ───────────────────────────────────────────────────────────────────

db_ok = get_conn() is not None
db_badge = '<span class="badge bg">DB CONNECTED</span>' if db_ok else '<span class="badge br" style="background:rgba(239,68,68,.15);color:#ef4444;border:1px solid rgba(239,68,68,.3);">DB NOT FOUND</span>'

st.markdown(f"""
<div class="topbar">
  <div class="topbar-logo">MLB<span>Scout</span></div>
  <div class="topbar-div"></div>
  <div class="topbar-sub">Box Score Viewer</div>
  <div class="topbar-right">
    <span class="badge bb">{date.today().strftime('%b %d, %Y')}</span>
    {db_badge}
  </div>
</div>
""", unsafe_allow_html=True)

st.markdown("<div class='main-wrap'>", unsafe_allow_html=True)

if not db_ok:
    st.error(f"Database not found at {DB_PATH}. Run create_db.py and load_mlb_stats.py first.")
    st.stop()

# ── Controls ──────────────────────────────────────────────────────────────────

c1, c2, c3, c4 = st.columns([1.2, 2.5, 1, 1])

with c1:
    sel_date = st.date_input(
        "Game date",
        value=date.today() - timedelta(days=1),
        key="bs_date",
    )

# Load game list for chosen date
game_list = q("""
    SELECT g.game_pk, g.game_start_utc, g.status,
           ta.abbreviation AS away, th.abbreviation AS home
    FROM   games g
    JOIN   teams th ON th.team_id = g.home_team_id
    JOIN   teams ta ON ta.team_id = g.away_team_id
    WHERE  g.game_date = ? AND g.game_type = 'R'
    ORDER  BY g.game_start_utc
""", (str(sel_date),))

with c2:
    if not game_list:
        st.selectbox("Game", ["No games found for this date"], key="bs_game_sel")
        sel_game_pk = None
    else:
        opts = {"All games": None}
        for r in game_list:
            label = f"{r['away']}@{r['home']}  [{r['status']}]"
            opts[label] = r["game_pk"]
        choice = st.selectbox("Game", list(opts.keys()), key="bs_game_sel")
        sel_game_pk = opts[choice]

with c3:
    auto_refresh = st.checkbox("Auto-refresh (60s)", key="bs_auto", value=False)

with c4:
    st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
    load_btn = st.button("⟳  Load", key="bs_load", type="primary",
                          use_container_width=True)

# Auto-refresh wiring
if auto_refresh:
    import time
    st.markdown(
        f"<div style='font-size:11px;color:#475569;font-family:IBM Plex Mono,monospace;"
        f"margin-bottom:12px;'>Auto-refresh active — next refresh at "
        f"{(datetime.now().replace(second=0,microsecond=0) + timedelta(minutes=1)).strftime('%H:%M:%S')}"
        f"</div>",
        unsafe_allow_html=True,
    )
    time.sleep(60)
    st.rerun()

# ── Render ────────────────────────────────────────────────────────────────────

if not game_list:
    st.markdown(
        "<div style='padding:40px 0;text-align:center;color:#475569;"
        "font-family:IBM Plex Mono,monospace;font-size:13px;'>"
        f"No regular-season games found for {sel_date}.<br>"
        "Run load_today.py or load_mlb_stats.py to populate this date.</div>",
        unsafe_allow_html=True,
    )
else:
    games_to_show = (
        [r["game_pk"] for r in game_list]
        if sel_game_pk is None
        else [sel_game_pk]
    )

    # Build all cards into one HTML blob — avoids per-game component overhead
    all_cards_html = ""
    for gpk in games_to_show:
        all_cards_html += build_game_card(gpk, str(sel_date))

    # Estimate height: Final game ≈ 900px; Scheduled ≈ 260px
    n_final = sum(1 for r in game_list
                  if (sel_game_pk is None or r["game_pk"] == sel_game_pk)
                  and r["status"] == "Final")
    n_sched = len(games_to_show) - n_final
    est_height = n_final * 920 + n_sched * 280 + 40

    # Inject the shared card CSS inline so the component is self-contained
    card_css = """
<style>
body { margin:0; padding:0; background:transparent; }
.bs-card {
  background:#14171f; border:1px solid #1e2330;
  border-radius:6px; margin-bottom:20px; overflow:hidden;
  font-family:'IBM Plex Mono',monospace;
}
.bs-header { background:#1f3864; padding:14px 18px; }
.bs-score  { font-family:'Bebas Neue',cursive; font-size:32px;
  letter-spacing:1.5px; color:#fff; line-height:1; }
.bs-winner { color:#7ec8e3; }
.bs-meta   { font-size:11px; color:rgba(255,255,255,.6); margin-top:4px; }
.bs-info {
  display:flex; flex-wrap:wrap; gap:.4rem 1.8rem;
  padding:9px 18px; background:#0f1420; border-bottom:1px solid #1e2330;
  font-size:12px;
}
.bs-info strong { color:#60a5fa; }
.bs-info span   { color:#94a3b8; }
.signal-bar {
  background:rgba(245,158,11,.08); border-bottom:2px solid #f59e0b;
  padding:7px 18px; font-size:12px; font-weight:600; color:#fbbf24;
}
.nosig-bar {
  background:rgba(16,185,129,.05); border-bottom:1px solid rgba(16,185,129,.2);
  padding:7px 18px; font-size:12px; color:#4ade80;
}
.odds-bar {
  display:flex; flex-wrap:wrap; gap:.4rem 1.8rem;
  padding:7px 18px; background:#14171f; border-bottom:1px solid #1e2330;
  font-size:12px;
}
.odds-bar .lbl { color:#475569; margin-right:4px; }
.sec-lbl {
  font-size:9px; font-weight:700; letter-spacing:.12em;
  text-transform:uppercase; color:#475569;
  padding:6px 18px 3px; background:#0a0c10;
  border-bottom:1px solid #1e2330; border-top:1px solid #1e2330;
}
.bs-table { width:100%; border-collapse:collapse; font-size:12px; }
.bs-table th {
  background:#070809; color:#475569; font-weight:500; font-size:9px;
  text-transform:uppercase; letter-spacing:1px; padding:5px 9px;
  border-bottom:1px solid #1e2330; text-align:right;
}
.bs-table th:first-child,.bs-table th:nth-child(2) { text-align:left; }
.bs-table td {
  padding:4px 9px; border-bottom:1px solid rgba(30,35,48,.8);
  color:#e2e8f0; text-align:right; font-size:12px;
}
.bs-table td:first-child { text-align:center; color:#475569; font-size:10px; width:26px; }
.bs-table td:nth-child(2){ text-align:left; font-weight:600; }
.bs-table tr:last-child td { border-bottom:none; }
.bs-table tr.totals td { background:rgba(30,35,48,.6); font-weight:700;
  border-top:1px solid #1e2330; }
.hl  { color:#60a5fa; font-weight:700; }
.dec { display:inline-block; font-size:9px; font-weight:700;
  padding:1px 5px; border-radius:2px; margin-left:4px; vertical-align:middle; }
.dec-w  { background:rgba(16,185,129,.25); color:#34d399; }
.dec-l  { background:rgba(239,68,68,.25);  color:#f87171; }
.dec-s  { background:rgba(59,130,246,.25); color:#60a5fa; }
.dec-h  { background:rgba(100,116,139,.25);color:#94a3b8; }
.dec-bs { background:rgba(239,68,68,.15);  color:#fca5a5; }
.preview-note { padding:22px 18px; font-size:13px; color:#475569; }
</style>
"""

    components.html(
        card_css + all_cards_html,
        height=est_height,
        scrolling=False,
    )

st.markdown("</div>", unsafe_allow_html=True)
