"""
╔══════════════════════════════════════════════════════════════╗
║   MLB Scout — Analytics Platform                        ║
║   Run with:  streamlit run scout.py                      ║
║                                                              ║
║   Segments:                                                  ║
║     1. Data Explorer   — reference data, stats, odds lookup  ║
║     2. Model Workbench — backtest + live predictions         ║
║     3. Operations      — data load triggers + health         ║
║     4. Scorecard       — model performance (backtest / live) ║
╚══════════════════════════════════════════════════════════════╝
"""

# CHANGE LOG (latest first)
# -------------------------
# 2026-04-13 22:15 ET  Default DB from get_db_path(); repo root on sys.path for core.* imports.
# 2026-04-13 16:24 ET  Refactor: route sqlite3.connect() calls through core.db.connection.connect().

import os
import sqlite3
import subprocess
import sys
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
import streamlit as st

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from core.db.connection import connect as db_connect, get_db_path

# ─────────────────────────────────────────────────────────────
#  PAGE CONFIG  (must be first Streamlit call)
#
#  Theme is controlled by .streamlit/config.toml — that file
#  sets the background, surface, text, and primaryColor that
#  Streamlit uses for its own native widgets (selectboxes,
#  sliders, tabs, progress bars).  The custom CSS below handles
#  everything else (topbar, badges, tables, buttons).
#
#  Two TOML files used by this app:
#    .streamlit/config.toml   — theme colours and font (shared with app.py)
#    .streamlit/secrets.toml  — optional password gate (create when sharing)
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="MLB Scout",
    page_icon="⚾",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────
#  OPTIONAL PASSWORD GATE
#
#  Inactive by default (personal local use).
#  To enable: create .streamlit/secrets.toml containing:
#
#      [auth]
#      password        = "your-app-password"
#      admin_password  = "your-admin-password"
#
#  Then flip the flags below.
#  admin_password guards the Operations segment only.
#  password guards the entire app.
# ─────────────────────────────────────────────────────────────
ENABLE_PASSWORD_GATE  = False   # ← set True when sharing the app
ENABLE_OPS_ADMIN_GATE = False   # ← set True before any public deployment

def _password_gate(secret_key: str, session_key: str, prompt: str) -> None:
    """Renders a centred password form.  Stops execution until correct."""
    try:
        expected = st.secrets["auth"][secret_key]
    except (KeyError, FileNotFoundError):
        st.error(
            "secrets.toml missing or [auth] section not found. "
            f"Add [auth] {secret_key} = \"...\" to .streamlit/secrets.toml.",
            icon="🔒",
        )
        st.stop()

    if st.session_state.get(session_key):
        return                          # already authenticated this session

    st.markdown("<div style='height:100px'></div>", unsafe_allow_html=True)
    _, mid, _ = st.columns([1, 2, 1])
    with mid:
        st.markdown(
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:11px;"
            f"color:#64748b;text-transform:uppercase;letter-spacing:1px;"
            f"margin-bottom:20px;text-align:center;'>{prompt}</div>",
            unsafe_allow_html=True,
        )
        entered = st.text_input("Password", type="password", key=f"_pw_{session_key}")
        if st.button("Unlock →", key=f"_btn_{session_key}", type="primary", use_container_width=True):
            if entered == expected:
                st.session_state[session_key] = True
                st.rerun()
            else:
                st.error("Incorrect password.", icon="🔒")
    st.stop()

if ENABLE_PASSWORD_GATE:
    _password_gate("password", "_auth_app", "MLB Scout — Enter Password to Continue")

# ─────────────────────────────────────────────────────────────
#  DATABASE PATH
# ─────────────────────────────────────────────────────────────
DB_PATH = get_db_path()
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))

# ─────────────────────────────────────────────────────────────
#  GLOBAL CSS — inherits app.py palette exactly
# ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:ital,wght@0,300;0,400;0,600;1,300&display=swap');

:root {
  --bg:      #0d0f14; --surface: #14171f; --border: #1e2330;
  --accent:  #e8a020; --accent2: #3b82f6; --accent3: #10b981;
  --danger:  #ef4444; --text:    #e2e8f0; --muted:   #64748b;
  --code-bg: #0a0c10;
}

html, body,
[data-testid="stAppViewContainer"],
[data-testid="stMain"],
.main, .block-container {
  background: var(--bg) !important;
  color: var(--text) !important;
  font-family: 'IBM Plex Sans', sans-serif !important;
  padding: 0 !important; margin: 0 !important; max-width: 100% !important;
}
[data-testid="stHeader"],[data-testid="stToolbar"],
[data-testid="collapsedControl"],footer { display:none !important; }


/* ── TOP BAR ── */
.topbar {
  position:fixed; top:0; left:0; right:0; z-index:1000;
  height:56px; background:var(--surface); border-bottom:1px solid var(--border);
  display:flex; align-items:center; padding:0 20px; gap:16px;
}
.topbar-logo { font-family:'Bebas Neue',sans-serif; font-size:22px; letter-spacing:2px; color:var(--accent); }
.topbar-logo span { color:var(--text); }
.topbar-div  { width:1px; height:28px; background:var(--border); }
.topbar-sub  { font-size:11px; color:var(--muted); font-family:'IBM Plex Mono',monospace; letter-spacing:1px; text-transform:uppercase; }
.topbar-right{ margin-left:auto; display:flex; align-items:center; gap:10px; }
.badge { font-family:'IBM Plex Mono',monospace; font-size:10px; padding:3px 8px; border-radius:2px; letter-spacing:.5px; text-transform:uppercase; }
.bg  { background:rgba(16,185,129,.15);  color:var(--accent3); border:1px solid rgba(16,185,129,.3); }
.bb  { background:rgba(59,130,246,.15);  color:var(--accent2); border:1px solid rgba(59,130,246,.3); }
.bo  { background:rgba(232,160,32,.15);  color:var(--accent);  border:1px solid rgba(232,160,32,.3); }
.br  { background:rgba(239,68,68,.15);   color:var(--danger);  border:1px solid rgba(239,68,68,.3); }

/* ── SIDEBAR ── */
/* ── NATIVE SIDEBAR ── */
[data-testid="stSidebar"] {
  background: var(--surface) !important;
  border-right: 1px solid var(--border) !important;
  padding-top: 16px !important;
}
[data-testid="stSidebar"] > div { padding-top: 0 !important; }

.sb-label {
  font-family:'IBM Plex Mono',monospace; font-size:9px;
  text-transform:uppercase; letter-spacing:2px; color:var(--muted);
  padding:18px 16px 6px;
}

/* ── MAIN AREA ── */
.main-wrap { margin-top:56px; padding:28px 32px; }

/* ── PANEL HEADER ── */
.ph { padding:0 0 20px 0; margin-bottom:24px; border-bottom:1px solid var(--border); }
.ph-super { font-family:'IBM Plex Mono',monospace; font-size:10px; letter-spacing:1.5px; text-transform:uppercase; color:var(--accent); margin-bottom:6px; }
.ph-title { font-family:'Bebas Neue',sans-serif; font-size:32px; letter-spacing:1.5px; color:var(--text); line-height:1; }
.ph-desc  { font-size:12px; color:var(--muted); margin-top:6px; line-height:1.6; }

/* ── STAT CARDS ── */
.stat-row { display:flex; gap:12px; margin-bottom:20px; flex-wrap:wrap; }
.stat-card {
  background:var(--surface); border:1px solid var(--border); border-radius:5px;
  padding:14px 18px; min-width:140px; flex:1; position:relative; overflow:hidden;
}
.stat-card::after { content:''; position:absolute; top:0; left:0; right:0; height:2px; background:var(--cc, var(--accent)); }
.stat-lbl { font-family:'IBM Plex Mono',monospace; font-size:9px; text-transform:uppercase; letter-spacing:1.5px; color:var(--muted); margin-bottom:6px; }
.stat-val { font-family:'Bebas Neue',sans-serif; font-size:28px; letter-spacing:1px; color:var(--text); line-height:1; }
.stat-sub { font-size:10px; color:var(--muted); margin-top:4px; }

/* ── DATA TABLE ── */
.data-table { width:100%; border-collapse:collapse; font-size:12px; font-family:'IBM Plex Mono',monospace; }
.data-table th { background:var(--surface); color:var(--muted); font-weight:500; font-size:10px; text-transform:uppercase; letter-spacing:1px; padding:8px 12px; border-bottom:1px solid var(--border); text-align:left; }
.data-table td { padding:7px 12px; border-bottom:1px solid rgba(30,35,48,.6); color:var(--text); }
.data-table tr:hover td { background:rgba(255,255,255,.02); }
.tbl-wrap { background:var(--surface); border:1px solid var(--border); border-radius:5px; overflow:hidden; margin-bottom:20px; }

/* ── QUERY CARD ── */
.qcard {
  background:var(--surface); border:1px solid var(--border); border-radius:5px;
  padding:14px 16px; margin-bottom:10px;
}
.qcard-title { font-family:'IBM Plex Mono',monospace; font-size:11px; color:var(--accent2); margin-bottom:4px; font-weight:600; }
.qcard-desc  { font-size:11px; color:var(--muted); line-height:1.5; }

/* ── SECTION DIVIDER ── */
.sec-div { border:none; border-top:1px solid var(--border); margin:24px 0; }

/* ── STATUS PILL ── */
.pill { display:inline-block; font-family:'IBM Plex Mono',monospace; font-size:10px; padding:2px 8px; border-radius:10px; text-transform:uppercase; letter-spacing:.5px; }
.pill-ok  { background:rgba(16,185,129,.15);  color:var(--accent3); border:1px solid rgba(16,185,129,.3); }
.pill-err { background:rgba(239,68,68,.15);   color:var(--danger);  border:1px solid rgba(239,68,68,.3); }
.pill-warn{ background:rgba(232,160,32,.15);  color:var(--accent);  border:1px solid rgba(232,160,32,.3); }
.pill-info{ background:rgba(59,130,246,.15);  color:var(--accent2); border:1px solid rgba(59,130,246,.3); }

/* ── LOG BLOCK ── */
.log-block { background:var(--code-bg); border:1px solid var(--border); border-radius:4px; padding:14px; font-family:'IBM Plex Mono',monospace; font-size:11px; color:#94a3b8; white-space:pre-wrap; max-height:320px; overflow-y:auto; margin-top:10px; line-height:1.7; }

/* ── BUTTONS ── */
button[data-testid="baseButton-secondary"] {
  background:#1e2330 !important; color:#e2e8f0 !important;
  border:1px solid #334155 !important; border-radius:3px !important;
  font-family:'IBM Plex Mono',monospace !important; font-size:11px !important;
}
button[data-testid="baseButton-secondary"]:hover {
  background:#263044 !important; color:#e8a020 !important; border-color:#e8a020 !important;
}
button[data-testid="baseButton-primary"] {
  background:#10b981 !important; color:#000 !important;
  border:none !important; border-radius:3px !important;
  font-family:'IBM Plex Mono',monospace !important; font-size:11px !important; font-weight:700 !important;
}
button[data-testid="baseButton-primary"]:hover { background:#0ed494 !important; color:#000 !important; }

/* ── SELECTS / INPUTS ── */
[data-baseweb="select"] > div,
[data-baseweb="input"] > div {
  background:var(--surface) !important; border-color:var(--border) !important; color:var(--text) !important;
}
[data-baseweb="select"] svg { color:var(--muted) !important; }
.stSelectbox label, .stNumberInput label, .stDateInput label, .stTextInput label {
  font-family:'IBM Plex Mono',monospace !important; font-size:10px !important;
  text-transform:uppercase; letter-spacing:1px; color:var(--muted) !important;
}

/* ── SCROLLBARS ── */
::-webkit-scrollbar{width:4px;height:4px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px}
::-webkit-scrollbar-thumb:hover{background:var(--muted)}

/* ── ALERT / WARNING BOXES ── */
.warn-box { background:rgba(232,160,32,.08); border:1px solid rgba(232,160,32,.3); border-radius:4px; padding:12px 16px; font-size:12px; color:var(--accent); margin-bottom:16px; }
.info-box { background:rgba(59,130,246,.08); border:1px solid rgba(59,130,246,.3); border-radius:4px; padding:12px 16px; font-size:12px; color:var(--accent2); margin-bottom:16px; }
.ok-box   { background:rgba(16,185,129,.08); border:1px solid rgba(16,185,129,.3); border-radius:4px; padding:12px 16px; font-size:12px; color:var(--accent3); margin-bottom:16px; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
#  DATABASE HELPERS
# ─────────────────────────────────────────────────────────────

@st.cache_resource
def get_connection():
    if not os.path.exists(DB_PATH):
        return None
    con = db_connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    con = get_connection()
    if con is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(sql, con, params=params)
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()


def db_available() -> bool:
    return get_connection() is not None


# ─────────────────────────────────────────────────────────────
#  SESSION STATE
# ─────────────────────────────────────────────────────────────

VIEWS = ["home", "explorer", "workbench", "operations", "scorecard"]

def init_state():
    if "view" not in st.session_state:
        st.session_state.view = "home"

def go(view: str):
    st.session_state.view = view
    st.rerun()

init_state()


# ─────────────────────────────────────────────────────────────
#  LAYOUT  — navigation lives in st.sidebar; main content is direct
# ─────────────────────────────────────────────────────────────


# ═════════════════════════════════════════════════════════════
#  TOP BAR
# ═════════════════════════════════════════════════════════════
db_status_html = (
    '<span class="badge bg">DB CONNECTED</span>'
    if db_available()
    else '<span class="badge br">DB NOT FOUND</span>'
)
today_str = date.today().strftime("%b %d, %Y")

st.markdown(f"""
<div class="topbar">
  <div class="topbar-logo">MLB<span>Scout</span></div>
  <div class="topbar-div"></div>
  <div class="topbar-sub">Analytics Platform</div>
  <div class="topbar-right">
    <span class="badge bb">{today_str}</span>
    {db_status_html}
  </div>
</div>
""", unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════
#  SIDEBAR — uses Streamlit's native sidebar
# ═════════════════════════════════════════════════════════════
NAV = [
    ("⌂  Home",            "home"),
    ("◎  Data Explorer",   "explorer"),
    ("⚗  Model Workbench", "workbench"),
    ("⚙  Operations",      "operations"),
    ("◈  Scorecard",       "scorecard"),
]

with st.sidebar:
    st.markdown('<div class="sb-label">Navigation</div>', unsafe_allow_html=True)
    for label, view_key in NAV:
        active = st.session_state.view == view_key
        if st.button(label, key=f"nav_{view_key}", use_container_width=True,
                     type="primary" if active else "secondary"):
            go(view_key)


# ═════════════════════════════════════════════════════════════
#  HELPERS — reusable UI components
# ═════════════════════════════════════════════════════════════

def panel_header(super_label: str, title: str, desc: str = ""):
    st.markdown(f"""
    <div class="ph">
      <div class="ph-super">{super_label}</div>
      <div class="ph-title">{title}</div>
      {"" if not desc else f'<div class="ph-desc">{desc}</div>'}
    </div>
    """, unsafe_allow_html=True)


def stat_cards(cards: list):
    """cards = list of (label, value, sub, color_css_var)"""
    cols = st.columns(len(cards), gap="small")
    for col, (lbl, val, sub, color) in zip(cols, cards):
        with col:
            st.markdown(f"""
            <div class="stat-card" style="--cc:{color}">
              <div class="stat-lbl">{lbl}</div>
              <div class="stat-val">{val}</div>
              {"" if not sub else f'<div class="stat-sub">{sub}</div>'}
            </div>
            """, unsafe_allow_html=True)


def show_df(df: pd.DataFrame, height: int = 400):
    if df.empty:
        st.markdown('<div class="info-box">No results for this query.</div>', unsafe_allow_html=True)
        return
    st.dataframe(
        df,
        use_container_width=True,
        height=height,
        hide_index=True,
    )


def no_db_warning():
    st.markdown(f"""
    <div class="warn-box">
      ⚠ Database not found at <code>{DB_PATH}</code><br>
      Run <code>python create_db.py</code> from your mlb_stats folder first.
    </div>
    """, unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════
#  VIEW: HOME
# ═════════════════════════════════════════════════════════════

def view_home():
    # ── Scope toggle — must come before queries so we know which scope to use ──
    # Render the banner first, then the checkbox, then the cards
    st.markdown(f"""
    <div style="background:linear-gradient(135deg,rgba(232,160,32,.08),rgba(59,130,246,.04));
                border:1px solid rgba(232,160,32,.2);border-radius:6px;
                padding:28px 32px;margin-bottom:20px;position:relative;overflow:hidden;">
      <div style="position:absolute;right:24px;top:50%;transform:translateY(-50%);
                  font-size:80px;opacity:.06;">⚾</div>
      <div style="font-family:'Bebas Neue',sans-serif;font-size:36px;letter-spacing:2px;
                  color:#e8a020;line-height:1;margin-bottom:8px;">MLB Scout</div>
      <div style="color:#64748b;font-size:13px;line-height:1.7;max-width:580px;">
        Personal analytics platform for the MLB backtesting database.<br>
        Explore stats and odds, run model evaluations, trigger data loads, and track prediction performance.
      </div>
    </div>
    """, unsafe_allow_html=True)

    all_time = st.checkbox("Show all-time stats", value=False, key="home_alltime")

    # Quick DB stats for the hero cards
    if db_available():
        # Determine current season from DB
        cur_season_row = query(
            "SELECT MAX(season) AS s FROM games WHERE game_type='R' AND status='Final'"
        )
        cur_season = int(cur_season_row["s"].iloc[0]) if not cur_season_row.empty and cur_season_row["s"].iloc[0] else 2026

        if all_time:
            scope_label  = "all-time · 2015 – present"
            season_where = ""                                  # no season filter
            season_param = {}
        else:
            scope_label  = f"{cur_season} season to date"
            season_where = "AND season = :s"
            season_param = {"s": cur_season}

        games_row = query(
            f"SELECT COUNT(*) AS n FROM games WHERE game_type='R' {season_where}",
            season_param
        )
        pgs_row = query(
            f"""SELECT COUNT(*) AS n FROM player_game_stats pgs
                JOIN games g ON g.game_pk = pgs.game_pk
                WHERE g.game_type = 'R' {season_where}""",
            season_param
        )
        pbp_row = query(
            f"""SELECT COUNT(*) AS n FROM play_by_play pbp
                JOIN games g ON g.game_pk = pbp.game_pk
                WHERE g.game_type = 'R' {season_where}""",
            season_param
        )
        odds_row = query(
            f"""SELECT COUNT(*) AS n FROM game_odds go
                JOIN games g ON g.game_pk = go.game_pk
                WHERE g.game_type = 'R' {season_where}""",
            season_param
        )

        try:
            if all_time:
                fires_row = query(
                    "SELECT COUNT(*) AS n FROM brief_picks WHERE session IN ('primary','early','afternoon','late')"
                )
            else:
                fires_row = query(
                    """SELECT COUNT(*) AS n FROM brief_picks
                       WHERE  session IN ('primary','early','afternoon','late')
                         AND  game_date >= date(:s || '-01-01')""",
                    {"s": str(cur_season)}
                )
            fires_n = f"{int(fires_row['n'].iloc[0]):,}" if not fires_row.empty else "—"
        except Exception:
            fires_n = "—"

        games_n = f"{int(games_row['n'].iloc[0]):,}" if not games_row.empty else "—"
        pgs_n   = f"{int(pgs_row['n'].iloc[0]):,}"   if not pgs_row.empty   else "—"
        pbp_n   = f"{int(pbp_row['n'].iloc[0]):,}"   if not pbp_row.empty   else "—"
        odds_n  = f"{int(odds_row['n'].iloc[0]):,}"  if not odds_row.empty  else "—"
    else:
        scope_label = "—"
        games_n = pgs_n = pbp_n = odds_n = fires_n = "—"

    # Paper bank and season coverage
    try:
        bank_row = query(
            """SELECT 500 + COALESCE(SUM(pnl_dollars),0) AS bank,
                      SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END)  AS wins,
                      SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) AS losses
               FROM daily_pnl dp JOIN games g ON g.game_pk=dp.game_pk
               WHERE g.game_type='R' AND g.season=:s""", {"s": cur_season}
        )
        if not bank_row.empty and bank_row["bank"].iloc[0] is not None:
            bank_v = f"${float(bank_row['bank'].iloc[0]):.2f}"
            bank_wl = f"W:{int(bank_row['wins'].iloc[0] or 0)} L:{int(bank_row['losses'].iloc[0] or 0)}"
        else:
            bank_v, bank_wl = "$500.00", "W:0 L:0"
    except Exception:
        bank_v, bank_wl = "—", ""

    try:
        cov_row = query(
            """SELECT COUNT(*) AS total,
                      SUM(CASE WHEN wind_mph IS NOT NULL THEN 1 ELSE 0 END) AS wind,
                      (SELECT COUNT(DISTINCT gpp.game_pk) FROM game_probable_pitchers gpp
                       JOIN games g2 ON g2.game_pk=gpp.game_pk WHERE g2.season=:s AND g2.game_type='R') AS starters
               FROM games g WHERE g.season=:s AND g.game_type='R' AND g.status='Final'""",
            {"s": cur_season}
        )
        if not cov_row.empty:
            tot  = int(cov_row["total"].iloc[0] or 0)
            wnd  = int(cov_row["wind"].iloc[0] or 0)
            stt  = int(cov_row["starters"].iloc[0] or 0)
            wind_pct = f"{wnd/tot*100:.0f}%" if tot else "—"
            stt_pct  = f"{stt/tot*100:.0f}%" if tot else "—"
        else:
            wind_pct = stt_pct = "—"
    except Exception:
        wind_pct = stt_pct = "—"

    stat_cards([
        ("Regular Season Games",  games_n, scope_label,          "#e8a020"),
        ("Player-Game Stat Rows", pgs_n,   "batting + pitching",  "#3b82f6"),
        ("Odds Snapshots",        odds_n,  "game lines",          "#8b5cf6"),
        ("Signal Fires",          fires_n, scope_label,           "#10b981"),
    ])
    stat_cards([
        ("Paper Bank",            bank_v,    bank_wl,                "#10b981"),
        ("Wind Coverage",         wind_pct,  f"{scope_label}",       "#3b82f6"),
        ("Starters on File",      stt_pct,   "of final games",       "#8b5cf6"),
        ("Play-by-Play Rows",     pbp_n,     scope_label,            "#e8a020"),
    ])

    st.markdown("<hr class='sec-div'>", unsafe_allow_html=True)

    # ── Yesterday's Results ────────────────────────────────
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    with st.expander(f"Yesterday's Results — {yesterday}", expanded=False):
        try:
            yest_games = query("""
                SELECT ta.abbreviation AS away, g.away_score,
                       th.abbreviation AS home, g.home_score,
                       g.game_type, g.status,
                       g.game_pk
                FROM   games g
                JOIN   teams th ON th.team_id = g.home_team_id
                JOIN   teams ta ON ta.team_id = g.away_team_id
                WHERE  g.game_date_et = ?
                  AND  g.status    = 'Final'
                  AND  g.game_type = 'R'
                ORDER  BY g.game_pk
            """, (yesterday,))

            yest_pick = query("""
                SELECT session, picks_count, games_covered, output_file, generated_at
                FROM   brief_log
                WHERE  game_date = ?
                  AND  session   = 'primary'
                ORDER  BY generated_at DESC LIMIT 1
            """, (yesterday,))

            if yest_games.empty:
                st.markdown(
                    f'<div class="info-box">No Final games found for {yesterday}. '
                    f'Run <code>load_mlb_stats.py</code> to pull yesterday\'s results.</div>',
                    unsafe_allow_html=True)
            else:
                rows_html = ""
                for _, r in yest_games.iterrows():
                    away_bold = "<strong>" if r.away_score > r.home_score else ""
                    away_end  = "</strong>" if r.away_score > r.home_score else ""
                    home_bold = "<strong>" if r.home_score > r.away_score else ""
                    home_end  = "</strong>" if r.home_score > r.away_score else ""
                    type_tag  = f'<span style="color:#475569;font-size:13px;"> [{r.game_type}]</span>' \
                                if r.game_type != 'R' else ''
                    rows_html += (
                        f'<div style="display:flex;justify-content:space-between;align-items:center;'
                        f'padding:9px 0;border-bottom:1px solid #1e2330;">'
                        f'<span style="font-family:\'IBM Plex Mono\',monospace;font-size:15px;font-weight:700;">'
                        f'{away_bold}{r.away}{away_end} {r.away_score} &nbsp;@&nbsp; '
                        f'{home_bold}{r.home}{home_end} {r.home_score}'
                        f'{type_tag}</span>'
                        f'</div>'
                    )

                if not yest_pick.empty:
                    p = yest_pick.iloc[0]
                    n_picks = int(p.picks_count) if p.picks_count else 0
                    pick_badge_color = "#10b981" if n_picks > 0 else "#475569"
                    pick_badge = (
                        f'<span style="background:{pick_badge_color}22;color:{pick_badge_color};'
                        f'border:1px solid {pick_badge_color}44;border-radius:3px;'
                        f'padding:4px 10px;font-size:13px;font-weight:700;font-family:\'IBM Plex Mono\',monospace;">'
                        f'{"⚡ " + str(n_picks) + " signal pick(s) fired" if n_picks > 0 else "No signal picks"}'
                        f'</span>'
                    )
                    file_hint = (f'<span style="color:#475569;font-size:13px;font-weight:600;margin-left:12px;">'
                                 f'Brief: {p.output_file or "console only"}</span>'
                                 if p.output_file else '')
                else:
                    pick_badge = ('<span style="background:#47456922;color:#64748b;border:1px solid #47456944;'
                                  'border-radius:3px;padding:4px 10px;font-size:13px;font-weight:700;'
                                  'font-family:\'IBM Plex Mono\',monospace;">No primary brief logged</span>')
                    file_hint = ('<span style="color:#475569;font-size:13px;font-weight:600;margin-left:12px;">'
                                 'Run generate_daily_brief.py --session prior to review</span>')

                st.markdown(f"""
                <div style="background:#14171f;border:1px solid #1e2330;border-radius:5px;
                            padding:16px 20px;margin-bottom:16px;">
                  <div style="display:flex;align-items:center;justify-content:space-between;
                              margin-bottom:12px;">
                    <span style="font-family:'IBM Plex Mono',monospace;font-size:13px;
                                 font-weight:700;text-transform:uppercase;letter-spacing:1.5px;color:#94a3b8;">
                      ◎  {yesterday}  ·  {len(yest_games)} games
                    </span>
                    <span>{pick_badge}{file_hint}</span>
                  </div>
                  {rows_html}
                </div>
                """, unsafe_allow_html=True)

        except Exception as e:
            st.markdown(f'<div class="warn-box">Yesterday\'s results unavailable: {e}</div>',
                        unsafe_allow_html=True)

    # ── Today's Predictions ────────────────────────────────
    today_str = date.today().isoformat()

    with st.expander(f"Today's Predictions — {today_str}", expanded=False):
        try:
            today_brief = query("""
                SELECT session, picks_count, games_covered, output_file, generated_at
                FROM   brief_log
                WHERE  game_date = ?
                ORDER  BY generated_at DESC
            """, (today_str,))

            today_games = query("""
                SELECT COUNT(*) AS n FROM games
                WHERE game_date = ? AND game_type = 'R'
            """, (today_str,))
            n_today = int(today_games["n"].iloc[0]) if not today_games.empty else 0

            SESSION_ORDER = ["prior", "morning", "early", "afternoon", "primary", "late", "closing"]
            SESSION_LABEL = {
                "prior":     ("Prior Day",    "#475569", "Yesterday's results reviewed"),
                "morning":   ("Morning",      "#3b82f6", "Watch list — opening lines"),
                "early":     ("Early",        "#8b5cf6", "1 PM games"),
                "afternoon": ("Afternoon",    "#f59e0b", "4 PM games"),
                "primary":   ("Primary",      "#10b981", "Main daily pick ← act on this"),
                "late":      ("Late",         "#f97316", "~8:15 PM — West Coast late games"),
                "closing":   ("Closing",      "#ef4444", "Confirmation brief"),
            }

            logged_sessions = set(today_brief["session"].tolist()) if not today_brief.empty else set()

            sessions_html = ""
            for sess in SESSION_ORDER:
                label, color, desc = SESSION_LABEL[sess]
                if sess in logged_sessions:
                    row = today_brief[today_brief["session"] == sess].iloc[0]
                    n_picks = int(row.picks_count) if row.picks_count else 0
                    pick_txt = f"⚡ {n_picks} pick(s)" if n_picks > 0 else "No signal"
                    pick_color = "#10b981" if n_picks > 0 else "#64748b"
                    file_txt = row.output_file.split("\\")[-1].split("/")[-1] if row.output_file else "console"
                    status_html = (
                        f'<span style="color:{pick_color};font-size:13px;font-weight:700;">{pick_txt}</span>'
                        f'<span style="color:#475569;font-size:12px;font-weight:600;margin-left:8px;">· {file_txt}</span>'
                    )
                    dot_color = "#10b981"
                    dot_title = "logged"
                else:
                    status_html = f'<span style="color:#475569;font-size:13px;font-weight:600;font-style:italic;">Not yet run</span>'
                    dot_color = "#1e2330"
                    dot_title = "pending"

                sessions_html += (
                    f'<div style="display:flex;align-items:center;justify-content:space-between;'
                    f'padding:10px 0;border-bottom:1px solid #1e2330;">'
                    f'<div style="display:flex;align-items:center;gap:12px;">'
                    f'<div style="width:10px;height:10px;border-radius:50%;background:{dot_color};'
                    f'border:1px solid {color}44;" title="{dot_title}"></div>'
                    f'<span style="font-family:\'IBM Plex Mono\',monospace;font-size:15px;font-weight:700;'
                    f'color:{color};width:110px;">{label}</span>'
                    f'<span style="color:#94a3b8;font-size:13px;font-weight:600;">{desc}</span>'
                    f'</div>'
                    f'{status_html}'
                    f'</div>'
                )

            n_run = len(logged_sessions)
            header_note = (f"{n_run} of {len(SESSION_ORDER)} sessions run today · {n_today} games scheduled"
                           if n_today > 0 else
                           f"{n_run} of {len(SESSION_ORDER)} sessions run today · "
                           f"no R games found (Opening Day? check game_type)")

            st.markdown(f"""
            <div style="background:#14171f;border:1px solid #1e2330;border-radius:5px;
                        padding:16px 20px;margin-bottom:16px;">
              <div style="font-family:'IBM Plex Mono',monospace;font-size:13px;
                          font-weight:700;text-transform:uppercase;letter-spacing:1.5px;color:#94a3b8;
                          margin-bottom:12px;">
                ⚡  {today_str}  ·  {header_note}
              </div>
              {sessions_html}
            </div>
            """, unsafe_allow_html=True)

            if n_run == 0:
                st.markdown(
                    '<div class="info-box">No briefs logged today yet. '
                    'Run <code>generate_daily_brief.py --session morning</code> after the 9 AM odds pull '
                    'or <code>--session primary</code> after the 5 PM pull.</div>',
                    unsafe_allow_html=True)

        except Exception as e:
            st.markdown(f'<div class="warn-box">Today\'s predictions unavailable: {e}</div>',
                        unsafe_allow_html=True)

    # Segment cards
    segments = [
        ("◎", "Data Explorer",   "explorer",   "#3b82f6",
         "Browse teams, players, games, standings, and odds.  All queries are pre-built — pick filters, get results."),
        ("⚗", "Model Workbench", "workbench",  "#e8a020",
         "Run backtests against historical closing lines.  Log live pre-game predictions for the current season."),
        ("⚙", "Operations",      "operations", "#10b981",
         "Trigger stats and odds data loads.  Monitor ingest logs, season coverage, and API quota."),
        ("◈", "Scorecard",       "scorecard",  "#ef4444",
         "Model performance: hit rate, ROI, CLV.  Backtest and live results shown separately — never combined."),
    ]

    c1, c2 = st.columns(2, gap="small")
    for i, (icon, title, view_key, color, desc) in enumerate(segments):
        col = c1 if i % 2 == 0 else c2
        with col:
            st.markdown(f"""
            <div style="background:#14171f;border:1px solid #1e2330;border-radius:5px;
                        padding:18px;position:relative;overflow:hidden;margin-bottom:4px;">
              <div style="position:absolute;top:0;left:0;right:0;height:2px;background:{color};"></div>
              <div style="font-family:'IBM Plex Mono',monospace;font-size:9px;
                          text-transform:uppercase;letter-spacing:1.5px;color:#64748b;margin-bottom:8px;">
                {icon}  {title}
              </div>
              <div style="font-size:12px;color:#94a3b8;line-height:1.6;">{desc}</div>
            </div>
            """, unsafe_allow_html=True)
            if st.button(f"Open {title} →", key=f"home_{view_key}", use_container_width=True):
                go(view_key)


# ═════════════════════════════════════════════════════════════
#  VIEW: DATA EXPLORER
# ═════════════════════════════════════════════════════════════

def view_explorer():
    panel_header("Data Explorer", "Reference & Lookup",
                 "Pre-built queries across teams, players, games, standings, and odds.  No free-form SQL.")

    if not db_available():
        no_db_warning(); return

    # Sub-navigation tabs
    tab_teams, tab_players, tab_games, tab_standings, tab_odds, tab_starters, tab_boxscore, tab_coverage = st.tabs([
        "Teams", "Players", "Games", "Standings", "Odds", "Starters", "Box Score", "Coverage"
    ])

    # ── Teams ──────────────────────────────────────────────
    with tab_teams:
        st.markdown("#### All Teams")
        df = query("""
            SELECT team_id, name, abbreviation, league, division,
                   venue_id
            FROM   teams
            ORDER  BY league, division, name
        """)
        show_df(df, 500)

    # ── Players ────────────────────────────────────────────
    with tab_players:
        st.markdown("#### Player Lookup")
        c1, c2, c3 = st.columns([2, 2, 1])
        with c1:
            name_search = st.text_input("Name search", placeholder="e.g. soto  or  juan soto")
        with c2:
            pos_opts = ["All", "P", "C", "1B", "2B", "3B", "SS", "OF", "DH"]
            pos_filter = st.selectbox("Position", pos_opts)
        with c3:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            run_player = st.button("Search", key="player_search", type="primary")

        if run_player or name_search:
            pos_clause = "" if pos_filter == "All" else f"AND primary_position = '{pos_filter}'"
            term = name_search.strip().lower()

            # Exact / partial match: search last_name and full_name case-insensitively
            df = query(f"""
                SELECT player_id, full_name, primary_position,
                       bats, throws, debut_date, active
                FROM   players
                WHERE  (LOWER(last_name) LIKE LOWER(:term)
                        OR LOWER(full_name) LIKE LOWER(:term))
                  {pos_clause}
                ORDER  BY last_name, first_name
                LIMIT  200
            """, {"term": f"%{term}%"})

            if df.empty and term:
                # Fuzzy fallback: split into tokens, match any token against last_name
                # e.g. "juann soto" → try each word
                tokens = term.split()
                fallback_frames = []
                for tok in tokens:
                    fb = query(f"""
                        SELECT player_id, full_name, primary_position,
                               bats, throws, debut_date, active
                        FROM   players
                        WHERE  LOWER(last_name) LIKE LOWER(:tok)
                          {pos_clause}
                        ORDER  BY last_name, first_name
                        LIMIT  10
                    """, {"tok": f"%{tok}%"})
                    fallback_frames.append(fb)

                import pandas as pd
                combined = pd.concat([f for f in fallback_frames if not f.empty])
                if not combined.empty:
                    combined = combined.drop_duplicates("player_id")
                    st.markdown(
                        f'<div class="warn-box">⚠ No exact match for '
                        f'<strong>"{name_search}"</strong>. '
                        f'Showing {len(combined)} similar player(s) — '
                        f'did you mean one of these?</div>',
                        unsafe_allow_html=True
                    )
                    show_df(combined, 300)
                else:
                    st.markdown(
                        f'<div class="warn-box">No players found matching '
                        f'<strong>"{name_search}"</strong>. '
                        f'Check the spelling or try a shorter search term.</div>',
                        unsafe_allow_html=True
                    )
            else:
                if not df.empty:
                    st.caption(f"{len(df)} player(s) found")
                show_df(df, 400)

        st.markdown("<hr class='sec-div'>", unsafe_allow_html=True)
        st.markdown("#### Player Season Stats")
        c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
        with c1:
            p_name = st.text_input("Player name (exact)", placeholder="e.g. Aaron Judge", key="pstat_name")
        with c2:
            p_season = st.selectbox("Season", list(range(2026, 2014, -1)), key="pstat_season")
        with c3:
            p_role = st.selectbox("Role", ["batter", "pitcher"], key="pstat_role")
        with c4:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            run_pstats = st.button("Search", key="pstat_run", type="primary")

        if run_pstats and p_name:
            try:
                if p_role == "batter":
                    df = query("""
                        SELECT g.game_date_et AS game_date,
                               COALESCE(t_opp.abbreviation, '???') AS opponent,
                               CASE WHEN g.home_team_id = pgs.team_id THEN 'H' ELSE 'A' END AS h_a,
                               pgs.at_bats, pgs.hits, pgs.doubles, pgs.triples,
                               pgs.home_runs, pgs.rbi, pgs.walks,
                               pgs.strikeouts_bat AS strikeouts,
                               pgs.stolen_bases, pgs.batting_avg, pgs.obp, pgs.slg, pgs.ops
                        FROM   player_game_stats pgs
                        JOIN   players p ON p.player_id = pgs.player_id
                        JOIN   games   g ON g.game_pk   = pgs.game_pk
                        LEFT JOIN teams t_opp ON t_opp.team_id = CASE
                                   WHEN g.home_team_id = pgs.team_id THEN g.away_team_id
                                   ELSE g.home_team_id END
                        WHERE  LOWER(p.full_name) = LOWER(?)
                          AND  g.season    = ?
                          AND  pgs.player_role = 'batter'
                        ORDER  BY g.game_date_et
                    """, (p_name, p_season))
                else:
                    df = query("""
                        SELECT g.game_date_et AS game_date,
                               COALESCE(t_opp.abbreviation, '???') AS opponent,
                               pgs.innings_pitched, pgs.hits_allowed, pgs.runs_allowed,
                               pgs.earned_runs, pgs.walks_allowed,
                               pgs.strikeouts_pit AS strikeouts,
                               pgs.hr_allowed, pgs.era, pgs.whip,
                               pgs.win, pgs.loss, pgs.save, pgs.quality_start
                        FROM   player_game_stats pgs
                        JOIN   players p ON p.player_id = pgs.player_id
                        JOIN   games   g ON g.game_pk   = pgs.game_pk
                        LEFT JOIN teams t_opp ON t_opp.team_id = CASE
                                   WHEN g.home_team_id = pgs.team_id THEN g.away_team_id
                                   ELSE g.home_team_id END
                        WHERE  LOWER(p.full_name) = LOWER(?)
                          AND  g.season    = ?
                          AND  pgs.player_role = 'pitcher'
                        ORDER  BY g.game_date_et
                    """, (p_name, p_season))

                if df.empty:
                    st.markdown(
                        f'<div class="warn-box">No {p_role} stats found for '
                        f'<strong>{p_name}</strong> in <strong>{p_season}</strong>. '
                        f'Check that the name is an exact match (e.g. "Aaron Judge") '
                        f'and that the season is loaded in the DB.</div>',
                        unsafe_allow_html=True
                    )
                else:
                    st.caption(f"{len(df)} game rows — {p_name} · {p_season} · {p_role}")
                    show_df(df, 450)

            except Exception as e:
                st.markdown(
                    f'<div class="warn-box">Query error: {e}</div>',
                    unsafe_allow_html=True
                )

    # ── Games ──────────────────────────────────────────────
    with tab_games:
        st.markdown("#### Game Results")
        c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
        with c1:
            g_start = st.date_input("From", value=date.today() - timedelta(days=7), key="g_start")
        with c2:
            g_end   = st.date_input("To",   value=date.today(), key="g_end")
        with c3:
            g_type  = st.selectbox("Game type",
                          ["Regular Season", "Postseason", "Spring Training", "All"],
                          key="g_type")
        with c4:
            teams_df = query("SELECT abbreviation FROM teams ORDER BY abbreviation")
            team_opts = ["All"] + list(teams_df["abbreviation"]) if not teams_df.empty else ["All"]
            g_team  = st.selectbox("Filter by team", team_opts, key="g_team")

        run_games = st.button("Load Games", key="g_run", type="primary")
        if run_games:
            type_map  = {"Regular Season": "'R'", "Postseason": "'P','F','D','L','W'",
                         "Spring Training": "'S','E'", "All": "'R','P','F','D','L','W','S','E'"}
            type_in   = type_map[g_type]
            team_clause = ""
            if g_team != "All":
                team_clause = "AND (th.abbreviation = :team OR ta.abbreviation = :team)"
            df = query(f"""
                SELECT g.game_date_et AS game_date,
                       ta.abbreviation || ' @ ' || th.abbreviation AS matchup,
                       g.away_score, g.home_score,
                       CASE WHEN g.home_score > g.away_score THEN th.abbreviation
                            WHEN g.away_score > g.home_score THEN ta.abbreviation
                            ELSE 'TBD' END AS winner,
                       g.wind_mph,
                       REPLACE(SUBSTR(g.wind_direction,
                           INSTR(g.wind_direction,', ')+2), '', g.wind_direction) AS wind_dir,
                       g.temp_f,
                       g.game_type, g.game_pk, g.status
                FROM   games g
                JOIN   teams th ON th.team_id = g.home_team_id
                JOIN   teams ta ON ta.team_id = g.away_team_id
                WHERE  g.game_date_et BETWEEN :start AND :end
                  AND  g.game_type IN ({type_in})
                  {team_clause}
                ORDER  BY g.game_date_et DESC, g.game_pk
            """, {"start": str(g_start), "end": str(g_end),
                  "team": g_team if g_team != "All" else ""})
            if df.empty:
                st.markdown(
                    f'<div class="info-box">No {g_type.lower()} games found for this date range.</div>',
                    unsafe_allow_html=True)
            else:
                st.caption(f"{len(df)} games — {g_start} to {g_end} · {g_type}")
                show_df(df, 450)

    # ── Standings ──────────────────────────────────────────
    with tab_standings:
        st.markdown("#### Season Standings")
        c1, c2 = st.columns([1, 1])
        with c1:
            s_season = st.selectbox("Season", list(range(2026, 2014, -1)), key="std_season")
        with c2:
            s_date_opts = ["End of season", "Custom date"]
            s_date_mode = st.selectbox("Snapshot", s_date_opts, key="std_mode")

        custom_date = None
        if s_date_mode == "Custom date":
            custom_date = st.date_input("Date", key="std_custom_date")

        if st.button("Load Standings", key="std_run", type="primary"):
            if custom_date:
                snap = str(custom_date)
            else:
                row = query("SELECT season_end FROM seasons WHERE season = ?", (s_season,))
                snap = row["season_end"].iloc[0] if not row.empty else f"{s_season}-09-29"

            df = query("""
                SELECT t.name, t.league, t.division,
                       s.wins, s.losses,
                       ROUND(s.wins * 1.0 / NULLIF(s.wins + s.losses, 0), 3) AS win_pct,
                       s.games_back, s.run_diff,
                       s.runs_scored, s.runs_allowed,
                       s.snapshot_date
                FROM   standings s
                JOIN   teams t ON t.team_id = s.team_id
                WHERE  s.snapshot_date = ?
                ORDER  BY t.league, t.division, s.games_back
            """, (snap,))
            if df.empty:
                # Try nearest available date
                df = query("""
                    SELECT t.name, t.league, t.division,
                           s.wins, s.losses,
                           ROUND(s.wins * 1.0 / NULLIF(s.wins + s.losses, 0), 3) AS win_pct,
                           s.games_back, s.run_diff, s.snapshot_date
                    FROM   standings s
                    JOIN   teams t ON t.team_id = s.team_id
                    WHERE  s.snapshot_date = (
                        SELECT MAX(snapshot_date) FROM standings
                        WHERE  snapshot_date <= ? AND season = ?
                    )
                    ORDER  BY t.league, t.division, s.games_back
                """, (snap, s_season))
            show_df(df, 500)

    # ── Odds ───────────────────────────────────────────────
    with tab_odds:
        odds_tables = query("SELECT name FROM sqlite_master WHERE type='table' AND name='game_odds'")
        if odds_tables.empty:
            st.markdown('<div class="warn-box">⚠ game_odds table not found. Run add_f5_table.py and load_odds.py first.</div>', unsafe_allow_html=True)
        else:
            st.markdown("#### Closing Lines by Date")
            c1, c2, c3 = st.columns([1, 1, 1])
            with c1:
                o_date = st.date_input("Game date", value=date.today() - timedelta(days=1), key="odds_date")
            with c2:
                o_book_df = query("SELECT DISTINCT bookmaker FROM game_odds ORDER BY bookmaker")
                book_opts = ["All"] + list(o_book_df["bookmaker"]) if not o_book_df.empty else ["All"]
                o_book = st.selectbox("Bookmaker", book_opts, key="odds_book")
            with c3:
                o_market = st.selectbox("Market", ["moneyline", "runline", "total"], key="odds_market")

            if st.button("Load Odds", key="odds_run", type="primary"):
                book_clause = "" if o_book == "All" else "AND go.bookmaker = :book"
                df = query(f"""
                    SELECT th.abbreviation AS home,
                           ta.abbreviation AS away,
                           go.bookmaker,
                           go.home_ml, go.away_ml,
                           go.home_rl_line, go.home_rl_odds, go.away_rl_odds,
                           go.total_line, go.over_odds, go.under_odds,
                           go.hours_before_game,
                           go.captured_at_utc
                    FROM   v_closing_game_odds go
                    JOIN   games g  ON g.game_pk    = go.game_pk
                    JOIN   teams th ON th.team_id   = g.home_team_id
                    JOIN   teams ta ON ta.team_id   = g.away_team_id
                    WHERE  g.game_date_et   = :gdate
                      AND  go.market_type = :market
                      {book_clause}
                    ORDER  BY go.bookmaker, th.abbreviation
                """, {"gdate": str(o_date), "market": o_market,
                      "book": o_book if o_book != "All" else ""})
                show_df(df, 420)

            st.markdown("<hr class='sec-div'>", unsafe_allow_html=True)
            st.markdown("#### Line Movement")
            c1, c2 = st.columns([1, 1])
            with c1:
                lm_date = st.date_input("Game date", value=date.today() - timedelta(days=1), key="lm_date")
            with c2:
                lm_book_df = query("SELECT DISTINCT bookmaker FROM line_movement ORDER BY bookmaker")
                lm_book_opts = ["All"] + list(lm_book_df["bookmaker"]) if not lm_book_df.empty else ["All"]
                lm_book = st.selectbox("Bookmaker", lm_book_opts, key="lm_book")

            if st.button("Load Movement", key="lm_run", type="primary"):
                book_clause = "" if lm_book == "All" else "AND lm.bookmaker = :book"
                df = query(f"""
                    SELECT th.abbreviation AS home,
                           ta.abbreviation AS away,
                           lm.bookmaker, lm.market_type,
                           lm.open_home_ml, lm.close_home_ml, lm.ml_move_cents,
                           lm.open_total, lm.close_total, lm.total_move,
                           lm.move_direction,
                           lm.steam_move, lm.reverse_line_move
                    FROM   line_movement lm
                    JOIN   games g  ON g.game_pk    = lm.game_pk
                    JOIN   teams th ON th.team_id   = g.home_team_id
                    JOIN   teams ta ON ta.team_id   = g.away_team_id
                    WHERE  g.game_date_et = :gdate {book_clause}
                    ORDER  BY lm.steam_move DESC, lm.ml_move_cents DESC
                """, {"gdate": str(lm_date),
                      "book": lm_book if lm_book != "All" else ""})
                show_df(df, 400)

            st.markdown("<hr class='sec-div'>", unsafe_allow_html=True)
            st.markdown("#### Opening vs Closing — CLV Lookup")
            st.markdown("""
            <div class="info-box">
              Compare opening and closing lines for any date range.
              Positive CLV (open implied &gt; close implied) means the opening price was
              better than the closing line — a sign of sharp-side entry.
              Based on the CLV timing study (Mar 2026): closing ROI beats opening
              by ~2pp across all signals. Use this view to investigate specific games.
            </div>
            """, unsafe_allow_html=True)
            c1, c2, c3 = st.columns([1, 1, 1])
            with c1:
                clv_from = st.date_input("From", value=date.today() - timedelta(days=7), key="clv_from")
            with c2:
                clv_to   = st.date_input("To",   value=date.today() - timedelta(days=1), key="clv_to")
            with c3:
                clv_book_df = query("SELECT DISTINCT bookmaker FROM game_odds ORDER BY bookmaker")
                clv_book_opts = ["All"] + list(clv_book_df["bookmaker"]) if not clv_book_df.empty else ["All"]
                clv_book = st.selectbox("Bookmaker", clv_book_opts, key="clv_book")

            if st.button("Load CLV Data", key="clv_run", type="primary"):
                book_clause = "" if clv_book == "All" else "AND go_open.bookmaker = :book"
                df_clv = query(f"""
                    SELECT g.game_date_et AS game_date,
                           th.abbreviation AS home,
                           ta.abbreviation AS away,
                           go_open.bookmaker,
                           go_open.home_ml   AS open_home_ml,
                           go_close.home_ml  AS close_home_ml,
                           go_open.away_ml   AS open_away_ml,
                           go_close.away_ml  AS close_away_ml,
                           go_open.total_line  AS open_total,
                           go_close.total_line AS close_total,
                           ROUND(
                               (CASE WHEN go_open.home_ml > 0
                                     THEN 100.0/(go_open.home_ml+100)
                                     ELSE ABS(go_open.home_ml)/(ABS(go_open.home_ml)+100.0) END
                               - CASE WHEN go_close.home_ml > 0
                                     THEN 100.0/(go_close.home_ml+100)
                                     ELSE ABS(go_close.home_ml)/(ABS(go_close.home_ml)+100.0) END
                               ) * 100, 2
                           ) AS home_clv_pp,
                           go_close.total_line - go_open.total_line AS total_move
                    FROM   games g
                    JOIN   teams th ON th.team_id = g.home_team_id
                    JOIN   teams ta ON ta.team_id = g.away_team_id
                    JOIN   game_odds go_open  ON go_open.game_pk  = g.game_pk
                                             AND go_open.market_type = 'moneyline'
                                             AND go_open.is_opening_line = 1
                    JOIN   game_odds go_close ON go_close.game_pk = g.game_pk
                                             AND go_close.market_type = 'moneyline'
                                             AND go_close.is_closing_line = 1
                                             AND go_close.bookmaker = go_open.bookmaker
                    WHERE  g.game_date_et BETWEEN :dfrom AND :dto
                      AND  g.game_type = 'R'
                      {book_clause}
                    ORDER  BY g.game_date_et DESC, home_clv_pp DESC
                """, {"dfrom": str(clv_from), "dto": str(clv_to),
                      "book": clv_book if clv_book != "All" else ""})
                if df_clv.empty:
                    st.markdown('<div class="info-box">No opening+closing pairs found. Requires game_odds rows with both is_opening_line=1 and is_closing_line=1.</div>', unsafe_allow_html=True)
                else:
                    show_df(df_clv, 420)


    # ── Starters ─────────────────────────────────────────────
    with tab_starters:
        st.markdown("#### Probable Starters by Date")
        c1, c2, c3 = st.columns([1, 2, 1])
        with c1:
            st_date = st.date_input("Date", value=date.today(), key="st_date")
        with c2:
            st_player = st.text_input("Filter by pitcher name", placeholder="e.g. Crochet", key="st_player")
        with c3:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            run_st = st.button("Load Starters", key="st_run", type="primary")

        if run_st or True:
            name_clause = "AND LOWER(p.full_name) LIKE LOWER(:nm)" if st_player.strip() else ""
            df_st = query(f"""
                SELECT g.game_date_et AS game_date,
                       ta.abbreviation || '@' || th.abbreviation AS matchup,
                       CASE WHEN gpp.team_id=g.home_team_id THEN 'HOME' ELSE 'AWAY' END AS side,
                       p.full_name AS pitcher,
                       p.era_season AS era,
                       gpp.fetched_at
                FROM   game_probable_pitchers gpp
                JOIN   games   g  ON g.game_pk   = gpp.game_pk
                JOIN   players p  ON p.player_id = gpp.player_id
                JOIN   teams   th ON th.team_id  = g.home_team_id
                JOIN   teams   ta ON ta.team_id  = g.away_team_id
                WHERE  g.game_date_et = :dt AND g.game_type = 'R'
                  {name_clause}
                ORDER  BY g.game_start_utc, side
            """, {"dt": str(st_date), "nm": f"%{st_player.strip()}%" if st_player.strip() else ""})
            if df_st.empty:
                st.markdown(f'<div class="info-box">No starters on file for {st_date}. Run load_weather.py --no-weather to populate.</div>', unsafe_allow_html=True)
            else:
                st.caption(f"{len(df_st)} starter rows for {st_date}")
                show_df(df_st, 420)

        st.markdown("<hr class='sec-div'>", unsafe_allow_html=True)
        st.markdown("#### Season Starter Search")
        c1, c2, c3 = st.columns([2, 1, 1])
        with c1:
            sp_name = st.text_input("Pitcher name", placeholder="e.g. Garrett Crochet", key="sp_name")
        with c2:
            sp_season = st.selectbox("Season", list(range(2026, 2014, -1)), key="sp_season")
        with c3:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            run_sp = st.button("Search", key="sp_run", type="primary")
        if run_sp and sp_name:
            df_sp = query("""
                SELECT g.game_date_et AS game_date,
                       ta.abbreviation || '@' || th.abbreviation AS matchup,
                       CASE WHEN gpp.team_id=g.home_team_id THEN 'H' ELSE 'A' END AS ha,
                       p.full_name AS pitcher, g.away_score, g.home_score, g.status
                FROM   game_probable_pitchers gpp
                JOIN   games   g  ON g.game_pk   = gpp.game_pk
                JOIN   players p  ON p.player_id = gpp.player_id
                JOIN   teams   th ON th.team_id  = g.home_team_id
                JOIN   teams   ta ON ta.team_id  = g.away_team_id
                WHERE  LOWER(p.full_name) LIKE LOWER(:nm)
                  AND  g.season = :s AND g.game_type = 'R'
                ORDER  BY g.game_date_et
            """, {"nm": f"%{sp_name.strip()}%", "s": sp_season})
            if df_sp.empty:
                st.markdown(f'<div class="warn-box">No starts found for {sp_name} in {sp_season}.</div>', unsafe_allow_html=True)
            else:
                st.caption(f"{len(df_sp)} starts — {sp_name} · {sp_season}")
                show_df(df_sp, 420)

    # ── Box Score ─────────────────────────────────────────────
    with tab_boxscore:
        # ── shared rendering helpers (mirrors boxscore_app.py) ─────────────
        import re as _re2
        import streamlit.components.v1 as _cmp

        try:
            from zoneinfo import ZoneInfo as _ZI2; _ET2 = _ZI2("America/New_York")
        except Exception:
            import datetime as _dtt2
            _ET2 = _dtt2.timezone(_dtt2.timedelta(hours=-4))

        def _wind_lbl(d):
            if not d: return "N/A"
            c = _re2.sub(r"^\d+\s*mph,?\s*", "", d.strip(), flags=_re2.IGNORECASE)
            u = c.upper()
            if u in ("NONE", ""): return "CALM"
            if any(k in u for k in ("OUT","BLOWING OUT")): return "OUT"
            if any(k in u for k in ("IN","BLOWING IN")):  return "IN"
            if u in ("L TO R","R TO L"): return "CROSS"
            if any(k in u for k in ("CROSS","LEFT","RIGHT")): return "CROSS"
            if any(k in u for k in ("CALM","STILL")): return "CALM"
            return c

        def _fmt_ip2(v):
            if v is None: return "N/A"
            v = float(v); w = int(v); f = v - w
            out = 0 if f<0.17 else 1 if f<0.50 else 2 if f<0.83 else 0
            if f >= 0.83: w += 1; out = 0
            return f"{w}.{out}"

        def _fmt_avg2(v):
            if v is None: return ".---"
            s = f"{float(v):.3f}".lstrip("0")
            return s or ".000"

        def _fmt_odds2(v):
            if v is None: return "N/A"
            return f"+{v}" if int(v) > 0 else str(int(v))

        def _game_time2(utc_str):
            if not utc_str: return ""
            try:
                from datetime import timezone as _tz2
                dt = datetime.fromisoformat(str(utc_str).rstrip("Z")).replace(tzinfo=_tz2.utc)
                et = dt.astimezone(_ET2)
                h = et.strftime("%I").lstrip("0") or "12"
                return f"{h}:{et.strftime('%M %p ET')}"
            except Exception: return ""

        def _streak2(team_id, game_date):
            con = get_connection()
            if con is None: return 0
            rows = con.execute("""
                SELECT CASE WHEN home_team_id=? THEN
                    CASE WHEN home_score>away_score THEN 'W' ELSE 'L' END
                ELSE CASE WHEN away_score>home_score THEN 'W' ELSE 'L' END
                END AS r
                FROM games WHERE (home_team_id=? OR away_team_id=?)
                  AND status='Final' AND game_date < ?
                ORDER BY game_date DESC, game_start_utc DESC LIMIT 15
            """, (team_id, team_id, team_id, game_date)).fetchall()
            if not rows: return 0
            cur = rows[0]["r"]; count = 0
            for r in rows:
                if r["r"] == cur: count += 1
                else: break
            return count if cur == "W" else -count

        def _slbl2(s):
            if s == 0: return "—"
            return f"W{s}" if s > 0 else f"L{abs(s)}"

        def _impl2(o):
            if o is None: return None
            o = int(o)
            return 100/(o+100) if o > 0 else abs(o)/(abs(o)+100)

        _H3B = {"Wrigley Field","Coors Field","Kauffman Stadium","Globe Life Field",
                "Progressive Field","PNC Park","Comerica Park","American Family Field",
                "Great American Ball Park","Nationals Park","Camden Yards","Fenway Park",
                "Yankee Stadium","Citi Field","Oakland Coliseum"}

        def _signals2(game, home_streak):
            sigs = []
            hml=game["home_ml"]; aml=game["away_ml"]; tot=game["total_line"]
            mph=game["wind_mph"] or 0; wdir=_wind_lbl(game["wind_direction"] or "")
            eff=(game["wind_effect"] or "HIGH").upper()
            venue=game["venue_name"] or ""; pf=game["park_factor_runs"] or 100
            ok=eff=="HIGH"; sup=eff=="SUPPRESSED"; ai=_impl2(aml)
            if ok and not sup and wdir=="IN" and mph>=10 and hml and -160<=hml<=-130:
                sigs.append(f"MV-F  Wind IN {mph} mph  →  AWAY ML {_fmt_odds2(aml)}")
            if ok and not sup and wdir=="OUT" and mph>=15 and ai and 0.35<=ai<=0.42:
                sigs.append(f"MV-B  Wind OUT {mph} mph  →  OVER {tot}")
            if home_streak>=5 and hml and -160<=hml<=-130:
                sigs.append(f"S1+H2  Home W{home_streak}  →  AWAY ML {_fmt_odds2(aml)}")
            elif home_streak>=6 and hml and -170<=hml<=-105:
                sigs.append(f"S1  Home W{home_streak}  →  AWAY ML {_fmt_odds2(aml)}")
            if ok and not sup and wdir=="OUT" and mph>=10 and tot and venue in _H3B and pf>=98:
                sigs.append(f"H3b  Wind OUT {mph} mph  {venue}  →  OVER {tot}")
            return sigs

        def _hl2(v, t=0):
            if v and int(v) > t: return f"<span class='bsc-hl'>{v}</span>"
            return str(v) if v is not None else "0"

        def _bat_table2(rows):
            if not rows: return ""
            h = ["<table class='bsc-t'><thead><tr>",
                 "<th>#</th><th>Player</th><th>Pos</th>",
                 "<th>AB</th><th>R</th><th>H</th><th>RBI</th>",
                 "<th>HR</th><th>2B</th><th>BB</th><th>SO</th><th>SB</th><th>AVG</th>",
                 "</tr></thead><tbody>"]
            tot = {k:0 for k in "ab r h rbi hr db bb so sb".split()}
            for r in rows:
                base=(r["batting_order"] or 0)//100
                sub=(r["batting_order"] or 0)%100 != 0
                oc = "↳" if sub else (str(base) if base else "")
                ab=r["at_bats"] or 0; run=r["runs"] or 0; hit=r["hits"] or 0
                rbi=r["rbi"] or 0; hr=r["home_runs"] or 0; db=r["doubles"] or 0
                bb=r["walks"] or 0; so=r["so"] or 0; sb=r["stolen_bases"] or 0
                tot["ab"]+=ab;tot["r"]+=run;tot["h"]+=hit;tot["rbi"]+=rbi
                tot["hr"]+=hr;tot["db"]+=db;tot["bb"]+=bb;tot["so"]+=so;tot["sb"]+=sb
                h.append(f"<tr><td>{oc}</td><td>{r['full_name'] or ''}</td>"
                         f"<td>{r['position'] or ''}</td>"
                         f"<td>{ab}</td><td>{_hl2(run)}</td><td>{_hl2(hit)}</td>"
                         f"<td>{_hl2(rbi)}</td><td>{_hl2(hr)}</td><td>{_hl2(db)}</td>"
                         f"<td>{_hl2(bb)}</td><td>{so}</td><td>{_hl2(sb)}</td>"
                         f"<td>{_fmt_avg2(r['batting_avg'])}</td></tr>")
            h.append(f"<tr class='bsc-tot'><td></td><td>Totals</td><td></td>"
                     f"<td>{tot['ab']}</td><td>{tot['r']}</td><td>{tot['h']}</td>"
                     f"<td>{tot['rbi']}</td><td>{tot['hr']}</td><td>{tot['db']}</td>"
                     f"<td>{tot['bb']}</td><td>{tot['so']}</td><td>{tot['sb']}</td>"
                     f"<td></td></tr>")
            h.append("</tbody></table>")
            return "".join(h)

        def _pit_table2(rows):
            if not rows: return ""
            h = ["<table class='bsc-t'><thead><tr>",
                 "<th></th><th>Pitcher</th><th>IP</th>",
                 "<th>H</th><th>R</th><th>ER</th><th>BB</th><th>SO</th><th>HR</th>",
                 "<th>Pitches</th><th>S%</th><th>ERA</th>",
                 "</tr></thead><tbody>"]
            for i,r in enumerate(rows):
                dec=""
                if r["win"]:       dec="<span class='bsc-dec bsc-w'>W</span>"
                elif r["loss"]:    dec="<span class='bsc-dec bsc-l'>L</span>"
                elif r["save"]:    dec="<span class='bsc-dec bsc-s'>S</span>"
                elif r["hold"]:    dec="<span class='bsc-dec bsc-h'>H</span>"
                elif r["blown_save"]: dec="<span class='bsc-dec bsc-bs'>BS</span>"
                role = "SP" if i==0 else "RP"
                pit=r["pitches_thrown"] or 0; strk=r["strikes_thrown"] or 0
                spct=f"{strk/pit*100:.0f}%" if pit else "—"
                era=f"{float(r['era']):.2f}" if r["era"] is not None else "—"
                so=r["so"] or 0
                soc=f"<span class='bsc-hl'>{so}</span>" if so>=7 else str(so)
                h.append(f"<tr><td>{role}</td><td>{r['full_name'] or ''}{dec}</td>"
                         f"<td>{_fmt_ip2(r['innings_pitched'])}</td>"
                         f"<td>{r['hits_allowed'] or 0}</td><td>{r['runs_allowed'] or 0}</td>"
                         f"<td>{r['earned_runs'] or 0}</td><td>{r['walks_allowed'] or 0}</td>"
                         f"<td>{soc}</td><td>{r['hr_allowed'] or 0}</td>"
                         f"<td>{pit}</td><td>{spct}</td><td>{era}</td></tr>")
            h.append("</tbody></table>")
            return "".join(h)

        def _build_card2(gpk, game_date):
            con = get_connection()
            if con is None: return ""
            def qq(sql, p=()):
                try: return con.execute(sql, p).fetchall()
                except: return []
            gr = qq("""
                SELECT g.game_pk,g.game_date_et AS game_date,g.game_start_utc,g.status,
                       g.home_score,g.away_score,g.wind_mph,g.wind_direction,
                       g.temp_f,g.wind_source,
                       th.team_id AS home_tid,th.abbreviation AS home_abbr,
                       ta.team_id AS away_tid,ta.abbreviation AS away_abbr,
                       v.name AS venue_name,v.wind_effect,v.park_factor_runs,
                       ml.home_ml,ml.away_ml,
                       tot.total_line
                FROM games g
                JOIN teams th ON th.team_id=g.home_team_id
                JOIN teams ta ON ta.team_id=g.away_team_id
                LEFT JOIN venues v ON v.venue_id=g.venue_id
                LEFT JOIN v_closing_game_odds ml  ON ml.game_pk=g.game_pk AND ml.market_type='moneyline'
                LEFT JOIN v_closing_game_odds tot ON tot.game_pk=g.game_pk AND tot.market_type='total'
                WHERE g.game_pk=?""", (gpk,))
            if not gr: return ""
            g = dict(gr[0])
            is_final = g["status"]=="Final"
            hs=g["home_score"]; aws=g["away_score"]
            ha=g["home_abbr"]; aa=g["away_abbr"]
            ht=g["home_tid"];  at=g["away_tid"]
            hs2=_streak2(ht,game_date); as2=_streak2(at,game_date)
            # starters
            starters={}
            for r in qq("SELECT gpp.team_id,p.full_name,p.era_season FROM game_probable_pitchers gpp JOIN players p ON p.player_id=gpp.player_id WHERE gpp.game_pk=?", (gpk,)):
                starters[r["team_id"]]={"name":r["full_name"],"era":r["era_season"]}
            def sp2(tid,ab):
                d=starters.get(tid,{})
                if not d: return f"{ab}: TBD"
                era=f" (ERA {d['era']:.2f})" if d.get("era") is not None else ""
                return f"{ab}: {d['name']}{era}"
            # opening
            op=qq("SELECT home_ml,away_ml FROM game_odds WHERE game_pk=? AND market_type='moneyline' AND is_opening_line=1 ORDER BY captured_at_utc ASC LIMIT 1",(gpk,))
            opening=dict(op[0]) if op else None
            # signals
            sigs=_signals2(g,hs2)
            # display
            mph=g["wind_mph"] or 0; wdir=_wind_lbl(g["wind_direction"] or "")
            wsrc=" (forecast)" if g.get("wind_source")=="forecast" else " (actual)"
            temp=f"{int(g['temp_f'])}°F  " if g["temp_f"] else ""
            wind_str=f"{temp}{mph} mph {wdir}{wsrc}" if mph else "Wind N/A"
            # score
            if is_final:
                hw=hs is not None and aws is not None and hs>aws
                aw=hs is not None and aws is not None and aws>hs
                score_html=(f"<span class='{' bsc-win' if aw else ''}{aa} {aws}</span>  —  "
                            f"<span class='{' bsc-win' if hw else ''}{ha} {hs}</span>"
                            f"  <span class='bsc-status'>Final</span>")
                # fix class attr
                score_html=(f"<span class='{'bsc-win' if aw else 'bsc-normal'}'>{aa} {aws}</span>"
                            f"  —  "
                            f"<span class='{'bsc-win' if hw else 'bsc-normal'}'>{ha} {hs}</span>"
                            f"  <span class='bsc-status'>Final</span>")
            else:
                score_html=f"{aa} @ {ha}  <span class='bsc-status'>{g['status']}</span>"
            # odds
            hml=g["home_ml"]; aml=g["away_ml"]; total=g["total_line"]
            ml_s=f"{ha} {_fmt_odds2(hml)} / {aa} {_fmt_odds2(aml)}" if hml else "N/A"
            tot_s=f"O/U {total}" if total else "N/A"
            clv_s=""
            if opening and hml and aml:
                ch=int(hml)-int(opening["home_ml"] or hml)
                ca=int(aml)-int(opening["away_ml"] or aml)
                clv_s=(f"<span><span class='bsc-lbl'>Opening</span>"
                       f"{ha} {_fmt_odds2(opening['home_ml'])} / {aa} {_fmt_odds2(opening['away_ml'])}</span>"
                       f"<span><span class='bsc-lbl'>CLV</span>{ha} {ch:+d}  {aa} {ca:+d}</span>")
            sig_bar=(f"<div class='bsc-sig'>&#9873;  " + "  |  ".join(sigs) + "</div>") if sigs else "<div class='bsc-nosig'>&mdash;  No model signals fired</div>"
            # batting/pitching
            bat=qq("SELECT pgs.team_id,pgs.batting_order,pgs.position,p.full_name,pgs.at_bats,pgs.runs,pgs.hits,pgs.rbi,pgs.home_runs,pgs.doubles,pgs.triples,pgs.walks AS bb,pgs.strikeouts_bat AS so,pgs.stolen_bases,pgs.batting_avg FROM player_game_stats pgs JOIN players p ON p.player_id=pgs.player_id WHERE pgs.game_pk=? AND pgs.player_role='batter' ORDER BY pgs.team_id,pgs.batting_order",(gpk,))
            pit=qq("SELECT pgs.team_id,p.full_name,pgs.innings_pitched,pgs.pitches_thrown,pgs.strikes_thrown,pgs.earned_runs,pgs.runs_allowed,pgs.hits_allowed,pgs.hr_allowed,pgs.walks_allowed,pgs.strikeouts_pit AS so,pgs.win,pgs.loss,pgs.save,pgs.hold,pgs.blown_save,pgs.era FROM player_game_stats pgs JOIN players p ON p.player_id=pgs.player_id WHERE pgs.game_pk=? AND pgs.player_role='pitcher' ORDER BY pgs.team_id,pgs.innings_pitched DESC",(gpk,))
            abat=[r for r in bat if r["team_id"]==at]; hbat=[r for r in bat if r["team_id"]==ht]
            apit=[r for r in pit if r["team_id"]==at]; hpit=[r for r in pit if r["team_id"]==ht]
            if bat or pit:
                box=(f"<div class='bsc-sec'>{aa} Batting</div>"+_bat_table2(abat)
                    +f"<div class='bsc-sec'>{ha} (h) Batting</div>"+_bat_table2(hbat)
                    +f"<div class='bsc-sec'>{aa} Pitching</div>"+_pit_table2(apit)
                    +f"<div class='bsc-sec'>{ha} (h) Pitching</div>"+_pit_table2(hpit))
            else:
                box="<div class='bsc-na'>Box score not yet available &mdash; game hasn't started or stats pending.</div>"
            return f"""
<div class='bsc-card'>
  <div class='bsc-hdr'>
    <div class='bsc-score'>{score_html}</div>
    <div class='bsc-meta'>{g['game_date']}  &middot;  {_game_time2(g['game_start_utc'])}  &middot;  {g['venue_name'] or ''}</div>
  </div>
  <div class='bsc-info'>
    <span><b>Wind</b> {wind_str}</span>
    <span><b>SP {aa}</b> {sp2(at,aa)}</span>
    <span><b>SP {ha}</b> {sp2(ht,ha)}</span>
    <span><b>Streaks</b> {aa} {_slbl2(as2)}  |  {ha} {_slbl2(hs2)}</span>
  </div>
  <div class='bsc-odds'>
    <span><span class='bsc-lbl'>Closing ML</span>{ml_s}</span>
    <span><span class='bsc-lbl'>Total</span>{tot_s}</span>
    {clv_s}
  </div>
  {sig_bar}
  {box}
</div>"""

        # ── Inline CSS (self-contained inside components.html) ────────────
        _BSC_CSS = """<style>
body{margin:0;padding:0;background:transparent;font-family:'IBM Plex Mono',monospace}
.bsc-card{background:#14171f;border:1px solid #1e2330;border-radius:6px;
  margin-bottom:20px;overflow:hidden}
.bsc-hdr{background:#1f3864;padding:14px 18px}
.bsc-score{font-family:'Bebas Neue',cursive;font-size:32px;
  letter-spacing:1.5px;color:#fff;line-height:1}
.bsc-win{color:#7ec8e3}.bsc-normal{color:#fff}
.bsc-status{font-size:15px;opacity:.5}
.bsc-meta{font-size:11px;color:rgba(255,255,255,.6);margin-top:4px}
.bsc-info{display:flex;flex-wrap:wrap;gap:.4rem 1.8rem;
  padding:9px 18px;background:#0f1420;border-bottom:1px solid #1e2330;font-size:12px}
.bsc-info b{color:#60a5fa}
.bsc-info span{color:#94a3b8}
.bsc-sig{background:rgba(245,158,11,.08);border-bottom:2px solid #f59e0b;
  padding:7px 18px;font-size:12px;font-weight:600;color:#fbbf24}
.bsc-nosig{background:rgba(16,185,129,.05);border-bottom:1px solid rgba(16,185,129,.2);
  padding:7px 18px;font-size:12px;color:#4ade80}
.bsc-odds{display:flex;flex-wrap:wrap;gap:.4rem 1.8rem;
  padding:7px 18px;background:#14171f;border-bottom:1px solid #1e2330;font-size:12px}
.bsc-lbl{color:#475569;margin-right:4px}
.bsc-sec{font-size:9px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;
  color:#475569;padding:6px 18px 3px;background:#0a0c10;
  border-bottom:1px solid #1e2330;border-top:1px solid #1e2330}
.bsc-t{width:100%;border-collapse:collapse;font-size:12px}
.bsc-t th{background:#070809;color:#475569;font-size:9px;text-transform:uppercase;
  letter-spacing:1px;padding:5px 9px;border-bottom:1px solid #1e2330;text-align:right}
.bsc-t th:first-child,.bsc-t th:nth-child(2){text-align:left}
.bsc-t td{padding:4px 9px;border-bottom:1px solid rgba(30,35,48,.8);
  color:#e2e8f0;text-align:right;font-size:12px}
.bsc-t td:first-child{text-align:center;color:#475569;font-size:10px;width:26px}
.bsc-t td:nth-child(2){text-align:left;font-weight:600}
.bsc-t tr:last-child td{border-bottom:none}
.bsc-tot td{background:rgba(30,35,48,.6);font-weight:700;border-top:1px solid #1e2330}
.bsc-hl{color:#60a5fa;font-weight:700}
.bsc-dec{display:inline-block;font-size:9px;font-weight:700;
  padding:1px 5px;border-radius:2px;margin-left:4px;vertical-align:middle}
.bsc-w{background:rgba(16,185,129,.25);color:#34d399}
.bsc-l{background:rgba(239,68,68,.25);color:#f87171}
.bsc-s{background:rgba(59,130,246,.25);color:#60a5fa}
.bsc-h{background:rgba(100,116,139,.25);color:#94a3b8}
.bsc-bs{background:rgba(239,68,68,.15);color:#fca5a5}
.bsc-na{padding:22px 18px;font-size:13px;color:#475569}
</style>"""

        # ── Controls ──────────────────────────────────────────────────────
        bc1, bc2, bc3 = st.columns([1, 2.5, 1])
        with bc1:
            bs_date = st.date_input("Date", value=date.today()-timedelta(days=1), key="bs_date")
        bs_game_list = query("""
            SELECT g.game_pk, g.game_start_utc, g.status,
                   ta.abbreviation || '@' || th.abbreviation AS matchup
            FROM   games g
            JOIN   teams th ON th.team_id=g.home_team_id
            JOIN   teams ta ON ta.team_id=g.away_team_id
            WHERE  g.game_date_et=? AND g.game_type='R'
            ORDER  BY g.game_start_utc
        """, (str(bs_date),))
        with bc2:
            if bs_game_list.empty:
                st.selectbox("Game", ["No games found"], key="bs_gsel")
                bs_gpks = []
            else:
                bs_opts = {"All games — full day": None}
                for _, r in bs_game_list.iterrows():
                    bs_opts[f"{r.matchup}  [{r.status}]"] = int(r.game_pk)
                bs_choice = st.selectbox("Game", list(bs_opts.keys()), key="bs_gsel")
                bs_gpks = (
                    [int(r.game_pk) for _, r in bs_game_list.iterrows()]
                    if bs_opts[bs_choice] is None
                    else [bs_opts[bs_choice]]
                )
        with bc3:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            bs_load = st.button("⟳  Load", key="bs_load2", type="primary", use_container_width=True)

        # ── Render ────────────────────────────────────────────────────────
        if not bs_game_list.empty and (bs_load or "bs_rendered" not in st.session_state or st.session_state.get("bs_last_date") != str(bs_date)):
            st.session_state["bs_last_date"] = str(bs_date)
            all_html = ""
            for gpk in bs_gpks:
                all_html += _build_card2(gpk, str(bs_date))
            n_final = sum(1 for gpk in bs_gpks
                if any(r.game_pk==gpk and r.status=="Final" for _,r in bs_game_list.iterrows()))
            n_sched = len(bs_gpks) - n_final
            est_h = n_final * 920 + n_sched * 280 + 40
            import streamlit.components.v1 as _cmp2
            _cmp2.html(_BSC_CSS + all_html, height=est_h, scrolling=False)
        elif bs_game_list.empty:
            st.markdown('<div class="info-box">No games found for this date. Run load_today.py to populate.</div>', unsafe_allow_html=True)

    # ── Coverage ───────────────────────────────────────────
    with tab_coverage:
        # Non-R game_type warning (Spring Training / Exhibition / Opening Series)
        non_r = query("""
            SELECT game_type, COUNT(*) AS games,
                   MIN(game_date) AS earliest, MAX(game_date) AS latest
            FROM   games
            WHERE  game_type != 'R'
            GROUP  BY game_type
            ORDER  BY games DESC
        """)
        if not non_r.empty:
            non_r_total = int(non_r["games"].sum())
            rows_md = "".join(
                f"<tr><td>{r.game_type}</td><td>{r.games}</td>"
                f"<td>{r.earliest}</td><td>{r.latest}</td></tr>"
                for r in non_r.itertuples()
            )
            st.markdown(f"""
            <div class="warn-box">
              ⚠ <strong>{non_r_total} non-regular-season games in DB</strong>
              (Spring Training 'S', Exhibition 'E', etc.). These are excluded from all
              brief and backtest queries which filter on <code>game_type='R'</code>.
              If Opening Day games loaded as 'S', re-run <code>load_mlb_stats.py</code>
              after the MLB API reclassifies them — usually within a few hours of first pitch.
              <table style="margin-top:8px;font-size:11px;width:100%;border-collapse:collapse;">
                <tr style="color:#64748b;text-transform:uppercase;font-size:10px;">
                  <th align="left">Type</th><th align="left">Games</th>
                  <th align="left">Earliest</th><th align="left">Latest</th>
                </tr>
                {rows_md}
              </table>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("#### Database Coverage by Season")
        df = query("""
            SELECT
                g.season,
                COUNT(DISTINCT g.game_pk)                               AS total_games,
                COUNT(DISTINCT il.game_pk) FILTER (WHERE il.status='success')   AS loaded_games,
                COUNT(DISTINCT il.game_pk) FILTER (WHERE il.pbp_rows > 0)       AS games_with_pbp,
                SUM(il.pbp_rows)                                        AS total_pbp_rows,
                COUNT(DISTINCT il.game_pk) FILTER (WHERE il.status='error')     AS error_games,
                ROUND(100.0 * COUNT(DISTINCT il.game_pk)
                    FILTER (WHERE il.pbp_rows > 0)
                    / NULLIF(COUNT(DISTINCT g.game_pk),0), 1)           AS pbp_pct
            FROM       games g
            LEFT JOIN  ingest_log il ON il.game_pk = g.game_pk
            WHERE      g.game_type = 'R'
            GROUP BY   g.season
            ORDER BY   g.season DESC
        """)
        show_df(df, 450)

        st.markdown("<hr class='sec-div'>", unsafe_allow_html=True)
        st.markdown("#### Odds Coverage by Season")
        df2 = query("""
            SELECT g.season,
                   COUNT(DISTINCT go.game_pk)  AS games_with_odds,
                   COUNT(*)                    AS total_snapshots,
                   COUNT(*) FILTER (WHERE go.is_opening_line=1) AS opening_lines,
                   COUNT(*) FILTER (WHERE go.is_closing_line=1) AS closing_lines,
                   MIN(go.captured_at_utc)     AS earliest,
                   MAX(go.captured_at_utc)     AS latest
            FROM   game_odds go
            JOIN   games g ON g.game_pk = go.game_pk
            WHERE  go.is_closing_line = 1
            GROUP  BY g.season
            ORDER  BY g.season DESC
        """)
        if df2.empty:
            st.markdown('<div class="info-box">No odds data loaded yet.</div>', unsafe_allow_html=True)
        else:
            show_df(df2, 350)


# ═════════════════════════════════════════════════════════════
#  VIEW: MODEL WORKBENCH
# ═════════════════════════════════════════════════════════════

def view_workbench():
    panel_header("Model Workbench", "Predictions & Backtesting",
                 "Log predictions and run backtest evaluations against historical closing lines.")

    if not db_available():
        no_db_warning(); return

    tab_log, tab_backtest = st.tabs(["Log a Prediction", "Run Backtest"])

    # ── Log a Prediction ───────────────────────────────────
    with tab_log:
        st.markdown("""
        <div class="info-box">
          Log a pre-game prediction before first pitch.  The grader will
          match it to the actual result after the game completes.
        </div>
        """, unsafe_allow_html=True)

        c1, c2, c3 = st.columns([1, 1, 1])
        with c1:
            pred_date   = st.date_input("Game date", value=date.today(), key="pred_date")
            model_ver   = st.text_input("Model version", value="v2.2", key="pred_model")
            signal_name = st.selectbox("Signal that fired",
                              ["S1+H2", "MV-F", "MV-B", "S1", "H3b", "Manual", "None"],
                              key="pred_signal",
                              help="Which model signal generated this pick. Used to track per-signal live performance.")
        with c2:
            pred_type   = st.selectbox("Prediction type",
                              ["game_winner","run_line","total","player_prop"], key="pred_type")
            pred_side   = st.selectbox("Predicted side",
                              ["home","away","over","under"], key="pred_side")
        with c3:
            pred_conf   = st.number_input("Confidence (0–1)", 0.0, 1.0, 0.55, 0.01, key="pred_conf")
            bet_made    = st.checkbox("Simulate a bet", value=True, key="pred_bet")
            bet_size    = st.number_input("Bet size (units)", 0.5, 10.0, 1.0, 0.5, key="pred_size")

        # Game picker for the selected date
        games_today = query("""
            SELECT g.game_pk,
                   ta.abbreviation || ' @ ' || th.abbreviation AS matchup,
                   g.game_start_utc
            FROM   games g
            JOIN   teams th ON th.team_id = g.home_team_id
            JOIN   teams ta ON ta.team_id = g.away_team_id
            WHERE  g.game_date_et = ? AND g.game_type = 'R'
            ORDER  BY g.game_start_utc
        """, (str(pred_date),))

        if games_today.empty:
            st.markdown('<div class="warn-box">No games found for this date in the database.</div>',
                        unsafe_allow_html=True)
        else:
            matchup_opts = dict(zip(games_today["matchup"], games_today["game_pk"]))
            sel_matchup  = st.selectbox("Select game", list(matchup_opts.keys()), key="pred_game")
            sel_game_pk  = matchup_opts[sel_matchup]

            pred_val  = st.number_input("Predicted value (e.g. win prob or stat total)",
                                         0.0, 999.0, 0.0, 0.01, key="pred_val")
            edge_mkt  = st.number_input("Edge over market (implied prob difference)",
                                         -1.0, 1.0, 0.0, 0.001, key="pred_edge")
            bet_odds  = st.number_input("Odds at time of bet (American, e.g. -110)",
                                         -5000, 5000, -110, 1, key="pred_odds")
            notes     = st.text_input("Notes (optional)", key="pred_notes")

            if st.button("⚡  Log Prediction", key="pred_submit", type="primary"):
                con = get_connection()
                now_utc = datetime.utcnow().isoformat()
                try:
                    # Encode signal into model_version string so per-signal
                    # slicing is possible without a schema migration.
                    # Format: "v2.2-MV-B"  or  "v2.2-Manual"
                    versioned = f"{model_ver}-{signal_name}" if signal_name != "None" else model_ver
                    con.execute("""
                        INSERT INTO model_predictions
                            (game_pk, prediction_type, predicted_side, predicted_value,
                             confidence, edge_over_market, model_version,
                             predicted_at_utc, bet_made, bet_odds_used, bet_size_units)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """, (sel_game_pk, pred_type, pred_side, pred_val,
                          pred_conf, edge_mkt, versioned, now_utc,
                          1 if bet_made else 0, bet_odds, bet_size))
                    con.commit()
                    st.markdown('<div class="ok-box">✓ Prediction logged successfully.</div>',
                                unsafe_allow_html=True)
                except Exception as e:
                    st.error(f"Error logging prediction: {e}")

    # ── Run Backtest ────────────────────────────────────────
    with tab_backtest:
        st.markdown("""
        <div class="info-box">
          This tab evaluates predictions <strong>logged via the Log a Prediction form above</strong>
          against actual outcomes and closing lines. It is <em>not</em> the same as running
          <code>backtest_top_pick.py</code> from the command line.<br><br>
          To run a full signal-engine backtest (the model's historical Top Pick performance),
          use the <strong>Generate Brief</strong> tab in Operations, or run directly:<br>
          <code>python backtest_top_pick.py --from 2023-04-01 --to 2025-09-30</code><br><br>
          Results here are never mixed with live predictions.
        </div>
        """, unsafe_allow_html=True)

        c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
        with c1:
            bt_model = st.text_input("Model version filter", value="", placeholder="all", key="bt_model")
        with c2:
            bt_type  = st.selectbox("Prediction type",
                          ["All","game_winner","run_line","total","player_prop"], key="bt_type")
        with c3:
            bt_start = st.date_input("From", value=date(2023, 4, 1), key="bt_start")
        with c4:
            bt_end   = st.date_input("To",   value=date.today(),      key="bt_end")

        if st.button("Run Backtest", key="bt_run", type="primary"):
            clauses = ["g.game_date_et BETWEEN :start AND :end",
                       "mp.predicted_at_utc < g.game_start_utc"]  # enforced pre-game only
            params  = {"start": str(bt_start), "end": str(bt_end)}

            if bt_model.strip():
                clauses.append("mp.model_version = :model")
                params["model"] = bt_model.strip()
            if bt_type != "All":
                clauses.append("mp.prediction_type = :ptype")
                params["ptype"] = bt_type

            where = " AND ".join(clauses)
            df = query(f"""
                SELECT mp.model_version, mp.prediction_type,
                       g.season, g.game_date_et AS game_date,
                       th.abbreviation AS home, ta.abbreviation AS away,
                       mp.predicted_side, mp.confidence, mp.edge_over_market,
                       mp.bet_odds_used, mp.bet_size_units,
                       br.prediction_correct, br.profit_loss_units,
                       br.closing_line_value, br.bet_outcome
                FROM   model_predictions mp
                JOIN   games   g  ON g.game_pk    = mp.game_pk
                JOIN   teams   th ON th.team_id   = g.home_team_id
                JOIN   teams   ta ON ta.team_id   = g.away_team_id
                LEFT JOIN backtest_results br ON br.prediction_id = mp.id
                WHERE  {where}
                ORDER  BY g.game_date_et DESC
            """, params)

            if df.empty:
                st.markdown('<div class="info-box">No predictions found for these filters.</div>',
                            unsafe_allow_html=True)
            else:
                total = len(df)
                graded = df["prediction_correct"].notna().sum()
                correct = (df["prediction_correct"] == 1).sum()
                hit_rate = round(100 * correct / graded, 1) if graded > 0 else 0
                total_units = round(df["profit_loss_units"].sum(), 2) if "profit_loss_units" in df.columns else 0
                avg_clv = round(df["closing_line_value"].mean(), 4) if "closing_line_value" in df.columns else 0

                stat_cards([
                    ("Total Predictions", str(total),      f"{graded} graded",   "#3b82f6"),
                    ("Hit Rate",          f"{hit_rate}%",  f"{correct} correct", "#10b981"),
                    ("Net Units",         str(total_units),"P&L",                "#e8a020"),
                    ("Avg CLV",           str(avg_clv),    "skill signal",       "#8b5cf6"),
                ])
                show_df(df, 420)


# ═════════════════════════════════════════════════════════════
#  VIEW: OPERATIONS
# ═════════════════════════════════════════════════════════════

def run_script(script_name: str, args: list = []) -> tuple[bool, str]:
    """Run a project script and return (success, output)."""
    script_path = os.path.join(SCRIPTS_DIR, script_name)
    if not os.path.exists(script_path):
        return False, f"Script not found: {script_path}"
    try:
        result = subprocess.run(
            [sys.executable, script_path] + args,
            capture_output=True, text=True, timeout=300,
            cwd=SCRIPTS_DIR
        )
        output = result.stdout + result.stderr
        return result.returncode == 0, output
    except subprocess.TimeoutExpired:
        return False, "Script timed out after 5 minutes."
    except Exception as e:
        return False, str(e)


def view_operations():
    panel_header("Operations", "Data Loading & Health",
                 "Trigger data loads, monitor ingest logs, and check API quota.")

    # Admin gate — enforced independently of the main app password gate.
    # Set ENABLE_OPS_ADMIN_GATE = True and add admin_password to secrets.toml
    # before sharing the app with anyone else.
    if ENABLE_OPS_ADMIN_GATE:
        _password_gate("admin_password", "_auth_ops", "Operations — Admin Access Required")

    if not db_available():
        no_db_warning(); return

    tab_health, tab_stats_load, tab_odds_load, tab_brief, tab_logs = st.tabs([
        "DB Health", "Load Stats", "Load Odds", "Generate Brief", "Logs"
    ])

    # ── Health ─────────────────────────────────────────────
    with tab_health:
        st.markdown("#### Row Counts")
        tables = ["seasons","venues","teams","players","games",
                  "player_game_stats","play_by_play","standings",
                  "game_odds","player_props",
                  "line_movement","model_predictions","backtest_results",
                  "game_probable_pitchers","brief_picks","daily_pnl",
                  "ingest_log","odds_ingest_log"]
        rows_data = []
        for t in tables:
            try:
                n = query(f"SELECT COUNT(*) AS n FROM {t}")
                count = int(n["n"].iloc[0]) if not n.empty else 0
                status = "empty" if count == 0 else "ok"
            except Exception:
                count = -1; status = "missing"
            rows_data.append({"Table": t, "Rows": f"{count:,}", "Status": status})
        df_health = pd.DataFrame(rows_data)
        show_df(df_health, 520)

        st.markdown("<hr class='sec-div'>", unsafe_allow_html=True)
        st.markdown("#### Recent Ingest Errors")
        df_err = query("""
            SELECT il.game_pk, g.game_date_et AS game_date,
                   th.abbreviation || ' @ ' || ta.abbreviation AS matchup,
                   il.status, il.error_message, il.last_attempted_utc
            FROM   ingest_log il
            JOIN   games g  ON g.game_pk    = il.game_pk
            JOIN   teams th ON th.team_id   = g.home_team_id
            JOIN   teams ta ON ta.team_id   = g.away_team_id
            WHERE  il.status = 'error'
            ORDER  BY il.last_attempted_utc DESC
            LIMIT  25
        """)
        if df_err.empty:
            st.markdown('<div class="ok-box">✓ No errors in ingest_log.</div>', unsafe_allow_html=True)
        else:
            show_df(df_err, 320)

    # ── Load Stats ─────────────────────────────────────────
    with tab_stats_load:
        st.markdown("""
        <div class="info-box">
          These buttons call load_mlb_stats.py directly.  Output is shown below.
          Long-running loads (full season) will show output when complete.
          For overnight loads use Windows Task Scheduler instead.
        </div>
        """, unsafe_allow_html=True)

        c1, c2 = st.columns([1, 1])

        st.markdown("**Morning Schedule Load** — run after 6 AM; loads today's games (including West Coast)")
        c0a, c0b = st.columns([1, 1])
        with c0a:
            ld_date = st.date_input("Date", value=date.today(), key="op_today_date")
        with c0b:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            if st.button("▶  Load Today's Schedule (load_today.py)", key="op_today", type="primary"):
                with st.spinner("Running load_today.py ..."):
                    ok, out = run_script("load_today.py", ["--date", str(ld_date)])
                st.markdown(f'<div class="log-block">{"✓ " if ok else "✕ "}{out}</div>', unsafe_allow_html=True)

        st.markdown("<hr class='sec-div'>", unsafe_allow_html=True)
        st.markdown("**Weather + Starters Refresh** — runs 3× daily (8:45 AM, noon, 5 PM); per-game starter lookup")
        c0c, c0d, c0e = st.columns([1, 1, 1])
        with c0c:
            wx_date = st.date_input("Date", value=date.today(), key="op_wx_date")
        with c0d:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            if st.button("▶  Weather + Starters (load_weather.py)", key="op_wx", type="primary"):
                with st.spinner("Running load_weather.py ..."):
                    ok, out = run_script("load_weather.py", ["--date", str(wx_date)])
                st.markdown(f'<div class="log-block">{"✓ " if ok else "✕ "}{out}</div>', unsafe_allow_html=True)
        with c0e:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            if st.button("▶  Starters Only (--no-weather)", key="op_wx_nostarters"):
                with st.spinner("Refreshing starters ..."):
                    ok, out = run_script("load_weather.py", ["--date", str(wx_date), "--no-weather"])
                st.markdown(f'<div class="log-block">{"✓ " if ok else "✕ "}{out}</div>', unsafe_allow_html=True)

        st.markdown("<hr class='sec-div'>", unsafe_allow_html=True)
        st.markdown("**Debug Starters** — diagnose why a game is missing probable pitchers")
        c0f, c0g, c0h = st.columns([1, 1, 1])
        with c0f:
            dbg_date = st.date_input("Date", value=date.today(), key="op_dbg_date")
        with c0g:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            if st.button("◈  Debug Starters (dry run)", key="op_dbg_starters"):
                with st.spinner("Running debug_starters.py ..."):
                    ok, out = run_script("debug_starters.py", ["--date", str(dbg_date)])
                st.markdown(f'<div class="log-block">{out}</div>', unsafe_allow_html=True)
        with c0h:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            if st.button("▶  Debug + Write", key="op_dbg_write", type="primary"):
                with st.spinner("Running debug_starters.py --write ..."):
                    ok, out = run_script("debug_starters.py", ["--date", str(dbg_date), "--write"])
                st.markdown(f'<div class="log-block">{"✓ " if ok else "✕ "}{out}</div>', unsafe_allow_html=True)

        st.markdown("<hr class='sec-div'>", unsafe_allow_html=True)
        st.markdown("**Backfill Starters** — one-time: populate starters for past games")
        c0i, c0j, c0k = st.columns([1, 1, 1])
        with c0i:
            bf_start = st.date_input("From", value=date(2026, 3, 25), key="op_bf_start")
            bf_end   = st.date_input("To",   value=date.today(),      key="op_bf_end")
        with c0j:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            if st.button("◈  Dry Run Backfill", key="op_bf_dry"):
                with st.spinner("Running backfill_starters.py --dry-run ..."):
                    ok, out = run_script("backfill_starters.py",
                        ["--start", str(bf_start), "--end", str(bf_end), "--dry-run"])
                st.markdown(f'<div class="log-block">{out}</div>', unsafe_allow_html=True)
        with c0k:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            if st.button("▶  Run Backfill", key="op_bf_run", type="primary"):
                with st.spinner("Running backfill_starters.py ..."):
                    ok, out = run_script("backfill_starters.py",
                        ["--start", str(bf_start), "--end", str(bf_end)])
                st.markdown(f'<div class="log-block">{"✓ " if ok else "✕ "}{out}</div>', unsafe_allow_html=True)

        st.markdown("<hr class='sec-div'>", unsafe_allow_html=True)
        c1, c2 = st.columns([1, 1])
        with c1:
            st.markdown("**Daily Update** — loads yesterday's games")
            if st.button("▶  Load Yesterday", key="op_daily", type="primary"):
                with st.spinner("Running load_mlb_stats.py ..."):
                    ok, out = run_script("load_mlb_stats.py")
                st.markdown(
                    f'<div class="log-block">{"✓ " if ok else "✕ "}{out}</div>',
                    unsafe_allow_html=True
                )

        with c2:
            st.markdown("**Retry Errors** — re-attempts all failed games")
            if st.button("↺  Retry Errors", key="op_retry"):
                with st.spinner("Running --retry-errors ..."):
                    ok, out = run_script("load_mlb_stats.py", ["--retry-errors"])
                st.markdown(
                    f'<div class="log-block">{"✓ " if ok else "✕ "}{out}</div>',
                    unsafe_allow_html=True
                )

        st.markdown("<hr class='sec-div'>", unsafe_allow_html=True)
        st.markdown("**Check Odds Readiness** — verify closing lines before generating brief")
        if st.button("◉  Run check_odds_ready.py", key="op_check_odds"):
            with st.spinner("Running check_odds_ready.py ..."):
                ok, out = run_script("check_odds_ready.py")
            st.markdown(
                f'<div class="log-block">{"✓ " if ok else "✕ "}{out}</div>',
                unsafe_allow_html=True
            )

    # ── Load Odds ──────────────────────────────────────────
    with tab_odds_load:
        odds_script = os.path.join(SCRIPTS_DIR, "load_odds.py")
        if not os.path.exists(odds_script):
            st.markdown('<div class="warn-box">⚠ load_odds.py not found in your mlb_stats folder.</div>',
                        unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="info-box">
              Requires THE_ODDS_API_KEY set in your .env file.
              Each pull is logged to odds_ingest_log with quota remaining.
            </div>
            """, unsafe_allow_html=True)

            # ── Quota monitor ─────────────────────────────
            quota_df = query("""
                SELECT api_quota_remaining
                FROM   odds_ingest_log
                ORDER  BY pulled_at_utc DESC LIMIT 1
            """)
            if not quota_df.empty:
                quota_val = int(quota_df["api_quota_remaining"].iloc[0])
                quota_color = "#ef4444" if quota_val < 500 else "#e8a020" if quota_val < 2000 else "#10b981"
                st.markdown(
                    f'<div style="background:#14171f;border:1px solid {quota_color}33;'                    f'border-radius:4px;padding:10px 14px;margin-bottom:12px;'                    f'font-family:IBM Plex Mono,monospace;font-size:11px;">'                    f'<span style="color:#64748b;text-transform:uppercase;letter-spacing:1px;">API Quota Remaining</span>'                    f'&nbsp;&nbsp;<span style="color:{quota_color};font-size:15px;font-weight:700;">{quota_val:,}</span>'                    f'&nbsp;&nbsp;<span style="color:#475569;">requests — check reset date at the-odds-api.com</span></div>',
                    unsafe_allow_html=True
                )

            c1, c2, c3 = st.columns([1, 1, 1])
            with c1:
                st.markdown("**Pregame Pull — game only** *(recommended)*")
                st.caption("Safest option. Avoids F5 422 error. All signals use game lines only.")
                if st.button("▶  Pregame (game only)", key="op_odds_game", type="primary"):
                    with st.spinner("Pulling game odds ..."):
                        ok, out = run_script("load_odds.py", ["--pregame", "--markets", "game"])
                    st.markdown(f'<div class="log-block">{"✓ " if ok else "✕ "}{out}</div>', unsafe_allow_html=True)
            with c2:
                st.markdown("**Pregame Pull — all markets**")
                st.caption("⚠ F5 markets (h2h_1st_5_innings etc.) may return 422 depending on plan.")
                if st.button("▶  Pregame (all markets)", key="op_odds_all"):
                    with st.spinner("Pulling all odds ..."):
                        ok, out = run_script("load_odds.py", ["--pregame"])
                    st.markdown(f'<div class="log-block">{"✓ " if ok else "✕ "}{out}</div>', unsafe_allow_html=True)
            with c3:
                st.markdown("**Late Games Pull** — ~8 PM for West Coast games")
                st.caption("Run after 8 PM when late West Coast lines are set.")
                if st.button("▶  Late Games (--late-games)", key="op_odds_late", type="primary"):
                    with st.spinner("Pulling late game odds ..."):
                        ok, out = run_script("load_odds.py", ["--late-games", "--markets", "game"])
                    st.markdown(f'<div class="log-block">{"✓ " if ok else "✕ "}{out}</div>', unsafe_allow_html=True)

            st.markdown("<hr class='sec-div'>", unsafe_allow_html=True)
            c4, c5 = st.columns([1, 1])
            with c4:
                st.markdown("**Compute Movement** — yesterday's games")
                st.caption("Run at 11:30 PM after games complete.")
                if st.button("◈  Compute Line Movement", key="op_movement"):
                    with st.spinner("Computing line movement ..."):
                        ok, out = run_script("load_odds.py", ["--compute-movement"])
                    st.markdown(f'<div class="log-block">{"✓ " if ok else "✕ "}{out}</div>', unsafe_allow_html=True)

            st.markdown("<hr class='sec-div'>", unsafe_allow_html=True)
            st.markdown("#### Odds Ingest Log")
            df_olog = query("""
                SELECT pulled_at_utc, pull_type, markets_pulled,
                       games_covered, odds_rows_inserted, props_rows_inserted,
                       api_requests_used, api_quota_remaining, status
                FROM   odds_ingest_log
                ORDER  BY pulled_at_utc DESC
                LIMIT  30
            """)
            if df_olog.empty:
                st.markdown('<div class="info-box">No odds pulls logged yet.</div>',
                            unsafe_allow_html=True)
            else:
                show_df(df_olog, 350)



    # ── Logs ──────────────────────────────────────────────────────
    with tab_logs:
        import os as _os
        log_dir = _os.path.join(SCRIPTS_DIR, "logs")
        if not _os.path.isdir(log_dir):
            st.markdown('<div class="info-box">logs/ folder not found. Run any script to create it.</div>', unsafe_allow_html=True)
        else:
            log_files = sorted(
                [f for f in _os.listdir(log_dir) if f.endswith(".log")],
                reverse=True
            )
            if not log_files:
                st.markdown('<div class="info-box">No log files found in logs/.</div>', unsafe_allow_html=True)
            else:
                selected_log = st.selectbox("Select log file", log_files, key="log_select")
                if selected_log:
                    log_path = _os.path.join(log_dir, selected_log)
                    try:
                        with open(log_path, "r", encoding="utf-8", errors="replace") as lf:
                            log_lines = lf.readlines()
                        n_lines = len(log_lines)
                        tail = "".join(log_lines[-300:])
                        st.caption(f"{selected_log}  ·  {n_lines} lines  ·  last 300 shown")
                        st.markdown(f'<div class="log-block">{tail}</div>', unsafe_allow_html=True)
                    except Exception as e:
                        st.error(f"Could not read log: {e}")

    # ── Generate Brief ─────────────────────────────────────
    with tab_brief:
        brief_script = os.path.join(SCRIPTS_DIR, "generate_daily_brief.py")
        if not os.path.exists(brief_script):
            st.markdown('<div class="warn-box">⚠ generate_daily_brief.py not found in your mlb_stats folder.</div>',
                        unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="info-box">
              Generate a brief directly from Scout without switching to a terminal.
              Uses <code>--dry-run</code> mode — output is shown here and not written
              to brief_log or the briefs/ folder. To save a brief, run from the
              command line without --dry-run.
            </div>
            """, unsafe_allow_html=True)

            c1, c2 = st.columns([1, 1])
            with c1:
                brief_date = st.date_input("Game date", value=date.today(), key="brief_date")
            with c2:
                brief_session = st.selectbox(
                    "Session",
                    ["prior", "morning", "early", "afternoon", "primary", "late", "closing"],
                    index=4,   # default to primary
                    key="brief_session",
                    help="prior=yesterday results | morning=watch list | primary=main pick | closing=confirmation"
                )

            st.markdown("""
            | Session | When to use | Odds needed |
            |---------|-------------|-------------|
            | prior | 6:30 AM — yesterday's results | Yesterday's closing lines |
            | morning | 9:30 AM — watch list | 9 AM odds pull |
            | primary | 5:30 PM — **main daily pick** | 5 PM odds pull |
            | closing | 6:45 PM — confirmation | 6:30 PM odds pull |
            """)

            col_run, col_warn = st.columns([1, 2])
            with col_run:
                run_brief = st.button("▶  Generate Brief (dry run)", key="brief_run", type="primary")
            with col_warn:
                st.markdown('<div style="padding-top:8px;font-size:11px;color:#64748b;">'
                             'Dry run only — output shown here, not saved to disk.</div>',
                             unsafe_allow_html=True)

            if run_brief:
                with st.spinner(f"Running {brief_session} brief for {brief_date} ..."):
                    ok, out = run_script(
                        "generate_daily_brief.py",
                        ["--session", brief_session,
                         "--date", str(brief_date),
                         "--dry-run",
                         "--warn-missing"]
                    )
                st.markdown(
                    f'<div class="log-block">{"✓ " if ok else "✕ "}{out}</div>',
                    unsafe_allow_html=True
                )

            st.markdown("<hr class='sec-div'>", unsafe_allow_html=True)
            st.markdown("#### Brief Log — Recent Sessions")
            try:
                df_blog = query("""
                    SELECT game_date, session, games_covered, picks_count,
                           output_file, generated_at
                    FROM   brief_log
                    ORDER  BY generated_at DESC
                    LIMIT  30
                """)
                if df_blog.empty:
                    st.markdown('<div class="info-box">No briefs logged yet. Run generate_daily_brief.py from the command line to populate brief_log.</div>',
                                unsafe_allow_html=True)
                else:
                    show_df(df_blog, 360)
            except Exception:
                st.markdown('<div class="info-box">brief_log table not found — run generate_daily_brief.py at least once to create it.</div>',
                             unsafe_allow_html=True)

            st.markdown("<hr class='sec-div'>", unsafe_allow_html=True)
            st.markdown("#### Run Backtest via Command Line")
            st.markdown("""
            The in-app Generate Brief uses dry-run mode for safety. For full signal backtests,
            copy and run these commands from your mlb_stats folder:
            """)
            st.code("python regression_2026.py --verbose --csv", language="bash")
            st.code("python regression_2026.py --missed-only", language="bash")
            st.code("python show_boxscore.py --date 2026-04-10", language="bash")
            st.code("python show_boxscore.py --date 2026-04-10 --game CWS@KC", language="bash")


# ═════════════════════════════════════════════════════════════
#  VIEW: SCORECARD
# ═════════════════════════════════════════════════════════════

def view_scorecard():
    panel_header("Scorecard", "Model Performance",
                 "Backtest and live results shown separately.  These views are never combined.")

    if not db_available():
        no_db_warning(); return

    # Check if any predictions exist
    pred_count = query("SELECT COUNT(*) AS n FROM model_predictions")
    total_preds = int(pred_count["n"].iloc[0]) if not pred_count.empty else 0

    if total_preds == 0:
        st.markdown("""
        <div class="info-box">
          No predictions logged yet.  Use the Model Workbench to log predictions,
          then come back here to track performance.
        </div>
        """, unsafe_allow_html=True)
        return

    tab_bt, tab_live, tab_live26, tab_regression, tab_compare = st.tabs(["Backtest Results", "Live Results (model_predictions)", "Live 2026 Signals", "Regression", "Model Comparison"])

    # ── Backtest Results ────────────────────────────────────
    with tab_bt:
        st.markdown("""
        <div class="warn-box">
          BACKTEST — predictions evaluated against historical data.
          Performance here reflects in-sample or out-of-sample historical testing,
          not live deployment.
        </div>
        """, unsafe_allow_html=True)

        df_bt = query("""
            SELECT mp.model_version, mp.prediction_type, mp.prop_type,
                   COUNT(*)                                          AS predictions,
                   SUM(mp.bet_made)                                  AS bets,
                   ROUND(100.0 * SUM(CASE WHEN br.prediction_correct=1 THEN 1 ELSE 0 END)
                       / NULLIF(COUNT(br.id),0), 1)                  AS hit_rate_pct,
                   ROUND(SUM(COALESCE(br.profit_loss_units,0)), 2)   AS net_units,
                   ROUND(100.0 * SUM(COALESCE(br.profit_loss_units,0))
                       / NULLIF(SUM(mp.bet_made),0), 2)              AS roi_pct,
                   ROUND(AVG(COALESCE(br.closing_line_value,0)), 4)  AS avg_clv
            FROM   model_predictions mp
            JOIN   games g ON g.game_pk = mp.game_pk
            LEFT JOIN backtest_results br ON br.prediction_id = mp.id
            WHERE  mp.predicted_at_utc < g.game_start_utc
            GROUP  BY mp.model_version, mp.prediction_type, mp.prop_type
            ORDER  BY roi_pct DESC NULLS LAST
        """)
        show_df(df_bt, 400)

    # ── Live Results ────────────────────────────────────────
    with tab_live:
        st.markdown("""
        <div class="ok-box">
          LIVE — predictions made in real time before first pitch.
          This is the only number that matters for evaluating true edge.
        </div>
        """, unsafe_allow_html=True)

        # Live = predictions where predicted_at_utc is within 24h of game_start
        # and the game has already been played
        df_live = query("""
            SELECT mp.model_version, mp.prediction_type,
                   COUNT(*)                                          AS predictions,
                   SUM(mp.bet_made)                                  AS bets,
                   ROUND(100.0 * SUM(CASE WHEN br.prediction_correct=1 THEN 1 ELSE 0 END)
                       / NULLIF(COUNT(br.id),0), 1)                  AS hit_rate_pct,
                   ROUND(SUM(COALESCE(br.profit_loss_units,0)), 2)   AS net_units,
                   ROUND(100.0 * SUM(COALESCE(br.profit_loss_units,0))
                       / NULLIF(SUM(mp.bet_made),0), 2)              AS roi_pct,
                   ROUND(AVG(COALESCE(br.closing_line_value,0)), 4)  AS avg_clv,
                   MIN(g.game_date_et)                               AS first_game,
                   MAX(g.game_date_et)                               AS last_game
            FROM   model_predictions mp
            JOIN   games g ON g.game_pk = mp.game_pk
            LEFT JOIN backtest_results br ON br.prediction_id = mp.id
            WHERE  mp.predicted_at_utc >= datetime(g.game_start_utc, '-24 hours')
              AND  mp.predicted_at_utc <  g.game_start_utc
              AND  g.status = 'Final'
            GROUP  BY mp.model_version, mp.prediction_type
            ORDER  BY roi_pct DESC NULLS LAST
        """)
        if df_live.empty:
            st.markdown('<div class="info-box">No live predictions graded yet.</div>',
                        unsafe_allow_html=True)
        else:
            show_df(df_live, 400)


    # ── Live 2026 Signals ───────────────────────────────────
    with tab_live26:
        st.markdown("""
        <div class="ok-box">
          LIVE 2026 — picks logged by generate_daily_brief.py (brief_picks) graded via the prior-day report (daily_pnl).
          This is the true live performance record for the current season.
        </div>
        """, unsafe_allow_html=True)

        cur_yr = date.today().year
        try:
            df_picks = query("""
                SELECT dp.game_date, dp.signal, dp.bet, dp.market,
                       dp.odds, dp.stake_dollars, dp.result,
                       dp.pnl_units, dp.pnl_dollars, dp.pick_tier,
                       ta.abbreviation || '@' || th.abbreviation AS matchup
                FROM   daily_pnl dp
                JOIN   games g  ON g.game_pk   = dp.game_pk
                JOIN   teams th ON th.team_id  = g.home_team_id
                JOIN   teams ta ON ta.team_id  = g.away_team_id
                WHERE  g.season = :yr AND g.game_type = 'R'
                ORDER  BY dp.game_date DESC
            """, {"yr": cur_yr})

            if df_picks.empty:
                st.markdown('<div class="info-box">No picks graded yet for 2026. Picks appear here after the prior-day report runs.</div>', unsafe_allow_html=True)
            else:
                # Summary cards
                wins   = int((df_picks["result"] == "WIN").sum())
                losses = int((df_picks["result"] == "LOSS").sum())
                net    = round(float(df_picks["pnl_dollars"].sum()), 2)
                net_u  = round(float(df_picks["pnl_units"].sum()), 2)
                bank   = round(500 + net, 2)
                hit    = round(100*wins/(wins+losses), 1) if (wins+losses) > 0 else 0
                stat_cards([
                    ("Paper Bank",   f"${bank:.2f}", f"W:{wins} L:{losses}", "#10b981"),
                    ("Net Units",    f"{net_u:+.2f}u", f"${net:+.2f}", "#e8a020"),
                    ("Hit Rate",     f"{hit}%", f"{wins}/{wins+losses} graded", "#3b82f6"),
                    ("Total Fires",  str(len(df_picks)), "2026 season", "#8b5cf6"),
                ])

                # Per-signal breakdown
                st.markdown("<hr class='sec-div'>", unsafe_allow_html=True)
                st.markdown("#### Per-signal breakdown")
                df_sig = query("""
                    SELECT dp.signal,
                           COUNT(*) AS fires,
                           SUM(CASE WHEN dp.result='WIN' THEN 1 ELSE 0 END) AS wins,
                           SUM(CASE WHEN dp.result='LOSS' THEN 1 ELSE 0 END) AS losses,
                           ROUND(100.0*SUM(CASE WHEN dp.result='WIN' THEN 1 ELSE 0 END)
                               /NULLIF(SUM(CASE WHEN dp.result IN('WIN','LOSS') THEN 1 ELSE 0 END),0),1) AS hit_pct,
                           ROUND(SUM(dp.pnl_units),2) AS net_units,
                           ROUND(SUM(dp.pnl_dollars),2) AS net_dollars
                    FROM   daily_pnl dp
                    JOIN   games g ON g.game_pk=dp.game_pk
                    WHERE  g.season=:yr AND g.game_type='R'
                    GROUP  BY dp.signal ORDER BY net_units DESC
                """, {"yr": cur_yr})
                show_df(df_sig, 280)

                # Full pick log
                st.markdown("<hr class='sec-div'>", unsafe_allow_html=True)
                st.markdown("#### Full pick log")
                show_df(df_picks, 420)

        except Exception as e:
            st.markdown(f'<div class="warn-box">Live 2026 signals unavailable: {e}</div>', unsafe_allow_html=True)

    # ── Regression ──────────────────────────────────────────
    with tab_regression:
        st.markdown("""
        <div class="info-box">
          Runs regression_2026.py retroactively — re-evaluates all signals on completed games
          using actual post-game wind and closing odds.  Surfaces missed signals and false fires.
        </div>
        """, unsafe_allow_html=True)

        c1, c2, c3 = st.columns([1, 1, 1])
        with c1:
            reg_start = st.date_input("From", value=date(2026, 3, 25), key="reg_start")
            reg_end   = st.date_input("To",   value=date.today() - timedelta(days=1), key="reg_end")
        with c2:
            reg_missed = st.checkbox("Missed signals only", key="reg_missed")
            reg_csv    = st.checkbox("Write CSV", key="reg_csv")
        with c3:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            run_reg = st.button("▶  Run Regression", key="reg_run", type="primary")

        if run_reg:
            reg_args = ["--start", str(reg_start), "--end", str(reg_end), "--verbose"]
            if reg_missed: reg_args.append("--missed-only")
            if reg_csv:    reg_args.append("--csv")
            with st.spinner("Running regression_2026.py ..."):
                ok, out = run_script("regression_2026.py", reg_args)
            st.markdown(f'<div class="log-block">{"✓ " if ok else "✕ "}{out}</div>', unsafe_allow_html=True)

        st.markdown("<hr class='sec-div'>", unsafe_allow_html=True)
        st.markdown("#### CLI commands")
        st.code("python regression_2026.py --verbose --csv", language="bash")
        st.code("python regression_2026.py --missed-only", language="bash")

    # ── Model Comparison ────────────────────────────────────
    with tab_compare:
        st.markdown("#### All Model Versions — Aggregate Scorecard")
        try:
            df_comp = query("SELECT * FROM v_model_performance")
        except Exception:
            df_comp = pd.DataFrame()
            st.markdown('<div class="info-box">v_model_performance view not found — run the schema migration.</div>', unsafe_allow_html=True)
        show_df(df_comp, 450)

        st.markdown("<hr class='sec-div'>", unsafe_allow_html=True)
        st.markdown("#### CLV Distribution by Model Version")
        st.markdown("""
        <div class="info-box">
          Consistent positive avg_clv = real edge.  Positive ROI without positive CLV = variance.
        </div>
        """, unsafe_allow_html=True)

        df_clv = query("""
            SELECT mp.model_version,
                   COUNT(br.id)                                    AS graded,
                   ROUND(AVG(br.closing_line_value)*100, 2)        AS avg_clv_pct,
                   ROUND(MIN(br.closing_line_value)*100, 2)        AS min_clv_pct,
                   ROUND(MAX(br.closing_line_value)*100, 2)        AS max_clv_pct,
                   SUM(CASE WHEN br.closing_line_value > 0 THEN 1 ELSE 0 END) AS positive_clv,
                   ROUND(100.0 * SUM(CASE WHEN br.closing_line_value > 0 THEN 1 ELSE 0 END)
                       / NULLIF(COUNT(br.id),0), 1)                AS pct_positive_clv
            FROM   model_predictions mp
            JOIN   backtest_results br ON br.prediction_id = mp.id
            GROUP  BY mp.model_version
            ORDER  BY avg_clv_pct DESC
        """)
        if df_clv.empty:
            st.markdown('<div class="info-box">No graded results yet.</div>', unsafe_allow_html=True)
        else:
            show_df(df_clv, 320)


# ═════════════════════════════════════════════════════════════
#  MAIN ROUTER
# ═════════════════════════════════════════════════════════════
# Spacer so content clears the fixed topbar
st.markdown("<div style='height:56px'></div>", unsafe_allow_html=True)
st.markdown('<div class="main-wrap">', unsafe_allow_html=True)

view = st.session_state.view
if   view == "home":       view_home()
elif view == "explorer":   view_explorer()
elif view == "workbench":  view_workbench()
elif view == "operations": view_operations()
elif view == "scorecard":  view_scorecard()

st.markdown('</div>', unsafe_allow_html=True)
