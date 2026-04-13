"""
generate_daily_brief.py
=======================
MLB Betting Model — Daily Brief Generator
Reads from mlb_stats.db and outputs the formatted betting brief.

CHANGE LOG (latest first)
──────────────────────────
2026-04-13 16:24 ET  Refactor: route sqlite3.connect() calls through core.db.connection.connect().

2026-04-11 19:00 ET  Feature: ⚡ MOVEMENT ALERT added to every pick in action
                     session briefs (early/afternoon/primary/late). When total
                     or ML moves ≥0.5 pts/10¢ since the earliest prior session
                     today, the alert is shown inline with the pick showing
                     direction and magnitude. brief_picks schema extended with
                     total_line column. ensure_brief_picks() auto-migrates.

2026-04-11 19:00 ET  Step 2: Updated Team_Strength_Analysis doc section 1.1
                     to correct prior analysis — MV-F WIN on Apr 10 prior
                     report was a fabricated retroactive pick, not a legitimate
                     graded pick from Apr 9. Apr 9 primary shows no signals.

2026-04-11 15:30 ET  Bug fix: prior report retroactive fallback now shows
                     [RETROACTIVE — not counted] label and NO graded result
                     box when brief_picks is empty for a date. Previously the
                     fallback rendered a full WIN/LOSS result using post-game
                     actual wind, fabricating a pick that was never shown
                     before the game was played. (Bug confirmed: Apr 10 prior
                     report showed MV-F WIN on PIT@CHC from Apr 9 — Apr 9
                     primary clearly showed "No confirmed signals fire".)

2026-04-11 15:30 ET  Feature: "Generated: YYYY-MM-DD HH:MM ET" timestamp
                     added to the first line of every session banner. Enables
                     validation of when each report was produced vs game times.

USAGE
-----
Morning brief   (after 9:00 AM odds pull):
    python generate_daily_brief.py --session morning

Primary brief   (after 5:00 PM odds pull)  ← act on these picks:
    python generate_daily_brief.py --session primary

Closing brief   (after 6:30 PM odds pull):
    python generate_daily_brief.py --session closing

Late West Coast games (after ~8:00 PM late-games odds pull):
    python load_odds.py --late-games --markets game
    python check_odds_ready.py
    python generate_daily_brief.py --session late

RECOVERY / RERUN FLAGS
----------------------
--date YYYY-MM-DD       Target a specific game date (default: today).
                        Use for reruns or catching up after a missed session.

--force                 Rerun even if a brief for this session/date already
                        exists in the brief_log table. Without this flag the
                        script warns and exits to prevent duplicate output.

--dry-run               Print the brief to the console but do NOT write to
                        brief_log or any output file.  Safe to run anytime.

--output FILE           Write the brief to FILE instead of (or in addition to)
                        the console.  Appends if file exists.

--no-file               Suppress file output; console only.  Overrides --output.

--warn-missing          Downgrade missing-data errors to warnings and continue.
                        By default the script exits if critical fields (ML odds,
                        totals) are absent.  Use after a partial odds pull.

--check-prereqs         Validate that the required odds pull for this session
                        was completed before generating the brief.  Exits with
                        a clear message if the pull is missing.  Recommended
                        for automated/scheduled runs.

--verbose               Print extra diagnostic rows from the DB (line counts,
                        data freshness, signal evaluation details).

--docx                  Also write a formatted Word (.docx) brief alongside the
                        .txt file.  Requires: pip install python-docx
                        File saved as briefs/YYYY-MM-DD_SESSION.docx.
                        Example:
                          python generate_daily_brief.py --session primary --docx

SESSION FILTERING MODEL
-----------------------
early / afternoon / primary all show the FULL unplayed slate for the
date — every game that hasn't started yet at the time you run the brief.
Session = when you pull odds and act, not which games start in that hour.

  · An all-afternoon slate correctly appears in --session primary because
    those games haven't started yet when you run it at 5:30 PM.
  · West Coast 10 PM ET games appear in --session afternoon (run 3:45 PM)
    because they haven't started yet.
  · A 1 PM game is dropped from --session primary (run 5:30 PM) because
    it already started. A 10-minute grace buffer prevents instant drop.

morning / closing show all games regardless of start time.

SIGNALS APPLIED (in priority order)
------------------------------------
1. MV-F   Wind in ≥10 mph + home fav −130 to −160  → fade ML (bet away)
          [CLV gate: only bet when CLV ≥ +0.5pp vs morning open — Mar 2026]
2. S1     Home team on W5+ win streak               → fade ML (bet away)
3. MV-B   Wind out ≥15 mph + home dog 35–42% impl.  → OVER  [band tightened Mar 2026]
4. S1+H2  W5+ home streak AND priced −130 to −160   → highest-priority stack
5. H3b    Wind out ≥10 mph + home dog (totals ctx)  → OVER (with MV-B only)

HOME TEAM INDICATOR
-------------------
Every matchup is shown as:  AWAY TEAM  vs  HOME TEAM (h)
"""

import argparse
import datetime
import os
import sqlite3
import sys
import textwrap
from pathlib import Path

from core.db.connection import connect as db_connect

# ── ET timezone helper ────────────────────────────────────────────────────
try:
    from zoneinfo import ZoneInfo as _ZI
    _ET = _ZI("America/New_York")
except Exception:
    # Windows tzdata fallback + Python < 3.9 — fixed UTC-4 (EDT, covers MLB Apr-Oct season)
    _ET = datetime.timezone(datetime.timedelta(hours=-4))


def _now_et() -> datetime.datetime:
    """Current datetime in US/Eastern. Use for all user-facing timestamps."""
    return datetime.datetime.now(tz=_ET)


def _game_start_et(game: dict) -> str:
    """Return game start time as an ET string e.g. '7:10 PM ET'.
    Returns empty string if game_start_utc is missing or unparseable.
    Cross-platform: avoids %-I which fails on Windows.
    """
    raw = game.get("game_start_utc") or ""
    if not raw or "T" not in raw:
        return ""
    try:
        utc_dt = datetime.datetime.fromisoformat(
            raw.rstrip("Z")).replace(tzinfo=datetime.timezone.utc)
        et_dt  = utc_dt.astimezone(_ET)
        # %I gives zero-padded hour; lstrip removes leading zero cross-platform
        t = et_dt.strftime("%I:%M %p").lstrip("0") or et_dt.strftime("%I:%M %p")
        return f"{t} ET"
    except (ValueError, AttributeError):
        return ""

# ── S6 pitcher streak monitoring signal ───────────────────────────────────
# Loaded conditionally — non-fatal if file not yet present.
try:
    from s6_pitcher_streak import check_s6_pitcher_streak, log_s6_fire_to_db
    S6_AVAILABLE = True
except ImportError:
    S6_AVAILABLE = False

# ── Optional Word output (python-docx) ────────────────────────────────────
try:
    from docx import Document
    from docx.shared import Pt, RGBColor, Inches
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.oxml.ns import qn
    from docx.oxml import OxmlElement
    DOCX_AVAILABLE = True
except ImportError:
    DOCX_AVAILABLE = False

# ── DB location (same directory as this script) ────────────────────────────
DB_PATH = Path(__file__).parent / "mlb_stats.db"

# ── Output directory for saved briefs ──────────────────────────────────────
OUTPUT_DIR = Path(__file__).parent / "briefs"

# ── Signal thresholds ──────────────────────────────────────────────────────
WIND_OUT_MIN_MPH   = 10     # H3b standalone OVER threshold (unchanged)
WIND_IN_MIN_MPH    = 10
WIND_OUT_MVB_MPH   = 15     # MV-B raised to 15 mph for higher signal confidence
HOME_FAV_MV_F_LOW  = -130   # e.g. -130
HOME_FAV_MV_F_HIGH = -160   # e.g. -160  (more negative = bigger fav)
MV_F_CLV_GATE      = 0.5    # CLV gate (pp): only bet MV-F when opening away ML
                             # implied prob is ≥0.5pp lower than closing implied.
                             # Mar 2026 backtest (N=137): CLV≥+0.5pp → SBRO +24.0%
                             # OW +10.6%. CLV<+0.5pp → SBRO −9.4%. Both eras pass.
                             # This is a LIVE EXECUTION filter — compare current
                             # away ML to the morning opening line at bet time.
S1_PRICE_LOW       = -105   # S1 standalone lower price bound
S1_PRICE_HIGH      = -170   # S1 standalone upper price bound
DOG_IMPL_LOW       = 0.35   # 35%
DOG_IMPL_HIGH      = 0.42   # 42% — tightened from 45% (Mar 2026 CLV study)
                             # 42.5-45.0% bucket: -24.1% ROI on 18 non-Oracle games.
                             # ≤42% bucket: +18.5% ROI on 26 games.
                             # Near-even dogs (42-45% impl) are priced efficiently;
                             # the wind-out edge does not survive the vig at that range.
STREAK_THRESHOLD   = 5      # W5+  — used by S1+H2 stack
S1_STANDALONE_MIN  = 6      # standalone S1 requires W6+
S6_WIN_STREAK_MIN  = 7      # S6 pitcher fade threshold (backtest: +25% ROI, 7/8 seasons positive)

# H3b park factor gate — only fire at venues with PF ≥ this value.
# Derived from 2024-2025 data: wind-out OVER effect is strongest at
# neutral-to-hitter-friendly parks. Pitcher-friendly parks (PF < 98)
# suppress the signal even with wind-out because the run environment
# is already suppressed by the park itself.
H3B_MIN_PARK_FACTOR = 98

# H3b late-season flag — Aug/Sep performance is structurally weaker.
# CLV study (Mar 2026, n=316 H3b-eligible games 2023-2025):
#   Aug 2024: 9 games, 22.2% over rate.  Sep 2024: 8 games, 37.5% over rate.
#   Apr-Jul 2024: 50-54% over rate — completely normal.
# Root cause: 2024 second-half run environment was unusually suppressed
# league-wide (not a signal flaw). Flag informs the user without suppressing
# the signal. Combined with --late-season-stake this is sufficient defence.
H3B_LATE_SEASON_MONTHS = {8, 9}   # August and September

# ── Paper trading account ────────────────────────────────────────────────
PAPER_BANK_START    = 500.00   # Starting bankroll in dollars
PAPER_STAKE_TOP     =  10.00   # Top pick stake ($)
PAPER_STAKE_ADD     =   5.00   # Additional picks #2-#6 stake ($)
PAPER_STAKE_REST    =   0.00   # 7th pick onward — not wagered
PAPER_LATE_FACTOR   =   0.50   # Half-stake multiplier for late-season H3b (Aug/Sep)

# July OVER signal constants — data shows 52.3% OVER rate in July
# (p=0.0006, n=875) vs 46.1% for all other months. Market consistently
# under-sets totals in July due to seasonal lag in line-setting.
JULY_OVER_MONTHS       = {7}           # calendar month numbers
JULY_OVER_MIN_PF       = 100           # only at neutral/hitter-friendly parks
JULY_OVER_MIN_TOTAL    = 7.5           # skip very low totals (already adjusted)
JULY_OVER_MAX_TOTAL    = 11.0          # skip extreme totals (park-specific outliers)

# ── Legal caveat — appended to every brief output (txt and docx) ──────────
# Displayed once at the foot of every session file.
# Identical wording appears in both .txt and .docx formats.
CAVEAT = (
    "\n"
    + "\u2550" * 72 + "\n"
    "  EDUCATIONAL USE ONLY \u2014 NOT FINANCIAL ADVICE\n"
    + "\u2500" * 72 + "\n"
    "  This brief is produced by a personal statistical model for\n"
    "  research and educational purposes only. Nothing in this output\n"
    "  constitutes financial advice, investment advice, or a recommendation\n"
    "  to place any wager of any kind. Sports betting carries substantial\n"
    "  financial risk and is not appropriate for all individuals.\n"
    "\n"
    "  Past signal performance does not guarantee future results. Odds,\n"
    "  lines, and conditions can change materially between the time this\n"
    "  brief is generated and game time. Always verify every line\n"
    "  independently with your bookmaker before placing any bet.\n"
    "\n"
    "  You are solely responsible for all betting decisions and any\n"
    "  resulting financial outcomes. Never bet more than you can afford\n"
    "  to lose. If gambling is causing financial or personal harm,\n"
    "  contact the National Problem Gambling Helpline: 1-800-522-4700\n"
    "  or visit ncpgambling.org.\n"
    + "\u2550" * 72 + "\n"
)

# H3b park whitelist — wind-out OVER only fires at venues where the effect
# is historically strongest and wind readings are reliable open-air.
# Oracle (SF), Tropicana (TB), loanDepot (MIA) are already SUPPRESSED.
# This list excludes modern enclosed or semi-enclosed parks where the
# wind_mph reading does not reflect in-play conditions reliably.
H3B_PARK_WHITELIST = {
    "Wrigley Field",           # Chicago Cubs  — canonical wind-out park
    "Coors Field",             # Colorado Rockies — altitude + wind
    "Kauffman Stadium",        # Kansas City Royals — open, wind-prone
    "Globe Life Field",        # Texas Rangers — open retractable (open config)
    "Progressive Field",       # Cleveland Guardians — open, Lake Erie wind
    "PNC Park",                # Pittsburgh Pirates — open, river gap wind
    "Comerica Park",           # Detroit Tigers — open, Great Lakes wind
    "American Family Field",   # Milwaukee Brewers — open config days
    "Great American Ball Park",# Cincinnati Reds — open, Ohio River wind
    "Nationals Park",          # Washington Nationals — open
    "Camden Yards",            # Baltimore Orioles — open
    "Fenway Park",             # Boston Red Sox — open
    "Yankee Stadium",          # New York Yankees — open (wind tunnel effect)
    "Citi Field",              # New York Mets — open
    "Oakland Coliseum",        # Athletics — very open, bay wind
}

# ── Session → expected pull window (for --check-prereqs) ──────────────────
SESSION_PULL_WINDOW = {
    "morning": ("06:00", "09:30"),   # after 9 AM odds pull
    "primary": ("09:30", "17:30"),   # after 5 PM odds pull
    "closing": ("17:30", "23:59"),   # after 6:30 PM odds pull
    "late":    ("20:00", "23:59"),   # after ~8 PM late-games pull
}


# ═══════════════════════════════════════════════════════════════════════════
# Utility helpers
# ═══════════════════════════════════════════════════════════════════════════

def american_to_implied(odds: int) -> float:
    """Convert American odds integer to implied probability (0–1)."""
    if odds is None:
        return None
    if odds > 0:
        return 100 / (odds + 100)
    else:
        return abs(odds) / (abs(odds) + 100)


def implied_to_american(prob: float) -> str:
    """Convert implied probability back to American odds string."""
    if prob is None or prob <= 0 or prob >= 1:
        return "N/A"
    if prob >= 0.5:
        odds = -(prob / (1 - prob)) * 100
        return f"{int(round(odds))}"
    else:
        odds = ((1 - prob) / prob) * 100
        return f"+{int(round(odds))}"


def wind_direction_label(direction: str) -> str:
    """Return OUT / IN / CROSS / CALM given a wind_direction string."""
    if not direction:
        return "UNKNOWN"
    d = direction.upper()
    # Treat 'OUT*', 'BLOWING OUT', 'OUT TO CF', etc. as OUT
    if any(k in d for k in ("OUT", "BLOWING OUT")):
        return "OUT"
    if any(k in d for k in ("IN", "BLOWING IN")):
        return "IN"
    if any(k in d for k in ("CROSS", "LEFT", "RIGHT")):
        return "CROSS"
    if any(k in d for k in ("CALM", "STILL", "0 MPH")):
        return "CALM"
    return direction  # pass through if unrecognised


def banner(text: str, width: int = 72) -> str:
    bar = "═" * width
    return f"\n{bar}\n  {text}\n{bar}"


def section(text: str, width: int = 72) -> str:
    return f"\n{'─' * width}\n  {text}\n{'─' * width}"


def fmt_odds(val) -> str:
    if val is None:
        return "N/A"
    v = int(val)
    return f"+{v}" if v > 0 else str(v)


def fmt_total(val) -> str:
    if val is None:
        return "N/A"
    return f"O/U {float(val):.1f}"


def missing(label: str, warn_only: bool) -> None:
    msg = f"[DATA MISSING] {label}"
    if warn_only:
        print(f"  ⚠  {msg}")
    else:
        print(f"\n  ✗  {msg}")
        print("     Re-run the required odds pull, or use --warn-missing to continue anyway.")
        sys.exit(1)


# ═══════════════════════════════════════════════════════════════════════════
# Database helpers
# ═══════════════════════════════════════════════════════════════════════════

def open_db(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        print(f"\n✗  Database not found: {db_path}")
        print("   Ensure you are running from the mlb_stats folder.")
        sys.exit(1)
    conn = db_connect(str(db_path), timeout=30)  # wait up to 30s if Scout holds a lock
    conn.row_factory = sqlite3.Row
    return conn


def check_prereqs(conn: sqlite3.Connection, game_date: str, session: str) -> None:
    """
    Verify that the required odds pull for this session was completed today.
    Exits with a helpful message if the pull appears missing.
    """
    pull_map = {
        "morning": "pregame",
        "primary": "pregame",
        "closing": "pregame",
    }
    pull_type = pull_map[session]

    # Look for an ingest log entry today for the right pull type
    cur = conn.execute(
        """
        SELECT pulled_at_utc, api_requests_used, status
        FROM   odds_ingest_log
        WHERE  date(pulled_at_utc) = ?
          AND  pull_type = ?
        ORDER  BY pulled_at_utc DESC
        LIMIT  1
        """,
        (game_date, pull_type),
    )
    row = cur.fetchone()
    if not row:
        print(f"\n✗  Prereq check failed for --session {session}:")
        print(f"   No '{pull_type}' pull found in odds_ingest_log for {game_date}.")
        print(f"   Run the required odds pull first:")
        print(f"   python load_odds.py --pregame --markets game")
        sys.exit(1)
    if row["status"] != "success":
        print(f"\n⚠  Last odds pull on {game_date} has status='{row['status']}' (not 'success').")
        print(f"   Consider re-running: python load_odds.py --pregame --markets game")
        sys.exit(1)
    print(f"  ✓ Prereq: odds pull found ({row['pulled_at_utc']}, {row['api_requests_used']} req used)")


def already_ran(conn: sqlite3.Connection, game_date: str, session: str) -> bool:
    """Return True if a brief_log entry exists for this date + session."""
    try:
        cur = conn.execute(
            "SELECT 1 FROM brief_log WHERE game_date=? AND session=? LIMIT 1",
            (game_date, session),
        )
        return cur.fetchone() is not None
    except sqlite3.OperationalError:
        # Table may not exist yet on first run
        return False


def ensure_brief_log(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS brief_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            game_date   TEXT    NOT NULL,
            session     TEXT    NOT NULL,
            generated_at TEXT   NOT NULL,
            games_covered INTEGER,
            picks_count   INTEGER,
            output_file   TEXT
        )
        """
    )
    conn.commit()
    ensure_daily_pnl(conn)   # create paper-trading ledger alongside
    ensure_brief_picks(conn)  # create confirmed-picks ledger alongside


def ensure_daily_pnl(conn: sqlite3.Connection) -> None:
    """Create the paper-trading ledger table if it does not exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_pnl (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            game_date       TEXT    NOT NULL,
            game_pk         INTEGER NOT NULL,
            signal          TEXT    NOT NULL,   -- e.g. S1+H2, MV-F, H3b
            pick_tier       TEXT    NOT NULL,   -- top / additional / rest
            bet             TEXT    NOT NULL,   -- e.g. "BOS ML" or "OVER 8.5"
            market          TEXT    NOT NULL,   -- ML or TOTAL
            odds            INTEGER,            -- American odds
            stake_dollars   REAL    NOT NULL,
            late_season     INTEGER NOT NULL DEFAULT 0, -- 1 = half-stake applied
            result          TEXT    NOT NULL,   -- WIN / LOSS / PUSH / NO RESULT
            pnl_units       REAL    NOT NULL,
            pnl_dollars     REAL    NOT NULL,
            recorded_at     TEXT    NOT NULL    -- ET timestamp
        )
    """)
    conn.commit()


def record_daily_pnl(conn: sqlite3.Connection, rows: list) -> None:
    """Insert paper-trading results for a game date. Idempotent on game_date+game_pk+signal."""
    if not rows:
        return
    for row in rows:
        conn.execute("""
            INSERT OR IGNORE INTO daily_pnl
                (game_date, game_pk, signal, pick_tier, bet, market, odds,
                 stake_dollars, late_season, result, pnl_units, pnl_dollars, recorded_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            row["game_date"], row["game_pk"], row["signal"], row["pick_tier"],
            row["bet"], row["market"], row["odds"],
            row["stake_dollars"], row["late_season"],
            row["result"], row["pnl_units"], row["pnl_dollars"],
            _now_et().strftime("%Y-%m-%d %H:%M ET"),
        ))
    conn.commit()


def load_season_pnl(conn: sqlite3.Connection, game_date: str) -> dict:
    """Load season-to-date paper P&L for the season containing game_date.
    Uses the seasons table (postseason_start as season end).
    Returns dict with keys: rows, total_dollars, wins, losses, pushes, bank.
    """
    try:
        season_year = int(game_date[:4])
        row = conn.execute(
            "SELECT season_start, postseason_start FROM seasons WHERE season=?",
            (season_year,)
        ).fetchone()
        if row:
            season_start = row["season_start"]
            season_end   = row["postseason_start"] or f"{season_year}-10-01"
        else:
            season_start = f"{season_year}-04-01"
            season_end   = f"{season_year}-10-01"
    except (ValueError, TypeError):
        return {"rows": [], "total_dollars": 0.0, "wins": 0,
                "losses": 0, "pushes": 0, "bank": PAPER_BANK_START}

    try:
        rows = conn.execute("""
            SELECT game_date, signal, pick_tier, bet, odds,
                   stake_dollars, late_season, result, pnl_units, pnl_dollars
            FROM   daily_pnl
            WHERE  game_date >= ? AND game_date < ?
            ORDER  BY game_date, id
        """, (season_start, season_end)).fetchall()
    except Exception:
        return {"rows": [], "total_dollars": 0.0, "wins": 0,
                "losses": 0, "pushes": 0, "bank": PAPER_BANK_START}

    total = sum(r["pnl_dollars"] for r in rows)
    wins  = sum(1 for r in rows if r["result"] == "WIN")
    losses= sum(1 for r in rows if r["result"] == "LOSS")
    pushes= sum(1 for r in rows if r["result"] == "PUSH")
    return {
        "rows":          rows,
        "total_dollars": round(total, 2),
        "wins":          wins,
        "losses":        losses,
        "pushes":        pushes,
        "bank":          round(PAPER_BANK_START + total, 2),
    }


def ensure_brief_picks(conn: sqlite3.Connection) -> None:
    """Create table that stores confirmed picks shown in each brief.
    Also migrates existing tables to add total_line column if absent."""
    # Migration: add total_line column to existing tables that lack it
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(brief_picks)").fetchall()]
        if cols and "total_line" not in cols:
            conn.execute("ALTER TABLE brief_picks ADD COLUMN total_line REAL")
            conn.commit()
    except Exception:
        pass
    conn.execute("""
        CREATE TABLE IF NOT EXISTS brief_picks (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            game_date   TEXT    NOT NULL,
            session     TEXT    NOT NULL,   -- primary / early / afternoon
            game_pk     INTEGER NOT NULL,
            pick_rank   INTEGER NOT NULL,   -- 1=top, 2-6=additional
            signal      TEXT    NOT NULL,
            bet         TEXT    NOT NULL,
            market      TEXT    NOT NULL,
            odds        INTEGER,
            total_line  REAL,              -- stored when market=TOTAL; enables movement alert
            recorded_at TEXT    NOT NULL,
            UNIQUE (game_date, session, game_pk, signal)
        )
    """)
    conn.commit()


def save_brief_picks(conn: sqlite3.Connection, game_date: str,
                     session: str, pick_entries: list) -> None:
    """Record picks shown in this brief for prior-report grading.
    pick_entries: sorted list of entry dicts from the brief builder.
    Only records top pick (rank 1) and additional picks (ranks 2-6).
    Idempotent — INSERT OR IGNORE on (game_date, session, game_pk, signal).
    """
    if not pick_entries:
        return
    now_et = _now_et().strftime("%Y-%m-%d %H:%M ET")
    for rank, entry in enumerate(pick_entries[:6], start=1):
        g = entry["game"]
        # Use highest-priority pick for this game
        p = sorted(entry["sigs"]["picks"], key=lambda x: x["priority"])[0]
        signal = ", ".join(entry["sigs"]["signals"])
        odds_raw = _parse_odds(p.get("odds", ""))
        total_line_val = None
        if p.get("market") == "TOTAL":
            total_line_val = entry["game"].get("total_line")
        try:
            conn.execute("""
                INSERT OR IGNORE INTO brief_picks
                    (game_date, session, game_pk, pick_rank, signal,
                     bet, market, odds, total_line, recorded_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (game_date, session, g["game_pk"], rank, signal,
                  p["bet"], p["market"], odds_raw, total_line_val, now_et))
        except Exception:
            pass
    conn.commit()


def load_brief_picks(conn: sqlite3.Connection, game_date: str,
                     session: str = "primary") -> list:
    """Load confirmed picks that were shown in yesterday's primary brief.
    Returns list of dicts ordered by pick_rank.
    """
    try:
        rows = conn.execute("""
            SELECT game_pk, pick_rank, signal, bet, market, odds
            FROM   brief_picks
            WHERE  game_date = ? AND session = ?
            ORDER  BY pick_rank
        """, (game_date, session)).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def load_todays_prior_sessions(conn: sqlite3.Connection, game_date: str,
                               current_session: str) -> list:
    """
    Load all picks recorded in earlier sessions TODAY, ordered by session then rank.
    Used to build the intra-day Signal Tracker.

    Session order: early < afternoon < primary  (closing is never a source for tracker)
    Returns list of dicts, each with:
        session, pick_rank, game_pk, signal, bet, market, odds
    """
    SESSION_ORDER = ["early", "afternoon", "primary", "closing", "late"]
    try:
        current_idx = SESSION_ORDER.index(current_session)
    except ValueError:
        current_idx = len(SESSION_ORDER)

    prior_sessions = SESSION_ORDER[:current_idx]
    if not prior_sessions:
        return []

    placeholders = ",".join("?" * len(prior_sessions))
    try:
        rows = conn.execute(f"""
            SELECT session, game_pk, pick_rank, signal, bet, market, odds,
                   total_line, recorded_at
            FROM   brief_picks
            WHERE  game_date  = ?
              AND  session    IN ({placeholders})
            ORDER  BY
                CASE session
                    WHEN 'early'     THEN 1
                    WHEN 'afternoon' THEN 2
                    WHEN 'primary'   THEN 3
                    ELSE 4
                END,
                pick_rank
        """, (game_date, *prior_sessions)).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


# Session display labels for the tracker
_SESSION_LABELS = {
    "early":     "EARLY",
    "afternoon": "AFTERNOON",
    "primary":   "PRIMARY",
    "closing":   "CLOSING",
    "late":      "LATE GAMES",
}



def movement_alert(conn: sqlite3.Connection, game_date: str,
                   current_session: str, game_pk: int,
                   current_total: float, current_home_ml: int) -> str:
    """
    Compare current total/ML against the earliest prior session pick for this
    game today. Returns a formatted alert string if movement exceeds thresholds,
    or empty string if no prior pick or no significant movement.

    Thresholds (matching ops guide):
      Total  : ≥ 0.5 points in either direction
      ML     : ≥ 10 cents implied probability (not raw points — catches -130→-160 etc.)

    Arrow convention:
      ⬇ Total dropped   = counter-signal for OVER picks (sharps see lower scoring)
      ⬆ Total rose      = confirms OVER pick
      ML move shown as implied probability shift
    """
    SESSION_ORDER = ["early", "afternoon", "primary", "closing", "late"]
    try:
        current_idx = SESSION_ORDER.index(current_session)
    except ValueError:
        return ""
    if current_idx == 0:
        return ""  # no prior session today

    prior_sessions = SESSION_ORDER[:current_idx]
    placeholders   = ",".join("?" * len(prior_sessions))

    try:
        row = conn.execute(f"""
            SELECT market, total_line, odds
            FROM   brief_picks
            WHERE  game_date = ? AND game_pk = ?
              AND  session IN ({placeholders})
            ORDER  BY
                CASE session
                    WHEN 'early'     THEN 1
                    WHEN 'afternoon' THEN 2
                    WHEN 'primary'   THEN 3
                    ELSE 4
                END
            LIMIT 1
        """, (game_date, game_pk, *prior_sessions)).fetchone()
    except Exception:
        return ""

    if not row:
        return ""

    alerts = []

    # ── Total movement ────────────────────────────────────────────────────
    prior_total = row["total_line"] if isinstance(row, dict) else row[1]
    if prior_total is not None and current_total is not None:
        try:
            delta = round(float(current_total) - float(prior_total), 2)
            if abs(delta) >= 0.5:
                direction = "⬇" if delta < 0 else "⬆"
                counter   = "  ← COUNTER-SIGNAL for OVER" if delta < 0 else "  ← confirms OVER"
                alerts.append(
                    f"⚡ MOVEMENT ALERT: Total {prior_total} → {current_total} "
                    f"({delta:+.1f}) {direction}{counter}"
                )
        except (TypeError, ValueError):
            pass

    # ── ML movement (implied probability shift) ───────────────────────────
    prior_ml_raw = row["odds"] if isinstance(row, dict) else row[2]
    prior_market  = row["market"] if isinstance(row, dict) else row[0]
    if prior_market == "ML" and prior_ml_raw is not None and current_home_ml is not None:
        try:
            def to_imp(ml):
                ml = int(ml)
                return abs(ml)/(abs(ml)+100) if ml < 0 else 100/(ml+100)
            prior_imp   = to_imp(prior_ml_raw)
            current_imp = to_imp(current_home_ml)
            shift_cents = round((current_imp - prior_imp) * 100, 1)
            if abs(shift_cents) >= 10:
                direction = "HOME more favoured" if shift_cents > 0 else "HOME less favoured"
                alerts.append(
                    f"⚡ MOVEMENT ALERT: ML shifted {shift_cents:+.0f}¢ implied "
                    f"({direction})"
                )
        except (TypeError, ValueError):
            pass

    return "\n".join(f"  {a}" for a in alerts)


def build_signal_tracker_block(prior_picks: list, current_games: list,
                                streaks: dict, current_session: str) -> str:
    """
    Build the intra-day Signal Tracker section for action briefs.

    Compares picks from earlier today's sessions against the current slate:
      · Still firing with same bet → ACTIVE (unchanged)
      · Still in picks but odds moved → ACTIVE (line moved N pts)
      · Dropped from picks entirely → DROPPED (with reason)
      · Was Top Pick, now lower rank → DEMOTED
      · New pick not in earlier sessions → shown in main picks section, not here

    Returns formatted text block (empty string if no prior sessions had picks).
    """
    if not prior_picks:
        return ""

    lines = []
    lines.append(section("📡  SIGNAL TRACKER  —  Today's Pick Status vs Earlier Sessions"))
    lines.append(
        "\n  Tracks how today's signals have evolved across brief sessions.\n"
        "  Early price = odds when signal first fired. Current = now.\n"
        "  If you got in early: your position is noted. Decision is yours.\n"
    )

    # Build a lookup: game_pk → current pick info (if still firing)
    current_pick_lookup = {}  # game_pk → {rank, signal, bet, market, odds}
    for rank, entry in enumerate(
        sorted(current_games, key=lambda e: min(p["priority"] for p in e["sigs"]["picks"]))
        if current_games else [], start=1
    ):
        if entry["sigs"]["picks"]:
            g   = entry["game"]
            p   = sorted(entry["sigs"]["picks"], key=lambda x: x["priority"])[0]
            current_pick_lookup[g["game_pk"]] = {
                "rank":   rank,
                "signal": ", ".join(entry["sigs"]["signals"]),
                "bet":    p["bet"],
                "market": p["market"],
                "odds":   _parse_odds(p.get("odds", "")) or p.get("odds"),
                "odds_str": p.get("odds", "N/A"),
            }

    # Group prior picks by game_pk, keep the EARLIEST session's record per game
    seen_pks = {}
    for row in prior_picks:
        pk = row["game_pk"]
        if pk not in seen_pks:
            seen_pks[pk] = row   # first occurrence = earliest session

    if not seen_pks:
        lines.append("  No picks were recorded in prior sessions today.\n")
        return "\n".join(lines)

    for pk, prior in seen_pks.items():
        sess_label   = _SESSION_LABELS.get(prior["session"], prior["session"].upper())
        prior_rank   = prior["pick_rank"]
        prior_odds   = prior["odds"]
        prior_bet    = prior["bet"]
        prior_signal = prior["signal"]
        rank_label   = "TOP PICK" if prior_rank == 1 else f"#{prior_rank}"
        prior_odds_str = fmt_odds(prior_odds) if prior_odds else "N/A"

        current = current_pick_lookup.get(pk)

        if current is None:
            # ── DROPPED ──────────────────────────────────────────────────
            # Signal no longer firing — determine most likely reason
            # We don't have full game context here, so describe generically
            curr_rank_label = "—"
            drop_note = (
                "CLV gate not cleared — line moved away from model. "
                "If you got in at the earlier price, position remains valid. "
                "Current price: your discretion."
            )
            lines.append(
                f"\n  ❌  {prior_bet}  [{prior_signal}]  —  was {rank_label} @ {sess_label}\n"
                f"      {sess_label} odds: {prior_odds_str}  →  Current: DROPPED\n"
                f"      {drop_note}\n"
            )
        else:
            curr_rank   = current["rank"]
            curr_odds   = current["odds"]
            curr_odds_str = current["odds_str"]
            curr_rank_label = "TOP PICK" if curr_rank == 1 else f"#{curr_rank}"

            if curr_odds is not None and prior_odds is not None:
                delta = curr_odds - prior_odds
                delta_str = (f"+{delta} pts" if delta > 0 else
                             f"{delta} pts"  if delta < 0 else
                             "unchanged")
            else:
                delta_str = "line unknown"

            if prior_rank == 1 and curr_rank > 1:
                # ── DEMOTED ──────────────────────────────────────────────
                lines.append(
                    f"\n  ⚠️   {prior_bet}  [{prior_signal}]  —  was TOP PICK @ {sess_label}, now {curr_rank_label}\n"
                    f"      {sess_label} odds: {prior_odds_str}  →  Now: {curr_odds_str}  ({delta_str})\n"
                    f"      Signal still active — higher-priority pick displaced it at top.\n"
                )
            elif prior_rank > 1 and curr_rank == 1:
                # ── PROMOTED ─────────────────────────────────────────────
                lines.append(
                    f"\n  ⬆️   {prior_bet}  [{prior_signal}]  —  was {rank_label} @ {sess_label}, now TOP PICK\n"
                    f"      {sess_label} odds: {prior_odds_str}  →  Now: {curr_odds_str}  ({delta_str})\n"
                    f"      Signal still active and elevated to top rank.\n"
                )
            else:
                # ── ACTIVE (same rank) ────────────────────────────────────
                status = "unchanged" if delta_str == "unchanged" else f"line moved {delta_str}"
                lines.append(
                    f"\n  ✅  {prior_bet}  [{prior_signal}]  —  {rank_label} since {sess_label}\n"
                    f"      {sess_label} odds: {prior_odds_str}  →  Now: {curr_odds_str}  ({status})\n"
                    f"      Signal active and holding.\n"
                )

    lines.append("")
    return "\n".join(lines)


def compute_paper_picks(evaluated: list, game_date: str) -> list:
    """Convert graded pick entries into paper-trading rows ready for daily_pnl.
    evaluated: list of entry dicts from build_prior_day_report.
    Returns list of row dicts.
    """
    pick_entries = sorted(
        [e for e in evaluated if e["graded"]],
        key=lambda e: min(p["priority"] for p in e["graded"])
    )
    top_pick   = pick_entries[:1]
    next_picks = pick_entries[1:6]
    # rest_picks (7th+) get $0 — not recorded

    pnl_rows = []
    for tier_label, tier_entries, base_stake in [
        ("top",        top_pick,   PAPER_STAKE_TOP),
        ("additional", next_picks, PAPER_STAKE_ADD),
    ]:
        for entry in tier_entries:
            g = entry["game"]
            # Use the highest-priority pick for this game
            p = sorted(entry["graded"], key=lambda x: x["priority"])[0]
            signal = ", ".join(entry["sigs"]["signals"])

            # Late-season H3b half-stake detection
            try:
                month = int(game_date[5:7])
            except (ValueError, TypeError):
                month = 0
            is_h3b    = "H3b" in entry["sigs"]["signals"]
            late_flag = 1 if (is_h3b and month in H3B_LATE_SEASON_MONTHS) else 0
            stake     = round(base_stake * (PAPER_LATE_FACTOR if late_flag else 1.0), 2)

            # Dollar P&L from units
            pnl_dollars = round(p["pnl"] * stake, 2)

            # Clean result string
            raw_result = p["result"]  # e.g. "✓ WIN", "✗ LOSS", "— PUSH"
            if "WIN"  in raw_result: result_clean = "WIN"
            elif "LOSS" in raw_result: result_clean = "LOSS"
            elif "PUSH" in raw_result: result_clean = "PUSH"
            else: result_clean = "NO RESULT"

            pnl_rows.append({
                "game_date":     game_date,
                "game_pk":       g["game_pk"],
                "signal":        signal,
                "pick_tier":     tier_label,
                "bet":           p["bet"],
                "market":        p["market"],
                "odds":          p.get("odds_raw") or _parse_odds(p.get("odds", "")),
                "stake_dollars": stake,
                "late_season":   late_flag,
                "result":        result_clean,
                "pnl_units":     round(p["pnl"], 4),
                "pnl_dollars":   pnl_dollars,
            })
    return pnl_rows


def _parse_odds(odds_str: str) -> int | None:
    """Parse a formatted odds string like '-130' or '+115' to int.
    Returns None if unparseable.
    """
    try:
        s = str(odds_str).replace("\u2212", "-").replace("\u202f", "").strip()
        return int(s)
    except (ValueError, TypeError):
        return None


def format_dollar_pnl_block(today_rows: list, season_pnl: dict,
                            game_date: str) -> str:
    """Format the dollar P&L section for the prior-day report."""
    lines = []
    lines.append(section("💵  PAPER ACCOUNT  —  Dollar Performance"))

    if not today_rows:
        lines.append("\n  No bets placed (no signals fired yesterday).\n")
    else:
        top_rows  = [r for r in today_rows if r["pick_tier"] == "top"]
        add_rows  = [r for r in today_rows if r["pick_tier"] == "additional"]
        today_pnl = sum(r["pnl_dollars"] for r in today_rows)
        today_staked = sum(r["stake_dollars"] for r in today_rows)

        # Stake summary line
        stake_parts = []
        for r in top_rows:
            flag = " ½-stake H3b late-season" if r["late_season"] else ""
            stake_parts.append(f"Top ${r['stake_dollars']:.0f}{flag}")
        if add_rows:
            stake_parts.append(f"{len(add_rows)} additional × ${add_rows[0]['stake_dollars']:.0f}")
        lines.append(f"\n  Bets placed:   {'  ·  '.join(stake_parts)}")
        lines.append(f"  Total staked:  ${today_staked:.2f}")

        # Per-bet detail
        lines.append("")
        for r in today_rows:
            tier  = "TOP" if r["pick_tier"] == "top" else "ADD"
            sign  = "+" if r["pnl_dollars"] >= 0 else ""
            flag  = " [½-stake]" if r["late_season"] else ""
            lines.append(
                f"  [{tier}] {r['bet']:<22} {r['result']:<8} "
                f"${r['stake_dollars']:.0f} → {sign}${r['pnl_dollars']:.2f}{flag}"
            )

        # Day total
        sign = "+" if today_pnl >= 0 else ""
        lines.append(f"\n  TODAY:   {sign}${today_pnl:.2f}")

    # Season to date
    stv = season_pnl
    sign = "+" if stv["total_dollars"] >= 0 else ""
    rec  = f"W:{stv['wins']}  L:{stv['losses']}"
    if stv["pushes"]:
        rec += f"  P:{stv['pushes']}"
    lines.append(
        f"  SEASON:  {sign}${stv['total_dollars']:.2f}  "
        f"({rec})"
    )
    lines.append(
        f"  BANK:    ${stv['bank']:.2f}  "
        f"(started ${PAPER_BANK_START:.0f})"
    )
    lines.append("")
    return "\n".join(lines)


def log_brief(conn, game_date, session, games_covered, picks_count,
              output_file, pick_entries=None):
    generated_at_et = _now_et().strftime("%Y-%m-%d %H:%M ET")
    conn.execute(
        """
        INSERT INTO brief_log (game_date, session, generated_at, games_covered, picks_count, output_file)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (game_date, session, generated_at_et, games_covered, picks_count, output_file),
    )
    conn.commit()
    # Save confirmed picks for prior-report grading (action sessions only)
    if pick_entries is not None and session in ("primary", "early", "afternoon", "late"):
        save_brief_picks(conn, game_date, session, pick_entries)


# ═══════════════════════════════════════════════════════════════════════════
# Data loaders
# ═══════════════════════════════════════════════════════════════════════════

def load_games(conn: sqlite3.Connection, game_date: str, verbose: bool) -> list:
    """
    Load today's games with odds, weather, and team info.
    Returns list of dicts with all fields needed for signal evaluation.
    """
    cur = conn.execute(
        """
        SELECT
            g.game_pk,
            g.game_date,
            g.game_start_utc,
            v.name          AS venue_name,
            g.temp_f,
            g.wind_mph,
            g.wind_direction,
            g.sky_condition,

            -- Venue intelligence (populated by add_stadium_data.py)
            v.wind_effect,
            v.wind_note,
            v.roof_type,
            v.elevation_ft,
            v.park_factor_runs,
            v.park_factor_hr,
            v.orientation_hp,

            th.team_id      AS home_team_id,
            th.abbreviation AS home_abbr,
            th.name         AS home_name,

            ta.team_id      AS away_team_id,
            ta.abbreviation AS away_abbr,
            ta.name         AS away_name,

            -- Moneyline fields
            ml.home_ml,
            ml.away_ml,
            -- Total fields
            tot.total_line,
            tot.over_odds,
            tot.under_odds,
            -- Run line fields
            rl.home_rl_line  AS home_rl,
            rl.away_rl_line  AS away_rl,
            rl.home_rl_odds,
            rl.away_rl_odds,
            COALESCE(ml.captured_at_utc, tot.captured_at_utc) AS odds_captured_at,
            ml.bookmaker AS odds_bookmaker

        FROM   games g
        JOIN   teams  th ON th.team_id  = g.home_team_id
        JOIN   teams  ta ON ta.team_id  = g.away_team_id
        LEFT JOIN venues v  ON v.venue_id   = g.venue_id
        -- Join each market type separately — one row per game
        LEFT JOIN v_closing_game_odds ml  ON ml.game_pk  = g.game_pk
                                         AND ml.market_type = 'moneyline'
        LEFT JOIN v_closing_game_odds tot ON tot.game_pk = g.game_pk
                                         AND tot.market_type = 'total'
        LEFT JOIN v_closing_game_odds rl  ON rl.game_pk  = g.game_pk
                                         AND rl.market_type = 'runline'
        WHERE  g.game_date = ?
          AND  g.game_type = 'R'          -- regular season only; Spring Training / Exhibition excluded
          AND  g.status    != 'Final'          -- skip already-completed games
        ORDER  BY g.game_start_utc
        """,
        (game_date,),
    )
    rows = [dict(r) for r in cur.fetchall()]

    if verbose:
        print(f"\n  [verbose] {len(rows)} upcoming games found for {game_date}")
        for r in rows:
            print(f"           {r['away_abbr']} @ {r['home_abbr']}  "
                  f"ML:{fmt_odds(r['home_ml'])}/{fmt_odds(r['away_ml'])}  "
                  f"Tot:{r['total_line']}  "
                  f"Wind:{r['wind_mph']} mph {r['wind_direction']}  "
                  f"WindEffect:{r['wind_effect']}")
    return rows


def load_streaks(conn: sqlite3.Connection, game_date: str, team_ids: list, verbose: bool) -> dict:
    """
    Compute current win/loss streak for each team entering today's game.
    Returns dict of {team_id: streak_int}  (+N = N-game win streak, -N = N-game loss streak).
    """
    streaks = {}
    for tid in team_ids:
        cur = conn.execute(
            """
            SELECT
                CASE WHEN home_team_id = ? THEN
                    CASE WHEN home_score > away_score THEN 'W' ELSE 'L' END
                ELSE
                    CASE WHEN away_score > home_score THEN 'W' ELSE 'L' END
                END AS result
            FROM   games
            WHERE  (home_team_id = ? OR away_team_id = ?)
              AND  status  = 'Final'
              AND  game_date < ?
            ORDER  BY game_date DESC, game_start_utc DESC
            LIMIT  15
            """,
            (tid, tid, tid, game_date),
        )
        results = [r["result"] for r in cur.fetchall()]
        if not results:
            streaks[tid] = 0
            continue
        current = results[0]
        count   = 0
        for r in results:
            if r == current:
                count += 1
            else:
                break
        streaks[tid] = count if current == "W" else -count

    if verbose:
        print(f"\n  [verbose] Team streaks entering {game_date}:")
        for tid, s in streaks.items():
            label = f"W{s}" if s > 0 else f"L{abs(s)}" if s < 0 else "—"
            print(f"           team_id={tid}  streak={label}")

    return streaks


def load_starters(conn: sqlite3.Connection, game_date: str, verbose: bool) -> dict:
    """
    Load probable starting pitchers for today's games.
    Returns dict of {game_pk: {home_starter, away_starter}} where available.
    Falls back gracefully if the table/column doesn't exist yet.
    """
    starters = {}
    try:
        cur = conn.execute(
            """
            SELECT
                gp.game_pk,
                gp.team_id,
                p.full_name,
                p.era_season
            FROM   game_probable_pitchers gp
            JOIN   players p ON p.player_id = gp.player_id
            JOIN   games   g ON g.game_pk   = gp.game_pk
            WHERE  g.game_date = ?
            """,
            (game_date,),
        )
        rows = cur.fetchall()
        for r in rows:
            if r["game_pk"] not in starters:
                starters[r["game_pk"]] = {}
            starters[r["game_pk"]][r["team_id"]] = {
                "name": r["full_name"],
                "era":  r["era_season"],
            }
        if verbose:
            print(f"\n  [verbose] Starters loaded for {len(starters)} games")
    except sqlite3.OperationalError as e:
        # Table not yet populated — non-fatal
        if verbose:
            print(f"\n  [verbose] game_probable_pitchers not available: {e}")
    return starters


def load_line_movement(conn: sqlite3.Connection, game_date: str, verbose: bool) -> dict:
    """
    Load open-to-close line movement for today's games.
    Returns dict of {game_pk: movement_row}.
    Only populated after --compute-movement has run (closing session).
    """
    movement = {}
    try:
        cur = conn.execute(
            """
            SELECT
                lm.game_pk,
                lm.ml_move_cents,
                lm.total_move,
                lm.move_direction,
                lm.steam_move,
                lm.reverse_line_move
            FROM   line_movement lm
            JOIN   games g ON g.game_pk = lm.game_pk
            WHERE  g.game_date = ?
            """,
            (game_date,),
        )
        for r in cur.fetchall():
            movement[r["game_pk"]] = dict(r)
        if verbose:
            print(f"\n  [verbose] Line movement data found for {len(movement)} games")
    except sqlite3.OperationalError as e:
        if verbose:
            print(f"\n  [verbose] line_movement table not available: {e}")
    return movement


# ═══════════════════════════════════════════════════════════════════════════
# Signal evaluation
# ═══════════════════════════════════════════════════════════════════════════

def evaluate_signals(game: dict, streaks: dict, session: str) -> dict:
    """
    Evaluate all model signals for a single game.
    Returns a dict describing which signals fired and what they recommend.
    """
    result = {
        "signals":      [],
        "picks":        [],   # list of {bet, market, odds_str, reason}
        "avoid":        False,
        "avoid_reason": None,
        "watch":        False,
        "watch_reason": None,
        "data_flags":   [],   # non-fatal missing/stale data notes
    }

    home_ml   = game.get("home_ml")
    away_ml   = game.get("away_ml")
    total     = game.get("total_line")
    wind_mph     = game.get("wind_mph") or 0
    wind_dir     = wind_direction_label(game.get("wind_direction") or "")
    wind_effect  = (game.get("wind_effect") or "HIGH").upper()
    wind_note_v  = game.get("wind_note") or ""
    roof_type    = (game.get("roof_type") or "Open")

    # Signal suppression tiers derived from venues.wind_effect:
    #   SUPPRESSED — never fire wind signals (Oracle, Tropicana, loanDepot)
    #   LOW        — retractable roof; note roof status, signals degraded
    #   MODERATE   — open air but sheltered; signals fire with reduced confidence
    #   HIGH       — full wind signal eligibility
    suppress_wind  = (wind_effect == "SUPPRESSED")
    is_retractable = (wind_effect == "LOW")
    wind_eligible  = (wind_effect == "HIGH")

    home_id   = game.get("home_team_id")
    away_id   = game.get("away_team_id")
    home_abbr = game.get("home_abbr", "HOME")
    away_abbr = game.get("away_abbr", "AWAY")

    # ── Implied probabilities ────────────────────────────────────────────
    home_impl = american_to_implied(home_ml) if home_ml else None
    away_impl = american_to_implied(away_ml) if away_ml else None

    if home_ml is None or away_ml is None:
        result["data_flags"].append("ML odds missing — signal evaluation limited")

    # ── Venue wind context ───────────────────────────────────────────────
    if suppress_wind:
        result["data_flags"].append(
            f"Wind signals suppressed at this venue ({wind_note_v[:80] if wind_note_v else roof_type})"
        )
    elif is_retractable:
        result["data_flags"].append(
            "Retractable roof venue — verify roof status before acting on wind signals"
        )

    # ── Home team streak ─────────────────────────────────────────────────
    home_streak = streaks.get(home_id, 0)
    away_streak = streaks.get(away_id, 0)

    streak_label = ""
    if home_streak >= STREAK_THRESHOLD:
        streak_label = f"W{home_streak}"
    elif home_streak <= -STREAK_THRESHOLD:
        streak_label = f"L{abs(home_streak)}"

    # ── Signal 4: S1 + H2 stack (highest priority combo) ────────────────
    # Requires W5+ (STREAK_THRESHOLD) AND home priced in the -130/-160 fade zone.
    # Stack fires at W5+ — the H2 price screen adds the second qualification.
    s1_h2_fired = False
    if (home_streak >= STREAK_THRESHOLD
            and home_ml is not None
            and HOME_FAV_MV_F_HIGH <= home_ml <= HOME_FAV_MV_F_LOW):
        s1_h2_fired = True
        result["signals"].append("S1+H2")
        result["picks"].append({
            "bet":    f"{away_abbr} ML",
            "market": "ML",
            "odds":   fmt_odds(away_ml),
            "reason": (f"S1+H2 STACK — Home team {home_abbr} on W{home_streak} streak "
                       f"AND priced {fmt_odds(home_ml)} (overpricing zone). "
                       f"Two signals simultaneously — highest-priority fade."),
            "priority": 1,
        })

    # ── Signal 1: MV-F (wind-in + home fav overpriced) ──────────────────
    if (wind_eligible
            and wind_dir == "IN"
            and wind_mph >= WIND_IN_MIN_MPH
            and home_ml is not None
            and HOME_FAV_MV_F_HIGH <= home_ml <= HOME_FAV_MV_F_LOW
            and not s1_h2_fired):
        result["signals"].append("MV-F")
        result["picks"].append({
            "bet":    f"{away_abbr} ML",
            "market": "ML",
            "odds":   fmt_odds(away_ml),
            "reason": (f"MV-F — Wind IN {wind_mph} mph. "
                       f"Home fav {fmt_odds(home_ml)} in fade zone (−130/−160). "
                       f"Wind-in suppresses scoring; overpriced home fav. "
                       f"CLV gate: only bet if away ML implied prob is ≥{MV_F_CLV_GATE}pp "
                       f"below morning open (CLV≥+{MV_F_CLV_GATE}pp). "
                       f"CLV≥+0.5pp fires: SBRO +24.0% ROI, OW +10.6% ROI. "
                       f"CLV<+0.5pp fires: SBRO −9.4% ROI. "
                       f"Compare current away ML to opening line at bet time."),
            "priority": 2,
        })

    # ── Signal 2: S1 standalone (win streak — tighter filters applied) ───
    # Fix 1: price band -105 to -170 only — outside this range the market
    #        has not loaded (too cheap) or the odds risk is too large (too dear).
    # Fix 2: threshold raised to W6+ (S1_STANDALONE_MIN) for standalone fires;
    #        W5+ is reserved for the S1+H2 stack where the price screen adds
    #        a second qualification.
    # Priority swap (Mar 2026): S1 standalone moved from priority 3 → 4.
    #   MV-B is now priority 3. 3-year data: MV-B ROI +18.9% (13 fires,
    #   positive all 3 years) vs S1 standalone ROI -5.3% (26 fires, negative
    #   in 2 of 3 years). On days both fire, MV-B is the better pick.
    s1_price_ok = (
        home_ml is not None
        and S1_PRICE_HIGH <= home_ml <= S1_PRICE_LOW   # -170 to -105
    )
    if (home_streak >= S1_STANDALONE_MIN    # W6+
            and not s1_h2_fired
            and s1_price_ok):
        result["signals"].append("S1")
        result["picks"].append({
            "bet":    f"{away_abbr} ML",
            "market": "ML",
            "odds":   fmt_odds(away_ml),
            "reason": (f"S1 — Home team {home_abbr} entering on W{home_streak} win streak "
                       f"priced {fmt_odds(home_ml)} (streak-premium zone −105/−170). "
                       f"Fade away ML. ROI: +7.50% SBRO / +8.99% OW (stronger at W7+). "
                       f"Filtered: W6+ only, price band −105/−170."),
            "priority": 4,   # Swapped: was 3, now below MV-B
        })

    # ── Signal 3: MV-B (wind-out + home dog implied 35–42%) ─────────────
    # Fix 3: wind floor raised to WIND_OUT_MVB_MPH (15 mph) for MV-B.
    #        H3b uses the lower WIND_OUT_MIN_MPH (10 mph) independently.
    # Priority swap (Mar 2026): MV-B moved from priority 4 → 3, above S1
    #   standalone. 3-year data: MV-B +18.9% ROI, positive all 3 years.
    #
    # Mar 2026 CLV timing study refinements (non-Oracle population, n=45):
    #   Change A — Implied band tightened to ≤42% (was ≤45%).
    #     42.5-45.0% bucket: -24.1% ROI on 18 games. Near-even dogs are
    #     efficiently priced; wind-out edge does not clear vig there.
    #     ≤42% bucket: +18.5% ROI on 26 games. Combined with CLV gate: +22.9%.
    #   Change B — CLV gate added for live betting.
    #     CLV>0 (market confirms pick): +12.2% ROI. CLV≤0: -18.1% ROI.
    #     In the brief, CLV is noted as a live filter. The signal fires
    #     pre-game; the CLV gate is applied at time of actual bet placement
    #     by comparing the current line to the morning opening line.
    #     Note: Oracle Park (wind_effect=SUPPRESSED) never reaches this block.
    #     The true eligible population is non-Oracle open-air venues only.
    if (wind_eligible
            and wind_dir == "OUT"
            and wind_mph >= WIND_OUT_MVB_MPH           # 15 mph floor
            and home_impl is not None
            and DOG_IMPL_LOW <= home_impl <= DOG_IMPL_HIGH   # 35-42% (tightened)
            and total is not None):
        result["signals"].append("MV-B")
        result["picks"].append({
            "bet":    f"OVER {total}",
            "market": "TOTAL",
            "odds":   fmt_odds(game.get("over_odds")) or "-110",
            "reason": (f"MV-B — Wind OUT {wind_mph} mph (≥15 mph threshold). "
                       f"Home dog {home_abbr} at {home_impl:.0%} implied ({fmt_odds(home_ml)}). "
                       f"Wind-out + dog bucket: 58.7% SBRO / 62.3% OW over rate. "
                       f"Impl band tightened to 35-42% (Mar 2026): near-even dogs 42-45% "
                       f"show -24.1% ROI on 18 games and are excluded. "
                       f"CLV gate: only bet if line has moved toward OVER since open "
                       f"(CLV>0). CLV>0 fires: +12.2% ROI. CLV≤0: -18.1% ROI."),
            "priority": 3,   # Swapped: was 4, now above S1 standalone
        })

    try:
        game_month = int((game.get("game_date") or "")[:10].split("-")[1])
    except (ValueError, IndexError):
        game_month = 0

    # ── Signal 5: H3b — independent wind-out OVER signal ────────────────
    # Fires as a standalone pick (priority 5) at whitelisted parks with
    # park factor ≥ H3B_MIN_PARK_FACTOR. Adding the PF gate ensures the
    # wind-out effect is not diluted at pitcher-friendly parks where the
    # run environment is already suppressed (2024-2025 data: effect
    # strongest at PF ≥ 98, weakest at PF < 95).
    venue_name   = game.get("venue_name") or ""
    park_factor  = game.get("park_factor_runs") or 100   # default neutral if missing
    h3b_park_ok  = venue_name in H3B_PARK_WHITELIST
    h3b_pf_ok    = park_factor >= H3B_MIN_PARK_FACTOR
    if (wind_eligible
            and wind_dir == "OUT"
            and wind_mph >= WIND_OUT_MIN_MPH
            and total is not None
            and h3b_park_ok
            and h3b_pf_ok):
        if "MV-B" in result["signals"]:
            result["signals"].append("H3b")
            for p in result["picks"]:
                if p["market"] == "TOTAL":
                    p["reason"] += (f" H3b confirms (wind-out {wind_mph} mph, "
                                    f"PF {park_factor}, z=2.99 p=0.003).")
                    break
        else:
            result["signals"].append("H3b")
            result["picks"].append({
                "bet":    f"OVER {total}",
                "market": "TOTAL",
                "odds":   fmt_odds(game.get("over_odds")) or "-110",
                "reason": (f"H3b — Wind OUT {wind_mph} mph at {venue_name} "
                           f"(whitelisted, PF {park_factor}). "
                           f"OVER edge: 52.2% over rate on 4,625 games "
                           f"(z=2.99, p=0.003 SBRO). Market under-adjusts for wind-out."),
                "priority": 5,
            })
        # Late-season performance flag — Aug/Sep over rate is historically weaker.
        # 2024 Aug: 22.2% over rate (n=9). 2024 Sep: 37.5% (n=8). Apr-Jul normal (50-54%).
        # Signal still fires — flag informs the user to size down (use --late-season-stake).
        if game_month in H3B_LATE_SEASON_MONTHS:
            result["data_flags"].append(
                f"H3b late-season caution ({venue_name}): "
                f"Aug/Sep wind-out OVER rate was 22–38% in 2024 vs 50–54% Apr–Jul. "
                f"Run environment suppression likely. "
                f"Signal valid but consider reduced stake (--late-season-stake)."
            )
    elif (wind_eligible
            and wind_dir == "OUT"
            and wind_mph >= WIND_OUT_MIN_MPH
            and total is not None):
        if not h3b_park_ok:
            result["data_flags"].append(
                f"Wind OUT {wind_mph} mph but {venue_name or 'this venue'} is not on the "
                f"H3b whitelist — wind reading may not reflect in-play conditions."
            )
        elif not h3b_pf_ok:
            result["data_flags"].append(
                f"Wind OUT {wind_mph} mph at {venue_name} but PF {park_factor} < {H3B_MIN_PARK_FACTOR} "
                f"— pitcher-friendly park offsets wind-out OVER edge."
            )

    # ── Signal 6: July OVER — seasonal line-setting lag (REINFORCER ONLY) ──
    # 3-year backtesting (2023-2025, n=74) shows JulyOVER as a standalone
    # Top Pick is consistently unprofitable: ROI -25.5% (2023), -8.2% (2024),
    # -5.8% (2025). The seasonal finding from the CSV data is real (52.3%
    # OVER rate, p=0.0006) but the signal fires on any qualifying game
    # including ones where the market has already priced the edge.
    # DECISION (Mar 2026): JulyOVER no longer generates standalone picks.
    # It only reinforces existing H3b or MV-B OVER picks with seasonal context.
    # Removing it as a standalone pick improves modelled 3-year ROI from
    # +2.27% to +7.28% — the single highest-impact change available.

    if (game_month in JULY_OVER_MONTHS
            and total is not None
            and JULY_OVER_MIN_TOTAL <= total <= JULY_OVER_MAX_TOTAL
            and park_factor >= JULY_OVER_MIN_PF
            and not suppress_wind):
        # Reinforce existing OVER picks only — never generate a standalone pick
        if "H3b" in result["signals"] or "MV-B" in result["signals"]:
            result["signals"].append("JulyOVER")
            for p in result["picks"]:
                if p["market"] == "TOTAL":
                    p["reason"] += f" JulyOVER seasonal edge confirms (52.3% rate, p=0.0006)."
                    break
        # If no OVER signal exists: note in data_flags for the brief consumer
        # but do NOT place a bet on the July seasonal edge alone.
        else:
            result["data_flags"].append(
                f"July seasonal OVER edge present (PF {park_factor}, total {total}) "
                f"but no wind/dog signal to stack it with — no bet placed. "
                f"JulyOVER is a reinforcer only, not a standalone signal."
            )

    # ── AVOID flags ──────────────────────────────────────────────────────
    avoid_reasons = []

    # Wind-in + over is a classic trap (HIGH wind venues only — suppressed handled separately)
    if (wind_eligible
            and wind_dir == "IN"
            and wind_mph >= WIND_IN_MIN_MPH
            and total is not None):
        avoid_reasons.append(
            f"Wind IN {wind_mph} mph at this venue — DO NOT bet OVER {total}. "
            f"Wind-in suppresses scoring even with strong lineups."
        )

    # Home fav on a 4-game (or worse) losing streak
    if home_streak <= -4 and home_ml is not None and home_ml < 0:
        avoid_reasons.append(
            f"{home_abbr} (home fav {fmt_odds(home_ml)}) on L{abs(home_streak)} streak. "
            f"Avoid home ML — back losing team at a price premium."
        )

    # Suppressed venue with no signal — note for consumer
    if suppress_wind and not result["signals"]:
        venue_name = game.get("venue_name", "this venue")
        avoid_reasons.append(
            f"Wind signals suppressed at {venue_name}. "
            f"Market is typically efficient here — no weather edge. "
            f"{wind_note_v[:100] if wind_note_v else ''}"
        )

    if avoid_reasons:
        result["avoid"]        = True
        result["avoid_reason"] = "; ".join(avoid_reasons)

    # ── WATCH flags (morning session) ────────────────────────────────────
    if session == "morning":
        if (wind_eligible
                and wind_dir in ("OUT", "IN")
                and wind_mph >= WIND_OUT_MIN_MPH):
            result["watch"]        = True
            result["watch_reason"] = (
                f"Wind {wind_dir} {wind_mph} mph — monitor for signal at Primary Brief. "
                f"Do not bet on opening lines alone."
            )
        if home_streak >= S1_STANDALONE_MIN or home_streak <= -4:
            result["watch"]        = True
            existing               = result["watch_reason"] or ""
            result["watch_reason"] = (
                existing
                + (" | " if existing else "")
                + f"Home team streak situation: {streak_label}. "
                  f"Track lineup and odds movement through Primary Brief."
            )

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Brief formatting helpers
# ═══════════════════════════════════════════════════════════════════════════

def matchup_line(game: dict) -> str:
    """Return formatted matchup string with (h) home indicator and ET start time."""
    away  = game.get("away_abbr", "AWAY")
    home  = game.get("home_abbr", "HOME")
    venue = game.get("venue_name") or ""

    # Convert game_start_utc → Eastern Time for display
    start_str = ""
    raw = game.get("game_start_utc") or ""
    if raw and "T" in raw:
        try:
            utc_dt = datetime.datetime.fromisoformat(raw.rstrip("Z")).replace(
                tzinfo=datetime.timezone.utc)
            et_dt  = utc_dt.astimezone(_ET)
            # %-I is POSIX-only and fails on Windows.
            # lstrip('0') on %I:%M %p achieves the same result cross-platform.
            # Guard: if stripping leaves ':MM %p' (midnight edge), keep '12'.
            t = et_dt.strftime('%I:%M %p').lstrip('0') or et_dt.strftime('%I:%M %p')
            start_str = f"  {t} ET"
        except (ValueError, AttributeError):
            pass

    return f"{away}  vs  {home} (h)    [{venue}]{start_str}"


def weather_line(game: dict) -> str:
    """Return formatted weather / conditions line."""
    wind_effect = (game.get("wind_effect") or "HIGH").upper()
    roof_type   = game.get("roof_type") or "Open"
    sky         = (game.get("sky_condition") or "").lower()

    parts = []

    # Roof / dome status prefix
    if wind_effect == "SUPPRESSED":
        # Use specific note when available, fall back to roof type
        note = game.get("wind_note") or ""
        first_sentence = note.split(".")[0] if note else f"{roof_type} — wind signals suppressed"
        parts.append(f"⚠ {first_sentence}")
    elif wind_effect == "LOW":
        roof_status = "closed" if "closed" in sky or "roof closed" in sky else "check roof"
        parts.append(f"Retractable roof ({roof_status})")

    # Temperature
    if game.get("temp_f") is not None:
        parts.append(f"{int(game['temp_f'])}°F")

    # Wind — only show signal flag for HIGH-effect venues
    if game.get("wind_mph") is not None:
        mph       = game["wind_mph"]
        direction = wind_direction_label(game.get("wind_direction") or "")
        parts.append(f"{mph} mph wind {direction}")
        if wind_effect == "HIGH" and mph >= WIND_OUT_MIN_MPH and direction in ("OUT", "IN"):
            parts.append("⚑ WIND SIGNAL")
        elif wind_effect == "MODERATE" and mph >= WIND_OUT_MIN_MPH and direction in ("OUT", "IN"):
            parts.append("~ wind (moderate venue)")

    return "  ".join(parts) if parts else "Conditions not available"


def odds_summary_line(game: dict) -> str:
    """Return compact odds summary with bookmaker source."""
    home = game.get("home_abbr", "HOME")
    away = game.get("away_abbr", "AWAY")
    hml  = fmt_odds(game.get("home_ml"))
    aml  = fmt_odds(game.get("away_ml"))
    tot  = fmt_total(game.get("total_line"))
    hrl  = game.get("home_rl")
    hrl_o = game.get("home_rl_odds")
    book = game.get("odds_bookmaker") or ""
    # Clean up bookmaker name for display
    book_display = {
        "draftkings": "DK", "fanduel": "FD", "betmgm": "MGM",
        "betonlineag": "BOL", "fanatics": "FAN", "betrivers": "BR",
        "williamhill_us": "WH", "bovada": "BOV", "betus": "BetUS",
        "lowvig": "LV", "mybookieag": "MYB",
    }.get(book, book.upper() if book else "")
    src = f"  [{book_display}]" if book_display else ""
    rl_str = ""
    if hrl is not None:
        rl_str = f"  RL: {home} {'+' if hrl >= 0 else ''}{hrl} ({fmt_odds(hrl_o)})"
    return f"ML: {home} {hml} / {away} {aml}  |  {tot}{rl_str}{src}"


def starter_line(game: dict, starters: dict) -> str:
    """Return starting pitcher line if available."""
    gpk = game.get("game_pk")
    if not gpk or gpk not in starters:
        return "Starters: not yet confirmed"
    s       = starters[gpk]
    home_id = game.get("home_team_id")
    away_id = game.get("away_team_id")
    home_s  = s.get(home_id, {})
    away_s  = s.get(away_id, {})

    def fmt_starter(d):
        if not d:
            return "TBD"
        era = f" (ERA {d['era']:.2f})" if d.get("era") is not None else ""
        return f"{d.get('name', 'TBD')}{era}"

    home_name = game.get("home_abbr", "HOME")
    away_name = game.get("away_abbr", "AWAY")
    return f"Starters: {away_name} {fmt_starter(away_s)}  vs  {home_name} (h) {fmt_starter(home_s)}"


def streak_line(game: dict, streaks: dict) -> str:
    """Return home/away streak line."""
    home_id = game.get("home_team_id")
    away_id = game.get("away_team_id")
    home_s  = streaks.get(home_id, 0)
    away_s  = streaks.get(away_id, 0)

    def label(s):
        if s > 0:
            return f"W{s}"
        if s < 0:
            return f"L{abs(s)}"
        return "—"

    h = game.get("home_abbr", "HOME")
    a = game.get("away_abbr", "AWAY")
    flags = []
    if abs(home_s) >= STREAK_THRESHOLD:
        flags.append(f"⚑ {h} (h) streak signal")
    return (f"Streak: {a} {label(away_s)}  |  {h} (h) {label(home_s)}"
            + (f"  {'  '.join(flags)}" if flags else ""))


def movement_line(game: dict, movement: dict) -> str:
    """Return line movement summary for closing session."""
    gpk = game.get("game_pk")
    if not gpk or gpk not in movement:
        return ""
    m = movement[gpk]
    parts = []
    if m.get("ml_move_cents") is not None:
        cents = float(m["ml_move_cents"])
        arrow = "▲" if cents > 0 else "▼" if cents < 0 else "→"
        parts.append(f"ML move: {arrow} {abs(cents):.1f}¢ ({m.get('move_direction', '?')})")
    if m.get("total_move") is not None:
        tm = float(m["total_move"])
        arrow = "▲" if tm > 0 else "▼" if tm < 0 else "→"
        parts.append(f"Total move: {arrow} {abs(tm):.1f}")
    flags = []
    if m.get("steam_move"):
        flags.append("🔥 STEAM")
    if m.get("reverse_line_move"):
        flags.append("↔ REVERSE LINE")
    if flags:
        parts.append("  ".join(flags))
    return "  |  ".join(parts) if parts else "No significant movement"


# ═══════════════════════════════════════════════════════════════════════════
# Brief body builders per session
# ═══════════════════════════════════════════════════════════════════════════


def build_prior_day_report(conn: sqlite3.Connection, game_date: str,
                            verbose: bool) -> str:
    """
    Prior day performance report — full slate with signal grading.

    Shows every game played yesterday with:
      · Final score, runs, winner, conditions
      · Closing odds and O/U outcome
      · Which model signals fired and whether they won
      · Top Pick, next 5 Additional Picks, and Avoids with outcomes
      · Full no-signal slate (all games accounted for)
      · Day-level P&L summary
    """
    lines = []
    generated_ts = _now_et().strftime("%Y-%m-%d %I:%M %p ET").lstrip("0")
    lines.append(banner(f"MLB Scout · PRIOR DAY REPORT  ·  Results for {game_date}"))
    lines.append(f"  Generated: {generated_ts}\n")
    lines.append(
        "\n  Full slate results with model signal grading.\n"
        "  Every game shown. Signal picks highlighted with ✓ WIN / ✗ LOSS / — PUSH.\n"
    )

    # ── Load all completed games ──────────────────────────────────────────
    cur = conn.execute("""
        SELECT
            g.game_pk,
            g.home_score, g.away_score,
            g.wind_mph, g.wind_direction, g.temp_f, g.sky_condition,
            g.game_start_utc,
            th.team_id   AS home_team_id,
            ta.team_id   AS away_team_id,
            th.abbreviation AS home_abbr,
            ta.abbreviation AS away_abbr,
            th.name         AS home_name,
            ta.name         AS away_name,
            v.name          AS venue_name,
            v.wind_effect, v.wind_note, v.roof_type,
            v.park_factor_runs, v.orientation_hp,
            go_ml.home_ml,   go_ml.away_ml,
            go_tot.total_line, go_tot.over_odds, go_tot.under_odds
        FROM   games g
        JOIN   teams  th     ON th.team_id  = g.home_team_id
        JOIN   teams  ta     ON ta.team_id  = g.away_team_id
        LEFT JOIN venues v   ON v.venue_id  = g.venue_id
        LEFT JOIN v_closing_game_odds go_ml  ON go_ml.game_pk  = g.game_pk
                                             AND go_ml.market_type  = 'moneyline'
        LEFT JOIN v_closing_game_odds go_tot ON go_tot.game_pk = g.game_pk
                                             AND go_tot.market_type = 'total'
        WHERE  g.game_date = ?
          AND  g.game_type = 'R'
          AND  g.status    = 'Final'
        ORDER  BY g.game_start_utc, g.game_pk
    """, (game_date,))
    games = [dict(r) for r in cur.fetchall()]

    if not games:
        lines.append(f"  No completed regular-season games found for {game_date}.")
        lines.append(f"  Ensure the 6 AM stats pull ran: python load_mlb_stats.py\n")
        lines.append(CAVEAT)
        return "\n".join(lines)

    # Build streaks as of game_date
    team_ids = list({g["home_team_id"] for g in games} | {g["away_team_id"] for g in games})
    streaks  = load_streaks(conn, game_date, team_ids, verbose)

    # ── Grading helpers ───────────────────────────────────────────────────
    def grade_ml(bet_side, hs, as_, odds):
        if hs is None or as_ is None:
            return "— NO RESULT", 0.0
        won = (bet_side == "home" and hs > as_) or (bet_side == "away" and as_ > hs)
        if won:
            pnl = odds / 100.0 if odds > 0 else 100.0 / abs(odds)
            return "✓ WIN", round(pnl, 2)
        return "✗ LOSS", -1.0

    def grade_total(bet, hs, as_, total, odds):
        if hs is None or as_ is None or total is None:
            return "— NO RESULT", 0.0
        runs = hs + as_
        if runs == total:
            return "— PUSH", 0.0
        hit = (bet == "over" and runs > total) or (bet == "under" and runs < total)
        if hit:
            pnl = odds / 100.0 if (odds and odds > 0) else 100.0 / abs(odds or 110)
            return "✓ WIN", round(pnl, 2)
        return "✗ LOSS", -1.0

    # ── Retroactive signal evaluation + grading ───────────────────────────
    evaluated = []
    for g in games:
        sigs = evaluate_signals(g, streaks, "primary")
        hs   = g["home_score"]; as_  = g["away_score"]
        tot  = g["total_line"]; hml  = g["home_ml"]; aml = g["away_ml"]
        runs = (hs + as_) if (hs is not None and as_ is not None) else None

        graded = []
        for p in sigs["picks"]:
            if p["market"] == "ML":
                side  = "away" if p["bet"].startswith(g["away_abbr"]) else "home"
                odds  = aml if side == "away" else hml
                res, pnl = grade_ml(side, hs, as_, odds or 0)
            elif p["market"] == "TOTAL":
                bet   = "over" if "OVER" in p["bet"].upper() else "under"
                odds  = g.get("over_odds") if bet == "over" else g.get("under_odds")
                res, pnl = grade_total(bet, hs, as_, tot, odds)
            else:
                res, pnl = "— UNKNOWN", 0.0
            graded.append({**p, "result": res, "pnl": pnl})

        ou_label = ""
        if tot is not None and runs is not None:
            ou_label = (f"OVER {tot} ({runs} runs)"  if runs > tot else
                        f"UNDER {tot} ({runs} runs)" if runs < tot else
                        f"PUSH {tot} ({runs} runs)")

        evaluated.append({
            "game": g, "sigs": sigs, "graded": graded,
            "ou_label": ou_label, "runs": runs,
            "winner": g["home_abbr"] if (hs or 0) > (as_ or 0) else g["away_abbr"],
        })

    pick_entries  = sorted([e for e in evaluated if e["graded"]],
                           key=lambda e: min(p["priority"] for p in e["graded"]))
    avoid_entries = [e for e in evaluated if e["sigs"]["avoid"] and not e["graded"]]
    nosig_entries = [e for e in evaluated if not e["graded"] and not e["sigs"]["avoid"]]
    top_pick      = pick_entries[:1]
    next_picks    = pick_entries[1:6]
    rest_picks    = pick_entries[6:]

    def pnl_str(pnl):
        return f"+{pnl:.2f}u" if pnl > 0 else (f"{pnl:.2f}u" if pnl < 0 else "push")

    def p_str(entry):
        """Signal label string for retroactive display."""
        return ", ".join(entry["sigs"]["signals"])

    def summarise(entries):
        picks = [p for e in entries for p in e["graded"]]
        if not picks:
            return None
        w = sum(1 for p in picks if p["pnl"] > 0)
        l = sum(1 for p in picks if p["pnl"] < 0)
        p = sum(1 for p in picks if p["pnl"] == 0)
        return w, l, p, sum(x["pnl"] for x in picks)

    def game_score_line(e, indent="  "):
        g = e["game"]
        hs = g["home_score"]; as_ = g["away_score"]
        s  = f"{g['away_abbr']} {as_}  –  {g['home_abbr']} {hs}  ({e['winner']} wins)"
        if e["ou_label"]:
            s += f"  |  {e['ou_label']}"
        return f"{indent}Final: {s}"

    def game_odds_line(e, indent="  "):
        g   = e["game"]
        hml = g["home_ml"]; aml = g["away_ml"]; tot = g["total_line"]
        if not hml and not aml:
            return ""
        return (f"{indent}ML: {g['home_abbr']} {fmt_odds(hml)} / "
                f"{g['away_abbr']} {fmt_odds(aml)}"
                + (f"  |  O/U {tot}" if tot else ""))

    # ════════════════════════════════════════════════════════════════════
    # LOAD CONFIRMED PICKS from brief_picks (what was actually shown)
    # ════════════════════════════════════════════════════════════════════
    confirmed_picks = load_brief_picks(conn, game_date, session="primary")
    # Also check early/afternoon sessions for that date
    if not confirmed_picks:
        for sess in ("early", "afternoon"):
            confirmed_picks = load_brief_picks(conn, game_date, sess)
            if confirmed_picks:
                break

    # Grade confirmed picks against actual outcomes
    confirmed_graded = []
    for cp in confirmed_picks:
        # Find matching evaluated entry
        match = next((e for e in evaluated
                      if e["game"]["game_pk"] == cp["game_pk"]), None)
        if match is None:
            continue
        g  = match["game"]
        hs = g["home_score"]; as_ = g["away_score"]
        hml = g["home_ml"];  aml = g["away_ml"]
        tot = g["total_line"]
        if cp["market"] == "ML":
            side = "away" if not cp["bet"].startswith(g["home_abbr"]) else "home"
            odds = aml if side == "away" else hml
            res, pnl = grade_ml(side, hs, as_, odds or 0)
        elif cp["market"] == "TOTAL":
            bet_side = "over" if "OVER" in cp["bet"].upper() else "under"
            odds = g.get("over_odds") if bet_side == "over" else g.get("under_odds")
            res, pnl = grade_total(bet_side, hs, as_, tot, odds)
        else:
            res, pnl = "— UNKNOWN", 0.0
        confirmed_graded.append({
            **cp, "result": res, "pnl": pnl,
            "game": g, "entry": match,
        })

    has_confirmed = bool(confirmed_graded)

    # ════════════════════════════════════════════════════════════════════
    # TOP PICK
    # ════════════════════════════════════════════════════════════════════
    lines.append(section("🔺  TOP PICK  —  Highest Priority Signal"))

    if has_confirmed:
        # Grade from what was ACTUALLY shown in the primary brief
        cp = confirmed_graded[0]
        g  = cp["game"]; e = cp["entry"]
        p  = e["graded"][0] if e["graded"] else None
        lines.append(f"\n  {matchup_line(g)}")
        lines.append(f"  {weather_line(g)}")
        lines.append(game_score_line(e))
        ol = game_odds_line(e)
        if ol: lines.append(ol)
        lines.append(f"\n  ┌─────────────────────────────────────────────────────────┐")
        lines.append(f"  │  BET:     {cp['bet']:<20}  ODDS: {fmt_odds(cp['odds']) if cp['odds'] else 'N/A':<8}        │")
        lines.append(f"  │  SIGNAL:  {cp['signal']:<47}  │")
        res_field = cp["result"]
        lines.append(f"  │  RESULT:  {res_field:<20}  P&L: {pnl_str(cp['pnl']):<16}    │")
        lines.append(f"  └─────────────────────────────────────────────────────────┘")
        if p:
            lines.append(f"\n  REASON: {textwrap.fill(p['reason'], width=66, subsequent_indent='          ')}")
        lines.append("")
    elif not top_pick:
        lines.append("\n  No model signals fired yesterday.\n")
    else:
        # No confirmed brief picks in brief_picks for this date.
        # This means either no brief was run before the games were played,
        # or the brief_picks table did not yet exist when the brief ran.
        #
        # CRITICAL: Do NOT grade or display any retroactive signal here as
        # a TOP PICK. Retroactive signals use post-game actual wind data —
        # they were never shown to the user before the game started and
        # cannot be counted as confirmed picks. They belong ONLY in the
        # RETROACTIVE SIGNALS section below, clearly labelled [RETROACTIVE].
        lines.append("\n  No confirmed picks on record for this date.\n")
        lines.append(
            "  ℹ  No brief was run before yesterday's games, or the session\n"
            "     did not fire any signals. The retroactive section below\n"
            "     shows what the model WOULD have selected using actual\n"
            "     post-game wind — these are for model tracking only and\n"
            "     are NOT counted in the paper account or P&L.\n"
        )

    # ════════════════════════════════════════════════════════════════════
    # ADDITIONAL MODEL SELECTIONS (next 5)
    # ════════════════════════════════════════════════════════════════════
    lines.append(section(f"📋  ADDITIONAL MODEL SELECTIONS  ({len(next_picks)})"))
    if not next_picks:
        lines.append("\n  No additional model selections yesterday.\n")
    else:
        for i, e in enumerate(next_picks, start=2):
            g = e["game"]; p = e["graded"][0]
            lines.append(f"\n  #{i}  {matchup_line(g)}")
            lines.append(f"       {weather_line(g)}")
            if g["home_score"] is not None:
                lines.append(f"       {game_score_line(e, indent='')}")
            ol = game_odds_line(e, indent="       ")
            if ol: lines.append(ol)
            lines.append(f"       BET: {p['bet']}  ODDS: {p['odds']}"
                         f"  SIGNAL: {', '.join(e['sigs']['signals'])}")
            lines.append(f"       RESULT: {p['result']}  P&L: {pnl_str(p['pnl'])}")
            lines.append(f"       {textwrap.fill(p['reason'], width=64, subsequent_indent='       ')}")
            for f in e["sigs"]["data_flags"]:
                lines.append(f"       ⚠ {f}")
            lines.append("")

    # Further signals beyond top 6
    if rest_picks:
        lines.append(section(f"📋  FURTHER SIGNALS  ({len(rest_picks)})"))
        for e in rest_picks:
            g = e["game"]; p = e["graded"][0]
            lines.append(f"\n  {matchup_line(g)}")
            if g["home_score"] is not None:
                lines.append(game_score_line(e))
            lines.append(f"  BET: {p['bet']}  SIGNAL: {', '.join(e['sigs']['signals'])}"
                         f"  →  {p['result']}  ({pnl_str(p['pnl'])})")
            lines.append("")

    # ════════════════════════════════════════════════════════════════════
    # BETS TO AVOID
    # ════════════════════════════════════════════════════════════════════
    lines.append(section(f"⛔  BETS TO AVOID  ({len(avoid_entries)} flagged)"))
    if not avoid_entries:
        lines.append("\n  No avoid flags were active yesterday.\n")
    else:
        lines.append("\n  These games were flagged to AVOID. Outcome confirms whether avoidance was correct.\n")
        for e in avoid_entries:
            g = e["game"]
            lines.append(f"  {matchup_line(g)}")
            lines.append(f"  {weather_line(g)}")
            if g["home_score"] is not None:
                lines.append(game_score_line(e))
            ol = game_odds_line(e)
            if ol: lines.append(ol)
            lines.append(f"  ⛔ AVOID: {textwrap.fill(e['sigs']['avoid_reason'], width=64, subsequent_indent='          ')}")
            lines.append("")

    # ════════════════════════════════════════════════════════════════════
    # NO SIGNAL — full remaining slate
    # ════════════════════════════════════════════════════════════════════
    lines.append(section(f"—  NO SIGNAL  ({len(nosig_entries)} games — market efficient or no qualifying conditions)"))
    if not nosig_entries:
        lines.append("\n  All games had a model signal or avoid flag yesterday.\n")
    else:
        lines.append("")
        for e in nosig_entries:
            g = e["game"]
            lines.append(f"  {matchup_line(g)}  |  {weather_line(g)}")
            if g["home_score"] is not None:
                hs = g["home_score"]; as_ = g["away_score"]
                ou = f"  |  {e['ou_label']}" if e["ou_label"] else ""
                lines.append(f"    Final: {g['away_abbr']} {as_}  –  {g['home_abbr']} {hs}"
                             f"  ({e['winner']} wins){ou}")
            ol = game_odds_line(e, indent="    ")
            if ol: lines.append(ol)
            for f in e["sigs"]["data_flags"]:
                lines.append(f"    ⚠ {f}")
            lines.append("")

    # ════════════════════════════════════════════════════════════════════
    # MODEL P&L SUMMARY
    # ════════════════════════════════════════════════════════════════════
    lines.append(section("📈  MODEL P&L SUMMARY"))
    all_picks = top_pick + next_picks + rest_picks

    for label, entries in [("Top Pick:             ", top_pick),
                            ("Additional (next 5):  ", next_picks),
                            ("All signals:          ", all_picks)]:
        s = summarise(entries)
        if s:
            w, l, p, total_pnl = s
            lines.append(f"\n  {label} {w}W {l}L {p}P   P&L: {pnl_str(total_pnl)}")

    if not summarise(all_picks):
        lines.append("\n  No model signals fired yesterday.")

    ou_games = [e for e in evaluated if e["game"]["total_line"] and e["runs"] is not None]
    if ou_games:
        overs  = sum(1 for e in ou_games if e["runs"] > e["game"]["total_line"])
        unders = sum(1 for e in ou_games if e["runs"] < e["game"]["total_line"])
        pushes = len(ou_games) - overs - unders
        lines.append(
            f"\n  Slate O/U:  {overs} Over / {unders} Under"
            + (f" / {pushes} Push" if pushes else "")
            + f"  ({overs/len(ou_games):.0%} over rate)"
        )
    lines.append("")

    # ════════════════════════════════════════════════════════════════════
    # RETROACTIVE SIGNALS (informational — with actual post-game wind)
    # ════════════════════════════════════════════════════════════════════
    retro_picks = top_pick + next_picks + rest_picks
    # Only show retroactive section if it differs from confirmed picks
    retro_signals = {(e["game"]["game_pk"], ",".join(e["sigs"]["signals"]))
                     for e in retro_picks}
    confirmed_signals = {(cp["game_pk"], cp["signal"])
                         for cp in confirmed_graded}
    has_new_retro = bool(retro_signals - confirmed_signals)

    if has_new_retro or (retro_picks and not has_confirmed):
        lines.append(section(
            "🔍  RETROACTIVE SIGNALS  —  Model with actual post-game wind"
        ))
        lines.append(
            "\n  ℹ  These signals fired using ACTUAL wind data recorded after games\n"
            "     finished. They were NOT shown in yesterday's forward-looking briefs\n"
            "     (wind is unavailable at brief time). Shown for model tracking only.\n"
            "     Paper account and confirmed P&L are based on briefs above only.\n"
        )
        if not retro_picks:
            lines.append("  No retroactive signals found either.\n")
        for e in retro_picks:
            g = e["game"]; p = e["graded"][0]
            # Skip if already shown as confirmed
            key = (g["game_pk"], ",".join(e["sigs"]["signals"]))
            if key in confirmed_signals:
                continue
            lines.append(f"  {matchup_line(g)}")
            lines.append(f"  {weather_line(g)}")
            lines.append(game_score_line(e))
            lines.append(f"  BET: {p['bet']}  SIGNAL: {p_str(e)}"
                         f"  →  {p['result']}  ({pnl_str(p['pnl'])})  [RETROACTIVE]")
            lines.append("")

    # ── Brief log ─────────────────────────────────────────────────────────
    lines.append(section("📋  BRIEF LOG — Yesterday's Sessions"))
    try:
        log_rows = conn.execute("""
            SELECT session, generated_at, games_covered, picks_count, output_file
            FROM   brief_log WHERE game_date = ? ORDER BY generated_at
        """, (game_date,)).fetchall()
        if not log_rows:
            lines.append(f"\n  No brief_log entries for {game_date}.\n")
        else:
            for r in log_rows:
                lines.append(f"  {r['session'].upper():<12}  {r['generated_at']}  "
                             f"· {r['picks_count']} pick(s)  "
                             f"· {r['output_file'] or 'no file saved'}")
            lines.append("\n  ℹ  Open any saved brief file above to review original picks.\n")
    except sqlite3.OperationalError:
        lines.append("\n  brief_log table not found.\n")

    lines.append(
        "  ─────────────────────────────────────────────────────────────────\n"
        "  CLV Reminder: compare your bet prices vs yesterday's closing lines.\n"
        "  Positive CLV (you beat the close) = process is working correctly.\n"
        "  If --compute-movement was not run last night, run it now:\n"
        "    python load_odds.py --compute-movement\n"
        "  ─────────────────────────────────────────────────────────────────\n"
    )
    # ── Dollar P&L (paper account) ─────────────────────────────────────
    # Use confirmed brief picks only — not retroactive evaluation.
    # Convert confirmed_graded rows into the format compute_paper_picks expects.
    if has_confirmed:
        # Build synthetic evaluated list from confirmed picks only
        confirmed_evaluated = [cp["entry"] for cp in confirmed_graded
                               if cp["entry"] is not None]
        paper_rows = compute_paper_picks(confirmed_evaluated, game_date)
    else:
        # No confirmed picks → no paper bets
        paper_rows = []
    record_daily_pnl(conn, paper_rows)
    season_pnl   = load_season_pnl(conn, game_date)
    lines.append(format_dollar_pnl_block(paper_rows, season_pnl, game_date))

    lines.append(CAVEAT)
    return "\n".join(lines)

def build_morning_brief(games, streaks, starters, game_date):
    lines = []
    generated_ts = _now_et().strftime("%Y-%m-%d %I:%M %p ET").lstrip("0")
    lines.append(banner(f"MLB BETTING BRIEF  ·  MORNING SESSION  ·  {game_date}"))
    lines.append(f"  Generated: {generated_ts}\n")
    lines.append(
        "\n  MORNING NOTE: These are early signal flags based on OPENING LINES.\n"
        "  Prices will move. DO NOT bet from this brief.\n"
        "  Flag games of interest and re-evaluate at the PRIMARY BRIEF (~5:30 PM).\n"
    )

    watch_games   = []
    dome_games    = []
    no_signal     = []

    for game in games:
        sigs = evaluate_signals(game, streaks, "morning")
        entry = {
            "game":   game,
            "sigs":   sigs,
            "starter": starter_line(game, starters),
            "streak":  streak_line(game, streaks),
        }
        we = (game.get("wind_effect") or "HIGH").upper()
        if we == "SUPPRESSED":
            dome_games.append(entry)
        elif sigs["watch"] or sigs["signals"]:
            watch_games.append(entry)
        else:
            no_signal.append(entry)

    # ── Watch list ───────────────────────────────────────────────────────
    lines.append(section(f"⚑  GAMES TO WATCH  ({len(watch_games)} of {len(games)})"))
    if not watch_games:
        lines.append("\n  No strong early signals on today's slate.\n")
    for e in watch_games:
        g    = e["game"]
        sigs = e["sigs"]
        lines.append(f"\n  {matchup_line(g)}")
        lines.append(f"  {weather_line(g)}")
        lines.append(f"  {odds_summary_line(g)}")
        lines.append(f"  {e['starter']}")
        lines.append(f"  {e['streak']}")
        if sigs["watch_reason"]:
            lines.append(f"\n  ▶ WATCH: {sigs['watch_reason']}")
        if sigs["signals"]:
            lines.append(f"  ▶ EARLY SIGNAL(S): {', '.join(sigs['signals'])}")
        if sigs["data_flags"]:
            for f in sigs["data_flags"]:
                lines.append(f"  ⚠ {f}")
        lines.append("")

    # ── Dome games ───────────────────────────────────────────────────────
    lines.append(section(f"⛔  SUPPRESSED / INDOOR VENUES  ({len(dome_games)})"))
    if not dome_games:
        lines.append("\n  No suppressed-wind venues today.\n")
    for e in dome_games:
        g = e["game"]
        lines.append(f"\n  {matchup_line(g)}  — No weather signal. Monitor for streak/price edge.")
        lines.append(f"  {odds_summary_line(g)}")
        lines.append(f"  {e['streak']}")
        lines.append("")

    # ── Full slate ───────────────────────────────────────────────────────
    lines.append(section(f"📋  FULL SLATE  ({len(games)} games)"))
    for game in games:
        lines.append(f"  {matchup_line(game)}  |  {weather_line(game)}")
        lines.append(f"    {odds_summary_line(game)}")
    lines.append("")
    lines.append(CAVEAT)
    return "\n".join(lines)


def build_primary_brief(games, streaks, starters, game_date,
                        session_label="PRIMARY", s6_fires=None,
                        conn=None, session=None):
    if s6_fires is None:
        s6_fires = {}
    _action = {
        "EARLY GAMES": "✅  EARLY GAMES ACTION WINDOW — All unplayed games. Decide before first pitch.",
        "AFTERNOON":   "✅  AFTERNOON ACTION WINDOW — All unplayed games. Decide before first pitch.",
        "PRIMARY":     "✅  PRIMARY ACTION WINDOW — All unplayed games. Make your betting decisions NOW.",
        "LATE GAMES":  "✅  LATE GAMES ACTION WINDOW — West Coast games still unstarted. Check odds and act now.",
    }.get(session_label, "✅  ACTION WINDOW — Make your betting decisions NOW.")
    lines = []
    generated_ts = _now_et().strftime("%Y-%m-%d %I:%M %p ET").lstrip("0")
    lines.append(banner(f"MLB BETTING BRIEF  ·  {session_label} SESSION  ·  {game_date}"))
    lines.append(f"  Generated: {generated_ts}\n")
    lines.append(
        f"\n  {_action}\n"
        "  Injury news absorbed. Lineups posting. Lines near closing.\n"
        "  Confirm each line before placing. No bet above 2% of bankroll.\n"
    )

    all_picks   = []
    avoid_games = []
    no_signal   = []

    for game in games:
        sigs = evaluate_signals(game, streaks, "primary")
        entry = {
            "game":    game,
            "sigs":    sigs,
            "starter": starter_line(game, starters),
            "streak":  streak_line(game, streaks),
        }
        if sigs["picks"]:
            all_picks.append(entry)
        elif sigs["avoid"]:
            avoid_games.append(entry)
        else:
            no_signal.append(entry)

    # Sort picks by priority (lower = higher priority)
    all_picks.sort(key=lambda e: min(p["priority"] for p in e["sigs"]["picks"]))

    # ── Signal Tracker — intra-day pick status vs earlier sessions ────────
    # Only runs when conn is available and there were prior sessions today.
    if conn is not None and session is not None:
        prior_picks = load_todays_prior_sessions(conn, game_date, session)
        tracker_block = build_signal_tracker_block(
            prior_picks, all_picks, streaks, session
        )
        if tracker_block:
            lines.append(tracker_block)

    # ── Top Pick ─────────────────────────────────────────────────────────
    lines.append(section("🔺  TOP PICK  —  Highest Probability Signal"))
    if not all_picks:
        lines.append("\n  No confirmed signals fire on today's slate.\n")
        lines.append("  Wait for tomorrow. Discipline > action.\n")
    else:
        top = all_picks[0]
        g   = top["game"]
        p   = sorted(top["sigs"]["picks"], key=lambda x: x["priority"])[0]

        lines.append(f"\n  {matchup_line(g)}")
        lines.append(f"  {weather_line(g)}")
        lines.append(f"  {top['starter']}")
        lines.append(f"  {top['streak']}")
        # Movement alert — compare vs earliest prior session pick today
        alert = movement_alert(conn, game_date, session,
                               g["game_pk"], g.get("total_line"), g.get("home_ml"))
        if alert:
            lines.append("")
            lines.append(alert)
        lines.append(f"\n  ┌─────────────────────────────────────────────────────────┐")
        lines.append(f"  │  BET:     {p['bet']:<20}  ODDS: {p['odds']:<8}        │")
        lines.append(f"  │  SIGNAL:  {', '.join(top['sigs']['signals']):<47}  │")
        lines.append(f"  └─────────────────────────────────────────────────────────┘")
        lines.append(f"\n  {odds_summary_line(g)}")
        lines.append(f"\n  REASON: {textwrap.fill(p['reason'], width=66, subsequent_indent='          ')}")
        if top["sigs"]["data_flags"]:
            for f in top["sigs"]["data_flags"]:
                lines.append(f"  ⚠ DATA: {f}")
        lines.append("")

    # ── Additional Picks ─────────────────────────────────────────────────
    rest = all_picks[1:]
    lines.append(section(f"📋  ADDITIONAL MODEL SELECTIONS  ({len(rest)})"))
    if not rest:
        lines.append("\n  No additional confirmed signals today.\n")
    for i, entry in enumerate(rest, start=1):
        g    = entry["game"]
        sigs = entry["sigs"]
        best = sorted(sigs["picks"], key=lambda x: x["priority"])[0]
        lines.append(f"\n  #{i}  {matchup_line(g)}")
        lines.append(f"       {weather_line(g)}")
        lines.append(f"       {entry['starter']}")
        lines.append(f"       {entry['streak']}")
        alert = movement_alert(conn, game_date, session,
                               g["game_pk"], g.get("total_line"), g.get("home_ml"))
        if alert:
            lines.append("")
            lines.append(alert)
        lines.append(f"       BET: {best['bet']:<20} ODDS: {best['odds']:<8} SIGNAL: {', '.join(sigs['signals'])}")
        lines.append(f"       {textwrap.fill(best['reason'], width=66, subsequent_indent='       ')}")
        if sigs["data_flags"]:
            for f in sigs["data_flags"]:
                lines.append(f"       ⚠ DATA: {f}")
        lines.append("")

    # ── S6 W≥7 Pitcher Streak (Monitoring Signal) ────────────────────────
    # Separate from main picks — monitoring status, half-stake until N≥50.
    # When S6 also has S1 active: displayed as double-confirmation note.
    # When S6 alone: displayed as standalone monitoring entry.
    lines.append(section(f"🔬  S6 PITCHER STREAK MONITOR  ({len(s6_fires)} fire(s))"))
    if not s6_fires:
        lines.append("\n  No S6 W≥7 pitcher streak fires today.\n")
    else:
        lines.append(
            "\n  STATUS: Monitoring signal — +25.0% ROI on 27 fires (2018–2025, all bookmakers).\n"
            "  74% of fires are independent of S1 team streaks (additive, not redundant).\n"
            "  Stake: 0.5 unit until cumulative N ≥ 50. Full unit when S1 also active.\n"
        )
        # Build game lookup for matchup display
        game_lookup = {g["game_pk"]: g for g in games}
        for gk, fire in s6_fires.items():
            g = game_lookup.get(gk)
            if g:
                lines.append(f"  {matchup_line(g)}")
                lines.append(f"  {odds_summary_line(g)}")
            lines.append(f"  SIGNAL: {fire['signal_label']}")
            lines.append(f"  BET: {fire['bet_side'].upper()} ML  "
                         f"  STREAK: W{fire['win_streak']} ({fire['start_count']} starts this season)")
            stake_note = (
                "  ★ FULL STAKE — S1 also active (double confirmation)"
                if fire["s1_also_active"]
                else f"  ◆ 0.5 UNIT STAKE — monitoring (N={fire['cumulative_n']}/{50})"
            )
            lines.append(stake_note)
            lines.append(f"\n  REASON: {textwrap.fill(fire['reason_text'], width=66, subsequent_indent='          ')}")
            lines.append("")

    # ── Bets to Avoid ────────────────────────────────────────────────────
    lines.append(section(f"⛔  BETS TO AVOID  ({len(avoid_games)})"))
    if not avoid_games:
        lines.append("\n  No active avoid flags today.\n")
    for entry in avoid_games:
        g    = entry["game"]
        sigs = entry["sigs"]
        lines.append(f"\n  {matchup_line(g)}")
        lines.append(f"  {weather_line(g)}")
        lines.append(f"  {odds_summary_line(g)}")
        lines.append(f"  {entry['streak']}")
        lines.append(f"  ⛔ AVOID: {textwrap.fill(sigs['avoid_reason'], width=64, subsequent_indent='          ')}")
        lines.append("")

    # ── No-signal slate ──────────────────────────────────────────────────
    if no_signal:
        lines.append(section(f"—  NO SIGNAL  ({len(no_signal)} games — market efficient or insufficient data)"))
        for entry in no_signal:
            g = entry["game"]
            lines.append(f"  {matchup_line(g)}  |  {weather_line(g)}")
            lines.append(f"    {odds_summary_line(g)}")
            if entry["sigs"]["data_flags"]:
                for f in entry["sigs"]["data_flags"]:
                    lines.append(f"    ⚠ {f}")
        lines.append("")

    lines.append(
        "\n  ─────────────────────────────────────────────────────────────────\n"
        "  All bets assume flat-unit sizing. No bet above 2% of bankroll.\n"
        "  Confirm wind direction and speed at game time via Weather.com\n"
        "  or Windy.com. Lines shown are from last odds pull.\n"
        "  ─────────────────────────────────────────────────────────────────\n"
    )
    lines.append(CAVEAT)
    return "\n".join(lines)


def build_closing_brief(games, streaks, starters, movement, game_date):
    lines = []
    generated_ts = _now_et().strftime("%Y-%m-%d %I:%M %p ET").lstrip("0")
    lines.append(banner(f"MLB BETTING BRIEF  ·  CLOSING SESSION  ·  {game_date}"))
    lines.append(f"  Generated: {generated_ts}\n")
    lines.append(
        "\n  CLOSING CONFIRMATION — Compare against Primary Brief picks.\n"
        "  No new bets unless closing price is BETTER than Primary Brief price.\n"
        "  Flag any line that moved 3+ cents since the Primary Brief.\n"
    )

    for game in games:
        sigs = evaluate_signals(game, streaks, "closing")
        g    = game
        lines.append(f"\n  {matchup_line(g)}")
        lines.append(f"  {weather_line(g)}")
        lines.append(f"  {starter_line(g, starters)}")
        lines.append(f"  {streak_line(g, streaks)}")
        lines.append(f"  {odds_summary_line(g)}")

        # Line movement
        mov = movement_line(g, movement)
        if mov:
            lines.append(f"  Movement: {mov}")

        # Signal hold check
        if sigs["picks"]:
            lines.append(f"  ✅ Signal(s) still ACTIVE: {', '.join(sigs['signals'])}")
            for p in sigs["picks"]:
                lines.append(f"     → {p['bet']}  {p['odds']}  ({p['reason'][:60]}…)")
        else:
            lines.append(f"  — No new signal fires at closing prices.")

        # Closing-specific flag: steam or reverse line move
        gpk = game.get("game_pk")
        if gpk and gpk in movement:
            m = movement[gpk]
            if m.get("steam_move") or m.get("reverse_line_move"):
                lines.append(f"  🔥 SHARP ACTION DETECTED — market may have information.")

        if sigs["data_flags"]:
            for f in sigs["data_flags"]:
                lines.append(f"  ⚠ DATA: {f}")
        lines.append("")

    lines.append(
        "  ─────────────────────────────────────────────────────────────────\n"
        "  CLV Note: Your edge is measured against these closing prices.\n"
        "  If you bet at Primary Brief prices, compare to these lines now.\n"
        "  Run --compute-movement at 11:30 PM for full movement analysis.\n"
        "  ─────────────────────────────────────────────────────────────────\n"
    )
    lines.append(CAVEAT)
    return "\n".join(lines)



# ═══════════════════════════════════════════════════════════════════════════
# WORD (DOCX) OUTPUT
# ═══════════════════════════════════════════════════════════════════════════
# Requires: pip install python-docx
# Produces briefs/YYYY-MM-DD_SESSION.docx alongside the .txt file.
# Add --docx flag to any session command:
#   python generate_daily_brief.py --session primary --docx
#
# Design:
#   · Dark accent colour (#1F3864 navy) for headers — readable on print
#   · Signal picks in a shaded table — stands out immediately
#   · Full slate as a compact 4-column table (away, home, ML, O/U)
#   · Avoid / Watch flags as bold callout paragraphs
#   · All sessions supported: prior, morning, early, afternoon, primary, closing, late
# ═══════════════════════════════════════════════════════════════════════════

def _set_cell_bg(cell, hex_color: str):
    """Set table cell background colour (shading)."""
    tc   = cell._tc
    tcPr = tc.get_or_add_tcPr()
    shd  = OxmlElement("w:shd")
    shd.set(qn("w:val"),   "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"),  hex_color)
    tcPr.append(shd)


def _cell_para(cell, text: str, bold=False, size_pt=10,
               color_hex: str = "000000", align=None):
    """Clear a cell and add a single styled paragraph."""
    if align is None:
        align = WD_ALIGN_PARAGRAPH.LEFT
    cell.paragraphs[0].clear()
    p   = cell.paragraphs[0]
    p.alignment = align
    run = p.add_run(text)
    run.bold      = bold
    run.font.size = Pt(size_pt)
    run.font.color.rgb = RGBColor(
        int(color_hex[0:2], 16),
        int(color_hex[2:4], 16),
        int(color_hex[4:6], 16),
    )
    return p


def _add_heading(doc, text: str, level: int = 1):
    """
    Banner heading (level=1) or section heading (level=2).
    Uses direct paragraph formatting so it works without style conflicts.
    """
    p   = doc.add_paragraph()
    run = p.add_run(text.upper())
    if level == 1:
        run.font.size  = Pt(16)
        run.font.bold  = True
        run.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
        # Navy shaded background via paragraph border hack
        pPr  = p._p.get_or_add_pPr()
        shd  = OxmlElement("w:shd")
        shd.set(qn("w:val"),   "clear")
        shd.set(qn("w:color"), "auto")
        shd.set(qn("w:fill"),  "1F3864")
        pPr.append(shd)
        p.paragraph_format.space_before = Pt(12)
        p.paragraph_format.space_after  = Pt(4)
        p.paragraph_format.left_indent  = Inches(0.1)
    else:
        run.font.size  = Pt(12)
        run.font.bold  = True
        run.font.color.rgb = RGBColor(0x1F, 0x38, 0x64)
        p.paragraph_format.space_before = Pt(10)
        p.paragraph_format.space_after  = Pt(2)
        # Bottom border only
        pPr = p._p.get_or_add_pPr()
        pBdr = OxmlElement("w:pBdr")
        bot  = OxmlElement("w:bottom")
        bot.set(qn("w:val"),   "single")
        bot.set(qn("w:sz"),    "6")
        bot.set(qn("w:space"), "1")
        bot.set(qn("w:color"), "1F3864")
        pBdr.append(bot)
        pPr.append(pBdr)
    return p


def _add_note(doc, text: str, italic=True, color_hex="475569"):
    """Small italic note paragraph."""
    p   = doc.add_paragraph()
    run = p.add_run(text)
    run.italic     = italic
    run.font.size  = Pt(9)
    run.font.color.rgb = RGBColor(
        int(color_hex[0:2], 16),
        int(color_hex[2:4], 16),
        int(color_hex[4:6], 16),
    )
    p.paragraph_format.space_before = Pt(0)
    p.paragraph_format.space_after  = Pt(4)
    return p


def _add_matchup_block(doc, game: dict, streaks: dict, starters: dict,
                       sigs: dict, show_movement: bool = False,
                       movement: dict = None, show_picks: bool = True):
    """
    Render one game block: matchup line, weather, odds, streak, starters,
    then any signals/picks/avoid/watch in a shaded pick table if relevant.
    """
    # ── Matchup header ───────────────────────────────────────────────────
    home = game.get("home_abbr", "HOME")
    away = game.get("away_abbr", "AWAY")
    venue = game.get("venue_name") or ""
    t     = _game_start_et(game)          # ET start time — always included
    start_suffix = f"  {t}" if t else ""
    matchup_txt = f"{away}  vs  {home} (h)    [{venue}]{start_suffix}"

    p   = doc.add_paragraph()
    run = p.add_run(matchup_txt)
    run.bold      = True
    run.font.size = Pt(11)
    run.font.color.rgb = RGBColor(0x1F, 0x38, 0x64)
    p.paragraph_format.space_before = Pt(8)
    p.paragraph_format.space_after  = Pt(1)

    # ── Details line (weather | odds | streak | starters) ────────────────
    details = [
        weather_line(game),
        odds_summary_line(game),
        streak_line(game, streaks),
        starter_line(game, starters),
    ]
    for detail in details:
        p   = doc.add_paragraph()
        run = p.add_run(detail)
        run.font.size = Pt(9)
        run.font.color.rgb = RGBColor(0x33, 0x33, 0x33)
        p.paragraph_format.space_before = Pt(0)
        p.paragraph_format.space_after  = Pt(0)
        p.paragraph_format.left_indent  = Inches(0.2)

    # ── Line movement (closing session) ──────────────────────────────────
    if show_movement and movement:
        mov = movement_line(game, movement)
        if mov:
            p   = doc.add_paragraph()
            run = p.add_run(f"Movement: {mov}")
            run.font.size = Pt(9)
            run.font.color.rgb = RGBColor(0x8B, 0x5C, 0xF6)
            p.paragraph_format.left_indent = Inches(0.2)
            p.paragraph_format.space_after = Pt(0)

    # ── Picks table ──────────────────────────────────────────────────────
    if sigs["picks"] and show_picks:
        tbl = doc.add_table(rows=1, cols=4)
        tbl.style = "Table Grid"
        tbl.autofit = False
        # Column widths: Signal | Bet | Odds | Reason  (total 9360 DXA = 6.5")
        widths = [1170, 1440, 720, 6030]   # DXA units
        for i, cell in enumerate(tbl.rows[0].cells):
            cell.width = widths[i]

        # Header row
        headers = ["SIGNAL", "BET", "ODDS", "REASON"]
        header_bg = "1F3864"
        for i, (cell, hdr) in enumerate(zip(tbl.rows[0].cells, headers)):
            _set_cell_bg(cell, header_bg)
            _cell_para(cell, hdr, bold=True, size_pt=9,
                       color_hex="FFFFFF", align=WD_ALIGN_PARAGRAPH.CENTER)

        for pick in sorted(sigs["picks"], key=lambda x: x["priority"]):
            row_cells = tbl.add_row().cells
            pick_bg   = "E8F5E9" if pick["market"] == "TOTAL" else "E3F2FD"
            for cell in row_cells:
                _set_cell_bg(cell, pick_bg)
            _cell_para(row_cells[0], ", ".join(sigs["signals"]),
                       bold=True, size_pt=9, color_hex="1F3864")
            _cell_para(row_cells[1], pick["bet"],
                       bold=True, size_pt=10, color_hex="0D47A1")
            _cell_para(row_cells[2], pick["odds"],
                       bold=True, size_pt=10, color_hex="1B5E20",
                       align=WD_ALIGN_PARAGRAPH.CENTER)
            # Wrap reason text
            reason_short = (pick["reason"][:300] + "…")                            if len(pick["reason"]) > 300 else pick["reason"]
            _cell_para(row_cells[3], reason_short, size_pt=8, color_hex="333333")

        doc.add_paragraph()  # spacer

    # ── Avoid flag ───────────────────────────────────────────────────────
    if sigs["avoid"]:
        p   = doc.add_paragraph()
        run = p.add_run(f"⛔  AVOID: {sigs['avoid_reason']}")
        run.bold      = True
        run.font.size = Pt(9)
        run.font.color.rgb = RGBColor(0xC6, 0x28, 0x28)
        p.paragraph_format.left_indent  = Inches(0.2)
        p.paragraph_format.space_before = Pt(2)
        p.paragraph_format.space_after  = Pt(2)

    # ── Watch flag ───────────────────────────────────────────────────────
    if sigs.get("watch") and sigs.get("watch_reason"):
        p   = doc.add_paragraph()
        run = p.add_run(f"▶  WATCH: {sigs['watch_reason']}")
        run.bold      = True
        run.font.size = Pt(9)
        run.font.color.rgb = RGBColor(0xE6, 0x5C, 0x00)
        p.paragraph_format.left_indent  = Inches(0.2)
        p.paragraph_format.space_before = Pt(2)
        p.paragraph_format.space_after  = Pt(2)

    # ── Data flags ───────────────────────────────────────────────────────
    for flag in sigs.get("data_flags", []):
        p   = doc.add_paragraph()
        run = p.add_run(f"⚠  {flag}")
        run.font.size = Pt(8)
        run.font.color.rgb = RGBColor(0x92, 0x40, 0x0E)
        p.paragraph_format.left_indent = Inches(0.2)
        p.paragraph_format.space_after = Pt(1)


def _add_full_slate_table(doc, games: list):
    """Compact 5-column table: Time | Away | Home | ML | O/U."""
    _add_heading(doc, "Full Slate", level=2)
    tbl = doc.add_table(rows=1, cols=5)
    tbl.style = "Table Grid"
    tbl.autofit = False
    # Total usable width ~7200 twips (page minus margins).
    # TIME col is narrowest; ML col is widest to fit odds pairs.
    widths = [900, 1200, 1200, 2400, 1200]
    for i, cell in enumerate(tbl.rows[0].cells):
        cell.width = widths[i]

    headers = ["TIME (ET)", "AWAY", "HOME", "MONEYLINE", "O/U"]
    for cell, hdr in zip(tbl.rows[0].cells, headers):
        _set_cell_bg(cell, "1F3864")
        _cell_para(cell, hdr, bold=True, size_pt=9,
                   color_hex="FFFFFF", align=WD_ALIGN_PARAGRAPH.CENTER)

    for game in games:
        home    = game.get("home_abbr", "HOME")
        away    = game.get("away_abbr", "AWAY")
        hml     = fmt_odds(game.get("home_ml"))
        aml     = fmt_odds(game.get("away_ml"))
        ml_txt  = f"{home} {hml} / {away} {aml}"
        tot_txt = fmt_total(game.get("total_line"))
        time_et = _game_start_et(game)   # "7:10 PM ET" or ""

        row_cells = tbl.add_row().cells
        _set_cell_bg(row_cells[0], "EEF2FF")   # light blue tint for time col
        _set_cell_bg(row_cells[1], "F5F5F5")
        _set_cell_bg(row_cells[2], "FFFFFF")
        _set_cell_bg(row_cells[3], "F5F5F5")
        _set_cell_bg(row_cells[4], "FFFFFF")
        _cell_para(row_cells[0], time_et or "—", size_pt=9, color_hex="1F3864",
                   align=WD_ALIGN_PARAGRAPH.CENTER)
        _cell_para(row_cells[1], away,            size_pt=10, color_hex="333333")
        _cell_para(row_cells[2], f"{home} (h)",   bold=True, size_pt=10, color_hex="1F3864")
        _cell_para(row_cells[3], ml_txt,           size_pt=9, color_hex="333333")
        _cell_para(row_cells[4], tot_txt,          size_pt=9, color_hex="333333",
                   align=WD_ALIGN_PARAGRAPH.CENTER)
    doc.add_paragraph()


def _new_brief_doc(session: str, game_date: str) -> "Document":
    """Create and configure a new Document with header/footer."""
    doc = Document()

    # Page margins — 0.75" all sides for denser layout
    for section in doc.sections:
        section.top_margin    = Inches(0.75)
        section.bottom_margin = Inches(0.75)
        section.left_margin   = Inches(0.75)
        section.right_margin  = Inches(0.75)

    # Default font
    style = doc.styles["Normal"]
    style.font.name = "Calibri"
    style.font.size = Pt(10)

    # Header
    hdr = doc.sections[0].header
    hdr.paragraphs[0].clear()
    htxt = f"MLB Scout  ·  {session.upper()} BRIEF  ·  {game_date}"
    run  = hdr.paragraphs[0].add_run(htxt)
    run.font.size = Pt(9)
    run.font.color.rgb = RGBColor(0x64, 0x74, 0x8B)
    run.bold = True
    hdr.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER

    return doc


def build_docx_brief(session: str, game_date: str,
                     games: list, streaks: dict, starters: dict,
                     movement: dict = None) -> "Document":
    """
    Build a Word Document for any session type.
    Returns a python-docx Document ready to save.
    """
    doc = _new_brief_doc(session, game_date)

    # ── Banner ────────────────────────────────────────────────────────────
    session_titles = {
        "morning":   "Morning Brief — Watch List",
        "early":     "Early Games Brief — 1 PM Action",
        "afternoon": "Afternoon Brief — 4 PM Action",
        "primary":   "Primary Brief — Evening Action",
        "closing":   "Closing Brief — Final Confirmation",
        "late":      "Late Games Brief — West Coast Action",
        "prior":     "Prior Day Report",
    }
    _add_heading(doc, f"MLB Betting Model  ·  {session_titles.get(session, session.upper())}  ·  {game_date}")

    # ── Session note ──────────────────────────────────────────────────────
    notes = {
        "morning":   "Early signal flags based on OPENING LINES. DO NOT bet from this brief. Re-evaluate at Primary (~5:30 PM).",
        "early":     "All unplayed games on today's slate. Decide now before first pitch. Confirm lineup and weather.",
        "afternoon": "All unplayed games on today's slate. Decide now before first pitch. Confirm lineup and weather.",
        "primary":   "PRIMARY ACTION WINDOW — All unplayed games on today's slate. Make your betting decisions NOW. Confirm each line before placing.",
        "closing":   "CLOSING CONFIRMATION — Compare against Primary Brief picks. No new bets unless price is BETTER than Primary.",
        "late":      "LATE GAMES ACTION WINDOW — West Coast games still unstarted. Check odds and act now. Confirm each line before placing.",
        "prior":     "Yesterday's results. Compare against picks from your Primary Brief.",
    }
    _add_note(doc, notes.get(session, ""), italic=True, color_hex="475569")
    doc.add_paragraph()

    if not games:
        _add_note(doc, "No games found for this session.", color_hex="C62828")
        return doc

    # ── Evaluate signals for all games ───────────────────────────────────
    entries = []
    for game in games:
        sigs = evaluate_signals(game, streaks, session)
        entries.append({"game": game, "sigs": sigs})

    # ── MORNING layout ────────────────────────────────────────────────────
    if session == "morning":
        watch   = [e for e in entries
                   if (e["game"].get("wind_effect") or "HIGH").upper() != "SUPPRESSED"
                   and (e["sigs"]["watch"] or e["sigs"]["signals"])]
        domes   = [e for e in entries
                   if (e["game"].get("wind_effect") or "HIGH").upper() == "SUPPRESSED"]
        no_sig  = [e for e in entries if e not in watch and e not in domes]

        _add_heading(doc, f"Games to Watch  ({len(watch)} of {len(games)})", level=2)
        if not watch:
            _add_note(doc, "No strong early signals on today's slate.", color_hex="475569")
        for e in watch:
            _add_matchup_block(doc, e["game"], streaks, starters, e["sigs"])

        _add_heading(doc, f"Suppressed / Indoor Venues  ({len(domes)})", level=2)
        if not domes:
            _add_note(doc, "No suppressed-wind venues today.", color_hex="475569")
        for e in domes:
            _add_matchup_block(doc, e["game"], streaks, starters, e["sigs"])

        _add_full_slate_table(doc, games)

    # ── PRIMARY / EARLY / AFTERNOON layout ───────────────────────────────
    elif session in ("primary", "early", "afternoon", "late"):
        picks_entries  = [e for e in entries if e["sigs"]["picks"]]
        avoid_entries  = [e for e in entries if not e["sigs"]["picks"] and e["sigs"]["avoid"]]
        nosig_entries  = [e for e in entries if not e["sigs"]["picks"] and not e["sigs"]["avoid"]]
        picks_entries.sort(key=lambda e: min(p["priority"] for p in e["sigs"]["picks"]))

        # Top Pick
        _add_heading(doc, "Top Pick  —  Highest Probability Signal", level=2)
        if not picks_entries:
            _add_note(doc, "No confirmed signals fire on today's slate. Wait for tomorrow. Discipline > action.", color_hex="C62828")
        else:
            _add_matchup_block(doc, picks_entries[0]["game"], streaks, starters,
                               picks_entries[0]["sigs"])

        # Additional picks
        rest = picks_entries[1:]
        _add_heading(doc, f"Additional Model Selections  ({len(rest)})", level=2)
        if not rest:
            _add_note(doc, "No additional confirmed signals today.", color_hex="475569")
        for e in rest:
            _add_matchup_block(doc, e["game"], streaks, starters, e["sigs"])

        # Avoid
        _add_heading(doc, f"Bets to Avoid  ({len(avoid_entries)})", level=2)
        if not avoid_entries:
            _add_note(doc, "No active avoid flags today.", color_hex="475569")
        for e in avoid_entries:
            _add_matchup_block(doc, e["game"], streaks, starters, e["sigs"])

        # No signal
        _add_heading(doc, f"No Signal  ({len(nosig_entries)} games)", level=2)
        _add_full_slate_table(doc, [e["game"] for e in nosig_entries])

    # ── PRIOR DAY layout ──────────────────────────────────────────────────
    elif session == "prior":
        # Re-evaluate signals retroactively on completed games and grade results.
        # Mirrors build_prior_day_report() logic using the docx helper functions.

        def _grade_ml(bet_side, hs, as_, odds):
            if hs is None or as_ is None:
                return "NO RESULT", 0.0
            won = (bet_side == "home" and hs > as_) or (bet_side == "away" and as_ > hs)
            if won:
                pnl = (odds / 100.0) if odds and odds > 0 else (100.0 / abs(odds)) if odds else 0.0
                return "WIN", round(pnl, 2)
            return "LOSS", -1.0

        def _grade_total(bet, hs, as_, total, odds):
            if hs is None or as_ is None or total is None:
                return "NO RESULT", 0.0
            runs = hs + as_
            if runs == total:
                return "PUSH", 0.0
            hit = (bet == "over" and runs > total) or (bet == "under" and runs < total)
            if hit:
                pnl = (odds / 100.0) if odds and odds > 0 else (100.0 / abs(odds or 110))
                return "WIN", round(pnl, 2)
            return "LOSS", -1.0

        def _ou_label(g):
            tot  = g.get("total_line"); hs = g.get("home_score"); as_ = g.get("away_score")
            if tot is None or hs is None or as_ is None:
                return ""
            runs = hs + as_
            if runs > tot:  return f"OVER {tot} ({runs} runs)"
            if runs < tot:  return f"UNDER {tot} ({runs} runs)"
            return f"PUSH {tot} ({runs} runs)"

        def _result_color(result: str) -> str:
            return "006400" if result == "WIN" else ("C00000" if result == "LOSS" else "555555")

        def _pnl_str(pnl: float) -> str:
            return f"+{pnl:.2f}u" if pnl > 0 else (f"{pnl:.2f}u" if pnl < 0 else "push")

        # Build evaluated list
        evaluated = []
        for g in games:
            sigs = evaluate_signals(g, streaks, "primary")
            hs   = g.get("home_score"); as_ = g.get("away_score")
            tot  = g.get("total_line"); hml = g.get("home_ml"); aml = g.get("away_ml")
            runs = (hs + as_) if (hs is not None and as_ is not None) else None

            graded = []
            for p in sigs["picks"]:
                if p["market"] == "ML":
                    side = "away" if p["bet"].startswith(g.get("away_abbr", "")) else "home"
                    odds = aml if side == "away" else hml
                    res, pnl = _grade_ml(side, hs, as_, odds or 0)
                elif p["market"] == "TOTAL":
                    bet  = "over" if "OVER" in p["bet"].upper() else "under"
                    odds = g.get("over_odds") if bet == "over" else g.get("under_odds")
                    res, pnl = _grade_total(bet, hs, as_, tot, odds)
                else:
                    res, pnl = "NO RESULT", 0.0
                graded.append({**p, "result": res, "pnl": pnl})

            winner = g.get("home_abbr", "") if (hs or 0) > (as_ or 0) else g.get("away_abbr", "")
            evaluated.append({
                "game": g, "sigs": sigs, "graded": graded,
                "ou_label": _ou_label(g), "runs": runs, "winner": winner,
            })

        pick_entries  = sorted([e for e in evaluated if e["graded"]],
                               key=lambda e: min(p["priority"] for p in e["graded"]))
        avoid_entries = [e for e in evaluated if e["sigs"]["avoid"] and not e["graded"]]
        nosig_entries = [e for e in evaluated if not e["graded"] and not e["sigs"]["avoid"]]
        top_pick      = pick_entries[:1]
        next_picks    = pick_entries[1:6]
        rest_picks    = pick_entries[6:]

        def _add_graded_pick_table(doc, graded_picks: list, sigs: dict):
            """Pick table with result and P&L columns added for prior-day grading."""
            tbl = doc.add_table(rows=1, cols=5)
            tbl.style = "Table Grid"
            tbl.autofit = False
            widths = [1080, 1260, 720, 1080, 5220]   # Signal | Bet | Odds | Result | Reason
            for i, cell in enumerate(tbl.rows[0].cells):
                cell.width = widths[i]
            headers = ["SIGNAL", "BET", "ODDS", "RESULT", "REASON"]
            for cell, hdr in zip(tbl.rows[0].cells, headers):
                _set_cell_bg(cell, "1F3864")
                _cell_para(cell, hdr, bold=True, size_pt=9,
                           color_hex="FFFFFF", align=WD_ALIGN_PARAGRAPH.CENTER)
            for pick in sorted(graded_picks, key=lambda x: x.get("priority", 99)):
                row_cells = tbl.add_row().cells
                result    = pick.get("result", "")
                pick_bg   = ("E2EFDA" if result == "WIN"
                             else "FCE4D6" if result == "LOSS"
                             else "FFF2CC")
                for cell in row_cells:
                    _set_cell_bg(cell, pick_bg)
                _cell_para(row_cells[0], ", ".join(sigs["signals"]),
                           bold=True, size_pt=9, color_hex="1F3864")
                _cell_para(row_cells[1], pick["bet"],
                           bold=True, size_pt=10, color_hex="0D47A1")
                _cell_para(row_cells[2], pick.get("odds", ""),
                           bold=True, size_pt=10, color_hex="1B5E20",
                           align=WD_ALIGN_PARAGRAPH.CENTER)
                result_disp = f"{'✓' if result=='WIN' else '✗' if result=='LOSS' else '—'} {result}  {_pnl_str(pick['pnl'])}"
                _cell_para(row_cells[3], result_disp, bold=True, size_pt=9,
                           color_hex=_result_color(result),
                           align=WD_ALIGN_PARAGRAPH.CENTER)
                reason_short = (pick["reason"][:260] + "…") if len(pick["reason"]) > 260 else pick["reason"]
                _cell_para(row_cells[4], reason_short, size_pt=8, color_hex="333333")
            doc.add_paragraph()

        def _add_score_line(doc, e: dict):
            """Score and O/U outcome line for a completed game."""
            g   = e["game"]
            hs  = g.get("home_score"); as_ = g.get("away_score")
            if hs is None:
                return
            ou  = f"  |  {e['ou_label']}" if e["ou_label"] else ""
            txt = f"Final: {g.get('away_abbr','')} {as_}  –  {g.get('home_abbr','')} {hs}  ({e['winner']} wins){ou}"
            p   = doc.add_paragraph()
            run = p.add_run(txt)
            run.bold = True
            run.font.size = Pt(10)
            run.font.color.rgb = RGBColor(0x1F, 0x38, 0x64)
            p.paragraph_format.space_before = Pt(2)
            p.paragraph_format.space_after  = Pt(2)
            p.paragraph_format.left_indent  = Inches(0.2)

        def _add_odds_line(doc, g: dict):
            hml = g.get("home_ml"); aml = g.get("away_ml"); tot = g.get("total_line")
            if not hml and not aml:
                return
            txt = (f"ML: {g.get('home_abbr','')} {fmt_odds(hml)} / "
                   f"{g.get('away_abbr','')} {fmt_odds(aml)}"
                   + (f"  |  O/U {tot}" if tot else ""))
            p   = doc.add_paragraph()
            run = p.add_run(txt)
            run.font.size = Pt(9)
            run.font.color.rgb = RGBColor(0x55, 0x55, 0x55)
            p.paragraph_format.space_before = Pt(0)
            p.paragraph_format.space_after  = Pt(2)
            p.paragraph_format.left_indent  = Inches(0.2)

        # ── TOP PICK ────────────────────────────────────────────────────
        _add_heading(doc, "🔺  Top Pick  —  Highest Priority Signal", level=2)
        if not top_pick:
            _add_note(doc, "No model signals fired yesterday.", color_hex="C62828")
        else:
            e = top_pick[0]; g = e["game"]
            # show_picks=False: prior report uses _add_graded_pick_table instead
            _add_matchup_block(doc, g, streaks, starters, e["sigs"],
                               show_picks=False)
            _add_score_line(doc, e)
            _add_odds_line(doc, g)
            _add_graded_pick_table(doc, e["graded"], e["sigs"])

        # ── ADDITIONAL PICKS ─────────────────────────────────────────────
        _add_heading(doc, f"📋  Additional Model Selections  ({len(next_picks)})", level=2)
        if not next_picks:
            _add_note(doc, "No additional model selections yesterday.", color_hex="475569")
        else:
            for i, e in enumerate(next_picks, start=2):
                g = e["game"]
                # Use _add_matchup_block for consistent header/weather/streak/starter
                # but suppress its picks table — graded table is added below.
                _add_matchup_block(doc, g, streaks, starters, e["sigs"],
                                   show_picks=False)
                _add_score_line(doc, e)
                _add_odds_line(doc, g)
                _add_graded_pick_table(doc, e["graded"], e["sigs"])

        if rest_picks:
            _add_heading(doc, f"Further Signals  ({len(rest_picks)})", level=2)
            for e in rest_picks:
                g = e["game"]
                _add_matchup_block(doc, g, streaks, starters, e["sigs"],
                                   show_picks=False)
                _add_score_line(doc, e)
                _add_graded_pick_table(doc, e["graded"], e["sigs"])

        # ── BETS TO AVOID ────────────────────────────────────────────────
        _add_heading(doc, f"⛔  Bets to Avoid  ({len(avoid_entries)} flagged)", level=2)
        if not avoid_entries:
            _add_note(doc, "No avoid flags were active yesterday.", color_hex="475569")
        else:
            _add_note(doc, "These games were flagged to AVOID. Outcome confirms whether avoidance was correct.", color_hex="C62828")
            for e in avoid_entries:
                g = e["game"]
                _add_matchup_block(doc, g, streaks, starters, e["sigs"])
                _add_score_line(doc, e)
                _add_odds_line(doc, g)

        # ── NO SIGNAL SLATE ──────────────────────────────────────────────
        _add_heading(doc, f"—  No Signal  ({len(nosig_entries)} games)", level=2)
        if nosig_entries:
            # Results table: Away | Home | Score | O/U | ML
            tbl = doc.add_table(rows=1, cols=5)
            tbl.style = "Table Grid"
            tbl.autofit = False
            widths = [1080, 1080, 2000, 2200, 3000]
            for i, cell in enumerate(tbl.rows[0].cells):
                cell.width = widths[i]
            for cell, hdr in zip(tbl.rows[0].cells,
                                  ["AWAY", "HOME", "FINAL SCORE", "O/U RESULT", "CLOSING ML"]):
                _set_cell_bg(cell, "1F3864")
                _cell_para(cell, hdr, bold=True, size_pt=9,
                           color_hex="FFFFFF", align=WD_ALIGN_PARAGRAPH.CENTER)
            for ri, e in enumerate(nosig_entries):
                g   = e["game"]
                hs  = g.get("home_score"); as_ = g.get("away_score")
                hml = fmt_odds(g.get("home_ml")); aml = fmt_odds(g.get("away_ml"))
                score_txt = (f"{g.get('away_abbr','')} {as_}  –  {g.get('home_abbr','')} {hs}  ({e['winner']} wins)"
                             if hs is not None else "TBD")
                bg = "F5F5F5" if ri % 2 == 0 else "FFFFFF"
                row_cells = tbl.add_row().cells
                for cell in row_cells:
                    _set_cell_bg(cell, bg)
                _cell_para(row_cells[0], g.get("away_abbr", ""), size_pt=10, color_hex="333333")
                _cell_para(row_cells[1], f"{g.get('home_abbr','')} (h)", bold=True, size_pt=10, color_hex="1F3864")
                _cell_para(row_cells[2], score_txt, size_pt=9, color_hex="333333")
                _cell_para(row_cells[3], e["ou_label"] or "—", size_pt=9, color_hex="333333",
                           align=WD_ALIGN_PARAGRAPH.CENTER)
                ml_txt = f"{g.get('home_abbr','')} {hml} / {g.get('away_abbr','')} {aml}"
                _cell_para(row_cells[4], ml_txt, size_pt=9, color_hex="333333")
            doc.add_paragraph()

        # ── MODEL P&L SUMMARY ────────────────────────────────────────────
        _add_heading(doc, "📈  Model P&L Summary", level=2)
        all_signal_entries = top_pick + next_picks + rest_picks

        def _summarise(entries):
            picks = [p for e in entries for p in e["graded"]]
            if not picks: return None
            w = sum(1 for p in picks if p["pnl"] > 0)
            l = sum(1 for p in picks if p["pnl"] < 0)
            pu = sum(1 for p in picks if p["pnl"] == 0)
            return w, l, pu, sum(x["pnl"] for x in picks)

        if not _summarise(all_signal_entries):
            _add_note(doc, "No model signals fired yesterday.", color_hex="C62828")
        else:
            # Summary table
            tbl = doc.add_table(rows=1, cols=5)
            tbl.style = "Table Grid"
            tbl.autofit = False
            widths = [2400, 900, 900, 900, 2160]
            for i, cell in enumerate(tbl.rows[0].cells):
                cell.width = widths[i]
            for cell, hdr in zip(tbl.rows[0].cells, ["CATEGORY", "W", "L", "P", "NET P&L"]):
                _set_cell_bg(cell, "1F3864")
                _cell_para(cell, hdr, bold=True, size_pt=9,
                           color_hex="FFFFFF", align=WD_ALIGN_PARAGRAPH.CENTER)
            for label, entries in [("Top Pick", top_pick),
                                   ("Additional (next 5)", next_picks),
                                   ("All signals", all_signal_entries)]:
                s = _summarise(entries)
                if not s: continue
                w, l, pu, total_pnl = s
                row_cells = tbl.add_row().cells
                bg = "E2EFDA" if total_pnl > 0 else "FCE4D6" if total_pnl < 0 else "FFFFFF"
                for cell in row_cells: _set_cell_bg(cell, bg)
                _cell_para(row_cells[0], label, bold=True, size_pt=9, color_hex="1F3864")
                _cell_para(row_cells[1], str(w), bold=True, size_pt=10, color_hex="006400", align=WD_ALIGN_PARAGRAPH.CENTER)
                _cell_para(row_cells[2], str(l), bold=True, size_pt=10, color_hex="C00000", align=WD_ALIGN_PARAGRAPH.CENTER)
                _cell_para(row_cells[3], str(pu), size_pt=10, color_hex="555555", align=WD_ALIGN_PARAGRAPH.CENTER)
                pnl_disp = f"+{total_pnl:.2f}u" if total_pnl > 0 else f"{total_pnl:.2f}u"
                _cell_para(row_cells[4], pnl_disp, bold=True, size_pt=10,
                           color_hex=_result_color("WIN" if total_pnl > 0 else "LOSS" if total_pnl < 0 else ""),
                           align=WD_ALIGN_PARAGRAPH.CENTER)
            doc.add_paragraph()

        # Slate O/U rate
        ou_games = [e for e in evaluated if e["game"].get("total_line") and e["runs"] is not None]
        if ou_games:
            overs  = sum(1 for e in ou_games if e["runs"] > e["game"]["total_line"])
            unders = sum(1 for e in ou_games if e["runs"] < e["game"]["total_line"])
            pushes = len(ou_games) - overs - unders
            ou_txt = (f"Slate O/U:  {overs} Over / {unders} Under"
                      + (f" / {pushes} Push" if pushes else "")
                      + f"  ({overs/len(ou_games):.0%} over rate)")
            _add_note(doc, ou_txt, italic=False, color_hex="333333")

        # CLV reminder
        doc.add_paragraph()
        _add_note(doc,
            "CLV Reminder: compare your bet prices vs yesterday's closing lines. "
            "Positive CLV (you beat the close) = process is working correctly. "
            "If --compute-movement was not run last night, run it now: "
            "python load_odds.py --compute-movement",
            color_hex="475569")

    # ── CLOSING layout ────────────────────────────────────────────────────
    elif session == "closing":
        _add_heading(doc, f"Closing Confirmation  —  {len(games)} Games", level=2)
        for e in entries:
            _add_matchup_block(doc, e["game"], streaks, starters, e["sigs"],
                               show_movement=True, movement=movement or {})
        _add_note(doc,
            "CLV Note: Your edge is measured against these closing prices. "
            "Run --compute-movement at 11:30 PM for full movement analysis.",
            color_hex="475569")

    # ── Footer note ───────────────────────────────────────────────────────
    doc.add_paragraph()
    _add_note(doc,
        "All bets assume flat-unit sizing. No bet above 2% of bankroll. "
        "Confirm wind and lineup at game time. Lines shown from last odds pull.",
        color_hex="64748b")

    # ── Legal caveat ─────────────────────────────────────────────────────
    doc.add_paragraph()
    caveat_tbl = doc.add_table(rows=1, cols=1)
    caveat_tbl.style = "Table Grid"
    caveat_cell = caveat_tbl.rows[0].cells[0]
    _set_cell_bg(caveat_cell, "FFF3CD")
    caveat_cell._tc.get_or_add_tcPr()

    # Heading line — bold
    heading_p = caveat_cell.paragraphs[0]
    heading_p.clear()
    heading_p.paragraph_format.space_before = Pt(4)
    heading_p.paragraph_format.space_after  = Pt(4)
    heading_p.paragraph_format.left_indent  = Inches(0.1)
    h_run = heading_p.add_run(
        "EDUCATIONAL USE ONLY \u2014 NOT FINANCIAL ADVICE"
    )
    h_run.bold            = True
    h_run.font.size       = Pt(9)
    h_run.font.color.rgb  = RGBColor(0x7B, 0x4F, 0x00)

    # Body lines
    caveat_body = (
        "This brief is produced by a personal statistical model for research "
        "and educational purposes only. Nothing in this output constitutes "
        "financial advice, investment advice, or a recommendation to place any "
        "wager of any kind. Sports betting carries substantial financial risk "
        "and is not appropriate for all individuals.\n"
        "Past signal performance does not guarantee future results. Odds, lines, "
        "and conditions can change materially between the time this brief is "
        "generated and game time. Always verify every line independently with "
        "your bookmaker before placing any bet.\n"
        "You are solely responsible for all betting decisions and any resulting "
        "financial outcomes. Never bet more than you can afford to lose. "
        "If gambling is causing financial or personal harm, contact the National "
        "Problem Gambling Helpline: 1-800-522-4700 or visit ncpgambling.org."
    )
    for line in caveat_body.split("\n"):
        body_p = caveat_cell.add_paragraph()
        body_p.paragraph_format.space_before = Pt(2)
        body_p.paragraph_format.space_after  = Pt(2)
        body_p.paragraph_format.left_indent  = Inches(0.1)
        b_run = body_p.add_run(line)
        b_run.font.size      = Pt(8)
        b_run.font.color.rgb = RGBColor(0x7B, 0x4F, 0x00)

    return doc

# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(
        description="MLB Betting Model — Daily Brief Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            EXAMPLES
            --------
            Prior day report (after 6 AM stats pull):
              python generate_daily_brief.py --session prior

            Morning brief (after 9 AM odds pull):
              python generate_daily_brief.py --session morning

            Early games brief (after 12 PM pull — 1 PM starters):
              python generate_daily_brief.py --session early

            Afternoon brief (after 3:30 PM pull — 4 PM starters):
              python generate_daily_brief.py --session afternoon

            Primary brief (after 5 PM odds pull — evening games):
              python generate_daily_brief.py --session primary

            Closing brief (after 6:30 PM odds pull):
              python generate_daily_brief.py --session closing

            Rerun yesterday's primary brief (recovery):
              python generate_daily_brief.py --session primary --date 2026-03-25 --force

            Dry-run to preview without writing to log:
              python generate_daily_brief.py --session primary --dry-run

            Run with prereq check (recommended for scheduled runs):
              python generate_daily_brief.py --session primary --check-prereqs
        """),
    )
    p.add_argument(
        "--session", required=True,
        choices=["prior", "morning", "early", "afternoon", "primary", "closing", "late"],
        help="Which brief to generate: prior | morning | early | afternoon | primary | closing | late",
    )
    p.add_argument(
        "--date", default=None,
        help="Game date to generate brief for (YYYY-MM-DD). Default: today.",
    )
    p.add_argument(
        "--force", action="store_true",
        help="Overwrite/rerun even if this session's brief was already generated today.",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Print brief to console only. Do NOT write to brief_log or output file.",
    )
    p.add_argument(
        "--output", default=None,
        help="Write brief to this file path (appends). Default: briefs/YYYY-MM-DD_SESSION.txt",
    )
    p.add_argument(
        "--no-file", action="store_true",
        help="Suppress file output. Console only. Overrides --output.",
    )
    p.add_argument(
        "--warn-missing", action="store_true",
        help="Downgrade missing-data exits to warnings. Continue with partial data.",
    )
    p.add_argument(
        "--check-prereqs", action="store_true",
        help="Verify the required odds pull was completed before generating the brief.",
    )
    p.add_argument(
        "--verbose", action="store_true",
        help="Print extra DB diagnostic info during execution.",
    )
    p.add_argument(
        "--docx", action="store_true",
        help="Also write a formatted Word (.docx) brief alongside the .txt file. "
             "Requires python-docx (pip install python-docx). "
             "File saved as briefs/YYYY-MM-DD_SESSION.docx.",
    )
    return p.parse_args()


def main():
    args   = parse_args()
    session = args.session
    today   = args.date or datetime.date.today().isoformat()

    # Validate date
    try:
        datetime.date.fromisoformat(today)
    except ValueError:
        print(f"✗  Invalid --date value: '{today}'. Use YYYY-MM-DD.")
        sys.exit(1)

    print(f"\n{'═'*72}")
    print(f"  MLB Betting Model · Daily Brief · {session.upper()} · {today}")
    print(f"{'═'*72}")

    conn = open_db(DB_PATH)

    # ── Prereq check ─────────────────────────────────────────────────────
    if args.check_prereqs:
        check_prereqs(conn, today, session)

    # ── Duplicate guard ──────────────────────────────────────────────────
    ensure_brief_log(conn)
    if not args.dry_run and not args.force:
        if already_ran(conn, today, session):
            print(f"\n  ⚠  A {session} brief for {today} already exists in brief_log.")
            print(f"     Use --force to regenerate, or --dry-run to preview.\n")
            sys.exit(0)

    # ── Prior day report — uses yesterday, not today ─────────────────────
    if session == "prior":
        yesterday = (datetime.date.fromisoformat(today)
                     - datetime.timedelta(days=1)).isoformat()
        brief_text = build_prior_day_report(conn, yesterday, args.verbose)
        print(brief_text)

        # ── Save txt file ──────────────────────────────────────────────────
        output_file = None
        if not args.no_file and not args.dry_run:
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            output_file = str(OUTPUT_DIR / f"{yesterday}_prior.txt")
            with open(output_file, "w", encoding="utf-8") as fh:
                fh.write(brief_text)
            print(f"\n  ✓ Prior day report saved to: {output_file}")

        # ── Word (.docx) output ────────────────────────────────────────────
        if getattr(args, "docx", False) and not args.dry_run and not args.no_file:
            if not DOCX_AVAILABLE:
                print("\n  ⚠  --docx requested but python-docx is not installed.")
                print("     Run: pip install python-docx")
            else:
                try:
                    # Load completed games so build_docx_brief has data to render
                    prior_games_cur = conn.execute("""
                        SELECT
                            g.game_pk, g.game_date, g.game_start_utc,
                            g.home_score, g.away_score,
                            g.wind_mph, g.wind_direction, g.temp_f, g.sky_condition,
                            th.team_id   AS home_team_id,
                            ta.team_id   AS away_team_id,
                            th.abbreviation AS home_abbr,
                            ta.abbreviation AS away_abbr,
                            v.name          AS venue_name,
                            v.wind_effect, v.wind_note, v.roof_type,
                            v.park_factor_runs, v.orientation_hp,
                            go_ml.home_ml,   go_ml.away_ml,
                            go_tot.total_line, go_tot.over_odds, go_tot.under_odds,
                            NULL AS home_rl, NULL AS away_rl,
                            NULL AS home_rl_odds, NULL AS away_rl_odds
                        FROM   games g
                        JOIN   teams  th     ON th.team_id  = g.home_team_id
                        JOIN   teams  ta     ON ta.team_id  = g.away_team_id
                        LEFT JOIN venues v   ON v.venue_id  = g.venue_id
                        LEFT JOIN v_closing_game_odds go_ml
                               ON go_ml.game_pk  = g.game_pk
                              AND go_ml.market_type  = 'moneyline'
                        LEFT JOIN v_closing_game_odds go_tot
                               ON go_tot.game_pk = g.game_pk
                              AND go_tot.market_type = 'total'
                        WHERE  g.game_date = ?
                          AND  g.game_type = 'R'
                          AND  g.status    = 'Final'
                        ORDER  BY g.game_start_utc, g.game_pk
                    """, (yesterday,))
                    prior_games   = [dict(r) for r in prior_games_cur.fetchall()]
                    prior_team_ids = list(
                        {g["home_team_id"] for g in prior_games} |
                        {g["away_team_id"] for g in prior_games}
                    )
                    prior_streaks = load_streaks(conn, yesterday,
                                                 prior_team_ids, args.verbose)
                    doc = build_docx_brief(
                        "prior", yesterday,
                        prior_games, prior_streaks, {},
                        movement={}
                    )
                    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
                    docx_path = str(OUTPUT_DIR / f"{yesterday}_prior.docx")
                    doc.save(docx_path)
                    print(f"  ✓ Word brief saved to: {docx_path}")
                except Exception as e:
                    print(f"\n  ⚠  Word output failed: {e}")
                    import traceback; traceback.print_exc()

        # ── Log ───────────────────────────────────────────────────────────
        if not args.dry_run:
            log_brief(conn, yesterday, "prior", 0, 0, output_file)
        conn.close()
        print(f"\n  Done.\n")
        return

    # ── Load data — for all forward-looking sessions ──────────────────────
    games = load_games(conn, today, args.verbose)

    if not games:
        print(f"\n  ⚠  No upcoming games found in DB for {today}.")
        print(f"     Possible causes:")
        print(f"     • Stats pull (6 AM) has not run yet")
        print(f"     • Odds pull has not run yet — run: python load_odds.py --pregame --markets game")
        print(f"     • All games for {today} are already marked Final")
        print(f"     • Games loaded as wrong game_type (e.g. 'S' for Spring Training on Opening Day)")
        print(f"       Check: SELECT game_date, game_type, COUNT(*) FROM games")
        print(f"               WHERE game_date='{today}' GROUP BY game_type;")
        print(f"       If type is 'S', re-run load_mlb_stats.py after the MLB API reclassifies (1-2 hrs).")
        print(f"     • Wrong date — check: python check_db.py\n")
        sys.exit(0)

    # ── Session game filtering ────────────────────────────────────────────
    # Design: session = when you pull odds and act, NOT which games start
    # in that hour. Every action session (early/afternoon/primary) shows
    # ALL unplayed games for the date — the full pickable slate.
    #
    # The only games excluded are those that have already started or
    # finished by the time the brief is run. This means:
    #   · An all-afternoon slate (Opening Week) correctly appears in
    #     --session primary because those games haven't started yet.
    #   · West Coast late games (10 PM ET) appear in --session afternoon
    #     because they haven't started at 3:45 PM when you run it.
    #   · A 1 PM game is excluded from --session primary (run at 5:30 PM)
    #     because it already started 4+ hours ago.
    #
    # morning / closing = all games regardless (watch list / confirmation)

    if session in ("early", "afternoon", "primary"):
        now_utc = datetime.datetime.utcnow()

        def game_start_utc_dt(g):
            """Parse game_start_utc to a datetime. Returns None if missing."""
            raw = g.get("game_start_utc") or ""
            if "T" in raw:
                try:
                    # Handle both '2026-03-26T20:10:00' and '2026-03-26T20:10:00Z'
                    return datetime.datetime.fromisoformat(raw.rstrip("Z"))
                except ValueError:
                    pass
            return None

        # Keep games that:
        #   a) have no start time (include — don't exclude on missing data), OR
        #   b) haven't started yet (start_utc > now_utc)
        # Exclude games already in progress or finished (start_utc <= now_utc)
        # Give a 10-minute grace buffer so a game that just tipped off isn't
        # immediately dropped (first pitch ≠ game over).
        GRACE_MINUTES = 10
        grace = datetime.timedelta(minutes=GRACE_MINUTES)

        filtered = [
            g for g in games
            if game_start_utc_dt(g) is None
            or game_start_utc_dt(g) > (now_utc - grace)
        ]

        if not filtered:
            print(f"\n  ℹ  No unplayed games remaining on today's slate for {session} session.")
            print(f"     All {len(games)} game(s) have already started or finished.")
            print(f"     Run --session closing to confirm lines, or check tomorrow's slate.\n")
            conn.close()
            return

        # Informational note if some games were filtered out as already started
        started = len(games) - len(filtered)
        if started > 0:
            started_teams = [
                f"{g['away_abbr']}@{g['home_abbr']}"
                for g in games
                if game_start_utc_dt(g) is not None
                and game_start_utc_dt(g) <= (now_utc - grace)
            ]
            print(f"  ℹ  {started} game(s) already started — excluded from {session} picks:")
            for t in started_teams:
                print(f"       {t}")
            print()

        games = filtered

    # Check for games missing odds — non-fatal, brief runs on games that have odds.
    missing_odds = [g for g in games if g.get("home_ml") is None]
    if missing_odds:
        missing_list = ', '.join(g['away_abbr'] + '@' + g['home_abbr']
                                 for g in missing_odds)
        with_odds = len(games) - len(missing_odds)
        print(f"\n  \u2139  {len(missing_odds)} of {len(games)} game(s) have no odds yet "
              f"\u2014 brief will run on the {with_odds} game(s) with odds.")
        print(f"     Pending odds: {missing_list}")
        print(f"     Re-run load_odds.py when lines are posted to update.\n")

    team_ids = list({g["home_team_id"] for g in games} | {g["away_team_id"] for g in games})
    streaks  = load_streaks(conn, today, team_ids, args.verbose)
    starters = load_starters(conn, today, args.verbose)
    movement = load_line_movement(conn, today, args.verbose) if session == "closing" else {}

    # ── S6 pitcher streak check (monitoring signal) ───────────────────────
    # Runs after streaks are loaded so we can pass S1-active game_pks.
    # S6 needs DB access for rolling streak computation — runs here, not
    # inside evaluate_signals() which is stateless.
    s6_fires = {}
    if S6_AVAILABLE and session in ("early", "afternoon", "primary"):
        try:
            # Determine which games have S1 W7+ active (for double-confirmation flag)
            s1_w7_pks = set()
            for g in games:
                home_streak = streaks.get(g.get("home_team_id"), 0)
                if home_streak >= S6_WIN_STREAK_MIN:
                    s1_w7_pks.add(g["game_pk"])

            s6_fires = check_s6_pitcher_streak(
                conn, today,
                season=int(today[:4]),
                s1_active_game_pks=s1_w7_pks,
            )
            if s6_fires and not args.dry_run:
                for fire in s6_fires.values():
                    log_s6_fire_to_db(conn, fire, game_start_utc=None)
            if args.verbose and s6_fires:
                print(f"\n  [verbose] S6 W≥7 pitcher fade fires today: {len(s6_fires)}")
                for gk, f in s6_fires.items():
                    print(f"           game_pk={gk}  {f['pitcher_name']}  "
                          f"W{f['win_streak']}  bet={f['bet_side']}  "
                          f"stake={f['stake_recommendation']}")
        except Exception as e:
            if args.verbose:
                print(f"\n  [verbose] S6 check failed (non-fatal): {e}")

    # ── Generate brief ───────────────────────────────────────────────────
    if session == "morning":
        brief_text = build_morning_brief(games, streaks, starters, today)
    elif session in ("early", "afternoon", "primary", "late"):
        label = {"early": "EARLY GAMES", "afternoon": "AFTERNOON",
                 "primary": "PRIMARY", "late": "LATE GAMES"}.get(session, "PRIMARY")
        brief_text = build_primary_brief(games, streaks, starters, today,
                                         session_label=label, s6_fires=s6_fires,
                                         conn=conn, session=session)
    else:
        brief_text = build_closing_brief(games, streaks, starters, movement, today)

    # ── Output ───────────────────────────────────────────────────────────
    print(brief_text)

    output_file = None
    if not args.no_file and not args.dry_run:
        if args.output:
            output_file = args.output
        else:
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            output_file = str(OUTPUT_DIR / f"{today}_{session}.txt")

        with open(output_file, "w", encoding="utf-8") as fh:
            fh.write(brief_text)
        print(f"\n  ✓ Brief saved to: {output_file}")

    # ── Word output ──────────────────────────────────────────────────────
    if getattr(args, "docx", False) and not args.dry_run and not args.no_file:
        if not DOCX_AVAILABLE:
            print("\n  ⚠  --docx requested but python-docx is not installed.")
            print("     Run: pip install python-docx")
        else:
            try:
                doc = build_docx_brief(
                    session, today, games, streaks, starters,
                    movement=(movement if session == "closing" else {})
                )
                OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
                docx_path = str(OUTPUT_DIR / f"{today}_{session}.docx")
                doc.save(docx_path)
                print(f"  ✓ Word brief saved to: {docx_path}")
            except Exception as e:
                print(f"\n  ⚠  Word output failed: {e}")
                import traceback; traceback.print_exc()

    # ── Log ──────────────────────────────────────────────────────────────
    if not args.dry_run:
        # Build pick_entries for action sessions so they can be saved
        # to brief_picks for confirmed prior-report grading.
        pick_entries_for_log = None
        if session in ("primary", "early", "afternoon", "late"):
            all_sig = []
            for g in games:
                sigs = evaluate_signals(g, streaks, session)
                if sigs["picks"]:
                    all_sig.append({"game": g, "sigs": sigs})
            all_sig.sort(key=lambda e: min(p["priority"]
                                          for p in e["sigs"]["picks"]))
            pick_entries_for_log = all_sig
        picks_count = sum(
            len(evaluate_signals(g, streaks, session)["picks"]) for g in games
        )
        log_brief(conn, today, session, len(games), picks_count,
                  output_file, pick_entries=pick_entries_for_log)
        if args.verbose:
            print(f"  [verbose] brief_log entry written: {today} / {session} / {picks_count} picks")

    conn.close()
    print(f"\n  Done.\n")


if __name__ == "__main__":
    main()
