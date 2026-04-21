"""
generate_daily_brief.py
=======================
MLB Betting Model — Daily Brief Generator
Reads from mlb_stats.db and outputs the formatted betting brief.

CHANGE LOG (latest first)
──────────────────────────
2026-04-20  Retractable roof removed as a model/brief factor (no AvoidFinding, no
            confidence penalty, no weather-line or prior-report retractable copy).
2026-04-19  Default brief filenames: ``brief-{slate_date}_{ET_run_stamp}_ET[ _prior].txt`` (Windows-safe);
            ``brief_log.output_file`` stores that path; legacy ``{date}_{session}.txt`` names may be archived.
2026-04-19  ``--as-of-time`` accepts full ``YYYY-MM-DD HH:MM`` (ET) or time-only ``HH:MM`` with ``--date``.
2026-04-19  Hybrid session CLI: ``--as-of-time HH:MM`` (ET) or full ``--as-of``;
            non-``prior`` sessions resolve from ``SESSION_WINDOWS`` (``SESSION_PULL_WINDOW``
            unchanged). ``--session`` ignored when a clock is supplied unless ``prior``.
2026-04-19  Fix: closing brief now skips games already past ``game_start_utc``.
            Active signal display and CLV confirmation only shown for unplayed games.
2026-04-19  Fix: removed H3b cross-reference from MV-B reason string.
            H3b is monitor-only and should not appear in other signals'
            reason text. MV-B reason now self-contained (``score_game._eval_mv_b``).
2026-04-17  brief_picks: skip inserts after game start (vs ``now``); cross-session
            dedupe on (game_date, game_pk, signal, market); ``model_version``
            column (default ``legacy``); ``ensure_brief_picks`` before save.
2026-04-17  CLI: --debug-wind prints per-game wind debug (DB row vs dressed
            GameEnvironment) during brief / prior; pair with --verbose for tiers.
2026-04-17  Venue wind suppression no longer emits an AvoidFinding (tier/env gates +
            data_flags only). Legacy avoid=True only for hard avoids or no-signal
            avoid rows; Word brief avoids duplicate AVOID banner when picks exist.
2026-04-17  Prior report: removed duplicate FURTHER SIGNALS block (ranks #7+ only in
            RETROACTIVE); ENV/venue avoids spell out “class” of bet skipped (not one ticket).
2026-04-17  Prior report: NEXT picks use the same ledger box as TOP; AVOID section
            spells out bet-to-skip + counterfactual verdict; bet_ledger unique key
            includes signal_at_time so AVOID rows (stake 0) materialize from
            signal_state; backfill_bet_ledger_from_signal_state runs on prior;
            grade_bet_ledger scores avoids as good_avoid / bad_avoid / push_avoid.
2026-04-17  Scoring function implemented. score_game() replaces evaluate_signals().
            All signal logic consolidated into per-signal evaluator functions in
            batch/pipeline/score_game.py. Three-dimension tier decision: signal_strength
            × env_ceiling × data_completeness. LHP_FADE signal added (replaces NF4 as
            primary formulation; NF4 retained as backward-compat alias). Hostile
            environment detection added. Avoid evaluation separated from signal evaluation.
            ScoredGame output dataclass added. enrich_game(conn, game, starters) is the
            integration point (starter inject + dress); evaluate_signals maps ScoredGame
            to the legacy dict and attaches output_tier / tier_basis / stake_multiplier.
2026-04-17  Ops: --sync-bet-ledger-only (no brief) + run_pipeline job_type bet_ledger_sync
            for recurring T−30 bet_ledger materialization without re-running briefs.
2026-04-17  Research: persist AVOID calls into brief_picks (pick_rank=0, signal='AVOID')
            so they can be tracked/grated later without generating bets.
2026-04-17  Fix starters column name: players.throws (was throw_hand). Update
            load_starters() SELECT + mapping, and enrich_game_with_starters().
2026-04-16  Finding 5 validated: NF4 implied prob gate confirmed as 60–67%.
            55–60% band shows only 5.5pp edge (not significant). Gate constants
            unchanged (already 0.60/0.67). Comments, signal reason, and constant
            block updated to document the band validation and independence from
            MV-F (wind-in + strong LHP co-occur too rarely to stack).
2026-04-16  NF4 signal added — Home Fav vs Strong LHP. Monitoring status,
            half-stake. Constants: NF4_HOME_IMP_LOW/HIGH, NF4_SP_ERA_MAX,
            NF4_OPS_MIN, NF4_MONTHS_OK. load_starters() extended to pull
            throws and team_rolling_ops. enrich_game_with_starters()
            helper injects these into game dict before signal evaluation.
            Priority order: S1+H2=1, MV-F=2, NF4=3, MV-B=4, S1=5, H3b=6.
2026-04-13 22:15 ET  Default DB from get_db_path(); repo root on sys.path for core.* imports.
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

--debug-wind            For each game, print wind classification debug to stdout:
                        raw DB wind_* / roof / park vs dressed wind_dir_label,
                        wind_in/out, suppression, env_ceiling (see evaluate_signals).

DOCX OUTPUT (DEFAULT)
--------------------
This script attempts to write a formatted Word (.docx) brief alongside the .txt
file by default (no flag required). Requires: pip install python-docx
File saved under outputs/briefs/ (default name: brief-SLATE-DATE_RUN-STAMP_ET.docx).

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

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from core.db.connection import connect as db_connect, get_db_path

# ── Optional .env support ────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    # Load in a stable order; avoids relying on "current working directory".
    load_dotenv(os.path.join(_REPO_ROOT, "config", ".env"), override=False)
    load_dotenv(os.path.join(_REPO_ROOT, ".env"), override=False)
    load_dotenv(override=False)  # fallback: cwd / parent-chain
except ImportError:
    pass

# ── ET timezone helper ────────────────────────────────────────────────────
try:
    from zoneinfo import ZoneInfo as _ZI
    _ET = _ZI("America/New_York")
except Exception:
    # Windows tzdata fallback + Python < 3.9 — fixed UTC-4 (EDT, covers MLB Apr-Oct season)
    _ET = datetime.timezone(datetime.timedelta(hours=-4))


def _now_et(override: datetime.datetime | None = None) -> datetime.datetime:
    """Current datetime in US/Eastern. Use for all user-facing timestamps."""
    if override is not None:
        return override
    return datetime.datetime.now(tz=_ET)


def _game_start_utc_dt(g: dict) -> datetime.datetime | None:
    """Parse game_start_utc to naive UTC datetime. None if missing or unparseable."""
    raw = g.get("game_start_utc") or ""
    if "T" not in raw:
        return None
    try:
        return datetime.datetime.fromisoformat(raw.rstrip("Z"))
    except ValueError:
        return None


def _game_started_or_in_progress_for_closing(game: dict, now: datetime.datetime) -> bool:
    """
    True if first pitch time is known and ``now`` is at or after start (UTC).
    Closing brief skips these games so active-signal lines are not shown for
    games already underway.
    """
    gst = _game_start_utc_dt(game)
    if gst is None:
        return False
    gst_utc = (
        gst.replace(tzinfo=datetime.timezone.utc)
        if gst.tzinfo is None
        else gst.astimezone(datetime.timezone.utc)
    )
    now_utc = now.astimezone(datetime.timezone.utc)
    return now_utc >= gst_utc


def _game_already_started_for_brief_picks(game: dict, now: datetime.datetime) -> bool:
    """
    True if game_start_utc is known and ``now`` is after first pitch (UTC).
    Used to skip brief_picks rows for retroactive/late-session writes.
    """
    gst = _game_start_utc_dt(game)
    if gst is None:
        return False
    gst_utc = (
        gst.replace(tzinfo=datetime.timezone.utc)
        if gst.tzinfo is None
        else gst.astimezone(datetime.timezone.utc)
    )
    now_utc = now.astimezone(datetime.timezone.utc)
    return now_utc > gst_utc


def _brief_pick_exists_cross_session(
    conn: sqlite3.Connection,
    game_date: str,
    game_pk: int,
    signal: str,
    market: str,
) -> bool:
    """True if any session already recorded this game/signal/market for game_date."""
    try:
        row = conn.execute(
            """
            SELECT 1 FROM brief_picks
            WHERE game_pk = ? AND game_date = ? AND signal = ? AND market = ?
            LIMIT 1
            """,
            (game_pk, game_date, signal, market),
        ).fetchone()
        return row is not None
    except Exception:
        return False


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


def build_docx_from_text(session: str, game_date: str, brief_text: str) -> "Document":
    """
    Render the exact same content as the .txt brief into a Word document.
    This keeps brief logic single-sourced in the text builders, while restoring
    the high-level Word formatting (title + section headers + matchup emphasis).
    """
    doc = Document()
    text = brief_text or ""

    # Base style (match older docs: default font, compact spacing)
    try:
        style = doc.styles["Normal"]
        style.font.size = Pt(9)
    except Exception:
        pass

    def _add_line(line: str, *, size_pt: float = 9, bold: bool | None = None, align=None, color_hex: str | None = None) -> None:
        p = doc.add_paragraph()
        if align is not None:
            try:
                p.alignment = align
            except Exception:
                pass
        r = p.add_run(line)
        try:
            r.font.size = Pt(size_pt)
        except Exception:
            pass
        if bold is not None:
            try:
                r.bold = bool(bold)
            except Exception:
                pass
        if color_hex:
            try:
                r.font.color.rgb = RGBColor(int(color_hex[0:2], 16), int(color_hex[2:4], 16), int(color_hex[4:6], 16))
            except Exception:
                pass
        try:
            p.paragraph_format.space_before = Pt(0)
            p.paragraph_format.space_after = Pt(0)
        except Exception:
            pass

    # Heuristic formatting based on the existing text layout.
    title_written = False
    for raw in text.splitlines():
        line = raw.rstrip("\n")
        s = line.strip()

        # Keep blank lines as blank paragraphs (for spacing consistency).
        if not s:
            doc.add_paragraph("")
            continue

        # Banner / separators: keep but de-emphasize.
        if set(s) <= {"═"} or set(s) <= {"─"}:
            _add_line(line, size_pt=8, color_hex="94A3B8")  # slate-300
            continue

        # Title line: first non-separator line.
        if not title_written and ("MLB" in s and ("BRIEF" in s or "REPORT" in s)):
            title_written = True
            _add_line(s, size_pt=16, bold=True, align=WD_ALIGN_PARAGRAPH.CENTER)
            continue

        # Section headings (Top Pick / Avoid / No Signal / Ledger Summary etc.)
        if any(k in s.upper() for k in ("TOP PICK", "ADDITIONAL MODEL SELECTIONS", "BETS TO AVOID", "NO SIGNAL", "BET LEDGER SUMMARY", "SIGNAL TRACKER", "S6 PITCHER")):
            _add_line(s, size_pt=12, bold=True)
            continue

        # Ledger / pick boxes (same visual weight as TOP pick)
        if s.startswith("┌") or s.startswith("│") or s.startswith("└"):
            _add_line(s, size_pt=9, bold=True)
            continue

        # Matchup lines: "XXX  vs  YYY (h)".
        if " vs " in s and "[" in s and "]" in s and "ET" in s:
            _add_line(s, size_pt=11, bold=True)
            continue

        # Emphasize avoid callouts.
        if s.upper().startswith("⛔") or s.upper().startswith("AVOID:") or "⛔ AVOID" in s.upper():
            _add_line(s, size_pt=9, bold=True, color_hex="991B1B")  # red-800
            continue

        # Default line.
        _add_line(s, size_pt=9)

    return doc

# ── DB location (env / config/.env / cwd fallback via get_db_path) ───────
DB_PATH = Path(get_db_path())

# When False, any persistence helpers (signal_state / bet_ledger) must no-op.
PERSIST_WRITES = True

# ── Output directory for saved briefs ──────────────────────────────────────
# Standard location across the repo: <repo root>/outputs/briefs
OUTPUT_DIR = Path(_REPO_ROOT) / "outputs" / "briefs"


def _default_brief_file_stem(
    game_date: str,
    now_et: datetime.datetime,
    *,
    is_prior: bool,
) -> str:
    """
    Windows-safe default filename stem (no extension).
    Pattern: brief-{slate_date}_{run_yyyymmdd_hhmmss}_ET[_prior]
    - slate_date: YYYY-MM-DD the brief is for (``today`` for forward briefs, ``yesterday`` for prior report).
    - run_*: generation instant in America/New_York (unique across intraday runs).
    Stored in brief_log.output_file as the full path to the .txt file.
    """
    wall = now_et.astimezone(_ET) if now_et.tzinfo else now_et.replace(tzinfo=_ET)
    wall = wall.replace(microsecond=0)
    run_stamp = wall.strftime("%Y%m%d_%H%M%S")
    base = f"brief-{game_date}_{run_stamp}_ET"
    if is_prior:
        return f"{base}_prior"
    return base


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

# ── Finding 4 / Finding 5: Home Fav vs Strong LHP (NF4) ─────────────────
# 2025 full-season regression (n=4,856 enriched game rows).
# Finding 4: high-OPS home teams (rolling OPS ≥ 0.736) vs strong LHP
#   (rolling 5-start ERA ≤ 3.04) win only 36.4% while priced at 62.6%.
#   Edge = +26.2pp, z = −3.11, p<.01.
# Finding 5 (band validation): the 60–67% implied band is where the signal
#   lives. z = −3.33, p<.01, n=34, win 35.3% vs 62.9% implied, edge +27.6pp.
#   The 55–60% band showed only 5.5pp edge (not significant) — confirmed
#   that moderate home favs do NOT show meaningful overpricing here.
#   The 60–67% gate (approximately −150 to −200 ML) is the validated range.
# Signal fires: fade home team (bet away ML).
# Independent of MV-F — wind-in and strong LHP co-occur too rarely to stack.
# Monitoring status — half-stake until N ≥ 50 live fires.
# September excluded: only month with inverted edge (66.7% win rate).
NF4_HOME_IMP_LOW   = 0.60   # validated lower bound (60%) — 55-60% not significant
NF4_HOME_IMP_HIGH  = 0.67   # validated upper bound (67%) — approx -150 to -200 ML
NF4_SP_ERA_MAX     = 3.04   # strong SP: rolling 5-start ERA ≤ p33 (2025 tertile)
NF4_OPS_MIN        = 0.736  # high-OPS home team: rolling OPS ≥ median (Finding 4)
NF4_MONTHS_OK      = {4, 5, 6, 7, 8}   # April–August only; September excluded

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
    # Added 2026-04-17: confirmed HIGH wind_effect in venues table,
    # PF >= 98, appeared in 2026 wind-out qualifying games with no H3b fire
    "Citizens Bank Park",      # Philadelphia Phillies — HIGH, PF 104
    "Target Field",            # Minnesota Twins — HIGH, PF 99
    "Rate Field",              # Chicago White Sox — HIGH, PF 102
}

# ── Session → expected pull window (for --check-prereqs) ──────────────────
SESSION_PULL_WINDOW = {
    "morning": ("06:00", "09:30"),   # after 9 AM odds pull
    "primary": ("09:30", "17:30"),   # after 5 PM odds pull
    "closing": ("17:30", "23:59"),   # after 6:30 PM odds pull
    "late":    ("20:00", "23:59"),   # after ~8 PM late-games pull
}

# ET wall-clock → session for hybrid mode (--as-of-time / full --as-of).
# Half-open minute ranges [start, end) on a 0–1440 minute-of-day axis. Does not alter
# SESSION_PULL_WINDOW (used by --check-prereqs). Boundaries align with simulate_day /
# operator schedule for early/afternoon/primary/closing/late.
_SESSION_WINDOW_RANGES_ET: tuple[tuple[int, int, str], ...] = (
    (0, 570, "morning"),       # 00:00–09:30
    (570, 735, "early"),       # 09:30–12:15
    (735, 945, "afternoon"),   # 12:15–15:45
    (945, 1125, "primary"),    # 15:45–18:45 (incl. 17:30 primary pull window)
    (1125, 1215, "closing"),   # 18:45–20:15
    (1215, 1440, "late"),      # 20:15–24:00
)
SESSION_WINDOWS = tuple(
    (f"{a // 60:02d}:{a % 60:02d}", f"{b // 60:02d}:{b % 60:02d}", s)
    for a, b, s in _SESSION_WINDOW_RANGES_ET
)


def _minute_of_day_et(dt: datetime.datetime) -> int:
    """Minute index 0..1439 in America/New_York."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_ET)
    else:
        dt = dt.astimezone(_ET)
    return dt.hour * 60 + dt.minute


def _session_from_et_datetime(dt: datetime.datetime) -> str:
    """Map an ET instant to a forward session using SESSION_WINDOWS ranges."""
    m = _minute_of_day_et(dt)
    for lo, hi, sess in _SESSION_WINDOW_RANGES_ET:
        if lo <= m < hi:
            return sess
    return "late"


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


def print_wind_debug_for_game(game: dict, fdg) -> None:
    """
    Print DB wind fields vs dressed ``FullyDressedGame.environment`` to stdout.
    Used with ``--debug-wind`` to trace OUT/IN/CROSS, suppression, and env_ceiling.
    """
    try:
        env = fdg.environment
        ids = fdg.identifiers
    except Exception as e:
        print(f"\n  [debug-wind] (could not read dressed game: {e})\n")
        return
    gpk = game.get("game_pk")
    ab = f"{ids.away_team_abbr}@{ids.home_team_abbr}"
    raw_dir = game.get("wind_direction")
    brief_lbl = wind_direction_label(str(raw_dir or ""))
    mph = game.get("wind_mph")
    try:
        mph_f = float(mph) if mph is not None else None
    except (TypeError, ValueError):
        mph_f = None
    mph_gate = 10.0
    mph_ok = mph_f is not None and mph_f >= mph_gate
    print("\n  [debug-wind] ───────────────────────────────────────────────────────────")
    print(f"  [debug-wind] game_pk={gpk}  {ab}  {ids.venue_name or ''}")
    print(
        f"  [debug-wind] DB row: wind_mph={mph!r}  temp_f={game.get('temp_f')!r}  "
        f"sky={game.get('sky_condition')!r}"
    )
    print(
        f"  [debug-wind] DB row: wind_direction(raw)={raw_dir!r}  "
        f"wind_effect={game.get('wind_effect')!r}  "
        f"wind_source={game.get('wind_source')!r}"
    )
    print(
        f"  [debug-wind] DB row: roof_type={game.get('roof_type')!r}  "
        f"orientation_hp={game.get('orientation_hp')!r}  "
        f"park_factor_runs={game.get('park_factor_runs')!r}"
    )
    print(
        f"  [debug-wind] brief.wind_direction_label(raw) -> {brief_lbl!r}  "
        f"(mph>={mph_gate} for in/out gates: {mph_ok})"
    )
    print(
        f"  [debug-wind] dressed: wind_dir_label={env.wind_dir_label!r}  "
        f"wind_in={env.wind_in}  wind_out={env.wind_out}"
    )
    print(
        f"  [debug-wind] dressed: is_wind_suppressed={env.is_wind_suppressed}  "
        f"env_ceiling={env.env_ceiling!r}  h3b_eligible={env.h3b_eligible}"
    )
    print(
        f"  [debug-wind] dressed: is_retractable={env.is_retractable}  "
        f"roof_status_known={env.roof_status_known}  wind_source={env.wind_source!r}"
    )
    if brief_lbl != env.wind_dir_label:
        print(
            f"  [debug-wind] NOTE: brief label {brief_lbl!r} != dressed {env.wind_dir_label!r} "
            f"(fully_dressed_game uses its own normalizer on the same row)."
        )
    print("  [debug-wind] ───────────────────────────────────────────────────────────\n")


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
        print("   Set MLB_DB_PATH or add MLB_DB_PATH=... to config/.env")
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
        "early": "pregame",
        "afternoon": "pregame",
        "primary": "pregame",
        "closing": "pregame",
        "late": "pregame",
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


def record_daily_pnl(conn: sqlite3.Connection, rows: list,
                     now: datetime.datetime | None = None) -> None:
    """Insert paper-trading results for a game date. Idempotent on game_date+game_pk+signal."""
    if not rows:
        return
    if now is None:
        now = _now_et()
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
            now.strftime("%Y-%m-%d %H:%M ET"),
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
    Migrates existing tables: total_line, model_version."""
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
            model_version TEXT DEFAULT 'legacy',
            UNIQUE (game_date, session, game_pk, signal)
        )
    """)
    conn.commit()
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(brief_picks)").fetchall()]
        if cols:
            if "total_line" not in cols:
                conn.execute("ALTER TABLE brief_picks ADD COLUMN total_line REAL")
            if "model_version" not in cols:
                conn.execute(
                    "ALTER TABLE brief_picks ADD COLUMN model_version TEXT DEFAULT 'legacy'"
                )
            conn.commit()
    except Exception:
        pass


def ensure_signal_state(conn: sqlite3.Connection) -> None:
    """Create signal_state table if it does not exist (append-only signal ledger)."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signal_state (
            id          INTEGER PRIMARY KEY,
            game_date   TEXT,
            game_pk     INTEGER,
            market_type TEXT,     -- 'moneyline','spread','total'
            signal_type TEXT,     -- 'top','next','avoid'
            bet         TEXT,
            odds        INTEGER,
            session     TEXT,     -- morning, early, primary, etc.
            recorded_at TEXT
        )
    """)
    conn.commit()


def ensure_bet_ledger(conn: sqlite3.Connection) -> None:
    """Create bet_ledger table if it does not exist and enforce idempotency."""
    if not PERSIST_WRITES:
        return
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bet_ledger (
            id            INTEGER PRIMARY KEY,
            game_date     TEXT,
            game_pk       INTEGER,
            market_type   TEXT,
            bet           TEXT,
            odds_taken    INTEGER,
            stake_units   REAL,
            signal_at_time TEXT,  -- 'top','next','avoid'
            session       TEXT,
            placed_at     TEXT,
            result        TEXT,   -- 'win','loss','push'
            pnl_units     REAL
        )
    """)
    # Allow top / next / avoid on the same (game_pk, market_type) as separate rows.
    try:
        conn.execute("DROP INDEX IF EXISTS idx_bet_ledger_game_market")
    except Exception:
        pass
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_bet_ledger_game_market_signal
        ON bet_ledger (game_pk, market_type, IFNULL(signal_at_time, ''))
    """)
    conn.commit()


def save_signal_state(conn: sqlite3.Connection, game_date: str, session: str,
                      top_entry: dict | None, next_entries: list,
                      avoid_entries: list, now: datetime.datetime | None = None) -> None:
    """Persist TOP / NEXT (up to 5) / AVOID signals into signal_state (append-only)."""
    if not PERSIST_WRITES:
        return
    if conn is None or not session:
        return
    if now is None:
        now = _now_et()
    recorded_at = now.strftime("%Y-%m-%d %H:%M ET")

    try:
        ensure_signal_state(conn)
    except Exception:
        return

    def _market_type_from_market(m: str | None) -> str | None:
        m = (m or "").upper().strip()
        if m in ("ML", "MONEYLINE"):
            return "moneyline"
        if m in ("TOTAL", "OU", "O/U"):
            return "total"
        if m in ("RL", "RUNLINE", "SPREAD"):
            return "spread"
        return None

    def _best_pick_fields(entry: dict) -> tuple[str | None, str | None, int | None]:
        try:
            p = sorted(entry["sigs"]["picks"], key=lambda x: x["priority"])[0]
            bet = p.get("bet")
            market_type = _market_type_from_market(p.get("market"))
            odds = _parse_odds(p.get("odds", ""))
            return market_type, bet, odds
        except Exception:
            return None, None, None

    def _avoid_fields(entry: dict) -> tuple[str | None, str | None]:
        """
        Return (market_type, bet_text) for avoid rows.
        Keep it descriptive and non-structured (no forced bet schema).
        """
        g = (entry.get("game") or {})
        sigs = (entry.get("sigs") or {})
        reason = (sigs.get("avoid_reason") or "").strip()
        reason_u = reason.upper()

        home = (g.get("home_abbr") or "").strip()
        away = (g.get("away_abbr") or "").strip()
        total_line = g.get("total_line")

        market_type = None
        bet_text = None

        # Heuristics based on existing avoid_reason text patterns.
        if (
            "WIND SIGNALS SUPPRESSED" in reason_u
            or ("SUPPRESSED" in reason_u and "WEATHER" in reason_u)
            or "NO WEATHER EDGE" in reason_u
        ):
            market_type = "environment"
            bet_text = "Weather-signal class (no single ticket) — see brief reason"
        elif "AVOID HOME ML" in reason_u and home:
            market_type = "moneyline"
            bet_text = f"Avoid {home} ML"
        elif "AVOID AWAY ML" in reason_u and away:
            market_type = "moneyline"
            bet_text = f"Avoid {away} ML"
        elif "DO NOT BET OVER" in reason_u and total_line is not None:
            market_type = "total"
            bet_text = f"Avoid OVER {total_line}"
        elif "DO NOT BET UNDER" in reason_u and total_line is not None:
            market_type = "total"
            bet_text = f"Avoid UNDER {total_line}"
        elif "OVER" in reason_u and "DO NOT BET" in reason_u and total_line is not None:
            market_type = "total"
            bet_text = f"Avoid OVER {total_line}"
        elif "UNDER" in reason_u and "DO NOT BET" in reason_u and total_line is not None:
            market_type = "total"
            bet_text = f"Avoid UNDER {total_line}"
        elif bet_text is None and "ML" in reason_u:
            market_type = "moneyline"
            bet_text = f"Avoid {home} ML" if home else "Avoid ML"
        elif bet_text is None and (
            "TOTAL" in reason_u or "O/U" in reason_u or "OVER" in reason_u or "UNDER" in reason_u
        ):
            market_type = "total"
            bet_text = f"Avoid total" if total_line is None else f"Avoid total ({total_line})"

        # Fallback: descriptive text to make it traceable.
        if not bet_text:
            bet_text = ("Avoid: " + reason) if reason else "Avoid"

        return market_type, bet_text

    rows = []
    if top_entry is not None:
        g = (top_entry.get("game") or {})
        market_type, bet, odds = _best_pick_fields(top_entry)
        if market_type is not None:
            rows.append((game_date, g.get("game_pk"), market_type, "top", bet, odds, session, recorded_at))

    for ne in (next_entries or []):
        g = (ne.get("game") or {})
        market_type, bet, odds = _best_pick_fields(ne)
        if market_type is not None:
            rows.append((game_date, g.get("game_pk"), market_type, "next", bet, odds, session, recorded_at))

    for e in (avoid_entries or []):
        g = (e.get("game") or {})
        mt, bt = _avoid_fields(e)
        rows.append((game_date, g.get("game_pk"), mt, "avoid", bt, None, session, recorded_at))

    if not rows:
        return

    try:
        conn.executemany("""
            INSERT INTO signal_state
                (game_date, game_pk, market_type, signal_type, bet, odds, session, recorded_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, rows)
        conn.commit()
    except Exception:
        pass


def _parse_recorded_at_et_ledger(s: str | None) -> datetime.datetime | None:
    if not s:
        return None
    raw = str(s).strip()
    for fmt in ("%Y-%m-%d %H:%M ET", "%Y-%m-%d %I:%M %p ET"):
        try:
            parsed = datetime.datetime.strptime(raw, fmt)
            return parsed.replace(tzinfo=_ET)
        except Exception:
            continue
    return None


def _ledger_latest_from_signal_rows(
    sig_rows: list,
    start_by_pk: dict[int, datetime.datetime],
    now_et: datetime.datetime,
    *,
    pregame_window_only: bool,
) -> dict[tuple[int, str, str], tuple[datetime.datetime, sqlite3.Row]]:
    """
    Latest signal_state row per (game_pk, market_type, signal_type).
    When pregame_window_only=True, only rows inside [start−30m, start) apply.
    """
    latest: dict[tuple[int, str, str], tuple[datetime.datetime, sqlite3.Row]] = {}
    for r in sig_rows:
        gpk = r["game_pk"]
        mt = r["market_type"]
        st = (r["signal_type"] or "").strip()
        if gpk is None or not mt or st not in ("top", "next", "avoid"):
            continue
        start_et = start_by_pk.get(int(gpk))
        if start_et is None:
            continue
        if pregame_window_only:
            window_start = start_et - datetime.timedelta(minutes=30)
            if not (window_start <= now_et < start_et):
                continue

        rec_dt = _parse_recorded_at_et_ledger(r["recorded_at"])
        if rec_dt is None:
            continue
        if rec_dt > now_et:
            continue

        key = (int(gpk), str(mt), st)
        prev = latest.get(key)
        if prev is None or rec_dt > prev[0]:
            latest[key] = (rec_dt, r)
    return latest


def _insert_bet_ledger_from_latest(
    conn: sqlite3.Connection,
    game_date: str,
    latest: dict[tuple[int, str, str], tuple[datetime.datetime, sqlite3.Row]],
) -> int:
    inserted = 0
    for (_gpk, _mt, _st), (_dt, r) in latest.items():
        sig_type = (r["signal_type"] or "").strip()
        if sig_type not in ("top", "next", "avoid"):
            continue
        stake = 0.0 if sig_type == "avoid" else 1.0
        odds_val = None if sig_type == "avoid" else r["odds"]
        try:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO bet_ledger
                    (game_date, game_pk, market_type, bet, odds_taken, stake_units,
                     signal_at_time, session, placed_at, result, pnl_units)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    game_date,
                    int(r["game_pk"]),
                    r["market_type"],
                    r["bet"],
                    odds_val,
                    stake,
                    sig_type,
                    r["session"],
                    r["recorded_at"],
                    None,
                    None,
                ),
            )
            if getattr(cur, "rowcount", 0) == 1:
                inserted += 1
        except sqlite3.OperationalError:
            continue
    return inserted


def generate_bets_from_signal_state(conn: sqlite3.Connection, game_date: str,
                                    now: datetime.datetime | None = None) -> int:
    """
    Create bet_ledger rows from signal_state in a rolling pregame window.

    A row is created when:
        game_start_utc - 30 minutes <= current_time < game_start_utc

    Selection:
      - Latest signal_state row per (game_pk, market_type, signal_type).
      - signal_type in ('top','next','avoid'). Avoid rows use stake_units=0.

    Idempotency:
      - UNIQUE (game_pk, market_type, signal_at_time) + INSERT OR IGNORE.

    Returns number of rows inserted.
    """
    if conn is None:
        return 0
    if not PERSIST_WRITES:
        return 0
    if now is None:
        now = _now_et()
    try:
        now_et = now if getattr(now, "tzinfo", None) else now.replace(tzinfo=_ET)
        now_et = now_et.astimezone(_ET)
    except Exception:
        now_et = _now_et()

    try:
        ensure_bet_ledger(conn)
    except Exception:
        return 0

    try:
        sig_rows = conn.execute(
            """
            SELECT
                game_pk, market_type, signal_type, bet, odds, session, recorded_at
            FROM signal_state
            WHERE game_date = ?
              AND game_pk IS NOT NULL
              AND market_type IS NOT NULL
              AND recorded_at IS NOT NULL
            """,
            (game_date,),
        ).fetchall()
    except sqlite3.OperationalError:
        return 0

    if not sig_rows:
        return 0

    game_pks = sorted({r["game_pk"] for r in sig_rows if r["game_pk"] is not None})
    if not game_pks:
        return 0
    placeholders = ",".join("?" * len(game_pks))
    try:
        g_rows = conn.execute(
            f"SELECT game_pk, game_start_utc FROM games WHERE game_pk IN ({placeholders})",
            tuple(game_pks),
        ).fetchall()
    except sqlite3.OperationalError:
        return 0

    start_by_pk: dict[int, datetime.datetime] = {}
    for r in g_rows:
        raw = r["game_start_utc"] or ""
        if "T" not in raw:
            continue
        try:
            utc_dt = datetime.datetime.fromisoformat(raw.rstrip("Z")).replace(
                tzinfo=datetime.timezone.utc
            )
            start_by_pk[int(r["game_pk"])] = utc_dt.astimezone(_ET)
        except Exception:
            continue

    latest = _ledger_latest_from_signal_rows(
        sig_rows, start_by_pk, now_et, pregame_window_only=True,
    )
    if not latest:
        return 0

    inserted = _insert_bet_ledger_from_latest(conn, game_date, latest)
    if inserted:
        try:
            conn.commit()
        except Exception:
            pass
    return inserted


def backfill_bet_ledger_from_signal_state(conn: sqlite3.Connection, game_date: str,
                                          now: datetime.datetime | None = None) -> int:
    """
    Materialize all top/next/avoid rows from signal_state for game_date into bet_ledger
    (no pregame window). Idempotent. Used by prior-day report so ledger matches
    archived signals including avoids.
    """
    if conn is None:
        return 0
    if now is None:
        now = _now_et()
    try:
        now_et = now if getattr(now, "tzinfo", None) else now.replace(tzinfo=_ET)
        now_et = now_et.astimezone(_ET)
    except Exception:
        now_et = _now_et()

    try:
        ensure_bet_ledger(conn)
    except Exception:
        return 0

    try:
        sig_rows = conn.execute(
            """
            SELECT
                game_pk, market_type, signal_type, bet, odds, session, recorded_at
            FROM signal_state
            WHERE game_date = ?
              AND game_pk IS NOT NULL
              AND market_type IS NOT NULL
              AND recorded_at IS NOT NULL
            """,
            (game_date,),
        ).fetchall()
    except sqlite3.OperationalError:
        return 0

    if not sig_rows:
        return 0

    game_pks = sorted({r["game_pk"] for r in sig_rows if r["game_pk"] is not None})
    placeholders = ",".join("?" * len(game_pks))
    try:
        g_rows = conn.execute(
            f"SELECT game_pk, game_start_utc FROM games WHERE game_pk IN ({placeholders})",
            tuple(game_pks),
        ).fetchall()
    except sqlite3.OperationalError:
        return 0

    start_by_pk: dict[int, datetime.datetime] = {}
    for r in g_rows:
        raw = r["game_start_utc"] or ""
        if "T" not in raw:
            continue
        try:
            utc_dt = datetime.datetime.fromisoformat(raw.rstrip("Z")).replace(
                tzinfo=datetime.timezone.utc
            )
            start_by_pk[int(r["game_pk"])] = utc_dt.astimezone(_ET)
        except Exception:
            continue

    latest = _ledger_latest_from_signal_rows(
        sig_rows, start_by_pk, now_et, pregame_window_only=False,
    )
    if not latest:
        return 0

    inserted = _insert_bet_ledger_from_latest(conn, game_date, latest)
    if inserted:
        try:
            conn.commit()
        except Exception:
            pass
    return inserted


def strip_avoid_bet_label(bet_text: str) -> str:
    """Strip leading 'Avoid:' / 'Avoid ' so ML/total parsers can grade counterfactuals."""
    s = (bet_text or "").strip()
    su = s.upper()
    if su.startswith("AVOID "):
        return s[6:].strip()
    if su.startswith("AVOID:"):
        return s[6:].strip()
    return s


def avoid_entry_bet_fields(entry: dict) -> tuple[str, str, float | None]:
    """Return (market, bet_label, total_line) for an AVOID entry (brief / prior display)."""
    g = (entry.get("game") or {})
    sigs = (entry.get("sigs") or {})
    reason = (sigs.get("avoid_reason") or "").strip()
    reason_u = reason.upper()
    home = (g.get("home_abbr") or "").strip()
    away = (g.get("away_abbr") or "").strip()
    total_line = g.get("total_line")
    venue = (g.get("venue_name") or "").strip()

    market = "OTHER"
    bet_text = None

    # Venue / environment — not a single ML or O/U ticket (wind-edge class warning).
    if (
        "WIND SIGNALS SUPPRESSED" in reason_u
        or ("SUPPRESSED" in reason_u and "WEATHER" in reason_u)
        or "NO WEATHER EDGE" in reason_u
    ):
        market = "ENV"
        where = f" ({venue})" if venue else ""
        bet_text = (
            f"Weather-driven model plays only (wind/MV-B/H3b-style edges) — "
            f"no single numbered bet{where}; venue treated as non-actionable for weather signals"
        )
    if bet_text is None:
        if "AVOID HOME ML" in reason_u and home:
            market = "ML"
            bet_text = f"Avoid {home} ML"
        elif "AVOID AWAY ML" in reason_u and away:
            market = "ML"
            bet_text = f"Avoid {away} ML"
        elif ("DO NOT BET OVER" in reason_u or ("OVER" in reason_u and "DO NOT BET" in reason_u)) and total_line is not None:
            market = "TOTAL"
            bet_text = f"Avoid OVER {total_line}"
        elif ("DO NOT BET UNDER" in reason_u or ("UNDER" in reason_u and "DO NOT BET" in reason_u)) and total_line is not None:
            market = "TOTAL"
            bet_text = f"Avoid UNDER {total_line}"
        elif "ML" in reason_u:
            market = "ML"
            bet_text = f"Avoid {home} ML" if home else "Avoid ML"
        elif "TOTAL" in reason_u or "O/U" in reason_u or "OVER" in reason_u or "UNDER" in reason_u:
            market = "TOTAL"
            bet_text = f"Avoid total" if total_line is None else f"Avoid total ({total_line})"

    if not bet_text:
        bet_text = ("Avoid: " + reason) if reason else "Avoid"

    return market, bet_text, (float(total_line) if total_line is not None else None)


def prior_avoid_outcome_lines(entry: dict) -> tuple[str, str, str] | None:
    """
    Human lines for prior report: (bet_to_skip, counterfactual, verdict).
    None if game not final or not parseable.
    """
    g = entry.get("game") or {}
    hs, as_ = g.get("home_score"), g.get("away_score")
    if hs is None or as_ is None:
        return None
    market, bet_label, _tl = avoid_entry_bet_fields(entry)
    if market == "ENV":
        return (
            f"  BET TO SKIP: {bet_label}",
            "  Counterfactual: N/A — this flags a class of weather-driven model plays, "
            "not one specific ML or O/U ticket at the closing line.",
            "  Verdict: N/A — informational; see model detail for venue / weather context.",
        )
    equiv = strip_avoid_bet_label(bet_label)
    home_abbr = (g.get("home_abbr") or "").strip().upper()
    away_abbr = (g.get("away_abbr") or "").strip().upper()
    runs = hs + as_

    def _parse_ml_team_local(bt: str) -> str | None:
        s = (bt or "").strip().upper()
        if not s:
            return None
        return s.split()[0]

    def _parse_total_local(bt: str) -> tuple[str | None, float | None]:
        s = (bt or "").strip().upper()
        if not s:
            return None, None
        side = "over" if s.startswith("OVER") else ("under" if s.startswith("UNDER") else None)
        if side is None:
            return None, None
        try:
            return side, float(s.split()[1])
        except Exception:
            return side, None

    hypo: str | None = None
    if market == "ML":
        team = _parse_ml_team_local(equiv)
        if not team:
            return None
        if team == home_abbr:
            hypo = "win" if hs > as_ else "loss"
        elif team == away_abbr:
            hypo = "win" if as_ > hs else "loss"
        else:
            return None
    elif market == "TOTAL":
        side, line = _parse_total_local(equiv)
        if side is None or line is None:
            return None
        if runs == line:
            hypo = "push"
        else:
            won = (side == "over" and runs > line) or (side == "under" and runs < line)
            hypo = "win" if won else "loss"
    else:
        bet_line = f"  BET TO SKIP: {bet_label}"
        tail = (
            "  (Counterfactual not parsed for this flag type — see model note below.)"
        )
        return (bet_line, tail, "  Verdict: —")

    bet_line = f"  BET TO SKIP: {bet_label}"
    if hypo == "push":
        cf = "  If you had taken it: PUSH vs closing total (no win/loss edge)."
        ver = "  Verdict on the call: — (push — neither good nor bad in 1u terms)."
    elif hypo == "win":
        cf = "  If you had taken it: would have WON (vs closing line / side above)."
        ver = "  Verdict on the call: ✗ Poor avoid — you skipped a winning side."
    else:
        cf = "  If you had taken it: would have LOST."
        ver = "  Verdict on the call: ✓ Good avoid — you dodged a losing bet."

    return (bet_line, cf, ver)


def grade_bet_ledger(conn: sqlite3.Connection, game_date: str | None = None) -> int:
    """
    Grade bets in bet_ledger for games that are Final.

    - Joins bet_ledger -> games on game_pk
    - Only processes games where games.status = 'Final'
    - Top/next rows: result in ('win','loss','push'), pnl_units at stake 1
    - Avoid rows (signal_at_time='avoid'): result good_avoid / bad_avoid / push_avoid —
      counterfactual for the skipped bet; pnl_units 0 (informational only)

    Market rules:
      moneyline:
        bet team wins -> win else loss
      total:
        compare total line vs actual runs
      run line:
        compare spread vs final score
    """
    if conn is None:
        return 0

    def _pnl_units_from_odds(odds: int | None, won: bool, push: bool) -> float:
        if push:
            return 0.0
        if not won:
            return -1.0
        if odds is None:
            # Missing odds -> assume even money
            return 1.0
        return (odds / 100.0) if odds > 0 else (100.0 / abs(odds))

    def _parse_total_bet(bet_text: str) -> tuple[str | None, float | None]:
        s = (bet_text or "").strip().upper()
        if not s:
            return None, None
        side = "over" if s.startswith("OVER") else ("under" if s.startswith("UNDER") else None)
        if side is None:
            return None, None
        # e.g. "OVER 8.5"
        try:
            num = float(s.split()[1])
            return side, num
        except Exception:
            return side, None

    def _parse_runline_bet(bet_text: str) -> tuple[str | None, float | None]:
        # e.g. "NYY -1.5" or "STL +1.5"
        s = (bet_text or "").strip().upper()
        if not s:
            return None, None
        parts = s.split()
        if len(parts) < 2:
            return None, None
        team = parts[0]
        try:
            line = float(parts[1])
        except Exception:
            return team, None
        return team, line

    def _parse_ml_team(bet_text: str) -> str | None:
        # e.g. "CHC ML" or "BAL ML"
        s = (bet_text or "").strip().upper()
        if not s:
            return None
        parts = s.split()
        if not parts:
            return None
        return parts[0]

    where_date = ""
    params: tuple = ()
    if game_date:
        where_date = "AND bl.game_date = ?"
        params = (game_date,)

    # Only grade ungraded bets (result is NULL or empty) for Final games.
    rows = conn.execute(
        f"""
        SELECT
            bl.id,
            bl.game_pk,
            bl.market_type,
            bl.bet,
            bl.odds_taken,
            bl.signal_at_time,
            g.home_team_id,
            g.away_team_id,
            g.home_score,
            g.away_score,
            th.abbreviation AS home_abbr,
            ta.abbreviation AS away_abbr
        FROM bet_ledger bl
        JOIN games g ON g.game_pk = bl.game_pk
        JOIN teams th ON th.team_id = g.home_team_id
        JOIN teams ta ON ta.team_id = g.away_team_id
        WHERE g.status = 'Final'
          AND (bl.result IS NULL OR TRIM(bl.result) = '')
          {where_date}
        """,
        params,
    ).fetchall()

    if not rows:
        return 0

    updated = 0
    for r in rows:
        market = (r["market_type"] or "").strip().lower()
        bet_text = r["bet"] or ""
        hs = r["home_score"]
        as_ = r["away_score"]
        if hs is None or as_ is None:
            continue

        res = None
        pnl = None
        sig_at = (r["signal_at_time"] or "").strip().lower()

        if sig_at == "avoid":
            equiv = strip_avoid_bet_label(bet_text)
            home_abbr = (r["home_abbr"] or "").upper()
            away_abbr = (r["away_abbr"] or "").upper()
            runs = hs + as_
            hypo: str | None = None

            if market == "moneyline":
                team = _parse_ml_team(equiv)
                if not team:
                    continue
                if team == home_abbr:
                    won = hs > as_
                elif team == away_abbr:
                    won = as_ > hs
                else:
                    continue
                hypo = "win" if won else "loss"
            elif market == "total":
                side, line = _parse_total_bet(equiv)
                if side is None or line is None:
                    continue
                if runs == line:
                    hypo = "push"
                else:
                    won = (side == "over" and runs > line) or (side == "under" and runs < line)
                    hypo = "win" if won else "loss"
            elif market in ("spread", "runline"):
                team, line = _parse_runline_bet(equiv)
                if team is None or line is None:
                    continue
                if team == home_abbr:
                    adj = hs + line
                    opp = as_
                elif team == away_abbr:
                    adj = as_ + line
                    opp = hs
                else:
                    continue
                if adj == opp:
                    hypo = "push"
                else:
                    won = adj > opp
                    hypo = "win" if won else "loss"
            else:
                continue

            if hypo == "push":
                res = "push_avoid"
            elif hypo == "win":
                res = "bad_avoid"
            else:
                res = "good_avoid"
            pnl = 0.0

            conn.execute(
                "UPDATE bet_ledger SET result = ?, pnl_units = ? WHERE id = ?",
                (res, float(round(pnl, 4)), r["id"]),
            )
            updated += 1
            continue

        if market == "moneyline":
            team = _parse_ml_team(bet_text)
            if not team:
                continue
            home_abbr = (r["home_abbr"] or "").upper()
            away_abbr = (r["away_abbr"] or "").upper()
            if team == home_abbr:
                won = hs > as_
            elif team == away_abbr:
                won = as_ > hs
            else:
                # Unknown team token
                continue
            res = "win" if won else "loss"
            pnl = _pnl_units_from_odds(r["odds_taken"], won=won, push=False)

        elif market == "total":
            side, line = _parse_total_bet(bet_text)
            if side is None or line is None:
                continue
            runs = hs + as_
            if runs == line:
                res = "push"
                pnl = _pnl_units_from_odds(r["odds_taken"], won=False, push=True)
            else:
                won = (side == "over" and runs > line) or (side == "under" and runs < line)
                res = "win" if won else "loss"
                pnl = _pnl_units_from_odds(r["odds_taken"], won=won, push=False)

        elif market in ("spread", "runline"):
            team, line = _parse_runline_bet(bet_text)
            if team is None or line is None:
                continue
            home_abbr = (r["home_abbr"] or "").upper()
            away_abbr = (r["away_abbr"] or "").upper()
            if team == home_abbr:
                adj = hs + line
                opp = as_
            elif team == away_abbr:
                adj = as_ + line
                opp = hs
            else:
                continue
            if adj == opp:
                res = "push"
                pnl = _pnl_units_from_odds(r["odds_taken"], won=False, push=True)
            else:
                won = adj > opp
                res = "win" if won else "loss"
                pnl = _pnl_units_from_odds(r["odds_taken"], won=won, push=False)
        else:
            continue

        if res is None or pnl is None:
            continue

        conn.execute(
            "UPDATE bet_ledger SET result = ?, pnl_units = ? WHERE id = ?",
            (res, float(round(pnl, 4)), r["id"]),
        )
        updated += 1

    if updated:
        conn.commit()
    return updated


def save_brief_picks(
    conn: sqlite3.Connection,
    game_date: str,
    session: str,
    pick_entries: list | None,
    *,
    avoid_entries: list | None = None,
    now: datetime.datetime | None = None,
    model_version: str = "legacy",
) -> None:
    """Record picks shown in this brief for prior-report grading.
    pick_entries: sorted list of entry dicts from the brief builder.
    Records top pick (rank 1) and additional picks (ranks 2-6).
    Also records AVOID flags for research as pick_rank=0 with signal='AVOID'.
    AVOID rows sync into bet_ledger via signal_state + backfill (stake 0).
    Idempotent — INSERT OR IGNORE on (game_date, session, game_pk, signal).

    Skips insert when the game has already started (vs ``now``) or when the same
    game_date + game_pk + signal + market was stored in an earlier session.
    """
    if now is None:
        now = _now_et()
    now_et = now.strftime("%Y-%m-%d %H:%M ET")

    ensure_brief_picks(conn)

    wrote_any = False

    if pick_entries:
        for rank, entry in enumerate(pick_entries[:6], start=1):
            g = entry["game"]
            # Use highest-priority pick for this game
            p = sorted(entry["sigs"]["picks"], key=lambda x: x["priority"])[0]
            signal = ", ".join(entry["sigs"]["signals"])
            if _game_already_started_for_brief_picks(g, now):
                continue
            if _brief_pick_exists_cross_session(
                conn, game_date, int(g["game_pk"]), signal, p["market"],
            ):
                continue
            odds_raw = _parse_odds(p.get("odds", ""))
            total_line_val = None
            if p.get("market") == "TOTAL":
                total_line_val = entry["game"].get("total_line")
            try:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO brief_picks
                        (game_date, session, game_pk, pick_rank, signal,
                         bet, market, odds, total_line, recorded_at, model_version)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        game_date,
                        session,
                        g["game_pk"],
                        rank,
                        signal,
                        p["bet"],
                        p["market"],
                        odds_raw,
                        total_line_val,
                        now_et,
                        model_version,
                    ),
                )
                wrote_any = True
            except Exception:
                pass

    if avoid_entries:
        for entry in avoid_entries:
            g = (entry.get("game") or {})
            gpk = g.get("game_pk")
            if gpk is None:
                continue
            if _game_already_started_for_brief_picks(g, now):
                continue
            market, bet_text, total_line_val = avoid_entry_bet_fields(entry)
            if _brief_pick_exists_cross_session(
                conn, game_date, int(gpk), "AVOID", market,
            ):
                continue
            try:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO brief_picks
                        (game_date, session, game_pk, pick_rank, signal,
                         bet, market, odds, total_line, recorded_at, model_version)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        game_date,
                        session,
                        int(gpk),
                        0,
                        "AVOID",
                        bet_text,
                        market,
                        None,
                        total_line_val,
                        now_et,
                        model_version,
                    ),
                )
                wrote_any = True
            except Exception:
                pass

    if wrote_any:
        conn.commit()


def load_brief_picks(
    conn: sqlite3.Connection,
    game_date: str,
    session: str = "primary",
    *,
    include_avoids: bool = False,
) -> list:
    """
    Load confirmed picks that were shown in yesterday's primary brief.
    Returns list of dicts ordered by pick_rank.

    By default excludes AVOID research rows (pick_rank=0, signal='AVOID').
    """
    try:
        avoid_clause = "" if include_avoids else " AND pick_rank > 0 "
        rows = conn.execute("""
            SELECT game_pk, pick_rank, signal, bet, market, odds
            FROM   brief_picks
            WHERE  game_date = ? AND session = ?
              {avoid_clause}
            ORDER  BY pick_rank
        """.format(avoid_clause=avoid_clause), (game_date, session)).fetchall()
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
              AND  pick_rank  > 0
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
              AND  pick_rank > 0
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


def log_brief(
    conn,
    game_date,
    session,
    games_covered,
    picks_count,
    output_file,
    pick_entries=None,
    avoid_entries=None,
    now: datetime.datetime | None = None,
):
    if now is None:
        now = _now_et()
    generated_at_et = now.strftime("%Y-%m-%d %H:%M ET")
    try:
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
            save_brief_picks(conn, game_date, session, pick_entries, avoid_entries=avoid_entries, now=now)
    except sqlite3.OperationalError:
        # Best-effort logging: don't fail the brief if another process holds a DB lock.
        try:
            conn.rollback()
        except Exception:
            pass
        return


# ═══════════════════════════════════════════════════════════════════════════
# Data loaders
# ═══════════════════════════════════════════════════════════════════════════

def load_games(conn: sqlite3.Connection, game_date: str, verbose: bool,
               as_of_dt: datetime.datetime | None = None) -> list:
    """
    Load today's games with odds, weather, and team info.
    Returns list of dicts with all fields needed for signal evaluation.

    When as_of_dt is set (historical simulation), loads all regular-season games
    for the date, including Final rows, so main() can filter by simulated clock.
    Live runs use as_of_dt=None and exclude Final games in SQL.
    """
    status_line = (
        ""
        if as_of_dt is not None
        else "          AND  g.status    != 'Final'          -- skip already-completed games\n"
    )
    cur = conn.execute(
        """
        SELECT
            g.game_pk,
            g.game_date_et AS game_date,
            g.season,
            g.venue_id,
            g.game_start_utc,
            v.name          AS venue_name,
            g.temp_f,
            g.wind_mph,
            g.wind_direction,
            g.sky_condition,
            COALESCE(g.wind_source, 'actual') AS wind_source,

            -- Venue intelligence (populated by add_stadium_data.py)
            v.wind_effect,
            v.wind_note,
            v.roof_type,
            v.elevation_ft,
            v.park_factor_runs,
            v.park_factor_hr,
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
        WHERE  g.game_date_et = ?
          AND  g.game_type = 'R'          -- regular season only; Spring Training / Exhibition excluded
"""
        + status_line
        + """        ORDER  BY g.game_start_utc
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
              AND  game_date_et < ?
            ORDER  BY game_date_et DESC, game_start_utc DESC
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
                p.era_season,
                p.throws,
                trs.rolling_ops        AS team_rolling_ops,
                trs.games_in_window    AS team_games_in_window
            FROM   game_probable_pitchers gp
            JOIN   players p   ON p.player_id  = gp.player_id
            JOIN   games   g   ON g.game_pk    = gp.game_pk
            LEFT JOIN team_rolling_stats trs
                   ON  trs.game_pk = gp.game_pk
                   AND trs.team_id = gp.team_id
            WHERE  g.game_date_et = ?
            """,
            (game_date,),
        )
        rows = cur.fetchall()
        for r in rows:
            if r["game_pk"] not in starters:
                starters[r["game_pk"]] = {}
            starters[r["game_pk"]][r["team_id"]] = {
                "name":        r["full_name"],
                "era":         r["era_season"],
                "throws":     r["throws"],            # 'L', 'R', or None
                "rolling_ops": r["team_rolling_ops"],  # home team rolling OPS from trs
                "ops_window":  r["team_games_in_window"],
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
            WHERE  g.game_date_et = ?
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

def enrich_game_with_starters(game: dict, starters: dict) -> None:
    """
    Inject away starter handedness, ERA, name, and home team rolling OPS
    into the game dict. Used by ``enrich_game()`` before dressing; may also be
    called alone when only the raw dict needs starter fields.
    Modifies game in place. Safe to call even if starters is empty.
    """
    gpk     = game.get("game_pk")
    home_id = game.get("home_team_id")
    away_id = game.get("away_team_id")
    game_starters = starters.get(gpk, {})

    away_s = game_starters.get(away_id, {})
    home_s = game_starters.get(home_id, {})

    game["away_starter_throw"] = away_s.get("throws")
    game["away_starter_era"]   = away_s.get("era")
    game["away_starter_name"]  = away_s.get("name")
    game["home_rolling_ops"]   = home_s.get("rolling_ops")
    game["home_ops_window"]    = home_s.get("ops_window") or 0


def enrich_game(conn: sqlite3.Connection, game: dict, starters: dict):
    """
    Starter-enrich the brief row dict and dress it for scoring.

    Returns ``FullyDressedGame`` (see ``batch.pipeline.score_game.dress_game_for_brief``).
    """
    enrich_game_with_starters(game, starters)
    from batch.pipeline.score_game import dress_game_for_brief

    return dress_game_for_brief(conn, game)


def evaluate_signals(
    conn: sqlite3.Connection,
    game: dict,
    streaks: dict,
    session: str,
    starters: dict | None = None,
    *,
    verbose: bool = False,
    debug_wind: bool = False,
) -> dict:
    """
    Evaluate all model signals for a single game.
    Returns a dict describing which signals fired and what they recommend.

    Flow: ``enrich_game`` → ``score_game`` → ``scored_game_to_eval_dict``. Extra keys:
    ``output_tier``, ``tier_basis``, ``stake_multiplier`` (from ``ScoredGame``).
    When ``verbose`` is True, prints tier_basis per game to stdout.
    When ``debug_wind`` is True, prints a wind classification dump per game (DB vs dressed env).
    """
    from batch.pipeline.score_game import score_game, scored_game_to_eval_dict

    if conn is None:
        raise TypeError("evaluate_signals requires a sqlite3.Connection (conn)")

    starters = starters if starters is not None else {}

    try:
        fdg = enrich_game(conn, game, starters)
        hid = int(game["home_team_id"])
        aid = int(game["away_team_id"])
        home_streak = int(streaks.get(hid, 0))
        away_streak = int(streaks.get(aid, 0))
        from dataclasses import replace as _dc_replace

        fdg = _dc_replace(
            fdg,
            brief_session=session,
            home_streak=home_streak,
            away_streak=away_streak,
        )
        if debug_wind:
            print_wind_debug_for_game(game, fdg)
        gd = fdg.identifiers.game_date_et
        game_month = int(gd[5:7]) if len(gd) >= 7 else 0
        scored = score_game(fdg, home_streak, game_month)
        out = scored_game_to_eval_dict(scored, session)
        out["output_tier"] = scored.output_tier
        out["tier_basis"] = scored.tier_basis
        out["stake_multiplier"] = scored.stake_multiplier
        if verbose and scored.tier_basis:
            ab = f"{fdg.identifiers.away_team_abbr}@{fdg.identifiers.home_team_abbr}"
            print(f"  [verbose] score {ab}: tier={scored.output_tier!r} | {scored.tier_basis}")
        return out
    except Exception as e:
        return {
            "signals": [],
            "picks": [],
            "avoid": False,
            "avoid_reason": None,
            "watch": False,
            "watch_reason": None,
            "data_flags": [f"Signal evaluation failed (dress/score): {e}"],
            "output_tier": None,
            "tier_basis": "",
            "stake_multiplier": 0.0,
        }


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

    parts = []

    # Roof / dome status prefix
    if wind_effect == "SUPPRESSED":
        # Use specific note when available, fall back to roof type
        note = game.get("wind_note") or ""
        first_sentence = note.split(".")[0] if note else f"{roof_type} — wind signals suppressed"
        parts.append(f"⚠ {first_sentence}")
    # LOW wind_effect: no extra venue headline (temp/wind only).

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


def _avoid_scope_line(game: dict, sigs: dict) -> str | None:
    """
    Return a concise, explicit description of WHAT is being avoided.
    Avoid flags should never read like "avoid the whole game" unless it truly is.
    """
    scope = (sigs.get("avoid_scope") or "").strip()
    if scope:
        return scope

    # Fallback for older avoids: infer from reason text.
    reason = (sigs.get("avoid_reason") or "").strip()
    ru = reason.upper()
    if "WIND" in ru and ("SUPPRESSED" in ru or "NO WEATHER" in ru):
        return "WIND SIGNALS ONLY (totals/wind edges) — other analysis OK"
    if "AVOID HOME ML" in ru or "AVOID AWAY ML" in ru or " ML" in ru:
        return "MONEYLINE ONLY (ML) — other markets OK"
    if "OVER" in ru or "UNDER" in ru or "TOTAL" in ru or "O/U" in ru:
        return "TOTALS ONLY (O/U) — other markets OK"
    return None


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
                            verbose: bool, now: datetime.datetime | None = None,
                            *, debug_wind: bool = False) -> str:
    """
    Prior day performance report — full slate with signal grading.

    Shows every game played yesterday with:
      · Final score, runs, winner, conditions
      · Closing odds and O/U outcome
      · Which model signals fired and whether they won
      · Full slate per-game bet outcomes (or NO SIGNAL)
      · Day-level P&L summary
    """
    lines = []
    if now is None:
        now = _now_et()
    generated_ts = now.strftime("%Y-%m-%d %I:%M %p ET").lstrip("0")
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
            g.game_date_et AS game_date,
            g.season,
            g.venue_id,
            g.home_score, g.away_score,
            g.wind_mph, g.wind_direction, g.temp_f, g.sky_condition,
            COALESCE(g.wind_source, 'actual') AS wind_source,
            g.game_start_utc,
            th.team_id   AS home_team_id,
            ta.team_id   AS away_team_id,
            th.abbreviation AS home_abbr,
            ta.abbreviation AS away_abbr,
            th.name         AS home_name,
            ta.name         AS away_name,
            v.name          AS venue_name,
            v.wind_effect, v.wind_note, v.roof_type,
            v.park_factor_runs, v.park_factor_hr, v.orientation_hp,
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
        WHERE  g.game_date_et = ?
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

    # Starters (for enrich_game / starter_line). Falls back gracefully if missing.
    starters = load_starters(conn, game_date, verbose)

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
        sigs = evaluate_signals(
            conn, g, streaks, "primary", starters,
            verbose=verbose, debug_wind=debug_wind,
        )
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

    def pnl_str(pnl):
        return f"+{pnl:.2f}u" if pnl > 0 else (f"{pnl:.2f}u" if pnl < 0 else "push")

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
    # BET LEDGER (source of truth) — backfill from signal_state, then grade
    # ════════════════════════════════════════════════════════════════════
    try:
        if PERSIST_WRITES:
            backfill_bet_ledger_from_signal_state(conn, game_date, now=now)
    except Exception:
        pass
    try:
        grade_bet_ledger(conn, game_date=game_date)
    except Exception:
        pass

    try:
        bet_rows = conn.execute("""
            SELECT
                id, game_pk, market_type, bet, odds_taken, stake_units,
                signal_at_time, session, placed_at, result, pnl_units
            FROM bet_ledger
            WHERE game_date = ?
            ORDER BY placed_at
        """, (game_date,)).fetchall()
        bet_rows = [dict(r) for r in bet_rows]
    except Exception:
        bet_rows = []

    def _ledger_signal_label(sig: str | None) -> str:
        s = (sig or "").strip().lower()
        if s == "top":
            return "TOP PICK"
        if s == "next":
            return "NEXT (additional)"
        if s == "avoid":
            return "AVOID (do not bet)"
        return (sig or "").upper() or "—"

    def _summarise_bets(rows: list) -> dict:
        stake_rows = [
            r for r in rows
            if (r.get("signal_at_time") or "").lower() != "avoid"
        ]
        avoid_rows = [
            r for r in rows
            if (r.get("signal_at_time") or "").lower() == "avoid"
        ]
        graded = [r for r in stake_rows if r.get("result") in ("win", "loss", "push")]
        wins   = sum(1 for r in graded if r["result"] == "win")
        losses = sum(1 for r in graded if r["result"] == "loss")
        pushes = sum(1 for r in graded if r["result"] == "push")
        units  = sum(float(r.get("pnl_units") or 0.0) for r in graded)
        n      = len(graded)
        roi    = (100.0 * units / n) if n else 0.0
        av_done = [r for r in avoid_rows if r.get("result") in ("good_avoid", "bad_avoid", "push_avoid")]
        agood = sum(1 for r in av_done if r["result"] == "good_avoid")
        abad = sum(1 for r in av_done if r["result"] == "bad_avoid")
        apush = sum(1 for r in av_done if r["result"] == "push_avoid")
        return {
            "bets": n,
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "units": units,
            "roi": roi,
            "avoid_graded": len(av_done),
            "avoid_good": agood,
            "avoid_bad": abad,
            "avoid_push": apush,
        }

    bet_summary = _summarise_bets(bet_rows)
    by_market: dict[str, list] = {}
    for r in bet_rows:
        m = (r.get("market_type") or "unknown").strip() or "unknown"
        by_market.setdefault(m, []).append(r)

    # ════════════════════════════════════════════════════════════════════
    # FULL SLATE — per-game, per-bet grading
    # ════════════════════════════════════════════════════════════════════
    lines.append(section(f"📋  FULL SLATE  ({len(evaluated)} games)"))
    lines.append(
        "\n  For each game: every fired model bet (if any) with WIN/LOSS/PUSH.\n"
        "  Games with no qualifying signals show: NO SIGNAL.\n"
    )

    for e in evaluated:
        g = e["game"]
        lines.append(f"\n  {matchup_line(g)}")
        lines.append(f"  {weather_line(g)}")
        if g.get("home_score") is not None:
            lines.append(game_score_line(e))
        ol = game_odds_line(e)
        if ol:
            lines.append(ol)

        if e["graded"]:
            sig_label = ", ".join(e["sigs"].get("signals") or [])
            for pick in sorted(e["graded"], key=lambda x: x.get("priority", 99)):
                res = (pick.get("result") or "—").strip() or "—"
                pnl = float(pick.get("pnl") or 0.0)
                bet = pick.get("bet") or ""
                odds = pick.get("odds") or ""
                lines.append(
                    f"  BET: {bet:<22} ODDS: {odds:<8}  "
                    f"SIGNAL: {sig_label:<16}  RESULT: {res:<8}  P&L: {pnl_str(pnl)}"
                )
        else:
            lines.append("  NO SIGNAL")

        if e["sigs"].get("data_flags"):
            for f in e["sigs"]["data_flags"]:
                lines.append(f"  ⚠ {f}")
        lines.append("")

    # ════════════════════════════════════════════════════════════════════
    # BET LEDGER SUMMARY (P&L source of truth)
    # ════════════════════════════════════════════════════════════════════
    lines.append(section("📈  BET LEDGER SUMMARY"))
    lines.append(
        f"\n  Staked plays: {bet_summary['bets']}   "
        f"{bet_summary['wins']}W {bet_summary['losses']}L {bet_summary['pushes']}P   "
        f"Units: {bet_summary['units']:+.2f}u   ROI: {bet_summary['roi']:.1f}%"
    )

    # Optional grouping by market_type
    for m, rows_m in sorted(by_market.items(), key=lambda x: x[0]):
        s = _summarise_bets(rows_m)
        if s["bets"]:
            lines.append(
                f"\n  {m:<10} {s['bets']} bet(s)  "
                f"{s['wins']}W {s['losses']}L {s['pushes']}P   "
                f"Units: {s['units']:+.2f}u   ROI: {s['roi']:.1f}%"
            )

    # Season-to-date (same season as this game_date, up through game_date_et)
    try:
        season_row = conn.execute(
            "SELECT season FROM games WHERE game_date_et = ? AND game_type = 'R' LIMIT 1",
            (game_date,),
        ).fetchone()
        season_int = int(season_row[0]) if season_row and season_row[0] is not None else int(str(game_date)[:4])
    except Exception:
        season_int = int(str(game_date)[:4])

    try:
        srows = conn.execute(
            """
            SELECT
                bl.game_date, bl.game_pk, bl.market_type, bl.bet, bl.odds_taken,
                bl.stake_units, bl.signal_at_time, bl.session, bl.placed_at,
                bl.result, bl.pnl_units
            FROM bet_ledger bl
            JOIN games g ON g.game_pk = bl.game_pk
            WHERE g.season = ?
              AND g.game_type = 'R'
              AND g.game_date_et <= ?
            ORDER BY g.game_date_et, bl.placed_at
            """,
            (season_int, game_date),
        ).fetchall()
        srows = [dict(r) for r in srows]
        ssummary = _summarise_bets(srows)
        lines.append(
            f"\n  Season-to-date ({season_int} through {game_date}): {ssummary['bets']} bet(s)   "
            f"{ssummary['wins']}W {ssummary['losses']}L {ssummary['pushes']}P   "
            f"Units: {ssummary['units']:+.2f}u   ROI: {ssummary['roi']:.1f}%"
        )
    except Exception:
        pass

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

    lines.append(CAVEAT)
    return "\n".join(lines)

def build_morning_brief(games, streaks, starters, game_date,
                        conn=None, session=None,
                        now: datetime.datetime | None = None,
                        verbose: bool = False,
                        debug_wind: bool = False):
    lines = []
    if now is None:
        now = _now_et()
    generated_ts = now.strftime("%Y-%m-%d %I:%M %p ET").lstrip("0")
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

    # For persistence (does not affect output)
    all_picks_entries = []
    avoid_entries     = []

    for game in games:
        sigs = evaluate_signals(
            conn, game, streaks, "morning", starters,
            verbose=verbose, debug_wind=debug_wind,
        )
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

        # Persistable classifications (no change to signal logic)
        if sigs.get("picks"):
            all_picks_entries.append(entry)
        elif sigs.get("avoid"):
            avoid_entries.append(entry)

    # ── Persist signal state (TOP / NEXT / AVOID) ─────────────────────────
    # Best-effort insert only; does not affect computation or report output.
    if conn is not None and session is not None:
        try:
            all_picks_entries.sort(key=lambda e: min(p["priority"] for p in e["sigs"]["picks"]))
        except Exception:
            pass
        top_entry    = all_picks_entries[0] if len(all_picks_entries) >= 1 else None
        next_entries = all_picks_entries[1:6] if len(all_picks_entries) >= 2 else []
        save_signal_state(conn, game_date, session, top_entry, next_entries, avoid_entries, now=now)

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

    # ── Suppressed / indoor venues ───────────────────────────────────────
    # Removed from default output: it is redundant with per-game weather_line()
    # and reads like an exclusion list. Keep only under --verbose for diagnostics.
    if verbose:
        lines.append(section(f"[verbose] SUPPRESSED / INDOOR VENUES  ({len(dome_games)})"))
        if not dome_games:
            lines.append("\n  No suppressed-wind venues today.\n")
        for e in dome_games:
            g = e["game"]
            lines.append(f"\n  {matchup_line(g)}")
            lines.append(f"  {weather_line(g)}")
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
                        conn=None, session=None, now: datetime.datetime | None = None,
                        verbose: bool = False,
                        debug_wind: bool = False):
    if s6_fires is None:
        s6_fires = {}
    _action = {
        "EARLY GAMES": "✅  EARLY GAMES ACTION WINDOW — All unplayed games. Decide before first pitch.",
        "AFTERNOON":   "✅  AFTERNOON ACTION WINDOW — All unplayed games. Decide before first pitch.",
        "PRIMARY":     "✅  PRIMARY ACTION WINDOW — All unplayed games. Make your betting decisions NOW.",
        "LATE GAMES":  "✅  LATE GAMES ACTION WINDOW — West Coast games still unstarted. Check odds and act now.",
    }.get(session_label, "✅  ACTION WINDOW — Make your betting decisions NOW.")
    lines = []
    if now is None:
        now = _now_et()
    generated_ts = now.strftime("%Y-%m-%d %I:%M %p ET").lstrip("0")
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
        sigs = evaluate_signals(
            conn, game, streaks, "primary", starters,
            verbose=verbose, debug_wind=debug_wind,
        )
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

    # ── Persist signal state (TOP / NEXT / AVOID) ─────────────────────────
    # Do not affect computation or report output; best-effort insert only.
    if conn is not None and session is not None:
        top_entry = all_picks[0] if len(all_picks) >= 1 else None
        next_entries = all_picks[1:6] if len(all_picks) >= 2 else []
        save_signal_state(conn, game_date, session, top_entry, next_entries, avoid_games, now=now)

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
        scope = _avoid_scope_line(g, sigs)
        if scope:
            lines.append(f"  BET TO SKIP: {scope}")
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


def build_closing_brief(games, streaks, starters, movement, game_date,
                        conn=None, session=None,
                        now: datetime.datetime | None = None,
                        verbose: bool = False,
                        debug_wind: bool = False):
    lines = []
    if now is None:
        now = _now_et()
    generated_ts = now.strftime("%Y-%m-%d %I:%M %p ET").lstrip("0")
    lines.append(banner(f"MLB BETTING BRIEF  ·  CLOSING SESSION  ·  {game_date}"))
    lines.append(f"  Generated: {generated_ts}\n")
    lines.append(
        "\n  CLOSING CONFIRMATION — Compare against Primary Brief picks.\n"
        "  No new bets unless closing price is BETTER than Primary Brief price.\n"
        "  Flag any line that moved 3+ cents since the Primary Brief.\n"
    )

    # For persistence (does not affect output)
    all_picks_entries = []
    avoid_entries     = []

    for game in games:
        if _game_started_or_in_progress_for_closing(game, now):
            continue
        sigs = evaluate_signals(
            conn, game, streaks, "closing", starters,
            verbose=verbose, debug_wind=debug_wind,
        )
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

        # Persistable classifications (no change to signal logic)
        entry = {
            "game":    g,
            "sigs":    sigs,
            "starter": starter_line(g, starters),
            "streak":  streak_line(g, streaks),
        }
        if sigs.get("picks"):
            all_picks_entries.append(entry)
        elif sigs.get("avoid"):
            avoid_entries.append(entry)

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

    # ── Persist signal state (TOP / NEXT / AVOID) ─────────────────────────
    # Best-effort insert only; does not affect computation or report output.
    if conn is not None and session is not None:
        try:
            all_picks_entries.sort(key=lambda e: min(p["priority"] for p in e["sigs"]["picks"]))
        except Exception:
            pass
        top_entry    = all_picks_entries[0] if len(all_picks_entries) >= 1 else None
        next_entries = all_picks_entries[1:6] if len(all_picks_entries) >= 2 else []
        save_signal_state(conn, game_date, session, top_entry, next_entries, avoid_entries, now=now)

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
# Produces outputs/briefs/YYYY-MM-DD_SESSION.docx alongside the .txt file (default).
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

    # ── Avoid flag (only when no picks — avoids duplicate banner when tier=Avoid+winds)
    if sigs["avoid"] and not sigs.get("picks"):
        scope = _avoid_scope_line(game, sigs)
        if scope:
            p0 = doc.add_paragraph()
            run0 = p0.add_run(f"BET TO SKIP: {scope}")
            run0.bold = True
            run0.font.size = Pt(9)
            run0.font.color.rgb = RGBColor(0x6B, 0x72, 0x80)  # slate gray
            p0.paragraph_format.left_indent = Inches(0.2)
            p0.paragraph_format.space_before = Pt(2)
            p0.paragraph_format.space_after = Pt(0)

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


def build_docx_brief(
    conn: sqlite3.Connection,
    session: str,
    game_date: str,
    games: list,
    streaks: dict,
    starters: dict,
    movement: dict = None,
    verbose: bool = False,
    debug_wind: bool = False,
) -> "Document":
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
        sigs = evaluate_signals(
            conn, game, streaks, session, starters,
            verbose=verbose, debug_wind=debug_wind,
        )
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

        # Keep suppressed/indoor list only under --verbose (diagnostic).
        if verbose:
            _add_heading(doc, f"[verbose] Suppressed / Indoor Venues  ({len(domes)})", level=2)
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
            sigs = evaluate_signals(
                conn, g, streaks, "primary", starters,
                verbose=verbose, debug_wind=debug_wind,
            )
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

        # FULL SLATE: one block per game; picks show graded table; otherwise "NO SIGNAL".
        # (No separate "Bets to Avoid" section in prior report.)

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

        _add_heading(doc, f"Full Slate  ({len(evaluated)} games)", level=2)
        for e in evaluated:
            g = e["game"]
            _add_matchup_block(doc, g, streaks, starters, e["sigs"], show_picks=False)
            _add_score_line(doc, e)
            _add_odds_line(doc, g)
            if e["graded"]:
                _add_graded_pick_table(doc, e["graded"], e["sigs"])
            else:
                _add_note(doc, "NO SIGNAL", italic=False, color_hex="475569")
                doc.add_paragraph()

        # ── MODEL P&L SUMMARY ────────────────────────────────────────────
        _add_heading(doc, "📈  Model P&L Summary", level=2)
        all_signal_entries = [e for e in evaluated if e["graded"]]

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
            for label, entries in [("All signals", all_signal_entries)]:
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

def _parse_as_of_arg(value: str) -> datetime.datetime:
    try:
        naive = datetime.datetime.strptime(value.strip(), "%Y-%m-%d %H:%M")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "expected 'YYYY-MM-DD HH:MM' (America/New_York wall time)"
        ) from exc
    return naive.replace(tzinfo=_ET)


def _parse_as_of_time_arg(value: str) -> datetime.datetime | datetime.time:
    """
    Parse --as-of-time as either:
    - full ET instant: YYYY-MM-DD HH:MM (same wall form as --as-of), or
    - time-only HH:MM[:SS] combined with --date (or today).
    """
    s = value.strip()
    try:
        naive = datetime.datetime.strptime(s, "%Y-%m-%d %H:%M")
        return naive.replace(tzinfo=_ET)
    except ValueError:
        pass
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            tt = datetime.datetime.strptime(s, fmt).time()
            return datetime.time(tt.hour, tt.minute, 0, 0)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(
        "expected --as-of-time as HH:MM or full 'YYYY-MM-DD HH:MM' (America/New_York)"
    )


def _et_datetime_from_date_and_time(game_date: str, t: datetime.time) -> datetime.datetime:
    d = datetime.date.fromisoformat(game_date)
    return datetime.datetime.combine(d, t, tzinfo=_ET)


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

            Primary brief (after 5 PM odds pull — evening games) — use clock for hybrid session:
              python generate_daily_brief.py --session primary --date 2026-04-17 --as-of-time 17:30

            Closing brief (after 6:30 PM odds pull):
              python generate_daily_brief.py --session closing --as-of-time 18:50

            Rerun yesterday's primary brief (recovery):
              python generate_daily_brief.py --session primary --date 2026-03-25 --as-of-time 17:30 --force

            Dry-run to preview without writing to log:
              python generate_daily_brief.py --session primary --dry-run

            Run with prereq check (recommended for scheduled runs):
              python generate_daily_brief.py --session primary --check-prereqs

            Sync bet_ledger from signal_state only (pregame T−30 window; no brief output):
              python generate_daily_brief.py --sync-bet-ledger-only --date YYYY-MM-DD

            Wind classification investigation:
              python generate_daily_brief.py --session primary --verbose --debug-wind
        """),
    )
    p.add_argument(
        "--sync-bet-ledger-only", action="store_true",
        help="Only run generate_bets_from_signal_state for --date (default today). "
             "No brief file, no brief_log duplicate check. Implies writes unless --dry-run.",
    )
    p.add_argument(
        "--session", required=False, default=None,
        choices=["prior", "morning", "early", "afternoon", "primary", "closing", "late"],
        help="Brief kind hint: prior is explicit; other values are ignored when --as-of-time or "
             "--as-of supplies a clock (session is derived from SESSION_WINDOWS). "
             "Required unless --sync-bet-ledger-only.",
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
        help="Write brief to this file path (appends). Default: outputs/briefs/brief-SLATE-DATE_RUN-STAMP_ET.txt",
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
        "--debug-wind", action="store_true",
        help="Per game, print wind classification debug: DB row vs dressed environment "
             "(direction, in/out gates, suppression, env_ceiling). Implies detailed stdout; "
             "pair with --verbose for tier lines too.",
    )
    p.add_argument(
        "--as-of",
        dest="as_of_dt",
        default=None,
        metavar="TS",
        type=_parse_as_of_arg,
        help='Full wall clock in America/New_York ("YYYY-MM-DD HH:MM"). Overrides --as-of-time.',
    )
    p.add_argument(
        "--as-of-time",
        dest="as_of_time",
        default=None,
        metavar="HH:MM",
        type=_parse_as_of_time_arg,
        help="ET clock: HH:MM with --date, or full YYYY-MM-DD HH:MM (same as --as-of). "
        "Required for non-prior sessions unless --as-of is set. Session from SESSION_WINDOWS.",
    )
    return p.parse_args()


def _maybe_email_report_docx(*, docx_path: str, slate_date: str, session: str) -> None:
    """
    After a Word brief is saved, optionally email it via ``delivery.email_sender``.
    Non-fatal: never raises; SMTP/import failures are logged only.
    """
    try:
        to_raw = (os.getenv("BRIEF_EMAIL_TO") or os.getenv("REPORT_EMAIL_TO") or "").strip()
        if not to_raw:
            return
        from delivery.email_sender import send_report_email

        subject = f"MLB brief {slate_date} ({session}) — {Path(docx_path).name}"
        body = (
            f"Slate date: {slate_date}\n"
            f"Session: {session}\n"
            f"Word report: {docx_path}\n"
        )
        ok, msg = send_report_email(docx_path, subject, to_raw, body=body)
        if ok:
            print(f"  ✓ [email] {msg}")
        else:
            print(f"  ⚠  [email] {msg}")
    except Exception as exc:
        print(f"  ⚠  [email] notification failed (non-fatal): {exc}")


def main():
    # ── Console encoding guard (Windows cp1252) ───────────────────────────
    # Some environments default to cp1252 and crash on unicode glyphs used
    # in headers (box drawing) and section icons. Prefer UTF-8; otherwise
    # replace unsupported characters instead of raising.
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    args = parse_args()
    raw_session = args.session
    has_clock = args.as_of_dt is not None or getattr(args, "as_of_time", None) is not None

    # now: precedence --as-of > --as-of-time + date > wall clock
    if args.as_of_dt is not None:
        now = _now_et(args.as_of_dt)
    elif getattr(args, "as_of_time", None) is not None:
        tot = args.as_of_time
        if isinstance(tot, datetime.datetime):
            now = _now_et(tot)
        else:
            d_str = args.date or datetime.datetime.now(tz=_ET).date().isoformat()
            now = _et_datetime_from_date_and_time(d_str, tot)
    else:
        now = _now_et(None)

    today = args.date or now.date().isoformat()

    # In dry-run, the brief should not write to any DB-backed ledgers.
    global PERSIST_WRITES
    PERSIST_WRITES = not args.dry_run

    print(f"[TIME] Running as-of: {now.strftime('%Y-%m-%d %H:%M ET')}")

    # Validate date
    try:
        datetime.date.fromisoformat(today)
    except ValueError:
        print(f"✗  Invalid --date value: '{today}'. Use YYYY-MM-DD.")
        sys.exit(1)

    if args.sync_bet_ledger_only:
        if raw_session is not None and args.verbose:
            print("  [verbose] --session ignored when using --sync-bet-ledger-only")
        print(f"\n{'═'*72}")
        print(f"  MLB Betting Model · bet_ledger sync (signal_state) · {today}")
        print(f"{'═'*72}")
        conn = open_db(DB_PATH)
        try:
            if args.dry_run:
                print("\n  (dry-run: no DB writes; bet_ledger sync skipped.)\n")
            else:
                n_bets = generate_bets_from_signal_state(conn, today, now=now)
                if n_bets:
                    print(f"\n  ✓ bet_ledger: inserted {n_bets} row(s) from signal_state (pregame window).\n")
                else:
                    msg = (
                        "no new rows (outside 30m window, duplicate keys, "
                        "or no qualifying signal_state rows)."
                    )
                    if args.verbose:
                        print(f"\n  [verbose] bet_ledger: {msg}\n")
                    else:
                        print(f"\n  bet_ledger: {msg}\n")
        except Exception as e:
            print(f"\n  ⚠  bet_ledger sync failed: {e}\n")
            sys.exit(1)
        finally:
            conn.close()
        return

    if raw_session is None:
        print("✗  --session is required unless using --sync-bet-ledger-only.")
        sys.exit(1)

    if raw_session == "prior":
        session = "prior"
    else:
        if not has_clock:
            print(
                "✗  Non-prior sessions require --as-of-time HH:MM (ET) or full "
                "--as-of 'YYYY-MM-DD HH:MM' (ET)."
            )
            sys.exit(2)
        session = _session_from_et_datetime(now)
        if raw_session != session:
            print(
                f"  [hybrid] resolved session={session!r} from clock "
                f"(ignoring --session {raw_session!r})"
            )

    # Slate simulation clock for load_games / filtering (full --as-of or hybrid now).
    as_of_for_slate = (
        args.as_of_dt if args.as_of_dt is not None else (now if has_clock else None)
    )

    print(f"\n{'═'*72}")
    print(f"  MLB Betting Model · Daily Brief · {session.upper()} · {today}")
    print(f"{'═'*72}")
    if args.debug_wind:
        print(
            "\n  [debug-wind] ENABLED — printing DB vs dressed wind fields for each game "
            "(see evaluate_signals / fully_dressed_game.build_game_environment).\n"
        )

    conn = open_db(DB_PATH)

    # ── Prereq check ─────────────────────────────────────────────────────
    if args.check_prereqs:
        check_prereqs(conn, today, session)

    # ── Duplicate guard ──────────────────────────────────────────────────
    # brief_log.game_date matches the slate the brief is for: forward sessions use
    # ``today``; ``prior`` uses the previous calendar day (same key as log_brief()).
    key_date = (
        (datetime.date.fromisoformat(today) - datetime.timedelta(days=1)).isoformat()
        if session == "prior"
        else today
    )
    ensure_brief_log(conn)
    if not args.dry_run and not args.force:
        if already_ran(conn, key_date, session):
            print(f"\n  ⚠  A {session} brief for {key_date} already exists in brief_log.")
            print(f"     Use --force to regenerate, or --dry-run to preview.\n")
            sys.exit(0)

    # ── Prior day report — uses yesterday, not today ─────────────────────
    if session == "prior":
        brief_text = build_prior_day_report(
            conn, key_date, args.verbose, now=now, debug_wind=args.debug_wind,
        )
        print(brief_text)

        # ── Save txt file ──────────────────────────────────────────────────
        output_file = None
        if not args.no_file and not args.dry_run:
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            stem = _default_brief_file_stem(key_date, now, is_prior=True)
            output_file = str(OUTPUT_DIR / f"{stem}.txt")
            with open(output_file, "w", encoding="utf-8") as fh:
                fh.write(brief_text)
            print(f"\n  ✓ Prior day report saved to: {output_file}")

        # ── Word (.docx) output (default) ───────────────────────────────────
        if not args.dry_run and not args.no_file:
            if not DOCX_AVAILABLE:
                print("\n  ⚠  Word output skipped: python-docx is not installed.")
                print("     Install with: pip install python-docx")
            else:
                try:
                    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
                    docx_path = str(OUTPUT_DIR / f"{stem}.docx")
                    doc = build_docx_from_text("prior", key_date, brief_text)
                    doc.save(docx_path)
                    print(f"  ✓ Word brief saved to: {docx_path}")
                    _maybe_email_report_docx(docx_path=docx_path, slate_date=key_date, session="prior")
                except Exception as e:
                    print(f"\n  ⚠  Word output failed: {e}")
                    import traceback; traceback.print_exc()

        # ── Log ───────────────────────────────────────────────────────────
        if not args.dry_run:
            log_brief(conn, key_date, "prior", 0, 0, output_file, now=now)
        conn.close()
        print(f"\n  Done.\n")
        return

    # ── Load data — for all forward-looking sessions ──────────────────────
    games = load_games(conn, today, args.verbose, as_of_dt=as_of_for_slate)

    # Historical --as-of: DB may already mark games Final; keep slate as-of simulated time
    if as_of_for_slate is not None and games:
        now_utc = now.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        grace = datetime.timedelta(minutes=10)
        games = [
            g for g in games
            if _game_start_utc_dt(g) is None
            or _game_start_utc_dt(g) > (now_utc - grace)
        ]

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
        print(f"     • Wrong date — check: python check_db.py")
        if as_of_for_slate is not None:
            print(
                "     • With --as-of / hybrid clock: no games were still unstarted at that simulated time, "
                "or the slate is empty for this date.\n"
            )
        else:
            print()
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

    if session in ("early", "afternoon", "primary") and as_of_for_slate is None:
        now_utc = now.astimezone(datetime.timezone.utc).replace(tzinfo=None)

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
            if _game_start_utc_dt(g) is None
            or _game_start_utc_dt(g) > (now_utc - grace)
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
                if _game_start_utc_dt(g) is not None
                and _game_start_utc_dt(g) <= (now_utc - grace)
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
    # outside evaluate_signals() (dress + score live in enrich_game / score_game).
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
        brief_text = build_morning_brief(
            games, streaks, starters, today,
            conn=conn, session=session, now=now,
            verbose=args.verbose, debug_wind=args.debug_wind,
        )
    elif session in ("early", "afternoon", "primary", "late"):
        label = {"early": "EARLY GAMES", "afternoon": "AFTERNOON",
                 "primary": "PRIMARY", "late": "LATE GAMES"}.get(session, "PRIMARY")
        brief_text = build_primary_brief(
            games, streaks, starters, today,
            session_label=label, s6_fires=s6_fires,
            conn=conn, session=session, now=now,
            verbose=args.verbose, debug_wind=args.debug_wind,
        )
    else:
        brief_text = build_closing_brief(
            games, streaks, starters, movement, today,
            conn=conn, session=session, now=now,
            verbose=args.verbose, debug_wind=args.debug_wind,
        )

    # ── Output ───────────────────────────────────────────────────────────
    print(brief_text)

    output_file = None
    default_stem = _default_brief_file_stem(today, now, is_prior=False)
    if not args.no_file and not args.dry_run:
        if args.output:
            output_file = args.output
        else:
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            output_file = str(OUTPUT_DIR / f"{default_stem}.txt")

        with open(output_file, "w", encoding="utf-8") as fh:
            fh.write(brief_text)
        print(f"\n  ✓ Brief saved to: {output_file}")

    # ── Word output (default) ─────────────────────────────────────────────
    if not args.dry_run and not args.no_file:
        if not DOCX_AVAILABLE:
            print("\n  ⚠  Word output skipped: python-docx is not installed.")
            print("     Install with: pip install python-docx")
        else:
            try:
                OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
                docx_path = (
                    str(Path(output_file).with_suffix(".docx"))
                    if output_file
                    else str(OUTPUT_DIR / f"{default_stem}.docx")
                )
                doc = build_docx_from_text(session, today, brief_text)
                doc.save(docx_path)
                print(f"  ✓ Word brief saved to: {docx_path}")
                _maybe_email_report_docx(docx_path=docx_path, slate_date=today, session=session)
            except Exception as e:
                print(f"\n  ⚠  Word output failed: {e}")
                import traceback; traceback.print_exc()

    # ── Log ──────────────────────────────────────────────────────────────
    if not args.dry_run:
        # Build pick_entries for action sessions so they can be saved
        # to brief_picks for confirmed prior-report grading.
        pick_entries_for_log = None
        avoid_entries_for_log = None
        if session in ("primary", "early", "afternoon", "late"):
            all_sig = []
            all_avoid = []
            for g in games:
                sigs = evaluate_signals(
                    conn, g, streaks, session, starters,
                    verbose=args.verbose, debug_wind=False,
                )
                if sigs["picks"]:
                    all_sig.append({"game": g, "sigs": sigs})
                elif sigs.get("avoid"):
                    all_avoid.append({"game": g, "sigs": sigs})
            all_sig.sort(key=lambda e: min(p["priority"]
                                          for p in e["sigs"]["picks"]))
            pick_entries_for_log = all_sig
            avoid_entries_for_log = all_avoid
        picks_count = 0
        for g in games:
            picks_count += len(
                evaluate_signals(
                    conn, g, streaks, session, starters,
                    verbose=args.verbose, debug_wind=False,
                )["picks"]
            )
        log_brief(conn, today, session, len(games), picks_count,
                  output_file, pick_entries=pick_entries_for_log, avoid_entries=avoid_entries_for_log, now=now)
        if args.verbose:
            print(f"  [verbose] brief_log entry written: {today} / {session} / {picks_count} picks")

        # Materialize bet_ledger from signal_state inside the 30-minute pregame window.
        if session != "prior":
            try:
                n_bets = generate_bets_from_signal_state(conn, today, now=now)
                if n_bets:
                    print(f"  ✓ bet_ledger: inserted {n_bets} row(s) from signal_state (pregame window).")
                elif args.verbose:
                    print(
                        "  [verbose] bet_ledger: no new rows "
                        "(outside 30m window, duplicate game_pk+market_type, or no top/next signals)."
                    )
            except Exception as e:
                print(f"  ⚠  bet_ledger sync failed (non-fatal): {e}")

    conn.close()
    print(f"\n  Done.\n")


if __name__ == "__main__":
    main()
