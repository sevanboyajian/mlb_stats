#!/usr/bin/env python3
"""
signal_timing_analysis.py
─────────────────────────
Analyze when model signals fire, when bets are committed, and whether
late-session briefs (through ~10 PM ET) capture material actionable volume.

USAGE:
  python batch/analysis/prediction/signal_timing_analysis.py
  python batch/analysis/prediction/signal_timing_analysis.py \\
      --start-date 2026-04-28 --end-date 2026-06-03 --output-csv
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import connect as db_connect, get_db_path

ET = ZoneInfo("America/New_York")
DEFAULT_OUTPUT_DIR = "outputs/reports"
DEFAULT_START_DATE = "2026-04-28"
DEFAULT_CUTOFF_HOURS = (18, 19, 20, 21, 22)

HOURS_BEFORE_BUCKETS = [
    ("0-1h", 0, 1),
    ("1-2h", 1, 2),
    ("2-4h", 2, 4),
    ("4-6h", 4, 6),
    ("6-12h", 6, 12),
    ("12h+", 12, 9999),
]

ET_HOUR_BUCKETS = list(range(6, 23))  # 6 AM … 10 PM

SESSION_ORDER = ("morning", "early", "afternoon", "primary", "late", "closing")

PRODUCT_SIGNAL_LABELS = {
    "AWAY_DOG_RL": "Away Dog RL",
    "UNDER": "Under",
    "OWM": "OWM",
    "ML": "ML",
    "RL": "RL",
}


def resolve_path(path: str) -> Path:
    p = Path(path)
    return p if p.is_absolute() else _REPO_ROOT / p


def today_et() -> str:
    return datetime.now(tz=ET).date().isoformat()


def parse_et_timestamp(raw: str | None) -> datetime | None:
    """Parse ``YYYY-MM-DD HH:MM ET`` (production format)."""
    if not raw:
        return None
    s = str(raw).strip()
    m = re.match(
        r"^(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{2})(?:\s+ET)?$",
        s,
        re.IGNORECASE,
    )
    if not m:
        return None
    try:
        d = date.fromisoformat(m.group(1))
        return datetime(
            d.year, d.month, d.day, int(m.group(2)), int(m.group(3)), tzinfo=ET,
        )
    except (TypeError, ValueError):
        return None


def parse_game_start_et(raw: str | None) -> datetime | None:
    """``game_start_utc`` is stored as UTC ISO (often without ``Z``)."""
    if not raw or "T" not in str(raw):
        return None
    try:
        s = str(raw).rstrip("Z")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        return dt.astimezone(ET)
    except (TypeError, ValueError):
        return None


def hours_before_first_pitch(event_et: datetime | None, start_et: datetime | None) -> float | None:
    if event_et is None or start_et is None:
        return None
    return round((start_et - event_et).total_seconds() / 3600.0, 2)


def bucket_hours_before(h: float | None) -> str:
    if h is None or pd.isna(h):
        return "unknown"
    for label, lo, hi in HOURS_BEFORE_BUCKETS:
        if lo <= h < hi:
            return label
    return "unknown"


def derive_session_from_hour(hour: int) -> str:
    if hour < 12:
        return "morning"
    if hour < 15:
        return "early"
    if hour < 18:
        return "afternoon"
    if hour < 21:
        return "primary"
    return "late"


def normalize_product_signal(
    signal_type: str | None,
    signal_at_time: str | None,
    brief_signal: str | None,
) -> str:
    st = (signal_type or "").strip().upper()
    if st in PRODUCT_SIGNAL_LABELS:
        return PRODUCT_SIGNAL_LABELS[st]
    sat = str(signal_at_time or "")
    if sat.startswith("score_today:"):
        tag = sat.split(":", 1)[1].strip().upper()
        if tag in PRODUCT_SIGNAL_LABELS:
            return PRODUCT_SIGNAL_LABELS[tag]
    bs = (brief_signal or "").strip().upper()
    if "AWAY DOG" in bs:
        return "Away Dog RL"
    if "UNDER" in bs:
        return "Under"
    if bs == "OWM":
        return "OWM"
    if st in ("TOP", "NEXT"):
        return st.title()
    return "Other"


def print_timezone_diagnostic(con: sqlite3.Connection) -> list[str]:
    lines = ["TIMEZONE DIAGNOSTIC", "-" * 60]
    lines.append(
        "Assumption: signal_state.recorded_at and bet_ledger.placed_at are "
        "Eastern (``… ET`` suffix). game_start_utc is UTC ISO → converted to ET."
    )
    try:
        rows = con.execute(
            """
            SELECT ss.recorded_at, g.game_start_utc, ta.abbreviation, th.abbreviation
            FROM signal_state ss
            JOIN games g ON g.game_pk = ss.game_pk
            JOIN teams ta ON ta.team_id = g.away_team_id
            JOIN teams th ON th.team_id = g.home_team_id
            WHERE ss.recorded_at IS NOT NULL AND g.game_start_utc IS NOT NULL
            ORDER BY ss.id DESC
            LIMIT 3
            """
        ).fetchall()
        for rec, start, away, home in rows:
            rec_et = parse_et_timestamp(rec)
            start_et = parse_game_start_et(start)
            hb = hours_before_first_pitch(rec_et, start_et)
            lines.append(
                f"  sample: {away}@{home}  recorded_at={rec!r}  "
                f"start_utc={start!r}  → hours_before_fp={hb}"
            )
    except sqlite3.Error as exc:
        lines.append(f"  (diagnostic query failed: {exc})")
    lines.append("")
    return lines


def load_signals(con: sqlite3.Connection, start_date: str, end_date: str) -> pd.DataFrame:
    sql = """
    SELECT
        ss.id              AS signal_id,
        ss.game_date,
        ss.game_pk,
        ss.market_type,
        ss.signal_type     AS rank_signal_type,
        ss.bet,
        ss.session,
        ss.recorded_at,
        g.game_start_utc,
        g.game_date_et,
        ta.abbreviation    AS away_team,
        th.abbreviation    AS home_team,
        bp.signal          AS brief_signal,
        bl.id              AS bet_ledger_id,
        bl.placed_at       AS bet_placed_at,
        bl.stake_units,
        bl.signal_type     AS bet_signal_type,
        bl.signal_at_time,
        bl.result,
        bl.pnl_units,
        bl.source          AS bet_source
    FROM signal_state ss
    JOIN games g  ON g.game_pk = ss.game_pk
    JOIN teams th ON th.team_id = g.home_team_id
    JOIN teams ta ON ta.team_id = g.away_team_id
    LEFT JOIN brief_picks bp
        ON bp.game_date = ss.game_date
       AND bp.game_pk = ss.game_pk
       AND bp.session = ss.session
       AND TRIM(COALESCE(bp.bet, '')) = TRIM(COALESCE(ss.bet, ''))
    LEFT JOIN bet_ledger bl
        ON bl.game_date = ss.game_date
       AND bl.game_pk = ss.game_pk
       AND bl.market_type = ss.market_type
       AND TRIM(COALESCE(bl.bet, '')) = TRIM(COALESCE(ss.bet, ''))
       AND bl.stake_units > 0
       AND lower(trim(coalesce(bl.signal_at_time, ''))) != 'avoid'
    WHERE g.game_type = 'R'
      AND g.game_date_et BETWEEN ? AND ?
      AND ss.signal_type IN ('top', 'next')
    ORDER BY ss.recorded_at
    """
    df = pd.read_sql_query(sql, con, params=(start_date, end_date))
    if df.empty:
        return df

    df["recorded_et"] = df["recorded_at"].map(parse_et_timestamp)
    df["start_et"] = df["game_start_utc"].map(parse_game_start_et)
    df["hours_before_fp"] = [
        hours_before_first_pitch(r, s)
        for r, s in zip(df["recorded_et"], df["start_et"])
    ]
    df["signal_et_hour"] = df["recorded_et"].map(
        lambda x: x.hour if x is not None else np.nan
    )
    df["session_norm"] = df.apply(
        lambda r: (
            str(r["session"]).strip().lower()
            if pd.notna(r["session"]) and str(r["session"]).strip()
            else derive_session_from_hour(int(r["signal_et_hour"]))
            if pd.notna(r["signal_et_hour"])
            else "unknown"
        ),
        axis=1,
    )
    df["product_signal"] = df.apply(
        lambda r: normalize_product_signal(
            r.get("bet_signal_type"),
            r.get("signal_at_time"),
            r.get("brief_signal"),
        ),
        axis=1,
    )
    df["was_bet"] = df["bet_ledger_id"].notna()
    df["bet_placed_et"] = df["bet_placed_at"].map(parse_et_timestamp)
    df["hours_bet_before_fp"] = [
        hours_before_first_pitch(b, s)
        for b, s in zip(df["bet_placed_et"], df["start_et"])
    ]
    df["hours_before_bucket"] = df["hours_before_fp"].map(bucket_hours_before)
    return df


def load_bets(con: sqlite3.Connection, start_date: str, end_date: str) -> pd.DataFrame:
    sql = """
    SELECT
        bl.id,
        bl.game_date,
        bl.game_pk,
        bl.market_type,
        bl.bet,
        bl.session,
        bl.placed_at,
        bl.late_signal,
        bl.signal_at_time,
        bl.signal_type,
        bl.source,
        bl.result,
        bl.pnl_units,
        bl.stake_units,
        g.game_start_utc,
        g.game_date_et,
        ta.abbreviation AS away_team,
        th.abbreviation AS home_team,
        bp.signal       AS brief_signal
    FROM bet_ledger bl
    JOIN games g  ON g.game_pk = bl.game_pk
    JOIN teams th ON th.team_id = g.home_team_id
    JOIN teams ta ON ta.team_id = g.away_team_id
    LEFT JOIN brief_picks bp
        ON bp.game_date = bl.game_date
       AND bp.game_pk = bl.game_pk
       AND bp.session = bl.session
       AND TRIM(COALESCE(bp.bet, '')) = TRIM(COALESCE(bl.bet, ''))
    WHERE g.game_type = 'R'
      AND bl.game_date BETWEEN ? AND ?
      AND bl.stake_units > 0
      AND lower(trim(coalesce(bl.signal_at_time, ''))) != 'avoid'
      AND bl.result IS NOT NULL
      AND TRIM(bl.result) != ''
    ORDER BY bl.placed_at
    """
    df = pd.read_sql_query(sql, con, params=(start_date, end_date))
    if df.empty:
        return df

    df["placed_et"] = df["placed_at"].map(parse_et_timestamp)
    df["start_et"] = df["game_start_utc"].map(parse_game_start_et)
    df["hours_before_fp"] = [
        hours_before_first_pitch(p, s)
        for p, s in zip(df["placed_et"], df["start_et"])
    ]
    df["placed_et_hour"] = df["placed_et"].map(
        lambda x: x.hour if x is not None else np.nan
    )
    df["session_norm"] = df.apply(
        lambda r: (
            str(r["session"]).strip().lower()
            if pd.notna(r["session"]) and str(r["session"]).strip()
            else derive_session_from_hour(int(r["placed_et_hour"]))
            if pd.notna(r["placed_et_hour"])
            else "unknown"
        ),
        axis=1,
    )
    df["product_signal"] = df.apply(
        lambda r: normalize_product_signal(
            r.get("signal_type"),
            r.get("signal_at_time"),
            r.get("brief_signal"),
        ),
        axis=1,
    )
    df["hours_before_bucket"] = df["hours_before_fp"].map(bucket_hours_before)
    df["first_pitch_hour"] = df["start_et"].map(
        lambda x: x.hour if x is not None else np.nan
    )
    df["late_game_8pm"] = df["first_pitch_hour"] >= 20
    return df


def load_brief_log(con: sqlite3.Connection, start_date: str, end_date: str) -> pd.DataFrame:
    try:
        return pd.read_sql_query(
            """
            SELECT game_date, session, generated_at, picks_count, games_covered
            FROM brief_log
            WHERE game_date BETWEEN ? AND ?
            ORDER BY game_date, generated_at
            """,
            con,
            params=(start_date, end_date),
        )
    except sqlite3.Error:
        return pd.DataFrame()


def pct_table(counts: pd.Series, total: int) -> list[str]:
    lines = []
    for k, v in counts.items():
        pct = 100.0 * v / total if total else 0.0
        lines.append(f"  {str(k):<16} {int(v):>5}  ({pct:5.1f}%)")
    return lines


def section_signal_timing(signals: pd.DataFrame) -> list[str]:
    lines = ["SECTION 1 — SIGNAL TIMING DISTRIBUTION", "=" * 60]
    n = len(signals)
    lines.append(f"Actionable signal_state rows (top/next): {n}")
    if n == 0:
        lines.append("  (no data)")
        lines.append("")
        return lines

    neg = int((signals["hours_before_fp"] < 0).sum())
    if neg:
        lines.append(
            f"  NOTE: {neg} signals have negative hours_before_fp "
            "(after first pitch or parse mismatch)."
        )

    lines.append("")
    lines.append("By ET hour (signal recorded_at):")
    hour_counts = (
        signals["signal_et_hour"]
        .dropna()
        .astype(int)
        .value_counts()
        .reindex(ET_HOUR_BUCKETS, fill_value=0)
    )
    for h, c in hour_counts.items():
        if c:
            lines.append(f"  {h:02d}:00 ET   {int(c):>5}")

    lines.append("")
    lines.append("By brief session:")
    sess = signals["session_norm"].value_counts()
    lines.extend(pct_table(sess, n))

    lines.append("")
    lines.append("By hours-before-first-pitch:")
    hb = signals["hours_before_bucket"].value_counts()
    for label, _, _ in HOURS_BEFORE_BUCKETS:
        lines.append(f"  {label:<8} {int(hb.get(label, 0)):>5}")

    lines.append("")
    lines.append("By product signal type:")
    ps = signals["product_signal"].value_counts()
    lines.extend(pct_table(ps, n))

    # Late games (FP >= 8 PM ET): first signal within 2h?
    late_games = signals[signals["start_et"].map(lambda x: x.hour >= 20 if x else False)]
    if not late_games.empty:
        lines.append("")
        lines.append("Late games (first pitch >= 8 PM ET):")
        lines.append(f"  Signal rows on late-start games: {len(late_games)}")
        within_2h = int((late_games["hours_before_fp"].between(0, 2, inclusive="both")).sum())
        lines.append(
            f"  Signals within 2h of first pitch: {within_2h} "
            f"({100 * within_2h / len(late_games):.1f}%)"
        )
        first_sess = (
            late_games.sort_values("recorded_et")
            .groupby(["game_pk", "market_type"], as_index=False)
            .first()[["session_norm"]]
            .value_counts()
        )
        lines.append("  First-seen session (per game+market):")
        for k, v in first_sess.items():
            lines.append(f"    {k[0]:<12} {int(v)}")

    lines.append("")
    return lines


def section_bet_timing(bets: pd.DataFrame) -> list[str]:
    lines = ["SECTION 2 — BET COMMITMENT TIMING", "=" * 60]
    n = len(bets)
    lines.append(f"Graded staked bets: {n}")
    if n == 0:
        lines.append("  (no data)")
        lines.append("")
        return lines

    lines.append("")
    lines.append("By ET hour (bet placed_at):")
    hour_counts = (
        bets["placed_et_hour"]
        .dropna()
        .astype(int)
        .value_counts()
        .reindex(ET_HOUR_BUCKETS, fill_value=0)
    )
    for h, c in hour_counts.items():
        if c:
            lines.append(f"  {h:02d}:00 ET   {int(c):>5}")

    lines.append("")
    lines.append("By session:")
    lines.extend(pct_table(bets["session_norm"].value_counts(), n))

    lines.append("")
    lines.append("By hours-before-first-pitch:")
    hb = bets["hours_before_bucket"].value_counts()
    for label, _, _ in HOURS_BEFORE_BUCKETS:
        lines.append(f"  {label:<8} {int(hb.get(label, 0)):>5}")

    late_flag = int(bets["late_signal"].fillna(0).astype(int).sum())
    lines.append("")
    lines.append(
        f"late_signal=1 (T−30 window): {late_flag} / {n} "
        f"({100 * late_flag / n:.1f}%)"
    )

    lines.append("")
    lines.append("P&L by session (graded):")
    lines.append(f"  {'session':<14} {'n':>4} {'W':>4} {'L':>4} {'units':>8} {'ROI%':>7}")
    for sess in list(SESSION_ORDER) + ["unknown", "score_today"]:
        sub = bets[bets["session_norm"] == sess]
        if sub.empty and sess != "score_today":
            sub = bets[bets["source"] == sess] if sess == "score_today" else sub
        if sess == "score_today":
            sub = bets[bets["source"] == "score_today"]
        if sub.empty:
            continue
        w = int((sub["result"] == "win").sum())
        l = int((sub["result"] == "loss").sum())
        units = float(sub["pnl_units"].fillna(0).sum())
        roi = 100.0 * units / len(sub) if len(sub) else 0.0
        lines.append(
            f"  {sess:<14} {len(sub):>4} {w:>4} {l:>4} {units:>+8.2f} {roi:>7.1f}"
        )

    lines.append("")
    return lines


def _cutoff_dt(placed_et: datetime, cutoff_hour: int) -> datetime:
    return placed_et.replace(hour=cutoff_hour, minute=0, second=0, microsecond=0)


def section_cutoff_simulation(
    bets: pd.DataFrame,
    cutoff_hours: tuple[int, ...],
) -> list[str]:
    lines = ["SECTION 3 — CUTOFF SIMULATION", "=" * 60]
    lines.append(
        "If brief generation stops at cutoff ET, bets placed AFTER that "
        "time on the same calendar day are 'missed'."
    )
    n = len(bets)
    if n == 0:
        lines.append("  (no graded bets)")
        lines.append("")
        return lines

    total_units = float(bets["pnl_units"].fillna(0).sum())
    lines.append("")
    lines.append(
        f"  {'Cutoff':<10} {'Captured':>8} {'Missed':>7} {'Miss%':>6} "
        f"{'Missed P&L':>11} {'Miss W%':>8} {'Miss late-game%':>16}"
    )
    lines.append("  " + "-" * 72)

    for ch in cutoff_hours:
        label = _cutoff_label(ch)

        missed_mask = []
        for _, row in bets.iterrows():
            pe = row["placed_et"]
            if pe is None:
                missed_mask.append(False)
                continue
            cut = _cutoff_dt(pe, ch)
            missed_mask.append(pe > cut)

        missed = bets[missed_mask]
        captured = bets[~np.array(missed_mask)]
        m_n = len(missed)
        miss_pct = 100.0 * m_n / n
        miss_units = float(missed["pnl_units"].fillna(0).sum())
        miss_w = int((missed["result"] == "win").sum())
        miss_w_pct = 100.0 * miss_w / m_n if m_n else 0.0
        late_game_miss = missed[missed["late_game_8pm"] == True]  # noqa: E712
        late_pct = 100.0 * len(late_game_miss) / m_n if m_n else 0.0

        lines.append(
            f"  {label:<10} {len(captured):>8} {m_n:>7} {miss_pct:>5.1f}% "
            f"{miss_units:>+10.2f}u {miss_w_pct:>7.1f}% {late_pct:>15.1f}%"
        )

    lines.append("")
    lines.append(f"Season graded P&L in range: {total_units:+.2f}u across {n} bets")
    lines.append("")
    return lines


def section_late_game_value(signals: pd.DataFrame, bets: pd.DataFrame) -> list[str]:
    lines = ["SECTION 4 — LATE GAME SIGNAL VALUE (West Coast / FP >= 8 PM ET)", "=" * 60]

    late_sig = signals[
        signals["start_et"].map(lambda x: x.hour >= 20 if x is not None else False)
    ]
    late_bets = bets[bets["late_game_8pm"] == True]  # noqa: E712

    if late_sig.empty and late_bets.empty:
        lines.append("  No late-start games in range.")
        lines.append("")
        return lines

    n_games = late_sig["game_pk"].nunique() if not late_sig.empty else 0
    sig_per_game = len(late_sig) / n_games if n_games else 0.0
    lines.append(f"Late-start games (FP >= 8 PM ET): {n_games}")
    lines.append(f"Avg signal_state rows per late game: {sig_per_game:.2f}")

    if not late_sig.empty:
        first = (
            late_sig.sort_values("recorded_et")
            .groupby("game_pk", as_index=False)
            .first()
        )
        primary_late = first["session_norm"].isin(["primary", "late"]).sum()
        lines.append(
            f"First signal in primary/late session: {primary_late}/{len(first)} "
            f"({100 * primary_late / len(first):.1f}%)"
        )

    if not late_bets.empty:
        w = int((late_bets["result"] == "win").sum())
        l = int((late_bets["result"] == "loss").sum())
        units = float(late_bets["pnl_units"].fillna(0).sum())
        roi = 100.0 * units / len(late_bets)
        lines.append("")
        lines.append(f"Late-game graded bets: {len(late_bets)}  {w}W-{l}L  "
                     f"{units:+.2f}u  ROI {roi:.1f}%")

        after_9pm = late_bets[late_bets["placed_et_hour"] >= 21]
        lines.append(
            f"Bets placed >= 9 PM ET on late-start games: {len(after_9pm)} "
            f"({100 * len(after_9pm) / len(late_bets):.1f}% of late-game bets)"
        )
    else:
        lines.append("Late-game graded bets: 0")

    lines.append("")
    lines.append(
        "West Coast-only brief at 9 PM ET would capture bets placed >= 21:00 "
        "on games not yet started (see Section 3 cutoff at 9 PM)."
    )
    lines.append("")
    return lines


def _cutoff_label(ch: int) -> str:
    if ch == 0:
        return "12 AM ET"
    if ch < 12:
        return f"{ch} AM ET"
    if ch == 12:
        return "12 PM ET"
    return f"{ch - 12} PM ET"


def section_recommendation(bets: pd.DataFrame, cutoff_hours: tuple[int, ...]) -> list[str]:
    lines = ["RECOMMENDATION", "=" * 60]
    n = len(bets)
    if n == 0:
        lines.append("Insufficient graded bet data for recommendation.")
        lines.append("")
        return lines

    total_units = float(bets["pnl_units"].fillna(0).sum())

    best_cutoff = None
    best_count_pct = 0.0
    best_pnl_pct = 0.0
    for ch in sorted(cutoff_hours):
        captured_n = 0
        captured_units = 0.0
        for _, row in bets.iterrows():
            pe = row["placed_et"]
            if pe is None:
                continue
            if pe <= _cutoff_dt(pe, ch):
                captured_n += 1
                captured_units += float(row["pnl_units"] or 0)
        count_pct = captured_n / n
        pnl_pct = (
            (captured_units / total_units)
            if abs(total_units) > 1e-6
            else count_pct
        )
        # Primary gate: bet count (actionable volume). P&L ratio shown for context only.
        meets = count_pct >= 0.95
        if meets:
            best_cutoff = ch
            best_count_pct = count_pct
            best_pnl_pct = pnl_pct
            break

    if best_cutoff is None:
        lines.append(
            "No cutoff hour in range captures >=95% of bets/P&L — "
            "late briefs through 10 PM ET appear material."
        )
    else:
        lines.append(
            f">=95% capture at cutoff {_cutoff_label(best_cutoff)} "
            f"({best_count_pct:.1%} of bets by count; "
            f"{best_pnl_pct:.1%} of season P&L in range)."
        )
        lines.append(
            f"Minimum schedule: run briefs through at least "
            f"{_cutoff_label(best_cutoff)} to capture ~95% of actionable bets."
        )

    late_pct = 100.0 * bets["late_signal"].fillna(0).astype(int).sum() / n
    lines.append(
        f"late_signal flag (T-30): {late_pct:.1f}% of graded bets — "
        + (
            "non-trivial last-minute volume justifies late sessions."
            if late_pct >= 5.0
            else "limited last-minute volume; weigh ops cost vs edge."
        )
    )
    lines.append("")
    return lines


def write_detail_csv(path: Path, signals: pd.DataFrame) -> None:
    if signals.empty:
        return
    cols = [
        "game_date",
        "away_team",
        "home_team",
        "game_pk",
        "session_norm",
        "product_signal",
        "rank_signal_type",
        "recorded_at",
        "signal_et_hour",
        "hours_before_fp",
        "was_bet",
        "bet_placed_at",
        "hours_bet_before_fp",
        "result",
        "pnl_units",
    ]
    out = signals[[c for c in cols if c in signals.columns]].copy()
    path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(path, index=False)


def write_report(
    path: Path,
    *,
    start_date: str,
    end_date: str,
    signals: pd.DataFrame,
    bets: pd.DataFrame,
    brief_log: pd.DataFrame,
    cutoff_hours: tuple[int, ...],
    diag_lines: list[str],
) -> None:
    lines = [
        "SIGNAL TIMING ANALYSIS",
        "=" * 60,
        f"Date range: {start_date} → {end_date}",
        f"Signals analyzed (top/next): {len(signals)}",
        f"Graded bets analyzed: {len(bets)}",
        f"Brief log rows: {len(brief_log)}",
        "",
    ]
    lines.extend(diag_lines)

    if not brief_log.empty:
        lines.append("BRIEF SESSION RUN TIMES (brief_log)")
        lines.append("-" * 60)
        for _, r in brief_log.head(30).iterrows():
            lines.append(
                f"  {r['game_date']}  {str(r['session']):<12}  "
                f"{r['generated_at']}  picks={r['picks_count']}"
            )
        if len(brief_log) > 30:
            lines.append(f"  … ({len(brief_log) - 30} more rows)")
        lines.append("")

    lines.extend(section_signal_timing(signals))
    lines.extend(section_bet_timing(bets))
    lines.extend(section_cutoff_simulation(bets, cutoff_hours))
    lines.extend(section_late_game_value(signals, bets))
    lines.extend(section_recommendation(bets, cutoff_hours))

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Analyze signal/bet timing relative to first pitch and brief cutoffs.",
    )
    p.add_argument("--db", default=get_db_path(), help="SQLite database path")
    p.add_argument("--start-date", default=DEFAULT_START_DATE)
    p.add_argument("--end-date", default=None, help="Default: today ET")
    p.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Report output directory",
    )
    p.add_argument(
        "--output-csv",
        action="store_true",
        help="Write signal_timing_detail.csv",
    )
    p.add_argument(
        "--cutoff-hours",
        type=int,
        nargs="+",
        default=list(DEFAULT_CUTOFF_HOURS),
        help="ET hour integers for cutoff simulation (default: 18-22 = 6-10 PM)",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    end_date = args.end_date or today_et()
    out_dir = resolve_path(args.output_dir)
    report_path = out_dir / "signal_timing_analysis.txt"
    csv_path = out_dir / "signal_timing_detail.csv"
    cutoff_hours = tuple(sorted(set(args.cutoff_hours)))

    con = db_connect(args.db)
    try:
        diag = print_timezone_diagnostic(con)
        signals = load_signals(con, args.start_date, end_date)
        bets = load_bets(con, args.start_date, end_date)
        brief_log = load_brief_log(con, args.start_date, end_date)
    finally:
        con.close()

    write_report(
        report_path,
        start_date=args.start_date,
        end_date=end_date,
        signals=signals,
        bets=bets,
        brief_log=brief_log,
        cutoff_hours=cutoff_hours,
        diag_lines=diag,
    )

    if args.output_csv:
        write_detail_csv(csv_path, signals)

    print(f"[signal_timing] Report -> {report_path}")
    print(f"[signal_timing] Signals={len(signals)} graded_bets={len(bets)}")
    if args.output_csv:
        print(f"[signal_timing] Detail CSV -> {csv_path}")

    # Echo Section 3 & 4 highlights
    for tag in ("SECTION 3", "SECTION 4", "RECOMMENDATION"):
        print(f"\n--- {tag} (preview) ---")
        text = report_path.read_text(encoding="utf-8")
        chunk = text.split(tag, 1)
        if len(chunk) > 1:
            preview = chunk[1].split("\n\n", 1)[0]
            safe = (tag + preview[:1200]).encode("ascii", errors="replace").decode("ascii")
            print(safe)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
