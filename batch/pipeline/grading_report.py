"""
grading_report.py
─────────────────
Signal performance rollups, threshold alerts, and email report formatting
for the daily grading agent.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Any

MODEL_V2_START_DATE = "2026-04-28"

SIGNAL_DISPLAY_MAP: dict[str, str] = {
    "OWM": "OWM",
    "MV-B": "MV-B",
    "MV-F": "MV-F",
    "LHP": "LHP",
    "STREAK": "STREAK",
    "AWAY_DOG_RL": "Away Dog RL",
    "ML": "ML (LogReg)",
    "RL": "RL (favorite)",
    "UNDER": "Under",
    "LEGACY": "Legacy",
    "UNCLASSIFIED": "Unclassified",
}

SIGNAL_DISPLAY_ORDER: list[str] = [
    "OWM",
    "AWAY_DOG_RL",
    "ML",
    "UNDER",
    "RL",
    "MV-B",
    "MV-F",
    "LHP",
    "STREAK",
    "LEGACY",
    "UNCLASSIFIED",
]

ROLLING_EXCLUDE = frozenset({"LEGACY", "UNCLASSIFIED"})
STATIC_SIGNALS = frozenset({"LEGACY"})
LIVE_SIGNALS_EXCLUDE = frozenset({"LEGACY", "MV-B"})

ALERT_THRESHOLDS: dict[str, dict[str, float | int]] = {
    "OWM": {"min_n": 10, "min_win_pct": 50.0},
    "MV-B": {"min_n": 10, "min_win_pct": 50.0},
    "AWAY_DOG_RL": {"min_n": 15, "min_win_pct": 55.0},
    "ML": {"min_n": 10, "min_win_pct": 65.0},
    "UNDER": {"min_n": 15, "min_win_pct": 48.0},
    "LHP": {"min_n": 5, "min_win_pct": 45.0},
    "STREAK": {"min_n": 5, "min_win_pct": 45.0},
}

# Back-compat alias for weekly alert ordering
SIGNAL_GROUP_ORDER = SIGNAL_DISPLAY_ORDER


def _signal_label(signal_type: str | None) -> str:
    key = (signal_type or "").strip().upper()
    if not key:
        return "Unknown"
    return SIGNAL_DISPLAY_MAP.get(key, signal_type or "Unknown")


def _norm_bet(bet: str | None) -> str:
    return " ".join(str(bet or "").upper().split())


def _src_rank(source: str | None) -> int:
    s = (source or "brief").strip().lower()
    if s == "brief":
        return 0
    if s == "brief_late":
        return 1
    if s == "score_today":
        return 2
    return 3


def _ensure_row_factory(conn: sqlite3.Connection) -> None:
    if conn.row_factory is not sqlite3.Row:
        conn.row_factory = sqlite3.Row


def _fetch_graded_bets(
    conn: sqlite3.Connection,
    through_date: str,
    *,
    season_year: int | None = None,
) -> list[dict[str, Any]]:
    """Graded staked bet_ledger rows through ``through_date`` (optionally one season)."""
    _ensure_row_factory(conn)
    params: list[Any] = [through_date]
    season_clause = ""
    if season_year is not None:
        season_clause = " AND substr(bl.game_date, 1, 4) = ?"
        params.append(str(season_year))

    rows = conn.execute(
        f"""
        SELECT
            bl.id,
            bl.game_date,
            bl.game_pk,
            bl.market_type,
            bl.bet,
            bl.result,
            bl.pnl_units,
            bl.stake_units,
            bl.signal_type,
            bl.source,
            bl.model_version
        FROM bet_ledger bl
        JOIN games g ON g.game_pk = bl.game_pk
        WHERE bl.game_date <= ?
          {season_clause}
          AND bl.stake_units > 0
          AND lower(trim(coalesce(bl.signal_at_time, ''))) != 'avoid'
          AND lower(trim(coalesce(bl.result, ''))) IN ('win', 'loss', 'push')
        ORDER BY bl.game_date DESC, bl.id DESC
        """,
        tuple(params),
    ).fetchall()

    bets: list[dict[str, Any]] = []
    for row in rows:
        sig = (row["signal_type"] or "").strip().upper() or None
        bets.append(
            {
                "id": int(row["id"]),
                "game_date": str(row["game_date"]),
                "game_pk": int(row["game_pk"]),
                "market_type": str(row["market_type"] or ""),
                "bet": str(row["bet"] or ""),
                "result": str(row["result"] or ""),
                "pnl_units": float(row["pnl_units"] or 0.0),
                "stake_units": float(row["stake_units"] or 0.0),
                "signal_type": sig,
                "source": str(row["source"] or "brief"),
                "model_version": str(row["model_version"] or ""),
            }
        )
    return bets


def _dedupe_graded_bets(bets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    One row per equivalent staked pick (game_pk + market + bet text).
    Prefer brief over score_today when both logged the same bet.
    """
    by_key: dict[tuple[int, str, str], dict[str, Any]] = {}
    for b in bets:
        key = (int(b["game_pk"]), str(b["market_type"]), _norm_bet(b["bet"]))
        prev = by_key.get(key)
        if prev is None or _src_rank(b["source"]) < _src_rank(prev["source"]):
            by_key[key] = b
    return list(by_key.values())


def _stats_from_bets(bets: list[dict[str, Any]]) -> dict[str, Any]:
    n = len(bets)
    wins = sum(1 for b in bets if (b.get("result") or "").lower() == "win")
    losses = sum(1 for b in bets if (b.get("result") or "").lower() == "loss")
    pushes = sum(1 for b in bets if (b.get("result") or "").lower() == "push")
    pnl = sum(float(b.get("pnl_units") or 0.0) for b in bets)
    stake = sum(float(b.get("stake_units") or 0.0) for b in bets)
    win_rate = (100.0 * wins / n) if n else 0.0
    roi_pct = (100.0 * pnl / stake) if stake else 0.0
    last_bet_date = bets[0]["game_date"] if bets else None
    return {
        "bets": n,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "win_rate": win_rate,
        "pnl_units": pnl,
        "roi_pct": roi_pct,
        "last_bet_date": last_bet_date,
        "alert": None,
    }


def _apply_alert(signal_type: str, stats: dict[str, Any]) -> dict[str, Any]:
    cfg = ALERT_THRESHOLDS.get(signal_type)
    if cfg is None:
        return stats
    min_n = int(cfg["min_n"])
    min_win_pct = float(cfg["min_win_pct"])
    if stats["bets"] >= min_n and stats["win_rate"] < min_win_pct:
        stats = dict(stats)
        label = _signal_label(signal_type)
        stats["alert"] = (
            f"{label} underperforming: {stats['win_rate']:.1f}% win rate on "
            f"{stats['bets']} bets (threshold {min_win_pct:.0f}%)"
        )
    return stats


def _bets_for_signal(
    bets: list[dict[str, Any]],
    signal_type: str,
    *,
    v2_only: bool = False,
) -> list[dict[str, Any]]:
    st = signal_type.strip().upper()
    out = [
        b
        for b in bets
        if (b.get("signal_type") or "").upper() == st
        and (not v2_only or str(b.get("game_date") or "") >= MODEL_V2_START_DATE)
    ]
    out.sort(key=lambda x: (x["game_date"], x["id"]), reverse=True)
    return out


def compute_rolling_signal_performance(
    conn: sqlite3.Connection,
    through_date: str,
    lookback_bets: int = 20,
) -> dict[str, dict[str, Any]]:
    """
    Rolling performance per ``bet_ledger.signal_type`` (last N graded bets each).
    LEGACY uses full history; LEGACY/UNCLASSIFIED excluded from rolling window sizing.
    """
    season_year = int(str(through_date)[:4])
    all_bets = _dedupe_graded_bets(_fetch_graded_bets(conn, through_date, season_year=season_year))
    out: dict[str, dict[str, Any]] = {}

    for signal_type in SIGNAL_DISPLAY_ORDER:
        pool = _bets_for_signal(all_bets, signal_type, v2_only=False)
        if signal_type in STATIC_SIGNALS:
            window = pool
        elif signal_type in ROLLING_EXCLUDE:
            window = []
        else:
            window = pool[:lookback_bets]
        stats = _stats_from_bets(window)
        out[signal_type] = _apply_alert(signal_type, stats)

    # Any signal_type in data but not in display order (future types)
    seen = {b.get("signal_type") for b in all_bets if b.get("signal_type")}
    for raw in sorted(seen - set(SIGNAL_DISPLAY_ORDER)):
        pool = _bets_for_signal(all_bets, str(raw))
        if raw in STATIC_SIGNALS:
            window = pool
        elif raw in ROLLING_EXCLUDE:
            window = []
        else:
            window = pool[:lookback_bets]
        stats = _stats_from_bets(window)
        out[str(raw)] = _apply_alert(str(raw), stats)

    return out


def compute_season_signal_totals(
    conn: sqlite3.Connection,
    through_date: str,
) -> dict[str, Any]:
    """Cumulative graded P&L per signal_type for the season through ``through_date``."""
    season_year = int(str(through_date)[:4])
    all_bets = _dedupe_graded_bets(_fetch_graded_bets(conn, through_date, season_year=season_year))
    by_signal: dict[str, dict[str, Any]] = {}

    for signal_type in SIGNAL_DISPLAY_ORDER:
        pool = _bets_for_signal(all_bets, signal_type)
        by_signal[signal_type] = _stats_from_bets(pool)

    seen = {b.get("signal_type") for b in all_bets if b.get("signal_type")}
    for raw in sorted(seen - set(SIGNAL_DISPLAY_ORDER)):
        by_signal[str(raw)] = _stats_from_bets(_bets_for_signal(all_bets, str(raw)))

    live_bets = [
        b
        for b in all_bets
        if (b.get("signal_type") or "") not in LIVE_SIGNALS_EXCLUDE
    ]
    live_stats = _stats_from_bets(live_bets)

    return {"by_signal": by_signal, "live_signals": live_stats}


def collect_alert_messages(signal_performance: dict[str, dict[str, Any]]) -> list[str]:
    alerts: list[str] = []
    for signal_type in SIGNAL_DISPLAY_ORDER:
        msg = (signal_performance.get(signal_type) or {}).get("alert")
        if msg:
            alerts.append(str(msg))
    for key, stats in signal_performance.items():
        if key in SIGNAL_DISPLAY_ORDER:
            continue
        msg = (stats or {}).get("alert")
        if msg:
            alerts.append(str(msg))
    return alerts


def _status_icon(stats: dict[str, Any], signal_type: str) -> str:
    n = int(stats.get("bets", 0))
    cfg = ALERT_THRESHOLDS.get(signal_type)
    if cfg is None or n < int(cfg["min_n"]):
        return "—"
    if stats.get("alert"):
        return "❌"
    win_rate = float(stats.get("win_rate", 0.0))
    min_win = float(cfg["min_win_pct"])
    if win_rate >= min_win:
        return "✅"
    return "⚠"


def _format_signal_table_rows(
    signal_performance: dict[str, dict[str, Any]],
    *,
    label_width: int = 18,
) -> list[str]:
    lines: list[str] = []
    rendered: set[str] = set()

    def _row(signal_type: str, stats: dict[str, Any]) -> str:
        label = _signal_label(signal_type)
        icon = _status_icon(stats, signal_type)
        return (
            f"{label:<{label_width}}"
            f"{int(stats.get('bets', 0)):>4}"
            f"{int(stats.get('wins', 0)):>5}"
            f"{int(stats.get('losses', 0)):>5}"
            f"{float(stats.get('win_rate', 0.0)):>6.1f}%"
            f"{float(stats.get('pnl_units', 0.0)):>+7.2f}u"
            f"{float(stats.get('roi_pct', 0.0)):>+7.1f}%"
            f"   {icon}"
        )

    for signal_type in SIGNAL_DISPLAY_ORDER:
        stats = signal_performance.get(signal_type) or _stats_from_bets([])
        lines.append(_row(signal_type, stats))
        rendered.add(signal_type)

    for signal_type in sorted(signal_performance.keys()):
        if signal_type in rendered:
            continue
        stats = signal_performance[signal_type]
        lines.append(_row(signal_type, stats))

    return lines


def _format_season_totals_section(season_totals: dict[str, Any]) -> list[str]:
    dash = "─" * 54
    by_signal = season_totals.get("by_signal") or {}
    live = season_totals.get("live_signals") or _stats_from_bets([])

    lines = [
        "",
        "SEASON TOTALS (all graded bets)",
        dash,
        f"{'Signal':<18}{'N':>4}{'W':>5}{'L':>5}{'Win%':>7}{'P&L':>8}{'ROI%':>8}",
    ]

    for signal_type in SIGNAL_DISPLAY_ORDER:
        stats = by_signal.get(signal_type) or _stats_from_bets([])
        label = _signal_label(signal_type)
        lines.append(
            f"{label:<18}"
            f"{int(stats.get('bets', 0)):>4}"
            f"{int(stats.get('wins', 0)):>5}"
            f"{int(stats.get('losses', 0)):>5}"
            f"{float(stats.get('win_rate', 0.0)):>6.1f}%"
            f"{float(stats.get('pnl_units', 0.0)):>+7.2f}u"
            f"{float(stats.get('roi_pct', 0.0)):>+7.1f}%"
        )

    lines.extend(
        [
            dash,
            (
                f"{'LIVE SIGNALS':<18}"
                f"{int(live.get('bets', 0)):>4}"
                f"{int(live.get('wins', 0)):>5}"
                f"{int(live.get('losses', 0)):>5}"
                f"{float(live.get('win_rate', 0.0)):>6.1f}%"
                f"{float(live.get('pnl_units', 0.0)):>+7.2f}u"
                f"{float(live.get('roi_pct', 0.0)):>+7.1f}%"
            ),
            "(excl. Legacy + MV-B)",
        ]
    )
    return lines


def _week_bet_ledger_summary(
    conn: sqlite3.Connection,
    week_start: str,
    as_of_date: str,
) -> dict[str, Any]:
    """Staked graded bets in ``[week_start, as_of_date]`` from bet_ledger (complete source)."""
    _ensure_row_factory(conn)
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS bets,
            SUM(CASE WHEN lower(trim(coalesce(bl.result,''))) = 'win' THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN lower(trim(coalesce(bl.result,''))) = 'loss' THEN 1 ELSE 0 END) AS losses,
            SUM(CASE WHEN lower(trim(coalesce(bl.result,''))) = 'push' THEN 1 ELSE 0 END) AS pushes,
            ROUND(COALESCE(SUM(bl.pnl_units), 0), 4) AS pnl_units
        FROM bet_ledger bl
        JOIN games g ON g.game_pk = bl.game_pk
        WHERE g.game_date_et BETWEEN ? AND ?
          AND g.game_type = 'R'
          AND bl.stake_units > 0
          AND lower(trim(coalesce(bl.signal_at_time,''))) != 'avoid'
          AND lower(trim(coalesce(bl.result,''))) IN ('win', 'loss', 'push')
        """,
        (week_start, as_of_date),
    ).fetchone()
    bets = int(row["bets"] or 0)
    pnl = float(row["pnl_units"] or 0.0)
    return {
        "bets": bets,
        "wins": int(row["wins"] or 0),
        "losses": int(row["losses"] or 0),
        "pushes": int(row["pushes"] or 0),
        "pnl_units": pnl,
        "roi_pct": (100.0 * pnl / bets) if bets else 0.0,
    }


def _week_slate_days(conn: sqlite3.Connection, week_start: str, as_of_date: str) -> int:
    _ensure_row_factory(conn)
    row = conn.execute(
        """
        SELECT COUNT(DISTINCT g.game_date_et) AS days
        FROM bet_ledger bl
        JOIN games g ON g.game_pk = bl.game_pk
        WHERE g.game_date_et BETWEEN ? AND ?
          AND g.game_type = 'R'
          AND bl.stake_units > 0
        """,
        (week_start, as_of_date),
    ).fetchone()
    return int(row["days"] or 0)


def _alert_category_key(alert: str) -> str:
    """Stable key for deduplicating alert messages that differ only by rolling stats."""
    s = (alert or "").strip().lower()
    for signal_type in SIGNAL_DISPLAY_ORDER:
        label = _signal_label(signal_type).lower()
        if label in s or signal_type.lower() in s:
            return signal_type
    return s


def _collect_weekly_alerts_from_grading_log(
    conn: sqlite3.Connection,
    week_start: str,
    as_of_date: str,
) -> list[str]:
    _ensure_row_factory(conn)
    rows = conn.execute(
        """
        SELECT alerts_json
        FROM grading_log
        WHERE game_date >= ? AND game_date <= ?
        ORDER BY game_date ASC, id ASC
        """,
        (week_start, as_of_date),
    ).fetchall()
    latest_by_category: dict[str, str] = {}
    for r in rows:
        try:
            arr = json.loads(r["alerts_json"] or "[]")
            if not isinstance(arr, list):
                continue
            for alert in arr:
                msg = str(alert)
                latest_by_category[_alert_category_key(msg)] = msg
        except Exception:
            pass
    ordered: list[str] = []
    for group in SIGNAL_DISPLAY_ORDER:
        msg = latest_by_category.get(group)
        if msg:
            ordered.append(msg)
    for key, msg in latest_by_category.items():
        if key not in SIGNAL_DISPLAY_ORDER and msg not in ordered:
            ordered.append(msg)
    return ordered


def _active_alerts_for_week(
    conn: sqlite3.Connection,
    as_of_date: str,
    *,
    week_start: str,
) -> list[str]:
    """
    Alerts for the weekly report: current rolling alerts at ``as_of_date``,
    falling back to deduped grading_log history for the window.
    """
    signal_performance = compute_rolling_signal_performance(conn, as_of_date)
    active = collect_alert_messages(signal_performance)
    if active:
        return active
    return _collect_weekly_alerts_from_grading_log(conn, week_start, as_of_date)


def _season_stats(conn: sqlite3.Connection, game_date: str) -> dict[str, Any]:
    from batch.pipeline.generate_daily_brief import ledger_season_staked_graded_stats

    season_int = int(str(game_date)[:4])
    total = ledger_season_staked_graded_stats(conn, season_int, game_date)
    v2 = ledger_season_staked_graded_stats(conn, season_int, game_date, model_version="v2")
    legacy = ledger_season_staked_graded_stats(conn, season_int, game_date, model_version="legacy")
    return {"total": total, "v2": v2, "legacy": legacy}


def _day_staked_summary(conn: sqlite3.Connection, game_date: str) -> dict[str, Any]:
    _ensure_row_factory(conn)
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS bets,
            SUM(CASE WHEN lower(trim(coalesce(result,''))) = 'win' THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN lower(trim(coalesce(result,''))) = 'loss' THEN 1 ELSE 0 END) AS losses,
            SUM(CASE WHEN lower(trim(coalesce(result,''))) = 'push' THEN 1 ELSE 0 END) AS pushes,
            ROUND(COALESCE(SUM(pnl_units), 0), 4) AS pnl_units
        FROM bet_ledger
        WHERE game_date = ?
          AND stake_units > 0
          AND lower(trim(coalesce(signal_at_time,''))) != 'avoid'
          AND lower(trim(coalesce(result,''))) IN ('win', 'loss', 'push')
        """,
        (game_date,),
    ).fetchone()
    bets = int(row["bets"] or 0)
    pnl = float(row["pnl_units"] or 0.0)
    return {
        "bets": bets,
        "wins": int(row["wins"] or 0),
        "losses": int(row["losses"] or 0),
        "pushes": int(row["pushes"] or 0),
        "pnl_units": pnl,
        "roi_pct": (100.0 * pnl / bets) if bets else 0.0,
    }


def _additional_selection_lines(conn: sqlite3.Connection, game_date: str, limit: int = 3) -> list[str]:
    from batch.pipeline.generate_daily_brief import get_additional_model_selections

    lines: list[str] = []
    try:
        rows = get_additional_model_selections(conn, game_date, None, limit=limit)
    except Exception:
        rows = []
    for row in rows[:limit]:
        away = row.get("away_team") or "?"
        home = row.get("home_team") or "?"
        side = row.get("bet_label") or row.get("best_side") or "—"
        score = row.get("score")
        eval_status = row.get("eval_status") or "—"
        edge = row.get("edge")
        edge_s = f"{float(edge) * 100:.1f}%" if edge is not None else "N/A"
        lines.append(
            f"  · {away} @ {home}: {side} score={score} "
            f"status={eval_status} edge={edge_s}"
        )
    if not lines:
        lines.append("  (none)")
    return lines


def format_email_report(
    record: dict[str, Any],
    signal_performance: dict[str, dict[str, Any]],
    game_date: str,
) -> tuple[str, str]:
    """Return (subject_line, plain_text_body) for the grading email."""
    season = record.get("season_stats") or {}
    season_totals = record.get("season_signal_totals") or {}
    day = record.get("day_staked") or {}
    total = season.get("total") or {}
    v2 = season.get("v2") or {}
    legacy = season.get("legacy") or {}
    alerts = record.get("alerts") or collect_alert_messages(signal_performance)
    v2_roi = float(v2.get("roi") or record.get("v2_roi_pct") or 0.0)

    day_wins = int(day.get("wins", record.get("wins", 0)))
    day_losses = int(day.get("losses", record.get("losses", 0)))

    subject = (
        f"MLB Scout · Grading {game_date} · {day_wins}W-{day_losses}L · v2 {v2_roi:+.1f}%"
    )
    if alerts:
        subject = f"⚠ {subject}"

    bar = "═" * 56
    dash = "─" * 54
    generated_at = record.get("graded_at") or "—"
    season_year = str(game_date)[:4]

    body_lines = [
        bar,
        f"  MLB SCOUT · DAILY GRADING REPORT · {game_date}",
        f"  Generated {generated_at}",
        bar,
        "",
        "DAY RESULT",
        f"  Staked: {int(day.get('bets', 0))}  {day_wins}W-{day_losses}L",
        f"  P&L (flat 1u): {float(day.get('pnl_units', record.get('pnl_units', 0.0))):+.4f}u",
        "",
        f"SEASON-TO-DATE ({season_year})",
        (
            f"  Total:  {int(total.get('n', 0))} bets  "
            f"{int(total.get('wins', 0))}W-{int(total.get('losses', 0))}L"
        ),
        (
            f"          {float(total.get('units', 0.0)):+.4f}u  "
            f"ROI: {float(total.get('roi', 0.0)):+.1f}%"
        ),
        (
            f"  v2:     {int(v2.get('n', 0))} bets  "
            f"{int(v2.get('wins', 0))}W-{int(v2.get('losses', 0))}L"
        ),
        (
            f"          {float(v2.get('units', 0.0)):+.4f}u  "
            f"ROI: {float(v2.get('roi', 0.0)):+.1f}%"
        ),
        (
            f"  Legacy: {int(legacy.get('n', 0))} bets  "
            f"{float(legacy.get('units', 0.0)):+.4f}u"
        ),
        "",
        "SIGNAL PERFORMANCE (rolling last 20 bets per signal)",
        dash,
        f"{'Signal':<18}{'N':>4}{'W':>5}{'L':>5}{'Win%':>7}{'P&L':>8}{'ROI%':>8}  Status",
    ]
    body_lines.extend(_format_signal_table_rows(signal_performance))
    body_lines.extend(_format_season_totals_section(season_totals))

    body_lines.extend(
        [
            "",
            f"⚠ ALERTS  ({len(alerts)} active)",
            dash,
        ]
    )
    if alerts:
        body_lines.extend(f"  · {a}" for a in alerts)
    else:
        body_lines.append("  None — all signals within threshold")

    body_lines.extend(
        [
            "",
            "ADDITIONAL MODEL SELECTIONS (non-staked, for context)",
            dash,
        ]
    )
    extra = record.get("additional_selections")
    if extra is None:
        body_lines.append("  (not loaded)")
    else:
        body_lines.extend(extra)

    body_lines.extend(
        [
            "",
            bar,
            "  EDUCATIONAL USE ONLY — NOT FINANCIAL ADVICE",
            bar,
        ]
    )
    return subject, "\n".join(body_lines)


def build_report_context(
    conn: sqlite3.Connection,
    record: dict[str, Any],
    game_date: str,
) -> dict[str, Any]:
    """Enrich a grading run record with stats used by format_email_report."""
    signal_performance = compute_rolling_signal_performance(conn, game_date)
    season_signal_totals = compute_season_signal_totals(conn, game_date)
    alerts = collect_alert_messages(signal_performance)
    season_stats = _season_stats(conn, game_date)
    day_staked = _day_staked_summary(conn, game_date)
    additional_selections = _additional_selection_lines(conn, game_date, limit=3)
    v2_roi_pct = float((season_stats.get("v2") or {}).get("roi") or 0.0)

    enriched = dict(record)
    enriched.update(
        {
            "signal_performance": signal_performance,
            "season_signal_totals": season_signal_totals,
            "alerts": alerts,
            "alert_count": len(alerts),
            "alerts_json": json.dumps(alerts),
            "season_stats": season_stats,
            "day_staked": day_staked,
            "v2_roi_pct": v2_roi_pct,
            "additional_selections": additional_selections,
        }
    )
    return enriched


def write_grading_report_file(game_date: str, body: str, repo_root: Path) -> Path:
    out_dir = repo_root / "outputs" / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"grading-{game_date}.txt"
    path.write_text(body, encoding="utf-8")
    return path


def build_weekly_signal_report(
    conn: sqlite3.Connection,
    as_of_date: str,
) -> str:
    """
    Trailing 7-day summary: week W-L from bet_ledger, alerts from grading_log.
    """
    _ensure_row_factory(conn)
    end_d = date.fromisoformat(as_of_date)
    start_s = (end_d - timedelta(days=6)).isoformat()

    week = _week_bet_ledger_summary(conn, start_s, as_of_date)
    slate_days = _week_slate_days(conn, start_s, as_of_date)
    week_alerts = _active_alerts_for_week(conn, as_of_date, week_start=start_s)

    signal_performance = compute_rolling_signal_performance(conn, as_of_date)
    season_totals = compute_season_signal_totals(conn, as_of_date)
    season = _season_stats(conn, as_of_date)
    v2_roi_today = float((season.get("v2") or {}).get("roi") or 0.0)

    week_ago = (end_d - timedelta(days=7)).isoformat()
    prior = conn.execute(
        """
        SELECT v2_roi_pct FROM grading_log
        WHERE game_date <= ?
        ORDER BY game_date DESC, id DESC
        LIMIT 1
        """,
        (week_ago,),
    ).fetchone()
    v2_roi_7d_ago = float(prior["v2_roi_pct"]) if prior and prior["v2_roi_pct"] is not None else v2_roi_today
    trend = v2_roi_today - v2_roi_7d_ago
    trend_str = f"{'↑' if trend > 0 else '↓'} {abs(trend):.1f}pp this week"

    bar = "═" * 56
    dash = "─" * 54
    lines = [
        bar,
        f"  MLB SCOUT · WEEKLY SIGNAL SUMMARY · through {as_of_date}",
        bar,
        "",
        f"WEEK ({start_s} → {as_of_date})",
        f"  Slate days in window: {slate_days}",
        f"  Record: {week['wins']}W-{week['losses']}L-{week['pushes']}P",
        f"  P&L (flat 1u): {week['pnl_units']:+.4f}u  ROI: {week['roi_pct']:+.1f}%",
        "",
        "V2 SEASON TREND",
        f"  Current v2 ROI: {v2_roi_today:+.1f}%",
        f"  vs ~7d ago: {v2_roi_7d_ago:+.1f}%  ({trend_str})",
        "",
        "SIGNAL PERFORMANCE (rolling last 20 bets per signal)",
        dash,
        f"{'Signal':<18}{'N':>4}{'W':>5}{'L':>5}{'Win%':>7}{'P&L':>8}{'ROI%':>8}  Status",
    ]
    lines.extend(_format_signal_table_rows(signal_performance))
    lines.extend(_format_season_totals_section(season_totals))

    lines.extend(["", f"ALERTS THIS WEEK ({len(week_alerts)})", dash])
    if week_alerts:
        for a in week_alerts:
            lines.append(f"  · {a}")
    else:
        lines.append("  None recorded in grading_log this week")

    lines.extend(["", bar, "  EDUCATIONAL USE ONLY — NOT FINANCIAL ADVICE", bar])
    return "\n".join(lines)
