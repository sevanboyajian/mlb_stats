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
SIGNAL_GROUP_ORDER = ("OWM", "MV-B", "MV-F", "LHP", "STREAK", "OTHER", "NON_OWM")

ALERT_THRESHOLDS: dict[str, tuple[int, float, str]] = {
    "OWM": (15, -10.0, "OWM underperforming: {roi:.1f}% ROI on {n} bets"),
    "MV-B": (8, -25.0, "MV-B alert: {roi:.1f}% ROI on {n} bets — gate review"),
    "MV-F": (8, -25.0, "MV-F alert: {roi:.1f}% ROI on {n} bets"),
    "LHP": (8, -25.0, "LHP alert: {roi:.1f}% ROI on {n} bets"),
    "STREAK": (8, -25.0, "Streak signal alert: {roi:.1f}% on {n} bets"),
}


def _normalize_signals_used(raw: str | None) -> str:
    if not raw:
        return ""
    s = str(raw).strip()
    if s.startswith("["):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return ", ".join(str(x) for x in arr)
        except Exception:
            pass
    return s


def _matches_signal_group(signals_text: str, group: str) -> bool:
    su = _normalize_signals_used(signals_text).upper()
    if group == "OWM":
        return "OWM" in su or "OFFENSE MATCHUP" in su
    if group == "MV-B":
        return "WIND BOOST" in su or "WIND →" in su or "WIND->" in su
    if group == "MV-F":
        return "WIND FADE" in su
    if group == "LHP":
        return "LHP" in su
    if group == "STREAK":
        return "STREAK" in su
    if group == "OTHER":
        return not any(
            _matches_signal_group(signals_text, g)
            for g in ("OWM", "MV-B", "MV-F", "LHP", "STREAK")
        )
    return False


def _stats_from_bets(bets: list[dict[str, Any]]) -> dict[str, Any]:
    n = len(bets)
    wins = sum(1 for b in bets if (b.get("result") or "").lower() == "win")
    losses = sum(1 for b in bets if (b.get("result") or "").lower() == "loss")
    pushes = sum(1 for b in bets if (b.get("result") or "").lower() == "push")
    pnl = sum(float(b.get("pnl_units") or 0.0) for b in bets)
    win_rate = (100.0 * wins / n) if n else 0.0
    roi_pct = (100.0 * pnl / n) if n else 0.0
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


def _apply_alert(group: str, stats: dict[str, Any]) -> dict[str, Any]:
    cfg = ALERT_THRESHOLDS.get(group)
    if cfg is None:
        return stats
    min_n, min_roi, tmpl = cfg
    if stats["bets"] >= min_n and stats["roi_pct"] < min_roi:
        stats = dict(stats)
        stats["alert"] = tmpl.format(
            roi=stats["roi_pct"],
            n=stats["bets"],
        )
    return stats


def _ensure_row_factory(conn: sqlite3.Connection) -> None:
    if conn.row_factory is not sqlite3.Row:
        conn.row_factory = sqlite3.Row


def _fetch_graded_v2_bets(
    conn: sqlite3.Connection,
    through_date: str,
) -> list[dict[str, Any]]:
    _ensure_row_factory(conn)
    rows = conn.execute(
        """
        SELECT
            bl.game_date,
            bl.result,
            bl.pnl_units,
            bs.signals_used
        FROM bet_ledger bl
        JOIN games g ON g.game_pk = bl.game_pk
        LEFT JOIN bet_snapshots bs
            ON bs.game_date = bl.game_date
           AND bs.game_pk = bl.game_pk
           AND bs.market_type = CASE lower(trim(coalesce(bl.market_type, '')))
                WHEN 'moneyline' THEN 'ML'
                WHEN 'total' THEN 'TOTAL'
                WHEN 'spread' THEN 'RL'
                WHEN 'runline' THEN 'RL'
                ELSE upper(trim(coalesce(bl.market_type, '')))
            END
        WHERE bl.game_date <= ?
          AND bl.game_date >= ?
          AND g.status = 'Final'
          AND bl.stake_units > 0
          AND lower(trim(coalesce(bl.signal_at_time, ''))) != 'avoid'
          AND lower(trim(coalesce(bl.result, ''))) IN ('win', 'loss', 'push')
        ORDER BY bl.game_date DESC, bl.id DESC
        """,
        (through_date, MODEL_V2_START_DATE),
    ).fetchall()
    bets: list[dict[str, Any]] = []
    for row in rows:
        bets.append(
            {
                "game_date": str(row["game_date"]),
                "result": str(row["result"] or ""),
                "pnl_units": float(row["pnl_units"] or 0.0),
                "signals_used": row["signals_used"],
            }
        )
    return bets


def compute_rolling_signal_performance(
    conn: sqlite3.Connection,
    through_date: str,
    lookback_bets: int = 20,
) -> dict[str, dict[str, Any]]:
    """
    Rolling performance per signal group over the last ``lookback_bets`` graded v2 bets.
    """
    all_bets = _fetch_graded_v2_bets(conn, through_date)
    out: dict[str, dict[str, Any]] = {}

    for group in ("OWM", "MV-B", "MV-F", "LHP", "STREAK", "OTHER"):
        group_bets = [
            b for b in all_bets if _matches_signal_group(str(b.get("signals_used") or ""), group)
        ][:lookback_bets]
        stats = _stats_from_bets(group_bets)
        out[group] = _apply_alert(group, stats)

    non_owm_bets = [
        b for b in all_bets if not _matches_signal_group(str(b.get("signals_used") or ""), "OWM")
    ][:lookback_bets]
    non_owm_stats = _stats_from_bets(non_owm_bets)
    if non_owm_stats["bets"] >= 10 and non_owm_stats["roi_pct"] < -30.0:
        non_owm_stats = dict(non_owm_stats)
        non_owm_stats["alert"] = (
            f"Non-OWM composite: {non_owm_stats['roi_pct']:.1f}% ROI on "
            f"{non_owm_stats['bets']} bets — moratorium review"
        )
    out["NON_OWM"] = non_owm_stats

    return out


def collect_alert_messages(signal_performance: dict[str, dict[str, Any]]) -> list[str]:
    alerts: list[str] = []
    for group in SIGNAL_GROUP_ORDER:
        msg = (signal_performance.get(group) or {}).get("alert")
        if msg:
            alerts.append(str(msg))
    return alerts


def _status_icon(stats: dict[str, Any], group: str) -> str:
    if stats.get("alert"):
        return "❌"
    min_n = ALERT_THRESHOLDS.get(group, (999, 0.0, ""))[0]
    if stats.get("bets", 0) < min_n:
        return "✅"
    if stats.get("roi_pct", 0.0) < 0:
        return "⚠"
    return "✅"


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
        "Signal    N    W    L   Win%    P&L     ROI%   Status",
    ]

    for group in ("OWM", "MV-B", "MV-F", "LHP", "STREAK", "OTHER"):
        stats = signal_performance.get(group) or {}
        icon = _status_icon(stats, group)
        body_lines.append(
            f"{group:<8}{int(stats.get('bets', 0)):>4}"
            f"{int(stats.get('wins', 0)):>5}{int(stats.get('losses', 0)):>5}"
            f"{float(stats.get('win_rate', 0.0)):>6.0f}%"
            f"{float(stats.get('pnl_units', 0.0)):>+7.2f}u"
            f"{float(stats.get('roi_pct', 0.0)):>+7.1f}%"
            f"   {icon}"
        )

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
    alerts = collect_alert_messages(signal_performance)
    season_stats = _season_stats(conn, game_date)
    day_staked = _day_staked_summary(conn, game_date)
    additional_selections = _additional_selection_lines(conn, game_date, limit=3)
    v2_roi_pct = float((season_stats.get("v2") or {}).get("roi") or 0.0)

    enriched = dict(record)
    enriched.update(
        {
            "signal_performance": signal_performance,
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
    Trailing 7-day summary from grading_log plus current signal rollups.
    """
    _ensure_row_factory(conn)
    end_d = date.fromisoformat(as_of_date)
    start_d = end_d - timedelta(days=6)
    start_s = start_d.isoformat()

    rows = conn.execute(
        """
        SELECT game_date, wins, losses, pushes, pnl_units, alert_count, alerts_json, v2_roi_pct
        FROM grading_log
        WHERE game_date >= ? AND game_date <= ?
        ORDER BY game_date ASC
        """,
        (start_s, as_of_date),
    ).fetchall()

    week_wins = sum(int(r["wins"] or 0) for r in rows)
    week_losses = sum(int(r["losses"] or 0) for r in rows)
    week_pushes = sum(int(r["pushes"] or 0) for r in rows)
    week_pnl = sum(float(r["pnl_units"] or 0.0) for r in rows)
    week_bets = week_wins + week_losses + week_pushes
    week_roi = (100.0 * week_pnl / week_bets) if week_bets else 0.0

    week_alerts: list[str] = []
    for r in rows:
        try:
            arr = json.loads(r["alerts_json"] or "[]")
            if isinstance(arr, list):
                week_alerts.extend(str(x) for x in arr)
        except Exception:
            pass

    signal_performance = compute_rolling_signal_performance(conn, as_of_date)
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
        f"  Bets graded days: {len(rows)}",
        f"  Record: {week_wins}W-{week_losses}L-{week_pushes}P",
        f"  P&L (flat 1u): {week_pnl:+.4f}u  ROI: {week_roi:+.1f}%",
        "",
        "V2 SEASON TREND",
        f"  Current v2 ROI: {v2_roi_today:+.1f}%",
        f"  vs ~7d ago: {v2_roi_7d_ago:+.1f}%  ({trend_str})",
        "",
        "SIGNAL PERFORMANCE (rolling last 20 bets per signal)",
        dash,
        "Signal    N    W    L   Win%    P&L     ROI%   Status",
    ]
    for group in ("OWM", "MV-B", "MV-F", "LHP", "STREAK", "OTHER"):
        stats = signal_performance.get(group) or {}
        icon = _status_icon(stats, group)
        lines.append(
            f"{group:<8}{int(stats.get('bets', 0)):>4}"
            f"{int(stats.get('wins', 0)):>5}{int(stats.get('losses', 0)):>5}"
            f"{float(stats.get('win_rate', 0.0)):>6.0f}%"
            f"{float(stats.get('pnl_units', 0.0)):>+7.2f}u"
            f"{float(stats.get('roi_pct', 0.0)):>+7.1f}%"
            f"   {icon}"
        )

    lines.extend(["", f"ALERTS THIS WEEK ({len(week_alerts)})", dash])
    if week_alerts:
        for a in week_alerts:
            lines.append(f"  · {a}")
    else:
        lines.append("  None recorded in grading_log this week")

    non_owm = signal_performance.get("NON_OWM") or {}
    if non_owm.get("alert"):
        lines.extend(["", "COMPOSITE", f"  · {non_owm['alert']}"])

    lines.extend(["", bar, "  EDUCATIONAL USE ONLY — NOT FINANCIAL ADVICE", bar])
    return "\n".join(lines)
