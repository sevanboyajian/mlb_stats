"""
format_report.py
────────────────
Mobile-friendly plain-text report for score_today CSV output.
"""

from __future__ import annotations

import csv
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
MAX_WIDTH = 65

_REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ENGINE_LOG = _REPO_ROOT / "outputs" / "reports" / "prediction_engine_log.csv"

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _strip_ansi(text: str) -> str:
    return ANSI_RE.sub("", text or "")


def _as_bool(val: object) -> bool:
    if val in (True, 1):
        return True
    if val in (False, 0):
        return False
    s = str(val or "").strip().lower()
    if s in ("", "nan", "none", "false", "no", "0", "0.0"):
        return False
    try:
        return float(s) != 0.0
    except ValueError:
        return s in ("1", "true", "yes")


def _as_float(val: object) -> float | None:
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", ""):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _as_int(val: object) -> int | None:
    f = _as_float(val)
    if f is None:
        return None
    return int(round(f))


def _pct(val: object) -> float | None:
    f = _as_float(val)
    if f is None:
        return None
    if abs(f) <= 1.0:
        return f * 100.0
    return f


def _game_time_et(utc_str: object) -> str:
    if utc_str is None or str(utc_str).strip() == "":
        return "TBD"
    raw = str(utc_str).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return "TBD"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    et = dt.astimezone(ET)
    hour = et.hour % 12 or 12
    return f"{hour}:{et.minute:02d} {et.strftime('%p')} ET"


def _parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=ET)


def _date_header_parts(date_str: str) -> tuple[str, str, str, str, str]:
    dt = _parse_date(date_str)
    return (
        dt.strftime("%A"),
        dt.strftime("%b").upper(),
        dt.strftime("%d").lstrip("0") or "0",
        dt.strftime("%B"),
        dt.strftime("%Y"),
    )


def _now_et_label() -> str:
    now = datetime.now(ET)
    hour = now.hour % 12 or 12
    return f"{hour}:{now.minute:02d} {now.strftime('%p')} ET"


def _odds_tier_label(odds: float | None) -> str:
    if odds is None:
        return "unknown"
    if odds <= -300:
        return "-300 or worse"
    if -199 <= odds <= -150:
        return "-150 to -199"
    return "outside target range"


def _humanize_skip(reason: str, *, min_games: int) -> str | None:
    """Return plain-English skip reason, or None if early-season GP filter."""
    if not reason or reason == "PASS":
        return None
    parts = [p.strip() for p in reason.split(",") if p.strip()]
    out: list[str] = []
    for part in parts:
        if part.startswith("GP<"):
            return None
        if part.startswith("conf<"):
            out.append("Below confidence threshold")
        elif part == "odds_tier":
            out.append("Odds not in target range")
        elif part == "no_odds":
            out.append("No odds available")
        elif part.startswith("edge<"):
            out.append("Market priced higher than model")
        else:
            out.append(part)
    if not out:
        return reason
    if len(out) == 1:
        return out[0]
    return " + ".join(out)


def _early_season_skip(reason: str, *, min_games: int) -> bool:
    if not reason:
        return False
    return any(p.strip().startswith("GP<") for p in reason.split(","))


def _wind_display(row: dict[str, str]) -> str:
    direction = (row.get("wind_direction") or "").strip()
    if direction and direction.lower() not in ("nan", "none", ""):
        if "in" in direction.lower():
            return direction
        return direction
    return "Unknown"


def _load_rows(score_csv_path: str | Path) -> list[dict[str, str]]:
    path = Path(score_csv_path)
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _signal_counts(rows: list[dict[str, str]]) -> tuple[int, int, int]:
    ml = sum(1 for r in rows if _as_bool(r.get("actionable")))
    rl = sum(1 for r in rows if _as_bool(r.get("rl_signal")))
    under = sum(1 for r in rows if _as_bool(r.get("under_signal")))
    return ml, rl, under


def _format_odds(val: object) -> str:
    n = _as_int(val)
    if n is None:
        return "n/a"
    return f"{n:+d}"


def _bet_risk_units(odds: object) -> float:
    o = _as_float(odds)
    if o is None or o == 0:
        return 1.0
    o = float(o)
    if o < 0:
        return abs(o) / 100.0
    return 100.0 / o


def _tracking_stats(engine_log_path: Path) -> dict[str, dict[str, float | int]] | None:
    if not engine_log_path.is_file():
        return None
    with engine_log_path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return None

    buckets: dict[str, dict[str, float | int]] = {
        "ML": {"n": 0, "w": 0, "l": 0, "pl": 0.0, "roi_sum": 0.0},
        "RL": {"n": 0, "w": 0, "l": 0, "pl": 0.0, "roi_sum": 0.0},
        "UNDER": {"n": 0, "w": 0, "l": 0, "pl": 0.0, "roi_sum": 0.0},
    }
    for row in rows:
        st = (row.get("signal_type") or "").strip().upper()
        if st not in buckets:
            continue
        result = (row.get("result") or "").strip().upper()
        if result not in ("W", "WIN", "L", "LOSS"):
            continue
        pl = _as_float(row.get("pl_units")) or 0.0
        risk = _bet_risk_units(row.get("odds"))
        pick_roi = (pl / risk * 100.0) if risk else 0.0
        buckets[st]["n"] = int(buckets[st]["n"]) + 1
        buckets[st]["pl"] = float(buckets[st]["pl"]) + pl
        buckets[st]["roi_sum"] = float(buckets[st]["roi_sum"]) + pick_roi
        if result in ("W", "WIN"):
            buckets[st]["w"] = int(buckets[st]["w"]) + 1
        elif result in ("L", "LOSS"):
            buckets[st]["l"] = int(buckets[st]["l"]) + 1

    if not any(int(b["n"]) > 0 for b in buckets.values()):
        return None
    return buckets


def format_prediction_report(
    score_csv_path: str | Path,
    date_str: str,
    *,
    trained_on_season: int = 2024,
    min_games: int = 20,
    engine_log_path: str | Path | None = None,
) -> tuple[str, str]:
    """
    Read score_today CSV and return (subject, body) for email / file output.
    Plain ASCII, max ~65 chars per line, no ANSI codes.
    """
    rows = _load_rows(score_csv_path)
    ml_count, rl_count, under_count = _signal_counts(rows)
    total_signals = ml_count + rl_count + under_count

    dow, mon_short, day_num, month_long, year = _date_header_parts(date_str)
    subject = (
        f"MLB Scout — {total_signals} Signal(s) Today · "
        f"{dow} {month_long} {day_num.lstrip('0') or day_num}"
    )

    lines: list[str] = [
        "=" * 49,
        "MLB SCOUT — PREDICTION ENGINE DAILY REPORT",
        f"{dow}, {month_long} {day_num}, {year}",
        f"Generated: {_now_et_label()}",
        "=" * 49,
        f"Model: LogReg (trained {trained_on_season})  |  Filter: >={min_games} GP",
        "-" * 49,
        f"TODAY: {ml_count}ML  {rl_count}RL  {under_count}Under  signal(s) found",
        "=" * 49,
        "",
        "",
        "── MONEYLINE PICKS ──────────────────────────────",
        "[Backtest: -150/-199 tier → 72.7% acc, +14.7% ROI]",
        "[Backtest: -300+ tier    → 85.2% acc, +10.9% ROI]",
        "",
    ]

    ml_rows = [r for r in rows if _as_bool(r.get("actionable"))]
    ml_rows.sort(
        key=lambda r: (
            -(_pct(r.get("confidence")) or 0),
            -(_pct(r.get("edge")) or 0),
        )
    )
    if not ml_rows:
        lines.append("  No moneyline picks today.")
    else:
        for i, row in enumerate(ml_rows, start=1):
            away = row.get("away_team", "???")
            home = row.get("home_team", "???")
            conf = _pct(row.get("confidence")) or _pct(row.get("model_pct")) or 0.0
            mkt = _pct(row.get("market_prob")) or 0.0
            edge = _pct(row.get("edge")) or 0.0
            odds = _as_float(row.get("odds_used"))
            lines.extend([
                f"  #{i}  {away} @ {home}",
                f"  Pick:       {row.get('predicted_winner', '?')} ML {_format_odds(odds)}",
                f"  Confidence: {conf:.0f}%",
                f"  Market:     {mkt:.0f}% implied  (edge {edge:+.1f}%)",
                f"  Tier:       {_odds_tier_label(odds)}",
                f"  Game:       {_game_time_et(row.get('game_start_utc'))}",
                "",
            ])

    lines.extend([
        "── RUN LINE PICKS ───────────────────────────────",
        "[Backtest: ML <=-301 → 63.2% cover, +21.1% ROI]",
        "[Note: 2026 YTD only 3 games — small sample]",
        "",
    ])
    rl_rows = [r for r in rows if _as_bool(r.get("rl_signal"))]
    rl_rows.sort(key=lambda r: _as_float(r.get("favorite_ml")) or 0)
    if not rl_rows:
        lines.append("  No run line picks today.")
    else:
        for row in rl_rows:
            away = row.get("away_team", "???")
            home = row.get("home_team", "???")
            fav = row.get("favorite_team", "?")
            fav_ml = _format_odds(row.get("favorite_ml"))
            rl_odds = row.get("fav_rl_odds")
            lines.extend([
                f"  {away} @ {home}",
                f"  Pick:    {fav} -1.5  ({_format_odds(rl_odds)})",
                f"  Fav ML:  {fav_ml}",
                f"  Game:    {_game_time_et(row.get('game_start_utc'))}",
            ])
            if _as_float(rl_odds) is None:
                lines.append(
                    "  RL odds not yet posted — check DK before first pitch."
                )
            lines.append("")

    lines.extend([
        "── UNDER PICKS ──────────────────────────────────",
        "[Backtest: Combined ERA <6.0 → 44.6% under rate]",
        "[Strong:   Combined ERA <5.0 + wind in → 41.6%]",
        "",
    ])
    under_rows = [r for r in rows if _as_bool(r.get("under_signal"))]
    under_rows.sort(key=lambda r: _as_float(r.get("combined_era")) or 999)
    if not under_rows:
        lines.append("  No under picks today.")
    else:
        for row in under_rows:
            away = row.get("away_team", "???")
            home = row.get("home_team", "???")
            combined = _as_float(row.get("combined_era"))
            h_era = _as_float(row.get("hsp_era_wma"))
            a_era = _as_float(row.get("asp_era_wma"))
            total = _as_float(row.get("total_line"))
            total_s = f"{total:.1f}" if total is not None else "?"
            strong = _as_bool(row.get("under_signal_strong"))
            combined_s = f"{combined:.2f}" if combined is not None else "?"
            tag = " STRONG" if strong else ""
            h_line = (
                f"  Home SP:    {home} — {h_era:.2f} ERA WMA"
                if h_era is not None
                else f"  Home SP:    {home} — n/a ERA WMA"
            )
            a_line = (
                f"  Away SP:    {away} — {a_era:.2f} ERA WMA"
                if a_era is not None
                else f"  Away SP:    {away} — n/a ERA WMA"
            )
            lines.extend([
                f"  {away} @ {home}",
                f"  Pick:       UNDER {total_s}  ({_format_odds(row.get('under_odds'))})",
                h_line,
                a_line,
                f"  Combined:   {combined_s}{tag}",
                f"  Wind:       {_wind_display(row)}",
                f"  Game:       {_game_time_et(row.get('game_start_utc'))}",
                "",
            ])

    lines.extend([
        "",
        "── NO SIGNAL TODAY ──────────────────────────────",
        "",
    ])
    no_signal: list[dict[str, str]] = []
    for row in rows:
        if _as_bool(row.get("actionable")):
            continue
        if _as_bool(row.get("under_signal")):
            continue
        if _as_bool(row.get("rl_signal")):
            continue
        reason = row.get("skip_reason", "")
        if _early_season_skip(reason, min_games=min_games):
            continue
        no_signal.append(row)

    if not no_signal:
        lines.append("  All eligible games fired at least one signal.")
    else:
        for row in sorted(no_signal, key=lambda r: r.get("game_start_utc", "")):
            away = row.get("away_team", "???")
            home = row.get("home_team", "???")
            conf = _pct(row.get("confidence")) or 0.0
            edge = _pct(row.get("edge")) or 0.0
            skip = _humanize_skip(row.get("skip_reason", ""), min_games=min_games) or "No signal"
            lines.append(f"  {away} @ {home}  {_game_time_et(row.get('game_start_utc'))}")
            lines.append(f"    ML conf {conf:.0f}%  edge {edge:+.1f}%  [{skip}]")

    early: list[dict[str, str]] = [
        r for r in rows
        if _early_season_skip(r.get("skip_reason", ""), min_games=min_games)
    ]
    if early:
        lines.extend([
            "",
            "",
            "── SKIPPED (early season filter) ────────────────",
            "",
        ])
        for row in sorted(early, key=lambda r: r.get("game_start_utc", "")):
            away = row.get("away_team", "???")
            home = row.get("home_team", "???")
            lines.append(f"  {away} @ {home} — Under {min_games} GP threshold")

    log_path = Path(engine_log_path) if engine_log_path else DEFAULT_ENGINE_LOG
    stats = _tracking_stats(log_path)
    if stats:
        lines.extend([
            "",
            "",
            "── LIVE TRACKING (season to date) ──────────────",
            "",
        ])
        total_n = total_w = total_l = 0
        total_pl = 0.0
        labels = {"ML": "ML", "RL": "RL", "UNDER": "Under"}
        for key in ("ML", "RL", "UNDER"):
            b = stats[key]
            n = int(b["n"])
            w = int(b["w"])
            l = int(b["l"])
            pl = float(b["pl"])
            total_n += n
            total_w += w
            total_l += l
            total_pl += pl
            graded = w + l
            roi = (float(b["roi_sum"]) / graded) if graded else 0.0
            lines.append(
                f"  {labels[key]:<5} {n} picks  {w}W-{l}L  {roi:+.1f}% ROI"
            )
        lines.extend([
            "  " + "─" * 45,
            f"  Total: {total_n} picks  {total_w}W-{total_l}L  {total_pl:+.2f}u",
            "  Note: result/P&L blank for today's picks",
        ])

    lines.extend([
        "",
        "",
        "=" * 49,
        "Not financial advice. Verify all lines before",
        "placing any wager. Lines from last odds pull —",
        "confirm current prices at your bookmaker.",
        "=" * 49,
        f"MLB Scout  ·  outputs/reports/prediction_engine_{date_str}.txt",
    ])

    body = _strip_ansi("\n".join(lines))
    return subject, body
