"""
usage_tracker.py
────────────────
Append-only call log + rolling quota snapshot for The Odds API.

Files (under BASE_DIR or repo root):
  logs/odds_api_calls.jsonl   — one JSON object per HTTP request
  logs/odds_api_usage.json    — latest quota snapshot for quick reads

Set ODDS_API_MONTHLY_LIMIT in config/.env to your plan size for
used/limit warnings (the API resets on your billing cycle, not calendar month).

Period stats (today / month / YTD) use America/New_York calendar boundaries.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator
from urllib.parse import urlparse

from core.utils.base_dir import resolve_base_path

log = logging.getLogger(__name__)

CALLS_LOG = resolve_base_path("logs", "odds_api_calls.jsonl")
USAGE_STATE = resolve_base_path("logs", "odds_api_usage.json")

try:
    from zoneinfo import ZoneInfo

    _ET = ZoneInfo("America/New_York")
except Exception:
    _ET = timezone(timedelta(hours=-4))


def quota_from_headers(headers: dict) -> dict:
    """Parse Odds API quota response headers."""
    return {
        "requests_last": int(headers.get("x-requests-last", 0) or 0),
        "requests_used_cumulative": int(headers.get("x-requests-used", 0) or 0),
        "requests_remaining": int(headers.get("x-requests-remaining", 0) or 0),
    }


def _endpoint_from_url(url: str) -> str:
    path = urlparse(url).path or url
    if "/events/" in path and path.endswith("/odds"):
        return "/events/{id}/odds"
    return path


def _monthly_limit() -> int | None:
    raw = (os.getenv("ODDS_API_MONTHLY_LIMIT") or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _ensure_log_dir() -> None:
    CALLS_LOG.parent.mkdir(parents=True, exist_ok=True)


def _iter_calls() -> Iterator[dict[str, Any]]:
    if not CALLS_LOG.exists():
        return
    with CALLS_LOG.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def _entry_at_et(entry: dict[str, Any]) -> datetime | None:
    raw = entry.get("at_utc")
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(_ET)
    except ValueError:
        return None


def compute_period_stats(now_et: datetime | None = None) -> dict[str, dict[str, Any]]:
    """Aggregate call counts and quota cost by ET calendar day, month, and year."""
    now_et = now_et or datetime.now(_ET)
    today = now_et.date()
    month_start = today.replace(day=1)
    year_start = today.replace(month=1, day=1)

    stats: dict[str, dict[str, Any]] = {
        "today": {
            "label": f"Today ({today.isoformat()} ET)",
            "date": today.isoformat(),
            "calls": 0,
            "cost": 0,
        },
        "month": {
            "label": now_et.strftime("%B %Y"),
            "month": today.strftime("%Y-%m"),
            "calls": 0,
            "cost": 0,
        },
        "ytd": {
            "label": f"{today.year} YTD",
            "year": today.year,
            "calls": 0,
            "cost": 0,
        },
        "all_time": {
            "label": "All time",
            "calls": 0,
            "cost": 0,
        },
    }

    for entry in _iter_calls():
        at_et = _entry_at_et(entry)
        if at_et is None:
            continue
        call_date = at_et.date()
        cost = int(entry.get("cost") or 0)

        stats["all_time"]["calls"] += 1
        stats["all_time"]["cost"] += cost

        if call_date == today:
            stats["today"]["calls"] += 1
            stats["today"]["cost"] += cost
        if call_date >= month_start:
            stats["month"]["calls"] += 1
            stats["month"]["cost"] += cost
        if call_date >= year_start:
            stats["ytd"]["calls"] += 1
            stats["ytd"]["cost"] += cost

    return stats


def record_call(
    url: str,
    status_code: int | None,
    headers: dict | None,
    *,
    caller: str = "api_get",
) -> None:
    """Append one API call to the log and refresh the quota snapshot."""
    headers = headers or {}
    quota = quota_from_headers(headers)
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")

    entry: dict[str, Any] = {
        "at_utc": now,
        "endpoint": _endpoint_from_url(url),
        "status": status_code,
        "cost": quota["requests_last"],
        "quota_used_cumulative": quota["requests_used_cumulative"],
        "quota_remaining": quota["requests_remaining"],
        "caller": caller,
    }

    _ensure_log_dir()
    with CALLS_LOG.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, separators=(",", ":")) + "\n")

    state = _read_state()
    state["updated_at_utc"] = now
    state["last_endpoint"] = entry["endpoint"]
    state["last_status"] = status_code
    state["total_calls"] = int(state.get("total_calls", 0)) + 1
    state["total_cost_logged"] = int(state.get("total_cost_logged", 0)) + quota["requests_last"]

    if quota["requests_used_cumulative"] > 0:
        state["quota_used_cumulative"] = quota["requests_used_cumulative"]
    if quota["requests_remaining"] > 0 or quota["requests_used_cumulative"] > 0:
        state["quota_remaining"] = quota["requests_remaining"]

    limit = _monthly_limit()
    if limit:
        state["monthly_limit_configured"] = limit

    _write_state(state)
    _maybe_warn(state)


def _read_state() -> dict:
    if not USAGE_STATE.exists():
        return {}
    try:
        return json.loads(USAGE_STATE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _write_state(state: dict) -> None:
    _ensure_log_dir()
    tmp = USAGE_STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    tmp.replace(USAGE_STATE)


def _maybe_warn(state: dict) -> None:
    remaining = state.get("quota_remaining")
    used = state.get("quota_used_cumulative")
    limit = state.get("monthly_limit_configured") or _monthly_limit()

    if remaining is not None and remaining <= 0:
        log.warning("Odds API quota exhausted (remaining=0).")
        return

    if remaining is not None and remaining < 500:
        log.warning("Odds API quota low: %s requests remaining.", f"{remaining:,}")

    if limit and used is not None and used >= limit * 0.9:
        log.warning(
            "Odds API usage at %.0f%% of configured monthly limit (%s / %s).",
            100 * used / limit,
            f"{used:,}",
            f"{limit:,}",
        )


def _format_period_line(label: str, calls: int, cost: int) -> str:
    return f"    {label:<22}  {calls:>5,} calls   {cost:>6,} cost units"


def get_usage_summary() -> dict:
    """Return current usage snapshot for CLI / UI."""
    state = _read_state()
    used = state.get("quota_used_cumulative")
    remaining = state.get("quota_remaining")
    limit = state.get("monthly_limit_configured") or _monthly_limit()

    inferred_limit = None
    if used is not None and remaining is not None and (used > 0 or remaining > 0):
        inferred_limit = used + remaining

    effective_limit = limit or inferred_limit
    pct_used = None
    if effective_limit and used is not None and effective_limit > 0:
        pct_used = round(100 * used / effective_limit, 1)

    periods = compute_period_stats()

    return {
        "updated_at_utc": state.get("updated_at_utc"),
        "quota_used_cumulative": used,
        "quota_remaining": remaining,
        "inferred_limit": inferred_limit,
        "monthly_limit_configured": limit,
        "effective_limit": effective_limit,
        "pct_used": pct_used,
        "total_calls_logged": periods["all_time"]["calls"],
        "total_cost_logged": periods["all_time"]["cost"],
        "periods": periods,
        "calls_log_path": str(CALLS_LOG),
        "state_path": str(USAGE_STATE),
    }


def format_usage_lines() -> list[str]:
    """Human-readable summary lines for logging / CLI."""
    s = get_usage_summary()
    lines = ["", "  Odds API usage (flat-file tracker):"]

    if not s["updated_at_utc"] and s["total_calls_logged"] == 0:
        lines.append("    No API calls logged yet.")
        lines.append(f"    Log file: {s['calls_log_path']}")
        limit = s["monthly_limit_configured"]
        if limit:
            lines.append(f"    Configured limit: {limit:,} (ODDS_API_MONTHLY_LIMIT)")
        else:
            lines.append("    Tip: set ODDS_API_MONTHLY_LIMIT in config/.env for limit warnings.")
        lines.append("")
        return lines

    used = s["quota_used_cumulative"]
    remaining = s["quota_remaining"]
    if used is not None and remaining is not None:
        lines.append(
            f"    Billing-period used : {used:,}  |  remaining : {remaining:,}"
        )
    elif used is not None:
        lines.append(f"    Billing-period used : {used:,}")
    elif remaining is not None:
        lines.append(f"    Quota remaining     : {remaining:,}")

    if s["effective_limit"]:
        pct = s["pct_used"]
        pct_str = f"  ({pct}%)" if pct is not None else ""
        src = "configured" if s["monthly_limit_configured"] else "inferred from API"
        lines.append(
            f"    Plan limit ({src}): {s['effective_limit']:,}{pct_str}"
        )

    lines.append("")
    lines.append("    Period totals (ET calendar):")
    periods = s["periods"]
    lines.append(_format_period_line(periods["today"]["label"], periods["today"]["calls"], periods["today"]["cost"]))
    lines.append(_format_period_line(periods["month"]["label"], periods["month"]["calls"], periods["month"]["cost"]))
    lines.append(_format_period_line(periods["ytd"]["label"], periods["ytd"]["calls"], periods["ytd"]["cost"]))
    lines.append(_format_period_line(periods["all_time"]["label"], periods["all_time"]["calls"], periods["all_time"]["cost"]))

    lines.append("")
    lines.append(f"    Last updated        : {s['updated_at_utc']} UTC")
    lines.append(f"    Call log            : {s['calls_log_path']}")
    lines.append("")
    return lines


def log_usage_summary(logger: logging.Logger | None = None) -> None:
    target = logger or log
    for line in format_usage_lines():
        if line:
            target.info(line)
        else:
            target.info("")
