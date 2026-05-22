"""Parse bet labels from brief / ledger text for grading."""

from __future__ import annotations

import sqlite3


def strip_avoid_bet_label(bet_text: str) -> str:
    """Strip leading 'Avoid:' / 'Avoid ' so ML/total parsers can grade counterfactuals."""
    s = (bet_text or "").strip()
    su = s.upper()
    if su.startswith("AVOID "):
        return s[6:].strip()
    if su.startswith("AVOID:"):
        return s[6:].strip()
    return s


def coalesce_float(*vals: object) -> float | None:
    for v in vals:
        if v is None:
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return None


def total_line_for_totals_grading(row: sqlite3.Row) -> float | None:
    """Ledger / brief picks line first; last pre-start snapshot last (never is_closing_line)."""
    return coalesce_float(
        row["ledger_total_line_at_bet"],
        row["bp_total_line_at_bet"],
        row["bp_total_line"],
        row["pre_start_total_line"],
    )


def parse_total_bet(bet_text: str) -> tuple[str | None, float | None]:
    s = (bet_text or "").strip().upper()
    if not s:
        return None, None
    side = "over" if s.startswith("OVER") else ("under" if s.startswith("UNDER") else None)
    if side is None:
        return None, None
    try:
        num = float(s.split()[1])
        return side, num
    except Exception:
        return side, None


def parse_runline_bet(bet_text: str) -> tuple[str | None, float | None]:
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


def parse_ml_team(bet_text: str) -> str | None:
    s = (bet_text or "").strip().upper()
    if not s:
        return None
    parts = s.split()
    if not parts:
        return None
    return parts[0]
