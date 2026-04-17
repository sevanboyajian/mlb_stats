"""
Fully Dressed Game — enrichment schema v1.0
===========================================
Standalone enrichment layer for signal design / backtests. Safe to run in
parallel with generate_daily_brief.py (no writes).

Spec: GameIdentifiers + GameEnvironment with env_ceiling and H3b gate.
Wind direction labeling aligns with generate_daily_brief.wind_direction_label().
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, replace
from typing import Any

try:
    from zoneinfo import ZoneInfo

    _ET = ZoneInfo("America/New_York")
except Exception:
    _ET = dt.timezone(dt.timedelta(hours=-4))


def _wind_direction_label(direction: str | None) -> str:
    """Match generate_daily_brief.wind_direction_label (OUT/IN/CROSS/CALM or pass-through)."""
    if not direction:
        return "UNKNOWN"
    d = str(direction).upper()
    if any(k in d for k in ("OUT", "BLOWING OUT")):
        return "OUT"
    if any(k in d for k in ("IN", "BLOWING IN")):
        return "IN"
    if any(k in d for k in ("CROSS", "LEFT", "RIGHT")):
        return "CROSS"
    if any(k in d for k in ("CALM", "STILL", "0 MPH")):
        return "CALM"
    return "UNKNOWN"


def _normalize_wind_dir_label(raw: str) -> str:
    u = (raw or "").upper().strip()
    if u in ("OUT", "IN", "CROSS", "CALM"):
        return u
    return "UNKNOWN"


def _fmt_game_start_et(game_start_utc: str | None) -> str:
    if not game_start_utc or "T" not in str(game_start_utc):
        raise ValueError("game_start_utc missing or invalid")
    raw = str(game_start_utc).strip().rstrip("Z")
    d = dt.datetime.fromisoformat(raw)
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.astimezone(_ET).strftime("%Y-%m-%d %H:%M ET")


@dataclass(frozen=True)
class GameIdentifiers:
    game_pk: int
    game_date_et: str
    game_start_et: str
    season: int
    home_team_id: int
    home_team_abbr: str
    home_team_name: str
    away_team_id: int
    away_team_abbr: str
    away_team_name: str
    venue_id: int
    venue_name: str


@dataclass(frozen=True)
class GameEnvironment:
    roof_type: str
    wind_effect: str
    park_factor_runs: float
    park_factor_hr: float
    orientation_hp: str | None
    wind_mph: float | None
    wind_direction: str | None
    wind_dir_label: str
    wind_in: bool
    wind_out: bool
    temp_f: float | None
    wind_source: str
    is_wind_suppressed: bool
    is_retractable: bool
    roof_status_known: bool
    env_ceiling: str
    h3b_eligible: bool


def derive_env_ceiling(env: GameEnvironment) -> str:
    """env_ceiling derivation (priority order) — spec v1.0."""
    if env.is_wind_suppressed:
        return "NoSignal"
    we = (env.wind_effect or "").strip().upper()
    if we == "LOW":
        return "Tier2"
    if we == "MODERATE":
        return "Tier2"
    if we == "HIGH":
        if env.wind_source == "forecast":
            return "Tier2"
        return "Tier1"
    return "Tier2"


def _h3b_park_whitelist() -> frozenset[str]:
    """Defer import so this module stays import-light without duplicating the set."""
    from batch.pipeline.generate_daily_brief import H3B_PARK_WHITELIST

    return frozenset(H3B_PARK_WHITELIST)


def build_game_environment(
    *,
    roof_type: str | None,
    wind_effect: str | None,
    park_factor_runs: float | None,
    park_factor_hr: float | None,
    orientation_hp: str | None,
    wind_mph: float | int | None,
    wind_direction: str | None,
    temp_f: float | int | None,
    wind_source: str | None,
    venue_name: str | None,
    wind_mph_threshold: float = 10.0,
) -> GameEnvironment:
    """
    Build GameEnvironment from raw DB / row dict fields.

    wind_source: NULL or missing column → treated as 'actual' (pre-dates column).
    roof_status_known: Open/Dome True; Retractable True only when wind_source == 'actual'.
    """
    rt = (roof_type or "Open").strip()
    we = (wind_effect or "").strip().upper() or "UNKNOWN"
    pfr = float(park_factor_runs) if park_factor_runs is not None else 100.0
    pfh = float(park_factor_hr) if park_factor_hr is not None else 100.0

    raw_lbl = _wind_direction_label(wind_direction)
    wdl = _normalize_wind_dir_label(raw_lbl)

    try:
        wmph = float(wind_mph) if wind_mph is not None else None
    except (TypeError, ValueError):
        wmph = None
    try:
        tf = float(temp_f) if temp_f is not None else None
    except (TypeError, ValueError):
        tf = None

    wsrc = (wind_source or "").strip().lower() if wind_source else ""
    if not wsrc:
        wsrc = "actual"

    mph_ok = (wmph is not None and wmph >= wind_mph_threshold)
    wind_in = mph_ok and wdl == "IN"
    wind_out = mph_ok and wdl == "OUT"

    rt_norm = rt.strip()
    is_suppressed = we == "SUPPRESSED"
    is_retract = rt_norm.lower() == "retractable"
    is_open = rt_norm.lower() == "open"
    is_dome = rt_norm.lower() == "dome"

    if is_open or is_dome:
        roof_known = True
    elif is_retract:
        roof_known = wsrc == "actual"
    else:
        roof_known = True

    vname = (venue_name or "").strip()
    h3b_ok = (vname in _h3b_park_whitelist()) and wind_out

    env = GameEnvironment(
        roof_type=rt_norm,
        wind_effect=we,
        park_factor_runs=pfr,
        park_factor_hr=pfh,
        orientation_hp=orientation_hp,
        wind_mph=wmph,
        wind_direction=str(wind_direction).strip() if wind_direction else None,
        wind_dir_label=wdl,
        wind_in=wind_in,
        wind_out=wind_out,
        temp_f=tf,
        wind_source=wsrc,
        is_wind_suppressed=is_suppressed,
        is_retractable=is_retract,
        roof_status_known=roof_known,
        env_ceiling="Tier2",
        h3b_eligible=h3b_ok,
    )
    return replace(env, env_ceiling=derive_env_ceiling(env))


def build_game_identifiers(row: dict[str, Any]) -> GameIdentifiers:
    """Require core identity fields; raise ValueError if any missing."""
    req = [
        "game_pk",
        "game_date_et",
        "game_start_utc",
        "season",
        "home_team_id",
        "home_abbr",
        "home_name",
        "away_team_id",
        "away_abbr",
        "away_name",
        "venue_id",
        "venue_name",
    ]
    missing = [k for k in req if row.get(k) in (None, "")]
    if missing:
        raise ValueError(f"Cannot dress game — missing fields: {missing}")

    return GameIdentifiers(
        game_pk=int(row["game_pk"]),
        game_date_et=str(row["game_date_et"]),
        game_start_et=_fmt_game_start_et(str(row["game_start_utc"])),
        season=int(row["season"]),
        home_team_id=int(row["home_team_id"]),
        home_team_abbr=str(row["home_abbr"]),
        home_team_name=str(row["home_name"]),
        away_team_id=int(row["away_team_id"]),
        away_team_abbr=str(row["away_abbr"]),
        away_team_name=str(row["away_name"]),
        venue_id=int(row["venue_id"]),
        venue_name=str(row["venue_name"]),
    )


def dress_game_row(row: dict[str, Any]) -> tuple[GameIdentifiers, GameEnvironment]:
    """One DB row dict → identifiers + environment."""
    ids = build_game_identifiers(row)
    env = build_game_environment(
        roof_type=row.get("roof_type"),
        wind_effect=row.get("wind_effect"),
        park_factor_runs=row.get("park_factor_runs"),
        park_factor_hr=row.get("park_factor_hr"),
        orientation_hp=row.get("orientation_hp"),
        wind_mph=row.get("wind_mph"),
        wind_direction=row.get("wind_direction"),
        temp_f=row.get("temp_f"),
        wind_source=row.get("wind_source"),
        venue_name=row.get("venue_name"),
    )
    return ids, env
