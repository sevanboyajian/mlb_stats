"""
Blocks 3–6 — matchup, market, data completeness, decision shell (Fully Dressed Game).
Read-only DB access. Intended for briefs / signal design alongside fully_dressed_game.py.

DB access for ``dress_full_game_row`` is batched: one ``sqlite_master`` probe for optional
tables, then at most four data queries (odds, probable starters, team rolling stats, batched
prior pitcher lines). No per-field ``execute`` loops.
"""

from __future__ import annotations

import datetime as dt
import sqlite3
from dataclasses import dataclass, field, replace
from typing import Any

from batch.ingestion.load_odds import implied_prob as american_to_implied
from batch.pipeline.fully_dressed_game import (
    GameEnvironment,
    GameIdentifiers,
    dress_game_row,
)

# MV-F home favourite band (matches generate_daily_brief)
HOME_FAV_MV_F_LOW = -130
HOME_FAV_MV_F_HIGH = -160

# Quality tier — policy thresholds (2024+2025 tertiles)
ERA_TERTILE_STRONG = 3.04
ERA_TERTILE_WEAK = 4.56

SIGNAL_STRENGTH: dict[str, str] = {
    "S1H2": "strong",
    "MV-F": "strong",
    "MV-B": "strong",
    "H3b": "moderate",
    "LHP_FADE": "moderate",
    "S1": "weak",
}

_BOOK_PRIORITY = ("draftkings", "fanduel", "betmgm", "pinnacle", "caesars")

_OPTIONAL_TABLES = (
    "game_probable_pitchers",
    "player_game_stats",
    "team_rolling_stats",
    "game_odds",
)


@dataclass(frozen=True)
class DressingBundle:
    """One fetch pass for matchup + market inputs (see ``fetch_dressing_bundle``)."""

    tables_present: frozenset[str]
    odds_rows: list[dict[str, Any]]
    starter_rows: list[dict[str, Any]]
    team_rolling_rows: list[dict[str, Any]]
    pitcher_prior_rows: list[dict[str, Any]]


@dataclass(frozen=True)
class PitcherProfile:
    player_id: int | None
    name: str | None
    hand: str | None
    hand_confirmed: bool
    era_rolling: float | None
    era_rolling_n: int
    era_season: float | None
    era_quality: float | None
    era_source: str
    era_confidence: str
    quality_tier: str | None


@dataclass(frozen=True)
class TeamOffenseProfile:
    team_id: int
    rolling_ops: float | None
    rolling_ops_home: float | None
    rolling_ops_road: float | None
    rolling_runs_pg: float | None
    rolling_k_pct: float | None
    rolling_iso: float | None
    rolling_hr_pg: float | None
    games_in_window: int
    ops_confidence: str
    stats_valid: bool
    rolling_ops_wma: float | None = None


@dataclass(frozen=True)
class GameMatchup:
    home_sp: PitcherProfile
    away_sp: PitcherProfile
    home_offense: TeamOffenseProfile
    away_offense: TeamOffenseProfile
    home_platoon_disadvantage: bool


@dataclass(frozen=True)
class MarketSnapshot:
    home_ml_open: int | None
    home_ml_current: int | None
    home_ml_close: int | None
    away_ml_open: int | None
    away_ml_current: int | None
    away_ml_close: int | None
    home_impl: float | None
    away_impl: float | None
    home_impl_open: float | None
    away_impl_open: float | None
    clv_away_delta: float | None
    clv_available: bool
    total_open: float | None
    total_current: float | None
    total_close: float | None
    over_odds: int | None
    under_odds: int | None
    # Run line (spread) snapshot (usually ±1.5 in MLB)
    home_rl_line: float | None
    away_rl_line: float | None
    home_rl_odds: int | None
    away_rl_odds: int | None
    rl_available: bool
    home_in_fade_band: bool
    home_in_heavy_band: bool
    home_is_dog: bool
    odds_source: str
    odds_age_minutes: int | None
    market_confidence: str


@dataclass
class DataCompleteness:
    mvf_blocked: bool
    mvb_blocked: bool
    h3b_blocked: bool
    s1h2_blocked: bool
    lhp_fade_blocked: bool
    clv_blocked: bool
    completeness_tier: str
    gaps: list[str]


@dataclass
class SignalFinding:
    signal_id: str
    signal_strength: str
    bet_side: str
    odds: str
    edge_basis: str
    fires: bool
    confidence_score: int = 0
    score_basis: str = ""


@dataclass
class AvoidFinding:
    avoid_type: str
    bet_type: str
    reason: str


@dataclass
class FullyDressedGame:
    identifiers: GameIdentifiers
    environment: GameEnvironment
    matchup: GameMatchup
    market: MarketSnapshot
    completeness: DataCompleteness
    signals: list[SignalFinding] = field(default_factory=list)
    avoids: list[AvoidFinding] = field(default_factory=list)
    output_tier: str | None = None
    tier_basis: str | None = None
    stake_multiplier: float = 1.0
    # Brief / signal bridge (not part of DB dress; set by score_game pipeline)
    brief_session: str = "primary"
    home_streak: int = 0
    away_streak: int = 0
    venue_wind_note: str | None = None


def _optional_tables_present(con: sqlite3.Connection) -> frozenset[str]:
    ph = ",".join("?" * len(_OPTIONAL_TABLES))
    cur = con.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name IN ({ph})",
        _OPTIONAL_TABLES,
    )
    return frozenset(str(r[0]) for r in cur.fetchall())


def fetch_dressing_bundle(con: sqlite3.Connection, game_pk: int, game_date_et: str) -> DressingBundle:
    """
    Single batched read for matchup + market raw rows (5 DB round-trips max:
    sqlite_master probe + odds + starters + team_rolling + batched pitcher history).
    """
    tabs = _optional_tables_present(con)
    odds_rows: list[dict[str, Any]] = []
    starter_rows: list[dict[str, Any]] = []
    trs_rows: list[dict[str, Any]] = []
    pgs_rows: list[dict[str, Any]] = []

    if "game_odds" in tabs:
        cur = con.execute(
            """
            SELECT bookmaker, market_type, home_ml, away_ml, total_line,
                   over_odds, under_odds, captured_at_utc,
                   is_opening_line, is_closing_line
            FROM game_odds
            WHERE game_pk = ?
              AND market_type IN ('moneyline', 'total')
            ORDER BY bookmaker, market_type, captured_at_utc
            """,
            (game_pk,),
        )
        odds_rows = [dict(r) for r in cur.fetchall()]

    if "game_probable_pitchers" in tabs:
        cur = con.execute(
            """
            SELECT gp.team_id AS team_id, gp.player_id AS player_id,
                   p.full_name AS full_name, p.throws AS throws, p.era_season AS era_season
            FROM game_probable_pitchers gp
            JOIN players p ON p.player_id = gp.player_id
            WHERE gp.game_pk = ?
            """,
            (game_pk,),
        )
        starter_rows = [dict(r) for r in cur.fetchall()]

    if "team_rolling_stats" in tabs:
        cur = con.execute(
            """
            SELECT trs.team_id, trs.games_in_window, trs.rolling_ops, trs.rolling_ops_wma, trs.rolling_ops_home, trs.rolling_ops_road,
                   trs.rolling_runs_scored_pg, trs.rolling_k_pct, trs.rolling_iso, trs.rolling_hr_pg
            FROM team_rolling_stats trs
            WHERE trs.game_pk = ?
            """,
            (game_pk,),
        )
        trs_rows = [dict(r) for r in cur.fetchall()]

    pids = sorted({int(r["player_id"]) for r in starter_rows})
    if pids and "player_game_stats" in tabs:
        ph = ",".join("?" * len(pids))
        cur = con.execute(
            f"""
            SELECT pgs.player_id AS player_id,
                   pgs.innings_pitched AS ip,
                   pgs.earned_runs AS er,
                   g.game_date AS gd,
                   g.game_pk AS gpk
            FROM player_game_stats pgs
            JOIN games g ON g.game_pk = pgs.game_pk
            WHERE pgs.player_id IN ({ph})
              AND pgs.player_role = 'pitcher'
              AND g.status = 'Final'
              AND g.game_type = 'R'
              AND (
                    g.game_date < ?
                 OR (g.game_date = ? AND g.game_pk < ?)
              )
            ORDER BY pgs.player_id, g.game_date DESC, g.game_pk DESC
            """,
            (*pids, game_date_et, game_date_et, game_pk),
        )
        pgs_rows = [dict(r) for r in cur.fetchall()]

    return DressingBundle(
        tables_present=tabs,
        odds_rows=odds_rows,
        starter_rows=starter_rows,
        team_rolling_rows=trs_rows,
        pitcher_prior_rows=pgs_rows,
    )


def _parse_utc(ts: str | None) -> dt.datetime | None:
    if not ts:
        return None
    raw = str(ts).strip().rstrip("Z")
    try:
        d = dt.datetime.fromisoformat(raw)
    except ValueError:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d


def _minutes_between(later: dt.datetime, earlier: dt.datetime) -> float:
    return (later - earlier).total_seconds() / 60.0


def min_tier(a: str, b: str) -> str:
    """Tier1 > Tier2 > Tier3 > Avoid > NoSignal (best to worst). Returns worse tier."""
    order = {
        "Tier1": 5,
        "Tier2": 4,
        "Tier3": 3,
        "Avoid": 2,
        "NoSignal": 1,
    }
    ka = order.get(a, 0)
    kb = order.get(b, 0)
    return a if ka < kb else b


def stake_multiplier_for_tier(tier: str | None) -> float:
    """1.0 (Tier1) | 0.5 (Tier2) | 0.0 (Tier3, Avoid, or no tier)."""
    if tier == "Tier1":
        return 1.0
    if tier == "Tier2":
        return 0.5
    return 0.0


def derive_output_tier(
    signal_strength: str,
    env_ceiling: str,
    completeness: str,
    hostile_env: bool,
    has_avoid: bool,
) -> tuple[str | None, str]:
    if has_avoid:
        return "Avoid", "avoid_flag"
    if completeness == "blocking":
        return "Tier3", "data_completeness_blocking"
    if signal_strength == "none":
        return None, "no_signal"

    base = {"strong": "Tier1", "moderate": "Tier2", "weak": "Tier3"}[signal_strength]
    effective = min_tier(base, env_ceiling)
    if hostile_env:
        effective = min_tier(effective, "Tier2")
    if completeness == "degraded":
        effective = min_tier(effective, "Tier2")
    basis = (
        f"signal={signal_strength}, env_ceiling={env_ceiling}, "
        f"completeness={completeness}, hostile={hostile_env}"
    )
    return effective, basis


def _quality_tier_from_era(era: float | None, era_confidence: str) -> str | None:
    if era is None or era_confidence in ("low", "none"):
        return None
    if era <= ERA_TERTILE_STRONG:
        return "strong"
    if era >= ERA_TERTILE_WEAK:
        return "weak"
    return "middle"


def _rolling_starts_era_from_rows(
    all_rows: list[dict[str, Any]],
    player_id: int,
    min_ip: float = 3.0,
    max_starts: int = 5,
) -> tuple[float | None, int]:
    """
    Last up to ``max_starts`` prior appearances with IP >= min_ip from a pre-fetched
    pitcher line list (already filtered to games before the target game).
    """
    rows = [r for r in all_rows if int(r["player_id"]) == int(player_id)]
    rows.sort(key=lambda r: (str(r["gd"]), int(r["gpk"])), reverse=True)
    starts: list[tuple[float, int]] = []
    for r in rows:
        ip = float(r["ip"] or 0.0)
        er = int(r["er"] or 0)
        if ip >= min_ip:
            starts.append((ip, er))
        if len(starts) >= max_starts:
            break
    if not starts:
        return None, 0
    t_ip = sum(x[0] for x in starts)
    t_er = sum(x[1] for x in starts)
    if t_ip <= 0:
        return None, len(starts)
    return round(9.0 * t_er / t_ip, 3), len(starts)


def _pitcher_profile_missing() -> PitcherProfile:
    return PitcherProfile(
        player_id=None,
        name=None,
        hand=None,
        hand_confirmed=False,
        era_rolling=None,
        era_rolling_n=0,
        era_season=None,
        era_quality=None,
        era_source="missing",
        era_confidence="none",
        quality_tier=None,
    )


def _pitcher_profile_from_starter_row(
    starter_row: dict[str, Any] | None,
    era_roll: tuple[float | None, int],
) -> PitcherProfile:
    if starter_row is None:
        return _pitcher_profile_missing()

    pid = int(starter_row["player_id"])
    name = str(starter_row["full_name"]) if starter_row.get("full_name") else None
    throws = starter_row.get("throws")
    hand = str(throws).strip().upper() if throws else None
    hand_confirmed = bool(hand)
    era_season = (
        float(starter_row["era_season"]) if starter_row.get("era_season") is not None else None
    )
    era_r, n_r = era_roll

    era_quality: float | None = None
    era_source = "missing"
    era_confidence = "none"

    if n_r >= 3 and era_r is not None:
        era_quality = era_r
        era_source = "rolling"
        era_confidence = "high" if n_r >= 5 else "medium"
    elif era_season is not None:
        era_quality = era_season
        era_source = "season"
        era_confidence = "low"
    else:
        era_quality = None
        era_source = "missing"
        era_confidence = "none"

    qt = _quality_tier_from_era(era_quality, era_confidence)

    return PitcherProfile(
        player_id=pid,
        name=name,
        hand=hand,
        hand_confirmed=hand_confirmed,
        era_rolling=era_r,
        era_rolling_n=n_r,
        era_season=era_season,
        era_quality=era_quality,
        era_source=era_source,
        era_confidence=era_confidence,
        quality_tier=qt,
    )


def _ops_confidence(games_in_window: int) -> str:
    if games_in_window >= 10:
        return "high"
    if games_in_window >= 5:
        return "medium"
    if games_in_window >= 1:
        return "low"
    return "none"


def _team_offense_empty(team_id: int) -> TeamOffenseProfile:
    return TeamOffenseProfile(
        team_id=team_id,
        rolling_ops=None,
        rolling_ops_home=None,
        rolling_ops_road=None,
        rolling_runs_pg=None,
        rolling_k_pct=None,
        rolling_iso=None,
        rolling_hr_pg=None,
        games_in_window=0,
        ops_confidence="none",
        stats_valid=False,
    )


def _build_team_offense_from_rows(
    trs_rows: list[dict[str, Any]],
    team_id: int,
) -> TeamOffenseProfile:
    row = next((r for r in trs_rows if int(r["team_id"]) == int(team_id)), None)
    if not row:
        return _team_offense_empty(team_id)

    giw = int(row["games_in_window"] or 0)
    oc = _ops_confidence(giw)

    def f(col: str) -> float | None:
        v = row[col]
        return float(v) if v is not None else None

    return TeamOffenseProfile(
        team_id=team_id,
        rolling_ops=f("rolling_ops"),
        rolling_ops_wma=float(row["rolling_ops_wma"]) if row["rolling_ops_wma"] is not None else None,
        rolling_ops_home=f("rolling_ops_home"),
        rolling_ops_road=f("rolling_ops_road"),
        rolling_runs_pg=f("rolling_runs_scored_pg"),
        rolling_k_pct=f("rolling_k_pct"),
        rolling_iso=f("rolling_iso"),
        rolling_hr_pg=f("rolling_hr_pg"),
        games_in_window=giw,
        ops_confidence=oc,
        stats_valid=giw >= 5,
    )


def _empty_market_snapshot() -> MarketSnapshot:
    return MarketSnapshot(
        home_ml_open=None,
        home_ml_current=None,
        home_ml_close=None,
        away_ml_open=None,
        away_ml_current=None,
        away_ml_close=None,
        home_impl=None,
        away_impl=None,
        home_impl_open=None,
        away_impl_open=None,
        clv_away_delta=None,
        clv_available=False,
        total_open=None,
        total_current=None,
        total_close=None,
        over_odds=None,
        under_odds=None,
        home_rl_line=None,
        away_rl_line=None,
        home_rl_odds=None,
        away_rl_odds=None,
        rl_available=False,
        home_in_fade_band=False,
        home_in_heavy_band=False,
        home_is_dog=False,
        odds_source="none",
        odds_age_minutes=None,
        market_confidence="none",
    )


def _pick_book_from_odds_rows(odds_rows: list[dict[str, Any]]) -> str | None:
    books = {
        str(r["bookmaker"]).lower()
        for r in odds_rows
        if str(r.get("market_type") or "") == "moneyline"
    }
    for b in _BOOK_PRIORITY:
        if b in books:
            return b
    if books:
        return sorted(books)[0]
    return None


def _ml_snapshots_from_book_rows(
    ml_rows: list[dict[str, Any]],
    game_start_utc: str | None,
) -> dict[str, Any]:
    gs = _parse_utc(game_start_utc)
    rows = sorted(ml_rows, key=lambda r: str(r["captured_at_utc"]))

    home_open = away_open = None
    for r in rows:
        if int(r["is_opening_line"] or 0) == 1:
            home_open = r["home_ml"]
            away_open = r["away_ml"]
            break

    home_close = away_close = None
    for r in reversed(rows):
        if int(r["is_closing_line"] or 0) == 1:
            home_close = r["home_ml"]
            away_close = r["away_ml"]
            break

    home_cur = away_cur = None
    cap_cur: str | None = None
    if gs and rows:
        pre = [
            r
            for r in rows
            if _parse_utc(r["captured_at_utc"]) and _parse_utc(r["captured_at_utc"]) <= gs
        ]
        if pre:
            last = pre[-1]
            home_cur = last["home_ml"]
            away_cur = last["away_ml"]
            cap_cur = last["captured_at_utc"]
    if home_cur is None and rows:
        last = rows[-1]
        home_cur = last["home_ml"]
        away_cur = last["away_ml"]
        cap_cur = last["captured_at_utc"]

    return {
        "home_open": home_open,
        "away_open": away_open,
        "home_close": home_close,
        "away_close": away_close,
        "home_current": home_cur,
        "away_current": away_cur,
        "captured_current": cap_cur,
    }


def _total_snapshots_from_book_rows(
    tt_rows: list[dict[str, Any]],
    game_start_utc: str | None,
) -> dict[str, Any]:
    gs = _parse_utc(game_start_utc)
    rows = sorted(tt_rows, key=lambda r: str(r["captured_at_utc"]))

    total_open = total_close = None
    for r in rows:
        if int(r["is_opening_line"] or 0) == 1:
            total_open = r["total_line"]
            break
    for r in reversed(rows):
        if int(r["is_closing_line"] or 0) == 1:
            total_close = r["total_line"]
            break

    tot_cur = over_o = under_o = None
    if gs and rows:
        pre = [
            r
            for r in rows
            if _parse_utc(r["captured_at_utc"]) and _parse_utc(r["captured_at_utc"]) <= gs
        ]
        if pre:
            last = pre[-1]
            tot_cur = last["total_line"]
            over_o = last["over_odds"]
            under_o = last["under_odds"]
    if tot_cur is None and rows:
        last = rows[-1]
        tot_cur = last["total_line"]
        over_o = last["over_odds"]
        under_o = last["under_odds"]

    return {
        "total_open": total_open,
        "total_close": total_close,
        "total_current": tot_cur,
        "over_odds": over_o,
        "under_odds": under_o,
    }


def build_market_snapshot_from_odds_rows(
    odds_rows: list[dict[str, Any]],
    game_start_utc: str | None,
    now_utc: dt.datetime | None = None,
) -> MarketSnapshot:
    """Build ``MarketSnapshot`` from a pre-fetched ``game_odds`` row list (no DB)."""
    now = now_utc or dt.datetime.now(dt.timezone.utc)
    book = _pick_book_from_odds_rows(odds_rows)
    if not book:
        return _empty_market_snapshot()

    b = book.lower()
    ml_rows = [r for r in odds_rows if r["market_type"] == "moneyline" and str(r["bookmaker"]).lower() == b]
    tt_rows = [r for r in odds_rows if r["market_type"] == "total" and str(r["bookmaker"]).lower() == b]
    if not ml_rows:
        return _empty_market_snapshot()

    ml = _ml_snapshots_from_book_rows(ml_rows, game_start_utc)
    tt = _total_snapshots_from_book_rows(tt_rows, game_start_utc)

    h_cur = ml["home_current"]
    a_cur = ml["away_current"]
    h_open = ml["home_open"]
    a_open = ml["away_open"]

    hi = american_to_implied(int(h_cur)) if h_cur is not None else None
    ai = american_to_implied(int(a_cur)) if a_cur is not None else None
    hio = american_to_implied(int(h_open)) if h_open is not None else None
    aio = american_to_implied(int(a_open)) if a_open is not None else None

    clv_delta: float | None = None
    clv_ok = False
    if ai is not None and aio is not None:
        clv_delta = round((ai - aio) * 100.0, 2)
        clv_ok = True

    age_min: int | None = None
    cap = ml["captured_current"]
    cap_dt = _parse_utc(cap)
    if cap_dt:
        age_min = int(round(_minutes_between(now, cap_dt)))

    if h_cur is None:
        mconf = "none"
    elif age_min is None:
        mconf = "low"
    elif age_min < 60:
        mconf = "high"
    elif age_min <= 180:
        mconf = "medium"
    else:
        mconf = "low"

    gs = _parse_utc(game_start_utc)
    cap2 = _parse_utc(ml["captured_current"])
    if gs and cap2 and cap2 > gs:
        mconf = "low"

    hml = int(h_cur) if h_cur is not None else None
    in_fade = hml is not None and HOME_FAV_MV_F_HIGH <= hml <= HOME_FAV_MV_F_LOW
    in_heavy = hi is not None and 0.60 <= hi <= 0.67
    is_dog = hi is not None and hi <= 0.42

    return MarketSnapshot(
        home_ml_open=int(h_open) if h_open is not None else None,
        home_ml_current=hml,
        home_ml_close=int(ml["home_close"]) if ml["home_close"] is not None else None,
        away_ml_open=int(a_open) if a_open is not None else None,
        away_ml_current=int(a_cur) if a_cur is not None else None,
        away_ml_close=int(ml["away_close"]) if ml["away_close"] is not None else None,
        home_impl=hi,
        away_impl=ai,
        home_impl_open=hio,
        away_impl_open=aio,
        clv_away_delta=clv_delta,
        clv_available=clv_ok,
        total_open=float(tt["total_open"]) if tt["total_open"] is not None else None,
        total_current=float(tt["total_current"]) if tt["total_current"] is not None else None,
        total_close=float(tt["total_close"]) if tt["total_close"] is not None else None,
        over_odds=int(tt["over_odds"]) if tt["over_odds"] is not None else None,
        under_odds=int(tt["under_odds"]) if tt["under_odds"] is not None else None,
        # Run line comes from the caller's game row in ``dress_full_game_row`` (replace).
        home_rl_line=None,
        away_rl_line=None,
        home_rl_odds=None,
        away_rl_odds=None,
        rl_available=False,
        home_in_fade_band=in_fade,
        home_in_heavy_band=in_heavy,
        home_is_dog=is_dog,
        odds_source=book,
        odds_age_minutes=age_min,
        market_confidence=mconf,
    )


def build_market_snapshot(
    con: sqlite3.Connection,
    game_pk: int,
    game_start_utc: str | None,
    now_utc: dt.datetime | None = None,
) -> MarketSnapshot:
    """Single ``game_odds`` SELECT, then in-memory market build (standalone helper)."""
    try:
        cur = con.execute(
            """
            SELECT bookmaker, market_type, home_ml, away_ml, total_line,
                   over_odds, under_odds, captured_at_utc,
                   is_opening_line, is_closing_line
            FROM game_odds
            WHERE game_pk = ?
              AND market_type IN ('moneyline', 'total')
            ORDER BY bookmaker, market_type, captured_at_utc
            """,
            (game_pk,),
        )
        odds_rows = [dict(r) for r in cur.fetchall()]
    except sqlite3.OperationalError:
        return _empty_market_snapshot()
    return build_market_snapshot_from_odds_rows(odds_rows, game_start_utc, now_utc)


def build_data_completeness(
    env: GameEnvironment,
    matchup: GameMatchup,
    market: MarketSnapshot,
) -> DataCompleteness:
    wind_unknown = env.wind_dir_label == "UNKNOWN"
    no_ml = market.home_ml_current is None
    no_total = market.total_current is None

    # Partial evaluation policy:
    # Warnings (wind unknown, short offense window, starter hand unknown) should
    # NOT hard-suppress signal creation. Keep these as confidence penalties via gaps
    # (score_game applies confidence_penalty and per-signal score modifiers).
    #
    # Only hard-block when market context is truly unusable (no odds).
    mvf = bool(market.market_confidence == "none")
    mvb = bool(market.market_confidence == "none")
    h3b = bool(market.market_confidence == "none")
    s1h2 = bool(market.market_confidence == "none")
    # Offense window <5 should reduce confidence, not block the model.
    # Keep LHP blocked only for truly missing critical inputs (hand/odds).
    lhp = (not matchup.away_sp.hand_confirmed) or (market.market_confidence == "none")

    gaps: list[str] = []
    if wind_unknown:
        gaps.append("wind direction unknown (wind signals suppressed)")
    if no_ml:
        gaps.append("no current moneyline")
    if no_total:
        gaps.append("no current total line")
    if not matchup.away_sp.hand_confirmed:
        gaps.append("away starter hand unknown")
    if not matchup.home_offense.stats_valid and matchup.home_offense.rolling_ops_wma is None:
        gaps.append("home rolling offense window < 5 games (confidence penalty)")
    if market.market_confidence == "none":
        gaps.append("no usable market odds")
    if not market.clv_available:
        gaps.append("opening line unavailable (CLV blocked)")

    clv_b = not market.clv_available

    degraded = (
        matchup.home_sp.era_confidence == "low"
        or matchup.away_sp.era_confidence == "low"
        or matchup.home_offense.ops_confidence in ("low", "none")
        or env.wind_source == "forecast"
    )

    # "blocking" should be reserved for truly unusable market context (no odds),
    # not for missing wind direction or partial rolling windows.
    if market.market_confidence == "none" or (no_ml and no_total):
        tier = "blocking"
    elif degraded:
        tier = "degraded"
    else:
        tier = "complete"

    return DataCompleteness(
        mvf_blocked=mvf,
        mvb_blocked=mvb,
        h3b_blocked=h3b,
        s1h2_blocked=s1h2,
        lhp_fade_blocked=lhp,
        clv_blocked=clv_b,
        completeness_tier=tier,
        gaps=gaps,
    )


def dress_full_game_row(con: sqlite3.Connection, row: dict[str, Any]) -> FullyDressedGame:
    """Identifiers + environment (blocks 1–2) + matchup + market + completeness shell."""
    ids, env = dress_game_row(row)
    bundle = fetch_dressing_bundle(con, ids.game_pk, ids.game_date_et)

    rolling: dict[int, tuple[float | None, int]] = {}
    if bundle.pitcher_prior_rows:
        for pid in {int(r["player_id"]) for r in bundle.starter_rows}:
            rolling[pid] = _rolling_starts_era_from_rows(bundle.pitcher_prior_rows, pid)

    home_sr = next((r for r in bundle.starter_rows if int(r["team_id"]) == ids.home_team_id), None)
    away_sr = next((r for r in bundle.starter_rows if int(r["team_id"]) == ids.away_team_id), None)
    home_roll = rolling.get(int(home_sr["player_id"]), (None, 0)) if home_sr else (None, 0)
    away_roll = rolling.get(int(away_sr["player_id"]), (None, 0)) if away_sr else (None, 0)

    home_sp = _pitcher_profile_from_starter_row(home_sr, home_roll)
    away_sp = _pitcher_profile_from_starter_row(away_sr, away_roll)
    home_off = _build_team_offense_from_rows(bundle.team_rolling_rows, ids.home_team_id)
    away_off = _build_team_offense_from_rows(bundle.team_rolling_rows, ids.away_team_id)

    platoon = (
        away_sp.hand == "L"
        and (home_off.stats_valid or home_off.rolling_ops_wma is not None)
        and away_sp.hand_confirmed
    )

    matchup = GameMatchup(
        home_sp=home_sp,
        away_sp=away_sp,
        home_offense=home_off,
        away_offense=away_off,
        home_platoon_disadvantage=platoon,
    )

    market = build_market_snapshot_from_odds_rows(bundle.odds_rows, row.get("game_start_utc"))
    # Run line fields are sourced from the caller's game row (load_games join),
    # not from game_odds snapshots.
    hrl_line = row.get("home_rl")
    arl_line = row.get("away_rl")
    hrl_odds = row.get("home_rl_odds")
    arl_odds = row.get("away_rl_odds")
    rl_avail = (hrl_odds is not None) or (arl_odds is not None)
    market = replace(
        market,
        home_rl_line=float(hrl_line) if hrl_line is not None else None,
        away_rl_line=float(arl_line) if arl_line is not None else None,
        home_rl_odds=int(hrl_odds) if hrl_odds is not None else None,
        away_rl_odds=int(arl_odds) if arl_odds is not None else None,
        rl_available=bool(rl_avail),
    )
    comp = build_data_completeness(env, matchup, market)

    return FullyDressedGame(
        identifiers=ids,
        environment=env,
        matchup=matchup,
        market=market,
        completeness=comp,
        signals=[],
        avoids=[],
        output_tier=None,
        tier_basis=None,
        stake_multiplier=1.0,
    )


def fully_dressed_to_json(obj: FullyDressedGame) -> dict[str, Any]:
    """JSON-serializable dict for CLI / logs."""

    return {
        "identifiers": {
            "game_pk": obj.identifiers.game_pk,
            "game_date_et": obj.identifiers.game_date_et,
            "game_start_et": obj.identifiers.game_start_et,
            "season": obj.identifiers.season,
            "home_team_id": obj.identifiers.home_team_id,
            "home_team_abbr": obj.identifiers.home_team_abbr,
            "home_team_name": obj.identifiers.home_team_name,
            "away_team_id": obj.identifiers.away_team_id,
            "away_team_abbr": obj.identifiers.away_team_abbr,
            "away_team_name": obj.identifiers.away_team_name,
            "venue_id": obj.identifiers.venue_id,
            "venue_name": obj.identifiers.venue_name,
        },
        "environment": {
            "roof_type": obj.environment.roof_type,
            "wind_effect": obj.environment.wind_effect,
            "park_factor_runs": obj.environment.park_factor_runs,
            "park_factor_hr": obj.environment.park_factor_hr,
            "orientation_hp": obj.environment.orientation_hp,
            "wind_mph": obj.environment.wind_mph,
            "wind_direction": obj.environment.wind_direction,
            "wind_dir_label": obj.environment.wind_dir_label,
            "wind_in": obj.environment.wind_in,
            "wind_out": obj.environment.wind_out,
            "temp_f": obj.environment.temp_f,
            "wind_source": obj.environment.wind_source,
            "is_wind_suppressed": obj.environment.is_wind_suppressed,
            "is_retractable": obj.environment.is_retractable,
            "roof_status_known": obj.environment.roof_status_known,
            "env_ceiling": obj.environment.env_ceiling,
            "h3b_eligible": obj.environment.h3b_eligible,
        },
        "matchup": {
            "home_sp": obj.matchup.home_sp.__dict__,
            "away_sp": obj.matchup.away_sp.__dict__,
            "home_offense": obj.matchup.home_offense.__dict__,
            "away_offense": obj.matchup.away_offense.__dict__,
            "home_platoon_disadvantage": obj.matchup.home_platoon_disadvantage,
        },
        "market": obj.market.__dict__,
        "completeness": {
            **obj.completeness.__dict__,
        },
        "signals": [s.__dict__ for s in obj.signals],
        "avoids": [a.__dict__ for a in obj.avoids],
        "output_tier": obj.output_tier,
        "tier_basis": obj.tier_basis,
        "stake_multiplier": obj.stake_multiplier,
        "brief_session": obj.brief_session,
        "home_streak": obj.home_streak,
        "away_streak": obj.away_streak,
        "venue_wind_note": obj.venue_wind_note,
    }
