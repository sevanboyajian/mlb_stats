"""
Central signal scoring — all model signal if/else logic lives here.

``score_game(FullyDressedGame, home_streak: int, game_month: int) -> ScoredGame``;
``generate_daily_brief.enrich_game`` + ``evaluate_signals`` dress the row and map
``ScoredGame`` back to the legacy dict via ``scored_game_to_eval_dict``.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, replace
from typing import Any

from batch.pipeline.dressed_game_blocks import (
    AvoidFinding,
    FullyDressedGame,
    SignalFinding,
    dress_full_game_row,
)

# ── Tier helpers (ScoredGame policy) ───────────────────────────────────────
# Higher index = less confident. min_tier returns the worse (less confident) tier.

TIER_ORDER: list[str | None] = ["Tier1", "Tier2", "Tier3", "Avoid", None]


def min_tier(a: str | None, b: str | None) -> str | None:
    """Return the lower (less confident) of two tier values."""
    ai = TIER_ORDER.index(a) if a in TIER_ORDER else len(TIER_ORDER)
    bi = TIER_ORDER.index(b) if b in TIER_ORDER else len(TIER_ORDER)
    return TIER_ORDER[max(ai, bi)]


def strength_to_base_tier(strength: str) -> str | None:
    return {"strong": "Tier1", "moderate": "Tier2", "weak": "Tier3"}.get(strength)


def _env_ceiling_to_cap_tier(env_ceiling: str) -> str | None:
    """Map GameEnvironment.env_ceiling onto output tier scale for min_tier."""
    ec = (env_ceiling or "").strip()
    if ec == "NoSignal":
        return "Tier3"
    if ec in ("Tier1", "Tier2"):
        return ec
    return "Tier2"


SIGNAL_PRIORITY: dict[str, int] = {
    "S1H2": 1,
    "MV-F": 2,
    "LHP_FADE": 3,
    "MV-B": 4,
    "S1": 5,
    "H3b": 6,
}

SIGNAL_STRENGTH: dict[str, str] = {
    "S1H2": "strong",
    "MV-F": "strong",
    "MV-B": "strong",
    "H3b": "moderate",
    "LHP_FADE": "moderate",
    "S1": "weak",
}

WIND_SIGNAL_IDS = frozenset({"MV-F", "MV-B", "H3b"})


def _fmt_odds(val: Any) -> str:
    if val is None:
        return "N/A"
    v = int(val)
    return f"+{v}" if v > 0 else str(v)


def _gdb():
    """Lazy import — avoids cycles while generate_daily_brief is loading."""
    import batch.pipeline.generate_daily_brief as m

    return m


def _h3b_whitelist() -> set[str]:
    return set(_gdb().H3B_PARK_WHITELIST)


def _game_month(game_date_et: str) -> int:
    try:
        return int((game_date_et or "")[:10].split("-")[1])
    except (ValueError, IndexError):
        return 0


def _wind_eligible(g: FullyDressedGame) -> bool:
    env = g.environment
    return (not env.is_wind_suppressed) and (env.wind_effect or "").upper() == "HIGH"


def _mph(g: FullyDressedGame) -> float:
    try:
        return float(g.environment.wind_mph or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _s1h2_fired(s1h2: SignalFinding) -> bool:
    return s1h2.fires


def _eval_s1h2(g: FullyDressedGame, home_streak: int) -> SignalFinding:
    gdb = _gdb()
    mkt = g.market
    fires = (
        home_streak >= gdb.STREAK_THRESHOLD
        and mkt.home_ml_current is not None
        and bool(mkt.home_in_fade_band)
        and not g.completeness.s1h2_blocked
    )
    hml = mkt.home_ml_current
    reason = (
        f"S1+H2 STACK — Home team {g.identifiers.home_team_abbr} on W{home_streak} streak "
        f"AND priced {_fmt_odds(hml)} (overpricing zone). "
        f"Two signals simultaneously — highest-priority fade."
    ) if fires else (
        f"S1+H2 blocked: streak={home_streak} "
        f"fade_band={mkt.home_in_fade_band} "
        f"blocked={g.completeness.s1h2_blocked} home_ml={hml!r}"
    )
    return SignalFinding(
        signal_id="S1H2",
        signal_strength=SIGNAL_STRENGTH["S1H2"],
        bet_side="away_ml",
        odds=_fmt_odds(mkt.away_ml_current),
        edge_basis=reason,
        fires=fires,
    )


def _eval_mv_f(g: FullyDressedGame, s1h2_fired: bool) -> SignalFinding:
    gdb = _gdb()
    mkt = g.market
    env = g.environment
    mph = _mph(g)
    wind_ok = _wind_eligible(g) and env.wind_in
    price_ok = mkt.home_ml_current is not None and bool(mkt.home_in_fade_band)
    env_ok = env.env_ceiling != "NoSignal"
    comp_ok = not g.completeness.mvf_blocked
    fires = wind_ok and price_ok and env_ok and comp_ok and (not s1h2_fired)

    clv_txt = (
        f"CLV gate: only bet if away ML implied is ≥{gdb.MV_F_CLV_GATE}pp "
        f"below morning open."
    )
    if mkt.clv_available and mkt.clv_away_delta is not None:
        clv_txt += f" Current vs open (away implied delta): {mkt.clv_away_delta:+.2f}pp."
    clv_txt += (
        f" CLV≥+{gdb.MV_F_CLV_GATE}pp fires: SBRO +24.0% ROI, OW +10.6% ROI. "
        f"CLV<+{gdb.MV_F_CLV_GATE}pp fires: SBRO −9.4% ROI. "
        f"Compare current away ML to opening line at bet time."
    )

    if fires:
        reason = (
            f"MV-F — Wind IN {mph:.0f} mph. "
            f"Home fav {_fmt_odds(mkt.home_ml_current)} in fade zone (−130/−160). "
            f"Wind-in suppresses scoring; overpriced home fav. "
            f"{clv_txt}"
        )
    else:
        reason = (
            f"MV-F blocked: wind_eligible={_wind_eligible(g)} wind_in={env.wind_in} "
            f"mph={mph} fade_band={mkt.home_in_fade_band} env_ceiling={env.env_ceiling!r} "
            f"mvf_blocked={g.completeness.mvf_blocked} s1h2_active={s1h2_fired}. "
            f"{clv_txt}"
        )
    return SignalFinding(
        signal_id="MV-F",
        signal_strength=SIGNAL_STRENGTH["MV-F"],
        bet_side="away_ml",
        odds=_fmt_odds(mkt.away_ml_current),
        edge_basis=reason,
        fires=fires,
    )


def _eval_lhp_fade(g: FullyDressedGame, game_month: int, s1h2_fired: bool) -> SignalFinding:
    gdb = _gdb()
    mkt = g.market
    away = g.matchup.away_sp
    home_off = g.matchup.home_offense

    core = (
        bool(mkt.home_in_heavy_band)
        and g.matchup.home_platoon_disadvantage
        and not g.completeness.lhp_fade_blocked
        and (not s1h2_fired)
        and game_month in gdb.NF4_MONTHS_OK
    )
    fires = core

    era_booster = away.quality_tier == "strong"
    ops_booster = (
        home_off.rolling_ops is not None
        and float(home_off.rolling_ops) >= gdb.NF4_OPS_MIN
    )
    booster_txt = []
    if era_booster:
        booster_txt.append(
            f"Confidence: away SP quality_tier=strong (ERA gate aligned with legacy NF4)."
        )
    if ops_booster:
        booster_txt.append(
            f"Confidence: home rolling OPS ≥ {gdb.NF4_OPS_MIN} (legacy NF4 OPS gate)."
        )
    dual = era_booster and ops_booster
    if dual:
        booster_txt.append(
            "Dual boosters present — eligible for Tier2→Tier1 upgrade pending N≥50 "
            "live confirmation; until then signal stays moderate (Tier2 basis)."
        )
    boost_block = (" " + " ".join(booster_txt)) if booster_txt else ""

    hi = mkt.home_impl
    if fires:
        sp_name = away.name or g.identifiers.away_team_abbr
        reason = (
            f"LHP_FADE — Home fav {g.identifiers.home_team_abbr} in heavy band "
            f"({hi:.0%} implied, {_fmt_odds(mkt.home_ml_current)}) vs LHP {sp_name} "
            f"(platoon disadvantage confirmed). Apr–Aug only.{boost_block}"
        )
    else:
        reason = (
            f"LHP_FADE blocked: heavy_band={mkt.home_in_heavy_band} "
            f"platoon_disadv={g.matchup.home_platoon_disadvantage} "
            f"lhp_fade_blocked={g.completeness.lhp_fade_blocked} s1h2_active={s1h2_fired} "
            f"month={game_month} in_ok={game_month in gdb.NF4_MONTHS_OK}{boost_block}"
        )
    return SignalFinding(
        signal_id="LHP_FADE",
        signal_strength=SIGNAL_STRENGTH["LHP_FADE"],
        bet_side="away_ml",
        odds=_fmt_odds(mkt.away_ml_current),
        edge_basis=reason,
        fires=fires,
    )


def _eval_mv_b(g: FullyDressedGame, game_month: int) -> SignalFinding:
    gdb = _gdb()
    env = g.environment
    mkt = g.market
    mph = _mph(g)
    pf = float(env.park_factor_runs or 0.0)

    fires = (
        env.wind_out
        and mph >= gdb.WIND_OUT_MIN_MPH
        and env.env_ceiling != "NoSignal"
        and env.h3b_eligible
        and pf >= gdb.H3B_MIN_PARK_FACTOR
        and mkt.total_current is not None
        and not g.completeness.h3b_blocked
    )
    # Aug/Sep: informational flag only (see score_game data_flags), not a fire gate.
    hi = mkt.home_impl
    venue_name = g.identifiers.venue_name or ""
    wind_effect = (env.wind_effect or "").strip() or "?"
    home_impl_s = f"{hi:.0%}" if hi is not None else "N/A"
    home_ml_s = _fmt_odds(mkt.home_ml_current)
    if fires:
        reason = (
            f"MV-B — Wind OUT {mph:.0f} mph at {venue_name} "
            f"(wind_effect={wind_effect}, PF {pf:.0f}). "
            f"Home dog implied {home_impl_s} ({home_ml_s}). "
            f"CLV gate: only bet if line has moved toward OVER since open (CLV>0). "
            f"CLV>0 fires: +12.2% ROI. CLV≤0: -18.1% ROI."
        )
    else:
        reason = (
            f"MV-B blocked: wind_out={env.wind_out} mph={mph} "
            f"env_ceiling={env.env_ceiling!r} h3b_eligible={env.h3b_eligible} "
            f"pf={pf} total={mkt.total_current!r} h3b_blocked={g.completeness.h3b_blocked}"
        )
    return SignalFinding(
        signal_id="MV-B",
        signal_strength=SIGNAL_STRENGTH["MV-B"],
        bet_side="over_total",
        odds=_fmt_odds(mkt.over_odds) if mkt.over_odds is not None else "-110",
        edge_basis=reason,
        fires=fires,
    )


def _eval_s1(g: FullyDressedGame, home_streak: int, s1h2_fired: bool) -> SignalFinding:
    gdb = _gdb()
    mkt = g.market
    s1_price_ok = mkt.home_ml_current is not None and (
        gdb.S1_PRICE_HIGH <= mkt.home_ml_current <= gdb.S1_PRICE_LOW
    )
    fires = (
        home_streak >= gdb.S1_STANDALONE_MIN
        and s1_price_ok
        and not g.completeness.s1h2_blocked
        and (not s1h2_fired)
    )
    hs = home_streak
    if fires:
        reason = (
            f"S1 — Home team {g.identifiers.home_team_abbr} entering on W{hs} win streak "
            f"priced {_fmt_odds(mkt.home_ml_current)} (streak-premium zone −105/−170). "
            f"Fade away ML. ROI: +7.50% SBRO / +8.99% OW (stronger at W7+). "
            f"Filtered: W6+ only, price band −105/−170."
        )
    else:
        reason = (
            f"S1 blocked: streak={hs} s1h2_active={s1h2_fired} s1h2_blocked={g.completeness.s1h2_blocked} "
            f"price_ok={s1_price_ok} home_ml={mkt.home_ml_current!r}"
        )
    return SignalFinding(
        signal_id="S1",
        signal_strength=SIGNAL_STRENGTH["S1"],
        bet_side="away_ml",
        odds=_fmt_odds(mkt.away_ml_current),
        edge_basis=reason,
        fires=fires,
    )


def _h3b_standalone_would_fire(g: FullyDressedGame) -> bool:
    gdb = _gdb()
    env = g.environment
    mkt = g.market
    mph = _mph(g)
    venue_name = g.identifiers.venue_name or ""
    pf = float(env.park_factor_runs or 100.0)
    h3b_park_ok = venue_name in _h3b_whitelist()
    h3b_pf_ok = pf >= gdb.H3B_MIN_PARK_FACTOR
    wind_ok = (
        _wind_eligible(g)
        and env.wind_dir_label == "OUT"
        and mph >= gdb.WIND_OUT_MIN_MPH
    )
    tot_ok = mkt.total_current is not None
    return bool(wind_ok and tot_ok and h3b_park_ok and h3b_pf_ok and not g.completeness.h3b_blocked)


def _eval_h3b(g: FullyDressedGame, mvb_fires: bool) -> SignalFinding:
    gdb = _gdb()
    env = g.environment
    mkt = g.market
    mph = _mph(g)
    venue_name = g.identifiers.venue_name or ""
    pf = float(env.park_factor_runs or 100.0)
    would = _h3b_standalone_would_fire(g)

    if mvb_fires and would:
        confirm = (
            f"H3b monitor-only: wind/total gates align (wind-out {mph:.0f} mph, PF {pf:.0f}); "
            f"no duplicate OVER pick when a higher-priority wind OVER already applies."
        )
        return SignalFinding(
            signal_id="H3b",
            signal_strength=SIGNAL_STRENGTH["H3b"],
            bet_side="over_total",
            odds=_fmt_odds(mkt.over_odds) if mkt.over_odds is not None else "-110",
            edge_basis=confirm,
            fires=False,
        )

    if mvb_fires:
        return SignalFinding(
            signal_id="H3b",
            signal_strength=SIGNAL_STRENGTH["H3b"],
            bet_side="over_total",
            odds=_fmt_odds(mkt.over_odds) if mkt.over_odds is not None else "-110",
            edge_basis=(
                f"H3b not active as separate pick (MV-B present); "
                f"wind_out={env.wind_out} whitelist/PF gates would be={would}."
            ),
            fires=False,
        )

    fires = would
    if fires:
        reason = (
            f"H3b — Wind OUT {mph:.0f} mph at {venue_name} "
            f"(whitelisted, PF {pf:.0f}). "
            f"OVER edge: 52.2% over rate on 4,625 games "
            f"(z=2.99, p=0.003 SBRO). Market under-adjusts for wind-out."
        )
    else:
        reason = (
            f"H3b blocked: standalone gates not met "
            f"(wind_out={env.wind_out} mph={mph} venue={venue_name!r} "
            f"pf={pf} h3b_blocked={g.completeness.h3b_blocked})."
        )
    return SignalFinding(
        signal_id="H3b",
        signal_strength=SIGNAL_STRENGTH["H3b"],
        bet_side="over_total",
        odds=_fmt_odds(mkt.over_odds) if mkt.over_odds is not None else "-110",
        edge_basis=reason,
        fires=fires,
    )


def _eval_avoids(g: FullyDressedGame) -> list[AvoidFinding]:
    """
    Hard avoids only — things that should persist as signal_state / brief AVOID rows.

    Venue wind suppression is NOT listed here: it is already encoded in
    GameEnvironment (env_ceiling=NoSignal, wind_effect=SUPPRESSED) and blocks
    MV-F / MV-B / H3b via gates, plus data_flags in score_game. Treating it as an
    AvoidFinding caused legacy ``avoid=True`` alongside matchup signals (e.g.
    LHP_FADE), which read as “override everything.”
    """
    avoids: list[AvoidFinding] = []
    if g.environment.is_retractable and g.environment.wind_source != "actual":
        avoids.append(
            AvoidFinding(
                avoid_type="retractable_roof_pregame",
                bet_type="wind_signals",
                reason=(
                    f"Retractable roof venue ({g.identifiers.venue_name}). "
                    f"Verify roof status before acting on wind signals."
                ),
            )
        )
    return avoids


def _is_hostile_environment(g: FullyDressedGame, signal: SignalFinding) -> bool:
    env = g.environment
    side = signal.bet_side
    if side in ("over_total", "over"):
        if env.wind_in:
            return True
        if env.temp_f is not None and float(env.temp_f) < 45:
            return True
    if side in ("under_total", "under"):
        if env.wind_out:
            return True
    return False


def _avoid_affects_top(avoids: list[AvoidFinding], top: SignalFinding) -> bool:
    for a in avoids:
        if a.bet_type in ("all", top.bet_side):
            return True
        if a.bet_type == "wind_signals" and top.signal_id in WIND_SIGNAL_IDS:
            return True
    return False


@dataclass
class ScoredGame:
    game: FullyDressedGame
    signals_fired: list[SignalFinding]
    signals_blocked: list[SignalFinding]
    avoids: list[AvoidFinding]
    output_tier: str | None
    tier_basis: str
    stake_multiplier: float
    top_pick: SignalFinding | None
    data_flags: list[str]


def score_game(g: FullyDressedGame, home_streak: int, game_month: int) -> ScoredGame:
    gdb = _gdb()
    mkt = g.market
    env = g.environment
    extra_flags: list[str] = []

    if mkt.home_ml_current is None or mkt.away_ml_current is None:
        extra_flags.append("ML odds missing — signal evaluation limited")

    if env.is_wind_suppressed:
        note = (g.venue_wind_note or "")[:80]
        extra_flags.append(
            f"Wind signals suppressed at this venue ({note or env.roof_type})"
        )
    elif env.is_retractable:
        extra_flags.append(
            "Retractable roof venue — verify roof status before acting on wind signals"
        )

    s1h2 = _eval_s1h2(g, home_streak)
    s1h2_fired = _s1h2_fired(s1h2)

    mvb = _eval_mv_b(g, game_month)
    h3b = _eval_h3b(g, mvb.fires)

    all_findings = [
        s1h2,
        _eval_mv_f(g, s1h2_fired),
        _eval_lhp_fade(g, game_month, s1h2_fired),
        mvb,
        _eval_s1(g, home_streak, s1h2_fired),
        h3b,
    ]

    fired = [f for f in all_findings if f.fires]
    blocked = [f for f in all_findings if not f.fires]
    avoids = _eval_avoids(g)

    tot = mkt.total_current
    pf = float(env.park_factor_runs or 100.0)
    july_ok = (
        game_month in gdb.JULY_OVER_MONTHS
        and tot is not None
        and gdb.JULY_OVER_MIN_TOTAL <= float(tot) <= gdb.JULY_OVER_MAX_TOTAL
        and pf >= gdb.JULY_OVER_MIN_PF
        and not env.is_wind_suppressed
    )
    fired_ids = {x.signal_id for x in fired}
    if july_ok and ("H3b" in fired_ids or "MV-B" in fired_ids):
        for s in fired:
            if s.signal_id in ("MV-B", "H3b") and s.bet_side == "over_total":
                s.edge_basis += " JulyOVER seasonal edge confirms (52.3% rate, p=0.0006)."
    elif july_ok:
        extra_flags.append(
            f"July seasonal OVER edge present (PF {pf:.0f}, total {tot}) "
            f"but no wind OVER signal to stack it with — no bet placed. "
            f"JulyOVER is a reinforcer only, not a standalone signal."
        )

    if (h3b.fires or mvb.fires) and game_month in gdb.H3B_LATE_SEASON_MONTHS:
        label = "MV-B" if mvb.fires and not h3b.fires else ("H3b" if h3b.fires else "OVER signal")
        extra_flags.append(
            f"{label} late-season caution ({g.identifiers.venue_name}): "
            f"Aug/Sep wind-out OVER rate was 22–38% in 2024 vs 50–54% Apr–Jul. "
            f"Consider reduced stake (--late-season-stake)."
        )

    if (
        _wind_eligible(g)
        and env.wind_dir_label == "OUT"
        and _mph(g) >= gdb.WIND_OUT_MIN_MPH
        and tot is not None
        and not h3b.fires
        and not mvb.fires
    ):
        vn = g.identifiers.venue_name or ""
        wl = _h3b_whitelist()
        if vn not in wl:
            extra_flags.append(
                f"Wind OUT {_mph(g):.0f} mph but {vn or 'this venue'} is not on the "
                f"H3b whitelist — wind reading may not reflect in-play conditions."
            )
        elif float(env.park_factor_runs or 100.0) < gdb.H3B_MIN_PARK_FACTOR:
            extra_flags.append(
                f"Wind OUT {_mph(g):.0f} mph at {vn} but PF "
                f"{float(env.park_factor_runs or 100.0):.0f} < {gdb.H3B_MIN_PARK_FACTOR} "
                f"— pitcher-friendly park offsets wind-out OVER edge."
            )

    data_flags = list(g.completeness.gaps) + extra_flags

    if not fired:
        return ScoredGame(
            game=g,
            signals_fired=[],
            signals_blocked=blocked,
            avoids=avoids,
            output_tier=None,
            tier_basis="no_signal",
            stake_multiplier=0.0,
            top_pick=None,
            data_flags=data_flags,
        )

    top = min(fired, key=lambda f: SIGNAL_PRIORITY.get(f.signal_id, 99))
    signal_strength = SIGNAL_STRENGTH[top.signal_id]
    base_tier = strength_to_base_tier(signal_strength)
    assert base_tier is not None

    if top.signal_id in WIND_SIGNAL_IDS:
        env_cap = _env_ceiling_to_cap_tier(env.env_ceiling)
        effective_tier = min_tier(base_tier, env_cap)
        env_note = f"env_ceiling={env.env_ceiling}"
    else:
        effective_tier = base_tier
        env_note = "env=N/A (matchup signal)"

    hostile = _is_hostile_environment(g, top)
    if hostile:
        effective_tier = min_tier(effective_tier, "Tier2")

    if g.completeness.completeness_tier == "degraded":
        effective_tier = min_tier(effective_tier, "Tier2")
    if g.completeness.completeness_tier == "blocking":
        effective_tier = min_tier(effective_tier, "Tier3")

    if avoids and _avoid_affects_top(avoids, top):
        effective_tier = "Avoid"

    # Dual LHP boosters: document only; N<50 keeps moderate tier (no upgrade to Tier1).
    tier_basis = (
        f"signal={signal_strength}({top.signal_id}), "
        f"{env_note}, "
        f"completeness={g.completeness.completeness_tier}, "
        f"hostile={hostile}"
    )
    if top.signal_id == "LHP_FADE":
        away = g.matchup.away_sp
        home_off = g.matchup.home_offense
        era_b = away.quality_tier == "strong"
        ops_b = (
            home_off.rolling_ops is not None
            and float(home_off.rolling_ops) >= gdb.NF4_OPS_MIN
        )
        if era_b and ops_b:
            tier_basis += "; lhp_dual_boosters=pending_Tier1_until_N50"

    stake = {"Tier1": 1.0, "Tier2": 0.5, "Tier3": 0.0, "Avoid": 0.0}.get(
        effective_tier or "", 0.0
    )

    return ScoredGame(
        game=g,
        signals_fired=fired,
        signals_blocked=blocked,
        avoids=avoids,
        output_tier=effective_tier,
        tier_basis=tier_basis,
        stake_multiplier=stake,
        top_pick=top,
        data_flags=data_flags,
    )


def _game_row_for_dress(game: dict[str, Any]) -> dict[str, Any]:
    row = dict(game)
    row["game_date_et"] = str(row.get("game_date_et") or row.get("game_date") or "")
    if row.get("wind_source") in (None, ""):
        row["wind_source"] = "actual"
    if row.get("season") is None:
        try:
            row["season"] = int(str(row["game_date_et"])[:4])
        except (TypeError, ValueError):
            row["season"] = 2025
    return row


def dress_game_for_brief(conn: sqlite3.Connection, game: dict[str, Any]) -> FullyDressedGame:
    """
    Dress a brief ``game`` row to ``FullyDressedGame`` (DB bundle).
    Caller must inject starters into ``game`` first (``enrich_game_with_starters`` / ``enrich_game``).
    """
    row = _game_row_for_dress(game)
    fdg = dress_full_game_row(conn, row)
    return replace(
        fdg,
        venue_wind_note=str(game.get("wind_note") or "") or None,
    )


def fully_dressed_from_game_dict(
    conn: sqlite3.Connection,
    game: dict[str, Any],
    streaks: dict[int, int],
    session: str,
) -> FullyDressedGame:
    fdg = dress_game_for_brief(conn, game)
    hid = int(game["home_team_id"])
    aid = int(game["away_team_id"])
    return replace(
        fdg,
        brief_session=session,
        home_streak=int(streaks.get(hid, 0)),
        away_streak=int(streaks.get(aid, 0)),
    )


def scored_game_to_eval_dict(scored: ScoredGame, session: str) -> dict[str, Any]:
    """Map ScoredGame → legacy evaluate_signals() dict for briefs and backtests."""
    g = scored.game
    ids = g.identifiers
    mkt = g.market

    legacy_signals: list[str] = []
    for s in scored.signals_fired:
        if s.signal_id == "S1H2":
            legacy_signals.append("S1+H2")
        elif s.signal_id == "LHP_FADE":
            legacy_signals.append("LHP_FADE")
            legacy_signals.append("NF4")
        else:
            legacy_signals.append(s.signal_id)

    gdb = _gdb()
    month = _game_month(ids.game_date_et)
    tot = mkt.total_current
    pf = float(g.environment.park_factor_runs or 100.0)
    july_ok = (
        month in gdb.JULY_OVER_MONTHS
        and tot is not None
        and gdb.JULY_OVER_MIN_TOTAL <= float(tot) <= gdb.JULY_OVER_MAX_TOTAL
        and pf >= gdb.JULY_OVER_MIN_PF
        and not g.environment.is_wind_suppressed
    )
    if july_ok and ("H3b" in legacy_signals or "MV-B" in legacy_signals):
        if "JulyOVER" not in legacy_signals:
            legacy_signals.append("JulyOVER")

    picks: list[dict[str, Any]] = []
    total_pick: dict[str, Any] | None = None

    fired_sorted = sorted(
        scored.signals_fired,
        key=lambda s: SIGNAL_PRIORITY.get(s.signal_id, 99),
    )
    for s in fired_sorted:
        pr = SIGNAL_PRIORITY.get(s.signal_id, 9)
        if s.bet_side == "away_ml":
            picks.append(
                {
                    "bet": f"{ids.away_team_abbr} ML",
                    "market": "ML",
                    "odds": s.odds,
                    "reason": s.edge_basis,
                    "priority": pr,
                }
            )
        elif s.bet_side == "over_total":
            tline = mkt.total_current
            bet_txt = f"OVER {tline}" if tline is not None else "OVER"
            if total_pick is None:
                total_pick = {
                    "bet": bet_txt,
                    "market": "TOTAL",
                    "odds": s.odds,
                    "reason": s.edge_basis,
                    "priority": pr,
                }
                picks.append(total_pick)
            else:
                total_pick["reason"] += " " + s.edge_basis
                total_pick["priority"] = min(total_pick["priority"], pr)

    watch = False
    watch_reason = None
    if session == "morning":
        env = g.environment
        mph = _mph(g)
        streak_label = ""
        if g.home_streak >= gdb.S1_STANDALONE_MIN:
            streak_label = f"W{g.home_streak}"
        elif g.home_streak <= -gdb.STREAK_THRESHOLD:
            streak_label = f"L{abs(g.home_streak)}"
        if (
            _wind_eligible(g)
            and env.wind_dir_label in ("OUT", "IN")
            and mph >= gdb.WIND_OUT_MIN_MPH
        ):
            watch = True
            watch_reason = (
                f"Wind {env.wind_dir_label} {mph:.0f} mph — monitor for signal at Primary Brief. "
                f"Do not bet on opening lines alone."
            )
        if g.home_streak >= gdb.S1_STANDALONE_MIN or g.home_streak <= -4:
            watch = True
            extra = (
                f"Home team streak situation: {streak_label or 'active'}. "
                f"Track lineup and odds movement through Primary Brief."
            )
            watch_reason = (
                (watch_reason + " | " if watch_reason else "") + extra
            )

    # Legacy ``avoid``: do not conflate “venue note” with “this card is an AVOID.”
    # If any signal fires, soft environment avoids must not set avoid=True (Word
    # brief showed picks + AVOID banner). Hard avoid = tier Avoid, or no signals
    # but a persisted AvoidFinding (e.g. retractable roof pregame).
    hard_avoid = bool(scored.avoids) and (
        scored.output_tier == "Avoid" or not scored.signals_fired
    )
    return {
        "signals": legacy_signals,
        "picks": picks,
        "avoid": hard_avoid,
        "avoid_reason": (
            "; ".join(a.reason for a in scored.avoids) if scored.avoids else None
        ),
        "watch": watch,
        "watch_reason": watch_reason,
        "data_flags": scored.data_flags,
    }


def evaluate_signals_scored(
    conn: sqlite3.Connection,
    game: dict[str, Any],
    streaks: dict[int, int],
    session: str,
    starters: dict[str, Any] | None = None,
) -> ScoredGame:
    """Dress + score (same path as ``generate_daily_brief.evaluate_signals``)."""
    import batch.pipeline.generate_daily_brief as gdb

    fdg = gdb.enrich_game(conn, game, starters or {})
    hid = int(game["home_team_id"])
    aid = int(game["away_team_id"])
    fdg = replace(
        fdg,
        brief_session=session,
        home_streak=int(streaks.get(hid, 0)),
        away_streak=int(streaks.get(aid, 0)),
    )
    gd = fdg.identifiers.game_date_et
    game_month = int(gd[5:7]) if len(gd) >= 7 else 0
    return score_game(fdg, fdg.home_streak, game_month)
