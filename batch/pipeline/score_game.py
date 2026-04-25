"""
Central signal scoring — all model signal if/else logic lives here.

``score_game(FullyDressedGame, home_streak: int, game_month: int) -> ScoredGame``;
``generate_daily_brief.enrich_game`` + ``evaluate_signals`` dress the row and map
``ScoredGame`` back to the legacy dict via ``scored_game_to_eval_dict``.
"""

from __future__ import annotations

import sqlite3
import os
from dataclasses import dataclass, field, replace
from typing import Any

from batch.pipeline.dressed_game_blocks import (
    AvoidFinding,
    FullyDressedGame,
    SignalFinding,
    dress_full_game_row,
)
from batch.pipeline.edge_utils import (
    EDGE_MAX,
    EDGE_MIN,
    EDGE_STRONG,
    american_to_implied_prob,
    compute_edge,
    fractional_kelly,
    score_to_model_prob,
)

# ── Tier helpers (ScoredGame policy) ───────────────────────────────────────
# Higher index = less confident. min_tier returns the worse (less confident) tier.

TIER_ORDER: list[str | None] = ["Tier1", "Tier2", "Tier3", "Avoid", None]


def min_tier(a: str | None, b: str | None) -> str | None:
    """Return the lower (less confident) of two tier values."""
    ai = TIER_ORDER.index(a) if a in TIER_ORDER else len(TIER_ORDER)
    bi = TIER_ORDER.index(b) if b in TIER_ORDER else len(TIER_ORDER)
    return TIER_ORDER[max(ai, bi)]


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
    "LHP_FADE_RL": 4,
    "MV-B": 5,
    "S1": 6,
    "H3b": 7,
}

SIGNAL_STRENGTH: dict[str, str] = {
    "S1H2": "strong",
    "MV-F": "strong",
    "MV-B": "strong",
    "H3b": "moderate",
    "LHP_FADE": "moderate",
    "LHP_FADE_RL": "moderate",
    "S1": "weak",
}

WIND_SIGNAL_IDS = frozenset({"MV-F", "MV-B", "H3b"})

SIGNAL_BASE_SCORE: dict[str, int] = {
    "S1H2": 8,
    "MV-F": 8,
    "MV-B": 7,
    "LHP_FADE": 7,
    "LHP_FADE_RL": 6,
    "S1": 5,
    "H3b": 3,
    "NF4": 3,
}

# Short reader-facing names for briefs (no internal codes shown to end users)
SIGNAL_DISPLAY_NAME: dict[str, str] = {
    "S1H2": "Streak Fade",
    "S1+H2": "Streak Fade",
    "MV-F": "Wind Fade (ML)",
    "MV-B": "Wind Boost (Over)",
    "H3b": "Wind → Over",
    "LHP_FADE": "LHP Mismatch",
    "LHP_FADE_RL": "LHP RL Edge",
    "S1": "Streak Pressure",
    "NF4": "Pitching Edge",
    "JulyOVER": "July over boost",
    "S6": "Hot pitcher fade",
}

BETTING_THRESHOLD = 5
FULL_STAKE_THRESHOLD = 7


def _fmt_odds(val: Any) -> str:
    if val is None:
        return "N/A"
    v = int(val)
    return f"+{v}" if v > 0 else str(v)


def signal_display_name(signal_id: str) -> str:
    """
    Map internal ``signal_id`` (may be compound ``A+B``) to a short reader label.
    Unknown ids fall back to the raw id (rare; helps catch new evaluators in dev).
    """
    raw = (signal_id or "").strip()
    if not raw:
        return ""
    if "+" in raw:
        parts = [p.strip() for p in raw.split("+") if p.strip()]
        return " + ".join(signal_display_name(p) for p in parts)
    return SIGNAL_DISPLAY_NAME.get(raw, raw)


def aggregate_score_to_confidence_pct(score: int) -> int:
    """Map bucket aggregate (sum of per-signal 1–10 scores on that side) to 50–95%."""
    s = int(score)
    return min(95, max(50, int(50 + (s - 5) * 2)))


def format_aggregate_for_brief(aggregate: int | None, tier: str | None) -> str:
    """
    Aggregated bucket total from ``score_game`` (not a single-signal 1–10) → display
    ``[HIGH · 95%]`` / ``[MED · XX%]`` / ``[LOW · XX%]``. Percent uses
    ``min(95, max(50, int(50 + (score - 5) * 2)))``. HIGH / MED / LOW follow
    ``output_tier``; if tier is missing, infer from the same 10 / 7 / 5 cutoffs as
    ``score_game``.
    """
    if aggregate is None:
        return ""
    n = int(aggregate)
    if n <= 0:
        return ""
    pct = aggregate_score_to_confidence_pct(n)
    t = (tier or "").strip()
    if t == "Tier1":
        band = "HIGH"
    elif t == "Tier2":
        band = "MED"
    elif t == "Tier3":
        band = "LOW"
    else:
        band = "HIGH" if n >= 10 else ("MED" if n >= 7 else "LOW")
    return f"[{band} · {pct}%]"


def _dedup_signal_id_scores(
    id_score_pairs: list[tuple[str, int]],
) -> list[tuple[str, int]]:
    """One row per ``signal_id``, highest ``confidence_score`` wins; sort score desc, then id."""
    best: dict[str, int] = {}
    for sid, sc in id_score_pairs:
        s = (sid or "").strip()
        if not s:
            continue
        v = int(sc)
        if s not in best or v > best[s]:
            best[s] = v
    return sorted(best.items(), key=lambda x: (-x[1], x[0]))


def format_signal_brief_scored(
    id_score_pairs: list[tuple[str, int]],
) -> str:
    """
    Build the "Signal stack" for the brief: human-readable labels only, sorted by
    per-signal ``confidence_score`` (descending), then grouped:
    score >= 7 → Core, 5–6 → Support, <= 4 → Minor.
    """
    ordered = _dedup_signal_id_scores(id_score_pairs)
    core: list[str] = []
    support: list[str] = []
    minor: list[str] = []
    for sid, sc in ordered:
        label = signal_display_name(sid)
        if not label:
            continue
        if sc >= 7:
            core.append(label)
        elif sc >= 5:
            support.append(label)
        else:
            minor.append(label)
    lines: list[str] = ["Signal stack"]
    if core:
        lines.append("  Core: " + " · ".join(core))
    if support:
        lines.append("  Support: " + " · ".join(support))
    if minor:
        lines.append("  Minor: " + " · ".join(minor))
    if len(lines) == 1:
        return ""
    return "\n".join(lines)


def _breakeven(odds: int) -> float:
    """Return breakeven win probability for American odds (e.g., -150, +120)."""
    o = int(odds)
    if o < 0:
        return (-o) / ((-o) + 100.0)
    return 100.0 / (o + 100.0)


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
    comp_ok = not g.completeness.mvf_blocked
    fires = wind_ok and comp_ok and (not s1h2_fired)

    clv_txt = "CLV note: monitor the current line vs morning open."
    if mkt.clv_available and mkt.clv_away_delta is not None:
        clv_txt += f" Current vs open (away implied delta): {mkt.clv_away_delta:+.2f}pp."
    clv_txt += (
        f" Historically, CLV-positive entries performed better; treat CLV as a soft modifier only."
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


def _eval_lhp_fade(
    g: FullyDressedGame, game_month: int, s1h2_fired: bool
) -> list[SignalFinding]:
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
    ml_finding = SignalFinding(
        signal_id="LHP_FADE",
        signal_strength=SIGNAL_STRENGTH["LHP_FADE"],
        bet_side="away_ml",
        odds=_fmt_odds(mkt.away_ml_current),
        edge_basis=reason,
        fires=fires,
    )

    rl_finding: SignalFinding | None = None
    # Supplementary RL — only when ERA gate confirmed and RL odds are present.
    if (
        away.quality_tier == "strong"
        and mkt.rl_available
        and mkt.away_rl_odds is not None
    ):
        be = _breakeven(int(mkt.away_rl_odds))
        rl_finding = SignalFinding(
            signal_id="LHP_FADE_RL",
            signal_strength="moderate",
            bet_side="away_rl",
            odds=_fmt_odds(mkt.away_rl_odds),
            edge_basis=(
                "LHP_FADE supplementary RL — ERA gate confirmed. "
                f"OW era 2022-2025: 66.1% cover rate vs {be:.1%} breakeven. "
                "Higher hit rate than ML at lower ROI per unit. "
                "Choose ML for max ROI, RL for higher hit rate."
            ),
            fires=bool(fires),
            confidence_score=0,  # computed from ML score later (ML-1)
            score_basis="pending",
        )

    return [f for f in (ml_finding, rl_finding) if f is not None]


def _eval_mv_b(g: FullyDressedGame, game_month: int) -> SignalFinding:
    gdb = _gdb()
    env = g.environment
    mkt = g.market
    mph = _mph(g)
    pf = float(env.park_factor_runs or 0.0)

    we = (env.wind_effect or "").strip().upper()
    fires = (
        env.wind_out
        and mph >= gdb.WIND_OUT_MIN_MPH
        and we in ("HIGH", "MODERATE")
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
            f"CLV note: line movement is a soft modifier only (not a gate)."
        )
    else:
        reason = (
            f"MV-B blocked: wind_out={env.wind_out} mph={mph} "
            f"wind_effect={env.wind_effect!r} (need HIGH/MODERATE) h3b_eligible={env.h3b_eligible} "
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
    fires = (
        home_streak >= gdb.S1_STANDALONE_MIN
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
            f"home_ml={mkt.home_ml_current!r}"
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
    Hard avoids for signal_state / brief AVOID rows.

    Retractable roof is not modeled as an outcome factor and does not emit
    AvoidFindings. Venue wind suppression remains on ``GameEnvironment`` only.
    """
    return []


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


def _compute_confidence_score(
    signal_id: str,
    fdg: FullyDressedGame,
    game_month: int,
    clv_delta_pp: float | None,
    second_signal: bool,
    hostile: bool,
) -> tuple[int, str]:
    """
    Returns (score, basis_string).
    Score is capped to [1, 10].
    """
    gdb = _gdb()
    base = SIGNAL_BASE_SCORE.get(signal_id, 5)
    mods: list[tuple[str, int]] = []

    if signal_id == "MV-F":
        # CLV is a soft modifier only (no blocking): negative → -1, positive → +1 (optional).
        if clv_delta_pp is not None:
            if float(clv_delta_pp) < 0:
                mods.append(("CLV negative (market faded)", -1))
            elif float(clv_delta_pp) > 0:
                mods.append(("CLV positive (market confirmed)", +1))

    if signal_id == "MV-B":
        w = float(fdg.environment.wind_mph or 0)
        if 10 <= w <= 11:
            mods.append(("wind 10-11 mph (historically strongest bucket)", +1))

    if signal_id in ("LHP_FADE", "NF4"):
        away_sp = fdg.matchup.away_sp
        home_off = fdg.matchup.home_offense
        if away_sp.quality_tier == "strong":
            mods.append(("away SP ERA gate met (<=3.04)", +1))
        ops_min = gdb.NF4_OPS_MIN
        if home_off.rolling_ops is not None and float(home_off.rolling_ops) >= ops_min:
            mods.append((f"home OPS gate met (>={ops_min})", +1))
        else:
            base = 6
        if game_month == 9:
            mods.append(("September — signal historically inverts", -1))

    wind_signals = {"MV-F", "MV-B", "H3b"}
    if signal_id in wind_signals:
        src = (fdg.environment.wind_source or "").strip().lower()
        mph = float(fdg.environment.wind_mph or 0)
        if src == "actual" and mph >= 15:
            mods.append(("actual wind confirmed >=15 mph", +1))
        elif src == "actual":
            mods.append(("actual wind confirmed", 0))
        elif src == "forecast":
            mods.append(("forecast wind — not yet confirmed", 0))
        else:
            mods.append(("wind source unknown", -2))

    comp = fdg.completeness
    if comp.completeness_tier == "complete":
        mods.append(("data complete", +1))
    elif comp.completeness_tier == "degraded":
        mods.append(("data degraded (fallbacks used)", 0))
    elif comp.completeness_tier == "blocking":
        mods.append(("blocking data gap", -2))

    if second_signal:
        mods.append(("second independent signal confirms", +1))

    if hostile:
        mods.append(("hostile environment — signal contradicted", -1))

    if signal_id == "MV-B":
        home_sp = fdg.matchup.home_sp
        away_sp = fdg.matchup.away_sp
        if home_sp.quality_tier == "weak" and away_sp.quality_tier == "weak":
            mods.append(("weak vs weak matchup", -1))
        elif away_sp.quality_tier == "strong":
            mods.append(("away SP strong — confirms OVER edge", +1))

    if signal_id == "H3b" and game_month in (8, 9):
        mods.append(("Aug/Sep — H3b historically weak", -1))

    total_mod = sum(v for _, v in mods)
    score = max(1, min(10, base + total_mod))

    mod_str = "  |  ".join(f"{name} ({v:+d})" for name, v in mods if v != 0)
    basis = f"base={base}"
    if mod_str:
        basis += f"  →  {mod_str}  →  total={score}"
    else:
        basis += f"  →  total={score}"

    return score, basis


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
    active_bets: list[SignalFinding] = field(default_factory=list)
    all_bets: list[SignalFinding] = field(default_factory=list)
    watch_list: list[SignalFinding] = field(default_factory=list)
    contradicted: list[SignalFinding] = field(default_factory=list)
    # Sum of per-signal 1–10 scores per bet_side bucket; tier uses max bucket total
    aggregated_by_side: dict[str, int] = field(default_factory=dict)
    best_side: str | None = None
    best_aggregate_score: int = 0
    model_p: float | None = None
    implied_p: float | None = None
    edge: float | None = None
    eval_status: str | None = None
    # Per-market evaluation to prevent cross-market signal leakage (ML/TOTAL/RL scored independently)
    market_evals: dict[str, dict[str, Any]] = field(default_factory=dict)


def score_game(g: FullyDressedGame, home_streak: int, game_month: int) -> ScoredGame:
    try:
        game_pk = int(g.identifiers.game_pk)
    except Exception:
        game_pk = -1
    print(f"[ENTER score_game] {game_pk}")

    gdb = _gdb()
    mkt = g.market
    env = g.environment

    extra_flags: list[str] = []

    if mkt.home_ml_current is None or mkt.away_ml_current is None:
        extra_flags.append("ML odds missing — signal evaluation limited")

    if env.is_wind_suppressed:
        note = (g.venue_wind_note or "")[:80]
        extra_flags.append(f"Wind signals suppressed at this venue ({note or env.roof_type})")

    # Confidence penalty for missing features (keeps model alive under partial data).
    # Applies at probability-layer (model_p), not as a hard block.
    try:
        missing_features = len(g.completeness.gaps or [])
    except Exception:
        missing_features = 0
    confidence_penalty = float(missing_features) * 0.01

    # --- Prior tier / env-cap logic (kept for reference, no longer used) ---
    # top_pick = active_bets[0] if active_bets else None
    # if top_pick is not None:
    #     if top_pick.confidence_score >= 9:
    #         effective_tier = "Tier1"
    #     elif top_pick.confidence_score >= 7:
    #         effective_tier = "Tier2"
    #     else:
    #         effective_tier = "Tier3"
    # else:
    #     effective_tier = None
    # if top_pick is not None:
    #     if top_pick.signal_id in WIND_SIGNAL_IDS:
    #         env_cap = _env_ceiling_to_cap_tier(env.env_ceiling)
    #         effective_tier = min_tier(effective_tier, env_cap)
    #     hostile = _is_hostile_environment(g, top_pick)
    #     if hostile:
    #         effective_tier = min_tier(effective_tier, "Tier2")
    # if effective_tier is not None:
    #     if g.completeness.completeness_tier == "degraded":
    #         effective_tier = min_tier(effective_tier, "Tier2")
    #     if g.completeness.completeness_tier == "blocking":
    #         effective_tier = min_tier(effective_tier, "Tier3")

    # --- Generate signals (unchanged) ---
    s1h2 = _eval_s1h2(g, home_streak)
    s1h2_fired = s1h2.fires

    mvf = _eval_mv_f(g, s1h2_fired)
    lhp_findings = _eval_lhp_fade(g, game_month, s1h2_fired)
    mvb = _eval_mv_b(g, game_month)
    s1 = _eval_s1(g, home_streak, s1h2_fired)
    h3b = _eval_h3b(g, mvb.fires)

    all_signals = [s1h2, mvf, *lhp_findings, mvb, s1, h3b]
    avoids = _eval_avoids(g)
    blocked = [f for f in all_signals if not f.fires]

    if os.getenv("DEBUG_SCORE_GAME") == "1":
        try:
            game_pk = int(g.identifiers.game_pk)
        except Exception:
            game_pk = -1
        signals = [f"{s.signal_id}({'Y' if s.fires else 'n'})" for s in all_signals]
        print(f"[DEBUG] {game_pk}: signals={signals}")

    # --- Score all signals (no fires gate) ---
    scored_signals: list[SignalFinding] = []
    for sig in all_signals:
        hostile = _is_hostile_environment(g, sig)

        clv_delta_pp = float(mkt.clv_away_delta) if (mkt.clv_available and mkt.clv_away_delta is not None) else None

        score, basis = _compute_confidence_score(
            signal_id=sig.signal_id,
            fdg=g,
            game_month=game_month,
            clv_delta_pp=clv_delta_pp,
            second_signal=False,
            hostile=hostile,
        )

        sig.confidence_score = int(score)
        sig.score_basis = basis
        scored_signals.append(sig)

    if os.getenv("DEBUG_SCORE_GAME") == "1":
        try:
            game_pk = int(g.identifiers.game_pk)
        except Exception:
            game_pk = -1
        signals = [
            f"{s.signal_id}({'Y' if s.fires else 'n'}):{int(s.confidence_score or 0)}"
            for s in scored_signals
        ]
        print(f"[DEBUG BEFORE FILTER] {game_pk}: signals={signals}")
        score = sum(int(s.confidence_score or 0) for s in scored_signals if bool(s.fires))
        print(f"[DEBUG] {game_pk}: final_score={score}")

    # --- Aggregate by bet side ---
    buckets: dict[str, list[SignalFinding]] = {}
    for sig in scored_signals:
        if not bool(sig.fires):
            continue
        if not sig.bet_side:
            continue
        buckets.setdefault(sig.bet_side, []).append(sig)

    if os.getenv("DEBUG_SCORE_GAME") == "1":
        try:
            game_pk = int(g.identifiers.game_pk)
        except Exception:
            game_pk = -1
        after: list[str] = []
        for side in sorted(buckets.keys()):
            for s in buckets[side]:
                after.append(f"{s.signal_id}({side}):{int(s.confidence_score or 0)}")
        signals = after
        print(f"[DEBUG AFTER FILTER] {game_pk}: signals={signals}")

    aggregated_scores: dict[str, int] = {}
    for side, sigs in buckets.items():
        non_wind_total = sum(
            int(s.confidence_score or 0) for s in sigs if s.signal_id not in WIND_SIGNAL_IDS
        )
        wind_present = any((s.signal_id in WIND_SIGNAL_IDS) and bool(s.fires) for s in sigs)
        wind_bonus = 1 if wind_present else 0
        aggregated_scores[side] = int(non_wind_total) + int(wind_bonus)

    # --- Per-market evaluations (signals remain game-level; each market decides applicability) ---
    def _eval_market(market: str) -> dict[str, Any]:
        # candidate bet_side keys for this market
        if market == "ML":
            candidates = [k for k in aggregated_scores.keys() if k.endswith("_ml")]
        elif market == "TOTAL":
            candidates = [k for k in aggregated_scores.keys() if k.endswith("_total")]
        elif market == "RL":
            candidates = [k for k in aggregated_scores.keys() if k.endswith("_rl")]
        else:
            candidates = []

        # Do not gate/skip scoring based on missing market data.
        # Missing odds/lines should flow through as NO_MODEL (implied_p/edge None), not "not evaluated".
        market_supported = market in ("ML", "TOTAL", "RL")

        market_out: dict[str, Any]
        if not market_supported:
            market_out = {"evaluated": False, "best_side": None, "score": 0}
        else:
            best_side_m: str | None = None
            best_score_m = 0
            for side in candidates:
                sc = int(aggregated_scores.get(side, 0) or 0)
                if sc > best_score_m:
                    best_score_m = sc
                    best_side_m = side

            # Resolve odds and bet text per market
            bet_txt = ""
            odds_taken: int | None = None
            if market == "ML":
                if best_side_m == "away_ml":
                    odds_taken = g.market.away_ml_current
                    bet_txt = f"{g.identifiers.away_team_abbr} ML"
                elif best_side_m == "home_ml":
                    odds_taken = g.market.home_ml_current
                    bet_txt = f"{g.identifiers.home_team_abbr} ML"
            elif market == "TOTAL":
                tline = g.market.total_current
                if best_side_m == "over_total":
                    odds_taken = g.market.over_odds
                    bet_txt = f"OVER {tline}" if tline is not None else "OVER"
                elif best_side_m == "under_total":
                    odds_taken = g.market.under_odds
                    bet_txt = f"UNDER {tline}" if tline is not None else "UNDER"
            elif market == "RL":
                rl_line = g.market.away_rl_line
                if best_side_m == "away_rl":
                    odds_taken = g.market.away_rl_odds
                    bet_txt = (
                        f"{g.identifiers.away_team_abbr} {rl_line:+g}"
                        if rl_line is not None
                        else f"{g.identifiers.away_team_abbr} +1.5"
                    )

            # Compute edge only if we have odds
            model_p_m = score_to_model_prob(int(best_score_m))
            if model_p_m is not None and confidence_penalty > 0:
                model_p_m = max(0.50, float(model_p_m) - float(confidence_penalty))
            implied_p_m = american_to_implied_prob(int(odds_taken) if odds_taken is not None else None)
            edge_m = compute_edge(float(model_p_m), implied_p_m)
            edge_ok_m = (edge_m is not None) and (edge_m >= EDGE_MIN)

            # Classification (skipped edge capture)
            if model_p_m is None or implied_p_m is None:
                eval_status = "NO_MODEL"
            elif edge_m is not None and edge_m >= EDGE_MIN:
                eval_status = "BET"
            elif edge_m is not None and edge_m > 0:
                eval_status = "SKIPPED_EDGE"
            else:
                eval_status = "NO_EDGE"

            # Signals used for this market/side (do NOT apply any post-threshold filtering here)
            fired_for_side = list(buckets.get(best_side_m or "", []) or [])
            signal_ids_used = [s.signal_id for s in fired_for_side if bool(s.fires)]
            # Dedup but keep stable order
            seen: set[str] = set()
            signal_ids_used = [x for x in signal_ids_used if not (x in seen or seen.add(x))]
            signals_used = [signal_display_name(sid) for sid in signal_ids_used]

            market_out = {
                "evaluated": True,
                "best_side": best_side_m,
                "score": int(best_score_m),
                "bet": bet_txt,
                "odds": odds_taken,
                "model_p": float(model_p_m),
                "implied_p": float(implied_p_m) if implied_p_m is not None else None,
                "edge": float(edge_m) if edge_m is not None else None,
                "edge_ok": bool(edge_ok_m),
                "eval_status": eval_status,
                "signal_ids": signal_ids_used,
                "signals": signals_used,
            }

        return market_out

    market_evals = {
        "ML": _eval_market("ML"),
        "TOTAL": _eval_market("TOTAL"),
        "RL": _eval_market("RL"),
    }

    # --- Pick best side ---
    best_side: str | None = None
    best_score = 0
    for side, total in aggregated_scores.items():
        if total > best_score:
            best_score = total
            best_side = side

    # --- env_ceiling penalty (soft, never blocks) ---
    env_penalty = 0
    ec = (g.environment.env_ceiling or "").strip()
    if ec == "NoSignal":
        env_penalty = -2
    elif ec == "Tier2":
        env_penalty = -1
    best_score = max(0, int(best_score) + int(env_penalty))

    # --- EDGE MODEL INTEGRATION ---
    # 1) Map side → odds (totals disabled for now)
    home_ml = g.market.home_ml_current
    away_ml = g.market.away_ml_current
    if best_side == "away_ml":
        odds = away_ml
    elif best_side == "home_ml":
        odds = home_ml
    else:
        odds = None

    # 2) Convert to probabilities
    model_p = score_to_model_prob(int(best_score))
    if model_p is not None and confidence_penalty > 0:
        model_p = max(0.50, float(model_p) - float(confidence_penalty))
    implied_p = american_to_implied_prob(int(odds) if odds is not None else None)
    edge = compute_edge(model_p, implied_p)

    # 3) Decide whether to bet — EDGE is necessary, plus minimum signal diversity.
    # Diversity rule: need at least one matchup-based and one context-based driver.
    matchup_ids = {"LHP_FADE", "LHP_FADE_RL", "NF4"}
    context_ids = {"S1H2", "S1+H2", "S1"}
    fired_best = [s for s in buckets.get(best_side or "", []) if bool(s.fires)] if best_side else []
    has_matchup = any(s.signal_id in matchup_ids for s in fired_best)
    has_context = any(s.signal_id in context_ids for s in fired_best)
    diversity_ok = bool(has_matchup and has_context)

    edge_ok = (edge is not None) and (edge >= EDGE_MIN) and diversity_ok

    # Overall evaluation status (best-side ML only; totals/RL are per-market in market_evals)
    if model_p is None or implied_p is None:
        eval_status = "NO_MODEL"
    elif edge is not None and edge >= EDGE_MIN:
        eval_status = "BET"
    elif edge is not None and edge > 0:
        eval_status = "SKIPPED_EDGE"
    else:
        eval_status = "NO_EDGE"

    # 4) Size the bet (fractional Kelly)
    stake_frac = 0.0
    if edge_ok and odds is not None:
        stake_frac = fractional_kelly(model_p, int(odds), fraction=0.25)

    # 5) Override tier / stake based on edge (caps are unit-sized for now)
    if not edge_ok:
        tier = None
        stake = 0.0
    else:
        if edge >= EDGE_STRONG:
            tier = "Tier1"
            stake = min(0.5, stake_frac)
        else:
            tier = "Tier2"
            stake = min(0.25, stake_frac)

    # --- Build outputs ---
    active_bets: list[SignalFinding] = []
    top_pick: SignalFinding | None = None
    if best_side:
        relevant = buckets.get(best_side, [])
        active_bets = list(relevant)
        if relevant:
            top_pick = max(relevant, key=lambda s: int(s.confidence_score or 0))

    # Customer briefs: gaps + venue/odds notes, plus calibration line for analysis
    implied_txt = f"{implied_p:.3f}" if implied_p is not None else "NA"
    edge_txt = f"{edge:.3f}" if edge is not None else "NA"
    data_flags = list(g.completeness.gaps) + extra_flags + [
        f"CALIB score={int(best_score)} model_p={model_p:.3f} implied_p={implied_txt} edge={edge_txt}"
    ]

    return ScoredGame(
        game=g,
        signals_fired=scored_signals,
        signals_blocked=blocked,
        avoids=avoids,
        output_tier=tier,
        tier_basis="aggregated_scoring",
        stake_multiplier=stake,
        top_pick=top_pick,
        data_flags=data_flags,
        active_bets=active_bets,
        all_bets=scored_signals,
        watch_list=[],
        contradicted=[],
        aggregated_by_side=aggregated_scores,
        best_side=best_side,
        best_aggregate_score=int(best_score),
        model_p=float(model_p) if model_p is not None else None,
        implied_p=float(implied_p) if implied_p is not None else None,
        edge=float(edge) if edge is not None else None,
        eval_status=eval_status,
        market_evals=market_evals,
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

    # Internal ids (model fire order) + July reinforcer as synthetic tag (analytics)
    internal_signal_ids: list[str] = [s.signal_id for s in scored.signals_fired]

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
    fired_id_set = {s.signal_id for s in scored.signals_fired}
    if july_ok and (fired_id_set & {"H3b", "MV-B"}) and "JulyOVER" not in internal_signal_ids:
        internal_signal_ids.append("JulyOVER")

    # Brief: (signal_id, per-signal confidence) for display grouping — sorted by score in output
    brief_findings: list[tuple[str, int]] = [
        (s.signal_id, int(s.confidence_score or 0)) for s in scored.signals_fired
    ]
    if july_ok and (fired_id_set & {"H3b", "MV-B"}) and "JulyOVER" not in {
        a for a, _ in brief_findings
    }:
        july_sc = 5
        for s in scored.signals_fired:
            if s.signal_id in ("H3b", "MV-B"):
                july_sc = max(july_sc, int(s.confidence_score or 0))
        brief_findings.append(("JulyOVER", july_sc))

    # Customer briefs: reader labels, best score per id, ordered by score (high → low)
    display_signals: list[str] = [
        signal_display_name(sid) for sid, _ in _dedup_signal_id_scores(brief_findings)
    ]

    picks: list[dict[str, Any]] = []
    # NO SIGNAL policy is driven by aggregated scoring only:
    # if best_aggregate_score < 5 → no pick; otherwise show a card for best_side.
    best_score = int(scored.best_aggregate_score or 0)
    if best_score >= 5 and scored.best_side:
        lhp_rl = next((s for s in scored.signals_fired if s.signal_id == "LHP_FADE_RL"), None)
        active_sorted = sorted(
            (s for s in (scored.active_bets or []) if s.signal_id != "LHP_FADE_RL"),
            key=lambda s: (-int(s.confidence_score or 0), SIGNAL_PRIORITY.get(s.signal_id, 99)),
        )
        if active_sorted:
            pr = min(SIGNAL_PRIORITY.get(s.signal_id, 9) for s in active_sorted)
            top = max(active_sorted, key=lambda s: int(s.confidence_score or 0))
            # Combine reasons in score order (keeps primary driver first).
            reason = " ".join((s.edge_basis or "").strip() for s in active_sorted if (s.edge_basis or "").strip())
            if scored.best_side == "away_ml":
                pick = {
                    "bet": f"{ids.away_team_abbr} ML",
                    "market": "ML",
                    "odds": _fmt_odds(mkt.away_ml_current),
                    "reason": reason or (top.edge_basis or ""),
                    "priority": pr,
                    "confidence_score": int(top.confidence_score or 0),
                    "score_basis": top.score_basis,
                    "signal_id": top.signal_id,
                    "bet_side": "away_ml",
                }
                # LHP_FADE: show RL as an alternative line (not a separate pick card).
                if any(s.signal_id == "LHP_FADE" for s in active_sorted) and lhp_rl is not None:
                    rl_line = mkt.away_rl_line
                    bet = (
                        f"{ids.away_team_abbr} {rl_line:+g}"
                        if rl_line is not None
                        else f"{ids.away_team_abbr} +1.5"
                    )
                    pick["alt"] = {
                        "bet": bet,
                        "odds": lhp_rl.odds,
                        "confidence_score": int(lhp_rl.confidence_score or 0),
                        "note": "Higher hit rate (66%) at lower ROI per unit.",
                        "signal_id": "LHP_FADE_RL",
                        "bet_side": "away_rl",
                    }
                # S1H2: show RL as informational only (no score).
                if any(s.signal_id == "S1H2" for s in active_sorted) and mkt.rl_available and mkt.away_rl_odds is not None:
                    rl_line = mkt.away_rl_line
                    bet = (
                        f"{ids.away_team_abbr} {rl_line:+g}"
                        if rl_line is not None
                        else f"{ids.away_team_abbr} +1.5"
                    )
                    pick["info"] = {
                        "bet": bet,
                        "odds": _fmt_odds(mkt.away_rl_odds),
                        "note": "RL at breakeven — ML is the preferred bet.",
                    }
                picks.append(pick)
            elif scored.best_side == "over_total":
                tline = mkt.total_current
                bet_txt = f"OVER {tline}" if tline is not None else "OVER"
                pick = {
                    "bet": bet_txt,
                    "market": "TOTAL",
                    "odds": _fmt_odds(mkt.over_odds_current),
                    "reason": reason or (top.edge_basis or ""),
                    "priority": pr,
                    "confidence_score": int(top.confidence_score or 0),
                    "score_basis": top.score_basis,
                    "signal_id": top.signal_id,
                    "bet_side": "over_total",
                }
                picks.append(pick)

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

    # Aggregated brief scores: sum per bet_side in scored.aggregated_by_side
    ab = scored.aggregated_by_side
    for pick in picks:
        bside = str(pick.get("bet_side") or "")
        if bside:
            pick["aggregate_score"] = int(ab.get(bside, 0))
        alt = pick.get("alt")
        if isinstance(alt, dict):
            # LHP RL alt: show same narrative total as the paired ML (away_ml bucket)
            alt["aggregate_score"] = int(ab.get("away_ml", 0))

    # Legacy ``avoid``: do not conflate “venue note” with “this card is an AVOID.”
    # If any signal fires, soft environment avoids must not set avoid=True (Word
    # brief showed picks + AVOID banner). Hard avoid = tier Avoid from model policy.
    hard_avoid = bool(scored.avoids) and (
        scored.output_tier == "Avoid" or not scored.signals_fired
    )
    avoid_types = [a.avoid_type for a in (scored.avoids or []) if getattr(a, "avoid_type", None)]
    avoid_bet_types = [a.bet_type for a in (scored.avoids or []) if getattr(a, "bet_type", None)]
    avoid_scope: str | None = None
    if avoid_bet_types:
        # Most avoids are class-based; make the scope explicit in the brief.
        if "all" in avoid_bet_types:
            avoid_scope = "ALL BETS (hard avoid)"
        elif "wind_signals" in avoid_bet_types:
            avoid_scope = "WIND SIGNALS ONLY (totals/wind edges) — other analysis OK"
        else:
            # Fall back to listing bet_type(s) rather than implying everything.
            avoid_scope = " / ".join(sorted(set(str(x) for x in avoid_bet_types)))
    return {
        "signals": display_signals,
        "signal_ids": internal_signal_ids,
        "picks": picks,
        "signal_brief": format_signal_brief_scored(brief_findings),
        "best_aggregate_score": int(scored.best_aggregate_score or 0),
        "avoid": hard_avoid,
        "avoid_scope": avoid_scope,
        "avoid_types": avoid_types,
        "avoid_bet_types": avoid_bet_types,
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
