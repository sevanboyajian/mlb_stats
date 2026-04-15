#!/usr/bin/env python3
"""
Populate team_rolling_stats: pre-game rolling team metrics per (game_pk, team_id).

Source of truth: games + player_game_stats (same starter rule as backtest: max IP per team per game).

Usage:
  python batch/analysis/build_team_rolling_stats.py --season 2025
  python batch/analysis/build_team_rolling_stats.py --season 2025 --dry-run

Requires: core/db/schema.sql team_rolling_stats applied to your DB.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from core.db.connection import connect as db_connect, get_db_path

try:
    from zoneinfo import ZoneInfo

    _ET = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover
    _ET = None

BAT_WINDOW = 15
SP_WINDOW = 5
SP_MIN_IP = 3.0  # treat as "start" for rolling SP stats (schema note)


@dataclass
class GameBatting:
    game_pk: int
    game_date: str
    season: int
    team_id: int
    is_home: int
    runs_scored: int
    runs_allowed: int
    pa: int
    ab: int
    h: int
    hr: int
    dbl: int
    trp: int
    k: int
    bb: int


@dataclass
class StarterLine:
    game_pk: int
    team_id: int
    ip: float
    er: int
    k: int
    ha: int
    bb: int


def _safe_div(a: float, b: float) -> Optional[float]:
    if b == 0:
        return None
    return a / b


def _slg(ab: int, h: int, hr: int, dbl: int, trp: int) -> Optional[float]:
    if ab <= 0:
        return None
    singles = h - dbl - trp - hr
    tb = singles + 2 * dbl + 3 * trp + 4 * hr
    return tb / ab


def _obp(ab: int, h: int, bb: int) -> Optional[float]:
    # Simplified: no HBP/SF in denominator
    den = ab + bb
    if den <= 0:
        return None
    return (h + bb) / den


def _iso(ab: int, h: int, hr: int, dbl: int, trp: int) -> Optional[float]:
    if ab <= 0:
        return None
    ba = h / ab
    slg = _slg(ab, h, hr, dbl, trp)
    if slg is None:
        return None
    return slg - ba


def _batting_rates_from_totals(
    pa: int, ab: int, h: int, hr: int, dbl: int, trp: int, k: int, bb: int
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]]:
    obp = _obp(ab, h, bb)
    slg = _slg(ab, h, hr, dbl, trp)
    ops = (obp + slg) if obp is not None and slg is not None else None
    iso = _iso(ab, h, hr, dbl, trp)
    k_pct = _safe_div(float(k), float(pa)) if pa > 0 else None
    bb_pct = _safe_div(float(bb), float(pa)) if pa > 0 else None
    return obp, slg, ops, iso, k_pct, bb_pct


def load_team_game_batting(con: sqlite3.Connection, season: int) -> Dict[Tuple[int, int], GameBatting]:
    rows = con.execute(
        """
        SELECT
            g.game_pk,
            g.game_date_et AS game_date,
            g.season,
            pgs.team_id,
            CASE WHEN g.home_team_id = pgs.team_id THEN 1 ELSE 0 END AS is_home,
            CASE WHEN g.home_team_id = pgs.team_id THEN g.home_score ELSE g.away_score END AS runs_scored,
            CASE WHEN g.home_team_id = pgs.team_id THEN g.away_score ELSE g.home_score END AS runs_allowed,
            SUM(COALESCE(pgs.plate_appearances, 0)) AS pa,
            SUM(COALESCE(pgs.at_bats, 0)) AS ab,
            SUM(COALESCE(pgs.hits, 0)) AS h,
            SUM(COALESCE(pgs.home_runs, 0)) AS hr,
            SUM(COALESCE(pgs.doubles, 0)) AS dbl,
            SUM(COALESCE(pgs.triples, 0)) AS trp,
            SUM(COALESCE(pgs.strikeouts_bat, 0)) AS k,
            SUM(COALESCE(pgs.walks, 0)) AS bb
        FROM games g
        JOIN player_game_stats pgs ON pgs.game_pk = g.game_pk
        WHERE g.season = ?
          AND g.game_type = 'R'
          AND g.status = 'Final'
          AND pgs.player_role = 'batter'
          AND COALESCE(pgs.plate_appearances, 0) > 0
        GROUP BY g.game_pk, pgs.team_id
        """,
        (season,),
    ).fetchall()
    out: Dict[Tuple[int, int], GameBatting] = {}
    for r in rows:
        key = (int(r["game_pk"]), int(r["team_id"]))
        out[key] = GameBatting(
            game_pk=int(r["game_pk"]),
            game_date=str(r["game_date"]),
            season=int(r["season"]),
            team_id=int(r["team_id"]),
            is_home=int(r["is_home"]),
            runs_scored=int(r["runs_scored"] or 0),
            runs_allowed=int(r["runs_allowed"] or 0),
            pa=int(r["pa"] or 0),
            ab=int(r["ab"] or 0),
            h=int(r["h"] or 0),
            hr=int(r["hr"] or 0),
            dbl=int(r["dbl"] or 0),
            trp=int(r["trp"] or 0),
            k=int(r["k"] or 0),
            bb=int(r["bb"] or 0),
        )
    return out


def load_starter_lines(con: sqlite3.Connection, season: int) -> Dict[Tuple[int, int], StarterLine]:
    """One starter per (game_pk, team_id): max IP among pitcher rows."""
    rows = con.execute(
        """
        SELECT
            g.game_pk,
            pgs.team_id,
            pgs.player_id,
            pgs.innings_pitched,
            pgs.earned_runs,
            pgs.strikeouts_pit,
            pgs.hits_allowed,
            pgs.walks_allowed
        FROM player_game_stats pgs
        JOIN games g ON g.game_pk = pgs.game_pk
        WHERE g.season = ?
          AND g.game_type = 'R'
          AND g.status = 'Final'
          AND pgs.player_role = 'pitcher'
          AND pgs.innings_pitched IS NOT NULL
          AND pgs.team_id IS NOT NULL
        """,
        (season,),
    ).fetchall()
    best: Dict[Tuple[int, int], StarterLine] = {}
    best_ip: Dict[Tuple[int, int], float] = {}
    for r in rows:
        key = (int(r["game_pk"]), int(r["team_id"]))
        ip = float(r["innings_pitched"])
        cur = best_ip.get(key)
        if cur is None or ip > cur:
            best_ip[key] = ip
            best[key] = StarterLine(
                game_pk=int(r["game_pk"]),
                team_id=int(r["team_id"]),
                ip=ip,
                er=int(r["earned_runs"] or 0),
                k=int(r["strikeouts_pit"] or 0),
                ha=int(r["hits_allowed"] or 0),
                bb=int(r["walks_allowed"] or 0),
            )
    return best


def sort_key_team_schedule(g: GameBatting) -> Tuple[str, int]:
    return (g.game_date, g.game_pk)


def rollup_batting(games: List[GameBatting]) -> Tuple[int, int, int, int, int, int, int, int, int]:
    pa = ab = h = hr = dbl = trp = k = bb = 0
    rs = ra = 0
    for g in games:
        pa += g.pa
        ab += g.ab
        h += g.h
        hr += g.hr
        dbl += g.dbl
        trp += g.trp
        k += g.k
        bb += g.bb
        rs += g.runs_scored
        ra += g.runs_allowed
    return pa, ab, h, hr, dbl, trp, k, bb, rs, ra


def rollup_sp(lines: List[StarterLine]) -> Tuple[float, float, float, float, float]:
    ip = er = k = ha = bb = 0.0
    for ln in lines:
        ip += ln.ip
        er += float(ln.er)
        k += float(ln.k)
        ha += float(ln.ha)
        bb += float(ln.bb)
    return ip, er, k, ha, bb


def computed_at_et_iso() -> str:
    if _ET is not None:
        return datetime.now(_ET).isoformat(timespec="seconds")
    return datetime.now().isoformat(timespec="seconds")


def main() -> None:
    p = argparse.ArgumentParser(description="Build team_rolling_stats table from games + player_game_stats.")
    p.add_argument("--season", type=int, required=True)
    p.add_argument("--dry-run", action="store_true", help="Compute only; do not write DB")
    args = p.parse_args()

    con = db_connect(get_db_path(), timeout=60)
    con.row_factory = sqlite3.Row

    bat = load_team_game_batting(con, args.season)
    starters = load_starter_lines(con, args.season)

    # Per team: chronological list of GameBatting for this season
    by_team: Dict[int, List[GameBatting]] = defaultdict(list)
    for g in bat.values():
        by_team[g.team_id].append(g)
    for tid in by_team:
        by_team[tid].sort(key=sort_key_team_schedule)

    insert_sql = """
        INSERT OR REPLACE INTO team_rolling_stats (
            game_pk, team_id, game_date, season,
            games_in_window,
            rolling_runs_scored_pg, rolling_runs_allowed_pg, rolling_run_diff_pg,
            rolling_obp, rolling_slg, rolling_ops, rolling_iso,
            rolling_k_pct, rolling_bb_pct,
            rolling_hr_pg,
            rolling_sp_era, rolling_sp_k9, rolling_sp_whip, sp_starts_in_window,
            rolling_runs_scored_home_pg, rolling_runs_scored_road_pg,
            rolling_ops_home, rolling_ops_road,
            home_games_in_window, road_games_in_window,
            computed_at
        ) VALUES (
            ?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        )
    """

    n = 0
    ca = computed_at_et_iso()
    for team_id, schedule in by_team.items():
        prior: List[GameBatting] = []
        prior_home: List[GameBatting] = []
        prior_road: List[GameBatting] = []
        prior_sp: List[StarterLine] = []

        for g in schedule:
            # Rolling windows = prior games only (strictly before this game)
            w = prior[-BAT_WINDOW:] if len(prior) > BAT_WINDOW else prior
            wh = prior_home[-BAT_WINDOW:] if len(prior_home) > BAT_WINDOW else prior_home
            wr = prior_road[-BAT_WINDOW:] if len(prior_road) > BAT_WINDOW else prior_road
            sp_lines = prior_sp[-SP_WINDOW:] if len(prior_sp) > SP_WINDOW else prior_sp

            gw = len(w)
            games_in_window = gw

            def _f(v: Optional[float]) -> Any:
                return v if gw > 0 else None

            if gw == 0:
                (
                    rolling_runs_scored_pg,
                    rolling_runs_allowed_pg,
                    rolling_run_diff_pg,
                    rolling_obp,
                    rolling_slg,
                    rolling_ops,
                    rolling_iso,
                    rolling_k_pct,
                    rolling_bb_pct,
                    rolling_hr_pg,
                ) = (None,) * 10
            else:
                pa, ab, h, hr, dbl, trp, k, bb, rs, ra = rollup_batting(w)
                rolling_runs_scored_pg = rs / gw
                rolling_runs_allowed_pg = ra / gw
                rolling_run_diff_pg = rolling_runs_scored_pg - rolling_runs_allowed_pg
                obp, slg, ops, iso, kp, bbp = _batting_rates_from_totals(pa, ab, h, hr, dbl, trp, k, bb)
                rolling_obp, rolling_slg, rolling_ops, rolling_iso = obp, slg, ops, iso
                rolling_k_pct, rolling_bb_pct = kp, bbp
                rolling_hr_pg = hr / gw

            # Home / road splits (independent windows)
            hgw = len(wh)
            rgw = len(wr)
            if hgw == 0:
                rsh, ops_h = None, None
            else:
                pa_h, ab_h, h_h, hr_h, dbl_h, trp_h, k_h, bb_h, rs_h, _ra_h = rollup_batting(wh)
                rsh = rs_h / hgw
                _, _, ops_h, _, _, _ = _batting_rates_from_totals(
                    pa_h, ab_h, h_h, hr_h, dbl_h, trp_h, k_h, bb_h
                )

            if rgw == 0:
                rsr, ops_r = None, None
            else:
                pa_r, ab_r, h_r, hr_r, dbl_r, trp_r, k_r, bb_r, rs_r, _ra_r = rollup_batting(wr)
                rsr = rs_r / rgw
                _, _, ops_r, _, _, _ = _batting_rates_from_totals(
                    pa_r, ab_r, h_r, hr_r, dbl_r, trp_r, k_r, bb_r
                )

            # SP rolling (last 5 prior qualifying starts; prior_sp only stores IP >= SP_MIN_IP)
            sp_use = sp_lines
            sp_n = len(sp_use)
            if sp_n == 0:
                sp_era = sp_k9 = sp_whip = None
            else:
                ip_tot, er_tot, k_tot, ha_tot, bb_tot = rollup_sp(sp_use)
                sp_era = _safe_div(9.0 * er_tot, ip_tot)
                sp_k9 = _safe_div(9.0 * k_tot, ip_tot)
                sp_whip = _safe_div(ha_tot + bb_tot, ip_tot)

            row = (
                g.game_pk,
                g.team_id,
                g.game_date,
                g.season,
                games_in_window,
                _f(rolling_runs_scored_pg),
                _f(rolling_runs_allowed_pg),
                _f(rolling_run_diff_pg),
                _f(rolling_obp),
                _f(rolling_slg),
                _f(rolling_ops),
                _f(rolling_iso),
                _f(rolling_k_pct),
                _f(rolling_bb_pct),
                _f(rolling_hr_pg),
                sp_era if sp_n else None,
                sp_k9 if sp_n else None,
                sp_whip if sp_n else None,
                sp_n,
                rsh if hgw else None,
                rsr if rgw else None,
                ops_h if hgw else None,
                ops_r if rgw else None,
                hgw,
                rgw,
                ca,
            )

            if not args.dry_run:
                con.execute(insert_sql, row)
            n += 1

            # Advance priors after snapshot for this game
            prior.append(g)
            if g.is_home:
                prior_home.append(g)
            else:
                prior_road.append(g)
            st = starters.get((g.game_pk, g.team_id))
            if st and st.ip >= SP_MIN_IP:
                prior_sp.append(st)

    if not args.dry_run:
        con.commit()
    con.close()
    print(f"team_rolling_stats: wrote {n} rows for season {args.season}" + (" (dry-run)" if args.dry_run else ""))


if __name__ == "__main__":
    main()
