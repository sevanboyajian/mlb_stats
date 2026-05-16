#!/usr/bin/env python3
"""
backtest_owm.py
───────────────
Retrospective analysis of the OWM (Offensive WMA vs Pitcher WMA Matchup)
signal across different threshold combinations.

Replicates the core OWM firing condition from _eval_owm:
  - Home team rolling_ops_wma >= ops_threshold
  - Away starter era_wma >= era_threshold
  - Home team is a favorite (home_ml < 0) — configurable
  - Regular season, Final games
  - Game month Apr–Sep (months 4–9)

Tests a grid of OPS and ERA threshold combinations and reports
win rate + ROI per combination so the optimal thresholds can be
selected with evidence.

USAGE:
  python -m batch.analysis.backtesting.backtest_owm --seasons 2025
  python -m batch.analysis.backtesting.backtest_owm --seasons 2024 2025
  python -m batch.analysis.backtesting.backtest_owm --seasons 2025 --show-games
  python -m batch.analysis.backtesting.backtest_owm --seasons 2025 --ops-min 0.780 --era-min 5.00
  python -m batch.analysis.backtesting.backtest_owm --seasons 2021 2022 2023 2024 2025 --segment

Place this file at:
  batch/analysis/backtesting/backtest_owm.py
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import connect as db_connect, get_db_path

# ── Threshold grid to test ────────────────────────────────────────────────────
# Each tuple: (ops_min, era_min)
THRESHOLD_GRID: list[tuple[float, float]] = [
    (0.740, 4.50),
    (0.760, 4.50),
    (0.760, 5.00),
    (0.760, 5.50),
    (0.780, 4.50),
    (0.780, 5.00),
    (0.780, 5.50),
    (0.800, 4.50),
    (0.800, 5.00),
    (0.820, 4.50),
    (0.820, 5.00),
]

# Months Apr–Sep
OK_MONTHS = {4, 5, 6, 7, 8, 9}


def american_to_implied(odds: int) -> float:
    o = int(odds)
    if o > 0:
        return 100.0 / (o + 100.0)
    return (-o) / ((-o) + 100.0)


@dataclass
class GameCandidate:
    game_pk: int
    game_date_et: str
    season: int
    home_team: str
    away_team: str
    away_sp_name: str
    home_ops_wma: float
    away_era_wma: float
    away_whip_wma: float | None
    home_era_wma: float | None
    home_whip_wma: float | None
    away_ops_wma: float | None
    ops_differential: float | None
    home_ml: int
    away_ml: int
    home_score: int
    away_score: int
    home_wins: bool


@dataclass
class ThresholdResult:
    ops_min: float
    era_min: float
    candidates: list[GameCandidate]

    @property
    def n(self) -> int:
        return len(self.candidates)

    @property
    def wins(self) -> int:
        return sum(1 for c in self.candidates if c.home_wins)

    @property
    def win_rate(self) -> float:
        return self.wins / self.n if self.n else 0.0

    @property
    def roi(self) -> float:
        """Flat 1-unit bet on home ML each game."""
        if not self.candidates:
            return 0.0
        total = 0.0
        for c in self.candidates:
            if c.home_wins:
                o = c.home_ml
                if o < 0:
                    total += 100.0 / (-o)
                else:
                    total += o / 100.0
            else:
                total -= 1.0
        return total / self.n

    @property
    def verdict(self) -> str:
        if self.n < 15:
            return "!  SMALL SAMPLE"
        if self.win_rate >= 0.57 and self.roi > 0.02:
            return "+  STRONG EDGE"
        if self.win_rate >= 0.54 and self.roi > 0.0:
            return "~  MARGINAL EDGE"
        if self.win_rate >= 0.50:
            return "!  BREAKEVEN"
        return "-  NEGATIVE EDGE"


SegmentStats = dict[str, int | float]


def _segment_metrics(candidates: list[GameCandidate]) -> SegmentStats:
    """n, wins, win_rate, roi for a candidate subset (same ROI as ThresholdResult)."""
    n = len(candidates)
    if n == 0:
        return {"n": 0, "wins": 0, "win_rate": 0.0, "roi": 0.0}
    wins = sum(1 for c in candidates if c.home_wins)
    total = 0.0
    for c in candidates:
        if c.home_wins:
            o = c.home_ml
            if o < 0:
                total += 100.0 / (-o)
            else:
                total += o / 100.0
        else:
            total -= 1.0
    return {
        "n": n,
        "wins": wins,
        "win_rate": wins / n,
        "roi": total / n,
    }


def _segment_verdict(stats: SegmentStats) -> str:
    n = int(stats["n"])
    win_rate = float(stats["win_rate"])
    roi = float(stats["roi"])
    if n < 15:
        return "!"
    if win_rate >= 0.57 and roi > 0.02:
        return "+"
    if win_rate < 0.50 or roi < 0.0:
        return "-"
    return "!"


def _is_ideal_owm(c: GameCandidate, ops_min: float, era_min: float) -> bool:
    if c.home_ops_wma < ops_min or c.away_era_wma < era_min:
        return False
    if c.home_era_wma is not None and c.home_era_wma >= 5.00:
        return False
    if c.away_ops_wma is not None and c.away_ops_wma >= 0.760:
        return False
    return True


def segment_owm_results(
    candidates: list[GameCandidate],
    ops_min: float,
    era_min: float,
) -> dict[str, SegmentStats]:
    """
    Segment OWM candidates (already filtered by ops_min / era_min) into
    home-SP, away-offense, OPS-diff, and combined ideal filters.
    """
    out: dict[str, SegmentStats] = {}

    def _add(name: str, bucket: list[GameCandidate]) -> None:
        out[name] = _segment_metrics(bucket)

    # Group A — home starter quality
    _add(
        "home_sp_bad    (home ERA WMA >= 5.00)",
        [c for c in candidates if c.home_era_wma is not None and c.home_era_wma >= 5.00],
    )
    _add(
        "home_sp_ok     (home ERA WMA < 5.00)",
        [c for c in candidates if c.home_era_wma is not None and c.home_era_wma < 5.00],
    )
    _add(
        "home_sp_no_wma (insufficient data)",
        [c for c in candidates if c.home_era_wma is None],
    )

    # Group B — away offense strength
    _add(
        "away_off_hot   (away OPS WMA >= 0.760)",
        [c for c in candidates if c.away_ops_wma is not None and c.away_ops_wma >= 0.760],
    )
    _add(
        "away_off_avg   (away OPS WMA 0.680-0.760)",
        [
            c
            for c in candidates
            if c.away_ops_wma is not None and 0.680 <= c.away_ops_wma < 0.760
        ],
    )
    _add(
        "away_off_weak  (away OPS WMA < 0.680)",
        [c for c in candidates if c.away_ops_wma is not None and c.away_ops_wma < 0.680],
    )

    # Group C — OPS differential
    _add(
        "diff_strong    (home OPS - away OPS >= 0.100)",
        [c for c in candidates if c.ops_differential is not None and c.ops_differential >= 0.100],
    )
    _add(
        "diff_moderate  (0.050 to 0.100)",
        [
            c
            for c in candidates
            if c.ops_differential is not None and 0.050 <= c.ops_differential < 0.100
        ],
    )
    _add(
        "diff_narrow    (< 0.050 or negative)",
        [
            c
            for c in candidates
            if c.ops_differential is None or c.ops_differential < 0.050
        ],
    )

    # Group D — combined filter
    ideal = [c for c in candidates if _is_ideal_owm(c, ops_min, era_min)]
    minus_ideal = [c for c in candidates if not _is_ideal_owm(c, ops_min, era_min)]
    _add("ideal_owm", ideal)
    _add("current_owm_minus_ideal", minus_ideal)

    return out


def load_all_candidates(
    con: sqlite3.Connection,
    seasons: list[int],
    home_fav_only: bool,
) -> list[GameCandidate]:
    """
    Pull all games where:
    - Home team has rolling_ops_wma in team_rolling_stats
    - Away probable starter has era_wma in pitcher_rolling_stats
    - Regular season, Final, Apr–Sep
    Uses best available closing ML odds; falls back to most recent current.
    """
    ph = ",".join("?" * len(seasons))
    fav_clause = "AND home_ml_best < 0" if home_fav_only else ""

    sql = f"""
        WITH best_odds AS (
            SELECT
                go.game_pk,
                -- Prefer closing line; fall back to most-recent current
                COALESCE(
                    MIN(CASE WHEN go.is_closing_line = 1 THEN go.home_ml END),
                    (SELECT go2.home_ml FROM game_odds go2
                     WHERE go2.game_pk = go.game_pk
                       AND go2.market_type = 'moneyline'
                     ORDER BY go2.captured_at_utc DESC LIMIT 1)
                ) AS home_ml_best,
                COALESCE(
                    MIN(CASE WHEN go.is_closing_line = 1 THEN go.away_ml END),
                    (SELECT go3.away_ml FROM game_odds go3
                     WHERE go3.game_pk = go.game_pk
                       AND go3.market_type = 'moneyline'
                     ORDER BY go3.captured_at_utc DESC LIMIT 1)
                ) AS away_ml_best
            FROM game_odds go
            WHERE go.market_type = 'moneyline'
            GROUP BY go.game_pk
        )
        SELECT
            g.game_pk,
            g.game_date_et,
            g.season,
            t_home.abbreviation     AS home_team,
            t_away.abbreviation     AS away_team,
            p.full_name             AS away_sp_name,
            trs.rolling_ops_wma     AS home_ops_wma,
            prs.era_wma             AS away_era_wma,
            prs.whip_wma            AS away_whip_wma,
            prs_home.era_wma        AS home_era_wma,
            prs_home.whip_wma       AS home_whip_wma,
            trs_away.rolling_ops_wma AS away_ops_wma,
            bo.home_ml_best         AS home_ml,
            bo.away_ml_best         AS away_ml,
            g.home_score,
            g.away_score
        FROM games g
        JOIN teams t_home ON t_home.team_id = g.home_team_id
        JOIN teams t_away ON t_away.team_id = g.away_team_id
        -- Home team WMA
        JOIN team_rolling_stats trs ON trs.game_pk = g.game_pk
            AND trs.team_id = g.home_team_id
            AND trs.rolling_ops_wma IS NOT NULL
        -- Away probable starter with WMA
        JOIN game_probable_pitchers gpp ON gpp.game_pk = g.game_pk
            AND gpp.team_id = g.away_team_id
        JOIN players p ON p.player_id = gpp.player_id
        JOIN pitcher_rolling_stats prs ON prs.game_pk = g.game_pk
            AND prs.player_id = gpp.player_id
            AND prs.era_wma IS NOT NULL
        -- Home probable starter WMA (optional)
        LEFT JOIN game_probable_pitchers gpp_home ON gpp_home.game_pk = g.game_pk
            AND gpp_home.team_id = g.home_team_id
        LEFT JOIN pitcher_rolling_stats prs_home ON prs_home.game_pk = g.game_pk
            AND prs_home.player_id = gpp_home.player_id
        -- Away team OPS WMA (optional)
        LEFT JOIN team_rolling_stats trs_away ON trs_away.game_pk = g.game_pk
            AND trs_away.team_id = g.away_team_id
        -- Odds
        JOIN best_odds bo ON bo.game_pk = g.game_pk
            AND bo.home_ml_best IS NOT NULL
            AND bo.away_ml_best IS NOT NULL
        WHERE g.season IN ({ph})
          AND g.game_type = 'R'
          AND g.status = 'Final'
          AND g.home_score IS NOT NULL
          AND g.away_score IS NOT NULL
          AND CAST(SUBSTR(g.game_date_et, 6, 2) AS INTEGER) IN (4,5,6,7,8,9)
          {fav_clause}
          -- Exclude obviously corrupt odds lines
          AND ABS(bo.home_ml_best) <= 600
          AND ABS(bo.away_ml_best) <= 600
        ORDER BY g.game_date_et, g.game_pk
    """
    rows = con.execute(sql, seasons).fetchall()

    candidates: list[GameCandidate] = []
    for row in rows:
        (game_pk, game_date, season, home_team, away_team, sp_name,
         home_ops_wma, away_era_wma, away_whip_wma,
         home_era_wma, home_whip_wma, away_ops_wma,
         home_ml, away_ml, home_score, away_score) = row

        h_ops = float(home_ops_wma)
        a_ops = float(away_ops_wma) if away_ops_wma is not None else None
        ops_diff = (h_ops - a_ops) if a_ops is not None else None

        candidates.append(GameCandidate(
            game_pk=int(game_pk),
            game_date_et=str(game_date),
            season=int(season),
            home_team=str(home_team),
            away_team=str(away_team),
            away_sp_name=str(sp_name),
            home_ops_wma=h_ops,
            away_era_wma=float(away_era_wma),
            away_whip_wma=float(away_whip_wma) if away_whip_wma is not None else None,
            home_era_wma=float(home_era_wma) if home_era_wma is not None else None,
            home_whip_wma=float(home_whip_wma) if home_whip_wma is not None else None,
            away_ops_wma=a_ops,
            ops_differential=ops_diff,
            home_ml=int(home_ml),
            away_ml=int(away_ml),
            home_score=int(home_score),
            away_score=int(away_score),
            home_wins=int(home_score) > int(away_score),
        ))
    return candidates


def run_grid(
    all_candidates: list[GameCandidate],
    grid: list[tuple[float, float]],
) -> list[ThresholdResult]:
    results: list[ThresholdResult] = []
    for ops_min, era_min in grid:
        filtered = [
            c for c in all_candidates
            if c.home_ops_wma >= ops_min and c.away_era_wma >= era_min
        ]
        results.append(ThresholdResult(
            ops_min=ops_min,
            era_min=era_min,
            candidates=filtered,
        ))
    return results


def print_grid_report(
    results: list[ThresholdResult],
    seasons: list[int],
    total_pool: int,
    home_fav_only: bool,
) -> None:
    fav_note = "home favorites only (home_ml < 0)" if home_fav_only else "all home implied >= 0.40"
    print()
    print("=" * 76)
    print(f"  OWM BACKTEST  -  Seasons: {seasons}  |  {fav_note}")
    print("=" * 76)
    print(f"  Total pool (WMA data available, Apr-Sep, Final): {total_pool}")
    print()
    print(f"  {'OPS_min':>8} {'ERA_min':>8} {'N':>5} {'W':>5} {'Win%':>6} {'ROI':>8}  Verdict")
    print(f"  {'-'*8} {'-'*8} {'-'*5} {'-'*5} {'-'*6} {'-'*8}  {'-'*22}")
    for r in results:
        print(
            f"  {r.ops_min:>8.3f} {r.era_min:>8.2f} {r.n:>5} {r.wins:>5} "
            f"{r.win_rate:>5.1%} {r.roi:>+7.1%}  {r.verdict}"
        )
    print()

    # Best performing threshold
    valid = [r for r in results if r.n >= 15]
    if valid:
        best = max(valid, key=lambda r: (r.win_rate, r.roi))
        print(f"  -- Best threshold (N>=15) -------------------------------------")
        print(f"  OPS>={best.ops_min:.3f}  ERA>={best.era_min:.2f}  "
              f"N={best.n}  Win%={best.win_rate:.1%}  ROI={best.roi:+.1%}")
        print()

    print("  -- Decision Guidance ------------------------------------------")
    print("  Win% >= 57% + ROI > +2%  -> Strong signal, use as designed")
    print("  Win% >= 54% + ROI > 0%   -> Marginal edge, monitor 1 month live")
    print("  Win% < 54% or ROI < 0%   -> Raise thresholds or hold signal")
    print()
    print("=" * 76)
    print()


def _pick_best_threshold(results: list[ThresholdResult]) -> ThresholdResult | None:
    valid = [r for r in results if r.n >= 15]
    if not valid:
        return None
    return max(valid, key=lambda r: (r.win_rate, r.roi))


def print_segment_report(
    segments: dict[str, SegmentStats],
    *,
    ops_min: float,
    era_min: float,
    full_signal: SegmentStats,
) -> None:
    """Print segmentation tables for the best OPS/ERA threshold combination."""
    print()
    print("=" * 76)
    print(
        f"  OWM SEGMENTATION  -  Best threshold OPS>={ops_min:.3f}  ERA>={era_min:.2f}"
    )
    print("=" * 76)

    group_sections: list[tuple[str, list[str]]] = [
        ("GROUP A - Home starter quality", [
            "home_sp_bad    (home ERA WMA >= 5.00)",
            "home_sp_ok     (home ERA WMA < 5.00)",
            "home_sp_no_wma (insufficient data)",
        ]),
        ("GROUP B - Away offense strength", [
            "away_off_hot   (away OPS WMA >= 0.760)",
            "away_off_avg   (away OPS WMA 0.680-0.760)",
            "away_off_weak  (away OPS WMA < 0.680)",
        ]),
        ("GROUP C - OPS differential (home - away)", [
            "diff_strong    (home OPS - away OPS >= 0.100)",
            "diff_moderate  (0.050 to 0.100)",
            "diff_narrow    (< 0.050 or negative)",
        ]),
    ]

    hdr = f"  {'Group':<42} {'N':>5} {'W':>4} {'Win%':>7} {'ROI':>8}  V"
    rule = "  " + "-" * 72
    print(hdr)
    print(rule)

    for _title, keys in group_sections:
        print()
        print(f"  {_title}")
        print(rule)
        for key in keys:
            s = segments[key]
            v = _segment_verdict(s)
            print(
                f"  {key:<42} {int(s['n']):>5} {int(s['wins']):>4} "
                f"{float(s['win_rate']):>6.1%} {float(s['roi']):>+7.1%}  {v}"
            )

    no_wma = segments["home_sp_no_wma (insufficient data)"]
    full_n = int(full_signal["n"])
    if full_n > 0:
        pct_no_wma = 100.0 * int(no_wma["n"]) / full_n
        if pct_no_wma > 70.0:
            print()
            print(
                f"  NOTE: home_sp_no_wma is {pct_no_wma:.0f}% of signal games "
                f"({no_wma['n']}/{full_n}) — home starter WMA coverage may be "
                "too sparse to trust Group A filters."
            )

    print()
    print("  COMBINED FILTER RESULT (Group D)")
    print(rule)
    ideal = segments["ideal_owm"]
    minus = segments["current_owm_minus_ideal"]
    print(
        f"  ideal_owm (home SP ok + away offense not hot):"
        f"  N={int(ideal['n'])}  Win%={float(ideal['win_rate']):.1%}  "
        f"ROI={float(ideal['roi']):+.1%}"
    )
    print(
        f"  current_owm_minus_ideal (filtered-out cases):"
        f"  N={int(minus['n'])}  Win%={float(minus['win_rate']):.1%}  "
        f"ROI={float(minus['roi']):+.1%}"
    )
    if int(full_signal["n"]) > 0 and int(ideal["n"]) > 0:
        d_win = (float(ideal["win_rate"]) - float(full_signal["win_rate"])) * 100.0
        d_roi = (float(ideal["roi"]) - float(full_signal["roi"])) * 100.0
        print(
            f"  Delta: ideal vs current full signal:"
            f"  Win% {d_win:+.1f}pp  ROI {d_roi:+.1f}pp"
        )
    print()
    print("=" * 76)
    print()


def print_game_list(candidates: list[GameCandidate], ops_min: float, era_min: float) -> None:
    print(f"\n-- Game list: OPS>={ops_min:.3f}  ERA>={era_min:.2f}  ({len(candidates)} games) --")
    print(f"  {'date':<12} {'home':<5} {'away':<5} {'away_sp':<22} "
          f"{'ops_wma':>8} {'era_wma':>8} {'score':<8} {'ml':>6} {'R'}")
    print(f"  {'-'*12} {'-'*5} {'-'*5} {'-'*22} {'-'*8} {'-'*8} {'-'*8} {'-'*6} {'-'}")
    for c in candidates:
        score = f"{c.home_score}-{c.away_score}"
        result = "W" if c.home_wins else "L"
        print(
            f"  {c.game_date_et:<12} {c.home_team:<5} {c.away_team:<5} "
            f"{c.away_sp_name[:22]:<22} {c.home_ops_wma:>8.3f} {c.away_era_wma:>8.2f} "
            f"{score:<8} {c.home_ml:>6} {result}"
        )


def main() -> None:
    p = argparse.ArgumentParser(
        description="Backtest OWM signal across OPS WMA and ERA WMA threshold combinations."
    )
    p.add_argument("--seasons", nargs="+", type=int, default=[2025])
    p.add_argument("--db", default=None)
    p.add_argument(
        "--home-fav-only", action="store_true", default=True,
        help="Only include games where home team is a favorite (default: True)"
    )
    p.add_argument(
        "--include-dogs", action="store_true",
        help="Include home underdog games (overrides --home-fav-only)"
    )
    p.add_argument(
        "--ops-min", type=float, default=None,
        help="Run single threshold: minimum home OPS WMA"
    )
    p.add_argument(
        "--era-min", type=float, default=None,
        help="Run single threshold: minimum away ERA WMA"
    )
    p.add_argument(
        "--show-games", action="store_true",
        help="Print game-level detail for the best threshold combination"
    )
    p.add_argument(
        "--segment",
        action="store_true",
        help="Print segmentation report for the best threshold (N>=15)",
    )
    args = p.parse_args()

    seasons = sorted(set(args.seasons))
    home_fav_only = args.home_fav_only and not args.include_dogs
    db_path = (str(Path(args.db).resolve()) if args.db
               else str(Path(get_db_path()).resolve()))

    print(f"[backtest_owm] seasons={seasons}  db={db_path}")
    print(f"[backtest_owm] home_fav_only={home_fav_only}")

    con = db_connect(db_path, timeout=60)
    con.row_factory = sqlite3.Row

    print("[backtest_owm] Loading candidates…")
    all_candidates = load_all_candidates(con, seasons, home_fav_only)
    con.close()
    print(f"[backtest_owm] {len(all_candidates)} games in pool")

    # Single threshold mode
    if args.ops_min is not None and args.era_min is not None:
        grid = [(args.ops_min, args.era_min)]
    else:
        grid = THRESHOLD_GRID

    results = run_grid(all_candidates, grid)
    print_grid_report(results, seasons, len(all_candidates), home_fav_only)

    best = _pick_best_threshold(results)

    if args.segment:
        if best is None:
            print("[backtest_owm] --segment skipped: no threshold with N>=15")
        else:
            segments = segment_owm_results(
                best.candidates, best.ops_min, best.era_min
            )
            print_segment_report(
                segments,
                ops_min=best.ops_min,
                era_min=best.era_min,
                full_signal=_segment_metrics(best.candidates),
            )

    if args.show_games and best is not None:
        print_game_list(best.candidates, best.ops_min, best.era_min)


if __name__ == "__main__":
    main()
