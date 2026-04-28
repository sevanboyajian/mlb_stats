#!/usr/bin/env python3
"""
backtest_offense_tiers.py
─────────────────────────
Tests the hypothesis: does a team's recent 5-game weighted OPS
predict win rate independent of pitching matchup?

Computes WMA OPS on the fly from player_game_stats (does not rely
on the pre-computed team_rolling_stats.rolling_ops_wma column, so
it works for any season where batting data exists — 2024 onward).

WMA weights: G-1=5, G-2=4, G-3=3, G-4=2, G-5=1 (divisor=15)
Qualifier: minimum 2 prior games in window before assigning a tier.

OPS TIER LABELS:
  Elite        >= 0.870
  Strong       >= 0.800
  Above avg    >= 0.750
  Average      >= 0.710
  Below avg    >= 0.660
  Weak         <  0.660

OUTPUT:
  1. Home win rate by home team tier (all games)
  2. Home win rate by tier MATCHUP (home tier vs away tier)
  3. Top 10 most lopsided matchups by win rate
  4. ROI analysis: betting home team when home tier >> away tier

USAGE:
  python -m batch.analysis.backtesting.backtest_offense_tiers --seasons 2024 2025 2026
  python -m batch.analysis.backtesting.backtest_offense_tiers --seasons 2025
  python -m batch.analysis.backtesting.backtest_offense_tiers --seasons 2024 2025 2026 --min-games 3
  python -m batch.analysis.backtesting.backtest_offense_tiers --seasons 2024 2025 2026 --show-games Elite Weak

Place at: batch/analysis/backtesting/backtest_offense_tiers.py
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import connect as db_connect, get_db_path

# ── WMA constants ──────────────────────────────────────────────────────────────
WMA_WEIGHTS = [5, 4, 3, 2, 1]
WMA_DIVISOR = float(sum(WMA_WEIGHTS))  # 15.0
WMA_WINDOW = len(WMA_WEIGHTS)  # 5
WMA_MIN_GAMES = 2  # need at least 2 prior games

MIN_IP_BATTER = 0.0  # batters: no IP filter needed

# ── Tier definitions (descending) ─────────────────────────────────────────────
TIERS: list[tuple[str, float, float]] = [
    ("Elite", 0.870, 99.0),
    ("Strong", 0.800, 0.870),
    ("Above avg", 0.750, 0.800),
    ("Average", 0.710, 0.750),
    ("Below avg", 0.660, 0.710),
    ("Weak", 0.000, 0.660),
]

TIER_ORDER = [t[0] for t in TIERS]


def ops_tier(wma: float | None) -> str:
    if wma is None:
        return "Unknown"
    for label, lo, hi in TIERS:
        if lo <= wma < hi:
            return label
    return "Weak"


def american_to_implied(odds: int) -> float:
    o = int(odds)
    if o > 0:
        return 100.0 / (o + 100.0)
    return (-o) / ((-o) + 100.0)


# ── Data structures ────────────────────────────────────────────────────────────


@dataclass
class TeamGameOPS:
    """Per-game team OPS computed from batting components."""

    game_pk: int
    game_date_et: str
    team_id: int
    obp: float | None
    slg: float | None

    @property
    def ops(self) -> float | None:
        if self.obp is None or self.slg is None:
            return None
        return round(self.obp + self.slg, 6)


@dataclass
class GameResult:
    game_pk: int
    game_date_et: str
    season: int
    home_team_id: int
    away_team_id: int
    home_team: str
    away_team: str
    home_wma: float | None
    away_wma: float | None
    home_tier: str
    away_tier: str
    home_score: int
    away_score: int
    home_ml: int | None
    home_wins: bool
    away_ml: int | None = None


@dataclass
class BucketStats:
    label: str
    n: int = 0
    home_wins: int = 0
    away_wins: int = 0
    roi_n: int = 0
    total_roi: float = 0.0
    away_roi_n: int = 0
    away_roi: float = 0.0

    @property
    def win_rate(self) -> float:
        return self.home_wins / self.n if self.n else 0.0

    @property
    def roi(self) -> float:
        return self.total_roi / self.roi_n if self.roi_n else 0.0


# ── DB queries ────────────────────────────────────────────────────────────────


def load_batting_by_team(
    con: sqlite3.Connection,
    seasons: list[int],
) -> dict[int, list[tuple[str, int, float | None]]]:
    """
    Load per-game team batting components and compute OPS.
    Returns { team_id: [(game_date_et, game_pk, ops), ...] } sorted ascending.
    """
    ph = ",".join("?" * len(seasons))
    sql = f"""
        SELECT
            pgs.team_id,
            g.game_date_et,
            pgs.game_pk,
            COALESCE(SUM(pgs.at_bats),    0) AS ab,
            COALESCE(SUM(pgs.hits),        0) AS h,
            COALESCE(SUM(pgs.doubles),     0) AS db,
            COALESCE(SUM(pgs.triples),     0) AS tr,
            COALESCE(SUM(pgs.home_runs),   0) AS hr,
            COALESCE(SUM(pgs.walks),       0) AS bb,
            COALESCE(SUM(pgs.hit_by_pitch),0) AS hbp,
            COALESCE(SUM(pgs.sac_flies),   0) AS sf
        FROM player_game_stats pgs
        JOIN games g ON g.game_pk = pgs.game_pk
        WHERE g.season IN ({ph})
          AND g.game_type = 'R'
          AND pgs.player_role = 'batter'
        GROUP BY pgs.team_id, g.game_date_et, pgs.game_pk
        ORDER BY pgs.team_id, g.game_date_et, pgs.game_pk
    """
    rows = con.execute(sql, seasons).fetchall()

    result: dict[int, list[tuple[str, int, float | None]]] = {}
    for row in rows:
        team_id, game_date, game_pk, ab, h, db, tr, hr, bb, hbp, sf = row
        ab, h, db, tr, hr, bb, hbp, sf = (
            int(ab),
            int(h),
            int(db),
            int(tr),
            int(hr),
            int(bb),
            int(hbp),
            int(sf),
        )
        # OBP
        obp_d = ab + bb + hbp + sf
        obp = (h + bb + hbp) / obp_d if obp_d > 0 else None
        # SLG
        singles = h - db - tr - hr
        slg = (singles + 2 * db + 3 * tr + 4 * hr) / ab if ab > 0 else None
        ops = round(obp + slg, 6) if (obp is not None and slg is not None) else None

        result.setdefault(int(team_id), []).append((str(game_date), int(game_pk), ops))
    return result


def load_completed_games(
    con: sqlite3.Connection,
    seasons: list[int],
) -> list[dict]:
    """Load all completed regular season games with best closing odds."""
    ph = ",".join("?" * len(seasons))
    sql = f"""
        SELECT
            g.game_pk,
            g.game_date_et,
            g.season,
            g.home_team_id,
            g.away_team_id,
            t_home.abbreviation AS home_team,
            t_away.abbreviation AS away_team,
            g.home_score,
            g.away_score,
            (
                SELECT go.home_ml FROM game_odds go
                WHERE go.game_pk = g.game_pk
                  AND go.market_type = 'moneyline'
                  AND ABS(go.home_ml) <= 600
                ORDER BY go.is_closing_line DESC, go.captured_at_utc DESC
                LIMIT 1
            ) AS home_ml
            ,
            (
                SELECT go.away_ml FROM game_odds go
                WHERE go.game_pk = g.game_pk
                  AND go.market_type = 'moneyline'
                  AND ABS(go.away_ml) <= 600
                ORDER BY go.is_closing_line DESC, go.captured_at_utc DESC
                LIMIT 1
            ) AS away_ml
        FROM games g
        JOIN teams t_home ON t_home.team_id = g.home_team_id
        JOIN teams t_away ON t_away.team_id = g.away_team_id
        WHERE g.season IN ({ph})
          AND g.game_type = 'R'
          AND g.status = 'Final'
          AND g.home_score IS NOT NULL
          AND g.away_score IS NOT NULL
        ORDER BY g.game_date_et, g.game_pk
    """
    rows = con.execute(sql, seasons).fetchall()
    keys = [
        "game_pk",
        "game_date_et",
        "season",
        "home_team_id",
        "away_team_id",
        "home_team",
        "away_team",
        "home_score",
        "away_score",
        "home_ml",
        "away_ml",
    ]
    return [dict(zip(keys, r)) for r in rows]


# ── WMA computation ───────────────────────────────────────────────────────────


def build_team_wma_map(
    batting: dict[int, list[tuple[str, int, float | None]]],
    games: list[dict],
) -> dict[tuple[int, int], float | None]:
    """
    For each (team_id, game_pk) in games, compute the 5-game WMA OPS
    using prior completed games (exclusive of current game).

    Returns { (team_id, game_pk): wma_ops_or_None }
    """
    # Build a set of game_pks we need to compute for per team
    needed: dict[int, set[int]] = defaultdict(set)
    for g in games:
        needed[int(g["home_team_id"])].add(int(g["game_pk"]))
        needed[int(g["away_team_id"])].add(int(g["game_pk"]))

    # Build a lookup: (team_id, game_pk) → (game_date, game_pk) for ordering
    game_pk_to_date: dict[int, str] = {int(g["game_pk"]): str(g["game_date_et"]) for g in games}

    result: dict[tuple[int, int], float | None] = {}

    for team_id, history in batting.items():
        if team_id not in needed:
            continue
        # history is sorted ascending by (game_date, game_pk)
        # Walk chronologically, maintain a rolling window
        prior: list[float | None] = []  # most-recent first

        for game_date, game_pk, ops in history:
            key = (team_id, game_pk)
            if game_pk in needed[team_id]:
                # Compute WMA from prior games
                usable = [v for v in prior if v is not None]
                n = len(usable)
                if n < WMA_MIN_GAMES:
                    result[key] = None
                else:
                    weights = WMA_WEIGHTS[:n]
                    divisor = float(sum(weights))
                    result[key] = round(sum(w * v for w, v in zip(weights, usable)) / divisor, 4)
            # Add this game to prior window (prepend = most recent first)
            prior.insert(0, ops)
            if len(prior) > WMA_WINDOW:
                prior.pop()

    return result


# ── Analysis engine ───────────────────────────────────────────────────────────


def build_game_results(
    games: list[dict],
    wma_map: dict[tuple[int, int], float | None],
) -> list[GameResult]:
    results = []
    for g in games:
        gpk = int(g["game_pk"])
        htid = int(g["home_team_id"])
        atid = int(g["away_team_id"])
        h_wma = wma_map.get((htid, gpk))
        a_wma = wma_map.get((atid, gpk))
        hml = int(g["home_ml"]) if g["home_ml"] is not None else None
        a_ml = int(g["away_ml"]) if g.get("away_ml") is not None else None
        results.append(
            GameResult(
                game_pk=gpk,
                game_date_et=str(g["game_date_et"]),
                season=int(g["season"]),
                home_team_id=htid,
                away_team_id=atid,
                home_team=str(g["home_team"]),
                away_team=str(g["away_team"]),
                home_wma=h_wma,
                away_wma=a_wma,
                home_tier=ops_tier(h_wma),
                away_tier=ops_tier(a_wma),
                home_score=int(g["home_score"]),
                away_score=int(g["away_score"]),
                home_ml=hml,
                away_ml=a_ml,
                home_wins=int(g["home_score"]) > int(g["away_score"]),
            )
        )
    return results


def analyze_by_home_tier(results: list[GameResult]) -> dict[str, BucketStats]:
    buckets: dict[str, BucketStats] = {t: BucketStats(t) for t in TIER_ORDER + ["Unknown"]}
    for r in results:
        if r.home_tier not in buckets:
            continue
        b = buckets[r.home_tier]
        b.n += 1
        if r.home_wins:
            b.home_wins += 1
        else:
            b.away_wins += 1

        # ROI only computed when odds are present (skip missing ML on both wins/losses).
        if r.home_ml is not None:
            b.roi_n += 1
            ml = r.home_ml
            if r.home_wins:
                b.total_roi += (100.0 / (-ml)) if ml < 0 else (ml / 100.0)
            else:
                b.total_roi -= 1.0

        # Away fade ROI only computed when odds are present (skip missing ML on both wins/losses).
        if r.away_ml is not None:
            b.away_roi_n += 1
            aml = r.away_ml
            if not r.home_wins:
                b.away_roi += (aml / 100.0) if aml > 0 else (100.0 / (-aml))
            else:
                b.away_roi -= 1.0
    return buckets


def analyze_by_matchup(results: list[GameResult]) -> dict[tuple[str, str], BucketStats]:
    buckets: dict[tuple[str, str], BucketStats] = {}
    for r in results:
        key = (r.home_tier, r.away_tier)
        if key not in buckets:
            buckets[key] = BucketStats(f"{r.home_tier} vs {r.away_tier}")
        b = buckets[key]
        b.n += 1
        if r.home_wins:
            b.home_wins += 1
        else:
            b.away_wins += 1

        # ROI only computed when odds are present (skip missing ML on both wins/losses).
        if r.home_ml is not None:
            b.roi_n += 1
            ml = r.home_ml
            if r.home_wins:
                b.total_roi += (100.0 / (-ml)) if ml < 0 else (ml / 100.0)
            else:
                b.total_roi -= 1.0

        # Away fade ROI only computed when odds are present (skip missing ML on both wins/losses).
        if r.away_ml is not None:
            b.away_roi_n += 1
            aml = r.away_ml
            if not r.home_wins:
                b.away_roi += (aml / 100.0) if aml > 0 else (100.0 / (-aml))
            else:
                b.away_roi -= 1.0
    return buckets


# ── Report printing ───────────────────────────────────────────────────────────


def print_home_tier_report(
    buckets: dict[str, BucketStats],
    seasons: list[int],
    total: int,
) -> None:
    print()
    print("═" * 70)
    print(f"  OFFENSIVE TIER BACKTEST  —  Seasons: {seasons}")
    print("═" * 70)
    print(f"  Total completed games with WMA data: {total}")
    print()
    print(f"  {'Home Tier':<14} {'N':>6} {'W':>6} {'Win%':>6} {'ROI':>8}  Verdict")
    print(f"  {'─'*14} {'─'*6} {'─'*6} {'─'*6} {'─'*8}  {'─'*20}")
    for tier in TIER_ORDER:
        b = buckets.get(tier)
        if not b or b.n == 0:
            continue
        if b.win_rate >= 0.57:
            verdict = "✅ STRONG EDGE"
        elif b.win_rate >= 0.54:
            verdict = "🟡 MARGINAL EDGE"
        elif b.win_rate >= 0.50:
            verdict = "⚠  SLIGHT EDGE"
        elif b.win_rate >= 0.47:
            verdict = "→  NEUTRAL"
        else:
            verdict = "❌ AVOID"
        print(f"  {tier:<14} {b.n:>6} {b.home_wins:>6} {b.win_rate:>5.1%} {b.roi:>+7.1%}  {verdict}")
    print()


def print_matchup_report(
    matchups: dict[tuple[str, str], BucketStats],
    min_n: int,
    away_fade: bool = False,
) -> None:
    header = (
        f"  {'Home Tier':<14} {'vs Away Tier':<14} {'N':>5} {'W':>5} "
        f"{'HWin%':>6} {'Home ROI':>9} {'Away ROI':>9}  Verdict"
        if away_fade
        else f"  {'Home Tier':<14} {'vs Away Tier':<14} {'N':>5} {'W':>5} {'Win%':>6} {'ROI':>8}  Verdict"
    )
    print(header)
    print(
        f"  {'─'*14} {'─'*14} {'─'*5} {'─'*5} {'─'*6} {'─'*8}"
        + (f" {'─'*9}" if away_fade else "")
        + f"  {'─'*20}"
    )

    # Sort by home_tier order, then away_tier order
    tier_idx = {t: i for i, t in enumerate(TIER_ORDER + ["Unknown"])}
    if away_fade:
        sorted_keys = sorted(
            matchups.keys(),
            key=lambda k: -(
                matchups[k].away_roi / matchups[k].away_roi_n
                if matchups[k].away_roi_n >= min_n
                else -99
            ),
        )
    else:
        sorted_keys = sorted(matchups.keys(), key=lambda k: (tier_idx.get(k[0], 99), tier_idx.get(k[1], 99)))
    for key in sorted_keys:
        b = matchups[key]
        if b.n < min_n:
            continue
        ht, at = key
        away_roi_val = b.away_roi / b.away_roi_n if b.away_roi_n else 0.0
        if away_fade:
            if away_roi_val >= 0.05:
                verdict = "✅ FADE HOME"
            elif away_roi_val >= 0.02:
                verdict = "🟡 LEAN AWAY"
            elif away_roi_val >= 0.00:
                verdict = "⚠  SLIGHT AWAY"
            elif away_roi_val >= -0.05:
                verdict = "→  NEUTRAL"
            else:
                verdict = "❌ BACK HOME"
            print(
                f"  {ht:<14} {at:<14} {b.n:>5} {b.home_wins:>5} "
                f"{b.win_rate:>5.1%} {b.roi:>+8.1%} {away_roi_val:>+8.1%}  {verdict}"
            )
            continue

        if b.win_rate >= 0.60:
            verdict = "✅ STRONG EDGE"
        elif b.win_rate >= 0.55:
            verdict = "🟡 MARGINAL EDGE"
        elif b.win_rate >= 0.50:
            verdict = "⚠  SLIGHT EDGE"
        elif b.win_rate >= 0.45:
            verdict = "→  NEUTRAL"
        else:
            verdict = "❌ AVOID HOME"
        print(f"  {ht:<14} {at:<14} {b.n:>5} {b.home_wins:>5} {b.win_rate:>5.1%} {b.roi:>+7.1%}  {verdict}")
    print()


def print_lopsided_matchups(
    matchups: dict[tuple[str, str], BucketStats],
    min_n: int,
) -> None:
    valid = [(k, b) for k, b in matchups.items() if b.n >= min_n and k[0] != "Unknown" and k[1] != "Unknown"]
    # Sort by win rate descending
    valid.sort(key=lambda x: -x[1].win_rate)
    print(f"  ── Top 10 home win rate matchups (N>={min_n}) ──────────────────")
    print(f"  {'Matchup':<32} {'N':>5} {'Win%':>6} {'ROI':>8}")
    print(f"  {'─'*32} {'─'*5} {'─'*6} {'─'*8}")
    for (ht, at), b in valid[:10]:
        label = f"{ht} home vs {at} away"
        print(f"  {label:<32} {b.n:>5} {b.win_rate:>5.1%} {b.roi:>+7.1%}")
    print()
    print(f"  ── Bottom 10 home win rate matchups (N>={min_n}) ───────────────")
    for (ht, at), b in valid[-10:]:
        label = f"{ht} home vs {at} away"
        print(f"  {label:<32} {b.n:>5} {b.win_rate:>5.1%} {b.roi:>+7.1%}")
    print()


def print_game_list(
    results: list[GameResult],
    home_tier: str,
    away_tier: str,
) -> None:
    filtered = [r for r in results if r.home_tier == home_tier and r.away_tier == away_tier]
    print(f"\n── Game list: {home_tier} home vs {away_tier} away ({len(filtered)} games) ──")
    print(f"  {'date':<12} {'home':<5} {'away':<5} {'h_wma':>7} {'a_wma':>7} {'score':<8} {'ml':>6} R")
    print(f"  {'─'*12} {'─'*5} {'─'*5} {'─'*7} {'─'*7} {'─'*8} {'─'*6} ─")
    for r in sorted(filtered, key=lambda x: x.game_date_et):
        score = f"{r.home_score}-{r.away_score}"
        res = "W" if r.home_wins else "L"
        wma_h = f"{r.home_wma:.3f}" if r.home_wma is not None else "  N/A"
        wma_a = f"{r.away_wma:.3f}" if r.away_wma is not None else "  N/A"
        ml_s = str(r.home_ml) if r.home_ml is not None else "N/A"
        print(
            f"  {r.game_date_et:<12} {r.home_team:<5} {r.away_team:<5} "
            f"{wma_h:>7} {wma_a:>7} {score:<8} {ml_s:>6} {res}"
        )


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    p = argparse.ArgumentParser(
        description=(
            "Backtest offensive WMA tier vs win rate across seasons.\n"
            "Tests whether teams with high recent OPS WMA win more often,\n"
            "independent of pitching matchup."
        )
    )
    p.add_argument("--seasons", nargs="+", type=int, default=[2024, 2025, 2026])
    p.add_argument("--db", default=None)
    p.add_argument("--min-n", type=int, default=20, help="Minimum games in a matchup bucket to display (default 20)")
    p.add_argument(
        "--min-games",
        type=int,
        default=WMA_MIN_GAMES,
        help=f"Minimum prior games in WMA window (default {WMA_MIN_GAMES})",
    )
    p.add_argument(
        "--show-games",
        nargs=2,
        metavar=("HOME_TIER", "AWAY_TIER"),
        help="Print game list for a specific matchup e.g. --show-games Elite Weak",
    )
    p.add_argument(
        "--home-fav-only",
        action="store_true",
        help="Only include games where home team is a favorite (home_ml < 0)",
    )
    p.add_argument(
        "--away-fade",
        action="store_true",
        help="Show away team ROI per matchup bucket (away fade analysis)",
    )
    args = p.parse_args()

    seasons = sorted(set(args.seasons))
    db_path = str(Path(args.db).resolve()) if args.db else str(Path(get_db_path()).resolve())

    print(f"[backtest_offense_tiers] seasons={seasons}  db={db_path}")

    con = db_connect(db_path, timeout=60)
    con.row_factory = sqlite3.Row

    print("[backtest_offense_tiers] Loading completed games…")
    games = load_completed_games(con, seasons)
    print(f"[backtest_offense_tiers] {len(games)} completed games found")

    if args.home_fav_only:
        games = [g for g in games if g["home_ml"] is not None and int(g["home_ml"]) < 0]
        print(f"[backtest_offense_tiers] {len(games)} games after home-fav filter")

    print("[backtest_offense_tiers] Loading batting data…")
    batting = load_batting_by_team(con, seasons)
    print(f"[backtest_offense_tiers] Batting data loaded for {len(batting)} teams")
    con.close()

    print("[backtest_offense_tiers] Computing WMA values…")
    wma_map = build_team_wma_map(batting, games)

    print("[backtest_offense_tiers] Building game results…")
    results = build_game_results(games, wma_map)

    # Filter to games where both teams have WMA data
    with_data = [r for r in results if r.home_wma is not None and r.away_wma is not None]
    no_data = len(results) - len(with_data)
    print(f"[backtest_offense_tiers] {len(with_data)} games with full WMA data ({no_data} excluded — insufficient history)")

    # ── Reports ────────────────────────────────────────────────────────────────
    home_tier_buckets = analyze_by_home_tier(with_data)
    matchup_buckets = analyze_by_matchup(with_data)

    print_home_tier_report(home_tier_buckets, seasons, len(with_data))

    print(f"  ── Win rate by home vs away tier matchup (N>={args.min_n}) ────────────")
    print_matchup_report(matchup_buckets, args.min_n, away_fade=bool(args.away_fade))

    print_lopsided_matchups(matchup_buckets, args.min_n)

    if args.show_games:
        ht, at = args.show_games
        print_game_list(with_data, ht, at)

    print("═" * 70)
    print()


if __name__ == "__main__":
    main()

