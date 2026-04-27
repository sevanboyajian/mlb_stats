#!/usr/bin/env python3
"""
backtest_lhp_fade_bands.py
──────────────────────────
Retrospective analysis of LHP_FADE signal across implied probability bands.

Replicates the core LHP_FADE firing condition from _eval_lhp_fade:
  - Away starter confirmed LHP (throws='L', hand_confirmed via game_probable_pitchers)
  - Home team in target implied probability band (from closing/current ML odds)
  - Regular season, Final games
  - Game month Apr–Aug (months 4–8)

Tests four bands and reports win rate + ROI per band.

USAGE:
  python -m batch.analysis.backtesting.backtest_lhp_fade_bands --seasons 2025
  python -m batch.analysis.backtesting.backtest_lhp_fade_bands --seasons 2024 2025
  python -m batch.analysis.backtesting.backtest_lhp_fade_bands --seasons 2025 --min-ip 3.0
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

# ── Band definitions ──────────────────────────────────────────────────────────
# (label, lo_inclusive, hi_inclusive) — all in implied probability (0–1)
# American odds equivalents shown for reference:
#   0.565 ≈ -130   0.600 ≈ -150   0.650 ≈ -186   0.670 ≈ -203   0.750 ≈ -300
BANDS: list[tuple[str, float, float]] = [
    ("A  [-130 to -150]", 0.565, 0.600),
    ("B  [-150 to -203]", 0.600, 0.670),
    ("C  [-203 to -300]", 0.670, 0.750),
    ("D  [-300+]        ", 0.750, 1.000),
    ("A+B[-130 to -203]", 0.565, 0.670),   # proposed expanded band
    ("ALL[-130+]        ", 0.565, 1.000),   # full range baseline
]

# Months Apr–Aug
OK_MONTHS = {4, 5, 6, 7, 8}


def american_to_implied(odds: int) -> float:
    o = int(odds)
    if o > 0:
        return 100.0 / (o + 100.0)
    return (-o) / ((-o) + 100.0)


def implied_to_american(p: float) -> str:
    if p >= 1.0:
        return "N/A"
    if p >= 0.5:
        o = -(p * 100) / (1 - p)
        return f"{round(o)}"
    o = (1 - p) * 100 / p
    return f"+{round(o)}"


@dataclass
class GameCandidate:
    game_pk: int
    game_date_et: str
    season: int
    home_team: str
    away_team: str
    away_sp_name: str
    away_sp_throws: str
    home_ml: int
    home_implied: float
    away_ml: int
    home_score: int
    away_score: int
    away_wins: bool   # LHP_FADE bet outcome


@dataclass
class BandResult:
    label: str
    lo: float
    hi: float
    candidates: list[GameCandidate]

    @property
    def n(self) -> int:
        return len(self.candidates)

    @property
    def wins(self) -> int:
        return sum(1 for c in self.candidates if c.away_wins)

    @property
    def win_rate(self) -> float:
        return self.wins / self.n if self.n else 0.0

    @property
    def roi(self) -> float:
        """Flat 1-unit bet on away ML each game."""
        if not self.candidates:
            return 0.0
        total_profit = 0.0
        for c in self.candidates:
            if c.away_wins:
                o = c.away_ml
                if o > 0:
                    total_profit += o / 100.0
                else:
                    total_profit += 100.0 / (-o)
            else:
                total_profit -= 1.0
        return total_profit / self.n


def load_candidates(
    con: sqlite3.Connection,
    seasons: list[int],
    min_ip: float,
) -> list[GameCandidate]:
    """
    Pull all games meeting LHP_FADE structural conditions.
    Uses best available closing odds, falls back to most recent current odds.
    """
    ph = ",".join("?" * len(seasons))
    sql = f"""
        SELECT
            g.game_pk,
            g.game_date_et,
            g.season,
            t_home.abbreviation     AS home_team,
            t_away.abbreviation     AS away_team,
            p.full_name             AS away_sp_name,
            p.throws                AS away_sp_throws,
            -- Best odds: prefer closing, fall back to most recent current
            COALESCE(
                (SELECT go2.home_ml FROM game_odds go2
                 WHERE go2.game_pk = g.game_pk
                   AND go2.market_type = 'moneyline'
                   AND go2.is_closing_line = 1
                 ORDER BY go2.captured_at_utc DESC LIMIT 1),
                (SELECT go3.home_ml FROM game_odds go3
                 WHERE go3.game_pk = g.game_pk
                   AND go3.market_type = 'moneyline'
                 ORDER BY go3.captured_at_utc DESC LIMIT 1)
            ) AS home_ml,
            COALESCE(
                (SELECT go4.away_ml FROM game_odds go4
                 WHERE go4.game_pk = g.game_pk
                   AND go4.market_type = 'moneyline'
                   AND go4.is_closing_line = 1
                 ORDER BY go4.captured_at_utc DESC LIMIT 1),
                (SELECT go5.away_ml FROM game_odds go5
                 WHERE go5.game_pk = g.game_pk
                   AND go5.market_type = 'moneyline'
                 ORDER BY go5.captured_at_utc DESC LIMIT 1)
            ) AS away_ml,
            g.home_score,
            g.away_score
        FROM games g
        JOIN teams t_home ON t_home.team_id = g.home_team_id
        JOIN teams t_away ON t_away.team_id = g.away_team_id
        -- Away probable starter must be confirmed LHP
        JOIN game_probable_pitchers gpp ON gpp.game_pk = g.game_pk
            AND gpp.team_id = g.away_team_id
        JOIN players p ON p.player_id = gpp.player_id
            AND p.throws = 'L'
        -- Away starter must have a qualifying start on record this game
        JOIN player_game_stats pgs ON pgs.game_pk = g.game_pk
            AND pgs.player_id = gpp.player_id
            AND pgs.player_role = 'pitcher'
            AND pgs.innings_pitched >= ?
        WHERE g.season IN ({ph})
          AND g.game_type = 'R'
          AND g.status = 'Final'
          AND g.home_score IS NOT NULL
          AND g.away_score IS NOT NULL
          -- Month filter: Apr–Aug only
          AND CAST(SUBSTR(g.game_date_et, 6, 2) AS INTEGER) IN (4,5,6,7,8)
        ORDER BY g.game_date_et, g.game_pk
    """
    rows = con.execute(sql, [min_ip, *seasons]).fetchall()

    candidates: list[GameCandidate] = []
    for row in rows:
        (game_pk, game_date, season, home_team, away_team,
         sp_name, sp_throws, home_ml, away_ml,
         home_score, away_score) = row

        if home_ml is None or away_ml is None:
            continue
        if int(home_ml) >= 0:
            # Home team is not a favorite — skip
            continue

        home_impl = american_to_implied(int(home_ml))
        away_wins = int(away_score) > int(home_score)

        candidates.append(GameCandidate(
            game_pk=int(game_pk),
            game_date_et=str(game_date),
            season=int(season),
            home_team=str(home_team),
            away_team=str(away_team),
            away_sp_name=str(sp_name),
            away_sp_throws=str(sp_throws),
            home_ml=int(home_ml),
            home_implied=home_impl,
            away_ml=int(away_ml),
            home_score=int(home_score),
            away_score=int(away_score),
            away_wins=away_wins,
        ))
    return candidates


def run_bands(
    candidates: list[GameCandidate],
) -> list[BandResult]:
    results: list[BandResult] = []
    for label, lo, hi in BANDS:
        band_games = [
            c for c in candidates
            if lo <= c.home_implied <= hi
        ]
        results.append(BandResult(label=label, lo=lo, hi=hi, candidates=band_games))
    return results


def print_report(
    results: list[BandResult],
    seasons: list[int],
    total_candidates: int,
) -> None:
    # Windows consoles may still be cp1252 even if UTF-8 is preferred; avoid hard-failing on box-drawing chars.
    try:
        bar = "═"
        dash = "─"
    except Exception:
        bar = "="
        dash = "-"
    try:
        _ = ("═" * 2).encode(sys.stdout.encoding or "utf-8", errors="strict")
        _ = ("─" * 2).encode(sys.stdout.encoding or "utf-8", errors="strict")
    except Exception:
        bar = "="
        dash = "-"

    print()
    print(bar * 72)
    print(f"  LHP_FADE BAND BACKTEST  —  Seasons: {seasons}")
    print(bar * 72)
    print(f"  Total qualifying games (away LHP, home fav, Apr–Aug): {total_candidates}")
    print()
    print(f"  {'Band':<22} {'N':>5} {'W':>5} {'Win%':>6} {'ROI':>8}  {'Verdict'}")
    print(f"  {dash*22} {dash*5} {dash*5} {dash*6} {dash*8}  {dash*20}")

    ok_check = "✅"
    warn = "⚠"
    bad_x = "❌"
    mid = "🟡"
    try:
        _ = (ok_check + warn + bad_x + mid).encode(sys.stdout.encoding or "utf-8", errors="strict")
    except Exception:
        ok_check = "[OK]"
        warn = "[~]"
        bad_x = "[X]"
        mid = "[?]"

    for r in results:
        if r.n == 0:
            verdict = "NO DATA"
        elif r.win_rate >= 0.56 and r.roi > 0.02:
            verdict = f"{ok_check} STRONG EDGE"
        elif r.win_rate >= 0.53 and r.roi > 0:
            verdict = f"{mid} MARGINAL EDGE"
        elif r.win_rate >= 0.50:
            verdict = f"{warn} BREAKEVEN"
        else:
            verdict = f"{bad_x} NEGATIVE EDGE"

        lo_str = implied_to_american(r.lo)
        hi_str = implied_to_american(r.hi) if r.hi < 1.0 else "∞"
        _ = (lo_str, hi_str)  # display kept in label for now; computed for quick debugging if needed

        print(
            f"  {r.label:<22} {r.n:>5} {r.wins:>5} {r.win_rate:>5.1%} "
            f"{r.roi:>+7.1%}  {verdict}"
        )

    print()

    # Current band highlight
    current = next((r for r in results if "B  " in r.label), None)
    proposed = next((r for r in results if "A+B" in r.label), None)
    if current and proposed:
        print(f"  {dash}{dash} Current vs Proposed {dash * 56}")
        print(f"  Current  heavy band (0.60-0.67): N={current.n:>3}  "
              f"Win%={current.win_rate:.1%}  ROI={current.roi:+.1%}")
        print(f"  Proposed heavy band (0.565-0.67): N={proposed.n:>3}  "
              f"Win%={proposed.win_rate:.1%}  ROI={proposed.roi:+.1%}")
        delta_n = proposed.n - current.n
        print(f"  Adding Band A adds {delta_n} games to the signal pool.")
        print()

    # Decision guidance
    print(f"  {dash}{dash} Decision Guidance {dash * 58}")
    print("  Win% >= 56% + ROI > +2%  -> Expand band, evidence supports it")
    print("  Win% >= 53% + ROI > 0%   -> Expand cautiously, monitor live")
    print("  Win% < 53% or ROI < 0%   -> Keep current floor, band too light")
    print()
    print(bar * 72)
    print()


def print_game_list(candidates: list[GameCandidate], band_label: str) -> None:
    """Print individual games for a band — useful for spot-checking."""
    print(f"\n── Game list: {band_label.strip()} ({len(candidates)} games) ──")
    print(f"  {'date':<12} {'away':<6} {'home':<6} {'away_sp':<22} "
          f"{'home_ml':>8} {'away_ml':>8} {'score':<10} {'result'}")
    print(f"  {'─'*12} {'─'*6} {'─'*6} {'─'*22} {'─'*8} {'─'*8} {'─'*10} {'─'*6}")
    for c in candidates:
        score = f"{c.away_score}-{c.home_score}"
        result = "W" if c.away_wins else "L"
        print(
            f"  {c.game_date_et:<12} {c.away_team:<6} {c.home_team:<6} "
            f"{c.away_sp_name[:22]:<22} {c.home_ml:>8} {c.away_ml:>8} "
            f"{score:<10} {result}"
        )


def main() -> None:
    p = argparse.ArgumentParser(
        description="Backtest LHP_FADE win rate across home favorite implied probability bands."
    )
    p.add_argument("--seasons", nargs="+", type=int, default=[2025])
    p.add_argument("--db", default=None)
    p.add_argument(
        "--min-ip", type=float, default=3.0,
        help="Minimum innings pitched to qualify as a start (default 3.0)"
    )
    p.add_argument(
        "--show-games", action="store_true",
        help="Print individual game list for Band A (the proposed expansion)"
    )
    p.add_argument(
        "--show-all-games", action="store_true",
        help="Print individual game list for every band"
    )
    args = p.parse_args()

    seasons = sorted(set(args.seasons))
    db_path = str(Path(args.db).resolve()) if args.db else str(Path(get_db_path()).resolve())

    print(f"[backtest_lhp_fade_bands] seasons={seasons}  db={db_path}")

    con = db_connect(db_path, timeout=60)
    con.row_factory = sqlite3.Row

    print("[backtest_lhp_fade_bands] Loading candidates…")
    candidates = load_candidates(con, seasons, args.min_ip)
    print(f"[backtest_lhp_fade_bands] {len(candidates)} qualifying games found")

    con.close()

    results = run_bands(candidates)
    print_report(results, seasons, len(candidates))

    if args.show_all_games:
        for r in results:
            if r.candidates:
                print_game_list(r.candidates, r.label)
    elif args.show_games:
        band_a = next((r for r in results if "A  " in r.label), None)
        if band_a and band_a.candidates:
            print_game_list(band_a.candidates, band_a.label)


if __name__ == "__main__":
    main()

