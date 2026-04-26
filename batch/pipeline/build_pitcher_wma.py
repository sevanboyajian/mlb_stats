#!/usr/bin/env python3
"""
build_pitcher_wma.py
────────────────────
Compute 5-start linearly-decayed WMA of pitcher performance and upsert
into pitcher_rolling_stats.

METRICS (all computed per qualifying start, then WMA applied):
  era_wma     = (earned_runs * 9.0) / innings_pitched
  k_per_9_wma = (strikeouts_pit * 9.0) / innings_pitched
  whip_wma    = (walks_allowed + hits_allowed) / innings_pitched

QUALIFIER: innings_pitched >= 3.0  (starters only)

WEIGHTS: G-1=5, G-2=4, G-3=3, G-4=2, G-5=1  (divisor=15)
WINDOW:  5 prior qualifying starts (exclusive of current game)
MINIMUM: 2 prior starts required before emitting a non-NULL value

SOURCE:  player_game_stats + game_probable_pitchers + games
TARGET:  pitcher_rolling_stats (upsert on player_id, game_pk)

USAGE:
  python -m batch.pipeline.build_pitcher_wma --seasons 2025 2026
  python -m batch.pipeline.build_pitcher_wma --seasons 2026 --dry-run
  python -m batch.pipeline.build_pitcher_wma --seasons 2026 --verbose
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import connect as db_connect, get_db_path

# ── WMA constants ──────────────────────────────────────────────────────────────
WMA_WEIGHTS: list[int] = [5, 4, 3, 2, 1]
WMA_WINDOW: int = len(WMA_WEIGHTS)        # 5
WMA_DIVISOR: float = float(sum(WMA_WEIGHTS))  # 15.0
WMA_MIN_STARTS: int = 2
MIN_IP: float = 3.0  # qualifying start threshold


# ── per-start metric computation ──────────────────────────────────────────────

def _start_metrics(
    innings_pitched: float,
    earned_runs: int,
    strikeouts_pit: int,
    walks_allowed: int,
    hits_allowed: int,
) -> tuple[float | None, float | None, float | None]:
    """
    Returns (era, k_per_9, whip) for a single start.
    Returns None for any metric where IP is zero or missing.
    """
    ip = float(innings_pitched or 0)
    if ip < MIN_IP:
        return None, None, None
    er = int(earned_runs or 0)
    k = int(strikeouts_pit or 0)
    bb = int(walks_allowed or 0)
    h = int(hits_allowed or 0)
    era = round((er * 9.0) / ip, 4)
    k9 = round((k * 9.0) / ip, 4)
    whip = round((bb + h) / ip, 4)
    return era, k9, whip


# ── WMA engine ────────────────────────────────────────────────────────────────

@dataclass
class PitcherStartRow:
    """One qualifying start for a pitcher."""
    game_date_et: str
    game_pk: int
    era: float | None
    k_per_9: float | None
    whip: float | None


@dataclass
class PitcherWMAResult:
    player_id: int
    game_pk: int
    game_date_et: str
    season: int
    team_id: int
    starts_in_window: int
    era_wma: float | None
    k_per_9_wma: float | None
    whip_wma: float | None


def _wma(values: list[float | None]) -> float | None:
    """
    Apply WMA weights to a list ordered most-recent first.
    Returns None if fewer than WMA_MIN_STARTS usable values.
    """
    usable = [v for v in values if v is not None]
    n = len(usable)
    if n < WMA_MIN_STARTS:
        return None
    weights = WMA_WEIGHTS[:n]
    divisor = float(sum(weights))
    return round(sum(w * v for w, v in zip(weights, usable)) / divisor, 4)


def _compute_wma_for_pitcher(
    _player_id: int,
    scheduled_games: list[tuple[str, int, int, int]],
    start_history: list[PitcherStartRow],
) -> list[PitcherWMAResult]:
    """
    For each scheduled game (game_date_et, game_pk, season, team_id),
    compute WMA from prior qualifying starts exclusively before that game.

    scheduled_games: [(game_date_et, game_pk, season, team_id), ...]
                     sorted ascending by (game_date_et, game_pk)
    start_history:   [PitcherStartRow, ...] sorted ascending
    """
    results: list[PitcherWMAResult] = []

    for game_date, game_pk, season, team_id in scheduled_games:
        # Prior qualifying starts strictly before this game
        prior = [
            s for s in start_history
            if (s.game_date_et, s.game_pk) < (game_date, game_pk)
        ]
        # Most recent first, capped at window
        prior_window = prior[-WMA_WINDOW:][::-1]

        n = len(prior_window)
        results.append(PitcherWMAResult(
            player_id=_player_id,
            game_pk=game_pk,
            game_date_et=game_date,
            season=season,
            team_id=team_id,
            starts_in_window=n,
            era_wma=_wma([s.era for s in prior_window]),
            k_per_9_wma=_wma([s.k_per_9 for s in prior_window]),
            whip_wma=_wma([s.whip for s in prior_window]),
        ))
    return results


# ── DB loaders ────────────────────────────────────────────────────────────────

def _load_scheduled_games(
    con: sqlite3.Connection,
    seasons: list[int],
) -> dict[int, list[tuple[str, int, int, int]]]:
    """
    Return all probable starter assignments for target seasons.
    { player_id: [(game_date_et, game_pk, season, team_id), ...] }
    sorted ascending by (game_date_et, game_pk).
    """
    if not seasons:
        return {}
    ph = ",".join("?" * len(seasons))
    sql = f"""
        SELECT
            gpp.player_id,
            g.game_date_et,
            gpp.game_pk,
            g.season,
            gpp.team_id
        FROM game_probable_pitchers gpp
        JOIN games g ON g.game_pk = gpp.game_pk
        WHERE g.season IN ({ph})
          AND g.game_type = 'R'
        ORDER BY gpp.player_id, g.game_date_et, gpp.game_pk
    """
    rows = con.execute(sql, seasons).fetchall()
    result: dict[int, list[tuple[str, int, int, int]]] = {}
    for player_id, game_date, game_pk, season, team_id in rows:
        if game_date is None:
            continue
        result.setdefault(int(player_id), []).append(
            (str(game_date), int(game_pk), int(season), int(team_id))
        )
    return result


def _load_start_history(
    con: sqlite3.Connection,
    seasons: list[int],
) -> dict[int, list[PitcherStartRow]]:
    """
    Return all qualifying starts (IP >= 3.0) for target seasons.
    { player_id: [PitcherStartRow, ...] } sorted ascending.
    """
    if not seasons:
        return {}
    ph = ",".join("?" * len(seasons))
    sql = f"""
        SELECT
            pgs.player_id,
            g.game_date_et,
            pgs.game_pk,
            pgs.innings_pitched,
            pgs.earned_runs,
            pgs.strikeouts_pit,
            pgs.walks_allowed,
            pgs.hits_allowed
        FROM player_game_stats pgs
        JOIN games g ON g.game_pk = pgs.game_pk
        WHERE g.season IN ({ph})
          AND g.game_type = 'R'
          AND g.status = 'Final'
          AND pgs.player_role = 'pitcher'
          AND pgs.innings_pitched >= ?
        ORDER BY pgs.player_id, g.game_date_et, pgs.game_pk
    """
    rows = con.execute(sql, [*seasons, MIN_IP]).fetchall()
    result: dict[int, list[PitcherStartRow]] = {}
    for (player_id, game_date, game_pk,
         ip, er, k, bb, h) in rows:
        if game_date is None:
            continue
        era, k9, whip = _start_metrics(ip, er, k, bb, h)
        result.setdefault(int(player_id), []).append(PitcherStartRow(
            game_date_et=str(game_date),
            game_pk=int(game_pk),
            era=era,
            k_per_9=k9,
            whip=whip,
        ))
    return result


# ── upsert ────────────────────────────────────────────────────────────────────

def _upsert_results(
    con: sqlite3.Connection,
    results: list[PitcherWMAResult],
    dry_run: bool,
    verbose: bool,
) -> int:
    updated = 0
    for r in results:
        if verbose:
            era_s = f"{r.era_wma:.3f}" if r.era_wma is not None else "NULL"
            k9_s = f"{r.k_per_9_wma:.3f}" if r.k_per_9_wma is not None else "NULL"
            whip_s = f"{r.whip_wma:.3f}" if r.whip_wma is not None else "NULL"
            print(
                f"  pid={r.player_id}  pk={r.game_pk}  date={r.game_date_et}"
                f"  starts={r.starts_in_window}"
                f"  era={era_s}  k9={k9_s}  whip={whip_s}"
            )
        if not dry_run:
            con.execute(
                """
                INSERT INTO pitcher_rolling_stats
                    (player_id, game_pk, game_date_et, season, team_id,
                     starts_in_window, era_wma, k_per_9_wma, whip_wma)
                VALUES (?,?,?,?,?,?,?,?,?)
                ON CONFLICT(player_id, game_pk) DO UPDATE SET
                    starts_in_window = excluded.starts_in_window,
                    era_wma          = excluded.era_wma,
                    k_per_9_wma      = excluded.k_per_9_wma,
                    whip_wma         = excluded.whip_wma,
                    updated_at       = datetime('now')
                """,
                (r.player_id, r.game_pk, r.game_date_et, r.season,
                 r.team_id, r.starts_in_window,
                 r.era_wma, r.k_per_9_wma, r.whip_wma),
            )
        updated += 1
    if not dry_run:
        con.commit()
    return updated


# ── validation output ─────────────────────────────────────────────────────────

def _print_sample(con: sqlite3.Connection, seasons: list[int]) -> None:
    if not seasons:
        return
    ph = ",".join("?" * len(seasons))
    rows = con.execute(
        f"""
        SELECT
            prs.game_date_et,
            prs.game_pk,
            p.full_name,
            prs.starts_in_window,
            prs.era_wma,
            prs.k_per_9_wma,
            prs.whip_wma
        FROM pitcher_rolling_stats prs
        JOIN players p ON p.player_id = prs.player_id
        WHERE prs.season IN ({ph})
          AND prs.era_wma IS NOT NULL
        ORDER BY prs.game_date_et DESC, prs.game_pk
        LIMIT 10
        """,
        seasons,
    ).fetchall()
    print("\n── Sample output (10 rows, most recent first) ───────────────")
    print(f"  {'date':<12} {'pk':<10} {'pitcher':<24} {'starts':<7} {'era':<7} {'k/9':<7} {'whip'}")
    print(f"  {'─'*12} {'─'*10} {'─'*24} {'─'*7} {'─'*7} {'─'*7} {'─'*6}")
    for date, pk, name, starts, era, k9, whip in rows:
        era_s = f"{era:.3f}" if era is not None else "  NULL"
        k9_s = f"{k9:.3f}" if k9 is not None else "  NULL"
        whip_s = f"{whip:.3f}" if whip is not None else "  NULL"
        print(f"  {date:<12} {pk:<10} {name:<24} {starts:<7} {era_s:<7} {k9_s:<7} {whip_s}")

    r = con.execute(
        f"""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN era_wma IS NOT NULL THEN 1 ELSE 0 END) AS filled,
               SUM(CASE WHEN era_wma IS NULL     THEN 1 ELSE 0 END) AS null_rows
        FROM pitcher_rolling_stats
        WHERE season IN ({ph})
        """,
        seasons,
    ).fetchone()
    if r:
        total, filled, nulls = r
        pct = 100.0 * filled / total if total else 0.0
        print(f"\n  Coverage: {filled}/{total} rows filled ({pct:.1f}%)  |  NULL: {nulls}\n")


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(
        description="Build 5-start WMA pitcher stats into pitcher_rolling_stats."
    )
    p.add_argument("--seasons", nargs="+", type=int, default=[2025, 2026])
    p.add_argument("--db", default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--skip-sample", action="store_true")
    args = p.parse_args()

    seasons = sorted(set(args.seasons))
    if not seasons:
        print("[build_pitcher_wma] ERROR: --seasons must include at least one year", file=sys.stderr)
        sys.exit(2)
    db_path = str(Path(args.db).resolve()) if args.db else str(Path(get_db_path()).resolve())

    print(f"[build_pitcher_wma] seasons={seasons}  db={db_path}")
    if args.dry_run:
        print("[build_pitcher_wma] DRY RUN — no writes")

    con = db_connect(db_path, timeout=60)
    con.row_factory = sqlite3.Row

    print("[build_pitcher_wma] Loading scheduled starts from game_probable_pitchers…")
    scheduled = _load_scheduled_games(con, seasons)
    print(f"[build_pitcher_wma] {sum(len(v) for v in scheduled.values())} assignments across {len(scheduled)} pitchers")

    print("[build_pitcher_wma] Loading qualifying start history from player_game_stats…")
    history = _load_start_history(con, seasons)
    print(f"[build_pitcher_wma] {sum(len(v) for v in history.values())} qualifying starts across {len(history)} pitchers")

    print("[build_pitcher_wma] Computing WMA values…")
    all_results: list[PitcherWMAResult] = []
    for player_id, games in sorted(scheduled.items()):
        starts = history.get(player_id, [])
        pr_results = _compute_wma_for_pitcher(player_id, games, starts)
        if args.verbose and pr_results:
            name_row = con.execute(
                "SELECT full_name FROM players WHERE player_id = ?", (player_id,)
            ).fetchone()
            name = name_row["full_name"] if name_row else str(player_id)
            print(f"\n── {name} (pid={player_id})  {len(pr_results)} games ──")
        all_results.extend(pr_results)

    non_null = sum(1 for r in all_results if r.era_wma is not None)
    print(f"[build_pitcher_wma] Computed {len(all_results)} entries")
    print(f"[build_pitcher_wma] Non-NULL ERA WMA: {non_null}  |  NULL: {len(all_results) - non_null}")

    action = "[DRY RUN] Would write" if args.dry_run else "Writing"
    print(f"[build_pitcher_wma] {action} {len(all_results)} rows…")
    updated = _upsert_results(con, all_results, args.dry_run, args.verbose)
    print(f"[build_pitcher_wma] {'Would update' if args.dry_run else 'Updated'}: {updated}")

    if not args.dry_run and not args.skip_sample:
        _print_sample(con, seasons)

    con.close()
    print("[build_pitcher_wma] Done.")


if __name__ == "__main__":
    main()
