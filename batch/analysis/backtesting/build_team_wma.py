#!/usr/bin/env python3
"""
build_team_wma.py
─────────────────
Compute a 5-game linearly-decayed Weighted Moving Average (WMA) of team
offensive OPS for each game in the target seasons and upsert the result into
team_rolling_stats.rolling_ops_wma.

WEIGHTS  (most-recent first):
  G-1: 5   G-2: 4   G-3: 3   G-4: 2   G-5: 1   divisor = 15

SOURCE:  player_game_stats  (batter rows only, regular season)
TARGET:  team_rolling_stats.rolling_ops_wma  (ALTER + upsert)

DESIGN CONTRACT
───────────────
• The column rolling_ops_wma is added only if it does not already exist.
• Each game row in team_rolling_stats is updated in-place; rows with
  < 2 prior games in the window get NULL (insufficient data, same policy
  as the existing equal-weight rolling_ops which requires games_in_window >= 1).
• Window is exclusive of the current game (pre-game metric, same as the
  existing rolling stats builder).
• Run modes:
    --seasons 2025 2026        (default)
    --seasons 2026             (single season re-run)
    --dry-run                  (print results, do not write)
    --verbose                  (per-game debug lines)

USAGE
─────
  # from repo root:
  python -m batch.pipeline.build_team_wma --seasons 2025 2026
  python -m batch.pipeline.build_team_wma --seasons 2026 --dry-run
  python -m batch.pipeline.build_team_wma --seasons 2025 2026 --verbose
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

# ── repo root on sys.path (mirrors dress_games.py / generate_daily_brief.py) ──
_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import connect as db_connect, get_db_path

# ── WMA constants ──────────────────────────────────────────────────────────────
WMA_WEIGHTS: list[int] = [5, 4, 3, 2, 1]   # index 0 = most-recent prior game
WMA_WINDOW: int = len(WMA_WEIGHTS)           # 5
WMA_DIVISOR: float = float(sum(WMA_WEIGHTS))  # 15.0
WMA_MIN_GAMES: int = 2  # require at least 2 games in window before emitting a value

WMA_COL = "rolling_ops_wma"


# ── OPS computation from raw batting components ────────────────────────────────

def _team_ops_from_components(
    hits: int,
    doubles: int,
    triples: int,
    home_runs: int,
    at_bats: int,
    walks: int,
    hit_by_pitch: int,
    sac_flies: int,
) -> float | None:
    """
    Compute team OPS from batting components for a single game.

    OBP = (H + BB + HBP) / (AB + BB + HBP + SF)
    SLG = (1B + 2×2B + 3×3B + 4×HR) / AB
    OPS = OBP + SLG

    Returns None if denominators are zero (e.g. no at-bats recorded).
    """
    ab = int(at_bats or 0)
    h  = int(hits or 0)
    db = int(doubles or 0)
    tr = int(triples or 0)
    hr = int(home_runs or 0)
    bb = int(walks or 0)
    hbp = int(hit_by_pitch or 0)
    sf  = int(sac_flies or 0)

    obp_denom = ab + bb + hbp + sf
    obp = (h + bb + hbp) / obp_denom if obp_denom > 0 else None

    slg_denom = ab
    singles = h - db - tr - hr
    slg = (singles + 2 * db + 3 * tr + 4 * hr) / slg_denom if slg_denom > 0 else None

    if obp is None or slg is None:
        return None
    return round(obp + slg, 6)


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _ensure_wma_column(con: sqlite3.Connection) -> None:
    """Add rolling_ops_wma to team_rolling_stats if it doesn't exist."""
    cols = {row[1] for row in con.execute("PRAGMA table_info(team_rolling_stats)").fetchall()}
    if WMA_COL not in cols:
        con.execute(f"ALTER TABLE team_rolling_stats ADD COLUMN {WMA_COL} REAL")
        con.commit()
        print(f"[migration] Added column team_rolling_stats.{WMA_COL}")
    else:
        print(f"[migration] Column {WMA_COL} already exists — skipping ALTER")


def _load_game_ops_by_team(
    con: sqlite3.Connection,
    seasons: list[int],
) -> dict[int, list[tuple[str, int, float | None]]]:
    """
    For each team, return a chronologically-sorted list of
    (game_date_et, game_pk, team_ops) for all regular-season batter rows.

    Returns: { team_id: [(game_date_et, game_pk, ops_or_None), ...] }

    We aggregate batting components at the team+game level from player_game_stats.
    player_game_stats has one row per batter per game, so we SUM across the lineup.
    """
    season_placeholders = ",".join("?" * len(seasons))
    sql = f"""
        SELECT
            g.game_date_et,
            pgs.game_pk,
            pgs.team_id,
            COALESCE(SUM(pgs.at_bats), 0)       AS ab,
            COALESCE(SUM(pgs.hits), 0)           AS h,
            COALESCE(SUM(pgs.doubles), 0)        AS db,
            COALESCE(SUM(pgs.triples), 0)        AS tr,
            COALESCE(SUM(pgs.home_runs), 0)      AS hr,
            COALESCE(SUM(pgs.walks), 0)          AS bb,
            COALESCE(SUM(pgs.hit_by_pitch), 0)   AS hbp,
            COALESCE(SUM(pgs.sac_flies), 0)      AS sf
        FROM player_game_stats pgs
        JOIN games g ON g.game_pk = pgs.game_pk
        WHERE g.season IN ({season_placeholders})
          AND g.game_type = 'R'
          AND pgs.player_role = 'batter'
        GROUP BY g.game_date_et, pgs.game_pk, pgs.team_id
        ORDER BY pgs.team_id, g.game_date_et, pgs.game_pk
    """
    rows = con.execute(sql, seasons).fetchall()

    result: dict[int, list[tuple[str, int, float | None]]] = {}
    for row in rows:
        game_date, game_pk, team_id, ab, h, db, tr, hr, bb, hbp, sf = row
        ops = _team_ops_from_components(h, db, tr, hr, ab, bb, hbp, sf)
        result.setdefault(team_id, []).append((game_date, game_pk, ops))

    return result


def _load_trs_game_pks(
    con: sqlite3.Connection,
    seasons: list[int],
) -> dict[tuple[int, int], str]:
    """
    Return { (game_pk, team_id): game_date } for all rows in team_rolling_stats
    for the target seasons. Used to match WMA values back to existing rows.
    """
    season_placeholders = ",".join("?" * len(seasons))
    sql = f"""
        SELECT game_pk, team_id, game_date
        FROM team_rolling_stats
        WHERE season IN ({season_placeholders})
    """
    rows = con.execute(sql, seasons).fetchall()
    return {(int(r[0]), int(r[1])): str(r[2]) for r in rows}


# ── WMA engine ────────────────────────────────────────────────────────────────

@dataclass
class WMAResult:
    game_pk: int
    team_id: int
    game_date: str
    wma_ops: float | None
    games_used: int       # how many prior games contributed to the WMA
    prior_ops: list[float | None]  # ordered most-recent → oldest (for debug)


def _compute_wma_for_team(
    team_id: int,
    game_history: list[tuple[str, int, float | None]],
    trs_lookup: dict[tuple[int, int], str],
) -> list[WMAResult]:
    """
    Walk chronologically through a team's game history.
    For each game that has a row in team_rolling_stats, compute the WMA
    using up to WMA_WINDOW prior completed games (exclusive of current game).

    game_history is sorted by (game_date_et, game_pk) ascending.
    """
    results: list[WMAResult] = []
    # prior_ops is a deque of OPS values for already-played games,
    # ordered most-recent-first (prepend on each step).
    prior_ops_window: list[float | None] = []  # most-recent first, capped at WMA_WINDOW

    for i, (game_date, game_pk, ops) in enumerate(game_history):
        # Does this game have a TRS row to update?
        trs_key = (game_pk, team_id)
        if trs_key in trs_lookup:
            # Compute WMA from whatever prior games we have
            usable = [v for v in prior_ops_window if v is not None]
            n = len(usable)
            if n < WMA_MIN_GAMES:
                wma = None
            else:
                # Apply weights to the usable values (most-recent = highest weight)
                # Truncate weight list to available games, re-normalise divisor.
                weights = WMA_WEIGHTS[:n]
                divisor = float(sum(weights))
                wma = round(
                    sum(w * v for w, v in zip(weights, usable)) / divisor,
                    6,
                )
            results.append(WMAResult(
                game_pk=game_pk,
                team_id=team_id,
                game_date=game_date,
                wma_ops=wma,
                games_used=n,
                prior_ops=list(prior_ops_window),
            ))

        # After recording the WMA for this game, add its OPS to the window
        # for the NEXT game's calculation (exclusive pre-game window).
        prior_ops_window.insert(0, ops)          # prepend → most-recent first
        if len(prior_ops_window) > WMA_WINDOW:
            prior_ops_window.pop()               # drop oldest beyond window

    return results


# ── upsert ────────────────────────────────────────────────────────────────────

def _upsert_wma_results(
    con: sqlite3.Connection,
    results: list[WMAResult],
    dry_run: bool,
    verbose: bool,
) -> tuple[int, int]:
    """
    Write WMA values back to team_rolling_stats.
    Returns (updated_count, skipped_count).
    """
    updated = 0
    skipped = 0
    for r in results:
        if verbose:
            ops_str = f"{r.wma_ops:.4f}" if r.wma_ops is not None else "NULL"
            print(
                f"  team={r.team_id}  pk={r.game_pk}  date={r.game_date}  "
                f"wma_ops={ops_str}  games_used={r.games_used}"
            )
        if dry_run:
            updated += 1
            continue
        con.execute(
            f"""
            UPDATE team_rolling_stats
               SET {WMA_COL} = ?
             WHERE game_pk = ? AND team_id = ?
            """,
            (r.wma_ops, r.game_pk, r.team_id),
        )
        updated += 1

    if not dry_run:
        con.commit()

    return updated, skipped


# ── validation query ──────────────────────────────────────────────────────────

def _print_sample(con: sqlite3.Connection, seasons: list[int]) -> None:
    """Print a 10-row sanity-check sample after writing."""
    season_placeholders = ",".join("?" * len(seasons))
    sql = f"""
        SELECT
            trs.game_date,
            trs.game_pk,
            t.abbreviation AS team,
            trs.rolling_ops,
            trs.{WMA_COL},
            trs.games_in_window
        FROM team_rolling_stats trs
        JOIN teams t ON t.team_id = trs.team_id
        WHERE trs.season IN ({season_placeholders})
          AND trs.{WMA_COL} IS NOT NULL
        ORDER BY trs.game_date DESC, trs.game_pk, t.abbreviation
        LIMIT 10
    """
    rows = con.execute(sql, seasons).fetchall()
    print("\n── Sample output (10 rows, most recent first) ───────────────────────")
    print(f"  {'date':<12} {'pk':<10} {'team':<6} {'eq_ops':<8} {'wma_ops':<9} {'window'}")
    print(f"  {'─'*12} {'─'*10} {'─'*6} {'─'*8} {'─'*9} {'─'*6}")
    for row in rows:
        date, pk, team, eq_ops, wma_ops, window = row
        eq_s  = f"{eq_ops:.4f}" if eq_ops is not None else "  NULL"
        wma_s = f"{wma_ops:.4f}" if wma_ops is not None else "   NULL"
        print(f"  {date:<12} {pk:<10} {team:<6} {eq_s:<8} {wma_s:<9} {window}")
    print()

    # Null coverage report
    sql2 = f"""
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN {WMA_COL} IS NOT NULL THEN 1 ELSE 0 END) AS filled,
            SUM(CASE WHEN {WMA_COL} IS NULL     THEN 1 ELSE 0 END) AS null_rows
        FROM team_rolling_stats
        WHERE season IN ({season_placeholders})
    """
    r = con.execute(sql2, seasons).fetchone()
    if r:
        total, filled, null_rows = r
        pct = 100.0 * filled / total if total else 0.0
        print(f"  Coverage: {filled}/{total} rows filled ({pct:.1f}%)  |  NULL: {null_rows}")
        print()


# ── comparison helper: WMA vs equal-weight ────────────────────────────────────

def _print_divergence_sample(con: sqlite3.Connection, seasons: list[int]) -> None:
    """Show games where WMA and equal-weight OPS diverge most — useful for validation."""
    season_placeholders = ",".join("?" * len(seasons))
    sql = f"""
        SELECT
            trs.game_date,
            t.abbreviation AS team,
            trs.rolling_ops,
            trs.{WMA_COL},
            ABS(trs.rolling_ops - trs.{WMA_COL}) AS divergence
        FROM team_rolling_stats trs
        JOIN teams t ON t.team_id = trs.team_id
        WHERE trs.season IN ({season_placeholders})
          AND trs.rolling_ops IS NOT NULL
          AND trs.{WMA_COL}  IS NOT NULL
        ORDER BY divergence DESC
        LIMIT 10
    """
    rows = con.execute(sql, seasons).fetchall()
    print("── Top divergence: WMA vs equal-weight OPS ──────────────────────────")
    print(f"  {'date':<12} {'team':<6} {'eq_ops':<9} {'wma_ops':<9} {'delta'}")
    print(f"  {'─'*12} {'─'*6} {'─'*9} {'─'*9} {'─'*8}")
    for row in rows:
        date, team, eq, wma, div = row
        print(f"  {date:<12} {team:<6} {eq:.4f}   {wma:.4f}   Δ{div:.4f}")
    print()


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(
        description=(
            "Build 5-game WMA offensive OPS and upsert into team_rolling_stats.\n"
            "Weights: G-1=5, G-2=4, G-3=3, G-4=2, G-5=1  (divisor=15).\n"
            "Requires team_rolling_stats rows to already exist for target games."
        )
    )
    p.add_argument(
        "--seasons",
        nargs="+",
        type=int,
        default=[2025, 2026],
        metavar="YEAR",
        help="Seasons to process (default: 2025 2026)",
    )
    p.add_argument(
        "--db",
        default=None,
        help="Path to mlb_stats.db (default: get_db_path())",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and print results but do NOT write to DB",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Print every game row as it is computed",
    )
    p.add_argument(
        "--skip-sample",
        action="store_true",
        help="Skip the post-run sample/coverage output",
    )
    args = p.parse_args()

    seasons: list[int] = sorted(set(args.seasons))
    db_path = str(Path(args.db).resolve()) if args.db else str(Path(get_db_path()).resolve())

    print(f"[build_team_wma] seasons={seasons}  db={db_path}")
    if args.dry_run:
        print("[build_team_wma] DRY RUN — no writes will occur")

    con = db_connect(db_path, timeout=60)
    con.row_factory = sqlite3.Row

    # ── Step 1: ensure column exists ──────────────────────────────────────────
    _ensure_wma_column(con)

    # ── Step 2: load per-game team OPS from player_game_stats ─────────────────
    print(f"[build_team_wma] Loading batting rows for seasons {seasons}…")
    team_game_ops = _load_game_ops_by_team(con, seasons)
    total_team_games = sum(len(v) for v in team_game_ops.values())
    print(f"[build_team_wma] Loaded {total_team_games} team-game rows across {len(team_game_ops)} teams")

    # ── Step 3: load existing team_rolling_stats keys ─────────────────────────
    print("[build_team_wma] Loading team_rolling_stats keys…")
    trs_lookup = _load_trs_game_pks(con, seasons)
    print(f"[build_team_wma] Found {len(trs_lookup)} rows in team_rolling_stats to update")

    if not trs_lookup:
        print(
            "[build_team_wma] WARNING: team_rolling_stats has no rows for the target seasons.\n"
            "  Run the rolling stats builder first, then re-run this script."
        )
        con.close()
        sys.exit(0)

    # ── Step 4: compute WMA for each team ────────────────────────────────────
    print("[build_team_wma] Computing WMA values…")
    all_results: list[WMAResult] = []
    for team_id, history in sorted(team_game_ops.items()):
        team_results = _compute_wma_for_team(team_id, history, trs_lookup)
        if args.verbose:
            abbr_row = con.execute(
                "SELECT abbreviation FROM teams WHERE team_id = ?", (team_id,)
            ).fetchone()
            abbr = abbr_row[0] if abbr_row else str(team_id)
            print(f"\n── {abbr} (team_id={team_id})  {len(team_results)} TRS rows ──")
        all_results.extend(team_results)

    print(f"[build_team_wma] Computed {len(all_results)} WMA entries")
    non_null = sum(1 for r in all_results if r.wma_ops is not None)
    print(f"[build_team_wma] Non-NULL WMA: {non_null}  |  NULL (< {WMA_MIN_GAMES} games): {len(all_results) - non_null}")

    # ── Step 5: upsert ────────────────────────────────────────────────────────
    print(f"[build_team_wma] {'[DRY RUN] Would write' if args.dry_run else 'Writing'} {len(all_results)} rows…")
    updated, skipped = _upsert_wma_results(con, all_results, args.dry_run, args.verbose)
    action = "Would update" if args.dry_run else "Updated"
    print(f"[build_team_wma] {action}: {updated}  Skipped: {skipped}")

    # ── Step 6: validation output ─────────────────────────────────────────────
    if not args.dry_run and not args.skip_sample:
        _print_sample(con, seasons)
        _print_divergence_sample(con, seasons)

    con.close()
    print("[build_team_wma] Done.")


if __name__ == "__main__":
    main()
