#!/usr/bin/env python3
"""
backfill_standings.py
─────────────────────
Reconstruct daily standings snapshots from completed game results in ``games``.
No external API — idempotent via INSERT OR REPLACE on (snapshot_date, team_id).

USAGE:
  python batch/pipeline/backfill_standings.py --db data/mlb_stats.db --seasons 2024 2025
  python batch/pipeline/backfill_standings.py --seasons 2024 2025 --dry-run
  python batch/pipeline/backfill_standings.py --seasons 2024 --verbose
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import connect as db_connect, get_db_path

DEFAULT_SEASONS = [2024, 2025]
ALL_STAR_TEAM_IDS = {159, 160}


@dataclass
class TeamInfo:
    team_id: int
    league: str
    division: str


@dataclass
class TeamRecord:
    wins: int = 0
    losses: int = 0
    runs_scored: int = 0
    runs_allowed: int = 0
    home_wins: int = 0
    home_losses: int = 0
    away_wins: int = 0
    away_losses: int = 0
    recent_results: list[str] = field(default_factory=list)  # 'W' / 'L', oldest → newest

    def apply_game(self, *, team_runs: int, opp_runs: int, is_home: bool) -> None:
        won = team_runs > opp_runs
        self.runs_scored += team_runs
        self.runs_allowed += opp_runs
        if won:
            self.wins += 1
            if is_home:
                self.home_wins += 1
            else:
                self.away_wins += 1
            self.recent_results.append("W")
        else:
            self.losses += 1
            if is_home:
                self.home_losses += 1
            else:
                self.away_losses += 1
            self.recent_results.append("L")
        if len(self.recent_results) > 10:
            self.recent_results = self.recent_results[-10:]

    def win_pct(self) -> float:
        total = self.wins + self.losses
        return self.wins / total if total else 0.500

    def pythag_win_pct(self) -> float:
        rs, ra = self.runs_scored, self.runs_allowed
        if rs == 0 or ra == 0:
            return 0.500
        return rs * rs / (rs * rs + ra * ra)

    def last_10(self) -> tuple[int, int]:
        last = self.recent_results[-10:]
        return last.count("W"), last.count("L")

    def streak(self) -> tuple[str | None, int | None, str | None]:
        if not self.recent_results:
            return None, None, None
        current = self.recent_results[-1]
        length = 0
        for result in reversed(self.recent_results):
            if result != current:
                break
            length += 1
        streak_str = f"{current}{length}"
        return streak_str, current, length


@dataclass
class GameRow:
    game_date_et: date
    home_team_id: int
    away_team_id: int
    home_score: int
    away_score: int


def parse_game_date(raw: str | None, fallback: str | None) -> date | None:
    value = (raw or "").strip() or (fallback or "").strip()
    if not value:
        return None
    return date.fromisoformat(value[:10])


def load_teams(con: sqlite3.Connection, season: int) -> dict[int, TeamInfo]:
    rows = con.execute(
        """
        SELECT DISTINCT t.team_id, t.league, t.division
        FROM teams t
        WHERE t.team_id IN (
            SELECT home_team_id FROM games WHERE season = ? AND game_type = 'R'
            UNION
            SELECT away_team_id FROM games WHERE season = ? AND game_type = 'R'
        )
          AND t.team_id NOT IN (159, 160)
        ORDER BY t.team_id
        """,
        (season, season),
    ).fetchall()
    return {
        int(team_id): TeamInfo(int(team_id), str(league), str(division))
        for team_id, league, division in rows
    }


def load_games(con: sqlite3.Connection, season: int) -> list[GameRow]:
    rows = con.execute(
        """
        SELECT
            COALESCE(NULLIF(TRIM(g.game_date_et), ''), g.game_date) AS game_date_et,
            g.home_team_id,
            g.away_team_id,
            g.home_score,
            g.away_score
        FROM games g
        WHERE g.season = ?
          AND g.game_type = 'R'
          AND g.status = 'Final'
          AND g.home_score IS NOT NULL
          AND g.away_score IS NOT NULL
        ORDER BY game_date_et, g.game_pk
        """,
        (season,),
    ).fetchall()

    games: list[GameRow] = []
    for game_date_raw, home_id, away_id, home_score, away_score in rows:
        game_date = parse_game_date(str(game_date_raw) if game_date_raw else None, None)
        if game_date is None:
            continue
        if int(home_id) in ALL_STAR_TEAM_IDS or int(away_id) in ALL_STAR_TEAM_IDS:
            continue
        games.append(
            GameRow(
                game_date_et=game_date,
                home_team_id=int(home_id),
                away_team_id=int(away_id),
                home_score=int(home_score),
                away_score=int(away_score),
            )
        )
    return games


def compute_games_back(records: dict[int, TeamRecord], teams: dict[int, TeamInfo]) -> dict[int, float]:
    groups: dict[tuple[str, str], list[int]] = defaultdict(list)
    for team_id, info in teams.items():
        groups[(info.league, info.division)].append(team_id)

    gb: dict[int, float] = {}
    for team_ids in groups.values():
        if not team_ids:
            continue
        leader_id = max(team_ids, key=lambda tid: (records[tid].wins, -records[tid].losses))
        leader = records[leader_id]
        for tid in team_ids:
            team = records[tid]
            if tid == leader_id:
                gb[tid] = 0.0
            else:
                gb[tid] = (leader.wins - team.wins + team.losses - leader.losses) / 2.0
    return gb


def neutral_row(season: int, snapshot_date: str, team_id: int) -> tuple:
    return (
        snapshot_date,
        team_id,
        season,
        0,
        0,
        0.500,
        0.0,
        None,
        None,
        None,
        None,
        0,
        0,
        None,
        None,
        None,
        0,
        0,
        0,
        0.500,
        0,
        0,
        0,
        0,
    )


def record_to_row(
    season: int,
    snapshot_date: str,
    team_id: int,
    record: TeamRecord,
    games_back: float,
) -> tuple:
    l10w, l10l = record.last_10()
    streak, streak_type, streak_length = record.streak()
    rs, ra = record.runs_scored, record.runs_allowed
    return (
        snapshot_date,
        team_id,
        season,
        record.wins,
        record.losses,
        record.win_pct(),
        games_back,
        None,
        None,
        None,
        None,
        l10w,
        l10l,
        streak,
        streak_type,
        streak_length,
        rs,
        ra,
        rs - ra,
        record.pythag_win_pct(),
        record.home_wins,
        record.home_losses,
        record.away_wins,
        record.away_losses,
    )


UPSERT_SQL = """
INSERT OR REPLACE INTO standings
    (snapshot_date, team_id, season,
     wins, losses, win_pct,
     games_back, wild_card_gb,
     division_rank, league_rank, wild_card_rank,
     last_10_wins, last_10_losses,
     streak, streak_type, streak_length,
     runs_scored, runs_allowed, run_diff, pythag_win_pct,
     home_wins, home_losses, away_wins, away_losses)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


def backfill_season(
    con: sqlite3.Connection,
    season: int,
    *,
    dry_run: bool = False,
    verbose: bool = False,
) -> int:
    teams = load_teams(con, season)
    games = load_games(con, season)
    if not teams:
        print(f"[standings] {season}: no teams found — skipping")
        return 0

    unique_dates = sorted({g.game_date_et for g in games})
    if not unique_dates:
        print(f"[standings] {season}: no completed games — skipping")
        return 0

    records = {team_id: TeamRecord() for team_id in teams}
    game_idx = 0
    total_rows = 0

    for game_date in unique_dates:
        snap_date = game_date - timedelta(days=1)
        snap_str = snap_date.isoformat()

        while game_idx < len(games) and games[game_idx].game_date_et <= snap_date:
            g = games[game_idx]
            home = records[g.home_team_id]
            away = records[g.away_team_id]
            home.apply_game(
                team_runs=g.home_score,
                opp_runs=g.away_score,
                is_home=True,
            )
            away.apply_game(
                team_runs=g.away_score,
                opp_runs=g.home_score,
                is_home=False,
            )
            game_idx += 1

        games_back = compute_games_back(records, teams)
        rows: list[tuple] = []
        for team_id in sorted(teams):
            if records[team_id].wins + records[team_id].losses == 0:
                rows.append(neutral_row(season, snap_str, team_id))
            else:
                rows.append(
                    record_to_row(season, snap_str, team_id, records[team_id], games_back[team_id])
                )

        if dry_run:
            if verbose:
                for row in rows[:3]:
                    print(f"  [dry-run sample] {row[0]} team={row[1]} W-L={row[3]}-{row[4]} pct={row[5]:.3f}")
        else:
            con.executemany(UPSERT_SQL, rows)

        total_rows += len(rows)
        print(f"[standings] {game_date.isoformat()}: {len(rows)} teams upserted")

    if not dry_run:
        con.commit()

    return total_rows


def verify_standings(con: sqlite3.Connection, seasons: list[int]) -> None:
    placeholders = ",".join("?" * len(seasons))
    count = con.execute(
        f"""
        SELECT COUNT(*) FROM standings
        WHERE season IN ({placeholders})
          AND snapshot_date BETWEEN '2024-04-01' AND '2025-09-30'
        """,
        seasons,
    ).fetchone()[0]

    stats = con.execute(
        f"""
        SELECT
            COUNT(*) AS n,
            SUM(CASE WHEN win_pct IS NULL THEN 1 ELSE 0 END) AS null_win_pct,
            SUM(CASE WHEN pythag_win_pct IS NULL THEN 1 ELSE 0 END) AS null_pythag,
            MIN(win_pct) AS min_win_pct,
            MAX(win_pct) AS max_win_pct
        FROM standings
        WHERE season IN ({placeholders})
          AND snapshot_date BETWEEN '2024-04-01' AND '2025-09-30'
        """,
        seasons,
    ).fetchone()

    print()
    print("[standings] Verification")
    print(f"  Rows in 2024-04-01 .. 2025-09-30: {count}")
    if stats:
        n, null_wp, null_py, min_wp, max_wp = stats
        print(f"  NULL win_pct: {null_wp or 0}  |  NULL pythag_win_pct: {null_py or 0}")
        print(f"  win_pct range: {min_wp:.3f} .. {max_wp:.3f}" if min_wp is not None else "  win_pct range: n/a")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill daily standings snapshots from completed game results."
    )
    parser.add_argument("--db", default=get_db_path(), help="SQLite database path")
    parser.add_argument(
        "--seasons",
        type=int,
        nargs="+",
        default=DEFAULT_SEASONS,
        help="Seasons to backfill (default: 2024 2025)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and print progress without writing to the database",
    )
    parser.add_argument("--verbose", action="store_true", help="Extra sample output")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    seasons = sorted(set(args.seasons))

    con = db_connect(args.db)
    con.row_factory = sqlite3.Row
    try:
        grand_total = 0
        for season in seasons:
            print(f"[standings] Backfilling season {season}…")
            rows = backfill_season(
                con,
                season,
                dry_run=args.dry_run,
                verbose=args.verbose,
            )
            grand_total += rows
            print(f"[standings] Season {season}: {rows} rows {'would be ' if args.dry_run else ''}upserted")

        print(f"[standings] Total: {grand_total} rows {'would be ' if args.dry_run else ''}upserted")
        if not args.dry_run:
            verify_standings(con, seasons)
    finally:
        con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
