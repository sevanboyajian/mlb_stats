#!/usr/bin/env python3
"""
backtest_team_vs_pitcher.py
────────────────────────────────────────────────────────────────────────────
Dataset builder: team rolling offense vs opposing starter rolling performance.

Outputs one row per team per game (2 rows per game):
  - home offense vs away starter
  - away offense vs home starter

Inputs (best effort):
  - games (Final regular-season only)
  - game_probable_pitchers (starter ids)
  - player_game_stats (pitcher appearances; not guaranteed to be starts)

Outputs:
  - CSV (default)
  - Optional new table (append-only)

Period selection:
  - --month YYYY-MM
  - --season YYYY (uses seasons table bounds when available)
  - or explicit --start/--end (ET dates)
"""

from __future__ import annotations

import argparse
import csv
import datetime
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, "..", "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from core.db.connection import connect as db_connect, get_db_path


def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)


def _mean(xs: List[float]) -> Optional[float]:
    xs2 = [x for x in xs if x is not None]
    if not xs2:
        return None
    return sum(xs2) / len(xs2)


def _safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b in (None, 0):
        return None
    return a / b


def _parse_month(s: str) -> Tuple[str, str]:
    """YYYY-MM -> (start_date, end_date) as YYYY-MM-DD (inclusive end)."""
    s = s.strip()
    dt = datetime.datetime.strptime(s, "%Y-%m")
    year, month = dt.year, dt.month
    start = datetime.date(year, month, 1)
    # last day of month
    if month == 12:
        next_m = datetime.date(year + 1, 1, 1)
    else:
        next_m = datetime.date(year, month + 1, 1)
    end = next_m - datetime.timedelta(days=1)
    return start.isoformat(), end.isoformat()


def _season_bounds(con: sqlite3.Connection, season: int) -> Tuple[str, str]:
    """
    Return (start_date, end_date) using seasons table when available.
    Uses postseason_start as season end when present (exclusive-ish); otherwise Oct 1.
    """
    try:
        row = con.execute(
            "SELECT season_start, postseason_start FROM seasons WHERE season=?",
            (season,),
        ).fetchone()
        if row and row[0]:
            start = str(row[0])
            end = str(row[1] or f"{season}-10-01")
            return start, end
    except Exception:
        pass
    return f"{season}-04-01", f"{season}-10-01"


def load_team_lookup(con: sqlite3.Connection) -> Dict[int, Dict[str, str]]:
    rows = con.execute(
        "SELECT team_id, abbreviation, name FROM teams"
    ).fetchall()
    out: Dict[int, Dict[str, str]] = {}
    for r in rows:
        out[int(r[0])] = {"abbr": r[1], "name": r[2]}
    return out


def load_player_lookup(con: sqlite3.Connection) -> Dict[int, str]:
    rows = con.execute("SELECT player_id, full_name, throws FROM players").fetchall()
    out: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        out[int(r[0])] = {"name": str(r[1]), "throws": r[2]}
    return out


def load_closing_moneylines(con: sqlite3.Connection, start: str, end: str) -> Dict[int, Dict[str, Any]]:
    """
    {game_pk: {home_ml, away_ml, bookmaker, captured_at_utc}}
    Best effort: prefers bookmaker='consensus' when present; otherwise newest captured_at_utc.
    Does NOT rely on an exact time window.
    """
    rows = con.execute(
        """
        SELECT
            go.game_pk,
            go.bookmaker,
            go.captured_at_utc,
            go.home_ml,
            go.away_ml
        FROM game_odds go
        JOIN games g ON g.game_pk = go.game_pk
        WHERE g.game_date_et BETWEEN ? AND ?
          AND go.market_type = 'moneyline'
          AND go.is_closing_line = 1
          AND (go.home_ml IS NOT NULL OR go.away_ml IS NOT NULL)
        """,
        (start, end),
    ).fetchall()

    best: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        gpk = int(r[0])
        cand = {
            "bookmaker": r[1],
            "captured_at_utc": r[2],
            "home_ml": r[3],
            "away_ml": r[4],
        }
        cur = best.get(gpk)
        if cur is None:
            best[gpk] = cand
            continue
        # Prefer consensus
        if (cur.get("bookmaker") != "consensus") and (cand.get("bookmaker") == "consensus"):
            best[gpk] = cand
            continue
        if (cur.get("bookmaker") == "consensus") and (cand.get("bookmaker") != "consensus"):
            continue
        # Else newest captured_at_utc
        if (cand.get("captured_at_utc") or "") > (cur.get("captured_at_utc") or ""):
            best[gpk] = cand
    return best


def implied_prob_from_ml(ml: Optional[int]) -> Optional[float]:
    if ml is None:
        return None
    ml_i = int(ml)
    if ml_i == 0:
        return None
    if ml_i > 0:
        return 100.0 / (ml_i + 100.0)
    return (-ml_i) / ((-ml_i) + 100.0)


def load_games(con: sqlite3.Connection, start: str, end: str, season: Optional[int]) -> List[Dict[str, Any]]:
    season_clause = "AND g.season = ?" if season is not None else ""
    params: List[Any] = [start, end]
    if season is not None:
        params.append(season)
    rows = con.execute(
        f"""
        SELECT
            g.game_pk,
            g.game_date_et AS game_date,
            g.game_start_utc,
            g.home_team_id,
            g.away_team_id,
            g.home_score,
            g.away_score
        FROM games g
        WHERE g.game_type = 'R'
          AND g.status = 'Final'
          AND g.game_date_et BETWEEN ? AND ?
          {season_clause}
        ORDER BY g.game_date_et, g.game_start_utc, g.game_pk
        """,
        tuple(params),
    ).fetchall()
    return [dict(r) for r in rows]


def build_team_runs_history(con: sqlite3.Connection, season: Optional[int]) -> Dict[int, List[Tuple[str, int]]]:
    """{team_id: [(game_date_et, runs_scored), ...]} ordered by date."""
    season_clause = "AND season = ?" if season is not None else ""
    params: Tuple[Any, ...] = (season, season) if season is not None else ()
    rows = con.execute(
        f"""
        SELECT
            game_date_et AS game_date,
            home_team_id AS team_id,
            home_score   AS runs
        FROM games
        WHERE game_type='R' AND status='Final' AND home_score IS NOT NULL
          {season_clause}

        UNION ALL

        SELECT
            game_date_et AS game_date,
            away_team_id AS team_id,
            away_score   AS runs
        FROM games
        WHERE game_type='R' AND status='Final' AND away_score IS NOT NULL
          {season_clause}
        ORDER BY game_date, team_id
        """,
        params,
    ).fetchall()
    hist: Dict[int, List[Tuple[str, int]]] = {}
    for r in rows:
        tid = int(r["team_id"])
        hist.setdefault(tid, []).append((str(r["game_date"]), int(r["runs"])))
    return hist


def rolling_team_offense(team_hist: Dict[int, List[Tuple[str, int]]],
                         team_id: int,
                         game_date: str,
                         n: int) -> Dict[str, Any]:
    """Rolling offense from last n games strictly before game_date."""
    rows = team_hist.get(team_id, [])
    prior = [runs for (d, runs) in rows if d < game_date]
    window = prior[-n:] if n > 0 else prior
    avg = _mean([float(x) for x in window])
    if window and len(window) >= 2 and avg is not None:
        var = sum((x - avg) ** 2 for x in window) / (len(window) - 1)
        std = var ** 0.5
    else:
        std = None
    return {
        "n_games": len(window),
        "avg_runs": avg,
        "std_runs": std,
        "last_runs": window[-min(len(window), 10):],
    }


def load_probable_starters(con: sqlite3.Connection, start: str, end: str) -> Dict[Tuple[int, int], int]:
    """{(game_pk, team_id): player_id} for games in date range (best effort)."""
    rows = con.execute(
        """
        SELECT gp.game_pk, gp.team_id, gp.player_id
        FROM game_probable_pitchers gp
        JOIN games g ON g.game_pk = gp.game_pk
        WHERE g.game_type = 'R'
          AND g.game_date_et BETWEEN ? AND ?
          AND gp.player_id IS NOT NULL
        """,
        (start, end),
    ).fetchall()
    m: Dict[Tuple[int, int], int] = {}
    for r in rows:
        m[(int(r["game_pk"]), int(r["team_id"]))] = int(r["player_id"])
    return m


def infer_starters_from_pgs(con: sqlite3.Connection, start: str, end: str) -> Dict[Tuple[int, int], int]:
    """
    Fallback starter inference for historical seasons.

    For each (game_pk, pitching_team_id), pick the pitcher appearance with the most innings pitched.
    This is a heuristic, but it is widely available historically unlike probable starters.
    """
    rows = con.execute(
        """
        SELECT
            g.game_pk,
            pgs.team_id,
            pgs.player_id,
            pgs.innings_pitched
        FROM player_game_stats pgs
        JOIN games g ON g.game_pk = pgs.game_pk
        WHERE pgs.player_role = 'pitcher'
          AND g.game_type = 'R'
          AND g.status = 'Final'
          AND g.game_date_et BETWEEN ? AND ?
          AND pgs.team_id IS NOT NULL
          AND pgs.player_id IS NOT NULL
          AND pgs.innings_pitched IS NOT NULL
        """,
        (start, end),
    ).fetchall()

    best: Dict[Tuple[int, int], Tuple[int, float]] = {}
    for r in rows:
        key = (int(r["game_pk"]), int(r["team_id"]))
        pid = int(r["player_id"])
        ip = float(r["innings_pitched"])
        cur = best.get(key)
        if cur is None or ip > cur[1]:
            best[key] = (pid, ip)
    return {k: v[0] for k, v in best.items()}


def build_pitcher_history(con: sqlite3.Connection, season: Optional[int]) -> Dict[int, List[Dict[str, Any]]]:
    """
    {pitcher_id: [ {game_date, game_pk, ip, er, h, bb, so}, ... ]} ordered by date.
    Best-effort: appearances from player_game_stats (not guaranteed to be starts).
    """
    season_clause = "AND g.season = ?" if season is not None else ""
    params: Tuple[Any, ...] = (season,) if season is not None else ()
    rows = con.execute(
        f"""
        SELECT
            pgs.player_id,
            pgs.game_pk,
            g.game_date_et AS game_date,
            pgs.innings_pitched,
            pgs.earned_runs,
            pgs.hits_allowed,
            pgs.walks_allowed,
            pgs.strikeouts_pit
        FROM player_game_stats pgs
        JOIN games g ON g.game_pk = pgs.game_pk
        WHERE pgs.player_role = 'pitcher'
          AND g.game_type = 'R'
          AND g.status = 'Final'
          {season_clause}
          AND pgs.innings_pitched IS NOT NULL
        ORDER BY g.game_date_et, pgs.game_pk
        """,
        params,
    ).fetchall()
    hist: Dict[int, List[Dict[str, Any]]] = {}
    for r in rows:
        pid = int(r["player_id"])
        hist.setdefault(pid, []).append(dict(r))
    return hist


def rolling_pitcher_metrics(p_hist: Dict[int, List[Dict[str, Any]]],
                            pitcher_id: int,
                            game_date: str,
                            n: int) -> Dict[str, Any]:
    """Rolling pitcher metrics from last n appearances strictly before game_date."""
    rows = p_hist.get(pitcher_id, [])
    prior = [r for r in rows if (str(r.get("game_date") or "")) < game_date]
    window = prior[-n:] if n > 0 else prior

    ip = [float(r.get("innings_pitched")) for r in window if r.get("innings_pitched") is not None]
    er = [float(r.get("earned_runs")) for r in window if r.get("earned_runs") is not None]
    h = [float(r.get("hits_allowed")) for r in window if r.get("hits_allowed") is not None]
    bb = [float(r.get("walks_allowed")) for r in window if r.get("walks_allowed") is not None]
    so = [float(r.get("strikeouts_pit")) for r in window if r.get("strikeouts_pit") is not None]

    ip_sum = sum(ip) if ip else None
    er_sum = sum(er) if er else None
    h_sum = sum(h) if h else None
    bb_sum = sum(bb) if bb else None
    so_sum = sum(so) if so else None

    era = None
    if ip_sum and ip_sum > 0 and er_sum is not None:
        era = 9.0 * er_sum / ip_sum

    whip = None
    if ip_sum and ip_sum > 0:
        whip = _safe_div((h_sum or 0.0) + (bb_sum or 0.0), ip_sum)

    kbb = _safe_div(so_sum, bb_sum) if (so_sum is not None and bb_sum is not None) else None

    return {
        "n_starts": len(window),
        "ip_sum": ip_sum,
        "era": era,
        "whip": whip,
        "kbb": kbb,
        "avg_ip": _mean(ip),
    }


def _quantile(sorted_vals: List[float], q: float) -> Optional[float]:
    if not sorted_vals:
        return None
    if q <= 0:
        return sorted_vals[0]
    if q >= 1:
        return sorted_vals[-1]
    idx = q * (len(sorted_vals) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def derive_pitcher_strength_labels(rows: List[Dict[str, Any]],
                                   min_starts: int = 3) -> Tuple[Optional[float], Optional[float]]:
    """
    Compute (strong_cutoff, weak_cutoff) on rolling ERA across rows.
    Strong = ERA <= 33rd percentile, Weak = ERA >= 67th percentile.
    Returns cutoffs (q33, q67). If insufficient data, returns (None, None).
    """
    eras: List[float] = []
    for r in rows:
        try:
            pm = json.loads(r.get("pitcher_metrics") or "{}")
        except Exception:
            continue
        era = pm.get("era")
        n = pm.get("n_starts") or 0
        if era is None or n < min_starts:
            continue
        eras.append(float(era))
    eras.sort()
    return _quantile(eras, 1 / 3), _quantile(eras, 2 / 3)


def _team_result(team_runs: Optional[int], opp_runs: Optional[int]) -> Optional[str]:
    if team_runs is None or opp_runs is None:
        return None
    if team_runs > opp_runs:
        return "W"
    if team_runs < opp_runs:
        return "L"
    return "T"


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    cols = list(rows[0].keys())
    try:
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows)
        return
    except PermissionError:
        # Common on Windows/OneDrive when the CSV is open in Excel.
        alt = path.parent / f"{path.name}.new"
        with alt.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows)
        print(f"[warn] Could not overwrite (file locked): {path}")
        print(f"[warn] Wrote instead: {alt}")
        return


def ensure_output_table(con: sqlite3.Connection, table: str) -> None:
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_pk INTEGER,
            game_date TEXT,
            period_label TEXT,
            team_window INTEGER,
            pitcher_window INTEGER,
            team_id INTEGER,
            team_name TEXT,
            is_home INTEGER,
            opponent_team_id INTEGER,
            opponent_team_name TEXT,
            opponent_pitcher_id INTEGER,
            opponent_pitcher_name TEXT,
            opponent_pitcher_throws TEXT,
            team_runs INTEGER,
            opponent_runs INTEGER,
            team_result TEXT,
            actual_win INTEGER,
            closing_ml INTEGER,
            implied_prob REAL,
            pitcher_strength TEXT,
            offensive_metrics TEXT,
            pitcher_metrics TEXT,
            UNIQUE (game_pk, team_id, period_label, team_window, pitcher_window)
        )
        """
    )
    con.commit()


def write_table(con: sqlite3.Connection, table: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    ensure_output_table(con, table)
    con.executemany(
        f"""
        INSERT OR REPLACE INTO {table}
            (game_pk, game_date, period_label, team_window, pitcher_window,
             team_id, team_name, is_home,
             opponent_team_id, opponent_team_name,
             opponent_pitcher_id, opponent_pitcher_name, opponent_pitcher_throws,
             team_runs, opponent_runs, team_result, actual_win,
             closing_ml, implied_prob,
             pitcher_strength, offensive_metrics, pitcher_metrics)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            (
                r["game_pk"],
                r["game_date"],
                r.get("period_label"),
                r.get("team_window"),
                r.get("pitcher_window"),
                r["team_id"],
                r["team_name"],
                r.get("is_home"),
                r["opponent_team_id"],
                r["opponent_team_name"],
                r["opponent_pitcher_id"],
                r["opponent_pitcher_name"],
                r.get("opponent_pitcher_throws"),
                r["team_runs"],
                r.get("opponent_runs"),
                r.get("team_result"),
                r.get("actual_win"),
                r.get("closing_ml"),
                r.get("implied_prob"),
                r.get("pitcher_strength"),
                r["offensive_metrics"],
                r["pitcher_metrics"],
            )
            for r in rows
        ],
    )
    con.commit()


def summarize_team_performance(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Simple summary: games, avg runs, total runs by team.
    (No ML / betting performance yet.)
    """
    by_team: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        tid = int(r["team_id"])
        s = by_team.setdefault(
            tid,
            {
                "team_id": tid,
                "team_name": r.get("team_name"),
                "games": 0,
                "total_runs": 0,
                "avg_runs": None,
                "wins_vs_strong_pitchers": 0,
                "losses_vs_strong_pitchers": 0,
                "wins_vs_weak_pitchers": 0,
                "losses_vs_weak_pitchers": 0,
            },
        )
        s["games"] += 1
        if r.get("team_runs") is not None:
            s["total_runs"] += int(r["team_runs"])

        strength = r.get("pitcher_strength")
        res = r.get("team_result")
        if strength == "strong":
            if res == "W":
                s["wins_vs_strong_pitchers"] += 1
            elif res == "L":
                s["losses_vs_strong_pitchers"] += 1
        elif strength == "weak":
            if res == "W":
                s["wins_vs_weak_pitchers"] += 1
            elif res == "L":
                s["losses_vs_weak_pitchers"] += 1
    out = []
    for tid, s in by_team.items():
        games = s["games"] or 0
        s["avg_runs"] = (s["total_runs"] / games) if games else None
        out.append(s)
    out.sort(key=lambda x: (x["avg_runs"] is None, -(x["avg_runs"] or 0.0)))
    return out


def summarize_pitcher_performance(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Simple pitcher-facing summary using the *offense rows*:
      - games_faced: number of rows where this pitcher was the opponent starter
      - total_runs_allowed: sum of offense team_runs in those rows
      - avg_runs_allowed: total_runs_allowed / games_faced

    This is intentionally simple and uses actual team runs scored against the pitcher.
    """
    by_pid: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        pid = r.get("opponent_pitcher_id")
        if pid is None:
            continue
        pid_i = int(pid)
        s = by_pid.setdefault(
            pid_i,
            {
                "pitcher_id": pid_i,
                "pitcher_name": r.get("opponent_pitcher_name"),
                "games_faced": 0,
                "total_runs_allowed": 0,
                "avg_runs_allowed": None,
            },
        )
        s["games_faced"] += 1
        if r.get("team_runs") is not None:
            s["total_runs_allowed"] += int(r["team_runs"])
    out: List[Dict[str, Any]] = []
    for _, s in by_pid.items():
        g = s["games_faced"] or 0
        s["avg_runs_allowed"] = (s["total_runs_allowed"] / g) if g else None
        out.append(s)
    out.sort(key=lambda x: (x["avg_runs_allowed"] is None, x["avg_runs_allowed"] or 0.0))
    return out


def _default_bundle_dir(period_label: str) -> Path:
    return Path(_REPO_ROOT) / "outputs" / "analysis" / "team_vs_pitcher" / period_label


def _slugify(s: str) -> str:
    s2 = []
    for ch in str(s):
        if ch.isalnum() or ch in ("-", "_"):
            s2.append(ch)
        else:
            s2.append("_")
    out = "".join(s2)
    while "__" in out:
        out = out.replace("__", "_")
    return out.strip("_") or "run"


def _resolve_bundle_paths(args: argparse.Namespace, period_label: str) -> Tuple[Path, Path, Path]:
    """
    Determine (dataset_csv, team_summary_csv, pitcher_summary_csv).

    Rules:
      - If --out-dir is provided: write fixed filenames into that directory.
      - Else if --out is a .csv file: write dataset to that file, and companions alongside it
        using <stem>_team_summary.csv and <stem>_pitcher_summary.csv.
      - Else: write into outputs/analysis/team_vs_pitcher/<period_label>/ with fixed filenames.
    """
    out_dir = Path(args.out_dir) if getattr(args, "out_dir", None) else None
    out = Path(args.out) if getattr(args, "out", None) else None

    period_slug = _slugify(period_label)
    prefix = f"team_vs_pitcher_{period_slug}_tw{int(args.team_window)}_pw{int(args.pitcher_window)}"

    if out_dir is not None:
        d = out_dir
        return (
            d / f"{prefix}.csv",
            d / f"{prefix}_team_summary.csv",
            d / f"{prefix}_pitcher_summary.csv",
        )

    if out is not None and out.suffix.lower() == ".csv":
        d = out.parent
        stem = out.stem
        suffix = f"_tw{int(args.team_window)}_pw{int(args.pitcher_window)}"
        # Avoid duplicating the window suffix if user already encoded it.
        stem2 = stem if stem.endswith(suffix) else f"{stem}{suffix}"
        return out, d / f"{stem2}_team_summary.csv", d / f"{stem2}_pitcher_summary.csv"

    d2 = _default_bundle_dir(period_label)
    return (
        d2 / f"{prefix}.csv",
        d2 / f"{prefix}_team_summary.csv",
        d2 / f"{prefix}_pitcher_summary.csv",
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Dataset: team offense vs opposing starter (rolling).")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--month", default=None, help="Month to backtest (YYYY-MM)")
    g.add_argument("--season", type=int, default=None, help="Season to backtest (YYYY)")
    g.add_argument("--range", nargs=2, metavar=("START", "END"), default=None, help="Explicit ET date range YYYY-MM-DD YYYY-MM-DD")

    p.add_argument("--team-window", type=int, default=10, help="Rolling team games window (default 10)")
    p.add_argument("--pitcher-window", type=int, default=5, help="Rolling pitcher starts window (default 5)")
    p.add_argument("--out", default=None, help="CSV output path (default: outputs/analysis/team_vs_pitcher.csv)")
    p.add_argument("--out-dir", default=None, help="Directory to write analytic bundle (dataset + summaries)")
    p.add_argument("--write-table", default=None, help="Optional table name to write results into")
    p.add_argument("--summary-out", default=None, help="Optional CSV path for team summary")
    args = p.parse_args()

    db_path = get_db_path()
    con = db_connect(db_path, timeout=30)
    con.row_factory = sqlite3.Row

    if args.month:
        start, end = _parse_month(args.month)
        season = int(args.month.split("-")[0])
        period_label = f"month_{args.month}"
    elif args.season is not None:
        season = int(args.season)
        start, end = _season_bounds(con, season)
        period_label = f"season_{season}"
    else:
        start = args.range[0]; end = args.range[1]
        datetime.date.fromisoformat(start)
        datetime.date.fromisoformat(end)
        season = None
        period_label = f"range_{start}_to_{end}"

    games = load_games(con, start, end, season)
    team_hist = build_team_runs_history(con, season=season)
    pitcher_hist = build_pitcher_history(con, season=season)
    starters = load_probable_starters(con, start, end)
    inferred = infer_starters_from_pgs(con, start, end)
    teams = load_team_lookup(con)
    players = load_player_lookup(con)
    closing_ml = load_closing_moneylines(con, start, end)

    out_rows: List[Dict[str, Any]] = []
    for g_ in games:
        gpk = int(g_["game_pk"])
        gdate = str(g_["game_date"])
        home_tid = int(g_["home_team_id"])
        away_tid = int(g_["away_team_id"])
        hs = g_.get("home_score")
        as_ = g_.get("away_score")

        home_team_name = teams.get(home_tid, {}).get("name")
        away_team_name = teams.get(away_tid, {}).get("name")

        # Opposing starters (best effort)
        # pitching team is opponent team
        home_opp_pitcher = starters.get((gpk, away_tid)) or inferred.get((gpk, away_tid))  # away pitcher faces home offense
        away_opp_pitcher = starters.get((gpk, home_tid)) or inferred.get((gpk, home_tid))  # home pitcher faces away offense

        home_cl = closing_ml.get(gpk) or {}
        home_ml = home_cl.get("home_ml")
        home_imp = implied_prob_from_ml(home_ml) if home_ml is not None else None

        out_rows.append({
            "game_pk": gpk,
            "game_date": gdate,
            "period_label": period_label,
            "team_window": int(args.team_window),
            "pitcher_window": int(args.pitcher_window),
            "team_id": home_tid,
            "team_name": home_team_name,
            "is_home": 1,
            "opponent_team_id": away_tid,
            "opponent_team_name": away_team_name,
            "opponent_pitcher_id": home_opp_pitcher,
            "opponent_pitcher_name": players.get(home_opp_pitcher, {}).get("name") if home_opp_pitcher else None,
            "opponent_pitcher_throws": players.get(home_opp_pitcher, {}).get("throws") if home_opp_pitcher else None,
            "team_runs": int(hs) if hs is not None else None,
            "opponent_runs": int(as_) if as_ is not None else None,
            "team_result": _team_result(int(hs) if hs is not None else None, int(as_) if as_ is not None else None),
            "actual_win": 1 if (hs is not None and as_ is not None and int(hs) > int(as_)) else (0 if (hs is not None and as_ is not None) else None),
            "closing_ml": int(home_ml) if home_ml is not None else None,
            "implied_prob": home_imp,
            "pitcher_strength": None,
            "offensive_metrics": _json(rolling_team_offense(team_hist, home_tid, gdate, args.team_window)),
            "pitcher_metrics": _json(rolling_pitcher_metrics(pitcher_hist, home_opp_pitcher, gdate, args.pitcher_window))
            if home_opp_pitcher else _json({"n_starts": 0}),
        })

        away_cl = closing_ml.get(gpk) or {}
        away_ml = away_cl.get("away_ml")
        away_imp = implied_prob_from_ml(away_ml) if away_ml is not None else None

        out_rows.append({
            "game_pk": gpk,
            "game_date": gdate,
            "period_label": period_label,
            "team_window": int(args.team_window),
            "pitcher_window": int(args.pitcher_window),
            "team_id": away_tid,
            "team_name": away_team_name,
            "is_home": 0,
            "opponent_team_id": home_tid,
            "opponent_team_name": home_team_name,
            "opponent_pitcher_id": away_opp_pitcher,
            "opponent_pitcher_name": players.get(away_opp_pitcher, {}).get("name") if away_opp_pitcher else None,
            "opponent_pitcher_throws": players.get(away_opp_pitcher, {}).get("throws") if away_opp_pitcher else None,
            "team_runs": int(as_) if as_ is not None else None,
            "opponent_runs": int(hs) if hs is not None else None,
            "team_result": _team_result(int(as_) if as_ is not None else None, int(hs) if hs is not None else None),
            "actual_win": 1 if (hs is not None and as_ is not None and int(as_) > int(hs)) else (0 if (hs is not None and as_ is not None) else None),
            "closing_ml": int(away_ml) if away_ml is not None else None,
            "implied_prob": away_imp,
            "pitcher_strength": None,
            "offensive_metrics": _json(rolling_team_offense(team_hist, away_tid, gdate, args.team_window)),
            "pitcher_metrics": _json(rolling_pitcher_metrics(pitcher_hist, away_opp_pitcher, gdate, args.pitcher_window))
            if away_opp_pitcher else _json({"n_starts": 0}),
        })

    # Derive pitcher strength thresholds and label each row
    q33, q67 = derive_pitcher_strength_labels(out_rows, min_starts=3)
    if q33 is not None and q67 is not None:
        for r in out_rows:
            try:
                pm = json.loads(r.get("pitcher_metrics") or "{}")
            except Exception:
                continue
            era = pm.get("era")
            n = pm.get("n_starts") or 0
            if era is None or n < 3:
                continue
            era_f = float(era)
            if era_f <= q33:
                r["pitcher_strength"] = "strong"
            elif era_f >= q67:
                r["pitcher_strength"] = "weak"

    dataset_csv, team_summary_csv, pitcher_summary_csv = _resolve_bundle_paths(args, period_label)
    write_csv(dataset_csv, out_rows)

    summary_rows = summarize_team_performance(out_rows)
    pitcher_rows = summarize_pitcher_performance(out_rows)

    # Back-compat: still honor --summary-out when explicitly provided.
    if args.summary_out:
        write_csv(Path(args.summary_out), summary_rows)
    else:
        write_csv(team_summary_csv, summary_rows)
        write_csv(pitcher_summary_csv, pitcher_rows)

    if args.write_table:
        write_table(con, args.write_table, out_rows)

    print(f"Wrote {len(out_rows)} rows -> {dataset_csv}")
    if args.summary_out:
        print(f"Wrote team summary -> {args.summary_out}")
    else:
        print(f"Wrote team summary -> {team_summary_csv}")
        print(f"Wrote pitcher summary -> {pitcher_summary_csv}")
    if args.write_table:
        print(f"Wrote table -> {args.write_table}")
    con.close()


if __name__ == "__main__":
    main()

