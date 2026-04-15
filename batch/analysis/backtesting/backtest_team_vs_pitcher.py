#!/usr/bin/env python3
"""
backtest_team_vs_pitcher.py
────────────────────────────────────────────────────────────────────────────
Dataset builder: team rolling offense vs opposing starter rolling performance.

Change log:
  2026-04-15  Added wind columns (wind_mph, wind_direction, temp_f, wind_in, wind_out)
              and team_rolling_stats join (trs_bat for batting team offensive metrics,
              trs_pit for pitching team SP metrics). Two validity flags added:
              bat_stats_valid (games_in_window >= 5) and pit_stats_valid
              (sp_starts_in_window >= 3). Output filename updated with _enriched suffix.

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
from collections import defaultdict
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


def load_player_lookup(con: sqlite3.Connection) -> Dict[int, Dict[str, Any]]:
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


def implied_prob_bucket_key(implied_prob: float) -> int:
    """5 percentage-point buckets on [0,100): 0-5, 5-10, ..."""
    p = max(0.0, min(1.0 - 1e-9, float(implied_prob)))
    return min(19, int(p * 100) // 5)


def compute_implied_bucket_win_rates(rows: List[Dict[str, Any]]) -> Dict[int, float]:
    """Season (sample) mean actual_win per 5pp implied-prob bucket."""
    sums: Dict[int, float] = defaultdict(float)
    cnts: Dict[int, int] = defaultdict(int)
    for r in rows:
        ip = r.get("implied_prob")
        aw = r.get("actual_win")
        if ip is None or aw is None:
            continue
        k = implied_prob_bucket_key(float(ip))
        sums[k] += float(int(aw))
        cnts[k] += 1
    out: Dict[int, float] = {}
    for k, c in cnts.items():
        if c:
            out[k] = sums[k] / c
    return out


def apply_home_field_adj_win_pct(rows: List[Dict[str, Any]], bucket_rate: Dict[int, float]) -> None:
    """
    For is_home=1 rows only: team's win% prior to this game minus sample-wide
    win rate in the same ±5pp implied-prob bucket (bucket = 5pp wide).
    """
    team_wins: Dict[int, int] = defaultdict(int)
    team_games: Dict[int, int] = defaultdict(int)
    ordered = sorted(
        rows,
        key=lambda r: (str(r.get("game_date") or ""), int(r.get("game_pk") or 0), int(r.get("team_id") or 0)),
    )
    for r in ordered:
        tid = int(r["team_id"])
        if r.get("is_home") == 1 and r.get("implied_prob") is not None:
            bk = implied_prob_bucket_key(float(r["implied_prob"]))
            br = bucket_rate.get(bk)
            prior_g = team_games.get(tid, 0)
            if prior_g > 0 and br is not None:
                pw = team_wins.get(tid, 0) / prior_g
                r["home_field_adj_win_pct"] = pw - br
            else:
                r["home_field_adj_win_pct"] = None
        else:
            r["home_field_adj_win_pct"] = None
        if r.get("actual_win") is not None:
            team_games[tid] += 1
            team_wins[tid] += int(r["actual_win"])


def _pick_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    lower = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None


def load_team_rolling_stats_map(
    con: sqlite3.Connection, table: str, rows_ref: List[Dict[str, Any]]
) -> Dict[Tuple[int, int], Dict[str, Any]]:
    """
    (game_pk, team_id) -> full row dict from team_rolling_stats (all columns).
    """
    out: Dict[Tuple[int, int], Dict[str, Any]] = {}
    if not table or not all(ch.isalnum() or ch == "_" for ch in table):
        return out
    gpks = sorted({int(r["game_pk"]) for r in rows_ref})
    if not gpks:
        return out
    try:
        ph = ",".join("?" for _ in gpks)
        raw = con.execute(f"SELECT * FROM {table} WHERE game_pk IN ({ph})", gpks).fetchall()
    except sqlite3.Error:
        print(f"[warn] team_rolling_stats: could not read table {table!r} — rolling columns left NULL", flush=True)
        return out
    if not raw:
        return out
    cols = list(raw[0].keys())
    tcol = _pick_col(cols, ["batting_team_id", "team_id", "batting_team"])
    gcol = _pick_col(cols, ["game_pk"])
    if not tcol or not gcol:
        print(f"[warn] team_rolling_stats: missing game_pk/team column in {table!r}", flush=True)
        return out

    for row in raw:
        gpk = int(row[gcol])
        try:
            tid = int(float(row[tcol]))
        except (TypeError, ValueError):
            continue
        out[(gpk, tid)] = dict(row)
    return out


def _trs_val(row: Optional[Dict[str, Any]], key: str) -> Any:
    if not row or key not in row:
        return None
    return row[key]


def _trs_float(row: Optional[Dict[str, Any]], key: str) -> Optional[float]:
    v = _trs_val(row, key)
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _trs_int(row: Optional[Dict[str, Any]], key: str) -> Optional[int]:
    v = _trs_val(row, key)
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def apply_team_rolling_columns(rows: List[Dict[str, Any]], tr_map: Dict[Tuple[int, int], Dict[str, Any]]) -> None:
    """Batting team (team_id) + opposing/pitching team (opponent_team_id) rolling rows from team_rolling_stats."""
    for r in rows:
        gpk = int(r["game_pk"])
        tid = int(r["team_id"])
        oid = int(r["opponent_team_id"])
        bat = tr_map.get((gpk, tid))
        pit = tr_map.get((gpk, oid))

        r["bat_games_in_window"] = _trs_int(bat, "games_in_window")
        r["bat_rolling_runs_pg"] = _trs_float(bat, "rolling_runs_scored_pg")
        r["bat_rolling_ra_pg"] = _trs_float(bat, "rolling_runs_allowed_pg")
        r["bat_rolling_run_diff"] = _trs_float(bat, "rolling_run_diff_pg")
        r["bat_rolling_ops"] = _trs_float(bat, "rolling_ops")
        r["bat_rolling_obp"] = _trs_float(bat, "rolling_obp")
        r["bat_rolling_slg"] = _trs_float(bat, "rolling_slg")
        r["bat_rolling_iso"] = _trs_float(bat, "rolling_iso")
        r["bat_rolling_k_pct"] = _trs_float(bat, "rolling_k_pct")
        r["bat_rolling_bb_pct"] = _trs_float(bat, "rolling_bb_pct")
        r["bat_rolling_hr_pg"] = _trs_float(bat, "rolling_hr_pg")
        r["bat_rolling_ops_home"] = _trs_float(bat, "rolling_ops_home")
        r["bat_rolling_ops_road"] = _trs_float(bat, "rolling_ops_road")
        r["bat_home_games_in_window"] = _trs_int(bat, "home_games_in_window")
        r["bat_road_games_in_window"] = _trs_int(bat, "road_games_in_window")

        r["pit_sp_starts_in_window"] = _trs_int(pit, "sp_starts_in_window")
        r["pit_rolling_sp_era"] = _trs_float(pit, "rolling_sp_era")
        r["pit_rolling_sp_whip"] = _trs_float(pit, "rolling_sp_whip")
        r["pit_rolling_sp_k9"] = _trs_float(pit, "rolling_sp_k9")
        r["pit_rolling_ra_pg"] = _trs_float(pit, "rolling_runs_allowed_pg")

        # Legacy export columns (batting team = same row as trs_bat)
        r["team_rolling_ops"] = r["bat_rolling_ops"]
        r["team_rolling_runs_pg"] = r["bat_rolling_runs_pg"]
        r["team_rolling_k_pct"] = r["bat_rolling_k_pct"]


def _wind_in(direction: Any) -> bool:
    if not direction:
        return False
    d = str(direction).upper()
    if "," in d:
        d = d.split(",", 1)[1].strip()
    return "IN" in d and "R TO" not in d and "L TO" not in d


def _wind_out(direction: Any) -> bool:
    if not direction:
        return False
    d = str(direction).upper()
    if "," in d:
        d = d.split(",", 1)[1].strip()
    return "OUT" in d


def apply_wind_in_out_flags(rows: List[Dict[str, Any]]) -> None:
    """Set wind_in / wind_out from wind_direction (pandas optional)."""
    try:
        import pandas as pd

        df = pd.DataFrame(rows)
        df["wind_in"] = df["wind_direction"].apply(_wind_in)
        df["wind_out"] = df["wind_direction"].apply(_wind_out)
        fixed = df.to_dict("records")
        rows.clear()
        rows.extend(fixed)
    except ImportError:
        for r in rows:
            wd = r.get("wind_direction")
            r["wind_in"] = _wind_in(wd)
            r["wind_out"] = _wind_out(wd)


def apply_stats_validity_flags(rows: List[Dict[str, Any]]) -> None:
    """
    Mark rows where team_rolling_stats-derived windows are large enough for analysis.
    Runs before implied-prob bucket / home-field adjustment (do not drop rows).
    """
    try:
        import pandas as pd

        df = pd.DataFrame(rows)
        df["bat_stats_valid"] = df["bat_games_in_window"] >= 5
        df["pit_stats_valid"] = df["pit_sp_starts_in_window"] >= 3
        fixed = df.to_dict("records")
        rows.clear()
        rows.extend(fixed)
    except ImportError:
        for r in rows:
            bg = r.get("bat_games_in_window")
            ps = r.get("pit_sp_starts_in_window")
            try:
                r["bat_stats_valid"] = int(bg) >= 5 if bg is not None else False
            except (TypeError, ValueError):
                r["bat_stats_valid"] = False
            try:
                r["pit_stats_valid"] = int(ps) >= 3 if ps is not None else False
            except (TypeError, ValueError):
                r["pit_stats_valid"] = False


def season_boundary_check(rows: List[Dict[str, Any]], boundary_season: Optional[int]) -> None:
    """
    For each pitcher (opponent_pitcher_id), first appearance in the sample should have
    rolling pitcher_metrics n_starts == 0 (no prior appearances in rolling window).
    """
    if boundary_season is None:
        return
    by_pid: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    prefix = str(boundary_season)
    for r in rows:
        if not str(r.get("game_date") or "").startswith(prefix):
            continue
        pid = r.get("opponent_pitcher_id")
        if pid is None:
            continue
        by_pid[int(pid)].append(r)
    fails: List[str] = []
    ok = 0
    for pid, lst in by_pid.items():
        lst.sort(key=lambda x: (str(x.get("game_date") or ""), int(x.get("game_pk") or 0)))
        first = lst[0]
        try:
            pm = json.loads(first.get("pitcher_metrics") or "{}")
        except Exception:
            pm = {}
        n0 = pm.get("n_starts")
        if n0 == 0:
            ok += 1
        else:
            fails.append(f"pitcher_id={pid} game_pk={first.get('game_pk')} date={first.get('game_date')} n_starts={n0!r}")
    print(
        f"[season_boundary_check] season={boundary_season}: first-game pitchers OK={ok} "
        f"FAIL={len(fails)} (expect n_starts=0)",
        flush=True,
    )
    for line in fails[:25]:
        print(f"  [season_boundary_check] FAIL {line}", flush=True)
    if len(fails) > 25:
        print(f"  [season_boundary_check] ... and {len(fails) - 25} more", flush=True)


def print_null_closing_ml_by_pitcher_strength(rows: List[Dict[str, Any]]) -> None:
    """Count rows with NULL closing_ml by pitcher_strength tier."""
    counts: Dict[str, int] = defaultdict(int)
    for r in rows:
        tier = r.get("pitcher_strength")
        key = str(tier) if tier else "unlabeled"
        if r.get("closing_ml") is None:
            counts[key] += 1
    print("[missing_odds] NULL closing_ml count by pitcher_strength:", flush=True)
    for k in sorted(counts.keys(), key=lambda x: (x != "strong", x != "weak", x != "unlabeled", x)):
        print(f"  {k}: {counts[k]}", flush=True)


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
            g.season AS season,
            g.game_start_utc,
            g.wind_mph,
            g.wind_direction,
            g.temp_f,
            g.home_team_id,
            g.away_team_id,
            g.home_score,
            g.away_score
        FROM games g
        WHERE g.game_type = 'R'
          AND g.status = 'Final'
          -- Drop games with no weather row (sparse historical DBs; 2026+ ingest typically has wind)
          AND g.wind_mph IS NOT NULL
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
    Best-effort: infers *starter* as the pitcher with most IP for that pitching team in that game.
    """
    season_clause = "AND g.season = ?" if season is not None else ""
    params: Tuple[Any, ...] = (season,) if season is not None else ()
    rows = con.execute(
        f"""
        SELECT
            pgs.player_id,
            pgs.game_pk,
            g.season AS season,
            g.game_date_et AS game_date,
            pgs.team_id,
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
          AND pgs.team_id IS NOT NULL
        ORDER BY g.game_date_et, pgs.game_pk
        """,
        params,
    ).fetchall()
    # Keep only "starter" appearances per (game_pk, team_id) by max IP.
    best_by_game_team: Dict[Tuple[int, int], Dict[str, Any]] = {}
    best_ip: Dict[Tuple[int, int], float] = {}
    for r in rows:
        key = (int(r["game_pk"]), int(r["team_id"]))
        ip = float(r["innings_pitched"])
        cur = best_ip.get(key)
        if cur is None or ip > cur:
            best_ip[key] = ip
            best_by_game_team[key] = dict(r)

    hist: Dict[int, List[Dict[str, Any]]] = {}
    for rec in best_by_game_team.values():
        pid = int(rec["player_id"])
        hist.setdefault(pid, []).append(rec)
    return hist


def rolling_pitcher_metrics(p_hist: Dict[int, List[Dict[str, Any]]],
                            pitcher_id: int,
                            game_date: str,
                            n: int,
                            *,
                            season: Optional[int] = None) -> Dict[str, Any]:
    """Rolling pitcher metrics from last n appearances strictly before game_date (season-scoped when season is set)."""
    rows = p_hist.get(pitcher_id, [])
    prior = [r for r in rows if (str(r.get("game_date") or "")) < game_date]
    if season is not None:
        prior = [r for r in prior if r.get("season") == season]
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
            wind_mph INTEGER,
            wind_direction TEXT,
            temp_f INTEGER,
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
            opponent_pitcher_era_rolling REAL,
            batting_team_avg_runs_last10 REAL,
            team_runs INTEGER,
            opponent_runs INTEGER,
            team_result TEXT,
            actual_win INTEGER,
            closing_ml INTEGER,
            implied_prob REAL,
            home_field_adj_win_pct REAL,
            bat_games_in_window INTEGER,
            bat_rolling_runs_pg REAL,
            bat_rolling_ra_pg REAL,
            bat_rolling_run_diff REAL,
            bat_rolling_ops REAL,
            bat_rolling_obp REAL,
            bat_rolling_slg REAL,
            bat_rolling_iso REAL,
            bat_rolling_k_pct REAL,
            bat_rolling_bb_pct REAL,
            bat_rolling_hr_pg REAL,
            bat_rolling_ops_home REAL,
            bat_rolling_ops_road REAL,
            bat_home_games_in_window INTEGER,
            bat_road_games_in_window INTEGER,
            pit_sp_starts_in_window INTEGER,
            pit_rolling_sp_era REAL,
            pit_rolling_sp_whip REAL,
            pit_rolling_sp_k9 REAL,
            pit_rolling_ra_pg REAL,
            team_rolling_ops REAL,
            team_rolling_runs_pg REAL,
            team_rolling_k_pct REAL,
            wind_in INTEGER,
            wind_out INTEGER,
            bat_stats_valid INTEGER,
            pit_stats_valid INTEGER,
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
            (game_pk, game_date, wind_mph, wind_direction, temp_f,
             period_label, team_window, pitcher_window,
             team_id, team_name, is_home,
             opponent_team_id, opponent_team_name,
             opponent_pitcher_id, opponent_pitcher_name, opponent_pitcher_throws,
             opponent_pitcher_era_rolling, batting_team_avg_runs_last10,
             team_runs, opponent_runs, team_result, actual_win,
             closing_ml, implied_prob,
             home_field_adj_win_pct,
             bat_games_in_window, bat_rolling_runs_pg, bat_rolling_ra_pg, bat_rolling_run_diff,
             bat_rolling_ops, bat_rolling_obp, bat_rolling_slg, bat_rolling_iso,
             bat_rolling_k_pct, bat_rolling_bb_pct, bat_rolling_hr_pg,
             bat_rolling_ops_home, bat_rolling_ops_road,
             bat_home_games_in_window, bat_road_games_in_window,
             pit_sp_starts_in_window, pit_rolling_sp_era, pit_rolling_sp_whip, pit_rolling_sp_k9, pit_rolling_ra_pg,
             team_rolling_ops, team_rolling_runs_pg, team_rolling_k_pct,
             wind_in, wind_out, bat_stats_valid, pit_stats_valid,
             pitcher_strength, offensive_metrics, pitcher_metrics)
        VALUES ({",".join(["?"] * 55)})
        """,
        [
            (
                r["game_pk"],
                r["game_date"],
                r.get("wind_mph"),
                r.get("wind_direction"),
                r.get("temp_f"),
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
                r.get("opponent_pitcher_era_rolling"),
                r.get("batting_team_avg_runs_last10"),
                r["team_runs"],
                r.get("opponent_runs"),
                r.get("team_result"),
                r.get("actual_win"),
                r.get("closing_ml"),
                r.get("implied_prob"),
                r.get("home_field_adj_win_pct"),
                r.get("bat_games_in_window"),
                r.get("bat_rolling_runs_pg"),
                r.get("bat_rolling_ra_pg"),
                r.get("bat_rolling_run_diff"),
                r.get("bat_rolling_ops"),
                r.get("bat_rolling_obp"),
                r.get("bat_rolling_slg"),
                r.get("bat_rolling_iso"),
                r.get("bat_rolling_k_pct"),
                r.get("bat_rolling_bb_pct"),
                r.get("bat_rolling_hr_pg"),
                r.get("bat_rolling_ops_home"),
                r.get("bat_rolling_ops_road"),
                r.get("bat_home_games_in_window"),
                r.get("bat_road_games_in_window"),
                r.get("pit_sp_starts_in_window"),
                r.get("pit_rolling_sp_era"),
                r.get("pit_rolling_sp_whip"),
                r.get("pit_rolling_sp_k9"),
                r.get("pit_rolling_ra_pg"),
                r.get("team_rolling_ops"),
                r.get("team_rolling_runs_pg"),
                r.get("team_rolling_k_pct"),
                1 if r.get("wind_in") else 0,
                1 if r.get("wind_out") else 0,
                1 if r.get("bat_stats_valid") else 0,
                1 if r.get("pit_stats_valid") else 0,
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
    enriched = f"_enriched_{datetime.date.today().isoformat()}"
    prefix = (
        f"team_vs_pitcher_{period_slug}_tw{int(args.team_window)}_pw{int(args.pitcher_window)}"
        f"{enriched}"
    )

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
        enriched = f"_enriched_{datetime.date.today().isoformat()}"
        # Avoid duplicating the window suffix if user already encoded it.
        stem2 = stem if stem.endswith(suffix) else f"{stem}{suffix}"
        if not stem2.endswith(enriched):
            stem2 = f"{stem2}{enriched}"
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
    p.add_argument(
        "--team-rolling-stats-table",
        default="team_rolling_stats",
        help="Table with (game_pk, batting_team_id) and rolling OPS / runs / K%%; use --skip-team-rolling-join if not built",
    )
    p.add_argument(
        "--skip-team-rolling-join",
        action="store_true",
        help="Do not join team_rolling_stats; team_rolling_* columns will be NULL",
    )
    args = p.parse_args()

    db_path = get_db_path()
    con = db_connect(db_path, timeout=30)
    con.row_factory = sqlite3.Row

    boundary_season: Optional[int] = None
    if args.month:
        start, end = _parse_month(args.month)
        season = int(args.month.split("-")[0])
        period_label = f"month_{args.month}"
        boundary_season = None  # month runs do not necessarily include season opener
    elif args.season is not None:
        season = int(args.season)
        start, end = _season_bounds(con, season)
        period_label = f"season_{season}"
        boundary_season = season
    else:
        start = args.range[0]; end = args.range[1]
        datetime.date.fromisoformat(start)
        datetime.date.fromisoformat(end)
        season = None
        period_label = f"range_{start}_to_{end}"
        try:
            boundary_season = int(str(start)[:4])
        except ValueError:
            boundary_season = None

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
        gseason = g_.get("season")
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

        wind_mph = g_.get("wind_mph")
        wind_direction = g_.get("wind_direction")
        temp_f = g_.get("temp_f")

        home_pitch = (
            rolling_pitcher_metrics(
                pitcher_hist,
                home_opp_pitcher,
                gdate,
                args.pitcher_window,
                season=int(gseason) if gseason is not None else None,
            )
            if home_opp_pitcher
            else {}
        )
        home_off = rolling_team_offense(team_hist, home_tid, gdate, args.team_window)
        home_off_10 = rolling_team_offense(team_hist, home_tid, gdate, 10)

        out_rows.append({
            "game_pk": gpk,
            "game_date": gdate,
            "wind_mph": wind_mph,
            "wind_direction": wind_direction,
            "temp_f": temp_f,
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
            "opponent_pitcher_era_rolling": home_pitch.get("era") if home_opp_pitcher else None,
            "batting_team_avg_runs_last10": home_off_10.get("avg_runs"),
            "team_runs": int(hs) if hs is not None else None,
            "opponent_runs": int(as_) if as_ is not None else None,
            "team_result": _team_result(int(hs) if hs is not None else None, int(as_) if as_ is not None else None),
            "actual_win": 1 if (hs is not None and as_ is not None and int(hs) > int(as_)) else (0 if (hs is not None and as_ is not None) else None),
            "closing_ml": int(home_ml) if home_ml is not None else None,
            "implied_prob": home_imp,
            "pitcher_strength": None,
            "offensive_metrics": _json(home_off),
            "pitcher_metrics": _json(home_pitch if home_opp_pitcher else {"n_starts": 0}),
        })

        away_cl = closing_ml.get(gpk) or {}
        away_ml = away_cl.get("away_ml")
        away_imp = implied_prob_from_ml(away_ml) if away_ml is not None else None

        away_pitch = (
            rolling_pitcher_metrics(
                pitcher_hist,
                away_opp_pitcher,
                gdate,
                args.pitcher_window,
                season=int(gseason) if gseason is not None else None,
            )
            if away_opp_pitcher
            else {}
        )
        away_off = rolling_team_offense(team_hist, away_tid, gdate, args.team_window)
        away_off_10 = rolling_team_offense(team_hist, away_tid, gdate, 10)

        out_rows.append({
            "game_pk": gpk,
            "game_date": gdate,
            "wind_mph": wind_mph,
            "wind_direction": wind_direction,
            "temp_f": temp_f,
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
            "opponent_pitcher_era_rolling": away_pitch.get("era") if away_opp_pitcher else None,
            "batting_team_avg_runs_last10": away_off_10.get("avg_runs"),
            "team_runs": int(as_) if as_ is not None else None,
            "opponent_runs": int(hs) if hs is not None else None,
            "team_result": _team_result(int(as_) if as_ is not None else None, int(hs) if hs is not None else None),
            "actual_win": 1 if (hs is not None and as_ is not None and int(as_) > int(hs)) else (0 if (hs is not None and as_ is not None) else None),
            "closing_ml": int(away_ml) if away_ml is not None else None,
            "implied_prob": away_imp,
            "pitcher_strength": None,
            "offensive_metrics": _json(away_off),
            "pitcher_metrics": _json(away_pitch if away_opp_pitcher else {"n_starts": 0}),
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

    tr_table = "" if args.skip_team_rolling_join else (args.team_rolling_stats_table or "").strip()
    if tr_table:
        tr_map = load_team_rolling_stats_map(con, tr_table, out_rows)
        apply_team_rolling_columns(out_rows, tr_map)
    else:
        apply_team_rolling_columns(out_rows, {})

    apply_wind_in_out_flags(out_rows)
    apply_stats_validity_flags(out_rows)

    bucket_rate = compute_implied_bucket_win_rates(out_rows)
    apply_home_field_adj_win_pct(out_rows, bucket_rate)
    season_boundary_check(out_rows, boundary_season)
    print_null_closing_ml_by_pitcher_strength(out_rows)

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

