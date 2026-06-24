#!/usr/bin/env python3
"""
Signal loss factor audit — 2026 live staked bets.

Exploratory analysis of supplementary pre-game factors vs bet outcomes.
Does NOT implement gates — surfaces patterns only.

USAGE:
  python scripts/analysis/loss_audit_2026.py
  python scripts/analysis/loss_audit_2026.py --db data/mlb_stats.db
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.db.connection import connect as db_connect, get_db_path

OUT_PATH = ROOT / "outputs" / "reports" / "loss_audit_2026.txt"
ET = ZoneInfo("America/New_York")

HOT_OFFENSE_RPG = 5.5
COLD_OFFENSE_RPG = 3.5
HOT_STREAK_LEN = 3
SP_BLOWUP_ERA_DELTA = 5.0
SP_DELIVERED_ERA_DELTA = 2.0
MIN_SP_IP = 4.0

LM_BOOKS = ("draftkings", "fanduel", "betmgm")
_GAME_DATE = "COALESCE(NULLIF(TRIM(g.game_date_et), ''), g.game_date)"
_GAME_DATE_G2 = "COALESCE(NULLIF(TRIM(g2.game_date_et), ''), g2.game_date)"


@dataclass
class BetRow:
    id: int
    game_date: str
    game_pk: int
    signal_type: str
    market_type: str
    pick_side: str | None
    bet: str
    odds_taken: int | None
    stake_units: float
    result: str
    pnl_units: float | None
    home_team_id: int
    away_team_id: int
    home_score: int | None
    away_score: int | None
    total_runs: int | None
    wind_mph: int | None
    wind_direction: str | None
    home_abbr: str
    away_abbr: str
    # rolling offense
    home_runs_pg: float | None = None
    away_runs_pg: float | None = None
    home_ops: float | None = None
    away_ops: float | None = None
    trs_coverage: bool = False
    # streaks
    home_streak_type: str | None = None
    home_streak_len: int | None = None
    away_streak_type: str | None = None
    away_streak_len: int | None = None
    # line movement
    total_move: float | None = None
    ml_move_cents: int | None = None
    move_direction: str | None = None
    steam_move: bool = False
    reverse_line_move: bool = False
    over_ticket_pct: float | None = None
    under_ticket_pct: float | None = None
    home_ticket_pct: float | None = None
    away_ticket_pct: float | None = None
    lm_book: str | None = None
    lm_coverage: bool = False
    # SP performance
    hsp_ip: float | None = None
    hsp_er: int | None = None
    hsp_start_era: float | None = None
    hsp_wma: float | None = None
    asp_ip: float | None = None
    asp_er: int | None = None
    asp_start_era: float | None = None
    asp_wma: float | None = None
    sp_coverage: bool = False
    # computed tags
    home_offense_hot: bool = False
    away_offense_hot: bool = False
    home_offense_cold: bool = False
    away_offense_cold: bool = False
    offense_stacked_against: bool = False
    offense_stacked_for: bool = False
    home_win_streak: int = 0
    away_win_streak: int = 0
    team_hot_streak_against: bool = False
    line_move_against_bet: bool = False
    line_move_with_bet: bool = False
    public_same_side: bool = False
    sp_blew_up: bool = False
    sp_delivered: bool = False
    wind_flip: bool | None = None  # None = unknown
    factor_count_against: int = 0
    factor_count_for: int = 0
    tags: dict[str, bool] = field(default_factory=dict)


def _normalize_signal(row: sqlite3.Row) -> str:
    st = (row["signal_type"] or "").strip().upper()
    ps = (row["pick_side"] or "").strip().lower()
    mt = (row["market_type"] or "").strip().lower()
    bet = (row["bet"] or "").upper()
    if st in ("UNDER",):
        return "UNDER"
    if st in ("OWM",):
        return "OWM"
    if st in ("AWAY_DOG_RL",):
        return "AWAY_DOG_RL"
    if st in ("ML", "MV-B", "MV-F", "LEGACY") or mt in ("moneyline", "ml"):
        return "ML"
    if st in ("RL",) or mt in ("spread", "runline"):
        return "RL"
    if st == "" and "ML" in bet:
        return "ML"
    if st == "" and ("RL" in bet or "+1.5" in bet):
        return "RL"
    if ps in ("under", "under_total") or "UNDER" in bet:
        return "UNDER"
    if ps in ("away_rl",):
        return "AWAY_DOG_RL"
    if ps in ("home_ml",) and st == "OWM":
        return "OWM"
    return st or "OTHER"


def _lm_market(signal: str, market_type: str) -> str:
    if signal == "UNDER":
        return "total"
    if signal in ("AWAY_DOG_RL", "RL"):
        return "runline"
    return "moneyline"


def _fetch_bets(con: sqlite3.Connection) -> list[sqlite3.Row]:
    con.row_factory = sqlite3.Row
    return list(
        con.execute(
            f"""
            SELECT
                bl.id,
                bl.game_date,
                bl.game_pk,
                bl.signal_type,
                bl.market_type,
                bl.pick_side,
                bl.bet,
                bl.odds_taken,
                bl.stake_units,
                bl.result,
                bl.pnl_units,
                g.home_team_id,
                g.away_team_id,
                g.home_score,
                g.away_score,
                g.home_score + g.away_score AS total_runs,
                g.wind_mph,
                g.wind_direction,
                th.abbreviation AS home_abbr,
                ta.abbreviation AS away_abbr
            FROM bet_ledger bl
            JOIN games g ON g.game_pk = bl.game_pk
            JOIN teams th ON th.team_id = g.home_team_id
            JOIN teams ta ON ta.team_id = g.away_team_id
            WHERE bl.result IN ('win', 'loss', 'push')
              AND bl.stake_units > 0
              AND g.season = 2026
            ORDER BY bl.game_date, bl.id
            """
        ).fetchall()
    )


def _fetch_team_rolling(con: sqlite3.Connection, game_pk: int, team_id: int, game_date: str) -> sqlite3.Row | None:
    row = con.execute(
        """
        SELECT rolling_runs_scored_pg, rolling_ops, 'game' AS src
        FROM team_rolling_stats
        WHERE game_pk = ? AND team_id = ?
        """,
        (game_pk, team_id),
    ).fetchone()
    if row is not None:
        return row
    return con.execute(
        f"""
        SELECT trs.rolling_runs_scored_pg, trs.rolling_ops, 'fallback' AS src
        FROM team_rolling_stats trs
        JOIN games g2 ON g2.game_pk = trs.game_pk
        WHERE trs.team_id = ?
          AND {_GAME_DATE_G2} < ?
        ORDER BY {_GAME_DATE_G2} DESC, trs.game_pk DESC
        LIMIT 1
        """,
        (team_id, game_date),
    ).fetchone()


def _fetch_standings(con: sqlite3.Connection, game_date: str, team_id: int) -> sqlite3.Row | None:
    return con.execute(
        """
        SELECT streak_type, streak_length
        FROM standings
        WHERE team_id = ?
          AND snapshot_date = (
              SELECT MAX(snapshot_date)
              FROM standings
              WHERE team_id = ?
                AND snapshot_date < ?
          )
        """,
        (team_id, team_id, game_date),
    ).fetchone()


def _fetch_line_movement(
    con: sqlite3.Connection,
    game_pk: int,
    market_type: str,
) -> sqlite3.Row | None:
    for book in LM_BOOKS:
        row = con.execute(
            """
            SELECT bookmaker, total_move, ml_move_cents, move_direction,
                   steam_move, reverse_line_move,
                   over_ticket_pct, under_ticket_pct,
                   home_ticket_pct, away_ticket_pct
            FROM line_movement
            WHERE game_pk = ? AND bookmaker = ? AND market_type = ?
            """,
            (game_pk, book, market_type),
        ).fetchone()
        if row is not None:
            return row
    return con.execute(
        """
        SELECT bookmaker, total_move, ml_move_cents, move_direction,
               steam_move, reverse_line_move,
               over_ticket_pct, under_ticket_pct,
               home_ticket_pct, away_ticket_pct
        FROM line_movement
        WHERE game_pk = ? AND market_type = ?
        LIMIT 1
        """,
        (game_pk, market_type),
    ).fetchone()


def _fetch_sp_lines(con: sqlite3.Connection, game_pk: int) -> dict[str, sqlite3.Row | None]:
    out: dict[str, sqlite3.Row | None] = {"home": None, "away": None}
    for side, col in (("home", "home_team_id"), ("away", "away_team_id")):
        out[side] = con.execute(
            f"""
            SELECT
                pgs.innings_pitched,
                pgs.earned_runs,
                CASE WHEN pgs.innings_pitched > 0
                     THEN pgs.earned_runs * 9.0 / pgs.innings_pitched
                     ELSE NULL END AS start_era,
                pgs.pitches_thrown,
                prs.era_wma AS pre_game_wma
            FROM player_game_stats pgs
            JOIN game_probable_pitchers gpp
              ON gpp.game_pk = pgs.game_pk AND gpp.player_id = pgs.player_id
            JOIN games g ON g.game_pk = pgs.game_pk
            LEFT JOIN pitcher_rolling_stats prs
              ON prs.game_pk = pgs.game_pk AND prs.player_id = pgs.player_id
            WHERE pgs.game_pk = ?
              AND gpp.team_id = g.{col}
              AND pgs.player_role = 'pitcher'
              AND pgs.innings_pitched >= 1.0
            LIMIT 1
            """,
            (game_pk,),
        ).fetchone()
    return out


def _sp_tag(ip: float | None, start_era: float | None, wma: float | None) -> tuple[bool, bool]:
    if ip is None or start_era is None:
        return False, False
    blew = float(ip) < MIN_SP_IP
    if wma is not None:
        blew = blew or float(start_era) > float(wma) + SP_BLOWUP_ERA_DELTA
    delivered = (
        wma is not None
        and float(start_era) <= float(wma) + SP_DELIVERED_ERA_DELTA
        and float(ip) >= MIN_SP_IP
    )
    return blew, delivered


def _line_against(signal: str, pick_side: str | None, lm: sqlite3.Row | None) -> tuple[bool, bool]:
    if lm is None:
        return False, False
    against = False
    with_bet = False
    ps = (pick_side or "").lower()

    if signal == "UNDER":
        tm = lm["total_move"]
        if tm is not None:
            against = float(tm) > 0.05
            with_bet = float(tm) < -0.05
    elif signal in ("OWM", "ML"):
        mc = lm["ml_move_cents"]
        if mc is not None:
            if ps == "home_ml" or signal == "OWM":
                against = int(mc) < -2
                with_bet = int(mc) > 2
            elif ps == "away_ml":
                against = int(mc) > 2
                with_bet = int(mc) < -2
    elif signal in ("AWAY_DOG_RL", "RL"):
        md = (lm["move_direction"] or "").lower()
        if md == "home":
            against = True
        elif md == "away":
            with_bet = True
    return against, with_bet


def _public_same_side(signal: str, pick_side: str | None, lm: sqlite3.Row | None) -> bool:
    if lm is None:
        return False
    ps = (pick_side or "").lower()
    if signal == "UNDER" and lm["under_ticket_pct"] is not None:
        return float(lm["under_ticket_pct"]) >= 55.0
    if signal == "UNDER" and lm["over_ticket_pct"] is not None:
        return float(lm["over_ticket_pct"]) < 45.0
    if ps == "home_ml" and lm["home_ticket_pct"] is not None:
        return float(lm["home_ticket_pct"]) >= 55.0
    if ps == "away_ml" and lm["away_ticket_pct"] is not None:
        return float(lm["away_ticket_pct"]) >= 55.0
    if signal in ("AWAY_DOG_RL", "RL") and lm["away_ticket_pct"] is not None:
        return float(lm["away_ticket_pct"]) >= 55.0
    return bool(int(lm["reverse_line_move"] or 0))


def _offense_against(signal: str, home_hot: bool, away_hot: bool, home_cold: bool, away_cold: bool) -> tuple[bool, bool]:
    if signal == "UNDER":
        return (home_hot or away_hot), (home_cold and away_cold)
    if signal == "OWM":
        return away_hot, home_hot
    if signal in ("AWAY_DOG_RL", "RL"):
        return home_hot, away_hot is False
    if signal == "ML":
        return False, False
    return (home_hot or away_hot), (home_cold or away_cold)


def _streak_against(signal: str, home_w: int, away_w: int) -> bool:
    if signal == "UNDER":
        return home_w >= HOT_STREAK_LEN or away_w >= HOT_STREAK_LEN
    if signal == "OWM":
        return away_w >= HOT_STREAK_LEN
    if signal in ("AWAY_DOG_RL", "RL"):
        return home_w >= HOT_STREAK_LEN
    return False


def _relevant_sp_tags(
    signal: str,
    home: sqlite3.Row | None,
    away: sqlite3.Row | None,
) -> tuple[bool, bool]:
    blew = False
    delivered = False

    def merge(row: sqlite3.Row | None) -> None:
        nonlocal blew, delivered
        if row is None:
            return
        b, d = _sp_tag(row["innings_pitched"], row["start_era"], row["pre_game_wma"])
        blew = blew or b
        delivered = delivered or d

    if signal == "UNDER":
        merge(home)
        merge(away)
    elif signal == "OWM":
        merge(away)
        merge(home)
    elif signal in ("AWAY_DOG_RL", "RL"):
        merge(home)
    else:
        merge(home)
        merge(away)
    return blew, delivered


def enrich_bet(con: sqlite3.Connection, row: sqlite3.Row) -> BetRow:
    signal = _normalize_signal(row)
    b = BetRow(
        id=int(row["id"]),
        game_date=str(row["game_date"]),
        game_pk=int(row["game_pk"]),
        signal_type=signal,
        market_type=str(row["market_type"] or ""),
        pick_side=row["pick_side"],
        bet=str(row["bet"] or ""),
        odds_taken=row["odds_taken"],
        stake_units=float(row["stake_units"] or 0),
        result=str(row["result"]),
        pnl_units=float(row["pnl_units"]) if row["pnl_units"] is not None else None,
        home_team_id=int(row["home_team_id"]),
        away_team_id=int(row["away_team_id"]),
        home_score=row["home_score"],
        away_score=row["away_score"],
        total_runs=row["total_runs"],
        wind_mph=row["wind_mph"],
        wind_direction=row["wind_direction"],
        home_abbr=str(row["home_abbr"]),
        away_abbr=str(row["away_abbr"]),
    )

    tr_h = _fetch_team_rolling(con, b.game_pk, b.home_team_id, b.game_date)
    tr_a = _fetch_team_rolling(con, b.game_pk, b.away_team_id, b.game_date)
    if tr_h or tr_a:
        b.trs_coverage = True
    if tr_h:
        b.home_runs_pg = tr_h["rolling_runs_scored_pg"]
        b.home_ops = tr_h["rolling_ops"]
    if tr_a:
        b.away_runs_pg = tr_a["rolling_runs_scored_pg"]
        b.away_ops = tr_a["rolling_ops"]

    st_h = _fetch_standings(con, b.game_date, b.home_team_id)
    st_a = _fetch_standings(con, b.game_date, b.away_team_id)
    if st_h:
        b.home_streak_type = st_h["streak_type"]
        b.home_streak_len = st_h["streak_length"]
        if st_h["streak_type"] == "W" and st_h["streak_length"]:
            b.home_win_streak = int(st_h["streak_length"])
    if st_a:
        b.away_streak_type = st_a["streak_type"]
        b.away_streak_len = st_a["streak_length"]
        if st_a["streak_type"] == "W" and st_a["streak_length"]:
            b.away_win_streak = int(st_a["streak_length"])

    lm = _fetch_line_movement(con, b.game_pk, _lm_market(signal, b.market_type))
    if lm:
        b.lm_coverage = True
        b.lm_book = lm["bookmaker"]
        b.total_move = lm["total_move"]
        b.ml_move_cents = lm["ml_move_cents"]
        b.move_direction = lm["move_direction"]
        b.steam_move = bool(int(lm["steam_move"] or 0))
        b.reverse_line_move = bool(int(lm["reverse_line_move"] or 0))
        b.over_ticket_pct = lm["over_ticket_pct"]
        b.under_ticket_pct = lm["under_ticket_pct"]
        b.home_ticket_pct = lm["home_ticket_pct"]
        b.away_ticket_pct = lm["away_ticket_pct"]

    sps = _fetch_sp_lines(con, b.game_pk)
    if sps["home"] or sps["away"]:
        b.sp_coverage = True
    if sps["home"]:
        b.hsp_ip = sps["home"]["innings_pitched"]
        b.hsp_er = sps["home"]["earned_runs"]
        b.hsp_start_era = sps["home"]["start_era"]
        b.hsp_wma = sps["home"]["pre_game_wma"]
    if sps["away"]:
        b.asp_ip = sps["away"]["innings_pitched"]
        b.asp_er = sps["away"]["earned_runs"]
        b.asp_start_era = sps["away"]["start_era"]
        b.asp_wma = sps["away"]["pre_game_wma"]

    b.home_offense_hot = b.home_runs_pg is not None and float(b.home_runs_pg) >= HOT_OFFENSE_RPG
    b.away_offense_hot = b.away_runs_pg is not None and float(b.away_runs_pg) >= HOT_OFFENSE_RPG
    b.home_offense_cold = b.home_runs_pg is not None and float(b.home_runs_pg) <= COLD_OFFENSE_RPG
    b.away_offense_cold = b.away_runs_pg is not None and float(b.away_runs_pg) <= COLD_OFFENSE_RPG

    b.offense_stacked_against, b.offense_stacked_for = _offense_against(
        signal, b.home_offense_hot, b.away_offense_hot,
        b.home_offense_cold, b.away_offense_cold,
    )
    b.team_hot_streak_against = _streak_against(signal, b.home_win_streak, b.away_win_streak)
    b.line_move_against_bet, b.line_move_with_bet = _line_against(signal, b.pick_side, lm)
    b.public_same_side = _public_same_side(signal, b.pick_side, lm)
    b.sp_blew_up, b.sp_delivered = _relevant_sp_tags(signal, sps["home"], sps["away"])
    b.wind_flip = None

    against_tags = {
        "offense_stacked_against": b.offense_stacked_against,
        "line_move_against_bet": b.line_move_against_bet and b.lm_coverage,
        "steam_move": b.steam_move and b.lm_coverage,
        "reverse_line_move": b.reverse_line_move and b.lm_coverage,
        "public_same_side": b.public_same_side and b.lm_coverage,
        "team_hot_streak_against": b.team_hot_streak_against,
        "sp_blew_up": b.sp_blew_up and b.sp_coverage,
    }
    for_tags = {
        "offense_stacked_for": b.offense_stacked_for,
        "line_move_with_bet": b.line_move_with_bet and b.lm_coverage,
        "sp_delivered": b.sp_delivered and b.sp_coverage,
    }
    b.tags = {**against_tags, **for_tags}
    b.factor_count_against = sum(1 for k, v in against_tags.items() if v)
    b.factor_count_for = sum(1 for k, v in for_tags.items() if v)
    return b


def _win_pct(w: int, n: int) -> str:
    return f"{100.0 * w / n:.1f}%" if n else "n/a"


def _segment_stats(bets: list[BetRow]) -> dict[str, float | int]:
    n = len(bets)
    w = sum(1 for b in bets if b.result == "win")
    l = sum(1 for b in bets if b.result == "loss")
    pnl = sum(float(b.pnl_units or 0) for b in bets)
    return {"n": n, "w": w, "l": l, "win_pct": w / n if n else 0.0, "pnl": pnl}


def _fmt_segment_row(label: str, s: dict) -> str:
    return (
        f"  {label:<28} | {s['n']:>4} | {s['w']:>3} | {s['l']:>3} | "
        f"{_win_pct(s['w'], s['n']):>6} | {s['pnl']:>+7.2f}u"
    )


def _hypothesis_line(name: str, win_with: float, n_with: int, win_without: float, n_without: int) -> str:
    if n_with < 3 or n_without < 3:
        return f"INSUFFICIENT N: {name} — need more bets in each bucket"
    diff_pp = (win_without - win_with) * 100.0
    if diff_pp >= 15.0 and n_with >= 10:
        return f"CONFIRMED: {name} predicts losses — win rate {win_with*100:.1f}% vs {win_without*100:.1f}% without ({diff_pp:+.1f}pp, N={n_with}/{n_without})"
    if diff_pp >= 10.0:
        return f"WEAK SIGNAL: {name} shows {diff_pp:+.1f}pp difference — insufficient for gate (N={n_with}/{n_without})"
    return f"NOT SUPPORTED: {name} shows no meaningful difference ({diff_pp:+.1f}pp, N={n_with}/{n_without})"


def build_report(bets: list[BetRow]) -> str:
    ts = datetime.now(tz=ET).strftime("%Y-%m-%d %I:%M %p ET")
    graded = [b for b in bets if b.result in ("win", "loss", "push")]
    wins = [b for b in graded if b.result == "win"]
    losses = [b for b in graded if b.result == "loss"]
    total_pnl = sum(float(b.pnl_units or 0) for b in graded)
    overall_wr = len(wins) / len(graded) if graded else 0.0

    trs_cov = sum(1 for b in graded if b.trs_coverage)
    lm_cov = sum(1 for b in graded if b.lm_coverage)
    sp_cov = sum(1 for b in graded if b.sp_coverage)

    lines: list[str] = [
        "========================================================",
        "SIGNAL LOSS FACTOR AUDIT — 2026 Season (through current)",
        f"Generated: {ts}",
        f"Total staked bets: {len(graded)}  |  Win rate: {overall_wr*100:.1f}%  |  P&L: {total_pnl:+.2f}u",
        f"Data coverage: team_rolling={trs_cov}/{len(graded)}  line_movement={lm_cov}/{len(graded)}  SP box={sp_cov}/{len(graded)}",
        "  NOTE: line_movement sparse for 2026 bet dates — run load_odds --compute-movement nightly.",
        "  NOTE: team_rolling uses game_pk row or latest prior fallback per team.",
        "========================================================",
        "",
    ]

    # A — factor count against
    lines.extend([
        "A. WIN RATE BY FACTOR COUNT AGAINST BET",
        "  factor_count_against |   N |   W |   L | Win%   | P&L",
        "  " + "-" * 58,
    ])
    for cnt in (0, 1, 2, 3, 4):
        bucket = [b for b in graded if b.factor_count_against == cnt]
        s = _segment_stats(bucket)
        lines.append(_fmt_segment_row(str(cnt), s))
    bucket5 = [b for b in graded if b.factor_count_against >= 5]
    lines.append(_fmt_segment_row("5+", _segment_stats(bucket5)))
    lines.append("")

    # B — line movement by signal
    lines.extend([
        "B. WIN RATE WHEN LINE MOVES AGAINST BET (by signal type)",
        "  Signal       | Line vs bet |   N | Win%   | With-bet Win%",
        "  " + "-" * 58,
    ])
    for sig in ("UNDER", "OWM", "AWAY_DOG_RL", "ML", "ALL"):
        pool = graded if sig == "ALL" else [b for b in graded if b.signal_type == sig]
        if not pool:
            continue
        against = [b for b in pool if b.line_move_against_bet and b.lm_coverage]
        with_b = [b for b in pool if b.line_move_with_bet and b.lm_coverage]
        no_lm = [b for b in pool if not b.lm_coverage]
        sa = _segment_stats(against)
        sw = _segment_stats(with_b)
        lines.append(
            f"  {sig:<12} | against     | {sa['n']:>4} | {_win_pct(sa['w'], sa['n']):>6} | "
            f"with={_win_pct(sw['w'], sw['n'])} (n={sw['n']})"
        )
        neutral = [b for b in pool if b.lm_coverage and not b.line_move_against_bet and not b.line_move_with_bet]
        sn = _segment_stats(neutral)
        lines.append(
            f"  {'':<12} | neutral     | {sn['n']:>4} | {_win_pct(sn['w'], sn['n']):>6} | "
            f"no_lm={len(no_lm)}"
        )
    lines.append("")

    # C — hot offense
    lines.extend([
        "C. WIN RATE WHEN OFFENSE IS HOT AGAINST OUR BET",
        "  Signal       | Hot off vs us |   N | Win%   | No-hot Win%",
        "  " + "-" * 58,
    ])
    for sig in ("UNDER", "OWM", "AWAY_DOG_RL", "ALL"):
        pool = graded if sig == "ALL" else [b for b in graded if b.signal_type == sig]
        hot = [b for b in pool if b.offense_stacked_against and b.trs_coverage]
        not_hot = [b for b in pool if not b.offense_stacked_against and b.trs_coverage]
        sh = _segment_stats(hot)
        sn = _segment_stats(not_hot)
        lines.append(
            f"  {sig:<12} | yes           | {sh['n']:>4} | {_win_pct(sh['w'], sh['n']):>6} | "
            f"no={_win_pct(sn['w'], sn['n'])} (n={sn['n']})"
        )
    lines.append("")

    # D — SP on losses
    lines.extend([
        "D. SP DELIVERY RATE ON SIGNAL LOSSES",
        "  Signal       | SP blew up | losses | % of sig losses",
        "  " + "-" * 58,
    ])
    for sig in ("UNDER", "OWM", "AWAY_DOG_RL", "ALL"):
        pool = losses if sig == "ALL" else [b for b in losses if b.signal_type == sig]
        if not pool:
            continue
        blew = [b for b in pool if b.sp_blew_up]
        pct = 100.0 * len(blew) / len(pool) if pool else 0
        lines.append(
            f"  {sig:<12} | yes        | {len(blew):>6} | {pct:>5.1f}% of {len(pool)} losses"
        )
    lines.append("")

    # E — contrarian composite
    composite = [b for b in graded if b.factor_count_against >= 2]
    clean = [b for b in graded if b.factor_count_against == 0]
    sc = _segment_stats(composite)
    sc0 = _segment_stats(clean)
    gate_save = -sc["pnl"]
    lines.extend([
        "E. CONTRARIAN COMPOSITE (2+ factors against)",
        f"  N bets with 2+ factors against: {sc['n']}",
        f"  Win rate: {_win_pct(sc['w'], sc['n'])}  vs overall: {overall_wr*100:.1f}%",
        f"  P&L on composite bets: {sc['pnl']:+.2f}u",
        f"  Estimated P&L improvement if gated out: {gate_save:+.2f}u",
        "",
        "F. CLEAN SIGNAL (0 factors against)",
        f"  N bets with 0 factors against: {sc0['n']}",
        f"  Win rate: {_win_pct(sc0['w'], sc0['n'])}  vs overall: {overall_wr*100:.1f}%",
        f"  P&L on clean bets: {sc0['pnl']:+.2f}u",
        "",
    ])

    # Individual factor breakdown
    lines.extend([
        "INDIVIDUAL FACTOR WIN RATES (graded bets with coverage)",
        "  " + "-" * 58,
    ])
    factor_names = [
        ("offense_stacked_against", lambda b: b.offense_stacked_against and b.trs_coverage),
        ("line_move_against_bet", lambda b: b.line_move_against_bet and b.lm_coverage),
        ("steam_move", lambda b: b.steam_move and b.lm_coverage),
        ("reverse_line_move", lambda b: b.reverse_line_move and b.lm_coverage),
        ("public_same_side", lambda b: b.public_same_side and b.lm_coverage),
        ("team_hot_streak_against", lambda b: b.team_hot_streak_against),
        ("sp_blew_up", lambda b: b.sp_blew_up and b.sp_coverage),
    ]
    hypotheses: list[str] = []
    for fname, pred in factor_names:
        with_f = [b for b in graded if pred(b)]
        without_f = [b for b in graded if not pred(b)]
        sw = _segment_stats(with_f)
        sn = _segment_stats(without_f)
        lines.append(
            f"  {fname:<28} yes={_win_pct(sw['w'], sw['n'])} (n={sw['n']})  "
            f"no={_win_pct(sn['w'], sn['n'])} (n={sn['n']})"
        )
        hypotheses.append(_hypothesis_line(fname, sw["win_pct"], sw["n"], sn["win_pct"], sn["n"]))

    lines.extend([
        "",
        "DETAIL — LOSSES ONLY (most recent 15)",
        "  " + "-" * 58,
    ])
    for b in sorted(losses, key=lambda x: x.game_date, reverse=True)[:15]:
        lines.append(
            f"  {b.game_date} {b.away_abbr}@{b.home_abbr} {b.signal_type:<12} "
            f"vs={b.factor_count_against} for={b.factor_count_for} "
            f"off={int(b.offense_stacked_against)} lm={int(b.line_move_against_bet)} "
            f"sp={int(b.sp_blew_up)} pnl={b.pnl_units:+.2f}u"
        )

    lines.extend([
        "",
        "HYPOTHESIS STATUS:",
    ])
    lines.extend([f"  {h}" for h in hypotheses])

    # Contrarian candidate
    best_diff = 0.0
    best_combo = None
    for fname, pred in factor_names:
        with_f = [b for b in graded if pred(b)]
        without_f = [b for b in graded if not pred(b)]
        if len(with_f) >= 10 and len(without_f) >= 10:
            diff = (_segment_stats(without_f)["win_pct"] - _segment_stats(with_f)["win_pct"]) * 100
            if diff > best_diff:
                best_diff = diff
                best_combo = fname

    comp_diff = (sc0["win_pct"] - sc["win_pct"]) * 100 if sc["n"] >= 5 and sc0["n"] >= 5 else 0
    lines.append("")
    lines.append("CONTRARIAN SIGNAL CANDIDATE:")
    if comp_diff >= 15 and sc["n"] >= 10:
        lines.append(
            f"  Recommend investigating 2+ factor composite as gate candidate "
            f"(clean win rate {sc0['win_pct']*100:.1f}% vs {sc['win_pct']*100:.1f}% composite, "
            f"{comp_diff:+.1f}pp, N={sc0['n']}/{sc['n']})"
        )
    elif best_combo and best_diff >= 15:
        lines.append(
            f"  Recommend investigating [{best_combo}] as gate candidate "
            f"(win rate difference {best_diff:+.1f}pp, N>=10)"
        )
    else:
        lines.append(
            "  No contrarian signal found at current sample size — revisit at N=300+"
        )
    lines.append("========================================================")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="2026 signal loss factor audit")
    parser.add_argument("--db", default=None)
    args = parser.parse_args()

    con = db_connect(str(args.db or get_db_path()))
    try:
        raw = _fetch_bets(con)
        bets = [enrich_bet(con, r) for r in raw]
        report = build_report(bets)
    finally:
        con.close()

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(report, encoding="utf-8")
    try:
        print(report)
    except UnicodeEncodeError:
        print(report.encode("ascii", errors="replace").decode("ascii"))
    print(f"[loss_audit_2026] Report saved to {OUT_PATH}")
    print(f"[loss_audit_2026] Factor rows: {len(bets)}")


if __name__ == "__main__":
    main()
