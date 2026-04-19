#!/usr/bin/env python3
"""
daily_results_report.py
═══════════════════════
Full-slate results report for any completed game date.

Shows every regular-season game played on the target date with:
  · Final score and winner
  · Closing moneyline, run line, and total (all bookmakers collapsed to one)
  · P&L for EVERY available bet if you had bet $100 flat on it
  · Model signal picks graded (same signals as generate_daily_brief.py)
  · Day summary: over/under rate, best/worst individual bets, signal P&L

USAGE
─────
    python daily_results_report.py                   # yesterday
    python daily_results_report.py --date 2026-04-15 # specific date
    python daily_results_report.py --season 2025     # full season summary
    python daily_results_report.py --csv             # also write CSV

REQUIREMENTS
────────────
    mlb_stats.db in the same folder as this script.
    Closing-line odds in game_odds (run load_odds.py --pregame first).
    Scores loaded (run load_mlb_stats.py first).
"""

# CHANGE LOG (latest first)
# -------------------------
# 2026-04-13 22:15 ET  Default DB from get_db_path(); repo root on sys.path for core.* imports.
# 2026-04-13 16:24 ET  Refactor: route sqlite3.connect() calls through core.db.connection.connect().

import argparse
import csv
import os
import sqlite3
import sys
from datetime import date, timedelta

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from core.db.connection import connect as db_connect, get_db_path

# ── DB location ──────────────────────────────────────────────────────────────
DEFAULT_DB = get_db_path()


def _reconfigure_stdio_utf8() -> None:
    """Avoid UnicodeEncodeError on Windows (cp1252) when printing box-drawing / icons."""
    for stream in (sys.stdout, sys.stderr):
        try:
            if hasattr(stream, "reconfigure"):
                stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

# ── Signal thresholds (mirror generate_daily_brief.py exactly) ───────────────
WIND_OUT_MIN_MPH    = 10
WIND_IN_MIN_MPH     = 10
WIND_OUT_MVB_MPH    = 15
HOME_FAV_MV_F_LOW   = -130
HOME_FAV_MV_F_HIGH  = -160
S1_PRICE_LOW        = -105
S1_PRICE_HIGH       = -170
DOG_IMPL_LOW        = 0.35
DOG_IMPL_HIGH       = 0.42
STREAK_THRESHOLD    = 5
S1_STANDALONE_MIN   = 6
H3B_MIN_PARK_FACTOR = 98
H3B_LATE_SEASON_MONTHS = {8, 9}

H3B_PARK_WHITELIST = {
    "Wrigley Field",
    "Coors Field",
    "Kauffman Stadium",
    "Globe Life Field",
    "Progressive Field",
    "PNC Park",
    "Comerica Park",
    "American Family Field",
    "Truist Park",
    "Great American Ball Park",
    "Fenway Park",
}


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════════════════════

def get_connection(db_path: str) -> sqlite3.Connection:
    if not os.path.exists(db_path):
        print(f"\n  ✗  Database not found: {db_path}")
        print("     Run from your mlb_stats folder.")
        sys.exit(1)
    con = db_connect(db_path)
    con.row_factory = sqlite3.Row
    return con


def load_completed_games(con: sqlite3.Connection, game_date: str) -> list:
    """Load all completed regular-season games for game_date with full context."""
    rows = con.execute("""
        SELECT
            g.game_pk,
            g.game_date_et AS game_date,
            g.home_score,
            g.away_score,
            g.wind_mph,
            g.wind_direction,
            g.temp_f,
            g.sky_condition,
            g.game_start_utc,

            th.team_id      AS home_team_id,
            th.abbreviation AS home_abbr,
            th.name         AS home_name,
            ta.team_id      AS away_team_id,
            ta.abbreviation AS away_abbr,
            ta.name         AS away_name,

            v.name          AS venue_name,
            v.wind_effect,
            v.wind_note,
            v.roof_type,
            v.park_factor_runs,
            v.orientation_hp,

            ml.home_ml,
            ml.away_ml,
            ml.bookmaker    AS ml_bookmaker,
            tot.total_line,
            tot.over_odds,
            tot.under_odds,
            rl.home_rl_line AS home_rl,
            rl.away_rl_line AS away_rl,
            rl.home_rl_odds,
            rl.away_rl_odds

        FROM   games g
        JOIN   teams  th     ON th.team_id  = g.home_team_id
        JOIN   teams  ta     ON ta.team_id  = g.away_team_id
        LEFT JOIN venues v   ON v.venue_id  = g.venue_id
        LEFT JOIN v_closing_game_odds ml
               ON ml.game_pk    = g.game_pk
              AND ml.market_type = 'moneyline'
        LEFT JOIN v_closing_game_odds tot
               ON tot.game_pk   = g.game_pk
              AND tot.market_type = 'total'
        LEFT JOIN v_closing_game_odds rl
               ON rl.game_pk    = g.game_pk
              AND rl.market_type = 'runline'
        WHERE  g.game_date_et = ?
          AND  g.game_type = 'R'
          AND  g.status    = 'Final'
        ORDER  BY g.game_start_utc, g.game_pk
    """, (game_date,)).fetchall()
    return [dict(r) for r in rows]


def load_streaks_for_date(con: sqlite3.Connection, game_date: str, team_ids: list) -> dict:
    """Rolling home win streak for each team entering game_date."""
    if not team_ids:
        return {}
    placeholders = ",".join("?" * len(team_ids))
    rows = con.execute(f"""
        SELECT
            team_id,
            SUM(CASE WHEN won THEN 1 ELSE -1 END) AS streak
        FROM (
            SELECT
                home_team_id AS team_id,
                home_score > away_score AS won,
                game_date_et AS game_date
            FROM games
            WHERE game_type = 'R'
              AND status = 'Final'
              AND game_date_et < ?
              AND home_team_id IN ({placeholders})
            ORDER BY game_date_et DESC
        )
        GROUP BY team_id
    """, [game_date] + team_ids).fetchall()

    streaks = {}
    for r in rows:
        streaks[r["team_id"]] = r["streak"] or 0
    return streaks


# ══════════════════════════════════════════════════════════════════════════════
# ODDS / BET MATH
# ══════════════════════════════════════════════════════════════════════════════

def american_to_implied(odds: int) -> float | None:
    if odds is None:
        return None
    if odds > 0:
        return 100 / (odds + 100)
    return abs(odds) / (abs(odds) + 100)


def pnl_ml(odds: int, won: bool, stake: float = 100.0) -> float:
    """P&L for a moneyline bet. Returns net profit/loss on $100 stake."""
    if odds is None:
        return 0.0
    if won:
        if odds > 0:
            return round(stake * odds / 100, 2)
        else:
            return round(stake * 100 / abs(odds), 2)
    return -stake


def pnl_total(odds: int | None, won: bool, stake: float = 100.0) -> float:
    """P&L for a totals bet (over or under)."""
    effective_odds = odds if odds is not None else -110
    return pnl_ml(effective_odds, won, stake)


def fmt_odds(odds: int | None) -> str:
    if odds is None:
        return "N/A"
    return f"+{odds}" if odds > 0 else str(odds)


def fmt_pnl(pnl: float) -> str:
    if pnl > 0:
        return f"+${pnl:.2f}"
    elif pnl < 0:
        return f"-${abs(pnl):.2f}"
    return "PUSH"


def wind_direction_label(raw: str) -> str:
    if not raw:
        return ""
    r = raw.upper()
    if "IN"  in r or "HOME" in r:
        return "IN"
    if "OUT" in r or "AWAY" in r or "CENTER" in r:
        return "OUT"
    return "CROSS"


# ══════════════════════════════════════════════════════════════════════════════
# BET GRADING — all available bets for one game
# ══════════════════════════════════════════════════════════════════════════════

def grade_all_bets(g: dict) -> list:
    """
    Return a list of dicts, one per available bet, each with:
      bet_label, odds, result (W/L/P/N/A), pnl
    """
    bets = []
    hs = g["home_score"]
    as_ = g["away_score"]
    home = g["home_abbr"]
    away = g["away_abbr"]

    if hs is None or as_ is None:
        return bets

    runs = hs + as_
    home_won = hs > as_

    # ── Moneyline ─────────────────────────────────────────────────────────
    if g["home_ml"] is not None:
        bets.append({
            "market":    "ML",
            "bet_label": f"{home} ML (home)",
            "odds":      g["home_ml"],
            "result":    "W" if home_won else "L",
            "pnl":       pnl_ml(g["home_ml"], home_won),
        })
    if g["away_ml"] is not None:
        bets.append({
            "market":    "ML",
            "bet_label": f"{away} ML (away)",
            "odds":      g["away_ml"],
            "result":    "W" if not home_won else "L",
            "pnl":       pnl_ml(g["away_ml"], not home_won),
        })

    # ── Run line ─────────────────────────────────────────────────────────
    if g["home_rl"] is not None and g["home_rl_odds"] is not None:
        rl = g["home_rl"]          # e.g. -1.5
        margin = hs - as_          # positive = home wins by margin
        home_rl_won = margin > abs(rl) if rl < 0 else margin >= rl
        away_rl_won = margin < rl  if rl < 0 else margin <= rl
        push_rl     = not home_rl_won and not away_rl_won

        if push_rl:
            bets.append({
                "market": "RL", "bet_label": f"{home} RL {fmt_odds(int(rl))} (home)",
                "odds": g["home_rl_odds"], "result": "P", "pnl": 0.0,
            })
            bets.append({
                "market": "RL", "bet_label": f"{away} RL +{abs(int(rl))} (away)",
                "odds": g["away_rl_odds"], "result": "P", "pnl": 0.0,
            })
        else:
            bets.append({
                "market": "RL", "bet_label": f"{home} RL {fmt_odds(int(rl))} (home)",
                "odds": g["home_rl_odds"], "result": "W" if home_rl_won else "L",
                "pnl": pnl_ml(g["home_rl_odds"], home_rl_won),
            })
            if g["away_rl_odds"] is not None:
                bets.append({
                    "market": "RL", "bet_label": f"{away} RL +{abs(int(rl))} (away)",
                    "odds": g["away_rl_odds"], "result": "W" if away_rl_won else "L",
                    "pnl": pnl_ml(g["away_rl_odds"], away_rl_won),
                })

    # ── Totals ───────────────────────────────────────────────────────────
    if g["total_line"] is not None:
        tot = g["total_line"]
        if runs == tot:
            result_ou = "P"
            over_pnl = under_pnl = 0.0
        else:
            over_won  = runs > tot
            result_ou = "N/A"  # placeholder
            over_pnl  = pnl_total(g["over_odds"],  runs > tot)
            under_pnl = pnl_total(g["under_odds"], runs < tot)

        bets.append({
            "market": "TOTAL", "bet_label": f"OVER {tot}  ({runs} runs)",
            "odds": g["over_odds"] or -110,
            "result": "P" if runs == tot else ("W" if runs > tot else "L"),
            "pnl": 0.0 if runs == tot else pnl_total(g["over_odds"], runs > tot),
        })
        bets.append({
            "market": "TOTAL", "bet_label": f"UNDER {tot}  ({runs} runs)",
            "odds": g["under_odds"] or -110,
            "result": "P" if runs == tot else ("W" if runs < tot else "L"),
            "pnl": 0.0 if runs == tot else pnl_total(g["under_odds"], runs < tot),
        })

    return bets


# ══════════════════════════════════════════════════════════════════════════════
# SIGNAL EVALUATION (mirrors generate_daily_brief.py exactly)
# ══════════════════════════════════════════════════════════════════════════════

def evaluate_signals(g: dict, streaks: dict) -> list:
    """
    Return list of signal picks that fired for this game.
    Each pick: {signal, bet_label, market, odds, bet_side}
    """
    picks = []
    home_ml   = g.get("home_ml")
    away_ml   = g.get("away_ml")
    total     = g.get("total_line")
    wind_mph  = g.get("wind_mph") or 0
    wind_dir  = wind_direction_label(g.get("wind_direction") or "")
    wind_eff  = (g.get("wind_effect") or "HIGH").upper()
    home_id   = g.get("home_team_id")
    home_abbr = g.get("home_abbr", "HOME")
    away_abbr = g.get("away_abbr", "AWAY")
    venue     = g.get("venue_name") or ""

    home_impl  = american_to_implied(home_ml)
    wind_ok    = wind_eff == "HIGH"
    home_streak = streaks.get(home_id, 0)

    try:
        month = int((g.get("game_date") or "")[:10].split("-")[1])
    except (ValueError, IndexError):
        month = 0

    park_factor = g.get("park_factor_runs") or 100

    # S1+H2 (priority 1)
    s1h2 = (home_streak >= STREAK_THRESHOLD and home_ml is not None
            and HOME_FAV_MV_F_HIGH <= home_ml <= HOME_FAV_MV_F_LOW)
    if s1h2:
        picks.append({
            "signal": "S1+H2", "priority": 1,
            "bet_label": f"{away_abbr} ML", "market": "ML",
            "odds": away_ml, "bet_side": "away_ml",
        })

    # MV-F (priority 2)
    if (wind_ok and wind_dir == "IN" and wind_mph >= WIND_IN_MIN_MPH
            and home_ml is not None
            and HOME_FAV_MV_F_HIGH <= home_ml <= HOME_FAV_MV_F_LOW
            and not s1h2):
        picks.append({
            "signal": "MV-F", "priority": 2,
            "bet_label": f"{away_abbr} ML", "market": "ML",
            "odds": away_ml, "bet_side": "away_ml",
        })

    # MV-B (priority 3)
    if (wind_ok and wind_dir == "OUT" and wind_mph >= WIND_OUT_MVB_MPH
            and home_impl is not None
            and DOG_IMPL_LOW <= home_impl <= DOG_IMPL_HIGH
            and total is not None):
        picks.append({
            "signal": "MV-B", "priority": 3,
            "bet_label": f"OVER {total}", "market": "TOTAL",
            "odds": g.get("over_odds") or -110, "bet_side": "over",
        })

    # S1 standalone (priority 4)
    s1_price_ok = (home_ml is not None
                   and S1_PRICE_HIGH <= home_ml <= S1_PRICE_LOW)
    if (home_streak >= S1_STANDALONE_MIN and not s1h2 and s1_price_ok):
        picks.append({
            "signal": "S1", "priority": 4,
            "bet_label": f"{away_abbr} ML", "market": "ML",
            "odds": away_ml, "bet_side": "away_ml",
        })

    # H3b (priority 5)
    h3b_ok = (wind_ok and wind_dir == "OUT" and wind_mph >= WIND_OUT_MIN_MPH
              and total is not None
              and venue in H3B_PARK_WHITELIST
              and park_factor >= H3B_MIN_PARK_FACTOR)
    if h3b_ok:
        # If MV-B already fired (TOTAL), H3b reinforces — don't duplicate
        if not any(p["signal"] == "MV-B" for p in picks):
            picks.append({
                "signal": "H3b", "priority": 5,
                "bet_label": f"OVER {total}", "market": "TOTAL",
                "odds": g.get("over_odds") or -110, "bet_side": "over",
            })

    return sorted(picks, key=lambda p: p["priority"])


def grade_signal_pick(pick: dict, g: dict) -> dict:
    """Grade a signal pick against actual result."""
    hs  = g["home_score"]
    as_ = g["away_score"]
    if hs is None or as_ is None:
        return {**pick, "result": "N/A", "pnl": 0.0}

    runs = hs + as_
    side = pick["bet_side"]

    if side == "away_ml":
        won = as_ > hs
        pnl = pnl_ml(pick["odds"], won)
        result = "W" if won else "L"
    elif side == "home_ml":
        won = hs > as_
        pnl = pnl_ml(pick["odds"], won)
        result = "W" if won else "L"
    elif side == "over":
        tot = g.get("total_line")
        if tot is None:
            return {**pick, "result": "N/A", "pnl": 0.0}
        if runs == tot:
            result, pnl = "P", 0.0
        else:
            won = runs > tot
            result = "W" if won else "L"
            pnl = pnl_total(pick["odds"], won)
    elif side == "under":
        tot = g.get("total_line")
        if tot is None:
            return {**pick, "result": "N/A", "pnl": 0.0}
        if runs == tot:
            result, pnl = "P", 0.0
        else:
            won = runs < tot
            result = "W" if won else "L"
            pnl = pnl_total(pick["odds"], won)
    else:
        result, pnl = "N/A", 0.0

    return {**pick, "result": result, "pnl": pnl}


# ══════════════════════════════════════════════════════════════════════════════
# REPORT BUILDERS
# ══════════════════════════════════════════════════════════════════════════════

RESULT_ICON = {"W": "✓ WIN ", "L": "✗ LOSS", "P": "— PUSH", "N/A": "  N/A "}


def format_game_block(g: dict, all_bets: list, signal_picks: list) -> str:
    lines = []
    hs     = g["home_score"]
    as_    = g["away_score"]
    home   = g["home_abbr"]
    away   = g["away_abbr"]
    runs   = (hs + as_) if hs is not None else None
    winner = home if (hs or 0) > (as_ or 0) else away

    # Game header
    lines.append(f"\n  {'─'*64}")
    lines.append(f"  {away}  @  {home}  │  "
                 f"{'Final: ' + away + ' ' + str(as_) + '  –  ' + home + ' ' + str(hs) + '  (' + winner + ' wins)' if hs is not None else 'Score not available'}")

    # Conditions
    wind_str = ""
    if g.get("wind_mph"):
        wind_str = f"  │  Wind {g['wind_mph']} mph {g.get('wind_direction','')}"
    temp_str = f"  {g['temp_f']}°F" if g.get("temp_f") else ""
    if g.get("venue_name"):
        lines.append(f"  {g['venue_name']}{temp_str}{wind_str}")

    # Odds line
    if g.get("home_ml"):
        rl_str = (f"  │  RL {fmt_odds(int(g['home_rl']))} / +{abs(int(g['away_rl']))}"
                  if g.get("home_rl") else "")
        ou_str = (f"  │  O/U {g['total_line']}" if g.get("total_line") else "")
        lines.append(f"  ML: {home} {fmt_odds(g['home_ml'])} / {away} {fmt_odds(g['away_ml'])}"
                     f"{rl_str}{ou_str}")

    # All bets graded
    if all_bets:
        lines.append(f"")
        lines.append(f"  {'BET':<32} {'ODDS':>6}  {'RESULT':<8}  {'P&L':>8}")
        lines.append(f"  {'─'*32}  {'─'*6}  {'─'*8}  {'─'*8}")
        for b in all_bets:
            icon = RESULT_ICON.get(b["result"], "      ")
            lines.append(
                f"  {b['bet_label']:<32} {fmt_odds(b['odds']):>6}  "
                f"{icon}  {fmt_pnl(b['pnl']):>8}"
            )

    # Signal picks highlighted
    if signal_picks:
        lines.append(f"")
        lines.append(f"  ▶ MODEL SIGNALS:")
        for sp in signal_picks:
            icon = RESULT_ICON.get(sp["result"], "      ")
            lines.append(
                f"    [{sp['signal']:<5}]  {sp['bet_label']:<24} "
                f"{fmt_odds(sp['odds']):>6}  {icon}  {fmt_pnl(sp['pnl']):>8}"
            )

    return "\n".join(lines)


def build_day_report(game_date: str, games: list, streaks: dict) -> tuple[str, list]:
    """
    Build the full day report string and return CSV rows for export.
    Returns (report_text, csv_rows)
    """
    lines = []
    csv_rows = []

    lines.append(f"\n{'═'*66}")
    lines.append(f"  MLB SCOUT — DAILY RESULTS REPORT")
    lines.append(f"  {game_date}  ({len(games)} games)")
    lines.append(f"{'═'*66}")

    day_bets    = []   # all individual bets across the day
    day_signals = []   # all signal picks across the day

    for g in games:
        all_bets    = grade_all_bets(g)
        sig_picks   = evaluate_signals(g, streaks)
        graded_sigs = [grade_signal_pick(sp, g) for sp in sig_picks]

        block = format_game_block(g, all_bets, graded_sigs)
        lines.append(block)

        day_bets.extend(all_bets)
        day_signals.extend(graded_sigs)

        # CSV rows
        for b in all_bets:
            csv_rows.append({
                "date":       game_date,
                "matchup":    f"{g['away_abbr']}@{g['home_abbr']}",
                "final":      (f"{g['away_abbr']} {g['away_score']}–{g['home_abbr']} {g['home_score']}"
                               if g["home_score"] is not None else "N/A"),
                "market":     b["market"],
                "bet":        b["bet_label"],
                "odds":       b["odds"],
                "result":     b["result"],
                "pnl_100":    b["pnl"],
                "signal":     "",
            })
        for sp in graded_sigs:
            csv_rows.append({
                "date":       game_date,
                "matchup":    f"{g['away_abbr']}@{g['home_abbr']}",
                "final":      (f"{g['away_abbr']} {g['away_score']}–{g['home_abbr']} {g['home_score']}"
                               if g["home_score"] is not None else "N/A"),
                "market":     sp["market"],
                "bet":        sp["bet_label"],
                "odds":       sp["odds"],
                "result":     sp["result"],
                "pnl_100":    sp["pnl"],
                "signal":     sp["signal"],
            })

    # ── DAY SUMMARY ───────────────────────────────────────────────────────
    lines.append(f"\n\n{'═'*66}")
    lines.append(f"  DAY SUMMARY  —  {game_date}")
    lines.append(f"{'═'*66}")

    # O/U summary
    ou_bets = [b for b in day_bets if b["market"] == "TOTAL" and "OVER" in b["bet_label"]]
    overs   = sum(1 for b in ou_bets if b["result"] == "W")
    unders  = sum(1 for b in ou_bets if b["result"] == "L")
    pushes  = sum(1 for b in ou_bets if b["result"] == "P")
    total_games = len(games)
    if ou_bets:
        lines.append(f"\n  O/U:  {overs} Over / {unders} Under"
                     + (f" / {pushes} Push" if pushes else "")
                     + f"  ({overs/len(ou_bets):.0%} over rate on {len(ou_bets)} games)")

    # Best and worst individual bets of the day
    graded = [b for b in day_bets if b["result"] in ("W", "L")]
    if graded:
        best  = max(graded, key=lambda b: b["pnl"])
        worst = min(graded, key=lambda b: b["pnl"])
        lines.append(f"  Best bet:   {best['bet_label']:<34} {fmt_pnl(best['pnl'])}")
        lines.append(f"  Worst bet:  {worst['bet_label']:<34} {fmt_pnl(worst['pnl'])}")

    # Signal P&L summary
    lines.append(f"\n  SIGNAL P&L  (flat $100 per pick):")
    lines.append(f"  {'─'*50}")

    by_signal: dict = {}
    for sp in day_signals:
        sig = sp["signal"]
        if sig not in by_signal:
            by_signal[sig] = {"W": 0, "L": 0, "P": 0, "pnl": 0.0}
        r = sp["result"]
        if r in by_signal[sig]:
            by_signal[sig][r] += 1
        by_signal[sig]["pnl"] += sp["pnl"]

    if by_signal:
        for sig, s in sorted(by_signal.items()):
            total = s["W"] + s["L"] + s["P"]
            lines.append(
                f"  {sig:<8}  {s['W']}W {s['L']}L {s['P']}P  "
                f"P&L: {fmt_pnl(s['pnl'])}"
            )
        total_sig_pnl = sum(s["pnl"] for s in by_signal.values())
        total_sig_w   = sum(s["W"]   for s in by_signal.values())
        total_sig_l   = sum(s["L"]   for s in by_signal.values())
        lines.append(f"  {'─'*50}")
        lines.append(f"  {'ALL':<8}  {total_sig_w}W {total_sig_l}L   "
                     f"P&L: {fmt_pnl(total_sig_pnl)}")
    else:
        lines.append("  No model signals fired.")

    # Home/away ML summary
    home_ml_bets = [b for b in day_bets
                    if b["market"] == "ML" and "(home)" in b["bet_label"]
                    and b["result"] in ("W", "L")]
    away_ml_bets = [b for b in day_bets
                    if b["market"] == "ML" and "(away)" in b["bet_label"]
                    and b["result"] in ("W", "L")]

    if home_ml_bets:
        home_wins = sum(1 for b in home_ml_bets if b["result"] == "W")
        lines.append(f"\n  Home teams:  {home_wins}/{len(home_ml_bets)} wins "
                     f"({home_wins/len(home_ml_bets):.0%} win rate)")
    if away_ml_bets:
        away_wins = sum(1 for b in away_ml_bets if b["result"] == "W")
        lines.append(f"  Away teams:  {away_wins}/{len(away_ml_bets)} wins "
                     f"({away_wins/len(away_ml_bets):.0%} win rate)")

    lines.append(f"\n{'═'*66}\n")
    return "\n".join(lines), csv_rows


def build_season_summary(con: sqlite3.Connection, season: int) -> str:
    """Aggregate signal P&L for a full season — no per-game detail."""
    lines = []
    lines.append(f"\n{'═'*66}")
    lines.append(f"  MLB SCOUT — SEASON SUMMARY  {season}")
    lines.append(f"{'═'*66}")

    # Get all completed game dates for the season
    dates = [r[0] for r in con.execute("""
        SELECT DISTINCT game_date_et AS game_date FROM games
        WHERE season = ? AND game_type = 'R' AND status = 'Final'
        ORDER BY game_date_et
    """, (season,)).fetchall()]

    if not dates:
        lines.append(f"\n  No completed games found for season {season}.")
        return "\n".join(lines)

    lines.append(f"\n  Dates processed: {dates[0]} → {dates[-1]}  ({len(dates)} game days)")

    sig_totals: dict = {}
    total_games = 0
    total_overs = 0
    total_ou_games = 0

    for gd in dates:
        games = load_completed_games(con, gd)
        if not games:
            continue
        total_games += len(games)

        team_ids = list({g["home_team_id"] for g in games} | {g["away_team_id"] for g in games})
        streaks = load_streaks_for_date(con, gd, team_ids)

        for g in games:
            # O/U tracking
            hs = g["home_score"]; as_ = g["away_score"]
            if hs is not None and g["total_line"]:
                total_ou_games += 1
                if hs + as_ > g["total_line"]:
                    total_overs += 1

            # Signal grading
            picks = evaluate_signals(g, streaks)
            for sp in picks:
                graded = grade_signal_pick(sp, g)
                sig = graded["signal"]
                if sig not in sig_totals:
                    sig_totals[sig] = {"fires": 0, "W": 0, "L": 0, "P": 0, "pnl": 0.0}
                sig_totals[sig]["fires"] += 1
                r = graded["result"]
                if r in sig_totals[sig]:
                    sig_totals[sig][r] += 1
                sig_totals[sig]["pnl"] += graded["pnl"]

    # Print season signal table
    lines.append(f"\n  Total games: {total_games}")
    if total_ou_games:
        lines.append(f"  O/U rate:    {total_overs/total_ou_games:.1%}  ({total_overs}/{total_ou_games})")

    lines.append(f"\n  SIGNAL PERFORMANCE (flat $100/game stake):")
    lines.append(f"  {'─'*60}")
    lines.append(f"  {'Signal':<8} {'Fires':>6} {'W':>5} {'L':>5} {'P':>5}  "
                 f"{'Hit%':>6}  {'P&L':>10}  {'ROI':>7}")
    lines.append(f"  {'─'*60}")

    total_fires = total_w = total_l = total_p = 0
    total_pnl = 0.0

    for sig, s in sorted(sig_totals.items(), key=lambda x: -abs(x[1]["pnl"])):
        fires = s["fires"]; w = s["W"]; l = s["L"]; p = s["P"]
        pnl   = s["pnl"]
        hit   = w / (w + l) if (w + l) > 0 else 0.0
        roi   = pnl / (fires * 100) * 100 if fires > 0 else 0.0
        lines.append(
            f"  {sig:<8} {fires:>6} {w:>5} {l:>5} {p:>5}  "
            f"{hit:>5.1%}  {fmt_pnl(pnl):>10}  {roi:>+.1f}%"
        )
        total_fires += fires; total_w += w; total_l += l; total_p += p
        total_pnl   += pnl

    if sig_totals:
        lines.append(f"  {'─'*60}")
        total_hit = total_w / (total_w + total_l) if (total_w + total_l) > 0 else 0.0
        total_roi = total_pnl / (total_fires * 100) * 100 if total_fires > 0 else 0.0
        lines.append(
            f"  {'ALL':<8} {total_fires:>6} {total_w:>5} {total_l:>5} {total_p:>5}  "
            f"{total_hit:>5.1%}  {fmt_pnl(total_pnl):>10}  {total_roi:>+.1f}%"
        )
    else:
        lines.append("  No signal fires found for this season.")

    lines.append(f"\n{'═'*66}\n")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(
        description="Daily or season results report with all bet outcomes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  python daily_results_report.py
      Full report for yesterday — every game, every bet, signal picks graded.

  python daily_results_report.py --date 2026-04-15
      Same report for a specific date.

  python daily_results_report.py --season 2025
      Season-level signal P&L summary for 2025. No per-game detail.

  python daily_results_report.py --date 2026-04-15 --csv
      Write a CSV alongside the text report (reports/YYYY-MM-DD_results.csv).
"""
    )
    p.add_argument("--date",
                   help="Target date YYYY-MM-DD (default: yesterday)")
    p.add_argument("--season", type=int,
                   help="Run season-level summary instead of a single day")
    p.add_argument("--csv", action="store_true",
                   help="Write CSV output to reports/ folder")
    p.add_argument("--db", default=DEFAULT_DB,
                   help=f"Database path (default: {DEFAULT_DB})")
    args = p.parse_args()
    _reconfigure_stdio_utf8()

    con = get_connection(args.db)

    # ── Season mode ───────────────────────────────────────────────────────
    if args.season:
        report = build_season_summary(con, args.season)
        print(report)
        con.close()
        return

    # ── Single-day mode ───────────────────────────────────────────────────
    target = args.date or (date.today() - timedelta(days=1)).isoformat()

    # Validate date
    try:
        date.fromisoformat(target)
    except ValueError:
        print(f"✗  Invalid date: '{target}'. Use YYYY-MM-DD.")
        sys.exit(1)

    games = load_completed_games(con, target)

    if not games:
        print(f"\n  No completed regular-season games found for {target}.")
        print("  Ensure load_mlb_stats.py ran and games are marked Final.")
        con.close()
        sys.exit(0)

    team_ids = list({g["home_team_id"] for g in games} |
                    {g["away_team_id"]  for g in games})
    streaks  = load_streaks_for_date(con, target, team_ids)

    report_text, csv_rows = build_day_report(target, games, streaks)
    print(report_text)

    # ── Save txt report ───────────────────────────────────────────────────
    reports_dir = os.path.join(os.path.dirname(os.path.abspath(args.db)), "reports")
    os.makedirs(reports_dir, exist_ok=True)

    txt_path = os.path.join(reports_dir, f"{target}_results.txt")
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(report_text)
    print(f"  ✓ Report saved: {txt_path}")

    # ── CSV export ────────────────────────────────────────────────────────
    if args.csv and csv_rows:
        csv_path = os.path.join(reports_dir, f"{target}_results.csv")
        fieldnames = ["date", "matchup", "final", "market",
                      "bet", "odds", "result", "pnl_100", "signal"]
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(csv_rows)
        print(f"  ✓ CSV saved:    {csv_path}")

    con.close()


if __name__ == "__main__":
    main()
