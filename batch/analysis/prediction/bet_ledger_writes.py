"""
Write score_today actionable picks into bet_ledger for prior-day grading.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from batch.pipeline.bet_ledger_schema import ensure_bet_ledger_extended
from batch.pipeline.score_game import AWAY_DOG_RL_STAKE

ET = ZoneInfo("America/New_York")

SIGNAL_TO_MARKET = {
    "ML": "moneyline",
    "RL": "spread",
    "UNDER": "total",
    "OWM": "moneyline",
    "AWAY_DOG_RL": "spread",
}

DEFAULT_STAKE = {
    "ML": 1.0,
    "RL": 1.0,
    "UNDER": 1.0,
    "OWM": 1.0,
    "AWAY_DOG_RL": AWAY_DOG_RL_STAKE,
}


def _now_et_label() -> str:
    return datetime.now(tz=ET).strftime("%Y-%m-%d %H:%M ET")


def _norm_bet(bet: str) -> str:
    return " ".join(str(bet or "").upper().split())


def _parse_odds(val: Any) -> int | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _total_line_at_bet(market_type: str, bet: str) -> float | None:
    if market_type != "total":
        return None
    parts = str(bet or "").strip().upper().split()
    if len(parts) >= 2:
        try:
            return float(parts[1].strip("()"))
        except ValueError:
            return None
    return None


def collect_score_today_picks(scored: pd.DataFrame, score_date: str) -> list[dict[str, Any]]:
    """Build bet_ledger row payloads for all actionable score_today signals."""
    picks: list[dict[str, Any]] = []
    if scored.empty:
        return picks

    for _, row in scored.iterrows():
        try:
            game_pk = int(row["game_pk"])
        except (TypeError, ValueError, KeyError):
            continue
        away = str(row.get("away_team") or "").strip()
        home = str(row.get("home_team") or "").strip()
        if not away or not home:
            continue

        def _add(
            signal_type: str,
            *,
            bet: str,
            odds: Any,
            pick_side: str,
            stake: float | None = None,
        ) -> None:
            mt = SIGNAL_TO_MARKET[signal_type]
            st = float(stake if stake is not None else DEFAULT_STAKE[signal_type])
            if st <= 0:
                return
            picks.append(
                {
                    "game_date": score_date,
                    "game_pk": game_pk,
                    "market_type": mt,
                    "signal_type": signal_type,
                    "pick_side": pick_side,
                    "bet": bet,
                    "odds_taken": _parse_odds(odds),
                    "stake_units": st,
                }
            )

        if bool(row.get("actionable")):
            winner = str(row.get("predicted_winner") or "").strip()
            if winner == home:
                odds = row.get("home_ml")
                bet = f"{home} ML"
                pick_side = "home_ml"
            elif winner == away:
                odds = row.get("away_ml")
                bet = f"{away} ML"
                pick_side = "away_ml"
            else:
                odds = row.get("odds_used")
                bet = f"{winner} ML"
                pick_side = "home_ml"
            _add("ML", bet=bet, odds=odds, pick_side=pick_side)

        if bool(row.get("under_signal")) and bool(row.get("both_sp_known")):
            tline = row.get("total_line")
            if pd.notna(tline):
                uo = row.get("under_odds")
                bet = f"UNDER {float(tline):g}"
                _add("UNDER", bet=bet, odds=uo, pick_side="under_total")

        if bool(row.get("rl_signal")) and pd.notna(row.get("favorite_ml")):
            fav = str(row.get("favorite_team") or "").strip()
            ro = row.get("fav_rl_odds")
            bet = f"{fav} -1.5"
            side = str(row.get("favorite_side") or "").strip().lower()
            pick_side = f"{side}_rl" if side in ("home", "away") else "home_rl"
            _add("RL", bet=bet, odds=ro, pick_side=pick_side)

        if bool(row.get("owm_signal")):
            hm = row.get("home_ml")
            _add("OWM", bet=f"{home} ML", odds=hm, pick_side="home_ml")

        if bool(row.get("away_dog_rl_actionable")):
            ro = row.get("away_rl_odds")
            bet = f"{away} +1.5"
            _add(
                "AWAY_DOG_RL",
                bet=bet,
                odds=ro,
                pick_side="away_rl",
                stake=AWAY_DOG_RL_STAKE,
            )

    return picks


def _should_skip_pick(conn: sqlite3.Connection, pick: dict[str, Any]) -> str | None:
    """Return skip reason if an equivalent staked row already exists."""
    gpk = int(pick["game_pk"])
    gd = str(pick["game_date"])
    mt = str(pick["market_type"])
    st = str(pick["signal_type"])
    bet_n = _norm_bet(pick["bet"])

    try:
        rows = conn.execute(
            """
            SELECT id, bet, source, signal_type, stake_units
            FROM bet_ledger
            WHERE game_date = ?
              AND game_pk = ?
              AND stake_units > 0
              AND lower(trim(coalesce(signal_at_time, ''))) != 'avoid'
            """,
            (gd, gpk),
        ).fetchall()
    except sqlite3.OperationalError:
        return None

    for r in rows:
        existing_bet = _norm_bet(r[1])
        if existing_bet == bet_n:
            src = (r[2] or "brief").strip()
            return f"same bet exists ({src})"
        if mt == "moneyline" and existing_bet.endswith(" ML") and bet_n.endswith(" ML"):
            if existing_bet.split()[0] == bet_n.split()[0]:
                src = (r[2] or "brief").strip()
                return f"same ML team exists ({src})"

    try:
        dup = conn.execute(
            """
            SELECT 1 FROM bet_ledger
            WHERE game_date = ?
              AND game_pk = ?
              AND source = 'score_today'
              AND COALESCE(signal_type, '') = ?
            LIMIT 1
            """,
            (gd, gpk, st),
        ).fetchone()
        if dup is not None:
            return "score_today signal_type already logged"
    except sqlite3.OperationalError:
        pass

    if mt == "spread":
        try:
            dup_spread = conn.execute(
                """
                SELECT 1 FROM bet_ledger
                WHERE game_date = ?
                  AND game_pk = ?
                  AND market_type = 'spread'
                  AND stake_units > 0
                  AND lower(trim(coalesce(signal_at_time, ''))) != 'avoid'
                  AND COALESCE(source, 'brief') IN ('brief', 'brief_late')
                  AND COALESCE(signal_type, '') IN ('RL', 'AWAY_DOG_RL')
                  AND upper(trim(bet)) = ?
                LIMIT 1
                """,
                (gd, gpk, bet_n),
            ).fetchone()
            if dup_spread is not None:
                return "brief spread pick already exists"
        except sqlite3.OperationalError:
            pass

    return None


def write_picks_to_bet_ledger(
    conn: sqlite3.Connection,
    picks: list[dict[str, Any]],
    *,
    score_date: str,
) -> dict[str, int]:
    """
    Insert score_today picks into bet_ledger. Never overwrites brief rows.
    Returns counts: written, skipped, errors.
    """
    ensure_bet_ledger_extended(conn)
    placed_at = _now_et_label()
    written = skipped = errors = 0

    for pick in picks:
        reason = _should_skip_pick(conn, pick)
        if reason:
            skipped += 1
            continue
        tlb = _total_line_at_bet(pick["market_type"], pick["bet"])
        try:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO bet_ledger (
                    game_date, game_pk, market_type, bet, odds_taken, stake_units,
                    signal_at_time, session, placed_at, total_line_at_bet, late_signal,
                    model_version, result, pnl_units, source, signal_type, pick_side
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    score_date,
                    int(pick["game_pk"]),
                    pick["market_type"],
                    pick["bet"],
                    pick.get("odds_taken"),
                    float(pick["stake_units"]),
                    f"score_today:{pick['signal_type']}",
                    "score_today",
                    placed_at,
                    tlb,
                    0,
                    "score_today",
                    None,
                    None,
                    "score_today",
                    pick["signal_type"],
                    pick.get("pick_side"),
                ),
            )
            if getattr(cur, "rowcount", 0) == 1:
                written += 1
            else:
                skipped += 1
        except sqlite3.Error:
            errors += 1

    try:
        conn.commit()
    except sqlite3.Error:
        pass

    return {"written": written, "skipped": skipped, "errors": errors}


def verify_bet_ledger_for_date(conn: sqlite3.Connection, score_date: str) -> None:
    """Print all bet_ledger rows for a date (diagnostic)."""
    ensure_bet_ledger_extended(conn)
    rows = conn.execute(
        """
        SELECT
            id, game_pk, market_type, signal_type, pick_side, bet,
            odds_taken, stake_units, source, signal_at_time, result, pnl_units
        FROM bet_ledger
        WHERE game_date = ?
        ORDER BY source, signal_type, game_pk
        """,
        (score_date,),
    ).fetchall()
    print(f"[score_today] bet_ledger rows for {score_date}: {len(rows)}")
    print(
        f"  {'id':>5} {'gpk':>8} {'mkt':<10} {'sig':<12} {'src':<11} "
        f"{'stake':>5} {'odds':>6} {'result':<8} bet"
    )
    for r in rows:
        res = r[10] if r[10] is not None else "—"
        print(
            f"  {int(r[0]):5d} {int(r[1]):8d} {str(r[2] or ''):<10} "
            f"{str(r[3] or ''):<12} {str(r[8] or ''):<11} "
            f"{float(r[7] or 0):5.2f} {str(r[6] or 'n/a'):>6} {str(res):<8} {r[5]}"
        )
