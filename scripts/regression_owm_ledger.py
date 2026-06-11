#!/usr/bin/env python3
"""
OWM vs AWAY_DOG_RL bet_ledger regression suite.

Exercises every classification / stake / write path so co-listed brief signals
(``OWM, Away Dog RL``) cannot again land ML home picks as AWAY_DOG_RL @ 0.10u.

Run:
    python scripts/regression_owm_ledger.py
    python scripts/regression_owm_ledger.py --db path/to/mlb_stats.db

Outputs (under outputs/regression/):
    owm_ledger_regression_<timestamp>.txt   human-readable report
    owm_ledger_regression_<timestamp>.json  machine-readable summary
"""

from __future__ import annotations

import json
import sqlite3
import sys
import traceback
import unittest.mock as mock
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from batch.analysis.prediction.bet_ledger_writes import (
    collect_score_today_picks,
    write_picks_to_bet_ledger,
)
from batch.pipeline.bet_ledger_schema import (
    ensure_bet_ledger_extended,
    repair_owm_ledger_classification,
    run_repairs,
)
from batch.pipeline.generate_daily_brief import (
    _dedupe_ledger_rows_for_prior_display,
    _insert_bet_ledger_from_snapshots,
    _ledger_signal_type_for_row,
    _ledger_stake_units_for_signal_row,
    _scored_game_is_owm_pick,
    _signal_type_from_brief_pick_signal,
    _stake_units_from_brief_pick_row,
    ensure_brief_picks,
    ensure_bet_snapshots,
    materialize_bet_ledger_from_brief_picks,
)
from batch.pipeline.score_game import AWAY_DOG_RL_STAKE, BRIEF_FLAT_STAKE
from core.db.connection import get_db_path

ET = ZoneInfo("America/New_York")
OUT_DIR = ROOT / "outputs" / "regression"


@dataclass
class CaseResult:
    name: str
    passed: bool
    detail: str = ""


@dataclass
class Report:
    started_at: str
    finished_at: str = ""
    cases: list[CaseResult] = field(default_factory=list)
    live_violations: list[dict[str, Any]] = field(default_factory=list)
    path_inventory: list[dict[str, str]] = field(default_factory=list)

    @property
    def passed(self) -> int:
        return sum(1 for c in self.cases if c.passed)

    @property
    def failed(self) -> int:
        return sum(1 for c in self.cases if not c.passed)

    def add(self, name: str, ok: bool, detail: str = "") -> None:
        self.cases.append(CaseResult(name=name, passed=ok, detail=detail))


def _ts() -> str:
    return datetime.now(tz=ET).strftime("%Y%m%d_%H%M%S_ET")


def _mem_conn() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    ensure_bet_ledger_extended(con)
    ensure_brief_picks(con)
    ensure_bet_snapshots(con)
    return con


def _ledger_row(con: sqlite3.Connection, game_pk: int, market_type: str) -> sqlite3.Row | None:
    return con.execute(
        """
        SELECT signal_type, pick_side, stake_units, bet, source
        FROM bet_ledger
        WHERE game_pk = ? AND market_type = ?
        ORDER BY id DESC LIMIT 1
        """,
        (game_pk, market_type),
    ).fetchone()


# ── Path inventory (documentation guard) ───────────────────────────────────

WRITE_PATHS = [
    {
        "path": "materialize_bet_ledger_from_brief_picks()",
        "file": "batch/pipeline/generate_daily_brief.py",
        "guard": "_signal_type_from_brief_pick_signal() OWM-before-Away-Dog on ML; "
        "_stake_units_from_brief_pick_row() OWM→1.00u; pick_side=home_ml",
    },
    {
        "path": "_insert_bet_ledger_from_latest()",
        "file": "batch/pipeline/generate_daily_brief.py",
        "guard": "_ledger_stake_units_for_signal_row() + _ledger_signal_type_for_row() "
        "check _scored_game_is_owm_pick() on ML bets",
    },
    {
        "path": "_insert_bet_ledger_from_snapshots()",
        "file": "batch/pipeline/generate_daily_brief.py",
        "guard": "is_owm_ml vs is_away_dog branches on signals_used + bet shape",
    },
    {
        "path": "collect_score_today_picks() / write_picks_to_bet_ledger()",
        "file": "batch/analysis/prediction/bet_ledger_writes.py",
        "guard": "DEFAULT_STAKE OWM=1.00 AWAY_DOG_RL=0.10; separate _add() per signal",
    },
    {
        "path": "repair_owm_ledger_classification()",
        "file": "batch/pipeline/bet_ledger_schema.py",
        "guard": "Backfill AWAY_DOG_RL+0.10u+% ML% rows → OWM 1.00u on ensure_bet_ledger_extended()",
    },
    {
        "path": "_dedupe_ledger_rows_for_prior_display()",
        "file": "batch/pipeline/generate_daily_brief.py",
        "guard": "Prefers brief over score_today — write path must label brief correctly",
    },
]


# ── Unit: signal classification ──────────────────────────────────────────

SIGNAL_CLASS_CASES = [
    # (signal, market, bet, expected)
    ("OWM, Away Dog RL", "ML", "MIA ML", "OWM"),
    ("Away Dog RL, OWM", "ML", "DET ML", "OWM"),
    ("OWM, Away Dog RL", "ML", "CHC ML", "OWM"),
    ("OWM, Away Dog RL", "RL", "AZ +1.5", "AWAY_DOG_RL"),
    ("Away Dog RL", "RL", "STL +1.5", "AWAY_DOG_RL"),
    ("OWM", "ML", "COL ML", "OWM"),
    ("Under", "TOTAL", "UNDER 8.5", "UNDER"),
    ("MV-F", "ML", "NYY ML", "ML"),
    ("Away Dog RL", "ML", "MIA ML", "AWAY_DOG_RL"),  # lone RL label on ML — edge case
]


def test_signal_classification(report: Report) -> None:
    for signal, market, bet, expected in SIGNAL_CLASS_CASES:
        got = _signal_type_from_brief_pick_signal(signal, market, bet)
        ok = got == expected
        report.add(
            f"signal_class:{bet}:{signal[:20]}",
            ok,
            f"expected={expected} got={got}",
        )


def test_scored_game_is_owm(report: Report) -> None:
    class Sig:
        def __init__(self, sid: str, fires: bool):
            self.signal_id = sid
            self.fires = fires

    owm_sg = SimpleNamespace(
        tier_basis="OWM signal (home ML edge)",
        best_side="home_ml",
        active_bets=[Sig("OWM", True), Sig("AWAY_DOG_RL", True)],
    )
    report.add("scored_game_is_owm:tier_basis", _scored_game_is_owm_pick(owm_sg))

    rl_only = SimpleNamespace(
        tier_basis="Away Dog RL",
        best_side="away_rl",
        active_bets=[Sig("AWAY_DOG_RL", True)],
    )
    report.add("scored_game_is_owm:rl_only", not _scored_game_is_owm_pick(rl_only))

    via_active = SimpleNamespace(
        tier_basis="",
        best_side="home_ml",
        active_bets=[Sig("OWM", True)],
    )
    report.add("scored_game_is_owm:active_bets", _scored_game_is_owm_pick(via_active))


# ── Integration: brief_picks materialization (primary bug path) ────────────

def test_materialize_brief_picks_co_listed(report: Report) -> None:
    """Reproduce 2026-06-09 MIA/DET: co-listed OWM + Away Dog RL on home ML."""
    scenarios = [
        (823858, "MIA ML", -132),
        (824267, "DET ML", -132),
        (999001, "ATL ML", -145),
    ]
    con = _mem_conn()
    gd = "2026-06-09"
    session = "primary"
    for gpk, bet, odds in scenarios:
        con.execute(
            """
            INSERT INTO brief_picks
                (game_date, session, game_pk, pick_rank, signal, bet, market, odds, recorded_at)
            VALUES (?, ?, ?, 1, 'OWM, Away Dog RL', ?, 'ML', ?, '2026-06-09 10:00 ET')
            """,
            (gd, session, gpk, bet, odds),
        )
    con.commit()
    n = materialize_bet_ledger_from_brief_picks(con, gd, session)
    report.add("materialize_brief:insert_count", n == len(scenarios), f"inserted={n}")

    for gpk, bet, _ in scenarios:
        row = _ledger_row(con, gpk, "moneyline")
        ok = (
            row is not None
            and row["signal_type"] == "OWM"
            and abs(float(row["stake_units"]) - BRIEF_FLAT_STAKE) < 1e-6
            and row["pick_side"] == "home_ml"
            and str(row["bet"]) == bet
        )
        report.add(
            f"materialize_brief:row:{bet}",
            ok,
            "none" if row is None else dict(row),
        )
    con.close()


def test_materialize_brief_rl_stays_away_dog(report: Report) -> None:
    con = _mem_conn()
    gd = "2026-06-09"
    con.execute(
        """
        INSERT INTO brief_picks
            (game_date, session, game_pk, pick_rank, signal, bet, market, odds, recorded_at)
        VALUES (?, 'primary', 777001, 1, 'OWM, Away Dog RL', 'STL +1.5', 'RL', -190, '2026-06-09 10:00 ET')
        """,
        (gd,),
    )
    con.commit()
    materialize_bet_ledger_from_brief_picks(con, gd, "primary")
    row = _ledger_row(con, 777001, "spread")
    ok = (
        row is not None
        and row["signal_type"] == "AWAY_DOG_RL"
        and abs(float(row["stake_units"]) - AWAY_DOG_RL_STAKE) < 1e-6
        and row["pick_side"] == "away_rl"
    )
    report.add("materialize_brief:rl_row", ok, "none" if row is None else dict(row))
    con.close()


def test_stake_units_from_brief_pick_row(report: Report) -> None:
    con = _mem_conn()
    gd = "2026-06-09"
    stake = _stake_units_from_brief_pick_row(
        con,
        gd,
        game_pk=823858,
        market="ML",
        bet="MIA ML",
        signal_text="OWM, Away Dog RL",
    )
    report.add(
        "stake_from_brief:owm_ml",
        abs(stake - BRIEF_FLAT_STAKE) < 1e-6,
        f"stake={stake}",
    )
    stake_rl = _stake_units_from_brief_pick_row(
        con,
        gd,
        game_pk=777001,
        market="RL",
        bet="STL +1.5",
        signal_text="OWM, Away Dog RL",
    )
    report.add(
        "stake_from_brief:away_dog_rl",
        abs(stake_rl - AWAY_DOG_RL_STAKE) < 1e-6,
        f"stake={stake_rl}",
    )
    con.close()


# ── Integration: snapshot materialization ──────────────────────────────────

def test_insert_from_snapshots(report: Report) -> None:
    con = _mem_conn()
    gd = "2026-06-09"
    cases = [
        (823858, "ML", "MIA ML", -132, '["OWM", "Away Dog RL"]', "OWM", 1.0, "home_ml"),
        (777002, "RL", "AZ +1.5", 115, '["AWAY_DOG_RL"]', "AWAY_DOG_RL", 0.10, "away_rl"),
        (777003, "ML", "NYY ML", -120, '["MV-F"]', None, 1.0, None),
    ]
    for gpk, mt, bet, odds, sigs, exp_sig, exp_stake, _exp_pick_side in cases:
        con.execute(
            """
            INSERT INTO bet_snapshots
                (game_date, game_pk, market_type, bet_side, bet, odds_taken, score,
                 eval_status, signals_used, placed_at)
            VALUES (?, ?, ?, 'home', ?, ?, 80, 'BET', ?, '2026-06-09 10:00 ET')
            """,
            (gd, gpk, mt, bet, odds, sigs),
        )
    con.commit()
    n = _insert_bet_ledger_from_snapshots(con, gd)
    report.add("snapshot_insert:count", n == len(cases), f"inserted={n}")

    for gpk, mt, bet, _, _, exp_sig, exp_stake, exp_pick_side in cases:
        ledger_mt = {"ML": "moneyline", "RL": "spread", "TOTAL": "total"}[mt]
        row = _ledger_row(con, gpk, ledger_mt)
        ok = row is not None and abs(float(row["stake_units"]) - exp_stake) < 1e-6
        if exp_sig:
            ok = ok and row["signal_type"] == exp_sig
        if exp_pick_side:
            ok = ok and row["pick_side"] == exp_pick_side
        report.add(f"snapshot_insert:{bet}", ok, "none" if row is None else dict(row))
    con.close()


# ── Integration: score_today write path ────────────────────────────────────

def test_score_today_collect_and_write(report: Report) -> None:
    """Same game fires OWM (home ML) and Away Dog RL (+1.5) — distinct rows."""
    gd = "2026-06-09"
    row = {
        "game_pk": 823858,
        "away_team": "AZ",
        "home_team": "MIA",
        "home_ml": -132,
        "away_ml": 109,
        "away_rl_odds": 115,
        "owm_signal": True,
        "away_dog_rl_actionable": True,
        "actionable": False,
        "under_signal": False,
        "both_sp_known": False,
        "rl_signal": False,
    }
    picks = collect_score_today_picks(pd.DataFrame([row]), gd)
    by_sig = {p["signal_type"]: p for p in picks}
    ok_owm = (
        "OWM" in by_sig
        and by_sig["OWM"]["bet"] == "MIA ML"
        and by_sig["OWM"]["pick_side"] == "home_ml"
        and abs(by_sig["OWM"]["stake_units"] - BRIEF_FLAT_STAKE) < 1e-6
    )
    ok_rl = (
        "AWAY_DOG_RL" in by_sig
        and by_sig["AWAY_DOG_RL"]["bet"] == "AZ +1.5"
        and by_sig["AWAY_DOG_RL"]["pick_side"] == "away_rl"
        and abs(by_sig["AWAY_DOG_RL"]["stake_units"] - AWAY_DOG_RL_STAKE) < 1e-6
    )
    report.add("score_today:collect_owm", ok_owm, str(by_sig.get("OWM")))
    report.add("score_today:collect_away_dog", ok_rl, str(by_sig.get("AWAY_DOG_RL")))

    con = _mem_conn()
    stats = write_picks_to_bet_ledger(con, picks, score_date=gd)
    report.add(
        "score_today:write_both",
        stats["written"] == 2,
        str(stats),
    )
    owm_ledger = _ledger_row(con, 823858, "moneyline")
    rl_ledger = _ledger_row(con, 823858, "spread")
    report.add(
        "score_today:ledger_owm",
        owm_ledger is not None and owm_ledger["signal_type"] == "OWM",
        dict(owm_ledger) if owm_ledger else "missing",
    )
    report.add(
        "score_today:ledger_rl",
        rl_ledger is not None and rl_ledger["signal_type"] == "AWAY_DOG_RL",
        dict(rl_ledger) if rl_ledger else "missing",
    )
    con.close()


# ── Integration: repair backfill ─────────────────────────────────────────

def test_repair_misclassified_rows(report: Report) -> None:
    con = _mem_conn()
    con.execute(
        """
        INSERT INTO bet_ledger
            (game_date, game_pk, market_type, bet, odds_taken, stake_units,
             signal_at_time, session, placed_at, source, signal_type, pick_side,
             result, pnl_units)
        VALUES
            ('2026-06-09', 823858, 'moneyline', 'MIA ML', -132, 0.10,
             'top', 'primary', '2026-06-09', 'brief', 'AWAY_DOG_RL', 'away_rl',
             'win', 0.0758),
            ('2026-06-05', 111111, 'moneyline', 'ATL ML', -145, 0.10,
             'top', 'primary', '2026-06-05', 'brief', 'AWAY_DOG_RL', 'away_rl',
             'win', 0.0690)
        """
    )
    con.commit()
    fixed = repair_owm_ledger_classification(con)
    con.commit()
    report.add("repair:rowcount", fixed == 2, f"fixed={fixed}")

    row = con.execute(
        "SELECT signal_type, stake_units, pick_side, pnl_units FROM bet_ledger WHERE game_pk=823858"
    ).fetchone()
    ok = (
        row is not None
        and row["signal_type"] == "OWM"
        and abs(float(row["stake_units"]) - 1.0) < 1e-6
        and row["pick_side"] == "home_ml"
        and abs(float(row["pnl_units"]) - 0.758) < 0.01
    )
    report.add("repair:mia_row", ok, dict(row) if row else "missing")

    # True away dog RL must survive repair
    con.execute(
        """
        INSERT INTO bet_ledger
            (game_date, game_pk, market_type, bet, odds_taken, stake_units,
             signal_at_time, session, placed_at, source, signal_type, pick_side)
        VALUES ('2026-06-09', 888001, 'spread', 'AZ +1.5', 115, 0.10,
                'top', 'primary', '2026-06-09', 'brief', 'AWAY_DOG_RL', 'away_rl')
        """
    )
    con.commit()
    repair_owm_ledger_classification(con)
    con.commit()
    rl = con.execute(
        "SELECT signal_type, stake_units FROM bet_ledger WHERE game_pk=888001"
    ).fetchone()
    report.add(
        "repair:preserves_true_rl",
        rl is not None and rl["signal_type"] == "AWAY_DOG_RL",
        dict(rl) if rl else "missing",
    )
    con.close()


# ── Integration: signal_state stake/signal helpers (T−30 path) ───────────────

def test_signal_state_ledger_helpers(report: Report) -> None:
    """_insert_bet_ledger_from_latest uses these helpers before INSERT."""
    con = _mem_conn()
    gd = "2026-06-09"
    gpk = 823858

    class Sig:
        def __init__(self, sid: str, fires: bool):
            self.signal_id = sid
            self.fires = fires

    owm_sg = SimpleNamespace(
        tier_basis="OWM signal",
        best_side="home_ml",
        active_bets=[Sig("OWM", True), Sig("AWAY_DOG_RL", True)],
        away_dog_rl_actionable=False,
        away_dog_rl_stake=0.10,
        pick_is_actionable=True,
        stake_multiplier=1.0,
    )

    sig_row = con.execute(
        """
        SELECT 'top' AS signal_type, 'moneyline' AS market_type, 'MIA ML' AS bet,
               823858 AS game_pk, 'primary' AS session, '2026-06-09 10:00 ET' AS recorded_at
        """
    ).fetchone()

    import batch.pipeline.generate_daily_brief as gdb

    with mock.patch.object(gdb, "_get_scored_game_for_signal_row", return_value=owm_sg):
        stake = _ledger_stake_units_for_signal_row(con, gd, sig_row)
        sig = _ledger_signal_type_for_row(con, gd, sig_row, stake)

    report.add(
        "signal_state:owm_ml_stake",
        abs(stake - BRIEF_FLAT_STAKE) < 1e-6,
        f"stake={stake}",
    )
    report.add("signal_state:owm_ml_signal_type", sig == "OWM", f"signal_type={sig}")

    rl_row = con.execute(
        """
        SELECT 'top' AS signal_type, 'spread' AS market_type, 'AZ +1.5' AS bet,
               777002 AS game_pk, 'primary' AS session, '2026-06-09 10:00 ET' AS recorded_at
        """
    ).fetchone()
    rl_sg = SimpleNamespace(
        tier_basis="Away Dog RL",
        best_side="away_rl",
        active_bets=[Sig("AWAY_DOG_RL", True)],
        away_dog_rl_actionable=True,
        away_dog_rl_stake=AWAY_DOG_RL_STAKE,
        pick_is_actionable=False,
        stake_multiplier=0.0,
    )
    with mock.patch.object(gdb, "_get_scored_game_for_signal_row", return_value=rl_sg):
        with mock.patch.object(gdb, "_snapshot_has_away_dog_rl_signal", return_value=True):
            rl_stake = _ledger_stake_units_for_signal_row(con, gd, rl_row)
            rl_sig = _ledger_signal_type_for_row(con, gd, rl_row, rl_stake)

    report.add(
        "signal_state:away_dog_rl_stake",
        abs(rl_stake - AWAY_DOG_RL_STAKE) < 1e-6,
        f"stake={rl_stake}",
    )
    report.add(
        "signal_state:away_dog_rl_signal_type",
        rl_sig == "AWAY_DOG_RL",
        f"signal_type={rl_sig}",
    )
    con.close()


# ── Integration: dedupe prefers brief (must be correct) ────────────────────

def test_dedupe_brief_wins(report: Report) -> None:
    rows = [
        {
            "game_pk": 823858,
            "market_type": "moneyline",
            "bet": "MIA ML",
            "stake_units": 1.0,
            "source": "brief",
            "signal_type": "OWM",
            "signal_at_time": "top",
            "result": "win",
            "pnl_units": 0.758,
        },
        {
            "game_pk": 823858,
            "market_type": "moneyline",
            "bet": "MIA ML",
            "stake_units": 1.0,
            "source": "score_today",
            "signal_type": "OWM",
            "signal_at_time": "score_today:OWM",
            "result": "win",
            "pnl_units": 0.7576,
        },
    ]
    deduped = _dedupe_ledger_rows_for_prior_display(rows)
    ok = len(deduped) == 1 and deduped[0]["source"] == "brief"
    report.add("dedupe:brief_preferred", ok, deduped[0] if deduped else "empty")

    bad_brief = [
        {
            "game_pk": 823858,
            "market_type": "moneyline",
            "bet": "MIA ML",
            "stake_units": 0.10,
            "source": "brief",
            "signal_type": "AWAY_DOG_RL",
            "signal_at_time": "top",
            "result": "win",
            "pnl_units": 0.0758,
        },
        {
            "game_pk": 823858,
            "market_type": "moneyline",
            "bet": "MIA ML",
            "stake_units": 1.0,
            "source": "score_today",
            "signal_type": "OWM",
            "signal_at_time": "score_today:OWM",
            "result": "win",
            "pnl_units": 0.7576,
        },
    ]
    deduped_bad = _dedupe_ledger_rows_for_prior_display(bad_brief)
    # Documents known dedupe behaviour: brief always wins — write path must be correct.
    report.add(
        "dedupe:bad_brief_would_win",
        len(deduped_bad) == 1 and deduped_bad[0]["source"] == "brief",
        "brief row wins even when misclassified — repair + write guards required",
    )


# ── Live DB audit ──────────────────────────────────────────────────────────

VIOLATION_QUERIES = {
    "away_dog_on_ml_bet": """
        SELECT id, game_date, game_pk, signal_type, pick_side, bet, stake_units, source, result, pnl_units
        FROM bet_ledger
        WHERE signal_type = 'AWAY_DOG_RL'
          AND upper(trim(bet)) LIKE '% ML'
          AND upper(trim(bet)) NOT LIKE '%+1.5%'
          AND stake_units > 0
    """,
    "owm_wrong_stake": """
        SELECT id, game_date, game_pk, signal_type, bet, stake_units, source
        FROM bet_ledger
        WHERE signal_type = 'OWM'
          AND abs(stake_units - 1.0) > 0.001
          AND stake_units > 0
    """,
    "ml_home_ml_wrong_side": """
        SELECT id, game_date, game_pk, signal_type, pick_side, bet, stake_units
        FROM bet_ledger
        WHERE signal_type = 'OWM'
          AND upper(trim(bet)) LIKE '% ML'
          AND pick_side IS NOT NULL
          AND pick_side NOT IN ('home_ml', 'away_ml')
    """,
    "co_listed_era_misclass": """
        SELECT bl.id, bl.game_date, bl.game_pk, bl.signal_type, bl.bet, bl.stake_units, bp.signal
        FROM bet_ledger bl
        INNER JOIN brief_picks bp
            ON bp.game_date = bl.game_date
           AND bp.game_pk = bl.game_pk
           AND upper(trim(bp.bet)) = upper(trim(bl.bet))
        WHERE bp.signal LIKE '%OWM%'
          AND bp.signal LIKE '%Away Dog%'
          AND upper(trim(bl.bet)) LIKE '% ML'
          AND bl.signal_type != 'OWM'
          AND bl.stake_units > 0
    """,
}


def audit_live_db(report: Report, db_path: str) -> None:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    ensure_bet_ledger_extended(con)

    for name, sql in VIOLATION_QUERIES.items():
        rows = [dict(r) for r in con.execute(sql).fetchall()]
        report.add(f"live_audit:{name}", len(rows) == 0, f"violations={len(rows)}")
        report.live_violations.extend({"query": name, **r} for r in rows)

    # Spot-check 06-09 canonical rows
    canon = con.execute(
        """
        SELECT game_pk, signal_type, pick_side, stake_units, result, pnl_units, source, bet
        FROM bet_ledger
        WHERE game_date = '2026-06-09'
          AND game_pk IN (823858, 824267)
          AND stake_units > 0
          AND lower(coalesce(signal_at_time,'')) != 'avoid'
        ORDER BY source, game_pk
        """
    ).fetchall()
    mia_det_ok = True
    detail_lines = []
    for r in canon:
        detail_lines.append(dict(r))
        if r["bet"] in ("MIA ML", "DET ML"):
            if (
                r["signal_type"] != "OWM"
                or abs(float(r["stake_units"]) - 1.0) > 0.001
                or r["pick_side"] != "home_ml"
            ):
                mia_det_ok = False
    report.add("live_audit:06-09_mia_det", mia_det_ok, json.dumps(detail_lines))

    # Season OWM stake consistency
    owm_stats = con.execute(
        """
        SELECT COUNT(*) AS n,
               MIN(stake_units) AS min_stake,
               MAX(stake_units) AS max_stake,
               SUM(CASE WHEN abs(stake_units - 1.0) > 0.001 THEN 1 ELSE 0 END) AS off_flat
        FROM bet_ledger
        WHERE signal_type = 'OWM' AND stake_units > 0
        """
    ).fetchone()
    report.add(
        "live_audit:owm_season_stakes",
        int(owm_stats["off_flat"] or 0) == 0,
        dict(owm_stats),
    )

    con.close()


# ── Anti-regression: old buggy classifier ──────────────────────────────────

def _legacy_buggy_signal_type(signal_text: str | None, market: str | None) -> str | None:
    """Pre-fix behaviour: AWAY DOG matched before OWM."""
    s = (signal_text or "").strip().upper()
    m = (market or "").strip().upper()
    if "AWAY DOG" in s or "AWAY_DOG_RL" in s:
        return "AWAY_DOG_RL"
    if "OWM" in s:
        return "OWM"
    if m == "ML":
        return "ML"
    return None


def test_old_classifier_would_fail(report: Report) -> None:
    got = _legacy_buggy_signal_type("OWM, Away Dog RL", "ML")
    report.add(
        "anti_regression:old_classifier_buggy",
        got == "AWAY_DOG_RL",
        "old path returns AWAY_DOG_RL — proves why fix was needed",
    )
    new = _signal_type_from_brief_pick_signal("OWM, Away Dog RL", "ML", "MIA ML")
    report.add(
        "anti_regression:new_classifier_fixed",
        new == "OWM",
        f"new={new}",
    )


# ── Runner ───────────────────────────────────────────────────────────────

def run_all(db_path: str) -> Report:
    report = Report(started_at=datetime.now(tz=ET).isoformat())
    report.path_inventory = WRITE_PATHS

    suites: list[tuple[str, Callable[[Report], None]]] = [
        ("signal_classification", test_signal_classification),
        ("scored_game_is_owm", test_scored_game_is_owm),
        ("stake_from_brief", test_stake_units_from_brief_pick_row),
        ("materialize_brief", test_materialize_brief_picks_co_listed),
        ("materialize_brief_rl", test_materialize_brief_rl_stays_away_dog),
        ("snapshot_insert", test_insert_from_snapshots),
        ("score_today", test_score_today_collect_and_write),
        ("repair", test_repair_misclassified_rows),
        ("signal_state_helpers", test_signal_state_ledger_helpers),
        ("dedupe", test_dedupe_brief_wins),
        ("anti_regression", test_old_classifier_would_fail),
    ]

    for suite_name, fn in suites:
        try:
            fn(report)
        except Exception as exc:
            report.add(f"{suite_name}:EXCEPTION", False, traceback.format_exc())
            report.add(f"{suite_name}:error", False, str(exc))

    try:
        audit_live_db(report, db_path)
    except Exception as exc:
        report.add("live_audit:EXCEPTION", False, traceback.format_exc())

    report.finished_at = datetime.now(tz=ET).isoformat()
    return report


def write_reports(report: Report) -> tuple[Path, Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = _ts()
    txt_path = OUT_DIR / f"owm_ledger_regression_{stamp}.txt"
    json_path = OUT_DIR / f"owm_ledger_regression_{stamp}.json"

    lines = [
        "=" * 72,
        "OWM / AWAY_DOG_RL bet_ledger regression report",
        f"Started:  {report.started_at}",
        f"Finished: {report.finished_at}",
        f"Result:   {report.passed} passed, {report.failed} FAILED",
        "=" * 72,
        "",
        "WRITE PATH INVENTORY",
        "-" * 72,
    ]
    for p in report.path_inventory:
        lines.append(f"  {p['path']}")
        lines.append(f"    file:  {p['file']}")
        lines.append(f"    guard: {p['guard']}")
        lines.append("")

    lines.append("TEST CASES")
    lines.append("-" * 72)
    for c in report.cases:
        status = "PASS" if c.passed else "FAIL"
        lines.append(f"  [{status}] {c.name}")
        if c.detail:
            lines.append(f"         {c.detail}")

    if report.live_violations:
        lines.append("")
        lines.append("LIVE DB VIOLATIONS")
        lines.append("-" * 72)
        for v in report.live_violations:
            lines.append(f"  {json.dumps(v)}")

    lines.append("")
    lines.append("=" * 72)
    summary = "ALL TESTS PASSED" if report.failed == 0 else f"{report.failed} TEST(S) FAILED"
    lines.append(summary)
    lines.append("=" * 72)

    txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    payload = {
        "started_at": report.started_at,
        "finished_at": report.finished_at,
        "passed": report.passed,
        "failed": report.failed,
        "all_passed": report.failed == 0,
        "cases": [asdict(c) for c in report.cases],
        "live_violations": report.live_violations,
        "path_inventory": report.path_inventory,
    }
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # Symlink-style "latest" copies for easy diffing
    latest_txt = OUT_DIR / "owm_ledger_regression_latest.txt"
    latest_json = OUT_DIR / "owm_ledger_regression_latest.json"
    latest_txt.write_text(txt_path.read_text(encoding="utf-8"), encoding="utf-8")
    latest_json.write_text(json_path.read_text(encoding="utf-8"), encoding="utf-8")

    return txt_path, json_path


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="OWM ledger regression suite")
    parser.add_argument("--db", default=get_db_path(), help="SQLite DB for live audit")
    args = parser.parse_args()

    print(f"[regression] DB: {args.db}")
    report = run_all(args.db)
    txt_path, json_path = write_reports(report)

    print(f"[regression] {report.passed} passed, {report.failed} failed")
    print(f"[regression] report: {txt_path}")
    print(f"[regression] json:   {json_path}")

    if report.failed:
        print("\nFailed cases:")
        for c in report.cases:
            if not c.passed:
                print(f"  - {c.name}: {c.detail}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
