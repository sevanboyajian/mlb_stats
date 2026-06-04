"""
grade_ledger.py
───────────────
Daily grading agent: grade bet_ledger rows for Final games, append calibration
rows, and persist run/row audit in grading_log / grading_log_rows.
"""

from __future__ import annotations

import csv
import datetime
import logging
import sqlite3
from pathlib import Path
from typing import Any

from core.grading.arithmetic import (
    avoid_result_from_hypothesis,
    counterfactual_hypothesis,
    grade_moneyline_side,
    grade_runline_side,
    grade_total_side,
)
from core.grading.parsing import (
    parse_ml_team,
    parse_runline_bet,
    parse_total_bet,
    strip_avoid_bet_label,
    total_line_for_totals_grading,
)

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent


def _use_row_factory(conn: sqlite3.Connection) -> None:
    if conn.row_factory is not sqlite3.Row:
        conn.row_factory = sqlite3.Row


def ensure_grading_log(conn: sqlite3.Connection) -> None:
    """Create grading audit tables if missing (idempotent)."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS grading_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            game_date       TEXT    NOT NULL,
            graded_at       TEXT    NOT NULL,
            trigger         TEXT    NOT NULL,
            ledger_rows     INTEGER,
            rows_attempted  INTEGER,
            rows_graded     INTEGER,
            odds_synced     INTEGER,
            wins            INTEGER,
            losses          INTEGER,
            pushes          INTEGER,
            avoid_good      INTEGER,
            avoid_bad       INTEGER,
            avoid_push      INTEGER,
            pnl_units       REAL,
            ungraded_after  INTEGER,
            error_message   TEXT,
            alert_count     INTEGER NOT NULL DEFAULT 0,
            alerts_json     TEXT,
            v2_roi_pct      REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS grading_log_rows (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            grading_log_id  INTEGER NOT NULL,
            bet_ledger_id   INTEGER NOT NULL,
            game_pk         INTEGER,
            market_type     TEXT,
            bet             TEXT,
            odds_taken      INTEGER,
            home_score      INTEGER,
            away_score      INTEGER,
            total_line_used REAL,
            result          TEXT,
            pnl_units       REAL,
            grade_status    TEXT    NOT NULL DEFAULT 'graded'
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_grading_log_game_date
            ON grading_log (game_date)
        """
    )
    _migrate_grading_log_columns(conn)
    conn.commit()


def _migrate_grading_log_columns(conn: sqlite3.Connection) -> None:
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(grading_log)").fetchall()}
        if "alert_count" not in cols:
            conn.execute(
                "ALTER TABLE grading_log ADD COLUMN alert_count INTEGER NOT NULL DEFAULT 0"
            )
        if "alerts_json" not in cols:
            conn.execute("ALTER TABLE grading_log ADD COLUMN alerts_json TEXT")
        if "v2_roi_pct" not in cols:
            conn.execute("ALTER TABLE grading_log ADD COLUMN v2_roi_pct REAL")
    except sqlite3.OperationalError:
        pass


def _ledger_market_to_snapshot_type(market_type: str) -> str | None:
    mt_u = (market_type or "").strip().lower()
    if mt_u == "moneyline":
        return "ML"
    if mt_u == "total":
        return "TOTAL"
    if mt_u in ("spread", "runline"):
        return "RL"
    return None


def first_fire_odds_taken(
    conn: sqlite3.Connection,
    game_date: str,
    game_pk: int,
    ledger_market_type: str,
) -> int | None:
    snap_mt = _ledger_market_to_snapshot_type(ledger_market_type)
    if not snap_mt:
        return None
    try:
        row = conn.execute(
            """
            SELECT odds_taken FROM bet_snapshots
            WHERE game_date = ? AND game_pk = ? AND market_type = ?
            LIMIT 1
            """,
            (game_date, int(game_pk), snap_mt),
        ).fetchone()
        if row is not None and row[0] is not None:
            return int(row[0])
    except Exception:
        pass
    return None


def sync_bet_ledger_odds_from_snapshots(
    conn: sqlite3.Connection,
    game_date: str,
) -> int:
    """Align bet_ledger.odds_taken with first-fire bet_snapshots before grading."""
    _use_row_factory(conn)
    updated = 0
    try:
        rows = conn.execute(
            """
            SELECT id, game_pk, market_type, odds_taken
            FROM bet_ledger
            WHERE game_date = ?
              AND stake_units > 0
              AND lower(trim(coalesce(signal_at_time, ''))) != 'avoid'
            """,
            (game_date,),
        ).fetchall()
        for r in rows:
            snap_odds = first_fire_odds_taken(
                conn, game_date, int(r["game_pk"]), str(r["market_type"] or ""),
            )
            if snap_odds is None:
                continue
            cur = r["odds_taken"]
            if cur is not None and int(cur) == snap_odds:
                continue
            conn.execute(
                "UPDATE bet_ledger SET odds_taken = ? WHERE id = ?",
                (snap_odds, int(r["id"])),
            )
            updated += 1
    except Exception as e:
        logging.warning("[bet_ledger] odds sync from snapshots failed: %s", e)
    return updated


def _now_et() -> datetime.datetime:
    from batch.pipeline.generate_daily_brief import _now_et as gdb_now_et

    return gdb_now_et()


def _log_grading_row(
    conn: sqlite3.Connection,
    grading_log_id: int,
    *,
    bet_ledger_id: int,
    game_pk: int | None,
    market_type: str | None,
    bet: str | None,
    odds_taken: int | None,
    home_score: int | None,
    away_score: int | None,
    total_line_used: float | None,
    result: str | None,
    pnl_units: float | None,
    grade_status: str = "graded",
) -> None:
    conn.execute(
        """
        INSERT INTO grading_log_rows (
            grading_log_id, bet_ledger_id, game_pk, market_type, bet,
            odds_taken, home_score, away_score, total_line_used,
            result, pnl_units, grade_status
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            grading_log_id,
            bet_ledger_id,
            game_pk,
            market_type,
            bet,
            odds_taken,
            home_score,
            away_score,
            total_line_used,
            result,
            pnl_units,
            grade_status,
        ),
    )


def _summarize_day(conn: sqlite3.Connection, game_date: str) -> dict[str, Any]:
    _use_row_factory(conn)
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS ledger_rows,
            SUM(CASE WHEN lower(trim(coalesce(result,''))) = 'win' THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN lower(trim(coalesce(result,''))) = 'loss' THEN 1 ELSE 0 END) AS losses,
            SUM(CASE WHEN lower(trim(coalesce(result,''))) = 'push' THEN 1 ELSE 0 END) AS pushes,
            SUM(CASE WHEN lower(trim(coalesce(result,''))) = 'good_avoid' THEN 1 ELSE 0 END) AS avoid_good,
            SUM(CASE WHEN lower(trim(coalesce(result,''))) = 'bad_avoid' THEN 1 ELSE 0 END) AS avoid_bad,
            SUM(CASE WHEN lower(trim(coalesce(result,''))) = 'push_avoid' THEN 1 ELSE 0 END) AS avoid_push,
            SUM(CASE WHEN stake_units > 0
                      AND lower(trim(coalesce(signal_at_time,''))) != 'avoid'
                 THEN coalesce(pnl_units, 0) ELSE 0 END) AS pnl_units
        FROM bet_ledger
        WHERE game_date = ?
        """,
        (game_date,),
    ).fetchone()
    return {
        "ledger_rows": int(row["ledger_rows"] or 0),
        "wins": int(row["wins"] or 0),
        "losses": int(row["losses"] or 0),
        "pushes": int(row["pushes"] or 0),
        "avoid_good": int(row["avoid_good"] or 0),
        "avoid_bad": int(row["avoid_bad"] or 0),
        "avoid_push": int(row["avoid_push"] or 0),
        "pnl_units": float(row["pnl_units"] or 0.0),
    }


def _count_ungraded_final(conn: sqlite3.Connection, game_date: str) -> int:
    return int(
        conn.execute(
            """
            SELECT COUNT(*) FROM bet_ledger bl
            JOIN games g ON g.game_pk = bl.game_pk
            WHERE bl.game_date = ?
              AND g.status = 'Final'
              AND (bl.result IS NULL OR TRIM(bl.result) = '')
              AND lower(trim(coalesce(bl.signal_at_time,''))) != 'avoid'
            """,
            (game_date,),
        ).fetchone()[0]
    )


def grade_bet_ledger(
    conn: sqlite3.Connection,
    game_date: str | None = None,
    *,
    grading_log_id: int | None = None,
) -> int:
    """
    Grade bets in bet_ledger for games that are Final.

    Top/next rows: result in ('win','loss','push'), pnl_units at flat 1u.
    Avoid rows: good_avoid / bad_avoid / push_avoid (pnl 0).
    """
    if conn is None:
        return 0

    _use_row_factory(conn)

    where_date = ""
    params: tuple = ()
    if game_date:
        where_date = "AND bl.game_date = ?"
        params = (game_date,)

    rows = conn.execute(
        f"""
        SELECT
            bl.id,
            bl.game_pk,
            g.game_date_et AS game_date_et,
            bl.market_type,
            bl.bet,
            bl.odds_taken,
            bl.stake_units,
            bl.signal_at_time,
            g.home_team_id,
            g.away_team_id,
            g.home_score,
            g.away_score,
            th.abbreviation AS home_abbr,
            ta.abbreviation AS away_abbr,
            bl.total_line_at_bet AS ledger_total_line_at_bet,
            bp.total_line_at_bet AS bp_total_line_at_bet,
            bp.total_line AS bp_total_line,
            (
                SELECT go.total_line
                FROM game_odds go
                WHERE go.game_pk = bl.game_pk
                  AND go.market_type = 'total'
                  AND g.game_start_utc IS NOT NULL
                  AND TRIM(CAST(g.game_start_utc AS TEXT)) != ''
                  AND go.captured_at_utc <= g.game_start_utc
                ORDER BY go.captured_at_utc DESC,
                    CASE lower(go.bookmaker)
                        WHEN 'draftkings' THEN 1
                        WHEN 'fanduel' THEN 2
                        WHEN 'betmgm' THEN 3
                        WHEN 'betonlineag' THEN 4
                        WHEN 'sbro' THEN 5
                        WHEN 'oddswarehouse' THEN 6
                        ELSE 7
                    END ASC
                LIMIT 1
            ) AS pre_start_total_line
        FROM bet_ledger bl
        JOIN games g ON g.game_pk = bl.game_pk
        JOIN teams th ON th.team_id = g.home_team_id
        JOIN teams ta ON ta.team_id = g.away_team_id
        LEFT JOIN brief_picks bp
            ON bp.game_date = bl.game_date
           AND bp.game_pk = bl.game_pk
           AND bp.market = 'TOTAL'
        WHERE g.status = 'Final'
          AND (bl.result IS NULL OR TRIM(bl.result) = '')
          {where_date}
        """,
        params,
    ).fetchall()

    if not rows:
        return 0

    from batch.pipeline.edge_utils import american_to_implied_prob, score_to_model_prob
    from batch.pipeline.generate_daily_brief import (
        evaluate_signals,
        load_games,
        load_starters,
        load_streaks,
    )

    cal_path = _REPO_ROOT / "data" / "calibration_log.csv"
    cal_path.parent.mkdir(parents=True, exist_ok=True)
    wrote_header = cal_path.exists() is False
    scored_cache: dict[int, object] = {}

    def _get_scored_for_game_pk(gpk: int, gd: str) -> object | None:
        if gpk in scored_cache:
            return scored_cache[gpk]
        try:
            as_of = _now_et()
            games = load_games(conn, gd, verbose=False, as_of_dt=as_of)
            gmap = {int(g["game_pk"]): g for g in games if g.get("game_pk") is not None}
            game = gmap.get(int(gpk))
            if not game:
                scored_cache[gpk] = None
                return None
            team_ids = list({game["home_team_id"], game["away_team_id"]})
            streaks = load_streaks(conn, gd, team_ids, verbose=False)
            starters, _ = load_starters(conn, gd, verbose=False)
            sigs = evaluate_signals(conn, game, streaks, "primary", starters)
            sg = sigs.get("_scored_game")
            scored_cache[gpk] = sg
            return sg
        except Exception:
            scored_cache[gpk] = None
            return None

    def _append_cal_row(
        *,
        gd: str,
        gpk: int,
        bet_type: str,
        market_type: str,
        odds_taken: int | None,
        result_str: str,
    ) -> None:
        nonlocal wrote_header
        sg = _get_scored_for_game_pk(gpk, gd)
        if sg is None:
            return
        try:
            me = getattr(sg, "market_evals", {}) or {}
            mt = str(market_type or "").strip().upper()
            mm = me.get(mt) or {}
            score = int(mm.get("score") or 0)
            model_p = float(mm.get("model_p") or score_to_model_prob(score))
            implied_p = (
                float(mm.get("implied_p"))
                if mm.get("implied_p") is not None
                else american_to_implied_prob(int(odds_taken) if odds_taken is not None else None)
            )
            edge = (
                float(mm.get("edge"))
                if mm.get("edge") is not None
                else ((model_p - implied_p) if implied_p is not None else None)
            )
        except Exception:
            score = 0
            model_p = float(score_to_model_prob(score))
            implied_p = american_to_implied_prob(int(odds_taken) if odds_taken is not None else None)
            edge = (model_p - implied_p) if implied_p is not None else None
        res_u = (result_str or "").strip().lower()
        if res_u == "win":
            res_val = 1
        elif res_u == "loss":
            res_val = 0
        else:
            return
        with cal_path.open("a", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            if wrote_header:
                w.writerow(
                    [
                        "date",
                        "game_pk",
                        "market_type",
                        "bet_type",
                        "score",
                        "model_p",
                        "implied_p",
                        "edge",
                        "result",
                    ]
                )
                wrote_header = False
            w.writerow(
                [
                    gd,
                    int(gpk),
                    str(market_type or "").strip().upper(),
                    bet_type,
                    int(score),
                    f"{model_p:.3f}",
                    (f"{implied_p:.3f}" if implied_p is not None else "NA"),
                    (f"{edge:.3f}" if edge is not None else "NA"),
                    int(res_val),
                ]
            )

    updated = 0
    for r in rows:
        market = (r["market_type"] or "").strip().lower()
        bet_text = r["bet"] or ""
        hs = r["home_score"]
        as_ = r["away_score"]
        if hs is None or as_ is None:
            continue

        home_abbr = (r["home_abbr"] or "").upper()
        away_abbr = (r["away_abbr"] or "").upper()
        sig_at = (r["signal_at_time"] or "").strip().lower()
        total_line_used = total_line_for_totals_grading(r)
        res: str | None = None
        pnl: float | None = None

        if sig_at == "avoid":
            equiv = strip_avoid_bet_label(bet_text)
            hypo = counterfactual_hypothesis(
                market=market,
                bet_text=equiv,
                home_abbr=home_abbr,
                away_abbr=away_abbr,
                home_score=int(hs),
                away_score=int(as_),
                total_line=total_line_used,
            )
            if hypo is None:
                continue
            res = avoid_result_from_hypothesis(hypo)
            pnl = 0.0
        elif market == "moneyline":
            team = parse_ml_team(bet_text)
            if not team:
                continue
            graded = grade_moneyline_side(
                team=team,
                home_abbr=home_abbr,
                away_abbr=away_abbr,
                home_score=int(hs),
                away_score=int(as_),
                odds=r["odds_taken"],
            )
            if graded is None:
                continue
            stake_mult = float(r["stake_units"] or 1.0)
            res, pnl = graded
            if pnl is not None:
                pnl = float(pnl) * stake_mult
        elif market == "total":
            side, parsed_line = parse_total_bet(bet_text)
            if side is None:
                continue
            line = total_line_used if total_line_used is not None else parsed_line
            if line is None:
                continue
            graded_total = grade_total_side(
                side=side,
                line=float(line),
                runs=int(hs) + int(as_),
                odds=r["odds_taken"],
            )
            stake_mult = float(r["stake_units"] or 1.0)
            if graded_total is not None:
                res, pnl = graded_total
                pnl = float(pnl) * stake_mult
            else:
                res, pnl = None, None
        elif market in ("spread", "runline"):
            team, line = parse_runline_bet(bet_text)
            if team is None or line is None:
                continue
            graded = grade_runline_side(
                team=team,
                line=float(line),
                home_abbr=home_abbr,
                away_abbr=away_abbr,
                home_score=int(hs),
                away_score=int(as_),
                odds=r["odds_taken"],
            )
            if graded is None:
                continue
            stake_mult = float(r["stake_units"] or 1.0)
            res, pnl = graded
            pnl = float(pnl) * stake_mult
        else:
            continue

        if res is None or pnl is None:
            continue

        conn.execute(
            "UPDATE bet_ledger SET result = ?, pnl_units = ? WHERE id = ?",
            (res, float(round(pnl, 4)), r["id"]),
        )
        updated += 1

        if grading_log_id is not None:
            _log_grading_row(
                conn,
                grading_log_id,
                bet_ledger_id=int(r["id"]),
                game_pk=int(r["game_pk"]),
                market_type=str(r["market_type"] or ""),
                bet=bet_text,
                odds_taken=int(r["odds_taken"]) if r["odds_taken"] is not None else None,
                home_score=int(hs),
                away_score=int(as_),
                total_line_used=total_line_used,
                result=res,
                pnl_units=float(round(pnl, 4)),
            )

        try:
            if (
                float(r["stake_units"] or 0.0) > 0
                and (r["odds_taken"] is not None)
                and (market in ("moneyline", "spread", "runline", "total"))
            ):
                mt_u = (r["market_type"] or "").strip().lower()
                mlabel = (
                    "ML"
                    if mt_u == "moneyline"
                    else ("TOTAL" if mt_u == "total" else ("RL" if mt_u in ("spread", "runline") else str(r["market_type"] or "").upper()))
                )
                _append_cal_row(
                    gd=str(r["game_date_et"] or game_date or ""),
                    gpk=int(r["game_pk"]),
                    bet_type=str(r["market_type"] or ""),
                    market_type=mlabel,
                    odds_taken=int(r["odds_taken"]) if r["odds_taken"] is not None else None,
                    result_str=str(res),
                )
        except Exception:
            pass

    if updated:
        conn.commit()
    return updated


def run_daily_grading(
    conn: sqlite3.Connection,
    game_date: str,
    *,
    trigger: str = "cli",
    now: datetime.datetime | None = None,
    sync_odds: bool = True,
    retry_ungraded: bool = True,
) -> dict[str, Any]:
    """
    Grade all eligible bet_ledger rows for ``game_date`` and write grading_log audit.

    Returns a summary dict (includes ``grading_log_id`` when successful).
    """
    _use_row_factory(conn)
    ensure_grading_log(conn)
    if now is None:
        now = _now_et()
    graded_at = now.strftime("%Y-%m-%d %H:%M ET")

    odds_synced = 0
    if sync_odds:
        odds_synced = sync_bet_ledger_odds_from_snapshots(conn, game_date)
        if odds_synced:
            conn.commit()

    rows_attempted = int(
        conn.execute(
            """
            SELECT COUNT(*) FROM bet_ledger bl
            JOIN games g ON g.game_pk = bl.game_pk
            WHERE bl.game_date = ?
              AND g.status = 'Final'
              AND (bl.result IS NULL OR TRIM(bl.result) = '')
            """,
            (game_date,),
        ).fetchone()[0]
    )

    cur = conn.execute(
        """
        INSERT INTO grading_log (
            game_date, graded_at, trigger, rows_attempted, odds_synced
        ) VALUES (?,?,?,?,?)
        """,
        (game_date, graded_at, trigger, rows_attempted, odds_synced),
    )
    grading_log_id = int(cur.lastrowid)
    conn.commit()

    error_message: str | None = None
    rows_graded = 0
    try:
        rows_graded = grade_bet_ledger(conn, game_date=game_date, grading_log_id=grading_log_id)
        if retry_ungraded:
            ungraded = _count_ungraded_final(conn, game_date)
            if ungraded > 0:
                logging.warning(
                    "[grading] %s bet_ledger row(s) for %s still ungraded; retrying.",
                    ungraded,
                    game_date,
                )
                rows_graded += grade_bet_ledger(
                    conn, game_date=game_date, grading_log_id=grading_log_id,
                )
    except Exception as e:
        error_message = str(e)
        logging.warning("[grading] grade_bet_ledger failed for %s: %s", game_date, e)

    summary = _summarize_day(conn, game_date)
    ungraded_after = _count_ungraded_final(conn, game_date)

    from batch.pipeline.grading_report import build_report_context

    report_ctx = build_report_context(
        conn,
        {
            "grading_log_id": grading_log_id,
            "game_date": game_date,
            "graded_at": graded_at,
            "trigger": trigger,
            "rows_attempted": rows_attempted,
            "rows_graded": rows_graded,
            "odds_synced": odds_synced,
            "ungraded_after": ungraded_after,
            "error_message": error_message,
            **summary,
        },
        game_date,
    )

    conn.execute(
        """
        UPDATE grading_log SET
            ledger_rows = ?,
            rows_graded = ?,
            wins = ?,
            losses = ?,
            pushes = ?,
            avoid_good = ?,
            avoid_bad = ?,
            avoid_push = ?,
            pnl_units = ?,
            ungraded_after = ?,
            error_message = ?,
            alert_count = ?,
            alerts_json = ?,
            v2_roi_pct = ?
        WHERE id = ?
        """,
        (
            summary["ledger_rows"],
            rows_graded,
            summary["wins"],
            summary["losses"],
            summary["pushes"],
            summary["avoid_good"],
            summary["avoid_bad"],
            summary["avoid_push"],
            summary["pnl_units"],
            ungraded_after,
            error_message,
            int(report_ctx.get("alert_count") or 0),
            report_ctx.get("alerts_json"),
            float(report_ctx.get("v2_roi_pct") or 0.0),
            grading_log_id,
        ),
    )
    conn.commit()

    return report_ctx


from batch.pipeline.grading_report import (  # noqa: E402
    build_weekly_signal_report,
    compute_rolling_signal_performance,
    format_email_report,
    write_grading_report_file,
)
