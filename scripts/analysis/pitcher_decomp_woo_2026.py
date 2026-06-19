#!/usr/bin/env python3
"""
Bryan Woo — 2026 start-by-start ERA WMA decomposition (ad hoc).

USAGE:
  python scripts/analysis/pitcher_decomp_woo_2026.py
  python scripts/analysis/pitcher_decomp_woo_2026.py --reference-date 2026-06-18
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.db.connection import connect as db_connect, get_db_path

OUT_PATH = ROOT / "outputs" / "reports" / "pitcher_decomp_woo_2026.txt"
ET = ZoneInfo("America/New_York")

WMA_WEIGHTS = [5, 4, 3, 2, 1]
WMA_DIVISOR = 15.0
MIN_IP = 3.0
DEFAULT_REF_DATE = "2026-06-18"

_GAME_DATE = "COALESCE(NULLIF(TRIM(g.game_date_et), ''), g.game_date)"


def _start_era(ip: float | None, er: int | None) -> float | None:
    if ip is None or float(ip) <= 0:
        return None
    return round((int(er or 0) * 9.0) / float(ip), 2)


def _wma(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    weights = WMA_WEIGHTS[: len(values)]
    divisor = float(sum(weights))
    return round(sum(w * v for w, v in zip(weights, values)) / divisor, 2)


def _find_player_id(con: sqlite3.Connection) -> tuple[int, str]:
    row = con.execute(
        """
        SELECT player_id, full_name
        FROM players
        WHERE full_name LIKE '%Woo%'
          AND full_name LIKE '%Bryan%'
        ORDER BY player_id
        LIMIT 1
        """
    ).fetchone()
    if row:
        return int(row[0]), str(row[1])

    row = con.execute(
        """
        SELECT player_id, full_name
        FROM players
        WHERE full_name LIKE '%Woo%'
        ORDER BY player_id
        LIMIT 5
        """
    ).fetchall()
    if not row:
        raise SystemExit("No player matching '%Woo%' found in players table.")
    if len(row) == 1:
        return int(row[0][0]), str(row[0][1])
    names = ", ".join(f"{r[1]} ({r[0]})" for r in row)
    raise SystemExit(f"Ambiguous Woo matches: {names}")


def _load_starts(con: sqlite3.Connection, player_id: int) -> list[sqlite3.Row]:
    con.row_factory = sqlite3.Row
    return list(
        con.execute(
            f"""
            SELECT
                {_GAME_DATE} AS game_date_et,
                g.game_pk,
                g.home_team_id,
                g.away_team_id,
                pgs.team_id,
                th.abbreviation AS home_abbr,
                ta.abbreviation AS away_abbr,
                pgs.innings_pitched,
                pgs.earned_runs,
                pgs.hits_allowed,
                pgs.walks_allowed,
                pgs.strikeouts_pit,
                pgs.pitches_thrown,
                pgs.hr_allowed,
                pgs.k_per_9,
                pgs.whip,
                CASE
                    WHEN pgs.innings_pitched > 0
                    THEN ROUND((pgs.earned_runs * 9.0) / pgs.innings_pitched, 2)
                    ELSE NULL
                END AS start_era,
                pgs.quality_start,
                pgs.win,
                pgs.loss
            FROM player_game_stats pgs
            JOIN games g ON g.game_pk = pgs.game_pk
            JOIN teams th ON th.team_id = g.home_team_id
            JOIN teams ta ON ta.team_id = g.away_team_id
            WHERE pgs.player_id = ?
              AND pgs.player_role = 'pitcher'
              AND pgs.innings_pitched >= ?
              AND g.season = 2026
            ORDER BY {_GAME_DATE} DESC, g.game_pk DESC
            LIMIT 8
            """,
            (player_id, MIN_IP),
        ).fetchall()
    )


def _load_rolling(con: sqlite3.Connection, player_id: int) -> list[sqlite3.Row]:
    return list(
        con.execute(
            """
            SELECT
                prs.game_date_et,
                prs.game_pk,
                prs.starts_in_window,
                prs.era_wma,
                prs.era_wma_home,
                prs.era_wma_away,
                prs.starts_in_window_home,
                prs.starts_in_window_away,
                prs.k_per_9_wma,
                prs.whip_wma,
                prs.updated_at
            FROM pitcher_rolling_stats prs
            WHERE prs.player_id = ?
              AND prs.season = 2026
            ORDER BY prs.game_date_et DESC, prs.game_pk DESC
            LIMIT 10
            """,
            (player_id,),
        ).fetchall()
    )


def _load_season_agg(con: sqlite3.Connection, player_id: int) -> sqlite3.Row:
    return con.execute(
        f"""
        SELECT
            COUNT(*) AS starts,
            SUM(pgs.innings_pitched) AS total_ip,
            SUM(pgs.earned_runs) AS total_er,
            ROUND((SUM(pgs.earned_runs) * 9.0) / SUM(pgs.innings_pitched), 2) AS season_era,
            ROUND(AVG(pgs.k_per_9), 2) AS avg_k9,
            ROUND(AVG(pgs.whip), 2) AS avg_whip,
            SUM(pgs.quality_start) AS quality_starts,
            SUM(pgs.win) AS wins,
            SUM(pgs.loss) AS losses
        FROM player_game_stats pgs
        JOIN games g ON g.game_pk = pgs.game_pk
        WHERE pgs.player_id = ?
          AND pgs.player_role = 'pitcher'
          AND pgs.innings_pitched >= ?
          AND g.season = 2026
        """,
        (player_id, MIN_IP),
    ).fetchone()


def _stored_wma_for_date(
    rolling: list[sqlite3.Row],
    ref_date: str,
) -> tuple[float | None, int | None, str | None]:
    for row in rolling:
        if str(row["game_date_et"]) == ref_date:
            era = row["era_wma"]
            return (
                round(float(era), 2) if era is not None else None,
                int(row["game_pk"]),
                str(row["updated_at"]) if row["updated_at"] else None,
            )
    for row in rolling:
        if str(row["game_date_et"]) <= ref_date:
            era = row["era_wma"]
            return (
                round(float(era), 2) if era is not None else None,
                int(row["game_pk"]),
                str(row["updated_at"]) if row["updated_at"] else None,
            )
    return None, None, None


def _matchup(row: sqlite3.Row) -> str:
    team_id = int(row["team_id"])
    home = str(row["home_abbr"])
    away = str(row["away_abbr"])
    if team_id == int(row["home_team_id"]):
        return f"vs {away} (h)"
    return f"@ {home} (a)"


def _wl(row: sqlite3.Row) -> str:
    if int(row["win"] or 0):
        return "W"
    if int(row["loss"] or 0):
        return "L"
    return "ND"


def _diagnose(start_eras: list[float]) -> str:
    if not start_eras:
        return "NO DATA: no qualifying starts available for decomposition."
    if len(start_eras) < 5:
        return (
            f"PARTIAL WINDOW: only {len(start_eras)} prior start(s) in WMA window "
            f"(need 5 for full decomposition)."
        )

    g1, g2 = start_eras[0], start_eras[1]
    tail = start_eras[2:]
    if (g1 > 9.0 or g2 > 9.0) and all(e < 5.0 for e in tail):
        which = "G-1" if g1 > 9.0 else "G-2"
        return (
            f"SINGLE OUTLIER: WMA driven primarily by one bad start "
            f"({which} ERA > 9.0, remaining starts ERA < 5.0)"
        )
    if sum(1 for e in start_eras if e > 5.0) >= 3:
        return "SUSTAINED DECLINE: 3+ of last 5 starts ERA > 5.0"
    return "MIXED: some poor starts but no clear pattern"


def _fmt_ip(ip: float | None) -> str:
    if ip is None:
        return "?"
    return f"{float(ip):.1f}"


def build_report(con: sqlite3.Connection, ref_date: str) -> str:
    player_id, full_name = _find_player_id(con)
    starts = _load_starts(con, player_id)
    rolling = _load_rolling(con, player_id)
    season = _load_season_agg(con, player_id)

    stored_wma, ref_game_pk, prs_updated = _stored_wma_for_date(rolling, ref_date)

    # Prior qualifying starts entering ref_date (exclude same-day start if present)
    all_starts_asc = list(
        reversed(
            list(
                con.execute(
                    f"""
                    SELECT
                        {_GAME_DATE} AS game_date_et,
                        g.game_pk,
                        pgs.innings_pitched,
                        pgs.earned_runs
                    FROM player_game_stats pgs
                    JOIN games g ON g.game_pk = pgs.game_pk
                    WHERE pgs.player_id = ?
                      AND pgs.player_role = 'pitcher'
                      AND pgs.innings_pitched >= ?
                      AND g.season = 2026
                      AND ({_GAME_DATE}, g.game_pk) < (?, COALESCE(?, 0))
                    ORDER BY {_GAME_DATE} ASC, g.game_pk ASC
                    """,
                    (player_id, MIN_IP, ref_date, ref_game_pk or 0),
                ).fetchall()
            )
        )
    )
    wma_window = all_starts_asc[:5]
    wma_eras = [
        _start_era(r["innings_pitched"], r["earned_runs"])
        for r in wma_window
    ]
    wma_eras_clean = [e for e in wma_eras if e is not None]
    computed_wma = _wma(wma_eras_clean) if wma_eras_clean else None

    ts = datetime.now(tz=ET).strftime("%Y-%m-%d %I:%M %p ET")
    lines: list[str] = [
        "════════════════════════════════════════════════════",
        f"{full_name.upper()} — 2026 START DECOMPOSITION",
        f"Generated: {ts}",
        f"Player ID: {player_id}",
        f"Reference date (WMA entering): {ref_date}",
        "════════════════════════════════════════════════════",
        "",
        "SEASON SUMMARY (qualifying starts only, IP >= 3.0)",
        (
            f"Starts: {int(season['starts'] or 0)}  |  "
            f"IP: {float(season['total_ip'] or 0):.1f}  |  "
            f"Season ERA: {season['season_era']}"
        ),
        (
            f"Avg K/9: {season['avg_k9']}  |  "
            f"Avg WHIP: {season['avg_whip']}  |  "
            f"QS: {int(season['quality_starts'] or 0)}  |  "
            f"W-L: {int(season['wins'] or 0)}-{int(season['losses'] or 0)}"
        ),
        "",
        "LAST 8 STARTS (most recent first)",
        "────────────────────────────────────────────────────",
        f"{'Date':<12}{'Matchup':<16}{'IP':>5}{'ER':>4}{'ERA':>9}"
        f"{'K':>4}{'BB':>4}{'HR':>4}{'Pit':>5}{'QS':>4}{'W/L':>4}",
    ]

    for row in starts:
        qs = "Y" if int(row["quality_start"] or 0) else "N"
        lines.append(
            f"{str(row['game_date_et']):<12}"
            f"{_matchup(row):<16}"
            f"{_fmt_ip(row['innings_pitched']):>5}"
            f"{int(row['earned_runs'] or 0):>4}"
            f"{float(row['start_era'] or 0):>9.2f}"
            f"{int(row['strikeouts_pit'] or 0):>4}"
            f"{int(row['walks_allowed'] or 0):>4}"
            f"{int(row['hr_allowed'] or 0):>4}"
            f"{int(row['pitches_thrown'] or 0):>5}"
            f"{qs:>4}"
            f"{_wl(row):>4}"
        )

    if not starts:
        lines.append("  (no qualifying starts found)")

    lines.extend([
        "",
        "ROLLING WMA (pitcher_rolling_stats, entering each game)",
        "────────────────────────────────────────────────────",
        f"{'Date':<12}{'Agg':>7}{'Home':>7}{'Away':>7}{'nH':>4}{'nA':>4}",
    ])
    for row in rolling:
        era = f"{float(row['era_wma']):.2f}" if row["era_wma"] is not None else "n/a"
        eh = (
            f"{float(row['era_wma_home']):.2f}"
            if row["era_wma_home"] is not None
            else "n/a"
        )
        ea = (
            f"{float(row['era_wma_away']):.2f}"
            if row["era_wma_away"] is not None
            else "n/a"
        )
        lines.append(
            f"{str(row['game_date_et']):<12}"
            f"{era:>7}"
            f"{eh:>7}"
            f"{ea:>7}"
            f"{int(row['starts_in_window_home'] or 0):>4}"
            f"{int(row['starts_in_window_away'] or 0):>4}"
        )
    if not rolling:
        lines.append("  (no pitcher_rolling_stats rows for 2026)")

    lines.extend([
        "",
        f"WMA DECOMPOSITION (last 5 qualifying starts entering {ref_date})",
        "────────────────────────────────────────────────────",
        f"{'Weight':<8}{'Date':<12}{'Start ERA':>10}{'Contribution':>14}",
    ])

    if wma_window:
        for i, row in enumerate(wma_window):
            era = _start_era(row["innings_pitched"], row["earned_runs"])
            if era is None:
                continue
            w = WMA_WEIGHTS[i]
            contrib = round(w * era / WMA_DIVISOR, 2)
            lines.append(
                f"{w} (G-{i + 1})"
                f"  {str(row['game_date_et']):<12}"
                f"{era:>10.2f}"
                f"{contrib:>14.2f}"
            )
    else:
        lines.append("  (fewer than 1 prior qualifying start before reference date)")

    match = "YES"
    if computed_wma is None or stored_wma is None:
        match = "N/A"
    elif abs(computed_wma - stored_wma) > 0.02:
        match = "NO"

    lines.extend([
        "──────────────────────────────────",
        (
            f"Computed WMA: {computed_wma if computed_wma is not None else 'n/a'}"
            f"  |  Stored WMA: {stored_wma if stored_wma is not None else 'n/a'}"
            f"  |  Match: {match}"
        ),
    ])
    if prs_updated:
        lines.append(f"Stored row updated_at: {prs_updated}")
    if ref_game_pk:
        lines.append(f"Reference game_pk: {ref_game_pk}")

    # WMA evolution note
    if len(rolling) >= 2:
        newest = rolling[0]
        oldest = rolling[-1]
        e_new = newest["era_wma"]
        e_old = oldest["era_wma"]
        if e_new is not None and e_old is not None:
            lines.extend([
                "",
                "WMA TRAJECTORY",
                f"  Latest ({newest['game_date_et']}): {float(e_new):.2f}",
                f"  Oldest in sample ({oldest['game_date_et']}): {float(e_old):.2f}",
                f"  Change: {float(e_new) - float(e_old):+.2f}",
            ])

    lines.extend([
        "",
        "DIAGNOSIS",
        "─────────────────────────────────────────────────────",
        _diagnose([e for e in wma_eras if e is not None]),
    ])

    # Explain 4.11 -> 7.12 jump if both dates present in rolling table
    wma_by_date = {str(r["game_date_et"]): r for r in rolling}
    row_611 = wma_by_date.get("2026-06-11")
    row_618 = wma_by_date.get(ref_date)
    if row_611 and row_618 and row_611["era_wma"] is not None and row_618["era_wma"] is not None:
        old_wma = float(row_611["era_wma"])
        new_wma = float(row_618["era_wma"])
        lines.extend([
            "",
            "WMA CHANGE (2026-06-11 entering -> 2026-06-18 entering)",
            f"  4.11 observed 2026-06-10 context -> stored entering 2026-06-11: {old_wma:.2f}",
            f"  Stored entering {ref_date}: {new_wma:.2f}  (delta {new_wma - old_wma:+.2f})",
            "  Primary driver: 2026-06-11 @ BAL — 5.0 IP, 7 ER, 12.60 start ERA",
            "    (G-1 weight 5 contributes 4.20 of 7.12 total WMA)",
            "  Window roll: dropped 2026-05-18 shutout (0.00 ERA) from G-5;",
            "    added 2026-06-11 blowup (12.60 ERA) at G-1.",
        ])

    lines.append("════════════════════════════════════════════════════")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Bryan Woo 2026 ERA WMA decomposition")
    parser.add_argument("--db", default=None, help="Path to mlb_stats.db")
    parser.add_argument(
        "--reference-date",
        default=DEFAULT_REF_DATE,
        help=f"Date entering which to decompose WMA (default {DEFAULT_REF_DATE})",
    )
    args = parser.parse_args()

    db_path = Path(args.db) if args.db else get_db_path()
    con = db_connect(str(db_path))
    try:
        report = build_report(con, args.reference_date)
    finally:
        con.close()

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(report, encoding="utf-8")
    try:
        print(report)
    except UnicodeEncodeError:
        print(report.encode("ascii", errors="replace").decode("ascii"))
    print(f"[pitcher_decomp_woo_2026] Report saved to {OUT_PATH}")


if __name__ == "__main__":
    main()
