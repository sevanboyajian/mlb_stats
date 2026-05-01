#!/usr/bin/env python3
"""
backtest_contrarian_dog.py
───────────────────────────
Historical backtest: contrarian underdog ROI in games classified as proxy
"No Signal" under a simplified reproduction of core brief structural gates.

IMPORTANT — PROXY v1 (NOT BRIEF-IDENTICAL)
─────────────────────────────────────────
Membership in the \"No Signal\" pool is approximate. Structural checks mirror
production where practical (OWM / LHP_FADE thresholds, MV-F = wind IN, MV-B /
H3b wind-OUT scaffolding), but edge scoring, diversification, completeness
blocks, and several secondary gates from ``score_game`` are omitted. Mis-
classification affects a small minority of games; headline ROI should be read
directionally only.

POINT-IN-TIME DATA
──────────────────
- Odds: earliest ``game_odds`` row **before first pitch**
  (``captured_at_utc < games.game_start_utc`` when start time is known).
- Team / pitcher WMA: ``team_rolling_stats`` and ``pitcher_rolling_stats`` are
  keyed by ``(game_pk, …)`` and are built as **pre-game** metrics for that game
  (see ``build_team_wma.py`` / ``build_pitcher_wma.py``). Joining them is the
  no-lookahead analogue of restricting to starts before ``game_date``.

Dependencies: ``games``, ``venues``, ``game_odds``, ``game_probable_pitchers``,
``players``, ``team_rolling_stats`` (rolling_ops_wma), ``pitcher_rolling_stats``.

Usage (from repo root):
  python -m batch.pipeline.backtest_contrarian_dog --seasons 2024 2025

  Single year::
    python -m batch.pipeline.backtest_contrarian_dog --seasons 2025

  July-August only (pooled across listed seasons)::
    python -m batch.pipeline.backtest_contrarian_dog --seasons 2024 2025 --months 7 8

  Write custom report paths::
    python -m batch.pipeline.backtest_contrarian_dog --seasons 2025 \\
        --output reports/contrarian_dog_2025.txt

  Phase 2 conditional filters (A-D) append automatically when ``--seasons`` includes
  2024 and/or 2025; omitted when only other years (e.g. 2026-only).
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.connection import connect as db_connect, get_db_path

from batch.pipeline.edge_utils import EDGE_MIN, american_to_implied_prob


# ── Local mirrors of scorer constants (avoid tight coupling imports on hot path)
OWM_OPS_MIN = 0.78
OWM_ERA_MIN = 5.0
OWM_MONTHS = {4, 5, 6, 7, 8, 9}
OWM_HOME_ML_CAP = -275

LHP_HEAVY_LOW = 0.60
LHP_HEAVY_HIGH = 0.67
LHP_MONTHS = {4, 5, 6, 7, 8}

WIND_MPH_MIN = 10
H3B_PF_MIN = 98

_OK_MONTH_CORE = {4, 5, 6, 7, 8, 9}


def _lazy_h3b_whitelist() -> frozenset[str]:
    from batch.pipeline.generate_daily_brief import H3B_PARK_WHITELIST

    return frozenset(H3B_PARK_WHITELIST)


def _game_month(game_date_et: str) -> int:
    try:
        return int((game_date_et or "")[5:7])
    except (ValueError, IndexError):
        return 0


def _wind_label_from_str(direction: str | None) -> str:
    if not direction:
        return "UNKNOWN"
    d = str(direction).upper()
    if any(k in d for k in ("OUT", "BLOWING OUT")):
        return "OUT"
    if any(k in d for k in ("IN", "BLOWING IN")):
        return "IN"
    if any(k in d for k in ("CROSS", "LEFT", "RIGHT")):
        return "CROSS"
    if any(k in d for k in ("CALM", "STILL", "0 MPH")):
        return "CALM"
    return "UNKNOWN"


def _is_oracle_park(name: str) -> bool:
    return "ORACLE" in name.upper()


def _is_dome(roof_type: str | None) -> bool:
    return (roof_type or "").strip().lower() == "dome"


def _venue_bucket(roof_type: str | None, wind_effect: str | None, name: str) -> str:
    if _is_dome(roof_type):
        return "dome"
    we = (wind_effect or "").strip().upper()
    if _is_oracle_park(name):
        return "wind-neutral"
    if we in ("HIGH", "MODERATE"):
        return "wind-active"
    return "wind-neutral"


def ml_pnl_units(am: int, won: bool) -> float:
    if not won:
        return -1.0
    a = int(am)
    if a > 0:
        return a / 100.0
    return 100.0 / float(-a)


def implied_to_breakeven_pct(impl: float) -> float:
    return impl * 100.0


@dataclass
class PackedGame:
    game_pk: int
    game_date_et: str
    season: int
    home_abbr: str
    away_abbr: str
    venue_name: str
    roof_type: str | None
    wind_effect: str | None
    park_factor_runs: float
    home_score: int
    away_score: int
    wind_mph: float | None
    wind_dir_raw: str | None
    home_ops_wma: float | None
    home_trs_games: int | None
    away_era_wma: float | None
    away_starts_win: int | None
    away_throws: str | None
    hm_open: int | None
    aw_open: int | None
    total_open: float | None
    data_quality: str
    completeness_notes: list[str] = field(default_factory=list)

    structural_owm: bool = False
    structural_lhp: bool = False
    structural_mvf: bool = False
    structural_mvb: bool = False
    structural_h3b: bool = False

    @property
    def any_signal(self) -> bool:
        return any(
            (
                self.structural_owm,
                self.structural_lhp,
                self.structural_mvf,
                self.structural_mvb,
                self.structural_h3b,
            )
        )

    @property
    def wind_label(self) -> str:
        try:
            mph = float(self.wind_mph) if self.wind_mph is not None else None
        except (TypeError, ValueError):
            mph = None
        if mph is None or mph < WIND_MPH_MIN:
            return "UNKNOWN"
        return _wind_label_from_str(self.wind_dir_raw)


def fetch_games(con, seasons: tuple[int, ...], months: frozenset[int]) -> list[PackedGame]:
    """Load Final regular-season rows for ``seasons`` limited to calendar ``months`` (4–9 typical)."""
    ph = ",".join(["?"] * len(seasons))
    month_clause = ",".join(str(m) for m in sorted(months))
    sql = f"""
        SELECT
            g.game_pk,
            COALESCE(NULLIF(TRIM(g.game_date_et), ''), g.game_date) AS game_date_et,
            g.season,
            th.abbreviation  AS home_abbr,
            ta.abbreviation  AS away_abbr,
            v.name           AS venue_name,
            v.roof_type      AS roof_type,
            v.wind_effect    AS wind_effect,
            COALESCE(v.park_factor_runs, 100) AS park_factor_runs,
            g.home_score,
            g.away_score,
            g.wind_mph,
            g.wind_direction,
            trs.rolling_ops_wma AS home_ops_wma,
            trs.games_in_window AS home_trs_games,
            prs.era_wma         AS away_era_wma,
            prs.starts_in_window AS away_starts_win,
            pl.throws           AS away_throws
        FROM games g
        JOIN teams th ON th.team_id = g.home_team_id
        JOIN teams ta ON ta.team_id = g.away_team_id
        LEFT JOIN venues v ON v.venue_id = g.venue_id
        LEFT JOIN team_rolling_stats trs
            ON trs.game_pk = g.game_pk AND trs.team_id = g.home_team_id
        LEFT JOIN game_probable_pitchers gpp
            ON gpp.game_pk = g.game_pk AND gpp.team_id = g.away_team_id
        LEFT JOIN pitcher_rolling_stats prs
            ON prs.game_pk = g.game_pk AND prs.player_id = gpp.player_id
        LEFT JOIN players pl ON pl.player_id = gpp.player_id
        WHERE g.season IN ({ph})
          AND g.game_type = 'R'
          AND g.status = 'Final'
          AND g.home_score IS NOT NULL
          AND g.away_score IS NOT NULL
          AND CAST(SUBSTR(COALESCE(NULLIF(TRIM(g.game_date_et), ''), g.game_date), 6, 2) AS INTEGER)
              IN ({month_clause})
        ORDER BY game_date_et, g.game_pk
    """
    cur = con.execute(sql, seasons)
    cols = list(cur.description or [])
    col_names = [d[0] for d in cols]
    rows = cur.fetchall()
    idx = {c: i for i, c in enumerate(col_names)}

    out: list[PackedGame] = []
    for r in rows:
        notes: list[str] = []

        hops = r[idx["home_ops_wma"]]
        hgames = r[idx["home_trs_games"]]
        aera = r[idx["away_era_wma"]]
        ast = r[idx["away_starts_win"]]

        dq = "complete"
        if hops is None or hgames is None or int(hgames or 0) < 2:
            dq = "incomplete"
            notes.append("home_ops_wma_missing")
        if aera is None or ast is None or int(ast or 0) < 2:
            dq = "incomplete"
            notes.append("away_pitcher_wma_missing")

        out.append(
            PackedGame(
                game_pk=int(r[idx["game_pk"]]),
                game_date_et=str(r[idx["game_date_et"]]),
                season=int(r[idx["season"]]),
                home_abbr=str(r[idx["home_abbr"]]),
                away_abbr=str(r[idx["away_abbr"]]),
                venue_name=str(r[idx["venue_name"] or ""]),
                roof_type=r[idx["roof_type"]],
                wind_effect=r[idx["wind_effect"]],
                park_factor_runs=float(r[idx["park_factor_runs"]] or 100),
                home_score=int(r[idx["home_score"]]),
                away_score=int(r[idx["away_score"]]),
                wind_mph=float(r[idx["wind_mph"]])
                if r[idx["wind_mph"]] is not None
                else None,
                wind_dir_raw=r[idx["wind_direction"]],
                home_ops_wma=float(hops) if hops is not None else None,
                home_trs_games=int(hgames) if hgames is not None else None,
                away_era_wma=float(aera) if aera is not None else None,
                away_starts_win=int(ast) if ast is not None else None,
                away_throws=str(r[idx["away_throws"]]).strip().upper()
                if r[idx["away_throws"]]
                else None,
                hm_open=None,
                aw_open=None,
                total_open=None,
                data_quality=dq,
                completeness_notes=notes,
            )
        )
    return out


def _pregame_snapshot(captured_utc: str | None, game_start_utc: str | None) -> bool:
    if not captured_utc or not game_start_utc:
        return True
    return str(captured_utc).strip() < str(game_start_utc).strip()


def load_opening_lines(
    con, game_pks: list[int], game_start_by_pk: dict[int, str | None]
) -> dict[int, dict[str, object]]:
    if not game_pks:
        return {}
    ph = ",".join(["?"] * len(game_pks))
    cur = con.execute(
        f"""
        SELECT game_pk, market_type, home_ml, away_ml, total_line, captured_at_utc
        FROM game_odds
        WHERE game_pk IN ({ph})
          AND market_type IN ('moneyline', 'total')
        ORDER BY game_pk, market_type, captured_at_utc ASC
        """,
        game_pks,
    )
    first: dict[int, dict[str, object]] = {}

    for row in cur:
        gpk = int(row[0])
        mkt = str(row[1])
        cap = row[5]
        gs = game_start_by_pk.get(gpk)
        if not _pregame_snapshot(str(cap) if cap is not None else None, str(gs) if gs else None):
            continue

        if gpk not in first:
            first[gpk] = {}
        slot = first[gpk]
        if mkt == "moneyline" and "home_ml" not in slot:
            slot["home_ml"] = row[2]
            slot["away_ml"] = row[3]
        elif mkt == "total" and "total_line" not in slot:
            slot["total_line"] = row[4]
    return first


def score_structural_flags(g: PackedGame, h3b_wl: frozenset[str]) -> None:
    month = _game_month(g.game_date_et)
    we = (g.wind_effect or "").strip().upper()
    mph_ok = g.wind_mph is not None and float(g.wind_mph) >= WIND_MPH_MIN
    wl = g.wind_label
    wind_in = mph_ok and wl == "IN"
    wind_out = mph_ok and wl == "OUT"

    hi = american_to_implied_prob(g.hm_open) if g.hm_open is not None else None
    ai = american_to_implied_prob(g.aw_open) if g.aw_open is not None else None

    venue_structural = not _is_dome(g.roof_type) and not _is_oracle_park(g.venue_name)

    # OWM (structural only — matches core numeric + month + simple market caps)
    owm = False
    if (
        g.home_ops_wma is not None
        and g.away_era_wma is not None
        and g.away_starts_win is not None
        and int(g.away_starts_win) >= 2
        and g.home_trs_games is not None
        and int(g.home_trs_games) >= 2
        and month in OWM_MONTHS
        and g.hm_open is not None
        and int(g.hm_open) >= OWM_HOME_ML_CAP
        and hi is not None
        and float(hi) >= 0.40
    ):
        owm = float(g.home_ops_wma) >= OWM_OPS_MIN and float(g.away_era_wma) >= OWM_ERA_MIN
    g.structural_owm = owm

    # LHP_FADE structural (no platoon / S1H2 / completeness blocks in v1)
    lhp = False
    if (
        month in LHP_MONTHS
        and (g.away_throws or "") == "L"
        and hi is not None
        and LHP_HEAVY_LOW <= float(hi) <= LHP_HEAVY_HIGH
    ):
        lhp = True
    g.structural_lhp = lhp

    # MV-F: wind IN, wind_effect HIGH (matches _wind_eligible), venue proxy
    mvf = bool(
        venue_structural
        and we == "HIGH"
        and wind_in
    )
    g.structural_mvf = mvf

    # MV-B proxy: wind OUT, moderate/high effect, PF gate, venue list, total present
    mvb = bool(
        venue_structural
        and we in ("HIGH", "MODERATE")
        and wind_out
        and g.park_factor_runs >= H3B_PF_MIN
        and g.venue_name in h3b_wl
        and g.total_open is not None
    )
    g.structural_mvb = mvb

    # H3b standalone (when MV-B absent in scorer this can still fire)
    h3b = bool(
        we == "HIGH"
        and wind_out
        and g.venue_name in h3b_wl
        and g.park_factor_runs >= H3B_PF_MIN
        and g.total_open is not None
    )
    g.structural_h3b = h3b


@dataclass
class DogView:
    dog_side: str  # 'home' | 'away'
    dog_ml: int
    fav_ml: int
    dog_won: bool


def dog_view_from_game(g: PackedGame) -> DogView | None:
    if g.hm_open is None or g.aw_open is None:
        return None
    hi = american_to_implied_prob(int(g.hm_open))
    ai = american_to_implied_prob(int(g.aw_open))
    if hi is None or ai is None:
        return None

    home_win = g.home_score > g.away_score
    if float(hi) < float(ai):
        side = "home"
        dml = int(g.hm_open)
        fml = int(g.aw_open)
        won = home_win
    else:
        side = "away"
        dml = int(g.aw_open)
        fml = int(g.hm_open)
        won = not home_win
    return DogView(dog_side=side, dog_ml=dml, fav_ml=fml, dog_won=won)


def dog_bucket(ml: int, bp: tuple[int, int, int] = (120, 150, 200)) -> str:
    b1, b2, b3 = bp
    if ml < b1:
        return f"<+{b1}"
    if ml < b2:
        return f"+{b1}-{b2 - 1}"
    if ml < b3:
        return f"+{b2}-{b3 - 1}"
    return f"+{b3}+"


def bucket_order(bp: tuple[int, int, int]) -> list[str]:
    b1, b2, b3 = bp
    return [f"<+{b1}", f"+{b1}-{b2 - 1}", f"+{b2}-{b3 - 1}", f"+{b3}+"]


def bucket_filter(ml: int, lo: int, hi: int) -> bool:
    if lo <= 0:
        return True
    return lo <= ml < hi


@dataclass
class BucketStats:
    n: int = 0
    wins: int = 0
    losses: int = 0
    units: float = 0.0
    odds_sum: float = 0.0

    def add(self, dog_ml: int, won: bool) -> None:
        self.n += 1
        if won:
            self.wins += 1
        else:
            self.losses += 1
        self.units += ml_pnl_units(dog_ml, won)
        self.odds_sum += float(dog_ml)

    def lines(self, label: str) -> str:
        if self.n <= 0:
            return f"{label:12} {'0':>5}  {'-':>5}"

        avg_odds = self.odds_sum / self.n
        be = american_to_implied_prob(int(round(avg_odds)))
        be_pct = implied_to_breakeven_pct(be) if be is not None else 0.0
        wp = self.wins / self.n * 100.0
        edge = wp - be_pct if be is not None else 0.0
        roi = self.units / self.n * 100.0

        nn = ""
        if self.n < 20:
            nn = "  [small sample N<20]"
        return (
            f"{label:14} {self.n:5d} {self.wins:4d}{self.losses:4d} "
            f"{wp:6.1f}% {be_pct:6.1f}% {edge:+6.1f}% "
            f"{self.units:+7.2f}u {roi:+7.1f}%{nn}"
        )


def run_validation_2026_sample(con, lines_out: list[str]) -> None:
    try:
        chk_start = dt.date(2026, 4, 25)
        chk_end = dt.date(2026, 4, 30)
        n = con.execute(
            """SELECT COUNT(*) FROM games
               WHERE game_date BETWEEN ? AND ? AND season = 2026""",
            (chk_start.isoformat(), chk_end.isoformat()),
        ).fetchone()[0]
        if not n:
            return
        lines_out.append("")
        lines_out.append("APR 2026 PARITY NOTE (sparse)")
        lines_out.append("2026Apr sample present -- compare live brief vs proxy externally; scorer path not replayed.")
    except Exception:
        return


# Phase 2: conditional slices (2024/2025 historical only; see main())
PHASE2_SEASONS: frozenset[int] = frozenset({2024, 2025})
P2_SMALL_SEASON = 20
P2_VERDICT_N = 30


def _p2_metrics(pairs: list[tuple[PackedGame, DogView]]) -> dict[str, float | int]:
    n = len(pairs)
    if n == 0:
        return {
            "n": 0,
            "w": 0,
            "l": 0,
            "win_pct": 0.0,
            "be_pct": 0.0,
            "edge": 0.0,
            "roi": 0.0,
            "units": 0.0,
        }
    w = sum(1 for _, d in pairs if d.dog_won)
    units = sum(ml_pnl_units(d.dog_ml, d.dog_won) for _, d in pairs)
    avg_ml = sum(d.dog_ml for _, d in pairs) / n
    be_impl = american_to_implied_prob(int(round(avg_ml)))
    win_pct = 100.0 * w / n
    be_pct = implied_to_breakeven_pct(be_impl) if be_impl else 0.0
    edge = win_pct - be_pct
    roi = 100.0 * units / n
    return {
        "n": n,
        "w": w,
        "l": n - w,
        "win_pct": win_pct,
        "be_pct": be_pct,
        "edge": edge,
        "roi": roi,
        "units": units,
    }


def _p2_season_line(label: str, m: dict[str, float | int]) -> str:
    if m["n"] == 0:
        return f"  {label}:  0 games  --"
    flag = f"  [!] Small sample (N={m['n']})" if m["n"] < P2_SMALL_SEASON else ""
    return (
        f"  {label}:  {int(m['n']):3d} games  {int(m['w']):2d}W-{int(m['l']):2d}L  "
        f"win% {m['win_pct']:.1f}%  BE% {m['be_pct']:.1f}%  "
        f"edge {m['edge']:+.1f}%  ROI {m['roi']:+.1f}%{flag}"
    )


def _p2_combined_line(m: dict[str, float | int]) -> str:
    if m["n"] == 0:
        return "  Combined:  0 games  --"
    return (
        f"  Combined:  {int(m['n']):3d} games  {int(m['w']):2d}W-{int(m['l']):2d}L  "
        f"ROI {m['roi']:+.1f}%"
    )


def _p2_reading(m: dict[str, float | int]) -> str:
    if m["n"] < P2_SMALL_SEASON:
        return "INCONCLUSIVE"
    if m["roi"] <= 0 or m["edge"] < 0:
        return "NOT SUPPORTING"
    if m["roi"] > 0 and m["edge"] > 0:
        return "CONSISTENT WITH EDGE"
    return "INCONCLUSIVE"


def _p2_table_verdict(
    n_by_season: dict[int, int],
    season_allow: frozenset[int],
    m_combined: dict[str, float | int],
) -> str:
    if m_combined["n"] == 0:
        return "SMALL"
    for y in (2024, 2025):
        if y in season_allow and n_by_season.get(y, 0) < P2_VERDICT_N:
            return "SMALL"
    if m_combined["roi"] <= 0 or m_combined["edge"] < 0:
        return "FAIL"
    if m_combined["roi"] > 5.0:
        return "PASS"
    return "FAIL"


def main() -> None:
    p = argparse.ArgumentParser(description="Contrarian dog backtest (proxy No Signal v1).")
    p.add_argument("--seasons", nargs="+", type=int, default=[2024, 2025])
    p.add_argument("--min-dog-odds", type=int, default=150)
    p.add_argument("--max-dog-odds", type=int, default=9999)
    p.add_argument(
        "--output",
        default="reports/contrarian_dog_backtest.txt",
        help="TXT report path (under repo)",
    )
    p.add_argument(
        "--csv-out",
        default="reports/contrarian_dog_backtest_detail.csv",
        help="Per-game CSV path",
    )
    p.add_argument(
        "--all-games",
        action="store_true",
        help="Ignore No-Signal filter (control group on full slate)",
    )
    p.add_argument(
        "--bucket-breakpoints",
        type=int,
        nargs=3,
        metavar=("B1", "B2", "B3"),
        default=[120, 150, 200],
        help="Dog ML bucket boundaries (positive American ladder)",
    )
    p.add_argument(
        "--months",
        nargs="+",
        type=int,
        default=None,
        metavar="M",
        help=(
            "Calendar months to include (Apr=4 … Sep=9). Default Apr–Sep. "
            "Pooled across every `--seasons` value, e.g. `--seasons 2024 2025 --months 7` "
            "is July 2024 plus July 2025."
        ),
    )
    p.add_argument("--db", default=None, help="Override path to mlb_stats.db")

    args = p.parse_args()
    seasons = tuple(sorted(set(args.seasons)))

    allowed_months = frozenset(range(4, 10))
    if args.months:
        ms = frozenset(args.months)
        if not ms <= allowed_months:
            unknown = sorted(ms - allowed_months)
            sys.exit(f"--months must be within 4-9 (Apr-Sep). Invalid: {unknown}")
        months_filter = ms
    else:
        months_filter = frozenset(_OK_MONTH_CORE)
    bp_raw = list(args.bucket_breakpoints)
    if len(bp_raw) != 3:
        sys.exit("--bucket-breakpoints requires exactly three ascending odds cutoffs")
    breakpoints_t = (bp_raw[0], bp_raw[1], bp_raw[2])
    if breakpoints_t != tuple(sorted(breakpoints_t)):
        sys.exit("--bucket-breakpoints must be strictly ascending (e.g. 120 150 200)")
    bk1, bk2, bk3 = breakpoints_t

    dbp = Path(args.db) if args.db else Path(get_db_path())
    reports_dir_txt = Path(_REPO_ROOT / args.output).parent
    reports_dir_txt.mkdir(parents=True, exist_ok=True)
    csv_path = Path(_REPO_ROOT / args.csv_out)

    lines: list[str] = []

    stamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M local")

    lines.append("")
    lines.append("=" * 80)
    lines.append("  CONTRARIAN DOG BACKTEST - PROXY NO-SIGNAL v1 (NOT BRIEF-IDENTICAL)")
    lines.append(f"  Seasons: {', '.join(str(s) for s in seasons)}")
    month_names = {4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep"}
    m_hdr = ", ".join(month_names[m] for m in sorted(months_filter))
    lines.append(f"  Calendar months: {m_hdr}")
    lines.append(f"  Generated: {stamp}")
    lines.append(
        f"  Filter: {'All games (control)' if args.all_games else 'Proxy no-signal only'} "
        f"| EDGE_MIN ref={EDGE_MIN} (informational)"
    )
    lines.append("")
    lines.append(
        "  Wind gates: MV-F => wind IN >=10mph at HIGH wind-effect outdoor parks "
        "(dome / Oracle excluded). MV-B / H3b proxy => wind OUT >=10mph, "
        "HIGH/MODERATE effect, PF>=98, H3b whitelist venues, opening total present."
    )
    lines.append("=" * 80)

    h3b_wl = _lazy_h3b_whitelist()
    con = db_connect(str(dbp))

    games = fetch_games(con, seasons, months_filter)

    gs_map: dict[int, str | None] = {}
    if games:
        starters = ",".join(["?"] * len(games))
        start_rows = con.execute(
            f"SELECT game_pk, game_start_utc FROM games WHERE game_pk IN ({starters})",
            [g.game_pk for g in games],
        ).fetchall()
        gs_map = {int(r[0]): r[1] for r in start_rows}

    openings = load_opening_lines(con, [g.game_pk for g in games], gs_map)
    skipped_odds = 0
    for g in games:
        o = openings.get(g.game_pk, {})
        g.hm_open = int(o["home_ml"]) if o.get("home_ml") is not None else None
        g.aw_open = int(o["away_ml"]) if o.get("away_ml") is not None else None
        tl = o.get("total_line")
        g.total_open = float(tl) if tl is not None else None
        if g.hm_open is None or g.aw_open is None:
            skipped_odds += 1

    dq_complete_games = sum(1 for g in games if g.data_quality == "complete")

    enriched: list[PackedGame] = []
    for ig, g in enumerate(games):
        if (ig + 1) % 500 == 0:
            print(f"Processing game {ig + 1}/{len(games)} ({g.game_date_et})...", flush=True)
        if g.hm_open is None or g.aw_open is None:
            continue
        score_structural_flags(g, h3b_wl)
        enriched.append(g)

    nosig = [g for g in enriched if not g.any_signal]
    slate = enriched if args.all_games else nosig

    def dog_rows(gs: list[PackedGame]) -> list[tuple[PackedGame, DogView]]:
        out: list[tuple[PackedGame, DogView]] = []
        for g in gs:
            dv = dog_view_from_game(g)
            if dv is None:
                continue
            out.append((g, dv))
        return out

    paired = dog_rows(slate)

    hi_thr = args.max_dog_odds + 1
    lo_thr = args.min_dog_odds

    hypo_pairs = [(g, d) for g, d in paired if bucket_filter(d.dog_ml, lo_thr, hi_thr)]

    lines.append("")
    lines.append("SUMMARY")
    lines.append("-" * 40)
    lines.append(f"Total schedule rows fetched:       {len(games):>6d}")
    lines.append(f"Rows with earliest pregame ML:     {len(enriched):>6d}")
    lines.append(f"Rows dropped (missing ML):          {skipped_odds:>6d}")
    lines.append(f"Proxy no-signal games:              {len(nosig):>6d}")
    lines.append(f"Games analyzed (selected filter):   {len(paired):>6d}")
    lines.append(f"Dog in [{args.min_dog_odds:+d},{args.max_dog_odds}] on filter:{len(hypo_pairs):>6d}")

    lines.append("")
    lines.append("DATA QUALITY")
    lines.append("-" * 40)
    dq_enriched = sum(1 for g in enriched if g.data_quality == "complete")
    lines.append(
        f"Full WMA join (home OPS + away SP, pre-game): {dq_enriched:>5d} / {len(enriched)} with odds"
    )
    lines.append(
        f"Full WMA join (schedule fetch):                 {dq_complete_games:>5d} / {len(games)}"
    )
    incom = len(games) - dq_complete_games
    lines.append(f"Schedule rows missing home/SP WMA gate:         {incom:>5d}")
    lines.append("")
    lines.append("* WMA keyed by game_pk reflects pre-game state for that slate game.")

    def section_pairs(title: str, prs: list[tuple[PackedGame, DogView]]) -> None:
        lines.append("")
        lines.append(title)
        lines.append("-" * len(title))

        agg = defaultdict(BucketStats)
        units_total = 0.0
        for _, d in prs:
            key = dog_bucket(d.dog_ml, breakpoints_t)
            agg[key].add(d.dog_ml, d.dog_won)
            units_total += ml_pnl_units(d.dog_ml, d.dog_won)

        n = len(prs)
        if not n:
            lines.append("(no rows)")
            return

        ws = sum(1 for _, d in prs if d.dog_won)
        avg_ml = sum(d.dog_ml for _, d in prs) / n
        be_impl = american_to_implied_prob(int(round(avg_ml)))
        wp = ws / n * 100
        be_pct = implied_to_breakeven_pct(be_impl) if be_impl else 0.0

        roi = units_total / n * 100.0 if n else 0.0
        lines.append(f"Dog record:           {ws}W {n - ws}L  ({wp:.1f}%)")
        lines.append(f"Breakeven (avg odds): {be_pct:.1f}%")
        lines.append(f"Edge vs BE:           {wp - be_pct:+.1f}%")
        lines.append(f"Units P/L (flat 1u):  {units_total:+.2f}u")
        lines.append(f"ROI per game bet:    {roi:+.1f}%")

        verdict = (
            "INCONCLUSIVE -- need more data"
            if n < 80
            else (
                "CONSISTENT with positive edge"
                if roi > 0 and wp > be_pct + 1
                else "NOT SUPPORTING positive edge"
                if roi <= 0
                else "MIXED -- directional noise"
            )
        )
        lines.append(f"Reading:              {verdict}")

        lines.append("")
        lines.append(f"Breakpoints: <{bk1}, {bk1}-{bk2}, {bk2}-{bk3}, {bk3}+")
        order = bucket_order(breakpoints_t)
        lines.append(
            f"{'Bucket':14} {'N':>5} {'W':>4}{'L':>4}{'Win%':>8}{'BE%':>8}{'Edge':>8}{'P&L':>9}{'ROI':>8}"
        )
        for k in order:
            if k not in agg:
                continue
            lines.append(agg[k].lines(k))

        # Season splits
        by_season: dict[int, list[tuple[PackedGame, DogView]]] = defaultdict(list)
        for g, d in prs:
            by_season[g.season].append((g, d))
        lines.append("")
        lines.append("SEASON SPLIT")
        lines.append("------------")
        for s in sorted(by_season):
            nn = len(by_season[s])
            uu = 0.0
            for _, d in by_season[s]:
                uu += ml_pnl_units(d.dog_ml, d.dog_won)
            roi_s = uu / nn * 100 if nn else 0
            lines.append(
                f"  {s}: n={nn:4d}  dog win%={100*sum(1 for _,d in by_season[s] if d.dog_won)/nn:.1f}%  roi={roi_s:+.2f}%"
            )

        # Month split April–September
        by_month: dict[int, list[tuple[PackedGame, DogView]]] = defaultdict(list)
        for g, d in prs:
            by_month[_game_month(g.game_date_et)].append((g, d))
        lines.append("")
        lines.append("MONTH SPLIT")
        lines.append("-----------")
        for m in range(4, 10):
            rows_m = by_month.get(m, [])
            if not rows_m:
                continue
            wt = sum(1 for _, d in rows_m if d.dog_won)
            nn = len(rows_m)
            uu = sum(ml_pnl_units(d.dog_ml, d.dog_won) for _, d in rows_m)
            roi_m = uu / nn * 100
            nm = dt.date(2000, m, 1).strftime("%B")
            lines.append(f"  {nm}: n={nn:4d}  {wt}W-{nn-wt}L  roi={roi_m:+.2f}%")

        # Home / away dogs
        hdog = [(g, d) for g, d in prs if d.dog_side == "home"]
        adog = [(g, d) for g, d in prs if d.dog_side == "away"]
        lines.append("")
        lines.append("HOME vs AWAY DOG")
        lines.append("----------------")
        for label, grp in (
            ("Home dog", hdog),
            ("Away dog", adog),
        ):
            nn = len(grp)
            if nn == 0:
                lines.append(f"  {label}: --")
                continue
            ww = sum(1 for _, d in grp if d.dog_won)
            uu = sum(ml_pnl_units(d.dog_ml, d.dog_won) for _, d in grp)
            lines.append(f"  {label}: {ww}W-{nn - ww}L  roi={uu/nn*100:+.2f}%")

        lines.append("")
        lines.append("VENUE TYPE SPLIT")
        lines.append("----------------")
        vb: dict[str, list[tuple[PackedGame, DogView]]] = defaultdict(list)
        for g, d in prs:
            vb[
                _venue_bucket(g.roof_type, g.wind_effect, g.venue_name)
            ].append((g, d))
        for vn in sorted(vb.keys()):
            grp = vb[vn]
            nn = len(grp)
            ww = sum(1 for _, d in grp if d.dog_won)
            uu = sum(ml_pnl_units(d.dog_ml, d.dog_won) for _, d in grp)
            lines.append(f"  {vn:14} n={nn:4d}  {ww}W-{nn-ww}L  roi={uu/nn*100:+.2f}%")

        # Wind-out subcohort inside filter
        wind_out_pairs = [(g, d) for g, d in prs if g.structural_h3b or g.structural_mvb]
        lines.append("")
        lines.append(f"WIND-OUT QUALIFIED STRUCTURAL ROWS (within filter): {len(wind_out_pairs)}")
        if wind_out_pairs:
            uu = sum(ml_pnl_units(d.dog_ml, d.dog_won) for _, d in wind_out_pairs)
            nn = len(wind_out_pairs)
            lines.append(f"  (... dog betting note: naive dog ROI inside that slice: {uu/nn*100:+.2f}%)")

    section_pairs(f"PRIMARY - dogs ML in [{args.min_dog_odds}, {args.max_dog_odds}] (opening)", hypo_pairs)

    trap = [(g, d) for g, d in paired if bucket_filter(d.dog_ml, bk1, bk2)]
    section_pairs(
        f"SUB - +{bk1}-{bk2 - 1} trap band on proxy No-Signal",
        trap,
    )

    high_tot = [(g, d) for g, d in hypo_pairs if g.total_open is not None and g.total_open >= 9.0]
    low_tot = [(g, d) for g, d in hypo_pairs if g.total_open is not None and g.total_open <= 7.0]

    lines.append("")
    lines.append(f"HIGH TOTAL OPEN >=9 ({len(high_tot)})")
    if high_tot:
        uu = sum(ml_pnl_units(d.dog_ml, d.dog_won) for _, d in high_tot)
        lines.append(f"  roi={uu/len(high_tot)*100:+.2f}%")

    lines.append("")
    lines.append(f"LOW TOTAL OPEN <=7 ({len(low_tot)})")
    if low_tot:
        uu = sum(ml_pnl_units(d.dog_ml, d.dog_won) for _, d in low_tot)
        lines.append(f"  roi={uu/len(low_tot)*100:+.2f}%")

    ctrl = [(g, d) for g, d in dog_rows(enriched) if bucket_filter(d.dog_ml, lo_thr, hi_thr)]

    lines.append("")
    lines.append("SUB-ANALYSIS: ALL SCHEDULE ROWS WITH ODDS - matched dog threshold (control)")
    lines.append("-" * 74)
    if not ctrl:
        lines.append("(none)")
    else:
        ww = sum(1 for _, d in ctrl if d.dog_won)
        nn = len(ctrl)
        uu = sum(ml_pnl_units(d.dog_ml, d.dog_won) for _, d in ctrl)
        lines.append(f" n={nn}  dogs {ww}-{nn - ww}  roi={uu/nn*100:+.2f}%")

    # --- Phase 2: conditional filters (2024/2025 only; no new CLI flags) ----------
    season_allow = frozenset(seasons) & PHASE2_SEASONS
    if not season_allow:
        lines.append("")
        lines.append("PHASE 2 -- skipped (insufficient historical seasons)")
    else:
        p2_base = [(g, d) for g, d in dog_rows(nosig) if g.season in season_allow]

        lines.append("")
        lines.append("PHASE 2 -- CONDITIONAL FILTER ANALYSIS")
        lines.append("-" * 44)
        lines.append("Pool: proxy no-signal only (same structural definition as Phase 1).")
        lines.append(
            "[!] N<20: flagged on season lines; N<30 in any listed season -> verdict SMALL; "
            "verdict PASS needs combined ROI > +5%, both seasons N>=30 when both in run."
        )

        if 2024 in season_allow:
            e24 = [g for g in enriched if g.season == 2024]
            c24 = sum(1 for g in e24 if g.data_quality == "complete")
            denom = len(e24)
            rate = 100.0 * c24 / denom if denom else 0.0
            lines.append(
                f"2024 WMA join rate (full home OPS + away SP, enriched w/ odds): "
                f"{rate:.1f}% ({c24}/{denom})"
            )
            if rate < 100.0:
                lines.append(
                    "[!] 2024 WMA join rate below full coverage -- signal proxy incomplete; "
                    "2024 Phase 2 rows directional only."
                )

        def _p2_n_by_season(pairs: list[tuple[PackedGame, DogView]]) -> dict[int, int]:
            out: dict[int, int] = {y: 0 for y in season_allow}
            for g, _ in pairs:
                if g.season in out:
                    out[g.season] = out.get(g.season, 0) + 1
            return out

        def _p2_emit_filter(
            title: str,
            subtitle: str,
            pairs: list[tuple[PackedGame, DogView]],
            table_label: str,
            table_rows: list[tuple[str, int, float, str]],
        ) -> None:
            lines.append("")
            lines.append(title)
            lines.append(subtitle)
            nbs = _p2_n_by_season(pairs)
            for y in sorted(season_allow):
                sub = [(g, d) for g, d in pairs if g.season == y]
                lines.append(_p2_season_line(str(y), _p2_metrics(sub)))
            m_all = _p2_metrics(pairs)
            lines.append(_p2_combined_line(m_all))
            warn30 = any(nbs.get(y, 0) < P2_VERDICT_N for y in season_allow)
            if warn30:
                parts = [f"{y} N={nbs.get(y, 0)}" for y in sorted(season_allow)]
                lines.append(f"  [!] Small sample (N<30 per season: {', '.join(parts)})")
            lines.append(f"  Reading: [{_p2_reading(m_all)}]")
            tv = _p2_table_verdict(nbs, season_allow, m_all)
            table_rows.append((table_label, int(m_all["n"]), float(m_all["roi"]), tv))

        p2_table: list[tuple[str, int, float, str]] = []

        # Filter A
        fa = [
            (g, d)
            for g, d in p2_base
            if 150 <= d.dog_ml <= 199
            and g.total_open is not None
            and g.total_open <= 8.0
        ]
        _p2_emit_filter(
            "Filter A -- Low total + heavy dog (O/U <= 8.0, dog +150-199)",
            "------------------------------------------------------------",
            fa,
            "A -- Low total + heavy dog",
            p2_table,
        )

        # Filter B
        fb = [
            (g, d)
            for g, d in p2_base
            if d.dog_ml >= 150 and _game_month(g.game_date_et) in (7, 8)
        ]
        _p2_emit_filter(
            "Filter B -- Midsummer window (Jul-Aug, dog +150+)",
            "------------------------------------------------------------",
            fb,
            "B -- Midsummer window",
            p2_table,
        )

        # Filter C
        fc = [
            (g, d)
            for g, d in p2_base
            if d.dog_ml >= 150
            and d.dog_side == "away"
            and g.total_open is not None
            and g.total_open <= 8.5
        ]
        _p2_emit_filter(
            "Filter C -- Away dog + low-mid total (total <= 8.5, dog +150+)",
            "------------------------------------------------------------",
            fc,
            "C -- Away dog + low-mid total",
            p2_table,
        )

        # Filter D
        fd = [
            (g, d)
            for g, d in p2_base
            if 150 <= d.dog_ml <= 199
            and d.dog_side == "away"
            and _game_month(g.game_date_et) in {5, 6, 7, 8, 9}
            and g.total_open is not None
            and g.total_open <= 8.5
        ]
        _p2_emit_filter(
            "Filter D -- All combined (May-Sep ex Apr; away; +150-199; total<=8.5)",
            "------------------------------------------------------------",
            fd,
            "D -- All combined",
            p2_table,
        )

        lines.append("")
        lines.append("PHASE 2 VERDICT SUMMARY")
        lines.append("------------------------")
        lines.append(
            f"{'Filter':<36} {'Combined N':>12} {'Combined ROI':>14} {'Verdict':>8}"
        )
        for name, n_c, roi_c, v in p2_table:
            lines.append(f"{name:<36} {n_c:>12} {roi_c:>+13.1f}% {v:>8}")

    run_validation_2026_sample(con, lines)

    lines.append("")
    lines.append("=" * 80)
    lines.append("  END")
    lines.append("=" * 80)

    text = "\n".join(lines) + "\n"
    outp = Path(_REPO_ROOT / args.output)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(text, encoding="utf-8")

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(
            [
                "game_date_et",
                "game_pk",
                "away_abbr",
                "home_abbr",
                "venue",
                "season",
                "dog_side",
                "dog_ml",
                "fav_ml",
                "dog_win",
                "signal_proxy_structural_any",
                "struct_owm",
                "struct_lhp",
                "struct_mvf",
                "struct_mvb",
                "struct_h3b",
                "wind_mph",
                "wind_dir_raw",
                "wind_effect",
                "away_era_wma",
                "home_ops_wma",
                "away_throws",
                "opening_total_line",
                "bucket",
                "units_pl_data_row",
                "data_quality",
            ]
        )
        for g in enriched:
            dv = dog_view_from_game(g)
            if dv is None:
                continue
            bkey = dog_bucket(dv.dog_ml, breakpoints_t)
            w.writerow(
                [
                    g.game_date_et,
                    g.game_pk,
                    g.away_abbr,
                    g.home_abbr,
                    g.venue_name,
                    g.season,
                    dv.dog_side,
                    dv.dog_ml,
                    dv.fav_ml,
                    int(dv.dog_won),
                    int(g.any_signal),
                    int(g.structural_owm),
                    int(g.structural_lhp),
                    int(g.structural_mvf),
                    int(g.structural_mvb),
                    int(g.structural_h3b),
                    "" if g.wind_mph is None else f"{g.wind_mph:.0f}",
                    g.wind_dir_raw or "",
                    g.wind_effect or "",
                    f"{g.away_era_wma:.4f}" if g.away_era_wma is not None else "",
                    f"{g.home_ops_wma:.4f}" if g.home_ops_wma is not None else "",
                    g.away_throws or "",
                    f"{g.total_open:.2f}" if g.total_open is not None else "",
                    bkey,
                    f"{ml_pnl_units(dv.dog_ml, dv.dog_won):.4f}",
                    g.data_quality,
                ]
            )

    print(text)


if __name__ == "__main__":
    main()
