"""
backtest_mvf_clv.py
===================
MV-F Signal CLV Gate Analysis
Isolates every MV-F fire across the database, computes CLV for each fire,
and splits results by CLV+ vs CLV- to evaluate whether a CLV gate improves
the already-positive MV-F ROI.

USAGE
-----
    python backtest_mvf_clv.py --bookmaker sbro
    python backtest_mvf_clv.py --bookmaker oddswarehouse
    python backtest_mvf_clv.py --bookmaker all

FLAGS
-----
    --db        PATH        Path to mlb_stats.db (default: mlb_stats.db)
    --bookmaker sbro|oddswarehouse|all
                            Bookmaker for closing odds (default: sbro)
    --seasons   YYYY [...]  Restrict to specific seasons (default: all)
    --out       PATH        Output markdown report path (default: auto)
    --verbose               Print per-game detail during run

SIGNAL DEFINITION (mirrors generate_daily_brief.py exactly)
------------------------------------------------------------
MV-F fires when ALL of the following are true:
  1. wind_effect = 'HIGH'   (not suppressed/retractable/moderate)
  2. wind_direction contains 'IN' (blowing in from outfield)
  3. wind_mph >= 10
  4. home_ml between -130 and -160 (home fav, fade zone)
  5. S1+H2 did NOT already fire on this game
     (S1+H2: home W5+ streak AND home_ml between -130 and -160)

BET: Away ML (fade the overpriced home favourite)

CLV DEFINITION (mirrors backtest_top_pick.py)
---------------------------------------------
For ML bets: CLV = (opening implied prob) - (closing implied prob)
  Positive CLV = you bet at better-than-closing odds (sharp money
                 confirmed your direction or moved away from you)
  Negative CLV = closing odds were better than what you got
                 (sharp money moved against your bet)

CLV gate hypothesis: MV-F CLV+ fires (+18.6% SBRO, +25.5% OW ROI)
dramatically outperform CLV- fires (-15.9% SBRO, -16.5% OW ROI).
A CLV+ gate should dramatically improve live-model signal quality.
"""

import argparse
import datetime
import sqlite3
import sys
from pathlib import Path

# ── Constants — mirror generate_daily_brief.py exactly ────────────────────────
WIND_IN_MIN_MPH     = 10
HOME_FAV_MV_F_LOW   = -130   # less negative bound
HOME_FAV_MV_F_HIGH  = -160   # more negative bound
S1_H2_STREAK_MIN    = 5      # W5+ triggers S1+H2 (suppresses MV-F)

DEFAULT_DB  = "mlb_stats.db"
DEFAULT_OUT = "reports/backtest_mvf_clv_{bookmaker}.md"

PASS_ROI    = 4.5   # minimum ROI to clear vig on flat -110
PASS_N      = 20    # minimum fires for a sub-population verdict


# ── DB helpers ─────────────────────────────────────────────────────────────────

def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def american_to_implied(ml: float) -> float:
    """American odds → implied win probability (0–1)."""
    if ml is None:
        return 0.5
    ml = float(ml)
    return abs(ml) / (abs(ml) + 100) if ml < 0 else 100 / (ml + 100)


def wind_direction_label(direction: str) -> str:
    """Classify wind direction string → OUT / IN / CROSS / CALM / UNKNOWN.
    Mirrors generate_daily_brief.py wind_direction_label() exactly."""
    if not direction:
        return "UNKNOWN"
    d = direction.upper()
    if any(k in d for k in ("OUT", "BLOWING OUT")):
        return "OUT"
    if any(k in d for k in ("IN", "BLOWING IN")):
        return "IN"
    if any(k in d for k in ("CROSS", "LEFT", "RIGHT")):
        return "CROSS"
    if any(k in d for k in ("CALM", "STILL", "0 MPH")):
        return "CALM"
    return "CROSS"   # treat unclassified as non-signal


def get_bookmaker_filter(bookmaker: str) -> set:
    if bookmaker == "sbro":
        return {"sbro"}
    if bookmaker == "oddswarehouse":
        return {"oddswarehouse"}
    return {"sbro", "oddswarehouse", "draftkings", "fanduel", "betmgm", "betonlineag"}


# ── Data loaders ───────────────────────────────────────────────────────────────

def load_seasons(conn: sqlite3.Connection, requested: list) -> list:
    rows = conn.execute(
        "SELECT season FROM seasons WHERE season >= 2015 ORDER BY season"
    ).fetchall()
    available = [r["season"] for r in rows]
    if requested:
        return [s for s in requested if s in available]
    return available


def load_streaks_entering(conn: sqlite3.Connection,
                          game_date: str,
                          team_ids: list) -> dict:
    """
    Compute overall win/loss streak for each team entering game_date.
    Returns {team_id: int}  (+N = N-game win streak, -N = loss streak).
    Mirrors load_streaks() in generate_daily_brief.py exactly.
    """
    streaks = {}
    for tid in team_ids:
        cur = conn.execute(
            """
            SELECT
                CASE WHEN home_team_id = ? THEN
                    CASE WHEN home_score > away_score THEN 'W' ELSE 'L' END
                ELSE
                    CASE WHEN away_score > home_score THEN 'W' ELSE 'L' END
                END AS result
            FROM   games
            WHERE  (home_team_id = ? OR away_team_id = ?)
              AND  status    = 'Final'
              AND  game_date < ?
            ORDER  BY game_date DESC, game_start_utc DESC
            LIMIT  15
            """,
            (tid, tid, tid, game_date),
        )
        results = [r["result"] for r in cur.fetchall()]
        if not results:
            streaks[tid] = 0
            continue
        current = results[0]
        count   = sum(1 for r in results if r == current)
        # stop at first streak break
        for r in results:
            if r != current:
                break
            count_check = True
        # recount correctly
        n = 0
        for r in results:
            if r == current:
                n += 1
            else:
                break
        streaks[tid] = n if current == "W" else -n
    return streaks


def load_games_for_season(conn: sqlite3.Connection,
                          season: int,
                          bookmaker_set: set) -> list:
    """
    Load all Final regular-season games for a season with closing ML odds,
    wind data, venue wind_effect, and team streaks.
    Returns list of dicts.
    """
    placeholders = ",".join("?" * len(bookmaker_set))
    rows = conn.execute(
        f"""
        SELECT
            g.game_pk,
            g.game_date,
            g.season,
            g.home_team_id,
            g.away_team_id,
            th.abbreviation  AS home_abbr,
            ta.abbreviation  AS away_abbr,
            g.wind_mph,
            g.wind_direction,
            g.home_score,
            g.away_score,
            v.wind_effect,
            ml.home_ml,
            ml.away_ml
        FROM   games g
        JOIN   teams  th  ON th.team_id  = g.home_team_id
        JOIN   teams  ta  ON ta.team_id  = g.away_team_id
        LEFT JOIN venues v ON v.venue_id  = g.venue_id
        JOIN   game_odds ml
               ON  ml.game_pk        = g.game_pk
               AND ml.market_type    = 'moneyline'
               AND ml.is_closing_line = 1
               AND ml.bookmaker       IN ({placeholders})
        WHERE  g.season    = ?
          AND  g.game_type = 'R'
          AND  g.status    = 'Final'
        ORDER  BY g.game_date, g.game_start_utc
        """,
        (*bookmaker_set, season),
    ).fetchall()
    return [dict(r) for r in rows]


def load_opening_ml(conn: sqlite3.Connection,
                    game_pks: list,
                    bookmaker_set: set) -> dict:
    """
    Load the earliest captured moneyline for each game_pk.
    Returns {game_pk: {home_ml_open, away_ml_open}}.
    Uses is_opening_line = 1 first; falls back to earliest captured_at_utc.
    """
    if not game_pks:
        return {}
    placeholders = ",".join("?" * len(game_pks))
    bk_placeholders = ",".join("?" * len(bookmaker_set))

    # Prefer is_opening_line = 1 rows
    rows = conn.execute(
        f"""
        SELECT game_pk, home_ml, away_ml, captured_at_utc
        FROM   game_odds
        WHERE  game_pk      IN ({placeholders})
          AND  bookmaker    IN ({bk_placeholders})
          AND  market_type  = 'moneyline'
          AND  is_opening_line = 1
        ORDER  BY game_pk, captured_at_utc ASC
        """,
        (*game_pks, *bookmaker_set),
    ).fetchall()

    opening = {}
    for r in rows:
        gpk = r["game_pk"]
        if gpk not in opening:
            opening[gpk] = {
                "home_ml_open": r["home_ml"],
                "away_ml_open": r["away_ml"],
            }

    # For games with no is_opening_line=1, fall back to earliest row
    missing = [pk for pk in game_pks if pk not in opening]
    if missing:
        mp = ",".join("?" * len(missing))
        fallback = conn.execute(
            f"""
            SELECT game_pk, home_ml, away_ml, captured_at_utc
            FROM   game_odds
            WHERE  game_pk    IN ({mp})
              AND  bookmaker  IN ({bk_placeholders})
              AND  market_type = 'moneyline'
            ORDER  BY game_pk, captured_at_utc ASC
            """,
            (*missing, *bookmaker_set),
        ).fetchall()
        for r in fallback:
            gpk = r["game_pk"]
            if gpk not in opening:
                opening[gpk] = {
                    "home_ml_open": r["home_ml"],
                    "away_ml_open": r["away_ml"],
                }

    return opening


# ── Signal evaluation ──────────────────────────────────────────────────────────

def s1_h2_fires(home_ml: int, home_streak: int) -> bool:
    """Return True if S1+H2 would suppress MV-F on this game."""
    if home_ml is None:
        return False
    return (home_streak >= S1_H2_STREAK_MIN
            and HOME_FAV_MV_F_HIGH <= home_ml <= HOME_FAV_MV_F_LOW)


def mvf_fires(game: dict, home_streak: int) -> bool:
    """
    Return True if MV-F signal fires on this game.
    Mirrors evaluate_signals() in generate_daily_brief.py exactly.
    """
    wind_effect = (game.get("wind_effect") or "HIGH").upper()
    wind_eligible = (wind_effect == "HIGH")
    if not wind_eligible:
        return False

    wind_dir = wind_direction_label(game.get("wind_direction") or "")
    wind_mph = game.get("wind_mph") or 0
    home_ml  = game.get("home_ml")

    if wind_dir != "IN":
        return False
    if wind_mph < WIND_IN_MIN_MPH:
        return False
    if home_ml is None:
        return False
    if not (HOME_FAV_MV_F_HIGH <= home_ml <= HOME_FAV_MV_F_LOW):
        return False
    if s1_h2_fires(home_ml, home_streak):
        return False

    return True


# ── CLV computation ────────────────────────────────────────────────────────────

def compute_clv(away_ml_open: float | None,
                away_ml_close: float | None) -> float | None:
    """
    CLV for an away ML bet = opening implied − closing implied (in pp).
    Positive = you bet at better-than-close price (CLV+).
    Negative = closing line was better than your entry (CLV-).
    """
    if away_ml_open is None or away_ml_close is None:
        return None
    impl_open  = american_to_implied(away_ml_open)
    impl_close = american_to_implied(away_ml_close)
    return (impl_open - impl_close) * 100   # in percentage points


# ── P&L computation ────────────────────────────────────────────────────────────

def grade_away_ml(game: dict, away_ml: float | None) -> float | None:
    """
    Grade a flat $1 away ML bet. Returns P&L or None if ungradeable.
    Positive = WIN, Negative = LOSS, 0 = PUSH.
    """
    hs = game.get("home_score")
    as_ = game.get("away_score")
    if hs is None or as_ is None or away_ml is None:
        return None
    if as_ > hs:   # away wins
        if away_ml > 0:
            return away_ml / 100.0
        else:
            return 100.0 / abs(away_ml)
    elif hs > as_:
        return -1.0
    else:
        return 0.0


# ── Report helpers ─────────────────────────────────────────────────────────────

def summarise(fires: list, label: str) -> dict:
    gradeable = [f for f in fires if f["pnl"] is not None]
    n      = len(gradeable)
    wins   = sum(1 for f in gradeable if f["pnl"] > 0)
    losses = sum(1 for f in gradeable if f["pnl"] < 0)
    pushes = sum(1 for f in gradeable if f["pnl"] == 0)
    pnl    = sum(f["pnl"] for f in gradeable)
    roi    = (pnl / n * 100) if n > 0 else 0.0
    hit    = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0.0
    avg_odds = (sum(f["away_ml_close"] for f in gradeable
                    if f["away_ml_close"] is not None) /
                max(1, sum(1 for f in gradeable
                           if f["away_ml_close"] is not None)))
    clv_known = [f for f in gradeable if f["clv"] is not None]
    avg_clv = (sum(f["clv"] for f in clv_known) /
               len(clv_known)) if clv_known else None
    return {
        "label":    label,
        "n":        n,
        "wins":     wins,
        "losses":   losses,
        "pushes":   pushes,
        "pnl":      pnl,
        "roi":      roi,
        "hit":      hit,
        "avg_odds": avg_odds,
        "avg_clv":  avg_clv,
        "n_total":  len(fires),
    }


def verdict(stats: dict, threshold_roi: float = PASS_ROI,
            threshold_n: int = PASS_N) -> str:
    n   = stats["n"]
    roi = stats["roi"]
    if n < threshold_n:
        return f"⚠️  Sample too small (N={n}). Do not draw conclusions."
    if roi >= threshold_roi:
        return f"✅  PASS — ROI {roi:+.1f}% on {n} fires (threshold {threshold_roi}%)."
    return f"❌  FAIL — ROI {roi:+.1f}% on {n} fires (threshold {threshold_roi}%)."


def fmt_tbl(rows: list, headers: list, col_widths: list = None) -> str:
    if not rows:
        return "(no data)\n"
    lines = []
    # header
    h = " | ".join(f"{str(headers[i]):<{col_widths[i] if col_widths else 12}}"
                   for i in range(len(headers)))
    sep = "-+-".join("-" * (col_widths[i] if col_widths else 12)
                     for i in range(len(headers)))
    lines.append(f"| {h} |")
    lines.append(f"|-{sep}-|")
    for row in rows:
        r = " | ".join(f"{str(row[i]):<{col_widths[i] if col_widths else 12}}"
                       for i in range(len(row)))
        lines.append(f"| {r} |")
    return "\n".join(lines) + "\n"


def build_report(fires: list,
                 bookmaker: str,
                 seasons_run: list,
                 run_ts: str) -> str:
    lines = []
    lines.append("# MV-F CLV Gate Backtest Report\n")
    lines.append(f"Generated: {run_ts}  \n")
    lines.append(f"Bookmaker: `{bookmaker}`  \n")
    lines.append(f"Seasons: {', '.join(str(s) for s in sorted(seasons_run))}  \n")
    lines.append(f"Signal: MV-F (wind-IN ≥10mph + home fav −130/−160, fade away ML)  \n")
    lines.append("\n---\n\n")

    # ── Overall ───────────────────────────────────────────────────────────────
    lines.append("## Overall MV-F Performance\n\n")
    all_stats = summarise(fires, "All MV-F fires")
    clv_known = [f for f in fires if f["clv"] is not None]
    clv_unknown = [f for f in fires if f["clv"] is None]

    lines.append(fmt_tbl(
        [[all_stats["n"], all_stats["wins"], all_stats["losses"],
          f"{all_stats['hit']:.1f}%",
          f"{all_stats['pnl']:+.2f}",
          f"{all_stats['roi']:+.1f}%",
          f"{all_stats['avg_odds']:+.0f}",
          f"{all_stats['avg_clv']:+.1f}pp" if all_stats['avg_clv'] is not None else "N/A"]],
        ["N", "W", "L", "Hit%", "P&L", "ROI%", "Avg Odds", "Avg CLV"],
        [6, 5, 5, 7, 9, 7, 10, 10],
    ))
    lines.append(f"\n**Verdict:** {verdict(all_stats)}\n\n")
    lines.append(f"CLV data available: {len(clv_known)}/{len(fires)} fires "
                 f"({len(clv_unknown)} missing opening line data)  \n\n")

    # ── CLV split ─────────────────────────────────────────────────────────────
    lines.append("---\n\n## CLV Split — The Core Analysis\n\n")
    lines.append(
        "CLV for an away ML bet = opening implied probability − closing implied probability.  \n"
        "**CLV+ (positive):** You bet at better-than-closing odds — "
        "sharp money confirmed your direction or didn't move against you.  \n"
        "**CLV− (negative):** Closing line was sharper than your entry — "
        "smart money moved against the away bet after you would have placed it.  \n\n"
    )

    clv_pos  = [f for f in fires if f["clv"] is not None and f["clv"] >  0]
    clv_zero = [f for f in fires if f["clv"] is not None and f["clv"] == 0]
    clv_neg  = [f for f in fires if f["clv"] is not None and f["clv"] <  0]

    sp = summarise(clv_pos,  "CLV+ (sharp confirms)")
    sn = summarise(clv_neg,  "CLV− (sharp fades)")
    sz = summarise(clv_zero, "CLV=0 (no movement)")
    sa = summarise(clv_known,"All CLV-known fires")

    tbl_rows = []
    for s in [sp, sn, sz, sa]:
        avg_c = f"{s['avg_clv']:+.1f}pp" if s['avg_clv'] is not None else "N/A"
        tbl_rows.append([
            s["label"], s["n"], s["wins"], s["losses"],
            f"{s['hit']:.1f}%",
            f"{s['pnl']:+.2f}",
            f"{s['roi']:+.1f}%",
            avg_c,
        ])

    lines.append(fmt_tbl(
        tbl_rows,
        ["Population", "N", "W", "L", "Hit%", "P&L", "ROI%", "Avg CLV"],
        [26, 5, 5, 5, 7, 9, 8, 10],
    ))

    lines.append(f"\n**CLV+ verdict:** {verdict(sp)}\n")
    lines.append(f"**CLV− verdict:** {verdict(sn)}\n\n")

    # Gap analysis
    if sp["n"] >= PASS_N and sn["n"] >= PASS_N:
        gap = sp["roi"] - sn["roi"]
        lines.append(f"**ROI gap (CLV+ minus CLV−):** {gap:+.1f}pp  \n")
        if gap >= 10:
            lines.append("**Gate recommendation: IMPLEMENT** — gap ≥10pp justifies a CLV+ filter.  \n\n")
        elif gap >= 5:
            lines.append("**Gate recommendation: MONITOR** — gap 5–10pp, "
                         "borderline. Confirm with more seasons.  \n\n")
        else:
            lines.append("**Gate recommendation: SKIP** — gap <5pp, "
                         "not large enough to justify a live filter.  \n\n")
    else:
        lines.append("*(Insufficient N in one or both CLV sub-groups to compute gap.)*  \n\n")

    # ── CLV threshold analysis ─────────────────────────────────────────────────
    lines.append("---\n\n## CLV Threshold Sensitivity\n\n")
    lines.append("ROI at different CLV cutoff levels (bet only when CLV ≥ threshold):  \n\n")

    thresholds = [3.0, 2.0, 1.0, 0.5, 0.0, -0.5, -1.0, -2.0]
    thr_rows = []
    for thr in thresholds:
        subset = [f for f in clv_known if f["clv"] >= thr]
        s = summarise(subset, f"CLV ≥ {thr:+.1f}pp")
        thr_rows.append([
            f"CLV ≥ {thr:+.1f}pp",
            s["n"],
            f"{s['roi']:+.1f}%",
            f"{s['hit']:.1f}%",
            "✓" if s["roi"] >= PASS_ROI and s["n"] >= PASS_N else
            "~" if s["roi"] >= 0 else "✗",
        ])
    lines.append(fmt_tbl(
        thr_rows,
        ["CLV Gate", "N fires", "ROI%", "Hit%", "Pass?"],
        [16, 8, 8, 7, 6],
    ))
    lines.append("\n")

    # ── Season breakdown ───────────────────────────────────────────────────────
    lines.append("---\n\n## Season-by-Season Breakdown\n\n")
    season_rows = []
    for s in sorted(seasons_run):
        sf = [f for f in fires if f["season"] == s]
        if not sf:
            continue
        st = summarise(sf, str(s))
        clv_p = len([f for f in sf if f["clv"] is not None and f["clv"] > 0])
        clv_n = len([f for f in sf if f["clv"] is not None and f["clv"] < 0])
        avg_c = (f"{st['avg_clv']:+.1f}pp"
                 if st["avg_clv"] is not None else "N/A")
        season_rows.append([
            s, st["n"], st["wins"], st["losses"],
            f"{st['hit']:.1f}%",
            f"{st['roi']:+.1f}%",
            avg_c,
            clv_p,
            clv_n,
        ])
    lines.append(fmt_tbl(
        season_rows,
        ["Season", "N", "W", "L", "Hit%", "ROI%", "Avg CLV", "CLV+", "CLV−"],
        [7, 5, 5, 5, 7, 8, 10, 6, 6],
    ))
    lines.append("\n")

    # ── Wind MPH breakdown ─────────────────────────────────────────────────────
    lines.append("---\n\n## Wind Speed Breakdown\n\n")
    mph_buckets = [(10, 12), (13, 15), (16, 20), (21, 99)]
    mph_labels  = ["10–12 mph", "13–15 mph", "16–20 mph", "21+ mph"]
    mph_rows = []
    for (lo, hi), label in zip(mph_buckets, mph_labels):
        subset = [f for f in fires if lo <= (f["wind_mph"] or 0) <= hi]
        s = summarise(subset, label)
        mph_rows.append([
            label, s["n"], f"{s['roi']:+.1f}%", f"{s['hit']:.1f}%",
            f"{s['avg_clv']:+.1f}pp" if s["avg_clv"] is not None else "N/A",
        ])
    lines.append(fmt_tbl(
        mph_rows,
        ["Wind Speed", "N", "ROI%", "Hit%", "Avg CLV"],
        [12, 5, 8, 7, 10],
    ))
    lines.append("\n")

    # ── Home ML bucket breakdown ───────────────────────────────────────────────
    lines.append("---\n\n## Home ML Bucket Breakdown (within −130/−160 range)\n\n")
    ml_buckets = [(-130, -135), (-136, -145), (-146, -155), (-156, -160)]
    ml_labels  = ["-130/-135", "-136/-145", "-146/-155", "-156/-160"]
    ml_rows = []
    for (lo, hi), label in zip(ml_buckets, ml_labels):
        # lo is less negative, hi is more negative
        subset = [f for f in fires
                  if f["home_ml_close"] is not None
                  and hi <= f["home_ml_close"] <= lo]
        s = summarise(subset, label)
        ml_rows.append([
            label, s["n"], f"{s['roi']:+.1f}%", f"{s['hit']:.1f}%",
            f"{s['avg_clv']:+.1f}pp" if s["avg_clv"] is not None else "N/A",
        ])
    lines.append(fmt_tbl(
        ml_rows,
        ["Home ML range", "N", "ROI%", "Hit%", "Avg CLV"],
        [15, 5, 8, 7, 10],
    ))
    lines.append("\n")

    # ── Per-game detail (verbose-style, last 20 fires) ─────────────────────────
    lines.append("---\n\n## Recent MV-F Fires (most recent 30)\n\n")
    recent = sorted(fires, key=lambda f: f["game_date"], reverse=True)[:30]
    detail_rows = []
    for f in recent:
        clv_str = f"{f['clv']:+.1f}pp" if f["clv"] is not None else "N/A"
        pnl_str = f"{f['pnl']:+.2f}" if f["pnl"] is not None else "N/A"
        result  = ("WIN" if f["pnl"] and f["pnl"] > 0 else
                   "LOSS" if f["pnl"] and f["pnl"] < 0 else
                   "PUSH" if f["pnl"] == 0 else "N/A")
        detail_rows.append([
            f["game_date"], f["away_abbr"], "vs", f["home_abbr"],
            f["home_ml_close"] or "N/A",
            f"{f['wind_mph']}mph",
            clv_str,
            result,
            pnl_str,
        ])
    lines.append(fmt_tbl(
        detail_rows,
        ["Date", "Away", "", "Home", "Home ML", "Wind", "CLV", "Result", "P&L"],
        [11, 5, 3, 5, 8, 8, 9, 6, 7],
    ))
    lines.append("\n")

    # ── Summary and recommendation ─────────────────────────────────────────────
    lines.append("---\n\n## Summary & Recommendation\n\n")
    lines.append(f"- **Total MV-F fires:** {len(fires)}\n")
    lines.append(f"- **Overall ROI:** {all_stats['roi']:+.1f}% "
                 f"(Hit: {all_stats['hit']:.1f}%)\n")
    lines.append(f"- **CLV+ fires:** {len(clv_pos)} "
                 f"(ROI: {sp['roi']:+.1f}%)\n")
    lines.append(f"- **CLV− fires:** {len(clv_neg)} "
                 f"(ROI: {sn['roi']:+.1f}%)\n")
    if sp["n"] >= PASS_N and sn["n"] >= PASS_N:
        gap = sp["roi"] - sn["roi"]
        lines.append(f"- **CLV+/CLV− ROI gap:** {gap:+.1f}pp\n")
        lines.append("\n")
        if sp["roi"] >= PASS_ROI and gap >= 10:
            lines.append(
                "**RECOMMENDATION: Implement CLV+ gate on MV-F.**  \n"
                "CLV+ fires clear the vig threshold and the gap vs CLV- fires "
                "justifies a live-betting filter. Only bet MV-F when the line "
                "has NOT moved against the away team since morning open.  \n"
            )
        elif sp["roi"] >= PASS_ROI:
            lines.append(
                "**RECOMMENDATION: CLV+ gate improves MV-F but gap is moderate.**  \n"
                "CLV+ fires clear threshold. Gap may not be wide enough to "
                "justify strict filtering — consider as a soft preference "
                "rather than a hard gate.  \n"
            )
        else:
            lines.append(
                "**RECOMMENDATION: No CLV gate.**  \n"
                "CLV+ fires do not clear the vig threshold in this era/bookmaker. "
                "MV-F edge is structural (wind + pricing), not CLV-driven.  \n"
            )
    lines.append("\n*End of report.*\n")
    return "".join(lines)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="MV-F CLV Gate Backtest — isolates MV-F fires and splits by CLV"
    )
    p.add_argument("--db",         default=DEFAULT_DB)
    p.add_argument("--bookmaker",  default="sbro",
                   choices=["sbro", "oddswarehouse", "all"])
    p.add_argument("--seasons",    nargs="+", type=int,
                   help="Restrict to specific seasons (default: all)")
    p.add_argument("--out",        default=None,
                   help="Output markdown file path")
    p.add_argument("--verbose",    action="store_true")
    args = p.parse_args()

    if not Path(args.db).exists():
        print(f"ERROR: DB not found: {args.db}")
        sys.exit(1)

    conn = connect(args.db)
    bookmaker_set = get_bookmaker_filter(args.bookmaker)
    seasons = load_seasons(conn, args.seasons or [])

    if not seasons:
        print("ERROR: No seasons found in database.")
        sys.exit(1)

    run_ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M ET")
    print(f"\n  MV-F CLV Backtest  |  Bookmaker: {args.bookmaker}")
    print(f"  Seasons: {seasons}\n")

    all_fires = []

    for season in seasons:
        games = load_games_for_season(conn, season, bookmaker_set)
        if not games:
            if args.verbose:
                print(f"  Season {season}: no games found — skipping")
            continue

        # Load streaks for all teams entering each game date
        dates = sorted(set(g["game_date"] for g in games))
        streak_cache = {}   # {game_date: {team_id: streak}}
        for date in dates:
            team_ids = list({g["home_team_id"] for g in games if g["game_date"] == date} |
                            {g["away_team_id"]  for g in games if g["game_date"] == date})
            streak_cache[date] = load_streaks_entering(conn, date, team_ids)

        # Find MV-F fires
        season_fires = []
        for g in games:
            home_streak = streak_cache.get(g["game_date"], {}).get(g["home_team_id"], 0)
            if mvf_fires(g, home_streak):
                season_fires.append({
                    "game_pk":       g["game_pk"],
                    "game_date":     g["game_date"],
                    "season":        season,
                    "home_abbr":     g["home_abbr"],
                    "away_abbr":     g["away_abbr"],
                    "home_ml_close": g["home_ml"],
                    "away_ml_close": g["away_ml"],
                    "wind_mph":      g["wind_mph"],
                    "wind_direction":g["wind_direction"],
                    "home_score":    g["home_score"],
                    "away_score":    g["away_score"],
                    "clv":           None,   # filled below
                    "pnl":           None,   # filled below
                })

        if not season_fires:
            if args.verbose:
                print(f"  Season {season}: 0 MV-F fires")
            continue

        # Load opening ML for all fired games
        fire_pks = [f["game_pk"] for f in season_fires]
        opening  = load_opening_ml(conn, fire_pks, bookmaker_set)

        # Compute CLV and P&L
        for f in season_fires:
            gpk = f["game_pk"]
            op  = opening.get(gpk, {})
            away_ml_open = op.get("away_ml_open")
            f["clv"] = compute_clv(away_ml_open, f["away_ml_close"])
            f["pnl"] = grade_away_ml(
                {"home_score": f["home_score"], "away_score": f["away_score"]},
                f["away_ml_close"]
            )

            if args.verbose:
                clv_str = f"{f['clv']:+.1f}pp" if f["clv"] is not None else "CLV N/A"
                res = ("WIN " if f["pnl"] and f["pnl"] > 0 else
                       "LOSS" if f["pnl"] and f["pnl"] < 0 else "PUSH")
                print(f"    {f['game_date']}  {f['away_abbr']:>3} @ {f['home_abbr']:<3}"
                      f"  ML:{f['home_ml_close']:>5}  Wind:{f['wind_mph']:>2}mph"
                      f"  CLV:{clv_str:>9}  {res}")

        print(f"  Season {season}: {len(season_fires)} MV-F fires")
        all_fires.extend(season_fires)

    print(f"\n  Total MV-F fires: {len(all_fires)}")

    if not all_fires:
        print("  No fires found. Check DB has wind data and closing odds.")
        sys.exit(0)

    # Build report
    report = build_report(all_fires, args.bookmaker, seasons, run_ts)

    # Write output
    if args.out:
        out_path = Path(args.out)
    else:
        out_path = Path("reports") / f"backtest_mvf_clv_{args.bookmaker}.md"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report, encoding="utf-8")
    print(f"  ✓ Report saved to: {out_path}\n")


if __name__ == "__main__":
    main()
