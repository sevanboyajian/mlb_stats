# MLB Scout — SQL & CLI Quick Reference

**Purpose:** One place for common **shell commands** and **SQLite** snippets used in day-to-day ops, plus backfill patterns. Descriptions are intentionally short (1–2 lines).

**Note:** This file is a curated index. It does not replace **`docs/Generate_Daily_Brief_Guide_2026-04.md`** or **`docs/Pipeline_Operations_Guide_2026-04.md`** for full context. Constants like **`MODEL_V2_START_DATE`** live in code (`generate_daily_brief.py`).

---

## 1. Conventions

Always run Python from the repo root **`mlb_stats`** (the folder that contains **`batch/`** and **`data/`**).

```text
cd C:\path\to\mlb_stats
```

Default DB is usually **`data/mlb_stats.db`** (override with **`MLB_DB_PATH`** / script **`--db`** where supported).

---

## 2. Daily brief (`generate_daily_brief.py`)

Invoke as a module path from repo root.

| Use | Command |
|-----|---------|
| Primary brief for a slate date | `python batch/pipeline/generate_daily_brief.py --session primary --date YYYY-MM-DD` |
| Prior-day brief (morning pipeline) | `python batch/pipeline/generate_daily_brief.py --session prior --date YYYY-MM-DD` |
| Morning sneak peek (slate only, no signals) | `python batch/pipeline/generate_daily_brief.py --session morning --date YYYY-MM-DD` |
| Closing confirmation | `python batch/pipeline/generate_daily_brief.py --session closing --date YYYY-MM-DD` |
| Early / afternoon / late sessions | `python batch/pipeline/generate_daily_brief.py --session early\|afternoon\|late --date YYYY-MM-DD` |
| Regenerate even if already in `brief_log` | Same as above with `--force` |
| No DB writes, no brief file (smoke test) | `--dry-run` |
| Fail if expected odds pull missing | `--check-prereqs` |
| Replay at a specific ET wall time | `--as-of "YYYY-MM-DD HH:MM"` or `--as-of-time` (see guide) |
| Verbose + wind debug | `--verbose --debug-wind` |
| Bet materialization only (`bet_ledger` from `signal_state`) | `python batch/pipeline/generate_daily_brief.py --sync-bet-ledger-only --date YYYY-MM-DD` |

---

## 3. Prior / results report (`daily_results_report.py`)

| Use | Command |
|-----|---------|
| Report for **yesterday** (default) | `python batch/pipeline/daily_results_report.py` |
| Specific completed slate date | `python batch/pipeline/daily_results_report.py --date YYYY-MM-DD` |
| Season rollup (no per-game detail) | `python batch/pipeline/daily_results_report.py --season YYYY` |
| Also write CSV under `reports/` | Add `--csv` |
| Non-default DB path | `--db PATH` |

---

## 4. Pipeline orchestration

| Use | Command |
|-----|---------|
| Schedule a full ET day (globals + groups + next-day hook) | `python batch/jobs/schedule_pipeline_day.py --date-et YYYY-MM-DD` |
| Morning globals only | `python batch/jobs/schedule_pipeline_day.py --globals-only --date-et YYYY-MM-DD` |
| Per-group jobs only (after games exist) | `python batch/jobs/schedule_pipeline_day.py --groups-only --date-et YYYY-MM-DD` |
| Planner dry-run (no `pipeline_jobs` inserts) | Add `--dry-run` |
| Runner: show queue / dependency help | `python batch/jobs/run_pipeline.py --status` / `--explain-deps YYYY-MM-DD` |
| Runner: long-lived “sleep until due” day | `python batch/jobs/run_pipeline.py --sleep-until-due --job-date-et YYYY-MM-DD --exit-when-no-pending` |
| Runner: single pass then exit | `--once` |

---

## 5. Ingestion & checks (frequently paired with briefs)

| Use | Command |
|-----|---------|
| Load today’s schedule | `python batch/ingestion/load_today.py --date YYYY-MM-DD` |
| Pregame odds (typical pipeline) | `python batch/ingestion/load_odds.py --pregame --markets game --date YYYY-MM-DD` (exact flags vary; see `run_pipeline` mapping) |
| Weather for slate | `python batch/ingestion/load_weather.py --date YYYY-MM-DD` |
| Team OPS WMA (morning global) | `python -m batch.pipeline.build_team_wma --seasons YYYY YYYY` |
| Odds readiness diagnostic | `python diagnostics/check_odds_ready.py --date YYYY-MM-DD` |
| Broad DB health summary | `python diagnostics/check_db.py` |

Closing-line / movement analysis (when your workflow uses it):

```text
python batch/ingestion/load_odds.py --compute-movement
```

---

## 6. Research / backtests

| Use | Command |
|-----|---------|
| Contrarian dog report (default report path) | `python -m batch.pipeline.backtest_contrarian_dog --seasons 2024 2025` |
| July–August only pooled | Same with `--months 7 8` |
| Custom text report path | `--output reports/your_report.txt` |

---

## 7. SQLite — `brief_picks` & model lineage

Row labels: **`legacy`** before **`2026-04-28`**, **`v2`** from **`2026-04-28`** onward (aligned with **`MODEL_V2_START_DATE`** in **`generate_daily_brief.py`**). Inserts choose version automatically.

**Audit distribution (row counts + date span):**

```sql
SELECT model_version,
       COUNT(*)       AS rows,
       MIN(game_date) AS first_date,
       MAX(game_date) AS last_date
FROM brief_picks
GROUP BY model_version
ORDER BY first_date;
```

**One-time backfill (safe to re-run idempotently; does not delete rows):**

```sql
UPDATE brief_picks SET model_version = 'v2' WHERE game_date >= '2026-04-28';
UPDATE brief_picks SET model_version = 'legacy' WHERE game_date < '2026-04-28';
```

**Picks saved for one slate:**

```sql
SELECT game_pk, session, pick_rank, signal, bet, market, odds, total_line, total_line_at_bet, model_version, recorded_at
FROM brief_picks
WHERE game_date = 'YYYY-MM-DD'
ORDER BY pick_rank, game_pk;
```

---

## 8. SQLite — Shadow Filter B (`shadow_filter_b_watch`)

Paper-track only (primary/closing writes from brief when rules match). See **`docs/Generate_Daily_Brief_Guide_2026-04.md`** for behavior.

**Row count / recent rows:**

```sql
SELECT COUNT(*) FROM shadow_filter_b_watch;

SELECT game_date, game_pk, dog_side, dog_ml, dog_won, first_session, result_graded_at
FROM shadow_filter_b_watch
ORDER BY game_date DESC, game_pk
LIMIT 50;
```

**September-style rollup (Jul–Aug study window, editable dates):**

```sql
SELECT game_date,
       COUNT(*) AS n,
       ROUND(100.0 * SUM(COALESCE(dog_won, 0)) / NULLIF(COUNT(*), 0), 2) AS dog_win_pct,
       ROUND(AVG(CASE WHEN dog_won = 1 THEN CAST(dog_ml AS REAL) / 100.0 ELSE -1.0 END), 4) AS flat_units_per_play
FROM shadow_filter_b_watch
WHERE game_date >= '2026-07-01'
  AND substr(game_date, 6, 2) IN ('07', '08')
GROUP BY game_date
ORDER BY game_date;
```

---

## 9. SQLite — `brief_log` & pipeline visibility

**Recent brief runs:**

```sql
SELECT game_date, session, generated_at, picks_count, output_file
FROM brief_log
ORDER BY generated_at DESC
LIMIT 30;
```

**Pending pipeline jobs for a slate date:**

```sql
SELECT job_id, job_type, status, scheduled_time_et, game_group_id, error_message
FROM pipeline_jobs
WHERE job_date_et = 'YYYY-MM-DD'
ORDER BY scheduled_time_et, job_id;
```

---

## 10. SQLite — Games & results (sanity)

**Final games on a date:**

```sql
SELECT game_pk, game_date_et, status, home_score, away_score
FROM games
WHERE game_date_et = 'YYYY-MM-DD'
  AND game_type = 'R'
ORDER BY game_start_utc;
```

---

## 11. App & email (optional)

| Use | Command / note |
|-----|----------------|
| Streamlit Scout UI | `cd online/app` then `streamlit run scout.py` |
| Brief email delivery | Configured via env (e.g. **`BRIEF_SMTP_*`**, **`BRIEF_EMAIL_TO`**); see **`delivery/email_sender.py`** and **`config/.env.template`** |

---

## Related docs

- `docs/Generate_Daily_Brief_Guide_2026-04.md` — sessions, `--sync-bet-ledger-only`, Shadow Filter B narrative
- `docs/Pipeline_Operations_Guide_2026-04.md` — `schedule_pipeline_day` / `run_pipeline` details
- `docs/MLB_Scout_Daily_Operations_Guide_2026-04.md` — Streamlit operations
- `README.md` — repo layout and high-level flow

---

## 12. SQLite — `bet_snapshots` (Added May 2026)

Written by `generate_daily_brief.py` at generation time. Source of truth for model state at decision time — not overwritten by subsequent line movement.

**Schema (add to `schema.sql` if missing):**

```sql
CREATE TABLE IF NOT EXISTS bet_snapshots (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    game_date    TEXT    NOT NULL,
    game_pk      INTEGER NOT NULL,
    market_type  TEXT    NOT NULL,
    bet_side     TEXT    NOT NULL,
    bet          TEXT,
    odds_taken   REAL,
    score        REAL,
    model_p      REAL,
    implied_p    REAL,
    edge         REAL,
    eval_status  TEXT,
    signals_used TEXT,
    placed_at    TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (game_date, game_pk, market_type, bet_side, placed_at)
);
```

**All snapshots for a slate date:**

```sql
SELECT game_date, game_pk, market_type, bet_side, bet,
       odds_taken, score, model_p, implied_p, edge,
       eval_status, signals_used, placed_at
FROM bet_snapshots
WHERE game_date = 'YYYY-MM-DD'
ORDER BY placed_at, game_pk, market_type;
```

**Compare bet_snapshots eval_status to brief_picks (phantom bet check):**

```sql
SELECT bp.game_date, bp.game_pk, bp.market, bp.bet,
       bs.eval_status, bs.odds_taken, bs.signals_used
FROM brief_picks bp
LEFT JOIN bet_snapshots bs
  ON bs.game_date = bp.game_date
 AND bs.game_pk   = bp.game_pk
 AND bs.market_type = bp.market
WHERE bp.game_date = 'YYYY-MM-DD'
ORDER BY bp.game_pk;
```

---

## 13. Season P&L Baseline — Manual Override (May 2026)

> ⚠ **The DB 2026 season totals are uncorrected.** Manual tracking is authoritative.

Corrected baseline as of **2026-05-04**: **66 bets, 31W–35L, −8.34u**

Do not use raw DB season P&L for 2026 until the cleanup query is run.

**Check current DB season totals (for comparison only):**

```sql
SELECT COUNT(*)                                    AS total_bets,
       SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) AS wins,
       SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) AS losses,
       ROUND(SUM(COALESCE(pnl_units, 0)), 2)       AS total_units
FROM bet_ledger
WHERE game_date >= '2026-04-28';  -- MODEL_V2_START_DATE
```

---

## 14. OWM — Directional Conflict Penalty (May 2026)

When ML directional signals conflict on the same game (e.g. LHP_FADE vs OWM), `score_game.py` subtracts the opposing signal score from the primary pick score.

**Brief audit flag to search for:**

```sql
-- Find games where a directional penalty was applied (from brief_log or bet_snapshots)
SELECT game_date, game_pk, signals_used
FROM bet_snapshots
WHERE signals_used LIKE '%OWM%'
  AND game_date >= '2026-05-01'
ORDER BY game_date DESC;
```

**Backtest pending** — target late June / July 2026 once N≥20 LHP_FADE / OWM conflict cases accumulate.

---

## 15. Open Items Tracking (May 2026)

Three open items documented in `findings_matrix_v2_4` Section 6.7:

| Item | Status | Target |
|------|--------|--------|
| LHP_FADE / OWM conflict backtest | PENDING | Late June / July 2026 (N≥20) |
| First-fire ML odds overwritten by closing line on `brief_picks` upsert | KNOWN BUG | Fix: add `first_fire_odds` column, write on INSERT only |
| 2026 season P&L DB baseline mismatch | PENDING cleanup query | Before end-of-season analysis |

**Query to surface the first-fire odds bug (picks with mismatched odds):**

```sql
-- brief_picks.odds is the closing line, not first-fire ML
-- Compare against bet_snapshots.odds_taken for the first session
SELECT bp.game_date, bp.game_pk, bp.market, bp.session,
       bp.odds       AS closing_odds,
       bs.odds_taken AS first_fire_odds
FROM brief_picks bp
JOIN bet_snapshots bs
  ON bs.game_date  = bp.game_date
 AND bs.game_pk    = bp.game_pk
 AND bs.market_type = bp.market
WHERE bp.game_date >= '2026-04-28'
  AND ABS(COALESCE(bp.odds,0) - COALESCE(bs.odds_taken,0)) > 5
ORDER BY bp.game_date DESC, bp.game_pk;
```
