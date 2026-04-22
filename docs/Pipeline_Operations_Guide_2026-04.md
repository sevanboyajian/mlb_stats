# Pipeline Operations Guide

**Version:** 2026-04 (markdown; aligns with `batch/jobs/run_pipeline.py`, `batch/jobs/schedule_pipeline_day.py`, `core/utils/game_start_grouping.py`)  
**Scope:** Populating **`pipeline_jobs`**, running the **single-threaded runner**, diagnostics, and practical “cloud-like” operation on a workstation.

---

## Conventions

- **Working directory:** always the repository root **`mlb_stats`** (folder containing `batch/`).

```text
cd C:\path\to\mlb_stats
```

- **Eastern slate date:** `job_date_et` / `--date-et` values are **`YYYY-MM-DD`** in **America/New_York** semantics for scheduling.

---

## Big picture

1. **`schedule_pipeline_day.py`** inserts rows into **`pipeline_jobs`** (globals, per-group odds/weather/brief/ledger, evening hook).
2. **`run_pipeline.py`** finds rows with **`status = 'pending'`** and **`scheduled_time <= now`**, respects **dependencies**, runs the mapped **subprocess** command, then updates status and **`pipeline_job_runs`**.
3. Individual scripts (e.g. `load_today.py`, `load_odds.py`, `generate_daily_brief.py`) do the real work; the runner only orchestrates.

---

## Scheduling: `schedule_pipeline_day.py`

### When to use

- After **`load_today.py`** has populated games for the slate date (for modes that need games).
- Re-run is mostly safe: **`INSERT OR IGNORE`** on `(job_type, scheduled_time_et, game_group_id)`.

### Modes

| Mode | Command | Purpose |
|------|---------|---------|
| Full day (default) | `python batch/jobs/schedule_pipeline_day.py --date-et YYYY-MM-DD` | Morning globals + per-group jobs + **`schedule_next_day_globals`** |
| Globals only | `python batch/jobs/schedule_pipeline_day.py --globals-only --date-et YYYY-MM-DD` | Group-0 morning jobs for that **calendar** date; no games required |
| Groups only | `python batch/jobs/schedule_pipeline_day.py --groups-only --date-et YYYY-MM-DD` | Per-group jobs + evening hook; **skips** the five morning globals |

### Important flags

```text
python batch/jobs/schedule_pipeline_day.py --date-et YYYY-MM-DD
```

| Flag | Default | Notes |
|------|---------|--------|
| `--group-window-min` | 30 | Start-time grouping window for `game_group_id` |
| `--odds-threshold-min` | 90 | Odds pull at **T0 − 90m** (per merged block) |
| `--odds-block-min` | 90 | Merge adjacent groups into one odds block within **N** minutes |
| `--weather-min` | 45 | Weather refresh at **T0 − N** (per merged block, aligned with odds blocks) |
| `--brief-min` | 30 | Primary **`group_brief`** at **T0 − 30m**; must be **>** `--ledger-min` |
| `--ledger-min` | 28 | **`ledger_snapshot`** at **T0 − 28m** (bet materialization window is **[T0−30m, T0)** — coordinate with runner) |
| `--brief-extra-minutes` | `15,5` | Extra **`group_brief`** rows (e.g. T0−15m, T0−5m). Use `none` to disable |
| `--dry-run` | off | Print plan; no DB writes |
| `--group-report PATH` | optional | Write slate/group report (UTF-8) |

### Morning globals (full-day schedule)

Inserted for **`job_date_et`** (typical ET times):

| Order | `job_type` | Scheduled (ET) |
|-------|------------|----------------|
| 1 | `stats_pull` | 06:00 |
| 2 | `load_today` | 06:05 |
| 3 | `load_weather` | 06:07 |
| 4 | `day_setup` | 06:10 → runs **`--groups-only`** for same date |
| 5 | `prior_report` | 06:15 |
| 6 | `early_peek` | 06:20 |

### Per-group job types (after games exist)

- **`odds_pull` / `odds_check` / `weather`** — one row per **merged odds block** or group per current script logic.
- **`group_brief`** — **`generate_daily_brief.py --session primary`** (may be multiple rows per group with **`--brief-extra-minutes`**).
- **`bet_ledger_sync`** — supported by **`run_pipeline.py`** if you **insert** `pipeline_jobs` rows yourself (current **`schedule_pipeline_day.py`** does **not** auto-insert these); maps to **`--sync-bet-ledger-only`** for recurring T−30 materialization.
- **`ledger_snapshot`** — **`daily_results_report.py`**.

Evening: **`schedule_next_day_globals`** at last group T0 + ~5m ET, running **`--globals-only`** for the **next** calendar day.

---

## Runner: `run_pipeline.py`

### Read-only diagnostics

```text
python batch/jobs/run_pipeline.py --status
python batch/jobs/run_pipeline.py --explain-deps YYYY-MM-DD
```

Optional: `--db PATH` to override `mlb_stats.db`.

### Execute due jobs (loop)

**Typical “all day” run for one slate (sleep until next scheduled job, exit when nothing left pending):**

```text
python batch/jobs/run_pipeline.py --sleep-until-due --job-date-et YYYY-MM-DD --exit-when-no-pending
```

| Flag | Meaning |
|------|--------|
| `--once` | One pass: run whatever is due **now**, then exit |
| `--ghost` | Print commands; **no** subprocess, **no** DB status updates |
| `--poll-seconds N` | If **not** using `--sleep-until-due`, idle polling interval (default 60) |
| `--sleep-until-due` | If nothing due, **`sleep`** until next **`scheduled_time_et`** of a **pending** row (scoped by `--job-date-et` if set) |
| `--job-date-et YYYY-MM-DD` | Scope sleep / exit logic to that **`job_date_et`** |
| `--exit-when-no-pending` | Exit when **`COUNT(*)`** of **`status='pending'`** rows (for that date if scoped) is **0** |
| `--max-sleep-hours H` | Cap one sleep chunk (default 24) before recomputing |
| `--stale-minutes` / `--timeout-minutes` | Recovery for stuck **`running`** rows |

**Important:** **`--exit-when-no-pending`** means “no rows still **`pending`** for that scope,” not “one group finished.” Completed jobs are **`complete`** / **`failed`** / etc., not **`pending`**.

### Workstation vs cloud

- **`--sleep-until-due`** avoids busy polling; it is still **one long-lived Python process**.
- If the **PC sleeps (suspend)**, timers do not run reliably — disable sleep for a true “server-like” day (see prior ops notes).
- For **no** daemon, use Windows **Task Scheduler** to invoke `run_pipeline.py --once` on a grid of times instead.

---

## Commands the runner invokes

Mappings live in **`_build_command()`** in `run_pipeline.py` (abbreviated):

| `job_type` | Command (from repo root) |
|------------|---------------------------|
| `stats_pull` | `python batch/ingestion/load_mlb_stats.py` |
| `load_today` | `python batch/ingestion/load_today.py --date {job_date_et}` |
| `load_weather` | `python batch/ingestion/load_weather.py --date {job_date_et}` |
| `day_setup` | `python batch/jobs/schedule_pipeline_day.py --groups-only --date-et {job_date_et}` |
| `prior_report` | `python batch/pipeline/generate_daily_brief.py --session prior --date {job_date_et}` |
| `early_peek` | `python batch/pipeline/generate_daily_brief.py --session morning --date {job_date_et}` |
| `odds_pull` | `python batch/ingestion/load_odds.py --pregame --markets game --date …` (+ `--force` if slate date ≠ local today) |
| `odds_check` | `python diagnostics/check_odds_ready.py --date {job_date_et}` |
| `weather` | `python batch/ingestion/load_weather.py --date {job_date_et}` |
| `group_brief` | `python batch/pipeline/generate_daily_brief.py --session primary --date {job_date_et}` |
| `bet_ledger_sync` | `python batch/pipeline/generate_daily_brief.py --sync-bet-ledger-only --date {job_date_et}` |
| `ledger_snapshot` | `python batch/pipeline/daily_results_report.py --date {job_date_et}` |
| `schedule_next_day_globals` | `python batch/jobs/schedule_pipeline_day.py --globals-only --date-et {next_calendar_day}` |

### Dependencies (high level)

Defined in **`_dependency_rules()`** — examples:

- **`group_brief`** waits on **`load_today`**, **`odds_pull`**, **`load_weather`** (among others).
- **`bet_ledger_sync`** waits on **`load_today`**.
- **`ledger_snapshot`** waits on **`load_today`**, **`odds_pull`**.

Upstream **`failed`** / **`timeout`** can still count as “resolved” so the slate does not deadlock; see **`--explain-deps`** and changelog in `run_pipeline.py`.

---

## Related documents

- `docs/Generate_Daily_Brief_Guide_2026-04.md` — brief sessions and **`--sync-bet-ledger-only`**
- `docs/MLB_Scout_Daily_Operations_Guide_2026-04.md` — Streamlit Scout + Admin
- `online/app/mlb_scout_admin.py` — operator UI (pipeline tables, `--status` / `--explain-deps`, ingestion triggers)
- `README.md` — tree and setup
