# Generate Daily Brief — Operations Guide

**Version:** 2026-04 (markdown; aligns with `batch/pipeline/generate_daily_brief.py`)  
**Scope:** CLI usage, sessions, outputs, and **`bet_ledger`** sync from **`signal_state`**.

---

## Conventions

- **Working directory:** repository root **`mlb_stats`**.

```text
cd C:\path\to\mlb_stats
```

- **Script path:** always invoke as:

```text
python batch/pipeline/generate_daily_brief.py [options]
```

---

## What it does

- Builds **text** (and optional file output) **daily betting briefs** by session: prior, morning, early, afternoon, primary, closing, late.
- Writes to **`outputs/briefs/`** by default and records runs in **`brief_log`** (unless **`--dry-run`**).
- **`--sync-bet-ledger-only`** skips brief generation and runs **`generate_bets_from_signal_state`** for **`--date`** (pregame materialization window — see script output and pipeline scheduling).

---

## Sessions (`--session`)

| Session | Typical use |
|---------|-------------|
| `prior` | Prior-day / morning context (paired with scheduling as **`prior_report`**) |
| `morning` | Morning view (**`early_peek`** job uses this session) |
| `early` | Early slate |
| `afternoon` | Afternoon slate |
| `primary` | Main evening slate (**`group_brief`** pipeline job uses **`--session primary`**) |
| `closing` | Closing lines |
| `late` | Late games |

**Required:** `--session` is required **unless** you pass **`--sync-bet-ledger-only`**.

---

## Common commands (from `mlb_stats`)

**Primary brief for a slate date:**

```text
python batch/pipeline/generate_daily_brief.py --session primary --date YYYY-MM-DD
```

**Prior and morning (as in global morning pipeline jobs):**

```text
python batch/pipeline/generate_daily_brief.py --session prior --date YYYY-MM-DD
python batch/pipeline/generate_daily_brief.py --session morning --date YYYY-MM-DD
```

**Regenerate even if already logged:**

```text
python batch/pipeline/generate_daily_brief.py --session primary --date YYYY-MM-DD --force
```

**Preview only (no DB writes, no brief file):**

```text
python batch/pipeline/generate_daily_brief.py --session primary --date YYYY-MM-DD --dry-run
```

**Require odds readiness before generating:**

```text
python batch/pipeline/generate_daily_brief.py --session primary --date YYYY-MM-DD --check-prereqs
```

**Bet ledger sync only (no brief file; used by `bet_ledger_sync` job type):**

```text
python batch/pipeline/generate_daily_brief.py --sync-bet-ledger-only --date YYYY-MM-DD
```

**Wind debugging:**

```text
python batch/pipeline/generate_daily_brief.py --session primary --date YYYY-MM-DD --verbose --debug-wind
```

**“As of” wall clock (America/New_York):**

```text
python batch/pipeline/generate_daily_brief.py --session primary --date YYYY-MM-DD --as-of "2026-04-17 18:30"
```

---

## Output and logging

| Topic | Location / behavior |
|-------|---------------------|
| Default file | `outputs/briefs/YYYY-MM-DD_SESSION.txt` (see `--output`) |
| **`--no-file`** | Console only |
| **`--output PATH`** | Append to given path |
| Duplicate guard | Skips if already in **`brief_log`** unless **`--force`** |
| **`--warn-missing`** | Continue with partial data (missing fields warned) |

---

## Pipeline integration

- **`group_brief`** → `python batch/pipeline/generate_daily_brief.py --session primary --date {job_date_et}`
- **`bet_ledger_sync`** → `python batch/pipeline/generate_daily_brief.py --sync-bet-ledger-only --date {job_date_et}`

Dependencies and ordering are enforced by **`run_pipeline.py`**, not by this script alone. See **`docs/Pipeline_Operations_Guide_2026-04.md`**.

---

## Windows note

The script sets **UTF-8** on stdout/stderr when possible so box-drawing characters in briefs do not crash **cp1252** consoles.

---

## Shadow Filter B paper track (`shadow_filter_b_watch`)

Contrarian dog backtest **Filter B** (midsummer **July–August** only, proxy NO SIGNAL universe, ML **dog ≥ +150**) is modeled in `batch/pipeline/backtest_contrarian_dog.py`. In production it is explicitly **shadow / research-only** — not a ranked signal.

From **2026-07-01** through August, when the brief classifies a game as **NO SIGNAL** and the matchup qualifies (calendar + ML underdog per implied probability at least **+150**), the next primary or closing brief run (without `--dry-run`) **inserts one row per `game_pk`** into **`shadow_filter_b_watch`** (`ON CONFLICT DO NOTHING`; first qualifying session persists). **`dog_win`** outcomes are filled when the game row is **`Final`** in **`games`**.

**Brief indicator:** flagged games carry an ASCII line **`[Shadow B] PAPER TRACK ONLY`** plus the dog side and ML (dim summary line counts how many flagged games appear that day).

**September review (example SQL):**

```sql
SELECT game_date,
       COUNT(*) AS n,
       ROUND(100.0 * SUM(COALESCE(dog_won, 0)) / NULLIF(COUNT(*), 0), 2) AS dog_win_pct,
       ROUND(AVG(CASE WHEN dog_won = 1 THEN CAST(dog_ml AS REAL)/100.0 ELSE -1.0 END), 4) AS flat_units_per_play
FROM shadow_filter_b_watch
WHERE game_date >= '2026-07-01' AND substr(game_date, 6, 2) IN ('07', '08')
GROUP BY game_date ORDER BY game_date;
```

Morning (`early_peek`) briefs skip signal evaluation — there is **no** shadow logging from that session unless that changes later.

---

## Related documents

- `docs/Pipeline_Operations_Guide_2026-04.md` — full pipeline
- `docs/MLB_Scout_Daily_Operations_Guide_2026-04.md` — Streamlit Operations UI (optional triggers)
- `README.md` — `outputs/briefs/` and repo layout
