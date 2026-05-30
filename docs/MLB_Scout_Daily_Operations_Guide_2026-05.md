# MLB Scout — Daily Operations Guide

**Version:** 2026-05 (supersedes 2026-04)
**Scope:** Full daily workflow — pipeline, briefs, and the new
Prediction Engine (score_today.py).

---

## Conventions

- **Repository root:** `mlb_stats`
- **Shell:** PowerShell or Command Prompt on Windows
- **Python:** Use the same interpreter / venv as all batch scripts

```text
cd C:\Users\<you>\OneDrive\Documents\Python_Scripts\mlb_stats
```

---

## Two parallel systems

As of May 2026 the project runs two signal systems side by side.

| System | Script | Purpose | Status |
|--------|--------|---------|--------|
| **Brief (OWM/MV-B)** | `generate_daily_brief.py` | Legacy rule-based signals; grading + ledger tracking | Active — grading only |
| **Prediction Engine** | `score_today.py` | ML logistic regression; ML / RL / Under picks | Active — primary decision tool |

The brief pipeline continues to run for grading and bet_ledger
tracking. The prediction engine is the primary source of actionable
picks. They share the same DB but do not depend on each other.

---

## Daily schedule (ET)

| Time | Job | Script |
|------|-----|--------|
| 6:00 AM | Stats pull (yesterday's scores) | `load_mlb_stats.py` |
| 6:05 AM | Load today's games | `load_today.py` |
| 6:10 AM | Prior day report + grading | `generate_daily_brief.py --session prior` |
| 9:00 AM | Opening odds load | `load_odds.py --pregame` |
| **10:00 AM** | **Prediction Engine** | **`score_today.py`** |
| 12:00 PM | Odds refresh | `load_odds.py --pregame` |
| Throughout | Group briefs (per game group) | `generate_daily_brief.py --session primary` |
| 11:30 PM | Line movement compute | `load_odds.py --compute-movement` |

The 10:00 AM score_today run fires automatically via
`pipeline_jobs` (job_type = `score_today`). It requires:
- Today's games loaded (6:05 AM job complete)
- Opening odds available (9:00 AM job complete)
- Probable pitchers posted (typically by 9–10 AM)

---

## Prediction Engine — score_today.py

### What it does

Loads the pre-trained LogReg model (`outputs/models/
outcome_model_logreg.joblib`), constructs identical features to
training for all unplayed games today, joins current odds from
`game_odds`, applies three decision rules, and outputs ranked picks
across three bet types.

### Three signal types

**Moneyline (ML)**
- Both teams ≥ 20 GP in current season
- LogReg confidence ≥ 65%
- Odds tier: −150 to −199 OR −300 or worse
- Backtest (2025, May+): −150/−199 tier → 72.7% acc, +14.7% ROI
  −300+ tier → 85.2% acc, +10.9% ROI

**Run Line (RL)**
- Market favorite ML ≤ −301
- Backtest: 63.2% cover rate, +21.1% ROI at avg −116 RL odds
- Note: 2026 YTD only 3 games — treat with caution

**Under**
- Both SP ERA WMA combined < 6.0 (confirmed SP data required)
- Strong tier: combined ERA < 5.0 + wind blowing in
- Backtest: 44.6% under rate on 652 games; strong tier 41.6%
- ROI on Under at −110: +14.8% standard, +20.6% strong tier

### Output files

| File | Purpose |
|------|---------|
| `outputs/reports/score_today_YYYY-MM-DD.txt` | Pipeline log (operational) |
| `outputs/reports/prediction_engine_YYYY-MM-DD.txt` | Formatted report — email body |
| `outputs/reports/score_today_YYYY-MM-DD.csv` | Full game data with all signal columns |
| `outputs/reports/prediction_engine_log.csv` | Cumulative live tracking log |

### Email delivery

The formatted report is emailed automatically after each run.
Subject: `MLB Scout — {N} Signal(s) Today · {Day} {Month} {DD}`
Attachment: `prediction_engine_YYYY-MM-DD.txt`

To run without email (local only):
```text
python batch/analysis/prediction/score_today.py \
  --db data/mlb_stats.db --no-email
```

### CLI reference

```text
# Run today (standard — includes email)
python batch/analysis/prediction/score_today.py --db data/mlb_stats.db

# Run for a specific date (no email)
python batch/analysis/prediction/score_today.py \
  --db data/mlb_stats.db --date 2026-05-25 --no-email

# Lower confidence threshold (experimental)
python batch/analysis/prediction/score_today.py \
  --db data/mlb_stats.db --threshold 0.62
```

### Model artifacts

Stored in `outputs/models/`:
- `outcome_model_logreg.joblib` — fitted pipeline (scaler + model)
- `outcome_model_gradboost.joblib` — GradBoost (context only; not used for picks)
- `outcome_model_meta.json` — feature list, train medians, thresholds

Retrain trigger: when 2026 season reaches ~80 games played per team
(~late June), retrain on 2024+2025 combined:
```text
python batch/analysis/prediction/outcome_model.py --db data/mlb_stats.db
```

### Early season filter

Both teams must have ≥ 20 games played in the current season before
the model produces a prediction. This filter eliminates early-season
noise where standings features (pythag_diff, win_pct_diff) are
unreliable. Backed by backtest showing −8.4% ROI before May 1 vs
+18.2% ROI from May 1 onward.

---

## Brief pipeline — generate_daily_brief.py

The brief system continues to run for grading and bet_ledger
tracking. It is no longer the primary decision tool. See
`docs/Generate_Daily_Brief_Guide_2026-04.md` for full CLI reference.

Key point: The brief's OWM / MV-B signals are rule-based and use
arbitrary ERA thresholds (≥ 5.0) and OPS gates (≥ 0.8). These gates
block many valid picks that the prediction engine correctly surfaces.
Do not use the brief for pick decisions.

### Brief sessions still running

| Session | Time | Purpose |
|---------|------|---------|
| `prior` | 6:10 AM | Prior day results + grading report |
| `primary` | Per game group | Full slate brief for grading context |
| `late` | ~8:40 PM | West Coast games |

---

## Streamlit apps

```text
# Main app
streamlit run online/app/scout.py

# Admin / pipeline ops
streamlit run online/app/mlb_scout_admin.py
```

Auth: `.streamlit/secrets.toml` → `[auth] admin_password`
Optional: set `MLB_SCOUT_ADMIN_NO_AUTH=1` for local dev

---

## Prediction Engine — live tracking log

`outputs/reports/prediction_engine_log.csv`

Accumulates every signal fired with columns:
```
date, signal_type, game, pick, odds, model_pct, market_pct,
edge, confidence, combined_era, favorite_ml, fav_rl_odds,
result, pl_units
```

`result` and `pl_units` are blank when the row is written
(game not yet played). They are populated manually after grading
or by a future grading automation step.

Signal types: `ML`, `RL`, `UNDER`

---

## Key model decisions (May 2026)

| Decision | Rationale |
|----------|-----------|
| LogReg C=10 (minimal regularization) | C=1.0 compressed all probabilities toward 50%; C=10 allows calibrated high-confidence predictions |
| GradBoost retired from picks | 65–70% bucket showed 34.3% accuracy — miscalibrated; used for context only |
| Odds tier filter replaces edge cap | Edge cap (0–12%) filtered out negative-edge heavy favorites that win 84.1% of the time; tier filter directly supported by backtest |
| May 1 / 20 GP early season filter | March/April accuracy 44–53% vs May+ 82% — structural model failure on small-sample standings features |
| Under uses combined ERA not individual | Each SP < 5.0 allowed combined ERAs of 8+ (no signal); combined < 6.0 matches the backtest bucket showing 44.6% under rate |

---

## Related documents

- `docs/Generate_Daily_Brief_Guide_2026-04.md` — brief CLI
- `docs/Pipeline_Operations_Guide_2026-04.md` — full pipeline schedule
- `outputs/models/outcome_model_meta.json` — live model config
- `outputs/reports/prediction_engine_log.csv` — live signal log
- `README.md` — repository layout
