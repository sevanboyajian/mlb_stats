# MLB Scout — Daily Operations Guide

**Version:** 2026-06 (supersedes 2026-05)
**Last updated:** 2026-06-02
**Changes from 2026-05:** OWM home SP gate; Away Dog RL signal; brief
format updates (capped/juice-blocked cards); grading log new signal type.
**Scope:** Full daily workflow — pipeline, briefs, and the Prediction
Engine (`score_today.py`).

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

As of June 2026 the project runs two signal systems side by side.
Both can fire actionable picks; they share the same DB but do not
depend on each other.

| System | Script | Purpose | Status |
|--------|--------|---------|--------|
| **Brief (OWM/MV-B/RL)** | `generate_daily_brief.py` | Rule-based signals including OWM, Away Dog RL, Streak Fade, LHP Mismatch; grading + ledger tracking | Active — primary brief signals |
| **Prediction Engine** | `score_today.py` | ML logistic regression; ML / RL / Under / OWM / Away Dog RL picks | Active — primary decision tool |

Use `score_today.py` for the formatted email report and CSV export.
Use `generate_daily_brief.py` for session briefs, bet_ledger, and
prior-day grading. Signal rules for OWM and Away Dog RL are aligned
between `score_today.py` and `batch/pipeline/score_game.py`.

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
`game_odds`, applies decision rules, and outputs ranked picks
across five bet types (ML, RL favorite, Under, OWM, Away Dog RL).

### Four signal types (plus ML)

**Moneyline (ML)** — unchanged from 2026-05
- Both teams ≥ 20 GP in current season
- LogReg confidence ≥ 65%
- Odds tier: −150 to −199 OR −300 or worse
- Backtest (2025, May+): −150/−199 tier → 72.7% acc, +14.7% ROI
  −300+ tier → 85.2% acc, +10.9% ROI

**Run Line (RL — favorite)** — unchanged from 2026-05
- Market favorite ML ≤ −301
- Backtest: 63.2% cover rate, +21.1% ROI at avg −116 RL odds
- Note: 2026 YTD only 3 games — treat with caution

**Under** — unchanged from 2026-05
- Both SP ERA WMA combined < 6.0 (confirmed SP data required)
- Strong tier: combined ERA < 5.0 + wind blowing in
- Backtest: 44.6% under rate on 652 games; strong tier 41.6%
- ROI on Under at −110: +14.8% standard, +20.6% strong tier

**OWM (Offense Matchup)** ← UPDATED 2026-06
- Home offense OPS WMA ≥ 0.80
- Away SP ERA WMA ≥ 5.0
- Home SP ERA WMA < 4.0 (Strong SP gate — added 2026-06-01)
- MIN_SP_STARTS = 3 on both pitchers
- Backtest: 66.7% win rate, +7.6% ROI (n=174, Strong SP tier only)
- Fires in both `score_today.py` and `score_game.py`

**Away Dog RL (standalone)** ← NEW 2026-06
- Away team is underdog (away ML > home ML)
- Away ML: +101 to +130 inclusive
- Closing total line ≤ 8.5
- Juice gate: RL odds must be −190 or better (−190 passes; −191 blocks)
- Daily cap: 4 bets maximum, sorted by best juice first
- Backtest: 66.1% cover rate, n=1,059 (May–Aug 2019–2025)
- Stake: 0.10u per qualifying staked game
- Standalone — no ML or OWM co-fire required
- Fires in both `score_today.py` and `score_game.py`

Away Dog RL report header example:
```
Away Dog RL signals: 5 fired → 4 staked (juice blocked 1, cap blocked 0)
Cap: 4 per day  |  Juice gate: RL odds must be -190 or better
```

### Output files

| File | Purpose |
|------|---------|
| `outputs/reports/score_today_YYYY-MM-DD.txt` | Pipeline log (operational) |
| `outputs/reports/prediction_engine_YYYY-MM-DD.txt` | Formatted report — email body |
| `outputs/reports/score_today_YYYY-MM-DD.csv` | Full game data with all signal columns |
| `outputs/reports/prediction_engine_log.csv` | Cumulative live tracking log |

CSV columns for Away Dog RL (among others):
`away_dog_rl_signal`, `away_dog_rl_fires`, `away_dog_rl_actionable`,
`away_dog_rl_rank`, `away_dog_rl_juice_blocked`, `away_dog_rl_cap_blocked`,
`away_dog_rl_stake`, `away_dog_rl_block_reason`

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
  --db data/mlb_stats.db --date 2026-06-02 --no-email

# Lower confidence threshold (experimental)
python batch/analysis/prediction/score_today.py \
  --db data/mlb_stats.db --threshold 0.62

# Preview primary brief (no DB writes)
python batch/pipeline/generate_daily_brief.py \
  --session primary --date 2026-06-02 --dry-run
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

The brief runs rule-based signals (including OWM and Away Dog RL),
grading, and `bet_ledger` tracking. See
`docs/Generate_Daily_Brief_Guide_2026-04.md` for full CLI reference.

OWM uses three conditions (home OPS ≥ 0.80, away SP ERA ≥ 5.0,
**home SP ERA < 4.0**). The old two-condition OWM rule (without the
Strong home SP gate) is retired.

Away Dog RL is evaluated per game, then slate-wide juice gate and
daily cap are applied via `apply_away_dog_rl_slate_limits()` after
all games are scored.

### Brief sessions still running

| Session | Time | Purpose |
|---------|------|---------|
| `prior` | 6:10 AM | Prior day results + grading report |
| `primary` | Per game group | Full slate brief — picks + NO SIGNAL |
| `late` | ~8:40 PM | West Coast games |

---

## Away Dog RL — Brief format reference

### ACTION SUMMARY line

```
Away Dog RL signals: {fired} fired → {staked} staked
(juice gate blocked {n}, cap blocked {n})
```

### Staked card

```
🔥 BET: AWAY +1.5 [{rl_odds}]   [AWAY DOG RL]
WHY: Away light underdog in low-total game — structural RL edge.
DATA: away ML +{ml} (band +101–+130)
DATA: total line {total} (gate ≤ 8.5)
DATA: away RL odds {rl_odds}
STAKE: 0.10u ← PLAY THIS
```

### Cap-blocked card (shown, not staked)

```
⛔ NO BET: AWAY +1.5 [{rl_odds}]   [AWAY DOG RL — CAP]
Signal qualifies but daily cap of 4 reached.
Shown for tracking only — do not stake.
DATA: rank {n}/{total} by juice — cap is 4
STAKE: 0.00u — NO BET
```

### Juice-blocked (NO SIGNAL block)

```
[Away Dog RL — {away}@{home} RL odds {x} worse than −190 juice gate]
```

### Near-miss (NO SIGNAL block)

```
[Away Dog RL — total {x} above 8.5 gate (need ≤ 8.5)]
[Away Dog RL — away ML +{x} outside +101–+130 band (not yet implemented tier)]
[OWM blocked — home SP ERA WMA {value} >= 4.0 (need Strong SP < 4.0)]
```

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

Accumulates every **staked** signal fired with columns:
```
date, signal_type, game, pick, odds, model_pct, market_pct,
edge, confidence, combined_era, favorite_ml, fav_rl_odds,
result, pl_units
```

`result` and `pl_units` are blank when the row is written
(game not yet played). They are populated manually after grading
or by a future grading automation step.

Signal types tracked: `ML`, `RL`, `UNDER`, `OWM`, `AWAY_DOG_RL`

Away Dog RL grading notes:
- Record result as WIN if away team wins outright OR favorite wins by
  exactly 1 run (away covers +1.5)
- Record result as LOSS if favorite wins by 2+ runs
- P&L at RL odds (e.g. −172 → win pays +0.58u per unit staked)
- Juice-blocked and cap-blocked games: do not appear in log
  (not staked — no result to record)

---

## Key model decisions (May–June 2026)

| Decision | Rationale |
|----------|-----------|
| LogReg C=10 (minimal regularization) | C=1.0 compressed all probabilities toward 50%; C=10 allows calibrated high-confidence predictions |
| GradBoost retired from picks | 65–70% bucket showed 34.3% accuracy — miscalibrated; used for context only |
| Odds tier filter replaces edge cap | Edge cap (0–12%) filtered out negative-edge heavy favorites that win 84.1% of the time; tier filter directly supported by backtest |
| May 1 / 20 GP early season filter | March/April accuracy 44–53% vs May+ 82% — structural model failure on small-sample standings features |
| Under uses combined ERA not individual | Each SP < 5.0 allowed combined ERAs of 8+ (no signal); combined < 6.0 matches the backtest bucket showing 44.6% under rate |
| OWM home SP gate (ERA < 4.0) | Average SP tier (4.0–5.5) showed 49.3% win rate at n=75 — below coin flip; Strong-only fires 66.7% |
| OWM away-offense veto rejected | Backtest 2019–2025 n=311: Away Hot 59.6% vs Away Cold 56.9% — hot away offense does not degrade OWM |
| Away Dog RL standalone signal | 28.1% of MLB games decided by 1 run; underdog +1.5 covers 66.1% in target band; market structurally underprices RL on light dogs |
| Away Dog RL juice gate −190 | Breakeven at 66.1% cover is −195; −190 gate leaves variance margin |
| Away Dog RL daily cap 4 | Prevents overexposure on high-fire slates (6 fired on first live day 2026-06-02) |

---

## Related documents

- `docs/Prediction_Engine_Guide_2026-06.md` — signal rules reference
- `docs/Generate_Daily_Brief_Guide_2026-04.md` — brief CLI
- `docs/Pipeline_Operations_Guide_2026-04.md` — full pipeline schedule
- `outputs/models/outcome_model_meta.json` — live model config
- `outputs/reports/prediction_engine_log.csv` — live signal log
- `outputs/reports/rl_margin_backtest.txt` — RL margin backtest results
- `outputs/reports/owm_veto_backtest.txt` — OWM gate backtest results
- `outputs/reports/series_momentum_backtest.txt` — series momentum results
- `README.md` — repository layout
