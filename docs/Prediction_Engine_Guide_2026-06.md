# Prediction Engine Guide

**Version:** 2026-06
**Last updated:** 2026-06-02
**Changes from 2026-05:** OWM home SP gate added; Away Dog RL signal added;
OWM away-offense veto investigated and rejected; series momentum and RL margin
backtests completed.
**Scope:** `batch/analysis/prediction/` — outcome model, live scoring,
odds overlay, O/U + run line backtesting, OWM gate analysis, RL margin analysis.

---

## Overview

The Prediction Engine is a logistic regression model that predicts
home team win probability using pre-game matchup data only — no odds,
no market lines in the model itself. Odds are applied after prediction
to filter for value. It works alongside the brief's rule-based signals
(OWM, Away Dog RL, Streak Fade, LHP Mismatch, MV-B, Under) as the
primary daily decision toolkit.

### Architecture

```
outcome_model.py          Train + backtest
  └─ outputs/models/      Saved artifacts
       └─ outcome_model_logreg.joblib
       └─ outcome_model_meta.json  (includes train_medians)

score_today.py            Daily live scoring
  └─ outputs/reports/
       └─ prediction_engine_YYYY-MM-DD.txt
       └─ score_today_YYYY-MM-DD.csv
       └─ prediction_engine_log.csv

format_report.py          Report formatter + email
odds_overlay.py           Historical odds overlay analysis
ou_rl_backtest.py         O/U and run line backtesting
owm_veto_backtest.py      OWM signal veto/gate analysis
rl_margin_backtest.py     RL margin distribution analysis
series_momentum_backtest.py  Series rubber game + hangover
```

Production brief scoring (same signal rules as `score_today.py` for
OWM and Away Dog RL): `batch/pipeline/score_game.py` via
`generate_daily_brief.py`.

---

## Model details

### Training

- **Algorithm:** LogisticRegression (C=10.0, class_weight='balanced',
  solver='lbfgs', max_iter=1000) with StandardScaler
- **Train set:** 2024 regular season, ≥ 20 GP filter applied
  (N ≈ 2,118 games)
- **Test set:** 2025 + 2026 YTD (N ≈ 2,156 games)
- **Target:** binary — home team wins (1) or away team wins (0)

### Features (17 total)

| Feature | Description |
|---------|-------------|
| `ops_diff` | Home OPS WMA minus away OPS WMA |
| `sp_era_diff` | Away SP ERA WMA minus home SP ERA WMA (positive = home advantage) |
| `sp_whip_diff` | Away SP WHIP WMA minus home SP WHIP WMA |
| `sp_k9_diff` | Home SP K/9 WMA minus away SP K/9 WMA |
| `pythag_diff` | Home pythag win% minus away pythag win% |
| `win_pct_diff` | Home win% minus away win% |
| `home_split_ops` | Home team home OPS minus away team road OPS |
| `run_diff_diff` | (removed — was collinear with pythag_diff) |
| `h_rolling_k_pct` | Home team strikeout rate |
| `a_rolling_k_pct` | Away team strikeout rate |
| `h_rolling_bb_pct` | Home team walk rate |
| `a_rolling_bb_pct` | Away team walk rate |
| `park_factor_runs` | Venue run park factor |
| `elevation_ft` | Venue elevation |
| `home_field` | Constant = 1 (home field advantage intercept) |
| `sp_data_missing` | 1 if either SP has no qualifying starts |
| `min_games_played` | Minimum games played by either team this season |

### Backtest performance

| Metric | Value |
|--------|-------|
| Baseline (always home) | 53.1% |
| Model accuracy (all games) | 54.7% |
| Brier score | 0.247 |
| ≥ 65% confidence pool | 180 games (8.3% of slate) |
| ≥ 65% accuracy | 70.0% |
| ≥ 68% accuracy | 73.3% |
| May 1+ accuracy ≥ 65% | 70.5% |

### Early season filter

Both teams must have ≥ 20 games played in current season.
Applied before training AND scoring — model never sees early games.

Rationale: March/April accuracy 44–53% due to noisy standings
features (pythag_diff, win_pct_diff) on small samples. May 1+
accuracy 82%+ at high confidence.

---

## Decision rules

Three rules applied sequentially. A pick requires all three.

### Rule 1 — Confidence threshold
LogReg predicted probability ≥ 65% for the predicted winner.
(Configurable via `--threshold` flag; default 0.65)

### Rule 2 — Favorites only
Predicted winner must be the market favorite (closing ML < 0).
Underdogs: 0 picks, 0 wins, −100% ROI in backtest — excluded.

### Rule 3 — Odds tier filter
Predicted winner closing ML must be in one of two tiers:

| Tier | ML range | Backtest acc | Backtest ROI |
|------|----------|-------------|-------------|
| Tier 1 | −150 to −199 | 72.7% | +14.7% |
| Tier 2 | −300 or worse | 85.2% | +10.9% |

Excluded tiers (underperform):
- −100 to −149: 55.0% acc, −1.9% ROI
- −200 to −299: 70.8% avg, −1.6% ROI
- Underdog (+odds): 0.0%, −100%

Edge is still computed and displayed for context but is NOT a gate.
The prior edge cap (0–12%) was removing negative-edge heavy favorites
that win 84.1% of the time — those games are now correctly included.

---

## Additional signals (O/U, Run Line, OWM, Away Dog RL)

### Under signal

**Rule:** Both SP ERA WMA (combined) < 6.0, confirmed SP data only.

**Strong tier:** Combined ERA < 5.0 AND wind blowing in.

| Condition | N (backtest) | Under rate | Under ROI at −110 |
|-----------|-------------|-----------|-------------------|
| Combined ERA < 5.0 | 346 | 60.2% | +20.6% |
| Combined ERA < 6.0 | 652 | 44.6% | +14.8% |
| ERA < 5.0 + wind in | ~113 | 41.6% | +20.6% |

Note: individual SP ERA < 5.0 is NOT the rule. The combined total
must be below the threshold. Two SPs each at 4.5 ERA = combined 9.0 —
no signal. Two SPs at 2.0 + 3.5 = combined 5.5 — signal fires.

### OWM signal (Offense Matchup)

**Rule:** All three conditions required.

| Condition | Threshold | Notes |
|-----------|-----------|-------|
| Home offense OPS WMA | ≥ 0.80 | Rolling weighted average |
| Away SP ERA WMA | ≥ 5.00 | MIN_SP_STARTS = 3 required |
| Home SP ERA WMA | < 4.00 | Strong SP gate — NEW 2026-06 |

**Backtest (2019–2025, May–Aug, n=174 Strong SP games):**

| Tier | N | Win rate | ROI |
|------|---|----------|-----|
| Home SP Strong (ERA < 4.0) | 174 | 66.7% | +7.6% |
| Home SP Average (ERA 4.0–5.5) | 75 | 49.3% | −4.2% |
| Home SP Weak (ERA ≥ 5.5) | 62 | 53.2% | −1.8% |

**Gate rationale:** The Average and Weak SP tiers drag OWM win rate
below the baseline. Strong-only fires 174 of 311 historical OWM
games — a 44% volume reduction in exchange for a 66.7% win rate
vs 59.8% baseline.

**Away-offense veto — INVESTIGATED AND REJECTED (2026-06-01):**
Hypothesis: block OWM when away offense OPS WMA ≥ 0.80 (two-sided
matchup). Backtest result: Away Hot 59.6% vs Away Cold 56.9% —
direction is backwards. The away-offense level does not degrade OWM
win rate. No veto was added.

**Brief output when fires:**
```
DATA: home SP ERA WMA {value} (gate < 4.0 — Strong)
```

**NO SIGNAL when home SP fails gate:**
```
[OWM blocked — home SP ERA WMA {value} >= 4.0 (need Strong SP < 4.0)]
```

Implemented in `score_today.py` (`_apply_owm_signal`) and
`batch/pipeline/score_game.py` (`_eval_owm`).

### Run line signal

**Rule:** Market favorite ML ≤ −301.

| Metric | Value |
|--------|-------|
| Backtest games | 57 (2024–2025) |
| Cover rate | 63.2% |
| Avg RL odds | −116 |
| ROI | +21.1% |
| 2026 YTD | 3 games — small sample |

The market prices these teams' run lines at −110 to −130 because it's
pricing uncertainty around covering by 2. Historical data shows these
dominant teams cover −1.5 at 63%+ — the market underprices it.

### Away Dog RL signal (Tier 1)

**Rule:** All three conditions required. Standalone — no ML or OWM
signal required to co-fire.

| Condition | Threshold |
|-----------|-----------|
| Away team is underdog | away ML > home ML |
| Away ML odds | +101 to +130 inclusive |
| Closing total line | ≤ 8.5 |

**Guardrails:**

| Guardrail | Value | Rationale |
|-----------|-------|-----------|
| Juice gate | RL odds must be −190 or better | Backtest breakeven at 66.1% cover is −195; −190 gate leaves variance margin |
| Daily cap | 4 bets maximum per day | Prevents overexposure on high-fire slates |
| Cap sort order | Best juice first (least negative) | Ensures highest-edge games are staked when cap is hit |

Juice gate is strict: `away_rl_odds < -190` blocks; **−190 passes**,
−191 and worse block. Missing RL odds: signal may still qualify
structurally with a verify-juice warning.

**Backtest (May–Aug 2019–2025, away dog +101–130, total ≤ 8.5):**

| Metric | Value |
|--------|-------|
| N | 1,059 |
| Cover rate | 66.1% |
| Implied probability | ~63.9% |
| Edge vs implied | +2.2% |
| Breakeven juice | −195 |

**Broader RL margin context (9,592 games, May–Aug 2019–2025):**
- 28.1% of all MLB games decided by exactly 1 run
- 45.8% decided by ≤ 2 runs (RL-competitive)
- Low-scoring games (≤ 7 runs total): 39.0% one-run rate
- Favorite −1.5: 42.0% cover vs 49% implied — NOT SUPPORTED in aggregate
- Exception: heavy dog +201+ → favorite covers 56.9% (VIABLE, n=552)

**Tiers deferred (not yet implemented):**

| Tier | Condition | Cover rate | Edge | N | Status |
|------|-----------|-----------|------|---|--------|
| Tier 2 | Dog +101–130 & both SP ERA < 4.0 | 66.4% | +10.2% | 405 | Deferred — pending Tier 1 live validation |
| Tier 3 | Home dog +101–130 & total ≤ 8.5 | 58.7% | +9.8% | 641 | Deferred — pending Tier 1 live validation |

**Brief output when fires:**
```
🔥 BET: AWAY +1.5 [{rl_odds}]   [AWAY DOG RL]
DATA: away ML +{ml} (band +101–+130)
DATA: total line {total} (gate ≤ 8.5)
DATA: away RL odds {rl_odds}
DATA: backtest cover rate 66.1% (n=1,059, May–Aug 2019–2025)
STAKE: 0.10u
```

**Near-miss notes:**
```
[Away Dog RL — total {x} above 8.5 gate (need ≤ 8.5)]
[Away Dog RL — RL odds {x} worse than −190 juice gate]
[Away Dog RL — away ML +{x} outside +101–+130 band (not yet implemented tier)]
[Away Dog RL — daily cap reached (4/4)]
```

**Cap-blocked card (shown, not staked):**
```
⛔ NO BET: AWAY +1.5 [{rl_odds}]   [AWAY DOG RL — CAP]
STAKE: 0.00u — NO BET
```

Implemented in `score_today.py` (`_apply_away_dog_rl_signal`,
`_finalize_away_dog_rl_slate`) and `batch/pipeline/score_game.py`
(`_eval_away_dog_rl`, `apply_away_dog_rl_slate_limits` in brief).

---

## Retraining

The model is currently trained on 2024 only. Retrain when:
- 2026 season reaches ~80 GP per team (~late June 2026)
- Significant roster changes affect top teams (trade deadline)

```text
python batch/analysis/prediction/outcome_model.py \
  --db data/mlb_stats.db
```

This regenerates both joblib files and `outcome_model_meta.json`.
After retraining, re-run `score_today.py` to verify output is
consistent with prior days.

---

## Analysis scripts

### outcome_model.py

Full train + backtest. Outputs report and CSVs.

```text
python batch/analysis/prediction/outcome_model.py \
  --db data/mlb_stats.db \
  [--min-games 20] \
  [--seasons 2024 2025 2026] \
  [--output-dir outputs/reports]
```

Outputs:
- `outputs/reports/outcome_model_backtest.txt`
- `outputs/reports/outcome_model_predictions_logreg.csv`
- `outputs/reports/outcome_model_predictions_gradboost.csv`
- `outputs/models/outcome_model_logreg.joblib`
- `outputs/models/outcome_model_meta.json`

### odds_overlay.py

Join prediction CSVs to historical closing odds. Quantifies
whether model edge translates to market value.

```text
python batch/analysis/prediction/odds_overlay.py \
  --db data/mlb_stats.db \
  --logreg outputs/reports/outcome_model_predictions_logreg.csv \
  --gradboost outputs/reports/outcome_model_predictions_gradboost.csv \
  [--threshold 0.65]
```

Outputs:
- `outputs/reports/odds_overlay_report.txt`
- `outputs/reports/odds_overlay_detail.csv`

### ou_rl_backtest.py

O/U and run line historical analysis across 2024–2026.

```text
python batch/analysis/prediction/ou_rl_backtest.py \
  --db data/mlb_stats.db \
  --logreg outputs/reports/outcome_model_predictions_logreg.csv
```

Outputs:
- `outputs/reports/ou_rl_backtest.txt`
- `outputs/reports/ou_backtest_detail.csv`
- `outputs/reports/rl_backtest_detail.csv`

### owm_veto_backtest.py

Tests OWM signal gate conditions (away offense veto, home SP tier).

```text
python batch/analysis/prediction/owm_veto_backtest.py \
  --start-year 2019 --end-year 2025 --output-csv
```

Outputs: `outputs/reports/owm_veto_backtest.txt`, `owm_veto_detail.csv`

### rl_margin_backtest.py

MLB game margin distribution + underdog/favorite RL cover rates.

```text
python batch/analysis/prediction/rl_margin_backtest.py \
  --start-year 2019 --end-year 2025 --output-csv
```

Outputs: `outputs/reports/rl_margin_backtest.txt`, `rl_margin_detail.csv`

### series_momentum_backtest.py

Series rubber game and hangover Under hypotheses.

```text
python batch/analysis/prediction/series_momentum_backtest.py \
  --start-year 2022 --end-year 2025 --output-csv
```

Outputs: `outputs/reports/series_momentum_backtest.txt`, `series_momentum_detail.csv`

---

## Key findings (May–June 2026 backtest)

### What the model does well

- ≥ 65% confidence games win 70% in out-of-sample test (2025–2026)
- May 1+ performance materially stronger than full-season headline
- Negative-edge games (market more confident than model) win 84.1% —
  the model correctly confirms direction even when it underestimates
  the probability
- September is the strongest month (team quality stabilized):
  87.5% accuracy, +30.4% ROI in backtest

### Signal improvements (2026-06)

| Change | Impact |
|--------|--------|
| OWM home SP gate (ERA < 4.0) | Win rate 59.8% → 66.7%; blocks Average SP tier (49.3%) |
| Away Dog RL signal added | New standalone signal; 66.1% cover rate, n=1,059 |
| OWM away-offense veto rejected | No change — away offense level does not degrade OWM |
| Series momentum backtest | Rubber game (49.8%) and hangover Under (48.9%) — NOT SUPPORTED |
| RL margin backtest | 28.1% one-run rate; underdog +1.5 structural edge confirmed |

### What the model does not do well

- Early season (March/April): 44–53% accuracy — excluded by filter
- Underdogs: systematic miscalibration, especially large-edge dogs
- −200 to −299 ML tier: underperforms (-1.6% ROI) — excluded by tier filter
- July: lowest monthly accuracy (49.5%) — worth monitoring live

### GradBoost status

GradBoost is built and saved but NOT used for picks. Its 65–70%
confidence bucket shows 34.3% accuracy — broken calibration from
isotonic fitting on small training set. It is retained for research
only. Do not add it back to the agreement filter without a full
recalibration on 2025 data.

---

## Related documents

- `docs/MLB_Scout_Daily_Operations_Guide_2026-06.md`
- `docs/Generate_Daily_Brief_Guide_2026-04.md` — brief CLI
- `outputs/models/outcome_model_meta.json` — live feature list
- `outputs/reports/prediction_engine_log.csv` — live signal log
- `outputs/reports/rl_margin_backtest.txt` — RL margin backtest results
- `outputs/reports/owm_veto_backtest.txt` — OWM gate backtest results
- `outputs/reports/series_momentum_backtest.txt` — series momentum results
