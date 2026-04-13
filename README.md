## MLB Stats / MLB Scout Analytics Platform

- **One-liner**: Brief summary (e.g. “Local MLB analytics + betting model platform built around a SQLite database, daily briefs, and a Streamlit app.”)
- **Key capabilities**:
  - Ingest MLB games, odds, and weather into `mlb_stats.db`
  - Run backtests and regression analysis on betting signals
  - Generate daily betting briefs (morning/primary/closing/late)
  - Explore data and operate jobs via the MLB Scout Streamlit app

## Repository Structure

- **`batch/` — automation & jobs**
  - **`batch/ingestion/`**: Data loaders (MLB stats, odds, weather, SBRO, etc.).
    - Example: `load_today.py` — loads today’s schedule into `mlb_stats.db`.
  - **`batch/analysis/`**: Feature engineering, models, studies, and backtests.
    - `analysis/backtesting/` — backtest scripts for signals and hypotheses.
    - `analysis/features/` / `regression/` / `studies/` — modeling utilities and experiments.
  - **`batch/pipeline/`**: End-to-end report generators.
    - `generate_daily_brief.py` — creates daily betting briefs (txt/docx) for sessions.
    - `daily_results_report.py` — other reporting pipelines.
  - **`batch/jobs/`**: Orchestrator or scheduled job entrypoints (if used).

- **`core/` — shared infrastructure**
  - **`core/db/`**: Database schema and helpers (`schema.py`, `schema.sql`).
  - **`core/utils/`**: Cross-cutting utilities (e.g. `log_manager.py`).
  - **`core/models/`**: Reusable domain/model code (signals, features, etc.).

- **`online/` & `app/` — user interfaces**
  - **`online/app/scout.py`**: MLB Scout Streamlit app.
    - Segments: Data Explorer, Model Workbench, Operations, Scorecard.
    - Optional password gate via `.streamlit/secrets.toml`.
  - **`online/components/`, `online/services/`**: UI components and service helpers for the app.
  - **`app/boxscore_app.py`**: Additional local UI (e.g. boxscore viewer).

- **`data/` — local data**
  - `mlb_stats.db` — primary SQLite database backing all scripts and apps.
  - `regression_2026.csv` — regression/model data export.

- **`diagnostics/` — health checks & troubleshooting**
  - `check_db.py`, `check_odds_ready.py`, `check_starters.py`, etc.
  - `diagnose_odds.py` — deeper odds diagnostics.

- **`outputs/` — generated artifacts (not source)**
  - `outputs/briefs/` — dated txt/docx daily briefs by session.
  - `outputs/logs/` — runtime logs (e.g. `load_today_YYYY-MM-DD.log`).
  - `outputs/reports/` — generated analysis/report files.

- **`docs/` & `archive/` — documentation and historical reports**
  - `docs/` — current user guides, methodology docs, and ops guides.
  - `archive/old_docs`, `archive/old_reports` — retired versions and legacy reports.
  - `archive/experiments/` — older experiment documents.

- **`config/` — configuration & secrets template**
  - `.env` — environment variables (local-only; not committed).
  - `secrets.toml.template` — template for `.streamlit/secrets.toml` used by MLB Scout.

- **Other**
  - `structure.txt` — auto-generated tree of the repository (for reference).
  - `.gitignore` — ignores local DB, secrets, `outputs/`, `archive/`, venv, etc.

## Getting Started

- **Prerequisites**
  - Python version, recommended OS, and required packages (list or refer to `requirements.txt`/`pyproject.toml` if you add one).
  - Local SQLite (bundled with Python) and approximate disk space.

- **Setup**
  - Clone the repo.
  - Create and activate a virtual environment.
  - Install dependencies.
  - Configure environment:
    - Copy `config/secrets.toml.template` → `.streamlit/secrets.toml` (optional; for password gate).
    - Create `.env` if needed (DB paths, environment flags).
  - Initialize or verify `mlb_stats.db` (run schema/loader script or first ingestion).

## Typical Workflows

- **1. Ingest data**
  - Use `batch/ingestion` scripts:
    - `python load_mlb_stats.py ...` (bulk historical / date range loads).
    - `python load_today.py` (daily schedule).
    - `python load_odds.py`, `python load_oddswarehouse.py`, `python load_weather.py`, `python load_sbro.py`, etc.
  - Optional: run diagnostics (`diagnostics/check_db.py`, `check_odds_ready.py`).

- **2. Run analysis & backtests**
  - Use `batch/analysis/backtesting/*.py` for signal evaluation.
  - Use `batch/analysis/features/*.py` and `regression_*` scripts for feature/model work.

- **3. Generate daily briefs**
  - After odds pulls, run:
    - `python generate_daily_brief.py --session morning|afternoon|primary|closing|late`
    - Optional flags: `--date`, `--force`, `--dry-run`, `--docx`, `--check-prereqs`.
  - Outputs written under `outputs/briefs/` and logged in the DB.

- **4. Use the MLB Scout app**
  - Start Streamlit:
    - `cd online/app`
    - `streamlit run scout.py`
  - Explore:
    - Data Explorer: DB queries and stats.
    - Model Workbench: backtests and live predictions.
    - Operations: trigger loads, monitor health.
    - Scorecard: performance over time.
  - Optional password gate:
    - Configure `.streamlit/secrets.toml`, set `ENABLE_PASSWORD_GATE` / `ENABLE_OPS_ADMIN_GATE` in `scout.py`.

## Configuration, Secrets, and Security

- **Secrets**
  - Never commit `.env` or `.streamlit/secrets.toml`.
  - Use `config/secrets.toml.template` as the only versioned template.
- **Paths**
  - Note any hard-coded Windows paths (e.g. default DB/odds directories) and how to override them for other environments.

## Development Notes

- **Coding style & version**
  - Python version and style guidelines.
- **Testing**
  - How to run any tests in `tests/` (if/when populated).
- **Contributing**
  - Branching/PR expectations, code review notes.

## License

- State the project’s license and any usage restrictions.
