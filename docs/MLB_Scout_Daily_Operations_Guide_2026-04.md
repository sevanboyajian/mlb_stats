# MLB Scout — Daily Operations Guide

**Version:** 2026-04 (markdown; supersedes day-to-day use of older `.docx` copies)  
**Scope:** Running the Streamlit **MLB Scout** app, using **Operations**, and how it relates to batch jobs.

---

## Conventions

- **Repository root:** `mlb_stats` — the folder that contains `batch/`, `core/`, `online/`, and `data/`.
- **Shell:** Commands below use **PowerShell or Command Prompt** on Windows. Adjust `cd` if your clone lives elsewhere.
- **Python:** Use the same interpreter you use for batch scripts (virtual environment recommended).

```text
cd C:\Users\<you>\OneDrive\Documents\Python_Scripts\mlb_stats
```

---

## What MLB Scout is

**MLB Scout** (`online/app/scout.py`) is a Streamlit application with four main areas:

| Segment | Purpose |
|--------|---------|
| **Data Explorer** | Reference data, stats, odds lookup |
| **Model Workbench** | Backtests and live-style predictions |
| **Operations** | Data load triggers, health checks, brief-related actions |
| **Scorecard** | Model performance over time |

The app resolves the project root and imports `core.db.connection` so it uses the same **`mlb_stats.db`** as batch jobs (via `get_db_path()`).

---

## Start the app

From the repository root:

```text
cd C:\path\to\mlb_stats
streamlit run online/app/scout.py
```

**MLB Scout Admin** (operators — pipeline, ingestion, logs; separate from `scout.py`):

```text
cd C:\path\to\mlb_stats
streamlit run online/app/mlb_scout_admin.py
```

- Auth: `.streamlit/secrets.toml` → `[auth] admin_password`, or set `MLB_SCOUT_ADMIN_NO_AUTH=1` for trusted local dev only.
- If Scout is already using port 8501: `streamlit run online/app/mlb_scout_admin.py --server.port 8503`

Your browser should open to the local Streamlit URL (typically `http://localhost:8501`).

---

## Operations segment

- **Operations** covers data loading triggers, health views, and UI paths to generate briefs where implemented.
- If **`ENABLE_OPS_ADMIN_GATE`** is set to `True` in `online/app/scout.py`, the Operations section requires **`admin_password`** from `.streamlit/secrets.toml`.
- Optional whole-app password: **`ENABLE_PASSWORD_GATE`** + `password` in secrets.

Secrets template: `config/secrets.toml.template` → copy/configure `.streamlit/secrets.toml` (not committed).

---

## How this ties to the pipeline

- **Batch / scheduled work** (loads, `pipeline_jobs`, briefs) is normally driven by **`batch/jobs/run_pipeline.py`** and scripts under **`batch/`**, not only by the UI.
- Use **Operations** for ad hoc checks and triggers; use **`docs/Pipeline_Operations_Guide_2026-04.md`** for the full job schedule and runner commands.
- Brief text files land under **`outputs/briefs/`** when you run `generate_daily_brief.py` (see **`docs/Generate_Daily_Brief_Guide_2026-04.md`**).

---

## Troubleshooting

| Issue | What to check |
|--------|----------------|
| Import / DB errors | Run from repo root or `online/app`; ensure `core/` is importable (app adds repo root to `sys.path`). |
| Wrong or missing DB | Confirm `get_db_path()` / `.env` / `data/mlb_stats.db` location. |
| Streamlit not found | `python -m pip install streamlit` (and project deps) in your venv. |
| Password errors | `ENABLE_*_GATE` flags and `.streamlit/secrets.toml` `[auth]` section. |

---

## Related documents

- `docs/Pipeline_Operations_Guide_2026-04.md` — schedule, `run_pipeline`, `schedule_pipeline_day`
- `docs/Generate_Daily_Brief_Guide_2026-04.md` — `generate_daily_brief.py` CLI
- `README.md` — repository layout and setup notes
