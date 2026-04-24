#!/usr/bin/env python3
"""
MLB Scout Admin — operator console (ingestion, pipeline, diagnostics).

Run from repository root::

    streamlit run online/app/mlb_scout_admin.py

**Cloud-ready layout**
  * **Execution control** (start/stop, log tail) is pluggable: see
    ``online/services/execution_backend.py`` and env ``MLB_ADMIN_EXECUTION_MODE``
    (``local`` / ``cloud`` / ``db_only``).
  * **Pipeline state** (``pipeline_jobs``, ``runner_lock``) and DB fixes are always
    first-class — same SQLite (or a connected replica) in the cloud.

The Streamlit file stays thin; swap **execution** for HTTP without rewriting the
pipeline tables UI.

``scout.py`` is intentionally not imported (it executes on import).

Environment:

- ``MLB_SCOUT_ADMIN_NO_AUTH=1`` — skip password gate (local dev only).
- Otherwise requires ``.streamlit/secrets.toml`` with ``[auth] admin_password = \"...\"``.
- ``MLB_ADMIN_EXECUTION_MODE`` — ``local`` (default), ``cloud``, or ``db_only``;
  for ``cloud`` set ``MLB_PIPELINE_CONTROL_URL`` (see ``execution_backend.py``).
"""

from __future__ import annotations

import os
import sys
import time
from datetime import date, datetime
from pathlib import Path

# Repo root on sys.path (for ``core`` + ``online`` package imports)
_REPO = Path(__file__).resolve().parent.parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

try:
    from zoneinfo import ZoneInfo

    _ET = ZoneInfo("America/New_York")
except Exception:
    _ET = None

import pandas as pd
import streamlit as st

from online.services.admin_pipeline import (
    clear_runner_lock,
    count_pending,
    fetch_brief_log_recent,
    fetch_last_job_runs,
    fetch_pipeline_job_by_id,
    fetch_pipeline_jobs,
    fetch_pipeline_jobs_all_columns,
    fetch_pipeline_jobs_multi_status,
    fetch_runner_lock,
    open_db,
    update_pipeline_job_row,
)
from online.services.admin_shell import run_repo_python
from online.services.execution_backend import get_execution_backend

# ── Optional password gate (on by default) ─────────────────────────────────
_ADMIN_NO_AUTH = os.environ.get("MLB_SCOUT_ADMIN_NO_AUTH", "").strip() == "1"


def _password_gate() -> None:
    if _ADMIN_NO_AUTH:
        return
    try:
        expected = st.secrets["auth"]["admin_password"]
    except (KeyError, FileNotFoundError, TypeError):
        st.error(
            "MLB Scout Admin requires `.streamlit/secrets.toml` with `[auth] admin_password = \"...\"` "
            "or set environment variable MLB_SCOUT_ADMIN_NO_AUTH=1 for local dev only.",
            icon="🔒",
        )
        st.stop()

    if st.session_state.get("_mlb_scout_admin_ok"):
        return

    st.markdown("<div style='height:80px'></div>", unsafe_allow_html=True)
    _, mid, _ = st.columns([1, 2, 1])
    with mid:
        st.markdown(
            "<div style='font-family:IBM Plex Mono,monospace;font-size:11px;color:#64748b;"
            "text-transform:uppercase;letter-spacing:1px;margin-bottom:16px;text-align:center;'>"
            "MLB Scout Admin — operator access</div>",
            unsafe_allow_html=True,
        )
        pw = st.text_input("Admin password", type="password", key="_admin_pw")
        if st.button("Unlock", type="primary", use_container_width=True, key="_admin_btn"):
            if pw == expected:
                st.session_state["_mlb_scout_admin_ok"] = True
                st.rerun()
            else:
                st.error("Incorrect password.", icon="🔒")
    st.stop()


def _today_et_iso() -> str:
    if _ET is not None:
        return datetime.now(tz=_ET).date().isoformat()
    return date.today().isoformat()


def _panel(title: str, subtitle: str = "") -> None:
    sub_html = ""
    if subtitle:
        sub_html = (
            "<div style='color:#64748b;font-size:12px;margin-top:6px;'>"
            f"{subtitle}</div>"
        )
    st.markdown(
        "<div style='margin-bottom:18px;border-bottom:1px solid #1e2330;padding-bottom:12px;'>"
        "<div style='font-family:Bebas Neue,sans-serif;font-size:28px;letter-spacing:1px;color:#e8a020;'>"
        f"{title}</div>"
        f"{sub_html}</div>",
        unsafe_allow_html=True,
    )


def _show_df(df: pd.DataFrame, height: int = 360) -> None:
    if df.empty:
        st.info("No rows.")
        return
    st.dataframe(df, use_container_width=True, height=height, hide_index=True)


def page_dashboard() -> None:
    _panel("Dashboard", "Pipeline snapshot and runner lock (read-only + lock clear in Runner page).")
    slate = st.session_state.get("admin_job_date_et") or _today_et_iso()
    st.caption(f"Eastern slate focus: **{slate}** (change in sidebar)")

    try:
        con, dbp = open_db(st.session_state.get("admin_db_path") or None)
    except Exception as exc:
        st.error(f"Database: {exc}")
        return

    try:
        lock = fetch_runner_lock(con)
        pending = count_pending(con, slate)
        df_run = fetch_last_job_runs(con, 8)
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Pending (scoped)", pending)
        with c2:
            st.metric("DB", Path(dbp).name)
        with c3:
            if lock:
                st.warning(f"runner_lock: pid={lock.get('pid')} host={lock.get('host')}")
            else:
                st.success("runner_lock: clear")

        st.subheader("Recent pipeline_job_runs")
        _show_df(df_run, 280)

        st.subheader("Non-complete jobs (scoped)")
        df = fetch_pipeline_jobs_multi_status(
            con,
            ("pending", "running", "failed", "timeout", "skipped"),
            job_date_et=slate,
        )
        _show_df(df, 400)
    finally:
        con.close()


def page_pipeline() -> None:
    _panel("Pipeline", "Browse `pipeline_jobs`, run explain-deps, fix single rows in the DB.")
    slate = st.session_state.get("admin_job_date_et") or _today_et_iso()
    try:
        con, dbp = open_db(st.session_state.get("admin_db_path") or None)
    except Exception as exc:
        st.error(f"Database: {exc}")
        return

    try:
        st.caption(f"Database: `{dbp}`")
        scope_all = st.checkbox("Show all dates (ignore slate filter)", value=False, key="pl_scope")
        jd = None if scope_all else slate

        tabs = st.tabs(
            [
                "Slate (wide)",
                "Pending",
                "Running",
                "Complete",
                "Skipped",
                "Failed / timeout",
                "Last runs",
            ]
        )
        with tabs[0]:
            st.caption("Up to 800 rows for the current slate; use sidebar `job_date_et` or clear scope above.")
            _show_df(fetch_pipeline_jobs_all_columns(con, job_date_et=jd, limit=800), 480)
        with tabs[1]:
            _show_df(fetch_pipeline_jobs(con, status="pending", job_date_et=jd))
        with tabs[2]:
            _show_df(fetch_pipeline_jobs(con, status="running", job_date_et=jd))
        with tabs[3]:
            _show_df(fetch_pipeline_jobs(con, status="complete", job_date_et=jd))
        with tabs[4]:
            _show_df(fetch_pipeline_jobs(con, status="skipped", job_date_et=jd))
        with tabs[5]:
            _show_df(fetch_pipeline_jobs_multi_status(con, ("failed", "timeout"), job_date_et=jd))
        with tabs[6]:
            _show_df(fetch_last_job_runs(con, 20))

        st.divider()
        st.subheader("Explain dependencies")
        ed = st.text_input("job_date_et", value=slate, key="explain_deps_date")
        if st.button("Run explain-deps", type="primary", key="btn_explain"):
            extra = []
            if st.session_state.get("admin_db_path"):
                extra = ["--db", str(st.session_state["admin_db_path"])]
            rc, out = run_repo_python(
                "batch/jobs/run_pipeline.py",
                ["--explain-deps", ed.strip()] + extra,
                timeout=120,
            )
            st.code(out or "(no output)", language="text")
            if rc != 0:
                st.error(f"Exit code {rc}")

        st.divider()
        st.subheader("Fix one job (`pipeline_jobs`)")
        st.caption(
            "Use to clear stuck `running` rows (→ `pending`), reset retries, or mark terminal states. "
            "Prefer fixing **one** row; invalid transitions can confuse the runner."
        )
        jfix = st.number_input("job_id", min_value=1, step=1, value=1, key="fix_job_id")
        row = fetch_pipeline_job_by_id(con, int(jfix))
        if not row:
            st.warning("No row for that job_id.")
        else:
            st.json(dict(row))

        n_status = st.selectbox(
            "New status",
            ["pending", "running", "complete", "failed", "timeout", "skipped"],
            key="fix_status",
            disabled=row is None,
        )
        n_err = st.text_input(
            "error_message (blank = NULL)",
            value=(row or {}).get("error_message") or "",
            key="fix_err",
            disabled=row is None,
        )
        n_retry = st.number_input(
            "retry_count (set)",
            value=int((row or {}).get("retry_count") or 0) if row else 0,
            min_value=0,
            max_value=1_000_000,
            step=1,
            key="fix_retry",
            disabled=row is None,
        )
        clear_ts = st.checkbox(
            "When status is `pending`, clear `started_at` and `completed_at` (re-queue)",
            value=True,
            key="fix_clear_ts",
            disabled=row is None,
        )
        if st.button("Apply UPDATE", type="primary", key="fix_apply", disabled=row is None):
            uerr: str | None
            uerr = update_pipeline_job_row(
                con,
                job_id=int(jfix),
                status=n_status,
                error_message=(n_err.strip() or None),
                retry_count=int(n_retry),
                clear_timestamps_for_retry=bool(clear_ts and n_status == "pending"),
            )
            if uerr:
                st.error(uerr)
            else:
                st.success("Updated.")
                st.rerun()
    finally:
        con.close()


def page_runner() -> None:
    _panel(
        "Runner & console",
        "Execution is pluggable (local / cloud / db_only). `pipeline_jobs` and `runner_lock` are always the DB view.",
    )
    dbp = (st.session_state.get("admin_db_path") or "").strip()
    extra_db: list[str] = ["--db", dbp] if dbp else []
    slate = (st.session_state.get("admin_job_date_et") or _today_et_iso()).strip()
    be = get_execution_backend(admin_db_path=dbp or None)
    st.info(f"**Execution backend:** {be.name}  ·  `kind={be.kind}`")

    st.subheader("Live console (execution plane)")
    tstat = be.status()
    log_hint = tstat.get("log_path")
    st.caption(
        (f"Log: `{log_hint}`" if log_hint else "Log path from control plane (or use text below).")
    )
    if tstat.get("running"):
        pid = tstat.get("pid")
        spid = f"pid ` {pid} `" if pid is not None else "active"
        st.success(
            f"**RUNNING** — {spid} (since {tstat.get('started_at', '—')})  ·  {tstat.get('message', '')}"
        )
    else:
        st.info(tstat.get("message", "Not running (or not reported by the execution backend)."))

    if be.supports_start_stop:
        c1, c2, c3 = st.columns(3)
        with c1:
            once_mode = st.checkbox("--once (single pass, no sleep-until-due)", value=False, key="rn_once")
        with c2:
            exit_no = st.checkbox(
                "--exit-when-no-pending",
                value=True,
                key="rn_exit",
                disabled=once_mode,
            )
        with c3:
            force_on_start = st.checkbox(
                "--force-unlock on start",
                value=True,
                key="rn_force",
            )

        if st.button("Start pipeline (remote / local per backend)", type="primary", key="rn_start"):
            run_args: list[str] = list(extra_db)
            if force_on_start:
                run_args.append("--force-unlock")
            if once_mode:
                run_args.append("--once")
            else:
                run_args.extend(
                    [
                        "--sleep-until-due",
                        "--job-date-et",
                        slate,
                    ]
                )
                if exit_no:
                    run_args.append("--exit-when-no-pending")
            ok, msg = be.start(run_args)
            if ok:
                st.success(msg)
            else:
                st.error(msg)
            time.sleep(0.3)
            st.rerun()

        c4, c5 = st.columns(2)
        with c4:
            if st.button("Stop (execution plane)", type="primary", key="rn_stop"):
                _ok, msg, warns = be.stop()
                st.success(msg)
                for w in warns:
                    st.warning(w)
                if st.session_state.get("clear_lock_on_stop", True) and be.kind == "local":
                    try:
                        con0, _ = open_db(dbp or None)
                        if clear_runner_lock(con0):
                            st.info("Cleared `runner_lock` in DB after stop (local backend).")
                        con0.close()
                    except OSError as exc:
                        st.warning(f"Lock clear: {exc}")
                time.sleep(0.2)
                st.rerun()
        with c5:
            st.checkbox(
                "Also clear `runner_lock` in DB after stop (local subprocess only)",
                value=True,
                key="clear_lock_on_stop",
                disabled=(be.kind != "local"),
            )
    else:
        st.caption("Start/Stop is disabled in this **execution** mode. Use your cloud scheduler, worker, or k8s.")

    frag = getattr(st, "fragment", None)
    use_manual = True
    if callable(frag):
        try:

            @frag(run_every=2)
            def _auto_console() -> None:
                st.code(
                    be.read_console(tail_lines=5000) or "(no console text yet)",
                    language="text",
                )

            _auto_console()
            use_manual = False
        except TypeError:
            use_manual = True
    if use_manual:
        st.caption("Auto-refresh needs Streamlit with `@st.fragment(run_every=…)`; use **Refresh**.")
        if st.button("Refresh console", key="rn_refresh_cons"):
            st.rerun()
        st.code(
            be.read_console(tail_lines=5000) or "(no console text yet)",
            language="text",
        )

    st.divider()
    st.subheader("Database lock (CLI runner)")

    try:
        con, path = open_db(dbp or None)
    except Exception as exc:
        st.error(f"Database: {exc}")
        return

    try:
        lock = fetch_runner_lock(con)
        if lock:
            st.write("Current **runner_lock** row:", lock)
        else:
            st.success("No `runner_lock` row (unlocked).")

        if st.button("Clear runner_lock (DB write)", key="pl_clear_rl"):
            if clear_runner_lock(con):
                st.success("runner_lock cleared.")
                st.rerun()
            else:
                st.error("Could not clear runner_lock.")
    finally:
        con.close()

    if be.kind == "local":
        st.divider()
        st.subheader("Read-only: `run_pipeline.py --status` (this machine only)")
        if st.button("Run `run_pipeline.py --status`", key="pl_run_status"):
            rc, out = run_repo_python(
                "batch/jobs/run_pipeline.py", ["--status"] + extra_db, timeout=120
            )
            st.code(out, language="text")
            if rc != 0:
                st.error(f"Exit code {rc}")
        st.divider()
        st.subheader("Task Scheduler hint (workstation, ET date)")
        st.markdown(
            "Use **Start in** = repo root. Example **Arguments** for PowerShell "
            "(computes today's ET date, then runs the sleeper):"
        )
        ps_cmd = (
            "-NoProfile -Command \""
            "$tz=[System.TimeZoneInfo]::FindSystemTimeZoneById('Eastern Standard Time'); "
            "$d=[System.TimeZoneInfo]::ConvertTime([datetime]::UtcNow,$tz).ToString('yyyy-MM-dd'); "
            "python batch/jobs/run_pipeline.py --sleep-until-due --job-date-et $d "
            "--exit-when-no-pending --force-unlock"
        )
        if dbp:
            ps_cmd += " --db \"" + str(dbp).replace('"', '`"') + '"'
        ps_cmd += '"'
        st.code(ps_cmd, language="powershell")
    else:
        st.caption("Subprocess / Task Scheduler hints are for **local** mode only. In cloud, wire logs and control to your provider (see `execution_backend.py`).")
    st.caption(f"ET slate in sidebar: **{slate}**")


def page_ingestion() -> None:
    _panel("Ingestion", "Runs scripts with cwd = repo root (paths match ``docs/``).")

    ld = st.date_input("Date", value=date.today(), key="ing_date")

    st.subheader("Schedule & weather")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("load_today.py"):
            rc, out = run_repo_python(
                "batch/ingestion/load_today.py",
                ["--date", ld.isoformat()],
            )
            st.code(out, language="text")
            if rc != 0:
                st.error(f"exit {rc}")
    with c2:
        if st.button("load_weather.py"):
            rc, out = run_repo_python(
                "batch/ingestion/load_weather.py",
                ["--date", ld.isoformat()],
            )
            st.code(out, language="text")
            if rc != 0:
                st.error(f"exit {rc}")

    st.subheader("Odds")
    c3, c4, c5 = st.columns(3)
    with c3:
        if st.button("load_odds pregame (game)"):
            rc, out = run_repo_python(
                "batch/ingestion/load_odds.py",
                ["--pregame", "--markets", "game"],
            )
            st.code(out, language="text")
            if rc != 0:
                st.error(f"exit {rc}")
    with c4:
        if st.button("load_odds late (game)"):
            rc, out = run_repo_python(
                "batch/ingestion/load_odds.py",
                ["--late-games", "--markets", "game"],
            )
            st.code(out, language="text")
            if rc != 0:
                st.error(f"exit {rc}")
    with c5:
        if st.button("check_odds_ready.py"):
            extra = ["--date", ld.isoformat()]
            rc, out = run_repo_python("diagnostics/check_odds_ready.py", extra)
            st.code(out, language="text")
            if rc != 0:
                st.error(f"exit {rc}")

    st.subheader("Stats load")
    if st.button("load_mlb_stats.py (daily default)"):
        rc, out = run_repo_python("batch/ingestion/load_mlb_stats.py", [])
        st.code(out, language="text")
        if rc != 0:
            st.error(f"exit {rc}")
    if st.button("load_mlb_stats.py --retry-errors"):
        rc, out = run_repo_python("batch/ingestion/load_mlb_stats.py", ["--retry-errors"])
        st.code(out, language="text")
        if rc != 0:
            st.error(f"exit {rc}")

    st.subheader("Starters backfill")
    c6, c7 = st.columns(2)
    with c6:
        d0 = st.date_input("From", value=date(2026, 3, 25), key="bf0")
    with c7:
        d1 = st.date_input("To", value=date.today(), key="bf1")
    if st.button("backfill_starters.py --dry-run"):
        rc, out = run_repo_python(
            "batch/ingestion/backfill_starters.py",
            ["--start", d0.isoformat(), "--end", d1.isoformat(), "--dry-run"],
            timeout=600,
        )
        st.code(out, language="text")
        if rc != 0:
            st.error(f"exit {rc}")


def page_briefs() -> None:
    _panel("Briefs (internal)", "QA and logs — not a customer delivery surface.")
    slate = st.session_state.get("admin_job_date_et") or _today_et_iso()

    try:
        con, _ = open_db(st.session_state.get("admin_db_path") or None)
    except Exception as exc:
        st.error(f"Database: {exc}")
        return

    try:
        st.subheader("brief_log (recent)")
        _show_df(fetch_brief_log_recent(con, 30), 360)
    finally:
        con.close()

    st.divider()
    st.subheader("Dry-run brief (stdout only)")
    sess = st.selectbox(
        "Session",
        ["prior", "morning", "early", "afternoon", "primary", "late", "closing"],
        index=4,
    )
    bdate = st.date_input("Game date", value=date.fromisoformat(slate), key="brief_internal_date")
    if st.button("Run generate_daily_brief.py --dry-run"):
        args = [
            "--session",
            sess,
            "--date",
            bdate.isoformat(),
            "--dry-run",
            "--warn-missing",
        ]
        if sess != "prior" and _ET is not None:
            now_et = datetime.now(tz=_ET)
            args.extend(["--as-of", f"{bdate.isoformat()} {now_et.strftime('%H:%M')}"])
        rc, out = run_repo_python("batch/pipeline/generate_daily_brief.py", args, timeout=600)
        st.code(out, language="text")
        if rc != 0:
            st.error(f"exit {rc}")


def page_logs() -> None:
    _panel("Logs", f"Tail files under `{_REPO / 'logs'}`.")
    log_dir = _REPO / "logs"
    if not log_dir.is_dir():
        st.info("No logs/ directory yet.")
        return
    files = sorted([p.name for p in log_dir.glob("*.log")], reverse=True)
    if not files:
        st.info("No .log files in logs/.")
        return
    name = st.selectbox("Log file", files)
    path = log_dir / name
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
        tail = "\n".join(text.splitlines()[-400:])
        st.caption(f"{name} — last ~400 lines")
        st.code(tail, language="text")
    except Exception as exc:
        st.error(str(exc))


def main() -> None:
    st.set_page_config(
        page_title="MLB Scout Admin",
        page_icon="⚙",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.markdown(
        """
<style>
@import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=IBM+Plex+Sans:wght@400;600&display=swap');
html, body, [data-testid="stAppViewContainer"], [data-testid="stMain"], .main {
  background:#0d0f14 !important; color:#e2e8f0 !important;
  font-family:'IBM Plex Sans',sans-serif !important;
}
[data-testid="stSidebar"] { background:#14171f !important; border-right:1px solid #1e2330 !important; }
</style>
""",
        unsafe_allow_html=True,
    )

    _password_gate()

    if "admin_job_date_et" not in st.session_state:
        st.session_state.admin_job_date_et = _today_et_iso()
    if "admin_db_path" not in st.session_state:
        st.session_state.admin_db_path = ""

    with st.sidebar:
        st.markdown("### MLB Scout Admin")
        page = st.radio(
            "Section",
            [
                "Dashboard",
                "Pipeline",
                "Runner & console",
                "Ingestion",
                "Briefs (internal)",
                "Logs",
            ],
            index=0,
        )
        st.session_state.admin_job_date_et = st.text_input(
            "job_date_et (ET slate)",
            value=st.session_state.admin_job_date_et,
        )
        st.session_state.admin_db_path = st.text_input(
            "Optional --db path",
            value=st.session_state.admin_db_path,
            help="Leave blank for get_db_path() default.",
        )
        st.caption("Repo root (cwd for scripts): `" + str(_REPO) + "`")
        st.caption("Cloud: replace this UI with FastAPI; keep `online/services/`.")

    if page == "Dashboard":
        page_dashboard()
    elif page == "Pipeline":
        page_pipeline()
    elif page == "Runner & console":
        page_runner()
    elif page == "Ingestion":
        page_ingestion()
    elif page == "Briefs (internal)":
        page_briefs()
    else:
        page_logs()


# ``streamlit run`` executes this file as a script; keep ``main()`` at module level
# (do not import this module from other Python code).
main()
