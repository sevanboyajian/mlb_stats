"""
Start / stop / tail ``batch/jobs/run_pipeline.py`` for MLB Scout Admin.

The runner is a long-lived process; we redirect stdout+stderr to a log under ``logs/``
and persist a small state JSON (PID) so the UI can offer Stop and live viewing.

Only one Admin-started runner is tracked at a time; starting again returns an error
if the recorded PID is still alive.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from online.services.admin_paths import repo_root


def _state_path() -> Path:
    return repo_root() / "logs" / "pipeline_admin_runner_state.json"


def _log_path() -> Path:
    return repo_root() / "logs" / "pipeline_admin_runner_console.log"


def _ensure_logs() -> None:
    d = repo_root() / "logs"
    d.mkdir(parents=True, exist_ok=True)


def _read_state() -> dict[str, Any] | None:
    p = _state_path()
    if not p.is_file():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_state(data: dict[str, Any]) -> None:
    _ensure_logs()
    _state_path().write_text(json.dumps(data, indent=2), encoding="utf-8")


def _clear_state() -> None:
    try:
        p = _state_path()
        if p.is_file():
            p.unlink()
    except OSError:
        pass


def pid_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            out = subprocess.run(
                ["tasklist", "/FI", f"PID eq {int(pid)}"],
                capture_output=True,
                text=True,
                timeout=15,
            ).stdout
        except OSError:
            return False
        return str(int(pid)) in (out or "")
    try:
        os.kill(int(pid), 0)
    except OSError:
        return False
    return True


def get_tracked_status() -> dict[str, Any]:
    """
    Return {running: bool, pid: int|None, started_at: str|None, args: list|None, log_path: str, message: str}
    """
    st = _read_state()
    log = str(_log_path().resolve())
    if not st:
        return {
            "running": False,
            "pid": None,
            "started_at": None,
            "args": None,
            "log_path": log,
            "message": "no runner was started from Admin",
        }
    pid = int(st.get("pid") or 0)
    alive = pid_is_running(pid) if pid else False
    if st and not alive:
        _clear_state()
        st = None
    return {
        "running": bool(alive),
        "pid": pid if alive else None,
        "started_at": (st or {}).get("started_at") if st else None,
        "args": (st or {}).get("args") if st else None,
        "log_path": log,
        "message": "running" if alive else "no Admin-started runner (state cleared if PID was gone)",
    }


def read_console_log(*, max_bytes: int = 1_200_000, tail_lines: int = 4000) -> str:
    p = _log_path()
    if not p.is_file():
        return ""
    try:
        raw = p.read_bytes()
        if len(raw) > max_bytes:
            raw = raw[-max_bytes:]
        text = raw.decode("utf-8", errors="replace")
    except OSError:
        return ""
    lines = text.splitlines()
    if len(lines) > tail_lines:
        lines = lines[-tail_lines:]
    return "\n".join(lines)


def _pump_to_log(proc: "subprocess.Popen[str]", log_path: Path) -> None:
    """Read child stdout+stderr in the parent and append to a log file (Windows-safe)."""
    try:
        with log_path.open("a", encoding="utf-8", errors="replace", buffering=1) as f:
            if proc.stdout is None:
                return
            for line in iter(proc.stdout.readline, ""):
                if not line and proc.poll() is not None:
                    break
                f.write(line)
    except OSError:
        return


def start_runner(
    run_args: list[str],
) -> tuple[bool, str]:
    """
    Start ``run_pipeline.py`` in the background; append stdout+stderr to the console log.

    ``run_args`` are arguments *after* the script (e.g. ['--once']).
    """
    _ensure_logs()
    status = get_tracked_status()
    if status["running"] and status.get("pid"):
        return False, f"A runner is already running (pid={status['pid']}). Stop it first."

    root = repo_root()
    script = root / "batch" / "jobs" / "run_pipeline.py"
    if not script.is_file():
        return False, f"Missing: {script}"

    log_path = _log_path()
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        with log_path.open("a", encoding="utf-8", errors="replace", buffering=1) as f:
            f.write(f"\n\n==== Admin start {ts} ====\nargs: {run_args!r}\n")
    except OSError as exc:
        return False, f"Could not open log: {exc}"

    env = {**os.environ, "PYTHONUTF8": "1"}
    cmd = [sys.executable, "-u", str(script), *run_args]
    try:
        if os.name == "nt":
            creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            proc = subprocess.Popen(
                cmd,
                cwd=str(root),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=env,
                creationflags=creationflags,
            )
        else:
            proc = subprocess.Popen(
                cmd,
                cwd=str(root),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=env,
                start_new_session=True,
            )
    except OSError as exc:
        return False, str(exc)

    t = threading.Thread(
        target=_pump_to_log, args=(proc, log_path), name="admin_pipeline_pump", daemon=True
    )
    t.start()

    _write_state(
        {
            "pid": int(proc.pid),
            "started_at": ts,
            "args": run_args,
        }
    )
    return True, f"Started pid={proc.pid}. Log: {log_path}"


def stop_runner() -> tuple[bool, str, list[str]]:
    """
    Kill the tracked process tree if any; return (ok, message, warnings).
    """
    warnings: list[str] = []
    st = _read_state() or {}
    pid = int(st.get("pid") or 0)
    if pid and pid_is_running(pid):
        if os.name == "nt":
            try:
                r = subprocess.run(
                    ["taskkill", "/PID", str(pid), "/T", "/F"],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if r.returncode not in (0, 128):
                    warnings.append(f"taskkill rc={r.returncode} stderr={r.stderr!s}")
            except OSError as exc:
                return False, str(exc), warnings
        else:
            try:
                import signal

                os.killpg(os.getpgid(pid), signal.SIGTERM)
            except Exception as exc:  # noqa: BLE001
                try:
                    os.kill(pid, signal.SIGTERM)
                except OSError as exc2:
                    return False, f"{exc!s}; {exc2!s}", warnings
        time.sleep(0.4)

    if pid and pid_is_running(pid):
        warnings.append(f"Process {pid} may still be running; check Task Manager.")

    _clear_state()
    try:
        p = _log_path()
        if p.is_file():
            t = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            with p.open("a", encoding="utf-8", errors="replace") as f:
                f.write(f"\n==== Admin stop {t} (pid {pid or 'unknown'}) ====\n")
    except OSError as exc:
        warnings.append(f"Could not append to log: {exc}")

    return True, f"Stop issued for previous pid {pid or 'n/a'}.", warnings
