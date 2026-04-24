"""
Pluggable **execution control** for MLB Scout Admin.

The Admin UI is DB-centric (``pipeline_jobs``, ``runner_lock``) and must work
unchanged in the cloud. **Process** start/stop and **log tail** are optional and
backed by this module so the same ``mlb_scout_admin.py`` can target:

* **local** — subprocess + log file (developer workstation)
* **cloud** — HTTP API to a worker (Cloud Run, ECS, etc.)
* **db_only** — no remote control; operator uses their platform for the runner

Environment (all optional)::

    MLB_ADMIN_EXECUTION_MODE   local | cloud | db_only   (default: local)
    MLB_PIPELINE_CONTROL_URL   base URL, e.g. https://api.example.com
    MLB_PIPELINE_CONTROL_TOKEN optional Bearer token

Cloud API contract (implement on your worker; JSON where noted)::

    GET  {BASE}/v1/execution/status
         -> 200 { "running": bool, "pid": int|null, "message": str,
                   "log_path": str|null }
    GET  {BASE}/v1/execution/log?lines=5000
         -> 200 text/plain body
    POST {BASE}/v1/execution/start
         Content-Type: application/json
         body: { "args": ["--sleep-until-due", "--job-date-et", "2026-04-22", ...] }
         -> 200 { "ok": true, "message": "..." }
    POST {BASE}/v1/execution/stop
         -> 200 { "ok": true, "message": "...", "warnings": [] }

``Authorization: Bearer <MLB_PIPELINE_CONTROL_TOKEN>`` if token is set.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from online.services import pipeline_runner_process as prp
from online.services.admin_pipeline import fetch_runner_lock, open_db


def _env_mode() -> str:
    m = (os.environ.get("MLB_ADMIN_EXECUTION_MODE") or "local").strip().lower()
    if m in ("local", "cloud", "db_only"):
        return m
    return "local"


def _control_url() -> str:
    return (os.environ.get("MLB_PIPELINE_CONTROL_URL") or "").strip().rstrip("/")


def _control_token() -> str:
    return (os.environ.get("MLB_PIPELINE_CONTROL_TOKEN") or "").strip()


class ExecutionBackend(ABC):
    """``kind`` is ``local`` | ``cloud`` | ``db_only`` | ``cloud_unconfigured`` for UI branching."""

    @property
    @abstractmethod
    def kind(self) -> str:
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Short label for the UI (e.g. ``local``)."""

    @property
    @abstractmethod
    def supports_start_stop(self) -> bool:
        """If False, hide or disable start/stop controls."""

    @abstractmethod
    def status(self) -> dict[str, Any]:
        """``running`` (bool), ``pid``, ``message``, ``log_path`` (optional)."""

    @abstractmethod
    def read_console(self, *, tail_lines: int) -> str:
        pass

    @abstractmethod
    def start(self, run_args: list[str]) -> tuple[bool, str]:
        pass

    @abstractmethod
    def stop(self) -> tuple[bool, str, list[str]]:
        pass


class LocalSubprocessBackend(ExecutionBackend):
    @property
    def kind(self) -> str:
        return "local"

    @property
    def name(self) -> str:
        return "local (subprocess)"

    @property
    def supports_start_stop(self) -> bool:
        return True

    def status(self) -> dict[str, Any]:
        t = prp.get_tracked_status()
        t["mode"] = "local"
        return t

    def read_console(self, *, tail_lines: int) -> str:
        return prp.read_console_log(tail_lines=tail_lines)

    def start(self, run_args: list[str]) -> tuple[bool, str]:
        return prp.start_runner(run_args)

    def stop(self) -> tuple[bool, str, list[str]]:
        return prp.stop_runner()


class DbOnlyBackend(ExecutionBackend):
    """
    No process control. ``runner_lock`` in SQLite is the only live signal
    the UI can show without a worker API.
    """

    def __init__(self, db_path: str | None) -> None:
        self._db_path = db_path

    @property
    def kind(self) -> str:
        return "db_only"

    @property
    def name(self) -> str:
        return "db_only (no start/stop)"

    @property
    def supports_start_stop(self) -> bool:
        return False

    def status(self) -> dict[str, Any]:
        try:
            con, _p = open_db(self._db_path)
            try:
                row = fetch_runner_lock(con)
            finally:
                con.close()
        except Exception as exc:
            return {
                "running": False,
                "pid": None,
                "message": f"DB: {exc}",
                "log_path": None,
                "mode": "db_only",
            }
        if row:
            return {
                "running": True,
                "pid": row.get("pid"),
                "message": "runner_lock present — a runner may be active (see your execution platform).",
                "log_path": None,
                "mode": "db_only",
            }
        return {
            "running": False,
            "pid": None,
            "message": "No runner_lock; use your scheduler / worker platform for process logs.",
            "log_path": None,
            "mode": "db_only",
        }

    def read_console(self, *, tail_lines: int) -> str:
        return (
            "Execution mode is `db_only`. Stream logs from your cloud logging/metrics system "
            "(CloudWatch, Cloud Logging, Loki, etc.). This app does not attach to a process.\n"
        )

    def start(self, run_args: list[str]) -> tuple[bool, str]:
        return (
            False,
            "db_only mode: start the worker in your environment (K8s, Cloud Run, VM).",
        )

    def stop(self) -> tuple[bool, str, list[str]]:
        return (
            False,
            "db_only mode: stop the worker in your environment.",
            [],
        )


class CloudHttpBackend(ExecutionBackend):
    def __init__(self, base_url: str, token: str) -> None:
        self._base = base_url
        self._token = token

    @property
    def kind(self) -> str:
        return "cloud"

    @property
    def name(self) -> str:
        return f"cloud ({self._base})"

    @property
    def supports_start_stop(self) -> bool:
        return bool(self._base)

    def _headers(self) -> dict[str, str]:
        h = {"Content-Type": "application/json", "Accept": "application/json"}
        if self._token:
            h["Authorization"] = f"Bearer {self._token}"
        return h

    def _req(
        self,
        method: str,
        path: str,
        *,
        body: bytes | None = None,
        expect_json: bool = True,
    ) -> Any:
        url = f"{self._base}{path}"
        r = urllib.request.Request(url, data=body, method=method)
        for k, v in self._headers().items():
            r.add_header(k, v)
        try:
            with urllib.request.urlopen(r, timeout=60) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            return {"_error": f"HTTP {e.code}: {e.read().decode('utf-8', errors='replace')[:2000]}"}
        except urllib.error.URLError as e:
            return {"_error": f"URL error: {e!s}"}
        if not expect_json:
            return raw.decode("utf-8", errors="replace")
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return {"_error": f"Invalid JSON: {raw[:500]!r}"}

    def status(self) -> dict[str, Any]:
        d = self._req("GET", "/v1/execution/status", expect_json=True)
        if isinstance(d, dict) and "_error" in d:
            return {
                "running": False,
                "pid": None,
                "message": str(d.get("_error")),
                "log_path": None,
                "mode": "cloud",
            }
        d = dict(d) if isinstance(d, dict) else {}
        d["mode"] = "cloud"
        d.setdefault("message", "")
        d.setdefault("running", False)
        d.setdefault("log_path", d.get("log_path") or d.get("log_uri"))
        return d

    def read_console(self, *, tail_lines: int) -> str:
        t = self._req(
            "GET",
            f"/v1/execution/log?lines={int(tail_lines)}",
            expect_json=False,
        )
        if isinstance(t, str):
            return t
        if isinstance(t, dict) and t.get("_error"):
            return str(t.get("_error"))
        return str(t)

    def start(self, run_args: list[str]) -> tuple[bool, str]:
        body = json.dumps({"args": list(run_args)}).encode("utf-8")
        d = self._req("POST", "/v1/execution/start", body=body, expect_json=True)
        if not isinstance(d, dict):
            return False, "unexpected response"
        if d.get("_error"):
            return False, str(d["_error"])
        if d.get("ok") is True:
            return True, str(d.get("message") or "ok")
        return False, str(d.get("message") or "start failed")

    def stop(self) -> tuple[bool, str, list[str]]:
        d = self._req("POST", "/v1/execution/stop", body=b"{}", expect_json=True)
        if not isinstance(d, dict):
            return False, "unexpected response", []
        if d.get("_error"):
            return False, str(d["_error"]), []
        if d.get("ok") is True:
            warns = d.get("warnings") or []
            return (
                True,
                str(d.get("message") or "stopped"),
                [str(x) for x in warns] if isinstance(warns, list) else [],
            )
        return False, str(d.get("message") or "stop failed"), []


class CloudUnconfiguredBackend(ExecutionBackend):
    """``MLB_ADMIN_EXECUTION_MODE=cloud`` but ``MLB_PIPELINE_CONTROL_URL`` is unset."""

    @property
    def kind(self) -> str:
        return "cloud_unconfigured"

    @property
    def name(self) -> str:
        return "cloud (set MLB_PIPELINE_CONTROL_URL)"

    @property
    def supports_start_stop(self) -> bool:
        return False

    def status(self) -> dict[str, Any]:
        return {
            "running": False,
            "pid": None,
            "message": "Set MLB_PIPELINE_CONTROL_URL to your worker base URL (see execution_backend.py).",
            "log_path": None,
            "mode": "cloud_unconfigured",
        }

    def read_console(self, *, tail_lines: int) -> str:
        return (
            "Cloud control plane not configured. Set MLB_PIPELINE_CONTROL_URL and implement the "
            "HTTP routes described in online/services/execution_backend.py (or use "
            "MLB_ADMIN_EXECUTION_MODE=db_only to hide execution controls).\n"
        )

    def start(self, run_args: list[str]) -> tuple[bool, str]:
        return (
            False,
            "Set MLB_PIPELINE_CONTROL_URL (and optional MLB_PIPELINE_CONTROL_TOKEN).",
        )

    def stop(self) -> tuple[bool, str, list[str]]:
        return (False, "Set MLB_PIPELINE_CONTROL_URL.", [])


def get_execution_backend(*, admin_db_path: str | None) -> ExecutionBackend:
    mode = _env_mode()
    if mode == "db_only":
        return DbOnlyBackend(admin_db_path)
    if mode == "cloud":
        u = _control_url()
        if not u:
            return CloudUnconfiguredBackend()
        return CloudHttpBackend(u, _control_token())
    return LocalSubprocessBackend()
