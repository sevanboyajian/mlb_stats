"""Run Python entrypoints from the repository root (correct ``cwd`` and imports)."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from online.services.admin_paths import repo_root


def run_repo_python(
    rel_script: str,
    args: list[str],
    *,
    timeout: int = 600,
) -> tuple[int, str]:
    """
    Run ``python <repo>/<rel_script>`` with ``cwd`` = repo root.

    ``rel_script`` uses forward slashes (e.g. ``batch/jobs/run_pipeline.py``).
    """
    root = repo_root()
    script = root / rel_script.replace("/", os.sep)
    if not script.is_file():
        return 1, f"Script not found: {script}"

    env = dict(os.environ)
    env.setdefault("PYTHONUTF8", "1")

    try:
        p = subprocess.run(
            [sys.executable, str(script)] + list(args),
            cwd=str(root),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            env=env,
        )
        out = (p.stdout or "") + (p.stderr or "")
        return int(p.returncode), out
    except subprocess.TimeoutExpired:
        return 1, f"Timed out after {timeout}s: {rel_script}"
    except Exception as exc:
        return 1, str(exc)
