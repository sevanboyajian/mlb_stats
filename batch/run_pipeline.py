#!/usr/bin/env python3
"""
Single CLI entry point for cron / VM: forwards to batch/jobs/run_pipeline.py unchanged.

Uses runpy so that module executes as ``__main__`` (same behavior as
``python batch/jobs/run_pipeline.py``).
"""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

_BATCH_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _BATCH_DIR.parent

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_JOBS_RUN_PIPELINE = _BATCH_DIR / "jobs" / "run_pipeline.py"

if __name__ == "__main__":
    runpy.run_path(str(_JOBS_RUN_PIPELINE), run_name="__main__")
