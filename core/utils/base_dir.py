"""
base_dir.py
───────────
Cloud-friendly base directory resolver.

Use BASE_DIR to control where runtime artifacts live (logs/ outputs/ backups/ etc.).
If BASE_DIR is not set, default to the repository root (local dev behavior).
"""

from __future__ import annotations

import os
from pathlib import Path


def _repo_root() -> Path:
    # core/utils/base_dir.py -> core -> repo root
    return Path(__file__).resolve().parents[2]


def get_base_dir() -> Path:
    """
    Return the configured base directory.

    Priority:
    - BASE_DIR env var (absolute or relative; relative is resolved against repo root)
    - repo root (default)
    """
    raw = (os.getenv("BASE_DIR") or "").strip()
    if not raw:
        return _repo_root()
    p = Path(raw)
    if p.is_absolute():
        return p
    return (_repo_root() / p).resolve()


def resolve_base_path(*parts: str) -> Path:
    """Join path parts onto BASE_DIR."""
    p = get_base_dir()
    for part in parts:
        p = p / str(part)
    return p

