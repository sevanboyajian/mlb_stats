"""Repository root resolution for ``online`` apps (no dependency on ``scout.py``)."""

from __future__ import annotations

from pathlib import Path


def repo_root() -> Path:
    """``mlb_stats`` root (parent of ``online/``)."""
    here = Path(__file__).resolve()
    # online/services/admin_paths.py -> online -> repo
    return here.parent.parent.parent
