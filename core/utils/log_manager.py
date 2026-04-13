"""
log_manager.py
==============
Shared log rotation utility for MLB Scout scripts.

Keeps the N most recent log files per prefix, deleting the oldest
before writing a new one. Safe to call from any script.

USAGE (from any script)
-----------------------
    from log_manager import rotate_logs, open_log

    # Option 1 — just rotate (delete old logs), then open file yourself:
    log_path = rotate_logs("load_today", date_str="2026-04-09")
    with open(log_path, "w") as fh:
        ...

    # Option 2 — rotate and get an open file handle in one call:
    with open_log("load_mlb_stats", date_str="2026-04-09") as fh:
        ...

LOG FILE NAMING
---------------
Files are named:  <prefix>_<date>.log
Example:          load_today_2026-04-09.log

All log files live in:  <mlb_stats folder>/logs/

RETENTION
---------
Default: keep the 7 most recent files per prefix.
Override: rotate_logs("load_today", keep=14)
"""

# CHANGE LOG (latest first)
# -------------------------
# 2026-04-13 16:24 ET  Chore: add persistent top-of-file change log header.

import os
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Optional

# ── Configuration ─────────────────────────────────────────────────────────────
DEFAULT_KEEP = 7          # number of log files to retain per prefix
LOG_DIR_NAME = "logs"     # subdirectory name relative to the calling script


def _log_dir(script_path: Optional[Path] = None) -> Path:
    """Return the logs/ directory, creating it if absent.

    Uses the directory of the calling script if provided,
    otherwise falls back to the directory of this file.
    """
    base = Path(script_path).parent if script_path else Path(__file__).parent
    d = base / LOG_DIR_NAME
    d.mkdir(parents=True, exist_ok=True)
    return d


def rotate_logs(
    prefix: str,
    date_str: Optional[str] = None,
    keep: int = DEFAULT_KEEP,
    script_path: Optional[Path] = None,
) -> Path:
    """Rotate log files for a given prefix, then return the path for the new log.

    Steps:
      1. List all existing <prefix>_*.log files in the logs/ directory.
      2. Sort by filename (ISO date names sort chronologically).
      3. Delete the oldest files so that after writing the new one there
         are at most `keep` files total.
      4. Return the Path for the new log file (not yet created).

    Args:
        prefix:      Script name, e.g. 'load_today' or 'load_mlb_stats'.
        date_str:    Date string for the new log filename, e.g. '2026-04-09'.
                     Defaults to today's date.
        keep:        Maximum number of log files to retain (default 7).
        script_path: Path of the calling script — used to locate the logs/
                     directory. Pass __file__ from the calling script.

    Returns:
        Path object for the new log file (e.g. logs/load_today_2026-04-09.log).
    """
    if date_str is None:
        date_str = date.today().isoformat()

    log_dir   = _log_dir(script_path)
    new_path  = log_dir / f"{prefix}_{date_str}.log"

    # Find existing logs for this prefix, sorted oldest-first
    existing = sorted(log_dir.glob(f"{prefix}_*.log"))

    # We're about to write a new file — remove old ones so we keep at most
    # `keep` files total (new file counts as one of the kept files).
    to_delete = existing[:max(0, len(existing) - (keep - 1))]
    for old_log in to_delete:
        try:
            old_log.unlink()
        except OSError as e:
            # Non-fatal — log to stderr but don't abort the calling script
            print(f"  ⚠  log_manager: could not delete {old_log.name}: {e}")

    return new_path


@contextmanager
def open_log(
    prefix: str,
    date_str: Optional[str] = None,
    keep: int = DEFAULT_KEEP,
    script_path: Optional[Path] = None,
    mode: str = "w",
    encoding: str = "utf-8",
):
    """Context manager: rotate logs and open the new log file.

    Usage:
        with open_log("load_today", script_path=__file__) as fh:
            subprocess.run(cmd, stdout=fh, stderr=fh)

    Yields an open file handle. The file is closed automatically on exit.
    """
    log_path = rotate_logs(prefix, date_str=date_str, keep=keep,
                           script_path=script_path)
    fh = open(log_path, mode, encoding=encoding)
    try:
        yield fh, log_path
    finally:
        fh.close()
