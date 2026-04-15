#!/usr/bin/env python3
"""
compute_team_vs_pitcher.py
──────────────────────────
Entry point alias for the team-vs-pitcher detail dataset (same implementation as
backtest_team_vs_pitcher.py).
"""

from __future__ import annotations

import os
import sys

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, "..", "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from batch.analysis.backtesting.backtest_team_vs_pitcher import main

if __name__ == "__main__":
    main()
