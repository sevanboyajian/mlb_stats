#!/usr/bin/env python3
"""Wrapper: run batch.pipeline.build_pitcher_wma (see --backfill)."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from batch.pipeline.build_pitcher_wma import main

if __name__ == "__main__":
    main()
