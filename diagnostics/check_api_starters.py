#!/usr/bin/env python3
"""
check_api_starters.py
--------------------
Direct MLB Stats API check for probable pitchers (no DB).

Useful to answer: "Is the API returning starters yet?" independent of our loaders.

Run:
  python diagnostics/check_api_starters.py
  python diagnostics/check_api_starters.py --date 2026-04-08
"""

# CHANGE LOG (latest first)
# -------------------------
# 2026-04-13 16:24 ET  Chore: add persistent top-of-file change log header.

import argparse
import json
import urllib.request
from datetime import date as _date

MLB_API = "https://statsapi.mlb.com/api/v1"

def main() -> None:
    p = argparse.ArgumentParser(description="Check MLB API probablePitchers for a date (no DB).")
    p.add_argument("--date", default=_date.today().isoformat(), help="YYYY-MM-DD (default: today)")
    args = p.parse_args()

    d = str(args.date).strip()
    url = (
        f"{MLB_API}/schedule"
        f"?sportId=1&startDate={d}&endDate={d}"
        f"&gameType=R&hydrate=probablePitchers,teams"
    )

    print(f"\n  MLB API probablePitchers check for {d}")
    print(f"  URL: {url}\n")

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "mlb_stats/check_api_starters"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        print(f"  ERROR: Could not reach MLB API — {e}")
        return

    any_games = False
    for date_obj in data.get("dates", []):
        for g in date_obj.get("games", []):
            any_games = True
            pk = g.get("gamePk")
            status = g.get("status", {}).get("detailedState") or g.get("status", {}).get("abstractGameState", "?")
            teams = g.get("teams", {})
            home = teams.get("home", {}).get("team", {}).get("name", "?")
            away = teams.get("away", {}).get("team", {}).get("name", "?")
            hp = teams.get("home", {}).get("probablePitcher", {}) or {}
            ap = teams.get("away", {}).get("probablePitcher", {}) or {}
            print(f"  pk={pk}  status={status}  {away} @ {home}")
            print(f"    home SP: {hp.get('fullName','TBD')} (id={hp.get('id','?')})")
            print(f"    away SP: {ap.get('fullName','TBD')} (id={ap.get('id','?')})")

    if not any_games:
        print("  No games found for this date.")


if __name__ == "__main__":
    main()