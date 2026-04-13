#!/usr/bin/env python3
"""
check_starters.py
-----------------
Quick diagnostic — shows what the MLB Stats API is returning for
probable pitchers for today's games.

Run:  python check_starters.py
      python check_starters.py --date 2026-04-05
"""

import argparse
import json
import urllib.request
from datetime import date, timedelta

MLB_API = "https://statsapi.mlb.com/api/v1"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=date.today().isoformat(),
                   help="Game date to check (default: today)")
    args = p.parse_args()

    game_date = args.date
    next_day  = (date.fromisoformat(game_date) + timedelta(days=1)).isoformat()

    url = (f"{MLB_API}/schedule?sportId=1"
           f"&startDate={game_date}&endDate={next_day}"
           f"&gameType=R"
           f"&hydrate=probablePitchers,teams")

    print(f"\n  Checking MLB API for probable starters on {game_date} ...")
    print(f"  URL: {url}\n")

    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "MLB-Scout/2.5"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        print(f"  ERROR: Could not reach MLB API — {e}")
        return

    dates = data.get("dates", [])
    if not dates:
        print("  No games found for this date.")
        return

    # Raw debug: show structure of first game
    first_game = dates[0].get("games", [{}])[0]
    home_raw   = first_game.get("teams", {}).get("home", {})
    print("  DEBUG — first game home team structure:")
    print(f"    team obj  : {home_raw.get('team', 'MISSING')}")
    print(f"    probablePitcher: {home_raw.get('probablePitcher', 'KEY NOT PRESENT')}")
    print(f"    all home keys: {list(home_raw.keys())}")
    print()

    total = 0
    announced = 0

    print(f"  {'MATCHUP':<14} {'HOME STARTER':<28} {'AWAY STARTER':<28}")
    print(f"  {'-'*14} {'-'*28} {'-'*28}")

    for date_obj in dates:
        for g in date_obj.get("games", []):
            total += 1
            teams  = g.get("teams", {})
            ht     = teams.get("home", {}).get("team", {}).get("abbreviation", "???")
            at     = teams.get("away", {}).get("team", {}).get("abbreviation", "???")
            hp     = teams.get("home", {}).get("probablePitcher")
            ap     = teams.get("away", {}).get("probablePitcher")

            h_name = hp.get("fullName", "TBD") if hp else "not announced"
            a_name = ap.get("fullName", "TBD") if ap else "not announced"

            if hp or ap:
                announced += 1

            matchup = f"{at}@{ht}"
            print(f"  {matchup:<14} {h_name:<28} {a_name:<28}")

    print()
    print(f"  Total games : {total}")
    print(f"  With starters announced: {announced}")

    if announced == 0:
        print()
        print("  No starters announced yet.")
        print("  This is normal early in the day — teams typically file")
        print("  probable pitchers 2-4 hours before first pitch.")
        print("  Re-run load_weather.py closer to game time.")
    else:
        print()
        print("  Starters are available. If load_weather.py still shows")
        print("  zero rows, there may be a DB write issue — contact support.")


if __name__ == "__main__":
    main()
