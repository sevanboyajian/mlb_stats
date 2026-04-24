"""
Check what dates the MLB Stats API files Apr 9 games under.
Run this locally to see the full picture.
"""

# CHANGE LOG (latest first)
# -------------------------
# 2026-04-13 16:24 ET  Chore: add persistent top-of-file change log header.

import argparse
import json
import urllib.request

def fetch(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "mlb_stats/check_mlb_dates"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())

def main() -> None:
    p = argparse.ArgumentParser(
        description="Inspect how MLB Stats API groups games by schedule date vs officialDate."
    )
    p.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    p.add_argument("--end", required=True, help="End date YYYY-MM-DD (inclusive)")
    args = p.parse_args()

    start = str(args.start).strip()
    end = str(args.end).strip()

    print(f"=== Querying {start} → {end} to find all games ===\n")
    data = fetch(
        "https://statsapi.mlb.com/api/v1/schedule?sportId=1"
        f"&startDate={start}&endDate={end}&gameType=R"
        "&hydrate=probablePitcher(note)"
    )

    for date_obj in data.get("dates", []):
        api_date = date_obj["date"]
        games = date_obj.get("games", [])
        print(f"API date: {api_date}  ({len(games)} games)")
        for g in games:
            home = g["teams"]["home"]
            away = g["teams"]["away"]
            hp = home.get("probablePitcher", {})
            ap = away.get("probablePitcher", {})
            status = g.get("status", {}).get("detailedState", "?")
            # officialDate is the LOCAL date MLB considers this game
            official = g.get("officialDate", "?")
            gdate = g.get("gameDate", "?")  # UTC datetime
            print(
                f"  [{official}]  {away['team']['name'][:18]:18} @ "
                f"{home['team']['name'][:18]:18}  "
                f"status={status:12}  "
                f"gameDate_utc={str(gdate)[:19]:19}  "
                f"home SP={hp.get('fullName','TBD')[:20]:20}  "
                f"away SP={ap.get('fullName','TBD')}"
            )
        print()


if __name__ == "__main__":
    main()
