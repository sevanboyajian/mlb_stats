"""
Check what dates the MLB Stats API files Apr 9 games under.
Run this locally to see the full picture.
"""
import json, urllib.request

def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent":"MLB-Scout/2.5"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())

# Query a wider window to catch all games regardless of UTC filing date
print("=== Querying Apr 8-11 to find all games ===\n")
data = fetch("https://statsapi.mlb.com/api/v1/schedule?sportId=1"
             "&startDate=2026-04-08&endDate=2026-04-11&gameType=R"
             "&hydrate=probablePitcher(note)")

for date_obj in data.get("dates", []):
    api_date = date_obj["date"]
    games    = date_obj.get("games", [])
    print(f"API date: {api_date}  ({len(games)} games)")
    for g in games:
        home    = g["teams"]["home"]
        away    = g["teams"]["away"]
        hp      = home.get("probablePitcher", {})
        ap      = away.get("probablePitcher", {})
        status  = g.get("status", {}).get("detailedState", "?")
        # officialDate is the LOCAL date MLB considers this game
        official = g.get("officialDate", "?")
        gdate    = g.get("gameDate", "?")  # UTC datetime
        print(f"  [{official}]  {away['team']['name'][:18]:18} @ "
              f"{home['team']['name'][:18]:18}  "
              f"status={status:12}  "
              f"home SP={hp.get('fullName','TBD')[:20]:20}  "
              f"away SP={ap.get('fullName','TBD')}")
    print()
