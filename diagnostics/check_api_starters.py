# paste this into a new file: check_api_starters.py
import json, urllib.request

date = "2026-04-08"
url = (f"https://statsapi.mlb.com/api/v1/schedule"
       f"?sportId=1&startDate={date}&endDate={date}"
       f"&gameType=R&hydrate=probablePitchers")

req = urllib.request.Request(url, headers={"User-Agent": "MLB-Scout/2.5"})
with urllib.request.urlopen(req, timeout=15) as resp:
    data = json.loads(resp.read())

for date_obj in data.get("dates", []):
    for g in date_obj.get("games", []):
        pk     = g.get("gamePk")
        status = g.get("status", {}).get("abstractGameState", "?")
        teams  = g.get("teams", {})
        home   = teams.get("home", {}).get("team", {}).get("name", "?")
        away   = teams.get("away", {}).get("team", {}).get("name", "?")
        hp     = teams.get("home", {}).get("probablePitcher", {})
        ap     = teams.get("away", {}).get("probablePitcher", {})
        print(f"  pk={pk}  status={status}  {away} @ {home}")
        print(f"    home SP: {hp.get('fullName','TBD')} (id={hp.get('id','?')})")
        print(f"    away SP: {ap.get('fullName','TBD')} (id={ap.get('id','?')})")