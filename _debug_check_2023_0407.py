import sqlite3
from core.db.connection import get_db_path


def main():
    db = get_db_path()
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    d = "2023-04-07"
    rows = con.execute(
        """
        SELECT
            g.game_pk,
            g.game_date_et,
            g.game_start_utc,
            ta.abbreviation AS away,
            th.abbreviation AS home,
            ta.name AS away_name,
            th.name AS home_name
        FROM games g
        JOIN teams th ON th.team_id = g.home_team_id
        JOIN teams ta ON ta.team_id = g.away_team_id
        WHERE g.season = 2023
          AND g.game_type = 'R'
          AND g.game_date_et = ?
        ORDER BY g.game_start_utc, g.game_pk
        """,
        (d,),
    ).fetchall()
    print("db:", db)
    print("count:", len(rows))
    for r in rows:
        print(r["away"], "@", r["home"], "pk", r["game_pk"], "start", r["game_start_utc"])

    wanted = {("NYM", "MIA"), ("BAL", "NYY"), ("PHI", "CIN"), ("MIN", "HOU"), ("DET", "BOS")}
    found = [r for r in rows if (r["away"], r["home"]) in wanted]
    print("\nfound wanted:", len(found))
    for r in found:
        print("FOUND", r["away"], "@", r["home"], "pk", r["game_pk"], "start", r["game_start_utc"])

    missing = con.execute(
        "SELECT COUNT(*) AS n FROM games WHERE season=2023 AND game_type='R' AND (game_start_utc IS NULL OR game_start_utc='')"
    ).fetchone()["n"]
    print("\nmissing game_start_utc (2023 R):", missing)


if __name__ == "__main__":
    main()

