# batch/analysis/experiments/team_vs_pitching.py
#
def run_experiment(conn):
    results = []

    for game in get_games(conn):
        features = build_features(conn, game["game_pk"])

        team_only = evaluate_team_strength(features)
        pitching_only = evaluate_pitching(features)
        combined = evaluate_combined(features)

        results.append({
            "game_pk": game["game_pk"],
            "team_signal": team_only,
            "pitching_signal": pitching_only,
            "combined_signal": combined
        })

    return results