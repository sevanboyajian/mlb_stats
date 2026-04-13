# batch/jobs/run_experiment.py
#
from batch.analysis.experiments.team_vs_pitching import run_experiment

def run():
    results = run_experiment(conn)

    save_to_file(results)   # NOT DB (yet)