# batch/jobs/run_experiment.py
#
# CHANGE LOG (latest first)
# -------------------------
# 2026-04-13 16:24 ET  Chore: add persistent top-of-file change log header.

from batch.analysis.experiments.team_vs_pitching import run_experiment

def run():
    results = run_experiment(conn)

    save_to_file(results)   # NOT DB (yet)