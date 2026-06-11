#!/usr/bin/env python3
"""Compare 2024-only backup vs 2024+2025 combined model on 2025 OOS holdout."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from batch.analysis.prediction.outcome_model import (
    FEATURES,
    apply_early_season_filter,
    compute_sp_data_missing,
    engineer_features,
    load_games,
    predicted_correct,
    winner_confidence,
)
from core.db.connection import connect, get_db_path


def _eval_pipeline(pipe, test_df: pd.DataFrame) -> dict:
    x = test_df[FEATURES].astype(float).values
    probs = pipe.predict_proba(x)[:, 1]
    conf = winner_confidence(probs)
    y = test_df["home_win"].astype(int).values
    correct = predicted_correct(probs, y)
    out = {"n": len(test_df), "accuracy": float(correct.mean()) * 100}
    for thr in (0.65, 0.68, 0.70):
        mask = conf >= thr
        out[f"ge{int(thr*100)}_n"] = int(mask.sum())
        out[f"ge{int(thr*100)}_acc"] = (
            float(correct[mask].mean()) * 100 if mask.any() else float("nan")
        )
    # 70-75 bucket calibration
    b70 = (conf >= 0.70) & (conf < 0.75)
    out["bucket_70_75_n"] = int(b70.sum())
    out["bucket_70_75_acc"] = (
        float(correct[b70].mean()) * 100 if b70.any() else float("nan")
    )
    return out


def main() -> None:
    models_dir = ROOT / "outputs" / "models"
    con = connect(get_db_path())
    try:
        df = load_games(con, [2024, 2025])
    finally:
        con.close()
    df, _ = apply_early_season_filter(df, 20)
    df["sp_data_missing"] = compute_sp_data_missing(df)
    df = engineer_features(df)
    train = df[df["season"] == 2024].copy()
    test = df[df["season"] == 2025].copy()

    results = {}
    for label, meta_name, model_name in [
        ("backup_2024only", "outcome_model_meta_2024only_backup.json", "outcome_model_logreg_2024only_backup.joblib"),
        ("new_2024plus2025", "outcome_model_meta.json", "outcome_model_logreg.joblib"),
    ]:
        meta = json.loads((models_dir / meta_name).read_text(encoding="utf-8"))
        medians = meta["feature_medians"]
        test_imp = test.copy()
        for col in FEATURES:
            fill = medians.get(col, 0.0)
            test_imp[col] = test_imp[col].fillna(fill)
        test_imp = test_imp.dropna(subset=FEATURES)
        pipe = joblib.load(models_dir / model_name)
        results[label] = _eval_pipeline(pipe, test_imp)

    lines = [
        "MODEL COMPARISON — 2025 holdout (train excludes 2025 for both evals)",
        "=" * 60,
        "Note: 'new' model was trained on 2024+2025 — 2025 metrics below are",
        "in-sample for the new model, OOS for the backup. Use 2026 OOS for go/no-go.",
        "",
    ]
    for label, r in results.items():
        lines.append(label)
        lines.append(f"  Overall:  N={r['n']}  Acc={r['accuracy']:.1f}%")
        lines.append(
            f"  >=65%:    n={r['ge65_n']}  acc={r['ge65_acc']:.1f}%"
        )
        lines.append(
            f"  >=68%:    n={r['ge68_n']}  acc={r['ge68_acc']:.1f}%"
        )
        lines.append(
            f"  >=70%:    n={r['ge70_n']}  acc={r['ge70_acc']:.1f}%"
        )
        lines.append(
            f"  70-75%:   n={r['bucket_70_75_n']}  acc={r['bucket_70_75_acc']:.1f}%"
        )
        lines.append("")
    out = ROOT / "outputs" / "reports" / "outcome_model_comparison_2025.txt"
    out.write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()
