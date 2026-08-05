"""Regression gate: fails CI if propensity_lgbm.pkl is ever swapped for a
materially worse model. Not a measure of true held-out generalisation (the
fixture rows may overlap the original training population) - it exists to
catch accidental model degradation, not to certify model quality.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from sklearn.metrics import roc_auc_score

from src.services.propensity import load_model

FIXTURE_PATH = Path(__file__).resolve().parent.parent / "fixtures" / "propensity_eval.parquet"
MIN_ACCEPTABLE_AUC = 0.65


def test_propensity_model_auc_meets_quality_bar() -> None:
    model = load_model(Path("src/models/propensity_lgbm.pkl"))
    eval_df = pd.read_parquet(FIXTURE_PATH)

    predictions = model.predict(eval_df[list(model.feature_name())])
    auc = roc_auc_score(eval_df["target"], predictions)

    assert auc >= MIN_ACCEPTABLE_AUC, (
        f"propensity_lgbm.pkl ROC-AUC dropped to {auc:.4f} (gate: >= {MIN_ACCEPTABLE_AUC})"
    )
