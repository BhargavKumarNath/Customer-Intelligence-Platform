from __future__ import annotations

import pickle
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.config import Settings
from src.db import DuckDBConnectionManager
from src.services.errors import InsufficientHistoryError
from src.services.propensity import PropensityService, load_model

_MODELS_DIR = Path(__file__).resolve().parents[2] / "src" / "models"
_TXT_MODEL = _MODELS_DIR / "propensity_lgbm.txt"
_PKL_MODEL = _MODELS_DIR / "propensity_lgbm.pkl"


def test_score_user_returns_probability_in_range(
    connections: DuckDBConnectionManager, settings: Settings
) -> None:
    model = load_model(settings.model_path)
    service = PropensityService(connections, model)
    with connections.cursor() as cur:
        user_id = cur.execute(
            "SELECT user_id FROM events WHERE event_time < '2019-11-01' GROUP BY user_id LIMIT 1"
        ).fetchone()[0]

    score = service.score_user(user_id)

    assert score.user_id == user_id
    assert 0.0 <= score.purchase_probability <= 1.0


def test_score_user_raises_for_user_with_no_october_history(
    connections: DuckDBConnectionManager, settings: Settings
) -> None:
    model = load_model(settings.model_path)
    service = PropensityService(connections, model)

    with pytest.raises(InsufficientHistoryError):
        service.score_user(-1)


# --- model-format migration (deployment_stages.md Phase 3 task 6 / P3-NEW-3) ---


def _feature_frame() -> pd.DataFrame:
    rng = np.random.default_rng(0)
    return pd.DataFrame(
        rng.integers(0, 50, size=(64, 7)),
        columns=[
            "oct_events",
            "oct_sessions",
            "oct_views",
            "oct_carts",
            "oct_removes",
            "active_span_days",
            "recency_oct",
        ],
    ).astype(float)


def test_native_text_model_is_prediction_identical_to_pickle() -> None:
    frame = _feature_frame()
    from_txt = load_model(_TXT_MODEL).predict(frame, num_threads=1)
    with _PKL_MODEL.open("rb") as fh:
        legacy = pickle.load(fh).predict(frame, num_threads=1)  # nosec B301
    assert np.array_equal(from_txt, legacy)


def test_load_model_prefers_sibling_txt_without_warning(recwarn: pytest.WarningsRecorder) -> None:
    # Passing the legacy .pkl path resolves to the sibling .txt silently.
    model = load_model(_PKL_MODEL)
    assert model.num_trees() > 0
    assert not [w for w in recwarn.list if issubclass(w.category, DeprecationWarning)]


def test_load_model_pickle_fallback_warns(tmp_path: Path) -> None:
    lonely_pkl = tmp_path / "propensity_lgbm.pkl"
    lonely_pkl.write_bytes(_PKL_MODEL.read_bytes())  # no sibling .txt next to it
    with pytest.warns(DeprecationWarning, match="pickle"):
        model = load_model(lonely_pkl)
    assert model.num_trees() > 0
