from __future__ import annotations

import pickle
import warnings
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from src.db import DuckDBConnectionManager
from src.domain.models import PropensityScore
from src.services.errors import InsufficientHistoryError

if TYPE_CHECKING:
    from lightgbm import Booster

# Matches the temporal split in src/models/train_propensity.py: features are built
# entirely from October behaviour, mirroring exactly what the checked-in model was
# trained on (see Booster.feature_name()).
_FEATURE_QUERY = """
    WITH oct_behavior AS (
        SELECT
            user_id,
            COUNT(*) AS oct_events,
            COUNT(DISTINCT user_session) AS oct_sessions,
            SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS oct_views,
            SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS oct_carts,
            SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) AS oct_removes,
            MAX(event_time) AS last_oct_event,
            date_diff('day', MIN(event_time), MAX(event_time)) AS active_span_days
        FROM events
        WHERE event_time < '2019-11-01' AND user_id = ?
        GROUP BY user_id
    )
    SELECT
        oct_events,
        oct_sessions,
        oct_views,
        oct_carts,
        oct_removes,
        active_span_days,
        date_diff('day', last_oct_event, DATE '2019-11-01') AS recency_oct
    FROM oct_behavior
"""

_FEATURE_ORDER = [
    "oct_events",
    "oct_sessions",
    "oct_views",
    "oct_carts",
    "oct_removes",
    "active_span_days",
    "recency_oct",
]


def load_model(model_path: Path) -> Booster:
    """Load the frozen propensity Booster.

    Native LightGBM text (``propensity_lgbm.txt``, written by ``booster.save_model``)
    is the supported format and is byte-for-byte prediction-identical to the legacy
    pickle. The ``.pkl`` branch is a one-release migration fallback: if the given
    ``.pkl`` has a sibling ``.txt`` the text file wins; otherwise the pickle is
    unpickled and a ``DeprecationWarning`` is emitted. Only ever called with the
    repo's own checked-in artifact path - never a caller-supplied path.
    """
    from lightgbm import Booster

    if model_path.suffix == ".txt":
        return Booster(model_file=str(model_path))

    sibling_txt = model_path.with_suffix(".txt")
    if sibling_txt.exists():
        return Booster(model_file=str(sibling_txt))

    warnings.warn(
        f"Loading the propensity model from a pickle ({model_path.name}) is deprecated; "
        "regenerate it as propensity_lgbm.txt via Booster.save_model(). "
        "The pickle fallback will be removed after one release.",
        DeprecationWarning,
        stacklevel=2,
    )
    with model_path.open("rb") as f:
        return pickle.load(f)  # type: ignore[no-any-return]  # nosec B301


class PropensityService:
    def __init__(self, connections: DuckDBConnectionManager, model: Booster) -> None:
        self._connections = connections
        self._model = model

    def score_user(self, user_id: int) -> PropensityScore:
        with self._connections.cursor() as cur:
            row = cur.execute(_FEATURE_QUERY, [user_id]).fetchone()
        if row is None:
            raise InsufficientHistoryError(
                f"No October activity for user_id={user_id}; cannot build propensity features"
            )
        features = pd.DataFrame([row], columns=_FEATURE_ORDER)
        # num_threads=1: a single-row prediction gets nothing from LightGBM's default
        # multi-threaded OpenMP path, and it avoids thread-spawning issues under
        # heavily CPU-throttled containers (e.g. Render free tier's 0.1 vCPU).
        probability = float(self._model.predict(features, num_threads=1)[0])
        return PropensityScore(user_id=user_id, purchase_probability=probability)
