from __future__ import annotations

import pytest

from src.config import Settings
from src.db import DuckDBConnectionManager
from src.services.errors import InsufficientHistoryError
from src.services.propensity import PropensityService, load_model


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
