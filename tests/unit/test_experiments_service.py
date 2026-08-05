from __future__ import annotations

import pytest

from src.db import DuckDBConnectionManager
from src.domain.models import ABTestRequest
from src.services.errors import SegmentNotFoundError
from src.services.experiments import ABTestService


def test_run_simulation_returns_valid_result(connections: DuckDBConnectionManager) -> None:
    service = ABTestService(connections)
    with connections.cursor() as cur:
        segment = cur.execute(
            "SELECT segment FROM user_rfm_segments GROUP BY segment ORDER BY COUNT(*) DESC LIMIT 1"
        ).fetchone()[0]

    result = service.run_simulation(ABTestRequest(segment=segment, lift=0.15))

    assert result.segment == segment
    assert result.control_visitors > 0
    assert result.treatment_visitors > 0
    assert 0.0 <= result.p_value <= 1.0
    assert result.ci_95_lower < result.ci_95_upper


def test_run_simulation_raises_for_unknown_segment(
    connections: DuckDBConnectionManager,
) -> None:
    service = ABTestService(connections)
    with pytest.raises(SegmentNotFoundError):
        service.run_simulation(ABTestRequest(segment="NoSuchSegment"))
