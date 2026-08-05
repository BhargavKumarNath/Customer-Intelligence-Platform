from __future__ import annotations

import pytest

from src.db import DuckDBConnectionManager
from src.services.errors import UserNotFoundError
from src.services.segmentation import SegmentationService


def test_get_segment_returns_typed_result(connections: DuckDBConnectionManager) -> None:
    service = SegmentationService(connections)
    with connections.cursor() as cur:
        user_id = cur.execute("SELECT user_id FROM user_rfm_segments LIMIT 1").fetchone()[0]

    segment = service.get_segment(user_id)

    assert segment.user_id == user_id
    assert 1 <= segment.r_score <= 5
    assert 1 <= segment.f_score <= 5
    assert 1 <= segment.m_score <= 5
    assert segment.segment


def test_get_segment_raises_for_unknown_user(connections: DuckDBConnectionManager) -> None:
    service = SegmentationService(connections)
    with pytest.raises(UserNotFoundError):
        service.get_segment(-1)
