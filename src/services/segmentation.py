from __future__ import annotations

from src.db import DuckDBConnectionManager
from src.domain.models import UserSegment
from src.services.errors import UserNotFoundError

_COLUMNS = [
    "user_id",
    "recency_days",
    "frequency",
    "monetary",
    "r_score",
    "f_score",
    "m_score",
    "rfm_total",
    "segment",
]

_QUERY = f"""
    SELECT {", ".join(_COLUMNS)}
    FROM user_rfm_segments
    WHERE user_id = ?
"""


class SegmentationService:
    """Looks up precomputed RFM segments (see src/analysis/segmentation.py for how the
    `user_rfm_segments` table is built)."""

    def __init__(self, connections: DuckDBConnectionManager) -> None:
        self._connections = connections

    def get_segment(self, user_id: int) -> UserSegment:
        with self._connections.cursor() as cur:
            row = cur.execute(_QUERY, [user_id]).fetchone()
        if row is None:
            raise UserNotFoundError(f"No RFM segment found for user_id={user_id}")
        return UserSegment(**dict(zip(_COLUMNS, row, strict=True)))
