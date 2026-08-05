from __future__ import annotations

from src.db import DuckDBConnectionManager
from src.domain.models import ProductRecommendation

_QUERY = """
    SELECT
        CASE WHEN product_a = ? THEN product_b ELSE product_a END AS product_id,
        pair_count,
        confidence,
        lift
    FROM predictions_product_affinity
    WHERE product_a = ? OR product_b = ?
    ORDER BY lift DESC
    LIMIT ?
"""


class RecommendationService:
    """Reads the precomputed `predictions_product_affinity` table (market-basket
    association rules built offline by src/models/recommendations.py). Never
    recomputes the underlying self-join on request."""

    def __init__(self, connections: DuckDBConnectionManager) -> None:
        self._connections = connections

    def get_recommendations(self, product_id: int, limit: int = 10) -> list[ProductRecommendation]:
        with self._connections.cursor() as cur:
            rows = cur.execute(_QUERY, [product_id, product_id, product_id, limit]).fetchall()
        return [
            ProductRecommendation(product_id=r[0], pair_count=r[1], confidence=r[2], lift=r[3])
            for r in rows
        ]
