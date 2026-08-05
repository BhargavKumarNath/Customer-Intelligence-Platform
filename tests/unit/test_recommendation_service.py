from __future__ import annotations

from src.db import DuckDBConnectionManager
from src.services.recommendations import RecommendationService


def test_get_recommendations_returns_sorted_by_lift(
    connections: DuckDBConnectionManager,
) -> None:
    service = RecommendationService(connections)
    with connections.cursor() as cur:
        product_id = cur.execute(
            "SELECT product_a FROM predictions_product_affinity LIMIT 1"
        ).fetchone()[0]

    recs = service.get_recommendations(product_id, limit=5)

    assert all(r.product_id != product_id for r in recs)
    lifts = [r.lift for r in recs]
    assert lifts == sorted(lifts, reverse=True)


def test_get_recommendations_respects_limit(connections: DuckDBConnectionManager) -> None:
    service = RecommendationService(connections)
    with connections.cursor() as cur:
        product_id = cur.execute(
            "SELECT product_a FROM predictions_product_affinity LIMIT 1"
        ).fetchone()[0]

    recs = service.get_recommendations(product_id, limit=1)

    assert len(recs) <= 1


def test_get_recommendations_empty_for_unknown_product(
    connections: DuckDBConnectionManager,
) -> None:
    service = RecommendationService(connections)
    assert service.get_recommendations(-1) == []
