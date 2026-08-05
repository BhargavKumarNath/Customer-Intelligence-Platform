from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from starlette.concurrency import run_in_threadpool

from api.deps import get_recommendation_service
from src.domain.models import ProductRecommendation
from src.services.recommendations import RecommendationService

router = APIRouter(prefix="/v1", tags=["recommendations"])


@router.get("/products/{product_id}/recommendations", response_model=list[ProductRecommendation])
async def get_product_recommendations(
    product_id: int,
    limit: int = Query(default=10, ge=1, le=50),
    service: RecommendationService = Depends(get_recommendation_service),
) -> list[ProductRecommendation]:
    return await run_in_threadpool(service.get_recommendations, product_id, limit)
