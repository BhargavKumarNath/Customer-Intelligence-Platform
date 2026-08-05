from __future__ import annotations

from fastapi import APIRouter, Depends
from starlette.concurrency import run_in_threadpool

from api.deps import get_propensity_service
from src.domain.models import PropensityScore
from src.services.propensity import PropensityService

router = APIRouter(prefix="/v1", tags=["propensity"])


@router.get("/users/{user_id}/propensity", response_model=PropensityScore)
async def get_user_propensity(
    user_id: int, service: PropensityService = Depends(get_propensity_service)
) -> PropensityScore:
    return await run_in_threadpool(service.score_user, user_id)
