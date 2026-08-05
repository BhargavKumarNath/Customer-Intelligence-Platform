from __future__ import annotations

from fastapi import APIRouter, Depends
from starlette.concurrency import run_in_threadpool

from api.deps import get_segmentation_service
from src.domain.models import UserSegment
from src.services.segmentation import SegmentationService

router = APIRouter(prefix="/v1", tags=["segments"])


@router.get("/users/{user_id}/segment", response_model=UserSegment)
async def get_user_segment(
    user_id: int, service: SegmentationService = Depends(get_segmentation_service)
) -> UserSegment:
    return await run_in_threadpool(service.get_segment, user_id)
