from __future__ import annotations

from fastapi import APIRouter, Depends
from starlette.concurrency import run_in_threadpool

from api.deps import get_metadata_service, get_segmentation_service
from src.domain.artifacts import SegmentSummary
from src.domain.models import UserSegment
from src.services.metadata import MetadataService
from src.services.segmentation import SegmentationService

router = APIRouter(prefix="/v1", tags=["segments"])


@router.get("/segments", response_model=list[SegmentSummary])
async def list_segments(
    metadata: MetadataService = Depends(get_metadata_service),
) -> list[SegmentSummary]:
    return await run_in_threadpool(metadata.list_segments)


@router.get("/segments/{name}", response_model=SegmentSummary)
async def get_segment(
    name: str, metadata: MetadataService = Depends(get_metadata_service)
) -> SegmentSummary:
    return await run_in_threadpool(metadata.get_segment_summary, name)


@router.get("/users/{user_id}/segment", response_model=UserSegment)
async def get_user_segment(
    user_id: int, service: SegmentationService = Depends(get_segmentation_service)
) -> UserSegment:
    return await run_in_threadpool(service.get_segment, user_id)
