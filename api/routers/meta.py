from __future__ import annotations

from fastapi import APIRouter, Depends
from starlette.concurrency import run_in_threadpool

from api.deps import get_metadata_service
from src.domain.artifacts import ArtifactMeta
from src.domain.models import ModelDetail, ModelSummary, VersionInfo
from src.services.metadata import MetadataService

router = APIRouter(tags=["meta"])


@router.get("/version", response_model=VersionInfo)
async def get_version(
    metadata: MetadataService = Depends(get_metadata_service),
) -> VersionInfo:
    return metadata.version()


@router.get("/v1/meta", response_model=ArtifactMeta)
async def get_meta(
    metadata: MetadataService = Depends(get_metadata_service),
) -> ArtifactMeta:
    return await run_in_threadpool(metadata.meta)


@router.get("/v1/models", response_model=list[ModelSummary])
async def list_models(
    metadata: MetadataService = Depends(get_metadata_service),
) -> list[ModelSummary]:
    return metadata.list_models()


@router.get("/v1/models/propensity", response_model=ModelDetail)
async def get_propensity_model(
    metadata: MetadataService = Depends(get_metadata_service),
) -> ModelDetail:
    return metadata.propensity_model()
