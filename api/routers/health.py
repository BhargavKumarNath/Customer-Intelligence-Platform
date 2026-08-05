from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from src.config import Settings, get_settings

router = APIRouter(tags=["health"])


@router.get("/healthz", summary="Liveness probe")
async def liveness() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready", summary="Readiness probe")
async def readiness(settings: Settings = Depends(get_settings)) -> dict[str, str]:
    if not settings.database_path.exists():
        raise HTTPException(status_code=503, detail="database artifact not found")
    if not settings.model_path.exists():
        raise HTTPException(status_code=503, detail="model artifact not found")
    return {"status": "ready"}
