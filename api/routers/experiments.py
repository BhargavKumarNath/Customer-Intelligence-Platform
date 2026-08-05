from __future__ import annotations

from fastapi import APIRouter, Depends
from starlette.concurrency import run_in_threadpool

from api.deps import get_ab_test_service
from src.domain.models import ABTestRequest, ABTestResult
from src.services.experiments import ABTestService

router = APIRouter(prefix="/v1", tags=["experiments"])


@router.post("/experiments/ab-test", response_model=ABTestResult)
async def simulate_ab_test(
    request: ABTestRequest, service: ABTestService = Depends(get_ab_test_service)
) -> ABTestResult:
    return await run_in_threadpool(service.run_simulation, request)
