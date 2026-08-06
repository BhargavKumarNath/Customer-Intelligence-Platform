from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.services.errors import InsufficientHistoryError, SegmentNotFoundError, UserNotFoundError


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(UserNotFoundError)
    async def _user_not_found(request: Request, exc: UserNotFoundError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(SegmentNotFoundError)
    async def _segment_not_found(request: Request, exc: SegmentNotFoundError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(InsufficientHistoryError)
    async def _insufficient_history(
        request: Request, exc: InsufficientHistoryError
    ) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(exc)})
