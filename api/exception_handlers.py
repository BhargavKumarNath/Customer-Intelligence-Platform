from __future__ import annotations

import traceback

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

    # TEMPORARY - remove once the live 500 on /v1/users/{id}/propensity is diagnosed.
    # Surfaces the real exception in the response body so it doesn't have to be
    # dug out of Render's log dashboard.
    @app.exception_handler(Exception)
    async def _debug_unhandled(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={
                "debug_error_type": type(exc).__name__,
                "debug_error_message": str(exc),
                "debug_traceback": traceback.format_exception(type(exc), exc, exc.__traceback__),
            },
        )
