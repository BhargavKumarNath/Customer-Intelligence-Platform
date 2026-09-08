"""RFC 9457 (``application/problem+json``) error contract for the API.

Every error response - domain errors, validation errors, bare ``HTTPException``s
raised by routes/probes, and unhandled exceptions - is serialised to the same
envelope::

    {"type", "title", "status", "detail", "instance", "request_id"}

``request_id`` mirrors the ``X-Request-ID`` response header set by
``RequestContextMiddleware`` (both come from ``request.state.request_id``). The
catch-all handler scrubs the body: no exception text, no stack trace, no internal
paths - only the status and the request id a caller can quote to an operator.
"""

from __future__ import annotations

import structlog
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from src.services.errors import InsufficientHistoryError, SegmentNotFoundError, UserNotFoundError

PROBLEM_CONTENT_TYPE = "application/problem+json"

logger = structlog.get_logger("api.error")


def _request_id(request: Request) -> str:
    return getattr(request.state, "request_id", "") or "unknown"


def problem_response(
    request: Request,
    *,
    status: int,
    title: str,
    detail: str,
    type_: str = "about:blank",
) -> JSONResponse:
    request_id = _request_id(request)
    body = {
        "type": type_,
        "title": title,
        "status": status,
        "detail": detail,
        "instance": request.url.path,
        "request_id": request_id,
    }
    return JSONResponse(
        status_code=status,
        content=body,
        media_type=PROBLEM_CONTENT_TYPE,
        headers={"X-Request-ID": request_id},
    )


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(UserNotFoundError)
    async def _user_not_found(request: Request, exc: UserNotFoundError) -> JSONResponse:
        return problem_response(request, status=404, title="User not found", detail=str(exc))

    @app.exception_handler(SegmentNotFoundError)
    async def _segment_not_found(request: Request, exc: SegmentNotFoundError) -> JSONResponse:
        return problem_response(request, status=404, title="Segment not found", detail=str(exc))

    @app.exception_handler(InsufficientHistoryError)
    async def _insufficient_history(
        request: Request, exc: InsufficientHistoryError
    ) -> JSONResponse:
        return problem_response(request, status=422, title="Insufficient history", detail=str(exc))

    @app.exception_handler(RequestValidationError)
    async def _validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        request_id = _request_id(request)
        detail = f"{len(exc.errors())} validation error(s); see 'errors'."
        return JSONResponse(
            status_code=422,
            media_type=PROBLEM_CONTENT_TYPE,
            headers={"X-Request-ID": request_id},
            content={
                "type": "about:blank",
                "title": "Request validation failed",
                "status": 422,
                "detail": detail,
                "instance": request.url.path,
                "request_id": request_id,
                "errors": _jsonable_errors(exc),
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http_exception(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        detail = exc.detail if isinstance(exc.detail, str) else "HTTP error"
        return problem_response(
            request,
            status=exc.status_code,
            title=_reason(exc.status_code),
            detail=detail,
        )

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
        logger.error("unhandled_exception", request_id=_request_id(request), exc_info=exc)
        return problem_response(
            request,
            status=500,
            title="Internal Server Error",
            detail=(
                "An unexpected error occurred. Quote this request_id when reporting the problem."
            ),
        )


def _jsonable_errors(exc: RequestValidationError) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for err in exc.errors():
        out.append(
            {
                "loc": [str(p) for p in err.get("loc", ())],
                "msg": str(err.get("msg", "")),
                "type": str(err.get("type", "")),
            }
        )
    return out


def _reason(status: int) -> str:
    return {
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        405: "Method Not Allowed",
        422: "Unprocessable Entity",
        429: "Too Many Requests",
        500: "Internal Server Error",
        503: "Service Unavailable",
    }.get(status, "HTTP Error")
