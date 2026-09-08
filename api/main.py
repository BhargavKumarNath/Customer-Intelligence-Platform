from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

from api.exception_handlers import register_exception_handlers
from api.logging_config import configure_logging
from api.middleware import RequestContextMiddleware
from api.routers import experiments, health, meta, propensity, recommendations, segments
from src.config import Environment, get_settings


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(json_logs=settings.environment != Environment.LOCAL)

    app = FastAPI(
        title="Customer Intelligence Platform API",
        description=(
            "Typed read endpoints over precomputed RFM segments, LightGBM propensity "
            "scores, market-basket recommendations, and an A/B test simulator."
        ),
        version="0.1.0",
    )

    limiter = Limiter(key_func=get_remote_address, default_limits=[settings.rate_limit])
    app.state.limiter = limiter
    # slowapi's handler signature is narrower than Starlette's generic exception-handler
    # type (takes RateLimitExceeded, not Exception) - a known stub mismatch, not a bug.
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore[arg-type]
    app.add_middleware(SlowAPIMiddleware)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allow_origins,
        allow_credentials=False,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )
    app.add_middleware(RequestContextMiddleware)
    # Trust X-Forwarded-* from the single managed proxy in front of the service
    # (Cloud Run / Cloudflare) so request.url.scheme and client IP are correct
    # for the rate limiter and logs. Configurable via CIP_FORWARDED_ALLOW_IPS.
    app.add_middleware(ProxyHeadersMiddleware, trusted_hosts=settings.forwarded_allow_ips)

    register_exception_handlers(app)

    app.include_router(health.router)
    app.include_router(meta.router)
    app.include_router(segments.router)
    app.include_router(propensity.router)
    app.include_router(recommendations.router)
    app.include_router(experiments.router)

    @app.get("/", include_in_schema=False)
    async def root() -> RedirectResponse:
        return RedirectResponse(url="/docs")

    return app


app = create_app()
