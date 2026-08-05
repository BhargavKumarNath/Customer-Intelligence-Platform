from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

from api.exception_handlers import register_exception_handlers
from api.logging_config import configure_logging
from api.middleware import RequestContextMiddleware
from api.routers import experiments, health, propensity, recommendations, segments
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

    register_exception_handlers(app)

    app.include_router(health.router)
    app.include_router(segments.router)
    app.include_router(propensity.router)
    app.include_router(recommendations.router)
    app.include_router(experiments.router)

    return app


app = create_app()
