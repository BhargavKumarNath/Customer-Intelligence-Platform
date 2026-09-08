"""Phase 3 — the RFC 9457 error contract, /ready failure branches, env-driven CORS,
and the proxy-headers hop. See deployment_stages.md Phase 3 test cases + P3-NEW-1.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.main import app, create_app
from src.config import Settings, get_settings

_PROBLEM = "application/problem+json"
_PROBLEM_KEYS = {"type", "title", "status", "detail", "instance", "request_id"}

client = TestClient(app)


def test_domain_404_is_problem_json_with_request_id() -> None:
    response = client.get("/v1/users/999999999/segment")
    assert response.status_code == 404
    assert response.headers["content-type"] == _PROBLEM
    body = response.json()
    assert body.keys() >= _PROBLEM_KEYS
    assert body["status"] == 404
    assert body["instance"] == "/v1/users/999999999/segment"
    assert body["request_id"] == response.headers["X-Request-ID"]


def test_insufficient_history_is_problem_json_422() -> None:
    response = client.get("/v1/users/999999999/propensity")
    assert response.status_code == 422
    assert response.headers["content-type"] == _PROBLEM
    assert response.json()["request_id"] == response.headers["X-Request-ID"]


def test_validation_error_is_problem_json_422() -> None:
    response = client.get("/v1/products/1/recommendations?limit=0")
    assert response.status_code == 422
    assert response.headers["content-type"] == _PROBLEM
    body = response.json()
    assert body.keys() >= _PROBLEM_KEYS
    assert isinstance(body["errors"], list) and body["errors"]


def test_unknown_route_is_problem_json_404() -> None:
    response = client.get("/does-not-exist")
    assert response.status_code == 404
    assert response.headers["content-type"] == _PROBLEM


def _boom_app() -> FastAPI:
    built = create_app()

    @built.get("/_boom")
    async def _boom() -> dict[str, str]:
        raise RuntimeError("secret internal detail /etc/passwd stack frame")

    return built


def test_500_is_scrubbed_problem_json() -> None:
    with TestClient(_boom_app(), raise_server_exceptions=False) as boom_client:
        response = boom_client.get("/_boom")
    assert response.status_code == 500
    assert response.headers["content-type"] == _PROBLEM
    body = response.json()
    assert body.keys() >= _PROBLEM_KEYS
    assert body["status"] == 500
    assert body["request_id"]
    assert body["request_id"] == response.headers["X-Request-ID"]
    blob = response.text.lower()
    assert "secret internal detail" not in blob
    assert "/etc/passwd" not in blob
    assert "traceback" not in blob
    assert "runtimeerror" not in blob


def _ready_app(tmp_path: Path, *, db_ok: bool, model_ok: bool) -> FastAPI:
    real = get_settings()
    db = real.database_path if db_ok else tmp_path / "missing.duckdb"
    model = real.model_path if model_ok else tmp_path / "missing.txt"
    built = create_app()
    built.dependency_overrides[get_settings] = lambda: Settings(database_path=db, model_path=model)
    return built


def test_ready_200_when_artifacts_present(tmp_path: Path) -> None:
    with TestClient(_ready_app(tmp_path, db_ok=True, model_ok=True)) as ready_client:
        response = ready_client.get("/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


@pytest.mark.parametrize(
    ("db_ok", "model_ok"),
    [(False, True), (True, False)],
    ids=["database-absent", "model-absent"],
)
def test_ready_503_problem_json_when_artifact_absent(
    tmp_path: Path, db_ok: bool, model_ok: bool
) -> None:
    with TestClient(_ready_app(tmp_path, db_ok=db_ok, model_ok=model_ok)) as ready_client:
        response = ready_client.get("/ready")
    assert response.status_code == 503
    assert response.headers["content-type"] == _PROBLEM
    body = response.json()
    assert body.keys() >= _PROBLEM_KEYS
    assert body["status"] == 503


def test_cors_allow_origins_sourced_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CIP_CORS_ALLOW_ORIGINS", "https://example.test")
    with TestClient(create_app()) as cors_client:
        allowed = cors_client.options(
            "/v1/meta",
            headers={
                "Origin": "https://example.test",
                "Access-Control-Request-Method": "GET",
            },
        )
        denied = cors_client.options(
            "/v1/meta",
            headers={
                "Origin": "https://not-allowed.test",
                "Access-Control-Request-Method": "GET",
            },
        )
    assert allowed.headers.get("access-control-allow-origin") == "https://example.test"
    assert "access-control-allow-origin" not in denied.headers


def test_proxy_headers_hop_does_not_break_requests() -> None:
    response = client.get(
        "/healthz",
        headers={"X-Forwarded-For": "203.0.113.7", "X-Forwarded-Proto": "https"},
    )
    assert response.status_code == 200
