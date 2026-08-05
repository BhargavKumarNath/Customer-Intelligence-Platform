from __future__ import annotations

from fastapi.testclient import TestClient

from api.main import app
from src.db import DuckDBConnectionManager

client = TestClient(app)

_UNKNOWN_ID = 999_999_999


def test_root_redirects_to_docs() -> None:
    response = client.get("/", follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/docs"


def test_healthz() -> None:
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready() -> None:
    response = client.get("/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


def test_segment_lookup_happy_path(connections: DuckDBConnectionManager) -> None:
    with connections.cursor() as cur:
        user_id = cur.execute("SELECT user_id FROM user_rfm_segments LIMIT 1").fetchone()[0]

    response = client.get(f"/v1/users/{user_id}/segment")

    assert response.status_code == 200
    assert response.json()["user_id"] == user_id


def test_segment_lookup_not_found() -> None:
    response = client.get(f"/v1/users/{_UNKNOWN_ID}/segment")
    assert response.status_code == 404


def test_propensity_lookup_happy_path(connections: DuckDBConnectionManager) -> None:
    with connections.cursor() as cur:
        user_id = cur.execute(
            "SELECT user_id FROM events WHERE event_time < '2019-11-01' GROUP BY user_id LIMIT 1"
        ).fetchone()[0]

    response = client.get(f"/v1/users/{user_id}/propensity")

    assert response.status_code == 200
    body = response.json()
    assert 0.0 <= body["purchase_probability"] <= 1.0


def test_propensity_lookup_no_history_returns_422() -> None:
    response = client.get(f"/v1/users/{_UNKNOWN_ID}/propensity")
    assert response.status_code == 422


def test_recommendations_happy_path(connections: DuckDBConnectionManager) -> None:
    with connections.cursor() as cur:
        product_id = cur.execute(
            "SELECT product_a FROM predictions_product_affinity LIMIT 1"
        ).fetchone()[0]

    response = client.get(f"/v1/products/{product_id}/recommendations?limit=3")

    assert response.status_code == 200
    assert len(response.json()) <= 3


def test_recommendations_rejects_invalid_limit() -> None:
    response = client.get("/v1/products/1/recommendations?limit=0")
    assert response.status_code == 422


def test_ab_test_endpoint_happy_path(connections: DuckDBConnectionManager) -> None:
    with connections.cursor() as cur:
        segment = cur.execute(
            "SELECT segment FROM user_rfm_segments GROUP BY segment ORDER BY COUNT(*) DESC LIMIT 1"
        ).fetchone()[0]

    response = client.post("/v1/experiments/ab-test", json={"segment": segment, "lift": 0.15})

    assert response.status_code == 200
    assert response.json()["segment"] == segment


def test_ab_test_endpoint_unknown_segment() -> None:
    response = client.post("/v1/experiments/ab-test", json={"segment": "DoesNotExist"})
    assert response.status_code == 404


def test_ab_test_endpoint_rejects_invalid_lift() -> None:
    response = client.post("/v1/experiments/ab-test", json={"segment": "Champions", "lift": 999})
    assert response.status_code == 422


def test_openapi_schema_exposes_all_routes() -> None:
    response = client.get("/openapi.json")
    assert response.status_code == 200
    paths = response.json()["paths"]
    assert {
        "/healthz",
        "/ready",
        "/v1/users/{user_id}/segment",
        "/v1/users/{user_id}/propensity",
        "/v1/products/{product_id}/recommendations",
        "/v1/experiments/ab-test",
    } <= paths.keys()
