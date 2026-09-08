"""Phase 3 — the discovery endpoints (/version, /v1/meta, /v1/segments, /v1/models).

Every assertion is against a live source of truth (sample.duckdb or
src/models/metrics.json), never a literal, so these stay honest as the frozen
dataset/model are what they are.
"""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from api.main import app
from src.db import DuckDBConnectionManager

client = TestClient(app)

_METRICS = json.loads(
    (Path(__file__).resolve().parents[2] / "src" / "models" / "metrics.json").read_text()
)


def test_version_shape() -> None:
    body = client.get("/version").json()
    assert client.get("/version").status_code == 200
    assert set(body) == {"git_sha", "built_at", "model_version"}
    assert body["model_version"] == _METRICS["git_sha"]
    assert body["git_sha"]
    assert body["built_at"]


def test_meta_matches_build_all_row_counts(connections: DuckDBConnectionManager) -> None:
    body = client.get("/v1/meta").json()
    assert client.get("/v1/meta").status_code == 200
    assert body["date_range"] == ["2019-10-01", "2019-11-30"]
    assert body["git_sha"] == _METRICS["git_sha"]

    with connections.cursor() as cur:
        for table, reported in body["row_counts"].items():
            actual = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            assert reported == actual, table
        events_rows = cur.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        propensity_users = cur.execute(
            "SELECT COUNT(DISTINCT user_id) FROM events WHERE event_time < '2019-11-01'"
        ).fetchone()[0]

    assert body["dataset_rows"] == events_rows
    assert body["propensity_users"] == propensity_users
    assert set(body["row_counts"]) == {
        "dim_products",
        "dim_users",
        "fact_sessions",
        "fact_daily_kpis",
        "user_rfm_segments",
        "predictions_product_affinity",
        "weekly_retention",
    }


def test_segments_list_populations_sum_to_table_count(
    connections: DuckDBConnectionManager,
) -> None:
    body = client.get("/v1/segments").json()
    assert client.get("/v1/segments").status_code == 200

    with connections.cursor() as cur:
        distinct = cur.execute("SELECT COUNT(DISTINCT segment) FROM user_rfm_segments").fetchone()[
            0
        ]
        total = cur.execute("SELECT COUNT(*) FROM user_rfm_segments").fetchone()[0]

    assert len(body) == distinct
    assert sum(row["population"] for row in body) == total
    assert abs(sum(row["pct_of_buyers"] for row in body) - 100.0) < 1e-6


def test_segment_detail_and_unknown(connections: DuckDBConnectionManager) -> None:
    with connections.cursor() as cur:
        name = cur.execute("SELECT segment FROM user_rfm_segments LIMIT 1").fetchone()[0]

    ok = client.get(f"/v1/segments/{name}")
    assert ok.status_code == 200
    assert ok.json()["segment"] == name

    missing = client.get("/v1/segments/NoSuchSegment")
    assert missing.status_code == 404
    assert missing.headers["content-type"] == "application/problem+json"


def test_models_list_and_propensity_detail() -> None:
    listed = client.get("/v1/models").json()
    assert client.get("/v1/models").status_code == 200
    assert any(m["name"] == "propensity" for m in listed)
    assert listed[0]["auc_roc"] == _METRICS["auc_roc"]

    detail = client.get("/v1/models/propensity")
    assert detail.status_code == 200
    payload = detail.json()
    assert payload["auc_roc"] == _METRICS["auc_roc"]
    assert payload["feature_importance_gain"] == _METRICS["feature_importance_gain"]
    assert payload["params"] == _METRICS["params"]


def test_propensity_response_carries_model_version_header(
    connections: DuckDBConnectionManager,
) -> None:
    response = client.get("/v1/users/512807853/propensity")
    assert response.status_code == 200
    assert response.headers["X-Model-Version"] == _METRICS["git_sha"]
