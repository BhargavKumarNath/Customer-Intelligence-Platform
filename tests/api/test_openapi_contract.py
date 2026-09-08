"""Phase 3 — OpenAPI contract sanity, without pulling in schemathesis.

FastAPI generates the document; this asserts it is a well-formed OpenAPI 3.1
object, every path has at least one valid operation with responses, and every
local ``$ref`` resolves against ``components``. The full fuzz
(``schemathesis run --checks all``) is deferred to when the API is actually
served (Phase 7) — there is no running server to fuzz while it stays undeployed.
"""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)

_HTTP_METHODS = {"get", "put", "post", "delete", "options", "head", "patch", "trace"}


def _spec() -> dict[str, Any]:
    response = client.get("/openapi.json")
    assert response.status_code == 200
    return response.json()  # type: ignore[no-any-return]


def _iter_refs(node: Any) -> Any:
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "$ref" and isinstance(value, str):
                yield value
            else:
                yield from _iter_refs(value)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_refs(item)


def test_document_is_openapi_31() -> None:
    spec = _spec()
    assert spec["openapi"].startswith("3.1")
    assert spec["info"]["title"]
    assert spec["info"]["version"]
    assert isinstance(spec["paths"], dict) and spec["paths"]


def test_every_operation_declares_responses() -> None:
    for path, item in _spec()["paths"].items():
        operations = {m: op for m, op in item.items() if m in _HTTP_METHODS}
        assert operations, f"{path} has no operations"
        for method, op in operations.items():
            assert op.get("responses"), f"{method.upper()} {path} declares no responses"


def test_all_local_refs_resolve() -> None:
    spec = _spec()
    for ref in _iter_refs(spec):
        assert ref.startswith("#/"), f"unexpected external ref: {ref}"
        node: Any = spec
        for part in ref.lstrip("#/").split("/"):
            assert part in node, f"dangling $ref {ref} (missing {part!r})"
            node = node[part]


def test_new_phase3_paths_are_documented() -> None:
    paths = _spec()["paths"]
    for path in (
        "/version",
        "/v1/meta",
        "/v1/segments",
        "/v1/segments/{name}",
        "/v1/models",
        "/v1/models/propensity",
    ):
        assert path in paths, path
        assert "get" in paths[path]
