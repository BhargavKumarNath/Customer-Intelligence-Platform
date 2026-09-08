"""Deprecation gate for the API test package (deployment_stages.md P3-NEW-2).

Every test collected under ``tests/api/`` is run with
``-W error::DeprecationWarning`` so a deprecation introduced in ``api/`` or the
service layer it exercises fails the build instead of rotting silently.

Two upstream-only warnings are allow-listed because they originate in
``starlette``/``anyio`` internals, not our code, and clearing them needs a
``httpx2`` / ``TestClient`` dependency bump that is out of Phase 3 scope:

* ``anyio.abc.BlockingPortal`` alias deprecation (raised from
  ``starlette.testclient`` at import).
* ``StarletteDeprecationWarning`` for using ``httpx`` with ``TestClient`` -- not a
  ``DeprecationWarning`` subclass, so it never trips the gate, but pinned here as
  a reminder that the migration is pending.
"""

from __future__ import annotations

import pathlib

import pytest

_HERE = str(pathlib.Path(__file__).parent).replace("\\", "/")

_UPSTREAM_ALLOWLIST = (
    "ignore:The anyio.abc.BlockingPortal alias is deprecated:DeprecationWarning",
    "ignore:Using `httpx` with `starlette.testclient` is deprecated",
)


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    for item in items:
        if not str(item.fspath).replace("\\", "/").startswith(_HERE):
            continue
        item.add_marker(pytest.mark.filterwarnings("error::DeprecationWarning"))
        for spec in _UPSTREAM_ALLOWLIST:
            item.add_marker(pytest.mark.filterwarnings(spec))
