"""Enforces the Stack E load-bearing invariant: the migration-critical core
(``src/`` service + processing layer and ``api/``) must not depend on Streamlit
or Plotly. Only ``app/`` may import them.

Why this matters: Phase 2's precompute job and the FastAPI image both install
``.[api,dev]`` only - neither ``streamlit`` nor ``plotly`` is present. A lazy
``import streamlit`` inside a ``src/`` function would still pass every other CI
job (they never install it) and would only blow up later, at precompute time.
This test makes that regression fail fast and locally instead.

See deployment_stages.md Phase 1 -> P1-NEW-3 (Finding F3).
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# The service/transport layer that the precompute pipeline and the API both
# import. Deliberately excludes the offline Hydra pipeline (src/ingestion,
# src/models, src/processing/{features,sessionization,initial_modeling},
# src/analysis/{segmentation,retention}) which is dev-only and never on the
# migration path.
CORE_MODULES = [
    "src.config",
    "src.db",
    "src.domain.models",
    "src.services.errors",
    "src.services.propensity",
    "src.services.segmentation",
    "src.services.recommendations",
    "src.services.experiments",
    "src.analysis.ab_testing",
    "src.processing.dimensional_model",
    "api.main",
    "api.deps",
    "api.middleware",
    "api.exception_handlers",
    "api.logging_config",
    "api.routers.health",
    "api.routers.propensity",
    "api.routers.segments",
    "api.routers.recommendations",
    "api.routers.experiments",
]

FORBIDDEN_ROOTS = {"streamlit", "plotly"}
_IMPORT_RE = re.compile(r"^\s*(?:import|from)\s+(streamlit|plotly)(?:[.\s]|$)")


def test_core_import_does_not_pull_in_streamlit_or_plotly() -> None:
    """Import every core module in a fresh interpreter; assert neither
    forbidden package landed in ``sys.modules``."""
    code = (
        "import importlib, sys\n"
        f"for m in {CORE_MODULES!r}:\n"
        "    importlib.import_module(m)\n"
        "bad = sorted({k.split('.')[0] for k in sys.modules} & "
        f"{FORBIDDEN_ROOTS!r})\n"
        "print(','.join(bad))\n"
        "sys.exit(1 if bad else 0)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        "Importing the migration-critical core pulled in a forbidden package "
        f"({result.stdout.strip()!r}).\nstderr:\n{result.stderr.strip()}"
    )


def test_no_streamlit_or_plotly_import_statements_in_core_tree() -> None:
    """Static guard: no ``import streamlit`` / ``import plotly`` anywhere under
    ``src/`` or ``api/`` (visualisation code belongs in ``app/``)."""
    offenders: list[str] = []
    for base in ("src", "api"):
        for path in sorted((PROJECT_ROOT / base).rglob("*.py")):
            rel = path.relative_to(PROJECT_ROOT).as_posix()
            for lineno, line in enumerate(path.read_text().splitlines(), start=1):
                if _IMPORT_RE.match(line):
                    offenders.append(f"{rel}:{lineno}: {line.strip()}")
    assert not offenders, "Streamlit/Plotly imports found in the core tree:\n" + "\n".join(
        offenders
    )
