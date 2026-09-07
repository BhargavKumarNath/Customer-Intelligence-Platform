"""JSON Schema registry for the Phase 2 static artifacts.

One `TypeAdapter` per emitted JSON file. The generated schema is checked in under
`schemas/`; `test_build_static_artifacts.py` both validates each artifact against
its adapter and asserts the checked-in file has not drifted from it.

Regenerate the checked-in files after an intentional model change::

    python tests/precompute/_schemas.py
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

from pydantic import Field, TypeAdapter

from src.domain.artifacts import (
    ABGridCell,
    AffinityFile,
    ArtifactMeta,
    DailyKpi,
    SegmentSummary,
    WeeklyRetentionRow,
)
from src.domain.models import UserSegment

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SCHEMA_DIR = PROJECT_ROOT / "schemas"

_Probability = Annotated[float, Field(ge=0.0, le=1.0)]

# artifact stem (file is "<stem>.json") -> validator for its whole payload
ADAPTERS: dict[str, TypeAdapter] = {
    "segments": TypeAdapter(dict[str, UserSegment]),
    "propensity": TypeAdapter(dict[str, _Probability]),
    "affinity": TypeAdapter(AffinityFile),
    "kpis": TypeAdapter(list[DailyKpi]),
    "retention": TypeAdapter(list[WeeklyRetentionRow]),
    "rfm_summary": TypeAdapter(list[SegmentSummary]),
    "ab_grid": TypeAdapter(list[ABGridCell]),
    "meta": TypeAdapter(ArtifactMeta),
}


def schema_for(stem: str) -> dict:
    return ADAPTERS[stem].json_schema()


def schema_path(stem: str) -> Path:
    return SCHEMA_DIR / f"{stem}.schema.json"


def write_all() -> list[Path]:
    SCHEMA_DIR.mkdir(exist_ok=True)
    written: list[Path] = []
    for stem in ADAPTERS:
        path = schema_path(stem)
        path.write_text(json.dumps(schema_for(stem), indent=2, sort_keys=True) + "\n")
        written.append(path)
    return written


if __name__ == "__main__":
    for path in write_all():
        print(f"wrote {path.relative_to(PROJECT_ROOT)}")
