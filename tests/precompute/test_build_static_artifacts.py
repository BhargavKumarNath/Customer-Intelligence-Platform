"""Parity + determinism coverage for scripts/build_static_artifacts.py.

Every test here maps to a Phase 2 test-case bullet or a P2-NEW item in
deployment_stages.md. The pipeline's whole job is to be a faithful, reproducible
mirror of the service layer, so the tests compare its output to the *same*
services the API exposes, run against the *same* star schema the pipeline built.
"""

from __future__ import annotations

import gzip
import json
import math
import random
from collections.abc import Iterator
from pathlib import Path

import duckdb
import pytest

from scripts.build_static_artifacts import (
    AB_GRID_CONFIDENCE_LEVELS,
    AB_GRID_LIFTS,
    PROPENSITY_CUTOFF,
    BuildConfig,
    BuildResult,
    main,
    run_build,
)
from src.config import Settings
from src.db import DuckDBConnectionManager
from src.domain.models import ABTestRequest
from src.services.experiments import ABTestService
from src.services.propensity import PropensityService, load_model
from src.services.recommendations import RecommendationService
from src.services.segmentation import SegmentationService
from tests.precompute._schemas import ADAPTERS, schema_for, schema_path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SAMPLE_PARQUET = PROJECT_ROOT / "data" / "sample" / "sample_optimized.parquet"
MODEL_PATH = PROJECT_ROOT / "src" / "models" / "propensity_lgbm.pkl"
METRICS_SRC = PROJECT_ROOT / "src" / "models" / "metrics.json"

FIXED_SHA = "phase2testsha"
FIXED_BUILT_AT = "2020-01-01T00:00:00+00:00"

EXPECTED_FILES = {
    "segments.json",
    "propensity.json",
    "affinity.json",
    "kpis.json",
    "retention.json",
    "rfm_summary.json",
    "ab_grid.json",
    "metrics.json",
    "meta.json",
    "events_trimmed.parquet",
}

FLOAT_TOL = 1e-9


@pytest.fixture(scope="session")
def artifacts(tmp_path_factory: pytest.TempPathFactory) -> BuildResult:
    """Build the full artifact set once; keep the derived DB for parity checks."""
    work = tmp_path_factory.mktemp("precompute")
    config = BuildConfig(
        parquet=SAMPLE_PARQUET,
        out_root=work / "dist",
        git_sha=FIXED_SHA,
        built_at=FIXED_BUILT_AT,
        model_path=MODEL_PATH,
        work_db=work / "sample.duckdb",
    )
    return run_build(config)


@pytest.fixture(scope="session")
def ref_manager(artifacts: BuildResult) -> Iterator[DuckDBConnectionManager]:
    manager = DuckDBConnectionManager(
        Settings(database_path=artifacts.work_db, model_path=MODEL_PATH)
    )
    yield manager
    manager.close()


def _load(artifacts: BuildResult, name: str) -> object:
    return json.loads((artifacts.out_dir / name).read_text())


# --------------------------------------------------------------------------- #
# Presence / shape
# --------------------------------------------------------------------------- #
def test_all_expected_files_present(artifacts: BuildResult) -> None:
    produced = {p.name for p in artifacts.out_dir.iterdir()}
    assert produced >= EXPECTED_FILES
    assert artifacts.out_dir.name == FIXED_SHA


def test_metrics_json_is_a_verbatim_copy(artifacts: BuildResult) -> None:
    assert (artifacts.out_dir / "metrics.json").read_bytes() == METRICS_SRC.read_bytes()


# --------------------------------------------------------------------------- #
# Row-count parity — asserted against build_all()'s return dict (P2-NEW-4)
# --------------------------------------------------------------------------- #
def test_row_count_parity(artifacts: BuildResult) -> None:
    counts = artifacts.counts
    segments = _load(artifacts, "segments.json")
    kpis = _load(artifacts, "kpis.json")
    retention = _load(artifacts, "retention.json")
    affinity = _load(artifacts, "affinity.json")
    rfm_summary = _load(artifacts, "rfm_summary.json")

    assert isinstance(segments, dict)
    assert len(segments) == counts["user_rfm_segments"]
    assert isinstance(kpis, list)
    assert len(kpis) == counts["fact_daily_kpis"]
    assert isinstance(retention, list)
    assert len(retention) == counts["weekly_retention"]
    assert isinstance(affinity, dict)
    assert len(affinity["pairs"]) == counts["predictions_product_affinity"]
    assert isinstance(rfm_summary, list)
    assert sum(row["population"] for row in rfm_summary) == counts["user_rfm_segments"]


def test_fact_sessions_count_matches_builder_not_distinct_sessions(
    artifacts: BuildResult, ref_manager: DuckDBConnectionManager
) -> None:
    """F5 / P2-NEW-4: fact_sessions groups by (user_session, user_id), so its row
    count is one higher than COUNT(DISTINCT user_session) — one session UUID in
    the sample carries two user_ids. Parity must key on build_all()'s dict."""
    with ref_manager.cursor() as cur:
        distinct_sessions = cur.execute(
            "SELECT COUNT(DISTINCT user_session) FROM events"
        ).fetchone()[0]
    assert artifacts.counts["fact_sessions"] == distinct_sessions + 1


# --------------------------------------------------------------------------- #
# Propensity parity + coverage
# --------------------------------------------------------------------------- #
def test_propensity_parity_against_service(
    artifacts: BuildResult, ref_manager: DuckDBConnectionManager
) -> None:
    propensity = _load(artifacts, "propensity.json")
    assert isinstance(propensity, dict)
    service = PropensityService(ref_manager, load_model(MODEL_PATH))

    sample = random.Random(42).sample(sorted(propensity), 200)
    for user_id in sample:
        expected = service.score_user(int(user_id)).purchase_probability
        assert abs(propensity[user_id] - expected) < FLOAT_TOL


def test_propensity_coverage_and_range(
    artifacts: BuildResult, ref_manager: DuckDBConnectionManager
) -> None:
    propensity = _load(artifacts, "propensity.json")
    assert isinstance(propensity, dict)
    with ref_manager.cursor() as cur:
        rows = cur.execute(
            f"SELECT DISTINCT user_id FROM events WHERE event_time < '{PROPENSITY_CUTOFF}'"
        ).fetchall()
    expected_ids = {str(r[0]) for r in rows}

    assert set(propensity) == expected_ids
    assert all(0.0 <= score <= 1.0 for score in propensity.values())


# --------------------------------------------------------------------------- #
# Segment parity
# --------------------------------------------------------------------------- #
def test_segment_parity_against_service(
    artifacts: BuildResult, ref_manager: DuckDBConnectionManager
) -> None:
    segments = _load(artifacts, "segments.json")
    assert isinstance(segments, dict)
    service = SegmentationService(ref_manager)

    sample = random.Random(7).sample(sorted(segments), 200)
    for user_id in sample:
        assert segments[user_id] == service.get_segment(int(user_id)).model_dump()


# --------------------------------------------------------------------------- #
# Recommendations parity — both-direction lookup (P2 test-case bullet)
# --------------------------------------------------------------------------- #
def test_recommendations_parity_against_service(
    artifacts: BuildResult, ref_manager: DuckDBConnectionManager
) -> None:
    affinity = _load(artifacts, "affinity.json")
    assert isinstance(affinity, dict)
    service = RecommendationService(ref_manager)

    product_ids: set[int] = set()
    for pair in affinity["pairs"]:
        product_ids.add(pair["product_a"])
        product_ids.add(pair["product_b"])

    for product_id in product_ids:
        expected = [rec.model_dump() for rec in service.get_recommendations(product_id, 50)]
        assert affinity["by_product"][str(product_id)] == expected


# --------------------------------------------------------------------------- #
# A/B grid parity + the NaN-power contract (P2-NEW-3)
# --------------------------------------------------------------------------- #
def test_ab_grid_parity_against_service(
    artifacts: BuildResult, ref_manager: DuckDBConnectionManager
) -> None:
    cells = _load(artifacts, "ab_grid.json")
    assert isinstance(cells, list)
    service = ABTestService(ref_manager)

    seen: set[tuple[str, float, float]] = set()
    for cell in cells:
        key = (cell["segment"], cell["lift"], cell["confidence_level"])
        seen.add(key)
        result = service.run_simulation(
            ABTestRequest(
                segment=cell["segment"],
                lift=cell["lift"],
                confidence_level=cell["confidence_level"],
            )
        )
        assert cell["control_visitors"] == result.control_visitors
        assert cell["treatment_visitors"] == result.treatment_visitors
        assert cell["is_significant"] == result.is_significant
        for field in (
            "control_conversion_rate",
            "treatment_conversion_rate",
            "relative_lift",
            "p_value",
            "ci_95_lower",
            "ci_95_upper",
        ):
            assert abs(cell[field] - getattr(result, field)) < FLOAT_TOL

        stored_power = cell["statistical_power"]
        recomputed = result.statistical_power
        if stored_power is None:
            assert cell["power_undefined"] is True
            assert recomputed is None or math.isnan(recomputed) or not (0.0 <= recomputed <= 1.0)
        else:
            assert abs(stored_power - recomputed) < FLOAT_TOL

    expected_keys = {
        (segment, lift, confidence)
        for segment in _segments_of(artifacts)
        for lift in AB_GRID_LIFTS
        for confidence in AB_GRID_CONFIDENCE_LEVELS
    }
    assert seen == expected_keys


def test_ab_grid_power_is_finite_fraction_or_null(artifacts: BuildResult) -> None:
    cells = _load(artifacts, "ab_grid.json")
    assert isinstance(cells, list)
    for cell in cells:
        power = cell["statistical_power"]
        if power is None:
            assert cell["power_undefined"] is True
        else:
            assert cell["power_undefined"] is False
            assert isinstance(power, float)
            assert 0.0 <= power <= 1.0
            assert not math.isnan(power)


def _segments_of(artifacts: BuildResult) -> list[str]:
    meta = _load(artifacts, "meta.json")
    assert isinstance(meta, dict)
    return list(meta["segments"])


# --------------------------------------------------------------------------- #
# meta.json
# --------------------------------------------------------------------------- #
def test_meta_correctness(artifacts: BuildResult) -> None:
    meta = _load(artifacts, "meta.json")
    assert isinstance(meta, dict)
    assert meta["git_sha"] == FIXED_SHA
    assert meta["built_at"] == FIXED_BUILT_AT
    assert meta["date_range"] == ["2019-10-01", "2019-11-30"]
    assert meta["row_counts"] == artifacts.counts
    assert meta["propensity_users"] == len(_load(artifacts, "propensity.json"))  # type: ignore[arg-type]
    assert meta["dataset_rows"] == 1_645_912


# --------------------------------------------------------------------------- #
# events_trimmed.parquet — P2-NEW-1 variant (a): all rows, id/ts re-encoded
# --------------------------------------------------------------------------- #
def test_events_trimmed_size_and_schema(artifacts: BuildResult) -> None:
    path = artifacts.out_dir / "events_trimmed.parquet"
    size_mb = path.stat().st_size / 1e6
    assert size_mb <= 15.0, f"events_trimmed.parquet is {size_mb:.2f} MB (budget 15 MB)"

    con = duckdb.connect(":memory:")
    try:
        cols = {
            row[0]
            for row in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{path.as_posix()}')"
            ).fetchall()
        }
        assert cols == {"ts", "event_type", "product_id", "price", "user_id", "session_id"}
        trimmed_rows = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{path.as_posix()}')"
        ).fetchone()[0]
    finally:
        con.close()

    meta = _load(artifacts, "meta.json")
    assert isinstance(meta, dict)
    assert trimmed_rows == meta["dataset_rows"]


def test_events_trimmed_encoding_is_lossless(artifacts: BuildResult) -> None:
    """The two re-encodings (user_session -> dense session_id, event_time -> int
    epoch seconds) must not lose information the dashboards depend on."""
    path = (artifacts.out_dir / "events_trimmed.parquet").as_posix()
    con = duckdb.connect(":memory:")
    try:
        # to_timestamp() renders in the session TZ; pin it so the reconstructed
        # wall clock lines up with the source's naive TIMESTAMP.
        con.execute("SET TimeZone='UTC'")
        con.execute(f"CREATE TABLE src AS SELECT * FROM read_parquet('{SAMPLE_PARQUET}')")
        con.execute(f"CREATE TABLE trim AS SELECT * FROM read_parquet('{path}')")

        src_sessions, src_users, src_min, src_max = con.execute(
            "SELECT COUNT(DISTINCT user_session), COUNT(DISTINCT user_id), "
            "MIN(event_time), MAX(event_time) FROM src"
        ).fetchone()
        trim_sessions, trim_users, trim_min, trim_max = con.execute(
            "SELECT COUNT(DISTINCT session_id), COUNT(DISTINCT user_id), "
            "to_timestamp(MIN(ts)), to_timestamp(MAX(ts)) FROM trim"
        ).fetchone()

        assert trim_sessions == src_sessions
        assert trim_users == src_users
        # to_timestamp yields a tz-aware UTC value; compare on the wall clock.
        assert trim_min.replace(tzinfo=None) == src_min
        assert trim_max.replace(tzinfo=None) == src_max

        src_dist = con.execute(
            "SELECT event_type, COUNT(*) FROM src GROUP BY 1 ORDER BY 1"
        ).fetchall()
        trim_dist = con.execute(
            "SELECT event_type, COUNT(*) FROM trim GROUP BY 1 ORDER BY 1"
        ).fetchall()
        assert src_dist == trim_dist

        # A representative dashboard aggregate (daily revenue) must be identical.
        src_rev = con.execute(
            "SELECT CAST(event_time AS DATE) d, "
            "SUM(CASE WHEN event_type='purchase' THEN price ELSE 0 END) r "
            "FROM src GROUP BY 1 ORDER BY 1"
        ).fetchall()
        trim_rev = con.execute(
            "SELECT CAST(to_timestamp(ts) AS DATE) d, "
            "SUM(CASE WHEN event_type='purchase' THEN price ELSE 0 END) r "
            "FROM trim GROUP BY 1 ORDER BY 1"
        ).fetchall()
        assert src_rev == trim_rev
    finally:
        con.close()


# --------------------------------------------------------------------------- #
# Determinism (Phase 2 test-case bullet)
# --------------------------------------------------------------------------- #
def test_output_is_byte_deterministic(artifacts: BuildResult, tmp_path: Path) -> None:
    rerun = run_build(
        BuildConfig(
            parquet=SAMPLE_PARQUET,
            out_root=tmp_path / "dist",
            git_sha=FIXED_SHA,
            built_at=FIXED_BUILT_AT,
            model_path=MODEL_PATH,
            work_db=tmp_path / "sample.duckdb",
        )
    )
    for name in EXPECTED_FILES:
        first = (artifacts.out_dir / name).read_bytes()
        second = (rerun.out_dir / name).read_bytes()
        assert first == second, f"{name} is not reproducible byte-for-byte"


# --------------------------------------------------------------------------- #
# Payload budget — gzip -9 is a safe upper bound for brotli -q11 on JSON text,
# so passing this guarantees the plan's brotli budgets (4 MB total / 2 MB
# propensity) are met without adding a brotli dependency.
# --------------------------------------------------------------------------- #
def test_payload_budget(artifacts: BuildResult) -> None:
    sizes = {
        p.name: len(gzip.compress(p.read_bytes(), 9)) for p in artifacts.out_dir.glob("*.json")
    }
    assert sum(sizes.values()) <= 4_000_000, sizes
    assert sizes["propensity.json"] <= 2_000_000


# --------------------------------------------------------------------------- #
# JSON Schema — checked-in under schemas/, kept honest here
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("stem", sorted(ADAPTERS))
def test_artifact_validates_against_checked_in_schema(artifacts: BuildResult, stem: str) -> None:
    payload = _load(artifacts, f"{stem}.json")
    # The adapter *is* the schema; validating through it is the real check.
    ADAPTERS[stem].validate_python(payload)

    on_disk = json.loads(schema_path(stem).read_text())
    assert on_disk == schema_for(stem), (
        f"schemas/{stem}.schema.json is stale — run `python tests/precompute/_schemas.py`"
    )


# --------------------------------------------------------------------------- #
# CLI entrypoint
# --------------------------------------------------------------------------- #
def test_main_entrypoint_writes_artifacts(tmp_path: Path) -> None:
    out_root = tmp_path / "dist"
    code = main(
        [
            "--out",
            str(out_root),
            "--parquet",
            str(SAMPLE_PARQUET),
            "--git-sha",
            "clitest",
            "--built-at",
            FIXED_BUILT_AT,
        ]
    )
    assert code == 0
    produced = {p.name for p in (out_root / "clitest").iterdir()}
    assert produced >= EXPECTED_FILES
