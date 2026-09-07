"""Phase 2 — build the static precompute artifacts the frontend consumes.

Turns the frozen sample dataset + frozen propensity model into a versioned,
byte-deterministic artifact set under ``<out>/<git_sha>/``:

    segments.json          {user_id: UserSegment}   — all user_rfm_segments rows
    propensity.json        {user_id: purchase_probability} for every user with
                           event_time < 2019-11-01 (75,915 on the sample dataset)
    affinity.json          predictions_product_affinity: raw pairs + a
                           both-direction ``by_product`` lookup whose values match
                           RecommendationService.get_recommendations(pid, 50)
    kpis.json              fact_daily_kpis time series
    retention.json         weekly_retention cohort grid
    rfm_summary.json       per-segment population + aggregates
    ab_grid.json           ABTestService.run_simulation over a
                           segment x lift x confidence grid (NaN power -> null,
                           power_undefined = true; see deployment_stages.md P2-NEW-3)
    metrics.json           verbatim copy of src/models/metrics.json
    meta.json              date range, row counts, segment list, git_sha, built_at
    events_trimmed.parquet column-trimmed events for DuckDB-WASM — P2-NEW-1
                           variant (a): every row kept, user_session UUID collapsed
                           to a dense int32 ``session_id``, event_time to an int32
                           ``ts`` (epoch seconds). ~14.7 MB vs 32.6 MB faithful.

Determinism: for a fixed ``--git-sha`` / ``--built-at`` every JSON file is
byte-stable across runs (sorted keys, compact separators, no embedded wall-clock;
the only timestamp in the set is meta.json ``built_at``).

Parity is guaranteed structurally: every per-row payload is built by constructing
the same pydantic model the API returns (UserSegment, ProductRecommendation,
ABTestResult), and the A/B grid is produced by calling ABTestService directly.

Usage::

    python scripts/build_static_artifacts.py \
        [--out dist/data] [--parquet data/sample/sample_optimized.parquet] \
        [--git-sha SHA] [--built-at ISO8601] [--model PATH] [--keep-work-db]
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess  # nosec B404 - only ever invokes `git` with a fixed arg list, no shell
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb

from src.config import Settings
from src.db import DuckDBConnectionManager
from src.domain.artifacts import (
    ABGridCell,
    AffinityFile,
    AffinityPair,
    ArtifactMeta,
    DailyKpi,
    SegmentSummary,
    WeeklyRetentionRow,
)
from src.domain.models import ABTestRequest, ProductRecommendation, UserSegment
from src.processing.dimensional_model import build_all
from src.services.experiments import ABTestService
from src.services.propensity import _FEATURE_ORDER, load_model

if TYPE_CHECKING:
    from lightgbm import Booster

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PARQUET = PROJECT_ROOT / "data" / "sample" / "sample_optimized.parquet"
DEFAULT_OUT_ROOT = PROJECT_ROOT / "dist" / "data"
DEFAULT_MODEL = PROJECT_ROOT / "src" / "models" / "propensity_lgbm.pkl"
METRICS_SRC = PROJECT_ROOT / "src" / "models" / "metrics.json"

# Match scripts/create_cloud_database.py so the star schema is byte-identical to
# the one CI/Docker bake into sample.duckdb.
MEMORY_LIMIT = "512MB"
THREADS = 2

# The propensity model is trained only on pre-November behaviour; every user in
# the sample has October rows, so this is also the full propensity.json key set.
PROPENSITY_CUTOFF = "2019-11-01"

# A/B grid axes. Lifts run up to the ABTestRequest ceiling (le=5.0) on purpose:
# the top of the range drives statsmodels' power solver into its NaN regime,
# which is exactly what P2-NEW-3 requires the pipeline to handle.
AB_GRID_LIFTS: tuple[float, ...] = (0.05, 0.1, 0.15, 0.2, 0.3, 0.5, 1.0, 2.0, 5.0)
AB_GRID_CONFIDENCE_LEVELS: tuple[float, ...] = (0.8, 0.9, 0.95, 0.99)

_SEGMENT_COLUMNS = (
    "user_id",
    "recency_days",
    "frequency",
    "monetary",
    "r_score",
    "f_score",
    "m_score",
    "rfm_total",
    "segment",
)

# Column expressions here must stay aligned with src.services.propensity's
# _FEATURE_QUERY / _FEATURE_ORDER — the bulk form is just the per-user query with
# the `AND user_id = ?` filter dropped and `user_id` added to the projection. The
# propensity parity test (200 sampled users, 1e-9) is the guard that they agree.
_BULK_FEATURE_QUERY = f"""
    WITH oct_behavior AS (
        SELECT
            user_id,
            COUNT(*) AS oct_events,
            COUNT(DISTINCT user_session) AS oct_sessions,
            SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS oct_views,
            SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS oct_carts,
            SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) AS oct_removes,
            MAX(event_time) AS last_oct_event,
            date_diff('day', MIN(event_time), MAX(event_time)) AS active_span_days
        FROM events
        WHERE event_time < '{PROPENSITY_CUTOFF}'
        GROUP BY user_id
    )
    SELECT
        user_id,
        oct_events,
        oct_sessions,
        oct_views,
        oct_carts,
        oct_removes,
        active_span_days,
        date_diff('day', last_oct_event, DATE '{PROPENSITY_CUTOFF}') AS recency_oct
    FROM oct_behavior
    ORDER BY user_id
"""

# Faithful column trim + two lossless re-encodings (see module docstring / P2-NEW-1).
# session_id is a dense rank over the (unique) session UUIDs, so it is a stable
# bijection; ts is whole seconds (the data has no sub-second component).
_EVENTS_TRIMMED_SELECT = """
    SELECT
        CAST(EXTRACT(EPOCH FROM event_time) AS INTEGER) AS ts,
        event_type,
        product_id,
        price,
        user_id,
        CAST(DENSE_RANK() OVER (ORDER BY user_session) AS INTEGER) AS session_id
    FROM events
"""


@dataclass(frozen=True)
class BuildConfig:
    parquet: Path = DEFAULT_PARQUET
    out_root: Path = DEFAULT_OUT_ROOT
    git_sha: str = "unknown"
    built_at: str = ""
    model_path: Path = DEFAULT_MODEL
    # When set, build the star schema here and never delete it (tests point their
    # parity services at this exact DB). When None, a temp DB is used and removed.
    work_db: Path | None = None
    keep_work_db: bool = False


@dataclass(frozen=True)
class BuildResult:
    out_dir: Path
    counts: dict[str, int]
    work_db: Path
    files: list[Path] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# JSON writing — one canonical form so "run it twice, diff the bytes" holds.
# --------------------------------------------------------------------------- #
def _write_json(path: Path, obj: Any, *, pretty: bool = False) -> None:
    if pretty:
        text = json.dumps(obj, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False)
    else:
        text = json.dumps(
            obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
    path.write_text(text + "\n", encoding="utf-8")


# --------------------------------------------------------------------------- #
# Star-schema build
# --------------------------------------------------------------------------- #
def _build_database(parquet: Path, db_path: Path) -> dict[str, int]:
    if not parquet.exists():
        raise FileNotFoundError(f"source parquet not found: {parquet}")
    con = duckdb.connect(str(db_path))
    try:
        con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
        con.execute(f"SET threads TO {THREADS}")
        con.execute(
            f"CREATE TABLE events AS SELECT * FROM read_parquet('{parquet}') ORDER BY event_time"
        )
        return build_all(con)
    finally:
        con.close()


# --------------------------------------------------------------------------- #
# Per-artifact builders. Each returns a plain JSON-able structure.
# --------------------------------------------------------------------------- #
def _build_segments(con: duckdb.DuckDBPyConnection) -> dict[str, dict[str, Any]]:
    cols = list(_SEGMENT_COLUMNS)
    rows = con.execute(
        f"SELECT {', '.join(cols)} FROM user_rfm_segments ORDER BY user_id"
    ).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        seg = UserSegment(**dict(zip(cols, row, strict=True)))
        out[str(seg.user_id)] = seg.model_dump()
    return out


def _build_propensity(con: duckdb.DuckDBPyConnection, model: Booster) -> dict[str, float]:
    frame = con.execute(_BULK_FEATURE_QUERY).fetchdf()
    # num_threads=1 mirrors PropensityService.score_user exactly; LightGBM scores
    # rows independently, so the bulk pass equals per-user scoring bit-for-bit.
    preds = model.predict(frame[list(_FEATURE_ORDER)], num_threads=1)
    user_ids = frame["user_id"].tolist()
    return {str(int(uid)): float(p) for uid, p in zip(user_ids, preds, strict=True)}


def _build_affinity(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    cols = ["product_a", "product_b", "pair_count", "confidence", "lift"]
    rows = con.execute(
        f"SELECT {', '.join(cols)} FROM predictions_product_affinity "
        "ORDER BY lift DESC, product_a, product_b"
    ).fetchall()
    pairs = [AffinityPair(**dict(zip(cols, row, strict=True))) for row in rows]

    product_ids = sorted({p.product_a for p in pairs} | {p.product_b for p in pairs})
    by_product: dict[str, list[ProductRecommendation]] = {}
    for pid in product_ids:
        scored: list[ProductRecommendation] = []
        for p in pairs:
            if p.product_a == pid:
                other = p.product_b
            elif p.product_b == pid:
                other = p.product_a
            else:
                continue
            scored.append(
                ProductRecommendation(
                    product_id=other,
                    pair_count=p.pair_count,
                    confidence=p.confidence,
                    lift=p.lift,
                )
            )
        # RecommendationService orders by `lift DESC`; product_id is a deterministic
        # tie-break (the sample's 11 pairs have distinct lifts, so it never bites).
        scored.sort(key=lambda r: (-r.lift, r.product_id))
        by_product[str(pid)] = scored

    return AffinityFile(pairs=pairs, by_product=by_product).model_dump()


def _build_kpis(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    cols = [
        "date",
        "daily_events",
        "dau",
        "daily_sessions",
        "views",
        "carts",
        "purchases",
        "daily_revenue",
    ]
    rows = con.execute(f"SELECT {', '.join(cols)} FROM fact_daily_kpis ORDER BY date").fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        record = dict(zip(cols, row, strict=True))
        record["date"] = record["date"].isoformat()
        out.append(DailyKpi(**record).model_dump())
    return out


def _build_retention(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    cols = ["cohort_week", "cohort_size", "weeks_since_first", "active_users", "retention_rate"]
    rows = con.execute(
        f"SELECT {', '.join(cols)} FROM weekly_retention ORDER BY cohort_week, weeks_since_first"
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        record = dict(zip(cols, row, strict=True))
        cohort_week = record["cohort_week"]
        record["cohort_week"] = (
            cohort_week.date().isoformat()
            if isinstance(cohort_week, datetime)
            else str(cohort_week)
        )
        out.append(WeeklyRetentionRow(**record).model_dump())
    return out


def _build_rfm_summary(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        SELECT
            segment,
            COUNT(*)          AS population,
            AVG(recency_days) AS avg_recency_days,
            AVG(frequency)    AS avg_frequency,
            AVG(monetary)     AS avg_monetary,
            SUM(monetary)     AS total_monetary,
            AVG(rfm_total)    AS avg_rfm_total
        FROM user_rfm_segments
        GROUP BY segment
        ORDER BY segment
        """
    ).fetchall()
    total = sum(int(r[1]) for r in rows) or 1
    out: list[dict[str, Any]] = []
    for segment, population, avg_recency, avg_freq, avg_mon, total_mon, avg_rfm in rows:
        out.append(
            SegmentSummary(
                segment=segment,
                population=int(population),
                pct_of_buyers=round(100.0 * int(population) / total, 6),
                avg_recency_days=float(avg_recency),
                avg_frequency=float(avg_freq),
                avg_monetary=float(avg_mon),
                total_monetary=float(total_mon),
                avg_rfm_total=float(avg_rfm),
            ).model_dump()
        )
    return out


def _build_ab_grid(
    manager: DuckDBConnectionManager, segments: Sequence[str]
) -> list[dict[str, Any]]:
    service = ABTestService(manager)
    cells: list[dict[str, Any]] = []
    for segment in sorted(segments):
        for lift in AB_GRID_LIFTS:
            for confidence_level in AB_GRID_CONFIDENCE_LEVELS:
                result = service.run_simulation(
                    ABTestRequest(segment=segment, lift=lift, confidence_level=confidence_level)
                )
                cell = ABGridCell.from_result(result, lift=lift, confidence_level=confidence_level)
                cells.append(cell.model_dump())
    return cells


def _write_events_trimmed(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    # Byte-reproducible output needs both a single writer thread (ZSTD-22 framing
    # is thread-order sensitive) and a fully determined row order (the source
    # `events` table's physical order is not stable across builds).
    con.execute("SET threads TO 1")
    try:
        # user_id, session_id ordering also clusters each session's rows, which
        # ZSTD compresses better than the source order (~13.9 vs ~14.7 MB).
        con.execute(
            f"COPY (SELECT * FROM ({_EVENTS_TRIMMED_SELECT}) "
            "ORDER BY user_id, session_id, ts, product_id, event_type, price) "
            f"TO '{path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 22)"
        )
    finally:
        con.execute(f"SET threads TO {THREADS}")


def _dataset_facts(con: duckdb.DuckDBPyConnection) -> tuple[str, str, int, int]:
    row = con.execute(
        "SELECT MIN(CAST(event_time AS DATE)), MAX(CAST(event_time AS DATE)), "
        "COUNT(*), COUNT(DISTINCT user_id) FROM events"
    ).fetchone()
    if row is None:  # pragma: no cover - events always has rows here
        raise RuntimeError("events table is empty")
    return row[0].isoformat(), row[1].isoformat(), int(row[2]), int(row[3])


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def run_build(config: BuildConfig) -> BuildResult:
    out_dir = config.out_root / config.git_sha
    out_dir.mkdir(parents=True, exist_ok=True)

    created_temp = config.work_db is None
    if config.work_db is not None:
        work_db = config.work_db
        work_db.parent.mkdir(parents=True, exist_ok=True)
    else:
        work_db = Path(tempfile.mkdtemp(prefix="cip-precompute-")) / "sample.duckdb"
    if work_db.exists():
        work_db.unlink()

    try:
        counts = _build_database(config.parquet, work_db)
        model = load_model(config.model_path)

        con = duckdb.connect(str(work_db), read_only=True)
        try:
            con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
            con.execute(f"SET threads TO {THREADS}")

            segments = _build_segments(con)
            segment_names = sorted({record["segment"] for record in segments.values()})
            min_date, max_date, dataset_rows, users = _dataset_facts(con)

            _write_json(out_dir / "segments.json", segments)
            _write_json(out_dir / "propensity.json", _build_propensity(con, model))
            _write_json(out_dir / "affinity.json", _build_affinity(con))
            _write_json(out_dir / "kpis.json", _build_kpis(con))
            _write_json(out_dir / "retention.json", _build_retention(con))
            _write_json(out_dir / "rfm_summary.json", _build_rfm_summary(con))
            _write_events_trimmed(con, out_dir / "events_trimmed.parquet")

            manager = DuckDBConnectionManager(
                Settings(database_path=work_db, model_path=config.model_path)
            )
            try:
                _write_json(out_dir / "ab_grid.json", _build_ab_grid(manager, segment_names))
            finally:
                manager.close()
        finally:
            con.close()

        shutil.copyfile(METRICS_SRC, out_dir / "metrics.json")

        meta = ArtifactMeta(
            git_sha=config.git_sha,
            built_at=config.built_at or datetime.now(UTC).isoformat(),
            date_range=[min_date, max_date],
            dataset_rows=dataset_rows,
            users=users,
            row_counts=counts,
            segments=segment_names,
            propensity_users=len(json.loads((out_dir / "propensity.json").read_text())),
        )
        _write_json(out_dir / "meta.json", meta.model_dump(), pretty=True)

        files = sorted(p for p in out_dir.iterdir() if p.is_file())
        return BuildResult(out_dir=out_dir, counts=counts, work_db=work_db, files=files)
    finally:
        if created_temp and not config.keep_work_db:
            shutil.rmtree(work_db.parent, ignore_errors=True)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _git(*args: str) -> str:
    try:
        result = subprocess.run(  # nosec B603 - fixed argv, no shell, trusted `git`
            ["git", *args],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return ""
    return result.stdout.strip()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the Phase 2 static precompute artifacts.")
    parser.add_argument(
        "--out", type=Path, default=DEFAULT_OUT_ROOT, help="output root (dist/data)"
    )
    parser.add_argument(
        "--parquet", type=Path, default=DEFAULT_PARQUET, help="source events parquet"
    )
    parser.add_argument(
        "--git-sha", default="", help="artifact version; defaults to $GITHUB_SHA / HEAD"
    )
    parser.add_argument(
        "--built-at", default="", help="ISO-8601; defaults to the git_sha commit time"
    )
    parser.add_argument(
        "--model", type=Path, default=DEFAULT_MODEL, help="propensity model artifact"
    )
    parser.add_argument(
        "--keep-work-db", action="store_true", help="keep the derived sample.duckdb"
    )
    args = parser.parse_args(argv)

    git_sha: str = args.git_sha or os.environ.get("GITHUB_SHA", "") or _git("rev-parse", "HEAD")
    if not git_sha:
        git_sha = "unknown"
    built_at: str = (
        args.built_at
        or _git("show", "-s", "--format=%cI", git_sha)
        or datetime.now(UTC).isoformat()
    )

    config = BuildConfig(
        parquet=args.parquet,
        out_root=args.out,
        git_sha=git_sha,
        built_at=built_at,
        model_path=args.model,
        keep_work_db=args.keep_work_db,
    )
    result = run_build(config)

    print(f"built {len(result.files)} artifacts for {git_sha} -> {result.out_dir}")
    for path in result.files:
        print(f"  {path.name:24s} {path.stat().st_size:>12,} B")
    print(f"row counts: {result.counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
