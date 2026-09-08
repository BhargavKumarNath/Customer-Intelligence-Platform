"""Read-only metadata over the precomputed star schema + the frozen model card.

Backs the Phase 3 discovery endpoints (``/version``, ``/v1/meta``,
``/v1/segments``, ``/v1/models``). Every value is derived live from
``sample.duckdb`` and ``src/models/metrics.json`` - nothing is hard-coded - so the
API and the Phase 2 static artifacts stay in agreement by construction. The
per-segment aggregate SQL is deliberately the same shape as
``scripts/build_static_artifacts._build_rfm_summary`` so ``/v1/segments`` matches
``rfm_summary.json``.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from src.db import DuckDBConnectionManager
from src.domain.artifacts import ArtifactMeta, SegmentSummary
from src.domain.models import ModelDetail, ModelSummary, VersionInfo
from src.services.errors import SegmentNotFoundError

# Keys mirror src.processing.dimensional_model.build_all()'s return dict.
_SCHEMA_TABLES = (
    "dim_products",
    "dim_users",
    "fact_sessions",
    "fact_daily_kpis",
    "user_rfm_segments",
    "predictions_product_affinity",
    "weekly_retention",
)

_PROPENSITY_CUTOFF = "2019-11-01"

_RFM_SUMMARY_SQL = """
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


@lru_cache(maxsize=4)
def load_metrics(metrics_path: Path) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(metrics_path.read_text(encoding="utf-8"))
    return data


def _one(cur: Any, sql: str, params: list[Any] | None = None) -> tuple[Any, ...]:
    """fetchone() that never returns None - every query here is a guaranteed-row aggregate."""
    row = cur.execute(sql, params).fetchone() if params else cur.execute(sql).fetchone()
    if row is None:  # pragma: no cover - aggregates always yield a row
        raise RuntimeError(f"expected a row from: {sql}")
    return row  # type: ignore[no-any-return]


class MetadataService:
    def __init__(
        self,
        connections: DuckDBConnectionManager,
        metrics_path: Path,
        *,
        git_sha: str = "",
        built_at: str = "",
    ) -> None:
        self._connections = connections
        self._metrics_path = metrics_path
        self._git_sha = git_sha
        self._built_at = built_at

    # -- version ---------------------------------------------------------------
    def version(self) -> VersionInfo:
        metrics = load_metrics(self._metrics_path)
        model_version = str(metrics["git_sha"])
        return VersionInfo(
            git_sha=self._git_sha or model_version,
            built_at=self._built_at or str(metrics["trained_at"]),
            model_version=model_version,
        )

    # -- dataset meta --------------------------------------------------------
    def meta(self) -> ArtifactMeta:
        metrics = load_metrics(self._metrics_path)
        with self._connections.cursor() as cur:
            min_date, max_date, dataset_rows, users = _one(
                cur,
                "SELECT MIN(CAST(event_time AS DATE)), MAX(CAST(event_time AS DATE)), "
                "COUNT(*), COUNT(DISTINCT user_id) FROM events",
            )
            # table names come only from the fixed _SCHEMA_TABLES tuple, never a caller.
            row_counts = {
                table: int(_one(cur, f"SELECT COUNT(*) FROM {table}")[0])  # nosec B608
                for table in _SCHEMA_TABLES
            }
            segments = [
                r[0]
                for r in cur.execute(
                    "SELECT DISTINCT segment FROM user_rfm_segments ORDER BY segment"
                ).fetchall()
            ]
            propensity_users = int(
                _one(
                    cur,
                    "SELECT COUNT(DISTINCT user_id) FROM events WHERE event_time < ?",
                    [_PROPENSITY_CUTOFF],
                )[0]
            )
        return ArtifactMeta(
            git_sha=self._git_sha or str(metrics["git_sha"]),
            built_at=self._built_at or str(metrics["trained_at"]),
            date_range=[min_date.isoformat(), max_date.isoformat()],
            dataset_rows=int(dataset_rows),
            users=int(users),
            row_counts=row_counts,
            segments=segments,
            propensity_users=propensity_users,
        )

    # -- segments ----------------------------------------------------------
    def list_segments(self) -> list[SegmentSummary]:
        with self._connections.cursor() as cur:
            rows = cur.execute(_RFM_SUMMARY_SQL).fetchall()
        total = sum(int(r[1]) for r in rows) or 1
        return [
            SegmentSummary(
                segment=segment,
                population=int(population),
                pct_of_buyers=round(100.0 * int(population) / total, 6),
                avg_recency_days=float(avg_recency),
                avg_frequency=float(avg_freq),
                avg_monetary=float(avg_mon),
                total_monetary=float(total_mon),
                avg_rfm_total=float(avg_rfm),
            )
            for segment, population, avg_recency, avg_freq, avg_mon, total_mon, avg_rfm in rows
        ]

    def get_segment_summary(self, name: str) -> SegmentSummary:
        for summary in self.list_segments():
            if summary.segment == name:
                return summary
        raise SegmentNotFoundError(f"RFM segment '{name}' does not exist")

    # -- models ----------------------------------------------------------
    def list_models(self) -> list[ModelSummary]:
        metrics = load_metrics(self._metrics_path)
        return [
            ModelSummary(
                name="propensity",
                kind="lightgbm-gbdt-binary",
                version=str(metrics["git_sha"]),
                auc_roc=float(metrics["auc_roc"]),
            )
        ]

    def propensity_model(self) -> ModelDetail:
        m = load_metrics(self._metrics_path)
        return ModelDetail(
            name="propensity",
            version=str(m["git_sha"]),
            trained_at=str(m["trained_at"]),
            auc_roc=float(m["auc_roc"]),
            precision_top5pct=float(m["precision_top5pct"]),
            recall_top5pct=float(m["recall_top5pct"]),
            lift_top5pct=float(m["lift_top5pct"]),
            baseline_conversion_rate=float(m["baseline_conversion_rate"]),
            feature_importance_gain={
                str(k): float(v) for k, v in m["feature_importance_gain"].items()
            },
            params=dict(m["params"]),
        )
