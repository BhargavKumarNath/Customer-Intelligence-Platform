"""Schemas for the Phase 2 static precompute artifacts.

These describe the JSON files `scripts/build_static_artifacts.py` emits into
`dist/data/<git_sha>/` and that the Phase 4/5 frontend (and, later, the Phase 3
`/v1/meta` + `/v1/segments` endpoints) consume. They live next to
`src/domain/models.py` because the frontend's generated TypeScript types and the
API's future response models should derive from one source, not from hand-copied
shapes.

Every model here is a plain declaration: no behaviour, no I/O. `model_json_schema`
output for each is checked in under `schemas/` and a test asserts the checked-in
copy has not drifted.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from src.domain.models import ABTestResult, ProductRecommendation

# Re-exported so callers can build `schemas/` for the user-keyed files from one
# place. segments.json is {user_id: UserSegment}; propensity.json is
# {user_id: probability}.
__all__ = [
    "ABGridCell",
    "AffinityFile",
    "AffinityPair",
    "ArtifactMeta",
    "DailyKpi",
    "SegmentSummary",
    "WeeklyRetentionRow",
]


class DailyKpi(BaseModel):
    """One row of `fact_daily_kpis`."""

    date: str = Field(description="ISO-8601 calendar date, e.g. '2019-10-01'")
    daily_events: int
    dau: int
    daily_sessions: int
    views: int
    carts: int
    purchases: int
    daily_revenue: float


class WeeklyRetentionRow(BaseModel):
    """One cell of the `weekly_retention` cohort grid."""

    cohort_week: str = Field(description="ISO-8601 date of the Monday the cohort's week starts")
    cohort_size: int
    weeks_since_first: int
    active_users: int
    retention_rate: float = Field(ge=0.0, le=1.0)


class SegmentSummary(BaseModel):
    """Per-segment population and aggregate stats, derived from `user_rfm_segments`."""

    segment: str
    population: int = Field(ge=0)
    pct_of_buyers: float = Field(ge=0.0, le=100.0)
    avg_recency_days: float
    avg_frequency: float
    avg_monetary: float
    total_monetary: float
    avg_rfm_total: float


class ABGridCell(BaseModel):
    """One precomputed A/B simulation: an `ABTestResult` plus the request knobs
    that produced it, so the parity test can rebuild the `ABTestRequest`.

    `statistical_power` is `None` when statsmodels' solver returns NaN (large lift
    over a large population saturates power at 1.0 and the root-finder fails); in
    that case `power_undefined` is `True`. See deployment_stages.md P2-NEW-3.
    """

    segment: str
    lift: float
    confidence_level: float
    control_visitors: int
    treatment_visitors: int
    control_conversion_rate: float
    treatment_conversion_rate: float
    relative_lift: float
    p_value: float
    is_significant: bool
    ci_95_lower: float
    ci_95_upper: float
    statistical_power: float | None
    power_undefined: bool

    @classmethod
    def from_result(
        cls, result: ABTestResult, *, lift: float, confidence_level: float
    ) -> ABGridCell:
        power: float | None = result.statistical_power
        undefined = power is None or not (0.0 <= power <= 1.0)
        if undefined:
            power = None
        return cls(
            segment=result.segment,
            lift=lift,
            confidence_level=confidence_level,
            control_visitors=result.control_visitors,
            treatment_visitors=result.treatment_visitors,
            control_conversion_rate=result.control_conversion_rate,
            treatment_conversion_rate=result.treatment_conversion_rate,
            relative_lift=result.relative_lift,
            p_value=result.p_value,
            is_significant=result.is_significant,
            ci_95_lower=result.ci_95_lower,
            ci_95_upper=result.ci_95_upper,
            statistical_power=power,
            power_undefined=undefined,
        )


class AffinityPair(BaseModel):
    """One row of `predictions_product_affinity` (product_a < product_b)."""

    product_a: int
    product_b: int
    pair_count: int
    confidence: float = Field(ge=0.0)
    lift: float = Field(ge=0.0)


class AffinityFile(BaseModel):
    """`affinity.json`: the raw pairs plus a both-direction lookup whose values
    match `RecommendationService.get_recommendations(product_id, 50)` element for
    element (same ordering: lift descending)."""

    pairs: list[AffinityPair]
    by_product: dict[str, list[ProductRecommendation]]


class ArtifactMeta(BaseModel):
    """`meta.json`: everything a consumer needs to trust and pin an artifact set."""

    git_sha: str
    built_at: str = Field(description="ISO-8601 UTC timestamp; the only wall-clock in the set")
    date_range: list[str] = Field(min_length=2, max_length=2, description="[min_date, max_date]")
    dataset_rows: int
    users: int
    row_counts: dict[str, int] = Field(description="build_all() return dict, table -> row count")
    segments: list[str]
    propensity_users: int = Field(description="users with event_time < 2019-11-01")
