from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, Field


class UserSegment(BaseModel):
    user_id: int
    recency_days: int = Field(description="Days since the user's most recent purchase")
    frequency: int = Field(description="Number of distinct days with a purchase")
    monetary: float = Field(description="Total historical spend")
    r_score: int = Field(ge=1, le=5)
    f_score: int = Field(ge=1, le=5)
    m_score: int = Field(ge=1, le=5)
    rfm_total: int
    segment: str = Field(description="RFM segment label, e.g. 'Champions', 'At Risk'")


class PropensityScore(BaseModel):
    user_id: int
    purchase_probability: float = Field(ge=0.0, le=1.0)
    scored_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ProductRecommendation(BaseModel):
    product_id: int = Field(description="The recommended (co-purchased) product")
    pair_count: int = Field(description="Number of sessions both products were purchased in")
    confidence: float = Field(ge=0.0)
    lift: float = Field(ge=0.0, description="Association strength; >1 means a positive affinity")


class ABTestRequest(BaseModel):
    segment: str = Field(description="RFM segment to simulate the campaign against")
    lift: float = Field(default=0.15, gt=-1.0, le=5.0, description="Expected relative lift")
    confidence_level: float = Field(default=0.95, gt=0.0, lt=1.0)


class ABTestResult(BaseModel):
    segment: str
    control_visitors: int
    treatment_visitors: int
    control_conversion_rate: float
    treatment_conversion_rate: float
    relative_lift: float
    p_value: float
    is_significant: bool
    ci_95_lower: float
    ci_95_upper: float
    statistical_power: float
