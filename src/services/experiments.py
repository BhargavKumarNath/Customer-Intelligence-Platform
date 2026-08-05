from __future__ import annotations

import pandas as pd

from src.analysis.ab_testing import ABTestEngine
from src.db import DuckDBConnectionManager
from src.domain.models import ABTestRequest, ABTestResult
from src.services.errors import SegmentNotFoundError

_MIN_SEGMENT_SIZE = 30


class ABTestService:
    """Wraps ABTestEngine (src/analysis/ab_testing.py) directly — its simulation
    logic only depends on population size, so no rewrite is needed here."""

    def __init__(self, connections: DuckDBConnectionManager) -> None:
        self._connections = connections

    def run_simulation(self, request: ABTestRequest) -> ABTestResult:
        with self._connections.cursor() as cur:
            row = cur.execute(
                "SELECT COUNT(*) FROM user_rfm_segments WHERE segment = ?", [request.segment]
            ).fetchone()
        population = row[0] if row else 0
        if population < _MIN_SEGMENT_SIZE:
            raise SegmentNotFoundError(
                f"Segment '{request.segment}' has too few users ({population}) to simulate"
            )

        engine = ABTestEngine(confidence_level=request.confidence_level)
        placeholder = pd.DataFrame(index=range(population))
        experiment = engine.generate_synthetic_experiment(placeholder, lift=request.lift)
        results = engine.analyze_experiment(experiment)

        summary = results["summary"]
        control = summary.loc["control"]
        treatment = summary.loc["treatment"]
        ci_lower, ci_upper = results["ci_95"]

        return ABTestResult(
            segment=request.segment,
            control_visitors=int(control["visitors"]),
            treatment_visitors=int(treatment["visitors"]),
            control_conversion_rate=float(control["conversion_rate"]),
            treatment_conversion_rate=float(treatment["conversion_rate"]),
            relative_lift=float(results["lift"]),
            p_value=float(results["p_value"]),
            is_significant=bool(results["stat_sig"]),
            ci_95_lower=float(ci_lower),
            ci_95_upper=float(ci_upper),
            statistical_power=float(results["power"]),
        )
