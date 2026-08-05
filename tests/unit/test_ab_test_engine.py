from __future__ import annotations

import pandas as pd

from src.analysis.ab_testing import ABTestEngine


def test_generate_synthetic_experiment_assigns_valid_groups_and_outcomes() -> None:
    engine = ABTestEngine()
    df = pd.DataFrame(index=range(1000))

    result = engine.generate_synthetic_experiment(df, lift=0.15)

    assert set(result["group"].unique()) <= {"control", "treatment"}
    assert result["converted"].isin([0, 1]).all()
    assert len(result) == 1000


def test_analyze_experiment_produces_internally_consistent_result() -> None:
    # A moderate effect size/sample size on purpose: statsmodels' power solver can
    # return NaN when power saturates near 1.0 at extreme effect sizes (e.g. a huge
    # lift over a huge n), which isn't a bug in ABTestEngine, just a numerically
    # unstable regime to avoid in a value-range assertion like this one.
    engine = ABTestEngine(confidence_level=0.95)
    df = pd.DataFrame(index=range(5000))
    experiment = engine.generate_synthetic_experiment(df, lift=0.15)

    results = engine.analyze_experiment(experiment)

    assert 0.0 <= results["p_value"] <= 1.0
    lower, upper = results["ci_95"]
    assert lower < upper
    assert results["stat_sig"] == (results["p_value"] < engine.alpha)
    assert 0.0 <= results["power"] <= 1.0


def test_higher_confidence_level_widens_the_interval() -> None:
    df = pd.DataFrame(index=range(5000))

    narrow_engine = ABTestEngine(confidence_level=0.80)
    wide_engine = ABTestEngine(confidence_level=0.99)

    narrow = narrow_engine.analyze_experiment(
        narrow_engine.generate_synthetic_experiment(df.copy(), lift=0.2)
    )
    wide = wide_engine.analyze_experiment(
        wide_engine.generate_synthetic_experiment(df.copy(), lift=0.2)
    )

    narrow_width = narrow["ci_95"][1] - narrow["ci_95"][0]
    wide_width = wide["ci_95"][1] - wide["ci_95"][0]
    assert wide_width > narrow_width
