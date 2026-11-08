"""The tie-aware top-5% rate used to compare propensity rankers."""

from __future__ import annotations

import numpy as np
import pytest

from src.models.evaluate_baselines import headline_top5_rate, top_frac_rate


def test_distinct_scores_take_exact_top_fraction() -> None:
    scores = np.arange(100, dtype=float)
    y = (scores >= 95).astype(np.int8)  # the 5 best-scored users all convert
    assert top_frac_rate(scores, y, 0.05) == pytest.approx(1.0)


def test_boundary_tie_group_counts_at_its_mean_rate() -> None:
    # 10 users: 2 top-scored (1 converts), then a tie group of 8 (2 convert).
    scores = np.array([9, 9, 5, 5, 5, 5, 5, 5, 5, 5], dtype=float)
    y = np.array([1, 0, 1, 1, 0, 0, 0, 0, 0, 0], dtype=np.int8)
    # Top 40% = 4 users: the 2 nines (1 positive) + 2 of the 8 fives at 2/8.
    assert top_frac_rate(scores, y, 0.4) == pytest.approx((1 + 2 * 2 / 8) / 4)


def test_all_tied_scores_give_the_base_rate() -> None:
    rng = np.random.default_rng(0)
    y = rng.binomial(1, 0.08, 10_000).astype(np.int8)
    assert top_frac_rate(np.zeros(10_000), y, 0.05) == pytest.approx(y.mean())


def test_order_of_rows_does_not_matter() -> None:
    rng = np.random.default_rng(1)
    scores = rng.integers(0, 30, 5_000).astype(float)
    y = rng.binomial(1, 0.1, 5_000).astype(np.int8)
    perm = rng.permutation(5_000)
    assert top_frac_rate(scores, y) == pytest.approx(top_frac_rate(scores[perm], y[perm]))


def test_headline_rule_includes_ties_at_the_threshold() -> None:
    scores = np.array([3.0] * 10 + [1.0] * 90)
    y = np.array([1] * 5 + [0] * 95, dtype=np.int8)
    # >= 95th percentile keeps all 10 tied top users, not 5.
    assert headline_top5_rate(scores, y) == pytest.approx(0.5)
