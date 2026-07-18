from __future__ import annotations

import pytest

from ocpm_engine import (
    TransitionCount,
    bottleneck_order,
    dfg_conformance,
    frequency_drift,
    next_activity,
    variant_conformance,
)

pytest.importorskip("ocpm_engine._native")


ROWS = (
    TransitionCount("A", "B", "Order", 80, 8),
    TransitionCount("B", "C", "Order", 15, 1),
    TransitionCount("B", "D", "Order", 5, 1),
)


def test_native_dfg_conformance() -> None:
    score = dfg_conformance(ROWS)
    assert score.fitness == 0.9
    assert score.deviations == 1
    assert score.model == (("A", "B", "Order"), ("B", "C", "Order"))


def test_native_next_activity_and_variant_conformance() -> None:
    prediction = next_activity(ROWS)
    variant = variant_conformance(["v1", "v2"], [95, 5], [9, 1])

    assert prediction.test_total == 10
    assert prediction.correct == 9
    assert variant[:4] == (0.9, 9, 1, 10)
    assert variant[4] == ("v1",)


def test_native_next_activity_ignores_test_only_transitions() -> None:
    prediction = next_activity(
        (
            TransitionCount("A", "B", "Order", 10, 0),
            TransitionCount("Z", "Y", "Order", 0, 5),
        )
    )

    assert prediction.test_total == 5
    assert prediction.correct == 0
    assert prediction.predictions == (("A", "Order", "B"),)


def test_native_bottleneck_rank_and_validation() -> None:
    assert bottleneck_order([10, 20, 30], [2.0, 2.0, 1.0]) == (1, 0, 2)
    with pytest.raises(ValueError, match="same length"):
        bottleneck_order([1], [])


def test_native_frequency_drift_is_bounded_and_explainable() -> None:
    score = frequency_drift(["A", "B"], [10, 0], [0, 10], top_n=2)

    assert score.divergence == 1.0
    assert score.baseline_total == 10
    assert score.current_total == 10
    assert tuple(item.label for item in score.contributors) == ("A", "B")
    assert tuple(item.js_contribution for item in score.contributors) == (0.5, 0.5)


def test_native_frequency_drift_validates_column_lengths_and_limit() -> None:
    with pytest.raises(ValueError, match="same length"):
        frequency_drift(["A"], [1], [])
    with pytest.raises(ValueError, match="nonnegative"):
        frequency_drift([], [], [], top_n=-1)
