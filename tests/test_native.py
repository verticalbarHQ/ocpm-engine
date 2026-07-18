from __future__ import annotations

import pytest

from ocpm_engine import (
    TransitionCount,
    bottleneck_order,
    dfg_conformance,
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
