from __future__ import annotations

import pytest

from ocpm_engine import (
    TransitionCount,
    binding_capsule_info,
    bottleneck_order,
    decode_binding_capsule,
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


def _varint(value: int) -> bytes:
    output = bytearray()
    while value >= 0x80:
        output.append((value & 0x7F) | 0x80)
        value >>= 7
    output.append(value)
    return bytes(output)


def _signed(value: int) -> bytes:
    return _varint((value << 1) ^ (value >> 63))


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


def test_native_binding_capsule_metadata_and_rows() -> None:
    capsule = b"OCPB" + bytes((1, 1)) + _varint(3)
    capsule += b"".join(_signed(delta) for delta in (10, 2, -1))
    capsule += bytes((0b0000_0101,))

    info = binding_capsule_info(capsule)
    assert (info.schema, info.row_count, info.factorized) == (1, 3, False)
    rows = decode_binding_capsule(capsule)
    assert tuple(row.ids for row in rows) == ((10,), (12,), (11,))
    assert tuple(row.violated for row in rows) == (True, False, True)


def test_native_binding_pair_capsule_expands_factorized_groups() -> None:
    capsule = b"OCPB" + bytes((1, 6)) + _varint(4)
    capsule += _varint(1) + _signed(7) + _varint(2)
    capsule += _signed(20) + _signed(1)
    capsule += _signed(200) + _signed(1)

    info = binding_capsule_info(capsule)
    assert (info.schema, info.row_count, info.factorized) == (6, 4, True)
    assert tuple(row.ids for row in decode_binding_capsule(capsule)) == (
        (7, 20, 20, 200, 200),
        (7, 20, 21, 200, 201),
        (7, 21, 20, 201, 200),
        (7, 21, 21, 201, 201),
    )
