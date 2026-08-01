from __future__ import annotations

import struct
from datetime import UTC, datetime, timedelta

import pytest

from ocpm_engine import (
    TransitionCount,
    binding_capsule_info,
    bottleneck_order,
    decode_binding_capsule,
    decode_binding_pair_groups,
    dfg_conformance,
    frequency_drift,
    next_activity,
    summarize_event_batch_rows,
    summarize_event_window_batch_rows,
    variant_conformance,
)
from ocpm_engine.event_batches import summarize_event_row_fallback

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
    groups = decode_binding_pair_groups(capsule)
    assert groups[0].source == 7
    assert groups[0].targets == (20, 21)
    assert groups[0].events == (200, 201)
    assert groups[0].expanded_row_count == 4


def _event_batch(
    window: int | None = None,
) -> tuple[object, ...]:
    path = ["A", "B", "A"]
    case_ids = struct.pack("=qq", 7, 9)
    timestamps = b"".join(
        (
            struct.pack("=iqqq", 3, 0, 2_000_000, 5_000_000),
            struct.pack("=iqqq", 3, 0, 4_000_000, 6_000_000),
        )
    )
    values: tuple[object, ...] = (path, 3, 2, case_ids, timestamps)
    return (window, *values) if window is not None else values


def test_native_event_batch_summary_stays_factorized() -> None:
    summary = summarize_event_batch_rows([_event_batch()])

    assert (summary.case_count, summary.event_count) == (2, 6)
    assert summary.payload_bytes == 72
    assert summary.variants[0].activity_path == ("A", "B", "A")
    assert summary.variants[0].frequency == 2
    assert summary.activities[0].case_frequency == 2
    assert summary.activities[0].occurrence_frequency == 4
    assert tuple(
        (edge.source, edge.target, edge.frequency, edge.mean_duration_seconds)
        for edge in summary.dfg
    ) == (("A", "B", 2, 3.0), ("B", "A", 2, 2.5))


def test_native_multi_window_summary_preserves_empty_windows() -> None:
    summaries = summarize_event_window_batch_rows([_event_batch(2)], window_count=3)

    assert tuple(summary.case_count for summary in summaries) == (0, 2, 0)
    assert tuple(summary.event_count for summary in summaries) == (0, 6, 0)


def test_native_event_batch_summary_is_incremental_across_chunks() -> None:
    summary = summarize_event_batch_rows(_event_batch() for _ in range(65))

    assert summary.case_count == 130
    assert summary.event_count == 390
    assert summary.payload_bytes == 65 * 72


def test_native_multi_window_summary_rejects_an_unexpected_window() -> None:
    with pytest.raises(ValueError, match="unexpected window 4"):
        summarize_event_window_batch_rows([_event_batch(4)], window_count=3)


def test_native_event_batch_rejects_corrupt_payloads() -> None:
    row = list(_event_batch())
    row[3] = b"short"
    with pytest.raises(ValueError, match="case-id payload length"):
        summarize_event_batch_rows([row])


def test_event_row_fallback_matches_factorized_summary_with_empty_window() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        (1, 7, "A", start, 1),
        (1, 7, "B", start + timedelta(seconds=2), 2),
        (1, 7, "A", start + timedelta(seconds=5), 3),
        (1, 9, "A", start, 1),
        (1, 9, "B", start + timedelta(seconds=4), 2),
        (1, 9, "A", start + timedelta(seconds=6), 3),
    ]

    fallback, empty = summarize_event_row_fallback(rows, window_count=2)
    factorized = summarize_event_batch_rows([_event_batch()])

    assert fallback == factorized.__class__(
        case_count=factorized.case_count,
        event_count=factorized.event_count,
        payload_bytes=0,
        variants=factorized.variants,
        dfg=factorized.dfg,
        activities=factorized.activities,
    )
    assert (empty.case_count, empty.event_count, empty.payload_bytes) == (0, 0, 0)


def test_event_row_fallback_rejects_invalid_order_and_unexpected_windows() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    with pytest.raises(RuntimeError, match="not ordered by case and ordinal"):
        summarize_event_row_fallback([(1, 7, "A", start, 2)], window_count=1)
    with pytest.raises(RuntimeError, match="unexpected window 2"):
        summarize_event_row_fallback([(2, 7, "A", start, 1)], window_count=1)
