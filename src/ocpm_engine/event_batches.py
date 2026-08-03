"""Factorized pg_ocpm 0.9 event-batch summaries."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

_NATIVE_BATCH_CHUNK_ROWS = 64


@dataclass(frozen=True, slots=True)
class EventVariantCount:
    activity_path: tuple[str, ...]
    frequency: int


@dataclass(frozen=True, slots=True)
class EventDfgEdge:
    source: str
    target: str
    frequency: int
    mean_duration_seconds: float


@dataclass(frozen=True, slots=True)
class EventActivityCount:
    activity: str
    case_frequency: int
    occurrence_frequency: int
    start_frequency: int
    end_frequency: int


@dataclass(frozen=True, slots=True)
class EventLogSummary:
    case_count: int
    event_count: int
    payload_bytes: int
    variants: tuple[EventVariantCount, ...]
    dfg: tuple[EventDfgEdge, ...]
    activities: tuple[EventActivityCount, ...]


@dataclass(frozen=True, slots=True)
class EventLogExecution:
    strategy: str
    database_rows: int
    expanded_event_rows: int
    summaries: tuple[EventLogSummary, ...]


def _extension():
    from . import _native

    return _native


def _summary_from_native(value: Sequence[Any]) -> EventLogSummary:
    return EventLogSummary(
        case_count=int(value[0]),
        event_count=int(value[1]),
        payload_bytes=int(value[2]),
        variants=tuple(
            EventVariantCount(tuple(path), int(frequency))
            for path, frequency in value[3]
        ),
        dfg=tuple(EventDfgEdge(*edge) for edge in value[4]),
        activities=tuple(EventActivityCount(*activity) for activity in value[5]),
    )


def summarize_event_batch_rows(rows: Iterable[Sequence[Any]]) -> EventLogSummary:
    """Validate and summarize compact single-window result rows in Rust."""

    builder = _extension().EventWindowSummaryBuilder(1)
    pending = []
    for activity_path, activity_count, case_count, case_ids, timestamps in rows:
        pending.append(
            (
                1,
                list(activity_path),
                int(activity_count),
                int(case_count),
                bytes(case_ids),
                bytes(timestamps),
            )
        )
        if len(pending) == _NATIVE_BATCH_CHUNK_ROWS:
            builder.push_batches(pending)
            pending.clear()
    if pending:
        builder.push_batches(pending)
    return _summary_from_native(builder.finish()[0])


def summarize_event_window_batch_rows(
    rows: Iterable[Sequence[Any]], *, window_count: int
) -> tuple[EventLogSummary, ...]:
    """Summarize one compact multi-window result without expanding events."""

    if window_count <= 0:
        raise ValueError("window_count must be positive")
    builder = _extension().EventWindowSummaryBuilder(window_count)
    pending = []
    for (
        window,
        activity_path,
        activity_count,
        case_count,
        case_ids,
        timestamps,
    ) in rows:
        pending.append(
            (
                int(window),
                list(activity_path),
                int(activity_count),
                int(case_count),
                bytes(case_ids),
                bytes(timestamps),
            )
        )
        if len(pending) == _NATIVE_BATCH_CHUNK_ROWS:
            builder.push_batches(pending)
            pending.clear()
    if pending:
        builder.push_batches(pending)
    return tuple(_summary_from_native(summary) for summary in builder.finish())


def summarize_event_row_fallback(
    rows: Iterable[Sequence[Any]], *, window_count: int
) -> tuple[EventLogSummary, ...]:
    """Compatibility path for pg_ocpm 0.8's event-row stream.

    pg_ocpm 0.9 callers should use the native factorized path above. This code
    remains exact, deterministic, and bounded to one case plus aggregate maps,
    but DB-API has already allocated one Python tuple per event.
    """

    if window_count <= 0:
        raise ValueError("window_count must be positive")
    summaries = []
    source_rows = iter(rows)
    pending = next(source_rows, None)
    for expected_window in range(1, window_count + 1):
        case_count = 0
        event_count = 0
        variants: Counter[tuple[str, ...]] = Counter()
        edges: dict[tuple[str, str], list[float]] = defaultdict(lambda: [0, 0.0])
        activities: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0, 0])
        current_case: int | None = None
        current_path: list[str] = []
        current_timestamps: list[datetime] = []
        expected_ordinal = 1

        def flush_case() -> None:
            nonlocal case_count
            if current_case is None:
                return
            case_count += 1
            path = tuple(current_path)
            variants[path] += 1
            for activity in set(path):
                activities[activity][0] += 1
            for activity in path:
                activities[activity][1] += 1
            activities[path[0]][2] += 1
            activities[path[-1]][3] += 1
            for index, (source, target) in enumerate(zip(path, path[1:])):
                accumulator = edges[(source, target)]
                accumulator[0] += 1
                accumulator[1] += (
                    current_timestamps[index + 1] - current_timestamps[index]
                ).total_seconds()

        while pending is not None and int(pending[0]) == expected_window:
            window, case_id, activity, timestamp, ordinal = pending
            del window
            case_id = int(case_id)
            ordinal = int(ordinal)
            if current_case is not None and case_id != current_case:
                flush_case()
                current_path = []
                current_timestamps = []
                expected_ordinal = 1
            if ordinal != expected_ordinal:
                raise RuntimeError(
                    "event-log fallback is not ordered by case and ordinal"
                )
            current_case = case_id
            current_path.append(str(activity))
            current_timestamps.append(timestamp)
            event_count += 1
            expected_ordinal += 1
            pending = next(source_rows, None)
        flush_case()
        summaries.append(
            EventLogSummary(
                case_count=case_count,
                event_count=event_count,
                payload_bytes=0,
                variants=tuple(
                    EventVariantCount(path, frequency)
                    for path, frequency in sorted(variants.items())
                ),
                dfg=tuple(
                    EventDfgEdge(source, target, int(values[0]), values[1] / values[0])
                    for (source, target), values in sorted(edges.items())
                ),
                activities=tuple(
                    EventActivityCount(activity, *counts)
                    for activity, counts in sorted(activities.items())
                ),
            )
        )
    if pending is not None:
        raise RuntimeError(
            f"event-log fallback returned unexpected window {pending[0]}"
        )
    return tuple(summaries)
