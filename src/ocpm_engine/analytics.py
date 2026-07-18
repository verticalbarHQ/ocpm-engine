"""Typed façade over the Rust model-construction and scoring kernels."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

try:
    from . import _native
except ImportError:  # Helpful message for an unbuilt source checkout.
    _native = None


@dataclass(frozen=True, slots=True)
class TransitionCount:
    source: str
    target: str
    edge_type: str
    train_count: int
    test_count: int


@dataclass(frozen=True, slots=True)
class ConformanceScore:
    fitness: float
    conforming: int
    deviations: int
    test_total: int
    model: tuple[tuple[str, str, str], ...]


@dataclass(frozen=True, slots=True)
class PredictionScore:
    accuracy: float
    correct: int
    test_total: int
    predictions: tuple[tuple[str, str, str], ...]


def _extension():
    if _native is None:
        raise RuntimeError(
            "ocpm-engine native module is not built; install a wheel or run "
            "`maturin develop`"
        )
    return _native


def _transition_columns(rows: Iterable[TransitionCount]) -> tuple[list, ...]:
    sources: list[str] = []
    targets: list[str] = []
    edge_types: list[str] = []
    train_counts: list[int] = []
    test_counts: list[int] = []
    for row in rows:
        sources.append(row.source)
        targets.append(row.target)
        edge_types.append(row.edge_type)
        train_counts.append(row.train_count)
        test_counts.append(row.test_count)
    return sources, targets, edge_types, train_counts, test_counts


def dfg_conformance(
    rows: Iterable[TransitionCount], *, coverage: float = 0.95
) -> ConformanceScore:
    """Build a frequency-covered DFG in Rust and score held-out counts."""

    result = _extension().dfg_conformance(*_transition_columns(rows), coverage)
    return ConformanceScore(
        fitness=result[0],
        conforming=result[1],
        deviations=result[2],
        test_total=result[3],
        model=tuple(tuple(item) for item in result[4]),
    )


def next_activity(rows: Iterable[TransitionCount]) -> PredictionScore:
    """Fit deterministic next-activity choices and score held-out counts."""

    result = _extension().next_activity(*_transition_columns(rows))
    return PredictionScore(
        accuracy=result[0],
        correct=result[1],
        test_total=result[2],
        predictions=tuple(tuple(item) for item in result[3]),
    )


def variant_conformance(
    variants: Sequence[str],
    train_counts: Sequence[int],
    test_counts: Sequence[int],
    *,
    coverage: float = 0.95,
) -> tuple[float, int, int, int, tuple[str, ...]]:
    """Build a frequency-covered complete-variant model and score it."""

    result = _extension().variant_conformance(
        list(variants), list(train_counts), list(test_counts), coverage
    )
    return result[0], result[1], result[2], result[3], tuple(result[4])


def bottleneck_order(
    frequencies: Sequence[int], mean_durations: Sequence[float]
) -> tuple[int, ...]:
    """Return stable indexes ranked by duration, frequency, then input order."""

    return tuple(_extension().bottleneck_order(list(frequencies), list(mean_durations)))
