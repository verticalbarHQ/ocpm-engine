# `ocpm_engine.analytics`

Typed facade over the Rust model-construction and scoring kernels. Models
are built and scored directly from sufficient-statistic rows; no event-level
data is needed.

## Input type

```python
TransitionCount(source: str, target: str, edge_type: str,
                train_count: int, test_count: int)
```

One aggregate row per transition, carrying its training-window and
test-window frequencies. All scoring functions below take an iterable of
these rows.

## Functions

```python
dfg_conformance(rows: Iterable[TransitionCount], *,
                coverage: float = 0.95) -> ConformanceScore
```
Build a frequency-covered DFG in Rust and score held-out counts. Returns
`ConformanceScore(fitness, conforming, deviations)`.

```python
variant_conformance(variants: Sequence[str],
                    train_counts: Sequence[int],
                    test_counts: Sequence[int], *,
                    coverage: float = 0.95)
    -> tuple[float, int, int, int, tuple[str, ...]]
```
Build a frequency-covered complete-variant model and score it. Returns
`(fitness, conforming, deviations, test_total, model_variants)`.

```python
next_activity(rows: Iterable[TransitionCount]) -> PredictionScore
```
Fit deterministic next-activity choices on training counts and score
held-out counts. Returns `PredictionScore(accuracy, correct, test_total,
predictions)`, where each prediction is a `(source, predicted, actual)`
label triple.

```python
bottleneck_order(frequencies: Sequence[int],
                 mean_durations: Sequence[float]) -> tuple[int, ...]
```
Stable transition indexes ranked by duration, frequency, then input order.

```python
frequency_drift(labels: Sequence[str],
                baseline_counts: Sequence[int],
                current_counts: Sequence[int], *,
                top_n: int = 10) -> DriftScore
```
Score and explain change between two aligned frequency distributions.
Returns `DriftScore(divergence, baseline_total, current_total,
contributors)`; each `DriftContributor` carries `label`, `baseline_share`,
`current_share`, `share_delta`, and `js_contribution`.

## Result types

| Type | Fields |
|---|---|
| `ConformanceScore` | `fitness`, `conforming`, `deviations` |
| `PredictionScore` | `accuracy`, `correct`, `test_total`, `predictions` |
| `DriftScore` | `divergence`, `baseline_total`, `current_total`, `contributors` |
| `DriftContributor` | `label`, `baseline_share`, `current_share`, `share_delta`, `js_contribution` |

## Example

```python
from ocpm_engine import TransitionCount, dfg_conformance, next_activity

rows = [
    TransitionCount("Create", "Approve", "directly_follows", 900, 95),
    TransitionCount("Create", "Reject", "directly_follows", 100, 5),
]

conformance = dfg_conformance(rows, coverage=0.95)
print(conformance.fitness, conformance.deviations)

prediction = next_activity(rows)
print(prediction.accuracy, prediction.predictions)
```
