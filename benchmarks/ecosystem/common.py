"""Shared, implementation-neutral contract for the ecosystem benchmark.

The benchmark deliberately keeps scoring outside each project.  Every arm must
derive the same lifecycle facts from the same OCEL source and then call these
functions.  This prevents a library-specific definition of a DFG, variant, or
time window from changing the question being measured.
"""

from __future__ import annotations

import hashlib
import json
import math
import statistics
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

DATASETS = {
    "rust4pm_p2p": {
        "pair": "rust4pm",
        "filename": "rust4pm-ocel2-p2p.sqlite",
        "source_url": ("https://zenodo.org/records/8412920/files/ocel2-p2p.sqlite"),
        "sqlite_sha256": (
            "0017c34aeecdcb7712004d4364b11b372f2cc1a9cf2639ffe295f95a0df1ee74"
        ),
        "source": {
            "title": (
                "Procure-To-Payment (P2P) Object-centric Event Log in OCEL 2.0 Standard"
            ),
            "identifier": "10.5281/zenodo.8412920",
            "upstream_project": "Rust4PM 0.6.0 test-data corpus",
            "upstream_revision": "b4c06f323fca55cf57eaf44ac25b46ea7c448cb4",
            "license": "CC BY 4.0",
        },
    },
    "ocpa_running_example": {
        "pair": "ocpa",
        "filename": "ocpa-running-example.sqlite",
        "source_url": (
            "https://raw.githubusercontent.com/ocpm/ocpa/"
            "de056e0203a3fa4a9bbc19a95e001eada323074a/"
            "sample_logs/ocel2/sqlite/running-example.sqlite"
        ),
        "sqlite_sha256": (
            "019202ee793cbd71c80636ca10d78f9701e83f3696ca818c52cc76f38d2bd38d"
        ),
        "source": {
            "title": "OCPA OCEL 2.0 running example",
            "identifier": (
                "ocpm/ocpa@de056e0203a3fa4a9bbc19a95e001eada323074a:"
                "sample_logs/ocel2/sqlite/running-example.sqlite"
            ),
            "upstream_project": "OCPA documented OCEL 2.0 import example",
            "upstream_revision": "de056e0203a3fa4a9bbc19a95e001eada323074a",
            "license": (
                "OCPA 1.3.4 wheel contains GPL-3.0; dataset-specific terms are "
                "not stated separately"
            ),
        },
    },
}

OBJECT_TYPE_SELECTION = (
    "maximum count of object lifecycles containing at least two events; "
    "then maximum event-object link count; then lexical object-type name"
)

WORKLOADS = (
    "dfg_conformance_95pct",
    "variant_conformance_95pct",
    "next_activity_prediction",
    "edge_bottleneck_ranking",
)


@dataclass(frozen=True)
class Fixture:
    dataset_name: str
    baseline_dataset_id: int
    ocpm_dataset_id: int
    tenant_id: int
    object_type: str
    from_time: datetime
    train_to: datetime
    test_from: datetime
    to_time: datetime
    cases: int
    train_cases: int
    test_cases: int

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "Fixture":
        restored = dict(value)
        for key in ("from_time", "train_to", "test_from", "to_time"):
            restored[key] = datetime.fromisoformat(restored[key])
        return cls(**restored)

    def to_dict(self) -> dict[str, Any]:
        result = dict(self.__dict__)
        for key in ("from_time", "train_to", "test_from", "to_time"):
            result[key] = result[key].isoformat()
        return result


def canonical(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )


def answer_sha256(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def canonical_variant(value: tuple[str, ...] | list[str]) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def dfg_score_from_counts(
    train: dict[tuple[str, str], int], test: dict[tuple[str, str], int]
) -> dict[str, Any]:
    keys = sorted(set(train) | set(test))
    ranked = sorted(keys, key=lambda key: (-int(train.get(key, 0)), key))
    target_frequency = math.ceil(sum(train.values()) * 0.95)
    covered = 0
    model = []
    for key in ranked:
        if covered >= target_frequency:
            break
        count = int(train.get(key, 0))
        if count:
            model.append((*key, "directly_follows"))
            covered += count
    accepted = {(source, target) for source, target, _ in model}
    test_total = sum(test.values())
    conforming = sum(count for key, count in test.items() if key in accepted)
    return {
        "fitness": round(conforming / test_total if test_total else 1.0, 12),
        "conforming": int(conforming),
        "deviations": int(test_total - conforming),
        "test_total": int(test_total),
        "model": sorted([list(item) for item in model]),
    }


def next_score_from_counts(
    train: dict[tuple[str, str], int], test: dict[tuple[str, str], int]
) -> dict[str, Any]:
    winners: dict[str, tuple[str, int]] = {}
    for (source, target), count in train.items():
        if count <= 0:
            continue
        current = winners.get(source)
        candidate = (target, int(count))
        if (
            current is None
            or candidate[1] > current[1]
            or (candidate[1] == current[1] and candidate[0] < current[0])
        ):
            winners[source] = candidate
    test_total = sum(test.values())
    correct = sum(
        count
        for (source, target), count in test.items()
        if source in winners and winners[source][0] == target
    )
    return {
        "accuracy": round(correct / test_total if test_total else 1.0, 12),
        "correct": int(correct),
        "test_total": int(test_total),
        "predictions": sorted(
            [
                [source, "directly_follows", target]
                for source, (target, _) in winners.items()
            ]
        ),
    }


def variant_score_from_counts(
    train: dict[str, int], test: dict[str, int]
) -> dict[str, Any]:
    ranked = sorted(train, key=lambda variant: (-train[variant], variant))
    target_frequency = math.ceil(sum(train.values()) * 0.95)
    covered = 0
    model = []
    for variant in ranked:
        if covered >= target_frequency:
            break
        count = int(train[variant])
        if count:
            model.append(variant)
            covered += count
    accepted = set(model)
    test_total = sum(test.values())
    conforming = sum(count for key, count in test.items() if key in accepted)
    return {
        "fitness": round(conforming / test_total if test_total else 1.0, 12),
        "conforming": int(conforming),
        "deviations": int(test_total - conforming),
        "test_total": int(test_total),
        "model": sorted(model),
    }


def score_lifecycles(
    lifecycles: list[list[tuple[str, datetime]]],
    fixture: Fixture,
    workload: str,
) -> dict[str, Any]:
    """Score fully contained object lifecycles under the fixed SAP contract."""

    train_dfg: Counter[tuple[str, str]] = Counter()
    test_dfg: Counter[tuple[str, str]] = Counter()
    train_variants: Counter[str] = Counter()
    test_variants: Counter[str] = Counter()
    all_durations: dict[tuple[str, str], list[float]] = {}
    selected_cases = 0
    selected_events = 0

    for events in lifecycles:
        if not events:
            continue
        start_time = events[0][1]
        end_time = events[-1][1]
        in_train = start_time >= fixture.from_time and end_time <= fixture.train_to
        in_test = start_time >= fixture.test_from and end_time <= fixture.to_time
        in_full = start_time >= fixture.from_time and end_time <= fixture.to_time
        if workload == "edge_bottleneck_ranking":
            if not in_full:
                continue
        elif not (in_train or in_test):
            continue

        selected_cases += 1
        selected_events += len(events)
        activities = [activity for activity, _timestamp in events]
        if workload == "variant_conformance_95pct":
            variant = canonical_variant(activities)
            if in_train:
                train_variants[variant] += 1
            elif in_test:
                test_variants[variant] += 1
            continue

        for (source, source_time), (target, target_time) in zip(events, events[1:]):
            key = (source, target)
            if workload == "edge_bottleneck_ranking":
                all_durations.setdefault(key, []).append(
                    (target_time - source_time).total_seconds()
                )
            elif in_train:
                train_dfg[key] += 1
            elif in_test:
                test_dfg[key] += 1

    if workload == "dfg_conformance_95pct":
        answer = dfg_score_from_counts(dict(train_dfg), dict(test_dfg))
        aggregate_rows = len(set(train_dfg) | set(test_dfg))
    elif workload == "next_activity_prediction":
        answer = next_score_from_counts(dict(train_dfg), dict(test_dfg))
        aggregate_rows = len(set(train_dfg) | set(test_dfg))
    elif workload == "variant_conformance_95pct":
        answer = variant_score_from_counts(dict(train_variants), dict(test_variants))
        aggregate_rows = len(set(train_variants) | set(test_variants))
    elif workload == "edge_bottleneck_ranking":
        answer = [
            [source, target, len(values), round(statistics.fmean(values), 6)]
            for (source, target), values in all_durations.items()
        ]
        answer.sort(key=lambda row: (-row[3], -row[2], row[0], row[1]))
        aggregate_rows = len(answer)
    else:
        raise ValueError(f"unknown workload: {workload}")

    return {
        "answer": answer,
        "input": {
            "selected_cases": selected_cases,
            "event_rows": selected_events,
            "aggregate_rows": aggregate_rows,
        },
    }


def percentile(samples: list[int], percentile_value: float) -> int:
    ordered = sorted(samples)
    index = min(len(ordered) - 1, math.ceil(len(ordered) * percentile_value) - 1)
    return ordered[index]


def latency_metrics(samples_ns: list[int]) -> dict[str, Any]:
    if not samples_ns:
        raise ValueError("latency metrics require a sample")
    ordered = sorted(samples_ns)
    return {
        "p50_ms": round(statistics.median(ordered) / 1_000_000, 3),
        "p95_ms": round(percentile(ordered, 0.95) / 1_000_000, 3),
        "minimum_ms": round(ordered[0] / 1_000_000, 3),
        "maximum_ms": round(ordered[-1] / 1_000_000, 3),
        "runs": len(ordered),
    }


def measure_serial(
    call: Callable[[str], dict[str, Any]],
    *,
    warmups: int,
    runs: int,
    epochs: int,
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for workload in WORKLOADS:
        preflight = call(workload)
        expected = canonical(preflight["answer"])
        for _ in range(warmups):
            measured = call(workload)
            if canonical(measured["answer"]) != expected:
                raise AssertionError(f"{workload}: warmup answer changed")
        all_samples: list[int] = []
        epoch_results = []
        for epoch in range(epochs):
            samples = []
            for _ in range(runs):
                started_ns = time.perf_counter_ns()
                measured = call(workload)
                elapsed_ns = time.perf_counter_ns() - started_ns
                if canonical(measured["answer"]) != expected:
                    raise AssertionError(f"{workload}: measured answer changed")
                samples.append(elapsed_ns)
            all_samples.extend(samples)
            epoch_results.append(
                {
                    "epoch": epoch + 1,
                    **latency_metrics(samples),
                    "samples_ns": samples,
                }
            )
        epoch_p95 = [epoch["p95_ms"] for epoch in epoch_results]
        result[workload] = {
            "correct_within_arm": True,
            "answer": preflight["answer"],
            "answer_sha256": answer_sha256(preflight["answer"]),
            "input": preflight["input"],
            **latency_metrics(all_samples),
            "epoch_count": epochs,
            "epoch_p95_median_ms": round(statistics.median(epoch_p95), 3),
            "epoch_p95_range_ms": [min(epoch_p95), max(epoch_p95)],
            "epochs": epoch_results,
        }
    return result
