"""Three-way PM benchmark on the official SAP O2C and P2P OCEL 2.0 logs."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import multiprocessing
import os
import platform
import random
import re
import statistics
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import Any

import psutil
import psycopg2

DATASETS = {
    "sap_o2c": {"object_type": "MATERIAL"},
    "sap_p2p": {"object_type": "EBELN_EBELP"},
}

WORKLOADS = (
    "dfg_conformance_95pct",
    "variant_conformance_95pct",
    "next_activity_prediction",
    "edge_bottleneck_ranking",
)

ENGINES = (
    "vanilla_pg_pm4py",
    "pg_ocpm_pm4py",
    "pg_ocpm_ocpm_engine",
)

_PM4PY = None


def pm4py_module():
    """Import PM4Py despite its PID-zero container parent-name probe."""

    global _PM4PY
    if _PM4PY is not None:
        return _PM4PY
    original_process = psutil.Process
    if os.getppid() == 0:
        psutil.Process = lambda pid=None: original_process(
            os.getpid() if pid == 0 else pid
        )
    try:
        import pm4py
    finally:
        psutil.Process = original_process
    _PM4PY = pm4py
    return _PM4PY


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-host", default="postgres_vanilla")
    parser.add_argument("--extension-host", default="postgres_ocpm")
    parser.add_argument("--database", default="ocel_benchmark")
    parser.add_argument("--datasets", default=",".join(DATASETS))
    parser.add_argument("--train-fraction", type=float, default=0.8)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--seed", type=int, default=20260718)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--concurrency", default="1,2,4,8")
    parser.add_argument("--concurrency-requests", type=int, default=16)
    parser.add_argument(
        "--output",
        default=".benchmarks/sap-pm4py-three-way.json",
    )
    parser.add_argument(
        "--report",
        default=".benchmarks/sap-pm4py-three-way.md",
    )
    parser.add_argument("--memory-worker", choices=ENGINES)
    parser.add_argument("--memory-dataset", choices=tuple(DATASETS))
    parser.add_argument("--memory-workload", choices=WORKLOADS)
    args = parser.parse_args()
    args.datasets = [value for value in args.datasets.split(",") if value]
    unknown = sorted(set(args.datasets) - set(DATASETS))
    if unknown:
        parser.error(f"unknown datasets: {', '.join(unknown)}")
    if not 0 < args.train_fraction < 1:
        parser.error("--train-fraction must be between zero and one")
    if args.warmups < 0 or args.runs < 1:
        parser.error("--warmups must be nonnegative and --runs must be positive")
    if args.memory_worker and not (args.memory_dataset and args.memory_workload):
        parser.error("--memory-worker requires dataset and workload")
    return args


def connect(host: str, database: str, timeout_seconds: int):
    connection = psycopg2.connect(
        host=host,
        port=5432,
        dbname=database,
        user="postgres",
        password="pg",
        connect_timeout=15,
    )
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute("SET jit=off")
        cursor.execute("SET statement_timeout=%s", (timeout_seconds * 1000,))
    return connection


FIXTURE_SQL = """
WITH selected_dataset AS (
    SELECT dataset_id,tenant_id
    FROM ocpm.dataset
    WHERE dataset_name=%(dataset_name)s
), candidate_cases AS MATERIALIZED (
    SELECT item.case_id,item.start_time,item.end_time
    FROM selected_dataset dataset
    JOIN ocpm.case_bucket bucket
      ON bucket.dataset_id=dataset.dataset_id
     AND bucket.tenant_id=dataset.tenant_id
    CROSS JOIN LATERAL unnest(
        bucket.case_ids,bucket.start_times,bucket.end_times
    ) AS item(case_id,start_time,end_time)
    WHERE bucket.object_type=%(object_type)s
), split AS (
    SELECT percentile_disc(%(train_fraction)s)
           WITHIN GROUP (ORDER BY end_time) AS train_to
    FROM candidate_cases
)
SELECT dataset.dataset_id,dataset.tenant_id,
       min(candidate.start_time)-interval '1 microsecond' AS from_time,
       split.train_to,
       split.train_to+interval '1 microsecond' AS test_from,
       max(candidate.end_time)+interval '1 microsecond' AS to_time,
       count(*)::bigint AS cases,
       count(*) FILTER (WHERE candidate.end_time<=split.train_to)::bigint
         AS train_cases,
       count(*) FILTER (WHERE candidate.start_time>split.train_to)::bigint
         AS test_cases
FROM selected_dataset dataset
CROSS JOIN split
CROSS JOIN candidate_cases candidate
GROUP BY dataset.dataset_id,dataset.tenant_id,split.train_to
"""


def query_rows(connection, sql: str, params: dict[str, Any]) -> list[tuple]:
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall()


def discover_fixture(
    extension_connection,
    baseline_connection,
    args: argparse.Namespace,
    dataset_name: str,
) -> Fixture:
    object_type = DATASETS[dataset_name]["object_type"]
    row = query_rows(
        extension_connection,
        FIXTURE_SQL,
        {
            "dataset_name": dataset_name,
            "object_type": object_type,
            "train_fraction": args.train_fraction,
        },
    )[0]
    baseline_row = query_rows(
        baseline_connection,
        "SELECT dataset_id FROM ocel.dataset WHERE dataset_name=%s",
        (dataset_name,),
    )
    if not baseline_row:
        raise SystemExit(f"missing baseline dataset {dataset_name}")
    if int(row[7]) == 0 or int(row[8]) == 0:
        raise SystemExit(f"{dataset_name}: empty train or test partition")
    return Fixture(
        dataset_name=dataset_name,
        baseline_dataset_id=int(baseline_row[0][0]),
        ocpm_dataset_id=int(row[0]),
        tenant_id=int(row[1]),
        object_type=object_type,
        from_time=row[2],
        train_to=row[3],
        test_from=row[4],
        to_time=row[5],
        cases=int(row[6]),
        train_cases=int(row[7]),
        test_cases=int(row[8]),
    )


VANILLA_EVENTS_SQL = """
WITH selected_cases AS MATERIALIZED (
    SELECT relation.object_key AS case_id
    FROM ocel.event_object relation
    JOIN ocel.event event
      ON event.dataset_id=relation.dataset_id
     AND event.event_key=relation.event_key
    WHERE relation.dataset_id=%(baseline_dataset_id)s
      AND relation.object_type=%(object_type)s
    GROUP BY relation.object_key
    HAVING min(event.event_timestamp) >= %(from_time)s
       AND max(event.event_timestamp) <= %(to_time)s
)
SELECT relation.object_key::text,event.activity,event.event_timestamp
FROM ocel.event_object relation
JOIN selected_cases selected ON selected.case_id=relation.object_key
JOIN ocel.event event
  ON event.dataset_id=relation.dataset_id
 AND event.event_key=relation.event_key
WHERE relation.dataset_id=%(baseline_dataset_id)s
  AND relation.object_type=%(object_type)s
ORDER BY relation.object_key,event.event_timestamp,event.event_key
"""


PG_OCPM_EVENTS_SQL = """
WITH selected_cases AS MATERIALIZED (
    SELECT item.case_id
    FROM ocpm.case_bucket bucket
    CROSS JOIN LATERAL unnest(
        bucket.case_ids,bucket.start_times,bucket.end_times
    ) AS item(case_id,start_time,end_time)
    WHERE bucket.dataset_id=%(ocpm_dataset_id)s
      AND bucket.tenant_id=%(tenant_id)s
      AND bucket.object_type=%(object_type)s
      AND item.start_time >= %(from_time)s
      AND item.end_time <= %(to_time)s
)
SELECT event.case_id::text,event.activity,event.event_timestamp
FROM ocpm.event_chunk chunk
CROSS JOIN LATERAL unnest(
    chunk.case_ids,chunk.activities,chunk.event_timestamps,chunk.event_sequences
) WITH ORDINALITY AS event(
    case_id,activity,event_timestamp,event_sequence,position
)
JOIN selected_cases selected ON selected.case_id=event.case_id
WHERE chunk.dataset_id=%(ocpm_dataset_id)s
  AND chunk.tenant_id=%(tenant_id)s
  AND chunk.object_type=%(object_type)s
ORDER BY event.case_id,event.event_timestamp,
         event.event_sequence,event.position
"""


DFG_COUNTS_SQL = """
WITH selected_cases AS MATERIALIZED (
    SELECT item.case_id,
           CASE
             WHEN item.start_time >= %(from_time)s
              AND item.end_time <= %(train_to)s THEN 1
             WHEN item.start_time >= %(test_from)s
              AND item.end_time <= %(to_time)s THEN 2
           END AS partition
    FROM ocpm.case_bucket bucket
    CROSS JOIN LATERAL unnest(
        bucket.case_ids,bucket.start_times,bucket.end_times
    ) AS item(case_id,start_time,end_time)
    WHERE bucket.dataset_id=%(ocpm_dataset_id)s
      AND bucket.tenant_id=%(tenant_id)s
      AND bucket.object_type=%(object_type)s
      AND ((item.start_time >= %(from_time)s AND item.end_time <= %(train_to)s)
        OR (item.start_time >= %(test_from)s AND item.end_time <= %(to_time)s))
), grouped AS (
    SELECT bucket.source_activity,bucket.target_activity,bucket.edge_type,
           count(*) FILTER (WHERE selected.partition=1)::bigint AS train_count,
           count(*) FILTER (WHERE selected.partition=2)::bigint AS test_count
    FROM ocpm.edge_bucket bucket
    CROSS JOIN LATERAL unnest(bucket.case_ids) AS edge(case_id)
    JOIN selected_cases selected ON selected.case_id=edge.case_id
    WHERE bucket.dataset_id=%(ocpm_dataset_id)s
      AND bucket.tenant_id=%(tenant_id)s
      AND bucket.source_object_type=%(object_type)s
      AND bucket.target_object_type=%(object_type)s
      AND bucket.edge_type='directly_follows'
    GROUP BY bucket.source_activity,bucket.target_activity,bucket.edge_type
)
SELECT source_activity,target_activity,edge_type,train_count,test_count
FROM grouped
WHERE train_count > 0 OR test_count > 0
ORDER BY source_activity,target_activity,edge_type
"""


VARIANT_COUNTS_SQL = """
WITH grouped AS (
    SELECT bucket.activity_path::text AS variant,
           ocpm.window_cardinalities(
               bucket.start_times,bucket.end_times,
               ARRAY[%(from_time)s,%(test_from)s]::timestamptz[],
               ARRAY[%(train_to)s,%(to_time)s]::timestamptz[]
           ) AS frequencies
    FROM ocpm.case_bucket bucket
    WHERE bucket.dataset_id=%(ocpm_dataset_id)s
      AND bucket.tenant_id=%(tenant_id)s
      AND bucket.object_type=%(object_type)s
    GROUP BY bucket.activity_path
)
SELECT variant,frequencies[1]::bigint,frequencies[2]::bigint
FROM grouped
WHERE frequencies[1] > 0 OR frequencies[2] > 0
ORDER BY variant
"""


EDGE_FEATURE_SQL = """
WITH grouped AS (
    SELECT bucket.source_activity,bucket.target_activity,
           ocpm.duration_stats_window(
               bucket.source_timestamps,bucket.target_timestamps,
               bucket.execution_times,%(from_time)s,%(to_time)s
           ) AS stats
    FROM ocpm.edge_bucket bucket
    WHERE bucket.dataset_id=%(ocpm_dataset_id)s
      AND bucket.tenant_id=%(tenant_id)s
      AND bucket.source_object_type=%(object_type)s
      AND bucket.target_object_type=%(object_type)s
      AND bucket.edge_type='directly_follows'
      AND bucket.min_source_timestamp<=%(to_time)s
      AND bucket.max_target_timestamp>=%(from_time)s
    GROUP BY bucket.source_activity,bucket.target_activity
)
SELECT source_activity,target_activity,stats[1]::bigint,stats[2]
FROM grouped
WHERE stats[1] > 0
ORDER BY source_activity,target_activity
"""


def fixture_params(fixture: Fixture) -> dict[str, Any]:
    return asdict(fixture)


def canonical_variant(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def load_pm4py_log(
    connection,
    fixture: Fixture,
    from_time: datetime,
    to_time: datetime,
    source: str,
):
    import pandas as pd

    pm4py = pm4py_module()
    if source not in ("vanilla_pg", "pg_ocpm"):
        raise ValueError(source)
    rows = query_rows(
        connection,
        VANILLA_EVENTS_SQL if source == "vanilla_pg" else PG_OCPM_EVENTS_SQL,
        {
            **fixture_params(fixture),
            "from_time": from_time,
            "to_time": to_time,
        },
    )
    frame = pd.DataFrame(
        rows,
        columns=("case:concept:name", "concept:name", "time:timestamp"),
    )
    frame = pm4py.format_dataframe(
        frame,
        case_id="case:concept:name",
        activity_key="concept:name",
        timestamp_key="time:timestamp",
    )
    return frame, {
        "source": source,
        "event_rows": len(frame),
        "dataframe_bytes": int(frame.memory_usage(index=True, deep=True).sum()),
    }


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


def pm4py_pair_counts(connection, fixture: Fixture, source: str):
    pm4py = pm4py_module()
    train_log, train_meta = load_pm4py_log(
        connection, fixture, fixture.from_time, fixture.train_to, source
    )
    test_log, test_meta = load_pm4py_log(
        connection, fixture, fixture.test_from, fixture.to_time, source
    )
    train, _, _ = pm4py.discover_dfg(train_log)
    test, _, _ = pm4py.discover_dfg(test_log)
    return (
        train,
        test,
        {
            "source": source,
            "event_rows": train_meta["event_rows"] + test_meta["event_rows"],
            "dataframe_bytes": (
                train_meta["dataframe_bytes"] + test_meta["dataframe_bytes"]
            ),
            "aggregate_rows": len(set(train) | set(test)),
        },
    )


def run_pm4py(
    connection, fixture: Fixture, workload: str, source: str
) -> dict[str, Any]:
    pm4py = pm4py_module()
    if workload in ("dfg_conformance_95pct", "next_activity_prediction"):
        train, test, telemetry = pm4py_pair_counts(connection, fixture, source)
        answer = (
            dfg_score_from_counts(train, test)
            if workload == "dfg_conformance_95pct"
            else next_score_from_counts(train, test)
        )
        return {"answer": answer, "input": telemetry}

    if workload == "variant_conformance_95pct":
        train_log, train_meta = load_pm4py_log(
            connection, fixture, fixture.from_time, fixture.train_to, source
        )
        test_log, test_meta = load_pm4py_log(
            connection, fixture, fixture.test_from, fixture.to_time, source
        )
        train_raw = pm4py.get_variants_as_tuples(train_log)
        test_raw = pm4py.get_variants_as_tuples(test_log)
        train = {
            canonical_variant(list(variant)): int(count)
            for variant, count in train_raw.items()
        }
        test = {
            canonical_variant(list(variant)): int(count)
            for variant, count in test_raw.items()
        }
        return {
            "answer": variant_score_from_counts(train, test),
            "input": {
                "source": source,
                "event_rows": train_meta["event_rows"] + test_meta["event_rows"],
                "dataframe_bytes": train_meta["dataframe_bytes"]
                + test_meta["dataframe_bytes"],
                "aggregate_rows": len(set(train) | set(test)),
            },
        }

    if workload == "edge_bottleneck_ranking":
        log, telemetry = load_pm4py_log(
            connection, fixture, fixture.from_time, fixture.to_time, source
        )
        frequencies, _, _ = pm4py.discover_dfg(log)
        performance, _, _ = pm4py.discover_performance_dfg(
            log, perf_aggregation_key="mean"
        )
        rows = [
            [
                source_activity,
                target_activity,
                int(frequency),
                round(float(performance[key]), 6),
            ]
            for key, frequency in frequencies.items()
            for source_activity, target_activity in [key]
        ]
        rows.sort(key=lambda row: (-row[3], -row[2], row[0], row[1]))
        return {
            "answer": rows,
            "input": {**telemetry, "aggregate_rows": len(rows)},
        }

    raise ValueError(workload)


def run_ocpm_engine(connection, fixture: Fixture, workload: str) -> dict[str, Any]:
    from ocpm_engine import (
        TransitionCount,
        bottleneck_order,
        dfg_conformance,
        next_activity,
        variant_conformance,
    )

    params = fixture_params(fixture)
    if workload in ("dfg_conformance_95pct", "next_activity_prediction"):
        rows = query_rows(connection, DFG_COUNTS_SQL, params)
        transitions = [
            TransitionCount(
                str(row[0]), str(row[1]), str(row[2]), int(row[3]), int(row[4])
            )
            for row in rows
        ]
        if workload == "dfg_conformance_95pct":
            score = dfg_conformance(transitions)
            answer = {
                "fitness": round(score.fitness, 12),
                "conforming": score.conforming,
                "deviations": score.deviations,
                "test_total": score.test_total,
                "model": sorted([list(item) for item in score.model]),
            }
        else:
            score = next_activity(transitions)
            answer = {
                "accuracy": round(score.accuracy, 12),
                "correct": score.correct,
                "test_total": score.test_total,
                "predictions": sorted([list(item) for item in score.predictions]),
            }
        return {"answer": answer, "input": {"aggregate_rows": len(rows)}}

    if workload == "variant_conformance_95pct":
        rows = query_rows(connection, VARIANT_COUNTS_SQL, params)
        variants = [canonical_variant(json.loads(row[0])) for row in rows]
        train = [int(row[1]) for row in rows]
        test = [int(row[2]) for row in rows]
        fitness, conforming, deviations, total, model = variant_conformance(
            variants, train, test
        )
        return {
            "answer": {
                "fitness": round(fitness, 12),
                "conforming": conforming,
                "deviations": deviations,
                "test_total": total,
                "model": sorted(model),
            },
            "input": {"aggregate_rows": len(rows)},
        }

    if workload == "edge_bottleneck_ranking":
        rows = query_rows(connection, EDGE_FEATURE_SQL, params)
        frequencies = [int(row[2]) for row in rows]
        durations = [float(row[3]) for row in rows]
        order = bottleneck_order(frequencies, durations)
        answer = [
            [
                str(rows[index][0]),
                str(rows[index][1]),
                frequencies[index],
                round(durations[index], 6),
            ]
            for index in order
        ]
        return {"answer": answer, "input": {"aggregate_rows": len(rows)}}

    raise ValueError(workload)


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def percentile(samples: list[float], percentile_value: float) -> float:
    ordered = sorted(samples)
    index = min(len(ordered) - 1, math.ceil(len(ordered) * percentile_value) - 1)
    return ordered[index]


def metrics(samples: list[float]) -> dict[str, Any]:
    return {
        "p50_ms": round(statistics.median(samples) * 1000, 3),
        "p95_ms": round(percentile(samples, 0.95) * 1000, 3),
        "minimum_ms": round(min(samples) * 1000, 3),
        "runs": len(samples),
    }


def timed_comparison(
    calls: dict[str, Callable[[], dict[str, Any]]],
    warmups: int,
    runs: int,
    rng: random.Random,
) -> dict[str, Any]:
    if tuple(calls) != ENGINES:
        raise ValueError("calls must follow ENGINES ordering")
    answers: dict[str, dict[str, Any]] = {}
    for _ in range(warmups):
        order = list(calls)
        rng.shuffle(order)
        for name in order:
            answers[name] = calls[name]()
    samples = {name: [] for name in calls}
    first_counts = {name: 0 for name in calls}
    for _ in range(runs):
        order = list(calls)
        rng.shuffle(order)
        first_counts[order[0]] += 1
        for name in order:
            started = time.perf_counter()
            answers[name] = calls[name]()
            samples[name].append(time.perf_counter() - started)
    canonical_answers = {
        name: canonical(result["answer"]) for name, result in answers.items()
    }
    expected = canonical_answers["pg_ocpm_ocpm_engine"]
    mismatches = {
        name: answer for name, answer in canonical_answers.items() if answer != expected
    }
    if mismatches:
        detail = "\n".join(
            f"{name}_sha256={hashlib.sha256(answer.encode()).hexdigest()}"
            for name, answer in mismatches.items()
        )
        raise AssertionError(
            "three-way correctness mismatch\n"
            "pg_ocpm_ocpm_engine_sha256="
            f"{hashlib.sha256(expected.encode()).hexdigest()}\n{detail}"
        )
    measured = {name: metrics(values) for name, values in samples.items()}
    vanilla = measured["vanilla_pg_pm4py"]["p50_ms"]
    pg_pm4py = measured["pg_ocpm_pm4py"]["p50_ms"]
    engine = measured["pg_ocpm_ocpm_engine"]["p50_ms"]
    return {
        "correct": True,
        **{
            name: {**measured[name], "input": answers[name]["input"]}
            for name in ENGINES
        },
        "speedups": {
            "pg_ocpm_pm4py_vs_vanilla": round(vanilla / max(pg_pm4py, 0.000001), 3),
            "pg_ocpm_ocpm_engine_vs_vanilla": round(vanilla / max(engine, 0.000001), 3),
            "pg_ocpm_ocpm_engine_vs_pg_ocpm_pm4py": round(
                pg_pm4py / max(engine, 0.000001), 3
            ),
        },
        "first_execution_counts": first_counts,
        "answer_sha256": hashlib.sha256(expected.encode()).hexdigest(),
    }


def serialize_fixture(fixture: Fixture) -> dict[str, Any]:
    result = asdict(fixture)
    for key in ("from_time", "train_to", "test_from", "to_time"):
        result[key] = result[key].isoformat()
    return result


def restore_fixture(payload: dict[str, Any]) -> Fixture:
    restored = dict(payload)
    for key in ("from_time", "train_to", "test_from", "to_time"):
        restored[key] = datetime.fromisoformat(restored[key])
    return Fixture(**restored)


_CONCURRENCY_STATE: dict[str, Any] = {}


def concurrency_initializer(
    engine: str,
    host: str,
    database: str,
    timeout_seconds: int,
    fixture: dict[str, Any],
) -> None:
    _CONCURRENCY_STATE.update(
        {
            "engine": engine,
            "connection": connect(host, database, timeout_seconds),
            "fixture": restore_fixture(fixture),
        }
    )


def concurrency_task(_: int) -> tuple[float, str]:
    engine = _CONCURRENCY_STATE["engine"]
    connection = _CONCURRENCY_STATE["connection"]
    fixture = _CONCURRENCY_STATE["fixture"]
    started = time.perf_counter()
    if engine == "vanilla_pg_pm4py":
        result = run_pm4py(connection, fixture, "dfg_conformance_95pct", "vanilla_pg")
    elif engine == "pg_ocpm_pm4py":
        result = run_pm4py(connection, fixture, "dfg_conformance_95pct", "pg_ocpm")
    else:
        result = run_ocpm_engine(connection, fixture, "dfg_conformance_95pct")
    return time.perf_counter() - started, canonical(result["answer"])


def concurrency_sweep(
    args: argparse.Namespace,
    fixture: Fixture,
    engine: str,
    expected: str,
) -> dict[str, Any]:
    results = {}
    context = multiprocessing.get_context("spawn")
    host = args.baseline_host if engine == "vanilla_pg_pm4py" else args.extension_host
    for workers in [int(value) for value in args.concurrency.split(",") if value]:
        requests = max(args.concurrency_requests, workers * 2)
        with ProcessPoolExecutor(
            max_workers=workers,
            mp_context=context,
            initializer=concurrency_initializer,
            initargs=(
                engine,
                host,
                args.database,
                args.timeout_seconds,
                serialize_fixture(fixture),
            ),
        ) as pool:
            warm = list(pool.map(concurrency_task, range(workers), chunksize=1))
            if any(answer != expected for _, answer in warm):
                raise AssertionError("concurrency warmup correctness mismatch")
            started = time.perf_counter()
            samples = list(pool.map(concurrency_task, range(requests), chunksize=1))
            wall = time.perf_counter() - started
        if any(answer != expected for _, answer in samples):
            raise AssertionError("concurrency correctness mismatch")
        latencies = [elapsed for elapsed, _ in samples]
        results[str(workers)] = {
            **metrics(latencies),
            "requests": requests,
            "wall_ms": round(wall * 1000, 3),
            "throughput_qps": round(requests / wall, 3),
            "correct": True,
        }
        print(
            f"  concurrency {fixture.dataset_name} {engine} x{workers}: "
            f"{results[str(workers)]['throughput_qps']:.2f} req/s",
            flush=True,
        )
    return results


def memory_worker(args: argparse.Namespace) -> None:
    extension = connect(args.extension_host, args.database, args.timeout_seconds)
    baseline = connect(args.baseline_host, args.database, args.timeout_seconds)
    fixture = discover_fixture(extension, baseline, args, args.memory_dataset)
    extension.close()
    baseline.close()
    host = (
        args.baseline_host
        if args.memory_worker == "vanilla_pg_pm4py"
        else args.extension_host
    )
    connection = connect(host, args.database, args.timeout_seconds)
    process = psutil.Process()
    baseline_rss = int(process.memory_info().rss)
    peak = baseline_rss
    stop = threading.Event()

    def sample_rss() -> None:
        nonlocal peak
        while not stop.wait(0.001):
            peak = max(peak, int(process.memory_info().rss))

    monitor = threading.Thread(target=sample_rss, daemon=True)
    monitor.start()
    started = time.perf_counter()
    try:
        if args.memory_worker == "vanilla_pg_pm4py":
            result = run_pm4py(connection, fixture, args.memory_workload, "vanilla_pg")
        elif args.memory_worker == "pg_ocpm_pm4py":
            result = run_pm4py(connection, fixture, args.memory_workload, "pg_ocpm")
        else:
            result = run_ocpm_engine(connection, fixture, args.memory_workload)
    finally:
        elapsed = time.perf_counter() - started
        peak = max(peak, int(process.memory_info().rss))
        stop.set()
        monitor.join()
    payload = {
        "engine": args.memory_worker,
        "dataset": args.memory_dataset,
        "workload": args.memory_workload,
        "baseline_rss_bytes": baseline_rss,
        "peak_rss_bytes": peak,
        "incremental_peak_bytes": max(0, peak - baseline_rss),
        "elapsed_ms": round(elapsed * 1000, 3),
        "input": result["input"],
        "answer_sha256": hashlib.sha256(
            canonical(result["answer"]).encode()
        ).hexdigest(),
    }
    print("MEMORY_RESULT=" + json.dumps(payload, default=str), flush=True)
    connection.close()


def measure_memory(
    args: argparse.Namespace, engine: str, dataset: str, workload: str
) -> dict[str, Any]:
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--baseline-host",
        args.baseline_host,
        "--extension-host",
        args.extension_host,
        "--database",
        args.database,
        "--train-fraction",
        str(args.train_fraction),
        "--timeout-seconds",
        str(args.timeout_seconds),
        "--memory-worker",
        engine,
        "--memory-dataset",
        dataset,
        "--memory-workload",
        workload,
    ]
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        timeout=args.timeout_seconds * 2,
    )
    for line in reversed(completed.stdout.splitlines()):
        if line.startswith("MEMORY_RESULT="):
            return json.loads(line.split("=", 1)[1])
    raise RuntimeError("memory worker did not emit MEMORY_RESULT")


def schema_storage(connection, schema: str) -> dict[str, Any]:
    sizes = query_rows(
        connection,
        """
        SELECT coalesce(sum(pg_relation_size(class.oid)),0)::bigint,
               coalesce(sum(pg_indexes_size(class.oid)),0)::bigint,
               coalesce(sum(CASE WHEN class.reltoastrelid=0 THEN 0
                    ELSE pg_total_relation_size(class.reltoastrelid) END),0)::bigint,
               coalesce(sum(pg_total_relation_size(class.oid)),0)::bigint
        FROM pg_class class
        JOIN pg_namespace namespace ON namespace.oid=class.relnamespace
        WHERE namespace.nspname=%s
          AND class.relkind IN ('r','m','p')
          AND class.relname NOT IN ('dataset','result_cache')
        """,
        (schema,),
    )[0]
    indexes = query_rows(
        connection,
        """
        SELECT tablename,indexname,indexdef,
               pg_relation_size((schemaname||'.'||indexname)::regclass)::bigint
        FROM pg_indexes
        WHERE schemaname=%s
        ORDER BY tablename,indexname
        """,
        (schema,),
    )
    return {
        **dict(
            zip(
                ("heap_bytes", "index_bytes", "toast_bytes", "total_bytes"),
                map(int, sizes),
            )
        ),
        "indexes": [
            {
                "table": str(table),
                "name": str(name),
                "definition": str(definition),
                "bytes": int(size),
            }
            for table, name, definition, size in indexes
        ],
    }


def distribution_size(name: str) -> int:
    distribution = metadata.distribution(name)
    total = 0
    for file in distribution.files or ():
        path = distribution.locate_file(file)
        try:
            if path.is_file():
                total += path.stat().st_size
        except OSError:
            continue
    return total


def requirement_name(requirement: str) -> str | None:
    match = re.match(r"\s*([A-Za-z0-9_.-]+)", requirement)
    return match.group(1) if match else None


def dependency_closure(root: str) -> list[str]:
    pending = [root]
    seen = set()
    while pending:
        name = pending.pop()
        normalized = re.sub(r"[-_.]+", "-", name).lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        try:
            distribution = metadata.distribution(name)
        except metadata.PackageNotFoundError:
            continue
        for requirement in distribution.requires or ():
            if "extra ==" in requirement:
                continue
            dependency = requirement_name(requirement)
            if dependency:
                pending.append(dependency)
    return sorted(seen)


def package_storage() -> dict[str, Any]:
    result = {}
    for engine, distribution in (
        ("pm4py", "pm4py"),
        ("ocpm_engine", "ocpm-engine"),
    ):
        closure = dependency_closure(distribution)
        closure_bytes = 0
        for dependency in closure:
            try:
                closure_bytes += distribution_size(dependency)
            except metadata.PackageNotFoundError:
                continue
        package = metadata.distribution(distribution)
        result[engine] = {
            "distribution": distribution,
            "version": package.version,
            "license_expression": package.metadata.get("License-Expression")
            or package.metadata.get("License"),
            "package_bytes": distribution_size(distribution),
            "dependency_closure_bytes": closure_bytes,
            "dependency_distributions": closure,
            "additional_database_bytes": 0,
        }
    result["shared_psycopg2_binary_bytes"] = distribution_size("psycopg2-binary")
    return result


def database_environment(connection, include_pg_ocpm: bool) -> dict[str, Any]:
    version_expression = "ocpm.version()" if include_pg_ocpm else "NULL::text"
    row = query_rows(
        connection,
        f"""
        SELECT {version_expression},current_setting('server_version'),
               current_setting('shared_buffers'),
               current_setting('effective_cache_size'),
               current_setting('work_mem'),
               current_setting('max_parallel_workers_per_gather'),
               current_setting('jit')
        """,
        {},
    )[0]
    return dict(
        zip(
            (
                "pg_ocpm_version",
                "postgres_version",
                "shared_buffers",
                "effective_cache_size",
                "work_mem",
                "max_parallel_workers_per_gather",
                "jit",
            ),
            row,
        )
    )


def source_counts(connection, fixture: Fixture) -> dict[str, int]:
    row = query_rows(
        connection,
        """
        SELECT
          (SELECT count(*) FROM ocel.event WHERE dataset_id=%s)::bigint,
          (SELECT count(*) FROM ocel.object WHERE dataset_id=%s)::bigint,
          (SELECT count(*) FROM ocel.event_object WHERE dataset_id=%s)::bigint,
          (SELECT count(*) FROM ocel.object_object WHERE dataset_id=%s)::bigint
        """,
        (fixture.baseline_dataset_id,) * 4,
    )[0]
    return dict(
        zip(
            ("events", "objects", "event_object_links", "object_object_links"),
            map(int, row),
        )
    )


def geometric(values: list[float]) -> float:
    return math.exp(sum(math.log(value) for value in values) / len(values))


def mib(value: int) -> float:
    return value / (1024**2)


def render(result: dict[str, Any]) -> str:
    lines = [
        "# SAP O2C and P2P three-way process-mining benchmark",
        "",
        (
            "The official SAP IDES OCEL 2.0 logs are compared through three paths: "
            "PM4Py over lightly indexed relational PostgreSQL, PM4Py over `pg_ocpm` "
            "event chunks, and ocpm-engine over `pg_ocpm` sufficient statistics. "
            "Every accepted sample passed exact three-way semantic comparison."
        ),
        "",
    ]
    for dataset in result["datasets"]:
        fixture = dataset["fixture"]
        counts = dataset["source_counts"]
        lines += [
            f"## {dataset['dataset']}",
            "",
            (
                f"Source: **{counts['events']:,} events**, "
                f"**{counts['objects']:,} objects**, and "
                f"**{counts['event_object_links']:,} event-object links**. "
                f"Backbone: **{fixture['object_type']}**, with "
                f"**{fixture['cases']:,} cases** "
                f"(**{fixture['train_cases']:,} train**, "
                f"**{fixture['test_cases']:,} test**)."
            ),
            "",
            (
                "| Workload | Vanilla PG + PM4Py | pg_ocpm + PM4Py | "
                "pg_ocpm + ocpm-engine | pg_ocpm PM4Py vs vanilla | "
                "Engine vs vanilla |"
            ),
            "|---|---:|---:|---:|---:|---:|",
        ]
        for row in dataset["latency"]:
            lines.append(
                f"| {row['workload']} | "
                f"{row['vanilla_pg_pm4py']['p50_ms']:,.2f} ms | "
                f"{row['pg_ocpm_pm4py']['p50_ms']:,.2f} ms | "
                f"{row['pg_ocpm_ocpm_engine']['p50_ms']:,.2f} ms | "
                f"{row['speedups']['pg_ocpm_pm4py_vs_vanilla']:,.2f}x | "
                f"{row['speedups']['pg_ocpm_ocpm_engine_vs_vanilla']:,.2f}x |"
            )
        summary = dataset["summary"]
        pg_pm4py_speedup = summary["geometric_mean_pg_ocpm_pm4py_speedup_vs_vanilla"]
        engine_speedup = summary["geometric_mean_ocpm_engine_speedup_vs_vanilla"]
        lines += [
            "",
            (
                "Geometric-mean speedup versus vanilla: "
                f"**{pg_pm4py_speedup:,.2f}x** "
                "for pg_ocpm + PM4Py and "
                f"**{engine_speedup:,.2f}x** "
                "for pg_ocpm + ocpm-engine. Correctness: "
                f"**{summary['correct_workloads']}/{summary['total_workloads']}**."
            ),
            "",
            "### DFG concurrency",
            "",
            (
                "| Workers | Vanilla PG + PM4Py | pg_ocpm + PM4Py | "
                "pg_ocpm + ocpm-engine | Engine/vanilla |"
            ),
            "|---:|---:|---:|---:|---:|",
        ]
        concurrency = dataset["concurrency"]
        for workers in concurrency["levels"]:
            vanilla = concurrency["vanilla_pg_pm4py"][workers]
            pg_pm4py = concurrency["pg_ocpm_pm4py"][workers]
            engine = concurrency["pg_ocpm_ocpm_engine"][workers]
            ratio = engine["throughput_qps"] / max(vanilla["throughput_qps"], 0.000001)
            lines.append(
                f"| {workers} | {vanilla['throughput_qps']:,.2f} req/s | "
                f"{pg_pm4py['throughput_qps']:,.2f} req/s | "
                f"{engine['throughput_qps']:,.2f} req/s | {ratio:,.2f}x |"
            )
        lines += [
            "",
            "### Isolated peak RSS",
            "",
            (
                "| Workload | Vanilla PG + PM4Py | pg_ocpm + PM4Py | "
                "pg_ocpm + ocpm-engine |"
            ),
            "|---|---:|---:|---:|",
        ]
        for workload in WORKLOADS:
            memory = dataset["memory"][workload]
            lines.append(
                f"| {workload} | "
                f"{mib(memory['vanilla_pg_pm4py']['peak_rss_bytes']):,.1f} MiB | "
                f"{mib(memory['pg_ocpm_pm4py']['peak_rss_bytes']):,.1f} MiB | "
                f"{mib(memory['pg_ocpm_ocpm_engine']['peak_rss_bytes']):,.1f} MiB |"
            )
        lines.append("")

    baseline_storage = result["storage"]["vanilla_pg_pm4py"]
    pg_storage = result["storage"]["shared_pg_ocpm"]
    packages = result["storage"]["client_packages"]
    lines += [
        "## Shared storage and client footprint",
        "",
        "| Representation | Total | Indexes |",
        "|---|---:|---:|",
        (
            "| Vanilla relational OCEL with one workload secondary B-tree | "
            f"{mib(baseline_storage['total_bytes']):,.1f} MiB | "
            f"{mib(baseline_storage['index_bytes']):,.1f} MiB |"
        ),
        (
            "| pg_ocpm serving schema | "
            f"{mib(pg_storage['total_bytes']):,.1f} MiB | "
            f"{mib(pg_storage['index_bytes']):,.1f} MiB |"
        ),
        "",
        (
            "The vanilla index total also includes primary-key and uniqueness indexes "
            "required for relational integrity. Only `ocel_e2o_object` is retained as "
            "a workload-specific secondary index."
        ),
        "",
        "| Client | Package only | Dependency closure |",
        "|---|---:|---:|",
        (
            f"| PM4Py | {mib(packages['pm4py']['package_bytes']):,.1f} MiB | "
            f"{mib(packages['pm4py']['dependency_closure_bytes']):,.1f} MiB |"
        ),
        (
            "| ocpm-engine | "
            f"{mib(packages['ocpm_engine']['package_bytes']):,.1f} MiB | "
            f"{mib(packages['ocpm_engine']['dependency_closure_bytes']):,.1f} MiB |"
        ),
        "",
        "## Methodology",
        "",
        (
            f"- {result['method']['warmups']} warmup and "
            f"{result['method']['measured_runs']} randomized measured runs per "
            "latency comparison."
        ),
        (
            "- Latency includes database extraction, client materialization, model "
            "construction, and scoring."
        ),
        (
            "- Train and test partitions contain complete cases only; cases spanning "
            "the temporal boundary are excluded from both partitions."
        ),
        (
            "- Concurrency uses prestarted, warmed process workers with one PostgreSQL "
            "connection per worker."
        ),
        "- Peak RSS uses a fresh process for each dataset, workload, and engine path.",
        "- Source: Zenodo DOI `10.5281/zenodo.8261133`, CC BY 4.0.",
        (
            "- PM4Py package licensing must be evaluated separately before product "
            "integration; installed metadata is retained in JSON."
        ),
        "",
    ]
    return "\n".join(lines)


def benchmark(args: argparse.Namespace) -> dict[str, Any]:
    rng = random.Random(args.seed)
    baseline = connect(args.baseline_host, args.database, args.timeout_seconds)
    extension = connect(args.extension_host, args.database, args.timeout_seconds)
    dataset_results = []
    overall_speedups = {
        name: []
        for name in (
            "pg_ocpm_pm4py_vs_vanilla",
            "pg_ocpm_ocpm_engine_vs_vanilla",
            "pg_ocpm_ocpm_engine_vs_pg_ocpm_pm4py",
        )
    }

    for dataset_name in args.datasets:
        fixture = discover_fixture(extension, baseline, args, dataset_name)
        print(
            f"fixture {dataset_name}: {fixture.object_type}, {fixture.cases:,} cases "
            f"({fixture.train_cases:,} train/{fixture.test_cases:,} test)",
            flush=True,
        )
        latency = []
        dataset_speedups = {name: [] for name in overall_speedups}
        for workload in WORKLOADS:
            print(f"benchmarking {dataset_name} {workload}", flush=True)
            measured = timed_comparison(
                {
                    "vanilla_pg_pm4py": lambda w=workload: run_pm4py(
                        baseline, fixture, w, "vanilla_pg"
                    ),
                    "pg_ocpm_pm4py": lambda w=workload: run_pm4py(
                        extension, fixture, w, "pg_ocpm"
                    ),
                    "pg_ocpm_ocpm_engine": lambda w=workload: run_ocpm_engine(
                        extension, fixture, w
                    ),
                },
                args.warmups,
                args.runs,
                rng,
            )
            latency.append({"workload": workload, **measured})
            for name, value in measured["speedups"].items():
                dataset_speedups[name].append(value)
                overall_speedups[name].append(value)
            print(
                "  pg_ocpm PM4Py/vanilla "
                f"{measured['speedups']['pg_ocpm_pm4py_vs_vanilla']:.2f}x; "
                "engine/vanilla "
                f"{measured['speedups']['pg_ocpm_ocpm_engine_vs_vanilla']:.2f}x; "
                "correctness passed",
                flush=True,
            )

        expected = canonical(
            run_ocpm_engine(extension, fixture, "dfg_conformance_95pct")["answer"]
        )
        concurrency = {
            "workload": "dfg_conformance_95pct",
            "levels": [value for value in args.concurrency.split(",") if value],
            **{
                engine: concurrency_sweep(args, fixture, engine, expected)
                for engine in ENGINES
            },
        }

        memory = {}
        for workload in WORKLOADS:
            print(
                f"measuring isolated memory: {dataset_name} {workload}",
                flush=True,
            )
            memory[workload] = {
                engine: measure_memory(args, engine, dataset_name, workload)
                for engine in ENGINES
            }
            hashes = {
                measurement["answer_sha256"]
                for measurement in memory[workload].values()
            }
            if len(hashes) != 1:
                raise AssertionError("memory worker correctness mismatch")

        dataset_results.append(
            {
                "dataset": dataset_name,
                "source_counts": source_counts(baseline, fixture),
                "fixture": serialize_fixture(fixture),
                "summary": {
                    "correct_workloads": len(latency),
                    "total_workloads": len(WORKLOADS),
                    "geometric_mean_pg_ocpm_pm4py_speedup_vs_vanilla": round(
                        geometric(dataset_speedups["pg_ocpm_pm4py_vs_vanilla"]),
                        3,
                    ),
                    "geometric_mean_ocpm_engine_speedup_vs_vanilla": round(
                        geometric(dataset_speedups["pg_ocpm_ocpm_engine_vs_vanilla"]),
                        3,
                    ),
                    "geometric_mean_ocpm_engine_speedup_vs_pg_ocpm_pm4py": round(
                        geometric(
                            dataset_speedups["pg_ocpm_ocpm_engine_vs_pg_ocpm_pm4py"]
                        ),
                        3,
                    ),
                },
                "latency": latency,
                "concurrency": concurrency,
                "memory": memory,
            }
        )

    environment = {
        "client": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "machine": platform.machine(),
            "logical_cpus_visible": os.cpu_count(),
            "pm4py_version": metadata.version("pm4py"),
            "ocpm_engine_version": metadata.version("ocpm-engine"),
        },
        "database": {
            "vanilla_pg": database_environment(baseline, False),
            "pg_ocpm": database_environment(extension, True),
        },
    }
    storage = {
        "vanilla_pg_pm4py": schema_storage(baseline, "ocel"),
        "shared_pg_ocpm": schema_storage(extension, "ocpm"),
        "client_packages": package_storage(),
    }
    baseline.close()
    extension.close()

    result = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "title": (
                "Collection of Object-Centric Event Logs (OCEL 2.0 relational SQLite)"
            ),
            "doi": "10.5281/zenodo.8261133",
            "license": "CC BY 4.0",
            "datasets": list(args.datasets),
        },
        "environment": environment,
        "method": {
            "warmups": args.warmups,
            "measured_runs": args.runs,
            "random_seed": args.seed,
            "train_fraction": args.train_fraction,
            "latency_scope": (
                "database extraction plus client materialization, model construction, "
                "and scoring"
            ),
            "execution_order": "randomized per measured three-way comparison",
            "correctness_gate": (
                "three-way canonical answer equality before sample acceptance"
            ),
            "concurrency_model": (
                "prestarted warmed process workers with one PostgreSQL connection per "
                "worker"
            ),
            "memory_model": (
                "fresh process baseline and peak RSS including engine import and one "
                "request"
            ),
            "vanilla_index_policy": (
                "relational integrity indexes plus one workload secondary B-tree, "
                "ocel_e2o_object"
            ),
            "result_cache_used": False,
        },
        "summary": {
            "correct_workloads": sum(
                item["summary"]["correct_workloads"] for item in dataset_results
            ),
            "total_workloads": len(dataset_results) * len(WORKLOADS),
            "geometric_mean_pg_ocpm_pm4py_speedup_vs_vanilla": round(
                geometric(overall_speedups["pg_ocpm_pm4py_vs_vanilla"]), 3
            ),
            "geometric_mean_ocpm_engine_speedup_vs_vanilla": round(
                geometric(overall_speedups["pg_ocpm_ocpm_engine_vs_vanilla"]),
                3,
            ),
            "geometric_mean_ocpm_engine_speedup_vs_pg_ocpm_pm4py": round(
                geometric(overall_speedups["pg_ocpm_ocpm_engine_vs_pg_ocpm_pm4py"]),
                3,
            ),
        },
        "datasets": dataset_results,
        "storage": storage,
    }
    encoded = json.dumps(result, indent=2, default=str) + "\n"
    result["payload_sha256"] = hashlib.sha256(encoded.encode()).hexdigest()
    output = Path(args.output)
    report = Path(args.report)
    output.parent.mkdir(parents=True, exist_ok=True)
    report.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, default=str) + "\n")
    report.write_text(render(result))
    print(f"wrote {output} and {report}", flush=True)
    return result


def main() -> None:
    args = parse_args()
    if args.memory_worker:
        memory_worker(args)
        return
    benchmark(args)


if __name__ == "__main__":
    main()
