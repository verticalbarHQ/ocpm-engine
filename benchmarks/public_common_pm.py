"""Public SAP OCEL common-PM benchmark: vanilla PostgreSQL versus pg_ocpm + Rust.

The benchmark uses the CC BY 4.0 SAP IDES O2C and P2P logs published at
doi:10.5281/zenodo.8261133. The database fixture is produced by
``benchmarks/public_fixture.py``. Every timed result passes a deterministic
cross-engine output gate before it contributes to latency or concurrency claims.
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import os
import platform
import random
import statistics
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from importlib import metadata
from pathlib import Path
from typing import Any, Callable, Iterable

import psycopg2

try:
    from benchmark_provenance import (
        PUBLIC_BENCHMARK_SCHEMA_VERSION,
        public_benchmark_provenance,
    )
except ModuleNotFoundError:  # imported as benchmarks.public_common_pm in tests
    from benchmarks.benchmark_provenance import (
        PUBLIC_BENCHMARK_SCHEMA_VERSION,
        public_benchmark_provenance,
    )

from ocpm_engine import (
    TransitionCount,
    bottleneck_order,
    dfg_conformance,
    frequency_drift,
    next_activity,
    variant_conformance,
)

PAIR_COUNTS_BASE = """
WITH ordered AS MATERIALIZED (
    SELECT eo.object_key AS case_id,
           event.activity AS source_activity,
           event.event_timestamp AS source_timestamp,
           lead(event.activity) OVER lifecycle AS target_activity,
           lead(event.event_timestamp) OVER lifecycle AS target_timestamp
    FROM ocel.event_object eo
    JOIN ocel.event event
      ON event.dataset_id=eo.dataset_id AND event.event_key=eo.event_key
    WHERE eo.dataset_id=%(dataset_id)s AND eo.object_type=%(object_type)s
    WINDOW lifecycle AS (
        PARTITION BY eo.object_key
        ORDER BY event.event_timestamp,event.event_key
    )
)
SELECT source_activity,target_activity,'directly_follows'::text AS edge_type,
       count(*) FILTER (
         WHERE source_timestamp >= %(from_time)s
           AND target_timestamp <= %(train_to)s
       )::bigint AS train_count,
       count(*) FILTER (
         WHERE source_timestamp >= %(test_from)s
           AND target_timestamp <= %(to_time)s
       )::bigint AS test_count
FROM ordered
WHERE target_timestamp IS NOT NULL
GROUP BY source_activity,target_activity
HAVING count(*) FILTER (
         WHERE source_timestamp >= %(from_time)s
           AND target_timestamp <= %(train_to)s
       ) > 0
    OR count(*) FILTER (
         WHERE source_timestamp >= %(test_from)s
           AND target_timestamp <= %(to_time)s
       ) > 0
ORDER BY source_activity,target_activity
"""

DFG_WINDOW_COUNTS_EXT = """
SELECT source_activity,target_activity,edge_type,
       frequencies[1] AS train_count,frequencies[2] AS test_count
FROM ocpm.dfg_window_counts(
    %(dataset_id)s,1,
    ARRAY[%(from_time)s,%(test_from)s]::timestamptz[],
    ARRAY[%(train_to)s,%(to_time)s]::timestamptz[],
    ARRAY[%(object_type)s],ARRAY[%(object_type)s],
    NULL,NULL,ARRAY['directly_follows'],NULL,1
)
ORDER BY source_activity,target_activity,edge_type
"""

VARIANT_COUNTS_BASE = """
WITH paths AS MATERIALIZED (
    SELECT eo.object_key AS case_id,
           min(event.event_timestamp) AS start_time,
           max(event.event_timestamp) AS end_time,
           substring(md5(json_agg(
             event.activity ORDER BY event.event_timestamp,event.event_key
           )::text),1,10) AS path_hash
    FROM ocel.event_object eo
    JOIN ocel.event event
      ON event.dataset_id=eo.dataset_id AND event.event_key=eo.event_key
    WHERE eo.dataset_id=%(dataset_id)s AND eo.object_type=%(object_type)s
    GROUP BY eo.object_key
)
SELECT path_hash,
       count(*) FILTER (
         WHERE start_time >= %(from_time)s AND end_time <= %(train_to)s
       )::bigint AS train_count,
       count(*) FILTER (
         WHERE start_time >= %(test_from)s AND end_time <= %(to_time)s
       )::bigint AS test_count
FROM paths
GROUP BY path_hash
HAVING count(*) FILTER (
         WHERE start_time >= %(from_time)s AND end_time <= %(train_to)s
       ) > 0
    OR count(*) FILTER (
         WHERE start_time >= %(test_from)s AND end_time <= %(to_time)s
       ) > 0
ORDER BY path_hash
"""

VARIANT_WINDOW_COUNTS_EXT = """
SELECT path_hash,frequencies[1] AS train_count,
       frequencies[2] AS test_count
FROM ocpm.variant_window_counts(
    %(dataset_id)s,1,
    ARRAY[%(from_time)s,%(test_from)s]::timestamptz[],
    ARRAY[%(train_to)s,%(to_time)s]::timestamptz[],
    ARRAY[%(object_type)s],NULL,NULL,NULL,NULL,1
)
ORDER BY path_hash
"""

REWORK_BASE = """
WITH ordered AS MATERIALIZED (
    SELECT eo.object_key AS case_id,
           event.activity AS source_activity,
           event.event_timestamp AS source_timestamp,
           lead(event.activity) OVER lifecycle AS target_activity,
           lead(event.event_timestamp) OVER lifecycle AS target_timestamp
    FROM ocel.event_object eo
    JOIN ocel.event event
      ON event.dataset_id=eo.dataset_id AND event.event_key=eo.event_key
    WHERE eo.dataset_id=%(dataset_id)s AND eo.object_type=%(object_type)s
    WINDOW lifecycle AS (
        PARTITION BY eo.object_key
        ORDER BY event.event_timestamp,event.event_key
    )
), multiplicity AS MATERIALIZED (
    SELECT source_activity,target_activity,case_id,count(*)::bigint AS occurrences
    FROM ordered
    WHERE target_timestamp IS NOT NULL
      AND source_timestamp >= %(from_time)s AND target_timestamp <= %(to_time)s
    GROUP BY source_activity,target_activity,case_id
), pair_stats AS (
    SELECT source_activity,target_activity,
           sum(occurrences)::bigint AS edge_count,
           count(*)::bigint AS unique_cases,
           count(*) FILTER (WHERE occurrences > 1)::bigint AS rework_cases,
           sum(greatest(occurrences-1,0))::bigint AS excess_transitions,
           max(occurrences)::bigint AS maximum_occurrences
    FROM multiplicity
    GROUP BY source_activity,target_activity
    HAVING count(*) FILTER (WHERE occurrences > 1) > 0
)
SELECT source_activity,target_activity,'directly_follows'::text AS edge_type,
       edge_count,unique_cases,rework_cases,excess_transitions,maximum_occurrences
FROM pair_stats ORDER BY source_activity,target_activity
"""

REWORK_EXT = """
SELECT source_activity,target_activity,edge_type,edge_count,unique_cases,
       rework_cases,excess_transitions,maximum_occurrences
FROM ocpm.rework_counts(
    %(dataset_id)s,1,%(from_time)s,%(to_time)s,
    ARRAY[%(object_type)s],ARRAY[%(object_type)s],
    NULL,NULL,ARRAY['directly_follows'],NULL,1
)
ORDER BY source_activity,target_activity,edge_type
"""

BOTTLENECK_BASE = """
WITH ordered AS MATERIALIZED (
    SELECT event.activity AS source_activity,
           event.event_timestamp AS source_timestamp,
           lead(event.activity) OVER lifecycle AS target_activity,
           lead(event.event_timestamp) OVER lifecycle AS target_timestamp
    FROM ocel.event_object eo
    JOIN ocel.event event
      ON event.dataset_id=eo.dataset_id AND event.event_key=eo.event_key
    WHERE eo.dataset_id=%(dataset_id)s AND eo.object_type=%(object_type)s
    WINDOW lifecycle AS (
        PARTITION BY eo.object_key
        ORDER BY event.event_timestamp,event.event_key
    )
)
SELECT source_activity,target_activity,'directly_follows'::text AS edge_type,
       count(*)::bigint AS frequency,
       avg(extract(epoch FROM target_timestamp-source_timestamp))::float8
         AS mean_duration
FROM ordered
WHERE target_timestamp IS NOT NULL
  AND source_timestamp >= %(from_time)s AND target_timestamp <= %(to_time)s
GROUP BY source_activity,target_activity
HAVING count(*) >= %(minimum_frequency)s
"""

BOTTLENECK_EXT = """
SELECT source_activity,target_activity,edge_type,frequency,mean_duration
FROM ocpm.edge_feature_aggregates(
    %(dataset_id)s,1,%(from_time)s,%(to_time)s,'Infinity'::float8,
    ARRAY[%(object_type)s],ARRAY[%(object_type)s],
    NULL,NULL,ARRAY['directly_follows'],NULL,%(minimum_frequency)s
)
"""

EDGE_FEATURE_BASE = """
WITH ordered AS MATERIALIZED (
    SELECT event.activity AS source_activity,
           event.event_timestamp AS source_timestamp,
           lead(event.activity) OVER lifecycle AS target_activity,
           lead(event.event_timestamp) OVER lifecycle AS target_timestamp
    FROM ocel.event_object eo
    JOIN ocel.event event
      ON event.dataset_id=eo.dataset_id AND event.event_key=eo.event_key
    WHERE eo.dataset_id=%(dataset_id)s AND eo.object_type=%(object_type)s
    WINDOW lifecycle AS (
        PARTITION BY eo.object_key
        ORDER BY event.event_timestamp,event.event_key
    )
), edges AS MATERIALIZED (
    SELECT source_activity,target_activity,source_timestamp,target_timestamp,
           extract(epoch FROM target_timestamp-source_timestamp)::float8 AS duration
    FROM ordered WHERE target_timestamp IS NOT NULL
), grouped AS (
    SELECT source_activity,target_activity,'directly_follows'::text AS edge_type,
           count(*)::bigint AS frequency,avg(duration)::float8 AS mean_duration,
           stddev_samp(duration)::float8 AS standard_deviation,
           count(*) FILTER (WHERE duration > %(slow_threshold)s)::bigint AS slow_count
    FROM edges
    WHERE source_timestamp >= %(window_from)s AND target_timestamp <= %(window_to)s
    GROUP BY source_activity,target_activity
)
SELECT source_activity,target_activity,edge_type,frequency,mean_duration,
       standard_deviation,slow_count,
       slow_count::float8/nullif(frequency,0)::float8 AS slow_rate
FROM grouped ORDER BY source_activity,target_activity
"""

EDGE_FEATURE_EXT = """
SELECT source_activity,target_activity,edge_type,frequency,mean_duration,
       standard_deviation,slow_count,slow_rate
FROM ocpm.edge_feature_aggregates(
    %(dataset_id)s,1,%(window_from)s,%(window_to)s,%(slow_threshold)s,
    ARRAY[%(object_type)s],ARRAY[%(object_type)s],
    NULL,NULL,ARRAY['directly_follows'],NULL,1
)
ORDER BY source_activity,target_activity,edge_type
"""

SERIES_BASE = """
WITH ordered AS MATERIALIZED (
    SELECT event.activity AS source_activity,
           event.event_timestamp AS source_timestamp,
           lead(event.activity) OVER lifecycle AS target_activity,
           lead(event.event_timestamp) OVER lifecycle AS target_timestamp
    FROM ocel.event_object eo
    JOIN ocel.event event
      ON event.dataset_id=eo.dataset_id AND event.event_key=eo.event_key
    WHERE eo.dataset_id=%(dataset_id)s AND eo.object_type=%(object_type)s
    WINDOW lifecycle AS (
        PARTITION BY eo.object_key
        ORDER BY event.event_timestamp,event.event_key
    )
), boundaries AS (
    SELECT boundary_start,boundary_end
    FROM unnest(
      %(boundaries)s::timestamptz[],
      %(boundary_ends)s::timestamptz[]
    ) AS item(boundary_start,boundary_end)
)
SELECT boundaries.boundary_start,boundaries.boundary_end,
       count(*)::bigint AS frequency,
       avg(extract(epoch FROM target_timestamp-source_timestamp))::float8
         AS mean_duration
FROM boundaries
JOIN ordered
  ON source_timestamp >= boundaries.boundary_start
 AND source_timestamp < boundaries.boundary_end
 AND target_timestamp <= %(series_end)s
WHERE source_activity=%(source_activity)s AND target_activity=%(target_activity)s
GROUP BY boundaries.boundary_start,boundaries.boundary_end
ORDER BY boundaries.boundary_start
"""

SERIES_EXT = """
SELECT buckets
FROM ocpm.edge_duration_time_series(
    %(dataset_id)s,1,%(all_boundaries)s,
    ARRAY[%(object_type)s],ARRAY[%(object_type)s],
    ARRAY[%(source_activity)s],ARRAY[%(target_activity)s],
    ARRAY['directly_follows'],NULL,1
)
"""

ACTIVITY_PROFILE_BASE = """
WITH ordered AS MATERIALIZED (
    SELECT eo.object_key AS case_id,
           event.activity,
           row_number() OVER lifecycle AS position,
           count(*) OVER lifecycle AS path_length,
           min(event.event_timestamp) OVER lifecycle AS case_start,
           max(event.event_timestamp) OVER lifecycle AS case_end
    FROM ocel.event_object eo
    JOIN ocel.event event
      ON event.dataset_id=eo.dataset_id AND event.event_key=eo.event_key
    WHERE eo.dataset_id=%(dataset_id)s AND eo.object_type=%(object_type)s
    WINDOW lifecycle AS (
        PARTITION BY eo.object_key
        ORDER BY event.event_timestamp,event.event_key
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
)
SELECT %(object_type)s::text AS object_type,activity,
       count(DISTINCT case_id)::bigint AS case_frequency,
       count(*)::bigint AS occurrence_frequency,
       count(*) FILTER (WHERE position=1)::bigint AS start_frequency,
       count(*) FILTER (WHERE position=path_length)::bigint AS end_frequency
FROM ordered
WHERE case_start >= %(from_time)s AND case_end <= %(to_time)s
GROUP BY activity
ORDER BY object_type,activity
"""

ACTIVITY_PROFILE_EXT = """
SELECT object_type,activity,case_frequency,occurrence_frequency,
       start_frequency,end_frequency
FROM ocpm.activity_profile(
    %(dataset_id)s,1,%(from_time)s,%(to_time)s,
    ARRAY[%(object_type)s],NULL,NULL,NULL,NULL,1
)
ORDER BY object_type,activity
"""


@dataclass(frozen=True)
class Fixture:
    name: str
    baseline_dataset_id: int
    extension_dataset_id: int
    object_type: str
    from_time: datetime
    train_to: datetime
    test_from: datetime
    to_time: datetime
    source_activity: str
    target_activity: str
    slow_threshold: float


class Database:
    def __init__(self, host: str, database: str, timeout_seconds: int):
        self.host = host
        self.database = database
        self.timeout_seconds = timeout_seconds
        self.connection = self.connect()

    def connect(self):
        connection = psycopg2.connect(
            host=self.host,
            port=5432,
            dbname=self.database,
            user="postgres",
            password="pg",
            connect_timeout=15,
        )
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute("SET jit=off")
            cursor.execute("SET statement_timeout=%s", (self.timeout_seconds * 1000,))
        return connection

    def rows(self, sql: str, params: dict[str, Any]) -> list[tuple]:
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    def one(self, sql: str, params: dict[str, Any]) -> Any:
        rows = self.rows(sql, params)
        return rows[0][0] if rows else None

    def close(self) -> None:
        self.connection.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-host", default="postgres_vanilla_clean")
    parser.add_argument("--extension-host", default="postgres_ocpm_clean")
    parser.add_argument("--database", help="use one database name for both sides")
    parser.add_argument("--baseline-db", default="ocel_benchmark")
    parser.add_argument("--extension-db", default="ocel_benchmark")
    parser.add_argument("--warmups", type=int, default=10)
    parser.add_argument(
        "--runs",
        type=int,
        default=30,
        help="measured rounds in each serial-latency epoch",
    )
    parser.add_argument("--latency-epochs", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--seed", type=int, default=20260718)
    parser.add_argument("--concurrency", default="1,4,8,16")
    parser.add_argument(
        "--concurrency-requests",
        "--concurrency-min-requests-per-worker",
        dest="concurrency_requests",
        type=int,
        default=32,
        help="minimum measured requests per worker in every concurrency epoch",
    )
    parser.add_argument("--concurrency-epochs", type=int, default=3)
    parser.add_argument("--concurrency-min-seconds", type=float, default=5.0)
    parser.add_argument(
        "--concurrency-only",
        action="store_true",
        help="replace only concurrency sections in the existing --output artifact",
    )
    parser.add_argument("--output", default="docs/results/public-common-pm-0.8.0.json")
    args = parser.parse_args()
    if args.database:
        args.baseline_db = args.database
        args.extension_db = args.database
    if args.warmups < 0 or args.runs < 1:
        parser.error("--warmups must be nonnegative and --runs must be positive")
    if args.latency_epochs < 1:
        parser.error("--latency-epochs must be positive")
    levels = [int(value) for value in args.concurrency.split(",") if value]
    if (
        not levels
        or any(value < 1 for value in levels)
        or len(set(levels)) != len(levels)
    ):
        parser.error("--concurrency must contain unique positive worker counts")
    if args.concurrency_epochs < 3:
        parser.error("--concurrency-epochs must be at least 3")
    if args.concurrency_min_seconds < 5.0:
        parser.error("--concurrency-min-seconds must be at least 5")
    if args.concurrency_requests < 32:
        parser.error("--concurrency-requests must be at least 32 per worker")
    return args


def transition_rows(rows: Iterable[tuple]) -> list[TransitionCount]:
    return [
        TransitionCount(row[0], row[1], row[2], int(row[3]), int(row[4]))
        for row in rows
    ]


def reference_dfg(rows: list[TransitionCount], coverage: float = 0.95) -> dict:
    ranked = sorted(
        rows, key=lambda row: (-row.train_count, row.source, row.target, row.edge_type)
    )
    target = math.ceil(sum(row.train_count for row in ranked) * coverage)
    covered = 0
    model = []
    for row in ranked:
        if covered >= target:
            break
        if row.train_count:
            model.append((row.source, row.target, row.edge_type))
            covered += row.train_count
    accepted = set(model)
    test_total = sum(row.test_count for row in rows)
    conforming = sum(
        row.test_count
        for row in rows
        if (row.source, row.target, row.edge_type) in accepted
    )
    return {
        "fitness": round(conforming / test_total if test_total else 1.0, 12),
        "conforming": conforming,
        "deviations": test_total - conforming,
        "test_total": test_total,
        "model": sorted(model),
    }


def native_dfg(rows: list[TransitionCount]) -> dict:
    score = dfg_conformance(rows)
    return {
        "fitness": round(score.fitness, 12),
        "conforming": score.conforming,
        "deviations": score.deviations,
        "test_total": score.test_total,
        "model": sorted(score.model),
    }


def reference_next(rows: list[TransitionCount]) -> dict:
    winners: dict[tuple[str, str], tuple[str, int]] = {}
    for row in rows:
        if row.train_count <= 0:
            continue
        group = (row.source, row.edge_type)
        candidate = (row.target, row.train_count)
        current = winners.get(group)
        if (
            current is None
            or candidate[1] > current[1]
            or (candidate[1] == current[1] and candidate[0] < current[0])
        ):
            winners[group] = candidate
    correct = sum(
        row.test_count
        for row in rows
        if (row.source, row.edge_type) in winners
        and winners[(row.source, row.edge_type)][0] == row.target
    )
    total = sum(row.test_count for row in rows)
    return {
        "accuracy": round(correct / total if total else 1.0, 12),
        "correct": correct,
        "test_total": total,
        "predictions": sorted(
            (source, edge_type, value[0])
            for (source, edge_type), value in winners.items()
        ),
    }


def native_next(rows: list[TransitionCount]) -> dict:
    score = next_activity(rows)
    return {
        "accuracy": round(score.accuracy, 12),
        "correct": score.correct,
        "test_total": score.test_total,
        "predictions": sorted(score.predictions),
    }


def reference_drift(rows: list[TransitionCount], top_n: int = 10) -> dict:
    baseline_total = sum(row.train_count for row in rows)
    current_total = sum(row.test_count for row in rows)
    contributors = []
    divergence = 0.0
    for row in rows:
        label = json.dumps(
            [row.source, row.target, row.edge_type], separators=(",", ":")
        )
        baseline_share = row.train_count / baseline_total if baseline_total else 0.0
        current_share = row.test_count / current_total if current_total else 0.0
        if baseline_total == 0 and current_total == 0:
            contribution = 0.0
        elif baseline_total == 0:
            contribution = current_share
        elif current_total == 0:
            contribution = baseline_share
        else:
            midpoint = (baseline_share + current_share) * 0.5
            baseline_term = (
                0.0
                if baseline_share == 0.0
                else 0.5 * baseline_share * math.log2(baseline_share / midpoint)
            )
            current_term = (
                0.0
                if current_share == 0.0
                else 0.5 * current_share * math.log2(current_share / midpoint)
            )
            contribution = baseline_term + current_term
        divergence += contribution
        contributors.append(
            [
                label,
                round(baseline_share, 12),
                round(current_share, 12),
                round(current_share - baseline_share, 12),
                round(contribution, 12),
            ]
        )
    contributors.sort(key=lambda item: (-item[4], item[0]))
    return {
        "divergence": round(min(max(divergence, 0.0), 1.0), 12),
        "baseline_total": baseline_total,
        "current_total": current_total,
        "contributors": contributors[:top_n],
    }


def native_drift(rows: list[TransitionCount], top_n: int = 10) -> dict:
    labels = [
        json.dumps([row.source, row.target, row.edge_type], separators=(",", ":"))
        for row in rows
    ]
    score = frequency_drift(
        labels,
        [row.train_count for row in rows],
        [row.test_count for row in rows],
        top_n=top_n,
    )
    return {
        "divergence": round(score.divergence, 12),
        "baseline_total": score.baseline_total,
        "current_total": score.current_total,
        "contributors": [
            [
                item.label,
                round(item.baseline_share, 12),
                round(item.current_share, 12),
                round(item.share_delta, 12),
                round(item.js_contribution, 12),
            ]
            for item in score.contributors
        ],
    }


def variant_result(rows: list[tuple], native: bool) -> dict:
    variants = [str(row[0]) for row in rows]
    train = [int(row[1]) for row in rows]
    test = [int(row[2]) for row in rows]
    if native:
        fitness, conforming, deviations, total, model = variant_conformance(
            variants, train, test
        )
    else:
        ranked = sorted(zip(variants, train), key=lambda row: (-row[1], row[0]))
        target = math.ceil(sum(train) * 0.95)
        covered = 0
        model_list = []
        for variant, frequency in ranked:
            if covered >= target:
                break
            if frequency:
                model_list.append(variant)
                covered += frequency
        model = tuple(sorted(model_list))
        accepted = set(model)
        total = sum(test)
        conforming = sum(
            count for variant, count in zip(variants, test) if variant in accepted
        )
        deviations = total - conforming
        fitness = conforming / total if total else 1.0
    return {
        "fitness": round(fitness, 12),
        "conforming": conforming,
        "deviations": deviations,
        "test_total": total,
        "model": sorted(model),
    }


def params(fixture: Fixture, extension: bool) -> dict[str, Any]:
    result = asdict(fixture)
    result["dataset_id"] = (
        fixture.extension_dataset_id if extension else fixture.baseline_dataset_id
    )
    result["minimum_frequency"] = 2
    return result


def base_pair_counts(db: Database, fixture: Fixture) -> list[tuple]:
    return db.rows(PAIR_COUNTS_BASE, params(fixture, False))


def ext_pair_counts(db: Database, fixture: Fixture) -> list[tuple]:
    return db.rows(DFG_WINDOW_COUNTS_EXT, params(fixture, True))


def base_variant_counts(db: Database, fixture: Fixture) -> list[tuple]:
    return db.rows(VARIANT_COUNTS_BASE, params(fixture, False))


def ext_variant_counts(db: Database, fixture: Fixture) -> list[tuple]:
    return db.rows(VARIANT_WINDOW_COUNTS_EXT, params(fixture, True))


def activity_profile(db: Database, fixture: Fixture, extension: bool) -> list[list]:
    rows = db.rows(
        ACTIVITY_PROFILE_EXT if extension else ACTIVITY_PROFILE_BASE,
        params(fixture, extension),
    )
    return normalize_rows(rows)


def normalize_rows(
    rows: list[tuple], float_columns: set[int] | None = None
) -> list[list]:
    float_columns = float_columns or set()
    return [
        [
            round(float(value), 6)
            if index in float_columns and value is not None
            else value
            for index, value in enumerate(row)
        ]
        for row in rows
    ]


def bottleneck(db: Database, fixture: Fixture, extension: bool) -> list[list]:
    rows = db.rows(
        BOTTLENECK_EXT if extension else BOTTLENECK_BASE, params(fixture, extension)
    )
    frequencies = [int(row[3]) for row in rows]
    means = [float(row[4]) for row in rows]
    if extension:
        order = bottleneck_order(frequencies, means)
    else:
        order = sorted(
            range(len(rows)),
            key=lambda index: (-means[index], -frequencies[index], index),
        )
    return normalize_rows([rows[index] for index in order[:10]], {4})


def edge_prediction(db: Database, fixture: Fixture, extension: bool) -> dict:
    common = params(fixture, extension)
    query = EDGE_FEATURE_EXT if extension else EDGE_FEATURE_BASE
    train = db.rows(
        query,
        {**common, "window_from": fixture.from_time, "window_to": fixture.train_to},
    )
    test = db.rows(
        query,
        {**common, "window_from": fixture.test_from, "window_to": fixture.to_time},
    )
    train_by_key = {tuple(row[:3]): row for row in train}
    squared_error = 0.0
    brier = 0.0
    total = 0
    evaluated_groups = 0
    for row in test:
        key = tuple(row[:3])
        trained = train_by_key.get(key)
        if trained is None:
            continue
        n = int(row[3])
        test_mean = float(row[4])
        test_stddev = float(row[5] or 0.0)
        test_slow = int(row[6])
        predicted_mean = float(trained[4])
        predicted_rate = float(trained[7])
        squared_error += max(n - 1, 0) * test_stddev**2
        squared_error += n * (test_mean - predicted_mean) ** 2
        brier += test_slow * (1.0 - predicted_rate) ** 2
        brier += (n - test_slow) * predicted_rate**2
        total += n
        evaluated_groups += 1
    return {
        "test_edges": total,
        "groups": evaluated_groups,
        "rmse": round(math.sqrt(squared_error / total), 6) if total else None,
        "brier": round(brier / total, 6) if total else None,
    }


def month_boundaries(fixture: Fixture) -> list[datetime]:
    current = datetime(
        fixture.from_time.year, fixture.from_time.month, 1, tzinfo=timezone.utc
    )
    end = fixture.to_time
    values = [current]
    while current <= end:
        current = (
            datetime(current.year + 1, 1, 1, tzinfo=timezone.utc)
            if current.month == 12
            else datetime(current.year, current.month + 1, 1, tzinfo=timezone.utc)
        )
        values.append(current)
    return values


def duration_series(db: Database, fixture: Fixture, extension: bool) -> list[list]:
    boundaries = month_boundaries(fixture)
    common = {
        **params(fixture, extension),
        "boundaries": boundaries[:-1],
        "boundary_ends": boundaries[1:],
        "all_boundaries": boundaries,
        "series_end": boundaries[-1],
    }
    if not extension:
        rows = db.rows(SERIES_BASE, common)
        return [
            [
                row[0].isoformat(),
                row[1].isoformat(),
                int(row[2]),
                round(float(row[3]), 3),
            ]
            for row in rows
        ]
    payload = db.one(SERIES_EXT, common) or []
    return [
        [
            datetime.fromisoformat(
                item["bucket_start"].replace("+00", "+00:00")
            ).isoformat(),
            datetime.fromisoformat(
                item["bucket_end"].replace("+00", "+00:00")
            ).isoformat(),
            int(item["frequency"]),
            round(float(item["mean_duration"]), 3),
        ]
        for item in payload
    ]


def discover_fixtures(baseline: Database, extension: Database) -> list[Fixture]:
    datasets = baseline.rows(
        "SELECT dataset_id,dataset_name FROM ocel.dataset ORDER BY dataset_id", {}
    )
    extension_ids = dict(
        extension.rows("SELECT dataset_name,dataset_id FROM ocpm.dataset", {})
    )
    fixtures = []
    for dataset_id, name in datasets:
        row = baseline.rows(
            """
            WITH selected_type AS (
                SELECT eo.object_type,count(*) AS event_links
                FROM ocel.event_object eo WHERE eo.dataset_id=%s
                GROUP BY eo.object_type ORDER BY event_links DESC,eo.object_type LIMIT 1
            ), bounds AS (
                SELECT min(event.event_timestamp),max(event.event_timestamp),
                       percentile_disc(0.8) WITHIN GROUP (
                         ORDER BY event.event_timestamp
                       )
                FROM ocel.event_object eo
                JOIN ocel.event event USING (dataset_id,event_key)
                JOIN selected_type type ON type.object_type=eo.object_type
                WHERE eo.dataset_id=%s
            )
            SELECT type.object_type,bounds.* FROM selected_type type CROSS JOIN bounds
            """,
            (dataset_id, dataset_id),
        )[0]
        object_type, minimum, maximum, split = row
        pair = baseline.rows(
            """
            WITH ordered AS (
                SELECT event.activity AS source_activity,
                       lead(event.activity) OVER (
                         PARTITION BY eo.object_key
                         ORDER BY event.event_timestamp,event.event_key
                       ) AS target_activity
                FROM ocel.event_object eo
                JOIN ocel.event event USING (dataset_id,event_key)
                WHERE eo.dataset_id=%s AND eo.object_type=%s
            )
            SELECT source_activity,target_activity,count(*) AS frequency
            FROM ordered WHERE target_activity IS NOT NULL
            GROUP BY source_activity,target_activity
            ORDER BY frequency DESC,source_activity,target_activity LIMIT 1
            """,
            (dataset_id, object_type),
        )[0]
        threshold = baseline.one(
            """
            WITH ordered AS (
                SELECT event.event_timestamp AS source_timestamp,
                       lead(event.event_timestamp) OVER (
                         PARTITION BY eo.object_key
                         ORDER BY event.event_timestamp,event.event_key
                       ) AS target_timestamp
                FROM ocel.event_object eo
                JOIN ocel.event event USING (dataset_id,event_key)
                WHERE eo.dataset_id=%s AND eo.object_type=%s
            )
            SELECT percentile_cont(0.9) WITHIN GROUP (
                     ORDER BY extract(epoch FROM target_timestamp-source_timestamp)
                   )::float8
            FROM ordered WHERE target_timestamp IS NOT NULL AND target_timestamp <= %s
            """,
            (dataset_id, object_type, split),
        )
        fixtures.append(
            Fixture(
                name=str(name),
                baseline_dataset_id=int(dataset_id),
                extension_dataset_id=int(extension_ids[str(name)]),
                object_type=str(object_type),
                from_time=minimum - timedelta(microseconds=1),
                train_to=split,
                test_from=split + timedelta(microseconds=1),
                to_time=maximum + timedelta(microseconds=1),
                source_activity=str(pair[0]),
                target_activity=str(pair[1]),
                slow_threshold=float(threshold),
            )
        )
    return fixtures


def percentile(samples: list[float], percentile_value: float) -> float:
    ordered = sorted(samples)
    index = min(len(ordered) - 1, math.ceil(len(ordered) * percentile_value) - 1)
    return ordered[index]


def serial_order_dictionary(engines: Iterable[str]) -> list[list[str]]:
    return [list(order) for order in itertools.permutations(tuple(engines))]


def serial_latency_method(
    warmups: int,
    samples_per_epoch: int,
    epochs: int,
    engines: Iterable[str],
) -> dict[str, Any]:
    return {
        "epochs": epochs,
        "samples_per_epoch": samples_per_epoch,
        "total_samples_per_arm": samples_per_epoch * epochs,
        "warmups": warmups,
        "clock": "time.perf_counter_ns",
        "percentile": "nearest-rank",
        "aggregation": (
            "pooled p50/p95 with retained per-epoch p50/p95/min/max and "
            "epoch p95 median/range"
        ),
        "raw_evidence": (
            "positive integer nanosecond samples and realized randomized "
            "arm-order codes"
        ),
        "arm_order_dictionary": serial_order_dictionary(engines),
    }


def serial_metrics_ns(samples_ns: list[int]) -> dict[str, Any]:
    if not samples_ns:
        raise ValueError("serial latency requires at least one sample")
    ordered = sorted(samples_ns)
    return {
        "p50_ms": round(statistics.median(ordered) / 1_000_000, 3),
        "p95_ms": round(percentile(ordered, 0.95) / 1_000_000, 3),
        "minimum_ms": round(ordered[0] / 1_000_000, 3),
        "maximum_ms": round(ordered[-1] / 1_000_000, 3),
        "runs": len(ordered),
    }


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def timed_pair(
    baseline_call: Callable[[], Any],
    extension_call: Callable[[], Any],
    warmups: int,
    runs: int,
    rng: random.Random,
    latency_epochs: int = 3,
) -> tuple[dict, Any]:
    calls = {"vanilla_postgres_python": baseline_call, "pg_ocpm_rust": extension_call}
    answers = {name: call() for name, call in calls.items()}
    expected = canonical(answers["vanilla_postgres_python"])
    if canonical(answers["pg_ocpm_rust"]) != expected:
        raise AssertionError("correctness mismatch during untimed preflight")
    for _ in range(warmups):
        order = list(calls)
        rng.shuffle(order)
        for name in order:
            answers[name] = calls[name]()
            if canonical(answers[name]) != expected:
                raise AssertionError(f"correctness mismatch during {name} warmup")
    order_dictionary = serial_order_dictionary(calls)
    order_codes = {tuple(order): code for code, order in enumerate(order_dictionary)}
    samples: dict[str, list[int]] = defaultdict(list)
    exact_samples = {name: 0 for name in calls}
    first_counts = {name: 0 for name in calls}
    serial_epochs = []
    for epoch_index in range(latency_epochs):
        epoch_samples: dict[str, list[int]] = {name: [] for name in calls}
        epoch_order_codes = []
        for _ in range(runs):
            order = list(calls)
            rng.shuffle(order)
            epoch_order_codes.append(order_codes[tuple(order)])
            first_counts[order[0]] += 1
            for name in order:
                started_ns = time.perf_counter_ns()
                answers[name] = calls[name]()
                elapsed_ns = time.perf_counter_ns() - started_ns
                if elapsed_ns <= 0:
                    raise RuntimeError("serial latency clock did not advance")
                epoch_samples[name].append(elapsed_ns)
                samples[name].append(elapsed_ns)
                if canonical(answers[name]) != expected:
                    raise AssertionError(
                        f"correctness mismatch during measured {name} sample"
                    )
                exact_samples[name] += 1
        serial_epochs.append(
            {
                "epoch": epoch_index + 1,
                "order_codes": epoch_order_codes,
                "arms": {
                    name: {
                        **serial_metrics_ns(values),
                        "samples_ns": values,
                    }
                    for name, values in epoch_samples.items()
                },
            }
        )
    measured = {}
    for name, values in samples.items():
        epoch_p95 = [epoch["arms"][name]["p95_ms"] for epoch in serial_epochs]
        measured[name] = {
            **serial_metrics_ns(values),
            "exact_samples": exact_samples[name],
            "epoch_count": latency_epochs,
            "epoch_p95_median_ms": round(statistics.median(epoch_p95), 3),
            "epoch_p95_range_ms": [min(epoch_p95), max(epoch_p95)],
        }
    measured["speedup"] = round(
        measured["vanilla_postgres_python"]["p50_ms"]
        / max(measured["pg_ocpm_rust"]["p50_ms"], 0.000001),
        3,
    )
    measured["correct"] = True
    measured["first_execution_counts"] = first_counts
    measured["serial_epochs"] = serial_epochs
    return measured, answers["pg_ocpm_rust"]


def schema_storage(db: Database, schema: str) -> dict:
    row = db.rows(
        """
        SELECT coalesce(sum(pg_relation_size(c.oid)),0)::bigint AS heap,
               coalesce(sum(pg_indexes_size(c.oid)),0)::bigint AS indexes,
               coalesce(sum(CASE WHEN c.reltoastrelid=0 THEN 0
                    ELSE pg_total_relation_size(c.reltoastrelid) END),0)::bigint
                 AS toast,
               coalesce(sum(pg_total_relation_size(c.oid)),0)::bigint AS total
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=%s AND c.relkind IN ('r','m','p')
        """,
        (schema,),
    )[0]
    return dict(
        zip(("heap_bytes", "index_bytes", "toast_bytes", "total_bytes"), map(int, row))
    )


def database_environment(db: Database) -> dict:
    row = db.rows(
        """
        SELECT current_setting('server_version'),
               current_setting('shared_buffers'),
               current_setting('effective_cache_size'),
               current_setting('work_mem'),
               current_setting('maintenance_work_mem'),
               current_setting('max_parallel_workers_per_gather'),
               current_setting('random_page_cost'),
               current_setting('jit')
        """,
        {},
    )[0]
    return dict(
        zip(
            (
                "postgres_version",
                "shared_buffers",
                "effective_cache_size",
                "work_mem",
                "maintenance_work_mem",
                "max_parallel_workers_per_gather",
                "random_page_cost",
                "jit",
            ),
            row,
        )
    )


CONCURRENCY_ENGINES = ("vanilla_postgres_python", "pg_ocpm_rust")


def concurrency_method(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "epochs_per_engine_level": args.concurrency_epochs,
        "minimum_epoch_seconds": args.concurrency_min_seconds,
        "minimum_requests_per_worker_per_epoch": args.concurrency_requests,
        "connection_model": (
            "prestarted thread workers with one persistent PostgreSQL connection "
            "per worker; connection setup and pool startup excluded"
        ),
        "warmup_gate": "one exact canonical warmup response from every worker",
        "correctness_gate": "exact canonical equality for every measured request",
        "aggregation": (
            "median epoch QPS and median epoch p50/p95/p99; every epoch retained"
        ),
        "arm_order": "deterministic rotation by worker level and epoch",
    }


def concurrency_epoch_metrics(samples: list[float]) -> dict[str, Any]:
    ordered = sorted(samples)
    return {
        "p50_ms": round(statistics.median(ordered) * 1000, 3),
        "p95_ms": round(percentile(ordered, 0.95) * 1000, 3),
        "p99_ms": round(percentile(ordered, 0.99) * 1000, 3),
        "minimum_ms": round(ordered[0] * 1000, 3),
        "maximum_ms": round(ordered[-1] * 1000, 3),
    }


def aggregate_concurrency_epochs(
    workers: int, epochs: list[dict[str, Any]]
) -> dict[str, Any]:
    return {
        "workers": workers,
        "epoch_count": len(epochs),
        "requests": sum(epoch["requests"] for epoch in epochs),
        "runs": sum(epoch["requests"] for epoch in epochs),
        "throughput_qps": round(
            statistics.median(epoch["throughput_qps"] for epoch in epochs), 3
        ),
        "p50_ms": round(statistics.median(epoch["p50_ms"] for epoch in epochs), 3),
        "p95_ms": round(statistics.median(epoch["p95_ms"] for epoch in epochs), 3),
        "p99_ms": round(statistics.median(epoch["p99_ms"] for epoch in epochs), 3),
        "minimum_ms": min(epoch["minimum_ms"] for epoch in epochs),
        "maximum_ms": max(epoch["maximum_ms"] for epoch in epochs),
        "minimum_epoch_wall_ms": min(epoch["wall_ms"] for epoch in epochs),
        "minimum_requests_per_worker": min(
            min(epoch["worker_request_counts"]) for epoch in epochs
        ),
        "correct": all(epoch["correct"] for epoch in epochs),
        "epochs": epochs,
    }


def run_concurrency_epoch(
    args: argparse.Namespace,
    fixture: Fixture,
    expected: str,
    extension: bool,
    drift: bool,
    workers: int,
) -> dict[str, Any]:
    host = args.extension_host if extension else args.baseline_host
    database = args.extension_db if extension else args.baseline_db
    state = threading.local()
    connections: list[Database] = []
    connection_lock = threading.Lock()
    ready = threading.Barrier(workers + 1)
    start_event = threading.Event()
    deadline = [0.0]

    def initialize_worker() -> None:
        state.database = Database(host, database, args.timeout_seconds)
        with connection_lock:
            connections.append(state.database)

    def request() -> tuple[float, str]:
        started = time.perf_counter()
        rows = (
            ext_pair_counts(state.database, fixture)
            if extension
            else base_pair_counts(state.database, fixture)
        )
        transitions = transition_rows(rows)
        if drift:
            answer = (
                native_drift(transitions) if extension else reference_drift(transitions)
            )
        else:
            answer = (
                native_dfg(transitions) if extension else reference_dfg(transitions)
            )
        elapsed = time.perf_counter() - started
        return elapsed, canonical(answer)

    def serve_epoch(_: int) -> dict[str, Any]:
        worker_id = threading.get_ident()
        error = None
        try:
            _elapsed, warm_answer = request()
            if warm_answer != expected:
                error = "warmup correctness fingerprint mismatch"
        except Exception as exc:  # pragma: no cover - exercised by live harness
            error = f"warmup failed: {exc}"
        try:
            ready.wait(timeout=min(args.timeout_seconds, 30))
        except threading.BrokenBarrierError:
            return {"worker_id": worker_id, "error": error or "startup barrier failed"}
        if not start_event.wait(timeout=min(args.timeout_seconds, 30)):
            return {"worker_id": worker_id, "error": error or "start signal timed out"}
        if error is not None:
            return {"worker_id": worker_id, "error": error}

        samples: list[float] = []
        while (
            len(samples) < args.concurrency_requests
            or time.perf_counter() < deadline[0]
        ):
            try:
                elapsed, answer = request()
            except Exception as exc:  # pragma: no cover - exercised by live harness
                return {"worker_id": worker_id, "error": f"request failed: {exc}"}
            if answer != expected:
                return {
                    "worker_id": worker_id,
                    "error": "measured correctness fingerprint mismatch",
                }
            samples.append(elapsed)
        return {"worker_id": worker_id, "samples": samples}

    try:
        with ThreadPoolExecutor(
            max_workers=workers,
            initializer=initialize_worker,
        ) as pool:
            futures = [pool.submit(serve_epoch, slot) for slot in range(workers)]
            try:
                ready.wait(timeout=min(args.timeout_seconds, 30))
            except threading.BrokenBarrierError as exc:
                ready.abort()
                start_event.set()
                raise RuntimeError("not every concurrency worker became ready") from exc
            started = time.perf_counter()
            deadline[0] = started + args.concurrency_min_seconds
            start_event.set()
            worker_results = [future.result() for future in futures]
            wall = time.perf_counter() - started
    finally:
        for connection in connections:
            connection.close()

    errors = [result["error"] for result in worker_results if "error" in result]
    if errors:
        raise AssertionError("; ".join(errors))
    worker_ids = [result["worker_id"] for result in worker_results]
    if len(set(worker_ids)) != workers:
        raise RuntimeError(f"warmed {len(set(worker_ids))}/{workers} thread workers")
    worker_request_counts = [len(result["samples"]) for result in worker_results]
    samples = [sample for result in worker_results for sample in result["samples"]]
    if wall < args.concurrency_min_seconds:
        raise RuntimeError("concurrency epoch ended before its duration floor")
    if min(worker_request_counts) < args.concurrency_requests:
        raise RuntimeError(
            "concurrency epoch ended before its per-worker request floor"
        )
    epoch = {
        **concurrency_epoch_metrics(samples),
        "requests": len(samples),
        "wall_ms": round(wall * 1000, 3),
        "throughput_qps": round(len(samples) / wall, 3),
        "warmed_worker_count": len(set(worker_ids)),
        "worker_ids": sorted(worker_ids),
        "worker_request_counts": sorted(worker_request_counts),
        "answer_sha256": hashlib.sha256(expected.encode()).hexdigest(),
        "correct": True,
    }
    return epoch


def concurrency_comparison(
    args: argparse.Namespace,
    fixture: Fixture,
    expected: str,
    drift: bool = False,
) -> dict[str, Any]:
    levels = [int(value) for value in args.concurrency.split(",") if value]
    epochs: dict[str, dict[str, list[dict[str, Any]]]] = {
        engine: {str(workers): [] for workers in levels}
        for engine in CONCURRENCY_ENGINES
    }
    epoch_arm_orders: dict[str, list[list[str]]] = {
        str(workers): [] for workers in levels
    }
    for level_index, workers in enumerate(levels):
        for epoch_index in range(args.concurrency_epochs):
            offset = (level_index + epoch_index + int(drift)) % len(CONCURRENCY_ENGINES)
            order = CONCURRENCY_ENGINES[offset:] + CONCURRENCY_ENGINES[:offset]
            epoch_arm_orders[str(workers)].append(list(order))
            for arm_position, engine in enumerate(order, start=1):
                print(
                    f"  concurrency {'drift' if drift else 'dfg'} {engine} "
                    f"x{workers} epoch {epoch_index + 1}/{args.concurrency_epochs}",
                    flush=True,
                )
                epoch = run_concurrency_epoch(
                    args,
                    fixture,
                    expected,
                    engine == "pg_ocpm_rust",
                    drift,
                    workers,
                )
                epoch.update(
                    {
                        "epoch": epoch_index + 1,
                        "arm_position": arm_position,
                    }
                )
                epochs[engine][str(workers)].append(epoch)

    return {
        "fixture": fixture.name,
        "workload": "dfg_frequency_drift" if drift else "dfg_conformance_95pct",
        "levels": [str(workers) for workers in levels],
        "epoch_arm_orders": epoch_arm_orders,
        **{
            engine: {
                str(workers): aggregate_concurrency_epochs(
                    workers, epochs[engine][str(workers)]
                )
                for workers in levels
            }
            for engine in CONCURRENCY_ENGINES
        },
    }


def write_artifact(path: Path, result: dict[str, Any]) -> dict[str, Any]:
    payload = dict(result)
    payload.pop("payload_sha256", None)
    encoded = json.dumps(payload, indent=2, default=str) + "\n"
    payload["payload_sha256"] = hashlib.sha256(encoded.encode()).hexdigest()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, default=str) + "\n")
    temporary.replace(path)
    return payload


def load_verified_artifact(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise SystemExit(f"concurrency-only artifact does not exist: {path}")
    result = json.loads(path.read_text())
    recorded = result.get("payload_sha256")
    unsigned = dict(result)
    unsigned.pop("payload_sha256", None)
    computed = hashlib.sha256(
        (json.dumps(unsigned, indent=2, default=str) + "\n").encode()
    ).hexdigest()
    if recorded != computed:
        raise SystemExit(
            f"refusing to update artifact with invalid payload digest: {path}"
        )
    return result


def jsonable(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def preserved_concurrency_only_payload(result: dict[str, Any]) -> dict[str, Any]:
    """Return every field that a concurrency-only refresh must not change."""

    preserved = jsonable(result)
    for key in (
        "payload_sha256",
        "schema_version",
        "generated_at",
        "section_generated_at",
        "concurrency",
        "drift_concurrency",
    ):
        preserved.pop(key, None)
    method = preserved.get("method", {})
    method.pop("concurrency", None)
    method.pop("concurrency_model", None)
    return preserved


def update_concurrency_only(args: argparse.Namespace) -> dict[str, Any]:
    target = Path(args.output)
    result = load_verified_artifact(target)
    if result.get(
        "schema_version"
    ) != PUBLIC_BENCHMARK_SCHEMA_VERSION or "serial_latency" not in result.get(
        "method", {}
    ):
        raise SystemExit(
            "concurrency-only refresh requires a schema-5 artifact with raw "
            "three-epoch serial latency evidence"
        )
    preserved_before = preserved_concurrency_only_payload(result)
    baseline = Database(args.baseline_host, args.baseline_db, args.timeout_seconds)
    extension = Database(args.extension_host, args.extension_db, args.timeout_seconds)
    try:
        version = str(extension.one("SELECT ocpm.version()", {}))
        expected_release = {
            "ocpm_engine": metadata.version("ocpm-engine"),
            "pg_ocpm": version,
        }
        if result.get("release") != expected_release:
            raise SystemExit("concurrency-only artifact release versions changed")
        fixtures = discover_fixtures(baseline, extension)
        expected_fixtures = {
            fixture.name: jsonable(asdict(fixture)) for fixture in fixtures
        }
        stored_fixtures = {
            item["fixture"]["name"]: item["fixture"] for item in result["datasets"]
        }
        if stored_fixtures != expected_fixtures:
            raise SystemExit(
                "concurrency-only fixture does not match existing artifact"
            )
        environment = {
            "client": {
                "python": sys.version.split()[0],
                "platform": platform.platform(),
                "machine": platform.machine(),
                "logical_cpus_visible": os.cpu_count(),
            },
            "vanilla_postgres": database_environment(baseline),
            "pg_ocpm_postgres": database_environment(extension),
        }
        if result.get("environment") != environment:
            raise SystemExit(
                "concurrency-only environment differs from the existing artifact"
            )
        if result.get("provenance") != public_benchmark_provenance():
            raise SystemExit(
                "concurrency-only source, host, or image provenance changed"
            )

        fixture = fixtures[0]
        expected = canonical(
            native_dfg(transition_rows(ext_pair_counts(extension, fixture)))
        )
        drift_expected = canonical(
            native_drift(transition_rows(ext_pair_counts(extension, fixture)))
        )
        result["concurrency"] = concurrency_comparison(args, fixture, expected)
        result["drift_concurrency"] = concurrency_comparison(
            args, fixture, drift_expected, True
        )
        original_generated_at = result["generated_at"]
        result["schema_version"] = PUBLIC_BENCHMARK_SCHEMA_VERSION
        result["generated_at"] = datetime.now(timezone.utc).isoformat()
        result["section_generated_at"] = {
            "latency_and_storage": result.get("section_generated_at", {}).get(
                "latency_and_storage", original_generated_at
            ),
            "concurrency": result["generated_at"],
        }
        result["method"]["concurrency"] = concurrency_method(args)
        if preserved_concurrency_only_payload(result) != preserved_before:
            raise SystemExit(
                "concurrency-only refresh changed latency, storage, fixture, "
                "or other preserved evidence"
            )
        result = write_artifact(target, result)
        print(
            f"updated concurrency only in {target}; latency, storage, and "
            "datasets preserved",
            flush=True,
        )
        return result
    finally:
        baseline.close()
        extension.close()


def benchmark(args: argparse.Namespace) -> dict:
    rng = random.Random(args.seed)
    baseline = Database(args.baseline_host, args.baseline_db, args.timeout_seconds)
    extension = Database(args.extension_host, args.extension_db, args.timeout_seconds)
    version = extension.one("SELECT ocpm.version()", {})
    fixtures = discover_fixtures(baseline, extension)
    datasets = []
    all_speedups = []
    correctness = 0
    first_dfg_answer = None
    for fixture in fixtures:
        print(f"benchmarking {fixture.name} ({fixture.object_type})", flush=True)
        workloads: list[tuple[str, Callable[[], Any], Callable[[], Any]]] = [
            (
                "dfg_conformance_95pct",
                lambda f=fixture: reference_dfg(
                    transition_rows(base_pair_counts(baseline, f))
                ),
                lambda f=fixture: native_dfg(
                    transition_rows(ext_pair_counts(extension, f))
                ),
            ),
            (
                "variant_conformance_95pct",
                lambda f=fixture: variant_result(
                    base_variant_counts(baseline, f), False
                ),
                lambda f=fixture: variant_result(
                    ext_variant_counts(extension, f), True
                ),
            ),
            (
                "next_activity_prediction",
                lambda f=fixture: reference_next(
                    transition_rows(base_pair_counts(baseline, f))
                ),
                lambda f=fixture: native_next(
                    transition_rows(ext_pair_counts(extension, f))
                ),
            ),
            (
                "dfg_frequency_drift",
                lambda f=fixture: reference_drift(
                    transition_rows(base_pair_counts(baseline, f))
                ),
                lambda f=fixture: native_drift(
                    transition_rows(ext_pair_counts(extension, f))
                ),
            ),
            (
                "repeated_transition_rework",
                lambda f=fixture: normalize_rows(
                    baseline.rows(REWORK_BASE, params(f, False))
                ),
                lambda f=fixture: normalize_rows(
                    extension.rows(REWORK_EXT, params(f, True))
                ),
            ),
            (
                "edge_bottleneck_ranking",
                lambda f=fixture: bottleneck(baseline, f, False),
                lambda f=fixture: bottleneck(extension, f, True),
            ),
            (
                "edge_bottleneck_prediction",
                lambda f=fixture: edge_prediction(baseline, f, False),
                lambda f=fixture: edge_prediction(extension, f, True),
            ),
            (
                "edge_duration_time_series",
                lambda f=fixture: duration_series(baseline, f, False),
                lambda f=fixture: duration_series(extension, f, True),
            ),
            (
                "activity_profile",
                lambda f=fixture: activity_profile(baseline, f, False),
                lambda f=fixture: activity_profile(extension, f, True),
            ),
        ]
        rows = []
        for name, baseline_call, extension_call in workloads:
            measured, answer = timed_pair(
                baseline_call,
                extension_call,
                args.warmups,
                args.runs,
                rng,
                args.latency_epochs,
            )
            rows.append({"workload": name, **measured})
            all_speedups.append(measured["speedup"])
            correctness += 1
            if first_dfg_answer is None and name == "dfg_conformance_95pct":
                first_dfg_answer = canonical(answer)
            print(f"  {name}: {measured['speedup']:.2f}x", flush=True)
        datasets.append({"fixture": asdict(fixture), "workloads": rows})

    concurrency_fixture = fixtures[0]
    expected = canonical(
        native_dfg(transition_rows(ext_pair_counts(extension, concurrency_fixture)))
    )
    concurrency = concurrency_comparison(args, concurrency_fixture, expected)
    drift_expected = canonical(
        native_drift(transition_rows(ext_pair_counts(extension, concurrency_fixture)))
    )
    drift_concurrency = concurrency_comparison(
        args, concurrency_fixture, drift_expected, True
    )
    storage = {
        "vanilla_postgres": schema_storage(baseline, "ocel"),
        "pg_ocpm": schema_storage(extension, "ocpm"),
    }
    environment = {
        "client": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "machine": platform.machine(),
            "logical_cpus_visible": os.cpu_count(),
        },
        "vanilla_postgres": database_environment(baseline),
        "pg_ocpm_postgres": database_environment(extension),
    }
    baseline.close()
    extension.close()
    geometric = math.exp(
        sum(math.log(value) for value in all_speedups) / len(all_speedups)
    )
    result = {
        "schema_version": PUBLIC_BENCHMARK_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "release": {
            "ocpm_engine": metadata.version("ocpm-engine"),
            "pg_ocpm": str(version),
        },
        "source": {
            "title": "Collection of Object-Centric Event Logs (SAP IDES O2C and P2P)",
            "doi": "10.5281/zenodo.8261133",
            "license": "CC BY 4.0",
        },
        "environment": environment,
        "provenance": public_benchmark_provenance(),
        "method": {
            "warmups": args.warmups,
            "measured_runs": args.runs * args.latency_epochs,
            "serial_latency": serial_latency_method(
                args.warmups,
                args.runs,
                args.latency_epochs,
                CONCURRENCY_ENGINES,
            ),
            "random_seed": args.seed,
            "latency_scope": (
                "database extraction/aggregation plus model construction and scoring"
            ),
            "baseline": (
                "indexed relational OCEL in vanilla PostgreSQL plus independent "
                "Python reference kernels"
            ),
            "candidate": "pg_ocpm compact aggregates plus ocpm-engine Rust kernels",
            "correctness_gate": "canonical result equality before latency inclusion",
            "execution_order": "randomized per measured pair with recorded seed",
            "concurrency": concurrency_method(args),
            "storage": "sum of PostgreSQL heap, index, and TOAST relation bytes",
        },
        "summary": {
            "correct_workloads": correctness,
            "total_workloads": len(fixtures) * 9,
            "geometric_mean_speedup": round(geometric, 3),
            "minimum_speedup": round(min(all_speedups), 3),
            "target_speedup": 10.0,
            "target_met": geometric >= 10.0 and min(all_speedups) >= 10.0,
        },
        "datasets": datasets,
        "storage": storage,
        "concurrency": concurrency,
        "drift_concurrency": drift_concurrency,
    }
    result["section_generated_at"] = {
        "latency_and_storage": result["generated_at"],
        "concurrency": result["generated_at"],
    }
    target = Path(args.output)
    result = write_artifact(target, result)
    print(f"wrote {target}; geometric mean {geometric:.2f}x", flush=True)
    if geometric < 10.0 or min(all_speedups) < 10.0:
        raise SystemExit(
            "release gate failed: geometric mean and every workload must be "
            f"at least 10.00x (geomean={geometric:.2f}x, "
            f"minimum={min(all_speedups):.2f}x)"
        )
    return result


if __name__ == "__main__":
    parsed_args = parse_args()
    if parsed_args.concurrency_only:
        update_concurrency_only(parsed_args)
    else:
        benchmark(parsed_args)
