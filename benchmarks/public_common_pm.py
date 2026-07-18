"""Public SAP OCEL common-PM benchmark: vanilla PostgreSQL versus pg_ocpm + Rust.

The benchmark uses the CC BY 4.0 SAP IDES O2C and P2P logs published at
doi:10.5281/zenodo.8261133.  The database fixture is produced by Dendrites'
``benchmark_open_ocel.py`` loader.  Every timed result passes a deterministic
cross-engine output gate before it contributes to latency or concurrency claims.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import random
import statistics
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import psycopg2

from ocpm_engine import (
    TransitionCount,
    bottleneck_order,
    dfg_conformance,
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
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--runs", type=int, default=9)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--seed", type=int, default=20260718)
    parser.add_argument("--concurrency", default="1,4,8,16")
    parser.add_argument("--concurrency-requests", type=int, default=32)
    parser.add_argument("--output", default="docs/results/public-common-pm-0.2.0.json")
    args = parser.parse_args()
    if args.database:
        args.baseline_db = args.database
        args.extension_db = args.database
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
        if winners[(row.source, row.edge_type)][0] == row.target
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


def metrics(samples: list[float]) -> dict:
    return {
        "p50_ms": round(statistics.median(samples) * 1000, 3),
        "p95_ms": round(percentile(samples, 0.95) * 1000, 3),
        "minimum_ms": round(min(samples) * 1000, 3),
        "runs": len(samples),
    }


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def timed_pair(
    baseline_call: Callable[[], Any],
    extension_call: Callable[[], Any],
    warmups: int,
    runs: int,
    rng: random.Random,
) -> tuple[dict, Any]:
    calls = {"vanilla_postgres_python": baseline_call, "pg_ocpm_rust": extension_call}
    answers: dict[str, Any] = {}
    for _ in range(warmups):
        order = list(calls)
        rng.shuffle(order)
        for name in order:
            answers[name] = calls[name]()
    samples: dict[str, list[float]] = defaultdict(list)
    for _ in range(runs):
        order = list(calls)
        rng.shuffle(order)
        for name in order:
            started = time.perf_counter()
            answers[name] = calls[name]()
            samples[name].append(time.perf_counter() - started)
    if canonical(answers["vanilla_postgres_python"]) != canonical(
        answers["pg_ocpm_rust"]
    ):
        raise AssertionError(
            "correctness mismatch\nbaseline="
            + canonical(answers["vanilla_postgres_python"])
            + "\nextension="
            + canonical(answers["pg_ocpm_rust"])
        )
    measured = {name: metrics(values) for name, values in samples.items()}
    measured["speedup"] = round(
        measured["vanilla_postgres_python"]["p50_ms"]
        / max(measured["pg_ocpm_rust"]["p50_ms"], 0.000001),
        3,
    )
    measured["correct"] = True
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


def concurrency_sweep(
    args: argparse.Namespace,
    fixture: Fixture,
    expected: str,
    extension: bool,
) -> dict:
    levels = [int(value) for value in args.concurrency.split(",") if value]
    host = args.extension_host if extension else args.baseline_host

    def task() -> float:
        database = args.extension_db if extension else args.baseline_db
        db = Database(host, database, args.timeout_seconds)
        started = time.perf_counter()
        rows = (
            ext_pair_counts(db, fixture) if extension else base_pair_counts(db, fixture)
        )
        answer = (
            native_dfg(transition_rows(rows))
            if extension
            else reference_dfg(transition_rows(rows))
        )
        elapsed = time.perf_counter() - started
        db.close()
        if canonical(answer) != expected:
            raise AssertionError("concurrency correctness fingerprint mismatch")
        return elapsed

    results = {}
    for workers in levels:
        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            samples = list(pool.map(lambda _: task(), range(args.concurrency_requests)))
        wall = time.perf_counter() - started
        results[str(workers)] = {
            **metrics(samples),
            "requests": args.concurrency_requests,
            "throughput_qps": round(args.concurrency_requests / wall, 3),
        }
    return results


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
        ]
        rows = []
        for name, baseline_call, extension_call in workloads:
            measured, answer = timed_pair(
                baseline_call, extension_call, args.warmups, args.runs, rng
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
    concurrency = {
        "fixture": concurrency_fixture.name,
        "workload": "dfg_conformance_95pct",
        "vanilla_postgres_python": concurrency_sweep(
            args, concurrency_fixture, expected, False
        ),
        "pg_ocpm_rust": concurrency_sweep(args, concurrency_fixture, expected, True),
    }
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
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "release": {"ocpm_engine": "0.2.0", "pg_ocpm": str(version)},
        "source": {
            "title": "Collection of Object-Centric Event Logs (SAP IDES O2C and P2P)",
            "doi": "10.5281/zenodo.8261133",
            "license": "CC BY 4.0",
        },
        "environment": environment,
        "method": {
            "warmups": args.warmups,
            "measured_runs": args.runs,
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
            "concurrency": (
                "32 requests per level; latency excludes connection setup, while "
                "throughput wall time includes it"
            ),
            "storage": "sum of PostgreSQL heap, index, and TOAST relation bytes",
        },
        "summary": {
            "correct_workloads": correctness,
            "total_workloads": len(fixtures) * 7,
            "geometric_mean_speedup": round(geometric, 3),
            "minimum_speedup": round(min(all_speedups), 3),
            "target_speedup": 10.0,
            "target_met": geometric >= 10.0 and min(all_speedups) >= 10.0,
        },
        "datasets": datasets,
        "storage": storage,
        "concurrency": concurrency,
    }
    encoded = json.dumps(result, indent=2, default=str) + "\n"
    result["payload_sha256"] = hashlib.sha256(encoded.encode()).hexdigest()
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(result, indent=2, default=str) + "\n")
    print(f"wrote {target}; geometric mean {geometric:.2f}x", flush=True)
    if geometric < 10.0 or min(all_speedups) < 10.0:
        raise SystemExit(
            "release gate failed: geometric mean and every workload must be "
            f"at least 10.00x (geomean={geometric:.2f}x, "
            f"minimum={min(all_speedups):.2f}x)"
        )
    return result


if __name__ == "__main__":
    benchmark(parse_args())
