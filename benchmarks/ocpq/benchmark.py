#!/usr/bin/env python3
"""Correctness-gated pg_ocpm + ocpm-engine comparison with published OCPQ."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from pathlib import Path

import psycopg2

from ocpm_engine import binding_capsule_info

PUBLISHED_OCPQ_RUNS_MS = {
    "Q1": [
        35.203509,
        34.680627,
        31.663711,
        37.800265,
        38.660466,
        36.360900,
        40.699492,
        41.570359,
        39.841553,
        37.787278,
    ],
    "Q2": [
        51.749260,
        56.473427,
        54.737583,
        60.586897,
        56.307774,
        57.108496,
        58.469437,
        66.073362,
        61.118326,
        56.056350,
    ],
    "Q3": [
        24.276018,
        22.902032,
        23.246654,
        22.058668,
        21.426379,
        23.920258,
        23.866331,
        22.628289,
        22.522565,
        21.787929,
    ],
    "Q4": [
        62.206418,
        65.713123,
        62.389904,
        61.365485,
        75.569389,
        71.414191,
        67.572819,
        72.593478,
        65.629224,
        66.597629,
    ],
    "Q5": [
        72.352029,
        70.940593,
        62.733944,
        66.587799,
        68.278073,
        69.040443,
        64.476313,
        62.233756,
        73.379683,
        75.391229,
    ],
    "Q6": [
        119.508900,
        66.745212,
        90.156047,
        77.105850,
        90.136674,
        98.089114,
        65.210890,
        64.798652,
        79.722797,
        92.941445,
    ],
    "Q7": [
        104.035426,
        75.295818,
        83.486755,
        75.069721,
        80.089326,
        70.291946,
        76.338114,
        66.614761,
        84.770065,
        82.535726,
    ],
}

DESCRIPTIONS = {
    "Q1": "application submitted exactly once",
    "Q2": "offer returned after creation",
    "Q3": "returned event has exactly one offer",
    "Q4": "associated offer accepted after application",
    "Q5": "accepting resource creates every offer",
    "Q6": "maximum creation-to-acceptance delay",
    "Q7": "offer pairs and creation events",
}

CAPSULE_QUERIES = {
    "Q1": "SELECT ocpm.binding_object_activity_count("
    "ocpm.dataset_id('bpic2017-ocpq'),1,'Application','A_Submitted',1,1)",
    "Q2": "SELECT ocpm.binding_requires_activity("
    "ocpm.dataset_id('bpic2017-ocpq'),1,'Offer','O_Created','O_Returned')",
    "Q3": "SELECT ocpm.binding_event_object_count("
    "ocpm.dataset_id('bpic2017-ocpq'),1,'Offer','O_Returned',1,1)",
    "Q4": "SELECT ocpm.binding_neighbor_eventually("
    "ocpm.dataset_id('bpic2017-ocpq'),1,'Application','A_Accepted',"
    "'Offer','O_Accepted')",
    "Q5": "SELECT ocpm.binding_neighbor_actor_equal("
    "ocpm.dataset_id('bpic2017-ocpq'),1,'Application','A_Accepted',"
    "'Offer','O_Created')",
    "Q6": "SELECT ocpm.binding_max_activity_delay("
    "ocpm.dataset_id('bpic2017-ocpq'),1,'Offer','O_Created','O_Accepted')",
    "Q7": "SELECT ocpm.binding_neighbor_pairs("
    "ocpm.dataset_id('bpic2017-ocpq'),1,'Application','Offer','O_Created')",
}

DECODE_SELECTS = {
    "Q1": "binding_ids[1],violated",
    "Q2": "binding_ids[1],binding_ids[2],violated",
    "Q3": "binding_ids[1],violated",
    "Q4": "binding_ids[1],binding_ids[2],violated",
    "Q5": "binding_ids[1],label,binding_ids[2],violated",
    "Q6": "value",
    "Q7": "binding_ids[1],binding_ids[2],binding_ids[3],binding_ids[4],binding_ids[5]",
}

EXPECTED = {
    "Q1": (
        31509,
        "b74e202f0ed23a23c5029a59a8864dc53e95ed5ee8bc6d5c7d2a16d2faa56115",
        11086,
    ),
    "Q2": (
        42995,
        "f1de4c51eb88f25c95dcfc701b6bfb4508fd022fffd9e172065e1fe5a085af5a",
        19690,
    ),
    "Q3": (
        23305,
        "6b1108a6aeb16cc736b79b26296bf95aa52d29e39c1cfb0f045da68b6bc7057b",
        0,
    ),
    "Q4": (
        31509,
        "ca4639b3de79a3dba90c8dcb09d0a838f505e37c792426787332baa672197b53",
        14281,
    ),
    "Q5": (
        31509,
        "4f82dfcfc8e47fabe853724802d1d9d37688f84aedcad048b5428ed13963bec7",
        6429,
    ),
    "Q6": (1, "b70fe5279f9a7e1315406879eddb63a72583b2a45859240ef3a9d4811afdecb6", None),
    "Q7": (
        74771,
        "428d92c918867c2f970ca3fe579e7accd3aec778bfb66e42a453d22a911c39fc",
        None,
    ),
}


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=os.getenv("OCPM_HOST", "postgres_ocpm"))
    parser.add_argument("--database", default="postgres")
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password", default="pg")
    parser.add_argument("--warmups", type=int, default=10)
    parser.add_argument("--runs", type=int, default=50)
    parser.add_argument("--concurrency", default="1,4,8,16")
    parser.add_argument("--requests-per-worker", type=int, default=1000)
    parser.add_argument("--output", default="docs/results/ocpq-bpic2017-0.4.0.json")
    return parser.parse_args()


def connect(args: argparse.Namespace):
    connection = psycopg2.connect(
        host=args.host,
        dbname=args.database,
        user=args.user,
        password=args.password,
        connect_timeout=15,
    )
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute("SET jit=off")
    return connection


def canonical_value(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float):
        return round(value, 6)
    return value


def correctness(connection, name: str) -> dict:
    query = (
        f"SELECT {DECODE_SELECTS[name]} FROM ocpm.binding_capsule_rows("
        f"({CAPSULE_QUERIES[name]}))"
    )
    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
    canonical = sorted(
        [tuple(canonical_value(value) for value in row) for row in rows],
        key=lambda row: json.dumps(row, sort_keys=True, default=str),
    )
    digest = hashlib.sha256(
        json.dumps(canonical, separators=(",", ":"), default=str).encode()
    ).hexdigest()
    violations = (
        sum(row[-1] is True for row in rows)
        if name in {"Q1", "Q2", "Q3", "Q4", "Q5"}
        else None
    )
    expected_rows, expected_digest, expected_violations = EXPECTED[name]
    if (len(rows), digest, violations) != (
        expected_rows,
        expected_digest,
        expected_violations,
    ):
        raise AssertionError(f"{name} correctness mismatch")
    result = {"rows": len(rows), "sha256": digest}
    if violations is not None:
        result["violations"] = violations
    if name == "Q6":
        result["value"] = canonical[0][0]
    return result


def timed_capsule(connection, query: str) -> tuple[int, float]:
    with connection.cursor() as cursor:
        started = time.perf_counter_ns()
        cursor.execute(query)
        capsule = bytes(cursor.fetchone()[0])
        binding_capsule_info(capsule)
        elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
    return len(capsule), elapsed_ms


def percentile(samples: list[float], fraction: float) -> float:
    ordered = sorted(samples)
    return ordered[min(len(ordered) - 1, math.ceil(len(ordered) * fraction) - 1)]


def latency(args: argparse.Namespace, connection) -> dict:
    result = {}
    for name, query in CAPSULE_QUERIES.items():
        fingerprint = correctness(connection, name)
        for _ in range(args.warmups):
            capsule_bytes, _ = timed_capsule(connection, query)
        samples = [timed_capsule(connection, query)[1] for _ in range(args.runs)]
        candidate = statistics.fmean(samples)
        published = statistics.fmean(PUBLISHED_OCPQ_RUNS_MS[name])
        result[name] = {
            "description": DESCRIPTIONS[name],
            "published_ocpq_mean_ms": round(published, 3),
            "pg_ocpm_ocpm_engine_mean_ms": round(candidate, 3),
            "speedup_vs_published_ocpq": round(published / candidate, 3),
            "capsule_bytes": capsule_bytes,
            "correctness": fingerprint,
            "runs_ms": [round(sample, 3) for sample in samples],
        }
    return result


def concurrency(args: argparse.Namespace) -> dict:
    def worker(worker_id: int, workers: int):
        connection = connect(args)
        barrier = barriers[workers]
        barrier.wait()
        samples = []
        with connection.cursor() as cursor:
            for request in range(args.requests_per_worker):
                name = tuple(CAPSULE_QUERIES)[(worker_id + request) % 7]
                started = time.perf_counter_ns()
                cursor.execute(CAPSULE_QUERIES[name])
                binding_capsule_info(bytes(cursor.fetchone()[0]))
                samples.append((time.perf_counter_ns() - started) / 1_000_000)
            cursor.execute(
                "SELECT coalesce(sum(total_bytes),0)::bigint "
                "FROM pg_backend_memory_contexts"
            )
            backend_memory = int(cursor.fetchone()[0])
        connection.close()
        return samples, backend_memory

    levels = [int(value) for value in args.concurrency.split(",") if value]
    barriers = {level: threading.Barrier(level + 1) for level in levels}
    result = {}
    for workers in levels:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(worker, index, workers) for index in range(workers)]
            barriers[workers].wait()
            started = time.perf_counter()
            measured = [future.result() for future in futures]
            elapsed = time.perf_counter() - started
        samples = [sample for item, _ in measured for sample in item]
        result[str(workers)] = {
            "requests": len(samples),
            "throughput_qps": round(len(samples) / elapsed, 3),
            "latency_p50_ms": round(statistics.median(samples), 3),
            "latency_p95_ms": round(percentile(samples, 0.95), 3),
            "latency_p99_ms": round(percentile(samples, 0.99), 3),
            "backend_memory_bytes_after_request": [item[1] for item in measured],
        }
    return result


def storage(connection) -> dict:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT coalesce(sum(pg_total_relation_size(c.oid)),0)::bigint,"
            "coalesce(sum(pg_indexes_size(c.oid)),0)::bigint "
            "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='ocpm' AND c.relkind IN ('r','m','p')"
        )
        total, indexes = map(int, cursor.fetchone())
        cursor.execute(
            "SELECT coalesce(sum(pg_total_relation_size(c.oid)),0)::bigint "
            "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='ocpm' AND c.relkind='r' "
            "AND c.relname LIKE 'binding_%'"
        )
        binding = int(cursor.fetchone()[0])
    return {
        "total_bytes": total,
        "index_bytes": indexes,
        "binding_total_bytes": binding,
    }


def main() -> None:
    args = arguments()
    connection = connect(args)
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT ocpm.version(),current_setting('server_version'),"
            "current_setting('shared_buffers'),current_setting('work_mem')"
        )
        extension, postgres, shared_buffers, work_mem = cursor.fetchone()
    results = latency(args, connection)
    artifact = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "release": {"pg_ocpm": extension, "ocpm_engine": "0.4.0"},
        "source": {
            "ocpq_eval_commit": "846dd4eb9f8600ae42355968453a9412ea4759c2",
            "ocpq_version": "0.6.7",
            "ocpq_commit": "80457e561edd7bb9e142d959dd7e0f96e6b03f2f",
            "dataset": "BPIC 2017-derived OCEL 2.0 SQLite",
            "dataset_zip_sha256": (
                "a5f422c72b0a911bd64383079f9faebfc247e3e5a217f30705ff9969e8547f2b"
            ),
        },
        "method": {
            "warmups": args.warmups,
            "measured_runs": args.runs,
            "published_ocpq": (
                "arithmetic mean of the ten author-published evaluation samples"
            ),
            "candidate": (
                "PostgreSQL execution, one binary capsule fetched over the wire, "
                "and native Rust decoding into an in-memory binding structure"
            ),
            "correctness": (
                "complete SQL expansion outside the timed region, checked by exact "
                "row count, violation/value totals, and canonical SHA-256"
            ),
            "comparison_scope": "published OCPQ versus pg_ocpm plus ocpm-engine",
        },
        "environment": {
            "postgres": postgres,
            "shared_buffers": shared_buffers,
            "work_mem": work_mem,
            "python": platform.python_version(),
            "machine": platform.machine(),
            "logical_cpus_visible": os.cpu_count(),
        },
        "queries": results,
        "summary": {
            "geometric_mean_published_ocpq_ms": round(
                math.exp(
                    sum(
                        math.log(item["published_ocpq_mean_ms"])
                        for item in results.values()
                    )
                    / 7
                ),
                3,
            ),
            "geometric_mean_candidate_ms": round(
                math.exp(
                    sum(
                        math.log(item["pg_ocpm_ocpm_engine_mean_ms"])
                        for item in results.values()
                    )
                    / 7
                ),
                3,
            ),
            "geometric_mean_speedup_vs_published_ocpq": round(
                math.exp(
                    sum(
                        math.log(item["speedup_vs_published_ocpq"])
                        for item in results.values()
                    )
                    / 7
                ),
                3,
            ),
            "minimum_speedup_vs_published_ocpq": min(
                item["speedup_vs_published_ocpq"] for item in results.values()
            ),
            "all_queries_at_least_10x": all(
                item["speedup_vs_published_ocpq"] >= 10 for item in results.values()
            ),
        },
        "concurrency": {
            "workload": "round-robin Q1-Q7",
            "requests_per_worker": args.requests_per_worker,
            "results": concurrency(args),
        },
        "storage": {
            "published_ocpq": None,
            "published_ocpq_note": (
                "The OCPQ evaluation does not publish storage measurements."
            ),
            "pg_ocpm": storage(connection),
        },
    }
    connection.close()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(artifact, indent=2) + "\n")
    print(json.dumps(artifact["summary"], indent=2))
    if not artifact["summary"]["all_queries_at_least_10x"]:
        raise SystemExit("release gate failed: every query must be at least 10x")


if __name__ == "__main__":
    main()
