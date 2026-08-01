#!/usr/bin/env python3
"""Fail-closed certification for the strict all-node OCPQ comparison."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import statistics
from datetime import datetime
from pathlib import Path
from typing import Any

QUERY_NAMES = tuple(f"Q{index}" for index in range(1, 8))
QUERY_NAME_SET = set(QUERY_NAMES)

REFERENCE_PATH = Path("docs/results/ocpq-reproduced-strict-all-node-0.9.0.json")
CANDIDATE_PATH = Path("docs/results/ocpq-bpic2017-pg_ocpm-0.9.0-ocpm-engine-0.9.0.json")

# Release validation pins the reviewed artifacts below. Preview validation
# always requires explicit digests calculated from the ignored staging files.
PUBLISHED_REFERENCE_SHA256: str | None = (
    "39894339697421834a652620406152c87a92b08831c3a68dc3f30acd6dc77964"
)
PUBLISHED_CANDIDATE_SHA256: str | None = (
    "317af340bff890551c1dfcafe0e0fc8777ade938865173362d56d20073222f1e"
)

EXPECTED_RELEASE = {"pg_ocpm": "0.9.0", "ocpm_engine": "0.9.0"}
EXPECTED_SOURCE = {
    "ocpq_eval_commit": "846dd4eb9f8600ae42355968453a9412ea4759c2",
    "ocpq_version": "0.6.7",
    "ocpq_commit": "80457e561edd7bb9e142d959dd7e0f96e6b03f2f",
    "docker_image": "ocpq:0.6.7-corrected-harness-0.9-final",
    "dataset_sqlite_sha256": (
        "02ac333a2c194b5a411cb8527dd64b4845e5110752d2ffddb531e48ce97556d7"
    ),
    "query_files_sha256": (
        "387aeb31398d86ef8e7b15393649cbabe75be56185fd67d27021744025873466"
    ),
    "author_published_results_commit": ("846dd4eb9f8600ae42355968453a9412ea4759c2"),
}

WARMUPS = 0
RUNS = 10
MINIMUM_QUERY_SPEEDUP = 5.0
MINIMUM_GEOMETRIC_MEAN_SPEEDUP = 10.0
MAXIMUM_TOTAL_SERVING_BYTES = 128 * 1024 * 1024
MAXIMUM_INDEX_BYTES = 16 * 1024 * 1024
MAXIMUM_BINDING_BYTES = 8 * 1024 * 1024
MAXIMUM_CLIENT_VMHWM_OVER_BASELINE_BYTES = 8 * 1024 * 1024
CONCURRENCY_LEVELS = (1, 4, 8, 16)
CONCURRENCY_EPOCHS = 3
MINIMUM_CONCURRENCY_REQUESTS_PER_CLIENT = 32
MAXIMUM_CONCURRENCY_REQUESTS_PER_CLIENT = 250_000
MINIMUM_CONCURRENCY_WALL_MS = 5_000.0
MAXIMUM_CONCURRENCY_THROUGHPUT_CV = 0.15
MINIMUM_CONCURRENCY_SCALING = 5.0
MAXIMUM_CONCURRENCY_MEDIAN_P95_MS = 10.0

CONSTRAINT_REASON = '{"ConstraintNotSatisfied":0}'
EXPECTED_Q6_ROOT_LABEL = {"type": "string", "value": "3140h7m38s"}
EXPECTED_Q6_DURATION_MICROSECONDS = 11_304_458_000_000

REFERENCE_TIMING_BOUNDARY = (
    "OCPQ tree.evaluate plus construction and collection of every node's "
    "EvaluationResultWithCount structures"
)
REFERENCE_CORRECTNESS_BOUNDARY = (
    "every node's complete object/event bindings, typed labels, exact violation "
    "reason, external-ID canonicalization, sorting, compact-JSON serialization, "
    "and SHA-256 outside the timed region"
)
CANDIDATE_TIMING_BOUNDARY = (
    "one prepared PostgreSQL generation/fetch of all root-first bytea columns, "
    "OCPB decode, and complete owned materialization of every node including "
    "exact violations and the typed Q6 label"
)
CANDIDATE_CORRECTNESS_BOUNDARY = (
    "outside the clock: external-ID conversion, duplicate-preserving canonical "
    "row sorting, compact JSON, SHA-256, every-node manifest comparison, and "
    "independent Q6 child-duration validation"
)
CANDIDATE_QUERY_PROTOCOL = (
    "exact upstream OCPQ primary protocol: zero warmups and ten direct measured "
    "evaluations per Q1-Q7"
)
CANDIDATE_PROCESS_SCOPE = (
    "one selected query in a fresh candidate process; publication aggregation "
    "requires one fresh container for each Q1-Q7"
)
CANDIDATE_P50_ESTIMATOR = (
    "conventional median; the middle two samples are averaged for the even "
    "ten-run sample"
)
CANDIDATE_P95_ESTIMATOR = (
    "nearest-rank p95; with ten measured runs this is the maximum sample"
)
MEMORY_MODE = "fresh-process-per-query-peak-rss-vmhwm"
MEMORY_MEASUREMENT_BOUNDARY = (
    "fresh Linux process: baseline after connection, provenance query, and one "
    "prepared statement; then one PostgreSQL generation/fetch, every OCPB node "
    "decode, and complete owned-tree materialization; RSS and VmHWM sampled "
    "while the entire tree remains live and before external-ID maps or "
    "canonicalization are loaded"
)
MEMORY_CORRECTNESS_BOUNDARY = (
    "after the memory sample, load external-ID maps and require "
    "duplicate-preserving canonical JSON SHA-256 and manifest parity for every "
    "node, including exact violations and Q6 typed label/child-derived duration"
)
BACKEND_MEMORY_SCOPE = (
    "non-peak retained-memory diagnostic from sum(total_bytes) in "
    "pg_backend_memory_contexts on the same PostgreSQL connection, sampled "
    "before and after the candidate request; client VmHWM is the peak-memory gate"
)
STORAGE_BOUNDARY = (
    "untimed serving footprint from pg_total_relation_size/pg_indexes_size for "
    "every ocpm schema serving relation, with relation-level evidence"
)
STORAGE_SCOPE = (
    "sum of pg_total_relation_size and pg_indexes_size for every ordinary, "
    "materialized-view, and partitioned relation in schema ocpm; database size "
    "is reported separately as a diagnostic"
)
CONCURRENCY_BOUNDARY = (
    "per-client persistent PostgreSQL connection and prepared Q1-Q7 tree set; "
    "every timed request includes PostgreSQL generation/fetch, decode of every "
    "OCPB node, and complete owned-tree materialization; canonicalization is "
    "excluded"
)
CONCURRENCY_PROTOCOL = (
    "fixed 1/4/8/16 clients, three epochs per level, each epoch at least five "
    "seconds and at least 32 requests per client; requests rotate Q1-Q7; every "
    "client performs exact every-node canonical parity for Q1-Q7 both before "
    "and after every timed epoch"
)
CONCURRENCY_RAW_EVIDENCE = (
    "positive integer nanosecond latency for every request, grouped by client ID "
    "in request order; zero-based array index is the client request ID and "
    "query=Q1..Q7[(client_id+request_id)%7]"
)
RESOURCE_CORRECTNESS_BOUNDARY = (
    "duplicate-preserving external-ID canonical JSON, exact violation reasons, "
    "typed Q6 label and child-derived duration, and every-node SHA-256/manifests"
)
RESOURCE_P50_ESTIMATOR = (
    "conventional median with middle-pair averaging for even sample counts"
)
RESOURCE_TAIL_ESTIMATOR = "nearest-rank"


class CertificationError(ValueError):
    """Raised when an artifact cannot satisfy the strict contract."""


def fail(message: str) -> None:
    raise CertificationError(message)


def node(
    objects: list[int],
    events: list[int],
    labels: list[str],
    situations: int,
    violations: int,
    canonical_json_bytes: int,
    canonical_sha256: str,
) -> dict[str, Any]:
    return {
        "object_variables": objects,
        "event_variables": events,
        "label_names": labels,
        "situation_count": situations,
        "situation_violated_count": violations,
        "violation_reason_counts": (
            {CONSTRAINT_REASON: violations} if violations else {}
        ),
        "canonical_json_bytes": canonical_json_bytes,
        "canonical_sha256": canonical_sha256,
    }


EXPECTED_NODES = {
    "Q1": [
        node(
            [0],
            [],
            [],
            31_509,
            11_086,
            1_179_826,
            "9eb852a60ab7b2ff4a1d989ea6abed9c516b185bc4b107ee1f61810a7512e4a9",
        ),
        node(
            [0],
            [0],
            [],
            20_423,
            0,
            838_966,
            "5adba64ba4fd2de7a84679f18727bed084fb2894772428f40924da9bfcbb666d",
        ),
    ],
    "Q2": [
        node(
            [0],
            [0],
            [],
            42_995,
            19_690,
            2_238_618,
            "3825e5116765ef3ce2df9b46d42d5a52ff87e9644ec89eab3182749123fc080b",
        ),
        node(
            [0],
            [0, 1],
            [],
            23_305,
            0,
            1_262_010,
            "cd75b910584db03c8b5f86740ea958e4076c82eb523829ecc57f350533c1aed6",
        ),
    ],
    "Q3": [
        node(
            [],
            [0],
            [],
            23_305,
            0,
            654_311,
            "0583f0d3c618765d51f689f8f5d1dc957a5266590d54c5b1c073fe8be0f48929",
        ),
        node(
            [1],
            [0],
            [],
            23_305,
            0,
            957_276,
            "af08f2b6b3e85bc2ae90e0b0580de98aa2ea4e9355e9f0774faed0d5d80ffb3b",
        ),
    ],
    "Q4": [
        node(
            [0],
            [0],
            [],
            31_509,
            14_281,
            1_636_959,
            "9af254c016bf0be2582045d1678681d95bd6bd13ed233d0a2a1a71d25e3d33a4",
        ),
        node(
            [0, 1],
            [0, 1],
            [],
            17_228,
            0,
            1_174_066,
            "4339d5f999c37a25f10680cec37ee8cdf4e40c7c7b1f906c491ed5cc860b6483",
        ),
    ],
    "Q5": [
        node(
            [0, 1],
            [0],
            [],
            31_509,
            6_429,
            1_889_637,
            "16689700f551e2de34c1977329b4c544456268f411a690675da87de6210e607d",
        ),
        node(
            [0, 1, 2],
            [0, 1],
            [],
            42_995,
            8_100,
            3_726_515,
            "6087f5fa8c0ea8e88e30abc061da7df6a44d9b1c27c6fa8110a75fde04fb734d",
        ),
    ],
    "Q6": [
        node(
            [],
            [],
            ["max_dur"],
            1,
            0,
            67,
            "4eb17ee6aaa5d37cac8ce9949978ade11d78abd1abd8aef8aa3ebe1506cce0a3",
        ),
        node(
            [0],
            [0, 1],
            [],
            17_228,
            0,
            932_874,
            "b621a19536fce92938375c293be1b6250de483da909d44e8a3ccf37b7af3fb43",
        ),
    ],
    "Q7": [
        node(
            [0, 1, 2],
            [1, 2],
            [],
            74_771,
            0,
            6_143_143,
            "b3f5f953f35d7454e31b61f29e79d7f783eba4c7e8b21598da7e4d21060f0370",
        )
    ],
}


def is_integer(value: object) -> bool:
    return type(value) is int


def is_number(value: object) -> bool:
    return type(value) in {int, float} and math.isfinite(value)


def require_mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(f"{label} must be an object")
    return value


def require_exact_keys(value: object, expected: set[str], label: str) -> dict[str, Any]:
    mapping = require_mapping(value, label)
    if set(mapping) != expected:
        fail(f"{label} must contain exactly {', '.join(sorted(expected))}")
    return mapping


def require_bool(value: object, label: str) -> bool:
    if type(value) is not bool:
        fail(f"{label} must be a boolean")
    return value


def require_integer(value: object, label: str, *, minimum: int = 0) -> int:
    if not is_integer(value) or value < minimum:
        fail(f"{label} must be an integer >= {minimum}, found {value!r}")
    return value


def require_number(value: object, label: str, *, minimum: float = 0.0) -> float:
    if not is_number(value) or value < minimum:
        fail(f"{label} must be finite and >= {minimum}, found {value!r}")
    return float(value)


def require_positive(value: object, label: str) -> float:
    number = require_number(value, label)
    if number <= 0:
        fail(f"{label} must be positive")
    return number


def require_sha256(value: object, label: str, *, prefix: bool = False) -> str:
    pattern = r"sha256:[0-9a-f]{64}" if prefix else r"[0-9a-f]{64}"
    if not isinstance(value, str) or re.fullmatch(pattern, value) is None:
        fail(f"{label} must be a lowercase SHA-256")
    return value


def close(actual: object, expected: float, label: str) -> None:
    if not is_number(actual) or not math.isclose(
        float(actual), expected, rel_tol=1e-12, abs_tol=1e-12
    ):
        fail(f"{label} is inconsistent: {actual!r} != {expected!r}")


def conventional_median(values: list[float]) -> float:
    return statistics.median(values)


def nearest_rank(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(len(ordered) * fraction) - 1)]


def geometric_mean(values: list[float]) -> float:
    return math.exp(statistics.fmean(math.log(value) for value in values))


def load_json(path: Path, expected_digest: str) -> dict[str, Any]:
    require_sha256(expected_digest, f"expected digest for {path}")
    try:
        payload = path.read_bytes()
    except OSError as error:
        fail(f"could not read {path}: {error}")
    actual = hashlib.sha256(payload).hexdigest()
    if actual != expected_digest:
        fail(f"{path} digest differs: {actual} != {expected_digest}")
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        fail(f"could not parse {path}: {error}")
    return require_mapping(value, str(path))


def expected_manifests() -> dict[str, list[dict[str, Any]]]:
    return {
        name: [
            {
                "rows": item["situation_count"],
                "violations": item["situation_violated_count"],
                "reasons": item["violation_reason_counts"],
                "objects": item["object_variables"],
                "events": item["event_variables"],
                "labels": item["label_names"],
            }
            for item in nodes
        ]
        for name, nodes in EXPECTED_NODES.items()
    }


def expected_fingerprints(name: str) -> list[dict[str, Any]]:
    return [
        {"node_index": index, **item} for index, item in enumerate(EXPECTED_NODES[name])
    ]


def validate_reference_method(method_value: object) -> None:
    method = require_exact_keys(
        method_value,
        {
            "warmups_per_query",
            "measured_runs_per_query",
            "fresh_container_per_query",
            "import_and_link_timed_separately",
            "author_published_samples_per_query",
            "author_published_results_are_cross_host_only",
            "upstream_backend_measure_performance_iterations",
            "upstream_backend_warmups",
            "timing_boundary",
            "correctness_boundary",
            "canonicalization",
        },
        "OCPQ reference method",
    )
    expected_scalars = {
        "warmups_per_query": WARMUPS,
        "measured_runs_per_query": RUNS,
        "fresh_container_per_query": True,
        "import_and_link_timed_separately": True,
        "author_published_samples_per_query": 10,
        "author_published_results_are_cross_host_only": True,
        "upstream_backend_measure_performance_iterations": RUNS,
        "upstream_backend_warmups": WARMUPS,
        "timing_boundary": REFERENCE_TIMING_BOUNDARY,
        "correctness_boundary": REFERENCE_CORRECTNESS_BOUNDARY,
    }
    for field, expected in expected_scalars.items():
        if method[field] != expected:
            fail(f"OCPQ reference method has an invalid {field}")
    canonical = require_exact_keys(
        method["canonicalization"],
        {
            "scope",
            "identifiers",
            "variables",
            "labels",
            "violation",
            "q6",
            "ordering",
            "row_shape",
            "expected_node_manifests",
        },
        "OCPQ reference canonicalization",
    )
    expected_semantics = {
        "scope": "every query-tree node and every materialized situation",
        "identifiers": "OCEL external object and event IDs",
        "variables": "object/event variable indexes retained in every row",
        "labels": "label names and exact serialized OCPQ label value types retained",
        "violation": (
            "exact serialized OCPQ ViolationReason retained; null means satisfied"
        ),
        "q6": (
            "root max_dur typed label retained exactly and independently checked "
            "against the maximum child duration in integer microseconds"
        ),
        "ordering": "lexicographic compact-JSON row order; duplicates retained",
        "row_shape": [
            "[[object_variable_index, external_object_id], ...]",
            "[[event_variable_index, external_event_id], ...]",
            "[[label_name, typed_label_value], ...]",
            "serialized_violation_reason_or_null",
        ],
        "expected_node_manifests": expected_manifests(),
    }
    if canonical != expected_semantics:
        fail("OCPQ reference canonicalization is not the strict all-node contract")


def validate_reference_query(name: str, query_value: object) -> None:
    query = require_exact_keys(
        query_value,
        {
            "author_published_runs_ms",
            "author_published_mean_ms",
            "tree_parse_ms",
            "import_ms",
            "link_ms",
            "runs_ms",
            "mean_ms",
            "p50_ms",
            "p95_ms",
            "all_node_situations",
            "root_node",
            "q6_root_label",
            "q6_duration_microseconds",
            "nodes",
        },
        f"{name} OCPQ reference query",
    )
    runs = query["runs_ms"]
    published = query["author_published_runs_ms"]
    if not isinstance(runs, list) or len(runs) != RUNS:
        fail(f"{name} must contain exactly ten direct OCPQ runs")
    if not isinstance(published, list) or len(published) != 10:
        fail(f"{name} author-published source samples are incomplete")
    if not all(is_number(value) and value > 0 for value in runs):
        fail(f"{name} OCPQ direct runs are invalid")
    if not all(is_number(value) and value > 0 for value in published):
        fail(f"{name} author-published source samples are invalid")
    close(query["mean_ms"], statistics.fmean(runs), f"{name} OCPQ mean")
    close(query["p50_ms"], conventional_median(runs), f"{name} OCPQ p50")
    close(query["p95_ms"], nearest_rank(runs, 0.95), f"{name} OCPQ p95")
    close(
        query["author_published_mean_ms"],
        statistics.fmean(published),
        f"{name} author-published mean",
    )
    for field in ("tree_parse_ms", "import_ms", "link_ms"):
        require_number(query[field], f"{name} {field}")
    expected_nodes = expected_fingerprints(name)
    if query["nodes"] != expected_nodes:
        fail(f"{name} is not the pinned strict every-node OCPQ result")
    expected_rows = sum(item["situation_count"] for item in expected_nodes)
    if query["all_node_situations"] != expected_rows or query["root_node"] != 0:
        fail(f"{name} all-node row count or root index is invalid")
    if name == "Q6":
        if (
            query["q6_root_label"] != EXPECTED_Q6_ROOT_LABEL
            or query["q6_duration_microseconds"] != EXPECTED_Q6_DURATION_MICROSECONDS
        ):
            fail("Q6 typed label or child-derived duration is invalid")
    elif (
        query["q6_root_label"] is not None
        or query["q6_duration_microseconds"] is not None
    ):
        fail(f"{name} contains unexpected Q6 evidence")


def validate_reference(reference: dict[str, Any], *, release: bool) -> None:
    require_exact_keys(
        reference,
        {
            "schema_version",
            "generated_at",
            "source",
            "environment",
            "method",
            "queries",
        },
        "OCPQ reference artifact",
    )
    if reference["schema_version"] != 4:
        fail("OCPQ reference must use strict all-node schema version 4")
    generated_at = reference["generated_at"]
    if not isinstance(generated_at, str):
        fail("reference timestamp must be an ISO-8601 string")
    try:
        parsed_timestamp = datetime.fromisoformat(generated_at)
    except ValueError as error:
        fail(f"reference timestamp is invalid: {error}")
    if parsed_timestamp.tzinfo is None:
        fail("reference timestamp must include a timezone")
    source = require_exact_keys(
        reference["source"],
        set(EXPECTED_SOURCE) | {"docker_image_id"},
        "OCPQ reference source",
    )
    for field, expected in EXPECTED_SOURCE.items():
        if source[field] != expected:
            fail(f"OCPQ reference source pin differs: {field}")
    require_sha256(source["docker_image_id"], "reference image ID", prefix=True)
    environment = require_exact_keys(
        reference["environment"],
        {
            "benchmark_host_id",
            "source_revision",
            "source_tree_clean",
            "platform",
            "machine",
            "logical_cpus_visible",
        },
        "OCPQ reference environment",
    )
    require_sha256(environment["benchmark_host_id"], "reference host ID", prefix=True)
    if (
        not isinstance(environment["source_revision"], str)
        or re.fullmatch(r"[0-9a-f]{40}", environment["source_revision"]) is None
    ):
        fail("reference source revision must be an exact Git commit")
    require_bool(environment["source_tree_clean"], "reference source-tree state")
    if release and environment["source_tree_clean"] is not True:
        fail("release certification requires a clean OCPQ harness tree")
    for field in ("platform", "machine"):
        if not isinstance(environment[field], str) or not environment[field].strip():
            fail(f"reference environment is missing {field}")
    require_integer(
        environment["logical_cpus_visible"], "reference logical CPU count", minimum=1
    )
    validate_reference_method(reference["method"])
    queries = require_exact_keys(
        reference["queries"], QUERY_NAME_SET, "OCPQ reference queries"
    )
    for name in QUERY_NAMES:
        validate_reference_query(name, queries[name])


def reject_cross_host_ratios(value: object, path: str = "candidate") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            lowered = key.lower()
            if ("speedup" in lowered or "ratio" in lowered) and (
                "author" in lowered or "published" in lowered
            ):
                fail(f"cross-host ratio is forbidden: {path}.{key}")
            reject_cross_host_ratios(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            reject_cross_host_ratios(child, f"{path}[{index}]")


def validate_candidate_method(method_value: object) -> None:
    method = require_mapping(method_value, "candidate method")
    latency = require_exact_keys(
        method.get("latency"),
        {
            "warmups_per_query",
            "measured_runs_per_query",
            "timing_boundary",
            "correctness_boundary",
            "query_protocol",
            "process_scope",
            "p50_estimator",
            "p95_estimator",
        },
        "candidate latency method",
    )
    expected_latency = {
        "warmups_per_query": WARMUPS,
        "measured_runs_per_query": RUNS,
        "timing_boundary": CANDIDATE_TIMING_BOUNDARY,
        "correctness_boundary": CANDIDATE_CORRECTNESS_BOUNDARY,
        "query_protocol": CANDIDATE_QUERY_PROTOCOL,
        "process_scope": CANDIDATE_PROCESS_SCOPE,
        "p50_estimator": CANDIDATE_P50_ESTIMATOR,
        "p95_estimator": CANDIDATE_P95_ESTIMATOR,
    }
    if latency != expected_latency:
        fail("candidate latency method is not the strict 0/10 all-node boundary")
    if method.get("latency_fresh_container_per_query") is not True or method.get(
        "latency_fresh_container_count"
    ) != len(QUERY_NAMES):
        fail("candidate method does not declare one fresh container per query")
    memory = require_exact_keys(
        method.get("memory"),
        {"mode", "measurement_boundary", "correctness_boundary"},
        "candidate memory method",
    )
    if memory != {
        "mode": MEMORY_MODE,
        "measurement_boundary": MEMORY_MEASUREMENT_BOUNDARY,
        "correctness_boundary": MEMORY_CORRECTNESS_BOUNDARY,
    }:
        fail("candidate memory method is invalid")
    resources = require_exact_keys(
        method.get("resources"),
        {
            "storage_boundary",
            "concurrency_boundary",
            "concurrency_protocol",
            "concurrency_raw_evidence",
            "correctness_boundary",
            "latency_p50_estimator",
            "latency_p95_p99_estimator",
        },
        "candidate resource method",
    )
    if resources != {
        "storage_boundary": STORAGE_BOUNDARY,
        "concurrency_boundary": CONCURRENCY_BOUNDARY,
        "concurrency_protocol": CONCURRENCY_PROTOCOL,
        "concurrency_raw_evidence": CONCURRENCY_RAW_EVIDENCE,
        "correctness_boundary": RESOURCE_CORRECTNESS_BOUNDARY,
        "latency_p50_estimator": RESOURCE_P50_ESTIMATOR,
        "latency_p95_p99_estimator": RESOURCE_TAIL_ESTIMATOR,
    }:
        fail("candidate resource method is invalid")


def complete_provenance(
    candidate: dict[str, Any], reference: dict[str, Any], *, release: bool
) -> tuple[bool, list[str]]:
    environments = require_exact_keys(
        candidate.get("environment"),
        {"latency_and_memory", "concurrency"},
        "candidate environments",
    )
    latency = require_mapping(
        environments["latency_and_memory"], "latency/memory environment"
    )
    concurrency = require_mapping(
        environments["concurrency"], "concurrency environment"
    )
    process_specific_fields = {
        "tokio_worker_threads_env",
        "process_run_id",
        "container_hostname",
        "client_process_id",
    }
    latency_common = {
        key: value
        for key, value in latency.items()
        if key not in process_specific_fields
    }
    concurrency_common = {
        key: value
        for key, value in concurrency.items()
        if key not in process_specific_fields
    }
    if latency_common != concurrency_common:
        fail("latency, memory, and concurrency provenance differs")
    reference_environment = reference["environment"]
    host = latency.get("benchmark_host_id")
    require_sha256(host, "candidate host ID", prefix=True)
    if host != reference_environment["benchmark_host_id"]:
        fail("reference and candidate were not run on the same Docker host")
    if (
        latency.get("source_revision") != reference_environment["source_revision"]
        or latency.get("source_tree_clean")
        != reference_environment["source_tree_clean"]
        or latency.get("same_docker_host_as_reference") is not True
        or latency.get("same_harness_revision_state_as_reference") is not True
    ):
        fail("reference and candidate harness provenance differs")
    if latency.get("postgres_jit") != "off":
        fail("candidate PostgreSQL JIT must be disabled")
    if (
        not isinstance(latency.get("postgres_server_version"), str)
        or not latency["postgres_server_version"].strip()
    ):
        fail("candidate PostgreSQL version is missing")
    require_integer(
        latency.get("postgres_server_version_num"),
        "candidate PostgreSQL version number",
        minimum=1,
    )
    for field in ("client_os", "client_arch"):
        if not isinstance(latency.get(field), str) or not latency[field].strip():
            fail(f"candidate environment is missing {field}")
    require_integer(
        latency.get("client_logical_cpus_visible"),
        "candidate logical CPU count",
        minimum=1,
    )
    if latency.get("tokio_worker_threads_env") != "1":
        fail("latency and memory must use one Tokio worker thread")
    concurrency_threads = concurrency.get("tokio_worker_threads_env")
    if (
        not isinstance(concurrency_threads, str)
        or not concurrency_threads.isdigit()
        or int(concurrency_threads) < 1
    ):
        fail("concurrency worker-thread provenance is invalid")

    blockers: list[str] = []
    source_tree_id = latency.get("source_tree_id")
    if (
        not isinstance(source_tree_id, str)
        or re.fullmatch(r"git-tree:[0-9a-f]{40}([0-9a-f]{24})?", source_tree_id) is None
    ):
        blockers.append("candidate source-tree ID is incomplete")
    try:
        require_sha256(latency.get("candidate_runner_sha256"), "candidate runner")
    except CertificationError:
        blockers.append("candidate runner digest is incomplete")
    for label_field, id_field in (
        ("candidate_image", "candidate_image_id"),
        ("database_image", "database_image_id"),
    ):
        if (
            not isinstance(latency.get(label_field), str)
            or not latency[label_field].strip()
            or latency[label_field] == "unspecified"
        ):
            blockers.append(f"{label_field} is incomplete")
        try:
            require_sha256(latency.get(id_field), id_field, prefix=True)
        except CertificationError:
            blockers.append(f"{id_field} is incomplete")
    pg_revision = latency.get("pg_ocpm_source_revision")
    if (
        not isinstance(pg_revision, str)
        or re.fullmatch(r"[0-9a-f]{40}", pg_revision) is None
    ):
        blockers.append("pg_ocpm source revision is incomplete")
    if latency.get("pg_ocpm_source_tree_clean") is not True:
        blockers.append("pg_ocpm source tree is not clean")
    if latency.get("source_tree_clean") is not True:
        blockers.append("ocpm-engine source tree is not clean")
    if reference_environment["source_tree_clean"] is not True:
        blockers.append("OCPQ reference harness tree is not clean")
    if latency.get("provenance_complete") is not True:
        blockers.append("runner marked provenance incomplete")
    if release and blockers:
        fail("release provenance is incomplete: " + "; ".join(blockers))
    return not blockers, blockers


def validate_fresh_process_evidence(
    candidate: dict[str, Any], *, release: bool
) -> tuple[bool, list[str]]:
    blockers: list[str] = []
    if candidate.get("fresh_container_per_query") is not True:
        blockers.append("fresh_container_per_query is missing or false")
    processes_value = candidate.get("latency_processes")
    if processes_value is None:
        blockers.append("per-query latency process evidence is missing")
    else:
        processes = require_exact_keys(
            processes_value, QUERY_NAME_SET, "latency process evidence"
        )
        container_ids: list[str] = []
        process_start_ids: list[str] = []
        for name in QUERY_NAMES:
            process = require_exact_keys(
                processes[name],
                {"container_id", "process_start_id", "client_process_id"},
                f"{name} latency process",
            )
            container_id = process["container_id"]
            start_id = process["process_start_id"]
            if not isinstance(container_id, str) or not container_id.strip():
                fail(f"{name} container ID is invalid")
            if not isinstance(start_id, str) or not start_id.strip():
                fail(f"{name} process-start ID is invalid")
            require_integer(
                process["client_process_id"], f"{name} client process ID", minimum=1
            )
            container_ids.append(container_id)
            process_start_ids.append(start_id)
        if len(set(container_ids)) != len(QUERY_NAMES):
            fail("latency queries did not use seven unique containers")
        if len(set(process_start_ids)) != len(QUERY_NAMES):
            fail("latency queries did not use seven unique processes")
        nested = require_mapping(candidate.get("method"), "candidate method").get(
            "latency_fresh_processes"
        )
        if nested is not None:
            if not isinstance(nested, list) or len(nested) != len(QUERY_NAMES):
                fail("nested fresh-process evidence is incomplete")
            nested_by_query = {
                item.get("query"): item
                for item in nested
                if isinstance(item, dict) and isinstance(item.get("query"), str)
            }
            if set(nested_by_query) != QUERY_NAME_SET:
                fail("nested fresh-process evidence does not cover Q1-Q7")
            for name in QUERY_NAMES:
                process = processes[name]
                nested_process = nested_by_query[name]
                if (
                    nested_process.get("container_hostname") != process["container_id"]
                    or nested_process.get("process_run_id")
                    != process["process_start_id"]
                    or nested_process.get("client_process_id")
                    != process["client_process_id"]
                ):
                    fail(f"{name} top-level and nested process evidence differs")
    if release and blockers:
        fail("release process-scope evidence is incomplete: " + "; ".join(blockers))
    return not blockers, blockers


def validate_candidate_query(
    name: str, query_value: object, reference_query: dict[str, Any]
) -> float:
    query = require_exact_keys(
        query_value,
        {
            "reference_ocpq_mean_ms",
            "candidate_mean_ms",
            "candidate_p50_ms",
            "candidate_p95_ms",
            "speedup_vs_reference_ocpq",
            "runs_ms",
            "all_node_situations",
            "capsule_bytes",
            "nodes",
            "every_node_exact",
        }
        | ({"q6_root_label", "q6_duration_microseconds"} if name == "Q6" else set()),
        f"{name} candidate query",
    )
    runs = query["runs_ms"]
    if not isinstance(runs, list) or len(runs) != RUNS:
        fail(f"{name} candidate must contain exactly ten direct runs")
    if not all(is_number(value) and value > 0 for value in runs):
        fail(f"{name} candidate runs are invalid")
    reference_mean = require_positive(
        reference_query["mean_ms"], f"{name} reference mean"
    )
    close(query["reference_ocpq_mean_ms"], reference_mean, f"{name} reference mean")
    candidate_mean = statistics.fmean(runs)
    close(query["candidate_mean_ms"], candidate_mean, f"{name} candidate mean")
    close(
        query["candidate_p50_ms"],
        conventional_median(runs),
        f"{name} conventional median",
    )
    close(
        query["candidate_p95_ms"],
        nearest_rank(runs, 0.95),
        f"{name} candidate p95",
    )
    speedup = reference_mean / candidate_mean
    close(query["speedup_vs_reference_ocpq"], speedup, f"{name} speedup")
    if speedup < MINIMUM_QUERY_SPEEDUP:
        fail(f"{name} speedup {speedup:.3f}x is below {MINIMUM_QUERY_SPEEDUP:.1f}x")
    if query["nodes"] != reference_query["nodes"]:
        fail(f"{name} candidate differs from OCPQ at one or more result nodes")
    if query["every_node_exact"] is not True:
        fail(f"{name} is not marked exact for every node")
    if query["all_node_situations"] != reference_query["all_node_situations"]:
        fail(f"{name} candidate all-node row count differs")
    require_integer(query["capsule_bytes"], f"{name} capsule bytes", minimum=1)
    if name == "Q6":
        if (
            query["q6_root_label"] != reference_query["q6_root_label"]
            or query["q6_duration_microseconds"]
            != reference_query["q6_duration_microseconds"]
        ):
            fail("Q6 candidate typed label or child-derived duration differs")
    return speedup


def validate_memory(
    candidate: dict[str, Any], reference: dict[str, Any]
) -> tuple[int, int, int]:
    memories = require_exact_keys(
        candidate.get("memory"), QUERY_NAME_SET, "candidate memory queries"
    )
    maximum_rss_peak = 0
    maximum_vmhwm_peak = 0
    maximum_owned = 0
    for name in QUERY_NAMES:
        memory = require_mapping(memories[name], f"{name} memory evidence")
        if memory.get("mode") != MEMORY_MODE:
            fail(f"{name} memory mode is invalid")
        if (
            memory.get("measurement_boundary") != MEMORY_MEASUREMENT_BOUNDARY
            or memory.get("correctness_boundary") != MEMORY_CORRECTNESS_BOUNDARY
        ):
            fail(f"{name} memory boundary is invalid")
        if memory.get("every_node_exact") is not True:
            fail(f"{name} memory run lacks every-node parity")
        reference_query = reference["queries"][name]
        if memory.get("nodes") != reference_query["nodes"]:
            fail(f"{name} memory run node fingerprints differ")
        expected_counts = [item["situation_count"] for item in reference_query["nodes"]]
        if (
            memory.get("node_situation_counts") != expected_counts
            or memory.get("all_node_situations") != sum(expected_counts)
            or memory.get("capsule_bytes")
            != candidate["queries"][name]["capsule_bytes"]
        ):
            fail(f"{name} memory result shape differs")
        owned = require_integer(
            memory.get("owned_tree_allocated_bytes"),
            f"{name} owned tree bytes",
            minimum=1,
        )
        baseline_rss = require_integer(
            memory.get("baseline_rss_bytes"), f"{name} baseline RSS"
        )
        baseline_hwm = require_integer(
            memory.get("baseline_vmhwm_bytes"), f"{name} baseline VmHWM"
        )
        after_rss = require_integer(memory.get("after_rss_bytes"), f"{name} after RSS")
        after_hwm = require_integer(
            memory.get("after_vmhwm_bytes"), f"{name} after VmHWM"
        )
        if (
            baseline_hwm < baseline_rss
            or after_hwm < after_rss
            or after_hwm < baseline_hwm
        ):
            fail(f"{name} RSS/VmHWM ordering is invalid")
        rss_delta = memory.get("rss_delta_bytes")
        vmhwm_delta = memory.get("vmhwm_delta_bytes")
        if not is_integer(rss_delta) or rss_delta != after_rss - baseline_rss:
            fail(f"{name} RSS delta is inconsistent")
        if not is_integer(vmhwm_delta) or vmhwm_delta != after_hwm - baseline_hwm:
            fail(f"{name} VmHWM delta is inconsistent")
        rss_peak = max(0, after_hwm - baseline_rss)
        vmhwm_peak = after_hwm - baseline_hwm
        if memory.get("peak_over_baseline_rss_bytes") != rss_peak:
            fail(f"{name} peak-over-baseline RSS is inconsistent")
        if memory.get("peak_over_baseline_vmhwm_bytes") != vmhwm_peak:
            fail(f"{name} peak-over-baseline VmHWM is inconsistent")
        if vmhwm_peak > MAXIMUM_CLIENT_VMHWM_OVER_BASELINE_BYTES:
            fail(
                f"{name} client VmHWM increase exceeds "
                f"{MAXIMUM_CLIENT_VMHWM_OVER_BASELINE_BYTES} bytes"
            )
        if memory.get("postgres_backend_memory_scope") != BACKEND_MEMORY_SCOPE:
            fail(f"{name} backend retained-memory scope is invalid")
        backend_before = require_integer(
            memory.get("postgres_backend_baseline_bytes"),
            f"{name} backend baseline",
            minimum=1,
        )
        backend_after = require_integer(
            memory.get("postgres_backend_after_bytes"),
            f"{name} backend after",
            minimum=1,
        )
        backend_delta = memory.get("postgres_backend_retained_delta_bytes")
        if (
            not is_integer(backend_delta)
            or backend_delta != backend_after - backend_before
        ):
            fail(f"{name} backend retained-memory diagnostic is inconsistent")
        maximum_rss_peak = max(maximum_rss_peak, rss_peak)
        maximum_vmhwm_peak = max(maximum_vmhwm_peak, vmhwm_peak)
        maximum_owned = max(maximum_owned, owned)
    return maximum_rss_peak, maximum_vmhwm_peak, maximum_owned


def validate_storage(
    candidate: dict[str, Any], *, release: bool
) -> tuple[bool, list[str]]:
    storage = require_mapping(candidate.get("storage"), "candidate storage")
    if storage.get("scope") != STORAGE_SCOPE:
        fail("serving-storage scope is invalid")
    relations = require_mapping(storage.get("relations"), "storage relations")
    if not relations:
        fail("storage relation evidence is empty")
    total = 0
    indexes = 0
    binding = 0
    for name, value in relations.items():
        if not isinstance(name, str) or not name:
            fail("storage contains an invalid relation name")
        relation = require_exact_keys(
            value,
            {"total_bytes", "index_bytes", "heap_toast_fsm_vm_bytes"},
            f"storage relation {name}",
        )
        relation_total = require_integer(relation["total_bytes"], f"{name} total bytes")
        relation_indexes = require_integer(
            relation["index_bytes"], f"{name} index bytes"
        )
        relation_heap = require_integer(
            relation["heap_toast_fsm_vm_bytes"], f"{name} heap/TOAST bytes"
        )
        if relation_total != relation_indexes + relation_heap:
            fail(f"{name} storage components are inconsistent")
        total += relation_total
        indexes += relation_indexes
        if name.startswith("binding_"):
            binding += relation_total
    if (
        storage.get("total_serving_bytes") != total
        or storage.get("index_bytes") != indexes
        or storage.get("heap_toast_fsm_vm_bytes") != total - indexes
        or storage.get("binding_summary_bytes") != binding
    ):
        fail("aggregate serving storage is inconsistent")
    database_bytes = require_integer(
        storage.get("database_bytes_diagnostic"), "database size", minimum=1
    )
    if database_bytes < total:
        fail("database size is smaller than retained serving storage")
    if total > MAXIMUM_TOTAL_SERVING_BYTES:
        fail("total serving storage exceeds 128 MiB")
    if indexes > MAXIMUM_INDEX_BYTES:
        fail("serving indexes exceed 16 MiB")
    if binding > MAXIMUM_BINDING_BYTES:
        fail("binding summaries exceed 8 MiB")
    limits = require_exact_keys(
        candidate.get("storage_limits"),
        {
            "maximum_total_serving_bytes",
            "maximum_index_bytes",
            "maximum_binding_summary_bytes",
        },
        "storage limits",
    )
    if limits != {
        "maximum_total_serving_bytes": MAXIMUM_TOTAL_SERVING_BYTES,
        "maximum_index_bytes": MAXIMUM_INDEX_BYTES,
        "maximum_binding_summary_bytes": MAXIMUM_BINDING_BYTES,
    }:
        fail("artifact storage limits are not the release ceilings")

    blockers: list[str] = []
    if "result_cache_rows" not in storage:
        blockers.append("result-cache row-count evidence is missing")
    elif storage["result_cache_rows"] != 0:
        fail("strict candidate used or retained result-cache rows")
    if "request_result_cache_enabled" not in storage:
        blockers.append("request result-cache mode evidence is missing")
    elif storage["request_result_cache_enabled"] is not False:
        fail("strict candidate enabled a request result cache")
    if release and blockers:
        fail("release no-cache evidence is incomplete: " + "; ".join(blockers))
    return not blockers, blockers


def validate_concurrency(candidate: dict[str, Any]) -> float:
    concurrency = require_exact_keys(
        candidate.get("concurrency"),
        {str(level) for level in CONCURRENCY_LEVELS},
        "candidate concurrency levels",
    )
    for clients in CONCURRENCY_LEVELS:
        label = str(clients)
        report = require_mapping(concurrency[label], f"{label}-client report")
        if (
            report.get("clients") != clients
            or report.get("epoch_count") != CONCURRENCY_EPOCHS
            or report.get("minimum_requests_per_client")
            != MINIMUM_CONCURRENCY_REQUESTS_PER_CLIENT
            or report.get("maximum_requests_per_client")
            != MAXIMUM_CONCURRENCY_REQUESTS_PER_CLIENT
            or report.get("minimum_wall_time_ms") != MINIMUM_CONCURRENCY_WALL_MS
            or report.get("every_pre_and_post_node_exact") is not True
        ):
            fail(f"{label}-client concurrency protocol is invalid")
        epochs = report.get("epochs")
        if not isinstance(epochs, list) or len(epochs) != CONCURRENCY_EPOCHS:
            fail(f"{label}-client concurrency epochs are incomplete")
        throughputs: list[float] = []
        p50s: list[float] = []
        p95s: list[float] = []
        p99s: list[float] = []
        total_requests = 0
        aggregate_query_counts = {name: 0 for name in QUERY_NAMES}
        for expected_epoch, epoch_value in enumerate(epochs, start=1):
            epoch = require_mapping(
                epoch_value, f"{label}-client epoch {expected_epoch}"
            )
            if (
                epoch.get("epoch") != expected_epoch
                or epoch.get("client_ids") != list(range(clients))
                or epoch.get("pre_epoch_exact_query_checks_per_client")
                != len(QUERY_NAMES)
                or epoch.get("post_epoch_exact_query_checks_per_client")
                != len(QUERY_NAMES)
                or epoch.get("every_pre_and_post_node_exact") is not True
            ):
                fail(f"{label}-client epoch {expected_epoch} parity is incomplete")
            client_counts = require_exact_keys(
                epoch.get("client_request_counts"),
                {str(index) for index in range(clients)},
                f"{label}-client epoch {expected_epoch} client counts",
            )
            if any(
                not is_integer(count)
                or not (
                    MINIMUM_CONCURRENCY_REQUESTS_PER_CLIENT
                    <= count
                    <= MAXIMUM_CONCURRENCY_REQUESTS_PER_CLIENT
                )
                for count in client_counts.values()
            ):
                fail(
                    f"{label}-client epoch {expected_epoch} has an out-of-bounds "
                    "client request count"
                )
            query_counts = require_exact_keys(
                epoch.get("query_request_counts"),
                QUERY_NAME_SET,
                f"{label}-client epoch {expected_epoch} query counts",
            )
            if any(
                not is_integer(count) or count <= 0 for count in query_counts.values()
            ):
                fail(f"{label}-client epoch {expected_epoch} lacks Q1-Q7 coverage")
            request_count = require_integer(
                epoch.get("request_count"),
                f"{label}-client epoch {expected_epoch} requests",
                minimum=1,
            )
            if (
                request_count < clients * MINIMUM_CONCURRENCY_REQUESTS_PER_CLIENT
                or sum(client_counts.values()) != request_count
                or sum(query_counts.values()) != request_count
            ):
                fail(f"{label}-client epoch {expected_epoch} totals differ")
            wall = require_positive(
                epoch.get("wall_time_ms"),
                f"{label}-client epoch {expected_epoch} wall time",
            )
            if wall < MINIMUM_CONCURRENCY_WALL_MS:
                fail(f"{label}-client epoch {expected_epoch} is shorter than 5s")
            raw_by_client = require_exact_keys(
                epoch.get("client_request_latencies_ns"),
                {str(index) for index in range(clients)},
                f"{label}-client epoch {expected_epoch} raw latency evidence",
            )
            raw_latencies_ns: list[int] = []
            raw_query_counts = {name: 0 for name in QUERY_NAMES}
            maximum_latency_ns = math.ceil(wall * 1_000_000.0)
            for client_id in range(clients):
                client_label = str(client_id)
                client_latencies = raw_by_client[client_label]
                if not isinstance(client_latencies, list):
                    fail(
                        f"{label}-client epoch {expected_epoch} client {client_id} "
                        "raw latencies must be an array"
                    )
                if len(client_latencies) != client_counts[client_label]:
                    fail(
                        f"{label}-client epoch {expected_epoch} client {client_id} "
                        "raw latency count differs"
                    )
                for request_id, latency_ns in enumerate(client_latencies):
                    if (
                        not is_integer(latency_ns)
                        or latency_ns <= 0
                        or latency_ns > maximum_latency_ns
                    ):
                        fail(
                            f"{label}-client epoch {expected_epoch} client "
                            f"{client_id} request {request_id} latency_ns is invalid"
                        )
                    query = QUERY_NAMES[(client_id + request_id) % len(QUERY_NAMES)]
                    raw_query_counts[query] += 1
                    raw_latencies_ns.append(latency_ns)
            if len(raw_latencies_ns) != request_count:
                fail(f"{label}-client epoch {expected_epoch} raw latency total differs")
            if raw_query_counts != query_counts:
                fail(
                    f"{label}-client epoch {expected_epoch} raw Q1-Q7 schedule differs"
                )
            throughput = require_positive(
                epoch.get("throughput_requests_per_second"),
                f"{label}-client epoch {expected_epoch} throughput",
            )
            close(
                throughput,
                request_count * 1000.0 / wall,
                f"{label}-client epoch {expected_epoch} throughput",
            )
            p50 = require_positive(
                epoch.get("latency_p50_ms"),
                f"{label}-client epoch {expected_epoch} p50",
            )
            p95 = require_positive(
                epoch.get("latency_p95_ms"),
                f"{label}-client epoch {expected_epoch} p95",
            )
            p99 = require_positive(
                epoch.get("latency_p99_ms"),
                f"{label}-client epoch {expected_epoch} p99",
            )
            raw_latencies_ms = [value / 1_000_000.0 for value in raw_latencies_ns]
            raw_p50 = conventional_median(raw_latencies_ms)
            raw_p95 = nearest_rank(raw_latencies_ms, 0.95)
            raw_p99 = nearest_rank(raw_latencies_ms, 0.99)
            close(p50, raw_p50, f"{label}-client epoch {expected_epoch} raw p50")
            close(p95, raw_p95, f"{label}-client epoch {expected_epoch} raw p95")
            close(p99, raw_p99, f"{label}-client epoch {expected_epoch} raw p99")
            throughputs.append(throughput)
            p50s.append(raw_p50)
            p95s.append(raw_p95)
            p99s.append(raw_p99)
            total_requests += request_count
            for name, count in query_counts.items():
                aggregate_query_counts[name] += count
        throughput_cv = statistics.pstdev(throughputs) / statistics.fmean(throughputs)
        expected_aggregates = {
            "median_epoch_throughput_requests_per_second": conventional_median(
                throughputs
            ),
            "minimum_epoch_throughput_requests_per_second": min(throughputs),
            "maximum_epoch_throughput_requests_per_second": max(throughputs),
            "epoch_throughput_cv": throughput_cv,
            "median_epoch_latency_p50_ms": conventional_median(p50s),
            "median_epoch_latency_p95_ms": conventional_median(p95s),
            "median_epoch_latency_p99_ms": conventional_median(p99s),
        }
        for field, expected in expected_aggregates.items():
            close(report.get(field), expected, f"{label}-client {field}")
        if throughput_cv > MAXIMUM_CONCURRENCY_THROUGHPUT_CV:
            fail(f"{label}-client throughput CV exceeds 15%")
        if report["median_epoch_latency_p95_ms"] >= MAXIMUM_CONCURRENCY_MEDIAN_P95_MS:
            fail(f"{label}-client median p95 is not below 10 ms")
        if report.get("total_request_count") != total_requests:
            fail(f"{label}-client total request count differs")
        if report.get("total_query_request_counts") != aggregate_query_counts:
            fail(f"{label}-client aggregate Q1-Q7 counts differ")
    scaling = (
        concurrency["16"]["median_epoch_throughput_requests_per_second"]
        / concurrency["1"]["median_epoch_throughput_requests_per_second"]
    )
    if scaling < MINIMUM_CONCURRENCY_SCALING:
        fail(f"16:1 concurrency scaling {scaling:.3f}x is below 5x")
    return scaling


def validate_publication_status(
    candidate: dict[str, Any], *, provenance_complete: bool
) -> None:
    status = require_mapping(candidate.get("publication_status"), "publication status")
    expected = {
        "latency_targets_met": True,
        "minimum_query_speedup_required": MINIMUM_QUERY_SPEEDUP,
        "geometric_mean_speedup_required": MINIMUM_GEOMETRIC_MEAN_SPEEDUP,
        "every_latency_query_and_node_exact": True,
        "every_memory_query_and_node_exact": True,
        "every_concurrency_level_pre_and_post_node_exact": True,
        "storage_within_limits": True,
        "provenance_complete": provenance_complete,
    }
    for field, value in expected.items():
        if status.get(field) != value:
            fail(f"publication status is inconsistent: {field}")
    require_bool(status.get("ready"), "artifact ready status")


def validate_candidate(
    candidate: dict[str, Any],
    reference: dict[str, Any],
    *,
    reference_digest: str,
    release: bool,
    expected_release: dict[str, str],
) -> dict[str, Any]:
    if candidate.get("schema_version") != 1 or candidate.get("artifact_kind") != (
        "strict-all-node-ocpq-publication-gates"
    ):
        fail("candidate must use the combined strict all-node artifact schema")
    require_integer(
        candidate.get("generated_at_unix_ms"),
        "candidate timestamp",
        minimum=1,
    )
    if candidate.get("release") != expected_release:
        fail(
            "candidate release must be pg_ocpm "
            f"{expected_release['pg_ocpm']} plus ocpm-engine "
            f"{expected_release['ocpm_engine']}"
        )
    if candidate.get("reference_schema_version") != 4:
        fail("candidate does not reference strict OCPQ schema version 4")
    if candidate.get("reference_artifact_sha256") != reference_digest:
        fail("candidate references a different OCPQ artifact digest")
    if candidate.get("reference_source") != reference["source"]:
        fail("candidate OCPQ source pins differ")
    if candidate.get("reference_environment") != reference["environment"]:
        fail("candidate OCPQ reference environment differs")
    reject_cross_host_ratios(candidate)
    validate_candidate_method(candidate.get("method"))
    provenance_ready, provenance_blockers = complete_provenance(
        candidate, reference, release=release
    )
    process_ready, process_blockers = validate_fresh_process_evidence(
        candidate, release=release
    )
    queries = require_exact_keys(
        candidate.get("queries"), QUERY_NAME_SET, "candidate queries"
    )
    speedups = [
        validate_candidate_query(name, queries[name], reference["queries"][name])
        for name in QUERY_NAMES
    ]
    speedup_geomean = geometric_mean(speedups)
    if speedup_geomean < MINIMUM_GEOMETRIC_MEAN_SPEEDUP:
        fail(
            f"geometric-mean speedup {speedup_geomean:.3f}x is below "
            f"{MINIMUM_GEOMETRIC_MEAN_SPEEDUP:.1f}x"
        )
    rss_peak, vmhwm_peak, maximum_owned = validate_memory(candidate, reference)
    cache_ready, cache_blockers = validate_storage(candidate, release=release)
    concurrency_scaling = validate_concurrency(candidate)

    reference_means = [reference["queries"][name]["mean_ms"] for name in QUERY_NAMES]
    candidate_means = [queries[name]["candidate_mean_ms"] for name in QUERY_NAMES]
    summary = require_mapping(candidate.get("summary"), "candidate summary")
    expected_summary = {
        "reference_ocpq_geometric_mean_ms": geometric_mean(reference_means),
        "candidate_geometric_mean_ms": geometric_mean(candidate_means),
        "speedup_geometric_mean": speedup_geomean,
        "minimum_query_speedup": min(speedups),
        "maximum_client_peak_over_baseline_rss_bytes": rss_peak,
        "maximum_client_peak_over_baseline_vmhwm_bytes": vmhwm_peak,
        "maximum_owned_tree_allocated_bytes": maximum_owned,
    }
    for field, expected in expected_summary.items():
        close(summary.get(field), expected, f"candidate summary {field}")
    validate_publication_status(candidate, provenance_complete=provenance_ready)

    blockers = provenance_blockers + process_blockers + cache_blockers
    if release and candidate["publication_status"]["ready"] is not True:
        fail("release artifact is not marked ready by its producer")
    publication_ready = release and not blockers
    if release and not publication_ready:
        fail("release artifact has unresolved publication blockers")
    return {
        "mode": "release" if release else "preview",
        "publication_ready": publication_ready,
        "every_query_and_node_exact": True,
        "minimum_query_speedup": min(speedups),
        "speedup_geometric_mean": speedup_geomean,
        "maximum_client_vmhwm_over_baseline_bytes": vmhwm_peak,
        "concurrency_16_to_1_scaling": concurrency_scaling,
        "provenance_complete": provenance_ready,
        "fresh_process_scope_complete": process_ready,
        "no_result_cache": cache_ready,
        "blocking_evidence": blockers + ([] if release else ["preview mode"]),
    }


def certify(
    reference: dict[str, Any],
    candidate: dict[str, Any],
    *,
    reference_digest: str,
    release: bool,
    expected_release: dict[str, str] = EXPECTED_RELEASE,
) -> dict[str, Any]:
    if release and expected_release != EXPECTED_RELEASE:
        fail("release validation uses only the published release pair")
    require_sha256(reference_digest, "reference digest")
    validate_reference(reference, release=release)
    return validate_candidate(
        candidate,
        reference,
        reference_digest=reference_digest,
        release=release,
        expected_release=expected_release,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", type=Path, default=REFERENCE_PATH)
    parser.add_argument("--candidate", type=Path, default=CANDIDATE_PATH)
    parser.add_argument("--expected-reference-sha256")
    parser.add_argument("--expected-candidate-sha256")
    parser.add_argument("--expected-pg-ocpm-version")
    parser.add_argument("--expected-ocpm-engine-version")
    parser.add_argument(
        "--preview",
        action="store_true",
        help=(
            "validate explicitly digest-pinned unpublished evidence; preview "
            "can never report publication_ready=true"
        ),
    )
    return parser.parse_args()


def resolve_expected_release(args: argparse.Namespace) -> dict[str, str]:
    versions = (
        args.expected_pg_ocpm_version,
        args.expected_ocpm_engine_version,
    )
    if not args.preview:
        if any(versions):
            fail("release validation uses only the published release pair")
        return EXPECTED_RELEASE
    if any(versions) and not all(versions):
        fail("preview release override requires both version values")
    if not any(versions):
        return EXPECTED_RELEASE
    if any(
        re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", value) is None for value in versions
    ):
        fail("preview release versions must use MAJOR.MINOR.PATCH")
    return {
        "pg_ocpm": args.expected_pg_ocpm_version,
        "ocpm_engine": args.expected_ocpm_engine_version,
    }


def resolve_digests(args: argparse.Namespace) -> tuple[str, str]:
    if args.preview:
        if not args.expected_reference_sha256 or not args.expected_candidate_sha256:
            fail("preview validation requires both explicit artifact digests")
        return args.expected_reference_sha256, args.expected_candidate_sha256
    if args.expected_reference_sha256 or args.expected_candidate_sha256:
        fail("release validation uses only source-pinned published digests")
    if PUBLISHED_REFERENCE_SHA256 is None or PUBLISHED_CANDIDATE_SHA256 is None:
        fail("strict OCPQ release artifact digests have not been published")
    return PUBLISHED_REFERENCE_SHA256, PUBLISHED_CANDIDATE_SHA256


def main() -> None:
    args = parse_args()
    try:
        reference_digest, candidate_digest = resolve_digests(args)
        expected_release = resolve_expected_release(args)
        reference = load_json(args.reference, reference_digest)
        candidate = load_json(args.candidate, candidate_digest)
        result = certify(
            reference,
            candidate,
            reference_digest=reference_digest,
            release=not args.preview,
            expected_release=expected_release,
        )
    except CertificationError as error:
        raise SystemExit(str(error)) from error
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
