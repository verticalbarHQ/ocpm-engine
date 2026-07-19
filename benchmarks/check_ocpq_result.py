#!/usr/bin/env python3
"""Validate the OCPQ reference, schema-4 candidate, and memory artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import statistics
from pathlib import Path

QUERY_NAMES = {f"Q{index}" for index in range(1, 8)}
REFERENCE_PATH = Path("docs/results/ocpq-reproduced-0.6.7.json")
CANDIDATE_PATH = Path("docs/results/ocpq-bpic2017-0.5.0.json")
MEMORY_PATH = Path("docs/results/ocpq-bpic2017-0.5.0-memory.json")
EXPECTED_REFERENCE_SHA256 = (
    "592c2a2e242980b9563dd492da93938d29713b65e7449705f6d1ecfe7d6a7687"
)
EXPECTED_CANDIDATE_SHA256 = (
    "aa7c8e7f5ce1b3f1ccda2811af8701562c358a146e02056ca2bee320fac7dc1a"
)
EXPECTED_MEMORY_SHA256 = (
    "ffddd5189ade14a1721b49cf378f9951493a6d284961ae3ce3882252a715a0f1"
)
EXPECTED_RELEASE = {"pg_ocpm": "0.6.0", "ocpm_engine": "0.5.0"}
EXPECTED_SOURCE = {
    "ocpq_eval_commit": "846dd4eb9f8600ae42355968453a9412ea4759c2",
    "ocpq_version": "0.6.7",
    "ocpq_commit": "80457e561edd7bb9e142d959dd7e0f96e6b03f2f",
    "docker_image": "ocpq:0.6.7-corrected-harness",
    "dataset_sqlite_sha256": (
        "02ac333a2c194b5a411cb8527dd64b4845e5110752d2ffddb531e48ce97556d7"
    ),
    "query_files_sha256": (
        "387aeb31398d86ef8e7b15393649cbabe75be56185fd67d27021744025873466"
    ),
    "author_published_results_commit": ("846dd4eb9f8600ae42355968453a9412ea4759c2"),
}
EXPECTED_REFERENCE_METHOD = {
    "warmups_per_query": 10,
    "measured_runs_per_query": 30,
    "fresh_container_per_query": True,
    "import_and_link_timed_separately": True,
    "author_published_samples_per_query": 10,
    "author_published_results_are_cross_host_only": True,
    "timing_boundary": (
        "OCPQ tree.evaluate plus construction and collection of every node's "
        "EvaluationResultWithCount structures"
    ),
    "correctness_boundary": (
        "root external-ID canonicalization, sorting, compact-JSON serialization, "
        "and SHA-256 outside the timed region"
    ),
    "canonicalization": {
        "scope": "root node situations",
        "identifiers": "OCEL external object and event IDs",
        "violation": "violation reason normalized to boolean",
        "q6": "maximum child duration normalized to integer microseconds",
        "ordering": ("lexicographic compact-JSON row order; duplicates retained"),
        "tuples": {
            "Q1": ["application_external_id", "violated"],
            "Q2": [
                "offer_external_id",
                "created_event_external_id",
                "violated",
            ],
            "Q3": ["returned_event_external_id", "violated"],
            "Q4": [
                "application_external_id",
                "accepted_event_external_id",
                "violated",
            ],
            "Q5": [
                "application_external_id",
                "case_r_external_id",
                "accepted_event_external_id",
                "violated",
            ],
            "Q6": ["duration_microseconds"],
            "Q7": [
                "application_external_id",
                "offer_1_external_id",
                "offer_2_external_id",
                "created_event_1_external_id",
                "created_event_2_external_id",
            ],
        },
    },
}
EXPECTED_CANDIDATE_METHOD = {
    "warmups_per_query": 10,
    "measured_runs_per_query": 30,
    "timing_boundary": (
        "persistent prepared PostgreSQL query/fetch, BindingCapsule decode, and "
        "full logical root-row materialization into owned Rust structs"
    ),
    "correctness_boundary": (
        "external-ID normalization, canonical sorting, compact JSON, and SHA-256 "
        "after the headline clock"
    ),
    "non_headline_diagnostic_boundary": (
        "diagnostic only, not the OCPQ-comparable headline: headline time plus "
        "separately timed external-ID canonicalization, sorting, compact JSON, "
        "and SHA-256"
    ),
    "memory_diagnostic_boundary": (
        "separate untimed materialization pass with deterministic owned-row bytes "
        "and retained PostgreSQL backend memory sampled immediately before and "
        "after; not peak memory and no allocator instrumentation"
    ),
    "storage_scope": (
        "pg_total_relation_size for the complete retained serving representation "
        "in the ocpm schema; total includes indexes, and binding summary covers "
        "binding_% relations"
    ),
    "concurrency_boundary": (
        "per-client persistent PostgreSQL connection and prepared Q1-Q7 set; one "
        "exact canonical Q1-Q7 check per client occurs before and after every timed "
        "epoch; timed requests cycle Q1-Q7, enforce the reference logical-row "
        "count, and include query/fetch, capsule decode, and complete owned-row "
        "expansion but exclude canonicalization"
    ),
    "concurrency_protocol": (
        "3 epochs per client level; every client completes at least 32 requests "
        "and the shared epoch wall clock runs for at least 5.000 seconds; aggregate "
        "throughput, wall time, and latency fields are medians of the corresponding "
        "epoch values"
    ),
    "q5_sql": "ocpm.binding_relation_universal_equal",
}

WARMUPS = 10
RUNS = 30
REQUIRED_MINIMUM_QUERY_SPEEDUP = 5.0
TARGET_MINIMUM_QUERY_SPEEDUP = 10.0
TARGET_GEOMETRIC_MEAN_SPEEDUP = 10.0
MAXIMUM_TOTAL_SERVING_BYTES = 128 * 1024 * 1024
MAXIMUM_INDEX_BYTES = 16 * 1024 * 1024
MAXIMUM_BINDING_BYTES = 8 * 1024 * 1024
MAXIMUM_FRESH_PROCESS_PEAK_BYTES = 8 * 1024 * 1024
MINIMUM_CONCURRENCY_SCALING = 5.0
MAXIMUM_CONCURRENCY_P95_MS = 10.0
CONCURRENCY_EPOCHS = 3
MINIMUM_CONCURRENCY_REQUESTS_PER_CLIENT = 32
MINIMUM_CONCURRENCY_WALL_MS = 5_000.0
MAXIMUM_CONCURRENCY_THROUGHPUT_CV = 0.15


def load(path: Path, expected_digest: str) -> dict:
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise SystemExit(f"could not load {path}: {error}") from error
    digest = hashlib.sha256(payload).hexdigest()
    if digest != expected_digest:
        fail(f"{path} digest is inconsistent: {digest} != {expected_digest}")
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SystemExit(f"could not parse {path}: {error}") from error
    if not isinstance(value, dict):
        raise SystemExit(f"{path} must contain a JSON object")
    return value


def fail(message: str) -> None:
    raise SystemExit(message)


def is_integer(value: object) -> bool:
    return type(value) is int


def is_finite_number(value: object) -> bool:
    if type(value) not in {int, float}:
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def require_positive(value: object, label: str) -> float:
    if not is_finite_number(value) or value <= 0:
        fail(f"{label} must be finite and positive, found {value!r}")
    return float(value)


def require_nonnegative(value: object, label: str) -> float:
    if not is_finite_number(value) or value < 0:
        fail(f"{label} must be finite and nonnegative, found {value!r}")
    return float(value)


def require_nonnegative_integer(value: object, label: str) -> int:
    if not is_integer(value) or value < 0:
        fail(f"{label} must be a nonnegative integer, found {value!r}")
    return value


def require_exact_keys(value: object, expected: set[str], label: str) -> dict:
    if not isinstance(value, dict) or set(value) != expected:
        fail(f"{label} must contain exactly {', '.join(sorted(expected))}")
    return value


def require_sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        fail(f"{label} must be a lowercase SHA-256")
    return value


def close(actual: float, expected: float, label: str) -> None:
    if not is_finite_number(actual) or not is_finite_number(expected):
        fail(f"{label} must compare finite numbers: {actual!r}, {expected!r}")
    if not math.isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-12):
        fail(f"{label} is inconsistent: {actual!r} != {expected!r}")


def geometric_mean(values: list[float]) -> float:
    return math.exp(statistics.fmean(math.log(value) for value in values))


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(len(ordered) * fraction) - 1)]


def validate_reference(reference: dict, allow_preview: bool) -> None:
    if (
        not is_integer(reference.get("schema_version"))
        or reference.get("schema_version") != 3
    ):
        fail("OCPQ reference must use schema version 3")
    source = require_exact_keys(
        reference.get("source"),
        set(EXPECTED_SOURCE) | {"docker_image_id"},
        "OCPQ reference source",
    )
    for field, expected in EXPECTED_SOURCE.items():
        if source.get(field) != expected:
            fail(f"unexpected OCPQ reference source pin: {field}")
    if (
        not isinstance(source.get("docker_image_id"), str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", source["docker_image_id"]) is None
    ):
        fail("OCPQ reference source has an invalid immutable Docker image ID")
    environment = reference.get("environment")
    if not isinstance(environment, dict):
        fail("OCPQ reference environment must be an object")
    host_id = environment.get("benchmark_host_id")
    if (
        not isinstance(host_id, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", host_id) is None
    ):
        fail("OCPQ reference benchmark-host fingerprint is invalid")
    source_revision = environment.get("source_revision")
    source_tree_clean = environment.get("source_tree_clean")
    if type(source_tree_clean) is not bool:
        fail("OCPQ reference source_tree_clean provenance must be a boolean")
    if (
        not isinstance(source_revision, str)
        or re.fullmatch(r"[0-9a-f]{40}", source_revision) is None
    ):
        fail("OCPQ reference source revision must be an exact Git revision")
    if not allow_preview and source_tree_clean is not True:
        fail("release validation requires a clean OCPQ reference harness tree")
    method = reference.get("method")
    if method != EXPECTED_REFERENCE_METHOD:
        fail("OCPQ reference methodology is not the matched schema-3 boundary")
    queries = require_exact_keys(
        reference.get("queries"), QUERY_NAMES, "OCPQ reference queries"
    )
    for name, query in queries.items():
        if not isinstance(query, dict):
            fail(f"{name} OCPQ reference query must be an object")
        samples = query.get("runs_ms", [])
        published = query.get("author_published_runs_ms", [])
        if not isinstance(samples, list) or len(samples) != RUNS:
            fail(f"{name} same-host OCPQ run count is incomplete")
        if not isinstance(published, list) or len(published) != 10:
            fail(f"{name} author-published source run count is incomplete")
        if not all(is_finite_number(value) and value > 0 for value in samples):
            fail(f"{name} same-host OCPQ samples are invalid")
        if not all(is_finite_number(value) and value > 0 for value in published):
            fail(f"{name} author-published OCPQ samples are invalid")
        require_positive(query.get("mean_ms"), f"{name} OCPQ mean")
        require_positive(
            query.get("author_published_mean_ms"),
            f"{name} author-published OCPQ mean",
        )
        close(statistics.fmean(samples), query["mean_ms"], f"{name} OCPQ mean")
        close(
            statistics.fmean(published),
            query["author_published_mean_ms"],
            f"{name} author-published mean",
        )
        output = query.get("canonical_output", {})
        if not isinstance(output, dict):
            fail(f"{name} canonical OCPQ output must be an object")
        rows = require_nonnegative_integer(output.get("rows"), f"{name} output rows")
        if rows == 0:
            fail(f"{name} canonical OCPQ output must contain rows")
        require_sha256(output.get("sha256"), f"{name} canonical output fingerprint")
        if name in {"Q1", "Q2", "Q3", "Q4", "Q5"}:
            violations = require_nonnegative_integer(
                output.get("violations"), f"{name} output violations"
            )
            if violations > rows or set(output) != {"rows", "sha256", "violations"}:
                fail(f"{name} canonical violation output is invalid")
        elif name == "Q6":
            duration = require_nonnegative_integer(
                output.get("duration_microseconds"), "Q6 output duration"
            )
            if (
                rows != 1
                or duration < 0
                or set(output)
                != {
                    "rows",
                    "sha256",
                    "duration_microseconds",
                }
            ):
                fail("Q6 canonical duration output is invalid")
        elif set(output) != {"rows", "sha256"}:
            fail("Q7 canonical output contains unexpected fields")


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


def validate_environment(environment: object, allow_preview: bool) -> dict:
    if not isinstance(environment, dict):
        fail("candidate environment must be an object")
    source_revision = environment.get("source_revision")
    source_tree_clean = environment.get("source_tree_clean")
    pg_ocpm_source_revision = environment.get("pg_ocpm_source_revision")
    pg_ocpm_source_tree_clean = environment.get("pg_ocpm_source_tree_clean")
    if type(source_tree_clean) is not bool:
        fail("candidate source_tree_clean provenance must be a boolean")
    if type(pg_ocpm_source_tree_clean) is not bool:
        fail("pg_ocpm source_tree_clean provenance must be a boolean")
    if allow_preview:
        if not isinstance(source_revision, str) or not source_revision.strip():
            fail("preview candidate source revision must not be empty")
        if (
            not isinstance(pg_ocpm_source_revision, str)
            or not pg_ocpm_source_revision.strip()
        ):
            fail("preview pg_ocpm source revision must not be empty")
    elif (
        not isinstance(source_revision, str)
        or re.fullmatch(r"[0-9a-f]{40}", source_revision) is None
        or source_tree_clean is not True
    ):
        fail(
            "release validation requires a clean source tree and an exact "
            "40-character lowercase Git revision; use --allow-preview only "
            "for unpublished working-tree results"
        )
    if not allow_preview and (
        not isinstance(pg_ocpm_source_revision, str)
        or re.fullmatch(r"[0-9a-f]{40}", pg_ocpm_source_revision) is None
        or pg_ocpm_source_tree_clean is not True
    ):
        fail(
            "release validation requires a clean pg_ocpm source tree and an "
            "exact 40-character lowercase Git revision"
        )

    benchmark_host_id = environment.get("benchmark_host_id")
    if (
        not isinstance(benchmark_host_id, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", benchmark_host_id) is None
    ):
        fail("candidate benchmark-host fingerprint is invalid")

    for label_field, id_field in (
        ("candidate_image", "candidate_image_id"),
        ("database_image", "database_image_id"),
    ):
        label = environment.get(label_field)
        image_id = environment.get(id_field)
        if allow_preview and image_id == "unspecified":
            if not isinstance(label, str) or not label.strip():
                fail(f"preview {label_field} must not be empty")
            continue
        if not isinstance(label, str) or label in {"", "unspecified"}:
            fail(f"candidate provenance is missing {label_field}")
        if (
            not isinstance(image_id, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is None
        ):
            fail(f"candidate provenance has an invalid {id_field}")

    if (
        environment.get("postgres_jit") != "off"
        or not isinstance(environment.get("postgres_server_version"), str)
        or not environment["postgres_server_version"].strip()
        or not is_integer(environment.get("postgres_server_version_num"))
        or environment["postgres_server_version_num"] <= 0
        or not isinstance(environment.get("client_os"), str)
        or not isinstance(environment.get("client_arch"), str)
    ):
        fail("candidate database or client environment provenance is invalid")
    return environment


def validate_candidate(candidate: dict, reference: dict, allow_preview: bool) -> None:
    if (
        not is_integer(candidate.get("schema_version"))
        or candidate.get("schema_version") != 4
    ):
        fail("candidate artifact must use schema version 4")
    if candidate.get("release") != EXPECTED_RELEASE:
        fail("unexpected OCPQ candidate release versions")
    require_nonnegative_integer(
        candidate.get("generated_at_unix_ms"), "candidate generation timestamp"
    )
    if candidate.get("reference_source") != reference.get("source"):
        fail("candidate does not pin the committed same-host OCPQ source")
    environment = validate_environment(candidate.get("environment"), allow_preview)
    if environment.get("benchmark_host_id") != reference.get("environment", {}).get(
        "benchmark_host_id"
    ):
        fail("candidate and OCPQ reference benchmark-host fingerprints differ")
    if environment.get("source_revision") != reference.get("environment", {}).get(
        "source_revision"
    ) or environment.get("source_tree_clean") != reference.get("environment", {}).get(
        "source_tree_clean"
    ):
        fail("candidate and OCPQ reference harness provenance differs")
    method = candidate.get("method")
    if method != EXPECTED_CANDIDATE_METHOD:
        fail("candidate methodology is not the matched Rust schema-3 boundary")
    reject_cross_host_ratios(candidate)

    queries = require_exact_keys(
        candidate.get("queries"), QUERY_NAMES, "candidate artifact queries"
    )
    speedups: list[float] = []
    candidate_means: list[float] = []
    reference_means: list[float] = []
    published_means: list[float] = []
    maximum_owned_rows = 0
    for name, query in queries.items():
        if not isinstance(query, dict):
            fail(f"{name} candidate query must be an object")
        expected = reference["queries"][name]
        samples = query.get("runs_ms", [])
        if not isinstance(samples, list) or len(samples) != RUNS:
            fail(f"{name} candidate run count is incomplete")
        if not all(is_finite_number(value) and value > 0 for value in samples):
            fail(f"{name} candidate samples are invalid")
        candidate_mean = statistics.fmean(samples)
        require_positive(query.get("mean_ms"), f"{name} candidate mean")
        require_positive(query.get("p50_ms"), f"{name} candidate p50")
        require_positive(query.get("p95_ms"), f"{name} candidate p95")
        if query["p50_ms"] > query["p95_ms"]:
            fail(f"{name} candidate percentiles are out of order")
        close(candidate_mean, query["mean_ms"], f"{name} candidate mean")
        close(percentile(samples, 0.5), query["p50_ms"], f"{name} candidate p50")
        close(percentile(samples, 0.95), query["p95_ms"], f"{name} candidate p95")
        close(
            query["reference_ocpq_mean_ms"],
            expected["mean_ms"],
            f"{name} same-host OCPQ mean",
        )
        close(
            query["author_published_ocpq_mean_ms"],
            expected["author_published_mean_ms"],
            f"{name} author-published source mean",
        )
        speedup = expected["mean_ms"] / candidate_mean
        require_positive(
            query.get("speedup_vs_reference_ocpq"), f"{name} same-host speedup"
        )
        close(speedup, query["speedup_vs_reference_ocpq"], f"{name} speedup")
        if speedup < REQUIRED_MINIMUM_QUERY_SPEEDUP:
            fail(
                f"{name} is below the required "
                f"{REQUIRED_MINIMUM_QUERY_SPEEDUP:.0f}x same-host speedup gate"
            )
        if speedup < TARGET_MINIMUM_QUERY_SPEEDUP:
            fail(
                f"{name} is below the release target of "
                f"{TARGET_MINIMUM_QUERY_SPEEDUP:.0f}x same-host speedup"
            )
        if query.get("canonical_output") != expected["canonical_output"]:
            fail(f"{name} canonical output differs from same-host OCPQ")
        logical_rows = query.get("logical_rows_materialized")
        if (
            query.get("semantic_parity") is not True
            or not is_integer(logical_rows)
            or logical_rows != expected["canonical_output"]["rows"]
        ):
            fail(f"{name} exact semantic parity is false")
        require_nonnegative_integer(query.get("capsule_bytes"), f"{name} capsule bytes")
        require_nonnegative_integer(
            query.get("canonical_json_bytes"), f"{name} canonical JSON bytes"
        )
        diagnostic_runs = query.get("diagnostic_canonicalization_runs_ms", [])
        if (
            not isinstance(diagnostic_runs, list)
            or len(diagnostic_runs) != RUNS
            or not all(
                is_finite_number(value) and value >= 0 for value in diagnostic_runs
            )
        ):
            fail(f"{name} canonicalization diagnostics are invalid")
        diagnostic_mean = statistics.fmean(
            headline + canonical
            for headline, canonical in zip(samples, diagnostic_runs, strict=True)
        )
        require_positive(
            query.get("diagnostic_query_to_fingerprint_mean_ms"),
            f"{name} diagnostic query-to-fingerprint mean",
        )
        close(
            diagnostic_mean,
            query["diagnostic_query_to_fingerprint_mean_ms"],
            f"{name} diagnostic query-to-fingerprint mean",
        )
        memory = query.get("memory", {})
        if not isinstance(memory, dict):
            fail(f"{name} query memory must be an object")
        for field in (
            "owned_rows_bytes",
            "postgres_backend_baseline_bytes",
            "postgres_backend_after_bytes",
        ):
            require_nonnegative_integer(memory.get(field), f"{name} {field}")
        backend_delta = memory.get("postgres_backend_delta_bytes")
        if not is_integer(backend_delta) or backend_delta != (
            memory["postgres_backend_after_bytes"]
            - memory["postgres_backend_baseline_bytes"]
        ):
            fail(f"{name} PostgreSQL backend memory delta is inconsistent")
        speedups.append(speedup)
        candidate_means.append(candidate_mean)
        reference_means.append(expected["mean_ms"])
        published_means.append(expected["author_published_mean_ms"])
        maximum_owned_rows = max(
            maximum_owned_rows,
            require_nonnegative_integer(
                memory.get("owned_rows_bytes"),
                f"{name} owned-row bytes",
            ),
        )

    summary = candidate.get("summary", {})
    if not isinstance(summary, dict):
        fail("candidate summary must be an object")
    for field in (
        "author_published_ocpq_geometric_mean_ms",
        "same_host_ocpq_geometric_mean_ms",
        "candidate_geometric_mean_ms",
        "speedup_geometric_mean",
        "minimum_query_speedup",
    ):
        require_positive(summary.get(field), f"candidate summary {field}")
    speedup_geomean = geometric_mean(speedups)
    close(
        geometric_mean(published_means),
        summary["author_published_ocpq_geometric_mean_ms"],
        "author-published OCPQ geometric mean",
    )
    close(
        geometric_mean(reference_means),
        summary["same_host_ocpq_geometric_mean_ms"],
        "same-host OCPQ geometric mean",
    )
    close(
        geometric_mean(candidate_means),
        summary["candidate_geometric_mean_ms"],
        "candidate geometric mean",
    )
    close(speedup_geomean, summary["speedup_geometric_mean"], "speedup geometric mean")
    close(min(speedups), summary["minimum_query_speedup"], "minimum query speedup")
    if speedup_geomean < TARGET_GEOMETRIC_MEAN_SPEEDUP:
        fail(
            "candidate is below the "
            f"{TARGET_GEOMETRIC_MEAN_SPEEDUP:.0f}x same-host geometric-mean target"
        )
    if summary.get("all_queries_exact") is not True:
        fail("candidate exact Q1-Q7 parity gate is false")
    if (
        not is_integer(summary.get("maximum_owned_rows_bytes"))
        or summary.get("maximum_owned_rows_bytes") != maximum_owned_rows
    ):
        fail("candidate maximum owned-row memory is inconsistent")

    storage = candidate.get("storage", {})
    if not isinstance(storage, dict):
        fail("candidate storage must be an object")
    storage_limits = {
        "total_serving_bytes": MAXIMUM_TOTAL_SERVING_BYTES,
        "index_bytes": MAXIMUM_INDEX_BYTES,
        "binding_summary_bytes": MAXIMUM_BINDING_BYTES,
    }
    for field, limit in storage_limits.items():
        value = storage.get(field)
        if not is_integer(value) or value < 0 or value > limit:
            fail(f"candidate storage gate failed for {field}: {value!r} > {limit}")
    if (
        storage["index_bytes"] > storage["total_serving_bytes"]
        or storage["binding_summary_bytes"] > storage["total_serving_bytes"]
    ):
        fail("candidate storage components exceed total serving storage")

    concurrency = require_exact_keys(
        candidate.get("concurrency"),
        {"1", "4", "8", "16"},
        "candidate concurrency sweep",
    )
    for label, report in concurrency.items():
        clients = int(label)
        if not isinstance(report, dict):
            fail(f"candidate concurrency report at {clients} clients must be an object")
        epochs = report.get("epochs", [])
        if (
            not is_integer(report.get("clients"))
            or report.get("clients") != clients
            or not is_integer(report.get("epoch_count"))
            or report.get("epoch_count") != CONCURRENCY_EPOCHS
            or not is_integer(report.get("minimum_requests_per_client"))
            or report.get("minimum_requests_per_client")
            != MINIMUM_CONCURRENCY_REQUESTS_PER_CLIENT
            or not is_finite_number(report.get("minimum_wall_time_ms"))
            or report.get("minimum_wall_time_ms") != MINIMUM_CONCURRENCY_WALL_MS
            or not isinstance(epochs, list)
            or len(epochs) != CONCURRENCY_EPOCHS
            or report.get("semantic_parity") is not True
        ):
            fail(f"candidate concurrency protocol failed at {clients} clients")

        epoch_throughputs: list[float] = []
        epoch_wall_times: list[float] = []
        epoch_p50: list[float] = []
        epoch_p95: list[float] = []
        epoch_p99: list[float] = []
        total_requests = 0
        total_query_counts = {name: 0 for name in QUERY_NAMES}
        expected_client_ids = list(range(clients))
        expected_client_keys = {str(client_id) for client_id in expected_client_ids}

        for epoch_index, epoch in enumerate(epochs, start=1):
            if (
                not isinstance(epoch, dict)
                or not is_integer(epoch.get("epoch"))
                or epoch.get("epoch") != epoch_index
            ):
                fail(
                    f"candidate concurrency epoch {epoch_index} is invalid at "
                    f"{clients} clients"
                )
            warmed_client_ids = epoch.get("warmed_client_ids")
            verified_client_ids = epoch.get("post_epoch_verified_client_ids")
            pre_checks = require_exact_keys(
                epoch.get("pre_epoch_exact_query_checks"),
                QUERY_NAMES,
                f"{clients}-client epoch {epoch_index} pre-epoch parity checks",
            )
            post_checks = require_exact_keys(
                epoch.get("post_epoch_exact_query_checks"),
                QUERY_NAMES,
                f"{clients}-client epoch {epoch_index} post-epoch parity checks",
            )
            if (
                not isinstance(warmed_client_ids, list)
                or not all(is_integer(value) for value in warmed_client_ids)
                or warmed_client_ids != expected_client_ids
                or not isinstance(verified_client_ids, list)
                or not all(is_integer(value) for value in verified_client_ids)
                or verified_client_ids != expected_client_ids
                or any(
                    not is_integer(value) or value != clients
                    for value in pre_checks.values()
                )
                or any(
                    not is_integer(value) or value != clients
                    for value in post_checks.values()
                )
                or epoch.get("semantic_parity") is not True
            ):
                fail(
                    f"candidate concurrency epoch {epoch_index} lacks complete "
                    f"pre/post exact-parity evidence at {clients} clients"
                )

            client_counts = require_exact_keys(
                epoch.get("client_request_counts"),
                expected_client_keys,
                f"{clients}-client epoch {epoch_index} client request counts",
            )
            for client_id, count in client_counts.items():
                if (
                    not is_integer(count)
                    or count < MINIMUM_CONCURRENCY_REQUESTS_PER_CLIENT
                ):
                    fail(
                        f"{clients}-client epoch {epoch_index} client {client_id} "
                        "did not meet the request floor"
                    )

            query_counts = require_exact_keys(
                epoch.get("query_request_counts"),
                QUERY_NAMES,
                f"{clients}-client epoch {epoch_index} query request counts",
            )
            if any(
                not is_integer(count) or count <= 0 for count in query_counts.values()
            ):
                fail(
                    f"{clients}-client epoch {epoch_index} Q1-Q7 counts must be "
                    "positive integers"
                )

            request_count = require_nonnegative_integer(
                epoch.get("request_count"),
                f"{clients}-client epoch {epoch_index} request count",
            )
            if (
                request_count < clients * MINIMUM_CONCURRENCY_REQUESTS_PER_CLIENT
                or sum(client_counts.values()) != request_count
                or sum(query_counts.values()) != request_count
            ):
                fail(
                    f"{clients}-client epoch {epoch_index} request totals "
                    "are inconsistent"
                )

            wall_time_ms = require_positive(
                epoch.get("wall_time_ms"),
                f"{clients}-client epoch {epoch_index} wall time",
            )
            throughput = require_positive(
                epoch.get("throughput_requests_per_second"),
                f"{clients}-client epoch {epoch_index} throughput",
            )
            p50 = require_positive(
                epoch.get("latency_p50_ms"),
                f"{clients}-client epoch {epoch_index} p50",
            )
            p95 = require_positive(
                epoch.get("latency_p95_ms"),
                f"{clients}-client epoch {epoch_index} p95",
            )
            p99 = require_positive(
                epoch.get("latency_p99_ms"),
                f"{clients}-client epoch {epoch_index} p99",
            )
            if wall_time_ms < MINIMUM_CONCURRENCY_WALL_MS or not p50 <= p95 <= p99:
                fail(
                    f"{clients}-client epoch {epoch_index} duration or percentile "
                    "ordering is invalid"
                )
            close(
                request_count * 1000.0 / wall_time_ms,
                throughput,
                f"{clients}-client epoch {epoch_index} throughput",
            )
            epoch_throughputs.append(throughput)
            epoch_wall_times.append(wall_time_ms)
            epoch_p50.append(p50)
            epoch_p95.append(p95)
            epoch_p99.append(p99)
            total_requests += request_count
            for name, count in query_counts.items():
                total_query_counts[name] += count

        throughput_mean = statistics.fmean(epoch_throughputs)
        throughput_cv = statistics.pstdev(epoch_throughputs) / throughput_mean
        aggregate_fields = {
            "median_epoch_wall_time_ms": percentile(epoch_wall_times, 0.5),
            "median_epoch_throughput_requests_per_second": percentile(
                epoch_throughputs, 0.5
            ),
            "minimum_epoch_throughput_requests_per_second": min(epoch_throughputs),
            "maximum_epoch_throughput_requests_per_second": max(epoch_throughputs),
            "epoch_throughput_cv": throughput_cv,
            "median_epoch_latency_p50_ms": percentile(epoch_p50, 0.5),
            "median_epoch_latency_p95_ms": percentile(epoch_p95, 0.5),
            "median_epoch_latency_p99_ms": percentile(epoch_p99, 0.5),
        }
        for field, expected_value in aggregate_fields.items():
            require_nonnegative(report.get(field), f"{clients}-client {field}")
            close(expected_value, report[field], f"{clients}-client {field}")
        if not (
            0
            < report["minimum_epoch_throughput_requests_per_second"]
            <= report["median_epoch_throughput_requests_per_second"]
            <= report["maximum_epoch_throughput_requests_per_second"]
        ):
            fail(f"{clients}-client aggregate throughput ordering is invalid")
        if not (
            0
            < report["median_epoch_latency_p50_ms"]
            <= report["median_epoch_latency_p95_ms"]
            <= report["median_epoch_latency_p99_ms"]
        ):
            fail(f"{clients}-client aggregate latency ordering is invalid")
        if report["epoch_throughput_cv"] > MAXIMUM_CONCURRENCY_THROUGHPUT_CV:
            fail(f"{clients}-client epoch throughput CV exceeds the release gate")
        if report["median_epoch_latency_p95_ms"] > MAXIMUM_CONCURRENCY_P95_MS:
            fail(f"{clients}-client median epoch p95 exceeds the release gate")
        if (
            not is_integer(report.get("total_request_count"))
            or report.get("total_request_count") != total_requests
        ):
            fail(f"{clients}-client total request count is inconsistent")
        aggregate_query_counts = require_exact_keys(
            report.get("total_query_request_counts"),
            QUERY_NAMES,
            f"{clients}-client aggregate query request counts",
        )
        if any(
            not is_integer(count) or count <= 0
            for count in aggregate_query_counts.values()
        ):
            fail(f"{clients}-client aggregate Q1-Q7 counts are invalid")
        if aggregate_query_counts != total_query_counts:
            fail(f"{clients}-client aggregate Q1-Q7 counts are inconsistent")

    scaling = (
        concurrency["16"]["median_epoch_throughput_requests_per_second"]
        / concurrency["1"]["median_epoch_throughput_requests_per_second"]
    )
    scaling_field = "concurrency_16_to_1_median_epoch_throughput_scaling"
    require_positive(summary.get(scaling_field), "16-to-1 median-epoch scaling")
    close(scaling, summary[scaling_field], "16-to-1 median-epoch scaling")
    if scaling < MINIMUM_CONCURRENCY_SCALING:
        fail(
            f"candidate 16-to-1 median-epoch throughput scaling is {scaling:.3f}x, "
            f"below the required {MINIMUM_CONCURRENCY_SCALING:.1f}x gate"
        )


def validate_memory(memory: dict, candidate: dict, reference: dict) -> None:
    if (
        not is_integer(memory.get("schema_version"))
        or memory.get("schema_version") != 3
    ):
        fail("fresh-process memory artifact must use schema version 3")
    if (
        memory.get("release") != EXPECTED_RELEASE
        or memory.get("environment") != candidate.get("environment")
        or memory.get("reference_source") != reference.get("source")
        or memory.get("mode") != "fresh-container-per-query-peak-rss-diagnostic"
        or memory.get("fresh_container_per_query") is not True
    ):
        fail("fresh-process memory provenance is inconsistent")
    require_nonnegative_integer(
        memory.get("generated_at_unix_ms"), "memory generation timestamp"
    )
    queries = require_exact_keys(
        memory.get("queries"), QUERY_NAMES, "fresh-process memory queries"
    )
    maximum_owned = 0
    maximum_peak = 0
    for name, query in queries.items():
        if not isinstance(query, dict):
            fail(f"{name} fresh-process memory query must be an object")
        expected = reference["queries"][name]["canonical_output"]
        candidate_query = candidate["queries"][name]
        logical_rows = query.get("logical_rows_materialized")
        expected_rows = query.get("expected_rows")
        if (
            not is_integer(query.get("schema_version"))
            or query.get("schema_version") != 1
            or query.get("semantic_parity") is not True
            or query.get("canonical_output") != expected
            or not is_integer(logical_rows)
            or logical_rows != expected["rows"]
            or not is_integer(expected_rows)
            or expected_rows != expected["rows"]
            or query.get("capsule_bytes") != candidate_query["capsule_bytes"]
            or query.get("owned_rows_bytes")
            != candidate_query["memory"]["owned_rows_bytes"]
        ):
            fail(f"{name} fresh-process exact parity or result shape is inconsistent")
        for field in (
            "capsule_bytes",
            "owned_rows_bytes",
            "baseline_rss_bytes",
            "baseline_vmhwm_bytes",
            "after_rss_bytes",
            "after_vmhwm_bytes",
            "peak_over_baseline_rss_bytes",
        ):
            require_nonnegative_integer(query.get(field), f"{name} memory {field}")
        if (
            query["baseline_vmhwm_bytes"] < query["baseline_rss_bytes"]
            or query["after_vmhwm_bytes"] < query["after_rss_bytes"]
            or query["after_vmhwm_bytes"] < query["baseline_vmhwm_bytes"]
            or query["after_vmhwm_bytes"] < query["baseline_rss_bytes"]
        ):
            fail(f"{name} RSS/VmHWM ordering is invalid")
        if not is_integer(query.get("rss_delta_bytes")):
            fail(f"{name} RSS delta must be an integer")
        if (
            query["rss_delta_bytes"]
            != query["after_rss_bytes"] - query["baseline_rss_bytes"]
        ):
            fail(f"{name} RSS delta is inconsistent")
        if not is_integer(query.get("vmhwm_delta_bytes")):
            fail(f"{name} VmHWM delta must be an integer")
        if (
            query["vmhwm_delta_bytes"]
            != query["after_vmhwm_bytes"] - query["baseline_vmhwm_bytes"]
        ):
            fail(f"{name} VmHWM delta is inconsistent")
        if (
            query["peak_over_baseline_rss_bytes"]
            != query["after_vmhwm_bytes"] - query["baseline_rss_bytes"]
        ):
            fail(f"{name} peak-over-baseline RSS is inconsistent")
        maximum_owned = max(maximum_owned, query["owned_rows_bytes"])
        maximum_peak = max(maximum_peak, query["peak_over_baseline_rss_bytes"])
    summary = memory.get("summary", {})
    if (
        not isinstance(summary, dict)
        or summary.get("all_queries_exact") is not True
        or not is_integer(summary.get("maximum_owned_rows_bytes"))
        or summary.get("maximum_owned_rows_bytes") != maximum_owned
        or not is_integer(summary.get("maximum_peak_over_baseline_rss_bytes"))
        or summary.get("maximum_peak_over_baseline_rss_bytes") != maximum_peak
    ):
        fail("fresh-process memory summary is inconsistent")
    if maximum_peak > MAXIMUM_FRESH_PROCESS_PEAK_BYTES:
        fail(
            "fresh-process peak-over-baseline memory exceeds "
            f"{MAXIMUM_FRESH_PROCESS_PEAK_BYTES} bytes"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", type=Path, default=REFERENCE_PATH)
    parser.add_argument("--candidate", type=Path, default=CANDIDATE_PATH)
    parser.add_argument("--memory", type=Path, default=MEMORY_PATH)
    parser.add_argument(
        "--expected-reference-sha256", default=EXPECTED_REFERENCE_SHA256
    )
    parser.add_argument(
        "--expected-candidate-sha256", default=EXPECTED_CANDIDATE_SHA256
    )
    parser.add_argument("--expected-memory-sha256", default=EXPECTED_MEMORY_SHA256)
    parser.add_argument(
        "--allow-preview",
        action="store_true",
        help=(
            "accept explicitly digest-pinned unpublished working-tree provenance; "
            "release validation requires a clean 40-hex revision and image IDs"
        ),
    )
    args = parser.parse_args()
    for field in (
        "expected_reference_sha256",
        "expected_candidate_sha256",
        "expected_memory_sha256",
    ):
        require_sha256(getattr(args, field), f"--{field.replace('_', '-')}")
    return args


def main() -> None:
    args = parse_args()
    reference = load(args.reference, args.expected_reference_sha256)
    candidate = load(args.candidate, args.expected_candidate_sha256)
    memory = load(args.memory, args.expected_memory_sha256)
    validate_reference(reference, args.allow_preview)
    validate_candidate(candidate, reference, args.allow_preview)
    validate_memory(memory, candidate, reference)
    mode = "preview" if args.allow_preview else "release"
    print(
        f"OCPQ schema-4 candidate passes {mode} exact parity, same-host speed, "
        "memory, storage, provenance, and concurrency gates"
    )


if __name__ == "__main__":
    main()
