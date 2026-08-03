#!/usr/bin/env python3
"""Verify the descriptive OCPQ four-way preview from raw evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import statistics
from pathlib import Path
from typing import Any

QUERIES = tuple(f"Q{index}" for index in range(1, 8))
DEFAULT_PG_OCPM_VERSION = "1.0.0"
DEFAULT_OCPM_ENGINE_VERSION = "1.0.0"
STABLE_SOURCE = {
    "ocpq_eval_commit": "846dd4eb9f8600ae42355968453a9412ea4759c2",
    "ocpq_version": "0.6.7",
    "ocpq_commit": "80457e561edd7bb9e142d959dd7e0f96e6b03f2f",
    "dataset_sqlite_sha256": (
        "02ac333a2c194b5a411cb8527dd64b4845e5110752d2ffddb531e48ce97556d7"
    ),
    "query_files_sha256": (
        "387aeb31398d86ef8e7b15393649cbabe75be56185fd67d27021744025873466"
    ),
    "author_published_results_commit": ("846dd4eb9f8600ae42355968453a9412ea4759c2"),
}
NODE_FIELDS = (
    "node_index",
    "object_variables",
    "event_variables",
    "label_names",
    "situation_count",
    "situation_violated_count",
    "violation_reason_counts",
    "canonical_json_bytes",
    "canonical_sha256",
)


class FourWayError(ValueError):
    """Raised when the preview evidence is inconsistent or unpinned."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise FourWayError(message)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_pinned(path: Path, expected_sha256: str) -> dict[str, Any]:
    actual = _sha256(path)
    _require(actual == expected_sha256, f"{path}: SHA-256 {actual} != expected")
    value = json.loads(path.read_text())
    _require(isinstance(value, dict), f"{path}: root must be an object")
    return value


def _stable_source(value: dict[str, Any]) -> dict[str, Any]:
    return {key: value.get(key) for key in STABLE_SOURCE}


def _nodes(value: dict[str, Any]) -> list[dict[str, Any]]:
    return [{key: node.get(key) for key in NODE_FIELDS} for node in value["nodes"]]


def _positive_finite_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and value > 0
    )


def _mean(samples: Any, label: str) -> float:
    _require(
        isinstance(samples, list)
        and len(samples) == 10
        and all(_positive_finite_number(item) for item in samples),
        f"{label}: expected ten positive measured samples",
    )
    return statistics.fmean(samples)


def _close(left: float, right: Any, label: str) -> None:
    _require(
        _positive_finite_number(right)
        and math.isclose(left, float(right), rel_tol=1e-12, abs_tol=1e-12),
        f"{label}: stored summary does not match raw samples",
    )


def _geomean(values: list[float]) -> float:
    return math.exp(statistics.fmean(math.log(value) for value in values))


def _publication_blockers(
    reference: dict[str, Any],
    vanilla: dict[str, Any],
    pg_pm4py: dict[str, Any],
    engine: dict[str, Any],
    *,
    pm4py_runner_sha256: str | None,
) -> list[str]:
    hosts = (
        reference.get("environment", {}).get("benchmark_host_id"),
        vanilla.get("environment", {}).get("benchmark_host_id"),
        pg_pm4py.get("environment", {}).get("benchmark_host_id"),
        engine.get("environment", {})
        .get("latency_and_memory", {})
        .get("benchmark_host_id"),
    )
    blockers = []
    if any(not isinstance(host, str) or not host for host in hosts):
        blockers.append("one or more artifacts omit the cross-arm benchmark_host_id")
    elif len(set(hosts)) != 1:
        blockers.append("artifact benchmark_host_id values differ across arms")

    pg_source = pg_pm4py.get("source", {})
    pg_revision = pg_source.get("pg_ocpm_source_revision")
    if (
        pg_source.get("pg_ocpm_source_tree_clean") is not True
        or not isinstance(pg_revision, str)
        or re.fullmatch(r"[0-9a-f]{40}", pg_revision) is None
    ):
        blockers.append("pg_ocpm evidence lacks a clean immutable source revision")
    if engine.get("publication_status", {}).get("ready") is not True:
        blockers.append("the strict engine artifact is not publication-ready")
    if not pm4py_runner_sha256 or any(
        artifact.get("source", {}).get("pm4py_runner_sha256") != pm4py_runner_sha256
        for artifact in (vanilla, pg_pm4py)
    ):
        blockers.append("PM4Py artifacts are not bound to the verified runner digest")
    blockers.append(
        "the four-way checker is descriptive-only; strict publication gates "
        "are separate"
    )
    return blockers


def certify(
    reference: dict[str, Any],
    vanilla: dict[str, Any],
    pg_pm4py: dict[str, Any],
    engine: dict[str, Any],
    *,
    reference_sha256: str,
    pm4py_runner_sha256: str | None = None,
    pg_ocpm_version: str = DEFAULT_PG_OCPM_VERSION,
    ocpm_engine_version: str = DEFAULT_OCPM_ENGINE_VERSION,
) -> dict[str, Any]:
    """Return recomputed evidence after validating the four artifact contracts."""

    for label, artifact in (
        ("reference", reference),
        ("vanilla", vanilla),
        ("pg_pm4py", pg_pm4py),
        ("engine", engine),
    ):
        _require(
            set(artifact.get("queries", {})) == set(QUERIES),
            f"{label}: Q1-Q7 required",
        )

    _require(
        _stable_source(reference.get("source", {})) == STABLE_SOURCE,
        "reference source pin mismatch",
    )
    _require(
        engine.get("reference_artifact_sha256") == reference_sha256,
        "engine reference digest mismatch",
    )
    _require(
        _stable_source(engine.get("reference_source", {})) == STABLE_SOURCE,
        "engine source pin mismatch",
    )
    _require(
        engine.get("fresh_container_per_query") is True,
        "engine requires one fresh container per query",
    )
    _require(
        engine.get("release")
        == {"pg_ocpm": pg_ocpm_version, "ocpm_engine": ocpm_engine_version},
        "engine release mismatch",
    )

    for label, artifact, backend in (
        ("vanilla", vanilla, "vanilla_pg"),
        ("pg_pm4py", pg_pm4py, "pg_ocpm"),
    ):
        source = artifact.get("source", {})
        method = artifact.get("method", {})
        _require(source.get("backend") == backend, f"{label}: backend mismatch")
        _require(
            source.get("dataset") == "bpic2017-ocpq",
            f"{label}: dataset mismatch",
        )
        _require(
            source.get("pm4py_version") == "2.7.23.3",
            f"{label}: PM4Py version mismatch",
        )
        if backend == "vanilla_pg":
            _require(
                source.get("pg_ocpm_version") is None
                and "pg_ocpm_source_revision" not in source
                and "pg_ocpm_source_tree_clean" not in source,
                "vanilla: pg_ocpm provenance must be absent",
            )
        else:
            _require(
                source.get("pg_ocpm_version") == pg_ocpm_version,
                "pg_pm4py: pg_ocpm version mismatch",
            )
            _require(
                isinstance(source.get("pg_ocpm_source_revision"), str)
                and type(source.get("pg_ocpm_source_tree_clean")) is bool,
                "pg_pm4py: pg_ocpm provenance is incomplete",
            )
        _require(
            _stable_source(source.get("reference_source", {})) == STABLE_SOURCE,
            f"{label}: source pin mismatch",
        )
        _require(
            source.get("reference_artifact_sha256") == reference_sha256,
            f"{label}: reference digest mismatch",
        )
        _require(method.get("warmups_per_query") == 0, f"{label}: warmup mismatch")
        _require(
            method.get("measured_runs_per_query") == 10,
            f"{label}: run-count mismatch",
        )
        _require(
            "fresh Docker process" in method.get("query_protocol", ""),
            f"{label}: fresh-process protocol missing",
        )
        _require(
            artifact.get("summary", {}).get("every_query_and_node_exact") is True,
            f"{label}: exactness summary failed",
        )

    rows: dict[str, dict[str, float]] = {}
    for query in QUERIES:
        reference_query = reference["queries"][query]
        vanilla_query = vanilla["queries"][query]
        pg_query = pg_pm4py["queries"][query]
        engine_query = engine["queries"][query]
        expected_nodes = _nodes(reference_query)
        for label, candidate in (
            ("vanilla", vanilla_query),
            ("pg_pm4py", pg_query),
            ("engine", engine_query),
        ):
            _require(
                candidate.get("every_node_exact") is True,
                f"{label}/{query}: exactness failed",
            )
            _require(
                _nodes(candidate) == expected_nodes,
                f"{label}/{query}: node fingerprint mismatch",
            )

        reference_mean = _mean(reference_query.get("runs_ms"), f"reference/{query}")
        vanilla_mean = _mean(vanilla_query.get("runs_ms"), f"vanilla/{query}")
        pg_mean = _mean(pg_query.get("runs_ms"), f"pg_pm4py/{query}")
        engine_mean = _mean(engine_query.get("runs_ms"), f"engine/{query}")
        _close(reference_mean, reference_query.get("mean_ms"), f"reference/{query}")
        _close(vanilla_mean, vanilla_query.get("mean_ms"), f"vanilla/{query}")
        _close(pg_mean, pg_query.get("mean_ms"), f"pg_pm4py/{query}")
        _close(engine_mean, engine_query.get("candidate_mean_ms"), f"engine/{query}")
        _close(
            reference_mean,
            engine_query.get("reference_ocpq_mean_ms"),
            f"engine reference/{query}",
        )
        rows[query] = {
            "ocpq_ms": reference_mean,
            "vanilla_pg_pm4py_ms": vanilla_mean,
            "pg_ocpm_pm4py_ms": pg_mean,
            "pg_ocpm_engine_ms": engine_mean,
        }

    geometric_means = {
        key: _geomean([row[key] for row in rows.values()])
        for key in next(iter(rows.values()))
    }
    engine_mean = geometric_means["pg_ocpm_engine_ms"]
    publication_blockers = _publication_blockers(
        reference,
        vanilla,
        pg_pm4py,
        engine,
        pm4py_runner_sha256=pm4py_runner_sha256,
    )
    return {
        "status": "verified_descriptive_preview",
        "publication_ready": not publication_blockers,
        "publication_blockers": publication_blockers,
        "release": {
            "pg_ocpm": pg_ocpm_version,
            "ocpm_engine": ocpm_engine_version,
        },
        "every_query_and_node_exact": True,
        "queries": rows,
        "geometric_means_ms": geometric_means,
        "engine_speedups": {
            "vs_ocpq": geometric_means["ocpq_ms"] / engine_mean,
            "vs_vanilla_pg_pm4py": geometric_means["vanilla_pg_pm4py_ms"] / engine_mean,
            "vs_pg_ocpm_pm4py": geometric_means["pg_ocpm_pm4py_ms"] / engine_mean,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reference", type=Path)
    parser.add_argument("vanilla", type=Path)
    parser.add_argument("pg_pm4py", type=Path)
    parser.add_argument("engine", type=Path)
    parser.add_argument("--reference-sha256", required=True)
    parser.add_argument("--vanilla-sha256", required=True)
    parser.add_argument("--pg-pm4py-sha256", required=True)
    parser.add_argument("--engine-sha256", required=True)
    parser.add_argument("--pm4py-runner", type=Path, required=True)
    parser.add_argument("--pm4py-runner-sha256", required=True)
    parser.add_argument("--pg-ocpm-version", default=DEFAULT_PG_OCPM_VERSION)
    parser.add_argument("--ocpm-engine-version", default=DEFAULT_OCPM_ENGINE_VERSION)
    args = parser.parse_args()

    reference = _load_pinned(args.reference, args.reference_sha256)
    vanilla = _load_pinned(args.vanilla, args.vanilla_sha256)
    pg_pm4py = _load_pinned(args.pg_pm4py, args.pg_pm4py_sha256)
    engine = _load_pinned(args.engine, args.engine_sha256)
    _require(
        _sha256(args.pm4py_runner) == args.pm4py_runner_sha256,
        f"{args.pm4py_runner}: PM4Py runner SHA-256 mismatch",
    )
    result = certify(
        reference,
        vanilla,
        pg_pm4py,
        engine,
        reference_sha256=args.reference_sha256,
        pm4py_runner_sha256=args.pm4py_runner_sha256,
        pg_ocpm_version=args.pg_ocpm_version,
        ocpm_engine_version=args.ocpm_engine_version,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
