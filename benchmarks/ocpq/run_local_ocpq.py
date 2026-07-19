#!/usr/bin/env python3
"""Run a pinned, result-consuming OCPQ Q1-Q7 reference benchmark."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import statistics
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

EXPECTED_EVAL_COMMIT = "846dd4eb9f8600ae42355968453a9412ea4759c2"
EXPECTED_OCPQ_COMMIT = "80457e561edd7bb9e142d959dd7e0f96e6b03f2f"
EXPECTED_DATASET_SHA256 = (
    "02ac333a2c194b5a411cb8527dd64b4845e5110752d2ffddb531e48ce97556d7"
)
EXPECTED_QUERY_FILES_SHA256 = (
    "387aeb31398d86ef8e7b15393649cbabe75be56185fd67d27021744025873466"
)
CANONICAL_TUPLES = {
    "Q1": ["application_external_id", "violated"],
    "Q2": ["offer_external_id", "created_event_external_id", "violated"],
    "Q3": ["returned_event_external_id", "violated"],
    "Q4": ["application_external_id", "accepted_event_external_id", "violated"],
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
}


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", default="ocpq:0.6.7-corrected-harness")
    parser.add_argument("--sqlite", required=True, type=Path)
    parser.add_argument("--eval", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--warmups", type=int, default=10)
    parser.add_argument("--runs", type=int, default=30)
    return parser.parse_args()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def query_files_sha256(eval_root: Path) -> str:
    files = {
        f"Q{index}": {
            "tree": sha256(eval_root / f"Q{index}" / "ocpq-tree.json"),
            "author_results": sha256(eval_root / f"Q{index}" / "ocpq-res.json"),
        }
        for index in range(1, 8)
    }
    encoded = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def docker_value(*arguments: str) -> str:
    value = subprocess.run(
        ["docker", *arguments],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if not value:
        raise RuntimeError(f"docker {' '.join(arguments)} returned an empty value")
    return value


def docker_host_fingerprint() -> str:
    """Return a non-reversible identity for the Docker daemon running the test."""
    daemon_id = docker_value("info", "--format", "{{.ID}}")
    return "sha256:" + hashlib.sha256(daemon_id.encode()).hexdigest()


def source_provenance() -> tuple[str, bool]:
    root = Path(__file__).resolve().parents[2]
    revision = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "--verify", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    status = subprocess.run(
        ["git", "-C", str(root), "status", "--porcelain", "--untracked-files=all"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return revision, not bool(status.strip())


def percentile(samples: list[float], fraction: float) -> float:
    ordered = sorted(samples)
    return ordered[min(len(ordered) - 1, math.ceil(len(ordered) * fraction) - 1)]


def canonical_output(payload: dict) -> dict:
    rows = sorted(
        payload.pop("canonical_rows"),
        key=lambda row: json.dumps(row, sort_keys=True, default=str),
    )
    encoded = json.dumps(rows, separators=(",", ":"), default=str).encode()
    if len(rows) != payload["root_rows"]:
        raise RuntimeError(f"{payload['query']}: canonical row count changed")
    result = {
        "rows": len(rows),
        "sha256": hashlib.sha256(encoded).hexdigest(),
    }
    if payload["root_violations"] is not None:
        result["violations"] = payload["root_violations"]
    if payload["duration_microseconds"] is not None:
        result["duration_microseconds"] = payload["duration_microseconds"]
    return result


def run_query(args: argparse.Namespace, query: str) -> dict:
    with tempfile.TemporaryDirectory(prefix=f"ocpq-{query.lower()}-") as temporary:
        temporary_path = Path(temporary)
        helper_output = temporary_path / "result.json"
        completed = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                "none",
                "-v",
                f"{args.sqlite.resolve()}:/benchmark/bpic2017.sqlite:ro",
                "-v",
                f"{args.eval.resolve()}:/queries:ro",
                "-v",
                f"{temporary_path.resolve()}:/output",
                args.image,
                "--ocel",
                "/benchmark/bpic2017.sqlite",
                "--bbox-tree",
                f"/queries/{query}/ocpq-tree.json",
                "--query",
                query,
                "--warmups",
                str(args.warmups),
                "--runs",
                str(args.runs),
                "--output",
                "/output/result.json",
            ],
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                f"{query}: OCPQ reference failed\n"
                f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
            )
        if not helper_output.exists():
            raise RuntimeError(f"{query}: OCPQ helper did not create its output")
        payload = json.loads(helper_output.read_text())
    if payload.get("schema_version") != 1:
        raise RuntimeError(f"{query}: unexpected helper schema")
    if payload.get("ocpq_commit") != EXPECTED_OCPQ_COMMIT:
        raise RuntimeError(f"{query}: unexpected helper OCPQ commit")
    if payload.get("query") != query:
        raise RuntimeError(f"{query}: helper query mismatch")
    if payload.get("warmups") != args.warmups:
        raise RuntimeError(f"{query}: helper warmup count mismatch")
    samples = payload.get("runs_ms", [])
    if payload.get("measured_runs") != args.runs or len(samples) != args.runs:
        raise RuntimeError(f"{query}: helper measured-run count mismatch")
    canonical = canonical_output(payload)
    published_seconds = json.loads((args.eval / query / "ocpq-res.json").read_text())
    if (
        not isinstance(published_seconds, list)
        or len(published_seconds) != 10
        or not all(
            isinstance(sample, (int, float)) and sample > 0
            for sample in published_seconds
        )
    ):
        raise RuntimeError(f"{query}: invalid author-published OCPQ samples")
    published_ms = [sample * 1000.0 for sample in published_seconds]
    return {
        "author_published_runs_ms": published_ms,
        "author_published_mean_ms": statistics.fmean(published_ms),
        "tree_parse_ms": payload["tree_parse_ms"],
        "import_ms": payload["import_ms"],
        "link_ms": payload["link_ms"],
        "runs_ms": samples,
        "mean_ms": statistics.fmean(samples),
        "p50_ms": statistics.median(samples),
        "p95_ms": percentile(samples, 0.95),
        "all_node_situations": payload["all_node_situations"],
        "canonical_json_bytes": payload["canonical_json_bytes"],
        "canonical_output": canonical,
    }


def main() -> None:
    args = arguments()
    if args.warmups < 0 or args.runs <= 0:
        raise ValueError("warmups must be nonnegative and runs must be positive")
    eval_commit = subprocess.run(
        ["git", "-C", str(args.eval.resolve()), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if eval_commit != EXPECTED_EVAL_COMMIT:
        raise RuntimeError(f"unexpected OCPQ evaluation commit: {eval_commit}")
    query_digest = query_files_sha256(args.eval)
    if query_digest != EXPECTED_QUERY_FILES_SHA256:
        raise RuntimeError(f"unexpected OCPQ query-file SHA-256: {query_digest}")
    dataset_sha256 = sha256(args.sqlite)
    if dataset_sha256 != EXPECTED_DATASET_SHA256:
        raise RuntimeError(f"unexpected OCPQ SQLite SHA-256: {dataset_sha256}")

    source_revision, source_tree_clean = source_provenance()
    image_id = docker_value("image", "inspect", "--format", "{{.Id}}", args.image)
    if re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is None:
        raise RuntimeError(f"unexpected OCPQ image ID: {image_id!r}")
    image_revision = docker_value(
        "image",
        "inspect",
        "--format",
        '{{index .Config.Labels "org.opencontainers.image.revision"}}',
        args.image,
    )
    if image_revision != source_revision:
        raise RuntimeError(
            "OCPQ helper image source label does not match the benchmark revision"
        )
    benchmark_host_id = docker_host_fingerprint()

    queries = {f"Q{index}": run_query(args, f"Q{index}") for index in range(1, 8)}
    artifact = {
        "schema_version": 3,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "ocpq_eval_commit": eval_commit,
            "ocpq_version": "0.6.7",
            "ocpq_commit": EXPECTED_OCPQ_COMMIT,
            "docker_image": args.image,
            "docker_image_id": image_id,
            "dataset_sqlite_sha256": dataset_sha256,
            "query_files_sha256": query_digest,
            "author_published_results_commit": eval_commit,
        },
        "method": {
            "warmups_per_query": args.warmups,
            "measured_runs_per_query": args.runs,
            "fresh_container_per_query": True,
            "import_and_link_timed_separately": True,
            "author_published_samples_per_query": 10,
            "author_published_results_are_cross_host_only": True,
            "timing_boundary": (
                "OCPQ tree.evaluate plus construction and collection of every "
                "node's EvaluationResultWithCount structures"
            ),
            "correctness_boundary": (
                "root external-ID canonicalization, sorting, compact-JSON "
                "serialization, and SHA-256 outside the timed region"
            ),
            "canonicalization": {
                "scope": "root node situations",
                "identifiers": "OCEL external object and event IDs",
                "violation": "violation reason normalized to boolean",
                "q6": "maximum child duration normalized to integer microseconds",
                "ordering": "lexicographic compact-JSON row order; duplicates retained",
                "tuples": CANONICAL_TUPLES,
            },
        },
        "environment": {
            "benchmark_host_id": benchmark_host_id,
            "source_revision": source_revision,
            "source_tree_clean": source_tree_clean,
            "platform": platform.platform(),
            "machine": platform.machine(),
            "logical_cpus_visible": os.cpu_count(),
        },
        "queries": queries,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(artifact, indent=2) + "\n")


if __name__ == "__main__":
    main()
