#!/usr/bin/env python3
"""Independently verify the matched SAP pg_ocpm release bridge artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import statistics
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESULT = ROOT / "docs/results/sap-common-pm-release-bridge-0.4.0-to-0.6.0.json"
FIXTURE_BASELINE = ROOT / "docs/results/public-common-pm-0.4.0-regression-baseline.json"

ARTIFACT_TYPE = "sap_common_pm_release_bridge"
EXPECTED_SOURCE = {
    "title": "Collection of Object-Centric Event Logs (SAP IDES O2C and P2P)",
    "doi": "10.5281/zenodo.8261133",
    "license": "CC BY 4.0",
}
EXPECTED_WORKLOADS = (
    "dfg_conformance_95pct",
    "variant_conformance_95pct",
    "next_activity_prediction",
    "dfg_frequency_drift",
    "repeated_transition_rework",
    "edge_bottleneck_ranking",
    "edge_bottleneck_prediction",
    "edge_duration_time_series",
    "activity_profile",
)
EXPECTED_RELEASES = {
    "prior": {
        "ocpm_engine": "0.4.0",
        "pg_ocpm": "0.5.0",
        "ocpm_engine_revision": "8427c36aa16da11b04ba642672df096d6f21e156",
        "pg_ocpm_revision": "e72c5ffc281a1f1019d07aef8ad479217823e4f2",
    },
    "current": {
        "ocpm_engine": "0.6.0",
        "pg_ocpm": "0.7.0",
        "ocpm_engine_revision": "c44e9341ced643e0b777a18d7b0d26a43127caa0",
        "pg_ocpm_revision": "279d81b3db0a0ae7470bf90824f1fbba9d188e70",
    },
}
EXPECTED_HARNESS_SHA256 = {
    "controller": "979eed86a7e23bd56a1cbc260060501ede6595cd8ee7198ca92bb2b679c66d43",
    "worker": "78fdfb5630c589dba54c73c855c27186aef6e18c4094523a66cbf756882ca483",
    "workload": "7a21984ff0eaf4d349b251c7a6aea1205cf22587cb85a0626b1cc9fd8623cf27",
}
EXPECTED_LOADER_SHA256 = {
    "prior": "85eeaa3ee733f188db1913f16d4f79ee8a6799513c9450f9139ea6ffc4a691b7",
    "current": "7221622a43d79e0cc58e8567a852b17c45455323ba3108185ac7824780033c4e",
}
ARMS = ("prior", "current")
ORDER_DICTIONARY = (("prior", "current"), ("current", "prior"))
EXPECTED_EPOCHS = 3
EXPECTED_SAMPLES_PER_EPOCH = 30
EXPECTED_TOTAL_SAMPLES = EXPECTED_EPOCHS * EXPECTED_SAMPLES_PER_EPOCH
EXPECTED_WARMUPS = 10
LATENCY_CEILING = 1.10
LATENCY_ABSOLUTE_SLACK_NS = 100_000
STORAGE_CEILING = 1.01
HEX_64 = re.compile(r"[0-9a-f]{64}")
REVISION = re.compile(r"[0-9a-f]{40}")
IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result", nargs="?", type=Path, default=DEFAULT_RESULT)
    parser.add_argument("--expected-payload-sha256")
    parser.add_argument("--preview", action="store_true")
    return parser.parse_args()


def fail(message: str) -> None:
    raise SystemExit(message)


def _is_number(value: object) -> bool:
    return type(value) in {int, float} and math.isfinite(value)  # type: ignore[arg-type]


def load_verified(path: Path, expected_digest: str | None = None) -> dict[str, Any]:
    try:
        result = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        fail(f"could not read {path}: {error}")
    if not isinstance(result, dict):
        fail(f"{path} must contain one JSON object")
    recorded = result.get("payload_sha256")
    unsigned = dict(result)
    unsigned.pop("payload_sha256", None)
    encoded = json.dumps(unsigned, indent=2, default=str) + "\n"
    computed = hashlib.sha256(encoded.encode()).hexdigest()
    if recorded != computed:
        fail(
            f"{path}: payload digest mismatch: "
            f"recorded={recorded!r}, computed={computed}"
        )
    if expected_digest is not None and recorded != expected_digest:
        fail(
            f"{path}: unexpected payload digest: "
            f"recorded={recorded!r}, expected={expected_digest}"
        )
    return result


def serial_metrics_ns(samples: list[int]) -> dict[str, Any]:
    ordered = sorted(samples)
    p95_index = math.ceil(len(ordered) * 0.95) - 1
    return {
        "p50_ms": round(statistics.median(ordered) / 1_000_000, 3),
        "p95_ms": round(ordered[p95_index] / 1_000_000, 3),
        "minimum_ms": round(ordered[0] / 1_000_000, 3),
        "maximum_ms": round(ordered[-1] / 1_000_000, 3),
        "runs": len(ordered),
    }


def _expected_fixtures() -> dict[str, dict[str, Any]]:
    try:
        baseline = json.loads(FIXTURE_BASELINE.read_text())
    except (OSError, json.JSONDecodeError) as error:
        fail(f"could not read fixture baseline: {error}")
    return {
        item["fixture"]["name"]: item["fixture"]
        for item in baseline.get("datasets", [])
    }


def validate_releases(releases: object) -> None:
    if not isinstance(releases, dict) or set(releases) != set(ARMS):
        fail("bridge release arms changed")
    expected_fields = {
        "ocpm_engine",
        "pg_ocpm",
        "ocpm_engine_revision",
        "pg_ocpm_revision",
        "worker_ocpm_engine",
        "worker_pg_ocpm",
    }
    for arm in ARMS:
        release = releases.get(arm)
        if not isinstance(release, dict) or set(release) != expected_fields:
            fail(f"{arm}: bridge release fields changed")
        for field, expected in EXPECTED_RELEASES[arm].items():
            if release.get(field) != expected:
                fail(f"{arm}: unexpected {field}")
        if release.get("worker_ocpm_engine") != release.get("ocpm_engine"):
            fail(f"{arm}: worker ocpm-engine version differs from source release")
        if release.get("worker_pg_ocpm") != release.get("pg_ocpm"):
            fail(f"{arm}: worker pg_ocpm version differs from database release")


def validate_environment(environment: object) -> None:
    if not isinstance(environment, dict) or set(environment) != {
        "client",
        "vanilla_postgres",
        "prior_postgres",
        "current_postgres",
    }:
        fail("bridge environment fields changed")
    client = environment["client"]
    if not isinstance(client, dict) or set(client) != {
        "python",
        "platform",
        "machine",
        "logical_cpus_visible",
    }:
        fail("bridge client environment is incomplete")
    if (
        not all(
            isinstance(client[field], str) and client[field]
            for field in (
                "python",
                "platform",
                "machine",
            )
        )
        or type(client["logical_cpus_visible"]) is not int
        or client["logical_cpus_visible"] < 1
    ):
        fail("invalid bridge client environment")
    database_fields = {
        "postgres_version",
        "shared_buffers",
        "effective_cache_size",
        "work_mem",
        "maintenance_work_mem",
        "max_parallel_workers_per_gather",
        "random_page_cost",
        "jit",
    }
    databases = []
    for name in ("vanilla_postgres", "prior_postgres", "current_postgres"):
        value = environment[name]
        if not isinstance(value, dict) or set(value) != database_fields:
            fail(f"{name}: bridge database environment is incomplete")
        if any(not isinstance(item, str) or not item for item in value.values()):
            fail(f"{name}: invalid database setting")
        databases.append(value)
    if not (databases[0] == databases[1] == databases[2]):
        fail("bridge PostgreSQL versions or settings differ across arms")


def validate_provenance(provenance: object, *, allow_dirty: bool) -> None:
    fields = {
        "benchmark_host_id",
        "client_image_id",
        "vanilla_database_image_id",
        "prior_database_image_id",
        "current_database_image_id",
        "controller_source_revision",
        "controller_source_tree_clean",
        "prior_engine_source_revision",
        "prior_engine_source_tree_clean",
        "prior_pg_ocpm_source_revision",
        "prior_pg_ocpm_source_tree_clean",
        "current_engine_source_revision",
        "current_engine_source_tree_clean",
        "current_pg_ocpm_source_revision",
        "current_pg_ocpm_source_tree_clean",
        "harness_sha256",
        "loader_sha256",
    }
    if not isinstance(provenance, dict) or set(provenance) != fields:
        fail("bridge provenance fields changed")
    for field in (
        "benchmark_host_id",
        "client_image_id",
        "vanilla_database_image_id",
        "prior_database_image_id",
        "current_database_image_id",
    ):
        if (
            not isinstance(provenance[field], str)
            or IMAGE_ID.fullmatch(provenance[field]) is None
        ):
            fail(f"invalid bridge image identity: {field}")
    controller_revision = provenance["controller_source_revision"]
    if (
        not isinstance(controller_revision, str)
        or REVISION.fullmatch(controller_revision) is None
    ):
        fail("invalid bridge controller source revision")
    expected_revisions = {
        "prior_engine_source_revision": EXPECTED_RELEASES["prior"][
            "ocpm_engine_revision"
        ],
        "prior_pg_ocpm_source_revision": EXPECTED_RELEASES["prior"]["pg_ocpm_revision"],
        "current_engine_source_revision": EXPECTED_RELEASES["current"][
            "ocpm_engine_revision"
        ],
        "current_pg_ocpm_source_revision": EXPECTED_RELEASES["current"][
            "pg_ocpm_revision"
        ],
    }
    for field, expected in expected_revisions.items():
        value = provenance[field]
        if (
            not isinstance(value, str)
            or REVISION.fullmatch(value) is None
            or value != expected
        ):
            fail(f"unexpected bridge source revision: {field}")
    cleanliness_fields = (
        "controller_source_tree_clean",
        "prior_engine_source_tree_clean",
        "prior_pg_ocpm_source_tree_clean",
        "current_engine_source_tree_clean",
        "current_pg_ocpm_source_tree_clean",
    )
    for field in cleanliness_fields:
        clean = provenance[field]
        if type(clean) is not bool:
            fail(f"invalid bridge cleanliness flag: {field}")
        if field != "controller_source_tree_clean" and clean is not True:
            fail("bridge measured-arm source trees must be clean")
        if (
            field == "controller_source_tree_clean"
            and not allow_dirty
            and clean is not True
        ):
            fail("release bridge requires a clean controller source tree")
    for field, expected_keys in (
        ("harness_sha256", {"controller", "worker", "workload"}),
        ("loader_sha256", set(ARMS)),
    ):
        value = provenance[field]
        if not isinstance(value, dict) or set(value) != expected_keys:
            fail(f"bridge {field} fields changed")
        if any(
            not isinstance(item, str) or HEX_64.fullmatch(item) is None
            for item in value.values()
        ):
            fail(f"invalid bridge {field}")
    if provenance["harness_sha256"] != EXPECTED_HARNESS_SHA256:
        fail("unexpected bridge harness source hashes")
    if provenance["loader_sha256"] != EXPECTED_LOADER_SHA256:
        fail("unexpected bridge loader source hashes")


def validate_method(method: object) -> None:
    fields = {
        "warmup_rounds",
        "latency_epochs",
        "samples_per_epoch",
        "total_samples_per_arm",
        "clock",
        "random_seed",
        "order_dictionary",
        "warmup_order_counts",
        "epoch_order_counts",
        "timing_scope",
        "ipc_excluded",
        "oracle",
        "correctness_gate",
        "storage",
    }
    if not isinstance(method, dict) or set(method) != fields:
        fail("bridge method fields changed")
    expected = {
        "warmup_rounds": EXPECTED_WARMUPS,
        "latency_epochs": EXPECTED_EPOCHS,
        "samples_per_epoch": EXPECTED_SAMPLES_PER_EPOCH,
        "total_samples_per_arm": EXPECTED_TOTAL_SAMPLES,
        "clock": "time.perf_counter_ns",
        "random_seed": 20260718,
        "order_dictionary": [list(order) for order in ORDER_DICTIONARY],
        "warmup_order_counts": {"0": 5, "1": 5},
        "epoch_order_counts": {"0": 15, "1": 15},
        "ipc_excluded": True,
    }
    for field, value in expected.items():
        if method.get(field) != value:
            fail(f"bridge method changed: {field}")
    for field in ("timing_scope", "oracle", "correctness_gate", "storage"):
        if not isinstance(method.get(field), str) or not method[field]:
            fail(f"bridge method lacks {field}")


def validate_latency_row(dataset_name: str, row: dict[str, Any]) -> tuple[float, bool]:
    label = f"{dataset_name}/{row.get('workload')}"
    expected_row_fields = {
        "workload",
        "oracle_answer_sha256",
        "correct",
        "prior",
        "current",
        "p50_ratio_prior_over_current",
        "first_execution_counts",
        "warmup_order_codes",
        "serial_epochs",
    }
    if set(row) != expected_row_fields:
        fail(f"{label}: bridge workload fields changed")
    oracle_hash = row.get("oracle_answer_sha256")
    if not isinstance(oracle_hash, str) or HEX_64.fullmatch(oracle_hash) is None:
        fail(f"{label}: invalid oracle answer hash")
    warmups = row.get("warmup_order_codes")
    if (
        not isinstance(warmups, list)
        or len(warmups) != EXPECTED_WARMUPS
        or warmups.count(0) != 5
        or warmups.count(1) != 5
    ):
        fail(f"{label}: warmup schedule is not exactly counterbalanced")
    first_counts = row.get("first_execution_counts")
    if first_counts != {"prior": 45, "current": 45}:
        fail(f"{label}: measured schedule is not exactly counterbalanced")
    epochs = row.get("serial_epochs")
    if not isinstance(epochs, list) or len(epochs) != EXPECTED_EPOCHS:
        fail(f"{label}: expected three retained epochs")

    pooled: dict[str, list[int]] = {arm: [] for arm in ARMS}
    epoch_p95: dict[str, list[float]] = {arm: [] for arm in ARMS}
    epoch_fields = {
        "p50_ms",
        "p95_ms",
        "minimum_ms",
        "maximum_ms",
        "runs",
        "samples_ns",
        "answer_sha256s",
    }
    decoded_first = {arm: 0 for arm in ARMS}
    for epoch_number, epoch in enumerate(epochs, start=1):
        if not isinstance(epoch, dict) or set(epoch) != {
            "epoch",
            "order_codes",
            "arms",
        }:
            fail(f"{label}: bridge epoch fields changed")
        codes = epoch.get("order_codes")
        if (
            epoch.get("epoch") != epoch_number
            or not isinstance(codes, list)
            or len(codes) != EXPECTED_SAMPLES_PER_EPOCH
            or codes.count(0) != 15
            or codes.count(1) != 15
        ):
            fail(f"{label}: epoch {epoch_number} is not exactly 15/15")
        for code in codes:
            decoded_first[ORDER_DICTIONARY[code][0]] += 1
        arms = epoch.get("arms")
        if not isinstance(arms, dict) or set(arms) != set(ARMS):
            fail(f"{label}: epoch arm set changed")
        for arm in ARMS:
            evidence = arms[arm]
            if not isinstance(evidence, dict) or set(evidence) != epoch_fields:
                fail(f"{label}/{arm}: epoch evidence fields changed")
            samples = evidence.get("samples_ns")
            hashes = evidence.get("answer_sha256s")
            if (
                not isinstance(samples, list)
                or len(samples) != EXPECTED_SAMPLES_PER_EPOCH
                or any(type(sample) is not int or sample <= 0 for sample in samples)
            ):
                fail(f"{label}/{arm}: invalid raw nanosecond samples")
            if (
                not isinstance(hashes, list)
                or len(hashes) != EXPECTED_SAMPLES_PER_EPOCH
                or any(value != oracle_hash for value in hashes)
            ):
                fail(f"{label}/{arm}: a measured answer differs from the oracle")
            recomputed = serial_metrics_ns(samples)
            if any(evidence.get(field) != value for field, value in recomputed.items()):
                fail(f"{label}/{arm}: epoch metrics do not match raw samples")
            pooled[arm].extend(samples)
            epoch_p95[arm].append(recomputed["p95_ms"])
    if decoded_first != first_counts:
        fail(f"{label}: retained orders differ from first-execution counts")

    aggregate_fields = {
        "p50_ms",
        "p95_ms",
        "minimum_ms",
        "maximum_ms",
        "runs",
        "exact_samples",
        "epoch_count",
        "epoch_p95_median_ms",
        "epoch_p95_range_ms",
        "position_stratified",
    }
    median_ns: dict[str, float] = {}
    for arm in ARMS:
        evidence = row.get(arm)
        if not isinstance(evidence, dict) or set(evidence) != aggregate_fields:
            fail(f"{label}/{arm}: aggregate fields changed")
        metrics = serial_metrics_ns(pooled[arm])
        position_samples = {"first": [], "second": []}
        for epoch in epochs:
            for order_code, sample in zip(
                epoch["order_codes"],
                epoch["arms"][arm]["samples_ns"],
            ):
                position = (
                    "first" if ORDER_DICTIONARY[order_code][0] == arm else "second"
                )
                position_samples[position].append(sample)
        position_stratified = {
            position: {
                **serial_metrics_ns(samples),
                "exact_samples": len(samples),
            }
            for position, samples in position_samples.items()
        }
        expected = {
            **metrics,
            "exact_samples": EXPECTED_TOTAL_SAMPLES,
            "epoch_count": EXPECTED_EPOCHS,
            "epoch_p95_median_ms": round(statistics.median(epoch_p95[arm]), 3),
            "epoch_p95_range_ms": [min(epoch_p95[arm]), max(epoch_p95[arm])],
            "position_stratified": position_stratified,
        }
        if evidence != expected:
            fail(f"{label}/{arm}: aggregate metrics do not match raw samples")
        median_ns[arm] = statistics.median(pooled[arm])

    expected_ratio = round(median_ns["prior"] / median_ns["current"], 3)
    if row.get("p50_ratio_prior_over_current") != expected_ratio:
        fail(f"{label}: p50 release ratio is inconsistent")
    non_regressed = median_ns["current"] <= max(
        median_ns["prior"] * LATENCY_CEILING,
        median_ns["prior"] + LATENCY_ABSOLUTE_SLACK_NS,
    )
    if row.get("correct") is not True:
        fail(f"{label}: correctness gate failed")
    return expected_ratio, non_regressed


def validate_storage(storage: object) -> None:
    if not isinstance(storage, dict) or set(storage) != {
        "prior_pg_ocpm",
        "current_pg_ocpm",
    }:
        fail("bridge storage representations changed")
    fields = {"heap_bytes", "index_bytes", "toast_bytes", "total_bytes"}
    for arm in ("prior_pg_ocpm", "current_pg_ocpm"):
        value = storage[arm]
        if (
            not isinstance(value, dict)
            or set(value) != fields
            or any(type(item) is not int or item < 0 for item in value.values())
            or value["index_bytes"] <= 0
            or value["total_bytes"] <= 0
        ):
            fail(f"{arm}: invalid bridge storage")
    for metric in ("index_bytes", "total_bytes"):
        prior = storage["prior_pg_ocpm"][metric]
        current = storage["current_pg_ocpm"][metric]
        if current > prior * STORAGE_CEILING:
            fail(
                f"current_pg_ocpm/{metric}: storage regression "
                f"{current} > {STORAGE_CEILING:.0%} of {prior}"
            )


def validate_contract(
    result: dict[str, Any], *, allow_dirty: bool
) -> dict[tuple[str, str], dict[str, Any]]:
    top_fields = {
        "schema_version",
        "artifact_type",
        "generated_at",
        "source",
        "releases",
        "environment",
        "provenance",
        "method",
        "datasets",
        "storage",
        "summary",
        "payload_sha256",
    }
    if set(result) != top_fields:
        fail("bridge artifact fields changed")
    if result.get("schema_version") != 1:
        fail("unexpected bridge schema version")
    if result.get("artifact_type") != ARTIFACT_TYPE:
        fail("unexpected bridge artifact type")
    if not isinstance(result.get("generated_at"), str) or not result["generated_at"]:
        fail("bridge generation timestamp is missing")
    if result.get("source") != EXPECTED_SOURCE:
        fail("bridge public source metadata changed")
    validate_releases(result.get("releases"))
    validate_environment(result.get("environment"))
    validate_provenance(result.get("provenance"), allow_dirty=allow_dirty)
    validate_method(result.get("method"))

    fixtures = _expected_fixtures()
    datasets = result.get("datasets")
    if not isinstance(datasets, list) or tuple(
        item.get("fixture", {}).get("name") for item in datasets
    ) != tuple(fixtures):
        fail("bridge requires SAP O2C and P2P in stable order")
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    ratios = []
    non_regressed = 0
    for dataset in datasets:
        if not isinstance(dataset, dict) or set(dataset) != {"fixture", "workloads"}:
            fail("bridge dataset fields changed")
        fixture = dataset["fixture"]
        name = fixture.get("name")
        if fixture != fixtures[name]:
            fail(f"{name}: bridge fixture differs from the committed SAP fixture")
        workloads = dataset["workloads"]
        if (
            not isinstance(workloads, list)
            or tuple(row.get("workload") for row in workloads) != EXPECTED_WORKLOADS
        ):
            fail(f"{name}: bridge workload set or order changed")
        for row in workloads:
            key = (name, row["workload"])
            ratio, passed = validate_latency_row(name, row)
            rows[key] = row
            ratios.append(ratio)
            non_regressed += int(passed)
    if non_regressed != len(rows):
        failed = len(rows) - non_regressed
        fail(f"release bridge found {failed} regressed SAP workloads")
    validate_storage(result.get("storage"))
    summary = result.get("summary")
    expected_summary = {
        "total_workloads": len(rows),
        "correct_workloads": len(rows),
        "non_regressed_workloads": non_regressed,
        "minimum_p50_ratio_prior_over_current": min(ratios),
        "target_met": True,
    }
    if summary != expected_summary:
        fail("bridge summary does not match retained evidence")
    return rows


def validate_for_public(
    bridge: dict[str, Any], public_result: dict[str, Any], *, allow_dirty: bool
) -> None:
    rows = validate_contract(bridge, allow_dirty=allow_dirty)
    if bridge.get("source") != public_result.get("source"):
        fail("bridge and public result use different SAP sources")
    public_provenance = public_result.get("provenance", {})
    bridge_provenance = bridge["provenance"]
    for public_field, bridge_field in (
        ("ocpm_engine_source_revision", "current_engine_source_revision"),
        ("pg_ocpm_source_revision", "current_pg_ocpm_source_revision"),
    ):
        if public_provenance.get(public_field) != bridge_provenance.get(bridge_field):
            fail("bridge current release differs from the public result")
    public_rows = {
        (dataset["fixture"]["name"], row["workload"])
        for dataset in public_result.get("datasets", [])
        for row in dataset.get("workloads", [])
    }
    if public_rows != set(rows):
        fail("bridge and public result workload sets differ")
    public_fixtures = {
        dataset["fixture"]["name"]: dataset["fixture"]
        for dataset in public_result.get("datasets", [])
    }
    bridge_fixtures = {
        dataset["fixture"]["name"]: dataset["fixture"] for dataset in bridge["datasets"]
    }
    if public_fixtures != bridge_fixtures:
        fail("bridge and public result fixtures differ")


def main() -> None:
    args = parse_args()
    if args.preview and args.expected_payload_sha256:
        fail("--preview and --expected-payload-sha256 are mutually exclusive")
    if not args.preview and args.expected_payload_sha256 is None:
        fail("release bridge verification requires --expected-payload-sha256")
    result = load_verified(args.result, args.expected_payload_sha256)
    rows = validate_contract(result, allow_dirty=args.preview)
    print(
        f"SAP release bridge verified: {len(rows)} exact, counterbalanced "
        "workloads; latency and storage gates passed"
    )


if __name__ == "__main__":
    main()
