#!/usr/bin/env python3
"""Validate the committed OCPQ comparison release artifact."""

from __future__ import annotations

import json
import math
import statistics
from pathlib import Path

path = Path("docs/results/ocpq-bpic2017-0.4.0.json")
result = json.loads(path.read_text())
reproduction_path = Path("docs/results/ocpq-reproduced-0.6.7.json")
reproduction = json.loads(reproduction_path.read_text())

expected_source = {
    "ocpq_eval_commit": "846dd4eb9f8600ae42355968453a9412ea4759c2",
    "ocpq_version": "0.6.7",
    "ocpq_commit": "80457e561edd7bb9e142d959dd7e0f96e6b03f2f",
    "docker_image": "ocpq:0.6.7-public-repro",
    "dataset_sqlite_sha256": (
        "02ac333a2c194b5a411cb8527dd64b4845e5110752d2ffddb531e48ce97556d7"
    ),
}
if reproduction["source"] != expected_source:
    raise SystemExit("unexpected reproduced OCPQ source pins")
if set(reproduction["queries"]) != {f"Q{index}" for index in range(1, 8)}:
    raise SystemExit("OCPQ reproduction must contain exactly Q1-Q7")
if result["reproduced_ocpq"] != reproduction:
    raise SystemExit("comparison does not embed the committed OCPQ reproduction")

if result["release"] != {"pg_ocpm": "0.5.0", "ocpm_engine": "0.4.0"}:
    raise SystemExit("unexpected OCPQ benchmark release versions")
if set(result["queries"]) != {f"Q{index}" for index in range(1, 8)}:
    raise SystemExit("OCPQ artifact must contain exactly Q1-Q7")
if not result["summary"]["all_queries_at_least_10x"]:
    raise SystemExit("OCPQ 10x release gate is false")
if result["summary"]["minimum_speedup_vs_published_ocpq"] < 10:
    raise SystemExit("an OCPQ query is below the 10x release gate")
for name, query in result["queries"].items():
    if query["speedup_vs_published_ocpq"] < 10:
        raise SystemExit(f"{name} is below the 10x release gate")
    if len(query["runs_ms"]) != result["method"]["measured_runs"]:
        raise SystemExit(f"{name} raw-run count is incomplete")
    if len(query["reproduced_ocpq_runs_ms"]) != 10:
        raise SystemExit(f"{name} reproduced OCPQ run count is incomplete")
    reproduced_mean = sum(query["reproduced_ocpq_runs_ms"]) / 10
    if abs(reproduced_mean - query["reproduced_ocpq_mean_ms"]) > 0.001:
        raise SystemExit(f"{name} reproduced OCPQ mean is inconsistent")
    reproduced_query = reproduction["queries"][name]
    if len(reproduced_query["runs_ms"]) != 10:
        raise SystemExit(f"{name} standalone OCPQ run count is incomplete")
    standalone_mean = statistics.fmean(reproduced_query["runs_ms"])
    if not math.isclose(standalone_mean, reproduced_query["mean_ms"], rel_tol=1e-12):
        raise SystemExit(f"{name} standalone OCPQ mean is inconsistent")
    if query["reproduced_ocpq_runs_ms"] != reproduced_query["runs_ms"]:
        raise SystemExit(f"{name} does not use the committed OCPQ reproduction")
    candidate_mean = statistics.fmean(query["runs_ms"])
    reproduced_ratio = standalone_mean / candidate_mean
    if abs(reproduced_ratio - query["speedup_vs_reproduced_ocpq"]) > 0.01:
        raise SystemExit(f"{name} reproduced OCPQ ratio is inconsistent")
    if len(query["correctness"]["sha256"]) != 64:
        raise SystemExit(f"{name} correctness fingerprint is invalid")
if result["storage"]["published_ocpq"] is not None:
    raise SystemExit("do not invent an unpublished OCPQ storage result")
reproduced_geomean = math.exp(
    statistics.fmean(
        math.log(query["reproduced_ocpq_mean_ms"])
        for query in result["queries"].values()
    )
)
if (
    abs(reproduced_geomean - result["summary"]["geometric_mean_reproduced_ocpq_ms"])
    > 0.001
):
    raise SystemExit("reproduced OCPQ geometric mean is inconsistent")
ratio_geomean = math.exp(
    statistics.fmean(
        math.log(query["speedup_vs_reproduced_ocpq"])
        for query in result["queries"].values()
    )
)
if (
    abs(ratio_geomean - result["summary"]["geometric_mean_speedup_vs_reproduced_ocpq"])
    > 0.001
):
    raise SystemExit("reproduced OCPQ speedup geometric mean is inconsistent")
print("OCPQ comparison artifact passes release gates")
