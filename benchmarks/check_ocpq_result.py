#!/usr/bin/env python3
"""Validate the committed OCPQ comparison release artifact."""

from __future__ import annotations

import json
from pathlib import Path

path = Path("docs/results/ocpq-bpic2017-0.4.0.json")
result = json.loads(path.read_text())

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
    if len(query["correctness"]["sha256"]) != 64:
        raise SystemExit(f"{name} correctness fingerprint is invalid")
if result["storage"]["published_ocpq"] is not None:
    raise SystemExit("do not invent an unpublished OCPQ storage result")
print("OCPQ comparison artifact passes release gates")
