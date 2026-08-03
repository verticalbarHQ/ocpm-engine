"""Prepare one upstream-native OCEL fixture for an ecosystem pair benchmark."""

from __future__ import annotations

import argparse
import json
import time
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from benchmarks import public_fixture as public
from benchmarks.ecosystem.common import DATASETS, OBJECT_TYPE_SELECTION


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=tuple(DATASETS), required=True)
    parser.add_argument("--baseline-host", default="postgres_vanilla")
    parser.add_argument("--extension-host", default="postgres_ocpm")
    parser.add_argument("--baseline-db", default="ocel_benchmark")
    parser.add_argument("--extension-db", default="ocel_benchmark")
    parser.add_argument("--data-dir", default="/data")
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def obtain_dataset(data_dir: Path, specification: dict[str, Any]) -> Path:
    data_dir.mkdir(parents=True, exist_ok=True)
    target = data_dir / specification["filename"]
    if not target.exists():
        temporary = target.with_suffix(target.suffix + ".download")
        print(f"downloading {specification['source_url']}", flush=True)
        with urllib.request.urlopen(specification["source_url"], timeout=180) as source:
            with temporary.open("wb") as output:
                while block := source.read(1024 * 1024):
                    output.write(block)
        temporary.replace(target)
    actual = public.sha256(target)
    if actual != specification["sqlite_sha256"]:
        raise SystemExit(f"SQLite digest mismatch for {target}: {actual}")
    return target


def select_object_type(dataset: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    """Apply the predeclared deterministic multi-event lifecycle rule."""

    object_types = {row[0]: row[2] for row in dataset["objects"]}
    links_by_object: Counter[int] = Counter(
        object_key
        for _event_key, object_key, _object_type, _qualifier in dataset["event_objects"]
    )
    by_type: dict[str, list[int]] = defaultdict(list)
    for object_key, count in links_by_object.items():
        by_type[object_types[object_key]].append(count)
    candidates = [
        {
            "object_type": object_type,
            "multi_event_lifecycles": sum(count >= 2 for count in counts),
            "event_object_links": sum(counts),
            "linked_objects": len(counts),
        }
        for object_type, counts in by_type.items()
    ]
    candidates.sort(
        key=lambda row: (
            -row["multi_event_lifecycles"],
            -row["event_object_links"],
            row["object_type"],
        )
    )
    if not candidates or candidates[0]["multi_event_lifecycles"] == 0:
        raise SystemExit("dataset has no object type with a multi-event lifecycle")
    return str(candidates[0]["object_type"]), candidates


def prepare(args: argparse.Namespace) -> dict[str, Any]:
    specification = DATASETS[args.dataset]
    path = obtain_dataset(Path(args.data_dir), specification)
    read_specification = {
        "name": args.dataset,
        "url": specification["source_url"],
        "archive_sha256": None,
        "sqlite_sha256": specification["sqlite_sha256"],
    }
    print(f"reading {args.dataset} from {path}", flush=True)
    dataset = public.read_ocel(
        path,
        read_specification,
        allow_orphan_object_relations=True,
        event_id_tiebreak=True,
    )
    selected_object_type, candidates = select_object_type(dataset)

    public.reset_database(args.baseline_host, args.baseline_db)
    public.reset_database(args.extension_host, args.extension_db)
    baseline = public.connect(args.baseline_host, args.baseline_db)
    extension = public.connect(args.extension_host, args.extension_db)
    baseline.cursor().execute(public.RAW_SCHEMA)
    extension.cursor().execute("CREATE EXTENSION pg_ocpm")
    extension.cursor().execute(public.RAW_SCHEMA)

    output: dict[str, Any] = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": specification["source"],
        "datasets": [],
    }
    try:
        started = time.perf_counter()
        public.load_raw(baseline, 1, dataset)
        baseline_load = time.perf_counter() - started
        started = time.perf_counter()
        public.load_raw(extension, 1, dataset)
        extension_load = time.perf_counter() - started
        row = {
            "dataset_id": 1,
            "name": dataset["name"],
            "filename": specification["filename"],
            "source_url": dataset["source_url"],
            "sqlite_sha256": dataset["sqlite_sha256"],
            "source_sqlite_bytes": path.stat().st_size,
            "counts": {
                "events": len(dataset["events"]),
                "objects": len(dataset["objects"]),
                "event_object_links": len(dataset["event_objects"]),
                "object_object_links": len(dataset["object_objects"]),
                "ignored_orphan_object_relations": dataset[
                    "ignored_orphan_object_relations"
                ],
            },
            "raw_load_s": {
                "baseline": round(baseline_load, 3),
                "extension_staging": round(extension_load, 3),
            },
            "object_type_selection": {
                "rule": OBJECT_TYPE_SELECTION,
                "selected": selected_object_type,
                "candidates": candidates,
            },
            "event_order": "event timestamp, then external event ID",
        }
        output["datasets"].append(row)

        for label, connection in (("baseline", baseline), ("extension", extension)):
            started = time.perf_counter()
            connection.cursor().execute(public.RAW_INDEXES)
            for table in ("event", "object", "event_object", "object_object"):
                connection.cursor().execute(f"VACUUM (ANALYZE) ocel.{table}")
            output[f"{label}_index_build_s"] = round(time.perf_counter() - started, 3)

        cursor = extension.cursor()
        cursor.execute(
            "SELECT ocpm.register_dataset(%s,1,%s::jsonb)",
            (
                dataset["name"],
                json.dumps(
                    {
                        "source": "OCEL 2.0 relational SQLite",
                        "source_url": dataset["source_url"],
                        "sqlite_sha256": dataset["sqlite_sha256"],
                    }
                ),
            ),
        )
        ocpm_dataset_id = int(cursor.fetchone()[0])
        parameters = {"raw_dataset_id": 1, "ocpm_dataset_id": ocpm_dataset_id}
        phases = {}
        for name, statement in (
            ("event_facts", public.NORMALIZE_EVENTS),
            ("directly_follows_edges", public.NORMALIZE_EDGES),
            ("object_adjacency", public.NORMALIZE_ADJACENCY),
            ("case_summaries", public.NORMALIZE_CASES),
        ):
            started = time.perf_counter()
            cursor.execute(statement, parameters)
            phases[name] = {
                "rows": int(cursor.rowcount),
                "elapsed_s": round(time.perf_counter() - started, 3),
            }
        started = time.perf_counter()
        cursor.execute("SELECT ocpm.finish_load(%s)", (ocpm_dataset_id,))
        row["normalized_counts"] = cursor.fetchone()[0]
        row["ocpm_dataset_id"] = ocpm_dataset_id
        row["phases"] = phases
        row["finalization_s"] = round(time.perf_counter() - started, 3)
        cursor.execute("DROP SCHEMA ocel CASCADE")
        cursor.execute("SELECT ocpm.version(), current_setting('server_version')")
        output["pg_ocpm_version"], output["postgres_version"] = cursor.fetchone()
        cursor.close()
        output["storage"] = {
            "vanilla_postgres": public.relation_bytes(baseline, "ocel"),
            "pg_ocpm": public.relation_bytes(extension, "ocpm"),
        }
    finally:
        baseline.close()
        extension.close()

    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(output, indent=2, default=str) + "\n")
    print(f"wrote {target}", flush=True)
    return output


if __name__ == "__main__":
    prepare(parse_args())
