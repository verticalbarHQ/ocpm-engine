from __future__ import annotations

import ctypes
import os

from ocpm_engine import StandaloneEngine, serialize_model


def canonical_fixture() -> dict:
    return {
        "dataset_id": "fixture",
        "tenant_id": "tenant",
        "events": [
            {
                "id": 1,
                "external_id": "e1",
                "activity": "create",
                "timestamp": {"epoch_nanos_utc": 1},
                "sequence": 0,
                "attributes": {},
            },
            {
                "id": 2,
                "external_id": "e2",
                "activity": "approve",
                "timestamp": {"epoch_nanos_utc": 2},
                "sequence": 0,
                "attributes": {},
            },
        ],
        "objects": [{"id": 1, "external_id": "o1", "object_type": "order"}],
        "event_object_relations": [
            {"relation_id": 1, "event_id": 1, "object_id": 1, "qualifier": ""},
            {"relation_id": 2, "event_id": 2, "object_id": 1, "qualifier": ""},
        ],
    }


def provision_duckdb_catalog(path) -> None:
    library = ctypes.CDLL(os.environ.get("OCPM_DUCKDB_LIBRARY", "libduckdb.so"))
    library.duckdb_open.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p)]
    library.duckdb_open.restype = ctypes.c_int
    library.duckdb_close.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
    database = ctypes.c_void_p()
    assert library.duckdb_open(os.fsencode(path), ctypes.byref(database)) == 0
    library.duckdb_close(ctypes.byref(database))


def test_standalone_profiles_queries_discovers_and_exports() -> None:
    engine = StandaloneEngine(canonical_fixture())

    assert engine.provider_name == "local"
    assert engine.profile()["event_count"] == 2
    query = engine.query(
        {
            "semantic_version": "1.0",
            "view": {},
            "constraint": {
                "kind": "event_activity",
                "activities": ["approve"],
            },
            "limit": 10,
        }
    )
    assert query["total_matches"] == 1
    artifact = engine.discover(
        {
            "semantic_version": "1.0",
            "view": {},
            "algorithm": "dfg",
            "algorithm_version": "1",
            "parameters": {},
        }
    )
    assert "create" in serialize_model(artifact, "dot")
    assert engine.canonical_json()["dataset_id"] == "fixture"
    assert engine.ocel2_json()["events"][0]["id"] == "e1"


def test_standalone_incremental_append_is_atomic() -> None:
    engine = StandaloneEngine(canonical_fixture())
    engine.append(
        {
            "events": {
                "event_id": [3],
                "external_event_id": ["e3"],
                "activity": ["ship"],
                "timestamp_nanos_utc": [3],
                "source_timestamp": [None],
                "sequence": [0],
                "lifecycle": [None],
                "attributes": [{}],
            },
            "objects": {
                "object_id": [],
                "external_object_id": [],
                "object_type": [],
            },
            "event_object_relations": {
                "event_id": [3],
                "object_id": [1],
                "qualifier": [""],
            },
            "object_object_relations": {
                "source_object_id": [],
                "target_object_id": [],
                "qualifier": [],
                "valid_from_nanos_utc": [],
                "valid_to_nanos_utc": [],
            },
            "object_attribute_history": {
                "object_id": [],
                "name": [],
                "valid_from_nanos_utc": [],
                "value": [],
            },
            "source_watermark": {"epoch_nanos_utc": 3},
        }
    )

    assert engine.profile()["event_count"] == 3


def test_duckdb_parquet_round_trip_uses_existing_client(tmp_path) -> None:
    root = tmp_path / "snapshot"
    catalog = tmp_path / "existing.duckdb"
    local = StandaloneEngine(canonical_fixture())
    result = local.write_parquet_snapshot(root, "v1")
    provision_duckdb_catalog(catalog)

    assert result["version"] == "v1"
    source = {
        "database": {
            "kind": "existing",
            "path": str(catalog),
            "read_only": True,
        },
        "location": {"kind": "local", "root": str(root)},
        "snapshot": {"kind": "current", "pointer": "CURRENT"},
        "layout": {"kind": "canonical_v1"},
        "cache": {"kind": "direct"},
        "validation": "strict",
        "options": {
            "memory_budget_bytes": 67_108_864,
            "max_parallelism": 1,
            "connection_pool_size": 1,
            "max_temp_bytes": 67_108_864,
            "result_cache_bytes": 1_048_576,
            "cache_canonical_fallback": True,
            "materialize_execution_relation": True,
            "extension_policy": "preinstalled",
        },
    }
    duckdb = StandaloneEngine.from_duckdb_parquet(source)

    assert duckdb.provider_name == "duckdb_parquet"
    assert duckdb.profile()["event_count"] == 2
    summary = duckdb.execution_summary(
        {
            "view": {"object_types": ["order"]},
            "leading_object_type": "order",
            "complete_lifecycle": True,
        }
    )
    assert summary["case_count"] == 1
    assert summary["variants"] == [
        {"activity_path": ["create", "approve"], "frequency": 1}
    ]
    query = duckdb.query(
        {
            "semantic_version": "1.0",
            "view": {"object_types": ["order"]},
            "constraint": {
                "kind": "directly_follows",
                "source": "create",
                "target": "approve",
            },
            "limit": 10,
        }
    )
    assert query["total_matches"] == 1
    artifact = duckdb.discover(
        {
            "semantic_version": "1.0",
            "view": {"object_types": ["order"]},
            "algorithm": "dfg",
            "algorithm_version": "1",
            "parameters": {"leading_object_type": "order"},
        }
    )
    conformance = duckdb.conformance(
        {
            "semantic_version": "1.0",
            "view": {"object_types": ["order"]},
            "method": "frequency_coverage",
            "model": artifact,
            "parameters": {"leading_object_type": "order"},
        }
    )
    assert conformance["exact"] is True
    enhancement = duckdb.enhance(
        {
            "semantic_version": "1.0",
            "view": {"object_types": ["order"]},
            "comparison_view": None,
            "kind": "process_map",
            "parameters": {"leading_object_type": "order"},
        }
    )
    assert enhancement["metrics"][0]["support"] == 1
    prediction = duckdb.fit_prediction(
        {
            "semantic_version": "1.0",
            "view": {"object_types": ["order"]},
            "target": "next_activity",
            "parameters": {"leading_object_type": "order"},
            "seed": 7,
        }
    )
    predicted = duckdb.predict(
        {
            "semantic_version": "1.0",
            "view": {"object_types": ["order"]},
            "target": "next_activity",
            "state": {
                "event_ids": [1],
                "object_ids": [1],
                "activities": ["create"],
                "as_of": {"epoch_nanos_utc": 1},
                "attributes": {},
            },
            "model_artifact": prediction,
            "parameters": {},
        }
    )
    assert predicted["candidates"][0]["label"] == "approve"
    restored = duckdb.canonical_json()
    assert restored["metadata"]["provider_layout"] == "canonical_v1"
    assert restored["metadata"]["snapshot_version"] == "v1"
    restored["metadata"] = {}
    assert restored == local.canonical_json()
    assert duckdb.ocel2_json() == local.ocel2_json()
