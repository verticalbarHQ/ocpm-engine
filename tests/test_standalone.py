from __future__ import annotations

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
        "objects": [
            {"id": 1, "external_id": "o1", "object_type": "order"}
        ],
        "event_object_relations": [
            {"relation_id": 1, "event_id": 1, "object_id": 1, "qualifier": ""},
            {"relation_id": 2, "event_id": 2, "object_id": 1, "qualifier": ""},
        ],
    }


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
