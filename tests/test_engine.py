from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

import pytest

from ocpm_engine import (
    EdgeFilter,
    NetworkFilter,
    OcpmEngine,
    ProcessMiningRequest,
)

NOW = datetime(2026, 6, 10, tzinfo=UTC)
PLACEHOLDER = re.compile(r"%\(([^)]+)\)s")


def request(endpoint: str, **overrides: object) -> ProcessMiningRequest:
    values = {
        "endpoint": endpoint,
        "backbone_type": "Order",
        "from_date": NOW - timedelta(days=3),
        "to_date": NOW,
        "source_activity": "Order:Complete",
        "target_activity": "Shipment:Ship",
    }
    values.update(overrides)
    return ProcessMiningRequest(**values)


@pytest.mark.parametrize(
    "endpoint",
    [
        "process_map",
        "variant_list",
        "timeline",
        "case_throughput",
        "edge_info",
        "case_list",
        "entire_process_map",
    ],
)
def test_every_endpoint_is_parameterized_and_uses_pg_ocpm(endpoint: str) -> None:
    plan = OcpmEngine(9, 12).build(request(endpoint))
    missing = set(PLACEHOLDER.findall(plan.sql)) - plan.params.keys()

    assert not missing
    assert "ocpm." in plan.sql


def test_short_and_wide_variants_select_different_exact_paths() -> None:
    engine = OcpmEngine(9, 12)
    short = engine.build(request("variant_list"))
    wide = engine.build(request("variant_list", from_date=NOW - timedelta(days=365)))

    assert "ocpm.case_window" in short.sql
    assert "ocpm.case_bucket" in wide.sql
    assert "ocpm.timestamp_decode" in wide.sql
    assert short.strategy == "case window"
    assert wide.strategy == "segmented case buckets"


def test_process_map_translates_generic_filters() -> None:
    network = NetworkFilter(
        activities=("Order:Complete", "Shipment:Ship"),
        edges=(EdgeFilter("Order:Complete", "Shipment:Ship", 0, 86_400),),
        min_execution_time=0,
        max_execution_time=7_200,
    )
    plan = OcpmEngine(9, 12).build(
        request("process_map", variants=("abc123",), network=network)
    )

    assert plan.params["variant_hashes"] == ["abc123"]
    assert plan.params["backbone_activities"] == ["Order:Complete"]
    assert plan.params["connected_activities"] == ["Shipment:Ship"]
    assert plan.params["included_edge_source"] == "Order:Complete"
    assert plan.params["included_edge_target"] == "Shipment:Ship"
    assert plan.params["case_max_execution"] == 7_200
    assert "matching_edges AS MATERIALIZED" in plan.sql
    assert "ocpm.connected_objects_one_hop" in plan.sql


def test_wide_process_map_uses_transitive_closure() -> None:
    plan = OcpmEngine(9, 12).build(
        request("process_map", from_date=NOW - timedelta(days=365))
    )

    assert "ocpm.connected_objects_closure" in plan.sql
    assert "ocpm.connected_objects_one_hop" not in plan.sql


def test_unbounded_dates_are_mapped_to_postgres_supported_bounds() -> None:
    plan = OcpmEngine(9, 12).build(request("timeline", from_date=None, to_date=None))

    assert plan.params["from_date"].year == 1
    assert plan.params["to_date"].year == 9999


def test_mapping_adapter_recovers_nested_filter_shape() -> None:
    parsed = ProcessMiningRequest.from_mapping(
        {
            "endpoint": "process_map",
            "backbone_type": "Order",
            "from_date": "2026-06-01T00:00:00Z",
            "to_date": "2026-06-10T00:00:00+00:00",
            "attribute_filter": {"status": ["complete"]},
            "variants": ["abc"],
            "network_filter": {
                "activities": {"include": ["Order:Complete"]},
                "edges": {
                    "include": [
                        {
                            "source": "Order:Complete",
                            "target": "Shipment:Ship",
                            "min_execution_time": 1,
                            "max_execution_time": 2,
                        }
                    ]
                },
                "execution_time_range": {
                    "min_execution_time": 3,
                    "max_execution_time": 4,
                },
            },
        }
    )

    assert parsed.status == "complete"
    assert parsed.variants == ("abc",)
    assert parsed.network.activities == ("Order:Complete",)
    assert parsed.network.edges[0].max_execution_time == 2
    assert parsed.network.min_execution_time == 3
    assert parsed.from_date and parsed.from_date.tzinfo is not None


def test_invalid_request_shapes_fail_before_sql_execution() -> None:
    engine = OcpmEngine(9, 12)

    with pytest.raises(ValueError, match="backbone_type"):
        engine.build(ProcessMiningRequest(endpoint="timeline"))
    with pytest.raises(ValueError, match="source_activity"):
        engine.build(request("edge_info", source_activity=None))
    with pytest.raises(ValueError, match="timeline period"):
        engine.build(request("timeline", timeline_period="decade"))
    with pytest.raises(ValueError, match="one included edge"):
        engine.build(
            request(
                "process_map",
                network=NetworkFilter(
                    edges=(
                        EdgeFilter("A", "B"),
                        EdgeFilter("B", "C"),
                    )
                ),
            )
        )


class FakeCursor:
    def __init__(self, row: tuple[object, ...]) -> None:
        self.row = row
        self.executed: tuple[str, dict[str, object] | None] | None = None

    def execute(self, query: str, params: dict[str, object] | None = None) -> None:
        self.executed = (query, params)

    def fetchone(self) -> tuple[object, ...]:
        return self.row


def test_execute_and_version_check_use_cursor_protocol() -> None:
    engine = OcpmEngine(9, 12)
    version_cursor = FakeCursor(("0.5.0",))
    query_cursor = FakeCursor(({"nodes": []},))

    assert engine.verify_pg_ocpm(version_cursor) == "0.5.0"
    assert engine.execute(query_cursor, request("entire_process_map")) == {"nodes": []}
    assert version_cursor.executed == ("SELECT ocpm.version()", None)
    assert query_cursor.executed and query_cursor.executed[1] == {
        "dataset_id": 9,
        "tenant_id": 12,
        "timeline_period": "week",
    }


def test_version_check_rejects_older_pg_ocpm() -> None:
    with pytest.raises(RuntimeError, match="0.5.0 or later"):
        OcpmEngine.verify_pg_ocpm(FakeCursor(("0.4.0",)))
