from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

import pytest

from ocpm_engine import (
    DynamicDfgRequest,
    DynamicFilter,
    EdgeFilter,
    EventAttributeFilter,
    NetworkFilter,
    OcpmEngine,
    ProcessMiningRequest,
    score_dynamic_dfg_rows,
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


def dynamic_request(**overrides: object) -> DynamicDfgRequest:
    values = {
        "backbone_type": "Order",
        "from_date": NOW - timedelta(days=3),
        "to_date": NOW,
    }
    values.update(overrides)
    return DynamicDfgRequest(**values)


def test_dynamic_filter_is_parameterized_across_every_predicate_family() -> None:
    dynamic_filter = DynamicFilter(
        statuses=("complete", "closed"),
        included_activities=("Order:Create",),
        excluded_activities=("Order:Cancel",),
        min_case_execution_time=1,
        max_case_execution_time=7_200,
        included_event_attributes=(EventAttributeFilter("actor", "Ada"),),
        excluded_event_attributes=(EventAttributeFilter("context", "CSV"),),
        included_related_object_types=("Shipment",),
        excluded_related_object_types=("Invoice",),
        included_edges=(EdgeFilter("Order:Create", "Order:Complete", 1, 10),),
        excluded_edges=(EdgeFilter("Order:Create", "Order:Cancel", 0, 20),),
    )
    engine = OcpmEngine(9, 12)
    ids = engine.build_dynamic_case_ids(dynamic_request(filter=dynamic_filter))
    dfg = engine.build_dynamic_dfg(dynamic_request(filter=dynamic_filter))

    for plan in (ids, dfg):
        assert not (set(PLACEHOLDER.findall(plan.sql)) - plan.params.keys())
        assert "INTERSECT" in plan.sql
        assert "EXCEPT" in plan.sql
        assert "ocpm.event_chunk" in plan.sql
        assert "ocpm.connected_objects_one_hop" in plan.sql
        assert "ocpm.event_log_rows" in plan.sql
        assert "event_locator" not in plan.sql
    assert "array_agg(case_id ORDER BY case_id)" in ids.sql
    assert "lifecycle_edges" in dfg.sql
    assert "ocpm.edge_bucket" not in dfg.sql
    assert dfg.strategy == "native event stream dynamic dfg"


def test_dynamic_case_filters_keep_the_fast_bucket_dfg_path() -> None:
    dynamic_filter = DynamicFilter(
        statuses=("complete",),
        included_activities=("Order:Complete",),
        excluded_activities=("Order:Cancel",),
        min_case_execution_time=0,
        max_case_execution_time=86_400,
    )
    plan = OcpmEngine(9, 12).build_dynamic_dfg(dynamic_request(filter=dynamic_filter))

    assert "ocpm.case_bucket" in plan.sql
    assert "ocpm.edge_bucket" in plan.sql
    assert "dfg_edges AS MATERIALIZED" in plan.sql
    assert "ocpm.event_log_rows" not in plan.sql
    assert plan.strategy == "compact bucket scan dynamic dfg"


def test_dynamic_mapping_covers_source_neutral_attributes_and_relationships() -> None:
    parsed = DynamicDfgRequest.from_mapping(
        {
            "backbone_type": "Order",
            "from_date": "2026-06-01T00:00:00Z",
            "to_date": "2026-06-10T00:00:00Z",
            "filter": {
                "status": "complete",
                "activities": {
                    "include": ["Order:Create"],
                    "exclude": ["Order:Cancel"],
                },
                "event_attributes": {
                    "include": [{"key": "sales_amount", "value": "100"}],
                    "exclude": [{"field": "location", "value": "remote"}],
                },
                "related_object_types": {"include": ["Shipment"]},
                "edges": {
                    "include": [
                        {
                            "source": "Order:Create",
                            "target": "Order:Complete",
                            "min_execution_time": 1,
                            "max_execution_time": 2,
                        }
                    ]
                },
            },
        }
    )

    assert parsed.filter.statuses == ("complete",)
    assert parsed.filter.included_event_attributes[0].key == "sales_amount"
    assert parsed.filter.excluded_event_attributes[0].key == "location"
    assert parsed.filter.included_related_object_types == ("Shipment",)
    assert parsed.filter.included_edges[0].max_execution_time == 2


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


def test_invalid_dynamic_filter_shapes_fail_before_sql_execution() -> None:
    engine = OcpmEngine(9, 12)
    with pytest.raises(ValueError, match="from_date"):
        engine.build_dynamic_dfg(
            dynamic_request(from_date=NOW, to_date=NOW - timedelta(days=1))
        )
    with pytest.raises(ValueError, match="both minimum and maximum"):
        engine.build_dynamic_dfg(
            dynamic_request(filter=DynamicFilter(min_case_execution_time=1))
        )
    with pytest.raises(ValueError, match="event attribute keys"):
        engine.build_dynamic_dfg(
            dynamic_request(
                filter=DynamicFilter(
                    included_event_attributes=(EventAttributeFilter("", "x"),)
                )
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

    def fetchall(self) -> list[tuple[object, ...]]:
        return [self.row]


def test_execute_and_version_check_use_cursor_protocol() -> None:
    engine = OcpmEngine(9, 12)
    version_cursor = FakeCursor(("0.8.0",))
    query_cursor = FakeCursor(({"nodes": []},))

    assert engine.verify_pg_ocpm(version_cursor) == "0.8.0"
    assert engine.execute(query_cursor, request("entire_process_map")) == {"nodes": []}
    assert version_cursor.executed == ("SELECT ocpm.version()", None)
    assert query_cursor.executed and query_cursor.executed[1] == {
        "dataset_id": 9,
        "tenant_id": 12,
        "timeline_period": "week",
    }


def test_version_check_rejects_older_pg_ocpm() -> None:
    with pytest.raises(RuntimeError, match="0.8.0 or later"):
        OcpmEngine.verify_pg_ocpm(FakeCursor(("0.7.0",)))


def test_dynamic_dfg_execution_uses_the_shared_native_ranking() -> None:
    rows = [
        (3, "A", "B", 10, 2.0),
        (3, "B", "C", 20, 2.0),
    ]

    class DfgCursor(FakeCursor):
        def fetchall(self) -> list[tuple[object, ...]]:
            return rows

    engine = OcpmEngine(9, 12)
    plan = engine.build_dynamic_dfg(dynamic_request())
    cursor = DfgCursor(rows[0])
    expected = {
        "selected_count": 3,
        "dfg": [["A", "B", 10, 2.0], ["B", "C", 20, 2.0]],
        "bottleneck_order": [["B", "C", 20, 2.0], ["A", "B", 10, 2.0]],
    }

    assert score_dynamic_dfg_rows(rows) == expected
    assert engine.execute_dynamic_dfg(cursor, plan) == expected
    assert cursor.executed == (plan.sql, plan.params)


def test_dynamic_dfg_preserves_an_empty_exact_result() -> None:
    assert score_dynamic_dfg_rows([(0, None, None, None, None)]) == {
        "selected_count": 0,
        "dfg": [],
        "bottleneck_order": [],
    }
