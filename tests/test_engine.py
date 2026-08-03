from __future__ import annotations

import re
import struct
from datetime import UTC, datetime, timedelta

import pytest

from ocpm_engine import (
    DynamicDfgRequest,
    DynamicFilter,
    EdgeFeatureRequest,
    EdgeFilter,
    EventAttributeFilter,
    EventLogRequest,
    EventLogWindow,
    LifecycleDfgRequest,
    LifecycleVariantRequest,
    NetworkFilter,
    OcpmEngine,
    PgOcpmCapabilities,
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
        assert "preselected AS MATERIALIZED" in plan.sql
        assert "JOIN preselected USING (case_id)" in plan.sql
        assert "event.case_id IN (SELECT case_id FROM base)" in plan.sql
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
        self.fetchall_called = False

    def execute(self, query: str, params: dict[str, object] | None = None) -> None:
        self.executed = (query, params)

    def fetchone(self) -> tuple[object, ...]:
        return self.row

    def fetchall(self) -> list[tuple[object, ...]]:
        self.fetchall_called = True
        return [self.row]

    def __iter__(self):
        return iter([self.row])


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


def event_log_request(*windows: tuple[datetime, datetime]) -> EventLogRequest:
    return EventLogRequest(
        object_type="Order",
        windows=tuple(EventLogWindow(start, end) for start, end in windows),
    )


def test_pg_ocpm_capabilities_are_detected_by_callable_surface() -> None:
    cursor = FakeCursor(("0.10.0", True, True, True, True, True))

    capabilities = OcpmEngine.inspect_pg_ocpm(cursor)

    assert capabilities.factorized_event_export
    assert capabilities.factorized_multi_window_export
    assert capabilities.lifecycle_dfg_pushdown
    assert capabilities.lifecycle_variant_pushdown
    assert cursor.executed and "to_regprocedure" in cursor.executed[0]


def test_binding_index_coverage_is_observable_and_exact() -> None:
    refreshed = NOW - timedelta(minutes=1)
    cursor = FakeCursor(
        (
            refreshed,
            NOW,
            True,
            '[["Order"]]',
            '[["Order","Create"]]',
            '[["Create"]]',
            '[["Order","Item","Create"]]',
            '[["Order","Create","Item","Approve","User"]]',
        )
    )

    coverage = OcpmEngine(9, 12).inspect_binding_index(cursor)

    assert coverage.refreshed_at == refreshed
    assert coverage.source_watermark == NOW
    assert coverage.event_identity_complete
    assert coverage.covers_object_type("Order")
    assert coverage.covers_activity("Order", "Create")
    assert coverage.covers_event_activity("Create")
    assert coverage.covers_neighbor("Order", "Item", "Create")
    assert coverage.covers_relation("Order", "Create", "Item", "Approve", "User")
    assert not coverage.covers_activity("Order", "Approve")
    assert cursor.executed and "ocpm.binding_relation_summary" in cursor.executed[0]


def test_binding_index_coverage_fails_closed_on_invalid_metadata() -> None:
    cursor = FakeCursor((NOW, NOW, True, "[]", '[["Order"]]', "[]", "[]", "[]"))

    with pytest.raises(RuntimeError, match="coverage width 2"):
        OcpmEngine(9, 12).inspect_binding_index(cursor)


def test_event_log_planner_uses_batches_with_row_fallback() -> None:
    engine = OcpmEngine(9, 12)
    request = event_log_request((NOW - timedelta(days=3), NOW))
    batch = engine.build_event_log_summary(
        request, PgOcpmCapabilities("0.9.0", True, True, True)
    )
    fallback = engine.build_event_log_summary(
        request, PgOcpmCapabilities("0.8.0", True, False, False)
    )

    assert "ocpm.event_log_batches" in batch.sql
    assert batch.strategy == "factorized event batch"
    assert "ocpm.event_log_rows" in fallback.sql
    assert fallback.strategy == "native event-row compatibility fallback"


def test_event_log_planner_uses_one_multi_window_scan() -> None:
    engine = OcpmEngine(9, 12)
    request = event_log_request(
        (NOW - timedelta(days=4), NOW - timedelta(days=2)),
        (NOW - timedelta(days=2), NOW),
    )
    plan = engine.build_event_log_summary(
        request, PgOcpmCapabilities("0.9.0", True, True, True)
    )

    assert "ocpm.event_log_window_batches" in plan.sql
    assert "ORDER BY" not in plan.sql
    assert plan.strategy == "factorized multi-window event batch"
    assert len(plan.params["from_dates"]) == 2


def test_event_log_planner_chunks_more_than_256_windows() -> None:
    windows = tuple((NOW - timedelta(days=1), NOW) for _ in range(257))
    plan = OcpmEngine(9, 12).build_event_log_summary(
        event_log_request(*windows),
        PgOcpmCapabilities("0.9.0", True, True, True),
    )

    assert "generate_series" in plan.sql
    assert "chunk.first_window + 255" in plan.sql
    assert "ORDER BY" not in plan.sql
    assert plan.strategy == "factorized chunked multi-window event batch"
    assert len(plan.params["from_dates"]) == 257


@pytest.mark.parametrize(
    "windows,capabilities",
    [
        (1, PgOcpmCapabilities("0.9.0", False, False, False)),
        (2, PgOcpmCapabilities("0.9.0", False, True, False)),
    ],
)
def test_event_log_planner_rejects_capabilities_without_a_usable_export(
    windows: int, capabilities: PgOcpmCapabilities
) -> None:
    request_windows = tuple(
        (NOW - timedelta(days=index + 1), NOW - timedelta(days=index))
        for index in reversed(range(windows))
    )

    with pytest.raises(RuntimeError, match="neither factorized event batches"):
        OcpmEngine(9, 12).build_event_log_summary(
            event_log_request(*request_windows), capabilities
        )


def test_event_log_execution_reports_zero_row_expansion() -> None:
    path = ["A", "B"]
    case_ids = struct.pack("=q", 7)
    timestamps = struct.pack("=iqq", 2, 0, 2_000_000)
    cursor = FakeCursor((1, path, 2, 1, case_ids, timestamps))
    engine = OcpmEngine(9, 12)
    plan = engine.build_event_log_summary(
        event_log_request((NOW - timedelta(days=1), NOW)),
        PgOcpmCapabilities("0.9.0", True, True, True),
    )

    execution = engine.execute_event_log_summary(cursor, plan)

    assert execution.database_rows == 1
    assert execution.expanded_event_rows == 0
    assert execution.summaries[0].case_count == 1
    assert execution.summaries[0].dfg[0].mean_duration_seconds == 2.0
    assert not cursor.fetchall_called


def test_event_log_fallback_streams_rows_and_reports_expansion() -> None:
    cursor = FakeCursor((1, 7, "A", NOW, 1))
    engine = OcpmEngine(9, 12)
    plan = engine.build_event_log_summary(
        event_log_request((NOW - timedelta(days=1), NOW)),
        PgOcpmCapabilities("0.8.0", True, False, False),
    )

    execution = engine.execute_event_log_summary(cursor, plan)

    assert execution.database_rows == 1
    assert execution.expanded_event_rows == 1
    assert execution.summaries[0].case_count == 1
    assert not cursor.fetchall_called


class FakeRowsCursor(FakeCursor):
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        super().__init__(rows[0] if rows else ())
        self.rows = rows

    def fetchall(self) -> list[tuple[object, ...]]:
        self.fetchall_called = True
        return self.rows

    def __iter__(self):
        return iter(self.rows)


def lifecycle_dfg_request(window_count: int = 2) -> LifecycleDfgRequest:
    return LifecycleDfgRequest(
        object_types=("Order",),
        windows=tuple(
            EventLogWindow(
                NOW - timedelta(days=window_count - index),
                NOW - timedelta(days=window_count - index - 1),
            )
            for index in range(window_count)
        ),
    )


def test_lifecycle_dfg_planner_uses_bounded_native_chunks() -> None:
    engine = OcpmEngine(9, 12)
    direct = engine.build_lifecycle_dfg(lifecycle_dfg_request())
    chunked = engine.build_lifecycle_dfg(lifecycle_dfg_request(257))

    assert "ocpm.lifecycle_dfg_window_counts" in direct.sql
    assert "generate_series" not in direct.sql
    assert direct.params["object_types"] == ["Order"]
    assert direct.strategy == "native lifecycle DFG window aggregate"
    assert "generate_series" in chunked.sql
    assert "chunk.first_window + 255" in chunked.sql
    assert "%(target_activities)s::text[],1::bigint" in chunked.sql
    assert chunked.strategy == "native chunked lifecycle DFG window aggregate"


def test_lifecycle_dfg_execution_returns_exact_aligned_counts() -> None:
    cursor = FakeRowsCursor(
        [
            (1, "Order", "A", "B", [10, 4]),
            (1, "Order", "B", "C", [2, 1]),
        ]
    )
    execution = OcpmEngine(9, 12).execute_lifecycle_dfg(
        cursor,
        lifecycle_dfg_request(),
        capabilities=PgOcpmCapabilities("0.10.0", True, True, True, True),
    )

    assert execution.strategy == "native lifecycle DFG window aggregate"
    assert execution.database_rows == 2
    assert execution.expanded_event_rows == 0
    assert [
        (count.object_type, count.source, count.target, count.frequencies)
        for count in execution.counts
    ] == [
        ("Order", "A", "B", (10, 4)),
        ("Order", "B", "C", (2, 1)),
    ]
    assert not cursor.fetchall_called


def test_lifecycle_dfg_applies_frequency_threshold_after_chunk_alignment() -> None:
    base = lifecycle_dfg_request(257)
    request = LifecycleDfgRequest(
        object_types=base.object_types,
        windows=base.windows,
        minimum_total_frequency=10,
    )
    cursor = FakeRowsCursor(
        [
            (1, "Order", "A", "B", [0] * 255 + [6]),
            (257, "Order", "A", "B", [5]),
        ]
    )

    execution = OcpmEngine(9, 12).execute_lifecycle_dfg(
        cursor,
        request,
        capabilities=PgOcpmCapabilities("0.10.0", True, True, True, True),
    )

    assert len(execution.counts) == 1
    assert sum(execution.counts[0].frequencies) == 11


def test_lifecycle_dfg_falls_back_without_changing_semantics() -> None:
    path = ["A", "B"]
    case_ids = struct.pack("=q", 7)
    timestamps = struct.pack("=iqq", 2, 0, 2_000_000)
    cursor = FakeCursor((1, path, 2, 1, case_ids, timestamps))
    execution = OcpmEngine(9, 12).execute_lifecycle_dfg(
        cursor,
        LifecycleDfgRequest(
            object_types=("Order",),
            windows=(EventLogWindow(NOW - timedelta(days=1), NOW),),
        ),
        capabilities=PgOcpmCapabilities("0.9.0", True, True, True, False),
    )

    assert execution.strategy == "factorized event-summary lifecycle DFG fallback"
    assert execution.database_rows == 1
    assert execution.expanded_event_rows == 0
    assert execution.counts[0].source == "A"
    assert execution.counts[0].target == "B"
    assert execution.counts[0].frequencies == (1,)


def lifecycle_variant_request(window_count: int = 2) -> LifecycleVariantRequest:
    dfg = lifecycle_dfg_request(window_count)
    return LifecycleVariantRequest(dfg.object_types, dfg.windows)


def test_lifecycle_variant_planner_uses_bounded_native_chunks() -> None:
    capabilities = PgOcpmCapabilities("0.10.0", True, True, True, True, True)
    engine = OcpmEngine(9, 12)
    direct = engine.build_lifecycle_variants(lifecycle_variant_request(), capabilities)
    chunked = engine.build_lifecycle_variants(
        lifecycle_variant_request(257), capabilities
    )

    assert "ocpm.lifecycle_variant_window_counts" in direct.sql
    assert "generate_series" not in direct.sql
    assert direct.strategy == "native lifecycle variant window aggregate"
    assert "generate_series" in chunked.sql
    assert "chunk.first_window + 255" in chunked.sql
    assert "%(exclude_activities)s::text[],1::bigint" in chunked.sql


def test_lifecycle_variant_execution_returns_paths_and_aligned_counts() -> None:
    cursor = FakeRowsCursor(
        [
            (1, "Order", "hash-a", ["A", "B"], [10, 4]),
            (1, "Order", "hash-b", [["A"], ["C"]], [2, 1]),
        ]
    )
    execution = OcpmEngine(9, 12).execute_lifecycle_variants(
        cursor,
        lifecycle_variant_request(),
        capabilities=PgOcpmCapabilities("0.10.0", True, True, True, True, True),
    )

    assert execution.database_rows == 2
    assert [
        (count.path_hash, count.activity_path, count.frequencies)
        for count in execution.counts
    ] == [
        ("hash-a", ("A", "B"), (10, 4)),
        ("hash-b", ("A", "C"), (2, 1)),
    ]
    assert not cursor.fetchall_called


def test_lifecycle_variant_applies_frequency_threshold_after_chunk_alignment() -> None:
    base = lifecycle_variant_request(257)
    request = LifecycleVariantRequest(
        object_types=base.object_types,
        windows=base.windows,
        minimum_total_frequency=10,
    )
    cursor = FakeRowsCursor(
        [
            (1, "Order", "hash-a", ["A", "B"], [0] * 255 + [6]),
            (257, "Order", "hash-a", ["A", "B"], [5]),
        ]
    )

    execution = OcpmEngine(9, 12).execute_lifecycle_variants(
        cursor,
        request,
        capabilities=PgOcpmCapabilities("0.10.0", True, True, True, True, True),
    )

    assert len(execution.counts) == 1
    assert sum(execution.counts[0].frequencies) == 11


def test_lifecycle_variant_old_version_uses_exact_per_window_fallback() -> None:
    cursor = FakeRowsCursor([(1, "Order", "hash-a", ["A", "B"], [3])])
    execution = OcpmEngine(9, 12).execute_lifecycle_variants(
        cursor,
        LifecycleVariantRequest(
            object_types=("Order",),
            windows=(EventLogWindow(NOW - timedelta(days=1), NOW),),
        ),
        capabilities=PgOcpmCapabilities("0.9.0", True, True, True, True, False),
    )

    assert execution.strategy == "exact per-window lifecycle variant fallback"
    assert execution.counts[0].frequencies == (3,)


def test_edge_feature_execution_streams_compact_general_statistics() -> None:
    cursor = FakeRowsCursor(
        [
            (
                "A",
                "B",
                "Order",
                "Order",
                "directly_follows",
                7,
                2.5,
                1.0,
                4.0,
                1.2,
                2,
                2 / 7,
            )
        ]
    )
    execution = OcpmEngine(9, 12).execute_edge_features(
        cursor,
        EdgeFeatureRequest(
            NOW - timedelta(days=1),
            NOW,
            source_object_types=("Order",),
            target_object_types=("Order",),
        ),
    )

    assert execution.database_rows == 1
    assert execution.features[0].frequency == 7
    assert execution.features[0].mean_duration == 2.5
    assert not cursor.fetchall_called


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
