"""Translate API request shapes into parameterized pg_ocpm queries."""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any, Protocol

from . import queries
from .analytics import bottleneck_order
from .dynamic_queries import Projection, compile_dynamic_dfg
from .event_batches import (
    EventLogExecution,
    summarize_event_row_fallback,
    summarize_event_window_batch_rows,
)
from .models import (
    BindingIndexCoverage,
    BindingNeighborCoverage,
    BindingRelationCoverage,
    DynamicDfgRequest,
    DynamicFilter,
    Endpoint,
    EventLogRequest,
    PgOcpmCapabilities,
    ProcessMiningRequest,
    QueryPlan,
)


class Cursor(Protocol):
    def execute(self, query: str, params: dict[str, Any] | None = None) -> Any: ...

    def fetchone(self) -> Any: ...

    def fetchall(self) -> Any: ...


_MIN_TIMESTAMP = datetime(1, 1, 1, tzinfo=UTC)
_MAX_TIMESTAMP = datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)
_TIMELINE_PERIODS = frozenset({"hour", "day", "week", "month", "quarter", "year"})
_MINIMUM_PG_OCPM_VERSION = (0, 8, 0)
_EVENT_BATCH_MAX_WINDOWS = 256

_PG_OCPM_CAPABILITIES_SQL = """SELECT ocpm.version(),
       to_regprocedure(
           'ocpm.event_log_rows(bigint,bigint,text,timestamptz,timestamptz)'
       ) IS NOT NULL,
       to_regprocedure(
           'ocpm.event_log_batches(bigint,bigint,text,timestamptz,timestamptz)'
       ) IS NOT NULL,
       to_regprocedure(
           'ocpm.event_log_window_batches(bigint,bigint,text,timestamptz[],timestamptz[])'
       ) IS NOT NULL"""

_SINGLE_EVENT_BATCH_SQL = """SELECT 1::integer AS window_ordinal,
       activity_path,activity_count,case_count,
       case_id_payloads,event_timestamp_payloads
FROM ocpm.event_log_batches(
    %(dataset_id)s,%(tenant_id)s,%(object_type)s,
    (%(from_dates)s::timestamptz[])[1],(%(to_dates)s::timestamptz[])[1]
)"""

_WINDOW_EVENT_BATCH_SQL = """SELECT window_ordinal,activity_path,activity_count,
       case_count,case_id_payloads,event_timestamp_payloads
FROM ocpm.event_log_window_batches(
    %(dataset_id)s,%(tenant_id)s,%(object_type)s,
    %(from_dates)s::timestamptz[],%(to_dates)s::timestamptz[]
)"""

_CHUNKED_WINDOW_EVENT_BATCH_SQL = """SELECT
       chunk.first_window - 1 + batch.window_ordinal AS window_ordinal,
       batch.activity_path,batch.activity_count,batch.case_count,
       batch.case_id_payloads,batch.event_timestamp_payloads
FROM generate_series(
    1,cardinality(%(from_dates)s::timestamptz[]),256
) AS chunk(first_window)
CROSS JOIN LATERAL ocpm.event_log_window_batches(
    %(dataset_id)s,%(tenant_id)s,%(object_type)s,
    (%(from_dates)s::timestamptz[])[
        chunk.first_window:
        LEAST(
            chunk.first_window + 255,
            cardinality(%(from_dates)s::timestamptz[])
        )
    ],
    (%(to_dates)s::timestamptz[])[
        chunk.first_window:
        LEAST(
            chunk.first_window + 255,
            cardinality(%(to_dates)s::timestamptz[])
        )
    ]
) AS batch"""

_EVENT_ROW_FALLBACK_SQL = """SELECT window_ordinal,case_id,activity,
       event_timestamp,event_ordinal
FROM unnest(
    %(from_dates)s::timestamptz[],%(to_dates)s::timestamptz[]
) WITH ORDINALITY AS window(from_date,to_date,window_ordinal)
CROSS JOIN LATERAL ocpm.event_log_rows(
    %(dataset_id)s,%(tenant_id)s,%(object_type)s,
    window.from_date,window.to_date
)
ORDER BY window_ordinal,case_id,event_ordinal"""

_BINDING_INDEX_COVERAGE_SQL = """SELECT dataset.refreshed_at,
       dataset.source_watermark,
       dataset.event_identity_complete,
       COALESCE((
           SELECT jsonb_agg(item ORDER BY item)::text
           FROM (
               SELECT jsonb_build_array(object_type) AS item
               FROM ocpm.binding_object
               WHERE dataset_id=%(dataset_id)s AND tenant_id=%(tenant_id)s
           ) objects
       ), '[]'),
       COALESCE((
           SELECT jsonb_agg(item ORDER BY item)::text
           FROM (
               SELECT jsonb_build_array(object_type,activity) AS item
               FROM ocpm.binding_activity
               WHERE dataset_id=%(dataset_id)s AND tenant_id=%(tenant_id)s
           ) activities
       ), '[]'),
       COALESCE((
           SELECT jsonb_agg(item ORDER BY item)::text
           FROM (
               SELECT jsonb_build_array(activity) AS item
               FROM ocpm.binding_event
               WHERE dataset_id=%(dataset_id)s AND tenant_id=%(tenant_id)s
           ) events
       ), '[]'),
       COALESCE((
           SELECT jsonb_agg(item ORDER BY item)::text
           FROM (
               SELECT jsonb_build_array(
                   source_object_type,target_object_type,activity
               ) AS item
               FROM ocpm.binding_neighbor_activity
               WHERE dataset_id=%(dataset_id)s AND tenant_id=%(tenant_id)s
           ) neighbors
       ), '[]'),
       COALESCE((
           SELECT jsonb_agg(item ORDER BY item)::text
           FROM (
               SELECT jsonb_build_array(
                   source_object_type,source_activity,
                   target_object_type,target_activity,related_object_type
               ) AS item
               FROM ocpm.binding_relation_summary
               WHERE dataset_id=%(dataset_id)s AND tenant_id=%(tenant_id)s
           ) relations
       ), '[]')
FROM ocpm.dataset AS dataset
WHERE dataset.dataset_id=%(dataset_id)s AND dataset.tenant_id=%(tenant_id)s"""


def _coverage_rows(value: Any, width: int) -> tuple[tuple[str, ...], ...]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as error:
            raise RuntimeError("invalid binding-index coverage JSON") from error
    if not isinstance(value, list) or any(
        not isinstance(row, list)
        or len(row) != width
        or any(not isinstance(item, str) for item in row)
        for row in value
    ):
        raise RuntimeError(f"invalid binding-index coverage width {width}")
    return tuple(tuple(row) for row in value)


def _cursor_rows(cursor: Cursor) -> Iterator[Any]:
    """Prefer incremental DB-API iteration, retaining a compatibility fallback."""

    iterator = getattr(cursor, "__iter__", None)
    if callable(iterator):
        return iter(cursor)
    return iter(cursor.fetchall())


def score_dynamic_dfg_rows(rows: list[tuple[Any, ...]]) -> dict[str, Any]:
    """Canonicalize DFG rows and rank bottlenecks with the Rust kernel."""

    if not rows:
        raise RuntimeError("dynamic DFG returned no count row")
    selected_counts = {int(row[0]) for row in rows}
    if len(selected_counts) != 1:
        raise RuntimeError("dynamic DFG returned inconsistent selected counts")
    transitions = sorted(
        [
            [str(row[1]), str(row[2]), int(row[3]), round(float(row[4]), 6)]
            for row in rows
            if row[1] is not None
        ]
    )
    order = bottleneck_order(
        [transition[2] for transition in transitions],
        [transition[3] for transition in transitions],
    )
    return {
        "selected_count": selected_counts.pop(),
        "dfg": transitions,
        "bottleneck_order": [transitions[index] for index in order],
    }


class OcpmEngine:
    """Build and execute process-mining read paths over an installed pg_ocpm."""

    def __init__(
        self,
        dataset_id: int,
        tenant_id: int,
        *,
        wide_window_days: int = 30,
    ) -> None:
        if dataset_id <= 0 or tenant_id < 0:
            raise ValueError("dataset_id must be positive and tenant_id non-negative")
        if wide_window_days < 1:
            raise ValueError("wide_window_days must be positive")
        self.dataset_id = dataset_id
        self.tenant_id = tenant_id
        self.wide_window_days = wide_window_days

    @staticmethod
    def verify_pg_ocpm(cursor: Cursor) -> str:
        """Fail early unless pg_ocpm is installed and return its version."""

        cursor.execute("SELECT ocpm.version()")
        row = cursor.fetchone()
        if not row or not row[0]:
            raise RuntimeError("pg_ocpm is not installed in the current database")
        version = str(row[0])
        try:
            parsed = tuple(int(part) for part in version.split(".")[:3])
        except ValueError as error:
            raise RuntimeError(f"invalid pg_ocpm version: {version}") from error
        if parsed < _MINIMUM_PG_OCPM_VERSION:
            raise RuntimeError("ocpm-engine requires pg_ocpm 0.8.0 or later")
        return version

    @staticmethod
    def inspect_pg_ocpm(cursor: Cursor) -> PgOcpmCapabilities:
        """Detect callable extension features and retain an exact fallback."""

        cursor.execute(_PG_OCPM_CAPABILITIES_SQL)
        row = cursor.fetchone()
        if not row or not row[0]:
            raise RuntimeError("pg_ocpm is not installed in the current database")
        version = str(row[0])
        try:
            parsed = tuple(int(part) for part in version.split(".")[:3])
        except ValueError as error:
            raise RuntimeError(f"invalid pg_ocpm version: {version}") from error
        if parsed < _MINIMUM_PG_OCPM_VERSION or not bool(row[1]):
            raise RuntimeError("ocpm-engine requires pg_ocpm 0.8.0 or later")
        return PgOcpmCapabilities(
            version=version,
            event_log_rows=bool(row[1]),
            event_log_batches=bool(row[2]),
            event_log_window_batches=bool(row[3]),
        )

    def inspect_binding_index(self, cursor: Cursor) -> BindingIndexCoverage:
        """Read exact declared binding coverage without assuming freshness."""

        cursor.execute(
            _BINDING_INDEX_COVERAGE_SQL,
            {"dataset_id": self.dataset_id, "tenant_id": self.tenant_id},
        )
        row = cursor.fetchone()
        if not row:
            raise RuntimeError(
                f"dataset {self.dataset_id} for tenant {self.tenant_id} does not exist"
            )
        object_types = _coverage_rows(row[3], 1)
        activities = _coverage_rows(row[4], 2)
        event_activities = _coverage_rows(row[5], 1)
        neighbors = _coverage_rows(row[6], 3)
        relations = _coverage_rows(row[7], 5)
        return BindingIndexCoverage(
            refreshed_at=row[0],
            source_watermark=row[1],
            event_identity_complete=bool(row[2]),
            object_types=tuple(item[0] for item in object_types),
            activities=tuple((item[0], item[1]) for item in activities),
            event_activities=tuple(item[0] for item in event_activities),
            neighbors=tuple(BindingNeighborCoverage(*item) for item in neighbors),
            relations=tuple(BindingRelationCoverage(*item) for item in relations),
        )

    def build_event_log_summary(
        self,
        request: EventLogRequest,
        capabilities: PgOcpmCapabilities,
    ) -> QueryPlan:
        """Choose a factorized 0.9 export or an exact 0.8 compatibility path."""

        self._validate_event_log(request)
        params = {
            "dataset_id": self.dataset_id,
            "tenant_id": self.tenant_id,
            "object_type": request.object_type,
            "from_dates": [window.from_date for window in request.windows],
            "to_dates": [window.to_date for window in request.windows],
        }
        if len(request.windows) == 1 and capabilities.event_log_batches:
            sql = _SINGLE_EVENT_BATCH_SQL
            strategy = "factorized event batch"
        elif capabilities.event_log_window_batches:
            if len(request.windows) <= _EVENT_BATCH_MAX_WINDOWS:
                sql = _WINDOW_EVENT_BATCH_SQL
                strategy = "factorized multi-window event batch"
            else:
                sql = _CHUNKED_WINDOW_EVENT_BATCH_SQL
                strategy = "factorized chunked multi-window event batch"
        else:
            if not capabilities.event_log_rows:
                raise RuntimeError(
                    "pg_ocpm exposes neither factorized event batches nor "
                    "the event-row compatibility export"
                )
            sql = _EVENT_ROW_FALLBACK_SQL
            strategy = "native event-row compatibility fallback"
        return QueryPlan(Endpoint.EVENT_LOG_SUMMARY, sql, params, strategy)

    def execute_event_log_summary(
        self,
        cursor: Cursor,
        request_or_plan: EventLogRequest | QueryPlan,
        *,
        capabilities: PgOcpmCapabilities | None = None,
    ) -> EventLogExecution:
        """Execute a native summary while reporting transfer and expansion costs."""

        if isinstance(request_or_plan, QueryPlan):
            plan = request_or_plan
            window_count = len(plan.params["from_dates"])
        else:
            capabilities = capabilities or self.inspect_pg_ocpm(cursor)
            plan = self.build_event_log_summary(request_or_plan, capabilities)
            window_count = len(request_or_plan.windows)
        if plan.endpoint is not Endpoint.EVENT_LOG_SUMMARY:
            raise ValueError("execute_event_log_summary requires an event-log plan")
        cursor.execute(plan.sql, plan.params)
        database_rows = 0

        def counted_rows() -> Iterator[Any]:
            nonlocal database_rows
            for row in _cursor_rows(cursor):
                database_rows += 1
                yield row

        rows = counted_rows()
        if plan.strategy.startswith("factorized"):
            summaries = summarize_event_window_batch_rows(
                rows, window_count=window_count
            )
            expanded_event_rows = 0
        else:
            summaries = summarize_event_row_fallback(rows, window_count=window_count)
            expanded_event_rows = database_rows
        return EventLogExecution(
            strategy=plan.strategy,
            database_rows=database_rows,
            expanded_event_rows=expanded_event_rows,
            summaries=summaries,
        )

    def build_dynamic_case_ids(self, request: DynamicDfgRequest) -> QueryPlan:
        """Build the exact selected-case projection for a dynamic request."""

        return self._build_dynamic(request, projection="case_ids")

    def build_dynamic_dfg(self, request: DynamicDfgRequest) -> QueryPlan:
        """Build an exact filtered DFG using one uniform filter contract."""

        return self._build_dynamic(request, projection="dfg")

    def execute_dynamic_dfg(
        self,
        cursor: Cursor,
        request_or_plan: DynamicDfgRequest | QueryPlan,
    ) -> dict[str, Any]:
        """Execute a dynamic DFG plan and apply the shared Rust ranking kernel."""

        plan = (
            request_or_plan
            if isinstance(request_or_plan, QueryPlan)
            else self.build_dynamic_dfg(request_or_plan)
        )
        if plan.endpoint is not Endpoint.DYNAMIC_DFG:
            raise ValueError("execute_dynamic_dfg requires a dynamic DFG plan")
        cursor.execute(plan.sql, plan.params)
        return score_dynamic_dfg_rows(list(cursor.fetchall()))

    def _build_dynamic(
        self,
        request: DynamicDfgRequest,
        *,
        projection: Projection,
    ) -> QueryPlan:
        self._validate_dynamic(request)
        sql, params, strategy = compile_dynamic_dfg(
            request,
            dataset_id=self.dataset_id,
            tenant_id=self.tenant_id,
            projection=projection,
        )
        return QueryPlan(
            Endpoint.DYNAMIC_DFG,
            sql,
            params,
            f"{strategy} dynamic {projection}",
        )

    def build(self, request: ProcessMiningRequest) -> QueryPlan:
        endpoint = Endpoint(request.endpoint)
        self._validate_common(request, endpoint)

        if endpoint is Endpoint.ENTIRE_PROCESS_MAP:
            return QueryPlan(
                endpoint,
                queries.ENTIRE_PROCESS_MAP_SQL,
                {
                    "dataset_id": self.dataset_id,
                    "tenant_id": self.tenant_id,
                    "timeline_period": request.timeline_period,
                },
                "whole-dataset rollups",
            )

        from_date = request.from_date or _MIN_TIMESTAMP
        to_date = request.to_date or _MAX_TIMESTAMP
        window_days = (to_date - from_date).total_seconds() / 86_400
        params: dict[str, Any] = {
            "dataset_id": self.dataset_id,
            "tenant_id": self.tenant_id,
            "from_date": from_date,
            "to_date": to_date,
            "backbone_type": request.backbone_type,
            "status": request.status,
            "timeline_period": request.timeline_period,
        }

        if endpoint is Endpoint.VARIANT_LIST:
            wide = window_days > self.wide_window_days
            return QueryPlan(
                endpoint,
                (
                    queries.CASE_BUCKET_VARIANT_SQL
                    if wide
                    else queries.CASE_WINDOW_VARIANT_SQL
                ),
                params,
                "segmented case buckets" if wide else "case window",
            )
        if endpoint is Endpoint.TIMELINE:
            return QueryPlan(
                endpoint,
                queries.CASE_WINDOW_TIMELINE_SQL,
                params,
                "selected-variant timeline",
            )
        if endpoint is Endpoint.CASE_THROUGHPUT:
            return QueryPlan(
                endpoint,
                queries.CASE_WINDOW_THROUGHPUT_SQL,
                params,
                "selected-variant duration histogram",
            )
        if endpoint is Endpoint.EDGE_INFO:
            params.update(
                {
                    "source_activity": request.source_activity,
                    "target_activity": request.target_activity,
                    "edge_context": request.edge_context,
                }
            )
            return QueryPlan(
                endpoint,
                queries.EDGE_INFO_SQL,
                params,
                "selected-case adjacency and edge buckets",
            )
        if endpoint is Endpoint.CASE_LIST:
            params.update({"limit": request.limit, "offset": request.offset})
            return QueryPlan(
                endpoint,
                queries.CASE_LIST_SQL,
                params,
                "paginated case hydration",
            )
        if endpoint is Endpoint.PROCESS_MAP:
            return self._build_process_map(request, params, window_days)
        raise AssertionError(f"unhandled endpoint: {endpoint}")

    def execute(self, cursor: Cursor, request: ProcessMiningRequest) -> Any:
        plan = self.build(request)
        cursor.execute(plan.sql, plan.params)
        row = cursor.fetchone()
        return None if row is None else row[0]

    def _build_process_map(
        self,
        request: ProcessMiningRequest,
        params: dict[str, Any],
        window_days: float,
    ) -> QueryPlan:
        if len(request.network.edges) > 1:
            raise ValueError("the current process-map plan supports one included edge")
        if (request.network.min_execution_time is None) != (
            request.network.max_execution_time is None
        ):
            raise ValueError(
                "case execution-time filters require both minimum and maximum"
            )
        if (
            request.network.min_execution_time is not None
            and request.network.min_execution_time > request.network.max_execution_time
        ):
            raise ValueError("case execution-time minimum must not exceed maximum")
        for edge in request.network.edges:
            if edge.min_execution_time > edge.max_execution_time:
                raise ValueError("edge execution-time minimum must not exceed maximum")

        backbone_prefix = f"{request.backbone_type}:"
        backbone_activities = tuple(
            activity
            for activity in request.network.activities
            if activity.startswith(backbone_prefix)
        )
        connected_activities = tuple(
            activity
            for activity in request.network.activities
            if not activity.startswith(backbone_prefix)
        )
        edge = request.network.edges[0] if request.network.edges else None
        params.update(
            {
                "variant_hashes": list(request.variants) or None,
                "backbone_activities": list(backbone_activities) or None,
                "connected_activities": list(connected_activities) or None,
                "case_min_execution": request.network.min_execution_time,
                "case_max_execution": request.network.max_execution_time,
                "included_edge_source": edge.source if edge else None,
                "included_edge_target": edge.target if edge else None,
                "included_edge_min": edge.min_execution_time if edge else None,
                "included_edge_max": edge.max_execution_time if edge else None,
            }
        )
        filtered_network = bool(connected_activities or edge)
        closure = request.transitive_closure
        if closure is None:
            closure = window_days > self.wide_window_days
        sql = queries.process_map_sql(
            filtered_network=filtered_network,
            transitive_closure=closure,
        )
        traversal = "closure" if closure else "one-hop"
        filter_strategy = "filtered" if filtered_network else "unfiltered"
        return QueryPlan(
            Endpoint.PROCESS_MAP,
            sql,
            params,
            f"native {traversal} {filter_strategy} process map",
        )

    @staticmethod
    def _validate_common(request: ProcessMiningRequest, endpoint: Endpoint) -> None:
        if request.timeline_period not in _TIMELINE_PERIODS:
            raise ValueError(f"unsupported timeline period: {request.timeline_period}")
        if (
            request.from_date
            and request.to_date
            and request.from_date > request.to_date
        ):
            raise ValueError("from_date must not be after to_date")
        if endpoint is not Endpoint.ENTIRE_PROCESS_MAP and not request.backbone_type:
            raise ValueError(f"backbone_type is required for {endpoint.value}")
        if endpoint is Endpoint.EDGE_INFO and (
            not request.source_activity or not request.target_activity
        ):
            raise ValueError("edge_info requires source_activity and target_activity")
        if request.limit < 1 or request.limit > 1_000:
            raise ValueError("limit must be between 1 and 1000")
        if request.offset < 0:
            raise ValueError("offset must be non-negative")

    @staticmethod
    def _validate_dynamic(request: DynamicDfgRequest) -> None:
        if not request.backbone_type.strip():
            raise ValueError("dynamic DFG backbone_type must not be empty")
        if request.from_date > request.to_date:
            raise ValueError("from_date must not be after to_date")
        dynamic_filter: DynamicFilter = request.filter
        paired_ranges = (
            (
                dynamic_filter.min_case_execution_time,
                dynamic_filter.max_case_execution_time,
                "case execution-time",
            ),
        )
        for minimum, maximum, label in paired_ranges:
            if (minimum is None) != (maximum is None):
                raise ValueError(f"{label} filters require both minimum and maximum")
            if minimum is not None and maximum is not None and minimum > maximum:
                raise ValueError(f"{label} minimum must not exceed maximum")
        for edge in (*dynamic_filter.included_edges, *dynamic_filter.excluded_edges):
            if not edge.source.strip() or not edge.target.strip():
                raise ValueError("edge activities must not be empty")
            if edge.min_execution_time > edge.max_execution_time:
                raise ValueError("edge execution-time minimum must not exceed maximum")
        for attribute in (
            *dynamic_filter.included_event_attributes,
            *dynamic_filter.excluded_event_attributes,
        ):
            if not attribute.key.strip():
                raise ValueError("event attribute keys must not be empty")

    @staticmethod
    def _validate_event_log(request: EventLogRequest) -> None:
        if not request.object_type:
            raise ValueError("event-log requests require a non-empty object_type")
        if not request.windows:
            raise ValueError("event-log requests require at least one window")
        for window in request.windows:
            if window.from_date > window.to_date:
                raise ValueError("event-log window from_date must not exceed to_date")
