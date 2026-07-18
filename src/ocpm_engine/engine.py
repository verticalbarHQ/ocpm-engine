"""Translate API request shapes into parameterized pg_ocpm queries."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Protocol

from . import queries
from .models import Endpoint, ProcessMiningRequest, QueryPlan


class Cursor(Protocol):
    def execute(self, query: str, params: dict[str, Any] | None = None) -> Any: ...

    def fetchone(self) -> Any: ...


_MIN_TIMESTAMP = datetime(1, 1, 1, tzinfo=UTC)
_MAX_TIMESTAMP = datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)
_TIMELINE_PERIODS = frozenset({"hour", "day", "week", "month", "quarter", "year"})
_MINIMUM_PG_OCPM_VERSION = (0, 3, 0)


class OcpmEngine:
    """Build and execute the Vertical Bar read paths over an installed pg_ocpm."""

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
            raise RuntimeError("ocpm-engine requires pg_ocpm 0.3.0 or later")
        return version

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
