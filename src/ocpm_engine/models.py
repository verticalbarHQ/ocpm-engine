"""Public request and query-plan models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Mapping


class Endpoint(StrEnum):
    """Generic process-mining request shapes."""

    PROCESS_MAP = "process_map"
    VARIANT_LIST = "variant_list"
    TIMELINE = "timeline"
    CASE_THROUGHPUT = "case_throughput"
    EDGE_INFO = "edge_info"
    CASE_LIST = "case_list"
    ENTIRE_PROCESS_MAP = "entire_process_map"
    DYNAMIC_DFG = "dynamic_dfg"
    EVENT_LOG_SUMMARY = "event_log_summary"


@dataclass(frozen=True, slots=True)
class EdgeFilter:
    source: str
    target: str
    min_execution_time: float = 0.0
    max_execution_time: float = float("inf")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> EdgeFilter:
        return cls(
            source=str(value["source"]),
            target=str(value["target"]),
            min_execution_time=float(value.get("min_execution_time", 0.0)),
            max_execution_time=float(value.get("max_execution_time", float("inf"))),
        )


@dataclass(frozen=True, slots=True)
class EventAttributeFilter:
    """Require a case event whose selected attribute equals one value.

    ``actor`` and ``context`` address the first-class event columns. Every
    other key addresses the source-neutral JSON attribute map.
    """

    key: str
    value: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> EventAttributeFilter:
        key = value.get("key") or value.get("name") or value.get("field")
        if key is None:
            raise ValueError("event attribute filters require a key")
        return cls(key=str(key), value=str(value["value"]))


@dataclass(frozen=True, slots=True)
class DynamicFilter:
    """Composable exact case predicates for dynamic DFG requests.

    Every tuple is conjunctive. Included predicates must exist; excluded
    predicates must not exist. Multiple statuses are alternatives.
    """

    statuses: tuple[str, ...] = ()
    included_activities: tuple[str, ...] = ()
    excluded_activities: tuple[str, ...] = ()
    min_case_execution_time: float | None = None
    max_case_execution_time: float | None = None
    included_event_attributes: tuple[EventAttributeFilter, ...] = ()
    excluded_event_attributes: tuple[EventAttributeFilter, ...] = ()
    included_related_object_types: tuple[str, ...] = ()
    excluded_related_object_types: tuple[str, ...] = ()
    included_edges: tuple[EdgeFilter, ...] = ()
    excluded_edges: tuple[EdgeFilter, ...] = ()

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> DynamicFilter:
        if not value:
            return cls()
        activities = value.get("activities") or {}
        attributes = value.get("event_attributes") or {}
        related = value.get("related_object_types") or {}
        edges = value.get("edges") or {}
        execution_range = value.get("case_execution_time_range") or {}
        statuses = value.get("statuses") or value.get("status") or ()
        if isinstance(statuses, str):
            statuses = (statuses,)
        return cls(
            statuses=tuple(str(item) for item in statuses),
            included_activities=tuple(
                str(item) for item in activities.get("include", ())
            ),
            excluded_activities=tuple(
                str(item) for item in activities.get("exclude", ())
            ),
            min_case_execution_time=execution_range.get("min_execution_time"),
            max_case_execution_time=execution_range.get("max_execution_time"),
            included_event_attributes=tuple(
                EventAttributeFilter.from_mapping(item)
                for item in attributes.get("include", ())
            ),
            excluded_event_attributes=tuple(
                EventAttributeFilter.from_mapping(item)
                for item in attributes.get("exclude", ())
            ),
            included_related_object_types=tuple(
                str(item) for item in related.get("include", ())
            ),
            excluded_related_object_types=tuple(
                str(item) for item in related.get("exclude", ())
            ),
            included_edges=tuple(
                EdgeFilter.from_mapping(item) for item in edges.get("include", ())
            ),
            excluded_edges=tuple(
                EdgeFilter.from_mapping(item) for item in edges.get("exclude", ())
            ),
        )


@dataclass(frozen=True, slots=True)
class DynamicDfgRequest:
    """A dynamic, exact directly-follows graph request."""

    backbone_type: str
    from_date: datetime
    to_date: datetime
    filter: DynamicFilter = field(default_factory=DynamicFilter)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> DynamicDfgRequest:
        from_date = _parse_datetime(value.get("from_date"))
        to_date = _parse_datetime(value.get("to_date"))
        if from_date is None or to_date is None:
            raise ValueError("dynamic DFG requests require from_date and to_date")
        return cls(
            backbone_type=str(value["backbone_type"]),
            from_date=from_date,
            to_date=to_date,
            filter=DynamicFilter.from_mapping(value.get("filter")),
        )


@dataclass(frozen=True, slots=True)
class EventLogWindow:
    from_date: datetime
    to_date: datetime

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> EventLogWindow:
        from_date = _parse_datetime(value.get("from_date"))
        to_date = _parse_datetime(value.get("to_date"))
        if from_date is None or to_date is None:
            raise ValueError("event-log windows require from_date and to_date")
        return cls(from_date=from_date, to_date=to_date)


@dataclass(frozen=True, slots=True)
class EventLogRequest:
    """One or more fully-contained case windows for native Rust summaries."""

    object_type: str
    windows: tuple[EventLogWindow, ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> EventLogRequest:
        source_windows = value.get("windows")
        if source_windows is None:
            source_windows = (value,)
        return cls(
            object_type=str(value["object_type"]),
            windows=tuple(EventLogWindow.from_mapping(item) for item in source_windows),
        )


@dataclass(frozen=True, slots=True)
class PgOcpmCapabilities:
    version: str
    event_log_rows: bool
    event_log_batches: bool
    event_log_window_batches: bool

    @property
    def factorized_event_export(self) -> bool:
        return self.event_log_batches

    @property
    def factorized_multi_window_export(self) -> bool:
        return self.event_log_window_batches


@dataclass(frozen=True, slots=True)
class BindingNeighborCoverage:
    source_object_type: str
    target_object_type: str
    activity: str


@dataclass(frozen=True, slots=True)
class BindingRelationCoverage:
    source_object_type: str
    source_activity: str
    target_object_type: str
    target_activity: str
    related_object_type: str


@dataclass(frozen=True, slots=True)
class BindingIndexCoverage:
    """Observable binding-index declarations and dataset refresh markers."""

    refreshed_at: datetime | None
    source_watermark: datetime | None
    event_identity_complete: bool
    object_types: tuple[str, ...]
    activities: tuple[tuple[str, str], ...]
    event_activities: tuple[str, ...]
    neighbors: tuple[BindingNeighborCoverage, ...]
    relations: tuple[BindingRelationCoverage, ...]

    def covers_object_type(self, object_type: str) -> bool:
        return object_type in self.object_types

    def covers_activity(self, object_type: str, activity: str) -> bool:
        return (object_type, activity) in self.activities

    def covers_event_activity(self, activity: str) -> bool:
        return activity in self.event_activities

    def covers_neighbor(
        self, source_object_type: str, target_object_type: str, activity: str
    ) -> bool:
        return (
            BindingNeighborCoverage(source_object_type, target_object_type, activity)
            in self.neighbors
        )

    def covers_relation(
        self,
        source_object_type: str,
        source_activity: str,
        target_object_type: str,
        target_activity: str,
        related_object_type: str,
    ) -> bool:
        return (
            BindingRelationCoverage(
                source_object_type,
                source_activity,
                target_object_type,
                target_activity,
                related_object_type,
            )
            in self.relations
        )


@dataclass(frozen=True, slots=True)
class NetworkFilter:
    activities: tuple[str, ...] = ()
    edges: tuple[EdgeFilter, ...] = ()
    min_execution_time: float | None = None
    max_execution_time: float | None = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> NetworkFilter:
        if not value:
            return cls()
        activities = value.get("activities") or {}
        edges = value.get("edges") or {}
        execution_range = value.get("execution_time_range") or {}
        return cls(
            activities=tuple(str(item) for item in activities.get("include", ())),
            edges=tuple(
                EdgeFilter.from_mapping(item) for item in edges.get("include", ())
            ),
            min_execution_time=execution_range.get("min_execution_time"),
            max_execution_time=execution_range.get("max_execution_time"),
        )


def _parse_datetime(value: Any) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    raise TypeError(f"expected datetime, ISO-8601 string, or None; got {type(value)!r}")


@dataclass(frozen=True, slots=True)
class ProcessMiningRequest:
    endpoint: Endpoint | str
    backbone_type: str | None = None
    from_date: datetime | None = None
    to_date: datetime | None = None
    timeline_period: str = "week"
    status: str | None = None
    variants: tuple[str, ...] = ()
    network: NetworkFilter = field(default_factory=NetworkFilter)
    source_activity: str | None = None
    target_activity: str | None = None
    edge_context: str | None = None
    limit: int = 20
    offset: int = 0
    transitive_closure: bool | None = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ProcessMiningRequest:
        attribute_filter = value.get("attribute_filter") or {}
        statuses = attribute_filter.get("status") or ()
        return cls(
            endpoint=Endpoint(str(value["endpoint"])),
            backbone_type=value.get("backbone_type"),
            from_date=_parse_datetime(value.get("from_date")),
            to_date=_parse_datetime(value.get("to_date")),
            timeline_period=str(value.get("timeline_period", "week")),
            status=value.get("status") or (statuses[0] if statuses else None),
            variants=tuple(str(item) for item in value.get("variants") or ()),
            network=NetworkFilter.from_mapping(value.get("network_filter")),
            source_activity=value.get("source_activity"),
            target_activity=value.get("target_activity"),
            edge_context=value.get("edge_context"),
            limit=int(value.get("limit", 20)),
            offset=int(value.get("offset", 0)),
            transitive_closure=value.get("transitive_closure"),
        )


@dataclass(frozen=True, slots=True)
class QueryPlan:
    endpoint: Endpoint
    sql: str
    params: dict[str, Any]
    strategy: str
