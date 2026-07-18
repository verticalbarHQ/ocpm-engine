"""Public request and query-plan models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Mapping


class Endpoint(StrEnum):
    """Process-mining request shapes used by the Vertical Bar API."""

    PROCESS_MAP = "process_map"
    VARIANT_LIST = "variant_list"
    TIMELINE = "timeline"
    CASE_THROUGHPUT = "case_throughput"
    EDGE_INFO = "edge_info"
    CASE_LIST = "case_list"
    ENTIRE_PROCESS_MAP = "entire_process_map"


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
