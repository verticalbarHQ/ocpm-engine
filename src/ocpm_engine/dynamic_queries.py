"""Compile composable dynamic filters into exact pg_ocpm DFG queries."""

from __future__ import annotations

from typing import Any, Literal

from .models import DynamicDfgRequest, EventAttributeFilter

Projection = Literal["case_ids", "dfg"]


_BASE_CTE = """base AS MATERIALIZED (
    SELECT item.case_id
    FROM ocpm.case_bucket bucket
    CROSS JOIN LATERAL unnest(
        bucket.case_ids,bucket.statuses,bucket.start_times,
        bucket.end_times,bucket.execution_times
    ) AS item(case_id,status,start_time,end_time,execution_time)
    WHERE bucket.dataset_id=%(dataset_id)s
      AND bucket.tenant_id=%(tenant_id)s
      AND bucket.object_type=%(backbone_type)s
      AND bucket.min_start_time<=%(to_date)s
      AND bucket.max_end_time>=%(from_date)s
      AND item.start_time>=%(from_date)s
      AND item.end_time<=%(to_date)s
      AND (%(statuses)s IS NULL OR item.status=ANY(%(statuses)s))
      AND (%(included_activities)s IS NULL
           OR bucket.activities @> %(included_activities)s)
      AND (%(excluded_activities)s IS NULL
           OR NOT bucket.activities && %(excluded_activities)s)
      AND (%(case_min_execution)s IS NULL
           OR item.execution_time BETWEEN %(case_min_execution)s
                                       AND %(case_max_execution)s)
)"""


_EVENT_ROWS_CTE = """event_rows AS MATERIALIZED (
    SELECT event.case_id,event.context,event.updated_by,event.attributes
    FROM ocpm.event_chunk chunk
    CROSS JOIN LATERAL unnest(
        chunk.case_ids,chunk.event_timestamps,
        chunk.contexts,chunk.updated_bys,chunk.attributes
    ) AS event(case_id,event_timestamp,context,updated_by,attributes)
    WHERE chunk.dataset_id=%(dataset_id)s
      AND chunk.tenant_id=%(tenant_id)s
      AND chunk.object_type=%(backbone_type)s
      AND event.event_timestamp>=%(from_date)s
      AND event.event_timestamp<=%(to_date)s
)"""


_CONNECTED_CTE = """connected AS MATERIALIZED (
    SELECT connected.backbone_case_id AS case_id,
           connected.object_type
    FROM ocpm.connected_objects_one_hop(
        %(dataset_id)s,%(tenant_id)s,%(backbone_type)s,
        %(from_date)s,%(to_date)s,
        ARRAY(SELECT case_id FROM base)
    ) AS connected
)"""


_LIFECYCLE_EVENTS_CTE = """lifecycle_events AS MATERIALIZED (
    SELECT case_id,activity,event_timestamp,event_ordinal
    FROM ocpm.event_log_rows(
        %(dataset_id)s,%(tenant_id)s,%(backbone_type)s,
        %(from_date)s,%(to_date)s
    )
)"""


_LIFECYCLE_EDGES_CTE = """lifecycle_edges AS MATERIALIZED (
    SELECT case_id,
           activity AS source_activity,
           lead(activity) OVER lifecycle AS target_activity,
           event_timestamp AS source_timestamp,
           lead(event_timestamp) OVER lifecycle AS target_timestamp
    FROM lifecycle_events
    WINDOW lifecycle AS (PARTITION BY case_id ORDER BY event_ordinal)
)"""


_BUCKET_EDGES_CTE = """dfg_edges AS MATERIALIZED (
    SELECT bucket.source_activity,bucket.target_activity,
           item.case_id,item.execution_time
    FROM ocpm.edge_bucket bucket
    CROSS JOIN LATERAL unnest(
        bucket.case_ids,bucket.source_timestamps,
        bucket.target_timestamps,bucket.execution_times
    ) AS item(case_id,source_timestamp,target_timestamp,execution_time)
    WHERE bucket.dataset_id=%(dataset_id)s
      AND bucket.tenant_id=%(tenant_id)s
      AND bucket.source_object_type=%(backbone_type)s
      AND bucket.target_object_type=%(backbone_type)s
      AND bucket.edge_type='intra'
      AND bucket.source_activity<>%(backbone_type)s
      AND bucket.min_source_timestamp<=%(to_date)s
      AND bucket.max_target_timestamp>=%(from_date)s
      AND item.source_timestamp>=%(from_date)s
      AND item.target_timestamp<=%(to_date)s
)"""


_BUCKET_DFG_CTE = """dfg AS (
    SELECT edge.source_activity,
           edge.target_activity,
           count(*)::bigint AS frequency,
           round(avg(edge.execution_time)::numeric,6)::double precision
               AS mean_duration
    FROM dfg_edges edge
    JOIN selected USING (case_id)
    GROUP BY edge.source_activity,edge.target_activity
)"""


_STREAM_DFG_CTE = """dfg AS (
    SELECT edge.source_activity,
           edge.target_activity,
           count(*)::bigint AS frequency,
           round(avg(extract(epoch FROM (
               edge.target_timestamp-edge.source_timestamp
           )))::numeric,6)::double precision AS mean_duration
    FROM lifecycle_edges edge
    JOIN selected USING (case_id)
    WHERE edge.target_activity IS NOT NULL
    GROUP BY edge.source_activity,edge.target_activity
)"""


_DFG_PROJECTION = """SELECT selection.selected_count,
       dfg.source_activity,dfg.target_activity,dfg.frequency,dfg.mean_duration
FROM (SELECT count(*)::bigint AS selected_count FROM selected) AS selection
LEFT JOIN dfg ON true
ORDER BY source_activity,target_activity"""


_CASE_ID_PROJECTION = """SELECT coalesce(
    array_agg(case_id ORDER BY case_id),'{}'::bigint[]
) FROM selected"""


def _attribute_expression(index: int, prefix: str) -> str:
    key = f"{prefix}_attribute_key_{index}"
    value = f"{prefix}_attribute_value_{index}"
    return f"""(CASE
        WHEN %({key})s IN ('actor','updated_by')
            THEN nullif(event_rows.updated_by,'')
        WHEN %({key})s='context'
            THEN nullif(event_rows.context,'')
        ELSE event_rows.attributes->>%({key})s
    END)=%({value})s"""


def _bind_attributes(
    params: dict[str, Any],
    filters: tuple[EventAttributeFilter, ...],
    prefix: str,
) -> list[str]:
    expressions = []
    for index, attribute in enumerate(filters):
        params[f"{prefix}_attribute_key_{index}"] = attribute.key
        params[f"{prefix}_attribute_value_{index}"] = attribute.value
        expressions.append(_attribute_expression(index, prefix))
    return expressions


def _edge_expression(index: int, prefix: str) -> str:
    return f"""(
        edge.source_activity=%({prefix}_edge_source_{index})s
        AND edge.target_activity=%({prefix}_edge_target_{index})s
        AND extract(epoch FROM (
            edge.target_timestamp-edge.source_timestamp
        )) BETWEEN %({prefix}_edge_min_{index})s
              AND %({prefix}_edge_max_{index})s
    )"""


def compile_dynamic_dfg(
    request: DynamicDfgRequest,
    *,
    dataset_id: int,
    tenant_id: int,
    projection: Projection,
) -> tuple[str, dict[str, Any], str]:
    """Return SQL, parameters, and the selected dynamic execution strategy."""

    dynamic_filter = request.filter
    params: dict[str, Any] = {
        "dataset_id": dataset_id,
        "tenant_id": tenant_id,
        "backbone_type": request.backbone_type,
        "from_date": request.from_date,
        "to_date": request.to_date,
        "statuses": list(dynamic_filter.statuses) or None,
        "included_activities": list(dynamic_filter.included_activities) or None,
        "excluded_activities": list(dynamic_filter.excluded_activities) or None,
        "case_min_execution": dynamic_filter.min_case_execution_time,
        "case_max_execution": dynamic_filter.max_case_execution_time,
        "included_related_types": (
            list(dynamic_filter.included_related_object_types) or None
        ),
        "included_related_type_count": len(
            set(dynamic_filter.included_related_object_types)
        ),
        "excluded_related_types": (
            list(dynamic_filter.excluded_related_object_types) or None
        ),
    }
    ctes = [_BASE_CTE]
    included_sets: list[str] = []
    excluded_sets: list[str] = []

    included_attributes = _bind_attributes(
        params, dynamic_filter.included_event_attributes, "included"
    )
    excluded_attributes = _bind_attributes(
        params, dynamic_filter.excluded_event_attributes, "excluded"
    )
    if included_attributes or excluded_attributes:
        ctes.append(_EVENT_ROWS_CTE)
    if included_attributes:
        included_attribute_having = " AND ".join(
            f"bool_or({item})" for item in included_attributes
        )
        ctes.append(
            "attribute_included AS MATERIALIZED (\n"
            "    SELECT case_id FROM event_rows\n"
            "    GROUP BY case_id\n"
            f"    HAVING {included_attribute_having}\n"
            ")"
        )
        included_sets.append("attribute_included")
    if excluded_attributes:
        ctes.append(
            "attribute_excluded AS MATERIALIZED (\n"
            "    SELECT DISTINCT case_id FROM event_rows\n"
            f"    WHERE {' OR '.join(excluded_attributes)}\n"
            ")"
        )
        excluded_sets.append("attribute_excluded")

    has_relations = bool(
        dynamic_filter.included_related_object_types
        or dynamic_filter.excluded_related_object_types
    )
    if has_relations:
        ctes.append(_CONNECTED_CTE)
    if dynamic_filter.included_related_object_types:
        ctes.append(
            "relation_included AS MATERIALIZED (\n"
            "    SELECT case_id FROM connected\n"
            "    WHERE object_type=ANY(%(included_related_types)s)\n"
            "    GROUP BY case_id\n"
            "    HAVING count(DISTINCT object_type)="
            "%(included_related_type_count)s\n"
            ")"
        )
        included_sets.append("relation_included")
    if dynamic_filter.excluded_related_object_types:
        ctes.append(
            "relation_excluded AS MATERIALIZED (\n"
            "    SELECT DISTINCT case_id FROM connected\n"
            "    WHERE object_type=ANY(%(excluded_related_types)s)\n"
            ")"
        )
        excluded_sets.append("relation_excluded")

    has_edge_filters = bool(
        dynamic_filter.included_edges or dynamic_filter.excluded_edges
    )
    if has_edge_filters:
        ctes.extend((_LIFECYCLE_EVENTS_CTE, _LIFECYCLE_EDGES_CTE))
    included_edges = []
    for index, edge in enumerate(dynamic_filter.included_edges):
        params[f"included_edge_source_{index}"] = edge.source
        params[f"included_edge_target_{index}"] = edge.target
        params[f"included_edge_min_{index}"] = edge.min_execution_time
        params[f"included_edge_max_{index}"] = edge.max_execution_time
        included_edges.append(_edge_expression(index, "included"))
    if included_edges:
        included_edge_having = " AND ".join(
            f"bool_or({item})" for item in included_edges
        )
        ctes.append(
            "edge_included AS MATERIALIZED (\n"
            "    SELECT case_id FROM lifecycle_edges edge\n"
            "    WHERE edge.target_activity IS NOT NULL\n"
            "    GROUP BY case_id\n"
            f"    HAVING {included_edge_having}\n"
            ")"
        )
        included_sets.append("edge_included")
    excluded_edges = []
    for index, edge in enumerate(dynamic_filter.excluded_edges):
        params[f"excluded_edge_source_{index}"] = edge.source
        params[f"excluded_edge_target_{index}"] = edge.target
        params[f"excluded_edge_min_{index}"] = edge.min_execution_time
        params[f"excluded_edge_max_{index}"] = edge.max_execution_time
        excluded_edges.append(_edge_expression(index, "excluded"))
    if excluded_edges:
        ctes.append(
            "edge_excluded AS MATERIALIZED (\n"
            "    SELECT DISTINCT case_id FROM lifecycle_edges edge\n"
            "    WHERE edge.target_activity IS NOT NULL\n"
            f"      AND ({' OR '.join(excluded_edges)})\n"
            ")"
        )
        excluded_sets.append("edge_excluded")

    selection = "SELECT case_id FROM base"
    for source in included_sets:
        selection += f"\nINTERSECT\nSELECT case_id FROM {source}"
    for source in excluded_sets:
        selection += f"\nEXCEPT\nSELECT case_id FROM {source}"
    ctes.append(f"selected AS MATERIALIZED (\n{selection}\n)")

    if projection == "case_ids":
        result_sql = _CASE_ID_PROJECTION
    else:
        if has_edge_filters:
            ctes.append(_STREAM_DFG_CTE)
        else:
            ctes.extend((_BUCKET_EDGES_CTE, _BUCKET_DFG_CTE))
        result_sql = _DFG_PROJECTION
    strategy = "native event stream" if has_edge_filters else "compact bucket scan"
    return "WITH " + ",\n".join(ctes) + "\n" + result_sql, params, strategy
