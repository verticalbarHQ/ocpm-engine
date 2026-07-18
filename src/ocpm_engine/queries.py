"""Parameterized SQL for process-mining response shapes."""

CASE_WINDOW_VARIANT_SQL = """
WITH cases AS MATERIALIZED (
    SELECT * FROM ocpm.case_window(
        %(dataset_id)s, %(tenant_id)s, %(backbone_type)s,
        %(from_date)s, %(to_date)s, %(status)s
    )
), variants AS (
    SELECT min(activity_path::text)::jsonb AS activity_path,
           path_text, path_hash, count(*)::bigint AS frequency,
           avg(execution_time) AS average_time,
           min(execution_time) AS minimum_time,
           max(execution_time) AS maximum_time,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY execution_time) AS median_time,
           stddev(execution_time) AS standard_deviation
    FROM cases
    GROUP BY path_text, path_hash
    ORDER BY frequency DESC, average_time
    LIMIT 101
)
SELECT coalesce(jsonb_agg(jsonb_build_array(
    activity_path, frequency,
    round(average_time::numeric, 6)::double precision,
    round(minimum_time::numeric, 6)::double precision,
    round(maximum_time::numeric, 6)::double precision,
    round(median_time::numeric, 6)::double precision,
    CASE WHEN standard_deviation IS NULL THEN NULL
         ELSE round(standard_deviation::numeric, 6)::double precision END,
    path_hash
) ORDER BY path_hash), '[]'::jsonb)
FROM variants
"""


CASE_BUCKET_VARIANT_SQL = """
WITH full_cases AS (
    SELECT bucket.path_text,
           bucket.path_hash,
           item.execution_time
    FROM ocpm.case_bucket bucket
    CROSS JOIN LATERAL unnest(
        bucket.statuses,
        bucket.start_times,
        bucket.end_times,
        bucket.execution_times
    ) AS item(status, start_time, end_time, execution_time)
    WHERE bucket.dataset_id = %(dataset_id)s
      AND bucket.tenant_id = %(tenant_id)s
      AND bucket.object_type = %(backbone_type)s
      AND bucket.min_start_time <= %(to_date)s
      AND bucket.max_start_time >= %(from_date)s
      AND item.start_time >= %(from_date)s
      AND item.end_time <= %(to_date)s
      AND (%(status)s IS NULL OR item.status = %(status)s)
), boundary_candidate AS MATERIALIZED (
    SELECT bucket.activity_path AS full_activity_path,
           item.status,
           item.start_time,
           item.end_time,
           item.event_timestamp_payload
    FROM ocpm.case_bucket bucket
    CROSS JOIN LATERAL unnest(
        bucket.statuses,
        bucket.start_times,
        bucket.end_times,
        bucket.event_timestamp_payloads
    ) AS item(status, start_time, end_time, event_timestamp_payload)
    WHERE bucket.dataset_id = %(dataset_id)s
      AND bucket.tenant_id = %(tenant_id)s
      AND bucket.object_type = %(backbone_type)s
      AND bucket.min_start_time <= %(to_date)s
      AND bucket.max_end_time >= %(from_date)s
      AND item.start_time <= %(to_date)s
      AND item.end_time >= %(from_date)s
      AND NOT (
          item.start_time >= %(from_date)s
          AND item.end_time <= %(to_date)s
      )
      AND (%(status)s IS NULL OR item.status = %(status)s)
), boundary_cases AS (
    SELECT clipped.path_text,
           substring(md5(clipped.path_text), 1, 10) AS path_hash,
           extract(epoch FROM (clipped.end_time - clipped.start_time))
               ::double precision AS execution_time
    FROM boundary_candidate candidate
    CROSS JOIN LATERAL (
        SELECT '[' || string_agg(
                   (candidate.full_activity_path ->
                       (position::integer - 1))::text,
                   E', \\n ' ORDER BY position
               ) || ']' AS path_text,
               min(event.timestamp) AS start_time,
               max(event.timestamp) AS end_time
        FROM unnest(ocpm.timestamp_decode(
            candidate.event_timestamp_payload
        )) WITH ORDINALITY AS event(timestamp, position)
        WHERE event.timestamp >= %(from_date)s
          AND event.timestamp <= %(to_date)s
    ) clipped
    WHERE clipped.start_time IS NOT NULL
), variant_cases AS (
    SELECT * FROM full_cases
    UNION ALL
    SELECT * FROM boundary_cases
), variants AS (
    SELECT min(path_text)::jsonb AS activity_path,
           path_text,
           path_hash,
           count(*)::bigint AS frequency,
           avg(execution_time) AS average_time,
           min(execution_time) AS minimum_time,
           max(execution_time) AS maximum_time,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY execution_time)
               AS median_time,
           stddev(execution_time) AS standard_deviation
    FROM variant_cases
    GROUP BY path_text, path_hash
    ORDER BY frequency DESC, average_time
    LIMIT 101
)
SELECT coalesce(jsonb_agg(jsonb_build_array(
    activity_path, frequency,
    round(average_time::numeric, 6)::double precision,
    round(minimum_time::numeric, 6)::double precision,
    round(maximum_time::numeric, 6)::double precision,
    round(median_time::numeric, 6)::double precision,
    CASE WHEN standard_deviation IS NULL THEN NULL
         ELSE round(standard_deviation::numeric, 6)::double precision END,
    path_hash
) ORDER BY path_hash), '[]'::jsonb)
FROM variants
"""


_CASE_WINDOW_PREFIX = """
WITH cases AS MATERIALIZED (
    SELECT * FROM ocpm.case_window(
        %(dataset_id)s, %(tenant_id)s, %(backbone_type)s,
        %(from_date)s, %(to_date)s, %(status)s
    )
), top_path AS (
    SELECT path_text, count(*) AS frequency, avg(execution_time) AS average_time
    FROM cases
    GROUP BY path_text
    ORDER BY frequency DESC, average_time
    LIMIT 1
), selected AS MATERIALIZED (
    SELECT cases.* FROM cases JOIN top_path USING (path_text)
)
"""


CASE_WINDOW_TIMELINE_SQL = (
    _CASE_WINDOW_PREFIX
    + """
SELECT coalesce(jsonb_agg(jsonb_build_array(bucket_us, case_count)
                          ORDER BY bucket_us), '[]'::jsonb)
FROM (
    SELECT
        (extract(epoch FROM date_trunc(%(timeline_period)s, start_time))
            * 1000000)::bigint AS bucket_us,
        count(*)::bigint AS case_count
    FROM selected
    GROUP BY 1
) timeline
"""
)


CASE_WINDOW_THROUGHPUT_SQL = (
    _CASE_WINDOW_PREFIX
    + """,
overall AS (
    SELECT count(*)::bigint AS total_cases,
           min(execution_time)::double precision AS minimum_time,
           max(execution_time)::double precision AS maximum_time,
           avg(execution_time)::double precision AS average_time,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY execution_time)
               ::double precision AS median_time,
           stddev(execution_time)::double precision AS standard_deviation
    FROM selected
), binned AS (
    SELECT execution_time,
           CASE WHEN (SELECT minimum_time FROM overall) =
                          (SELECT maximum_time FROM overall)
                THEN 0
                ELSE width_bucket(
                    execution_time,
                    (SELECT minimum_time FROM overall),
                    (SELECT maximum_time FROM overall), 19
                ) - 1 END AS bin_index
    FROM selected
), bin_stats AS (
    SELECT bin_index, count(*)::bigint AS case_count,
           min(execution_time)::double precision AS minimum_time,
           max(execution_time)::double precision AS maximum_time,
           avg(execution_time)::double precision AS average_time,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY execution_time)
               ::double precision AS median_time,
           stddev(execution_time)::double precision AS standard_deviation
    FROM binned
    GROUP BY bin_index
)
SELECT jsonb_build_object(
    'overall', (SELECT jsonb_build_array(
        total_cases,
        round(minimum_time::numeric, 6)::double precision,
        round(maximum_time::numeric, 6)::double precision,
        round(average_time::numeric, 6)::double precision,
        round(median_time::numeric, 6)::double precision,
        CASE WHEN standard_deviation IS NULL THEN NULL
             ELSE round(standard_deviation::numeric, 6)::double precision END
    ) FROM overall),
    'bins', (SELECT coalesce(jsonb_agg(jsonb_build_array(
        bin_index, case_count,
        round(minimum_time::numeric, 6)::double precision,
        round(maximum_time::numeric, 6)::double precision,
        round(average_time::numeric, 6)::double precision,
        round(median_time::numeric, 6)::double precision,
        CASE WHEN standard_deviation IS NULL THEN NULL
             ELSE round(standard_deviation::numeric, 6)::double precision END
    ) ORDER BY bin_index), '[]'::jsonb) FROM bin_stats)
)
"""
)


EDGE_INFO_SQL = """
WITH cases AS MATERIALIZED (
    SELECT * FROM ocpm.case_window(
        %(dataset_id)s, %(tenant_id)s, %(backbone_type)s,
        %(from_date)s, %(to_date)s, %(status)s
    )
), top_path AS (
    SELECT path_text, count(*) AS frequency, avg(execution_time) AS average_time
    FROM cases
    GROUP BY path_text
    ORDER BY frequency DESC, average_time
    LIMIT 1
), selected AS MATERIALIZED (
    SELECT cases.* FROM cases JOIN top_path USING (path_text)
), selected_ids AS MATERIALIZED (
    SELECT array_agg(case_id ORDER BY case_id) AS case_ids
    FROM selected
), ids AS MATERIALIZED (
    SELECT DISTINCT expanded.object_id
    FROM ocpm.case_bucket bucket
    CROSS JOIN selected_ids
    CROSS JOIN LATERAL ocpm.adjacency_selected_id_rows(
        bucket.case_ids,
        bucket.link_payloads,
        selected_ids.case_ids,
        %(from_date)s,
        %(to_date)s
    ) AS expanded(object_id)
    WHERE bucket.dataset_id = %(dataset_id)s
      AND bucket.tenant_id = %(tenant_id)s
      AND bucket.object_type = %(backbone_type)s
      AND bucket.min_start_time <= %(to_date)s
      AND bucket.max_end_time >= %(from_date)s
), durations AS MATERIALIZED (
    SELECT item.execution_time
    FROM ocpm.edge_bucket bucket
    CROSS JOIN LATERAL unnest(
        bucket.source_object_ids,
        bucket.target_object_ids,
        bucket.source_timestamps,
        bucket.target_timestamps,
        bucket.execution_times
    ) AS item(
        source_object_id, target_object_id,
        source_timestamp, target_timestamp, execution_time
    )
    JOIN ids source_ids ON source_ids.object_id = item.source_object_id
    JOIN ids target_ids ON target_ids.object_id = item.target_object_id
    WHERE bucket.dataset_id = %(dataset_id)s
      AND bucket.tenant_id = %(tenant_id)s
      AND bucket.source_activity = %(source_activity)s
      AND bucket.target_activity = %(target_activity)s
      AND (%(edge_context)s IS NULL OR bucket.context = %(edge_context)s)
      AND bucket.min_source_timestamp <= %(to_date)s
      AND bucket.max_source_timestamp >= %(from_date)s
      AND item.source_timestamp >= %(from_date)s
      AND item.target_timestamp <= %(to_date)s
), min_max AS MATERIALIZED (
    SELECT min(execution_time) AS min_value,
           max(execution_time) AS max_value
    FROM durations
), binned AS (
    SELECT execution_time,
           CASE WHEN min_value < max_value
                THEN width_bucket(execution_time, min_value, max_value, 19)
                ELSE 1 END AS bucket
    FROM durations, min_max
), bin_stats AS (
    SELECT bucket, count(*)::bigint AS bin_count,
           min(execution_time)::double precision AS min_value,
           max(execution_time)::double precision AS max_value,
           avg(execution_time)::double precision AS mean_value,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY execution_time)
               ::double precision AS median_value,
           stddev(execution_time)::double precision AS std_dev
    FROM binned
    GROUP BY bucket
)
SELECT coalesce(jsonb_agg(jsonb_build_array(
    bucket, bin_count,
    round((bin_count * 100.0 / nullif((SELECT sum(bin_count)
          FROM bin_stats), 0))::numeric, 2)::double precision,
    round(min_value::numeric, 6)::double precision,
    round(max_value::numeric, 6)::double precision,
    round(mean_value::numeric, 6)::double precision,
    round(median_value::numeric, 6)::double precision,
    CASE WHEN std_dev IS NULL THEN NULL
         ELSE round(std_dev::numeric, 6)::double precision END
) ORDER BY bucket), '[]'::jsonb)
FROM bin_stats
"""


CASE_LIST_SQL = """
WITH cases AS MATERIALIZED (
    SELECT * FROM ocpm.case_window(
        %(dataset_id)s, %(tenant_id)s, %(backbone_type)s,
        %(from_date)s, %(to_date)s, %(status)s
    )
), top_path AS (
    SELECT path_text, count(*) AS frequency, avg(execution_time) AS average_time
    FROM cases
    GROUP BY path_text
    ORDER BY frequency DESC, average_time
    LIMIT 1
), selected AS MATERIALIZED (
    SELECT cases.* FROM cases JOIN top_path USING (path_text)
), page AS MATERIALIZED (
    SELECT * FROM selected
    ORDER BY start_time DESC, case_id
    LIMIT %(limit)s OFFSET %(offset)s
), case_objects_raw AS MATERIALIZED (
    SELECT * FROM ocpm.connected_objects_one_hop(
        %(dataset_id)s, %(tenant_id)s, %(backbone_type)s,
        %(from_date)s, %(to_date)s,
        ARRAY(SELECT case_id FROM page)
    )
), case_objects AS MATERIALIZED (
    SELECT DISTINCT ON (backbone_case_id, object_id, object_type)
           backbone_case_id AS case_id,
           object_id, object_type, depth, visited_ids
    FROM case_objects_raw
    ORDER BY backbone_case_id, object_id, object_type, depth
), object_ids AS MATERIALIZED (
    SELECT array_agg(DISTINCT object_id ORDER BY object_id) AS ids
    FROM case_objects
), case_activities AS MATERIALIZED (
    SELECT objects.case_id,
           jsonb_agg(jsonb_build_array(
               event.object_id,
               locator.object_type,
               event.activity,
               (extract(epoch FROM event.event_timestamp) * 1000000)::bigint,
               nullif(event.context, ''),
               nullif(event.updated_by, '')
           ) ORDER BY event.event_timestamp, event.activity, event.object_id)
               AS activities
    FROM case_objects objects
    JOIN ocpm.event_locator locator
      ON locator.dataset_id = %(dataset_id)s
     AND locator.tenant_id = %(tenant_id)s
     AND locator.object_id = objects.object_id
     AND locator.object_type = objects.object_type
    JOIN ocpm.event_chunk chunk
      ON chunk.dataset_id = locator.dataset_id
     AND chunk.tenant_id = locator.tenant_id
     AND chunk.object_type = locator.object_type
     AND chunk.chunk_id = locator.chunk_id
    CROSS JOIN LATERAL unnest(
        chunk.object_ids[locator.start_offset:locator.end_offset],
        chunk.activities[locator.start_offset:locator.end_offset],
        chunk.event_timestamps[locator.start_offset:locator.end_offset],
        chunk.contexts[locator.start_offset:locator.end_offset],
        chunk.updated_bys[locator.start_offset:locator.end_offset]
    ) AS event(
        object_id, activity, event_timestamp, context, updated_by
    )
    WHERE event.object_id = objects.object_id
      AND event.event_timestamp >= %(from_date)s
      AND event.event_timestamp <= %(to_date)s
    GROUP BY objects.case_id
), candidate_edges AS MATERIALIZED (
    SELECT bucket.source_activity AS source,
           item.source_object_id,
           bucket.target_activity AS target,
           item.target_object_id,
           nullif(bucket.context, '') AS context,
           nullif(item.updated_by, '') AS updated_by,
           item.target_timestamp AS edge_timestamp
    FROM ocpm.edge_bucket bucket
    CROSS JOIN object_ids
    CROSS JOIN LATERAL unnest(
        bucket.source_object_ids,
        bucket.target_object_ids,
        bucket.source_timestamps,
        bucket.target_timestamps,
        bucket.updated_bys
    ) AS item(
        source_object_id, target_object_id,
        source_timestamp, target_timestamp, updated_by
    )
    WHERE bucket.dataset_id = %(dataset_id)s
      AND bucket.tenant_id = %(tenant_id)s
      AND bucket.min_source_timestamp <= %(to_date)s
      AND bucket.max_target_timestamp >= %(from_date)s
      AND bucket.source_object_ids && object_ids.ids
      AND bucket.target_object_ids && object_ids.ids
      AND item.source_object_id = ANY(object_ids.ids)
      AND item.target_object_id = ANY(object_ids.ids)
      AND item.source_timestamp >= %(from_date)s
      AND item.target_timestamp <= %(to_date)s
), case_edges AS MATERIALIZED (
    SELECT objects.case_id,
           jsonb_agg(jsonb_build_array(
               edge.source, edge.target, edge.context, edge.updated_by,
               (extract(epoch FROM edge.edge_timestamp) * 1000000)::bigint
           ) ORDER BY edge.edge_timestamp, edge.source, edge.target,
                      edge.context NULLS FIRST, edge.updated_by NULLS FIRST)
               AS edges
    FROM case_objects objects
    JOIN candidate_edges edge
      ON edge.source_object_id = objects.object_id
    GROUP BY objects.case_id
), grouped_objects AS MATERIALIZED (
    SELECT case_id,
           jsonb_agg(jsonb_build_array(
               object_id, object_type, depth, visited_ids
           ) ORDER BY object_id, object_type, depth) AS objects
    FROM case_objects
    GROUP BY case_id
)
SELECT jsonb_build_object(
    'total_cases', (SELECT count(*)::bigint FROM selected),
    'cases', coalesce(jsonb_agg(jsonb_build_array(
        page.case_id,
        round(page.execution_time::numeric, 6)::double precision,
        page.activity_count,
        activities.activities,
        edges.edges,
        objects.objects
    ) ORDER BY page.start_time DESC, page.case_id), '[]'::jsonb)
)
FROM page
JOIN case_activities activities USING (case_id)
JOIN case_edges edges USING (case_id)
JOIN grouped_objects objects USING (case_id)
"""


ENTIRE_PROCESS_MAP_SQL = """
WITH edges AS MATERIALIZED (
    SELECT *
    FROM ocpm.edge_summary
    WHERE dataset_id = %(dataset_id)s
      AND tenant_id = %(tenant_id)s
), nodes AS (
    SELECT target_activity AS activity,
           target_object_type AS object_type,
           sum(case_count)::bigint AS case_count
    FROM edges
    GROUP BY target_activity, target_object_type
    UNION
    SELECT source_activity, source_object_type, sum(case_count)::bigint
    FROM edges
    WHERE source_activity NOT IN (
        SELECT DISTINCT target_activity FROM edges
    )
    GROUP BY source_activity, source_object_type
), timeline AS (
    SELECT
        (extract(epoch FROM date_trunc(
            %(timeline_period)s, start_day::timestamptz
        )) * 1000000)::bigint AS bucket_us,
        sum(case_count)::bigint AS case_count
    FROM ocpm.case_start_day_rollup
    WHERE dataset_id = %(dataset_id)s
      AND tenant_id = %(tenant_id)s
    GROUP BY 1
)
SELECT jsonb_build_object(
    'nodes', (SELECT coalesce(jsonb_agg(jsonb_build_array(
        activity, object_type, case_count
    ) ORDER BY activity COLLATE "C", object_type COLLATE "C"), '[]'::jsonb)
    FROM nodes),
    'edges', (SELECT coalesce(jsonb_agg(jsonb_build_array(
        source_activity, source_object_type,
        target_activity, target_object_type,
        case_count, frequency,
        round(average_execution_time::numeric, 6)::double precision,
        round(minimum_execution_time::numeric, 6)::double precision,
        round(maximum_execution_time::numeric, 6)::double precision,
        round(median_execution_time::numeric, 6)::double precision,
        CASE WHEN standard_deviation IS NULL THEN NULL
             ELSE round(standard_deviation::numeric, 6)::double precision END
    ) ORDER BY source_activity COLLATE "C", source_object_type COLLATE "C",
               target_activity COLLATE "C", target_object_type COLLATE "C"),
        '[]'::jsonb)
    FROM edges),
    'timeline', (SELECT coalesce(jsonb_agg(jsonb_build_array(
        bucket_us, case_count
    ) ORDER BY bucket_us), '[]'::jsonb) FROM timeline)
)
"""


_PROCESS_MAP_RESULT_SQL = """,
activities AS MATERIALIZED (
    SELECT DISTINCT activity.value AS activity
    FROM selected
    CROSS JOIN LATERAL jsonb_array_elements(activity_path) AS path(grouped)
    CROSS JOIN LATERAL jsonb_array_elements_text(grouped) AS activity(value)
), native AS MATERIALIZED (
    SELECT ocpm.process_map_summary(
        bucket.source_activity,
        bucket.source_object_type,
        bucket.target_activity,
        bucket.target_object_type,
        bucket.context,
        bucket.case_ids,
        bucket.source_object_ids,
        bucket.target_object_ids,
        bucket.source_timestamps,
        bucket.target_timestamps,
        bucket.execution_times,
        bucket.updated_bys,
        bucket.source_contexts,
        bucket.source_updated_bys,
        %(from_date)s,
        %(to_date)s,
        ARRAY(SELECT object_id FROM all_ids),
        ARRAY(SELECT object_id FROM filter_ids),
        ARRAY(SELECT case_id FROM selected),
        ARRAY(SELECT activity FROM activities),
        %(backbone_type)s
    ) AS result
    FROM ocpm.edge_bucket bucket
    WHERE bucket.dataset_id = %(dataset_id)s
      AND bucket.tenant_id = %(tenant_id)s
      AND bucket.min_source_timestamp <= %(to_date)s
      AND bucket.max_source_timestamp >= %(from_date)s
), timeline AS (
    SELECT
        (extract(epoch FROM date_trunc(%(timeline_period)s, start_time))
            * 1000000)::bigint AS bucket_us,
        count(*)::bigint AS case_count
    FROM selected
    GROUP BY 1
)
SELECT jsonb_build_object(
    'event_stats', jsonb_build_array(
        (SELECT count(*)::bigint FROM cases),
        (SELECT count(DISTINCT activity.value)::bigint
         FROM cases
         CROSS JOIN LATERAL jsonb_array_elements(activity_path) AS path(grouped)
         CROSS JOIN LATERAL jsonb_array_elements_text(grouped) AS activity(value)),
        (SELECT count(*)::bigint FROM selected),
        (SELECT count(*)::bigint FROM activities)
    ),
    'nodes', (SELECT result->'nodes' FROM native),
    'nodes_by_context', (SELECT result->'nodes_by_context' FROM native),
    'nodes_by_user', (SELECT result->'nodes_by_user' FROM native),
    'edges', (SELECT result->'edges' FROM native),
    'timeline', (SELECT coalesce(jsonb_agg(jsonb_build_array(
        bucket_us, case_count
    ) ORDER BY bucket_us), '[]'::jsonb) FROM timeline)
)
"""


def process_map_sql(*, filtered_network: bool, transitive_closure: bool) -> str:
    """Return the native process-map plan for the requested traversal scope."""

    seed_filter = """
      AND (%(case_min_execution)s IS NULL
           OR candidate.execution_time BETWEEN %(case_min_execution)s
                                           AND %(case_max_execution)s)
      AND (%(backbone_activities)s IS NULL OR EXISTS (
          SELECT 1
          FROM jsonb_array_elements(candidate.activity_path) AS path(grouped)
          CROSS JOIN LATERAL jsonb_array_elements_text(grouped) AS activity(value)
          WHERE activity.value = ANY(%(backbone_activities)s)
      ))
    """
    traversal = (
        "ocpm.connected_objects_closure"
        if transitive_closure
        else "ocpm.connected_objects_one_hop"
    )
    prefix = f"""
WITH RECURSIVE cases AS MATERIALIZED (
    SELECT * FROM ocpm.case_window(
        %(dataset_id)s, %(tenant_id)s, %(backbone_type)s,
        %(from_date)s, %(to_date)s, %(status)s
    )
), eligible AS MATERIALIZED (
    SELECT * FROM cases
    WHERE (%(variant_hashes)s IS NULL OR path_hash = ANY(%(variant_hashes)s))
), top_path AS (
    SELECT path_text, count(*) AS frequency, avg(execution_time) AS average_time
    FROM eligible
    GROUP BY path_text
    ORDER BY frequency DESC, average_time
    LIMIT 1
), seed AS MATERIALIZED (
    SELECT candidate.*
    FROM eligible candidate
    JOIN top_path USING (path_text)
    WHERE true
{seed_filter}
), connected AS MATERIALIZED (
    SELECT * FROM {traversal}(
        %(dataset_id)s, %(tenant_id)s, %(backbone_type)s,
        %(from_date)s, %(to_date)s,
        ARRAY(SELECT case_id FROM seed)
    )
), all_ids AS MATERIALIZED (
    SELECT DISTINCT object_id FROM connected
)
"""
    if not filtered_network:
        return (
            prefix
            + """,
selected AS MATERIALIZED (SELECT * FROM seed),
filter_ids AS MATERIALIZED (SELECT * FROM all_ids)
"""
            + _PROCESS_MAP_RESULT_SQL
        )

    return (
        prefix
        + """,
matching_edges AS MATERIALIZED (
    SELECT item.source_object_id, item.target_object_id
    FROM ocpm.edge_bucket bucket
    CROSS JOIN LATERAL unnest(
        bucket.source_object_ids,
        bucket.target_object_ids,
        bucket.source_timestamps,
        bucket.target_timestamps,
        bucket.execution_times
    ) AS item(
        source_object_id, target_object_id,
        source_timestamp, target_timestamp, execution_time
    )
    JOIN all_ids source_ids ON source_ids.object_id = item.source_object_id
    JOIN all_ids target_ids ON target_ids.object_id = item.target_object_id
    WHERE bucket.dataset_id = %(dataset_id)s
      AND bucket.tenant_id = %(tenant_id)s
      AND bucket.min_source_timestamp <= %(to_date)s
      AND bucket.max_source_timestamp >= %(from_date)s
      AND item.source_timestamp >= %(from_date)s
      AND item.target_timestamp <= %(to_date)s
      AND (
          (%(connected_activities)s IS NOT NULL AND (
              bucket.source_activity = ANY(%(connected_activities)s)
              OR bucket.target_activity = ANY(%(connected_activities)s)
          ))
          OR (%(included_edge_source)s IS NOT NULL
              AND bucket.source_activity = %(included_edge_source)s
              AND bucket.target_activity = %(included_edge_target)s
              AND item.execution_time BETWEEN %(included_edge_min)s
                                          AND %(included_edge_max)s)
      )
), matched_cases AS MATERIALIZED (
    SELECT connected.backbone_case_id AS case_id
    FROM connected
    JOIN matching_edges
      ON matching_edges.source_object_id = connected.object_id
    UNION
    SELECT connected.backbone_case_id
    FROM connected
    JOIN matching_edges
      ON matching_edges.target_object_id = connected.object_id
), selected AS MATERIALIZED (
    SELECT seed.* FROM seed JOIN matched_cases USING (case_id)
), filter_ids AS MATERIALIZED (
    SELECT DISTINCT connected.object_id
    FROM connected
    JOIN matched_cases ON matched_cases.case_id = connected.backbone_case_id
)
"""
        + _PROCESS_MAP_RESULT_SQL
    )
