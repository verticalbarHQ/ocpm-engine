"""Three-way Goodr benchmark for common process-mining operations.

The suite compares index-light PostgreSQL, the current Vertical Bar relational
index layout, and pg_ocpm. Every operation returns deterministic JSON and must
pass an exact three-way correctness gate before its latency is reported.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import statistics
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

REL_CASES = """
timestamp_groups AS MATERIALIZED (
    SELECT
        case_id,
        min(object_type) AS object_type,
        "timestamp",
        array_agg(activity_id ORDER BY activity_id) AS activities
    FROM mv_ocel_event_log
    WHERE tenant_id = %(tenant_id)s
    GROUP BY case_id, "timestamp"
), cases AS MATERIALIZED (
    SELECT
        case_id,
        min(object_type) AS object_type,
        min("timestamp") AS start_time,
        max("timestamp") AS end_time,
        extract(epoch FROM (max("timestamp") - min("timestamp")))::float8
            AS execution_time,
        jsonb_agg(to_jsonb(activities) ORDER BY "timestamp") AS activity_path,
        substring(
            md5(json_agg(activities ORDER BY "timestamp")::text), 1, 10
        ) AS path_hash
    FROM timestamp_groups
    GROUP BY case_id
)
"""


OCPM_CASES = """
cases AS MATERIALIZED (
    SELECT
        item.case_id,
        bucket.object_type,
        item.start_time,
        item.end_time,
        item.execution_time,
        bucket.activity_path,
        bucket.path_hash
    FROM ocpm.case_bucket bucket
    CROSS JOIN LATERAL unnest(
        bucket.case_ids,
        bucket.start_times,
        bucket.end_times,
        bucket.execution_times
    ) AS item(case_id, start_time, end_time, execution_time)
    WHERE bucket.dataset_id = %(dataset_id)s
      AND bucket.tenant_id = %(tenant_id)s
)
"""


REL_PAIR_COUNTS = """
pair_counts AS (
    SELECT
        source,
        target,
        edge_type,
        count(*) FILTER (
            WHERE source_timestamp >= %(from_ts)s
              AND target_timestamp <= %(split_ts)s
        )::bigint AS train_count,
        count(*) FILTER (
            WHERE source_timestamp >= %(split_ts)s
              AND target_timestamp <= %(to_ts)s
        )::bigint AS test_count
    FROM mv_ocel_process_map_edge
    WHERE tenant_id = %(tenant_id)s
    GROUP BY source, target, edge_type
)
"""


OCPM_PAIR_COUNTS = """
pair_counts AS (
    SELECT
        source_activity AS source,
        target_activity AS target,
        edge_type,
        sum(ocpm.window_cardinality(
            source_timestamps,
            target_timestamps,
            %(from_ts)s,
            %(split_ts)s
        ))::bigint AS train_count,
        sum(ocpm.window_cardinality(
            source_timestamps,
            target_timestamps,
            %(split_ts)s,
            %(to_ts)s
        ))::bigint AS test_count
    FROM ocpm.edge_bucket
    WHERE dataset_id = %(dataset_id)s
      AND tenant_id = %(tenant_id)s
      AND min_source_timestamp <= %(to_ts)s
      AND max_target_timestamp >= %(from_ts)s
    GROUP BY source_activity, target_activity, edge_type
)
"""


REL_BOTTLENECK = """
WITH pair_stats AS (
    SELECT
        source,
        target,
        edge_type,
        count(*)::bigint AS frequency,
        avg(execution_time)::float8 AS mean_duration,
        min(execution_time)::float8 AS minimum_duration,
        max(execution_time)::float8 AS maximum_duration,
        percentile_cont(0.5) WITHIN GROUP (
            ORDER BY execution_time
        )::float8 AS median_duration,
        stddev_samp(execution_time)::float8 AS standard_deviation
    FROM mv_ocel_process_map_edge
    WHERE tenant_id = %(tenant_id)s
      AND source_timestamp >= %(from_ts)s
      AND target_timestamp <= %(to_ts)s
    GROUP BY source, target, edge_type
    HAVING count(*) >= %(minimum_frequency)s
), ranked AS (
    SELECT *
    FROM pair_stats
    ORDER BY mean_duration DESC, frequency DESC, source, target, edge_type
    LIMIT %(limit)s
)
SELECT coalesce(jsonb_agg(jsonb_build_object(
    'source', source,
    'target', target,
    'edge_type', edge_type,
    'frequency', frequency,
    'mean_duration', round(mean_duration::numeric, 6),
    'minimum_duration', round(minimum_duration::numeric, 6),
    'maximum_duration', round(maximum_duration::numeric, 6),
    'median_duration', round(median_duration::numeric, 6),
    'standard_deviation', round(standard_deviation::numeric, 6)
) ORDER BY mean_duration DESC, frequency DESC, source, target, edge_type),
'[]'::jsonb)
FROM ranked
"""


OCPM_BOTTLENECK = """
WITH pair_stats AS (
    SELECT
        source_activity AS source,
        target_activity AS target,
        edge_type,
        ocpm.duration_stats_window(
            source_timestamps,
            target_timestamps,
            execution_times,
            %(from_ts)s,
            %(to_ts)s
        ) AS stats
    FROM ocpm.edge_bucket
    WHERE dataset_id = %(dataset_id)s
      AND tenant_id = %(tenant_id)s
      AND min_source_timestamp <= %(to_ts)s
      AND max_target_timestamp >= %(from_ts)s
    GROUP BY source_activity, target_activity, edge_type
), ranked AS (
    SELECT
        source,
        target,
        edge_type,
        stats[1]::bigint AS frequency,
        stats[2] AS mean_duration,
        stats[3] AS minimum_duration,
        stats[4] AS maximum_duration,
        stats[5] AS median_duration,
        stats[6] AS standard_deviation
    FROM pair_stats
    WHERE stats[1] >= %(minimum_frequency)s
    ORDER BY mean_duration DESC, frequency DESC, source, target, edge_type
    LIMIT %(limit)s
)
SELECT coalesce(jsonb_agg(jsonb_build_object(
    'source', source,
    'target', target,
    'edge_type', edge_type,
    'frequency', frequency,
    'mean_duration', round(mean_duration::numeric, 6),
    'minimum_duration', round(minimum_duration::numeric, 6),
    'maximum_duration', round(maximum_duration::numeric, 6),
    'median_duration', round(median_duration::numeric, 6),
    'standard_deviation', round(standard_deviation::numeric, 6)
) ORDER BY mean_duration DESC, frequency DESC, source, target, edge_type),
'[]'::jsonb)
FROM ranked
"""


REL_BOTTLENECK_DRIFT = """
WITH pair_stats AS (
    SELECT source, target, edge_type, count(*)::bigint AS frequency,
           avg(execution_time)::float8 AS mean_duration
    FROM mv_ocel_process_map_edge
    WHERE tenant_id = %(tenant_id)s
      AND source_timestamp >= %(from_ts)s
      AND target_timestamp <= %(to_ts)s
    GROUP BY source, target, edge_type
    HAVING count(*) >= %(minimum_frequency)s
), top_pair AS (
    SELECT source, target, edge_type
    FROM pair_stats
    ORDER BY mean_duration DESC, frequency DESC, source, target, edge_type
    LIMIT 1
), monthly AS (
    SELECT
        date_trunc('month', edge.source_timestamp) AS month,
        count(*)::bigint AS frequency,
        avg(edge.execution_time)::float8 AS mean_duration,
        percentile_cont(0.5) WITHIN GROUP (
            ORDER BY edge.execution_time
        )::float8 AS median_duration
    FROM mv_ocel_process_map_edge edge
    JOIN top_pair pair USING (source, target, edge_type)
    WHERE edge.tenant_id = %(tenant_id)s
      AND edge.source_timestamp >= %(from_ts)s
      AND edge.target_timestamp <= %(to_ts)s
    GROUP BY date_trunc('month', edge.source_timestamp)
)
SELECT jsonb_build_object(
    'source', (SELECT source FROM top_pair),
    'target', (SELECT target FROM top_pair),
    'edge_type', (SELECT edge_type FROM top_pair),
    'months', coalesce((SELECT jsonb_agg(jsonb_build_object(
        'month', to_char(month, 'YYYY-MM'),
        'frequency', frequency,
        'mean_duration', round(mean_duration::numeric, 6),
        'median_duration', round(median_duration::numeric, 6)
    ) ORDER BY month) FROM monthly), '[]'::jsonb)
)
"""


OCPM_BOTTLENECK_DRIFT = """
WITH pair_stats AS (
    SELECT
        source_activity AS source,
        target_activity AS target,
        edge_type,
        ocpm.duration_stats_window(
            source_timestamps,
            target_timestamps,
            execution_times,
            %(from_ts)s,
            %(to_ts)s
        ) AS stats
    FROM ocpm.edge_bucket
    WHERE dataset_id = %(dataset_id)s
      AND tenant_id = %(tenant_id)s
      AND min_source_timestamp <= %(to_ts)s
      AND max_target_timestamp >= %(from_ts)s
    GROUP BY source_activity, target_activity, edge_type
), top_pair AS (
    SELECT source, target, edge_type
    FROM pair_stats
    WHERE stats[1] >= %(minimum_frequency)s
    ORDER BY stats[2] DESC, stats[1] DESC, source, target, edge_type
    LIMIT 1
), expanded AS (
    SELECT
        date_trunc('month', item.source_timestamp) AS month,
        item.execution_time
    FROM ocpm.edge_bucket bucket
    JOIN top_pair pair
      ON pair.source = bucket.source_activity
     AND pair.target = bucket.target_activity
     AND pair.edge_type = bucket.edge_type
    CROSS JOIN LATERAL unnest(
        bucket.source_timestamps,
        bucket.target_timestamps,
        bucket.execution_times
    ) AS item(source_timestamp, target_timestamp, execution_time)
    WHERE bucket.dataset_id = %(dataset_id)s
      AND bucket.tenant_id = %(tenant_id)s
      AND item.source_timestamp >= %(from_ts)s
      AND item.target_timestamp <= %(to_ts)s
), monthly AS (
    SELECT
        month,
        count(*)::bigint AS frequency,
        avg(execution_time)::float8 AS mean_duration,
        percentile_cont(0.5) WITHIN GROUP (
            ORDER BY execution_time
        )::float8 AS median_duration
    FROM expanded
    GROUP BY month
)
SELECT jsonb_build_object(
    'source', (SELECT source FROM top_pair),
    'target', (SELECT target FROM top_pair),
    'edge_type', (SELECT edge_type FROM top_pair),
    'months', coalesce((SELECT jsonb_agg(jsonb_build_object(
        'month', to_char(month, 'YYYY-MM'),
        'frequency', frequency,
        'mean_duration', round(mean_duration::numeric, 6),
        'median_duration', round(median_duration::numeric, 6)
    ) ORDER BY month) FROM monthly), '[]'::jsonb)
)
"""


REL_REWORK = """
WITH repeated AS (
    SELECT
        case_id,
        source,
        target,
        edge_type,
        count(*)::bigint AS occurrences
    FROM mv_ocel_process_map_edge
    WHERE tenant_id = %(tenant_id)s
      AND source_timestamp >= %(from_ts)s
      AND target_timestamp <= %(to_ts)s
    GROUP BY case_id, source, target, edge_type
    HAVING count(*) > 1
), patterns AS (
    SELECT
        source,
        target,
        edge_type,
        count(*)::bigint AS rework_cases,
        sum(occurrences - 1)::bigint AS excess_transitions,
        max(occurrences)::bigint AS maximum_occurrences
    FROM repeated
    GROUP BY source, target, edge_type
    ORDER BY excess_transitions DESC, rework_cases DESC,
             source, target, edge_type
    LIMIT %(limit)s
)
SELECT jsonb_build_object(
    'rework_cases', (SELECT count(DISTINCT case_id)::bigint FROM repeated),
    'excess_transitions', (
        SELECT coalesce(sum(occurrences - 1), 0)::bigint FROM repeated
    ),
    'patterns', coalesce((SELECT jsonb_agg(jsonb_build_object(
        'source', source,
        'target', target,
        'edge_type', edge_type,
        'rework_cases', rework_cases,
        'excess_transitions', excess_transitions,
        'maximum_occurrences', maximum_occurrences
    ) ORDER BY excess_transitions DESC, rework_cases DESC,
               source, target, edge_type) FROM patterns), '[]'::jsonb)
)
"""


OCPM_REWORK = """
WITH expanded AS (
    SELECT
        item.case_id,
        bucket.source_activity AS source,
        bucket.target_activity AS target,
        bucket.edge_type
    FROM ocpm.edge_bucket bucket
    CROSS JOIN LATERAL unnest(
        bucket.case_ids,
        bucket.source_timestamps,
        bucket.target_timestamps
    ) AS item(case_id, source_timestamp, target_timestamp)
    WHERE bucket.dataset_id = %(dataset_id)s
      AND bucket.tenant_id = %(tenant_id)s
      AND item.source_timestamp >= %(from_ts)s
      AND item.target_timestamp <= %(to_ts)s
), repeated AS (
    SELECT case_id, source, target, edge_type, count(*)::bigint AS occurrences
    FROM expanded
    GROUP BY case_id, source, target, edge_type
    HAVING count(*) > 1
), patterns AS (
    SELECT
        source,
        target,
        edge_type,
        count(*)::bigint AS rework_cases,
        sum(occurrences - 1)::bigint AS excess_transitions,
        max(occurrences)::bigint AS maximum_occurrences
    FROM repeated
    GROUP BY source, target, edge_type
    ORDER BY excess_transitions DESC, rework_cases DESC,
             source, target, edge_type
    LIMIT %(limit)s
)
SELECT jsonb_build_object(
    'rework_cases', (SELECT count(DISTINCT case_id)::bigint FROM repeated),
    'excess_transitions', (
        SELECT coalesce(sum(occurrences - 1), 0)::bigint FROM repeated
    ),
    'patterns', coalesce((SELECT jsonb_agg(jsonb_build_object(
        'source', source,
        'target', target,
        'edge_type', edge_type,
        'rework_cases', rework_cases,
        'excess_transitions', excess_transitions,
        'maximum_occurrences', maximum_occurrences
    ) ORDER BY excess_transitions DESC, rework_cases DESC,
               source, target, edge_type) FROM patterns), '[]'::jsonb)
)
"""


def sla_sql(case_ctes: str) -> str:
    return f"""
WITH {case_ctes},
thresholds AS (
    SELECT
        object_type,
        percentile_cont(0.9) WITHIN GROUP (
            ORDER BY execution_time
        )::float8 AS threshold,
        count(*)::bigint AS train_cases
    FROM cases
    WHERE start_time >= %(from_ts)s
      AND start_time < %(split_ts)s
    GROUP BY object_type
    HAVING count(*) >= %(minimum_cases)s
), scored AS (
    SELECT
        cases.object_type,
        thresholds.train_cases,
        thresholds.threshold,
        cases.execution_time,
        cases.execution_time > thresholds.threshold AS breached
    FROM cases
    JOIN thresholds USING (object_type)
    WHERE cases.start_time >= %(split_ts)s
      AND cases.start_time <= %(to_ts)s
), summary AS (
    SELECT
        object_type,
        min(train_cases)::bigint AS train_cases,
        count(*)::bigint AS test_cases,
        min(threshold)::float8 AS threshold,
        count(*) FILTER (WHERE breached)::bigint AS breach_cases,
        avg(execution_time) FILTER (WHERE breached)::float8 AS mean_breach_duration
    FROM scored
    GROUP BY object_type
    HAVING count(*) >= %(minimum_test_cases)s
)
SELECT coalesce(jsonb_agg(jsonb_build_object(
    'object_type', object_type,
    'train_cases', train_cases,
    'test_cases', test_cases,
    'threshold', round(threshold::numeric, 6),
    'breach_cases', breach_cases,
    'breach_rate', round((breach_cases::numeric / test_cases), 6),
    'mean_breach_duration', round(mean_breach_duration::numeric, 6)
) ORDER BY breach_cases::numeric / test_cases DESC,
           test_cases DESC, object_type), '[]'::jsonb)
FROM summary
"""


def dfg_conformance_sql(pair_counts: str) -> str:
    return f"""
WITH {pair_counts},
train_ranked AS (
    SELECT
        source,
        target,
        edge_type,
        train_count,
        sum(train_count) OVER (
            ORDER BY train_count DESC, source, target, edge_type
        ) AS cumulative_count,
        sum(train_count) OVER () AS total_count
    FROM pair_counts
    WHERE train_count > 0
), model AS (
    SELECT source, target, edge_type
    FROM train_ranked
    WHERE cumulative_count - train_count < total_count * %(coverage)s
), scored AS (
    SELECT
        pairs.*,
        model.source IS NOT NULL AS conforming
    FROM pair_counts pairs
    LEFT JOIN model USING (source, target, edge_type)
    WHERE pairs.test_count > 0
), deviations AS (
    SELECT source, target, edge_type, test_count
    FROM scored
    WHERE NOT conforming
    ORDER BY test_count DESC, source, target, edge_type
    LIMIT %(limit)s
)
SELECT jsonb_build_object(
    'model_edges', (SELECT count(*)::bigint FROM model),
    'test_edges', (SELECT coalesce(sum(test_count), 0)::bigint FROM scored),
    'conforming_edges', (
        SELECT coalesce(sum(test_count) FILTER (WHERE conforming), 0)::bigint
        FROM scored
    ),
    'deviation_edges', (
        SELECT coalesce(sum(test_count) FILTER (WHERE NOT conforming), 0)::bigint
        FROM scored
    ),
    'fitness', coalesce((
        SELECT round(
            (sum(test_count) FILTER (WHERE conforming))::numeric
            / nullif(sum(test_count), 0),
            6
        )
        FROM scored
    ), 0),
    'top_deviations', coalesce((SELECT jsonb_agg(jsonb_build_object(
        'source', source,
        'target', target,
        'edge_type', edge_type,
        'test_count', test_count
    ) ORDER BY test_count DESC, source, target, edge_type)
    FROM deviations), '[]'::jsonb)
)
"""


def variant_conformance_sql(case_ctes: str, ocpm: bool) -> str:
    if ocpm:
        counts = """
variant_counts AS (
    SELECT
        path_hash,
        sum(ocpm.window_cardinality(
            bucket.start_times,
            bucket.end_times,
            %(from_ts)s,
            %(split_ts)s
        ))::bigint AS train_count,
        sum(ocpm.window_cardinality(
            bucket.start_times,
            bucket.end_times,
            %(split_ts)s,
            %(to_ts)s
        ))::bigint AS test_count
    FROM ocpm.case_bucket bucket
    WHERE bucket.dataset_id = %(dataset_id)s
      AND bucket.tenant_id = %(tenant_id)s
      AND bucket.min_start_time <= %(to_ts)s
      AND bucket.max_end_time >= %(from_ts)s
    GROUP BY path_hash
)
"""
        prefix = counts
    else:
        prefix = f"""
{case_ctes},
variant_counts AS (
    SELECT
        path_hash,
        count(*) FILTER (
            WHERE start_time >= %(from_ts)s AND end_time <= %(split_ts)s
        )::bigint AS train_count,
        count(*) FILTER (
            WHERE start_time >= %(split_ts)s AND end_time <= %(to_ts)s
        )::bigint AS test_count
    FROM cases
    GROUP BY path_hash
)
"""
    return f"""
WITH {prefix},
train_ranked AS (
    SELECT
        path_hash,
        train_count,
        sum(train_count) OVER (
            ORDER BY train_count DESC, path_hash
        ) AS cumulative_count,
        sum(train_count) OVER () AS total_count
    FROM variant_counts
    WHERE train_count > 0
), model AS (
    SELECT path_hash
    FROM train_ranked
    WHERE cumulative_count - train_count < total_count * %(coverage)s
), scored AS (
    SELECT variants.*, model.path_hash IS NOT NULL AS conforming
    FROM variant_counts variants
    LEFT JOIN model USING (path_hash)
    WHERE variants.test_count > 0
), deviations AS (
    SELECT path_hash, test_count
    FROM scored
    WHERE NOT conforming
    ORDER BY test_count DESC, path_hash
    LIMIT %(limit)s
)
SELECT jsonb_build_object(
    'model_variants', (SELECT count(*)::bigint FROM model),
    'test_cases', (SELECT coalesce(sum(test_count), 0)::bigint FROM scored),
    'conforming_cases', (
        SELECT coalesce(sum(test_count) FILTER (WHERE conforming), 0)::bigint
        FROM scored
    ),
    'deviation_cases', (
        SELECT coalesce(sum(test_count) FILTER (WHERE NOT conforming), 0)::bigint
        FROM scored
    ),
    'fitness', coalesce((
        SELECT round(
            (sum(test_count) FILTER (WHERE conforming))::numeric
            / nullif(sum(test_count), 0),
            6
        )
        FROM scored
    ), 0),
    'top_deviations', coalesce((SELECT jsonb_agg(jsonb_build_object(
        'path_hash', path_hash,
        'test_count', test_count
    ) ORDER BY test_count DESC, path_hash) FROM deviations), '[]'::jsonb)
)
"""


def next_activity_sql(pair_counts: str) -> str:
    return f"""
WITH {pair_counts},
model AS (
    SELECT source, edge_type, target AS predicted_target, train_count
    FROM (
        SELECT
            source,
            target,
            edge_type,
            train_count,
            row_number() OVER (
                PARTITION BY source, edge_type
                ORDER BY train_count DESC, target
            ) AS rank
        FROM pair_counts
        WHERE train_count > 0
    ) ranked
    WHERE rank = 1
), scored AS (
    SELECT
        pairs.source,
        pairs.target,
        pairs.edge_type,
        pairs.test_count,
        model.predicted_target,
        pairs.target = model.predicted_target AS correct
    FROM pair_counts pairs
    JOIN model USING (source, edge_type)
    WHERE pairs.test_count > 0
), by_source AS (
    SELECT
        source,
        edge_type,
        min(predicted_target) AS predicted_target,
        sum(test_count)::bigint AS test_edges,
        sum(test_count) FILTER (WHERE correct)::bigint AS correct_edges
    FROM scored
    GROUP BY source, edge_type
), top_sources AS (
    SELECT *
    FROM by_source
    ORDER BY test_edges DESC, source, edge_type
    LIMIT %(limit)s
)
SELECT jsonb_build_object(
    'test_edges', (SELECT sum(test_edges)::bigint FROM by_source),
    'correct_edges', (SELECT sum(correct_edges)::bigint FROM by_source),
    'accuracy', (SELECT round(
        sum(correct_edges)::numeric / nullif(sum(test_edges), 0), 6
    ) FROM by_source),
    'sources', coalesce((SELECT jsonb_agg(jsonb_build_object(
        'source', source,
        'edge_type', edge_type,
        'predicted_target', predicted_target,
        'test_edges', test_edges,
        'correct_edges', correct_edges,
        'accuracy', round(correct_edges::numeric / test_edges, 6)
    ) ORDER BY test_edges DESC, source, edge_type) FROM top_sources), '[]'::jsonb)
)
"""


REL_PREDICTION_EDGES = """
edges AS MATERIALIZED (
    SELECT
        source,
        source_object_type,
        edge_type,
        source_timestamp,
        target_timestamp,
        execution_time
    FROM mv_ocel_process_map_edge
    WHERE tenant_id = %(tenant_id)s
      AND edge_type = 'link'
      AND source_timestamp >= %(from_ts)s
      AND target_timestamp <= %(to_ts)s
)
"""


OCPM_PREDICTION_EDGES = """
edges AS MATERIALIZED (
    SELECT
        bucket.source_activity AS source,
        bucket.source_object_type,
        bucket.edge_type,
        item.source_timestamp,
        item.target_timestamp,
        item.execution_time
    FROM ocpm.edge_bucket bucket
    CROSS JOIN LATERAL unnest(
        bucket.source_timestamps,
        bucket.target_timestamps,
        bucket.execution_times
    ) AS item(source_timestamp, target_timestamp, execution_time)
    WHERE bucket.dataset_id = %(dataset_id)s
      AND bucket.tenant_id = %(tenant_id)s
      AND bucket.edge_type = 'link'
      AND bucket.min_source_timestamp <= %(to_ts)s
      AND bucket.max_target_timestamp >= %(from_ts)s
      AND item.source_timestamp >= %(from_ts)s
      AND item.target_timestamp <= %(to_ts)s
)
"""


def edge_bottleneck_prediction_sql(edge_ctes: str) -> str:
    return f"""
WITH {edge_ctes},
threshold AS (
    SELECT percentile_cont(0.75) WITHIN GROUP (
        ORDER BY execution_time
    )::float8 AS value
    FROM edges
    WHERE source_timestamp < %(split_ts)s
      AND target_timestamp <= %(split_ts)s
), training AS (
    SELECT
        source,
        source_object_type,
        edge_type,
        count(*)::bigint AS train_edges,
        avg(execution_time)::float8 AS predicted_duration,
        avg((execution_time > threshold.value)::int)::float8 AS predicted_risk
    FROM edges
    CROSS JOIN threshold
    WHERE source_timestamp < %(split_ts)s
      AND target_timestamp <= %(split_ts)s
    GROUP BY source, source_object_type, edge_type
    HAVING count(*) >= %(minimum_prefix_cases)s
), scored AS (
    SELECT
        edges.execution_time,
        training.predicted_duration,
        training.predicted_risk,
        edges.execution_time > threshold.value AS actual_bottleneck,
        training.predicted_risk >= %(risk_cutoff)s AS predicted_bottleneck
    FROM edges
    JOIN training USING (source, source_object_type, edge_type)
    CROSS JOIN threshold
    WHERE edges.source_timestamp >= %(split_ts)s
      AND edges.target_timestamp <= %(to_ts)s
), totals AS (
    SELECT
        count(*)::bigint AS test_edges,
        avg(abs(predicted_duration - execution_time))::float8 AS mae,
        sqrt(avg(power(predicted_duration - execution_time, 2)))::float8 AS rmse,
        avg(power(predicted_risk - actual_bottleneck::int, 2))::float8
            AS brier_score,
        count(*) FILTER (WHERE actual_bottleneck)::bigint
            AS actual_bottlenecks,
        count(*) FILTER (WHERE predicted_bottleneck)::bigint
            AS predicted_bottlenecks,
        count(*) FILTER (
            WHERE actual_bottleneck AND predicted_bottleneck
        )::bigint AS true_positives
    FROM scored
)
SELECT jsonb_build_object(
    'test_edges', test_edges,
    'mae', round(mae::numeric, 6),
    'rmse', round(rmse::numeric, 6),
    'brier_score', round(brier_score::numeric, 6),
    'actual_bottlenecks', actual_bottlenecks,
    'predicted_bottlenecks', predicted_bottlenecks,
    'true_positives', true_positives,
    'precision', coalesce(round(
        true_positives::numeric / nullif(predicted_bottlenecks, 0), 6
    ), 0),
    'recall', coalesce(round(
        true_positives::numeric / nullif(actual_bottlenecks, 0), 6
    ), 0)
)
FROM totals
"""


SCENARIOS = (
    (
        "edge_bottleneck_ranking",
        "bottleneck_detection",
        REL_BOTTLENECK,
        OCPM_BOTTLENECK,
    ),
    (
        "bottleneck_monthly_drift",
        "bottleneck_detection",
        REL_BOTTLENECK_DRIFT,
        OCPM_BOTTLENECK_DRIFT,
    ),
    ("repeated_transition_rework", "rework", REL_REWORK, OCPM_REWORK),
    (
        "sla_breach_detection",
        "performance_compliance",
        sla_sql(REL_CASES),
        sla_sql(OCPM_CASES),
    ),
    (
        "dfg_conformance",
        "conformance",
        dfg_conformance_sql(REL_PAIR_COUNTS),
        dfg_conformance_sql(OCPM_PAIR_COUNTS),
    ),
    (
        "variant_conformance",
        "conformance",
        variant_conformance_sql(REL_CASES, False),
        variant_conformance_sql(OCPM_CASES, True),
    ),
    (
        "next_activity_prediction",
        "prediction",
        next_activity_sql(REL_PAIR_COUNTS),
        next_activity_sql(OCPM_PAIR_COUNTS),
    ),
    (
        "edge_bottleneck_prediction",
        "prediction",
        edge_bottleneck_prediction_sql(REL_PREDICTION_EDGES),
        edge_bottleneck_prediction_sql(OCPM_PREDICTION_EDGES),
    ),
)


class Pg:
    def __init__(self, host: str):
        self.host = host
        self.conn = psycopg2.connect(
            host=host,
            port=5432,
            dbname="dendrites",
            user="postgres",
            password="pg",
            connect_timeout=10,
        )
        self.conn.autocommit = True
        cursor = self.conn.cursor()
        cursor.execute("SET work_mem='1GB'")
        cursor.execute("SET jit=off")
        cursor.close()

    def run(self, sql: str, params: dict):
        cursor = self.conn.cursor()
        cursor.execute(sql, params)
        value = cursor.fetchone()[0]
        cursor.close()
        return value


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plain-host", default="postgres_plain_goodr")
    parser.add_argument("--verticalbar-host", default="postgres_vanilla_goodr")
    parser.add_argument("--ocpm-host", default="postgres_ocpm_goodr")
    parser.add_argument("--dataset", default="goodr_ocpm")
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--runs", type=int, default=9)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--seed", type=int, default=20260718)
    parser.add_argument("--scenario", action="append")
    parser.add_argument(
        "--output",
        default=str(
            Path(__file__).resolve().parents[1]
            / "docs/results/common-pm-goodr-three-way-2026-07-18.json"
        ),
    )
    return parser.parse_args()


def metrics(samples: list[float]) -> dict:
    ordered = sorted(samples)
    return {
        "status": "completed",
        "p50_ms": round(statistics.median(ordered), 3),
        "p95_ms": round(ordered[math.ceil(len(ordered) * 0.95) - 1], 3),
        "min_ms": round(min(ordered), 3),
        "max_ms": round(max(ordered), 3),
        "mean_ms": round(statistics.mean(ordered), 3),
        "runs": len(ordered),
    }


def timed(call):
    started = time.perf_counter()
    answer = call()
    return (time.perf_counter() - started) * 1000, answer


def geomean(values: list[float]) -> float:
    return math.exp(sum(math.log(value) for value in values) / len(values))


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def digest(value) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()[:16]


def configure(engine: Pg, timeout_seconds: int):
    cursor = engine.conn.cursor()
    cursor.execute("SET statement_timeout=%s", (timeout_seconds * 1000,))
    cursor.execute("SELECT pg_stat_reset()")
    cursor.close()


def fixture(plain: Pg, ocpm: Pg, dataset_name: str) -> dict:
    cursor = plain.conn.cursor()
    cursor.execute(
        """
        WITH cases AS (
            SELECT case_id, min("timestamp") AS start_time
            FROM mv_ocel_event_log
            WHERE tenant_id=1
            GROUP BY case_id
        )
        SELECT
            min(start_time) - interval '1 second',
            percentile_disc(0.8) WITHIN GROUP (ORDER BY start_time),
            max(start_time) + interval '2 hours',
            count(*)::bigint
        FROM cases
        """
    )
    from_ts, split_ts, to_ts, cases = cursor.fetchone()
    cursor.execute("SELECT count(*)::bigint FROM mv_ocel_event_log WHERE tenant_id=1")
    events = cursor.fetchone()[0]
    cursor.execute(
        "SELECT count(*)::bigint FROM mv_ocel_process_map_edge WHERE tenant_id=1"
    )
    edges = cursor.fetchone()[0]
    cursor.close()

    cursor = ocpm.conn.cursor()
    cursor.execute(
        "SELECT dataset_id,tenant_id FROM ocpm.dataset WHERE dataset_name=%s",
        (dataset_name,),
    )
    dataset_id, tenant_id = cursor.fetchone()
    cursor.close()
    return {
        "dataset_id": dataset_id,
        "tenant_id": tenant_id,
        "from_ts": from_ts,
        "split_ts": split_ts,
        "to_ts": to_ts,
        "events": events,
        "edges": edges,
        "cases": cases,
    }


def runtime(engine: Pg, include_ocpm: bool = False) -> dict:
    cursor = engine.conn.cursor()
    cursor.execute(
        """
        SELECT current_setting('server_version'),
               current_setting('shared_buffers'),
               current_setting('effective_cache_size'),
               current_setting('work_mem'),
               current_setting('max_parallel_workers_per_gather'),
               current_setting('random_page_cost'),
               current_setting('jit'),
               current_setting('shared_preload_libraries')
        """
    )
    keys = (
        "postgres_version",
        "shared_buffers",
        "effective_cache_size",
        "work_mem",
        "max_parallel_workers_per_gather",
        "random_page_cost",
        "jit",
        "shared_preload_libraries",
    )
    result = dict(zip(keys, cursor.fetchone()))
    if include_ocpm:
        cursor.execute("SELECT ocpm.version()")
        result["pg_ocpm_version"] = cursor.fetchone()[0]
    cursor.close()
    return result


def storage(engine: Pg, ocpm: bool) -> dict:
    cursor = engine.conn.cursor()
    try:
        cursor.execute("SELECT pg_stat_force_next_flush()")
    except psycopg2.errors.UndefinedFunction:
        pass
    if ocpm:
        predicate = "schemaname='ocpm' AND relname NOT IN ('dataset','result_cache')"
        values = ()
    else:
        predicate = """schemaname='public' AND relname = ANY(%s)"""
        values = (
            [
                "mv_ocel_event_log",
                "mv_ocel_process_map_edge",
                "netsuite_transaction",
                "vbar_object_actor",
                "system_note",
            ],
        )
    cursor.execute(
        f"""
        SELECT
            coalesce(sum(pg_relation_size(relid)),0)::bigint,
            coalesce(sum(pg_indexes_size(relid)),0)::bigint,
            coalesce(sum(pg_total_relation_size(relid)-pg_relation_size(relid)
                -pg_indexes_size(relid)),0)::bigint,
            coalesce(sum(pg_total_relation_size(relid)),0)::bigint,
            coalesce(sum(seq_tup_read),0)::bigint
        FROM pg_stat_user_tables
        WHERE {predicate}
        """,
        values,
    )
    row = cursor.fetchone()
    cursor.execute(
        f"""
        SELECT coalesce(sum(idx_scan),0)::bigint,
               coalesce(sum(idx_tup_read),0)::bigint
        FROM pg_stat_user_indexes
        WHERE {predicate}
        """,
        values,
    )
    row += cursor.fetchone()
    cursor.close()
    keys = (
        "heap_bytes",
        "index_bytes",
        "toast_bytes",
        "total_bytes",
        "sequential_tuples_read",
        "index_scans",
        "index_tuples_read",
    )
    return dict(zip(keys, map(int, row)))


def main():
    args = parse_args()
    selected = set(args.scenario or ())
    scenarios = [item for item in SCENARIOS if not selected or item[0] in selected]
    missing = selected - {item[0] for item in scenarios}
    if missing:
        raise SystemExit(f"unknown scenarios: {sorted(missing)}")

    engines = {
        "vanilla_postgres": Pg(args.plain_host),
        "verticalbar_optimized": Pg(args.verticalbar_host),
        "pg_ocpm": Pg(args.ocpm_host),
    }
    for engine in engines.values():
        configure(engine, args.timeout_seconds)
    fixture_data = fixture(
        engines["vanilla_postgres"], engines["pg_ocpm"], args.dataset
    )
    params = {
        **fixture_data,
        "minimum_frequency": 100,
        "minimum_cases": 100,
        "minimum_test_cases": 20,
        "minimum_prefix_cases": 50,
        "coverage": 0.95,
        "risk_cutoff": 0.25,
        "limit": 20,
    }
    rng = random.Random(args.seed)
    rows = []
    speedups = {
        key: []
        for key in (
            "verticalbar_vs_vanilla",
            "pg_ocpm_vs_vanilla",
            "pg_ocpm_vs_verticalbar",
        )
    }
    for index, (name, family, relational_sql, ocpm_sql) in enumerate(scenarios, 1):
        print(f"common-pm {index}/{len(scenarios)}: {name}", flush=True)
        calls = {
            "vanilla_postgres": lambda sql=relational_sql: engines[
                "vanilla_postgres"
            ].run(sql, params),
            "verticalbar_optimized": lambda sql=relational_sql: engines[
                "verticalbar_optimized"
            ].run(sql, params),
            "pg_ocpm": lambda sql=ocpm_sql: engines["pg_ocpm"].run(sql, params),
        }
        answers = {}
        warmup_timeouts = Counter()
        for _ in range(args.warmups):
            order = list(calls)
            rng.shuffle(order)
            for engine_name in order:
                try:
                    answers[engine_name] = calls[engine_name]()
                except psycopg2.errors.QueryCanceled:
                    warmup_timeouts[engine_name] += 1

        samples = {engine_name: [] for engine_name in calls}
        statuses = {engine_name: "completed" for engine_name in calls}
        first_counts = Counter()
        for _ in range(args.runs):
            order = [key for key, value in statuses.items() if value == "completed"]
            rng.shuffle(order)
            if order:
                first_counts[order[0]] += 1
            for engine_name in order:
                try:
                    elapsed, answer = timed(calls[engine_name])
                    samples[engine_name].append(elapsed)
                    answers[engine_name] = answer
                except psycopg2.errors.QueryCanceled:
                    statuses[engine_name] = f"timeout_at_{args.timeout_seconds}s"
                    samples[engine_name] = []

        engine_metrics = {}
        for engine_name in calls:
            if samples[engine_name]:
                engine_metrics[engine_name] = metrics(samples[engine_name])
            else:
                engine_metrics[engine_name] = {
                    "status": statuses[engine_name],
                    "timeout_ms": args.timeout_seconds * 1000,
                }
        completed = all(
            value["status"] == "completed" for value in engine_metrics.values()
        )
        agreement = (
            completed
            and len(answers) == 3
            and len({canonical(value) for value in answers.values()}) == 1
        )
        if not agreement:
            print("  WARNING: payload mismatch or measured timeout", flush=True)
        case_speedups = {}
        comparisons = {
            "verticalbar_vs_vanilla": (
                "verticalbar_optimized",
                "vanilla_postgres",
            ),
            "pg_ocpm_vs_vanilla": ("pg_ocpm", "vanilla_postgres"),
            "pg_ocpm_vs_verticalbar": ("pg_ocpm", "verticalbar_optimized"),
        }
        for key, (candidate, reference) in comparisons.items():
            if agreement:
                value = (
                    engine_metrics[reference]["p50_ms"]
                    / engine_metrics[candidate]["p50_ms"]
                )
                case_speedups[key] = round(value, 3)
                speedups[key].append(value)
            else:
                case_speedups[key] = None
        rows.append(
            {
                "scenario": name,
                "family": family,
                "agreement": agreement,
                "answer_hash": digest(answers["vanilla_postgres"])
                if agreement
                else None,
                "answer": answers["vanilla_postgres"] if agreement else None,
                "speedups": case_speedups,
                "warmup_timeouts": dict(warmup_timeouts),
                "randomized_first_counts": dict(first_counts),
                **engine_metrics,
            }
        )

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "comparison": (
            "common process-mining operations on index-light PostgreSQL, "
            "Vertical Bar optimized PostgreSQL, and pg_ocpm"
        ),
        "dataset": {
            key: value
            for key, value in fixture_data.items()
            if key not in ("dataset_id", "tenant_id")
        },
        "modeling": {
            "temporal_split": fixture_data["split_ts"],
            "train_fraction_by_case_start": 0.8,
            "dfg_and_variant_model_coverage": params["coverage"],
            "bottleneck_threshold_quantile": 0.75,
            "risk_cutoff": params["risk_cutoff"],
            "prediction_features": "source activity, source object type, edge type",
        },
        "methodology": {
            "warmups_per_engine": args.warmups,
            "measured_runs_per_engine": args.runs,
            "timeout_seconds": args.timeout_seconds,
            "randomized_engine_order": True,
            "seed": args.seed,
            "correctness_gate": "exact canonical JSON across all three engines",
            "result_cache": False,
            "latency_scope": (
                "end-to-end database query including model fit and held-out scoring"
            ),
        },
        "runtime": {
            "vanilla_postgres": runtime(engines["vanilla_postgres"]),
            "verticalbar_optimized": runtime(engines["verticalbar_optimized"]),
            "pg_ocpm": runtime(engines["pg_ocpm"], include_ocpm=True),
        },
        "cases": rows,
        "storage_and_index_usage": {
            "vanilla_postgres": storage(engines["vanilla_postgres"], False),
            "verticalbar_optimized": storage(engines["verticalbar_optimized"], False),
            "pg_ocpm": storage(engines["pg_ocpm"], True),
        },
        "summary": {
            "total_cases": len(rows),
            "matched_cases": sum(row["agreement"] for row in rows),
            "mismatches_or_timeouts": sum(not row["agreement"] for row in rows),
            "geomean_verticalbar_vs_vanilla": round(
                geomean(speedups["verticalbar_vs_vanilla"]), 3
            )
            if speedups["verticalbar_vs_vanilla"]
            else None,
            "geomean_pg_ocpm_vs_vanilla": round(
                geomean(speedups["pg_ocpm_vs_vanilla"]), 3
            )
            if speedups["pg_ocpm_vs_vanilla"]
            else None,
            "geomean_pg_ocpm_vs_verticalbar": round(
                geomean(speedups["pg_ocpm_vs_verticalbar"]), 3
            )
            if speedups["pg_ocpm_vs_verticalbar"]
            else None,
            "fastest_counts": {
                engine_name: sum(
                    row["agreement"]
                    and row[engine_name]["p50_ms"]
                    == min(row[key]["p50_ms"] for key in engines)
                    for row in rows
                )
                for engine_name in engines
            },
        },
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, default=str) + "\n")
    print(
        f"wrote {output}; exact matches "
        f"{result['summary']['matched_cases']}/{len(rows)}",
        flush=True,
    )


if __name__ == "__main__":
    main()
