//! Thin asynchronous adapter over pg_ocpm aggregate, binding, and event-stream
//! interfaces.

use futures_util::TryStreamExt;
use ocpm_core::{
    TransitionKey,
    binding::{BindingCapsule, BindingDecodeError},
    event_batch::{EventBatch, EventBatchError, EventLogSummary, EventSummaryBuilder},
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio_postgres::{
    Client, Row, RowStream, Statement,
    types::{ToSql, Type},
};

pub type PgClient = Client;

pub const DFG_COUNTS_SQL: &str = r#"
SELECT source_activity, target_activity, edge_type, frequency
FROM ocpm.dfg_counts(
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
)
ORDER BY source_activity, target_activity, edge_type
"#;

pub const VARIANT_COUNTS_SQL: &str = r#"
SELECT path_hash, frequency
FROM ocpm.variant_counts(
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
)
ORDER BY path_hash
"#;

pub const DFG_WINDOW_COUNTS_SQL: &str = r#"
SELECT source_activity, target_activity, source_object_type,
       target_object_type, edge_type, frequencies
FROM ocpm.dfg_window_counts($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ORDER BY source_activity, target_activity, source_object_type,
         target_object_type, edge_type
"#;

pub const LIFECYCLE_DFG_WINDOW_COUNTS_SQL: &str = r#"
SELECT chunk.first_window,
       dfg.object_type, dfg.source_activity, dfg.target_activity,
       dfg.frequencies
FROM generate_series(
    1, cardinality($3::timestamptz[]), 256
) AS chunk(first_window)
CROSS JOIN LATERAL ocpm.lifecycle_dfg_window_counts(
    $1, $2,
    ($3::timestamptz[])[
        chunk.first_window:
        LEAST(chunk.first_window + 255, cardinality($3::timestamptz[]))
    ],
    ($4::timestamptz[])[
        chunk.first_window:
        LEAST(chunk.first_window + 255, cardinality($4::timestamptz[]))
    ],
    $5, $6, $7, 1::bigint
) AS dfg
ORDER BY chunk.first_window, dfg.object_type,
         dfg.source_activity, dfg.target_activity
"#;

pub const VARIANT_WINDOW_COUNTS_SQL: &str = r#"
SELECT object_type, path_hash, frequencies
FROM ocpm.variant_window_counts($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ORDER BY object_type, path_hash
"#;

pub const ACTIVITY_PROFILE_SQL: &str = r#"
SELECT object_type, activity, case_frequency, occurrence_frequency,
       start_frequency, end_frequency
FROM ocpm.activity_profile(
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
)
ORDER BY object_type, activity
"#;

pub const EVENT_LOG_ROWS_SQL: &str = r#"
SELECT case_id, activity, event_timestamp, event_ordinal
FROM ocpm.event_log_rows($1, $2, $3, $4, $5)
"#;

pub const EVENT_LOG_BATCHES_SQL: &str = r#"
SELECT activity_path, activity_count, case_count,
       case_id_payloads, event_timestamp_payloads
FROM ocpm.event_log_batches($1, $2, $3, $4, $5)
"#;

pub const EVENT_LOG_WINDOW_BATCHES_SQL: &str = r#"
SELECT chunk.first_window - 1 + batch.window_ordinal AS window_ordinal,
       batch.activity_path, batch.activity_count, batch.case_count,
       batch.case_id_payloads, batch.event_timestamp_payloads
FROM generate_series(
    1, cardinality($4::timestamptz[]), 256
) AS chunk(first_window)
CROSS JOIN LATERAL ocpm.event_log_window_batches(
    $1, $2, $3,
    ($4::timestamptz[])[
        chunk.first_window:
        LEAST(chunk.first_window + 255, cardinality($4::timestamptz[]))
    ],
    ($5::timestamptz[])[
        chunk.first_window:
        LEAST(chunk.first_window + 255, cardinality($5::timestamptz[]))
    ]
) AS batch
"#;

pub const PG_OCPM_CAPABILITIES_SQL: &str = r#"
SELECT ocpm.version(),
       to_regprocedure(
           'ocpm.event_log_rows(bigint,bigint,text,timestamptz,timestamptz)'
       ) IS NOT NULL,
       to_regprocedure(
           'ocpm.event_log_batches(bigint,bigint,text,timestamptz,timestamptz)'
       ) IS NOT NULL,
       to_regprocedure(
           'ocpm.event_log_window_batches(bigint,bigint,text,timestamptz[],timestamptz[])'
       ) IS NOT NULL,
       to_regprocedure(
           'ocpm.lifecycle_dfg_window_counts(bigint,bigint,timestamptz[],timestamptz[],text[],text[],text[],bigint)'
       ) IS NOT NULL,
       to_regprocedure(
           'ocpm.lifecycle_variant_window_counts(bigint,bigint,timestamptz[],timestamptz[],text[],text[],text[],text[],text[],bigint)'
       ) IS NOT NULL
"#;

pub const BINDING_INDEX_COVERAGE_SQL: &str = r#"
SELECT dataset.refreshed_at,
       dataset.source_watermark,
       dataset.event_identity_complete,
       COALESCE((
           SELECT jsonb_agg(item ORDER BY item)::text
           FROM (
               SELECT jsonb_build_array(object_type) AS item
               FROM ocpm.binding_object
               WHERE dataset_id=$1 AND tenant_id=$2
           ) objects
       ), '[]'),
       COALESCE((
           SELECT jsonb_agg(item ORDER BY item)::text
           FROM (
               SELECT jsonb_build_array(object_type,activity) AS item
               FROM ocpm.binding_activity
               WHERE dataset_id=$1 AND tenant_id=$2
           ) activities
       ), '[]'),
       COALESCE((
           SELECT jsonb_agg(item ORDER BY item)::text
           FROM (
               SELECT jsonb_build_array(activity) AS item
               FROM ocpm.binding_event
               WHERE dataset_id=$1 AND tenant_id=$2
           ) events
       ), '[]'),
       COALESCE((
           SELECT jsonb_agg(item ORDER BY item)::text
           FROM (
               SELECT jsonb_build_array(
                   source_object_type,target_object_type,activity
               ) AS item
               FROM ocpm.binding_neighbor_activity
               WHERE dataset_id=$1 AND tenant_id=$2
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
               WHERE dataset_id=$1 AND tenant_id=$2
           ) relations
       ), '[]')
FROM ocpm.dataset AS dataset
WHERE dataset.dataset_id=$1 AND dataset.tenant_id=$2
"#;

pub const BINDING_OBJECT_ACTIVITY_COUNT_SQL: &str =
    "SELECT ocpm.binding_object_activity_count($1, $2, $3, $4, $5, $6)";
pub const BINDING_REQUIRES_ACTIVITY_SQL: &str =
    "SELECT ocpm.binding_requires_activity($1, $2, $3, $4, $5)";
pub const BINDING_EVENT_OBJECT_COUNT_SQL: &str =
    "SELECT ocpm.binding_event_object_count($1, $2, $3, $4, $5, $6)";
pub const BINDING_NEIGHBOR_EVENTUALLY_SQL: &str =
    "SELECT ocpm.binding_neighbor_eventually($1, $2, $3, $4, $5, $6)";
pub const BINDING_NEIGHBOR_ACTOR_EQUAL_SQL: &str =
    "SELECT ocpm.binding_neighbor_actor_equal($1, $2, $3, $4, $5, $6)";
pub const BINDING_MAX_ACTIVITY_DELAY_SQL: &str =
    "SELECT ocpm.binding_max_activity_delay($1, $2, $3, $4, $5)";
pub const BINDING_NEIGHBOR_PAIRS_SQL: &str =
    "SELECT ocpm.binding_neighbor_pairs($1, $2, $3, $4, $5)";
pub const BINDING_RELATION_UNIVERSAL_EQUAL_SQL: &str =
    "SELECT ocpm.binding_relation_universal_equal($1, $2, $3, $4, $5, $6, $7)";

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
    #[error(transparent)]
    Binding(#[from] BindingDecodeError),
    #[error("binding query must return exactly one bytea column")]
    InvalidBindingResultShape,
    #[error("binding query returned a NULL capsule")]
    NullBindingCapsule,
    #[error("binding-tree query must return one or more bytea columns")]
    InvalidBindingTreeResultShape,
    #[error("binding-tree query returned a NULL capsule at node {0}")]
    NullBindingTreeCapsule(usize),
    #[error("the requested materialized relation summary has not been declared or rebuilt")]
    MissingRelationSummary,
    #[error("event-log query must return bigint, text, timestamptz, integer")]
    InvalidEventLogResultShape,
    #[error("event-batch query returned an unexpected result shape")]
    InvalidEventBatchResultShape,
    #[error("event-window-batch query returned an unexpected result shape")]
    InvalidEventWindowBatchResultShape,
    #[error(transparent)]
    EventBatch(#[from] EventBatchError),
    #[error("event-log row stream is not ordered by case and ordinal")]
    InvalidEventLogOrder,
    #[error("event-log timestamp is outside the supported PostgreSQL range")]
    TimestampOverflow,
    #[error("event-log database row count overflowed u64")]
    DatabaseRowCountOverflow,
    #[error("dataset {dataset_id} for tenant {tenant_id} does not exist")]
    DatasetNotFound { dataset_id: i64, tenant_id: i64 },
    #[error("binding-index coverage metadata is invalid: {0}")]
    InvalidBindingIndexCoverage(String),
    #[error("event-log window arrays must be nonempty and have equal lengths")]
    InvalidEventWindows,
    #[error("event-log batch returned invalid window ordinal {0}")]
    InvalidWindowOrdinal(i32),
    #[error("lifecycle DFG query returned an invalid chunk")]
    InvalidLifecycleDfgChunk,
}

/// A decoded binding capsule plus its encoded wire size.
#[derive(Clone, Debug, PartialEq)]
pub struct BindingQueryResult {
    pub capsule: BindingCapsule,
    pub encoded_bytes: usize,
}

/// Fully decoded node capsules from one binding-tree request.
#[derive(Clone, Debug, PartialEq)]
pub struct BindingTreeQueryResult {
    pub capsules: Vec<BindingCapsule>,
    pub encoded_bytes: usize,
}

/// An owned caller result produced while the PostgreSQL `bytea` value was
/// borrowed from its result row.
#[derive(Clone, Debug, PartialEq)]
pub struct BindingQueryOutput<T> {
    pub value: T,
    pub encoded_bytes: usize,
}

/// A connection-specific prepared query that returns one pg_ocpm binding capsule.
#[derive(Clone, Debug)]
pub struct PreparedBindingQuery {
    statement: Statement,
}

/// A connection-specific prepared query whose bytea columns are binding-tree
/// nodes in root-first order. One PostgreSQL round trip produces every node.
#[derive(Clone, Debug)]
pub struct PreparedBindingTreeQuery {
    statement: Statement,
    node_count: usize,
}

/// One event decoded from pg_ocpm's native case-bucket stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventLogRow {
    pub case_id: i64,
    pub activity: String,
    pub event_timestamp: SystemTime,
    pub event_ordinal: i32,
}

/// A connection-specific prepared native event-log stream.
///
/// `query` returns PostgreSQL's asynchronous `RowStream`; callers apply
/// backpressure by polling it and never need to allocate a complete event log.
#[derive(Clone, Debug)]
pub struct PreparedEventLogQuery {
    statement: Statement,
}

/// Features detected from the connected extension instead of inferred only
/// from its version string. This keeps forks and partial upgrades fail-closed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PgOcpmCapabilities {
    pub version: String,
    pub event_log_rows: bool,
    pub event_log_batches: bool,
    pub event_log_window_batches: bool,
    pub lifecycle_dfg_window_counts: bool,
    pub lifecycle_variant_window_counts: bool,
}

impl PgOcpmCapabilities {
    pub fn factorized_event_export(&self) -> bool {
        self.event_log_batches
    }

    pub fn factorized_multi_window_export(&self) -> bool {
        self.event_log_window_batches
    }

    pub fn lifecycle_dfg_pushdown(&self) -> bool {
        self.lifecycle_dfg_window_counts
    }

    pub fn lifecycle_variant_pushdown(&self) -> bool {
        self.lifecycle_variant_window_counts
    }
}

pub async fn pg_ocpm_capabilities(client: &Client) -> Result<PgOcpmCapabilities, AdapterError> {
    let row = client.query_one(PG_OCPM_CAPABILITIES_SQL, &[]).await?;
    Ok(PgOcpmCapabilities {
        version: row.try_get(0)?,
        event_log_rows: row.try_get(1)?,
        event_log_batches: row.try_get(2)?,
        event_log_window_batches: row.try_get(3)?,
        lifecycle_dfg_window_counts: row.try_get(4)?,
        lifecycle_variant_window_counts: row.try_get(5)?,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EventLogStrategy {
    FactorizedBatch,
    NativeRowFallback,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventLogExecution {
    pub strategy: EventLogStrategy,
    pub database_rows: u64,
    pub summary: EventLogSummary,
}

/// A connection-specific pg_ocpm 0.9 factorized event-batch request.
#[derive(Clone, Debug)]
pub struct PreparedEventBatchQuery {
    statement: Statement,
}

/// One factorized batch assigned to a one-based input window.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WindowedEventBatch {
    pub window_ordinal: i32,
    pub batch: EventBatch,
}

/// A connection-specific pg_ocpm 0.9 multi-window batch request.
#[derive(Clone, Debug)]
pub struct PreparedEventWindowBatchQuery {
    statement: Statement,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NeighborBindingCoverage {
    pub source_object_type: String,
    pub target_object_type: String,
    pub activity: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationBindingCoverage {
    pub source_object_type: String,
    pub source_activity: String,
    pub target_object_type: String,
    pub target_activity: String,
    pub related_object_type: String,
}

/// Observable index coverage plus the dataset refresh markers that invalidate
/// every binding summary. Absence is explicit; no result cache is consulted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BindingIndexCoverage {
    pub refreshed_at: Option<SystemTime>,
    pub source_watermark: Option<SystemTime>,
    pub event_identity_complete: bool,
    pub object_types: Vec<String>,
    pub activities: Vec<(String, String)>,
    pub event_activities: Vec<String>,
    pub neighbors: Vec<NeighborBindingCoverage>,
    pub relations: Vec<RelationBindingCoverage>,
}

impl BindingIndexCoverage {
    pub fn covers_object_type(&self, object_type: &str) -> bool {
        self.object_types
            .binary_search_by(|item| item.as_str().cmp(object_type))
            .is_ok()
    }

    pub fn covers_activity(&self, object_type: &str, activity: &str) -> bool {
        self.activities
            .binary_search_by(|item| {
                (item.0.as_str(), item.1.as_str()).cmp(&(object_type, activity))
            })
            .is_ok()
    }

    pub fn covers_event_activity(&self, activity: &str) -> bool {
        self.event_activities
            .binary_search_by(|item| item.as_str().cmp(activity))
            .is_ok()
    }

    pub fn covers_neighbor(
        &self,
        source_object_type: &str,
        target_object_type: &str,
        activity: &str,
    ) -> bool {
        self.neighbors
            .binary_search_by(|item| {
                (
                    item.source_object_type.as_str(),
                    item.target_object_type.as_str(),
                    item.activity.as_str(),
                )
                    .cmp(&(source_object_type, target_object_type, activity))
            })
            .is_ok()
    }

    pub fn covers_relation(
        &self,
        source_object_type: &str,
        source_activity: &str,
        target_object_type: &str,
        target_activity: &str,
        related_object_type: &str,
    ) -> bool {
        self.relations
            .binary_search_by(|item| {
                (
                    item.source_object_type.as_str(),
                    item.source_activity.as_str(),
                    item.target_object_type.as_str(),
                    item.target_activity.as_str(),
                    item.related_object_type.as_str(),
                )
                    .cmp(&(
                        source_object_type,
                        source_activity,
                        target_object_type,
                        target_activity,
                        related_object_type,
                    ))
            })
            .is_ok()
    }
}

pub async fn binding_index_coverage(
    client: &Client,
    dataset_id: i64,
    tenant_id: i64,
) -> Result<BindingIndexCoverage, AdapterError> {
    let row = client
        .query_opt(BINDING_INDEX_COVERAGE_SQL, &[&dataset_id, &tenant_id])
        .await?
        .ok_or(AdapterError::DatasetNotFound {
            dataset_id,
            tenant_id,
        })?;
    let object_rows = json_string_rows(&row.try_get::<_, String>(3)?, 1)?;
    let activity_rows = json_string_rows(&row.try_get::<_, String>(4)?, 2)?;
    let event_rows = json_string_rows(&row.try_get::<_, String>(5)?, 1)?;
    let neighbor_rows = json_string_rows(&row.try_get::<_, String>(6)?, 3)?;
    let relation_rows = json_string_rows(&row.try_get::<_, String>(7)?, 5)?;
    Ok(BindingIndexCoverage {
        refreshed_at: row.try_get(0)?,
        source_watermark: row.try_get(1)?,
        event_identity_complete: row.try_get(2)?,
        object_types: object_rows.into_iter().map(|row| row[0].clone()).collect(),
        activities: activity_rows
            .into_iter()
            .map(|row| (row[0].clone(), row[1].clone()))
            .collect(),
        event_activities: event_rows.into_iter().map(|row| row[0].clone()).collect(),
        neighbors: neighbor_rows
            .into_iter()
            .map(|row| NeighborBindingCoverage {
                source_object_type: row[0].clone(),
                target_object_type: row[1].clone(),
                activity: row[2].clone(),
            })
            .collect(),
        relations: relation_rows
            .into_iter()
            .map(|row| RelationBindingCoverage {
                source_object_type: row[0].clone(),
                source_activity: row[1].clone(),
                target_object_type: row[2].clone(),
                target_activity: row[3].clone(),
                related_object_type: row[4].clone(),
            })
            .collect(),
    })
}

fn json_string_rows(source: &str, width: usize) -> Result<Vec<Vec<String>>, AdapterError> {
    let rows: Vec<Vec<String>> = serde_json::from_str(source)
        .map_err(|error| AdapterError::InvalidBindingIndexCoverage(error.to_string()))?;
    if rows.iter().any(|row| row.len() != width) {
        return Err(AdapterError::InvalidBindingIndexCoverage(format!(
            "expected width {width}"
        )));
    }
    Ok(rows)
}

fn validate_event_log_result_shape<'a>(
    column_types: impl IntoIterator<Item = &'a Type>,
) -> Result<(), AdapterError> {
    let actual = column_types.into_iter().collect::<Vec<_>>();
    let expected = [Type::INT8, Type::TEXT, Type::TIMESTAMPTZ, Type::INT4];
    if actual.len() != expected.len()
        || actual
            .into_iter()
            .zip(expected)
            .any(|(actual_type, expected_type)| *actual_type != expected_type)
    {
        return Err(AdapterError::InvalidEventLogResultShape);
    }
    Ok(())
}

fn validate_exact_shape<'a>(
    column_types: impl IntoIterator<Item = &'a Type>,
    expected: &[Type],
    error: AdapterError,
) -> Result<(), AdapterError> {
    let actual = column_types.into_iter().collect::<Vec<_>>();
    if actual.len() != expected.len()
        || actual
            .into_iter()
            .zip(expected)
            .any(|(actual_type, expected_type)| actual_type != expected_type)
    {
        return Err(error);
    }
    Ok(())
}

impl PreparedEventLogQuery {
    /// Prepare the pg_ocpm 0.8 native event stream and validate its row shape.
    pub async fn prepare(client: &Client) -> Result<Self, AdapterError> {
        let statement = client.prepare(EVENT_LOG_ROWS_SQL).await?;
        validate_event_log_result_shape(statement.columns().iter().map(|column| column.type_()))?;
        Ok(Self { statement })
    }

    /// Start a storage-neutral event stream for cases fully contained in the
    /// inclusive time window.
    pub async fn query(
        &self,
        client: &Client,
        dataset_id: i64,
        tenant_id: i64,
        object_type: &str,
        from_timestamp: SystemTime,
        to_timestamp: SystemTime,
    ) -> Result<RowStream, AdapterError> {
        Ok(client
            .query_raw(
                &self.statement,
                [
                    &dataset_id as &(dyn ToSql + Sync),
                    &tenant_id,
                    &object_type,
                    &from_timestamp,
                    &to_timestamp,
                ],
            )
            .await?)
    }

    /// Decode one borrowed PostgreSQL row into an owned event.
    pub fn decode(row: &Row) -> Result<EventLogRow, AdapterError> {
        Ok(EventLogRow {
            case_id: row.try_get(0)?,
            activity: row.try_get(1)?,
            event_timestamp: row.try_get(2)?,
            event_ordinal: row.try_get(3)?,
        })
    }
}

impl PreparedEventBatchQuery {
    pub async fn prepare(client: &Client) -> Result<Self, AdapterError> {
        let statement = client.prepare(EVENT_LOG_BATCHES_SQL).await?;
        validate_exact_shape(
            statement.columns().iter().map(|column| column.type_()),
            &[
                Type::TEXT_ARRAY,
                Type::INT4,
                Type::INT4,
                Type::BYTEA,
                Type::BYTEA,
            ],
            AdapterError::InvalidEventBatchResultShape,
        )?;
        Ok(Self { statement })
    }

    pub async fn query(
        &self,
        client: &Client,
        dataset_id: i64,
        tenant_id: i64,
        object_type: &str,
        from_timestamp: SystemTime,
        to_timestamp: SystemTime,
    ) -> Result<RowStream, AdapterError> {
        Ok(client
            .query_raw(
                &self.statement,
                [
                    &dataset_id as &(dyn ToSql + Sync),
                    &tenant_id,
                    &object_type,
                    &from_timestamp,
                    &to_timestamp,
                ],
            )
            .await?)
    }

    pub fn decode(row: &Row) -> Result<EventBatch, AdapterError> {
        Ok(EventBatch::decode(
            row.try_get(0)?,
            row.try_get(1)?,
            row.try_get(2)?,
            row.try_get(3)?,
            row.try_get(4)?,
        )?)
    }
}

impl PreparedEventWindowBatchQuery {
    pub async fn prepare(client: &Client) -> Result<Self, AdapterError> {
        let statement = client.prepare(EVENT_LOG_WINDOW_BATCHES_SQL).await?;
        validate_exact_shape(
            statement.columns().iter().map(|column| column.type_()),
            &[
                Type::INT4,
                Type::TEXT_ARRAY,
                Type::INT4,
                Type::INT4,
                Type::BYTEA,
                Type::BYTEA,
            ],
            AdapterError::InvalidEventWindowBatchResultShape,
        )?;
        Ok(Self { statement })
    }

    pub async fn query(
        &self,
        client: &Client,
        dataset_id: i64,
        tenant_id: i64,
        object_type: &str,
        from_timestamps: &[SystemTime],
        to_timestamps: &[SystemTime],
    ) -> Result<RowStream, AdapterError> {
        validate_event_windows(from_timestamps, to_timestamps)?;
        Ok(client
            .query_raw(
                &self.statement,
                [
                    &dataset_id as &(dyn ToSql + Sync),
                    &tenant_id,
                    &object_type,
                    &from_timestamps,
                    &to_timestamps,
                ],
            )
            .await?)
    }

    pub fn decode(row: &Row) -> Result<WindowedEventBatch, AdapterError> {
        Ok(WindowedEventBatch {
            window_ordinal: row.try_get(0)?,
            batch: EventBatch::decode(
                row.try_get(1)?,
                row.try_get(2)?,
                row.try_get(3)?,
                row.try_get(4)?,
                row.try_get(5)?,
            )?,
        })
    }
}

fn validate_event_windows(
    from_timestamps: &[SystemTime],
    to_timestamps: &[SystemTime],
) -> Result<(), AdapterError> {
    if from_timestamps.is_empty() || from_timestamps.len() != to_timestamps.len() {
        return Err(AdapterError::InvalidEventWindows);
    }
    Ok(())
}

/// Select pg_ocpm 0.9's factorized path when available and retain the 0.8 row
/// stream only as a compatibility fallback.
pub async fn event_log_summary(
    client: &Client,
    dataset_id: i64,
    tenant_id: i64,
    object_type: &str,
    from_timestamp: SystemTime,
    to_timestamp: SystemTime,
) -> Result<EventLogExecution, AdapterError> {
    let capabilities = pg_ocpm_capabilities(client).await?;
    if capabilities.event_log_batches {
        let query = PreparedEventBatchQuery::prepare(client).await?;
        let mut stream = std::pin::pin!(
            query
                .query(
                    client,
                    dataset_id,
                    tenant_id,
                    object_type,
                    from_timestamp,
                    to_timestamp,
                )
                .await?
        );
        let mut builder = EventSummaryBuilder::new();
        let mut database_rows = 0_u64;
        while let Some(row) = stream.try_next().await? {
            database_rows = database_rows
                .checked_add(1)
                .ok_or(AdapterError::DatabaseRowCountOverflow)?;
            builder.push_batch(&PreparedEventBatchQuery::decode(&row)?)?;
        }
        return Ok(EventLogExecution {
            strategy: EventLogStrategy::FactorizedBatch,
            database_rows,
            summary: builder.finish(),
        });
    }
    if !capabilities.event_log_rows {
        return Err(AdapterError::InvalidEventLogResultShape);
    }
    event_log_row_summary(
        client,
        dataset_id,
        tenant_id,
        object_type,
        from_timestamp,
        to_timestamp,
    )
    .await
}

/// Summarize aligned windows in one PostgreSQL statement containing pg_ocpm
/// 0.9 bucket scans of at most 256 windows each. The 0.8 compatibility path
/// remains exact but performs one native row scan per window because the
/// extension has no multi-window export.
pub async fn event_log_window_summaries(
    client: &Client,
    dataset_id: i64,
    tenant_id: i64,
    object_type: &str,
    from_timestamps: &[SystemTime],
    to_timestamps: &[SystemTime],
) -> Result<Vec<EventLogExecution>, AdapterError> {
    validate_event_windows(from_timestamps, to_timestamps)?;
    let capabilities = pg_ocpm_capabilities(client).await?;
    if capabilities.event_log_window_batches {
        let query = PreparedEventWindowBatchQuery::prepare(client).await?;
        let mut stream = std::pin::pin!(
            query
                .query(
                    client,
                    dataset_id,
                    tenant_id,
                    object_type,
                    from_timestamps,
                    to_timestamps,
                )
                .await?
        );
        let mut builders = (0..from_timestamps.len())
            .map(|_| EventSummaryBuilder::new())
            .collect::<Vec<_>>();
        let mut database_rows = vec![0_u64; from_timestamps.len()];
        while let Some(row) = stream.try_next().await? {
            let batch = PreparedEventWindowBatchQuery::decode(&row)?;
            let index = batch
                .window_ordinal
                .checked_sub(1)
                .and_then(|ordinal| usize::try_from(ordinal).ok())
                .filter(|index| *index < builders.len())
                .ok_or(AdapterError::InvalidWindowOrdinal(batch.window_ordinal))?;
            database_rows[index] = database_rows[index]
                .checked_add(1)
                .ok_or(AdapterError::DatabaseRowCountOverflow)?;
            builders[index].push_batch(&batch.batch)?;
        }
        return Ok(builders
            .into_iter()
            .zip(database_rows)
            .map(|(builder, database_rows)| EventLogExecution {
                strategy: EventLogStrategy::FactorizedBatch,
                database_rows,
                summary: builder.finish(),
            })
            .collect());
    }

    if !capabilities.event_log_rows {
        return Err(AdapterError::InvalidEventLogResultShape);
    }

    let mut results = Vec::new();
    results
        .try_reserve_exact(from_timestamps.len())
        .map_err(|_| EventBatchError::DimensionOverflow)?;
    for (&from_timestamp, &to_timestamp) in from_timestamps.iter().zip(to_timestamps) {
        results.push(
            event_log_row_summary(
                client,
                dataset_id,
                tenant_id,
                object_type,
                from_timestamp,
                to_timestamp,
            )
            .await?,
        );
    }
    Ok(results)
}

async fn event_log_row_summary(
    client: &Client,
    dataset_id: i64,
    tenant_id: i64,
    object_type: &str,
    from_timestamp: SystemTime,
    to_timestamp: SystemTime,
) -> Result<EventLogExecution, AdapterError> {
    let query = PreparedEventLogQuery::prepare(client).await?;
    let mut stream = std::pin::pin!(
        query
            .query(
                client,
                dataset_id,
                tenant_id,
                object_type,
                from_timestamp,
                to_timestamp,
            )
            .await?
    );
    let mut builder = EventSummaryBuilder::new();
    let mut database_rows = 0_u64;
    let mut current_case = None;
    let mut expected_ordinal = 1_i32;
    let mut activities = Vec::new();
    let mut timestamps = Vec::new();
    while let Some(row) = stream.try_next().await? {
        database_rows = database_rows
            .checked_add(1)
            .ok_or(AdapterError::DatabaseRowCountOverflow)?;
        let event = PreparedEventLogQuery::decode(&row)?;
        if current_case.is_some_and(|case_id| case_id != event.case_id) {
            builder.push_case(&activities, &timestamps)?;
            activities.clear();
            timestamps.clear();
            expected_ordinal = 1;
        }
        if event.event_ordinal != expected_ordinal {
            return Err(AdapterError::InvalidEventLogOrder);
        }
        current_case = Some(event.case_id);
        expected_ordinal = expected_ordinal
            .checked_add(1)
            .ok_or(AdapterError::InvalidEventLogOrder)?;
        activities.push(event.activity);
        timestamps.push(system_time_to_pg_micros(event.event_timestamp)?);
    }
    if current_case.is_some() {
        builder.push_case(&activities, &timestamps)?;
    }
    Ok(EventLogExecution {
        strategy: EventLogStrategy::NativeRowFallback,
        database_rows,
        summary: builder.finish(),
    })
}

fn system_time_to_pg_micros(timestamp: SystemTime) -> Result<i64, AdapterError> {
    const POSTGRES_EPOCH_UNIX_MICROS: i128 = 946_684_800_000_000;
    let unix_micros = match timestamp.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => {
            i128::try_from(duration.as_micros()).map_err(|_| AdapterError::TimestampOverflow)?
        }
        Err(error) => -i128::try_from(error.duration().as_micros())
            .map_err(|_| AdapterError::TimestampOverflow)?,
    };
    i64::try_from(unix_micros - POSTGRES_EPOCH_UNIX_MICROS)
        .map_err(|_| AdapterError::TimestampOverflow)
}

#[cfg(test)]
mod event_log_tests {
    use super::*;

    #[test]
    fn event_log_shape_is_exact() {
        assert!(
            validate_event_log_result_shape([
                &Type::INT8,
                &Type::TEXT,
                &Type::TIMESTAMPTZ,
                &Type::INT4,
            ])
            .is_ok()
        );
        assert!(matches!(
            validate_event_log_result_shape([
                &Type::INT8,
                &Type::TEXT,
                &Type::TIMESTAMP,
                &Type::INT4,
            ]),
            Err(AdapterError::InvalidEventLogResultShape)
        ));
        assert!(matches!(
            validate_event_log_result_shape([&Type::INT8, &Type::TEXT]),
            Err(AdapterError::InvalidEventLogResultShape)
        ));
    }

    #[test]
    fn event_window_vectors_fail_closed_before_querying() {
        let timestamp = SystemTime::UNIX_EPOCH;

        assert!(matches!(
            validate_event_windows(&[], &[]),
            Err(AdapterError::InvalidEventWindows)
        ));
        assert!(matches!(
            validate_event_windows(&[timestamp], &[timestamp, timestamp]),
            Err(AdapterError::InvalidEventWindows)
        ));
        assert!(validate_event_windows(&[timestamp], &[timestamp]).is_ok());
    }

    #[test]
    fn factorized_batch_shapes_are_exact() {
        assert!(
            validate_exact_shape(
                [
                    &Type::TEXT_ARRAY,
                    &Type::INT4,
                    &Type::INT4,
                    &Type::BYTEA,
                    &Type::BYTEA,
                ],
                &[
                    Type::TEXT_ARRAY,
                    Type::INT4,
                    Type::INT4,
                    Type::BYTEA,
                    Type::BYTEA,
                ],
                AdapterError::InvalidEventBatchResultShape,
            )
            .is_ok()
        );
        assert!(matches!(
            validate_exact_shape(
                [&Type::TEXT_ARRAY, &Type::INT4],
                &[
                    Type::TEXT_ARRAY,
                    Type::INT4,
                    Type::INT4,
                    Type::BYTEA,
                    Type::BYTEA,
                ],
                AdapterError::InvalidEventBatchResultShape,
            ),
            Err(AdapterError::InvalidEventBatchResultShape)
        ));
    }

    #[test]
    fn binding_coverage_json_rejects_wrong_width() {
        assert_eq!(
            json_string_rows(r#"[["Order","Create"]]"#, 2).unwrap(),
            vec![vec!["Order", "Create"]]
        );
        assert!(matches!(
            json_string_rows(r#"[["Order"]]"#, 2),
            Err(AdapterError::InvalidBindingIndexCoverage(_))
        ));
    }

    #[test]
    fn binding_coverage_lookups_are_explicit_and_exact() {
        let coverage = BindingIndexCoverage {
            refreshed_at: None,
            source_watermark: None,
            event_identity_complete: true,
            object_types: vec!["Order".into()],
            activities: vec![("Order".into(), "Create".into())],
            event_activities: vec!["Create".into()],
            neighbors: vec![NeighborBindingCoverage {
                source_object_type: "Order".into(),
                target_object_type: "Item".into(),
                activity: "Create".into(),
            }],
            relations: vec![RelationBindingCoverage {
                source_object_type: "Order".into(),
                source_activity: "Create".into(),
                target_object_type: "Item".into(),
                target_activity: "Approve".into(),
                related_object_type: "User".into(),
            }],
        };

        assert!(coverage.covers_object_type("Order"));
        assert!(coverage.covers_activity("Order", "Create"));
        assert!(coverage.covers_event_activity("Create"));
        assert!(coverage.covers_neighbor("Order", "Item", "Create"));
        assert!(coverage.covers_relation("Order", "Create", "Item", "Approve", "User"));
        assert!(!coverage.covers_activity("Order", "Approve"));
        assert!(!coverage.covers_neighbor("Item", "Order", "Create"));
    }
}

fn validate_binding_tree_result_shape<'a>(
    column_types: impl IntoIterator<Item = &'a Type>,
) -> Result<usize, AdapterError> {
    let mut node_count = 0_usize;
    for column_type in column_types {
        if *column_type != Type::BYTEA {
            return Err(AdapterError::InvalidBindingTreeResultShape);
        }
        node_count += 1;
    }
    if node_count == 0 {
        return Err(AdapterError::InvalidBindingTreeResultShape);
    }
    Ok(node_count)
}

fn decode_binding_tree_capsules<'a>(
    node_count: usize,
    mut encoded_node: impl FnMut(usize) -> Result<Option<&'a [u8]>, AdapterError>,
) -> Result<BindingTreeQueryResult, AdapterError> {
    let mut capsules = Vec::new();
    capsules
        .try_reserve_exact(node_count)
        .map_err(|_| BindingDecodeError::AllocationFailed)?;
    let mut encoded_bytes = 0_usize;
    for node in 0..node_count {
        let bytes = encoded_node(node)?.ok_or(AdapterError::NullBindingTreeCapsule(node))?;
        encoded_bytes = encoded_bytes
            .checked_add(bytes.len())
            .ok_or(BindingDecodeError::CountOverflow)?;
        capsules.push(BindingCapsule::decode(bytes)?);
    }
    Ok(BindingTreeQueryResult {
        capsules,
        encoded_bytes,
    })
}

impl PreparedBindingTreeQuery {
    pub async fn prepare(client: &Client, sql: &str) -> Result<Self, AdapterError> {
        let statement = client.prepare(sql).await?;
        let node_count = validate_binding_tree_result_shape(
            statement.columns().iter().map(|column| column.type_()),
        )?;
        Ok(Self {
            statement,
            node_count,
        })
    }

    pub fn node_count(&self) -> usize {
        self.node_count
    }

    /// Execute one request and decode every node while the PostgreSQL row is
    /// still borrowed. The result owns all decoded columns.
    pub async fn execute(
        &self,
        client: &Client,
        parameters: &[&(dyn ToSql + Sync)],
    ) -> Result<BindingTreeQueryResult, AdapterError> {
        let row = client.query_one(&self.statement, parameters).await?;
        decode_binding_tree_capsules(self.node_count, |node| {
            row.try_get::<_, Option<&[u8]>>(node)
                .map_err(AdapterError::from)
        })
    }
}

#[cfg(test)]
mod binding_tree_tests {
    use super::*;

    const TWO_IDS: &[u8] = b"OCPB\x01\x08\x01\x14\x28";
    const FIVE_IDS_VIOLATION: &[u8] = b"OCPB\x01\x0b\x01\x02\x04\x06\x08\x0a\x01";

    #[test]
    fn binding_tree_shape_requires_one_or_more_bytea_columns() {
        assert_eq!(
            validate_binding_tree_result_shape([&Type::BYTEA, &Type::BYTEA]).unwrap(),
            2
        );
        assert!(matches!(
            validate_binding_tree_result_shape(std::iter::empty::<&Type>()),
            Err(AdapterError::InvalidBindingTreeResultShape)
        ));
        assert!(matches!(
            validate_binding_tree_result_shape([&Type::BYTEA, &Type::INT4]),
            Err(AdapterError::InvalidBindingTreeResultShape)
        ));
    }

    #[test]
    fn binding_tree_decode_preserves_order_and_accounts_for_encoded_bytes() {
        let nodes: [Option<&[u8]>; 2] = [Some(TWO_IDS), Some(FIVE_IDS_VIOLATION)];
        let result = decode_binding_tree_capsules(nodes.len(), |node| Ok(nodes[node])).unwrap();

        assert_eq!(
            result.encoded_bytes,
            TWO_IDS.len() + FIVE_IDS_VIOLATION.len()
        );
        assert_eq!(
            result.capsules[0].schema(),
            ocpm_core::binding::BindingSchema::TwoIds
        );
        assert_eq!(
            result.capsules[1].schema(),
            ocpm_core::binding::BindingSchema::FiveIdsViolation
        );
        assert_eq!(result.capsules[0].rows().next().unwrap().ids(), &[10, 20]);
        let violation = result.capsules[1].rows().next().unwrap();
        assert_eq!(violation.ids(), &[1, 2, 3, 4, 5]);
        assert_eq!(violation.violated, Some(true));
    }

    #[test]
    fn binding_tree_decode_reports_the_null_node_position() {
        let nodes: [Option<&[u8]>; 2] = [Some(TWO_IDS), None];
        assert!(matches!(
            decode_binding_tree_capsules(nodes.len(), |node| Ok(nodes[node])),
            Err(AdapterError::NullBindingTreeCapsule(1))
        ));
    }
}

impl PreparedBindingQuery {
    /// Prepare and validate a query that returns exactly one `bytea` column.
    pub async fn prepare(client: &Client, sql: &str) -> Result<Self, AdapterError> {
        let statement = client.prepare(sql).await?;
        if statement.columns().len() != 1 || *statement.columns()[0].type_() != Type::BYTEA {
            return Err(AdapterError::InvalidBindingResultShape);
        }
        Ok(Self { statement })
    }

    /// Execute the prepared query and process its sole `bytea` value without
    /// first copying it into an owned `Vec<u8>`.
    ///
    /// The borrowed bytes are valid only for the duration of `consume`; the
    /// returned value must own anything it retains.
    pub async fn execute_with<T>(
        &self,
        client: &Client,
        parameters: &[&(dyn ToSql + Sync)],
        consume: impl FnOnce(&[u8]) -> T,
    ) -> Result<BindingQueryOutput<T>, AdapterError> {
        let row = client.query_one(&self.statement, parameters).await?;
        let bytes = row
            .try_get::<_, Option<&[u8]>>(0)?
            .ok_or(AdapterError::NullBindingCapsule)?;
        Ok(BindingQueryOutput {
            encoded_bytes: bytes.len(),
            value: consume(bytes),
        })
    }

    /// Execute the prepared query, fetch its sole row, and decode its capsule.
    pub async fn execute(
        &self,
        client: &Client,
        parameters: &[&(dyn ToSql + Sync)],
    ) -> Result<BindingQueryResult, AdapterError> {
        let output = self
            .execute_with(client, parameters, BindingCapsule::decode)
            .await?;
        let capsule = output.value?;
        Ok(BindingQueryResult {
            capsule,
            encoded_bytes: output.encoded_bytes,
        })
    }
}

async fn binding_capsule(
    client: &Client,
    sql: &str,
    parameters: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<BindingCapsule, AdapterError> {
    let query = PreparedBindingQuery::prepare(client, sql).await?;
    Ok(query.execute(client, parameters).await?.capsule)
}

pub async fn binding_object_activity_count(
    client: &Client,
    dataset_id: i64,
    tenant_id: i64,
    object_type: &str,
    activity: &str,
    minimum: i32,
    maximum: i32,
) -> Result<BindingCapsule, AdapterError> {
    binding_capsule(
        client,
        BINDING_OBJECT_ACTIVITY_COUNT_SQL,
        &[
            &dataset_id,
            &tenant_id,
            &object_type,
            &activity,
            &minimum,
            &maximum,
        ],
    )
    .await
}

pub async fn binding_requires_activity(
    client: &Client,
    dataset_id: i64,
    tenant_id: i64,
    object_type: &str,
    source_activity: &str,
    required_activity: &str,
) -> Result<BindingCapsule, AdapterError> {
    binding_capsule(
        client,
        BINDING_REQUIRES_ACTIVITY_SQL,
        &[
            &dataset_id,
            &tenant_id,
            &object_type,
            &source_activity,
            &required_activity,
        ],
    )
    .await
}

pub async fn binding_event_object_count(
    client: &Client,
    dataset_id: i64,
    tenant_id: i64,
    object_type: &str,
    activity: &str,
    minimum: i32,
    maximum: i32,
) -> Result<BindingCapsule, AdapterError> {
    binding_capsule(
        client,
        BINDING_EVENT_OBJECT_COUNT_SQL,
        &[
            &dataset_id,
            &tenant_id,
            &object_type,
            &activity,
            &minimum,
            &maximum,
        ],
    )
    .await
}

pub async fn binding_neighbor_eventually(
    client: &Client,
    dataset_id: i64,
    tenant_id: i64,
    source_object_type: &str,
    source_activity: &str,
    target_object_type: &str,
    target_activity: &str,
) -> Result<BindingCapsule, AdapterError> {
    binding_capsule(
        client,
        BINDING_NEIGHBOR_EVENTUALLY_SQL,
        &[
            &dataset_id,
            &tenant_id,
            &source_object_type,
            &source_activity,
            &target_object_type,
            &target_activity,
        ],
    )
    .await
}

pub async fn binding_neighbor_actor_equal(
    client: &Client,
    dataset_id: i64,
    tenant_id: i64,
    source_object_type: &str,
    source_activity: &str,
    target_object_type: &str,
    target_activity: &str,
) -> Result<BindingCapsule, AdapterError> {
    binding_capsule(
        client,
        BINDING_NEIGHBOR_ACTOR_EQUAL_SQL,
        &[
            &dataset_id,
            &tenant_id,
            &source_object_type,
            &source_activity,
            &target_object_type,
            &target_activity,
        ],
    )
    .await
}

pub async fn binding_max_activity_delay(
    client: &Client,
    dataset_id: i64,
    tenant_id: i64,
    object_type: &str,
    source_activity: &str,
    target_activity: &str,
) -> Result<BindingCapsule, AdapterError> {
    binding_capsule(
        client,
        BINDING_MAX_ACTIVITY_DELAY_SQL,
        &[
            &dataset_id,
            &tenant_id,
            &object_type,
            &source_activity,
            &target_activity,
        ],
    )
    .await
}

pub async fn binding_neighbor_pairs(
    client: &Client,
    dataset_id: i64,
    tenant_id: i64,
    source_object_type: &str,
    target_object_type: &str,
    activity: &str,
) -> Result<BindingCapsule, AdapterError> {
    binding_capsule(
        client,
        BINDING_NEIGHBOR_PAIRS_SQL,
        &[
            &dataset_id,
            &tenant_id,
            &source_object_type,
            &target_object_type,
            &activity,
        ],
    )
    .await
}

/// Declares the typed relation consumed by a relation binding operator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationBindingSpec<'a> {
    pub source_object_type: &'a str,
    pub source_activity: &'a str,
    pub target_object_type: &'a str,
    pub target_activity: &'a str,
    pub related_object_type: &'a str,
}

/// Evaluate a workload-declared typed relation summary.
///
/// Each returned row contains the source object, its related object, and the
/// source event. A violation means at least one target binding for that source
/// is related to a different object. The relation summary must first be
/// declared through `ocpm.rebuild_binding_index(..., relation_specs)`.
pub async fn binding_relation_universal_equal(
    client: &Client,
    dataset_id: i64,
    tenant_id: i64,
    spec: &RelationBindingSpec<'_>,
) -> Result<BindingCapsule, AdapterError> {
    match binding_capsule(
        client,
        BINDING_RELATION_UNIVERSAL_EQUAL_SQL,
        &[
            &dataset_id,
            &tenant_id,
            &spec.source_object_type,
            &spec.source_activity,
            &spec.target_object_type,
            &spec.target_activity,
            &spec.related_object_type,
        ],
    )
    .await
    {
        Err(AdapterError::NullBindingCapsule) => Err(AdapterError::MissingRelationSummary),
        result => result,
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DfgCount {
    pub transition: TransitionKey,
    pub frequency: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VariantCount {
    pub path_hash: String,
    pub frequency: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WindowedDfgCount {
    pub transition: TransitionKey,
    pub source_object_type: String,
    pub target_object_type: String,
    pub frequencies: Vec<i64>,
}

/// Filters applied before lifecycle DFG counts leave PostgreSQL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LifecycleDfgFilter {
    pub object_types: Option<Vec<String>>,
    pub source_activities: Option<Vec<String>>,
    pub target_activities: Option<Vec<String>>,
    pub minimum_total_frequency: i64,
}

impl Default for LifecycleDfgFilter {
    fn default() -> Self {
        Self {
            object_types: None,
            source_activities: None,
            target_activities: None,
            minimum_total_frequency: 1,
        }
    }
}

/// Exact directly-follows frequencies for one object type across aligned windows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WindowedLifecycleDfgCount {
    pub object_type: String,
    pub transition: TransitionKey,
    pub frequencies: Vec<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WindowedVariantCount {
    pub object_type: String,
    pub path_hash: String,
    pub frequencies: Vec<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ActivityProfile {
    pub object_type: String,
    pub activity: String,
    pub case_frequency: i64,
    pub occurrence_frequency: i64,
    pub start_frequency: i64,
    pub end_frequency: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActivityProfileFilter {
    pub object_types: Option<Vec<String>>,
    pub statuses: Option<Vec<String>>,
    pub variant_hashes: Option<Vec<String>>,
    pub include_activities: Option<Vec<String>>,
    pub exclude_activities: Option<Vec<String>>,
    pub minimum_case_frequency: i64,
}

impl Default for ActivityProfileFilter {
    fn default() -> Self {
        Self {
            object_types: None,
            statuses: None,
            variant_hashes: None,
            include_activities: None,
            exclude_activities: None,
            minimum_case_frequency: 1,
        }
    }
}

/// Prepared statements are cached by tokio-postgres/PostgreSQL for repeated use;
/// callers should reuse the same client or a pool rather than reconnect per query.
pub async fn dfg_counts(
    client: &Client,
    dataset_id: i64,
    tenant_id: i64,
    from_timestamp: SystemTime,
    to_timestamp: SystemTime,
) -> Result<Vec<DfgCount>, AdapterError> {
    let statement = client.prepare_typed(DFG_COUNTS_SQL, &[]).await?;
    let rows = client
        .query(
            &statement,
            &[
                &dataset_id,
                &tenant_id,
                &from_timestamp,
                &to_timestamp,
                &Option::<Vec<String>>::None,
                &Option::<Vec<String>>::None,
                &Option::<Vec<String>>::None,
                &Option::<Vec<String>>::None,
                &Option::<Vec<String>>::None,
                &Option::<Vec<String>>::None,
                &1_i64,
            ],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| DfgCount {
            transition: TransitionKey {
                source: row.get(0),
                target: row.get(1),
                edge_type: row.get(2),
            },
            frequency: row.get(3),
        })
        .collect())
}

/// Fetch the exact variant-frequency sufficient statistics without moving
/// case paths through the middleware layer.
pub async fn variant_counts(
    client: &Client,
    dataset_id: i64,
    tenant_id: i64,
    from_timestamp: SystemTime,
    to_timestamp: SystemTime,
) -> Result<Vec<VariantCount>, AdapterError> {
    let statement = client.prepare(VARIANT_COUNTS_SQL).await?;
    let rows = client
        .query(
            &statement,
            &[
                &dataset_id,
                &tenant_id,
                &from_timestamp,
                &to_timestamp,
                &Option::<Vec<String>>::None,
                &Option::<Vec<String>>::None,
                &Option::<Vec<String>>::None,
                &Option::<Vec<String>>::None,
                &Option::<Vec<String>>::None,
                &1_i64,
            ],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| VariantCount {
            path_hash: row.get(0),
            frequency: row.get(1),
        })
        .collect())
}

/// Fetch aligned DFG sufficient statistics for multiple time windows in one
/// database request and one native aggregate state per transition group.
pub async fn dfg_window_counts(
    client: &Client,
    dataset_id: i64,
    tenant_id: i64,
    window_starts: Vec<SystemTime>,
    window_ends: Vec<SystemTime>,
) -> Result<Vec<WindowedDfgCount>, AdapterError> {
    let statement = client.prepare(DFG_WINDOW_COUNTS_SQL).await?;
    let rows = client
        .query(
            &statement,
            &[
                &dataset_id,
                &tenant_id,
                &window_starts,
                &window_ends,
                &Option::<Vec<String>>::None,
                &Option::<Vec<String>>::None,
                &Option::<Vec<String>>::None,
                &Option::<Vec<String>>::None,
                &Option::<Vec<String>>::None,
                &Option::<Vec<String>>::None,
                &1_i64,
            ],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| WindowedDfgCount {
            transition: TransitionKey {
                source: row.get(0),
                target: row.get(1),
                edge_type: row.get(4),
            },
            source_object_type: row.get(2),
            target_object_type: row.get(3),
            frequencies: row.get(5),
        })
        .collect())
}

/// Fetch exact lifecycle-contained DFG counts for arbitrary aligned windows.
///
/// PostgreSQL receives at most 256 windows per aggregate invocation. Larger
/// requests are chunked inside one statement and stitched into zero-filled,
/// aligned vectors without reconstructing events or case identifiers.
pub async fn lifecycle_dfg_window_counts(
    client: &Client,
    dataset_id: i64,
    tenant_id: i64,
    window_starts: &[SystemTime],
    window_ends: &[SystemTime],
    filter: &LifecycleDfgFilter,
) -> Result<Vec<WindowedLifecycleDfgCount>, AdapterError> {
    validate_event_windows(window_starts, window_ends)?;
    if filter.minimum_total_frequency < 1 {
        return Err(AdapterError::InvalidLifecycleDfgChunk);
    }
    let statement = client.prepare(LIFECYCLE_DFG_WINDOW_COUNTS_SQL).await?;
    let mut stream = std::pin::pin!(
        client
            .query_raw(
                &statement,
                [
                    &dataset_id as &(dyn ToSql + Sync),
                    &tenant_id,
                    &window_starts,
                    &window_ends,
                    &filter.object_types,
                    &filter.source_activities,
                    &filter.target_activities,
                ],
            )
            .await?
    );
    let mut counts = BTreeMap::<(String, String, String), Vec<i64>>::new();
    let mut seen_chunks = BTreeSet::<(i32, String, String, String)>::new();
    while let Some(row) = stream.try_next().await? {
        let first_window: i32 = row.try_get(0)?;
        let object_type: String = row.try_get(1)?;
        let source: String = row.try_get(2)?;
        let target: String = row.try_get(3)?;
        let frequencies: Vec<i64> = row.try_get(4)?;
        let offset = first_window
            .checked_sub(1)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or(AdapterError::InvalidLifecycleDfgChunk)?;
        let expected = window_starts.len().saturating_sub(offset).min(256);
        if expected == 0
            || frequencies.len() != expected
            || frequencies.iter().any(|frequency| *frequency < 0)
        {
            return Err(AdapterError::InvalidLifecycleDfgChunk);
        }
        if !seen_chunks.insert((
            first_window,
            object_type.clone(),
            source.clone(),
            target.clone(),
        )) {
            return Err(AdapterError::InvalidLifecycleDfgChunk);
        }
        let aligned = counts
            .entry((object_type, source, target))
            .or_insert_with(|| vec![0; window_starts.len()]);
        aligned[offset..offset + expected].copy_from_slice(&frequencies);
    }
    counts
        .into_iter()
        .filter_map(|((object_type, source, target), frequencies)| {
            let total = frequencies
                .iter()
                .try_fold(0_i64, |total, frequency| total.checked_add(*frequency));
            match total {
                Some(total) if total >= filter.minimum_total_frequency => {
                    Some(Ok(WindowedLifecycleDfgCount {
                        object_type,
                        transition: TransitionKey {
                            source,
                            target,
                            edge_type: "directly_follows".to_owned(),
                        },
                        frequencies,
                    }))
                }
                Some(_) => None,
                None => Some(Err(AdapterError::InvalidLifecycleDfgChunk)),
            }
        })
        .collect::<Result<Vec<_>, _>>()
}

/// Fetch aligned variant frequencies for multiple time windows without
/// transferring paths or case rows.
pub async fn variant_window_counts(
    client: &Client,
    dataset_id: i64,
    tenant_id: i64,
    window_starts: Vec<SystemTime>,
    window_ends: Vec<SystemTime>,
) -> Result<Vec<WindowedVariantCount>, AdapterError> {
    let statement = client.prepare(VARIANT_WINDOW_COUNTS_SQL).await?;
    let rows = client
        .query(
            &statement,
            &[
                &dataset_id,
                &tenant_id,
                &window_starts,
                &window_ends,
                &Option::<Vec<String>>::None,
                &Option::<Vec<String>>::None,
                &Option::<Vec<String>>::None,
                &Option::<Vec<String>>::None,
                &Option::<Vec<String>>::None,
                &1_i64,
            ],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| WindowedVariantCount {
            object_type: row.get(0),
            path_hash: row.get(1),
            frequencies: row.get(2),
        })
        .collect())
}

/// Fetch storage-neutral activity frequencies derived from filtered case
/// capsules. PostgreSQL applies every filter before the compact rows cross the
/// client boundary.
pub async fn activity_profile(
    client: &Client,
    dataset_id: i64,
    tenant_id: i64,
    from_timestamp: SystemTime,
    to_timestamp: SystemTime,
    filter: &ActivityProfileFilter,
) -> Result<Vec<ActivityProfile>, AdapterError> {
    let statement = client.prepare(ACTIVITY_PROFILE_SQL).await?;
    let rows = client
        .query(
            &statement,
            &[
                &dataset_id,
                &tenant_id,
                &from_timestamp,
                &to_timestamp,
                &filter.object_types,
                &filter.statuses,
                &filter.variant_hashes,
                &filter.include_activities,
                &filter.exclude_activities,
                &filter.minimum_case_frequency,
            ],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| ActivityProfile {
            object_type: row.get(0),
            activity: row.get(1),
            case_frequency: row.get(2),
            occurrence_frequency: row.get(3),
            start_frequency: row.get(4),
            end_frequency: row.get(5),
        })
        .collect())
}

/// Load one canonical pg_ocpm 1.0 snapshot for provider-neutral execution.
///
/// The caller supplies an already-authorized connection. This function sets the
/// session tenant scope before any canonical table read, verifies that the
/// dataset has a published generation, and validates the resulting OCEL before
/// returning it. High-volume aggregate APIs above remain preferable when an
/// algorithm can consume sufficient statistics directly.
pub async fn load_canonical_snapshot(
    client: &Client,
    tenant_id: i64,
    dataset_id: i64,
) -> Result<ocpm_core::CanonicalLog, AdapterError> {
    client
        .execute(
            "SELECT set_config('ocpm.tenant_id', $1, false)",
            &[&tenant_id.to_string()],
        )
        .await?;
    let dataset = client
        .query_opt(
            "SELECT dataset_name, source_watermark, active_generation
             FROM ocpm.dataset
             WHERE tenant_id=$1 AND dataset_id=$2
               AND active_generation IS NOT NULL",
            &[&tenant_id, &dataset_id],
        )
        .await?
        .ok_or(AdapterError::DatasetNotFound {
            dataset_id,
            tenant_id,
        })?;
    let source_watermark = dataset
        .get::<_, Option<SystemTime>>(1)
        .map(system_timestamp);

    let object_rows = client
        .query(
            "SELECT object_id, external_object_id, object_type
             FROM ocpm.object_entity
             WHERE tenant_id=$1 AND dataset_id=$2
             ORDER BY object_type, external_object_id",
            &[&tenant_id, &dataset_id],
        )
        .await?;
    let objects = object_rows
        .into_iter()
        .map(|row| {
            Ok(ocpm_core::Object {
                id: positive_id(row.get::<_, i64>(0))?,
                external_id: row.get(1),
                object_type: row.get(2),
            })
        })
        .collect::<Result<Vec<_>, AdapterError>>()?;

    let event_rows = client
        .query(
            "SELECT event_id, external_event_id, activity,
                    event_timestamp, event_submicro_nanos,
                    source_timestamp, event_sequence, lifecycle, attributes
             FROM ocpm.event_entity
             WHERE tenant_id=$1 AND dataset_id=$2
             ORDER BY event_timestamp, event_submicro_nanos,
                      event_sequence, external_event_id",
            &[&tenant_id, &dataset_id],
        )
        .await?;
    let events = event_rows
        .into_iter()
        .map(|row| {
            let base = system_timestamp(row.get(3));
            let submicro = row.get::<_, i16>(4) as i128;
            let source = row.get::<_, Option<String>>(5);
            let attributes = json_object_attributes(row.get(8))?;
            Ok(ocpm_core::Event {
                id: positive_id(row.get::<_, i64>(0))?,
                external_id: row.get(1),
                activity: row.get(2),
                timestamp: ocpm_core::Timestamp {
                    epoch_nanos_utc: base.epoch_nanos_utc + submicro,
                    source,
                },
                sequence: positive_or_zero(row.get::<_, i64>(6))?,
                lifecycle: row.get(7),
                attributes,
            })
        })
        .collect::<Result<Vec<_>, AdapterError>>()?;

    let e2o_rows = client
        .query(
            "SELECT relation_id, event_id, object_id, qualifier
             FROM ocpm.event_object_relation
             WHERE tenant_id=$1 AND dataset_id=$2
             ORDER BY relation_id",
            &[&tenant_id, &dataset_id],
        )
        .await?;
    let event_object_relations = e2o_rows
        .into_iter()
        .map(|row| {
            Ok(ocpm_core::EventObjectRelation {
                relation_id: positive_id(row.get::<_, i64>(0))?,
                event_id: positive_id(row.get::<_, i64>(1))?,
                object_id: positive_id(row.get::<_, i64>(2))?,
                qualifier: row.get(3),
            })
        })
        .collect::<Result<Vec<_>, AdapterError>>()?;

    let o2o_rows = client
        .query(
            "SELECT relation_id, source_object_id, target_object_id,
                    qualifier, valid_from, valid_to
             FROM ocpm.object_object_relation
             WHERE tenant_id=$1 AND dataset_id=$2
             ORDER BY relation_id",
            &[&tenant_id, &dataset_id],
        )
        .await?;
    let object_object_relations = o2o_rows
        .into_iter()
        .map(|row| {
            Ok(ocpm_core::ObjectObjectRelation {
                relation_id: positive_id(row.get::<_, i64>(0))?,
                source_object_id: positive_id(row.get::<_, i64>(1))?,
                target_object_id: positive_id(row.get::<_, i64>(2))?,
                qualifier: row.get(3),
                valid_from: row.get::<_, Option<SystemTime>>(4).map(system_timestamp),
                valid_to: row.get::<_, Option<SystemTime>>(5).map(system_timestamp),
            })
        })
        .collect::<Result<Vec<_>, AdapterError>>()?;

    let attribute_rows = client
        .query(
            "SELECT object_id, attribute_name, valid_from, value
             FROM ocpm.object_attribute_history
             WHERE tenant_id=$1 AND dataset_id=$2
             ORDER BY object_id, attribute_name, valid_from",
            &[&tenant_id, &dataset_id],
        )
        .await?;
    let object_attribute_history = attribute_rows
        .into_iter()
        .map(|row| {
            Ok(ocpm_core::ObjectAttributeChange {
                object_id: positive_id(row.get::<_, i64>(0))?,
                name: row.get(1),
                valid_from: system_timestamp(row.get(2)),
                value: json_attribute(row.get(3))?,
            })
        })
        .collect::<Result<Vec<_>, AdapterError>>()?;

    let mut log = ocpm_core::CanonicalLog {
        dataset_id: dataset.get(0),
        tenant_id: tenant_id.to_string(),
        source_watermark,
        events,
        objects,
        event_object_relations,
        object_object_relations,
        object_attribute_history,
        metadata: BTreeMap::from([(
            "pg_ocpm_generation".to_owned(),
            serde_json::json!(dataset.get::<_, i64>(2)),
        )]),
    };
    log.validate().map_err(|error| {
        AdapterError::InvalidBindingIndexCoverage(format!(
            "canonical provider contract violation: {error}"
        ))
    })?;
    log.sort_canonical();
    Ok(log)
}

fn positive_id(value: i64) -> Result<u64, AdapterError> {
    u64::try_from(value).map_err(|_| {
        AdapterError::InvalidBindingIndexCoverage("canonical ID must be nonnegative".to_owned())
    })
}

fn positive_or_zero(value: i64) -> Result<u64, AdapterError> {
    positive_id(value)
}

fn system_timestamp(value: SystemTime) -> ocpm_core::Timestamp {
    let nanos = match value.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos() as i128,
        Err(error) => -(error.duration().as_nanos() as i128),
    };
    ocpm_core::Timestamp::from_epoch_nanos(nanos)
}

fn json_object_attributes(
    value: serde_json::Value,
) -> Result<BTreeMap<String, ocpm_core::AttributeValue>, AdapterError> {
    let serde_json::Value::Object(values) = value else {
        return Err(AdapterError::InvalidBindingIndexCoverage(
            "canonical event attributes must be a JSON object".to_owned(),
        ));
    };
    values
        .into_iter()
        .map(|(name, value)| Ok((name, json_attribute(value)?)))
        .collect()
}

fn json_attribute(
    value: serde_json::Value,
) -> Result<ocpm_core::AttributeValue, AdapterError> {
    match value {
        serde_json::Value::Null => Ok(ocpm_core::AttributeValue::Null),
        serde_json::Value::String(value) => Ok(ocpm_core::AttributeValue::String(value)),
        serde_json::Value::Bool(value) => Ok(ocpm_core::AttributeValue::Boolean(value)),
        serde_json::Value::Number(value) => value
            .as_i64()
            .map(ocpm_core::AttributeValue::Integer)
            .or_else(|| value.as_f64().map(ocpm_core::AttributeValue::Float))
            .ok_or_else(|| {
                AdapterError::InvalidBindingIndexCoverage(
                    "canonical numeric attribute is outside supported range".to_owned(),
                )
            }),
        _ => Err(AdapterError::InvalidBindingIndexCoverage(
            "canonical attributes must be scalar JSON values".to_owned(),
        )),
    }
}
