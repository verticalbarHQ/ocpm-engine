//! Thin asynchronous adapter over pg_ocpm 0.5 aggregate and binding interfaces.

use ocpm_core::{
    TransitionKey,
    binding::{BindingCapsule, BindingDecodeError},
};
use std::time::SystemTime;
use thiserror::Error;
use tokio_postgres::Client;

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

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
    #[error(transparent)]
    Binding(#[from] BindingDecodeError),
}

async fn binding_capsule(
    client: &Client,
    sql: &str,
    parameters: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<BindingCapsule, AdapterError> {
    let statement = client.prepare(sql).await?;
    let row = client.query_one(&statement, parameters).await?;
    let bytes: Vec<u8> = row.get(0);
    Ok(BindingCapsule::decode(&bytes)?)
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
