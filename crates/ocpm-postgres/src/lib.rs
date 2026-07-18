//! Thin asynchronous adapter over pg_ocpm 0.3 aggregate interfaces.

use ocpm_core::TransitionKey;
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

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
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
