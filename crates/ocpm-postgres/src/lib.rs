//! Thin asynchronous adapter over pg_ocpm 0.7 aggregate and binding interfaces.

use ocpm_core::{
    TransitionKey,
    binding::{BindingCapsule, BindingDecodeError},
};
use std::time::SystemTime;
use thiserror::Error;
use tokio_postgres::{
    Client, Statement,
    types::{ToSql, Type},
};

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
