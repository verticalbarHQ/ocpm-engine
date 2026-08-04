use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DuckDbDatabase {
    /// Open an existing DuckDB database file. Read-only is recommended for
    /// independently scaled query workers over immutable Parquet snapshots.
    Existing { path: PathBuf, read_only: bool },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ParquetLocation {
    Local {
        root: PathBuf,
    },
    S3 {
        uri: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        region: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        endpoint: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        url_style: Option<S3UrlStyle>,
        #[serde(skip_serializing_if = "Option::is_none")]
        use_ssl: Option<bool>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum S3UrlStyle {
    Path,
    Vhost,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SnapshotSelection {
    Root,
    Current {
        #[serde(default = "default_pointer")]
        pointer: String,
    },
    Fixed {
        version: String,
    },
}

fn default_pointer() -> String {
    "CURRENT".to_owned()
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceValidationPolicy {
    Fast,
    #[default]
    Balanced,
    Strict,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AmbiguousTimePolicy {
    #[default]
    Reject,
    Earliest,
    Latest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SourceTimestampPolicy {
    Utc,
    FixedOffset {
        seconds_east: i32,
    },
    Iana {
        zone: String,
        #[serde(default)]
        ambiguous: AmbiguousTimePolicy,
    },
}

impl Default for SourceTimestampPolicy {
    fn default() -> Self {
        Self::Utc
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EntityLinkSnapshotV1 {
    #[serde(default = "event_log_file")]
    pub event_log_file: String,
    #[serde(default = "object_file")]
    pub object_file: String,
    #[serde(default = "object_link_file")]
    pub object_link_file: String,
    #[serde(default = "event_group_file")]
    pub event_group_file: String,
    pub dataset_id: String,
    pub tenant_id: i64,
    pub timestamp_policy: SourceTimestampPolicy,
}

fn event_log_file() -> String {
    "event_log.parquet".to_owned()
}

fn object_file() -> String {
    "txn.parquet".to_owned()
}

fn object_link_file() -> String {
    "object_link.parquet".to_owned()
}

fn event_group_file() -> String {
    "case_event_group.parquet".to_owned()
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "config", rename_all = "snake_case")]
pub enum ParquetLayout {
    CanonicalV1,
    EntityLinkSnapshotV1(EntityLinkSnapshotV1),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S3CredentialReference {
    #[serde(default = "default_chain")]
    pub chain: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
}

fn default_chain() -> String {
    "env;config;sts;instance".to_owned()
}

impl Default for S3CredentialReference {
    fn default() -> Self {
        Self {
            chain: default_chain(),
            profile: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionPolicy {
    #[default]
    Preinstalled,
    InstallCore,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ParquetCachePolicy {
    /// Scan the immutable Parquet snapshot directly. Repeated exact aggregate
    /// results may still use the separately byte-bounded in-process cache.
    Direct,
}

impl Default for ParquetCachePolicy {
    fn default() -> Self {
        Self::Direct
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DuckDbOptions {
    #[serde(default = "default_memory")]
    pub memory_budget_bytes: u64,
    #[serde(default = "default_parallelism")]
    pub max_parallelism: usize,
    #[serde(default = "default_pool")]
    pub connection_pool_size: usize,
    #[serde(default = "default_temp_bytes")]
    pub max_temp_bytes: u64,
    /// Maximum heap bytes retained for exact aggregate results. Entries are
    /// scoped to one immutable snapshot and evicted least-recently-used.
    /// Set to zero to measure or operate without a result cache.
    #[serde(default = "default_result_cache_bytes")]
    pub result_cache_bytes: u64,
    /// Retain one canonical in-process representation after an operation that
    /// cannot be pushed into DuckDB. Disable this to minimize resident memory
    /// when fallback operations are rare.
    #[serde(default = "default_cache_canonical_fallback")]
    pub cache_canonical_fallback: bool,
    /// Build a connection-local, lifecycle-ordered execution relation at open
    /// time. This trades startup time and DuckDB-managed memory for lower
    /// latency on varied lifecycle, DFG, variant, and prediction requests.
    #[serde(default)]
    pub materialize_execution_relation: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temp_directory: Option<PathBuf>,
    #[serde(default)]
    pub extension_policy: ExtensionPolicy,
}

const fn default_memory() -> u64 {
    512 * 1024 * 1024
}

fn default_parallelism() -> usize {
    std::thread::available_parallelism()
        .map_or(1, usize::from)
        .min(8)
}

const fn default_pool() -> usize {
    4
}

const fn default_temp_bytes() -> u64 {
    4 * 1024 * 1024 * 1024
}

const fn default_result_cache_bytes() -> u64 {
    64 * 1024 * 1024
}

const fn default_cache_canonical_fallback() -> bool {
    true
}

impl Default for DuckDbOptions {
    fn default() -> Self {
        Self {
            memory_budget_bytes: default_memory(),
            max_parallelism: default_parallelism(),
            connection_pool_size: default_pool(),
            max_temp_bytes: default_temp_bytes(),
            result_cache_bytes: default_result_cache_bytes(),
            cache_canonical_fallback: default_cache_canonical_fallback(),
            materialize_execution_relation: false,
            temp_directory: None,
            extension_policy: ExtensionPolicy::Preinstalled,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DuckDbParquetSource {
    pub database: DuckDbDatabase,
    pub location: ParquetLocation,
    #[serde(default = "default_snapshot")]
    pub snapshot: SnapshotSelection,
    pub layout: ParquetLayout,
    #[serde(default)]
    pub cache: ParquetCachePolicy,
    #[serde(default)]
    pub validation: SourceValidationPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials: Option<S3CredentialReference>,
    #[serde(default)]
    pub options: DuckDbOptions,
}

fn default_snapshot() -> SnapshotSelection {
    SnapshotSelection::Current {
        pointer: default_pointer(),
    }
}
