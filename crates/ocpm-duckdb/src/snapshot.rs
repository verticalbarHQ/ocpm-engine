#[cfg(feature = "s3")]
use crate::S3UrlStyle;
use crate::{
    AmbiguousTimePolicy, DuckDbDatabase, DuckDbOptions, DuckDbParquetSource, DuckDbProviderError,
    EntityLinkSnapshotV1, ExtensionPolicy, ParquetLayout, ParquetLocation, S3CredentialReference,
    SnapshotSelection, SourceTimestampPolicy, SourceValidationPolicy, lowercase_hex, quote_literal,
};
use duckdb::{AccessMode, Config, Connection, params};
use ocpm_core::Timestamp;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::Read,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug)]
pub(crate) struct ResolvedSnapshot {
    pub version: String,
    pub version_root: String,
    pub dataset_id: String,
    pub tenant_id: String,
    pub source_watermark: Option<Timestamp>,
    pub manifest_sha256: String,
    pub timestamp_policy: SourceTimestampPolicy,
    pub layout_name: &'static str,
    pub is_s3: bool,
}

pub(crate) fn open_database(
    source: &DuckDbParquetSource,
) -> Result<(Connection, ResolvedSnapshot), DuckDbProviderError> {
    validate_options(&source.options)?;
    let mut config = client_config(source)?;
    let DuckDbDatabase::Existing { path, read_only } = &source.database;
    if !path.is_file() {
        return Err(DuckDbProviderError::InvalidSource(format!(
            "existing DuckDB database does not exist: {}",
            path.display()
        )));
    }
    config = config.access_mode(if *read_only {
        AccessMode::ReadOnly
    } else {
        AccessMode::ReadWrite
    })?;
    initialize_database(Connection::open_with_flags(path, config)?, source)
}

pub(crate) fn open_isolated_database(
    source: &DuckDbParquetSource,
) -> Result<(Connection, ResolvedSnapshot), DuckDbProviderError> {
    validate_options(&source.options)?;
    initialize_database(
        Connection::open_with_flags(":memory:", client_config(source)?)?,
        source,
    )
}

fn client_config(source: &DuckDbParquetSource) -> Result<Config, DuckDbProviderError> {
    Ok(Config::default()
        .max_memory(&format!("{}B", source.options.memory_budget_bytes))?
        .threads(i64::try_from(source.options.max_parallelism).map_err(|_| {
            DuckDbProviderError::InvalidSource("max_parallelism does not fit i64".to_owned())
        })?)?)
}

fn initialize_database(
    connection: Connection,
    source: &DuckDbParquetSource,
) -> Result<(Connection, ResolvedSnapshot), DuckDbProviderError> {
    validate_client_version(&connection)?;
    configure_resources(&connection, &source.options)?;
    let is_s3 = matches!(source.location, ParquetLocation::S3 { .. });
    if is_s3 {
        configure_s3(
            &connection,
            &source.location,
            source.credentials.as_ref(),
            source.options.extension_policy,
        )?;
    }
    let resolved = resolve_snapshot(&connection, source)?;
    configure_allowed_source(&connection, &resolved, source)?;
    create_views(&connection, &resolved, &source.layout)?;
    validate_views(&connection, source.validation)?;
    create_execution_relation(&connection, source.options.materialize_execution_relation)?;
    lock_security_configuration(&connection)?;
    Ok((connection, resolved))
}

fn create_execution_relation(
    connection: &Connection,
    materialize: bool,
) -> Result<(), DuckDbProviderError> {
    let relation = r#"
        SELECT
          o.object_id,
          o.external_object_id,
          o.object_type,
          r.qualifier,
          e.event_id,
          e.external_event_id,
          e.activity,
          e.timestamp_nanos_utc,
          e.source_timestamp,
          e.sequence,
          e.lifecycle,
          e.attributes_json,
          min(e.timestamp_nanos_utc) OVER (PARTITION BY o.object_id) AS lifecycle_start,
          max(e.timestamp_nanos_utc) OVER (PARTITION BY o.object_id) AS lifecycle_end
        FROM ocpm_objects o
        JOIN ocpm_e2o r ON r.object_id=o.object_id
        JOIN ocpm_events e ON e.event_id=r.event_id
    "#;
    if materialize {
        connection.execute_batch(&format!(
            "CREATE TEMP TABLE ocpm_execution_events AS {relation} \
             ORDER BY object_id,timestamp_nanos_utc,sequence,external_event_id"
        ))?;
    } else {
        connection.execute_batch(&format!(
            "CREATE TEMP VIEW ocpm_execution_events AS {relation}"
        ))?;
    }
    Ok(())
}

fn validate_client_version(connection: &Connection) -> Result<(), DuckDbProviderError> {
    let version = connection.query_row("SELECT version()", [], |row| row.get::<_, String>(0))?;
    if !version.starts_with("v1.5.") {
        return Err(DuckDbProviderError::InvalidSource(format!(
            "unsupported DuckDB client {version}; this build requires DuckDB 1.5.x"
        )));
    }
    Ok(())
}

fn validate_options(options: &DuckDbOptions) -> Result<(), DuckDbProviderError> {
    if options.memory_budget_bytes < 16 * 1024 * 1024 {
        return Err(DuckDbProviderError::InvalidSource(
            "memory_budget_bytes must be at least 16 MiB".to_owned(),
        ));
    }
    if options.max_parallelism == 0 || options.connection_pool_size == 0 {
        return Err(DuckDbProviderError::InvalidSource(
            "max_parallelism and connection_pool_size must be positive".to_owned(),
        ));
    }
    if options.max_temp_bytes == 0 {
        return Err(DuckDbProviderError::InvalidSource(
            "max_temp_bytes must be positive".to_owned(),
        ));
    }
    Ok(())
}

fn configure_resources(
    connection: &Connection,
    options: &DuckDbOptions,
) -> Result<(), DuckDbProviderError> {
    connection.execute_batch(&format!(
        "SET max_temp_directory_size = {}; SET preserve_insertion_order = false;",
        quote_literal(&format!("{}B", options.max_temp_bytes))
    ))?;
    if let Some(directory) = &options.temp_directory {
        fs::create_dir_all(directory)?;
        let canonical = directory.canonicalize()?;
        connection.execute_batch(&format!(
            "SET temp_directory = {}",
            quote_literal(&canonical.to_string_lossy())
        ))?;
    }
    connection.execute_batch(
        "SET allow_community_extensions = false; SET allow_unsigned_extensions = false;",
    )?;
    Ok(())
}

fn configure_s3(
    connection: &Connection,
    location: &ParquetLocation,
    credentials: Option<&S3CredentialReference>,
    extension_policy: ExtensionPolicy,
) -> Result<(), DuckDbProviderError> {
    #[cfg(not(feature = "s3"))]
    {
        let _ = (connection, location, credentials, extension_policy);
        Err(DuckDbProviderError::Unsupported(
            "S3 support requires the ocpm-duckdb s3 feature".to_owned(),
        ))
    }
    #[cfg(feature = "s3")]
    {
        if extension_policy == ExtensionPolicy::InstallCore {
            connection.execute_batch("INSTALL httpfs; INSTALL aws;")?;
        }
        connection.execute_batch("LOAD httpfs; LOAD aws;")?;
        let ParquetLocation::S3 {
            region,
            endpoint,
            url_style,
            use_ssl,
            ..
        } = location
        else {
            return Ok(());
        };
        let credentials = credentials.cloned().unwrap_or_default();
        validate_chain(&credentials.chain)?;
        let mut fields = vec![
            "TYPE s3".to_owned(),
            "PROVIDER credential_chain".to_owned(),
            format!("CHAIN {}", quote_literal(&credentials.chain)),
        ];
        if let Some(profile) = credentials.profile {
            validate_plain_token("profile", &profile)?;
            fields.push(format!("PROFILE {}", quote_literal(&profile)));
        }
        if let Some(region) = region {
            validate_plain_token("region", region)?;
            fields.push(format!("REGION {}", quote_literal(region)));
        }
        if let Some(endpoint) = endpoint {
            validate_endpoint(endpoint)?;
            fields.push(format!("ENDPOINT {}", quote_literal(endpoint)));
        }
        if let Some(url_style) = url_style {
            fields.push(format!(
                "URL_STYLE {}",
                match url_style {
                    S3UrlStyle::Path => "path",
                    S3UrlStyle::Vhost => "vhost",
                }
            ));
        }
        if let Some(use_ssl) = use_ssl {
            fields.push(format!("USE_SSL {}", use_ssl));
        }
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TEMPORARY SECRET ocpm_s3 ({})",
            fields.join(", ")
        ))?;
        connection.execute_batch(
            "SET enable_http_metadata_cache = true; SET s3_version_id_pinning = true;",
        )?;
        Ok(())
    }
}

#[cfg(feature = "s3")]
fn validate_chain(chain: &str) -> Result<(), DuckDbProviderError> {
    const ALLOWED: &[&str] = &["config", "sts", "sso", "env", "instance", "process"];
    let values = chain.split(';').collect::<Vec<_>>();
    if values.is_empty() || values.iter().any(|value| !ALLOWED.contains(value)) {
        return Err(DuckDbProviderError::InvalidSource(
            "credential chain contains an unsupported provider".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(feature = "s3")]
fn validate_plain_token(name: &str, value: &str) -> Result<(), DuckDbProviderError> {
    if value.is_empty()
        || value.len() > 256
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/' | b':')
        })
    {
        return Err(DuckDbProviderError::InvalidSource(format!(
            "invalid S3 {name}"
        )));
    }
    Ok(())
}

#[cfg(feature = "s3")]
fn validate_endpoint(value: &str) -> Result<(), DuckDbProviderError> {
    validate_plain_token("endpoint", value)
}

fn resolve_snapshot(
    connection: &Connection,
    source: &DuckDbParquetSource,
) -> Result<ResolvedSnapshot, DuckDbProviderError> {
    let (root, is_s3) = match &source.location {
        ParquetLocation::Local { root } => {
            let canonical = root.canonicalize().map_err(|error| {
                DuckDbProviderError::InvalidSource(format!(
                    "local source root cannot be resolved: {error}"
                ))
            })?;
            (canonical.to_string_lossy().into_owned(), false)
        }
        ParquetLocation::S3 { uri, .. } => (validate_s3_uri(uri)?, true),
    };
    let version = match &source.snapshot {
        SnapshotSelection::Root => "root".to_owned(),
        SnapshotSelection::Fixed { version } => {
            validate_version(version)?;
            version.clone()
        }
        SnapshotSelection::Current { pointer } => {
            validate_relative_file(pointer)?;
            let pointer_uri = join_uri(&root, pointer);
            let content = if is_s3 {
                connection.query_row(
                    "SELECT content FROM read_text(?)",
                    params![pointer_uri],
                    |row| row.get::<_, String>(0),
                )?
            } else {
                fs::read_to_string(pointer_uri)?
            };
            let version = content.trim();
            validate_version(version)?;
            version.to_owned()
        }
    };
    let version_root = match source.snapshot {
        SnapshotSelection::Root => root.clone(),
        _ => join_uri(&join_uri(&root, "versions"), &version),
    };
    if !is_s3 {
        let path = PathBuf::from(&version_root).canonicalize()?;
        let root_path = PathBuf::from(&root);
        if !path.starts_with(&root_path) {
            return Err(DuckDbProviderError::InvalidSource(
                "resolved snapshot escapes its configured root".to_owned(),
            ));
        }
    }
    let manifest_uri = join_uri(&version_root, "manifest.json");
    let manifest_bytes = if is_s3 {
        connection
            .query_row(
                "SELECT content FROM read_text(?)",
                params![manifest_uri],
                |row| row.get::<_, String>(0),
            )?
            .into_bytes()
    } else {
        fs::read(manifest_uri)?
    };
    let manifest_json: JsonValue = serde_json::from_slice(&manifest_bytes)?;
    let manifest_sha256 = lowercase_hex(Sha256::digest(&manifest_bytes));
    let (dataset_id, tenant_id, watermark, timestamp_policy, layout_name) = match &source.layout {
        ParquetLayout::CanonicalV1 => {
            let manifest: crate::CanonicalParquetManifest =
                serde_json::from_value(manifest_json.clone())?;
            if manifest.layout_version != "canonical_parquet_v1" {
                return Err(DuckDbProviderError::InvalidSource(format!(
                    "unsupported canonical layout {}",
                    manifest.layout_version
                )));
            }
            if !matches!(source.snapshot, SnapshotSelection::Root)
                && manifest.snapshot_version != version
            {
                return Err(DuckDbProviderError::InvalidSource(
                    "manifest snapshot version does not match resolved version".to_owned(),
                ));
            }
            validate_canonical_manifest(
                connection,
                &version_root,
                is_s3,
                source.validation,
                &manifest,
            )?;
            (
                manifest.dataset_id,
                manifest.tenant_id,
                manifest.source_watermark,
                SourceTimestampPolicy::Utc,
                "canonical_v1",
            )
        }
        ParquetLayout::EntityLinkSnapshotV1(layout) => (
            layout.dataset_id.clone(),
            layout.tenant_id.to_string(),
            manifest_watermark(&manifest_json, &layout.timestamp_policy),
            layout.timestamp_policy.clone(),
            "entity_link_snapshot_v1",
        ),
    };
    Ok(ResolvedSnapshot {
        version,
        version_root,
        dataset_id,
        tenant_id,
        source_watermark: watermark,
        manifest_sha256,
        timestamp_policy,
        layout_name,
        is_s3,
    })
}

fn validate_canonical_manifest(
    connection: &Connection,
    version_root: &str,
    is_s3: bool,
    policy: SourceValidationPolicy,
    manifest: &crate::CanonicalParquetManifest,
) -> Result<(), DuckDbProviderError> {
    const REQUIRED: [(&str, &str); 5] = [
        ("events", "events/events.parquet"),
        ("objects", "objects/objects.parquet"),
        (
            "event_object_relations",
            "event_object_relations/event_object_relations.parquet",
        ),
        (
            "object_object_relations",
            "object_object_relations/object_object_relations.parquet",
        ),
        (
            "object_attribute_history",
            "object_attribute_history/object_attribute_history.parquet",
        ),
    ];
    if manifest.files.len() != REQUIRED.len() {
        return Err(DuckDbProviderError::InvalidSource(
            "canonical manifest must contain exactly the five canonical relation files".to_owned(),
        ));
    }
    for (relation, expected_path) in REQUIRED {
        let file = manifest.files.get(relation).ok_or_else(|| {
            DuckDbProviderError::InvalidSource(format!("canonical manifest is missing {relation}"))
        })?;
        if file.path != expected_path {
            return Err(DuckDbProviderError::InvalidSource(format!(
                "canonical manifest path for {relation} is not the canonical path"
            )));
        }
        validate_relative_file(&file.path)?;
        if file.sha256.len() != 64 || !file.sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(DuckDbProviderError::InvalidSource(format!(
                "canonical manifest hash for {relation} is invalid"
            )));
        }
        if policy != SourceValidationPolicy::Strict {
            continue;
        }
        let uri = join_uri(version_root, &file.path);
        let actual = if is_s3 {
            connection.query_row(
                "SELECT sha256(content) FROM read_blob(?)",
                params![uri],
                |row| row.get::<_, String>(0),
            )?
        } else {
            file_sha256(Path::new(&uri))?
        };
        if !actual.eq_ignore_ascii_case(&file.sha256) {
            return Err(DuckDbProviderError::InvalidSource(format!(
                "canonical Parquet hash mismatch for {relation}"
            )));
        }
    }
    Ok(())
}

fn file_sha256(path: &Path) -> Result<String, DuckDbProviderError> {
    let mut file = fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let bytes = file.read(&mut buffer)?;
        if bytes == 0 {
            break;
        }
        digest.update(&buffer[..bytes]);
    }
    Ok(lowercase_hex(digest.finalize()))
}

fn manifest_watermark(value: &JsonValue, policy: &SourceTimestampPolicy) -> Option<Timestamp> {
    let text = value
        .get("freezeT")
        .or_else(|| value.pointer("/baseline/freezeT"))?
        .as_str()?;
    crate::source_timestamp_to_utc(text, policy).ok()
}

fn configure_allowed_source(
    connection: &Connection,
    resolved: &ResolvedSnapshot,
    source: &DuckDbParquetSource,
) -> Result<(), DuckDbProviderError> {
    let mut roots = vec![resolved.version_root.clone()];
    if let Some(temp) = &source.options.temp_directory {
        roots.push(temp.canonicalize()?.to_string_lossy().into_owned());
    }
    let values = roots
        .iter()
        .map(|root| quote_literal(root))
        .collect::<Vec<_>>()
        .join(",");
    connection.execute_batch(&format!("SET allowed_directories = [{values}]"))?;
    Ok(())
}

fn create_views(
    connection: &Connection,
    resolved: &ResolvedSnapshot,
    layout: &ParquetLayout,
) -> Result<(), DuckDbProviderError> {
    match layout {
        ParquetLayout::CanonicalV1 => create_canonical_views(connection, resolved),
        ParquetLayout::EntityLinkSnapshotV1(config) => {
            create_entity_link_views(connection, resolved, config)
        }
    }
}

fn create_canonical_views(
    connection: &Connection,
    resolved: &ResolvedSnapshot,
) -> Result<(), DuckDbProviderError> {
    let path = |value: &str| quote_literal(&join_uri(&resolved.version_root, value));
    connection.execute_batch(&format!(
        r#"
        CREATE TEMP VIEW ocpm_events AS
          SELECT * FROM read_parquet({}, union_by_name=true);
        CREATE TEMP VIEW ocpm_objects AS
          SELECT * FROM read_parquet({}, union_by_name=true);
        CREATE TEMP VIEW ocpm_e2o AS
          SELECT * FROM read_parquet({}, union_by_name=true);
        CREATE TEMP VIEW ocpm_o2o AS
          SELECT * FROM read_parquet({}, union_by_name=true);
        CREATE TEMP VIEW ocpm_object_attributes AS
          SELECT * FROM read_parquet({}, union_by_name=true);
        "#,
        path("events/*.parquet"),
        path("objects/*.parquet"),
        path("event_object_relations/*.parquet"),
        path("object_object_relations/*.parquet"),
        path("object_attribute_history/*.parquet"),
    ))?;
    Ok(())
}

fn create_entity_link_views(
    connection: &Connection,
    resolved: &ResolvedSnapshot,
    config: &EntityLinkSnapshotV1,
) -> Result<(), DuckDbProviderError> {
    for value in [
        &config.event_log_file,
        &config.object_file,
        &config.object_link_file,
        &config.event_group_file,
    ] {
        validate_relative_file(value)?;
    }
    let event_path = quote_literal(&join_uri(&resolved.version_root, &config.event_log_file));
    let object_path = quote_literal(&join_uri(&resolved.version_root, &config.object_file));
    let link_path = quote_literal(&join_uri(&resolved.version_root, &config.object_link_file));
    let group_path = quote_literal(&join_uri(&resolved.version_root, &config.event_group_file));
    let tenant = config.tenant_id;
    connection.execute_batch(&format!(
        r#"
        CREATE TEMP VIEW ocpm_entity_events_raw AS
          SELECT
            case_id,
            timestamp AS event_timestamp,
            CAST(json_extract_string(display, '$.system_note_id') AS UBIGINT) AS stable_event_id,
            activity_id,
            object_type AS event_object_type,
            CAST(object_id AS UBIGINT) AS event_object_id,
            display
          FROM read_parquet({event_path})
          WHERE tenant_id = {tenant};

        CREATE TEMP VIEW ocpm_entity_groups_raw AS
          SELECT case_id,timestamp AS event_timestamp,system_note_ids,time_rank
          FROM read_parquet({group_path})
          WHERE tenant_id = {tenant};

        CREATE TEMP VIEW ocpm_events AS
        WITH groups AS (
          -- Membership is flattened to one scalar row per (physical group row, member), carrying
          -- the member's 1-based ordinal. `generate_subscripts` aligns with `unnest` and is
          -- 1-based, so the ordinal equals what `list_position` returned.
          --
          -- This is what lets the join below be an equi-join. Testing membership with
          -- `list_contains` inside the ON clause makes DuckDB discard the equi-keys and plan a
          -- BLOCKWISE_NL_JOIN, because a list predicate cannot be a hash key: on a 6.4M-event
          -- snapshot with 5.0M group rows that is 3.2e13 comparisons and does not complete.
          --
          -- `min(member_ordinal)` per (row, member) is REQUIRED for equivalence, not a tidiness
          -- pass: `list_position` returns the FIRST occurrence, so a list carrying the same id
          -- twice matched once before and would otherwise emit one joined row per occurrence.
          -- The grouping key includes a per-row identity so that two DISTINCT group rows holding
          -- the same member still fan out exactly as they did before — a global DISTINCT would
          -- silently collapse them.
          --
          -- The tenant predicate stays on the parquet scan rather than moving above the LATERAL,
          -- so row-group pruning is not lost.
          SELECT
            g.case_id,
            g.event_timestamp,
            g.time_rank,
            u.member_id,
            min(u.member_ordinal) AS member_ordinal
          FROM (
            SELECT row_number() OVER () AS group_row_id, *
            FROM ocpm_entity_groups_raw
          ) g,
          LATERAL (
            SELECT
              unnest(g.system_note_ids) AS member_id,
              generate_subscripts(g.system_note_ids, 1) AS member_ordinal
          ) u
          GROUP BY
            g.group_row_id,g.case_id,g.event_timestamp,g.time_rank,u.member_id
        )
        SELECT
          events.stable_event_id AS event_id,
          CAST(events.stable_event_id AS VARCHAR) AS external_event_id,
          events.activity_id AS activity,
          epoch_ns(events.event_timestamp)::HUGEINT AS timestamp_nanos_utc,
          CAST(events.event_timestamp AS VARCHAR) AS source_timestamp,
          CAST(
            coalesce(groups.time_rank, 0) * 1000000
            + coalesce(groups.member_ordinal, 0)
            AS UBIGINT
          ) AS sequence,
          NULL::VARCHAR AS lifecycle,
          events.display AS attributes_json
        FROM ocpm_entity_events_raw events
        LEFT JOIN groups
          ON groups.case_id = events.case_id
         AND groups.event_timestamp = events.event_timestamp
         AND groups.member_id = events.stable_event_id;

        CREATE TEMP VIEW ocpm_objects AS
        SELECT
          CAST(id AS UBIGINT) AS object_id,
          CAST(id AS VARCHAR) AS external_object_id,
          type AS object_type
        FROM read_parquet({object_path})
        WHERE tenant_id = {tenant};

        CREATE TEMP VIEW ocpm_e2o AS
        SELECT
          CAST(json_extract_string(display, '$.system_note_id') AS UBIGINT) AS relation_id,
          CAST(json_extract_string(display, '$.system_note_id') AS UBIGINT) AS event_id,
          CAST(object_id AS UBIGINT) AS object_id,
          object_type AS qualifier
        FROM read_parquet({event_path})
        WHERE tenant_id = {tenant};

        CREATE TEMP VIEW ocpm_o2o AS
        SELECT
          row_number() OVER (
            ORDER BY source_object_id, target_object_id, source_object_type, target_object_type
          )::UBIGINT AS relation_id,
          CAST(source_object_id AS UBIGINT) AS source_object_id,
          CAST(target_object_id AS UBIGINT) AS target_object_id,
          source_object_type || '->' || target_object_type AS qualifier,
          NULL::HUGEINT AS valid_from_nanos_utc,
          NULL::HUGEINT AS valid_to_nanos_utc
        FROM read_parquet({link_path})
        WHERE tenant_id = {tenant};

        CREATE TEMP VIEW ocpm_object_attributes AS
        SELECT
          CAST(id AS UBIGINT) AS object_id,
          attribute_name AS name,
          9223372036854775807::HUGEINT AS valid_from_nanos_utc,
          json_object('type', 'string', 'value', attribute_value)::VARCHAR AS value_json
        FROM (
          UNPIVOT (
            SELECT * FROM read_parquet({object_path}) WHERE tenant_id = {tenant}
          )
          ON status, currency, subsidiary, trandate, recordtype,
             customform, entity, postingperiod, lastmodifiedby
          INTO NAME attribute_name VALUE raw_attribute_value
        ) values_source
        CROSS JOIN LATERAL (
          SELECT CASE
            WHEN raw_attribute_value = '' THEN NULL
            WHEN attribute_name = 'status' AND raw_attribute_value = 'Y' THEN NULL
            ELSE raw_attribute_value
          END AS attribute_value
        ) normalized
        WHERE attribute_value IS NOT NULL;
        "#
    ))?;
    Ok(())
}

fn validate_views(
    connection: &Connection,
    policy: SourceValidationPolicy,
) -> Result<(), DuckDbProviderError> {
    for view in [
        "ocpm_events",
        "ocpm_objects",
        "ocpm_e2o",
        "ocpm_o2o",
        "ocpm_object_attributes",
    ] {
        connection.query_row(
            &format!("SELECT count(*) FROM {view} WHERE false"),
            [],
            |_| Ok(()),
        )?;
    }
    if policy == SourceValidationPolicy::Fast {
        return Ok(());
    }
    let invalid: u64 = connection.query_row(
        r#"
        SELECT
          (SELECT count(*) FROM ocpm_events WHERE event_id IS NULL OR activity IS NULL)
          + (SELECT count(*) FROM ocpm_objects WHERE object_id IS NULL OR object_type IS NULL)
          + (SELECT count(*) FROM ocpm_e2o WHERE event_id IS NULL OR object_id IS NULL)
        "#,
        [],
        |row| row.get(0),
    )?;
    if invalid != 0 {
        return Err(DuckDbProviderError::InvalidSource(format!(
            "canonical views contain {invalid} null required values"
        )));
    }
    if policy == SourceValidationPolicy::Balanced {
        return Ok(());
    }
    let violations: u64 = connection.query_row(
        r#"
        SELECT
          (SELECT count(*) FROM (SELECT event_id FROM ocpm_events GROUP BY event_id HAVING count(*) <> 1))
          + (SELECT count(*) FROM (SELECT object_id FROM ocpm_objects GROUP BY object_id HAVING count(*) <> 1))
          + (SELECT count(*) FROM ocpm_e2o r LEFT JOIN ocpm_events e USING(event_id) WHERE e.event_id IS NULL)
          + (SELECT count(*) FROM ocpm_e2o r LEFT JOIN ocpm_objects o USING(object_id) WHERE o.object_id IS NULL)
          + (SELECT count(*) FROM ocpm_o2o r LEFT JOIN ocpm_objects o ON o.object_id=r.source_object_id WHERE o.object_id IS NULL)
          + (SELECT count(*) FROM ocpm_o2o r LEFT JOIN ocpm_objects o ON o.object_id=r.target_object_id WHERE o.object_id IS NULL)
        "#,
        [],
        |row| row.get(0),
    )?;
    if violations != 0 {
        return Err(DuckDbProviderError::InvalidSource(format!(
            "strict canonical validation found {violations} identity or referential violations"
        )));
    }
    Ok(())
}

fn lock_security_configuration(connection: &Connection) -> Result<(), DuckDbProviderError> {
    connection.execute_batch(
        r#"
        SET autoinstall_known_extensions = false;
        SET autoload_known_extensions = false;
        SET lock_configuration = true;
        "#,
    )?;
    Ok(())
}

fn validate_s3_uri(uri: &str) -> Result<String, DuckDbProviderError> {
    let value = uri.trim_end_matches('/');
    let Some(rest) = value.strip_prefix("s3://") else {
        return Err(DuckDbProviderError::InvalidSource(
            "S3 source must start with s3://".to_owned(),
        ));
    };
    if rest.is_empty()
        || rest.starts_with('/')
        || rest.contains("..")
        || rest
            .chars()
            .any(|value| matches!(value, '?' | '#' | '\\' | '\0'))
        || rest.chars().any(char::is_whitespace)
    {
        return Err(DuckDbProviderError::InvalidSource(
            "S3 source contains an unsafe bucket or prefix".to_owned(),
        ));
    }
    Ok(value.to_owned())
}

fn validate_relative_file(value: &str) -> Result<(), DuckDbProviderError> {
    if value.is_empty()
        || value.starts_with('/')
        || value.contains("..")
        || value.chars().any(|value| matches!(value, '\\' | '\0'))
    {
        return Err(DuckDbProviderError::InvalidSource(
            "snapshot-relative path is unsafe".to_owned(),
        ));
    }
    Ok(())
}

fn validate_version(value: &str) -> Result<(), DuckDbProviderError> {
    if value.is_empty()
        || value.len() > 128
        || value.contains("..")
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(DuckDbProviderError::InvalidSource(
            "snapshot version is unsafe".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn join_uri(root: &str, child: &str) -> String {
    format!(
        "{}/{}",
        root.trim_end_matches('/'),
        child.trim_start_matches('/')
    )
}

#[allow(dead_code)]
fn _assert_time_policy_is_public(value: AmbiguousTimePolicy) -> AmbiguousTimePolicy {
    value
}
