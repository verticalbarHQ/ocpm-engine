use crate::{DuckDbProviderError, lowercase_hex, quote_literal};
use duckdb::{Connection, appender_params_from_iter, types::Value};
use ocpm_core::{CanonicalLog, Timestamp};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

static STAGING_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalParquetFile {
    pub path: String,
    pub rows: u64,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CanonicalParquetManifest {
    pub layout_version: String,
    pub canonical_semantic_version: String,
    pub snapshot_version: String,
    pub dataset_id: String,
    pub tenant_id: String,
    pub source_watermark: Option<Timestamp>,
    pub files: BTreeMap<String, CanonicalParquetFile>,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotWriteResult {
    pub version: String,
    pub version_root: PathBuf,
    pub manifest_path: PathBuf,
    pub total_bytes: u64,
}

pub fn write_canonical_snapshot(
    log: &CanonicalLog,
    root: impl AsRef<Path>,
    version: &str,
) -> Result<SnapshotWriteResult, DuckDbProviderError> {
    validate_version(version)?;
    let mut log = log.clone();
    log.validate()?;
    log.sort_canonical();

    let root = root.as_ref();
    fs::create_dir_all(root.join("versions"))?;
    let final_root = root.join("versions").join(version);
    if final_root.exists() {
        return Err(DuckDbProviderError::InvalidSource(format!(
            "snapshot version already exists: {version}"
        )));
    }
    let staging = root.join("versions").join(format!(
        ".{version}.staging-{}-{}",
        std::process::id(),
        STAGING_ID.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&staging)?;

    let result = write_staging(&log, version, &staging).and_then(|manifest| {
        fs::rename(&staging, &final_root)?;
        let pointer_temp = root.join(format!(
            ".CURRENT.tmp-{}-{}",
            std::process::id(),
            STAGING_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&pointer_temp, format!("{version}\n"))?;
        fs::rename(pointer_temp, root.join("CURRENT"))?;
        let total_bytes = manifest.files.values().map(|file| file.bytes).sum();
        Ok(SnapshotWriteResult {
            version: version.to_owned(),
            version_root: final_root.clone(),
            manifest_path: final_root.join("manifest.json"),
            total_bytes,
        })
    });
    if result.is_err() && staging.exists() {
        let _ = fs::remove_dir_all(&staging);
    }
    result
}

fn write_staging(
    log: &CanonicalLog,
    version: &str,
    staging: &Path,
) -> Result<CanonicalParquetManifest, DuckDbProviderError> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch(
        r#"
        CREATE TABLE events (
            event_id UBIGINT NOT NULL,
            external_event_id VARCHAR NOT NULL,
            activity VARCHAR NOT NULL,
            timestamp_nanos_utc HUGEINT NOT NULL,
            source_timestamp VARCHAR,
            sequence UBIGINT NOT NULL,
            lifecycle VARCHAR,
            attributes_json VARCHAR NOT NULL
        );
        CREATE TABLE objects (
            object_id UBIGINT NOT NULL,
            external_object_id VARCHAR NOT NULL,
            object_type VARCHAR NOT NULL
        );
        CREATE TABLE event_object_relations (
            relation_id UBIGINT NOT NULL,
            event_id UBIGINT NOT NULL,
            object_id UBIGINT NOT NULL,
            qualifier VARCHAR NOT NULL
        );
        CREATE TABLE object_object_relations (
            relation_id UBIGINT NOT NULL,
            source_object_id UBIGINT NOT NULL,
            target_object_id UBIGINT NOT NULL,
            qualifier VARCHAR NOT NULL,
            valid_from_nanos_utc HUGEINT,
            valid_to_nanos_utc HUGEINT
        );
        CREATE TABLE object_attribute_history (
            object_id UBIGINT NOT NULL,
            name VARCHAR NOT NULL,
            valid_from_nanos_utc HUGEINT NOT NULL,
            value_json VARCHAR NOT NULL
        );
        "#,
    )?;

    {
        let mut appender = connection.appender("events")?;
        for event in &log.events {
            appender.append_row(appender_params_from_iter(vec![
                Value::UBigInt(event.id),
                Value::Text(event.external_id.clone()),
                Value::Text(event.activity.clone()),
                Value::HugeInt(event.timestamp.epoch_nanos_utc),
                event
                    .timestamp
                    .source
                    .clone()
                    .map_or(Value::Null, Value::Text),
                Value::UBigInt(event.sequence),
                event.lifecycle.clone().map_or(Value::Null, Value::Text),
                Value::Text(serde_json::to_string(&event.attributes)?),
            ]))?;
        }
        appender.flush()?;
    }
    {
        let mut appender = connection.appender("objects")?;
        for object in &log.objects {
            appender.append_row(appender_params_from_iter(vec![
                Value::UBigInt(object.id),
                Value::Text(object.external_id.clone()),
                Value::Text(object.object_type.clone()),
            ]))?;
        }
        appender.flush()?;
    }
    {
        let mut appender = connection.appender("event_object_relations")?;
        for relation in &log.event_object_relations {
            appender.append_row(appender_params_from_iter(vec![
                Value::UBigInt(relation.relation_id),
                Value::UBigInt(relation.event_id),
                Value::UBigInt(relation.object_id),
                Value::Text(relation.qualifier.clone()),
            ]))?;
        }
        appender.flush()?;
    }
    {
        let mut appender = connection.appender("object_object_relations")?;
        for relation in &log.object_object_relations {
            appender.append_row(appender_params_from_iter(vec![
                Value::UBigInt(relation.relation_id),
                Value::UBigInt(relation.source_object_id),
                Value::UBigInt(relation.target_object_id),
                Value::Text(relation.qualifier.clone()),
                relation
                    .valid_from
                    .as_ref()
                    .map_or(Value::Null, |timestamp| {
                        Value::HugeInt(timestamp.epoch_nanos_utc)
                    }),
                relation.valid_to.as_ref().map_or(Value::Null, |timestamp| {
                    Value::HugeInt(timestamp.epoch_nanos_utc)
                }),
            ]))?;
        }
        appender.flush()?;
    }
    {
        let mut appender = connection.appender("object_attribute_history")?;
        for change in &log.object_attribute_history {
            appender.append_row(appender_params_from_iter(vec![
                Value::UBigInt(change.object_id),
                Value::Text(change.name.clone()),
                Value::HugeInt(change.valid_from.epoch_nanos_utc),
                Value::Text(serde_json::to_string(&change.value)?),
            ]))?;
        }
        appender.flush()?;
    }

    let targets = [
        ("events", "events/events.parquet", log.events.len() as u64),
        (
            "objects",
            "objects/objects.parquet",
            log.objects.len() as u64,
        ),
        (
            "event_object_relations",
            "event_object_relations/event_object_relations.parquet",
            log.event_object_relations.len() as u64,
        ),
        (
            "object_object_relations",
            "object_object_relations/object_object_relations.parquet",
            log.object_object_relations.len() as u64,
        ),
        (
            "object_attribute_history",
            "object_attribute_history/object_attribute_history.parquet",
            log.object_attribute_history.len() as u64,
        ),
    ];
    let mut files = BTreeMap::new();
    for (table, relative, rows) in targets {
        let path = staging.join(relative);
        fs::create_dir_all(path.parent().expect("Parquet path has a parent"))?;
        connection.execute_batch(&format!(
            "COPY {table} TO {} (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)",
            quote_literal(&path.to_string_lossy())
        ))?;
        let bytes = fs::metadata(&path)?.len();
        files.insert(
            table.to_owned(),
            CanonicalParquetFile {
                path: relative.to_owned(),
                rows,
                bytes,
                sha256: file_sha256(&path)?,
            },
        );
    }

    let manifest = CanonicalParquetManifest {
        layout_version: "canonical_parquet_v1".to_owned(),
        canonical_semantic_version: "1.0".to_owned(),
        snapshot_version: version.to_owned(),
        dataset_id: log.dataset_id.clone(),
        tenant_id: log.tenant_id.clone(),
        source_watermark: log.source_watermark.clone(),
        files,
        metadata: log.metadata.clone(),
    };
    let manifest_path = staging.join("manifest.json");
    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;
    Ok(manifest)
}

fn validate_version(version: &str) -> Result<(), DuckDbProviderError> {
    if version.is_empty()
        || version.len() > 128
        || version == "."
        || version == ".."
        || version.contains("..")
        || !version
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(DuckDbProviderError::InvalidSource(
            "snapshot version must contain only ASCII letters, digits, dot, underscore, or dash"
                .to_owned(),
        ));
    }
    Ok(())
}

fn file_sha256(path: &Path) -> Result<String, DuckDbProviderError> {
    let mut digest = Sha256::new();
    let mut file = fs::File::open(path)?;
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
