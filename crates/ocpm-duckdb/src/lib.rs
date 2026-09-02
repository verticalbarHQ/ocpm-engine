//! DuckDB-backed local and S3 Parquet provider for `ocpm-engine`.
//!
//! The provider is independently implemented over the source-neutral contracts
//! and DuckDB's public API. No process-mining library implementation source was
//! consulted.

mod config;
mod provider;
mod snapshot;
mod writer;

pub use config::*;
pub use provider::DuckDbProvider;
pub use writer::{
    CanonicalParquetFile, CanonicalParquetManifest, SnapshotWriteResult, write_canonical_snapshot,
};

use chrono::{DateTime, FixedOffset, LocalResult, NaiveDateTime, TimeZone, Utc};
use ocpm_core::{AttributeValue, OcpmError, OcpmErrorCode, Timestamp};
use std::{collections::BTreeMap, fmt::Write};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DuckDbProviderError {
    #[error("DuckDB operation failed: {0}")]
    DuckDb(#[from] duckdb::Error),
    #[error("filesystem operation failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON operation failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid DuckDB Parquet source: {0}")]
    InvalidSource(String),
    #[error("unsupported DuckDB Parquet operation: {0}")]
    Unsupported(String),
    #[error("canonical provider operation failed: {0}")]
    Canonical(#[from] OcpmError),
}

impl From<DuckDbProviderError> for OcpmError {
    fn from(error: DuckDbProviderError) -> Self {
        match error {
            DuckDbProviderError::InvalidSource(message) => {
                OcpmError::new(OcpmErrorCode::InvalidData, message)
            }
            DuckDbProviderError::Unsupported(message) => {
                OcpmError::new(OcpmErrorCode::UnsupportedFormat, message)
            }
            DuckDbProviderError::Canonical(error) => error,
            DuckDbProviderError::DuckDb(error) => classify_duckdb_failure(error.to_string()),
            other => OcpmError::new(OcpmErrorCode::ProviderUnavailable, other.to_string()),
        }
    }
}

fn classify_duckdb_failure(message: String) -> OcpmError {
    let normalized = message.to_ascii_lowercase();
    if normalized.contains("max_temp_directory_size")
        || normalized.contains("temp directory size limit")
    {
        return resource_limit_without_observed_values(message, "max_temp_bytes");
    }
    if normalized.contains("out of memory") || normalized.contains("memory limit") {
        return resource_limit_without_observed_values(message, "memory_budget_bytes");
    }
    OcpmError::new(OcpmErrorCode::ProviderUnavailable, message)
}

fn resource_limit_without_observed_values(message: String, field_path: &str) -> OcpmError {
    OcpmError {
        code: OcpmErrorCode::ResourceLimit,
        message,
        retryable: true,
        field_path: Some(field_path.to_owned()),
        limit: None,
        actual: None,
    }
}

pub(crate) fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(crate) fn lowercase_hex(value: impl AsRef<[u8]>) -> String {
    let bytes = value.as_ref();
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

pub fn source_timestamp_to_utc(
    value: &str,
    policy: &SourceTimestampPolicy,
) -> Result<Timestamp, DuckDbProviderError> {
    if let Ok(parsed) = DateTime::parse_from_rfc3339(value) {
        return Ok(Timestamp {
            epoch_nanos_utc: parsed
                .timestamp_nanos_opt()
                .map(i128::from)
                .ok_or_else(|| {
                    DuckDbProviderError::InvalidSource(
                        "source timestamp is outside nanosecond range".to_owned(),
                    )
                })?,
            source: Some(value.to_owned()),
        });
    }
    let naive = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f").map_err(|error| {
        DuckDbProviderError::InvalidSource(format!("invalid source timestamp {value:?}: {error}"))
    })?;
    let utc = match policy {
        SourceTimestampPolicy::Utc => naive.and_utc(),
        SourceTimestampPolicy::FixedOffset { seconds_east } => FixedOffset::east_opt(*seconds_east)
            .ok_or_else(|| {
                DuckDbProviderError::InvalidSource("invalid fixed timestamp offset".to_owned())
            })?
            .from_local_datetime(&naive)
            .single()
            .ok_or_else(|| {
                DuckDbProviderError::InvalidSource(
                    "fixed-offset timestamp did not resolve uniquely".to_owned(),
                )
            })?
            .with_timezone(&Utc),
        SourceTimestampPolicy::Iana { zone, ambiguous } => {
            let zone: chrono_tz::Tz = zone.parse().map_err(|_| {
                DuckDbProviderError::InvalidSource(format!("invalid IANA time zone {zone:?}"))
            })?;
            match zone.from_local_datetime(&naive) {
                LocalResult::Single(value) => value.with_timezone(&Utc),
                LocalResult::Ambiguous(first, second) => match ambiguous {
                    AmbiguousTimePolicy::Reject => {
                        return Err(DuckDbProviderError::InvalidSource(format!(
                            "ambiguous source timestamp {value:?} in {zone}"
                        )));
                    }
                    AmbiguousTimePolicy::Earliest => first.min(second).with_timezone(&Utc),
                    AmbiguousTimePolicy::Latest => first.max(second).with_timezone(&Utc),
                },
                LocalResult::None => {
                    return Err(DuckDbProviderError::InvalidSource(format!(
                        "nonexistent source timestamp {value:?} in {zone}"
                    )));
                }
            }
        }
    };
    Ok(Timestamp {
        epoch_nanos_utc: utc.timestamp_nanos_opt().map(i128::from).ok_or_else(|| {
            DuckDbProviderError::InvalidSource(
                "source timestamp is outside nanosecond range".to_owned(),
            )
        })?,
        source: Some(value.to_owned()),
    })
}

pub(crate) fn utc_nanos_to_source_nanos(
    value: i128,
    policy: &SourceTimestampPolicy,
) -> Result<i128, DuckDbProviderError> {
    let nanos = i64::try_from(value).map_err(|_| {
        DuckDbProviderError::InvalidSource(
            "timestamp is outside DuckDB nanosecond range".to_owned(),
        )
    })?;
    let utc = DateTime::<Utc>::from_timestamp_nanos(nanos);
    let naive = match policy {
        SourceTimestampPolicy::Utc => utc.naive_utc(),
        SourceTimestampPolicy::FixedOffset { seconds_east } => FixedOffset::east_opt(*seconds_east)
            .ok_or_else(|| {
                DuckDbProviderError::InvalidSource("invalid fixed timestamp offset".to_owned())
            })?
            .from_utc_datetime(&utc.naive_utc())
            .naive_local(),
        SourceTimestampPolicy::Iana { zone, .. } => {
            let zone: chrono_tz::Tz = zone.parse().map_err(|_| {
                DuckDbProviderError::InvalidSource(format!("invalid IANA time zone {zone:?}"))
            })?;
            utc.with_timezone(&zone).naive_local()
        }
    };
    naive
        .and_utc()
        .timestamp_nanos_opt()
        .map(i128::from)
        .ok_or_else(|| {
            DuckDbProviderError::InvalidSource(
                "localized timestamp is outside nanosecond range".to_owned(),
            )
        })
}

pub(crate) fn parse_attribute_map(
    value: &str,
) -> Result<BTreeMap<String, AttributeValue>, DuckDbProviderError> {
    let parsed: serde_json::Value = serde_json::from_str(value)?;
    let object = parsed.as_object().ok_or_else(|| {
        DuckDbProviderError::InvalidSource("event attributes must be a JSON object".to_owned())
    })?;
    object
        .iter()
        .map(|(name, value)| Ok((name.clone(), parse_attribute(value)?)))
        .collect()
}

pub(crate) fn parse_attribute(
    value: &serde_json::Value,
) -> Result<AttributeValue, DuckDbProviderError> {
    if value
        .as_object()
        .is_some_and(|object| object.contains_key("type") && object.contains_key("value"))
    {
        return Ok(serde_json::from_value(value.clone())?);
    }
    Ok(match value {
        serde_json::Value::Null => AttributeValue::Null,
        serde_json::Value::Bool(value) => AttributeValue::Boolean(*value),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                AttributeValue::Integer(value)
            } else {
                AttributeValue::Float(value.as_f64().ok_or_else(|| {
                    DuckDbProviderError::InvalidSource(
                        "JSON number cannot be represented as an attribute".to_owned(),
                    )
                })?)
            }
        }
        serde_json::Value::String(value) => AttributeValue::String(value.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            AttributeValue::String(serde_json::to_string(value)?)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duckdb_resource_failures_have_stable_typed_fields() {
        let memory = classify_duckdb_failure("Out of Memory Error".to_owned());
        assert_eq!(memory.code, OcpmErrorCode::ResourceLimit);
        assert_eq!(memory.field_path.as_deref(), Some("memory_budget_bytes"));
        assert_eq!((memory.limit, memory.actual), (None, None));

        let spill = classify_duckdb_failure("max_temp_directory_size reached".to_owned());
        assert_eq!(spill.code, OcpmErrorCode::ResourceLimit);
        assert_eq!(spill.field_path.as_deref(), Some("max_temp_bytes"));

        let provider = classify_duckdb_failure("network read failed".to_owned());
        assert_eq!(provider.code, OcpmErrorCode::ProviderUnavailable);
    }
}
