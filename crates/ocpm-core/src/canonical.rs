use crate::{OcpmError, OcpmErrorCode, OcpmResult};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

pub fn canonical_json<T: Serialize>(value: &T) -> OcpmResult<Vec<u8>> {
    let value = serde_json::to_value(value).map_err(|error| {
        OcpmError::new(
            OcpmErrorCode::InvalidData,
            format!("value cannot be serialized: {error}"),
        )
    })?;
    let normalized = normalize(value);
    serde_json::to_vec(&normalized).map_err(|error| {
        OcpmError::new(
            OcpmErrorCode::Internal,
            format!("canonical serialization failed: {error}"),
        )
    })
}

pub fn content_hash<T: Serialize>(value: &T) -> OcpmResult<String> {
    let digest = Sha256::digest(canonical_json(value)?);
    let mut output = String::with_capacity(71);
    output.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(output)
}

fn normalize(value: Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.into_iter().map(normalize).collect()),
        Value::Object(items) => {
            let mut entries = items.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, normalize(value)))
                    .collect(),
            )
        }
        other => other,
    }
}

