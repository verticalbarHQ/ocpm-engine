use crate::{OcpmError, OcpmResult};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub type EventId = u64;
pub type ObjectId = u64;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum AttributeValue {
    Null,
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Timestamp(Timestamp),
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Timestamp {
    pub epoch_nanos_utc: i128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

impl Timestamp {
    pub const MIN: Self = Self {
        epoch_nanos_utc: i128::MIN,
        source: None,
    };
    pub const MAX: Self = Self {
        epoch_nanos_utc: i128::MAX,
        source: None,
    };

    pub fn from_epoch_nanos(epoch_nanos_utc: i128) -> Self {
        Self {
            epoch_nanos_utc,
            source: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub id: EventId,
    pub external_id: String,
    pub activity: String,
    pub timestamp: Timestamp,
    #[serde(default)]
    pub sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<String>,
    #[serde(default)]
    pub attributes: BTreeMap<String, AttributeValue>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Object {
    pub id: ObjectId,
    pub external_id: String,
    pub object_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventObjectRelation {
    pub relation_id: u64,
    pub event_id: EventId,
    pub object_id: ObjectId,
    #[serde(default)]
    pub qualifier: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectObjectRelation {
    pub relation_id: u64,
    pub source_object_id: ObjectId,
    pub target_object_id: ObjectId,
    #[serde(default)]
    pub qualifier: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub valid_from: Option<Timestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub valid_to: Option<Timestamp>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ObjectAttributeChange {
    pub object_id: ObjectId,
    pub name: String,
    pub valid_from: Timestamp,
    pub value: AttributeValue,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CanonicalLog {
    #[serde(default)]
    pub dataset_id: String,
    #[serde(default)]
    pub tenant_id: String,
    #[serde(default)]
    pub source_watermark: Option<Timestamp>,
    #[serde(default)]
    pub events: Vec<Event>,
    #[serde(default)]
    pub objects: Vec<Object>,
    #[serde(default)]
    pub event_object_relations: Vec<EventObjectRelation>,
    #[serde(default)]
    pub object_object_relations: Vec<ObjectObjectRelation>,
    #[serde(default)]
    pub object_attribute_history: Vec<ObjectAttributeChange>,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CanonicalEventBatch {
    #[serde(default)]
    pub event_id: Vec<EventId>,
    #[serde(default)]
    pub external_event_id: Vec<String>,
    #[serde(default)]
    pub activity: Vec<String>,
    #[serde(default)]
    pub timestamp_nanos_utc: Vec<i128>,
    #[serde(default)]
    pub source_timestamp: Vec<Option<String>>,
    #[serde(default)]
    pub sequence: Vec<u64>,
    #[serde(default)]
    pub lifecycle: Vec<Option<String>>,
    #[serde(default)]
    pub attributes: Vec<BTreeMap<String, AttributeValue>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectBatch {
    #[serde(default)]
    pub object_id: Vec<ObjectId>,
    #[serde(default)]
    pub external_object_id: Vec<String>,
    #[serde(default)]
    pub object_type: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct E2oBatch {
    #[serde(default)]
    pub event_id: Vec<EventId>,
    #[serde(default)]
    pub object_id: Vec<ObjectId>,
    #[serde(default)]
    pub qualifier: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct O2oBatch {
    #[serde(default)]
    pub source_object_id: Vec<ObjectId>,
    #[serde(default)]
    pub target_object_id: Vec<ObjectId>,
    #[serde(default)]
    pub qualifier: Vec<String>,
    #[serde(default)]
    pub valid_from_nanos_utc: Vec<Option<i128>>,
    #[serde(default)]
    pub valid_to_nanos_utc: Vec<Option<i128>>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ObjectAttributeBatch {
    #[serde(default)]
    pub object_id: Vec<ObjectId>,
    #[serde(default)]
    pub name: Vec<String>,
    #[serde(default)]
    pub valid_from_nanos_utc: Vec<i128>,
    #[serde(default)]
    pub value: Vec<AttributeValue>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AppendBatch {
    #[serde(default)]
    pub events: CanonicalEventBatch,
    #[serde(default)]
    pub objects: ObjectBatch,
    #[serde(default)]
    pub event_object_relations: E2oBatch,
    #[serde(default)]
    pub object_object_relations: O2oBatch,
    #[serde(default)]
    pub object_attribute_history: ObjectAttributeBatch,
    pub source_watermark: Option<Timestamp>,
}

impl AppendBatch {
    pub const MAX_ENCODED_BYTES: usize = 16 * 1024 * 1024;

    pub fn validate(&self) -> OcpmResult<()> {
        equal_lengths(
            "events",
            &[
                self.events.event_id.len(),
                self.events.external_event_id.len(),
                self.events.activity.len(),
                self.events.timestamp_nanos_utc.len(),
                self.events.source_timestamp.len(),
                self.events.sequence.len(),
                self.events.lifecycle.len(),
                self.events.attributes.len(),
            ],
        )?;
        equal_lengths(
            "objects",
            &[
                self.objects.object_id.len(),
                self.objects.external_object_id.len(),
                self.objects.object_type.len(),
            ],
        )?;
        equal_lengths(
            "event_object_relations",
            &[
                self.event_object_relations.event_id.len(),
                self.event_object_relations.object_id.len(),
                self.event_object_relations.qualifier.len(),
            ],
        )?;
        equal_lengths(
            "object_object_relations",
            &[
                self.object_object_relations.source_object_id.len(),
                self.object_object_relations.target_object_id.len(),
                self.object_object_relations.qualifier.len(),
                self.object_object_relations.valid_from_nanos_utc.len(),
                self.object_object_relations.valid_to_nanos_utc.len(),
            ],
        )?;
        equal_lengths(
            "object_attribute_history",
            &[
                self.object_attribute_history.object_id.len(),
                self.object_attribute_history.name.len(),
                self.object_attribute_history.valid_from_nanos_utc.len(),
                self.object_attribute_history.value.len(),
            ],
        )?;
        let bytes = serde_json::to_vec(self)
            .map_err(|error| OcpmError::invalid_data(format!("append batch is invalid: {error}")))?
            .len();
        if bytes > Self::MAX_ENCODED_BYTES {
            return Err(OcpmError::resource_limit(
                "append batch exceeds the 16 MiB provider boundary",
                Self::MAX_ENCODED_BYTES as u64,
                bytes as u64,
            ));
        }
        Ok(())
    }
}

fn equal_lengths(name: &str, lengths: &[usize]) -> OcpmResult<()> {
    if lengths.windows(2).all(|pair| pair[0] == pair[1]) {
        Ok(())
    } else {
        Err(OcpmError::invalid_data(format!(
            "{name} batch columns must have equal lengths: {lengths:?}"
        )))
    }
}

impl CanonicalLog {
    pub fn validate(&self) -> OcpmResult<()> {
        let mut event_ids = BTreeSet::new();
        let mut external_event_ids = BTreeSet::new();
        for (index, event) in self.events.iter().enumerate() {
            if event.external_id.is_empty() || event.activity.is_empty() {
                return Err(OcpmError::invalid_data(
                    "event external_id and activity must be nonempty",
                )
                .at(format!("events[{index}]")));
            }
            if !event_ids.insert(event.id) || !external_event_ids.insert(&event.external_id) {
                return Err(OcpmError::invalid_data("event IDs must be unique")
                    .at(format!("events[{index}]")));
            }
            for value in event.attributes.values() {
                validate_attribute(value).map_err(|error| error.at(format!("events[{index}]")))?;
            }
        }

        let mut object_ids = BTreeSet::new();
        let mut external_object_ids = BTreeSet::new();
        for (index, object) in self.objects.iter().enumerate() {
            if object.external_id.is_empty() || object.object_type.is_empty() {
                return Err(OcpmError::invalid_data(
                    "object external_id and object_type must be nonempty",
                )
                .at(format!("objects[{index}]")));
            }
            if !object_ids.insert(object.id) || !external_object_ids.insert(&object.external_id) {
                return Err(OcpmError::invalid_data("object IDs must be unique")
                    .at(format!("objects[{index}]")));
            }
        }

        let mut relation_ids = BTreeSet::new();
        for (index, relation) in self.event_object_relations.iter().enumerate() {
            if !relation_ids.insert((0_u8, relation.relation_id)) {
                return Err(OcpmError::invalid_data("E2O relation IDs must be unique")
                    .at(format!("event_object_relations[{index}]")));
            }
            if !event_ids.contains(&relation.event_id) || !object_ids.contains(&relation.object_id)
            {
                return Err(OcpmError::invalid_data("E2O relation endpoint does not exist")
                    .at(format!("event_object_relations[{index}]")));
            }
        }
        for (index, relation) in self.object_object_relations.iter().enumerate() {
            if !relation_ids.insert((1_u8, relation.relation_id)) {
                return Err(OcpmError::invalid_data("O2O relation IDs must be unique")
                    .at(format!("object_object_relations[{index}]")));
            }
            if !object_ids.contains(&relation.source_object_id)
                || !object_ids.contains(&relation.target_object_id)
            {
                return Err(OcpmError::invalid_data("O2O relation endpoint does not exist")
                    .at(format!("object_object_relations[{index}]")));
            }
            if relation
                .valid_from
                .as_ref()
                .zip(relation.valid_to.as_ref())
                .is_some_and(|(from, to)| to <= from)
            {
                return Err(OcpmError::invalid_data("O2O valid_to must be after valid_from")
                    .at(format!("object_object_relations[{index}]")));
            }
        }
        let mut attribute_keys = BTreeSet::new();
        for (index, change) in self.object_attribute_history.iter().enumerate() {
            if !object_ids.contains(&change.object_id) || change.name.is_empty() {
                return Err(OcpmError::invalid_data("invalid object attribute history row")
                    .at(format!("object_attribute_history[{index}]")));
            }
            if !attribute_keys.insert((change.object_id, &change.name, &change.valid_from)) {
                return Err(OcpmError::invalid_data(
                    "object attribute changes must be unique per object/name/time",
                )
                .at(format!("object_attribute_history[{index}]")));
            }
            validate_attribute(&change.value)
                .map_err(|error| error.at(format!("object_attribute_history[{index}]")))?;
        }
        Ok(())
    }

    pub fn sort_canonical(&mut self) {
        self.events.sort_by(|left, right| {
            left.timestamp
                .cmp(&right.timestamp)
                .then_with(|| left.sequence.cmp(&right.sequence))
                .then_with(|| left.external_id.cmp(&right.external_id))
        });
        self.objects.sort_by(|left, right| {
            left.object_type
                .cmp(&right.object_type)
                .then_with(|| left.external_id.cmp(&right.external_id))
        });
        self.event_object_relations.sort_by_key(|relation| relation.relation_id);
        self.object_object_relations.sort_by_key(|relation| relation.relation_id);
        self.object_attribute_history.sort_by(|left, right| {
            left.object_id
                .cmp(&right.object_id)
                .then_with(|| left.name.cmp(&right.name))
                .then_with(|| left.valid_from.cmp(&right.valid_from))
        });
    }

    pub fn event(&self, id: EventId) -> Option<&Event> {
        self.events.iter().find(|event| event.id == id)
    }

    pub fn object(&self, id: ObjectId) -> Option<&Object> {
        self.objects.iter().find(|object| object.id == id)
    }
}

fn validate_attribute(value: &AttributeValue) -> OcpmResult<()> {
    if matches!(value, AttributeValue::Float(value) if !value.is_finite()) {
        Err(OcpmError::invalid_data(
            "floating-point attributes must be finite",
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_missing_relation_endpoint() {
        let log = CanonicalLog {
            event_object_relations: vec![EventObjectRelation {
                relation_id: 1,
                event_id: 1,
                object_id: 2,
                qualifier: String::new(),
            }],
            ..CanonicalLog::default()
        };
        assert_eq!(log.validate().unwrap_err().code, crate::OcpmErrorCode::InvalidData);
    }
}
