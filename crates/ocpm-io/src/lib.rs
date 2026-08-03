//! Independent import/export for canonical OCEL data.
//!
//! PROVENANCE: object-centric event-log semantics follow the peer-reviewed
//! OCEL standard paper doi:10.1007/978-3-030-85082-1_16. XES exchange
//! semantics follow the peer-reviewed XES paper
//! doi:10.1007/978-3-642-17722-4_5. Format code was independently authored
//! without consulting process-mining library source code.

use chrono::{DateTime, Utc};
use ocpm_core::{
    AttributeValue, CanonicalLog, Event, EventObjectRelation, Object,
    ObjectAttributeChange, ObjectObjectRelation, OcpmError, OcpmErrorCode, OcpmResult,
    Timestamp,
};
use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event as XmlEvent};
use rusqlite::Connection;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::io::{BufRead, Read, Write};
use std::path::Path;

pub const PROVENANCE: &[&str] = &[
    "doi:10.1007/978-3-030-85082-1_16",
    "doi:10.1007/978-3-642-17722-4_5",
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CsvMapping {
    pub case_id: String,
    pub activity: String,
    pub timestamp: String,
    pub object_type: String,
    pub event_id: Option<String>,
    pub timestamp_format: Option<String>,
    pub delimiter: u8,
}

impl Default for CsvMapping {
    fn default() -> Self {
        Self {
            case_id: "case_id".to_owned(),
            activity: "activity".to_owned(),
            timestamp: "timestamp".to_owned(),
            object_type: "case".to_owned(),
            event_id: Some("event_id".to_owned()),
            timestamp_format: None,
            delimiter: b',',
        }
    }
}

pub fn read_canonical_json(reader: impl Read) -> OcpmResult<CanonicalLog> {
    let value: Value = serde_json::from_reader(reader)
        .map_err(|error| OcpmError::invalid_data(format!("invalid JSON: {error}")))?;
    if let Ok(mut log) = serde_json::from_value::<CanonicalLog>(value.clone()) {
        log.validate()?;
        log.sort_canonical();
        return Ok(log);
    }
    read_ocel2_json_value(&value)
}

pub fn write_canonical_json(mut writer: impl Write, log: &CanonicalLog) -> OcpmResult<()> {
    log.validate()?;
    serde_json::to_writer(&mut writer, log)
        .map_err(|error| OcpmError::invalid_data(format!("JSON export failed: {error}")))
}

pub fn write_ocel2_json(mut writer: impl Write, log: &CanonicalLog) -> OcpmResult<()> {
    log.validate()?;
    let objects_by_id = log
        .objects
        .iter()
        .map(|object| (object.id, object))
        .collect::<BTreeMap<_, _>>();
    let mut objects = Vec::with_capacity(log.objects.len());
    for object in &log.objects {
        let attributes = log
            .object_attribute_history
            .iter()
            .filter(|change| change.object_id == object.id)
            .map(|change| {
                let value = attribute_to_json(&change.value)?;
                Ok(serde_json::json!({
                    "name": change.name,
                    "time": timestamp_text(&change.valid_from)?,
                    "value": value,
                }))
            })
            .collect::<OcpmResult<Vec<_>>>()?;
        let relationships = log
            .object_object_relations
            .iter()
            .filter(|relation| relation.source_object_id == object.id)
            .filter_map(|relation| {
                objects_by_id.get(&relation.target_object_id).map(|target| {
                    serde_json::json!({
                        "objectId": target.external_id,
                        "qualifier": relation.qualifier,
                    })
                })
            })
            .collect::<Vec<_>>();
        objects.push(serde_json::json!({
            "id": object.external_id,
            "type": object.object_type,
            "attributes": attributes,
            "relationships": relationships,
        }));
    }
    let mut events = Vec::with_capacity(log.events.len());
    for event in &log.events {
        let attributes = event
            .attributes
            .iter()
            .map(|(name, value)| {
                Ok(serde_json::json!({
                    "name": name,
                    "value": attribute_to_json(value)?,
                }))
            })
            .collect::<OcpmResult<Vec<_>>>()?;
        let relationships = log
            .event_object_relations
            .iter()
            .filter(|relation| relation.event_id == event.id)
            .filter_map(|relation| {
                objects_by_id.get(&relation.object_id).map(|object| {
                    serde_json::json!({
                        "objectId": object.external_id,
                        "qualifier": relation.qualifier,
                    })
                })
            })
            .collect::<Vec<_>>();
        events.push(serde_json::json!({
            "id": event.external_id,
            "type": event.activity,
            "time": timestamp_text(&event.timestamp)?,
            "attributes": attributes,
            "relationships": relationships,
        }));
    }
    serde_json::to_writer(
        &mut writer,
        &serde_json::json!({
            "datasetId": log.dataset_id,
            "tenantId": log.tenant_id,
            "objects": objects,
            "events": events,
        }),
    )
    .map_err(|error| OcpmError::invalid_data(format!("OCEL JSON export failed: {error}")))
}

pub fn read_ocel2_json(reader: impl Read) -> OcpmResult<CanonicalLog> {
    let value: Value = serde_json::from_reader(reader)
        .map_err(|error| OcpmError::invalid_data(format!("invalid OCEL JSON: {error}")))?;
    read_ocel2_json_value(&value)
}

fn read_ocel2_json_value(root: &Value) -> OcpmResult<CanonicalLog> {
    let root = root
        .as_object()
        .ok_or_else(|| OcpmError::invalid_data("OCEL JSON root must be an object"))?;
    let mut raw_objects = required_array(root, "objects")?
        .iter()
        .map(parse_raw_object)
        .collect::<OcpmResult<Vec<_>>>()?;
    raw_objects.sort_by(|left, right| left.0.cmp(&right.0));
    let mut objects = Vec::with_capacity(raw_objects.len());
    let mut object_ids = BTreeMap::new();
    let mut object_attribute_history = Vec::new();
    let mut raw_o2o = Vec::new();
    for (index, (external_id, object_type, attributes, relationships)) in
        raw_objects.into_iter().enumerate()
    {
        let id = index as u64 + 1;
        object_ids.insert(external_id.clone(), id);
        objects.push(Object {
            id,
            external_id,
            object_type,
        });
        for (name, valid_from, value) in attributes {
            object_attribute_history.push(ObjectAttributeChange {
                object_id: id,
                name,
                valid_from,
                value,
            });
        }
        raw_o2o.extend(
            relationships
                .into_iter()
                .map(|(target, qualifier)| (id, target, qualifier)),
        );
    }

    let mut raw_events = required_array(root, "events")?
        .iter()
        .map(parse_raw_event)
        .collect::<OcpmResult<Vec<_>>>()?;
    raw_events.sort_by(|left, right| {
        left.2
            .cmp(&right.2)
            .then_with(|| left.0.cmp(&right.0))
    });
    let mut events = Vec::with_capacity(raw_events.len());
    let mut event_object_relations = Vec::new();
    for (index, (external_id, activity, timestamp, attributes, relationships)) in
        raw_events.into_iter().enumerate()
    {
        let event_id = index as u64 + 1;
        events.push(Event {
            id: event_id,
            external_id,
            activity,
            timestamp,
            sequence: 0,
            lifecycle: None,
            attributes,
        });
        for (target, qualifier) in relationships {
            let object_id = object_ids.get(&target).copied().ok_or_else(|| {
                OcpmError::invalid_data(format!(
                    "event relationship references unknown object {target}"
                ))
            })?;
            event_object_relations.push(EventObjectRelation {
                relation_id: event_object_relations.len() as u64 + 1,
                event_id,
                object_id,
                qualifier,
            });
        }
    }
    let mut object_object_relations = Vec::new();
    for (source_object_id, target, qualifier) in raw_o2o {
        let target_object_id = object_ids.get(&target).copied().ok_or_else(|| {
            OcpmError::invalid_data(format!(
                "object relationship references unknown object {target}"
            ))
        })?;
        object_object_relations.push(ObjectObjectRelation {
            relation_id: object_object_relations.len() as u64 + 1,
            source_object_id,
            target_object_id,
            qualifier,
            valid_from: None,
            valid_to: None,
        });
    }
    let mut log = CanonicalLog {
        dataset_id: root
            .get("datasetId")
            .or_else(|| root.get("dataset_id"))
            .and_then(Value::as_str)
            .unwrap_or("ocel2-json")
            .to_owned(),
        tenant_id: root
            .get("tenantId")
            .or_else(|| root.get("tenant_id"))
            .and_then(Value::as_str)
            .unwrap_or("default")
            .to_owned(),
        events,
        objects,
        event_object_relations,
        object_object_relations,
        object_attribute_history,
        ..CanonicalLog::default()
    };
    log.validate()?;
    log.sort_canonical();
    Ok(log)
}

type RawObject = (
    String,
    String,
    Vec<(String, Timestamp, AttributeValue)>,
    Vec<(String, String)>,
);

fn parse_raw_object(value: &Value) -> OcpmResult<RawObject> {
    let value = value
        .as_object()
        .ok_or_else(|| OcpmError::invalid_data("OCEL object must be an object"))?;
    let id = required_string_any(value, &["id", "ocel:oid"])?;
    let object_type = required_string_any(value, &["type", "ocel:type"])?;
    let attributes = value
        .get("attributes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|attribute| {
            let attribute = attribute.as_object().ok_or_else(|| {
                OcpmError::invalid_data("object attribute must be an object")
            })?;
            Ok((
                required_string_any(attribute, &["name", "ocel:name"] )?,
                parse_timestamp(
                    attribute
                        .get("time")
                        .or_else(|| attribute.get("ocel:time"))
                        .ok_or_else(|| OcpmError::invalid_data("object attribute needs time"))?,
                    None,
                )?,
                json_to_attribute(attribute.get("value").unwrap_or(&Value::Null))?,
            ))
        })
        .collect::<OcpmResult<Vec<_>>>()?;
    let relationships = parse_relationships(value.get("relationships"))?;
    Ok((id, object_type, attributes, relationships))
}

type RawEvent = (
    String,
    String,
    Timestamp,
    BTreeMap<String, AttributeValue>,
    Vec<(String, String)>,
);

fn parse_raw_event(value: &Value) -> OcpmResult<RawEvent> {
    let value = value
        .as_object()
        .ok_or_else(|| OcpmError::invalid_data("OCEL event must be an object"))?;
    let id = required_string_any(value, &["id", "ocel:eid"])?;
    let activity = required_string_any(value, &["type", "activity", "ocel:activity"])?;
    let timestamp = parse_timestamp(
        value
            .get("time")
            .or_else(|| value.get("timestamp"))
            .or_else(|| value.get("ocel:timestamp"))
            .ok_or_else(|| OcpmError::invalid_data("OCEL event needs time"))?,
        None,
    )?;
    let attributes = parse_event_attributes(value.get("attributes"))?;
    let relationships = parse_relationships(value.get("relationships"))?;
    Ok((id, activity, timestamp, attributes, relationships))
}

fn parse_event_attributes(value: Option<&Value>) -> OcpmResult<BTreeMap<String, AttributeValue>> {
    match value {
        None => Ok(BTreeMap::new()),
        Some(Value::Object(values)) => values
            .iter()
            .map(|(name, value)| Ok((name.clone(), json_to_attribute(value)?)))
            .collect(),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                let value = value.as_object().ok_or_else(|| {
                    OcpmError::invalid_data("event attribute must be an object")
                })?;
                Ok((
                    required_string_any(value, &["name", "ocel:name"] )?,
                    json_to_attribute(value.get("value").unwrap_or(&Value::Null))?,
                ))
            })
            .collect(),
        Some(_) => Err(OcpmError::invalid_data(
            "event attributes must be an object or array",
        )),
    }
}

fn parse_relationships(value: Option<&Value>) -> OcpmResult<Vec<(String, String)>> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|relation| {
            let relation = relation
                .as_object()
                .ok_or_else(|| OcpmError::invalid_data("relationship must be an object"))?;
            Ok((
                required_string_any(relation, &["objectId", "object_id", "ocel:oid"] )?,
                relation
                    .get("qualifier")
                    .or_else(|| relation.get("ocel:qualifier"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            ))
        })
        .collect()
}

pub fn read_csv(reader: impl Read, mapping: &CsvMapping) -> OcpmResult<CanonicalLog> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(mapping.delimiter)
        .from_reader(reader);
    let headers = reader
        .headers()
        .map_err(|error| OcpmError::invalid_data(format!("invalid CSV header: {error}")))?
        .clone();
    let column = |name: &str| {
        headers
            .iter()
            .position(|header| header == name)
            .ok_or_else(|| OcpmError::invalid_data(format!("CSV column {name} is missing")))
    };
    let case_column = column(&mapping.case_id)?;
    let activity_column = column(&mapping.activity)?;
    let timestamp_column = column(&mapping.timestamp)?;
    let event_id_column = mapping.event_id.as_deref().map(column).transpose()?;
    let reserved = [case_column, activity_column, timestamp_column]
        .into_iter()
        .chain(event_id_column)
        .collect::<std::collections::BTreeSet<_>>();
    let mut rows = Vec::new();
    for (row_index, row) in reader.records().enumerate() {
        let row = row.map_err(|error| {
            OcpmError::invalid_data(format!("invalid CSV row {}: {error}", row_index + 2))
        })?;
        let required = |index: usize, name: &str| {
            row.get(index)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    OcpmError::invalid_data(format!(
                        "CSV row {} has no {name}",
                        row_index + 2
                    ))
                })
        };
        let case_id = required(case_column, &mapping.case_id)?.to_owned();
        let activity = required(activity_column, &mapping.activity)?.to_owned();
        let timestamp = parse_timestamp(
            &Value::String(required(timestamp_column, &mapping.timestamp)?.to_owned()),
            mapping.timestamp_format.as_deref(),
        )?;
        let event_id = event_id_column
            .and_then(|index| row.get(index))
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("row-{}", row_index + 2));
        let attributes = headers
            .iter()
            .enumerate()
            .filter(|(index, _)| !reserved.contains(index))
            .filter_map(|(index, name)| {
                row.get(index)
                    .filter(|value| !value.is_empty())
                    .map(|value| (name.to_owned(), AttributeValue::String(value.to_owned())))
            })
            .collect();
        rows.push((event_id, case_id, activity, timestamp, attributes));
    }
    rows.sort_by(|left, right| left.3.cmp(&right.3).then_with(|| left.0.cmp(&right.0)));
    build_flat_log(rows, &mapping.object_type, "csv")
}

pub fn write_csv(
    writer: impl Write,
    log: &CanonicalLog,
    mapping: &CsvMapping,
) -> OcpmResult<()> {
    log.validate()?;
    let mut attributes = log
        .events
        .iter()
        .flat_map(|event| event.attributes.keys().cloned())
        .collect::<std::collections::BTreeSet<_>>();
    attributes.remove(&mapping.case_id);
    attributes.remove(&mapping.activity);
    attributes.remove(&mapping.timestamp);
    if let Some(event_id) = &mapping.event_id {
        attributes.remove(event_id);
    }
    let attributes = attributes.into_iter().collect::<Vec<_>>();
    let mut csv = csv::WriterBuilder::new()
        .delimiter(mapping.delimiter)
        .from_writer(writer);
    let mut header = vec![
        mapping.case_id.clone(),
        mapping.activity.clone(),
        mapping.timestamp.clone(),
    ];
    if let Some(event_id) = &mapping.event_id {
        header.push(event_id.clone());
    }
    header.extend(attributes.iter().cloned());
    csv.write_record(&header)
        .map_err(|error| OcpmError::invalid_data(format!("CSV export failed: {error}")))?;
    let objects = log
        .objects
        .iter()
        .map(|object| (object.id, object))
        .collect::<BTreeMap<_, _>>();
    let events = log
        .events
        .iter()
        .map(|event| (event.id, event))
        .collect::<BTreeMap<_, _>>();
    for relation in &log.event_object_relations {
        let Some(event) = events.get(&relation.event_id) else {
            continue;
        };
        let Some(object) = objects.get(&relation.object_id) else {
            continue;
        };
        if object.object_type != mapping.object_type {
            continue;
        }
        let mut row = vec![
            object.external_id.clone(),
            event.activity.clone(),
            timestamp_text(&event.timestamp)?,
        ];
        if mapping.event_id.is_some() {
            row.push(event.external_id.clone());
        }
        row.extend(attributes.iter().map(|name| {
            event
                .attributes
                .get(name)
                .map(attribute_text)
                .unwrap_or_default()
        }));
        csv.write_record(&row)
            .map_err(|error| OcpmError::invalid_data(format!("CSV export failed: {error}")))?;
    }
    csv.flush()
        .map_err(|error| OcpmError::invalid_data(format!("CSV export failed: {error}")))
}

pub fn read_xes(reader: impl BufRead) -> OcpmResult<CanonicalLog> {
    let mut xml = Reader::from_reader(reader);
    xml.config_mut().trim_text(true);
    let mut buffer = Vec::new();
    let mut in_trace = false;
    let mut in_event = false;
    let mut case_id: Option<String> = None;
    let mut event_attributes = BTreeMap::new();
    let mut event_id: Option<String> = None;
    let mut activity: Option<String> = None;
    let mut timestamp: Option<Timestamp> = None;
    let mut rows = Vec::new();
    let mut row_number = 0_u64;
    loop {
        match xml.read_event_into(&mut buffer) {
            Ok(XmlEvent::Start(start)) if start.name().as_ref() == b"trace" => {
                in_trace = true;
                case_id = None;
            }
            Ok(XmlEvent::Start(start)) if start.name().as_ref() == b"event" => {
                in_event = true;
                event_attributes.clear();
                event_id = None;
                activity = None;
                timestamp = None;
            }
            Ok(XmlEvent::Empty(start)) | Ok(XmlEvent::Start(start))
                if matches!(start.name().as_ref(), b"string" | b"date" | b"int" | b"float" | b"boolean") =>
            {
                let (key, value) = xes_attribute(&xml, &start)?;
                if in_event {
                    match key.as_str() {
                        "concept:name" => activity = Some(value.clone()),
                        "identity:id" => event_id = Some(value.clone()),
                        "time:timestamp" => {
                            timestamp = Some(parse_timestamp(&Value::String(value.clone()), None)?)
                        }
                        _ => {
                            event_attributes.insert(key, AttributeValue::String(value));
                        }
                    }
                } else if in_trace && key == "concept:name" {
                    case_id = Some(value);
                }
            }
            Ok(XmlEvent::End(end)) if end.name().as_ref() == b"event" => {
                row_number += 1;
                rows.push((
                    event_id.take().unwrap_or_else(|| format!("event-{row_number}")),
                    case_id.clone().unwrap_or_else(|| "trace-unknown".to_owned()),
                    activity.take().ok_or_else(|| {
                        OcpmError::invalid_data("XES event lacks concept:name")
                    })?,
                    timestamp.take().ok_or_else(|| {
                        OcpmError::invalid_data("XES event lacks time:timestamp")
                    })?,
                    std::mem::take(&mut event_attributes),
                ));
                in_event = false;
            }
            Ok(XmlEvent::End(end)) if end.name().as_ref() == b"trace" => in_trace = false,
            Ok(XmlEvent::Eof) => break,
            Err(error) => {
                return Err(OcpmError::invalid_data(format!(
                    "invalid XES at byte {}: {error}",
                    xml.buffer_position()
                )))
            }
            _ => {}
        }
        buffer.clear();
    }
    rows.sort_by(|left, right| left.3.cmp(&right.3).then_with(|| left.0.cmp(&right.0)));
    build_flat_log(rows, "case", "xes")
}

pub fn write_xes(mut writer: impl Write, log: &CanonicalLog, object_type: &str) -> OcpmResult<()> {
    log.validate()?;
    writer
        .write_all(b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<log xes.version=\"1.0\">\n")
        .map_err(io_error)?;
    let events = log
        .events
        .iter()
        .map(|event| (event.id, event))
        .collect::<BTreeMap<_, _>>();
    for object in log
        .objects
        .iter()
        .filter(|object| object.object_type == object_type)
    {
        writeln!(writer, "  <trace>").map_err(io_error)?;
        writeln!(
            writer,
            "    <string key=\"concept:name\" value=\"{}\"/>",
            xml_text(&object.external_id)
        )
        .map_err(io_error)?;
        for relation in log
            .event_object_relations
            .iter()
            .filter(|relation| relation.object_id == object.id)
        {
            let Some(event) = events.get(&relation.event_id) else {
                continue;
            };
            writeln!(writer, "    <event>").map_err(io_error)?;
            for (key, value) in [
                ("concept:name", event.activity.as_str()),
                ("identity:id", event.external_id.as_str()),
            ] {
                writeln!(
                    writer,
                    "      <string key=\"{key}\" value=\"{}\"/>",
                    xml_text(value)
                )
                .map_err(io_error)?;
            }
            writeln!(
                writer,
                "      <date key=\"time:timestamp\" value=\"{}\"/>",
                xml_text(&timestamp_text(&event.timestamp)?)
            )
            .map_err(io_error)?;
            for (name, value) in &event.attributes {
                writeln!(
                    writer,
                    "      <string key=\"{}\" value=\"{}\"/>",
                    xml_text(name),
                    xml_text(&attribute_text(value))
                )
                .map_err(io_error)?;
            }
            writeln!(writer, "    </event>").map_err(io_error)?;
        }
        writeln!(writer, "  </trace>").map_err(io_error)?;
    }
    writer.write_all(b"</log>\n").map_err(io_error)
}

fn xes_attribute<R: BufRead>(xml: &Reader<R>, start: &BytesStart<'_>) -> OcpmResult<(String, String)> {
    let mut key = None;
    let mut value = None;
    for attribute in start.attributes() {
        let attribute = attribute
            .map_err(|error| OcpmError::invalid_data(format!("invalid XES attribute: {error}")))?;
        let decoded = attribute
            .decode_and_unescape_value(xml.decoder())
            .map_err(|error| OcpmError::invalid_data(format!("invalid XES text: {error}")))?
            .into_owned();
        match attribute.key.as_ref() {
            b"key" => key = Some(decoded),
            b"value" => value = Some(decoded),
            _ => {}
        }
    }
    Ok((
        key.ok_or_else(|| OcpmError::invalid_data("XES attribute lacks key"))?,
        value.ok_or_else(|| OcpmError::invalid_data("XES attribute lacks value"))?,
    ))
}

type FlatRow = (String, String, String, Timestamp, BTreeMap<String, AttributeValue>);

fn build_flat_log(rows: Vec<FlatRow>, object_type: &str, dataset_id: &str) -> OcpmResult<CanonicalLog> {
    let object_external_ids = rows
        .iter()
        .map(|row| row.1.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let object_ids = object_external_ids
        .into_iter()
        .enumerate()
        .map(|(index, external_id)| (external_id, index as u64 + 1))
        .collect::<BTreeMap<_, _>>();
    let objects = object_ids
        .iter()
        .map(|(external_id, id)| Object {
            id: *id,
            external_id: external_id.clone(),
            object_type: object_type.to_owned(),
        })
        .collect();
    let mut events = Vec::new();
    let mut relations = Vec::new();
    for (index, (external_id, case_id, activity, timestamp, attributes)) in
        rows.into_iter().enumerate()
    {
        let event_id = index as u64 + 1;
        events.push(Event {
            id: event_id,
            external_id,
            activity,
            timestamp,
            sequence: 0,
            lifecycle: None,
            attributes,
        });
        relations.push(EventObjectRelation {
            relation_id: event_id,
            event_id,
            object_id: object_ids[&case_id],
            qualifier: String::new(),
        });
    }
    let mut log = CanonicalLog {
        dataset_id: dataset_id.to_owned(),
        tenant_id: "default".to_owned(),
        events,
        objects,
        event_object_relations: relations,
        ..CanonicalLog::default()
    };
    log.validate()?;
    log.sort_canonical();
    Ok(log)
}

pub fn read_sqlite(path: impl AsRef<Path>) -> OcpmResult<CanonicalLog> {
    let connection = Connection::open(path).map_err(sqlite_error)?;
    read_sqlite_connection(&connection)
}

pub fn write_sqlite(path: impl AsRef<Path>, log: &CanonicalLog) -> OcpmResult<()> {
    log.validate()?;
    let mut connection = Connection::open(path).map_err(sqlite_error)?;
    let transaction = connection.transaction().map_err(sqlite_error)?;
    transaction
        .execute_batch(
            "CREATE TABLE object(id TEXT PRIMARY KEY, type TEXT NOT NULL);\n\
             CREATE TABLE event(id TEXT PRIMARY KEY, type TEXT NOT NULL, time TEXT NOT NULL);\n\
             CREATE TABLE event_object(event_id TEXT NOT NULL, object_id TEXT NOT NULL, qualifier TEXT NOT NULL);\n\
             CREATE INDEX event_object_object_event ON event_object(object_id,event_id);",
        )
        .map_err(sqlite_error)?;
    for object in &log.objects {
        transaction
            .execute(
                "INSERT INTO object(id,type) VALUES (?1,?2)",
                (&object.external_id, &object.object_type),
            )
            .map_err(sqlite_error)?;
    }
    for event in &log.events {
        transaction
            .execute(
                "INSERT INTO event(id,type,time) VALUES (?1,?2,?3)",
                (&event.external_id, &event.activity, timestamp_text(&event.timestamp)?),
            )
            .map_err(sqlite_error)?;
    }
    let event_ids = log
        .events
        .iter()
        .map(|event| (event.id, &event.external_id))
        .collect::<BTreeMap<_, _>>();
    let object_ids = log
        .objects
        .iter()
        .map(|object| (object.id, &object.external_id))
        .collect::<BTreeMap<_, _>>();
    for relation in &log.event_object_relations {
        transaction
            .execute(
                "INSERT INTO event_object(event_id,object_id,qualifier) VALUES (?1,?2,?3)",
                (
                    event_ids[&relation.event_id],
                    object_ids[&relation.object_id],
                    &relation.qualifier,
                ),
            )
            .map_err(sqlite_error)?;
    }
    transaction.commit().map_err(sqlite_error)
}

pub fn read_sqlite_connection(connection: &Connection) -> OcpmResult<CanonicalLog> {
    let mut objects = Vec::new();
    let mut object_ids = BTreeMap::new();
    {
        let mut statement = connection
            .prepare("SELECT id, type FROM object ORDER BY id")
            .map_err(sqlite_error)?;
        let rows = statement
            .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))
            .map_err(sqlite_error)?;
        for row in rows {
            let (external_id, object_type) = row.map_err(sqlite_error)?;
            let id = objects.len() as u64 + 1;
            object_ids.insert(external_id.clone(), id);
            objects.push(Object {
                id,
                external_id,
                object_type,
            });
        }
    }
    let mut events = Vec::new();
    let mut event_ids = BTreeMap::new();
    {
        let mut statement = connection
            .prepare("SELECT id, type, time FROM event ORDER BY time, id")
            .map_err(sqlite_error)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(sqlite_error)?;
        for row in rows {
            let (external_id, activity, time) = row.map_err(sqlite_error)?;
            let id = events.len() as u64 + 1;
            event_ids.insert(external_id.clone(), id);
            events.push(Event {
                id,
                external_id,
                activity,
                timestamp: parse_timestamp(&Value::String(time), None)?,
                sequence: 0,
                lifecycle: None,
                attributes: BTreeMap::new(),
            });
        }
    }
    let mut event_object_relations = Vec::new();
    {
        let mut statement = connection
            .prepare(
                "SELECT event_id, object_id, COALESCE(qualifier, '') FROM event_object ORDER BY event_id, object_id, qualifier",
            )
            .map_err(sqlite_error)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(sqlite_error)?;
        for row in rows {
            let (event, object, qualifier) = row.map_err(sqlite_error)?;
            event_object_relations.push(EventObjectRelation {
                relation_id: event_object_relations.len() as u64 + 1,
                event_id: *event_ids.get(&event).ok_or_else(|| {
                    OcpmError::invalid_data(format!("unknown SQLite event {event}"))
                })?,
                object_id: *object_ids.get(&object).ok_or_else(|| {
                    OcpmError::invalid_data(format!("unknown SQLite object {object}"))
                })?,
                qualifier,
            });
        }
    }
    let mut log = CanonicalLog {
        dataset_id: "sqlite".to_owned(),
        tenant_id: "default".to_owned(),
        events,
        objects,
        event_object_relations,
        ..CanonicalLog::default()
    };
    log.validate()?;
    log.sort_canonical();
    Ok(log)
}

fn sqlite_error(error: rusqlite::Error) -> OcpmError {
    OcpmError::new(
        OcpmErrorCode::InvalidData,
        format!("SQLite import failed: {error}"),
    )
}

fn required_array<'a>(root: &'a Map<String, Value>, name: &str) -> OcpmResult<&'a Vec<Value>> {
    root.get(name)
        .and_then(Value::as_array)
        .ok_or_else(|| OcpmError::invalid_data(format!("OCEL JSON requires array {name}")))
}

fn required_string_any(root: &Map<String, Value>, names: &[&str]) -> OcpmResult<String> {
    names
        .iter()
        .find_map(|name| root.get(*name).and_then(Value::as_str))
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| OcpmError::invalid_data(format!("missing string field {names:?}")))
}

fn parse_timestamp(value: &Value, format: Option<&str>) -> OcpmResult<Timestamp> {
    if let Some(value) = value.as_i64() {
        return Ok(Timestamp::from_epoch_nanos(value as i128));
    }
    let value = value
        .as_str()
        .ok_or_else(|| OcpmError::invalid_data("timestamp must be a string or integer nanoseconds"))?;
    let parsed = if let Some(format) = format {
        chrono::NaiveDateTime::parse_from_str(value, format)
            .map(|value| value.and_utc())
            .map_err(|error| OcpmError::invalid_data(format!("invalid timestamp {value}: {error}")))?
    } else {
        DateTime::parse_from_rfc3339(value)
            .map(|value| value.with_timezone(&Utc))
            .map_err(|error| OcpmError::invalid_data(format!("invalid RFC3339 timestamp {value}: {error}")))?
    };
    let nanos = parsed.timestamp() as i128 * 1_000_000_000
        + parsed.timestamp_subsec_nanos() as i128;
    Ok(Timestamp {
        epoch_nanos_utc: nanos,
        source: Some(value.to_owned()),
    })
}

fn json_to_attribute(value: &Value) -> OcpmResult<AttributeValue> {
    match value {
        Value::Null => Ok(AttributeValue::Null),
        Value::String(value) => Ok(AttributeValue::String(value.clone())),
        Value::Bool(value) => Ok(AttributeValue::Boolean(*value)),
        Value::Number(value) => value
            .as_i64()
            .map(AttributeValue::Integer)
            .or_else(|| value.as_f64().map(AttributeValue::Float))
            .ok_or_else(|| OcpmError::invalid_data("unsupported JSON number")),
        _ => Err(OcpmError::invalid_data(
            "OCEL attribute values must be scalar",
        )),
    }
}

fn timestamp_text(timestamp: &Timestamp) -> OcpmResult<String> {
    let seconds = timestamp.epoch_nanos_utc.div_euclid(1_000_000_000);
    let nanos = timestamp.epoch_nanos_utc.rem_euclid(1_000_000_000) as u32;
    let seconds = i64::try_from(seconds)
        .map_err(|_| OcpmError::invalid_data("timestamp is outside RFC3339 range"))?;
    DateTime::<Utc>::from_timestamp(seconds, nanos)
        .map(|value| value.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
        .ok_or_else(|| OcpmError::invalid_data("timestamp is outside RFC3339 range"))
}

fn attribute_to_json(value: &AttributeValue) -> OcpmResult<Value> {
    Ok(match value {
        AttributeValue::Null => Value::Null,
        AttributeValue::String(value) => Value::String(value.clone()),
        AttributeValue::Integer(value) => Value::from(*value),
        AttributeValue::Float(value) => Value::from(*value),
        AttributeValue::Boolean(value) => Value::from(*value),
        AttributeValue::Timestamp(value) => Value::String(timestamp_text(value)?),
    })
}

fn attribute_text(value: &AttributeValue) -> String {
    match value {
        AttributeValue::Null => String::new(),
        AttributeValue::String(value) => value.clone(),
        AttributeValue::Integer(value) => value.to_string(),
        AttributeValue::Float(value) => value.to_string(),
        AttributeValue::Boolean(value) => value.to_string(),
        AttributeValue::Timestamp(value) => timestamp_text(value).unwrap_or_default(),
    }
}

fn xml_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn io_error(error: std::io::Error) -> OcpmError {
    OcpmError::invalid_data(format!("export failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn imports_small_ocel2_json() {
        let input = br#"{
          "objects":[{"id":"o1","type":"order","attributes":[],"relationships":[]}],
          "events":[{"id":"e1","type":"create","time":"2024-01-01T00:00:00Z","attributes":[],"relationships":[{"objectId":"o1","qualifier":"order"}]}]
        }"#;
        let log = read_ocel2_json(&input[..]).unwrap();
        assert_eq!(log.events.len(), 1);
        assert_eq!(log.event_object_relations.len(), 1);
    }

    #[test]
    fn imports_quoted_csv() {
        let input = b"case_id,activity,timestamp,event_id,note\n1,create,2024-01-01T00:00:00Z,e1,\"a,b\"\n";
        let log = read_csv(&input[..], &CsvMapping::default()).unwrap();
        assert_eq!(log.events[0].attributes["note"], AttributeValue::String("a,b".to_owned()));
    }
}
