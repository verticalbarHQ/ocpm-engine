//! Complete local provider over a canonical OCEL.
//!
//! PROVENANCE: process-execution semantics are independently implemented from
//! doi:10.1109/ICPM57379.2022.9980730; object-centric query composition follows
//! doi:10.1007/978-3-031-92474-3_23. No upstream library source was consulted.

use ocpm_core::{
    CanonicalLog, Constraint, DatasetProfile, DatasetView, Event, EventId, ObjectId, OcpmError,
    OcpmResult, QueryBinding, QueryRequest, QueryResult,
};
use ocpm_provider::{
    CapabilityCoverage, CapabilityReport, ExecutionMode, OcpmProvider, ProcessExecution,
    ProviderCapability, ProviderEstimate,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

pub struct LocalProvider {
    log: Arc<CanonicalLog>,
    objects_by_event: BTreeMap<EventId, Vec<ObjectId>>,
}

impl LocalProvider {
    pub fn new(mut log: CanonicalLog) -> OcpmResult<Self> {
        log.validate()?;
        log.sort_canonical();
        let mut objects_by_event = BTreeMap::<EventId, Vec<ObjectId>>::new();
        for relation in &log.event_object_relations {
            objects_by_event
                .entry(relation.event_id)
                .or_default()
                .push(relation.object_id);
        }
        for ids in objects_by_event.values_mut() {
            ids.sort_unstable();
        }
        Ok(Self {
            log: Arc::new(log),
            objects_by_event,
        })
    }

    pub fn log(&self) -> &CanonicalLog {
        &self.log
    }

    fn object_attribute_value_at(
        &self,
        object_id: ObjectId,
        name: &str,
        before: Option<&ocpm_core::Timestamp>,
    ) -> Option<&ocpm_core::AttributeValue> {
        self.log
            .object_attribute_history
            .iter()
            .filter(|change| {
                change.object_id == object_id
                    && change.name == name
                    && before.is_none_or(|end| change.valid_from < *end)
            })
            .max_by(|left, right| left.valid_from.cmp(&right.valid_from))
            .map(|change| &change.value)
    }

    fn object_matches_view(&self, view: &DatasetView, object_id: ObjectId) -> bool {
        let Some(object) = self.log.object(object_id) else {
            return false;
        };
        (view.object_types.is_empty() || view.object_types.contains(&object.object_type))
            && (view.object_ids.is_empty() || view.object_ids.contains(&object.id))
            && view.object_attributes.iter().all(|(name, value)| {
                self.object_attribute_value_at(object.id, name, view.end.as_ref()) == Some(value)
            })
            && (view.related_object_types.is_empty()
                || self.log.object_object_relations.iter().any(|relation| {
                    let related = if relation.source_object_id == object.id {
                        self.log.object(relation.target_object_id)
                    } else if relation.target_object_id == object.id {
                        self.log.object(relation.source_object_id)
                    } else {
                        None
                    };
                    related.is_some_and(|related| {
                        view.related_object_types.contains(&related.object_type)
                    })
                }))
    }

    fn event_matches_view(&self, view: &DatasetView, event: &Event) -> bool {
        view.contains_timestamp(&event.timestamp)
            && (view.event_ids.is_empty() || view.event_ids.contains(&event.id))
            && (view.activities.is_empty() || view.activities.contains(&event.activity))
            && view
                .event_attributes
                .iter()
                .all(|(name, value)| event.attributes.get(name) == Some(value))
            && (view.statuses.is_empty()
                || event.attributes.get("status").is_some_and(|value| {
                    matches!(value, ocpm_core::AttributeValue::String(status) if view.statuses.contains(status))
                }))
    }

    fn execution_duration_matches(view: &DatasetView, events: &[Event]) -> bool {
        let duration = events
            .first()
            .zip(events.last())
            .map(|(first, last)| {
                last.timestamp
                    .epoch_nanos_utc
                    .saturating_sub(first.timestamp.epoch_nanos_utc)
            })
            .unwrap_or_default();
        view.minimum_execution_duration_nanos
            .is_none_or(|minimum| duration >= minimum)
            && view
                .maximum_execution_duration_nanos
                .is_none_or(|maximum| duration <= maximum)
    }

    fn filtered_events_for_objects(
        &self,
        view: &DatasetView,
        object_ids: &[ObjectId],
    ) -> Vec<Event> {
        let mut event_ids = BTreeSet::new();
        let allowed_qualifiers = view.qualifiers.iter().collect::<BTreeSet<_>>();
        for object_id in object_ids {
            event_ids.extend(
                self.log
                    .event_object_relations
                    .iter()
                    .filter_map(|relation| {
                        (relation.object_id == *object_id
                            && (allowed_qualifiers.is_empty()
                                || allowed_qualifiers.contains(&relation.qualifier)))
                        .then_some(relation.event_id)
                    }),
            );
        }
        let mut events = self
            .log
            .events
            .iter()
            .filter(|event| event_ids.contains(&event.id) && self.event_matches_view(view, event))
            .cloned()
            .collect::<Vec<_>>();
        events.sort_by(|left, right| {
            left.timestamp
                .cmp(&right.timestamp)
                .then_with(|| left.sequence.cmp(&right.sequence))
                .then_with(|| left.external_id.cmp(&right.external_id))
        });
        events
    }

    fn leading_executions(
        &self,
        view: &DatasetView,
        leading_object_type: Option<&str>,
    ) -> Vec<ProcessExecution> {
        let leading_type =
            leading_object_type.or_else(|| view.object_types.first().map(String::as_str));
        self.log
            .objects
            .iter()
            .filter(|object| {
                leading_type.is_none_or(|kind| object.object_type == kind)
                    && self.object_matches_view(view, object.id)
            })
            .map(|object| ProcessExecution {
                id: format!("object:{}", object.external_id),
                object_type: object.object_type.clone(),
                object_ids: vec![object.id],
                events: self.filtered_events_for_objects(view, &[object.id]),
                event_object_ids: self.event_objects(&[object.id]),
            })
            .filter(|execution| {
                !execution.events.is_empty()
                    && Self::execution_duration_matches(view, &execution.events)
            })
            .collect()
    }

    fn connected_executions(&self, view: &DatasetView) -> Vec<ProcessExecution> {
        let object_count = self.log.objects.len();
        let mut indexes = HashMap::new();
        for (index, object) in self.log.objects.iter().enumerate() {
            indexes.insert(object.id, index);
        }
        let mut union = UnionFind::new(object_count);
        for object_ids in self.objects_by_event.values() {
            if let Some((&first, rest)) = object_ids.split_first() {
                if let Some(&first_index) = indexes.get(&first) {
                    for object_id in rest {
                        if let Some(&index) = indexes.get(object_id) {
                            union.join(first_index, index);
                        }
                    }
                }
            }
        }
        for relation in &self.log.object_object_relations {
            if let (Some(&left), Some(&right)) = (
                indexes.get(&relation.source_object_id),
                indexes.get(&relation.target_object_id),
            ) {
                union.join(left, right);
            }
        }

        let mut groups = BTreeMap::<usize, Vec<ObjectId>>::new();
        for (index, object) in self.log.objects.iter().enumerate() {
            if self.object_matches_view(view, object.id) {
                groups.entry(union.root(index)).or_default().push(object.id);
            }
        }
        groups
            .into_iter()
            .filter_map(|(root, mut object_ids)| {
                object_ids.sort_unstable();
                let events = self.filtered_events_for_objects(view, &object_ids);
                (!events.is_empty() && Self::execution_duration_matches(view, &events)).then(|| {
                    ProcessExecution {
                        id: format!("component:{root}"),
                        object_type: "connected_component".to_owned(),
                        event_object_ids: self.event_objects(&object_ids),
                        object_ids,
                        events,
                    }
                })
            })
            .collect()
    }

    fn evaluate_constraint(
        &self,
        execution: &ProcessExecution,
        constraint: &Constraint,
        labels: &mut Vec<String>,
    ) -> bool {
        match constraint {
            Constraint::EventId { event_ids } => execution
                .events
                .iter()
                .any(|event| event_ids.contains(&event.id)),
            Constraint::ObjectId { object_ids } => execution
                .object_ids
                .iter()
                .any(|object_id| object_ids.contains(object_id)),
            Constraint::EventActivity { activities } => execution
                .events
                .iter()
                .any(|event| activities.contains(&event.activity)),
            Constraint::EventAttributeEquals { name, value } => execution
                .events
                .iter()
                .any(|event| event.attributes.get(name) == Some(value)),
            Constraint::ObjectAttributeEquals { name, value } => {
                execution.object_ids.iter().any(|object_id| {
                    self.object_attribute_value_at(
                        *object_id,
                        name,
                        execution.events.last().map(|event| &event.timestamp),
                    ) == Some(value)
                })
            }
            Constraint::ObjectType { object_types } => execution.object_ids.iter().any(|id| {
                self.log
                    .object(*id)
                    .is_some_and(|object| object_types.contains(&object.object_type))
            }),
            Constraint::E2oQualifier { qualifiers } => {
                let events = execution
                    .events
                    .iter()
                    .map(|event| event.id)
                    .collect::<BTreeSet<_>>();
                let objects = execution
                    .object_ids
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>();
                self.log.event_object_relations.iter().any(|relation| {
                    events.contains(&relation.event_id)
                        && objects.contains(&relation.object_id)
                        && qualifiers.contains(&relation.qualifier)
                })
            }
            Constraint::O2oQualifier { qualifiers } => {
                let objects = execution
                    .object_ids
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>();
                self.log.object_object_relations.iter().any(|relation| {
                    objects.contains(&relation.source_object_id)
                        && objects.contains(&relation.target_object_id)
                        && qualifiers.contains(&relation.qualifier)
                })
            }
            Constraint::DirectlyFollows { source, target } => execution
                .events
                .windows(2)
                .any(|pair| pair[0].activity == *source && pair[1].activity == *target),
            Constraint::EventuallyFollows { source, target } => {
                let mut source_seen = false;
                execution.events.iter().any(|event| {
                    let matched = source_seen && event.activity == *target;
                    if event.activity == *source {
                        source_seen = true;
                    }
                    matched
                })
            }
            Constraint::TemporalDistance {
                source,
                target,
                minimum_nanos,
                maximum_nanos,
            } => execution
                .events
                .iter()
                .enumerate()
                .any(|(left_index, left)| {
                    left.activity == *source
                        && execution.events[left_index + 1..].iter().any(|right| {
                            if right.activity != *target {
                                return false;
                            }
                            let distance = right
                                .timestamp
                                .epoch_nanos_utc
                                .saturating_sub(left.timestamp.epoch_nanos_utc);
                            minimum_nanos.is_none_or(|minimum| distance >= minimum)
                                && maximum_nanos.is_none_or(|maximum| distance <= maximum)
                        })
                }),
            Constraint::Relationship {
                source_object_type,
                target_object_type,
                qualifier,
            } => {
                let objects = execution
                    .object_ids
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>();
                self.log.object_object_relations.iter().any(|relation| {
                    objects.contains(&relation.source_object_id)
                        && objects.contains(&relation.target_object_id)
                        && qualifier
                            .as_ref()
                            .is_none_or(|value| relation.qualifier == *value)
                        && source_object_type.as_ref().is_none_or(|value| {
                            self.log
                                .object(relation.source_object_id)
                                .is_some_and(|object| object.object_type == *value)
                        })
                        && target_object_type.as_ref().is_none_or(|value| {
                            self.log
                                .object(relation.target_object_id)
                                .is_some_and(|object| object.object_type == *value)
                        })
                })
            }
            Constraint::ChildCount {
                child,
                minimum,
                maximum,
            } => {
                let count = execution
                    .events
                    .iter()
                    .filter(|event| {
                        let sub = ProcessExecution {
                            id: execution.id.clone(),
                            object_type: execution.object_type.clone(),
                            object_ids: execution.object_ids.clone(),
                            events: vec![(*event).clone()],
                            event_object_ids: execution.event_object_ids.clone(),
                        };
                        self.evaluate_constraint(&sub, child, &mut Vec::new())
                    })
                    .count() as u64;
                count >= *minimum && maximum.is_none_or(|maximum| count <= maximum)
            }
            Constraint::And { children } => children
                .iter()
                .all(|child| self.evaluate_constraint(execution, child, labels)),
            Constraint::Or { children } => children
                .iter()
                .any(|child| self.evaluate_constraint(execution, child, labels)),
            Constraint::Not { child } => !self.evaluate_constraint(execution, child, labels),
            Constraint::Label { name, child } => {
                let matched = self.evaluate_constraint(execution, child, labels);
                if matched {
                    labels.push(name.clone());
                }
                matched
            }
        }
    }

    fn event_objects(&self, selected: &[ObjectId]) -> BTreeMap<EventId, Vec<ObjectId>> {
        let selected = selected.iter().copied().collect::<BTreeSet<_>>();
        self.objects_by_event
            .iter()
            .filter_map(|(event_id, object_ids)| {
                let values = object_ids
                    .iter()
                    .copied()
                    .filter(|object_id| selected.contains(object_id))
                    .collect::<Vec<_>>();
                (!values.is_empty()).then_some((*event_id, values))
            })
            .collect()
    }
}

impl OcpmProvider for LocalProvider {
    fn name(&self) -> &'static str {
        "local"
    }

    fn capabilities(&self) -> Vec<ProviderCapability> {
        vec![
            ProviderCapability::CanonicalScan,
            ProviderCapability::ProcessExecutions,
            ProviderCapability::ObjectCentricQuery,
            ProviderCapability::DfgAggregate,
            ProviderCapability::VariantAggregate,
            ProviderCapability::PerformanceAggregate,
            ProviderCapability::BottleneckObservations,
            ProviderCapability::PredictionFeatures,
        ]
    }

    fn capability_report(&self) -> OcpmResult<CapabilityReport> {
        Ok(CapabilityReport::new(
            CapabilityCoverage::available(),
            CapabilityCoverage::available(),
            CapabilityCoverage::available(),
            CapabilityCoverage::unavailable("resource_calendar_not_supplied"),
        ))
    }

    fn profile(&self, view: &DatasetView) -> OcpmResult<DatasetProfile> {
        let allowed_qualifiers = view.qualifiers.iter().collect::<BTreeSet<_>>();
        let selected_object_ids = self
            .log
            .objects
            .iter()
            .filter(|object| self.object_matches_view(view, object.id))
            .map(|object| object.id)
            .collect::<BTreeSet<_>>();
        let selected_relations = self
            .log
            .event_object_relations
            .iter()
            .filter(|relation| {
                selected_object_ids.contains(&relation.object_id)
                    && (allowed_qualifiers.is_empty()
                        || allowed_qualifiers.contains(&relation.qualifier))
            })
            .collect::<Vec<_>>();
        let selected_event_ids = selected_relations
            .iter()
            .map(|relation| relation.event_id)
            .collect::<BTreeSet<_>>();
        let events = self
            .log
            .events
            .iter()
            .filter(|event| {
                self.event_matches_view(view, event)
                    && (view.object_types.is_empty()
                        && view.object_ids.is_empty()
                        && view.object_attributes.is_empty()
                        && view.related_object_types.is_empty()
                        || selected_event_ids.contains(&event.id))
            })
            .collect::<Vec<_>>();
        let objects = self
            .log
            .objects
            .iter()
            .filter(|object| self.object_matches_view(view, object.id))
            .collect::<Vec<_>>();
        let mut profile = DatasetProfile {
            dataset_id: self.log.dataset_id.clone(),
            tenant_id: self.log.tenant_id.clone(),
            source_watermark: self.log.source_watermark.clone(),
            event_count: events.len() as u64,
            object_count: objects.len() as u64,
            e2o_count: selected_relations.len() as u64,
            o2o_count: self
                .log
                .object_object_relations
                .iter()
                .filter(|relation| {
                    selected_object_ids.contains(&relation.source_object_id)
                        && selected_object_ids.contains(&relation.target_object_id)
                        && (allowed_qualifiers.is_empty()
                            || allowed_qualifiers.contains(&relation.qualifier))
                        && relation
                            .valid_from
                            .as_ref()
                            .is_none_or(|from| view.end.as_ref().is_none_or(|end| from < end))
                        && relation
                            .valid_to
                            .as_ref()
                            .is_none_or(|to| view.start.as_ref().is_none_or(|start| to > start))
                })
                .count() as u64,
            object_attribute_change_count: self
                .log
                .object_attribute_history
                .iter()
                .filter(|change| {
                    selected_object_ids.contains(&change.object_id)
                        && view.contains_timestamp(&change.valid_from)
                })
                .count() as u64,
            ..DatasetProfile::default()
        };
        for event in events {
            *profile
                .activities
                .entry(event.activity.clone())
                .or_default() += 1;
            profile.start = Some(profile.start.map_or_else(
                || event.timestamp.clone(),
                |value| value.min(event.timestamp.clone()),
            ));
            profile.end = Some(profile.end.map_or_else(
                || event.timestamp.clone(),
                |value| value.max(event.timestamp.clone()),
            ));
        }
        for object in objects {
            *profile
                .object_types
                .entry(object.object_type.clone())
                .or_default() += 1;
        }
        Ok(profile)
    }

    fn process_executions(
        &self,
        view: &DatasetView,
        mode: ExecutionMode,
        leading_object_type: Option<&str>,
    ) -> OcpmResult<Vec<ProcessExecution>> {
        Ok(match mode {
            ExecutionMode::LeadingObject => self.leading_executions(view, leading_object_type),
            ExecutionMode::ConnectedComponent => self.connected_executions(view),
        })
    }

    fn query(&self, request: &QueryRequest) -> OcpmResult<QueryResult> {
        if request.semantic_version != "1.0" {
            return Err(OcpmError::invalid_request("semantic_version must be 1.0"));
        }
        let executions = self.leading_executions(&request.view, None);
        let mut bindings = Vec::new();
        let mut total_matches = 0_u64;
        for execution in executions {
            let mut labels = Vec::new();
            if self.evaluate_constraint(&execution, &request.constraint, &mut labels) {
                total_matches = total_matches.saturating_add(1);
                if bindings.len() < request.limit as usize {
                    bindings.push(QueryBinding {
                        event_ids: execution.events.iter().map(|event| event.id).collect(),
                        object_ids: execution.object_ids,
                        labels,
                        violated: false,
                    });
                }
            }
        }
        Ok(QueryResult {
            truncated: total_matches > bindings.len() as u64,
            bindings,
            total_matches,
        })
    }

    fn snapshot(&self, view: &DatasetView) -> OcpmResult<CanonicalLog> {
        let object_ids = self
            .log
            .objects
            .iter()
            .filter(|object| self.object_matches_view(view, object.id))
            .map(|object| object.id)
            .collect::<BTreeSet<_>>();
        let allowed_qualifiers = view.qualifiers.iter().collect::<BTreeSet<_>>();
        let relation_event_ids = self
            .log
            .event_object_relations
            .iter()
            .filter(|relation| {
                object_ids.contains(&relation.object_id)
                    && (allowed_qualifiers.is_empty()
                        || allowed_qualifiers.contains(&relation.qualifier))
            })
            .map(|relation| relation.event_id)
            .collect::<BTreeSet<_>>();
        let events = self
            .log
            .events
            .iter()
            .filter(|event| {
                self.event_matches_view(view, event)
                    && (view.object_types.is_empty()
                        && view.object_ids.is_empty()
                        && view.object_attributes.is_empty()
                        && view.related_object_types.is_empty()
                        || relation_event_ids.contains(&event.id))
            })
            .cloned()
            .collect::<Vec<_>>();
        let event_ids = events.iter().map(|event| event.id).collect::<BTreeSet<_>>();
        let mut snapshot = CanonicalLog {
            dataset_id: self.log.dataset_id.clone(),
            tenant_id: self.log.tenant_id.clone(),
            source_watermark: self.log.source_watermark.clone(),
            events,
            objects: self
                .log
                .objects
                .iter()
                .filter(|object| object_ids.contains(&object.id))
                .cloned()
                .collect(),
            event_object_relations: self
                .log
                .event_object_relations
                .iter()
                .filter(|relation| {
                    event_ids.contains(&relation.event_id)
                        && object_ids.contains(&relation.object_id)
                        && (allowed_qualifiers.is_empty()
                            || allowed_qualifiers.contains(&relation.qualifier))
                })
                .cloned()
                .collect(),
            object_object_relations: self
                .log
                .object_object_relations
                .iter()
                .filter(|relation| {
                    object_ids.contains(&relation.source_object_id)
                        && object_ids.contains(&relation.target_object_id)
                        && (allowed_qualifiers.is_empty()
                            || allowed_qualifiers.contains(&relation.qualifier))
                        && relation
                            .valid_from
                            .as_ref()
                            .is_none_or(|from| view.end.as_ref().is_none_or(|end| from < end))
                        && relation
                            .valid_to
                            .as_ref()
                            .is_none_or(|to| view.start.as_ref().is_none_or(|start| to > start))
                })
                .cloned()
                .collect(),
            object_attribute_history: self
                .log
                .object_attribute_history
                .iter()
                .filter(|change| object_ids.contains(&change.object_id))
                .cloned()
                .collect(),
            metadata: self.log.metadata.clone(),
        };
        snapshot.sort_canonical();
        snapshot.validate()?;
        Ok(snapshot)
    }

    fn estimate(&self, view: &DatasetView, _operation: ProviderCapability) -> ProviderEstimate {
        let rows = self
            .log
            .events
            .iter()
            .filter(|event| view.contains_timestamp(&event.timestamp))
            .count() as u64;
        ProviderEstimate {
            startup_ns: 1_000,
            rows_read: rows,
            rows_returned: rows,
            bytes_transferred: 0,
            peak_memory_bytes: rows.saturating_mul(64),
            confidence: 1.0,
        }
    }
}

struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<u8>,
}

impl UnionFind {
    fn new(size: usize) -> Self {
        Self {
            parent: (0..size).collect(),
            rank: vec![0; size],
        }
    }

    fn root(&mut self, value: usize) -> usize {
        if self.parent[value] != value {
            self.parent[value] = self.root(self.parent[value]);
        }
        self.parent[value]
    }

    fn join(&mut self, left: usize, right: usize) {
        let left = self.root(left);
        let right = self.root(right);
        if left == right {
            return;
        }
        match self.rank[left].cmp(&self.rank[right]) {
            std::cmp::Ordering::Less => self.parent[left] = right,
            std::cmp::Ordering::Greater => self.parent[right] = left,
            std::cmp::Ordering::Equal => {
                self.parent[right] = left;
                self.rank[left] = self.rank[left].saturating_add(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ocpm_core::{EventObjectRelation, Object, Timestamp};
    use ocpm_provider::PopulationSelector;

    fn fixture() -> CanonicalLog {
        CanonicalLog {
            dataset_id: "fixture".to_owned(),
            tenant_id: "tenant".to_owned(),
            events: vec![
                Event {
                    id: 1,
                    external_id: "e1".to_owned(),
                    activity: "create".to_owned(),
                    timestamp: Timestamp::from_epoch_nanos(1),
                    sequence: 0,
                    lifecycle: None,
                    attributes: BTreeMap::new(),
                },
                Event {
                    id: 2,
                    external_id: "e2".to_owned(),
                    activity: "approve".to_owned(),
                    timestamp: Timestamp::from_epoch_nanos(2),
                    sequence: 0,
                    lifecycle: None,
                    attributes: BTreeMap::new(),
                },
            ],
            objects: vec![Object {
                id: 10,
                external_id: "o1".to_owned(),
                object_type: "order".to_owned(),
            }],
            event_object_relations: vec![
                EventObjectRelation {
                    relation_id: 1,
                    event_id: 1,
                    object_id: 10,
                    qualifier: String::new(),
                },
                EventObjectRelation {
                    relation_id: 2,
                    event_id: 2,
                    object_id: 10,
                    qualifier: String::new(),
                },
            ],
            ..CanonicalLog::default()
        }
    }

    #[test]
    fn leading_execution_is_ordered() {
        let provider = LocalProvider::new(fixture()).unwrap();
        let executions = provider
            .process_executions(
                &DatasetView::default(),
                ExecutionMode::LeadingObject,
                Some("order"),
            )
            .unwrap();
        assert_eq!(executions[0].activity_path(), vec!["create", "approve"]);
    }

    #[test]
    fn case_set_population_intersects_the_base_view_and_preserves_empty() {
        let mut log = fixture();
        log.events.push(Event {
            id: 3,
            external_id: "e3".to_owned(),
            activity: "create".to_owned(),
            timestamp: Timestamp::from_epoch_nanos(3),
            sequence: 0,
            lifecycle: None,
            attributes: BTreeMap::new(),
        });
        log.objects.push(Object {
            id: 20,
            external_id: "o2".to_owned(),
            object_type: "order".to_owned(),
        });
        log.event_object_relations.push(EventObjectRelation {
            relation_id: 3,
            event_id: 3,
            object_id: 20,
            qualifier: String::new(),
        });
        let provider = LocalProvider::new(log).unwrap();
        let base_view = DatasetView {
            object_ids: vec![10],
            ..DatasetView::default()
        };

        let disjoint = provider
            .resolve_population(
                &base_view,
                &PopulationSelector::CaseSet {
                    object_ids: vec![20],
                },
                Some("order"),
            )
            .unwrap();
        assert_eq!(disjoint.object_count, 0);
        assert!(disjoint.view.is_none());

        let overlap = provider
            .resolve_population(
                &base_view,
                &PopulationSelector::CaseSet {
                    object_ids: vec![10, 20],
                },
                Some("order"),
            )
            .unwrap();
        assert_eq!(overlap.object_count, 1);
        assert_eq!(overlap.view.unwrap().object_ids, vec![10]);

        let explicitly_empty = provider
            .resolve_population(
                &DatasetView::default(),
                &PopulationSelector::CaseSet { object_ids: vec![] },
                Some("order"),
            )
            .unwrap();
        assert_eq!(explicitly_empty.object_count, 0);
        assert!(explicitly_empty.view.is_none());
    }
}
