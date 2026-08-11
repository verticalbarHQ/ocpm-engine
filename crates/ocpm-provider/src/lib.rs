//! Source-neutral provider contract.
//!
//! PROVENANCE: data semantics follow doi:10.1007/978-3-030-85082-1_16;
//! process executions follow doi:10.1109/ICPM57379.2022.9980730. No library
//! implementation source was consulted.

use ocpm_core::{
    AttributeValue, CanonicalLog, DatasetProfile, DatasetView, Event, EventId, ObjectId, OcpmError,
    OcpmErrorCode, OcpmResult, QueryRequest, QueryResult, event_batch::EventLogSummary,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCapability {
    CanonicalScan,
    ProcessExecutions,
    ObjectCentricQuery,
    DfgAggregate,
    VariantAggregate,
    PerformanceAggregate,
    BottleneckObservations,
    PredictionFeatures,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    LeadingObject,
    ConnectedComponent,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProcessExecution {
    pub id: String,
    pub object_type: String,
    pub object_ids: Vec<ObjectId>,
    pub events: Vec<Event>,
    #[serde(default)]
    pub event_object_ids: BTreeMap<ocpm_core::EventId, Vec<ObjectId>>,
}

impl ProcessExecution {
    pub fn activity_path(&self) -> Vec<String> {
        self.events
            .iter()
            .map(|event| event.activity.clone())
            .collect()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ProviderEstimate {
    pub startup_ns: u64,
    pub rows_read: u64,
    pub rows_returned: u64,
    pub bytes_transferred: u64,
    pub peak_memory_bytes: u64,
    pub confidence: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionSummaryRequest {
    pub view: DatasetView,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leading_object_type: Option<String>,
    #[serde(default)]
    pub complete_lifecycle: bool,
}

/// Storage-neutral input to the performance-analysis kernels.
///
/// Providers may produce these rows beside the data. The engine owns every
/// threshold, attribution, statistical test, and ranking rule so accelerated
/// and fallback execution remain semantically identical.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BottleneckObservation {
    pub object_id: ObjectId,
    pub object_type: String,
    pub source_event_id: EventId,
    pub source_activity: String,
    pub source_timestamp_nanos: i128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_lifecycle: Option<String>,
    #[serde(default)]
    pub source_attributes: BTreeMap<String, AttributeValue>,
    pub target_event_id: EventId,
    pub target_activity: String,
    pub target_timestamp_nanos: i128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_lifecycle: Option<String>,
    #[serde(default)]
    pub target_attributes: BTreeMap<String, AttributeValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BottleneckObservationRequest {
    pub view: DatasetView,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leading_object_type: Option<String>,
}

pub trait OcpmProvider: Send + Sync {
    fn name(&self) -> &'static str;
    fn semantic_version(&self) -> &'static str {
        "1.0"
    }
    fn capabilities(&self) -> Vec<ProviderCapability>;
    fn profile(&self, view: &DatasetView) -> OcpmResult<DatasetProfile>;
    fn process_executions(
        &self,
        view: &DatasetView,
        mode: ExecutionMode,
        leading_object_type: Option<&str>,
    ) -> OcpmResult<Vec<ProcessExecution>>;
    fn query(&self, request: &QueryRequest) -> OcpmResult<QueryResult>;
    fn execution_summary(
        &self,
        _request: &ExecutionSummaryRequest,
    ) -> OcpmResult<Option<EventLogSummary>> {
        Ok(None)
    }
    fn bottleneck_observations(
        &self,
        request: &BottleneckObservationRequest,
    ) -> OcpmResult<Vec<BottleneckObservation>> {
        let leading_types = bottleneck_leading_types(request);
        let mut observations = Vec::new();
        for leading in leading_types {
            let executions =
                self.process_executions(&request.view, ExecutionMode::LeadingObject, leading)?;
            observations.extend(observations_from_executions(&executions));
        }
        Ok(observations)
    }
    fn snapshot(&self, _view: &DatasetView) -> OcpmResult<CanonicalLog> {
        Err(OcpmError::new(
            OcpmErrorCode::ProviderUnavailable,
            "provider does not expose canonical snapshot export",
        ))
    }
    fn estimate(&self, view: &DatasetView, operation: ProviderCapability) -> ProviderEstimate;
}

pub fn bottleneck_leading_types(request: &BottleneckObservationRequest) -> Vec<Option<&str>> {
    if let Some(leading) = request.leading_object_type.as_deref() {
        vec![Some(leading)]
    } else if request.view.object_types.is_empty() {
        vec![None]
    } else {
        request
            .view
            .object_types
            .iter()
            .map(|value| Some(value.as_str()))
            .collect()
    }
}

/// Exact reference projection used by providers without a pushdown operator.
pub fn observations_from_executions(executions: &[ProcessExecution]) -> Vec<BottleneckObservation> {
    let mut observations = Vec::new();
    for execution in executions {
        let Some(&object_id) = execution.object_ids.first() else {
            continue;
        };
        for pair in execution.events.windows(2) {
            let source = &pair[0];
            let target = &pair[1];
            observations.push(BottleneckObservation {
                object_id,
                object_type: execution.object_type.clone(),
                source_event_id: source.id,
                source_activity: source.activity.clone(),
                source_timestamp_nanos: source.timestamp.epoch_nanos_utc,
                source_lifecycle: source.lifecycle.clone(),
                source_attributes: source.attributes.clone(),
                target_event_id: target.id,
                target_activity: target.activity.clone(),
                target_timestamp_nanos: target.timestamp.epoch_nanos_utc,
                target_lifecycle: target.lifecycle.clone(),
                target_attributes: target.attributes.clone(),
            });
        }
    }
    observations
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bottleneck_projection_keeps_every_selected_object_type() {
        let request = BottleneckObservationRequest {
            view: DatasetView {
                object_types: vec!["order".to_owned(), "item".to_owned()],
                ..DatasetView::default()
            },
            leading_object_type: None,
        };
        assert_eq!(
            bottleneck_leading_types(&request),
            vec![Some("order"), Some("item")]
        );
    }

    #[test]
    fn explicit_leading_type_has_precedence() {
        let request = BottleneckObservationRequest {
            view: DatasetView {
                object_types: vec!["order".to_owned(), "item".to_owned()],
                ..DatasetView::default()
            },
            leading_object_type: Some("shipment".to_owned()),
        };
        assert_eq!(bottleneck_leading_types(&request), vec![Some("shipment")]);
    }
}
