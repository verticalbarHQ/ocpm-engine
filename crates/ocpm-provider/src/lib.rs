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
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Clone, Debug, Default)]
pub struct ExecutionCancellation {
    cancelled: Arc<AtomicBool>,
}

impl ExecutionCancellation {
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// Request-scoped cooperative cancellation and deadline state.
///
/// This is an additive execution surface: legacy synchronous provider methods
/// remain valid and context-aware methods default to checking the context
/// immediately before and after calling them. Providers must opt into finer
/// cancellation checkpoints or backend interruption during the operation.
#[derive(Clone, Debug, Default)]
pub struct ExecutionContext {
    cancellation: ExecutionCancellation,
    deadline: Option<Instant>,
}

impl ExecutionContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        let now = Instant::now();
        Self {
            cancellation: ExecutionCancellation::default(),
            // An unrepresentable deadline must fail closed instead of silently
            // disabling the timeout.
            deadline: Some(now.checked_add(timeout).unwrap_or(now)),
        }
    }

    pub fn with_deadline(deadline: Instant) -> Self {
        Self {
            cancellation: ExecutionCancellation::default(),
            deadline: Some(deadline),
        }
    }

    pub fn cancellation(&self) -> ExecutionCancellation {
        self.cancellation.clone()
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    pub fn is_timed_out(&self) -> bool {
        self.deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
    }

    pub fn check(&self) -> OcpmResult<()> {
        if self.is_cancelled() {
            return Err(OcpmError::new(
                OcpmErrorCode::Cancelled,
                "execution was cancelled",
            ));
        }
        if self.is_timed_out() {
            return Err(OcpmError::new(
                OcpmErrorCode::Timeout,
                "execution deadline was exceeded",
            ));
        }
        Ok(())
    }
}

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
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum PopulationSelector {
    /// Keep events in the half-open interval and every leading object touched
    /// by at least one retained event.
    EventTime {
        start: ocpm_core::Timestamp,
        end: ocpm_core::Timestamp,
    },
    /// Keep leading objects whose first execution event after non-time base
    /// filters falls in the half-open interval, then analyze their full
    /// filtered execution.
    LeadingObjectStart {
        start: ocpm_core::Timestamp,
        end: ocpm_core::Timestamp,
    },
    /// Keep leading objects whose first and last execution events after
    /// non-time base filters are both contained in the half-open interval.
    ExecutionContained {
        start: ocpm_core::Timestamp,
        end: ocpm_core::Timestamp,
    },
    /// Intersect an externally supplied stable leading-object set with the
    /// base view and source contents.
    CaseSet { object_ids: Vec<ObjectId> },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PopulationResolution {
    /// The resolved view, or `None` when the selector resolved to an explicitly
    /// empty population. `DatasetView::object_ids == []` means "no filter", so
    /// it cannot safely represent an empty population.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub view: Option<DatasetView>,
    pub object_count: u64,
}

impl PopulationResolution {
    pub fn new(view: DatasetView, object_count: u64) -> Self {
        Self {
            view: (object_count > 0).then_some(view),
            object_count,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CapabilityCoverage {
    pub available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<String>,
}

impl CapabilityCoverage {
    pub fn available() -> Self {
        Self {
            available: true,
            reason_code: None,
        }
    }

    pub fn unavailable(reason_code: impl Into<String>) -> Self {
        Self {
            available: false,
            reason_code: Some(reason_code.into()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CapabilityReport {
    pub lifecycle: CapabilityCoverage,
    pub resource: CapabilityCoverage,
    pub shared_event: CapabilityCoverage,
    pub resource_calendar: CapabilityCoverage,
}

impl Default for CapabilityReport {
    fn default() -> Self {
        let unknown = || CapabilityCoverage::unavailable("provider_capability_not_declared");
        Self {
            lifecycle: unknown(),
            resource: unknown(),
            shared_event: unknown(),
            resource_calendar: unknown(),
        }
    }
}

impl CapabilityReport {
    pub fn new(
        lifecycle: CapabilityCoverage,
        resource: CapabilityCoverage,
        shared_event: CapabilityCoverage,
        resource_calendar: CapabilityCoverage,
    ) -> Self {
        Self {
            lifecycle,
            resource,
            shared_event,
            resource_calendar,
        }
    }
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

/// The subset of optional observation payloads used by an analysis request.
///
/// Providers that can project fields at the source may use this hint to avoid
/// materializing unrelated event attributes. The legacy observation method
/// remains the compatibility fallback and therefore existing providers do not
/// need to implement a new method.
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct BottleneckObservationProjection {
    pub attribute_names: Vec<String>,
}

impl BottleneckObservationProjection {
    pub fn new(attribute_names: Vec<String>) -> Self {
        Self { attribute_names }
    }
}

pub trait OcpmProvider: Send + Sync {
    fn name(&self) -> &'static str;
    fn semantic_version(&self) -> &'static str {
        "1.0"
    }
    fn capabilities(&self) -> Vec<ProviderCapability>;
    /// Report which optional analysis inputs the provider contract preserves.
    ///
    /// This is a source-contract declaration, not a scan for non-null values in
    /// one view. An available capability can therefore produce a valid empty
    /// result, while an unavailable capability means the provider cannot
    /// evaluate that metric family from its source contract.
    fn capability_report(&self) -> OcpmResult<CapabilityReport> {
        Ok(CapabilityReport::default())
    }
    fn profile(&self, view: &DatasetView) -> OcpmResult<DatasetProfile>;
    fn profile_with_context(
        &self,
        view: &DatasetView,
        context: &ExecutionContext,
    ) -> OcpmResult<DatasetProfile> {
        context.check()?;
        let result = self.profile(view)?;
        context.check()?;
        Ok(result)
    }
    fn process_executions(
        &self,
        view: &DatasetView,
        mode: ExecutionMode,
        leading_object_type: Option<&str>,
    ) -> OcpmResult<Vec<ProcessExecution>>;
    fn process_executions_with_context(
        &self,
        view: &DatasetView,
        mode: ExecutionMode,
        leading_object_type: Option<&str>,
        context: &ExecutionContext,
    ) -> OcpmResult<Vec<ProcessExecution>> {
        context.check()?;
        let result = self.process_executions(view, mode, leading_object_type)?;
        context.check()?;
        Ok(result)
    }
    fn resolve_population(
        &self,
        base_view: &DatasetView,
        selector: &PopulationSelector,
        leading_object_type: Option<&str>,
    ) -> OcpmResult<PopulationResolution> {
        let leading_object_type = leading_object_type.ok_or_else(|| {
            OcpmError::invalid_request("population resolution requires a leading object type")
        })?;
        match selector {
            PopulationSelector::EventTime { start, end }
            | PopulationSelector::LeadingObjectStart { start, end }
            | PopulationSelector::ExecutionContained { start, end }
                if start >= end =>
            {
                return Err(OcpmError::invalid_request(
                    "population window must have start before end",
                ));
            }
            _ => {}
        }
        let mut view = base_view.clone();
        let matches_window =
            |value: &ocpm_core::Timestamp,
             start: &ocpm_core::Timestamp,
             end: &ocpm_core::Timestamp| { value >= start && value < end };
        let mut selected_ids = match selector {
            PopulationSelector::EventTime { start, end } => {
                view.start = Some(start.clone());
                view.end = Some(end.clone());
                self.process_executions(
                    &view,
                    ExecutionMode::LeadingObject,
                    Some(leading_object_type),
                )?
                .into_iter()
                .filter_map(|execution| execution.object_ids.first().copied())
                .collect::<Vec<_>>()
            }
            PopulationSelector::LeadingObjectStart { start, end } => {
                view.start = None;
                view.end = None;
                self.process_executions(
                    &view,
                    ExecutionMode::LeadingObject,
                    Some(leading_object_type),
                )?
                .into_iter()
                .filter(|execution| {
                    execution
                        .events
                        .first()
                        .is_some_and(|event| matches_window(&event.timestamp, start, end))
                })
                .filter_map(|execution| execution.object_ids.first().copied())
                .collect::<Vec<_>>()
            }
            PopulationSelector::ExecutionContained { start, end } => {
                view.start = None;
                view.end = None;
                self.process_executions(
                    &view,
                    ExecutionMode::LeadingObject,
                    Some(leading_object_type),
                )?
                .into_iter()
                .filter(|execution| {
                    execution
                        .events
                        .first()
                        .zip(execution.events.last())
                        .is_some_and(|(first, last)| {
                            &first.timestamp >= start && &last.timestamp < end
                        })
                })
                .filter_map(|execution| execution.object_ids.first().copied())
                .collect::<Vec<_>>()
            }
            PopulationSelector::CaseSet { object_ids } => {
                let requested = object_ids
                    .iter()
                    .copied()
                    .collect::<std::collections::BTreeSet<_>>();
                if view.object_ids.is_empty() {
                    view.object_ids = requested.into_iter().collect();
                } else {
                    view.object_ids
                        .retain(|object_id| requested.contains(object_id));
                }
                if view.object_ids.is_empty() {
                    Vec::new()
                } else {
                    self.process_executions(
                        &view,
                        ExecutionMode::LeadingObject,
                        Some(leading_object_type),
                    )?
                    .into_iter()
                    .filter_map(|execution| execution.object_ids.first().copied())
                    .collect::<Vec<_>>()
                }
            }
        };
        selected_ids.sort_unstable();
        selected_ids.dedup();
        if !matches!(selector, PopulationSelector::EventTime { .. }) {
            view.object_ids = selected_ids.clone();
        }
        Ok(PopulationResolution::new(view, selected_ids.len() as u64))
    }
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
    fn projected_bottleneck_observations(
        &self,
        request: &BottleneckObservationRequest,
        _projection: &BottleneckObservationProjection,
    ) -> OcpmResult<Vec<BottleneckObservation>> {
        self.bottleneck_observations(request)
    }
    fn projected_bottleneck_observations_with_context(
        &self,
        request: &BottleneckObservationRequest,
        projection: &BottleneckObservationProjection,
        context: &ExecutionContext,
    ) -> OcpmResult<Vec<BottleneckObservation>> {
        context.check()?;
        let result = self.projected_bottleneck_observations(request, projection)?;
        context.check()?;
        Ok(result)
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
    use std::time::{Duration, Instant};

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

    #[test]
    fn execution_context_distinguishes_cancelled_and_timeout() {
        let cancelled = ExecutionContext::new();
        cancelled.cancellation().cancel();
        assert_eq!(
            cancelled.check().unwrap_err().code,
            OcpmErrorCode::Cancelled
        );

        let timeout = ExecutionContext::with_deadline(Instant::now() - Duration::from_millis(1));
        assert_eq!(timeout.check().unwrap_err().code, OcpmErrorCode::Timeout);

        let unrepresentable = ExecutionContext::with_timeout(Duration::MAX);
        assert_eq!(
            unrepresentable.check().unwrap_err().code,
            OcpmErrorCode::Timeout
        );
    }
}
