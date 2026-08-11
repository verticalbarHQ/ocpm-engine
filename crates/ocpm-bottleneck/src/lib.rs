//! Provider-neutral object-centric bottleneck analysis.
//!
//! The provider supplies canonical transition observations; this crate owns all
//! analytical semantics. Algorithms are derived only from the peer-reviewed
//! publications named by the `PROVENANCE_*` constants below. No implementation
//! source from another process-mining project was consulted.

use ocpm_core::{AttributeValue, DatasetView, EventId, ObjectId, OcpmError, OcpmResult};
use ocpm_provider::{BottleneckObservation, BottleneckObservationRequest, OcpmProvider};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

pub const PROVENANCE_OBJECT_CENTRIC_PERFORMANCE: &str = "doi:10.1007/978-3-031-17995-2_20";
pub const PROVENANCE_OCDFG_PERFORMANCE: &str = "doi:10.1007/978-3-031-70418-5_11";
pub const PROVENANCE_WAITING_CAUSES: &str = "doi:10.1016/j.is.2024.102434";
pub const PROVENANCE_QUEUE_MINING: &str = "doi:10.1016/j.is.2015.03.010";
pub const PROVENANCE_PERFORMANCE_SPECTRUM: &str = "doi:10.1007/978-3-319-98648-7_9";
pub const PROVENANCE_BATCH_DETECTION: &str = "doi:10.1007/978-3-030-37453-2_15";
pub const PROVENANCE_DRIFT: &str = "doi:10.1016/j.is.2023.102177";
pub const PROVENANCE_CAUSAL_HYPOTHESES: &str = "doi:10.1016/j.engappai.2023.107145";
pub const PROVENANCE_BLOCKING_CASCADES: &str = "doi:10.1007/s44311-026-00038-8";

fn default_semantic_version() -> String {
    "1.0".to_owned()
}
fn default_minimum_support() -> u64 {
    5
}
fn default_tail_quantile() -> f64 {
    0.95
}
fn default_batch_tolerance_seconds() -> f64 {
    60.0
}
fn default_minimum_batch_size() -> u64 {
    2
}
fn default_resource_attribute() -> String {
    "org:resource".to_owned()
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AvailabilityInterval {
    pub start_nanos: i128,
    pub end_nanos: i128,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TemporalHypothesis {
    pub id: String,
    pub cause_activity: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause_attribute: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause_value: Option<AttributeValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effect_source_activity: Option<String>,
    pub effect_target_activity: String,
    pub effect_threshold_seconds: f64,
    #[serde(default)]
    pub minimum_lag_seconds: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maximum_lag_seconds: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BottleneckRequest {
    #[serde(default = "default_semantic_version")]
    pub semantic_version: String,
    #[serde(default)]
    pub view: DatasetView,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comparison_view: Option<DatasetView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leading_object_type: Option<String>,
    #[serde(default = "default_minimum_support")]
    pub minimum_support: u64,
    #[serde(default = "default_tail_quantile")]
    pub tail_quantile: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slow_threshold_seconds: Option<f64>,
    #[serde(default = "default_batch_tolerance_seconds")]
    pub batch_tolerance_seconds: f64,
    #[serde(default = "default_minimum_batch_size")]
    pub minimum_batch_size: u64,
    #[serde(default = "default_resource_attribute")]
    pub resource_attribute: String,
    #[serde(default)]
    pub resource_calendars: BTreeMap<String, Vec<AvailabilityInterval>>,
    #[serde(default)]
    pub hypotheses: Vec<TemporalHypothesis>,
}

impl Default for BottleneckRequest {
    fn default() -> Self {
        Self {
            semantic_version: default_semantic_version(),
            view: DatasetView::default(),
            comparison_view: None,
            leading_object_type: None,
            minimum_support: default_minimum_support(),
            tail_quantile: default_tail_quantile(),
            slow_threshold_seconds: None,
            batch_tolerance_seconds: default_batch_tolerance_seconds(),
            minimum_batch_size: default_minimum_batch_size(),
            resource_attribute: default_resource_attribute(),
            resource_calendars: BTreeMap::new(),
            hypotheses: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct BottleneckSignal {
    pub object_type: String,
    pub source_activity: String,
    pub target_activity: String,
    pub support: u64,
    pub mean_seconds: f64,
    pub median_seconds: f64,
    pub p90_seconds: f64,
    pub p95_seconds: f64,
    pub tail_mean_seconds: f64,
    pub threshold_seconds: f64,
    pub affected_count: u64,
    pub affected_rate: f64,
    pub affected_rate_interval_95: (f64, f64),
    pub impact_seconds: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SynchronizationAttribution {
    pub activity: String,
    pub object_type: String,
    pub support: u64,
    pub lagging_count: u64,
    pub mean_readiness_lag_seconds: f64,
    pub p95_readiness_lag_seconds: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct WaitingCauseAttribution {
    pub activity: String,
    pub resource: String,
    pub support: u64,
    pub total_wait_seconds: f64,
    pub batching_seconds: f64,
    pub prioritization_seconds: f64,
    pub contention_seconds: f64,
    pub unavailability_seconds: f64,
    pub extraneous_seconds: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ResourcePressure {
    pub activity: String,
    pub resource: String,
    pub support: u64,
    pub arrival_rate_per_hour: f64,
    pub throughput_per_hour: f64,
    pub utilization: f64,
    pub mean_wait_seconds: f64,
    pub p95_wait_seconds: f64,
    pub mean_service_seconds: f64,
    pub maximum_queue: u64,
    pub backlog_delta: i64,
    pub unstable: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct BottleneckChange {
    pub object_type: String,
    pub source_activity: String,
    pub target_activity: String,
    pub baseline_support: u64,
    pub current_support: u64,
    pub median_delta_seconds: f64,
    pub p95_delta_seconds: f64,
    pub impact_delta_seconds: f64,
    pub ks_statistic: f64,
    pub p_value: f64,
    pub q_value: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PerformancePattern {
    pub activity: String,
    pub resource: String,
    pub support: u64,
    pub batch_rate: f64,
    pub overtaking_rate: f64,
    pub burstiness: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct BlockingCascade {
    pub activities: Vec<String>,
    pub resources: Vec<String>,
    pub support: u64,
    pub maximum_depth: u64,
    pub attributed_delay_seconds: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CausalHypothesisResult {
    pub id: String,
    pub exposed: u64,
    pub unexposed: u64,
    pub effect_given_cause: f64,
    pub effect_without_cause: f64,
    pub risk_difference: f64,
    pub risk_ratio: Option<f64>,
    pub p_value: f64,
    pub q_value: f64,
    pub supported: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct BottleneckDiagnostics {
    pub observation_count: u64,
    pub lifecycle_instance_count: u64,
    pub synchronized_event_count: u64,
    pub provider: String,
    pub exact: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct BottleneckResult {
    pub semantic_version: String,
    pub signals: Vec<BottleneckSignal>,
    pub synchronization: Vec<SynchronizationAttribution>,
    pub waiting_causes: Vec<WaitingCauseAttribution>,
    pub resource_pressure: Vec<ResourcePressure>,
    pub changes: Vec<BottleneckChange>,
    pub patterns: Vec<PerformancePattern>,
    pub cascades: Vec<BlockingCascade>,
    pub hypotheses: Vec<CausalHypothesisResult>,
    pub warnings: Vec<String>,
    pub diagnostics: BottleneckDiagnostics,
}

pub fn analyze(
    provider: &dyn OcpmProvider,
    request: &BottleneckRequest,
) -> OcpmResult<BottleneckResult> {
    validate_request(request)?;
    let observations = provider.bottleneck_observations(&BottleneckObservationRequest {
        view: request.view.clone(),
        leading_object_type: request.leading_object_type.clone(),
    })?;
    let comparison = request
        .comparison_view
        .as_ref()
        .map(|view| {
            provider.bottleneck_observations(&BottleneckObservationRequest {
                view: view.clone(),
                leading_object_type: request.leading_object_type.clone(),
            })
        })
        .transpose()?;
    analyze_observations(
        provider.name(),
        &observations,
        comparison.as_deref(),
        request,
    )
}

pub fn analyze_observations(
    provider_name: &str,
    observations: &[BottleneckObservation],
    comparison: Option<&[BottleneckObservation]>,
    request: &BottleneckRequest,
) -> OcpmResult<BottleneckResult> {
    validate_request(request)?;
    let (signals, distributions) = edge_signals(observations, request);
    let synchronization = synchronization(observations, request.minimum_support);
    let instances = lifecycle_instances(observations, &request.resource_attribute);
    let mut warnings = Vec::new();
    if instances.is_empty() {
        warnings.push(
            "queue, waiting-cause, and blocking results require paired start/complete lifecycle events"
                .to_owned(),
        );
    }
    if !instances.is_empty()
        && instances
            .iter()
            .all(|instance| instance.resource.is_empty())
    {
        warnings.push(format!(
            "resource analysis requires event attribute '{}' (or 'resource')",
            request.resource_attribute
        ));
    }
    if request.resource_calendars.is_empty() {
        warnings.push(
            "unavailability is not inferred; provide explicit resource_calendars to attribute it"
                .to_owned(),
        );
    }
    let waiting_causes = waiting_causes(&instances, request);
    let resource_pressure = resource_pressure(&instances, request.minimum_support);
    let patterns = performance_patterns(&instances, request);
    let cascades = blocking_cascades(&instances, request.minimum_support);
    let changes = comparison
        .map(|baseline| drift_changes(baseline, &distributions, request))
        .unwrap_or_default();
    let hypotheses = temporal_hypotheses(observations, request);
    Ok(BottleneckResult {
        semantic_version: "1.0".to_owned(),
        signals,
        synchronization,
        waiting_causes,
        resource_pressure,
        changes,
        patterns,
        cascades,
        hypotheses,
        warnings,
        diagnostics: BottleneckDiagnostics {
            observation_count: observations.len() as u64,
            lifecycle_instance_count: instances.len() as u64,
            synchronized_event_count: synchronized_event_count(observations),
            provider: provider_name.to_owned(),
            exact: true,
        },
    })
}

fn validate_request(request: &BottleneckRequest) -> OcpmResult<()> {
    if request.semantic_version != "1.0" {
        return Err(OcpmError::invalid_request(
            "bottleneck semantic_version must be 1.0",
        ));
    }
    if !(0.5..1.0).contains(&request.tail_quantile) {
        return Err(OcpmError::invalid_request(
            "tail_quantile must be in [0.5, 1.0)",
        ));
    }
    if request.minimum_support == 0 || request.minimum_batch_size < 2 {
        return Err(OcpmError::invalid_request(
            "minimum_support must be positive and minimum_batch_size at least 2",
        ));
    }
    if request.batch_tolerance_seconds < 0.0
        || request
            .slow_threshold_seconds
            .is_some_and(|value| value < 0.0 || !value.is_finite())
    {
        return Err(OcpmError::invalid_request(
            "duration thresholds must be finite and nonnegative",
        ));
    }
    for intervals in request.resource_calendars.values() {
        if intervals
            .iter()
            .any(|interval| interval.end_nanos <= interval.start_nanos)
        {
            return Err(OcpmError::invalid_request(
                "resource calendar intervals must have positive duration",
            ));
        }
    }
    for hypothesis in &request.hypotheses {
        if hypothesis.id.is_empty()
            || hypothesis.cause_activity.is_empty()
            || hypothesis.effect_target_activity.is_empty()
            || !hypothesis.effect_threshold_seconds.is_finite()
            || hypothesis.effect_threshold_seconds < 0.0
            || !hypothesis.minimum_lag_seconds.is_finite()
            || hypothesis.minimum_lag_seconds < 0.0
            || hypothesis.maximum_lag_seconds.is_some_and(|maximum| {
                !maximum.is_finite() || maximum < hypothesis.minimum_lag_seconds
            })
            || hypothesis.cause_attribute.is_some() != hypothesis.cause_value.is_some()
        {
            return Err(OcpmError::invalid_request(
                "temporal hypotheses require nonempty activities, finite ordered lags, a nonnegative threshold, and paired cause_attribute/cause_value",
            ));
        }
    }
    Ok(())
}

type EdgeKey = (String, String, String);

fn edge_distributions(observations: &[BottleneckObservation]) -> BTreeMap<EdgeKey, Vec<f64>> {
    let mut grouped = BTreeMap::<EdgeKey, Vec<f64>>::new();
    for observation in observations {
        let duration = nanos_to_seconds(
            observation
                .target_timestamp_nanos
                .saturating_sub(observation.source_timestamp_nanos),
        );
        if duration >= 0.0 && duration.is_finite() {
            grouped
                .entry((
                    observation.object_type.clone(),
                    observation.source_activity.clone(),
                    observation.target_activity.clone(),
                ))
                .or_default()
                .push(duration);
        }
    }
    for values in grouped.values_mut() {
        values.sort_by(f64::total_cmp);
    }
    grouped
}

fn edge_signals(
    observations: &[BottleneckObservation],
    request: &BottleneckRequest,
) -> (Vec<BottleneckSignal>, BTreeMap<EdgeKey, Vec<f64>>) {
    let distributions = edge_distributions(observations);
    let mut signals = distributions
        .iter()
        .filter(|(_, values)| values.len() as u64 >= request.minimum_support)
        .map(|((object_type, source, target), values)| {
            let q1 = quantile(values, 0.25);
            let q3 = quantile(values, 0.75);
            let threshold = request
                .slow_threshold_seconds
                .unwrap_or(q3 + 1.5 * (q3 - q1).max(0.0));
            let affected = values.iter().filter(|value| **value > threshold).count() as u64;
            let tail_start = lower_bound(values, quantile(values, request.tail_quantile));
            let tail = &values[tail_start..];
            BottleneckSignal {
                object_type: object_type.clone(),
                source_activity: source.clone(),
                target_activity: target.clone(),
                support: values.len() as u64,
                mean_seconds: mean(values),
                median_seconds: quantile(values, 0.5),
                p90_seconds: quantile(values, 0.9),
                p95_seconds: quantile(values, 0.95),
                tail_mean_seconds: mean(tail),
                threshold_seconds: threshold,
                affected_count: affected,
                affected_rate: affected as f64 / values.len() as f64,
                affected_rate_interval_95: wilson_interval(affected, values.len() as u64),
                impact_seconds: values
                    .iter()
                    .map(|value| (value - threshold).max(0.0))
                    .sum(),
            }
        })
        .collect::<Vec<_>>();
    signals.sort_by(|left, right| {
        right
            .impact_seconds
            .total_cmp(&left.impact_seconds)
            .then_with(|| right.p95_seconds.total_cmp(&left.p95_seconds))
            .then_with(|| right.affected_rate.total_cmp(&left.affected_rate))
            .then_with(|| right.support.cmp(&left.support))
            .then_with(|| left.object_type.cmp(&right.object_type))
            .then_with(|| left.source_activity.cmp(&right.source_activity))
            .then_with(|| left.target_activity.cmp(&right.target_activity))
    });
    (signals, distributions)
}

fn synchronized_event_count(observations: &[BottleneckObservation]) -> u64 {
    let mut types = BTreeMap::<EventId, BTreeSet<&str>>::new();
    for observation in observations {
        types
            .entry(observation.target_event_id)
            .or_default()
            .insert(&observation.object_type);
    }
    types.values().filter(|values| values.len() > 1).count() as u64
}

fn synchronization(
    observations: &[BottleneckObservation],
    minimum_support: u64,
) -> Vec<SynchronizationAttribution> {
    let mut by_event = BTreeMap::<EventId, Vec<&BottleneckObservation>>::new();
    for observation in observations {
        by_event
            .entry(observation.target_event_id)
            .or_default()
            .push(observation);
    }
    let mut grouped = BTreeMap::<(String, String), (Vec<f64>, u64)>::new();
    for values in by_event.values() {
        let object_types = values
            .iter()
            .map(|value| &value.object_type)
            .collect::<BTreeSet<_>>();
        if object_types.len() < 2 {
            continue;
        }
        let latest = values
            .iter()
            .map(|value| value.source_timestamp_nanos)
            .max()
            .unwrap_or_default();
        for value in values {
            let lag = nanos_to_seconds(latest.saturating_sub(value.source_timestamp_nanos));
            let entry = grouped
                .entry((value.target_activity.clone(), value.object_type.clone()))
                .or_default();
            entry.0.push(lag);
            if value.source_timestamp_nanos == latest {
                entry.1 += 1;
            }
        }
    }
    grouped
        .into_iter()
        .filter_map(|((activity, object_type), (mut lags, lagging_count))| {
            if (lags.len() as u64) < minimum_support {
                return None;
            }
            lags.sort_by(f64::total_cmp);
            Some(SynchronizationAttribution {
                activity,
                object_type,
                support: lags.len() as u64,
                lagging_count,
                mean_readiness_lag_seconds: mean(&lags),
                p95_readiness_lag_seconds: quantile(&lags, 0.95),
            })
        })
        .collect()
}

#[derive(Clone, Debug)]
struct EventPoint {
    event_id: EventId,
    activity: String,
    timestamp: i128,
    lifecycle: Option<String>,
    attributes: BTreeMap<String, AttributeValue>,
}

#[derive(Clone, Debug)]
struct ActivityInstance {
    id: usize,
    activity: String,
    resource: String,
    ready: i128,
    start: i128,
    complete: i128,
}

fn lifecycle_instances(
    observations: &[BottleneckObservation],
    resource_attribute: &str,
) -> Vec<ActivityInstance> {
    let mut points =
        BTreeMap::<(ObjectId, String), BTreeMap<(i128, u64, EventId), EventPoint>>::new();
    for observation in observations {
        let events = [
            EventPoint {
                event_id: observation.source_event_id,
                activity: observation.source_activity.clone(),
                timestamp: observation.source_timestamp_nanos,
                lifecycle: observation.source_lifecycle.clone(),
                attributes: observation.source_attributes.clone(),
            },
            EventPoint {
                event_id: observation.target_event_id,
                activity: observation.target_activity.clone(),
                timestamp: observation.target_timestamp_nanos,
                lifecycle: observation.target_lifecycle.clone(),
                attributes: observation.target_attributes.clone(),
            },
        ];
        for point in events {
            points
                .entry((observation.object_id, observation.object_type.clone()))
                .or_default()
                .entry((point.timestamp, point.event_id, point.event_id))
                .or_insert(point);
        }
    }
    let mut result = Vec::new();
    for ((_object_id, _object_type), ordered) in points {
        let values = ordered.into_values().collect::<Vec<_>>();
        let mut starts = BTreeMap::<String, VecDeque<(i128, i128, String)>>::new();
        for (index, point) in values.iter().enumerate() {
            let lifecycle = point
                .lifecycle
                .as_deref()
                .unwrap_or_default()
                .to_ascii_lowercase();
            if matches!(lifecycle.as_str(), "start" | "begin" | "resume") {
                let ready = index
                    .checked_sub(1)
                    .map(|previous| values[previous].timestamp)
                    .unwrap_or(point.timestamp);
                starts
                    .entry(point.activity.clone())
                    .or_default()
                    .push_back((ready, point.timestamp, resource(point, resource_attribute)));
            } else if matches!(lifecycle.as_str(), "complete" | "end" | "suspend") {
                let Some((ready, start, start_resource)) = starts
                    .get_mut(&point.activity)
                    .and_then(VecDeque::pop_front)
                else {
                    continue;
                };
                if point.timestamp < start {
                    continue;
                }
                let complete_resource = resource(point, resource_attribute);
                let resource = if complete_resource.is_empty() {
                    start_resource
                } else {
                    complete_resource
                };
                result.push(ActivityInstance {
                    id: result.len(),
                    activity: point.activity.clone(),
                    resource,
                    ready,
                    start,
                    complete: point.timestamp,
                });
            }
        }
    }
    result
}

fn resource(point: &EventPoint, requested: &str) -> String {
    [requested, "resource"]
        .into_iter()
        .find_map(|name| match point.attributes.get(name) {
            Some(AttributeValue::String(value)) => Some(value.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

fn waiting_causes(
    instances: &[ActivityInstance],
    request: &BottleneckRequest,
) -> Vec<WaitingCauseAttribution> {
    let mut batch_delay = HashMap::<usize, f64>::new();
    let tolerance = seconds_to_nanos(request.batch_tolerance_seconds);
    let mut by_group = BTreeMap::<(String, String), Vec<&ActivityInstance>>::new();
    for instance in instances.iter().filter(|value| !value.resource.is_empty()) {
        by_group
            .entry((instance.activity.clone(), instance.resource.clone()))
            .or_default()
            .push(instance);
    }
    for values in by_group.values_mut() {
        values.sort_by_key(|value| value.start);
        let mut begin = 0;
        while begin < values.len() {
            let mut end = begin + 1;
            while end < values.len()
                && values[end].start.saturating_sub(values[end - 1].start) <= tolerance
            {
                end += 1;
            }
            if (end - begin) as u64 >= request.minimum_batch_size {
                let release = values[begin..end]
                    .iter()
                    .map(|value| value.ready)
                    .max()
                    .unwrap_or_default();
                for value in &values[begin..end] {
                    batch_delay.insert(
                        value.id,
                        nanos_to_seconds(release.saturating_sub(value.ready)).max(0.0),
                    );
                }
            }
            begin = end;
        }
    }

    let busy_by_resource = busy_timelines(instances);
    let overtaking_by_instance = overtaking_delays(instances);

    let mut grouped = BTreeMap::<(String, String), WaitingCauseAttribution>::new();
    for instance in instances.iter().filter(|value| !value.resource.is_empty()) {
        let total = nanos_to_seconds(instance.start.saturating_sub(instance.ready)).max(0.0);
        let mut remaining = total;
        let batching = batch_delay
            .get(&instance.id)
            .copied()
            .unwrap_or_default()
            .min(remaining);
        remaining -= batching;
        let contention = busy_by_resource
            .get(&instance.resource)
            .map(|timeline| timeline.overlap_seconds(instance.ready, instance.start))
            .unwrap_or_default()
            .min(remaining);
        remaining -= contention;
        let overtaking = overtaking_by_instance
            .get(&instance.id)
            .copied()
            .unwrap_or_default()
            .min(remaining);
        remaining -= overtaking;
        let unavailable = request
            .resource_calendars
            .get(&instance.resource)
            .map(|available| {
                (total
                    - union_overlap_seconds(
                        instance.ready,
                        instance.start,
                        available
                            .iter()
                            .map(|interval| (interval.start_nanos, interval.end_nanos)),
                    ))
                .max(0.0)
            })
            .unwrap_or_default()
            .min(remaining);
        remaining -= unavailable;
        let entry = grouped
            .entry((instance.activity.clone(), instance.resource.clone()))
            .or_insert_with(|| WaitingCauseAttribution {
                activity: instance.activity.clone(),
                resource: instance.resource.clone(),
                ..WaitingCauseAttribution::default()
            });
        entry.support += 1;
        entry.total_wait_seconds += total;
        entry.batching_seconds += batching;
        entry.contention_seconds += contention;
        entry.prioritization_seconds += overtaking;
        entry.unavailability_seconds += unavailable;
        entry.extraneous_seconds += remaining.max(0.0);
    }
    grouped
        .into_values()
        .filter(|value| value.support >= request.minimum_support)
        .collect()
}

fn resource_pressure(
    instances: &[ActivityInstance],
    minimum_support: u64,
) -> Vec<ResourcePressure> {
    let mut grouped = BTreeMap::<(String, String), Vec<&ActivityInstance>>::new();
    for instance in instances.iter().filter(|value| !value.resource.is_empty()) {
        grouped
            .entry((instance.activity.clone(), instance.resource.clone()))
            .or_default()
            .push(instance);
    }
    grouped
        .into_iter()
        .filter_map(|((activity, resource), values)| {
            if (values.len() as u64) < minimum_support {
                return None;
            }
            let first = values.iter().map(|value| value.ready).min()?;
            let last = values.iter().map(|value| value.complete).max()?;
            let mut waits = values
                .iter()
                .map(|value| nanos_to_seconds(value.start.saturating_sub(value.ready)).max(0.0))
                .collect::<Vec<_>>();
            waits.sort_by(f64::total_cmp);
            let services = values
                .iter()
                .map(|value| nanos_to_seconds(value.complete.saturating_sub(value.start)).max(0.0))
                .collect::<Vec<_>>();
            let busy = BusyTimeline::new(
                values
                    .iter()
                    .map(|value| (value.start, value.complete))
                    .collect(),
            )
            .overlap_seconds(first, last);
            let maximum_queue = maximum_queue(&values);
            let span = last.saturating_sub(first);
            let early = first.saturating_add(span / 4);
            let late = first.saturating_add(span.saturating_mul(3) / 4);
            let queue_at = |timestamp| {
                values
                    .iter()
                    .filter(|value| value.ready <= timestamp && value.start > timestamp)
                    .count() as i64
            };
            let backlog_delta = queue_at(late) - queue_at(early);
            let midpoint = first.saturating_add(span / 2);
            let half_hours =
                (nanos_to_seconds(last.saturating_sub(midpoint)) / 3600.0).max(1.0 / 3600.0);
            let arrival = values
                .iter()
                .filter(|value| value.ready >= midpoint)
                .count() as f64
                / half_hours;
            let throughput = values
                .iter()
                .filter(|value| value.complete >= midpoint && value.complete <= last)
                .count() as f64
                / half_hours;
            Some(ResourcePressure {
                activity,
                resource,
                support: values.len() as u64,
                arrival_rate_per_hour: arrival,
                throughput_per_hour: throughput,
                utilization: (busy
                    / nanos_to_seconds(last.saturating_sub(first)).max(f64::EPSILON))
                .clamp(0.0, 1.0),
                mean_wait_seconds: mean(&waits),
                p95_wait_seconds: quantile(&waits, 0.95),
                mean_service_seconds: mean(&services),
                maximum_queue,
                backlog_delta,
                unstable: arrival > throughput * 1.01 || backlog_delta > 0,
            })
        })
        .collect()
}

fn maximum_queue(values: &[&ActivityInstance]) -> u64 {
    let mut changes = Vec::with_capacity(values.len() * 2);
    for value in values {
        changes.push((value.ready, 1_i64));
        changes.push((value.start, -1_i64));
    }
    changes.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| right.1.cmp(&left.1)));
    let mut current = 0_i64;
    let mut maximum = 0_i64;
    for (_, change) in changes {
        current += change;
        maximum = maximum.max(current);
    }
    maximum.max(0) as u64
}

fn performance_patterns(
    instances: &[ActivityInstance],
    request: &BottleneckRequest,
) -> Vec<PerformancePattern> {
    let mut grouped = BTreeMap::<(String, String), Vec<&ActivityInstance>>::new();
    for instance in instances.iter().filter(|value| !value.resource.is_empty()) {
        grouped
            .entry((instance.activity.clone(), instance.resource.clone()))
            .or_default()
            .push(instance);
    }
    grouped
        .into_iter()
        .filter_map(|((activity, resource), mut values)| {
            if (values.len() as u64) < request.minimum_support {
                return None;
            }
            values.sort_by_key(|value| value.start);
            let tolerance = seconds_to_nanos(request.batch_tolerance_seconds);
            let mut batched = 0_u64;
            let mut cluster = 1_u64;
            for pair in values.windows(2) {
                if pair[1].start.saturating_sub(pair[0].start) <= tolerance {
                    cluster += 1;
                } else {
                    if cluster >= request.minimum_batch_size {
                        batched += cluster;
                    }
                    cluster = 1;
                }
            }
            if cluster >= request.minimum_batch_size {
                batched += cluster;
            }
            let pairs = values.len().saturating_mul(values.len().saturating_sub(1)) / 2;
            let overtakes =
                inversion_count(&values.iter().map(|value| value.ready).collect::<Vec<_>>());
            let interarrival = values
                .windows(2)
                .map(|pair| nanos_to_seconds(pair[1].ready.saturating_sub(pair[0].ready)).abs())
                .collect::<Vec<_>>();
            let average = mean(&interarrival);
            let deviation = standard_deviation(&interarrival, average);
            let burstiness = if deviation + average > 0.0 {
                (deviation - average) / (deviation + average)
            } else {
                0.0
            };
            Some(PerformancePattern {
                activity,
                resource,
                support: values.len() as u64,
                batch_rate: batched as f64 / values.len() as f64,
                overtaking_rate: if pairs == 0 {
                    0.0
                } else {
                    overtakes as f64 / pairs as f64
                },
                burstiness,
            })
        })
        .collect()
}

fn blocking_cascades(instances: &[ActivityInstance], minimum_support: u64) -> Vec<BlockingCascade> {
    let mut blocker = HashMap::<usize, (usize, f64)>::new();
    for waiting in instances
        .iter()
        .filter(|value| !value.resource.is_empty() && value.start > value.ready)
    {
        if let Some((other, overlap)) = instances
            .iter()
            .filter(|other| {
                other.id != waiting.id
                    && other.resource == waiting.resource
                    && other.start < waiting.start
                    && other.complete > waiting.ready
            })
            .map(|other| {
                let overlap = nanos_to_seconds(
                    waiting
                        .start
                        .min(other.complete)
                        .saturating_sub(waiting.ready.max(other.start)),
                )
                .max(0.0);
                (other, overlap)
            })
            .max_by(|left, right| left.1.total_cmp(&right.1))
        {
            if overlap > 0.0 {
                blocker.insert(waiting.id, (other.id, overlap));
            }
        }
    }
    let by_id = instances
        .iter()
        .map(|value| (value.id, value))
        .collect::<HashMap<_, _>>();
    let mut grouped = BTreeMap::<(Vec<String>, Vec<String>), BlockingCascade>::new();
    for instance in instances {
        let mut current = instance.id;
        let mut visited = BTreeSet::new();
        let mut activities = vec![instance.activity.clone()];
        let mut resources = vec![instance.resource.clone()];
        let mut delay = 0.0;
        while let Some((next, overlap)) = blocker.get(&current) {
            if !visited.insert(current) || *next == current {
                break;
            }
            let Some(value) = by_id.get(next) else { break };
            activities.push(value.activity.clone());
            resources.push(value.resource.clone());
            delay += overlap;
            current = *next;
            if activities.len() >= 32 {
                break;
            }
        }
        if activities.len() < 2 {
            continue;
        }
        let entry = grouped
            .entry((activities.clone(), resources.clone()))
            .or_insert_with(|| BlockingCascade {
                activities,
                resources,
                ..BlockingCascade::default()
            });
        entry.support += 1;
        entry.maximum_depth = entry.maximum_depth.max(entry.activities.len() as u64 - 1);
        entry.attributed_delay_seconds += delay;
    }
    grouped
        .into_values()
        .filter(|value| value.support >= minimum_support)
        .collect()
}

fn drift_changes(
    baseline: &[BottleneckObservation],
    current: &BTreeMap<EdgeKey, Vec<f64>>,
    request: &BottleneckRequest,
) -> Vec<BottleneckChange> {
    let baseline = edge_distributions(baseline);
    let keys = baseline
        .keys()
        .chain(current.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut changes = keys
        .into_iter()
        .filter_map(|key| {
            let left = baseline.get(&key)?;
            let right = current.get(&key)?;
            if (left.len() as u64) < request.minimum_support
                || (right.len() as u64) < request.minimum_support
            {
                return None;
            }
            let statistic = ks_statistic(left, right);
            let effective = (left.len() * right.len()) as f64 / (left.len() + right.len()) as f64;
            let p_value = kolmogorov_survival(effective.sqrt() * statistic);
            let threshold = request.slow_threshold_seconds.unwrap_or_else(|| {
                let q1 = quantile(left, 0.25);
                let q3 = quantile(left, 0.75);
                q3 + 1.5 * (q3 - q1).max(0.0)
            });
            let impact = |values: &[f64]| {
                values
                    .iter()
                    .map(|value| (value - threshold).max(0.0))
                    .sum::<f64>()
            };
            Some(BottleneckChange {
                object_type: key.0,
                source_activity: key.1,
                target_activity: key.2,
                baseline_support: left.len() as u64,
                current_support: right.len() as u64,
                median_delta_seconds: quantile(right, 0.5) - quantile(left, 0.5),
                p95_delta_seconds: quantile(right, 0.95) - quantile(left, 0.95),
                impact_delta_seconds: impact(right) - impact(left),
                ks_statistic: statistic,
                p_value,
                q_value: 1.0,
            })
        })
        .collect::<Vec<_>>();
    let q_values = benjamini_hochberg(
        &changes
            .iter()
            .map(|value| value.p_value)
            .collect::<Vec<_>>(),
    );
    for (change, q_value) in changes.iter_mut().zip(q_values) {
        change.q_value = q_value;
    }
    changes.sort_by(|left, right| {
        left.q_value.total_cmp(&right.q_value).then_with(|| {
            right
                .impact_delta_seconds
                .abs()
                .total_cmp(&left.impact_delta_seconds.abs())
        })
    });
    changes
}

fn temporal_hypotheses(
    observations: &[BottleneckObservation],
    request: &BottleneckRequest,
) -> Vec<CausalHypothesisResult> {
    let mut by_object = BTreeMap::<ObjectId, Vec<&BottleneckObservation>>::new();
    for observation in observations {
        by_object
            .entry(observation.object_id)
            .or_default()
            .push(observation);
    }
    for values in by_object.values_mut() {
        values.sort_by_key(|value| value.source_timestamp_nanos);
    }
    let mut results = Vec::new();
    for hypothesis in &request.hypotheses {
        let mut exposed_effect = 0_u64;
        let mut exposed_no_effect = 0_u64;
        let mut unexposed_effect = 0_u64;
        let mut unexposed_no_effect = 0_u64;
        for values in by_object.values() {
            let cause_timestamps = values
                .iter()
                .filter(|cause| {
                    cause.source_activity == hypothesis.cause_activity
                        && hypothesis.cause_attribute.as_ref().is_none_or(|name| {
                            cause.source_attributes.get(name) == hypothesis.cause_value.as_ref()
                        })
                })
                .map(|cause| cause.source_timestamp_nanos)
                .collect::<Vec<_>>();
            for effect in values.iter().filter(|value| {
                value.target_activity == hypothesis.effect_target_activity
                    && hypothesis
                        .effect_source_activity
                        .as_ref()
                        .is_none_or(|source| &value.source_activity == source)
            }) {
                let duration = nanos_to_seconds(
                    effect
                        .target_timestamp_nanos
                        .saturating_sub(effect.source_timestamp_nanos),
                );
                let effect_present = duration > hypothesis.effect_threshold_seconds;
                let latest = effect
                    .source_timestamp_nanos
                    .saturating_sub(seconds_to_nanos(hypothesis.minimum_lag_seconds));
                let earliest = hypothesis
                    .maximum_lag_seconds
                    .map(|maximum| {
                        effect
                            .source_timestamp_nanos
                            .saturating_sub(seconds_to_nanos(maximum))
                    })
                    .unwrap_or(i128::MIN);
                let exposed = cause_timestamps.partition_point(|value| *value < earliest)
                    < cause_timestamps.partition_point(|value| *value <= latest);
                match (exposed, effect_present) {
                    (true, true) => exposed_effect += 1,
                    (true, false) => exposed_no_effect += 1,
                    (false, true) => unexposed_effect += 1,
                    (false, false) => unexposed_no_effect += 1,
                }
            }
        }
        let exposed = exposed_effect + exposed_no_effect;
        let unexposed = unexposed_effect + unexposed_no_effect;
        let p_exposed = ratio(exposed_effect, exposed);
        let p_unexposed = ratio(unexposed_effect, unexposed);
        let p_value = chi_square_p_value(
            exposed_effect,
            exposed_no_effect,
            unexposed_effect,
            unexposed_no_effect,
        );
        results.push(CausalHypothesisResult {
            id: hypothesis.id.clone(),
            exposed,
            unexposed,
            effect_given_cause: p_exposed,
            effect_without_cause: p_unexposed,
            risk_difference: p_exposed - p_unexposed,
            risk_ratio: (p_unexposed > 0.0).then_some(p_exposed / p_unexposed),
            p_value,
            q_value: 1.0,
            supported: false,
        });
    }
    let q_values = benjamini_hochberg(
        &results
            .iter()
            .map(|value| value.p_value)
            .collect::<Vec<_>>(),
    );
    for (result, q_value) in results.iter_mut().zip(q_values) {
        result.q_value = q_value;
        result.supported = q_value <= 0.05
            && result.risk_difference > 0.0
            && result.exposed >= request.minimum_support
            && result.unexposed >= request.minimum_support;
    }
    results
}

#[derive(Clone, Debug, Default)]
struct BusyTimeline {
    intervals: Vec<(i128, i128)>,
    prefix_nanos: Vec<i128>,
}

impl BusyTimeline {
    fn new(mut intervals: Vec<(i128, i128)>) -> Self {
        intervals.retain(|(start, end)| end > start);
        intervals.sort_by_key(|value| value.0);
        let mut merged: Vec<(i128, i128)> = Vec::new();
        for (start, end) in intervals {
            if let Some(last) = merged.last_mut().filter(|last| start <= last.1) {
                last.1 = last.1.max(end);
            } else {
                merged.push((start, end));
            }
        }
        let mut prefix_nanos = Vec::with_capacity(merged.len() + 1);
        prefix_nanos.push(0_i128);
        for (start, end) in &merged {
            let next = prefix_nanos
                .last()
                .copied()
                .unwrap_or_default()
                .saturating_add(end.saturating_sub(*start));
            prefix_nanos.push(next);
        }
        Self {
            intervals: merged,
            prefix_nanos,
        }
    }

    fn overlap_seconds(&self, start: i128, end: i128) -> f64 {
        if end <= start || self.intervals.is_empty() {
            return 0.0;
        }
        let first = self
            .intervals
            .partition_point(|(_, interval_end)| *interval_end <= start);
        let after_last = self
            .intervals
            .partition_point(|(interval_start, _)| *interval_start < end);
        if first >= after_last {
            return 0.0;
        }
        let total = self.prefix_nanos[after_last].saturating_sub(self.prefix_nanos[first]);
        let left_clip = start.saturating_sub(self.intervals[first].0).max(0);
        let right_clip = self.intervals[after_last - 1].1.saturating_sub(end).max(0);
        nanos_to_seconds(
            total
                .saturating_sub(left_clip)
                .saturating_sub(right_clip)
                .max(0),
        )
    }
}

fn busy_timelines(instances: &[ActivityInstance]) -> BTreeMap<String, BusyTimeline> {
    let mut intervals = BTreeMap::<String, Vec<(i128, i128)>>::new();
    for instance in instances.iter().filter(|value| !value.resource.is_empty()) {
        intervals
            .entry(instance.resource.clone())
            .or_default()
            .push((instance.start, instance.complete));
    }
    intervals
        .into_iter()
        .map(|(resource, values)| (resource, BusyTimeline::new(values)))
        .collect()
}

fn overtaking_delays(instances: &[ActivityInstance]) -> HashMap<usize, f64> {
    let mut grouped = BTreeMap::<String, Vec<&ActivityInstance>>::new();
    for instance in instances.iter().filter(|value| !value.resource.is_empty()) {
        grouped
            .entry(instance.resource.clone())
            .or_default()
            .push(instance);
    }
    let mut output = HashMap::new();
    for values in grouped.values_mut() {
        values.sort_by_key(|value| (value.start, value.id));
        let mut readies = values.iter().map(|value| value.ready).collect::<Vec<_>>();
        readies.sort_unstable();
        readies.dedup();
        let mut minimum_starts = FenwickMinimum::new(readies.len());
        let mut begin = 0;
        while begin < values.len() {
            let mut end = begin + 1;
            while end < values.len() && values[end].start == values[begin].start {
                end += 1;
            }
            for instance in &values[begin..end] {
                let upper = readies.partition_point(|ready| *ready <= instance.ready);
                let later_count = readies.len().saturating_sub(upper);
                if let Some(earliest_start) = minimum_starts.minimum(later_count) {
                    output.insert(
                        instance.id,
                        nanos_to_seconds(instance.start.saturating_sub(earliest_start)).max(0.0),
                    );
                }
            }
            for instance in &values[begin..end] {
                let position = readies.partition_point(|ready| *ready < instance.ready);
                minimum_starts.insert(readies.len() - position, instance.start);
            }
            begin = end;
        }
    }
    output
}

struct FenwickMinimum {
    values: Vec<i128>,
}

impl FenwickMinimum {
    fn new(size: usize) -> Self {
        Self {
            values: vec![i128::MAX; size + 1],
        }
    }

    fn insert(&mut self, mut index: usize, value: i128) {
        while index < self.values.len() {
            self.values[index] = self.values[index].min(value);
            index += index & index.wrapping_neg();
        }
    }

    fn minimum(&self, mut index: usize) -> Option<i128> {
        let mut value = i128::MAX;
        while index > 0 {
            value = value.min(self.values[index]);
            index &= index - 1;
        }
        (value != i128::MAX).then_some(value)
    }
}

fn inversion_count(values: &[i128]) -> u64 {
    let mut ordered = values.to_vec();
    ordered.sort_unstable();
    ordered.dedup();
    let mut tree = vec![0_u64; ordered.len() + 1];
    let mut count = 0_u64;
    for (seen, value) in values.iter().enumerate() {
        let position = ordered.partition_point(|candidate| candidate < value) + 1;
        let mut index = position;
        let mut not_greater = 0_u64;
        while index > 0 {
            not_greater = not_greater.saturating_add(tree[index]);
            index &= index - 1;
        }
        count = count.saturating_add(seen as u64 - not_greater);
        let mut index = position;
        while index < tree.len() {
            tree[index] = tree[index].saturating_add(1);
            index += index & index.wrapping_neg();
        }
    }
    count
}

fn union_overlap_seconds(
    start: i128,
    end: i128,
    intervals: impl Iterator<Item = (i128, i128)>,
) -> f64 {
    let mut intervals = intervals
        .map(|(left, right)| (left.max(start), right.min(end)))
        .filter(|(left, right)| right > left)
        .collect::<Vec<_>>();
    intervals.sort_by_key(|value| value.0);
    let mut total = 0_i128;
    let mut current: Option<(i128, i128)> = None;
    for interval in intervals {
        match current {
            Some((left, right)) if interval.0 <= right => {
                current = Some((left, right.max(interval.1)))
            }
            Some((left, right)) => {
                total = total.saturating_add(right.saturating_sub(left));
                current = Some(interval);
            }
            None => current = Some(interval),
        }
    }
    if let Some((left, right)) = current {
        total = total.saturating_add(right.saturating_sub(left));
    }
    nanos_to_seconds(total)
}

fn ks_statistic(left: &[f64], right: &[f64]) -> f64 {
    let mut i = 0;
    let mut j = 0;
    let mut maximum: f64 = 0.0;
    while i < left.len() || j < right.len() {
        let next = match (left.get(i), right.get(j)) {
            (Some(left), Some(right)) => {
                if left <= right {
                    *left
                } else {
                    *right
                }
            }
            (Some(left), None) => *left,
            (None, Some(right)) => *right,
            (None, None) => break,
        };
        while i < left.len() && left[i] <= next {
            i += 1;
        }
        while j < right.len() && right[j] <= next {
            j += 1;
        }
        maximum = maximum.max((i as f64 / left.len() as f64 - j as f64 / right.len() as f64).abs());
    }
    maximum
}

fn benjamini_hochberg(values: &[f64]) -> Vec<f64> {
    let mut ranked = values.iter().copied().enumerate().collect::<Vec<_>>();
    ranked.sort_by(|left, right| left.1.total_cmp(&right.1));
    let mut output = vec![1.0; values.len()];
    let mut previous: f64 = 1.0;
    for (rank, (index, value)) in ranked.iter().enumerate().rev() {
        let adjusted = (*value * values.len() as f64 / (rank + 1) as f64)
            .min(previous)
            .min(1.0);
        output[*index] = adjusted;
        previous = adjusted;
    }
    output
}

fn chi_square_p_value(a: u64, b: u64, c: u64, d: u64) -> f64 {
    let n = (a + b + c + d) as f64;
    let denominator = ((a + b) * (c + d) * (a + c) * (b + d)) as f64;
    if n == 0.0 || denominator == 0.0 {
        return 1.0;
    }
    let cross = (a as f64 * d as f64 - b as f64 * c as f64).abs();
    let chi_square = n * cross * cross / denominator;
    normal_two_sided_survival(chi_square.sqrt())
}

fn normal_two_sided_survival(z: f64) -> f64 {
    let z = z.abs();
    if z == 0.0 {
        return 1.0;
    }
    if z >= 9.0 {
        return 0.0;
    }
    // Deterministic Simpson integration of the standard-normal density. The
    // hypothesis count is small, so numerical accuracy is preferred over a
    // coefficient approximation hidden behind a dependency.
    let steps = 512_usize;
    let width = z / steps as f64;
    let density = |value: f64| (-0.5 * value * value).exp() / (2.0 * std::f64::consts::PI).sqrt();
    let mut integral = density(0.0) + density(z);
    for index in 1..steps {
        integral += if index % 2 == 0 { 2.0 } else { 4.0 } * density(index as f64 * width);
    }
    let upper_tail = (0.5 - integral * width / 3.0).max(0.0);
    (2.0 * upper_tail).clamp(0.0, 1.0)
}

fn kolmogorov_survival(lambda: f64) -> f64 {
    if lambda <= 0.0 {
        return 1.0;
    }
    let mut sum = 0.0;
    for order in 1..=100 {
        let term = (-2.0 * (order as f64).powi(2) * lambda * lambda).exp();
        sum += if order % 2 == 1 { term } else { -term };
        if term < 1e-14 {
            break;
        }
    }
    (2.0 * sum).clamp(0.0, 1.0)
}

fn wilson_interval(successes: u64, total: u64) -> (f64, f64) {
    if total == 0 {
        return (0.0, 0.0);
    }
    let z = 1.959_963_984_540_054;
    let n = total as f64;
    let p = successes as f64 / n;
    let denominator = 1.0 + z * z / n;
    let center = (p + z * z / (2.0 * n)) / denominator;
    let margin = z * ((p * (1.0 - p) / n + z * z / (4.0 * n * n)).sqrt()) / denominator;
    ((center - margin).max(0.0), (center + margin).min(1.0))
}

fn lower_bound(values: &[f64], needle: f64) -> usize {
    values.partition_point(|value| value.total_cmp(&needle) == Ordering::Less)
}
fn quantile(values: &[f64], probability: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let position = (values.len() - 1) as f64 * probability.clamp(0.0, 1.0);
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    if lower == upper {
        values[lower]
    } else {
        values[lower] + (values[upper] - values[lower]) * (position - lower as f64)
    }
}
fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}
fn standard_deviation(values: &[f64], average: f64) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    (values
        .iter()
        .map(|value| (value - average).powi(2))
        .sum::<f64>()
        / values.len() as f64)
        .sqrt()
}
fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}
fn nanos_to_seconds(value: i128) -> f64 {
    value as f64 / 1_000_000_000.0
}
fn seconds_to_nanos(value: f64) -> i128 {
    (value * 1_000_000_000.0).round() as i128
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observation(
        object_id: u64,
        object_type: &str,
        event: u64,
        source: &str,
        target: &str,
        start: i128,
        end: i128,
    ) -> BottleneckObservation {
        BottleneckObservation {
            object_id,
            object_type: object_type.to_owned(),
            source_event_id: event,
            source_activity: source.to_owned(),
            source_timestamp_nanos: start,
            source_lifecycle: None,
            source_attributes: BTreeMap::new(),
            target_event_id: event + 1,
            target_activity: target.to_owned(),
            target_timestamp_nanos: end,
            target_lifecycle: None,
            target_attributes: BTreeMap::new(),
        }
    }

    fn lifecycle_observations(object_id: u64, offset_seconds: i128) -> Vec<BottleneckObservation> {
        let resource = BTreeMap::from([(
            "org:resource".to_owned(),
            AttributeValue::String("r1".to_owned()),
        )]);
        let ready = offset_seconds * 1_000_000_000;
        let start = ready + 10_000_000_000;
        let complete = start + 20_000_000_000;
        vec![
            BottleneckObservation {
                object_id,
                object_type: "order".to_owned(),
                source_event_id: object_id * 10,
                source_activity: "ready".to_owned(),
                source_timestamp_nanos: ready,
                source_lifecycle: None,
                source_attributes: BTreeMap::new(),
                target_event_id: object_id * 10 + 1,
                target_activity: "work".to_owned(),
                target_timestamp_nanos: start,
                target_lifecycle: Some("start".to_owned()),
                target_attributes: resource.clone(),
            },
            BottleneckObservation {
                object_id,
                object_type: "order".to_owned(),
                source_event_id: object_id * 10 + 1,
                source_activity: "work".to_owned(),
                source_timestamp_nanos: start,
                source_lifecycle: Some("start".to_owned()),
                source_attributes: resource.clone(),
                target_event_id: object_id * 10 + 2,
                target_activity: "work".to_owned(),
                target_timestamp_nanos: complete,
                target_lifecycle: Some("complete".to_owned()),
                target_attributes: resource,
            },
        ]
    }

    #[test]
    fn ranks_by_population_impact_not_only_extreme_latency() {
        let mut values = Vec::new();
        for id in 0..10 {
            values.push(observation(
                id,
                "order",
                id * 10,
                "a",
                "b",
                0,
                20_000_000_000,
            ));
        }
        for id in 20..25 {
            values.push(observation(
                id,
                "order",
                id * 10,
                "x",
                "y",
                0,
                if id == 24 {
                    1_000_000_000_000
                } else {
                    1_000_000_000
                },
            ));
        }
        let request = BottleneckRequest {
            minimum_support: 5,
            slow_threshold_seconds: Some(5.0),
            ..BottleneckRequest::default()
        };
        let result = analyze_observations("test", &values, None, &request).unwrap();
        assert_eq!(result.signals[0].source_activity, "x");
        assert_eq!(result.diagnostics.provider, "test");
    }

    #[test]
    fn synchronization_attributes_lag_to_object_type() {
        let values = vec![
            observation(1, "order", 1, "ready", "ship", 0, 10_000_000_000),
            observation(2, "item", 2, "ready", "ship", 5_000_000_000, 10_000_000_000),
        ];
        let mut values = values;
        values[1].target_event_id = values[0].target_event_id;
        let result = synchronization(&values, 1);
        assert_eq!(result.len(), 2);
        assert!(
            result.iter().any(
                |value| value.object_type == "order" && value.mean_readiness_lag_seconds == 5.0
            )
        );
    }

    #[test]
    fn multiple_testing_is_monotone_and_bounded() {
        let adjusted = benjamini_hochberg(&[0.01, 0.03, 0.2]);
        assert!(adjusted.iter().all(|value| (0.0..=1.0).contains(value)));
        assert!(adjusted[0] <= adjusted[1] && adjusted[1] <= adjusted[2]);
    }

    #[test]
    fn spectrum_overtaking_count_uses_fifo_inversions() {
        assert_eq!(inversion_count(&[1, 3, 2, 0]), 4);
        assert_eq!(inversion_count(&[1, 1, 2]), 0);
    }

    #[test]
    fn busy_timeline_merges_overlaps_and_queries_subranges() {
        let timeline = BusyTimeline::new(vec![(0, 10), (5, 20), (30, 40)]);
        assert_eq!(timeline.overlap_seconds(8, 35), 17.0 / 1_000_000_000.0);
    }

    #[test]
    fn waiting_causes_are_non_overlapping_and_resource_metrics_are_available() {
        let mut values = Vec::new();
        values.extend(lifecycle_observations(1, 0));
        values.extend(lifecycle_observations(2, 5));
        let request = BottleneckRequest {
            minimum_support: 1,
            minimum_batch_size: 2,
            ..BottleneckRequest::default()
        };
        let result = analyze_observations("test", &values, None, &request).unwrap();
        assert_eq!(result.diagnostics.lifecycle_instance_count, 2);
        assert_eq!(result.resource_pressure.len(), 1);
        let causes = &result.waiting_causes[0];
        let attributed = causes.batching_seconds
            + causes.prioritization_seconds
            + causes.contention_seconds
            + causes.unavailability_seconds
            + causes.extraneous_seconds;
        assert!((attributed - causes.total_wait_seconds).abs() < 1e-9);
    }
}
