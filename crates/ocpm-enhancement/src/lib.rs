//! Process enhancement: performance, rework, organizational analysis, and drift.
//!
//! PROVENANCE: object-centric performance semantics follow
//! doi:10.1007/s10009-022-00668-w. Drift characterization follows
//! doi:10.1016/j.is.2025.102584 and the Jensen-Shannon measure follows
//! doi:10.1109/18.61115. No source code from another process-mining library
//! was consulted.

use ocpm_core::{
    AttributeValue, EnhancementKind, EnhancementRequest, EnhancementResult, OcpmError,
    OcpmResult, PerformanceMetric,
};
use ocpm_provider::{ExecutionMode, OcpmProvider, ProcessExecution};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub const PROVENANCE: &[&str] = &[
    "doi:10.1007/s10009-022-00668-w",
    "doi:10.1016/j.is.2025.102584",
    "doi:10.1109/18.61115",
];

pub fn enhance(
    provider: &dyn OcpmProvider,
    request: &EnhancementRequest,
) -> OcpmResult<EnhancementResult> {
    if request.semantic_version != "1.0" {
        return Err(OcpmError::invalid_request("semantic_version must be 1.0"));
    }
    let mode = match request.parameters.get("execution_mode").and_then(|v| v.as_str()) {
        Some("connected_component") => ExecutionMode::ConnectedComponent,
        _ => ExecutionMode::LeadingObject,
    };
    let leading = request
        .parameters
        .get("leading_object_type")
        .and_then(|value| value.as_str());
    let executions = provider.process_executions(&request.view, mode, leading)?;
    match request.kind {
        EnhancementKind::ProcessMap => process_map(&executions),
        EnhancementKind::Timeline => timeline(&executions),
        EnhancementKind::Histogram => histogram(&executions),
        EnhancementKind::Performance => performance(&executions),
        EnhancementKind::Rework => rework(&executions),
        EnhancementKind::Organizational => organizational(&executions, request),
        EnhancementKind::WindowComparison | EnhancementKind::Drift => {
            let comparison = request.comparison_view.as_ref().ok_or_else(|| {
                OcpmError::invalid_request("comparison_view is required for drift analysis")
            })?;
            let other = provider.process_executions(comparison, mode, leading)?;
            drift(&executions, &other)
        }
    }
}

fn process_map(executions: &[ProcessExecution]) -> OcpmResult<EnhancementResult> {
    let mut frequencies = BTreeMap::<(String, String), u64>::new();
    let mut durations = BTreeMap::<(String, String), Vec<f64>>::new();
    for execution in executions {
        for pair in execution.events.windows(2) {
            let key = (pair[0].activity.clone(), pair[1].activity.clone());
            *frequencies.entry(key.clone()).or_default() += 1;
            durations.entry(key).or_default().push(duration_seconds(
                pair[0].timestamp.epoch_nanos_utc,
                pair[1].timestamp.epoch_nanos_utc,
            )?);
        }
    }
    let edges = frequencies
        .into_iter()
        .map(|((source, target), frequency)| {
            let values = &durations[&(source.clone(), target.clone())];
            serde_json::json!({
                "source": source,
                "target": target,
                "frequency": frequency,
                "duration": metric("edge_duration", "seconds", values),
            })
        })
        .collect::<Vec<_>>();
    Ok(EnhancementResult {
        metrics: vec![PerformanceMetric {
            name: "process_map_edges".to_owned(),
            unit: "edges".to_owned(),
            support: edges.len() as u64,
            ..PerformanceMetric::default()
        }],
        groups: BTreeMap::from([("edges".to_owned(), serde_json::json!(edges))]),
        warnings: Vec::new(),
    })
}

fn timeline(executions: &[ProcessExecution]) -> OcpmResult<EnhancementResult> {
    let rows = executions
        .iter()
        .flat_map(|execution| {
            execution.events.iter().map(|event| {
                serde_json::json!({
                    "execution_id": execution.id,
                    "event_id": event.id,
                    "activity": event.activity,
                    "timestamp": event.timestamp,
                    "object_ids": execution.event_object_ids.get(&event.id),
                })
            })
        })
        .collect::<Vec<_>>();
    Ok(EnhancementResult {
        metrics: vec![PerformanceMetric {
            name: "timeline_events".to_owned(),
            unit: "events".to_owned(),
            support: rows.len() as u64,
            ..PerformanceMetric::default()
        }],
        groups: BTreeMap::from([("dotted_chart".to_owned(), serde_json::json!(rows))]),
        warnings: Vec::new(),
    })
}

fn histogram(executions: &[ProcessExecution]) -> OcpmResult<EnhancementResult> {
    let values = cycle_times(executions)?;
    if values.is_empty() {
        return Ok(EnhancementResult {
            metrics: vec![metric("execution_cycle_time", "seconds", &[])],
            warnings: vec!["histogram requires nonempty executions".to_owned()],
            ..EnhancementResult::default()
        });
    }
    let minimum = values.iter().copied().min_by(f64::total_cmp).unwrap_or_default();
    let maximum = values.iter().copied().max_by(f64::total_cmp).unwrap_or(minimum);
    let width = ((maximum - minimum) / 10.0).max(f64::EPSILON);
    let mut bins = vec![0_u64; 10];
    for value in &values {
        let index = (((value - minimum) / width) as usize).min(9);
        bins[index] += 1;
    }
    let bins = bins
        .into_iter()
        .enumerate()
        .map(|(index, count)| {
            serde_json::json!({
                "lower_seconds": minimum + index as f64 * width,
                "upper_seconds": minimum + (index + 1) as f64 * width,
                "count": count,
            })
        })
        .collect::<Vec<_>>();
    Ok(EnhancementResult {
        metrics: vec![metric("execution_cycle_time", "seconds", &values)],
        groups: BTreeMap::from([("histogram".to_owned(), serde_json::json!(bins))]),
        warnings: Vec::new(),
    })
}

fn performance(executions: &[ProcessExecution]) -> OcpmResult<EnhancementResult> {
    let mut all = Vec::new();
    let mut edge_values = BTreeMap::<(String, String), Vec<f64>>::new();
    let mut cycle_values = Vec::new();
    let mut service_values = Vec::new();
    let mut waiting_values = Vec::new();
    let mut sojourn_values = Vec::new();
    let mut synchronization_values = Vec::new();
    let mut pooling_values = Vec::new();
    let mut lagging_values = Vec::new();
    let mut flow_values = Vec::new();
    for execution in executions {
        if let (Some(first), Some(last)) = (execution.events.first(), execution.events.last()) {
            cycle_values.push(duration_seconds(first.timestamp.epoch_nanos_utc, last.timestamp.epoch_nanos_utc)?);
        }
        for pair in execution.events.windows(2) {
            let duration = duration_seconds(
                pair[0].timestamp.epoch_nanos_utc,
                pair[1].timestamp.epoch_nanos_utc,
            )?;
            all.push(duration);
            edge_values
                .entry((pair[0].activity.clone(), pair[1].activity.clone()))
                .or_default()
                .push(duration);
        }
        let mut starts = BTreeMap::<String, VecDeque<i128>>::new();
        let mut previous_completion = None;
        let mut object_ready = BTreeMap::<u64, i128>::new();
        for event in &execution.events {
            let involved = execution
                .event_object_ids
                .get(&event.id)
                .cloned()
                .unwrap_or_else(|| execution.object_ids.clone());
            let readiness = involved
                .iter()
                .filter_map(|object_id| object_ready.get(object_id).copied())
                .collect::<Vec<_>>();
            if let (Some(minimum), Some(maximum)) =
                (readiness.iter().min().copied(), readiness.iter().max().copied())
            {
                if readiness.len() > 1 {
                    synchronization_values.push(duration_seconds(minimum, maximum)?);
                }
                pooling_values.push(duration_seconds(maximum, event.timestamp.epoch_nanos_utc)?);
                lagging_values.push(duration_seconds(minimum, event.timestamp.epoch_nanos_utc)?);
                for ready in readiness {
                    flow_values.push(duration_seconds(ready, event.timestamp.epoch_nanos_utc)?);
                }
            }
            for object_id in involved {
                object_ready.insert(object_id, event.timestamp.epoch_nanos_utc);
            }
            match event.lifecycle.as_deref().map(str::to_ascii_lowercase).as_deref() {
                Some("start") => {
                    starts
                        .entry(event.activity.clone())
                        .or_default()
                        .push_back(event.timestamp.epoch_nanos_utc);
                }
                Some("complete") => {
                    if let Some(start) = starts
                        .get_mut(&event.activity)
                        .and_then(VecDeque::pop_front)
                    {
                        service_values.push(duration_seconds(
                            start,
                            event.timestamp.epoch_nanos_utc,
                        )?);
                        if let Some(previous) = previous_completion {
                            waiting_values.push(duration_seconds(previous, start)?);
                            sojourn_values.push(duration_seconds(
                                previous,
                                event.timestamp.epoch_nanos_utc,
                            )?);
                        }
                    }
                    previous_completion = Some(event.timestamp.epoch_nanos_utc);
                }
                _ => {}
            }
        }
    }
    let mut groups = BTreeMap::new();
    let mut bottlenecks = Vec::new();
    for ((source, target), values) in edge_values {
        let edge_metric = metric("edge_duration", "seconds", &values);
        bottlenecks.push(serde_json::json!({
            "source": &source,
            "target": &target,
            "support": edge_metric.support,
            "mean_seconds": edge_metric.mean,
        }));
        groups.insert(
            format!("edge:{source}\u{1f}{target}"),
            serde_json::to_value(edge_metric).expect("metric is serializable"),
        );
    }
    bottlenecks.sort_by(|left, right| {
        right["mean_seconds"]
            .as_f64()
            .unwrap_or_default()
            .total_cmp(&left["mean_seconds"].as_f64().unwrap_or_default())
            .then_with(|| left["source"].as_str().cmp(&right["source"].as_str()))
            .then_with(|| left["target"].as_str().cmp(&right["target"].as_str()))
    });
    groups.insert("bottleneck_edges".to_owned(), serde_json::json!(bottlenecks));
    let mut warnings = Vec::new();
    if service_values.is_empty() {
        warnings.push(
            "service, waiting, and sojourn time require start/complete lifecycle labels"
                .to_owned(),
        );
    }
    Ok(EnhancementResult {
        metrics: vec![
            metric("directly_follows_duration", "seconds", &all),
            metric("execution_cycle_time", "seconds", &cycle_values),
            metric("service_time", "seconds", &service_values),
            metric("waiting_time", "seconds", &waiting_values),
            metric("sojourn_time", "seconds", &sojourn_values),
            metric("synchronization_time", "seconds", &synchronization_values),
            metric("pooling_time", "seconds", &pooling_values),
            metric("lagging_time", "seconds", &lagging_values),
            metric("flow_time", "seconds", &flow_values),
        ],
        groups,
        warnings,
    })
}

fn rework(executions: &[ProcessExecution]) -> OcpmResult<EnhancementResult> {
    let mut repeated_per_execution = Vec::new();
    let mut by_activity = BTreeMap::<String, u64>::new();
    for execution in executions {
        let mut counts = BTreeMap::<&str, u64>::new();
        for event in &execution.events {
            *counts.entry(&event.activity).or_default() += 1;
        }
        let repeated = counts.values().map(|count| count.saturating_sub(1)).sum::<u64>();
        repeated_per_execution.push(repeated as f64);
        for (activity, count) in counts {
            *by_activity.entry(activity.to_owned()).or_default() += count.saturating_sub(1);
        }
    }
    Ok(EnhancementResult {
        metrics: vec![metric(
            "repeated_activity_occurrences",
            "events_per_execution",
            &repeated_per_execution,
        )],
        groups: by_activity
            .into_iter()
            .map(|(activity, count)| (activity, serde_json::json!(count)))
            .collect(),
        warnings: Vec::new(),
    })
}

fn organizational(
    executions: &[ProcessExecution],
    request: &EnhancementRequest,
) -> OcpmResult<EnhancementResult> {
    let attribute = request
        .parameters
        .get("resource_attribute")
        .and_then(|value| value.as_str())
        .unwrap_or("org:resource");
    let mut resources = BTreeMap::<String, u64>::new();
    let mut resource_activities = BTreeMap::<(String, String), u64>::new();
    let mut resource_executions = BTreeMap::<String, BTreeSet<String>>::new();
    let mut handovers = BTreeMap::<(String, String), u64>::new();
    let mut missing = 0_u64;
    for execution in executions {
        let path = execution
            .events
            .iter()
            .filter_map(|event| match event.attributes.get(attribute) {
                Some(AttributeValue::String(resource)) => {
                    *resources.entry(resource.clone()).or_default() += 1;
                    *resource_activities
                        .entry((resource.clone(), event.activity.clone()))
                        .or_default() += 1;
                    resource_executions
                        .entry(resource.clone())
                        .or_default()
                        .insert(execution.id.clone());
                    Some(resource.clone())
                }
                _ => {
                    missing += 1;
                    None
                }
            })
            .collect::<Vec<_>>();
        for pair in path.windows(2) {
            *handovers.entry((pair[0].clone(), pair[1].clone())).or_default() += 1;
        }
    }
    let workload = resources.values().map(|value| *value as f64).collect::<Vec<_>>();
    let mut groups = BTreeMap::new();
    groups.insert("resources".to_owned(), serde_json::json!(resources));
    groups.insert(
        "handovers".to_owned(),
        serde_json::json!(
            handovers
                .into_iter()
                .map(|((source, target), frequency)| serde_json::json!({
                    "source": source,
                    "target": target,
                    "frequency": frequency,
                }))
                .collect::<Vec<_>>()
        ),
    );
    groups.insert(
        "resource_activities".to_owned(),
        serde_json::json!(
            resource_activities
                .into_iter()
                .map(|((resource, activity), frequency)| serde_json::json!({
                    "resource": resource,
                    "activity": activity,
                    "frequency": frequency,
                }))
                .collect::<Vec<_>>()
        ),
    );
    groups.insert(
        "resource_executions".to_owned(),
        serde_json::json!(resource_executions),
    );
    Ok(EnhancementResult {
        metrics: vec![metric("resource_workload", "events", &workload)],
        groups,
        warnings: (missing > 0)
            .then(|| format!("{missing} events lacked string attribute {attribute}"))
            .into_iter()
            .collect(),
    })
}

fn drift(
    baseline: &[ProcessExecution],
    comparison: &[ProcessExecution],
) -> OcpmResult<EnhancementResult> {
    let (activity_divergence, activity_contributions, activity_support) =
        distribution_drift(&activity_counts(baseline), &activity_counts(comparison));
    let (dfg_divergence, dfg_contributions, dfg_support) =
        distribution_drift(&dfg_counts(baseline), &dfg_counts(comparison));
    let (variant_divergence, variant_contributions, variant_support) =
        distribution_drift(&variant_counts(baseline), &variant_counts(comparison));
    let baseline_cycles = cycle_times(baseline)?;
    let comparison_cycles = cycle_times(comparison)?;
    let baseline_mean = mean(&baseline_cycles);
    let comparison_mean = mean(&comparison_cycles);
    let performance_delta = comparison_mean.zip(baseline_mean).map(|(right, left)| right - left);
    let mut groups = BTreeMap::new();
    groups.insert("activity".to_owned(), serde_json::json!(activity_contributions));
    groups.insert("dfg".to_owned(), serde_json::json!(dfg_contributions));
    groups.insert("variant".to_owned(), serde_json::json!(variant_contributions));
    groups.insert(
        "performance".to_owned(),
        serde_json::json!({
            "baseline_mean_cycle_seconds": baseline_mean,
            "comparison_mean_cycle_seconds": comparison_mean,
            "mean_cycle_delta_seconds": performance_delta,
        }),
    );
    Ok(EnhancementResult {
        metrics: vec![
            scalar_metric("activity_jensen_shannon_divergence", "bits", activity_support, activity_divergence),
            scalar_metric("dfg_jensen_shannon_divergence", "bits", dfg_support, dfg_divergence),
            scalar_metric("variant_jensen_shannon_divergence", "bits", variant_support, variant_divergence),
            PerformanceMetric {
                name: "mean_cycle_time_delta".to_owned(),
                unit: "seconds".to_owned(),
                support: (baseline_cycles.len() + comparison_cycles.len()) as u64,
                mean: performance_delta,
                minimum: None,
                maximum: None,
                median: None,
            },
        ],
        groups,
        warnings: Vec::new(),
    })
}

fn dfg_counts(executions: &[ProcessExecution]) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for pair in executions
        .iter()
        .flat_map(|execution| execution.events.windows(2))
    {
        *counts
            .entry(format!("{}\u{1f}{}", pair[0].activity, pair[1].activity))
            .or_default() += 1;
    }
    counts
}

fn variant_counts(executions: &[ProcessExecution]) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for execution in executions {
        let key = serde_json::to_string(&execution.activity_path())
            .expect("activity paths are serializable");
        *counts.entry(key).or_default() += 1;
    }
    counts
}

fn distribution_drift(
    baseline: &BTreeMap<String, u64>,
    comparison: &BTreeMap<String, u64>,
) -> (f64, BTreeMap<String, serde_json::Value>, u64) {
    let labels = baseline
        .keys()
        .chain(comparison.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let baseline_total = baseline.values().sum::<u64>();
    let comparison_total = comparison.values().sum::<u64>();
    let mut divergence = 0.0;
    let mut contributions = BTreeMap::new();
    for label in labels {
        let left = ratio(*baseline.get(&label).unwrap_or(&0), baseline_total);
        let right = ratio(*comparison.get(&label).unwrap_or(&0), comparison_total);
        let midpoint = (left + right) / 2.0;
        let contribution = 0.5 * kl_term(left, midpoint) + 0.5 * kl_term(right, midpoint);
        divergence += contribution;
        contributions.insert(
            label,
            serde_json::json!({
                "baseline_share": left,
                "comparison_share": right,
                "share_delta": right - left,
                "js_contribution": contribution,
            }),
        );
    }
    (divergence, contributions, baseline_total + comparison_total)
}

fn cycle_times(executions: &[ProcessExecution]) -> OcpmResult<Vec<f64>> {
    executions
        .iter()
        .filter_map(|execution| execution.events.first().zip(execution.events.last()))
        .map(|(first, last)| {
            duration_seconds(first.timestamp.epoch_nanos_utc, last.timestamp.epoch_nanos_utc)
        })
        .collect()
}

fn mean(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

fn scalar_metric(name: &str, unit: &str, support: u64, value: f64) -> PerformanceMetric {
    PerformanceMetric {
        name: name.to_owned(),
        unit: unit.to_owned(),
        support,
        mean: Some(value),
        minimum: None,
        maximum: None,
        median: None,
    }
}

fn activity_counts(executions: &[ProcessExecution]) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for event in executions.iter().flat_map(|execution| &execution.events) {
        *counts.entry(event.activity.clone()).or_default() += 1;
    }
    counts
}

fn duration_seconds(start: i128, end: i128) -> OcpmResult<f64> {
    if end < start {
        return Err(OcpmError::invalid_data(
            "canonical event ordering produced a negative duration",
        ));
    }
    Ok((end - start) as f64 / 1_000_000_000.0)
}

fn metric(name: &str, unit: &str, values: &[f64]) -> PerformanceMetric {
    if values.is_empty() {
        return PerformanceMetric {
            name: name.to_owned(),
            unit: unit.to_owned(),
            ..PerformanceMetric::default()
        };
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let median = if sorted.len() % 2 == 0 {
        (sorted[sorted.len() / 2 - 1] + sorted[sorted.len() / 2]) / 2.0
    } else {
        sorted[sorted.len() / 2]
    };
    PerformanceMetric {
        name: name.to_owned(),
        unit: unit.to_owned(),
        support: sorted.len() as u64,
        mean: Some(sorted.iter().sum::<f64>() / sorted.len() as f64),
        minimum: sorted.first().copied(),
        maximum: sorted.last().copied(),
        median: Some(median),
    }
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn kl_term(value: f64, midpoint: f64) -> f64 {
    if value == 0.0 || midpoint == 0.0 {
        0.0
    } else {
        value * (value / midpoint).log2()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_median_is_deterministic() {
        let value = metric("duration", "seconds", &[4.0, 1.0, 2.0, 3.0]);
        assert_eq!(value.median, Some(2.5));
    }
}
