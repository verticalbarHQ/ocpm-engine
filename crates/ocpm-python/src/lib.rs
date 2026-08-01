use ocpm_core::{
    TransitionKey,
    binding::BindingCapsule,
    dfg_frequency_conformance,
    event_batch::{EventBatch, EventLogSummary, EventSummaryBuilder},
    frequency_drift as score_frequency_drift, next_activity_prediction, rank_bottlenecks,
    variant_frequency_conformance,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

type TransitionTuple = (String, String, String);
type DfgConformanceOutput = (f64, u64, u64, u64, Vec<TransitionTuple>);
type NextActivityOutput = (f64, u64, u64, Vec<TransitionTuple>);
type DriftContributorOutput = (String, f64, f64, f64, f64);
type FrequencyDriftOutput = (f64, u64, u64, Vec<DriftContributorOutput>);
type BindingRowOutput = (Vec<i64>, Option<String>, Option<bool>, Option<f64>);
type EventBatchInput = (Vec<String>, i32, i32, Vec<u8>, Vec<u8>);
type WindowedEventBatchInput = (i32, Vec<String>, i32, i32, Vec<u8>, Vec<u8>);
type EventVariantOutput = (Vec<String>, u64);
type EventDfgOutput = (String, String, u64, f64);
type EventActivityOutput = (String, u64, u64, u64, u64);
type BindingPairGroupOutput = (i64, Vec<i64>, Vec<i64>);
type EventSummaryOutput = (
    u64,
    u64,
    u64,
    Vec<EventVariantOutput>,
    Vec<EventDfgOutput>,
    Vec<EventActivityOutput>,
);

fn transitions(
    sources: Vec<String>,
    targets: Vec<String>,
    edge_types: Vec<String>,
    train_counts: Vec<u64>,
    test_counts: Vec<u64>,
) -> PyResult<Vec<(TransitionKey, u64, u64)>> {
    let length = sources.len();
    if targets.len() != length
        || edge_types.len() != length
        || train_counts.len() != length
        || test_counts.len() != length
    {
        return Err(PyValueError::new_err(
            "all input columns must have the same length",
        ));
    }
    Ok(sources
        .into_iter()
        .zip(targets)
        .zip(edge_types)
        .zip(train_counts)
        .zip(test_counts)
        .map(|((((source, target), edge_type), train), test)| {
            (
                TransitionKey {
                    source,
                    target,
                    edge_type,
                },
                train,
                test,
            )
        })
        .collect())
}

#[pyfunction]
#[pyo3(signature = (sources, targets, edge_types, train_counts, test_counts, coverage=0.95))]
fn dfg_conformance(
    py: Python<'_>,
    sources: Vec<String>,
    targets: Vec<String>,
    edge_types: Vec<String>,
    train_counts: Vec<u64>,
    test_counts: Vec<u64>,
    coverage: f64,
) -> PyResult<DfgConformanceOutput> {
    let rows = transitions(sources, targets, edge_types, train_counts, test_counts)?;
    let result = py
        .detach(move || dfg_frequency_conformance(rows, coverage))
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    Ok((
        result.fitness,
        result.conforming,
        result.deviations,
        result.test_total,
        result
            .model
            .into_iter()
            .map(|key| (key.source, key.target, key.edge_type))
            .collect(),
    ))
}

#[pyfunction]
fn next_activity(
    py: Python<'_>,
    sources: Vec<String>,
    targets: Vec<String>,
    edge_types: Vec<String>,
    train_counts: Vec<u64>,
    test_counts: Vec<u64>,
) -> PyResult<NextActivityOutput> {
    let rows = transitions(sources, targets, edge_types, train_counts, test_counts)?;
    let result = py.detach(move || next_activity_prediction(rows));
    Ok((
        result.accuracy,
        result.correct,
        result.test_total,
        result.predictions,
    ))
}

#[pyfunction]
#[pyo3(signature = (variants, train_counts, test_counts, coverage=0.95))]
fn variant_conformance(
    py: Python<'_>,
    variants: Vec<String>,
    train_counts: Vec<u64>,
    test_counts: Vec<u64>,
    coverage: f64,
) -> PyResult<(f64, u64, u64, u64, Vec<String>)> {
    if variants.len() != train_counts.len() || variants.len() != test_counts.len() {
        return Err(PyValueError::new_err(
            "all input columns must have the same length",
        ));
    }
    let rows = variants
        .into_iter()
        .zip(train_counts)
        .zip(test_counts)
        .map(|((variant, train), test)| (variant, train, test))
        .collect::<Vec<_>>();
    let result = py
        .detach(move || variant_frequency_conformance(rows, coverage))
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    Ok((
        result.fitness,
        result.conforming,
        result.deviations,
        result.test_total,
        result.model,
    ))
}

#[pyfunction]
fn bottleneck_order(
    py: Python<'_>,
    frequencies: Vec<u64>,
    mean_durations: Vec<f64>,
) -> PyResult<Vec<usize>> {
    py.detach(move || rank_bottlenecks(&frequencies, &mean_durations))
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(signature = (labels, baseline_counts, current_counts, top_n=10))]
fn frequency_drift(
    py: Python<'_>,
    labels: Vec<String>,
    baseline_counts: Vec<u64>,
    current_counts: Vec<u64>,
    top_n: usize,
) -> PyResult<FrequencyDriftOutput> {
    if labels.len() != baseline_counts.len() || labels.len() != current_counts.len() {
        return Err(PyValueError::new_err(
            "all input columns must have the same length",
        ));
    }
    let rows = labels
        .into_iter()
        .zip(baseline_counts)
        .zip(current_counts)
        .map(|((label, baseline), current)| (label, baseline, current))
        .collect::<Vec<_>>();
    let result = py
        .detach(move || score_frequency_drift(rows, top_n))
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    Ok((
        result.divergence,
        result.baseline_total,
        result.current_total,
        result
            .contributors
            .into_iter()
            .map(|item| {
                (
                    item.label,
                    item.baseline_share,
                    item.current_share,
                    item.share_delta,
                    item.js_contribution,
                )
            })
            .collect(),
    ))
}

#[pyfunction]
fn binding_capsule_info(py: Python<'_>, capsule: Vec<u8>) -> PyResult<(u8, usize, bool)> {
    py.detach(move || {
        let capsule = BindingCapsule::decode(&capsule)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        Ok((
            capsule.schema() as u8,
            capsule.row_count(),
            capsule.is_factorized(),
        ))
    })
}

#[pyfunction]
fn decode_binding_capsule(py: Python<'_>, capsule: Vec<u8>) -> PyResult<Vec<BindingRowOutput>> {
    py.detach(move || {
        let capsule = BindingCapsule::decode(&capsule)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        Ok(capsule
            .rows()
            .map(|row| {
                (
                    row.ids().to_vec(),
                    row.label.map(str::to_owned),
                    row.violated,
                    row.value,
                )
            })
            .collect())
    })
}

fn decode_event_batch(input: EventBatchInput) -> PyResult<EventBatch> {
    EventBatch::decode(input.0, input.1, input.2, input.3, input.4)
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

fn event_summary_output(summary: EventLogSummary) -> EventSummaryOutput {
    (
        summary.case_count,
        summary.event_count,
        summary.payload_bytes,
        summary
            .variants
            .into_iter()
            .map(|variant| (variant.activity_path, variant.frequency))
            .collect(),
        summary
            .dfg
            .into_iter()
            .map(|edge| {
                (
                    edge.source,
                    edge.target,
                    edge.frequency,
                    edge.mean_duration_seconds,
                )
            })
            .collect(),
        summary
            .activities
            .into_iter()
            .map(|activity| {
                (
                    activity.activity,
                    activity.case_frequency,
                    activity.occurrence_frequency,
                    activity.start_frequency,
                    activity.end_frequency,
                )
            })
            .collect(),
    )
}

fn push_event_batch(builder: &mut EventSummaryBuilder, input: EventBatchInput) -> PyResult<()> {
    let batch = decode_event_batch(input)?;
    builder
        .push_batch(&batch)
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

fn push_windowed_event_batch(
    builders: &mut [EventSummaryBuilder],
    input: WindowedEventBatchInput,
) -> PyResult<()> {
    let (window_ordinal, path, activity_count, case_count, case_ids, timestamps) = input;
    let index = window_ordinal
        .checked_sub(1)
        .and_then(|value| usize::try_from(value).ok())
        .filter(|index| *index < builders.len())
        .ok_or_else(|| {
            PyValueError::new_err(format!(
                "event batch returned unexpected window {window_ordinal}"
            ))
        })?;
    push_event_batch(
        &mut builders[index],
        (path, activity_count, case_count, case_ids, timestamps),
    )
}

#[pyfunction]
fn event_batch_summary(
    py: Python<'_>,
    batches: Vec<EventBatchInput>,
) -> PyResult<EventSummaryOutput> {
    py.detach(move || {
        let mut builder = EventSummaryBuilder::new();
        for batch in batches {
            push_event_batch(&mut builder, batch)?;
        }
        Ok(event_summary_output(builder.finish()))
    })
}

#[pyfunction]
fn event_window_batch_summaries(
    py: Python<'_>,
    batches: Vec<WindowedEventBatchInput>,
) -> PyResult<Vec<(i32, EventSummaryOutput)>> {
    py.detach(move || {
        let mut builders = std::collections::BTreeMap::<i32, EventSummaryBuilder>::new();
        for (window, path, activity_count, case_count, case_ids, timestamps) in batches {
            if window <= 0 {
                return Err(PyValueError::new_err("window ordinals must be positive"));
            }
            push_event_batch(
                builders.entry(window).or_default(),
                (path, activity_count, case_count, case_ids, timestamps),
            )?;
        }
        Ok(builders
            .into_iter()
            .map(|(window, builder)| (window, event_summary_output(builder.finish())))
            .collect())
    })
}

/// Incremental Python-facing builder used by DB-API cursor iteration. Only the
/// current PostgreSQL row and compact native aggregate maps remain live.
#[pyclass(name = "EventWindowSummaryBuilder")]
struct PyEventWindowSummaryBuilder {
    builders: Option<Vec<EventSummaryBuilder>>,
}

#[pymethods]
impl PyEventWindowSummaryBuilder {
    #[new]
    fn new(window_count: usize) -> PyResult<Self> {
        if window_count == 0 || window_count > i32::MAX as usize {
            return Err(PyValueError::new_err(
                "window_count must be between 1 and 2147483647",
            ));
        }
        let mut builders = Vec::new();
        builders
            .try_reserve_exact(window_count)
            .map_err(|_| PyValueError::new_err("window_count is too large"))?;
        builders.resize_with(window_count, EventSummaryBuilder::new);
        Ok(Self {
            builders: Some(builders),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn push_batch(
        &mut self,
        py: Python<'_>,
        window_ordinal: i32,
        activity_path: Vec<String>,
        activity_count: i32,
        case_count: i32,
        case_ids: Vec<u8>,
        timestamps: Vec<u8>,
    ) -> PyResult<()> {
        let builders = self
            .builders
            .as_mut()
            .ok_or_else(|| PyValueError::new_err("event summary builder is finished"))?;
        py.detach(move || {
            push_windowed_event_batch(
                builders,
                (
                    window_ordinal,
                    activity_path,
                    activity_count,
                    case_count,
                    case_ids,
                    timestamps,
                ),
            )
        })
    }

    fn push_batches(
        &mut self,
        py: Python<'_>,
        batches: Vec<WindowedEventBatchInput>,
    ) -> PyResult<()> {
        let builders = self
            .builders
            .as_mut()
            .ok_or_else(|| PyValueError::new_err("event summary builder is finished"))?;
        py.detach(move || {
            for batch in batches {
                push_windowed_event_batch(builders, batch)?;
            }
            Ok(())
        })
    }

    fn finish(&mut self, py: Python<'_>) -> PyResult<Vec<EventSummaryOutput>> {
        let builders = self
            .builders
            .take()
            .ok_or_else(|| PyValueError::new_err("event summary builder is finished"))?;
        Ok(py.detach(move || {
            builders
                .into_iter()
                .map(|builder| event_summary_output(builder.finish()))
                .collect()
        }))
    }
}

#[pyfunction]
fn decode_binding_pair_groups(
    py: Python<'_>,
    capsule: Vec<u8>,
) -> PyResult<Vec<BindingPairGroupOutput>> {
    py.detach(move || {
        let capsule = BindingCapsule::decode(&capsule)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let groups = capsule
            .pair_groups()
            .ok_or_else(|| PyValueError::new_err("binding capsule is not factorized"))?;
        Ok(groups
            .groups()
            .map(|group| (group.source, group.targets.to_vec(), group.events.to_vec()))
            .collect())
    })
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", "0.9.0")?;
    module.add_function(wrap_pyfunction!(dfg_conformance, module)?)?;
    module.add_function(wrap_pyfunction!(next_activity, module)?)?;
    module.add_function(wrap_pyfunction!(variant_conformance, module)?)?;
    module.add_function(wrap_pyfunction!(bottleneck_order, module)?)?;
    module.add_function(wrap_pyfunction!(frequency_drift, module)?)?;
    module.add_function(wrap_pyfunction!(binding_capsule_info, module)?)?;
    module.add_function(wrap_pyfunction!(decode_binding_capsule, module)?)?;
    module.add_function(wrap_pyfunction!(decode_binding_pair_groups, module)?)?;
    module.add_function(wrap_pyfunction!(event_batch_summary, module)?)?;
    module.add_function(wrap_pyfunction!(event_window_batch_summaries, module)?)?;
    module.add_class::<PyEventWindowSummaryBuilder>()?;
    Ok(())
}
