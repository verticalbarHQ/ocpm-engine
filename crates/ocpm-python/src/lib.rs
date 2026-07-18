use ocpm_core::{
    TransitionKey, dfg_frequency_conformance, next_activity_prediction, rank_bottlenecks,
    variant_frequency_conformance,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

type TransitionTuple = (String, String, String);
type DfgConformanceOutput = (f64, u64, u64, u64, Vec<TransitionTuple>);
type NextActivityOutput = (f64, u64, u64, Vec<TransitionTuple>);

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

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", "0.2.0")?;
    module.add_function(wrap_pyfunction!(dfg_conformance, module)?)?;
    module.add_function(wrap_pyfunction!(next_activity, module)?)?;
    module.add_function(wrap_pyfunction!(variant_conformance, module)?)?;
    module.add_function(wrap_pyfunction!(bottleneck_order, module)?)?;
    Ok(())
}
