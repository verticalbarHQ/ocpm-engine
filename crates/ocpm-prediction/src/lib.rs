//! Deterministic predictive monitoring over object-centric process executions.
//!
//! PROVENANCE: remaining-time prefix features follow doi:10.1145/3331449;
//! outcome prediction follows doi:10.1109/TSC.2016.2645153; lifecycle and
//! evaluation boundaries follow doi:10.1007/s44311-024-00002-4. No source
//! code from another process-mining library was consulted.

use ocpm_core::{
    AttributeValue, DatasetView, FitPredictionRequest, OcpmError, OcpmErrorCode, OcpmResult,
    PredictionCandidate, PredictionRequest, PredictionResult, PredictionTarget, content_hash,
};
use ocpm_provider::{ExecutionMode, OcpmProvider};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const PROVENANCE: &[&str] = &[
    "doi:10.1145/3331449",
    "doi:10.1109/TSC.2016.2645153",
    "doi:10.1007/s44311-024-00002-4",
];

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NumericSummary {
    pub support: u64,
    pub mean: f64,
    pub lower: f64,
    pub upper: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BinarySummary {
    pub positive: u64,
    pub total: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PredictionArtifact {
    pub schema_version: String,
    pub target: PredictionTarget,
    pub dataset_id: String,
    pub maximum_order: usize,
    #[serde(default = "default_feature_encoding")]
    pub feature_encoding: String,
    pub next_counts: BTreeMap<String, BTreeMap<String, u64>>,
    pub remaining: BTreeMap<String, NumericSummary>,
    pub binary: BTreeMap<String, BinarySummary>,
    pub target_attribute: Option<String>,
    pub positive_value: Option<AttributeValue>,
    pub content_hash: String,
}

fn default_feature_encoding() -> String {
    "sequential".to_owned()
}

impl PredictionArtifact {
    fn finalize_hash(&mut self) -> OcpmResult<()> {
        self.content_hash.clear();
        self.content_hash = content_hash(self)?;
        Ok(())
    }

    fn validate(&self) -> OcpmResult<()> {
        if self.schema_version != "1.0" {
            return Err(OcpmError::new(
                OcpmErrorCode::ArtifactIncompatible,
                "prediction artifact schema_version must be 1.0",
            ));
        }
        let mut expected = self.clone();
        expected.finalize_hash()?;
        if expected.content_hash != self.content_hash {
            return Err(OcpmError::new(
                OcpmErrorCode::ArtifactIncompatible,
                "prediction artifact content hash does not match",
            ));
        }
        Ok(())
    }
}

pub fn fit(
    provider: &dyn OcpmProvider,
    request: &FitPredictionRequest,
) -> OcpmResult<PredictionArtifact> {
    if request.semantic_version != "1.0" {
        return Err(OcpmError::invalid_request("semantic_version must be 1.0"));
    }
    let maximum_order = request
        .parameters
        .get("maximum_order")
        .and_then(|value| value.as_u64())
        .unwrap_or(3) as usize;
    if maximum_order == 0 || maximum_order > 32 {
        return Err(OcpmError::invalid_request(
            "maximum_order must be between 1 and 32",
        ));
    }
    let feature_encoding = request
        .parameters
        .get("feature_encoding")
        .and_then(|value| value.as_str())
        .unwrap_or("sequential")
        .to_owned();
    if !matches!(
        feature_encoding.as_str(),
        "sequential" | "tabular" | "graph"
    ) {
        return Err(OcpmError::invalid_request(
            "feature_encoding must be sequential, tabular, or graph",
        ));
    }
    let leading = request
        .parameters
        .get("leading_object_type")
        .and_then(|value| value.as_str());
    let executions =
        provider.process_executions(&request.view, ExecutionMode::LeadingObject, leading)?;
    let profile = provider.profile(&request.view)?;
    if executions.is_empty() {
        return Err(OcpmError::new(
            OcpmErrorCode::InsufficientData,
            "prediction fitting requires process executions",
        ));
    }
    let target_attribute = request
        .parameters
        .get("target_attribute")
        .and_then(|value| value.as_str())
        .map(str::to_owned);
    let positive_value = request
        .parameters
        .get("positive_value")
        .map(json_attribute)
        .transpose()?;
    build_artifact(
        &executions,
        profile.dataset_id,
        request.target.clone(),
        maximum_order,
        feature_encoding,
        target_attribute,
        positive_value,
    )
}

fn build_artifact(
    executions: &[ocpm_provider::ProcessExecution],
    dataset_id: String,
    target: PredictionTarget,
    maximum_order: usize,
    feature_encoding: String,
    target_attribute: Option<String>,
    positive_value: Option<AttributeValue>,
) -> OcpmResult<PredictionArtifact> {
    let mut next_counts = BTreeMap::<String, BTreeMap<String, u64>>::new();
    let mut remaining_samples = BTreeMap::<String, Vec<f64>>::new();
    let mut binary = BTreeMap::<String, BinarySummary>::new();
    for execution in executions {
        let path = execution.activity_path();
        let terminal = execution.events.last();
        let outcome = target_attribute
            .as_deref()
            .and_then(|attribute| terminal?.attributes.get(attribute))
            .zip(positive_value.as_ref())
            .map(|(value, positive)| value == positive);
        for index in 0..path.len() {
            for order in 0..=maximum_order.min(index) {
                let prefix = feature_key(
                    &path[index - order..index],
                    &feature_encoding,
                    index,
                    execution.object_ids.len(),
                );
                if let Some(next) = path.get(index) {
                    *next_counts
                        .entry(prefix.clone())
                        .or_default()
                        .entry(next.clone())
                        .or_default() += 1;
                }
                if let (Some(current), Some(last)) = (execution.events.get(index), terminal) {
                    let remaining =
                        (last.timestamp.epoch_nanos_utc - current.timestamp.epoch_nanos_utc) as f64
                            / 1_000_000_000.0;
                    remaining_samples
                        .entry(prefix.clone())
                        .or_default()
                        .push(remaining.max(0.0));
                }
                if let Some(positive) = outcome {
                    let summary = binary.entry(prefix).or_insert(BinarySummary {
                        positive: 0,
                        total: 0,
                    });
                    summary.total += 1;
                    summary.positive += u64::from(positive);
                }
            }
        }
    }
    let remaining = remaining_samples
        .into_iter()
        .map(|(key, mut values)| {
            values.sort_by(f64::total_cmp);
            let support = values.len() as u64;
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let lower = quantile(&values, 0.05);
            let upper = quantile(&values, 0.95);
            (
                key,
                NumericSummary {
                    support,
                    mean,
                    lower,
                    upper,
                },
            )
        })
        .collect();
    let mut artifact = PredictionArtifact {
        schema_version: "1.0".to_owned(),
        target,
        dataset_id,
        maximum_order,
        feature_encoding,
        next_counts,
        remaining,
        binary,
        target_attribute,
        positive_value,
        content_hash: String::new(),
    };
    artifact.finalize_hash()?;
    Ok(artifact)
}

pub fn predict(
    provider: &dyn OcpmProvider,
    request: &PredictionRequest,
) -> OcpmResult<PredictionResult> {
    if request.semantic_version != "1.0" {
        return Err(OcpmError::invalid_request("semantic_version must be 1.0"));
    }
    let artifact = if let Some(value) = &request.model_artifact {
        serde_json::from_value::<PredictionArtifact>(value.clone()).map_err(|error| {
            OcpmError::new(
                OcpmErrorCode::ArtifactIncompatible,
                format!("invalid prediction artifact: {error}"),
            )
        })?
    } else {
        fit(
            provider,
            &FitPredictionRequest {
                semantic_version: request.semantic_version.clone(),
                view: request.view.clone(),
                target: request.target.clone(),
                parameters: request.parameters.clone(),
                seed: 0,
            },
        )?
    };
    artifact.validate()?;
    if artifact.target != request.target {
        return Err(OcpmError::new(
            OcpmErrorCode::ArtifactIncompatible,
            "prediction artifact target differs from request target",
        ));
    }
    let (key, order) = backoff_key(
        &request.state.activities,
        request.state.object_ids.len(),
        artifact.maximum_order,
        &artifact.feature_encoding,
        |candidate| match request.target {
            PredictionTarget::NextActivity => artifact.next_counts.contains_key(candidate),
            PredictionTarget::RemainingTime => artifact.remaining.contains_key(candidate),
            PredictionTarget::Outcome | PredictionTarget::Risk => {
                artifact.binary.contains_key(candidate)
            }
        },
    )
    .ok_or_else(|| {
        OcpmError::new(
            OcpmErrorCode::InsufficientData,
            "no prediction support exists for this prefix",
        )
    })?;
    let mut result = PredictionResult {
        backoff_level: format!("order_{order}"),
        model_hash: Some(artifact.content_hash.clone()),
        ..PredictionResult::default()
    };
    result.diagnostics.insert(
        "feature_encoding".to_owned(),
        serde_json::json!(artifact.feature_encoding),
    );
    match request.target {
        PredictionTarget::NextActivity => {
            let counts = &artifact.next_counts[&key];
            let total = counts.values().sum::<u64>();
            result.support = total;
            result.candidates = counts
                .iter()
                .map(|(label, count)| PredictionCandidate {
                    label: label.clone(),
                    probability: *count as f64 / total as f64,
                })
                .collect();
            result.candidates.sort_by(|left, right| {
                right
                    .probability
                    .total_cmp(&left.probability)
                    .then_with(|| left.label.cmp(&right.label))
            });
        }
        PredictionTarget::RemainingTime => {
            let summary = &artifact.remaining[&key];
            result.support = summary.support;
            result.point_estimate_seconds = Some(summary.mean);
            result.interval_seconds = Some((summary.lower, summary.upper));
        }
        PredictionTarget::Outcome | PredictionTarget::Risk => {
            let summary = &artifact.binary[&key];
            result.support = summary.total;
            let probability = summary.positive as f64 / summary.total as f64;
            result.candidates = vec![
                PredictionCandidate {
                    label: "positive".to_owned(),
                    probability,
                },
                PredictionCandidate {
                    label: "negative".to_owned(),
                    probability: 1.0 - probability,
                },
            ];
        }
    }
    Ok(result)
}

pub fn evaluate_temporal_holdout(
    provider: &dyn OcpmProvider,
    view: &DatasetView,
    target: PredictionTarget,
    holdout_fraction: f64,
    parameters: BTreeMap<String, serde_json::Value>,
) -> OcpmResult<PredictionResult> {
    if !(0.0..1.0).contains(&holdout_fraction) {
        return Err(OcpmError::invalid_request(
            "holdout_fraction must be between 0 and 1",
        ));
    }
    let mut executions = provider.process_executions(view, ExecutionMode::LeadingObject, None)?;
    executions.sort_by(|left, right| {
        left.events
            .last()
            .map(|event| &event.timestamp)
            .cmp(&right.events.last().map(|event| &event.timestamp))
            .then_with(|| left.id.cmp(&right.id))
    });
    let split = ((executions.len() as f64) * (1.0 - holdout_fraction)).floor() as usize;
    if split == 0 || split >= executions.len() {
        return Err(OcpmError::new(
            OcpmErrorCode::InsufficientData,
            "temporal holdout requires nonempty train and test partitions",
        ));
    }
    let maximum_order = parameters
        .get("maximum_order")
        .and_then(|value| value.as_u64())
        .unwrap_or(3) as usize;
    let feature_encoding = parameters
        .get("feature_encoding")
        .and_then(|value| value.as_str())
        .unwrap_or("sequential")
        .to_owned();
    let target_attribute = parameters
        .get("target_attribute")
        .and_then(|value| value.as_str())
        .map(str::to_owned);
    let positive_value = parameters
        .get("positive_value")
        .map(json_attribute)
        .transpose()?;
    let dataset_id = provider.profile(view)?.dataset_id;
    let artifact = build_artifact(
        &executions[..split],
        dataset_id,
        target.clone(),
        maximum_order,
        feature_encoding,
        target_attribute,
        positive_value,
    )?;
    let mut evaluated = 0_u64;
    let mut correct = 0_u64;
    let mut absolute_error = 0.0_f64;
    let mut interval_hits = 0_u64;
    let mut brier = 0.0_f64;
    for execution in &executions[split..] {
        let path = execution.activity_path();
        let terminal = execution.events.last();
        for index in 0..path.len() {
            let Some((key, _)) = backoff_key(
                &path[..index],
                execution.object_ids.len(),
                artifact.maximum_order,
                &artifact.feature_encoding,
                |candidate| match target {
                    PredictionTarget::NextActivity => artifact.next_counts.contains_key(candidate),
                    PredictionTarget::RemainingTime => artifact.remaining.contains_key(candidate),
                    PredictionTarget::Outcome | PredictionTarget::Risk => {
                        artifact.binary.contains_key(candidate)
                    }
                },
            ) else {
                continue;
            };
            match target {
                PredictionTarget::NextActivity => {
                    let counts = &artifact.next_counts[&key];
                    let winner = counts.iter().max_by(|left, right| {
                        left.1.cmp(right.1).then_with(|| right.0.cmp(left.0))
                    });
                    if winner.is_some_and(|(label, _)| *label == path[index]) {
                        correct += 1;
                    }
                    evaluated += 1;
                }
                PredictionTarget::RemainingTime => {
                    let Some(current) = execution.events.get(index) else {
                        continue;
                    };
                    let Some(last) = terminal else {
                        continue;
                    };
                    let actual =
                        (last.timestamp.epoch_nanos_utc - current.timestamp.epoch_nanos_utc) as f64
                            / 1_000_000_000.0;
                    let summary = &artifact.remaining[&key];
                    absolute_error += (summary.mean - actual).abs();
                    interval_hits += u64::from(actual >= summary.lower && actual <= summary.upper);
                    evaluated += 1;
                }
                PredictionTarget::Outcome | PredictionTarget::Risk => {
                    let Some(attribute) = artifact.target_attribute.as_deref() else {
                        continue;
                    };
                    let Some(positive_value) = artifact.positive_value.as_ref() else {
                        continue;
                    };
                    let Some(actual) = terminal
                        .and_then(|event| event.attributes.get(attribute))
                        .map(|value| value == positive_value)
                    else {
                        continue;
                    };
                    let summary = &artifact.binary[&key];
                    let probability = summary.positive as f64 / summary.total as f64;
                    let actual = if actual { 1.0 } else { 0.0 };
                    brier += (probability - actual).powi(2);
                    evaluated += 1;
                }
            }
        }
    }
    if evaluated == 0 {
        return Err(OcpmError::new(
            OcpmErrorCode::InsufficientData,
            "temporal holdout has no supported test prefixes",
        ));
    }
    let metrics = match target {
        PredictionTarget::NextActivity => serde_json::json!({
            "accuracy": correct as f64 / evaluated as f64,
            "correct": correct,
        }),
        PredictionTarget::RemainingTime => serde_json::json!({
            "mean_absolute_error_seconds": absolute_error / evaluated as f64,
            "interval_coverage": interval_hits as f64 / evaluated as f64,
        }),
        PredictionTarget::Outcome | PredictionTarget::Risk => serde_json::json!({
            "brier_score": brier / evaluated as f64,
        }),
    };
    Ok(PredictionResult {
        backoff_level: "evaluation_manifest".to_owned(),
        support: evaluated,
        model_hash: Some(artifact.content_hash),
        diagnostics: BTreeMap::from([
            ("target".to_owned(), serde_json::json!(target)),
            ("train_executions".to_owned(), serde_json::json!(split)),
            (
                "test_executions".to_owned(),
                serde_json::json!(executions.len() - split),
            ),
            ("parameters".to_owned(), serde_json::json!(parameters)),
            ("metrics".to_owned(), metrics),
            (
                "leakage_guard".to_owned(),
                serde_json::json!(
                    "executions are sorted by completion time; the model is fit only on the earlier partition"
                ),
            ),
        ]),
        ..PredictionResult::default()
    })
}

fn prefix_key(values: &[String]) -> String {
    serde_json::to_string(values).expect("activity vectors are serializable")
}

fn feature_key(
    values: &[String],
    encoding: &str,
    prefix_length: usize,
    object_count: usize,
) -> String {
    match encoding {
        "tabular" => serde_json::to_string(&serde_json::json!({
            "encoding": "tabular",
            "last_activity": values.last(),
            "context_order": values.len(),
            "prefix_length": prefix_length,
            "object_count": object_count,
        }))
        .expect("tabular features are serializable"),
        "graph" => serde_json::to_string(&serde_json::json!({
            "encoding": "graph",
            "activity_suffix": values,
            "object_count": object_count,
        }))
        .expect("graph features are serializable"),
        _ => prefix_key(values),
    }
}

fn backoff_key(
    path: &[String],
    object_count: usize,
    maximum_order: usize,
    encoding: &str,
    exists: impl Fn(&str) -> bool,
) -> Option<(String, usize)> {
    for order in (0..=maximum_order.min(path.len())).rev() {
        let key = feature_key(
            &path[path.len() - order..],
            encoding,
            path.len(),
            object_count,
        );
        if exists(&key) {
            return Some((key, order));
        }
    }
    None
}

fn quantile(values: &[f64], probability: f64) -> f64 {
    let index = ((values.len() - 1) as f64 * probability).round() as usize;
    values[index]
}

fn json_attribute(value: &serde_json::Value) -> OcpmResult<AttributeValue> {
    match value {
        serde_json::Value::Null => Ok(AttributeValue::Null),
        serde_json::Value::String(value) => Ok(AttributeValue::String(value.clone())),
        serde_json::Value::Bool(value) => Ok(AttributeValue::Boolean(*value)),
        serde_json::Value::Number(value) => value
            .as_i64()
            .map(AttributeValue::Integer)
            .or_else(|| value.as_f64().map(AttributeValue::Float))
            .ok_or_else(|| OcpmError::invalid_request("positive_value number is unsupported")),
        _ => Err(OcpmError::invalid_request(
            "positive_value must be a scalar JSON value",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_prefers_longest_supported_suffix() {
        let path = vec!["a".to_owned(), "b".to_owned()];
        let supported = prefix_key(&["b".to_owned()]);
        assert_eq!(
            backoff_key(&path, 0, 3, "sequential", |key| key == supported),
            Some((supported, 1))
        );
    }
}
