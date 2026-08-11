//! Optional provider-neutral graph neural networks for `ocpm-engine`.
//!
//! The built-in bottleneck detector is a deterministic, full-batch, two-layer
//! mean-aggregation network. Storage providers expose only canonical transition
//! observations; graph construction, training, scoring, and ranking remain in
//! this crate. The implementation is intentionally CPU-only and has no tensor
//! runtime dependency.
//!
//! PROVENANCE: the heterogeneous object/event context follows the representation
//! studied in doi:10.1007/978-3-031-61057-8_14. Inductive neighborhood mean
//! aggregation follows the equations in the peer-reviewed GraphSAGE paper,
//! proceedings.neurips.cc/paper/6703. The use of an optional GNN path is also
//! informed by doi:10.1007/978-3-031-50974-2_39. No third-party process-mining or
//! GNN implementation source was consulted.

use ocpm_core::{
    AttributeValue, DatasetView, OcpmError, OcpmErrorCode, OcpmResult, PredictionResult,
    content_hash,
};
use ocpm_provider::{BottleneckObservation, BottleneckObservationRequest, OcpmProvider};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const PROVENANCE_GNN: &[&str] = &[
    "doi:10.1007/978-3-031-61057-8_14",
    "proceedings.neurips.cc/paper/6703",
    "doi:10.1007/978-3-031-50974-2_39",
];

const ALGORITHM: &str = "mean_graphsage_bottleneck_v1";
const HASH_FEATURE_OFFSET: usize = 9;

fn default_semantic_version() -> String {
    "1.0".to_owned()
}
fn default_minimum_support() -> u64 {
    5
}
fn default_resource_attribute() -> String {
    "org:resource".to_owned()
}
fn default_feature_dimensions() -> usize {
    32
}
fn default_hidden_dimensions() -> usize {
    16
}
fn default_epochs() -> u64 {
    100
}
fn default_patience() -> u64 {
    15
}
fn default_learning_rate() -> f64 {
    0.01
}
fn default_l2() -> f64 {
    0.0001
}
fn default_validation_fraction() -> f64 {
    0.2
}
fn default_label_quantile() -> f64 {
    0.9
}
fn default_maximum_nodes() -> u64 {
    100_000
}
fn default_maximum_neighbors() -> usize {
    32
}
fn default_seed() -> u64 {
    0x4f43_504d_474e_4e31
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GnnTask {
    NextActivity,
    RemainingTime,
    Outcome,
    Risk,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GnnRequest {
    pub semantic_version: String,
    pub view: DatasetView,
    pub task: GnnTask,
    #[serde(default)]
    pub parameters: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GnnArtifact {
    pub semantic_version: String,
    pub backend: String,
    pub model_hash: String,
    pub payload: serde_json::Value,
}

/// Compatibility boundary for independently distributed predictive backends.
pub trait GnnBackend: Send + Sync {
    fn name(&self) -> &str;
    fn fit(&self, request: &GnnRequest) -> OcpmResult<GnnArtifact>;
    fn predict(&self, request: &GnnRequest, artifact: &GnnArtifact)
    -> OcpmResult<PredictionResult>;
}

pub fn backend_required() -> OcpmError {
    OcpmError::new(
        OcpmErrorCode::ProviderUnavailable,
        "the requested predictive GNN task requires a registered backend; the built-in module currently provides provider-neutral bottleneck risk detection",
    )
}

/// Configuration for the built-in graph-aware bottleneck detector.
///
/// Target duration is deliberately excluded from node input features. It is
/// used only to derive training labels and observed impact, avoiding the trivial
/// leakage of teaching the model to reproduce its own threshold.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GnnBottleneckRequest {
    #[serde(default = "default_semantic_version")]
    pub semantic_version: String,
    #[serde(default)]
    pub view: DatasetView,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leading_object_type: Option<String>,
    #[serde(default = "default_minimum_support")]
    pub minimum_support: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slow_threshold_seconds: Option<f64>,
    #[serde(default = "default_resource_attribute")]
    pub resource_attribute: String,
    #[serde(default = "default_feature_dimensions")]
    pub feature_dimensions: usize,
    #[serde(default = "default_hidden_dimensions")]
    pub hidden_dimensions: usize,
    #[serde(default = "default_epochs")]
    pub epochs: u64,
    #[serde(default = "default_patience")]
    pub patience: u64,
    #[serde(default = "default_learning_rate")]
    pub learning_rate: f64,
    #[serde(default = "default_l2")]
    pub l2: f64,
    #[serde(default = "default_validation_fraction")]
    pub validation_fraction: f64,
    #[serde(default = "default_label_quantile")]
    pub label_quantile: f64,
    #[serde(default = "default_maximum_nodes")]
    pub maximum_nodes: u64,
    #[serde(default = "default_maximum_neighbors")]
    pub maximum_neighbors: usize,
    #[serde(default = "default_seed")]
    pub seed: u64,
}

impl Default for GnnBottleneckRequest {
    fn default() -> Self {
        Self {
            semantic_version: default_semantic_version(),
            view: DatasetView::default(),
            leading_object_type: None,
            minimum_support: default_minimum_support(),
            slow_threshold_seconds: None,
            resource_attribute: default_resource_attribute(),
            feature_dimensions: default_feature_dimensions(),
            hidden_dimensions: default_hidden_dimensions(),
            epochs: default_epochs(),
            patience: default_patience(),
            learning_rate: default_learning_rate(),
            l2: default_l2(),
            validation_fraction: default_validation_fraction(),
            label_quantile: default_label_quantile(),
            maximum_nodes: default_maximum_nodes(),
            maximum_neighbors: default_maximum_neighbors(),
            seed: default_seed(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GnnEdgeThreshold {
    pub object_type: String,
    pub source_activity: String,
    pub target_activity: String,
    pub training_support: u64,
    pub threshold_seconds: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct GnnTrainingDiagnostics {
    pub training_count: u64,
    pub validation_count: u64,
    pub positive_training_count: u64,
    pub epochs_completed: u64,
    pub training_loss: f64,
    pub validation_loss: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation_auc: Option<f64>,
    pub validation_accuracy: f64,
    pub early_stopped: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GnnModelWeights {
    pub first_self: Vec<f32>,
    pub first_neighbor: Vec<f32>,
    pub first_bias: Vec<f32>,
    pub second_self: Vec<f32>,
    pub second_neighbor: Vec<f32>,
    pub second_bias: Vec<f32>,
    pub output: Vec<f32>,
    pub output_bias: f32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GnnBottleneckArtifact {
    pub semantic_version: String,
    pub algorithm: String,
    pub model_hash: String,
    pub feature_dimensions: usize,
    pub hidden_dimensions: usize,
    pub maximum_neighbors: usize,
    pub minimum_support: u64,
    pub resource_attribute: String,
    pub seed: u64,
    pub global_threshold_seconds: f64,
    pub edge_thresholds: Vec<GnnEdgeThreshold>,
    pub weights: GnnModelWeights,
    pub training: GnnTrainingDiagnostics,
    #[serde(default)]
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct GnnBottleneckSignal {
    pub object_type: String,
    pub source_activity: String,
    pub target_activity: String,
    pub support: u64,
    pub threshold_seconds: f64,
    pub mean_risk: f64,
    pub maximum_risk: f64,
    pub predicted_slow_count: u64,
    pub predicted_slow_rate: f64,
    pub observed_slow_count: u64,
    pub risk_weighted_excess_seconds: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct GnnBottleneckDiagnostics {
    pub provider: String,
    pub observation_count: u64,
    pub message_passing_arc_count: u64,
    pub feature_dimensions: u64,
    pub hidden_dimensions: u64,
    pub maximum_observed_neighbors: u64,
    pub deterministic: bool,
    pub exact: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GnnBottleneckResult {
    pub semantic_version: String,
    pub model_hash: String,
    pub signals: Vec<GnnBottleneckSignal>,
    pub training: GnnTrainingDiagnostics,
    pub diagnostics: GnnBottleneckDiagnostics,
    pub warnings: Vec<String>,
}

/// Fit a portable model artifact using canonical observations from any provider.
pub fn fit(
    provider: &dyn OcpmProvider,
    request: &GnnBottleneckRequest,
) -> OcpmResult<GnnBottleneckArtifact> {
    validate_request(request)?;
    let observations = load_observations(provider, request)?;
    fit_observations(&observations, request)
}

/// Score a view with an existing artifact. The artifact, not the storage layer,
/// owns feature and threshold semantics.
pub fn score(
    provider: &dyn OcpmProvider,
    request: &GnnBottleneckRequest,
    artifact: &GnnBottleneckArtifact,
) -> OcpmResult<GnnBottleneckResult> {
    validate_request(request)?;
    validate_artifact(artifact)?;
    validate_request_artifact(request, artifact)?;
    let observations = load_observations(provider, request)?;
    score_observations(provider.name(), &observations, request, artifact)
}

/// Fit and score without asking the provider to materialize the projection
/// twice. The exact statistical bottleneck suite remains the default engine API;
/// this probabilistic result is explicitly separate.
pub fn detect(
    provider: &dyn OcpmProvider,
    request: &GnnBottleneckRequest,
) -> OcpmResult<GnnBottleneckResult> {
    validate_request(request)?;
    let observations = load_observations(provider, request)?;
    let artifact = fit_observations(&observations, request)?;
    score_observations(provider.name(), &observations, request, &artifact)
}

/// Fit an artifact from an already accelerated canonical projection.
///
/// This is intended for asynchronous adapters such as `ocpm-postgres`; it does
/// not move graph or model semantics into the provider.
pub fn fit_from_observations(
    observations: &[BottleneckObservation],
    request: &GnnBottleneckRequest,
) -> OcpmResult<GnnBottleneckArtifact> {
    validate_request(request)?;
    fit_observations(observations, request)
}

/// Score an already accelerated canonical projection with a portable artifact.
pub fn score_from_observations(
    provider_name: &str,
    observations: &[BottleneckObservation],
    request: &GnnBottleneckRequest,
    artifact: &GnnBottleneckArtifact,
) -> OcpmResult<GnnBottleneckResult> {
    validate_request(request)?;
    validate_artifact(artifact)?;
    validate_request_artifact(request, artifact)?;
    score_observations(provider_name, observations, request, artifact)
}

/// Fit and score an already accelerated canonical projection in one pass.
pub fn detect_from_observations(
    provider_name: &str,
    observations: &[BottleneckObservation],
    request: &GnnBottleneckRequest,
) -> OcpmResult<GnnBottleneckResult> {
    validate_request(request)?;
    let artifact = fit_observations(observations, request)?;
    score_observations(provider_name, observations, request, &artifact)
}

fn load_observations(
    provider: &dyn OcpmProvider,
    request: &GnnBottleneckRequest,
) -> OcpmResult<Vec<BottleneckObservation>> {
    provider.bottleneck_observations(&BottleneckObservationRequest {
        view: request.view.clone(),
        leading_object_type: request.leading_object_type.clone(),
    })
}

fn validate_request(request: &GnnBottleneckRequest) -> OcpmResult<()> {
    if request.semantic_version != "1.0" {
        return Err(OcpmError::new(
            OcpmErrorCode::UnsupportedSemanticVersion,
            "GNN bottleneck semantic_version must be 1.0",
        ));
    }
    if request.minimum_support == 0
        || request.feature_dimensions < 16
        || request.feature_dimensions > 256
        || request.hidden_dimensions == 0
        || request.hidden_dimensions > 256
        || request.epochs == 0
        || request.epochs > 10_000
        || request.patience == 0
        || request.maximum_nodes < 2
        || request.maximum_neighbors == 0
        || request.maximum_neighbors > 4096
    {
        return Err(OcpmError::invalid_request(
            "GNN dimensions, training bounds, support, node limit, and neighbor limit are invalid",
        ));
    }
    if !request.learning_rate.is_finite()
        || request.learning_rate <= 0.0
        || request.learning_rate > 1.0
        || !request.l2.is_finite()
        || request.l2 < 0.0
        || !(0.0..0.5).contains(&request.validation_fraction)
        || !(0.5..1.0).contains(&request.label_quantile)
        || request
            .slow_threshold_seconds
            .is_some_and(|value| !value.is_finite() || value < 0.0)
    {
        return Err(OcpmError::invalid_request(
            "GNN learning rate, regularization, validation fraction, label quantile, or threshold is invalid",
        ));
    }
    if request.resource_attribute.trim().is_empty() {
        return Err(OcpmError::invalid_request(
            "resource_attribute must not be empty",
        ));
    }
    Ok(())
}

fn validate_request_artifact(
    request: &GnnBottleneckRequest,
    artifact: &GnnBottleneckArtifact,
) -> OcpmResult<()> {
    if request.feature_dimensions != artifact.feature_dimensions
        || request.hidden_dimensions != artifact.hidden_dimensions
        || request.maximum_neighbors != artifact.maximum_neighbors
        || request.minimum_support != artifact.minimum_support
        || request.resource_attribute != artifact.resource_attribute
    {
        return Err(OcpmError::new(
            OcpmErrorCode::ArtifactIncompatible,
            "request feature, neighborhood, support, or resource semantics differ from the GNN artifact",
        ));
    }
    Ok(())
}

fn validate_artifact(artifact: &GnnBottleneckArtifact) -> OcpmResult<()> {
    if artifact.semantic_version != "1.0" || artifact.algorithm != ALGORITHM {
        return Err(OcpmError::new(
            OcpmErrorCode::ArtifactIncompatible,
            "unsupported GNN bottleneck artifact",
        ));
    }
    let f = artifact.feature_dimensions;
    let h = artifact.hidden_dimensions;
    let weights = &artifact.weights;
    if weights.first_self.len() != f * h
        || weights.first_neighbor.len() != f * h
        || weights.first_bias.len() != h
        || weights.second_self.len() != h * h
        || weights.second_neighbor.len() != h * h
        || weights.second_bias.len() != h
        || weights.output.len() != h
    {
        return Err(OcpmError::new(
            OcpmErrorCode::ArtifactIncompatible,
            "GNN artifact tensor shapes are invalid",
        ));
    }
    let mut digest = artifact.clone();
    digest.model_hash.clear();
    if content_hash(&digest)? != artifact.model_hash {
        return Err(OcpmError::new(
            OcpmErrorCode::ArtifactIncompatible,
            "GNN artifact content hash does not match its payload",
        ));
    }
    Ok(())
}

fn fit_observations(
    observations: &[BottleneckObservation],
    request: &GnnBottleneckRequest,
) -> OcpmResult<GnnBottleneckArtifact> {
    let observations = canonical_observations(observations, request.maximum_nodes)?;
    if observations.len() < 2 {
        return Err(OcpmError::new(
            OcpmErrorCode::InsufficientData,
            "GNN bottleneck fitting requires at least two transition observations",
        ));
    }
    let training_count = temporal_training_count(observations.len(), request.validation_fraction);
    let (global_threshold, edge_thresholds) =
        training_thresholds(&observations, training_count, request);
    let labels = labels(&observations, global_threshold, &edge_thresholds);
    let graph = build_graph(&observations, request)?;
    let (weights, training, mut warnings) = train_model(&graph, &labels, training_count, request);
    if edge_thresholds.is_empty() {
        warnings.push(
            "no transition met minimum_support in the temporal training partition; the global threshold was used"
                .to_owned(),
        );
    }
    let mut artifact = GnnBottleneckArtifact {
        semantic_version: "1.0".to_owned(),
        algorithm: ALGORITHM.to_owned(),
        model_hash: String::new(),
        feature_dimensions: request.feature_dimensions,
        hidden_dimensions: request.hidden_dimensions,
        maximum_neighbors: request.maximum_neighbors,
        minimum_support: request.minimum_support,
        resource_attribute: request.resource_attribute.clone(),
        seed: request.seed,
        global_threshold_seconds: global_threshold,
        edge_thresholds,
        weights,
        training,
        warnings,
    };
    artifact.model_hash = content_hash(&artifact)?;
    Ok(artifact)
}

fn score_observations(
    provider_name: &str,
    observations: &[BottleneckObservation],
    request: &GnnBottleneckRequest,
    artifact: &GnnBottleneckArtifact,
) -> OcpmResult<GnnBottleneckResult> {
    let observations = canonical_observations(observations, request.maximum_nodes)?;
    let graph = build_graph(&observations, request)?;
    let probabilities =
        forward(&graph, &artifact.weights, artifact.hidden_dimensions).probabilities;
    let thresholds = artifact
        .edge_thresholds
        .iter()
        .map(|value| (edge_key_parts(value), value.threshold_seconds))
        .collect::<BTreeMap<_, _>>();
    let mut groups = BTreeMap::<EdgeKey, SignalAccumulator>::new();
    for (index, observation) in observations.iter().enumerate() {
        let key = edge_key(observation);
        let threshold = thresholds
            .get(&key)
            .copied()
            .unwrap_or(artifact.global_threshold_seconds);
        let duration = duration_seconds(observation);
        groups
            .entry(key)
            .or_default()
            .push(probabilities[index] as f64, duration, threshold);
    }
    let mut signals = groups
        .into_iter()
        .filter(|(_, value)| value.support >= artifact.minimum_support)
        .map(
            |((object_type, source_activity, target_activity), value)| GnnBottleneckSignal {
                object_type,
                source_activity,
                target_activity,
                support: value.support,
                threshold_seconds: value.threshold_seconds,
                mean_risk: value.risk_sum / value.support as f64,
                maximum_risk: value.maximum_risk,
                predicted_slow_count: value.predicted_slow_count,
                predicted_slow_rate: value.predicted_slow_count as f64 / value.support as f64,
                observed_slow_count: value.observed_slow_count,
                risk_weighted_excess_seconds: value.risk_weighted_excess_seconds,
            },
        )
        .collect::<Vec<_>>();
    signals.sort_by(|left, right| {
        right
            .risk_weighted_excess_seconds
            .total_cmp(&left.risk_weighted_excess_seconds)
            .then_with(|| right.mean_risk.total_cmp(&left.mean_risk))
            .then_with(|| right.support.cmp(&left.support))
            .then_with(|| left.object_type.cmp(&right.object_type))
            .then_with(|| left.source_activity.cmp(&right.source_activity))
            .then_with(|| left.target_activity.cmp(&right.target_activity))
    });
    let mut warnings = artifact.warnings.clone();
    warnings.push(
        "GNN scores are associative risk signals, not causal effects; use explicit temporal hypotheses for causal screening"
            .to_owned(),
    );
    Ok(GnnBottleneckResult {
        semantic_version: "1.0".to_owned(),
        model_hash: artifact.model_hash.clone(),
        signals,
        training: artifact.training.clone(),
        diagnostics: GnnBottleneckDiagnostics {
            provider: provider_name.to_owned(),
            observation_count: observations.len() as u64,
            message_passing_arc_count: graph.neighbors.len() as u64,
            feature_dimensions: graph.feature_dimensions as u64,
            hidden_dimensions: artifact.hidden_dimensions as u64,
            maximum_observed_neighbors: graph.maximum_degree as u64,
            deterministic: true,
            exact: false,
        },
        warnings,
    })
}

type EdgeKey = (String, String, String);

fn edge_key(observation: &BottleneckObservation) -> EdgeKey {
    (
        observation.object_type.clone(),
        observation.source_activity.clone(),
        observation.target_activity.clone(),
    )
}

fn edge_key_parts(value: &GnnEdgeThreshold) -> EdgeKey {
    (
        value.object_type.clone(),
        value.source_activity.clone(),
        value.target_activity.clone(),
    )
}

fn canonical_observations(
    observations: &[BottleneckObservation],
    maximum_nodes: u64,
) -> OcpmResult<Vec<BottleneckObservation>> {
    if observations.len() as u64 > maximum_nodes {
        return Err(OcpmError::resource_limit(
            "GNN observation count exceeds maximum_nodes",
            maximum_nodes,
            observations.len() as u64,
        ));
    }
    let mut values = observations.to_vec();
    values.sort_by(|left, right| {
        left.target_timestamp_nanos
            .cmp(&right.target_timestamp_nanos)
            .then_with(|| {
                left.source_timestamp_nanos
                    .cmp(&right.source_timestamp_nanos)
            })
            .then_with(|| left.target_event_id.cmp(&right.target_event_id))
            .then_with(|| left.source_event_id.cmp(&right.source_event_id))
            .then_with(|| left.object_type.cmp(&right.object_type))
            .then_with(|| left.object_id.cmp(&right.object_id))
            .then_with(|| left.source_activity.cmp(&right.source_activity))
            .then_with(|| left.target_activity.cmp(&right.target_activity))
    });
    if values.iter().any(|value| {
        value.target_timestamp_nanos < value.source_timestamp_nanos
            || !duration_seconds(value).is_finite()
    }) {
        return Err(OcpmError::invalid_data(
            "GNN bottleneck observations require finite, nonnegative transition durations",
        ));
    }
    Ok(values)
}

fn temporal_training_count(count: usize, validation_fraction: f64) -> usize {
    if validation_fraction == 0.0 {
        return count;
    }
    ((count as f64 * (1.0 - validation_fraction)).floor() as usize).clamp(1, count - 1)
}

fn training_thresholds(
    observations: &[BottleneckObservation],
    training_count: usize,
    request: &GnnBottleneckRequest,
) -> (f64, Vec<GnnEdgeThreshold>) {
    let mut all = observations[..training_count]
        .iter()
        .map(duration_seconds)
        .collect::<Vec<_>>();
    all.sort_by(f64::total_cmp);
    let global = request
        .slow_threshold_seconds
        .unwrap_or_else(|| quantile(&all, request.label_quantile));
    let mut groups = BTreeMap::<EdgeKey, Vec<f64>>::new();
    for observation in &observations[..training_count] {
        groups
            .entry(edge_key(observation))
            .or_default()
            .push(duration_seconds(observation));
    }
    let mut thresholds = Vec::new();
    for ((object_type, source_activity, target_activity), mut values) in groups {
        if (values.len() as u64) < request.minimum_support {
            continue;
        }
        values.sort_by(f64::total_cmp);
        thresholds.push(GnnEdgeThreshold {
            object_type,
            source_activity,
            target_activity,
            training_support: values.len() as u64,
            threshold_seconds: request
                .slow_threshold_seconds
                .unwrap_or_else(|| quantile(&values, request.label_quantile)),
        });
    }
    (global, thresholds)
}

fn labels(
    observations: &[BottleneckObservation],
    global_threshold: f64,
    thresholds: &[GnnEdgeThreshold],
) -> Vec<f32> {
    let thresholds = thresholds
        .iter()
        .map(|value| (edge_key_parts(value), value.threshold_seconds))
        .collect::<BTreeMap<_, _>>();
    observations
        .iter()
        .map(|observation| {
            let threshold = thresholds
                .get(&edge_key(observation))
                .copied()
                .unwrap_or(global_threshold);
            f32::from(duration_seconds(observation) > threshold)
        })
        .collect()
}

fn duration_seconds(observation: &BottleneckObservation) -> f64 {
    (observation.target_timestamp_nanos - observation.source_timestamp_nanos) as f64
        / 1_000_000_000.0
}

fn quantile(sorted: &[f64], probability: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let position = probability * (sorted.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    if lower == upper {
        sorted[lower]
    } else {
        sorted[lower] + (sorted[upper] - sorted[lower]) * (position - lower as f64)
    }
}

struct Graph {
    features: Vec<f32>,
    neighbor_features: Vec<f32>,
    feature_dimensions: usize,
    offsets: Vec<usize>,
    neighbors: Vec<usize>,
    maximum_degree: usize,
}

fn build_graph(
    observations: &[BottleneckObservation],
    request: &GnnBottleneckRequest,
) -> OcpmResult<Graph> {
    let mut adjacency = vec![Vec::<usize>::new(); observations.len()];
    let mut by_object = BTreeMap::<u64, Vec<usize>>::new();
    for (index, observation) in observations.iter().enumerate() {
        by_object
            .entry(observation.object_id)
            .or_default()
            .push(index);
    }
    for indexes in by_object.values_mut() {
        indexes.sort_by_key(|index| {
            (
                observations[*index].source_timestamp_nanos,
                observations[*index].target_timestamp_nanos,
                observations[*index].source_event_id,
                observations[*index].target_event_id,
            )
        });
        for pair in indexes.windows(2) {
            connect(&mut adjacency, pair[0], pair[1], request.maximum_neighbors);
        }
    }

    let mut by_event = BTreeMap::<u64, Vec<usize>>::new();
    for (index, observation) in observations.iter().enumerate() {
        by_event
            .entry(observation.source_event_id)
            .or_default()
            .push(index);
        if observation.target_event_id != observation.source_event_id {
            by_event
                .entry(observation.target_event_id)
                .or_default()
                .push(index);
        }
    }
    for indexes in by_event.values_mut() {
        indexes.sort_unstable();
        indexes.dedup();
        for (position, &left) in indexes.iter().enumerate() {
            for &right in indexes.iter().skip(position + 1) {
                connect(&mut adjacency, left, right, request.maximum_neighbors);
                if adjacency[left].len() >= request.maximum_neighbors {
                    break;
                }
            }
        }
    }
    for values in &mut adjacency {
        values.sort_unstable();
        values.dedup();
    }
    let maximum_degree = adjacency.iter().map(Vec::len).max().unwrap_or_default();
    let mut offsets = Vec::with_capacity(adjacency.len() + 1);
    let mut neighbors = Vec::with_capacity(adjacency.iter().map(Vec::len).sum());
    offsets.push(0);
    for values in adjacency {
        neighbors.extend(values);
        offsets.push(neighbors.len());
    }
    let mut graph = Graph {
        features: Vec::new(),
        neighbor_features: Vec::new(),
        feature_dimensions: request.feature_dimensions,
        offsets,
        neighbors,
        maximum_degree,
    };
    graph.features = build_features(observations, &graph, request);
    // Features and topology are immutable. Retaining this one bounded
    // aggregate removes two full edge traversals per training epoch without
    // increasing the forward-pass peak allocation.
    graph.neighbor_features = neighbor_means(&graph, &graph.features, graph.feature_dimensions);
    Ok(graph)
}

fn connect(adjacency: &mut [Vec<usize>], left: usize, right: usize, maximum: usize) {
    if left == right {
        return;
    }
    if adjacency[left].len() < maximum && !adjacency[left].contains(&right) {
        adjacency[left].push(right);
    }
    if adjacency[right].len() < maximum && !adjacency[right].contains(&left) {
        adjacency[right].push(left);
    }
}

fn build_features(
    observations: &[BottleneckObservation],
    graph: &Graph,
    request: &GnnBottleneckRequest,
) -> Vec<f32> {
    let dimensions = request.feature_dimensions;
    let mut output = vec![0.0_f32; observations.len() * dimensions];
    for (index, observation) in observations.iter().enumerate() {
        let row = &mut output[index * dimensions..(index + 1) * dimensions];
        row[0] = 1.0;
        let seconds = observation.source_timestamp_nanos.div_euclid(1_000_000_000);
        let day = 86_400_i128;
        let week = day * 7;
        let day_angle = std::f64::consts::TAU * seconds.rem_euclid(day) as f64 / day as f64;
        let week_angle = std::f64::consts::TAU * seconds.rem_euclid(week) as f64 / week as f64;
        row[1] = day_angle.sin() as f32;
        row[2] = day_angle.cos() as f32;
        row[3] = week_angle.sin() as f32;
        row[4] = week_angle.cos() as f32;
        row[5] = f32::from(observation.source_lifecycle.is_some());
        row[6] = f32::from(observation.target_lifecycle.is_some());
        row[7] = f32::from(observation.source_activity == observation.target_activity);
        let degree = graph.offsets[index + 1] - graph.offsets[index];
        row[8] = ((degree + 1) as f32).ln() / ((request.maximum_neighbors + 1) as f32).ln();

        let source_resource = attribute_token(
            observation
                .source_attributes
                .get(&request.resource_attribute),
        );
        let target_resource = attribute_token(
            observation
                .target_attributes
                .get(&request.resource_attribute),
        );
        let tokens = [
            format!("object:{}", observation.object_type),
            format!("source:{}", observation.source_activity),
            format!("target:{}", observation.target_activity),
            format!(
                "transition:{}\u{1f}{}",
                observation.source_activity, observation.target_activity
            ),
            format!("source_resource:{source_resource}"),
            format!("target_resource:{target_resource}"),
        ];
        let scale = 1.0_f32 / (tokens.len() as f32).sqrt();
        for token in tokens {
            let hash = fnv1a(token.as_bytes());
            let bucket = HASH_FEATURE_OFFSET + hash as usize % (dimensions - HASH_FEATURE_OFFSET);
            let sign = if hash & (1_u64 << 63) == 0 { 1.0 } else { -1.0 };
            row[bucket] += sign * scale;
        }
    }
    output
}

fn attribute_token(value: Option<&AttributeValue>) -> String {
    match value {
        Some(AttributeValue::Null) | None => "missing".to_owned(),
        Some(AttributeValue::String(value)) => value.clone(),
        Some(AttributeValue::Integer(value)) => value.to_string(),
        Some(AttributeValue::Float(value)) => format!("{value:.8e}"),
        Some(AttributeValue::Boolean(value)) => value.to_string(),
        Some(AttributeValue::Timestamp(value)) => value.epoch_nanos_utc.to_string(),
    }
}

fn fnv1a(value: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn train_model(
    graph: &Graph,
    labels: &[f32],
    training_count: usize,
    request: &GnnBottleneckRequest,
) -> (GnnModelWeights, GnnTrainingDiagnostics, Vec<String>) {
    let mut model = initialize_weights(
        graph.feature_dimensions,
        request.hidden_dimensions,
        request.seed,
    );
    let positives = labels[..training_count]
        .iter()
        .filter(|value| **value > 0.5)
        .count();
    let negatives = training_count - positives;
    let mut warnings = Vec::new();
    if positives == 0 || negatives == 0 {
        let probability =
            ((positives as f32 + 0.5) / (training_count as f32 + 1.0)).clamp(1.0e-6, 1.0 - 1.0e-6);
        zero_weights(&mut model);
        model.output_bias = (probability / (1.0 - probability)).ln();
        let outputs = forward(graph, &model, request.hidden_dimensions).probabilities;
        let diagnostics = diagnostics(labels, &outputs, training_count, positives, 0, false);
        warnings.push(
            "the temporal training partition contained only one label class; an intercept-only smoothed model was fitted"
                .to_owned(),
        );
        return (model, diagnostics, warnings);
    }

    let positive_weight = (negatives as f32 / positives as f32).clamp(1.0, 100.0);
    let mut adam = AdamState::new(&model);
    let mut best_model = model.clone();
    let mut best_validation_loss = f64::INFINITY;
    let mut stale_epochs = 0_u64;
    let mut completed = 0_u64;
    let mut early_stopped = false;
    for epoch in 1..=request.epochs {
        let pass = forward(graph, &model, request.hidden_dimensions);
        let gradients = backward(
            graph,
            &model,
            &pass,
            labels,
            training_count,
            positive_weight,
            request.l2 as f32,
        );
        adam.update(&mut model, &gradients, epoch, request.learning_rate as f32);
        completed = epoch;
        let current = forward(graph, &model, request.hidden_dimensions).probabilities;
        let validation_loss = if training_count < labels.len() {
            binary_loss(&current[training_count..], &labels[training_count..])
        } else {
            binary_loss(&current[..training_count], &labels[..training_count])
        };
        if validation_loss + 1.0e-8 < best_validation_loss {
            best_validation_loss = validation_loss;
            best_model = model.clone();
            stale_epochs = 0;
        } else {
            stale_epochs += 1;
            if stale_epochs >= request.patience {
                early_stopped = true;
                break;
            }
        }
    }
    model = best_model;
    let outputs = forward(graph, &model, request.hidden_dimensions).probabilities;
    (
        model,
        diagnostics(
            labels,
            &outputs,
            training_count,
            positives,
            completed,
            early_stopped,
        ),
        warnings,
    )
}

fn diagnostics(
    labels: &[f32],
    probabilities: &[f32],
    training_count: usize,
    positives: usize,
    epochs_completed: u64,
    early_stopped: bool,
) -> GnnTrainingDiagnostics {
    let validation = if training_count < labels.len() {
        training_count..labels.len()
    } else {
        0..training_count
    };
    let correct = validation
        .clone()
        .filter(|index| (probabilities[*index] >= 0.5) == (labels[*index] >= 0.5))
        .count();
    GnnTrainingDiagnostics {
        training_count: training_count as u64,
        validation_count: validation.len() as u64,
        positive_training_count: positives as u64,
        epochs_completed,
        training_loss: binary_loss(&probabilities[..training_count], &labels[..training_count]),
        validation_loss: binary_loss(
            &probabilities[validation.clone()],
            &labels[validation.clone()],
        ),
        validation_auc: auc(
            &probabilities[validation.clone()],
            &labels[validation.clone()],
        ),
        validation_accuracy: correct as f64 / validation.len().max(1) as f64,
        early_stopped,
    }
}

fn binary_loss(probabilities: &[f32], labels: &[f32]) -> f64 {
    if probabilities.is_empty() {
        return 0.0;
    }
    probabilities
        .iter()
        .zip(labels)
        .map(|(probability, label)| {
            let value = probability.clamp(1.0e-7, 1.0 - 1.0e-7);
            -f64::from(*label * value.ln() + (1.0 - *label) * (1.0 - value).ln())
        })
        .sum::<f64>()
        / probabilities.len() as f64
}

fn auc(probabilities: &[f32], labels: &[f32]) -> Option<f64> {
    let positives = labels.iter().filter(|value| **value > 0.5).count();
    let negatives = labels.len().saturating_sub(positives);
    if positives == 0 || negatives == 0 {
        return None;
    }
    let mut rows = probabilities
        .iter()
        .copied()
        .zip(labels.iter().copied())
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.total_cmp(&right.0));
    let mut positive_rank_sum = 0.0_f64;
    let mut start = 0;
    while start < rows.len() {
        let mut end = start + 1;
        while end < rows.len() && rows[end].0 == rows[start].0 {
            end += 1;
        }
        let average_rank = (start + 1 + end) as f64 / 2.0;
        positive_rank_sum +=
            rows[start..end].iter().filter(|row| row.1 > 0.5).count() as f64 * average_rank;
        start = end;
    }
    Some(
        (positive_rank_sum - (positives * (positives + 1) / 2) as f64)
            / (positives * negatives) as f64,
    )
}

#[derive(Clone)]
struct ForwardPass {
    first: Vec<f32>,
    first_neighbor: Vec<f32>,
    second: Vec<f32>,
    probabilities: Vec<f32>,
}

fn forward(graph: &Graph, weights: &GnnModelWeights, hidden: usize) -> ForwardPass {
    let nodes = graph.offsets.len() - 1;
    let features = graph.feature_dimensions;
    let mut first = vec![0.0_f32; nodes * hidden];
    for node in 0..nodes {
        for output in 0..hidden {
            let mut value = weights.first_bias[output];
            for input in 0..features {
                value += graph.features[node * features + input]
                    * weights.first_self[input * hidden + output];
                value += graph.neighbor_features[node * features + input]
                    * weights.first_neighbor[input * hidden + output];
            }
            first[node * hidden + output] = value.tanh();
        }
    }
    let first_neighbor = neighbor_means(graph, &first, hidden);
    let mut second = vec![0.0_f32; nodes * hidden];
    let mut probabilities = vec![0.0_f32; nodes];
    for node in 0..nodes {
        let mut logit = weights.output_bias;
        for output in 0..hidden {
            let mut value = weights.second_bias[output];
            for input in 0..hidden {
                value +=
                    first[node * hidden + input] * weights.second_self[input * hidden + output];
                value += first_neighbor[node * hidden + input]
                    * weights.second_neighbor[input * hidden + output];
            }
            let activation = value.tanh();
            second[node * hidden + output] = activation;
            logit += activation * weights.output[output];
        }
        probabilities[node] = sigmoid(logit);
    }
    ForwardPass {
        first,
        first_neighbor,
        second,
        probabilities,
    }
}

fn neighbor_means(graph: &Graph, values: &[f32], dimensions: usize) -> Vec<f32> {
    let nodes = graph.offsets.len() - 1;
    let mut output = vec![0.0_f32; nodes * dimensions];
    for node in 0..nodes {
        let neighbors = &graph.neighbors[graph.offsets[node]..graph.offsets[node + 1]];
        if neighbors.is_empty() {
            continue;
        }
        let scale = 1.0 / neighbors.len() as f32;
        for &neighbor in neighbors {
            for dimension in 0..dimensions {
                output[node * dimensions + dimension] +=
                    values[neighbor * dimensions + dimension] * scale;
            }
        }
    }
    output
}

fn sigmoid(value: f32) -> f32 {
    if value >= 0.0 {
        1.0 / (1.0 + (-value).exp())
    } else {
        let exponent = value.exp();
        exponent / (1.0 + exponent)
    }
}

struct Gradients {
    first_self: Vec<f32>,
    first_neighbor: Vec<f32>,
    first_bias: Vec<f32>,
    second_self: Vec<f32>,
    second_neighbor: Vec<f32>,
    second_bias: Vec<f32>,
    output: Vec<f32>,
    output_bias: f32,
}

fn backward(
    graph: &Graph,
    weights: &GnnModelWeights,
    pass: &ForwardPass,
    labels: &[f32],
    training_count: usize,
    positive_weight: f32,
    l2: f32,
) -> Gradients {
    let nodes = graph.offsets.len() - 1;
    let features = graph.feature_dimensions;
    let hidden = weights.output.len();
    let normalization = labels[..training_count]
        .iter()
        .map(|label| if *label > 0.5 { positive_weight } else { 1.0 })
        .sum::<f32>()
        .max(1.0);
    let mut gradients = Gradients {
        first_self: weights.first_self.iter().map(|value| l2 * value).collect(),
        first_neighbor: weights
            .first_neighbor
            .iter()
            .map(|value| l2 * value)
            .collect(),
        first_bias: vec![0.0; hidden],
        second_self: weights.second_self.iter().map(|value| l2 * value).collect(),
        second_neighbor: weights
            .second_neighbor
            .iter()
            .map(|value| l2 * value)
            .collect(),
        second_bias: vec![0.0; hidden],
        output: weights.output.iter().map(|value| l2 * value).collect(),
        output_bias: 0.0,
    };
    let mut second_delta = vec![0.0_f32; nodes * hidden];
    for node in 0..training_count {
        let class_weight = if labels[node] > 0.5 {
            positive_weight
        } else {
            1.0
        };
        let logit_delta = (pass.probabilities[node] - labels[node]) * class_weight / normalization;
        gradients.output_bias += logit_delta;
        for output in 0..hidden {
            gradients.output[output] += pass.second[node * hidden + output] * logit_delta;
            second_delta[node * hidden + output] = logit_delta
                * weights.output[output]
                * (1.0 - pass.second[node * hidden + output].powi(2));
        }
    }

    let mut first_delta = vec![0.0_f32; nodes * hidden];
    let mut first_neighbor_delta = vec![0.0_f32; nodes * hidden];
    for node in 0..nodes {
        for output in 0..hidden {
            let delta = second_delta[node * hidden + output];
            gradients.second_bias[output] += delta;
            for input in 0..hidden {
                gradients.second_self[input * hidden + output] +=
                    pass.first[node * hidden + input] * delta;
                gradients.second_neighbor[input * hidden + output] +=
                    pass.first_neighbor[node * hidden + input] * delta;
                first_delta[node * hidden + input] +=
                    weights.second_self[input * hidden + output] * delta;
                first_neighbor_delta[node * hidden + input] +=
                    weights.second_neighbor[input * hidden + output] * delta;
            }
        }
    }
    distribute_neighbor_gradients(graph, &first_neighbor_delta, &mut first_delta, hidden);
    for node in 0..nodes {
        for output in 0..hidden {
            first_delta[node * hidden + output] *= 1.0 - pass.first[node * hidden + output].powi(2);
            let delta = first_delta[node * hidden + output];
            gradients.first_bias[output] += delta;
            for input in 0..features {
                gradients.first_self[input * hidden + output] +=
                    graph.features[node * features + input] * delta;
                gradients.first_neighbor[input * hidden + output] +=
                    graph.neighbor_features[node * features + input] * delta;
            }
        }
    }
    gradients
}

fn distribute_neighbor_gradients(
    graph: &Graph,
    source: &[f32],
    target: &mut [f32],
    dimensions: usize,
) {
    for node in 0..graph.offsets.len() - 1 {
        let neighbors = &graph.neighbors[graph.offsets[node]..graph.offsets[node + 1]];
        if neighbors.is_empty() {
            continue;
        }
        let scale = 1.0 / neighbors.len() as f32;
        for &neighbor in neighbors {
            for dimension in 0..dimensions {
                target[neighbor * dimensions + dimension] +=
                    source[node * dimensions + dimension] * scale;
            }
        }
    }
}

#[derive(Clone)]
struct AdamVector {
    first: Vec<f32>,
    second: Vec<f32>,
}

impl AdamVector {
    fn new(length: usize) -> Self {
        Self {
            first: vec![0.0; length],
            second: vec![0.0; length],
        }
    }

    fn update(&mut self, values: &mut [f32], gradients: &[f32], epoch: u64, rate: f32) {
        let correction_first = 1.0 - 0.9_f32.powi(epoch.min(i32::MAX as u64) as i32);
        let correction_second = 1.0 - 0.999_f32.powi(epoch.min(i32::MAX as u64) as i32);
        for (((value, gradient), first), second) in values
            .iter_mut()
            .zip(gradients)
            .zip(&mut self.first)
            .zip(&mut self.second)
        {
            *first = 0.9 * *first + 0.1 * *gradient;
            *second = 0.999 * *second + 0.001 * *gradient * *gradient;
            let first_hat = *first / correction_first;
            let second_hat = *second / correction_second;
            *value -= rate * first_hat / (second_hat.sqrt() + 1.0e-8);
        }
    }
}

struct AdamState {
    first_self: AdamVector,
    first_neighbor: AdamVector,
    first_bias: AdamVector,
    second_self: AdamVector,
    second_neighbor: AdamVector,
    second_bias: AdamVector,
    output: AdamVector,
    output_bias: AdamVector,
}

impl AdamState {
    fn new(weights: &GnnModelWeights) -> Self {
        Self {
            first_self: AdamVector::new(weights.first_self.len()),
            first_neighbor: AdamVector::new(weights.first_neighbor.len()),
            first_bias: AdamVector::new(weights.first_bias.len()),
            second_self: AdamVector::new(weights.second_self.len()),
            second_neighbor: AdamVector::new(weights.second_neighbor.len()),
            second_bias: AdamVector::new(weights.second_bias.len()),
            output: AdamVector::new(weights.output.len()),
            output_bias: AdamVector::new(1),
        }
    }

    fn update(
        &mut self,
        weights: &mut GnnModelWeights,
        gradients: &Gradients,
        epoch: u64,
        rate: f32,
    ) {
        self.first_self
            .update(&mut weights.first_self, &gradients.first_self, epoch, rate);
        self.first_neighbor.update(
            &mut weights.first_neighbor,
            &gradients.first_neighbor,
            epoch,
            rate,
        );
        self.first_bias
            .update(&mut weights.first_bias, &gradients.first_bias, epoch, rate);
        self.second_self.update(
            &mut weights.second_self,
            &gradients.second_self,
            epoch,
            rate,
        );
        self.second_neighbor.update(
            &mut weights.second_neighbor,
            &gradients.second_neighbor,
            epoch,
            rate,
        );
        self.second_bias.update(
            &mut weights.second_bias,
            &gradients.second_bias,
            epoch,
            rate,
        );
        self.output
            .update(&mut weights.output, &gradients.output, epoch, rate);
        let mut bias = [weights.output_bias];
        self.output_bias
            .update(&mut bias, &[gradients.output_bias], epoch, rate);
        weights.output_bias = bias[0];
    }
}

fn initialize_weights(features: usize, hidden: usize, seed: u64) -> GnnModelWeights {
    let mut random = XorShift64::new(seed);
    GnnModelWeights {
        first_self: xavier(&mut random, features * hidden, features, hidden),
        first_neighbor: xavier(&mut random, features * hidden, features, hidden),
        first_bias: vec![0.0; hidden],
        second_self: xavier(&mut random, hidden * hidden, hidden, hidden),
        second_neighbor: xavier(&mut random, hidden * hidden, hidden, hidden),
        second_bias: vec![0.0; hidden],
        output: xavier(&mut random, hidden, hidden, 1),
        output_bias: 0.0,
    }
}

fn xavier(random: &mut XorShift64, length: usize, fan_in: usize, fan_out: usize) -> Vec<f32> {
    let limit = (6.0_f32 / (fan_in + fan_out) as f32).sqrt();
    (0..length)
        .map(|_| (random.unit() * 2.0 - 1.0) * limit)
        .collect()
}

fn zero_weights(weights: &mut GnnModelWeights) {
    weights.first_self.fill(0.0);
    weights.first_neighbor.fill(0.0);
    weights.first_bias.fill(0.0);
    weights.second_self.fill(0.0);
    weights.second_neighbor.fill(0.0);
    weights.second_bias.fill(0.0);
    weights.output.fill(0.0);
}

struct XorShift64(u64);

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 { default_seed() } else { seed })
    }

    fn unit(&mut self) -> f32 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        (value >> 40) as f32 / (1_u32 << 24) as f32
    }
}

#[derive(Default)]
struct SignalAccumulator {
    support: u64,
    threshold_seconds: f64,
    risk_sum: f64,
    maximum_risk: f64,
    predicted_slow_count: u64,
    observed_slow_count: u64,
    risk_weighted_excess_seconds: f64,
}

impl SignalAccumulator {
    fn push(&mut self, risk: f64, duration: f64, threshold: f64) {
        self.support += 1;
        self.threshold_seconds = threshold;
        self.risk_sum += risk;
        self.maximum_risk = self.maximum_risk.max(risk);
        self.predicted_slow_count += u64::from(risk >= 0.5);
        self.observed_slow_count += u64::from(duration > threshold);
        self.risk_weighted_excess_seconds += risk * (duration - threshold).max(0.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observation(
        index: u64,
        object: u64,
        duration: u64,
        target_event: u64,
    ) -> BottleneckObservation {
        BottleneckObservation {
            object_id: object,
            object_type: if object % 2 == 0 { "item" } else { "order" }.to_owned(),
            source_event_id: target_event - 1,
            source_activity: if index % 2 == 0 { "approve" } else { "create" }.to_owned(),
            source_timestamp_nanos: i128::from(index) * 100_000_000_000,
            source_lifecycle: Some("complete".to_owned()),
            source_attributes: BTreeMap::from([(
                "org:resource".to_owned(),
                AttributeValue::String(format!("r{}", object % 3)),
            )]),
            target_event_id: target_event,
            target_activity: "ship".to_owned(),
            target_timestamp_nanos: i128::from(index) * 100_000_000_000
                + i128::from(duration) * 1_000_000_000,
            target_lifecycle: Some("start".to_owned()),
            target_attributes: BTreeMap::new(),
        }
    }

    fn fixture() -> Vec<BottleneckObservation> {
        (1..=30)
            .map(|index| {
                let duration = if index % 7 == 0 { 80 } else { 2 + index % 5 };
                observation(index, 1 + index % 4, duration, 1_000 + index / 2)
            })
            .collect()
    }

    fn request() -> GnnBottleneckRequest {
        GnnBottleneckRequest {
            minimum_support: 2,
            epochs: 20,
            patience: 5,
            ..GnnBottleneckRequest::default()
        }
    }

    #[test]
    fn fitting_is_deterministic_and_hash_verified() {
        let first = fit_observations(&fixture(), &request()).unwrap();
        let second = fit_observations(&fixture(), &request()).unwrap();
        assert_eq!(first, second);
        validate_artifact(&first).unwrap();
        let mut damaged = first;
        damaged.weights.output_bias += 1.0;
        assert_eq!(
            validate_artifact(&damaged).unwrap_err().code,
            OcpmErrorCode::ArtifactIncompatible
        );
    }

    #[test]
    fn duration_is_not_an_input_feature() {
        let mut values = fixture();
        let first = build_graph(&values, &request()).unwrap().features;
        values[0].target_timestamp_nanos += 10_000_000_000_000;
        let second = build_graph(&values, &request()).unwrap().features;
        assert_eq!(first, second);
    }

    #[test]
    fn graph_connects_object_and_shared_event_context_with_a_hard_cap() {
        let mut configured = request();
        configured.maximum_neighbors = 2;
        let graph = build_graph(&fixture(), &configured).unwrap();
        assert!(graph.maximum_degree <= 2);
        assert!(!graph.neighbors.is_empty());
    }

    #[test]
    fn temporal_holdout_is_reported_separately() {
        let artifact = fit_observations(&fixture(), &request()).unwrap();
        assert_eq!(artifact.training.training_count, 24);
        assert_eq!(artifact.training.validation_count, 6);
        assert!(artifact.training.training_loss.is_finite());
        assert!(artifact.training.validation_loss.is_finite());
    }

    #[test]
    fn resource_limit_fails_closed_before_graph_allocation() {
        let mut configured = request();
        configured.maximum_nodes = 10;
        let error = fit_observations(&fixture(), &configured).unwrap_err();
        assert_eq!(error.code, OcpmErrorCode::ResourceLimit);
        assert_eq!(error.limit, Some(10));
    }

    #[test]
    fn score_ranking_is_stable() {
        let values = fixture();
        let configured = request();
        let artifact = fit_observations(&values, &configured).unwrap();
        let result = score_observations("fixture", &values, &configured, &artifact).unwrap();
        assert_eq!(result.diagnostics.provider, "fixture");
        assert_eq!(result.diagnostics.observation_count, 30);
        assert!(!result.signals.is_empty());
        assert!(result.signals.windows(2).all(
            |pair| pair[0].risk_weighted_excess_seconds >= pair[1].risk_weighted_excess_seconds
        ));
    }
}
