use crate::{EventId, ObjectId, ProcessModel, Timestamp};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DatasetProfile {
    pub dataset_id: String,
    pub tenant_id: String,
    pub source_watermark: Option<Timestamp>,
    pub event_count: u64,
    pub object_count: u64,
    pub e2o_count: u64,
    pub o2o_count: u64,
    pub object_attribute_change_count: u64,
    pub activities: BTreeMap<String, u64>,
    pub object_types: BTreeMap<String, u64>,
    pub start: Option<Timestamp>,
    pub end: Option<Timestamp>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryBinding {
    pub event_ids: Vec<EventId>,
    pub object_ids: Vec<ObjectId>,
    pub labels: Vec<String>,
    pub violated: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryResult {
    pub bindings: Vec<QueryBinding>,
    pub total_matches: u64,
    pub truncated: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ConformanceResultV1 {
    pub fitness: Option<f64>,
    pub precision: Option<f64>,
    pub generalization: Option<f64>,
    pub simplicity: Option<f64>,
    pub conforming: u64,
    pub deviations: u64,
    pub exact: bool,
    pub violations: Vec<QueryBinding>,
    pub diagnostics: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PerformanceMetric {
    pub name: String,
    pub unit: String,
    pub support: u64,
    pub mean: Option<f64>,
    pub minimum: Option<f64>,
    pub maximum: Option<f64>,
    pub median: Option<f64>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EnhancementResult {
    pub metrics: Vec<PerformanceMetric>,
    pub groups: BTreeMap<String, serde_json::Value>,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PredictionCandidate {
    pub label: String,
    pub probability: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PredictionResult {
    pub candidates: Vec<PredictionCandidate>,
    pub point_estimate_seconds: Option<f64>,
    pub interval_seconds: Option<(f64, f64)>,
    pub backoff_level: String,
    pub support: u64,
    pub model_hash: Option<String>,
    pub diagnostics: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionStep {
    pub operator: String,
    pub provider: String,
    pub estimated_rows: u64,
    pub estimated_bytes: u64,
    pub pushed_predicates: Vec<String>,
    pub fallback_reason: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub semantic_version: String,
    pub steps: Vec<ExecutionStep>,
    pub estimated_total_ns: u64,
    pub estimated_peak_memory_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum EngineResult {
    Query(QueryResult),
    Model(ProcessModel),
    Conformance(ConformanceResultV1),
    Enhancement(EnhancementResult),
    Prediction(PredictionResult),
}
