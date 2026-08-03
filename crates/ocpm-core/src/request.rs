use crate::{AttributeValue, EventId, ModelArtifact, ObjectId, Timestamp};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DatasetView {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<Timestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<Timestamp>,
    #[serde(default)]
    pub object_types: Vec<String>,
    #[serde(default)]
    pub activities: Vec<String>,
    #[serde(default)]
    pub qualifiers: Vec<String>,
    #[serde(default)]
    pub event_ids: Vec<EventId>,
    #[serde(default)]
    pub object_ids: Vec<ObjectId>,
    #[serde(default)]
    pub event_attributes: BTreeMap<String, AttributeValue>,
    #[serde(default)]
    pub object_attributes: BTreeMap<String, AttributeValue>,
    #[serde(default)]
    pub statuses: Vec<String>,
    #[serde(default)]
    pub related_object_types: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub minimum_execution_duration_nanos: Option<i128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maximum_execution_duration_nanos: Option<i128>,
}

impl DatasetView {
    pub fn contains_timestamp(&self, timestamp: &Timestamp) -> bool {
        self.start.as_ref().is_none_or(|start| timestamp >= start)
            && self.end.as_ref().is_none_or(|end| timestamp < end)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Constraint {
    EventId { event_ids: Vec<EventId> },
    ObjectId { object_ids: Vec<ObjectId> },
    EventActivity { activities: Vec<String> },
    EventAttributeEquals { name: String, value: AttributeValue },
    ObjectAttributeEquals { name: String, value: AttributeValue },
    ObjectType { object_types: Vec<String> },
    E2oQualifier { qualifiers: Vec<String> },
    O2oQualifier { qualifiers: Vec<String> },
    DirectlyFollows { source: String, target: String },
    EventuallyFollows { source: String, target: String },
    TemporalDistance {
        source: String,
        target: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        minimum_nanos: Option<i128>,
        #[serde(skip_serializing_if = "Option::is_none")]
        maximum_nanos: Option<i128>,
    },
    Relationship {
        #[serde(skip_serializing_if = "Option::is_none")]
        source_object_type: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        target_object_type: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        qualifier: Option<String>,
    },
    ChildCount { child: Box<Constraint>, minimum: u64, maximum: Option<u64> },
    And { children: Vec<Constraint> },
    Or { children: Vec<Constraint> },
    Not { child: Box<Constraint> },
    Label { name: String, child: Box<Constraint> },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryRequest {
    pub semantic_version: String,
    pub view: DatasetView,
    pub constraint: Constraint,
    #[serde(default = "default_result_limit")]
    pub limit: u64,
}

fn default_result_limit() -> u64 {
    100_000
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryAlgorithm {
    Dfg,
    OcDfg,
    Alpha,
    InductiveProcessTree,
    PetriNet,
    ObjectCentricPetriNet,
    OcDeclare,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryRequest {
    pub semantic_version: String,
    pub view: DatasetView,
    pub algorithm: DiscoveryAlgorithm,
    #[serde(default = "default_algorithm_version")]
    pub algorithm_version: String,
    #[serde(default)]
    pub parameters: BTreeMap<String, serde_json::Value>,
}

fn default_algorithm_version() -> String {
    "1".to_owned()
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConformanceMethod {
    FrequencyCoverage,
    TokenReplay,
    Alignment,
    OcpnQuality,
    OcDeclare,
    Constraints,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConformanceRequest {
    pub semantic_version: String,
    pub view: DatasetView,
    pub method: ConformanceMethod,
    pub model: ModelArtifact,
    #[serde(default)]
    pub parameters: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnhancementKind {
    ProcessMap,
    Timeline,
    Histogram,
    Performance,
    Rework,
    Organizational,
    WindowComparison,
    Drift,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EnhancementRequest {
    pub semantic_version: String,
    pub view: DatasetView,
    pub comparison_view: Option<DatasetView>,
    pub kind: EnhancementKind,
    #[serde(default)]
    pub parameters: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PredictionTarget {
    NextActivity,
    RemainingTime,
    Outcome,
    Risk,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PredictionState {
    #[serde(default)]
    pub event_ids: Vec<EventId>,
    #[serde(default)]
    pub object_ids: Vec<ObjectId>,
    #[serde(default)]
    pub activities: Vec<String>,
    pub as_of: Timestamp,
    #[serde(default)]
    pub attributes: BTreeMap<String, AttributeValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PredictionRequest {
    pub semantic_version: String,
    pub view: DatasetView,
    pub target: PredictionTarget,
    pub state: PredictionState,
    #[serde(default)]
    pub model_artifact: Option<serde_json::Value>,
    #[serde(default)]
    pub parameters: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FitPredictionRequest {
    pub semantic_version: String,
    pub view: DatasetView,
    pub target: PredictionTarget,
    #[serde(default)]
    pub parameters: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub seed: u64,
}
