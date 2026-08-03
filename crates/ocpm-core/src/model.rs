use crate::{Timestamp, content_hash};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DfgEdge {
    pub source: String,
    pub target: String,
    pub object_type: String,
    pub frequency: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DfgModel {
    pub activities: Vec<String>,
    pub start_activities: BTreeMap<String, u64>,
    pub end_activities: BTreeMap<String, u64>,
    pub edges: Vec<DfgEdge>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ProcessTree {
    Activity(String),
    Sequence(Vec<ProcessTree>),
    Exclusive(Vec<ProcessTree>),
    Parallel(Vec<ProcessTree>),
    Loop {
        body: Box<ProcessTree>,
        redo: Box<ProcessTree>,
    },
    Tau,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Place {
    pub id: String,
    pub initial_tokens: u64,
    pub final_tokens: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Transition {
    pub id: String,
    pub label: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PetriArc {
    pub source: String,
    pub target: String,
    pub weight: u64,
    pub object_type: Option<String>,
    pub variable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PetriNet {
    pub places: Vec<Place>,
    pub transitions: Vec<Transition>,
    pub arcs: Vec<PetriArc>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectCentricPetriNet {
    pub object_types: Vec<String>,
    pub net: PetriNet,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeclareTemplate {
    Existence,
    Absence,
    Exactly,
    Init,
    End,
    Response,
    Precedence,
    Succession,
    Coexistence,
    NotCoexistence,
    Choice,
    ExclusiveChoice,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeclareConstraint {
    pub template: DeclareTemplate,
    pub activation: String,
    pub target: Option<String>,
    pub object_type: Option<String>,
    pub support: f64,
    pub confidence: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "model", rename_all = "snake_case")]
pub enum ProcessModel {
    Dfg(DfgModel),
    ProcessTree(ProcessTree),
    PetriNet(PetriNet),
    ObjectCentricPetriNet(ObjectCentricPetriNet),
    OcDeclare(Vec<DeclareConstraint>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelArtifact {
    pub schema_version: String,
    pub algorithm: String,
    pub algorithm_version: String,
    pub parameters: BTreeMap<String, serde_json::Value>,
    pub dataset_id: String,
    pub source_watermark: Option<Timestamp>,
    pub model: ProcessModel,
    pub content_hash: String,
}

impl ModelArtifact {
    pub fn finalize_hash(&mut self) -> crate::OcpmResult<()> {
        self.content_hash.clear();
        self.content_hash = content_hash(self)?;
        Ok(())
    }
}
