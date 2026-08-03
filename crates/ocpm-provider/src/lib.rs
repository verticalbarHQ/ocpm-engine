//! Source-neutral provider contract.
//!
//! PROVENANCE: data semantics follow doi:10.1007/978-3-030-85082-1_16;
//! process executions follow doi:10.1109/ICPM57379.2022.9980730. No library
//! implementation source was consulted.

use ocpm_core::{
    CanonicalLog, DatasetProfile, DatasetView, Event, ObjectId, OcpmError, OcpmErrorCode,
    OcpmResult, QueryRequest, QueryResult,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCapability {
    CanonicalScan,
    ProcessExecutions,
    ObjectCentricQuery,
    DfgAggregate,
    VariantAggregate,
    PerformanceAggregate,
    PredictionFeatures,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    LeadingObject,
    ConnectedComponent,
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

pub trait OcpmProvider: Send + Sync {
    fn name(&self) -> &'static str;
    fn semantic_version(&self) -> &'static str {
        "1.0"
    }
    fn capabilities(&self) -> Vec<ProviderCapability>;
    fn profile(&self, view: &DatasetView) -> OcpmResult<DatasetProfile>;
    fn process_executions(
        &self,
        view: &DatasetView,
        mode: ExecutionMode,
        leading_object_type: Option<&str>,
    ) -> OcpmResult<Vec<ProcessExecution>>;
    fn query(&self, request: &QueryRequest) -> OcpmResult<QueryResult>;
    fn snapshot(&self, _view: &DatasetView) -> OcpmResult<CanonicalLog> {
        Err(OcpmError::new(
            OcpmErrorCode::ProviderUnavailable,
            "provider does not expose canonical snapshot export",
        ))
    }
    fn estimate(&self, view: &DatasetView, operation: ProviderCapability) -> ProviderEstimate;
}
