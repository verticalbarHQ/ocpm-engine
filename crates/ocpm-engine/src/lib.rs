//! Standalone, provider-neutral object-centric process-mining facade.
//!
//! `Engine` owns the public contracts. PostgreSQL is an optional provider: a
//! local canonical log remains fully functional, while pg_ocpm can push scans
//! and aggregates next to stored data. Algorithm provenance is maintained by
//! each kernel crate and summarized in `docs/academic-implementation-provenance.md`.

use ocpm_core::{
    AppendBatch, CanonicalLog, ConformanceRequest, ConformanceResultV1, DatasetProfile,
    DatasetView, DiscoveryRequest, EnhancementRequest, EnhancementResult, ExecutionPlan,
    ExecutionStep, FitPredictionRequest, ModelArtifact, OcpmResult, PredictionRequest,
    PredictionResult, QueryRequest, QueryResult,
    event_batch::{EventLogSummary, EventSummaryBuilder},
};
use ocpm_local::LocalProvider;
use ocpm_prediction::PredictionArtifact;
use ocpm_provider::{ExecutionSummaryRequest, OcpmProvider, ProviderCapability};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

pub use ocpm_bottleneck::{
    AvailabilityInterval, BlockingCascade, BottleneckChange, BottleneckRequest, BottleneckResult,
    BottleneckSignal, CausalHypothesisResult, PerformancePattern, ResourcePressure,
    SynchronizationAttribution, TemporalHypothesis, WaitingCauseAttribution,
};
#[cfg(feature = "duckdb")]
pub use ocpm_duckdb::{
    DuckDbOptions, DuckDbParquetSource, DuckDbProvider, EntityLinkSnapshotV1, ExtensionPolicy,
    ParquetCachePolicy, ParquetLayout, ParquetLocation, S3CredentialReference, SnapshotSelection,
    SnapshotWriteResult, SourceTimestampPolicy, SourceValidationPolicy,
};
pub use ocpm_io::CsvMapping;
pub use ocpm_provider::{
    CapabilityCoverage, CapabilityReport, ExecutionCancellation, ExecutionContext, ExecutionMode,
    PopulationSelector, ProcessExecution,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PopulationBottleneckRequest {
    pub request: BottleneckRequest,
    pub population: PopulationSelector,
}

impl PopulationBottleneckRequest {
    pub fn new(request: BottleneckRequest, population: PopulationSelector) -> Self {
        Self {
            request,
            population,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PopulationSummary {
    pub selector: PopulationSelector,
    pub object_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct UnsupportedMetricFamily {
    pub family: String,
    pub reason_code: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PopulationBottleneckResult {
    pub result: BottleneckResult,
    pub population: PopulationSummary,
    pub capabilities: CapabilityReport,
    pub unsupported: Vec<UnsupportedMetricFamily>,
}

#[cfg(feature = "gnn")]
pub use ocpm_gnn::{
    GnnArtifact, GnnBackend, GnnBottleneckArtifact, GnnBottleneckDiagnostics, GnnBottleneckRequest,
    GnnBottleneckResult, GnnBottleneckSignal, GnnEdgeThreshold, GnnModelWeights, GnnRequest,
    GnnTask, GnnTrainingDiagnostics,
};

pub struct Engine {
    provider: Arc<dyn OcpmProvider>,
}

fn push_unsupported(
    values: &mut Vec<UnsupportedMetricFamily>,
    family: &str,
    requirements: &[&CapabilityCoverage],
) {
    if let Some(coverage) = requirements
        .iter()
        .copied()
        .find(|coverage| !coverage.available)
    {
        values.push(UnsupportedMetricFamily {
            family: family.to_owned(),
            reason_code: coverage
                .reason_code
                .clone()
                .unwrap_or_else(|| "capability_unavailable".to_owned()),
        });
    }
}

impl Engine {
    pub fn from_provider(provider: Arc<dyn OcpmProvider>) -> Self {
        Self { provider }
    }

    pub fn from_log(log: CanonicalLog) -> OcpmResult<Self> {
        Ok(Self::from_provider(Arc::new(LocalProvider::new(log)?)))
    }

    pub fn from_canonical_json(reader: impl std::io::Read) -> OcpmResult<Self> {
        Self::from_log(ocpm_io::read_canonical_json(reader)?)
    }

    pub fn from_ocel2_json(reader: impl std::io::Read) -> OcpmResult<Self> {
        Self::from_log(ocpm_io::read_ocel2_json(reader)?)
    }

    pub fn from_csv(reader: impl std::io::Read, mapping: &CsvMapping) -> OcpmResult<Self> {
        Self::from_log(ocpm_io::read_csv(reader, mapping)?)
    }

    pub fn from_xes(reader: impl std::io::BufRead) -> OcpmResult<Self> {
        Self::from_log(ocpm_io::read_xes(reader)?)
    }

    pub fn from_sqlite(path: impl AsRef<std::path::Path>) -> OcpmResult<Self> {
        Self::from_log(ocpm_io::read_sqlite(path)?)
    }

    #[cfg(feature = "duckdb")]
    pub fn from_duckdb_parquet(source: DuckDbParquetSource) -> OcpmResult<Self> {
        Ok(Self::from_provider(Arc::new(DuckDbProvider::open(source)?)))
    }

    #[cfg(feature = "duckdb")]
    pub fn from_duckdb_parquet_isolated(source: DuckDbParquetSource) -> OcpmResult<Self> {
        Ok(Self::from_provider(Arc::new(
            DuckDbProvider::open_isolated(source)?,
        )))
    }

    #[cfg(feature = "postgres")]
    pub async fn from_postgres_snapshot(
        client: &ocpm_postgres::PgClient,
        tenant_id: i64,
        dataset_id: i64,
    ) -> OcpmResult<Self> {
        let log = ocpm_postgres::load_canonical_snapshot(client, tenant_id, dataset_id)
            .await
            .map_err(|error| {
                ocpm_core::OcpmError::new(
                    ocpm_core::OcpmErrorCode::ProviderUnavailable,
                    format!("pg_ocpm snapshot failed: {error}"),
                )
            })?;
        Self::from_log(log)
    }

    /// Execute bottleneck analysis through pg_ocpm when its canonical
    /// transition projection can preserve every predicate. Unsupported views
    /// use the exact canonical snapshot path automatically.
    #[cfg(feature = "postgres")]
    pub async fn bottlenecks_postgres(
        client: &ocpm_postgres::PgClient,
        tenant_id: i64,
        dataset_id: i64,
        request: &BottleneckRequest,
    ) -> OcpmResult<BottleneckResult> {
        let capabilities = ocpm_postgres::pg_ocpm_capabilities(client)
            .await
            .map_err(postgres_error)?;
        if !capabilities.bottleneck_pushdown()
            || !postgres_bottleneck_view_supported(&request.view)
            || request
                .comparison_view
                .as_ref()
                .is_some_and(|view| !postgres_bottleneck_view_supported(view))
        {
            return Self::from_postgres_snapshot(client, tenant_id, dataset_id)
                .await?
                .bottlenecks(request);
        }
        let observations = ocpm_postgres::load_bottleneck_observations(
            client,
            tenant_id,
            dataset_id,
            &request.view,
            request.leading_object_type.as_deref(),
        )
        .await
        .map_err(postgres_error)?;
        let comparison = match &request.comparison_view {
            Some(view) => Some(
                ocpm_postgres::load_bottleneck_observations(
                    client,
                    tenant_id,
                    dataset_id,
                    view,
                    request.leading_object_type.as_deref(),
                )
                .await
                .map_err(postgres_error)?,
            ),
            None => None,
        };
        ocpm_bottleneck::analyze_observations(
            "pg_ocpm",
            &observations,
            comparison.as_deref(),
            request,
        )
    }

    /// Run graph-aware bottleneck detection over pg_ocpm's canonical
    /// observation projection. Unsupported predicates use the exact snapshot
    /// path, while all graph and learning semantics remain in ocpm-engine.
    #[cfg(all(feature = "postgres", feature = "gnn"))]
    pub async fn gnn_bottlenecks_postgres(
        client: &ocpm_postgres::PgClient,
        tenant_id: i64,
        dataset_id: i64,
        request: &GnnBottleneckRequest,
    ) -> OcpmResult<GnnBottleneckResult> {
        let capabilities = ocpm_postgres::pg_ocpm_capabilities(client)
            .await
            .map_err(postgres_error)?;
        if !capabilities.bottleneck_pushdown() || !postgres_bottleneck_view_supported(&request.view)
        {
            return Self::from_postgres_snapshot(client, tenant_id, dataset_id)
                .await?
                .gnn_bottlenecks(request);
        }
        let observations = ocpm_postgres::load_bottleneck_observations(
            client,
            tenant_id,
            dataset_id,
            &request.view,
            request.leading_object_type.as_deref(),
        )
        .await
        .map_err(postgres_error)?;
        ocpm_gnn::detect_from_observations("pg_ocpm", &observations, request)
    }

    pub fn provider_name(&self) -> &'static str {
        self.provider.name()
    }

    pub fn capabilities(&self) -> Vec<ProviderCapability> {
        self.provider.capabilities()
    }

    pub fn analysis_capabilities(&self) -> OcpmResult<CapabilityReport> {
        self.provider.capability_report()
    }

    pub fn profile(&self, view: &DatasetView) -> OcpmResult<DatasetProfile> {
        self.provider.profile(view)
    }

    pub fn profile_with_context(
        &self,
        view: &DatasetView,
        context: &ExecutionContext,
    ) -> OcpmResult<DatasetProfile> {
        self.provider.profile_with_context(view, context)
    }

    pub fn snapshot(&self, view: &DatasetView) -> OcpmResult<CanonicalLog> {
        self.provider.snapshot(view)
    }

    pub fn append(&self, batch: AppendBatch) -> OcpmResult<Self> {
        batch.validate()?;
        let mut log = self.snapshot(&DatasetView::default())?;
        if log
            .source_watermark
            .as_ref()
            .zip(batch.source_watermark.as_ref())
            .is_some_and(|(current, incoming)| incoming < current)
        {
            return Err(ocpm_core::OcpmError::invalid_request(
                "source_watermark must be monotonic",
            ));
        }
        log.events.extend(
            batch
                .events
                .event_id
                .into_iter()
                .zip(batch.events.external_event_id)
                .zip(batch.events.activity)
                .zip(batch.events.timestamp_nanos_utc)
                .zip(batch.events.source_timestamp)
                .zip(batch.events.sequence)
                .zip(batch.events.lifecycle)
                .zip(batch.events.attributes)
                .map(
                    |(
                        (
                            (((((id, external_id), activity), timestamp), source), sequence),
                            lifecycle,
                        ),
                        attributes,
                    )| {
                        ocpm_core::Event {
                            id,
                            external_id,
                            activity,
                            timestamp: ocpm_core::Timestamp {
                                epoch_nanos_utc: timestamp,
                                source,
                            },
                            sequence,
                            lifecycle,
                            attributes,
                        }
                    },
                ),
        );
        log.objects.extend(
            batch
                .objects
                .object_id
                .into_iter()
                .zip(batch.objects.external_object_id)
                .zip(batch.objects.object_type)
                .map(|((id, external_id), object_type)| ocpm_core::Object {
                    id,
                    external_id,
                    object_type,
                }),
        );
        let mut relation_id = log
            .event_object_relations
            .iter()
            .map(|relation| relation.relation_id)
            .chain(
                log.object_object_relations
                    .iter()
                    .map(|relation| relation.relation_id),
            )
            .max()
            .unwrap_or_default();
        for ((event_id, object_id), qualifier) in batch
            .event_object_relations
            .event_id
            .into_iter()
            .zip(batch.event_object_relations.object_id)
            .zip(batch.event_object_relations.qualifier)
        {
            relation_id = relation_id.checked_add(1).ok_or_else(|| {
                ocpm_core::OcpmError::resource_limit("relation ID overflow", u64::MAX, u64::MAX)
            })?;
            log.event_object_relations
                .push(ocpm_core::EventObjectRelation {
                    relation_id,
                    event_id,
                    object_id,
                    qualifier,
                });
        }
        for ((((source_object_id, target_object_id), qualifier), valid_from), valid_to) in batch
            .object_object_relations
            .source_object_id
            .into_iter()
            .zip(batch.object_object_relations.target_object_id)
            .zip(batch.object_object_relations.qualifier)
            .zip(batch.object_object_relations.valid_from_nanos_utc)
            .zip(batch.object_object_relations.valid_to_nanos_utc)
        {
            relation_id = relation_id.checked_add(1).ok_or_else(|| {
                ocpm_core::OcpmError::resource_limit("relation ID overflow", u64::MAX, u64::MAX)
            })?;
            log.object_object_relations
                .push(ocpm_core::ObjectObjectRelation {
                    relation_id,
                    source_object_id,
                    target_object_id,
                    qualifier,
                    valid_from: valid_from.map(ocpm_core::Timestamp::from_epoch_nanos),
                    valid_to: valid_to.map(ocpm_core::Timestamp::from_epoch_nanos),
                });
        }
        log.object_attribute_history.extend(
            batch
                .object_attribute_history
                .object_id
                .into_iter()
                .zip(batch.object_attribute_history.name)
                .zip(batch.object_attribute_history.valid_from_nanos_utc)
                .zip(batch.object_attribute_history.value)
                .map(
                    |(((object_id, name), valid_from), value)| ocpm_core::ObjectAttributeChange {
                        object_id,
                        name,
                        valid_from: ocpm_core::Timestamp::from_epoch_nanos(valid_from),
                        value,
                    },
                ),
        );
        if batch.source_watermark.is_some() {
            log.source_watermark = batch.source_watermark;
        }
        Self::from_log(log)
    }

    pub fn write_canonical_json(
        &self,
        writer: impl std::io::Write,
        view: &DatasetView,
    ) -> OcpmResult<()> {
        ocpm_io::write_canonical_json(writer, &self.snapshot(view)?)
    }

    pub fn write_ocel2_json(
        &self,
        writer: impl std::io::Write,
        view: &DatasetView,
    ) -> OcpmResult<()> {
        ocpm_io::write_ocel2_json(writer, &self.snapshot(view)?)
    }

    pub fn write_csv(
        &self,
        writer: impl std::io::Write,
        view: &DatasetView,
        mapping: &CsvMapping,
    ) -> OcpmResult<()> {
        ocpm_io::write_csv(writer, &self.snapshot(view)?, mapping)
    }

    pub fn write_xes(
        &self,
        writer: impl std::io::Write,
        view: &DatasetView,
        object_type: &str,
    ) -> OcpmResult<()> {
        ocpm_io::write_xes(writer, &self.snapshot(view)?, object_type)
    }

    pub fn write_sqlite(
        &self,
        path: impl AsRef<std::path::Path>,
        view: &DatasetView,
    ) -> OcpmResult<()> {
        ocpm_io::write_sqlite(path, &self.snapshot(view)?)
    }

    #[cfg(feature = "duckdb")]
    pub fn write_parquet_snapshot(
        &self,
        root: impl AsRef<std::path::Path>,
        version: &str,
        view: &DatasetView,
    ) -> OcpmResult<SnapshotWriteResult> {
        Ok(ocpm_duckdb::write_canonical_snapshot(
            &self.snapshot(view)?,
            root,
            version,
        )?)
    }

    pub fn execution_summary(
        &self,
        request: &ExecutionSummaryRequest,
    ) -> OcpmResult<EventLogSummary> {
        if let Some(summary) = self.provider.execution_summary(request)? {
            return Ok(summary);
        }
        let mut scan_view = request.view.clone();
        if request.complete_lifecycle {
            scan_view.start = None;
            scan_view.end = None;
        }
        let executions = self.provider.process_executions(
            &scan_view,
            ExecutionMode::LeadingObject,
            request.leading_object_type.as_deref(),
        )?;
        let mut builder = EventSummaryBuilder::new();
        for execution in executions {
            if request.complete_lifecycle
                && (execution.events.first().is_none_or(|event| {
                    request
                        .view
                        .start
                        .as_ref()
                        .is_none_or(|start| &event.timestamp < start)
                }) || execution.events.last().is_none_or(|event| {
                    request
                        .view
                        .end
                        .as_ref()
                        .is_none_or(|end| &event.timestamp >= end)
                }))
            {
                continue;
            }
            let activity_path = execution.activity_path();
            let timestamps = execution
                .events
                .iter()
                .map(|event| {
                    i64::try_from(event.timestamp.epoch_nanos_utc / 1_000).map_err(|_| {
                        ocpm_core::OcpmError::invalid_request(
                            "event timestamp is outside microsecond summary range",
                        )
                    })
                })
                .collect::<OcpmResult<Vec<_>>>()?;
            builder
                .push_case(&activity_path, &timestamps)
                .map_err(|error| {
                    ocpm_core::OcpmError::resource_limit(error.to_string(), u64::MAX, u64::MAX)
                })?;
        }
        Ok(builder.finish())
    }

    pub fn query(&self, request: &QueryRequest) -> OcpmResult<QueryResult> {
        ocpm_query::execute(self.provider.as_ref(), request)
    }

    pub fn discover(&self, request: &DiscoveryRequest) -> OcpmResult<ModelArtifact> {
        ocpm_discovery::discover(self.provider.as_ref(), request)
    }

    pub fn conformance(&self, request: &ConformanceRequest) -> OcpmResult<ConformanceResultV1> {
        ocpm_conformance::check(self.provider.as_ref(), request)
    }

    pub fn enhance(&self, request: &EnhancementRequest) -> OcpmResult<EnhancementResult> {
        ocpm_enhancement::enhance(self.provider.as_ref(), request)
    }

    /// Run the complete provider-neutral bottleneck suite. Providers may push
    /// only the canonical observation projection down to storage; analytical
    /// semantics always execute in the shared kernel.
    pub fn bottlenecks(&self, request: &BottleneckRequest) -> OcpmResult<BottleneckResult> {
        ocpm_bottleneck::analyze(self.provider.as_ref(), request)
    }

    pub fn bottlenecks_with_context(
        &self,
        request: &BottleneckRequest,
        context: &ExecutionContext,
    ) -> OcpmResult<BottleneckResult> {
        ocpm_bottleneck::analyze_with_context(self.provider.as_ref(), request, context)
    }

    /// Resolve an explicit leading-object population before running the
    /// unchanged bottleneck kernel. Legacy `bottlenecks` view semantics remain
    /// the default and are never inferred from this opt-in request.
    pub fn bottlenecks_for_population(
        &self,
        typed: &PopulationBottleneckRequest,
    ) -> OcpmResult<PopulationBottleneckResult> {
        if typed.request.view.start.is_some()
            || typed.request.view.end.is_some()
            || !typed.request.view.object_ids.is_empty()
        {
            return Err(ocpm_core::OcpmError::invalid_request(
                "explicit population cannot be combined with legacy view time or object_ids",
            ));
        }
        if typed.request.comparison_view.is_some() {
            return Err(ocpm_core::OcpmError::invalid_request(
                "explicit population does not infer comparison population semantics",
            ));
        }
        match &typed.population {
            PopulationSelector::EventTime { start, end }
            | PopulationSelector::LeadingObjectStart { start, end }
            | PopulationSelector::ExecutionContained { start, end }
                if start >= end =>
            {
                return Err(ocpm_core::OcpmError::invalid_request(
                    "population window must have start before end",
                ));
            }
            _ => {}
        }
        let leading_object_type = typed
            .request
            .leading_object_type
            .as_deref()
            .or_else(|| {
                (typed.request.view.object_types.len() == 1)
                    .then(|| typed.request.view.object_types[0].as_str())
            })
            .ok_or_else(|| {
                ocpm_core::OcpmError::invalid_request(
                    "explicit population requires exactly one leading object type",
                )
            })?;
        let resolved = self.provider.resolve_population(
            &typed.request.view,
            &typed.population,
            Some(leading_object_type),
        )?;
        let mut capabilities = self.provider.capability_report()?;
        if !typed.request.resource_calendars.is_empty() {
            capabilities.resource_calendar = CapabilityCoverage::available();
        }
        let mut request = typed.request.clone();
        let result = if let Some(view) = resolved.view {
            request.view = view;
            self.bottlenecks(&request)?
        } else {
            ocpm_bottleneck::analyze_observations(self.provider.name(), &[], None, &request)?
        };
        let mut unsupported = Vec::new();
        push_unsupported(
            &mut unsupported,
            "synchronization",
            &[&capabilities.shared_event],
        );
        for family in [
            "waiting_causes",
            "resource_pressure",
            "performance_patterns",
            "blocking_cascades",
        ] {
            push_unsupported(
                &mut unsupported,
                family,
                &[&capabilities.lifecycle, &capabilities.resource],
            );
        }
        push_unsupported(
            &mut unsupported,
            "resource_unavailability",
            &[
                &capabilities.lifecycle,
                &capabilities.resource,
                &capabilities.resource_calendar,
            ],
        );
        Ok(PopulationBottleneckResult {
            result,
            population: PopulationSummary {
                selector: typed.population.clone(),
                object_count: resolved.object_count,
            },
            capabilities,
            unsupported,
        })
    }

    pub fn fit_prediction(&self, request: &FitPredictionRequest) -> OcpmResult<PredictionArtifact> {
        ocpm_prediction::fit(self.provider.as_ref(), request)
    }

    pub fn predict(&self, request: &PredictionRequest) -> OcpmResult<PredictionResult> {
        ocpm_prediction::predict(self.provider.as_ref(), request)
    }

    #[cfg(feature = "gnn")]
    pub fn fit_gnn(
        &self,
        backend: &dyn GnnBackend,
        request: &GnnRequest,
    ) -> OcpmResult<GnnArtifact> {
        backend.fit(request)
    }

    #[cfg(feature = "gnn")]
    pub fn predict_gnn(
        &self,
        backend: &dyn GnnBackend,
        request: &GnnRequest,
        artifact: &GnnArtifact,
    ) -> OcpmResult<PredictionResult> {
        backend.predict(request, artifact)
    }

    /// Fit the built-in provider-neutral graph bottleneck model.
    #[cfg(feature = "gnn")]
    pub fn fit_gnn_bottlenecks(
        &self,
        request: &GnnBottleneckRequest,
    ) -> OcpmResult<GnnBottleneckArtifact> {
        ocpm_gnn::fit(self.provider.as_ref(), request)
    }

    /// Score graph-aware bottleneck risk with a portable model artifact.
    #[cfg(feature = "gnn")]
    pub fn score_gnn_bottlenecks(
        &self,
        request: &GnnBottleneckRequest,
        artifact: &GnnBottleneckArtifact,
    ) -> OcpmResult<GnnBottleneckResult> {
        ocpm_gnn::score(self.provider.as_ref(), request, artifact)
    }

    /// Fit and score graph-aware bottleneck risk with one provider projection.
    #[cfg(feature = "gnn")]
    pub fn gnn_bottlenecks(
        &self,
        request: &GnnBottleneckRequest,
    ) -> OcpmResult<GnnBottleneckResult> {
        ocpm_gnn::detect(self.provider.as_ref(), request)
    }

    pub fn evaluate_prediction(
        &self,
        view: &DatasetView,
        target: ocpm_core::PredictionTarget,
        holdout_fraction: f64,
        parameters: BTreeMap<String, serde_json::Value>,
    ) -> OcpmResult<PredictionResult> {
        ocpm_prediction::evaluate_temporal_holdout(
            self.provider.as_ref(),
            view,
            target,
            holdout_fraction,
            parameters,
        )
    }

    pub fn explain(&self, view: &DatasetView, operation: ProviderCapability) -> ExecutionPlan {
        let estimate = self.provider.estimate(view, operation);
        ExecutionPlan {
            semantic_version: "1.0".to_owned(),
            estimated_total_ns: estimate.startup_ns,
            estimated_peak_memory_bytes: estimate.peak_memory_bytes,
            steps: vec![ExecutionStep {
                operator: format!("{:?}", operation).to_lowercase(),
                provider: self.provider.name().to_owned(),
                estimated_rows: estimate.rows_returned,
                estimated_bytes: estimate.bytes_transferred,
                pushed_predicates: predicate_summary(view),
                fallback_reason: None,
            }],
        }
    }
}

fn predicate_summary(view: &DatasetView) -> Vec<String> {
    let mut predicates = Vec::new();
    if view.start.is_some() || view.end.is_some() {
        predicates.push("timestamp_range".to_owned());
    }
    if !view.object_types.is_empty() {
        predicates.push("object_types".to_owned());
    }
    if !view.activities.is_empty() {
        predicates.push("activities".to_owned());
    }
    if !view.qualifiers.is_empty() {
        predicates.push("qualifiers".to_owned());
    }
    if !view.event_ids.is_empty() {
        predicates.push("event_ids".to_owned());
    }
    if !view.object_ids.is_empty() {
        predicates.push("object_ids".to_owned());
    }
    if !view.event_attributes.is_empty() {
        predicates.push("event_attributes".to_owned());
    }
    if !view.object_attributes.is_empty() {
        predicates.push("object_attributes".to_owned());
    }
    if !view.statuses.is_empty() {
        predicates.push("statuses".to_owned());
    }
    if !view.related_object_types.is_empty() {
        predicates.push("related_object_types".to_owned());
    }
    if view.minimum_execution_duration_nanos.is_some()
        || view.maximum_execution_duration_nanos.is_some()
    {
        predicates.push("execution_duration".to_owned());
    }
    predicates
}

#[cfg(feature = "postgres")]
fn postgres_bottleneck_view_supported(view: &DatasetView) -> bool {
    view.qualifiers.is_empty()
        && view.event_ids.is_empty()
        && view.object_ids.is_empty()
        && view.event_attributes.is_empty()
        && view.object_attributes.is_empty()
        && view.statuses.is_empty()
        && view.related_object_types.is_empty()
        && view.minimum_execution_duration_nanos.is_none()
        && view.maximum_execution_duration_nanos.is_none()
}

#[cfg(feature = "postgres")]
fn postgres_error(error: impl std::fmt::Display) -> ocpm_core::OcpmError {
    ocpm_core::OcpmError::new(
        ocpm_core::OcpmErrorCode::ProviderUnavailable,
        format!("pg_ocpm bottleneck projection failed: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ocpm_core::{Event, EventObjectRelation, Object, Timestamp};
    use std::collections::BTreeMap;

    #[test]
    fn standalone_engine_profiles_without_postgres() {
        let log = CanonicalLog {
            dataset_id: "standalone".to_owned(),
            tenant_id: "tenant".to_owned(),
            events: vec![Event {
                id: 1,
                external_id: "e1".to_owned(),
                activity: "create".to_owned(),
                timestamp: Timestamp::from_epoch_nanos(1),
                sequence: 0,
                lifecycle: None,
                attributes: BTreeMap::new(),
            }],
            objects: vec![Object {
                id: 1,
                external_id: "o1".to_owned(),
                object_type: "order".to_owned(),
            }],
            event_object_relations: vec![EventObjectRelation {
                relation_id: 1,
                event_id: 1,
                object_id: 1,
                qualifier: String::new(),
            }],
            ..CanonicalLog::default()
        };
        let engine = Engine::from_log(log).unwrap();
        assert_eq!(engine.provider_name(), "local");
        assert_eq!(
            engine.profile(&DatasetView::default()).unwrap().event_count,
            1
        );
    }

    #[test]
    fn bottleneck_entrypoint_preserves_multi_object_synchronization() {
        let event = |id, activity: &str, timestamp| Event {
            id,
            external_id: format!("e{id}"),
            activity: activity.to_owned(),
            timestamp: Timestamp::from_epoch_nanos(timestamp),
            sequence: 0,
            lifecycle: None,
            attributes: BTreeMap::new(),
        };
        let log = CanonicalLog {
            dataset_id: "synchronization".to_owned(),
            tenant_id: "tenant".to_owned(),
            events: vec![
                event(1, "order_ready", 0),
                event(2, "item_ready", 5_000_000_000),
                event(3, "ship", 10_000_000_000),
            ],
            objects: vec![
                Object {
                    id: 1,
                    external_id: "o1".to_owned(),
                    object_type: "order".to_owned(),
                },
                Object {
                    id: 2,
                    external_id: "i1".to_owned(),
                    object_type: "item".to_owned(),
                },
            ],
            event_object_relations: vec![
                EventObjectRelation {
                    relation_id: 1,
                    event_id: 1,
                    object_id: 1,
                    qualifier: String::new(),
                },
                EventObjectRelation {
                    relation_id: 2,
                    event_id: 2,
                    object_id: 2,
                    qualifier: String::new(),
                },
                EventObjectRelation {
                    relation_id: 3,
                    event_id: 3,
                    object_id: 1,
                    qualifier: String::new(),
                },
                EventObjectRelation {
                    relation_id: 4,
                    event_id: 3,
                    object_id: 2,
                    qualifier: String::new(),
                },
            ],
            ..CanonicalLog::default()
        };
        let engine = Engine::from_log(log).unwrap();
        let result = engine
            .bottlenecks(&BottleneckRequest {
                view: DatasetView {
                    object_types: vec!["order".to_owned(), "item".to_owned()],
                    ..DatasetView::default()
                },
                minimum_support: 1,
                ..BottleneckRequest::default()
            })
            .unwrap();
        assert_eq!(result.diagnostics.synchronized_event_count, 1);
        assert_eq!(result.synchronization.len(), 2);
    }

    #[test]
    fn explicit_populations_are_distinct_and_legacy_behavior_is_unchanged() {
        let event = |id, activity: &str, timestamp| Event {
            id,
            external_id: format!("e{id}"),
            activity: activity.to_owned(),
            timestamp: Timestamp::from_epoch_nanos(timestamp),
            sequence: 0,
            lifecycle: None,
            attributes: BTreeMap::new(),
        };
        let log = CanonicalLog {
            dataset_id: "explicit-populations".to_owned(),
            tenant_id: "tenant".to_owned(),
            events: vec![
                event(1, "a", 10),
                event(2, "b", 30),
                event(3, "a", 0),
                event(4, "b", 20),
            ],
            objects: vec![
                Object {
                    id: 1,
                    external_id: "o1".to_owned(),
                    object_type: "order".to_owned(),
                },
                Object {
                    id: 2,
                    external_id: "o2".to_owned(),
                    object_type: "order".to_owned(),
                },
            ],
            event_object_relations: (1..=4)
                .map(|id| EventObjectRelation {
                    relation_id: id,
                    event_id: id,
                    object_id: if id <= 2 { 1 } else { 2 },
                    qualifier: String::new(),
                })
                .collect(),
            ..CanonicalLog::default()
        };
        let engine = Engine::from_log(log).unwrap();
        let request = BottleneckRequest {
            view: DatasetView {
                object_types: vec!["order".to_owned()],
                ..DatasetView::default()
            },
            leading_object_type: Some("order".to_owned()),
            minimum_support: 1,
            ..BottleneckRequest::default()
        };
        let legacy = engine.bottlenecks(&request).unwrap();
        assert!(engine.analysis_capabilities().unwrap().lifecycle.available);
        let analyze_population = |population| {
            engine
                .bottlenecks_for_population(&PopulationBottleneckRequest::new(
                    request.clone(),
                    population,
                ))
                .unwrap()
        };
        let event_time = analyze_population(PopulationSelector::EventTime {
            start: Timestamp::from_epoch_nanos(10),
            end: Timestamp::from_epoch_nanos(30),
        });
        assert_eq!(event_time.population.object_count, 2);
        assert_eq!(event_time.result.diagnostics.observation_count, 0);

        let leading_start = analyze_population(PopulationSelector::LeadingObjectStart {
            start: Timestamp::from_epoch_nanos(10),
            end: Timestamp::from_epoch_nanos(40),
        });
        assert_eq!(leading_start.population.object_count, 1);
        assert_eq!(leading_start.result.diagnostics.observation_count, 1);
        assert!(leading_start.capabilities.lifecycle.available);
        assert!(leading_start.capabilities.resource.available);
        assert!(leading_start.capabilities.shared_event.available);
        assert!(leading_start.unsupported.iter().any(|value| {
            value.family == "resource_unavailability"
                && value.reason_code == "resource_calendar_not_supplied"
        }));

        let contained = analyze_population(PopulationSelector::ExecutionContained {
            start: Timestamp::from_epoch_nanos(5),
            end: Timestamp::from_epoch_nanos(40),
        });
        assert_eq!(contained.population.object_count, 1);
        assert_eq!(contained.result.diagnostics.observation_count, 1);

        let case_set = analyze_population(PopulationSelector::CaseSet {
            object_ids: vec![2, 2, 999],
        });
        assert_eq!(case_set.population.object_count, 1);
        assert_eq!(case_set.result.diagnostics.observation_count, 1);

        let empty_case_set = analyze_population(PopulationSelector::CaseSet {
            object_ids: vec![999],
        });
        assert_eq!(empty_case_set.population.object_count, 0);
        assert_eq!(empty_case_set.result.diagnostics.observation_count, 0);
        assert!(empty_case_set.result.diagnostics.exact);
        assert_eq!(empty_case_set.result.semantic_version, "1.0");

        let mut ambiguous = request.clone();
        ambiguous.comparison_view = Some(DatasetView::default());
        let error = engine
            .bottlenecks_for_population(&PopulationBottleneckRequest::new(
                ambiguous,
                PopulationSelector::CaseSet {
                    object_ids: vec![1],
                },
            ))
            .unwrap_err();
        assert_eq!(error.code, ocpm_core::OcpmErrorCode::InvalidRequest);
        assert_eq!(engine.bottlenecks(&request).unwrap(), legacy);
    }

    #[test]
    fn unsupported_family_reports_the_first_missing_requirement() {
        let available = CapabilityCoverage::available();
        let missing_resource = CapabilityCoverage::unavailable("resource_not_projected");
        let mut unsupported = Vec::new();
        push_unsupported(
            &mut unsupported,
            "waiting_causes",
            &[&available, &missing_resource],
        );
        assert_eq!(
            unsupported,
            vec![UnsupportedMetricFamily {
                family: "waiting_causes".to_owned(),
                reason_code: "resource_not_projected".to_owned(),
            }]
        );
    }
}
