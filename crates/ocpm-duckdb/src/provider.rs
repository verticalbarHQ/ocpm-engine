use crate::{
    DuckDbParquetSource, DuckDbProviderError, parse_attribute, parse_attribute_map,
    snapshot::ResolvedSnapshot, snapshot::open_database, source_timestamp_to_utc,
    utc_nanos_to_source_nanos,
};
use duckdb::{Connection, params_from_iter, types::Value};
use ocpm_core::{
    AttributeValue, CanonicalLog, DatasetProfile, DatasetView, Event, EventObjectRelation, Object,
    ObjectAttributeChange, ObjectObjectRelation, OcpmError, OcpmErrorCode, OcpmResult,
    QueryRequest, QueryResult, Timestamp,
    event_batch::{
        EventActivityCount, EventDfgEdge, EventLogSummary, EventSummaryBuilder, EventVariantCount,
    },
};
use ocpm_local::LocalProvider;
use ocpm_provider::{
    ExecutionMode, ExecutionSummaryRequest, OcpmProvider, ProcessExecution, ProviderCapability,
    ProviderEstimate,
};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Default)]
struct SummaryCache {
    entries: HashMap<String, (EventLogSummary, u64)>,
    lru: VecDeque<String>,
    retained_bytes: u64,
    maximum_bytes: u64,
}

impl SummaryCache {
    fn with_limit(maximum_bytes: u64) -> Self {
        Self {
            maximum_bytes,
            ..Self::default()
        }
    }

    fn get(&mut self, key: &str) -> Option<EventLogSummary> {
        let value = self.entries.get(key).map(|(value, _)| value.clone())?;
        self.lru.retain(|existing| existing != key);
        self.lru.push_back(key.to_owned());
        Some(value)
    }

    fn insert(&mut self, key: String, value: EventLogSummary) {
        if self.maximum_bytes == 0 {
            return;
        }
        let Ok(serialized) = serde_json::to_vec(&value) else {
            return;
        };
        let bytes = u64::try_from(key.len().saturating_add(serialized.len())).unwrap_or(u64::MAX);
        if bytes > self.maximum_bytes {
            return;
        }
        if let Some((_, replaced_bytes)) = self.entries.remove(&key) {
            self.retained_bytes = self.retained_bytes.saturating_sub(replaced_bytes);
            self.lru.retain(|existing| existing != &key);
        }
        while self.retained_bytes.saturating_add(bytes) > self.maximum_bytes {
            let Some(oldest) = self.lru.pop_front() else {
                break;
            };
            if let Some((_, evicted_bytes)) = self.entries.remove(&oldest) {
                self.retained_bytes = self.retained_bytes.saturating_sub(evicted_bytes);
            }
        }
        self.retained_bytes = self.retained_bytes.saturating_add(bytes);
        self.lru.push_back(key.clone());
        self.entries.insert(key, (value, bytes));
    }
}

/// A source-neutral provider over immutable Parquet snapshots queried through a
/// deployment-supplied DuckDB client library.
///
/// Each operation checks out one independent DuckDB connection. This avoids sharing a
/// connection across threads and lets concurrent read-only scans use the immutable snapshot
/// without serializing through a global lock.
pub struct DuckDbProvider {
    connections: Vec<Mutex<Connection>>,
    next_connection: AtomicUsize,
    resolved: ResolvedSnapshot,
    memory_budget_bytes: u64,
    summary_cache: Mutex<SummaryCache>,
    cache_canonical_fallback: bool,
    canonical_fallback: Mutex<Option<Arc<LocalProvider>>>,
}

impl DuckDbProvider {
    pub fn open(source: DuckDbParquetSource) -> Result<Self, DuckDbProviderError> {
        let (connection, resolved) = open_database(&source)?;
        let connection_count = source.options.connection_pool_size.max(1);
        let mut connections = Vec::with_capacity(connection_count);
        connections.push(Mutex::new(connection));
        for _ in 1..connection_count {
            let (connection, other) = open_database(&source)?;
            if other.manifest_sha256 != resolved.manifest_sha256
                || other.version_root != resolved.version_root
            {
                return Err(DuckDbProviderError::InvalidSource(
                    "snapshot changed while the DuckDB connection pool was opening".to_owned(),
                ));
            }
            connections.push(Mutex::new(connection));
        }
        Ok(Self {
            connections,
            next_connection: AtomicUsize::new(0),
            resolved,
            memory_budget_bytes: source.options.memory_budget_bytes,
            summary_cache: Mutex::new(SummaryCache::with_limit(source.options.result_cache_bytes)),
            cache_canonical_fallback: source.options.cache_canonical_fallback,
            canonical_fallback: Mutex::new(None),
        })
    }

    pub fn snapshot_version(&self) -> &str {
        &self.resolved.version
    }

    pub fn snapshot_manifest_sha256(&self) -> &str {
        &self.resolved.manifest_sha256
    }

    fn with_connection<T>(
        &self,
        operation: impl FnOnce(&Connection) -> Result<T, DuckDbProviderError>,
    ) -> Result<T, DuckDbProviderError> {
        let index = self.next_connection.fetch_add(1, Ordering::Relaxed) % self.connections.len();
        let connection = self.connections[index].lock().map_err(|_| {
            DuckDbProviderError::InvalidSource("DuckDB connection lock was poisoned".to_owned())
        })?;
        operation(&connection)
    }

    fn full_log(&self) -> Result<CanonicalLog, DuckDbProviderError> {
        self.with_connection(|connection| load_log(connection, &self.resolved))
    }

    fn exact_local(&self) -> Result<Arc<LocalProvider>, DuckDbProviderError> {
        if !self.cache_canonical_fallback {
            return Ok(Arc::new(LocalProvider::new(self.full_log()?)?));
        }
        let mut cached = self.canonical_fallback.lock().map_err(|_| {
            DuckDbProviderError::InvalidSource(
                "DuckDB canonical fallback cache lock was poisoned".to_owned(),
            )
        })?;
        if let Some(provider) = cached.as_ref() {
            return Ok(Arc::clone(provider));
        }
        let provider = Arc::new(LocalProvider::new(self.full_log()?)?);
        *cached = Some(Arc::clone(&provider));
        Ok(provider)
    }

    fn scan_executions<T>(
        &self,
        view: &DatasetView,
        leading_object_type: Option<&str>,
        complete_lifecycle: bool,
        mut consume: impl FnMut(ProcessExecution) -> Result<(), DuckDbProviderError>,
    ) -> Result<T, DuckDbProviderError>
    where
        T: Default,
    {
        let (sql, values) = execution_sql(
            view,
            leading_object_type,
            complete_lifecycle,
            &self.resolved,
        )?;
        self.with_connection(|connection| {
            let mut statement = connection.prepare(&sql)?;
            let mut rows = statement.query(params_from_iter(values.iter()))?;
            let mut current: Option<ProcessExecution> = None;
            let mut lifecycle_bounds: Option<(i128, i128)> = None;
            while let Some(row) = rows.next()? {
                let object_id = row.get::<_, u64>(0)?;
                let external_object_id = row.get::<_, String>(1)?;
                let object_type = row.get::<_, String>(2)?;
                if current
                    .as_ref()
                    .is_some_and(|value| value.object_ids[0] != object_id)
                {
                    let finished = current.take().expect("checked current execution");
                    if execution_matches(
                        view,
                        &finished,
                        complete_lifecycle,
                        lifecycle_bounds.take(),
                    ) {
                        consume(finished)?;
                    }
                }
                let execution = current.get_or_insert_with(|| ProcessExecution {
                    id: format!("object:{external_object_id}"),
                    object_type,
                    object_ids: vec![object_id],
                    events: Vec::new(),
                    event_object_ids: BTreeMap::new(),
                });
                if complete_lifecycle && lifecycle_bounds.is_none() {
                    lifecycle_bounds = Some((
                        source_epoch_nanos_to_utc(row.get::<_, i128>(11)?, &self.resolved)?,
                        source_epoch_nanos_to_utc(row.get::<_, i128>(12)?, &self.resolved)?,
                    ));
                }
                let event = row_event(row, &self.resolved)?;
                if event_matches_post_scan(view, &event)
                    && execution
                        .events
                        .last()
                        .is_none_or(|value| value.id != event.id)
                {
                    execution
                        .event_object_ids
                        .entry(event.id)
                        .or_default()
                        .push(object_id);
                    execution.events.push(event);
                }
            }
            if let Some(finished) = current.take() {
                if execution_matches(view, &finished, complete_lifecycle, lifecycle_bounds.take()) {
                    consume(finished)?;
                }
            }
            Ok(T::default())
        })
    }

    fn leading_executions(
        &self,
        view: &DatasetView,
        leading_object_type: Option<&str>,
    ) -> Result<Vec<ProcessExecution>, DuckDbProviderError> {
        let mut executions = Vec::new();
        self.scan_executions::<()>(view, leading_object_type, false, |execution| {
            executions.push(execution);
            Ok(())
        })?;
        Ok(executions)
    }

    fn summarize(
        &self,
        request: &ExecutionSummaryRequest,
    ) -> Result<EventLogSummary, DuckDbProviderError> {
        let cache_key = serde_json::to_string(request)?;
        if let Some(value) = self
            .summary_cache
            .lock()
            .map_err(|_| {
                DuckDbProviderError::InvalidSource(
                    "DuckDB summary cache lock was poisoned".to_owned(),
                )
            })?
            .get(&cache_key)
        {
            return Ok(value);
        }
        let result = if self.resolved.layout_name == "canonical_v1"
            && request.view.event_attributes.is_empty()
            && request.view.statuses.is_empty()
        {
            self.summarize_aggregated(request)?
        } else if request.view.event_attributes.is_empty() && request.view.statuses.is_empty() {
            self.summarize_projected(request)?
        } else {
            let mut builder = EventSummaryBuilder::new();
            self.scan_executions::<()>(
                &request.view,
                request.leading_object_type.as_deref(),
                request.complete_lifecycle,
                |execution| {
                    let activity_path = execution.activity_path();
                    let timestamps = execution
                        .events
                        .iter()
                        .map(|event| {
                            i64::try_from(event.timestamp.epoch_nanos_utc / 1_000).map_err(|_| {
                                DuckDbProviderError::InvalidSource(
                                    "event timestamp is outside microsecond summary range"
                                        .to_owned(),
                                )
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    builder
                        .push_case(&activity_path, &timestamps)
                        .map_err(|error| {
                            DuckDbProviderError::Canonical(OcpmError::new(
                                OcpmErrorCode::ResourceLimit,
                                error.to_string(),
                            ))
                        })?;
                    Ok(())
                },
            )?;
            builder.finish()
        };
        self.summary_cache
            .lock()
            .map_err(|_| {
                DuckDbProviderError::InvalidSource(
                    "DuckDB summary cache lock was poisoned".to_owned(),
                )
            })?
            .insert(cache_key, result.clone());
        Ok(result)
    }

    /// Execute high-cardinality work inside DuckDB and cross the client
    /// boundary with bounded aggregate rows. The plan is derived from the
    /// source-neutral view rather than from dataset or workload identifiers.
    fn summarize_aggregated(
        &self,
        request: &ExecutionSummaryRequest,
    ) -> Result<EventLogSummary, DuckDbProviderError> {
        let (sql, values) = aggregated_summary_sql(
            &request.view,
            request.leading_object_type.as_deref(),
            request.complete_lifecycle,
            &self.resolved,
        )?;
        let (case_count, event_count, variants_json, dfg_json, activities_json) = self
            .with_connection(|connection| {
                Ok(
                    connection.query_row(&sql, params_from_iter(values.iter()), |row| {
                        Ok((
                            row.get::<_, u64>(0)?,
                            row.get::<_, u64>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                        ))
                    })?,
                )
            })?;
        Ok(EventLogSummary {
            case_count,
            event_count,
            payload_bytes: 0,
            variants: serde_json::from_str::<Vec<EventVariantCount>>(&variants_json)?,
            dfg: serde_json::from_str::<Vec<EventDfgEdge>>(&dfg_json)?,
            activities: serde_json::from_str::<Vec<EventActivityCount>>(&activities_json)?,
        })
    }

    /// Stream only the columns required by the aggregate kernel. This is the
    /// common process-mining path and avoids allocating canonical event objects
    /// or decoding JSON attributes that the summary cannot observe.
    fn summarize_projected(
        &self,
        request: &ExecutionSummaryRequest,
    ) -> Result<EventLogSummary, DuckDbProviderError> {
        let (sql, values) = summary_execution_sql(
            &request.view,
            request.leading_object_type.as_deref(),
            request.complete_lifecycle,
            &self.resolved,
        )?;
        self.with_connection(|connection| {
            let mut statement = connection.prepare(&sql)?;
            let mut rows = statement.query(params_from_iter(values.iter()))?;
            let mut builder = EventSummaryBuilder::new();
            let mut object_id = None;
            let mut last_event_id = None;
            let mut activities = Vec::new();
            let mut timestamps = Vec::new();
            let mut lifecycle_bounds = None;

            let flush = |activities: &mut Vec<String>,
                         timestamps: &mut Vec<i64>,
                         lifecycle_bounds: &mut Option<(i128, i128)>,
                         builder: &mut EventSummaryBuilder|
             -> Result<(), DuckDbProviderError> {
                if activities.is_empty() {
                    *lifecycle_bounds = None;
                    return Ok(());
                }
                let first = i128::from(timestamps[0]) * 1_000;
                let last = i128::from(timestamps[timestamps.len() - 1]) * 1_000;
                let lifecycle_matches = !request.complete_lifecycle
                    || lifecycle_bounds.is_none_or(|(start, end)| {
                        request
                            .view
                            .start
                            .as_ref()
                            .is_none_or(|minimum| start >= minimum.epoch_nanos_utc)
                            && request
                                .view
                                .end
                                .as_ref()
                                .is_none_or(|maximum| end < maximum.epoch_nanos_utc)
                    });
                let duration = last.saturating_sub(first);
                let duration_matches = request
                    .view
                    .minimum_execution_duration_nanos
                    .is_none_or(|minimum| duration >= minimum)
                    && request
                        .view
                        .maximum_execution_duration_nanos
                        .is_none_or(|maximum| duration <= maximum);
                if lifecycle_matches && duration_matches {
                    builder.push_case(activities, timestamps).map_err(|error| {
                        DuckDbProviderError::Canonical(OcpmError::new(
                            OcpmErrorCode::ResourceLimit,
                            error.to_string(),
                        ))
                    })?;
                }
                activities.clear();
                timestamps.clear();
                *lifecycle_bounds = None;
                Ok(())
            };

            while let Some(row) = rows.next()? {
                let next_object_id = row.get::<_, u64>(0)?;
                if object_id.is_some_and(|current| current != next_object_id) {
                    flush(
                        &mut activities,
                        &mut timestamps,
                        &mut lifecycle_bounds,
                        &mut builder,
                    )?;
                    last_event_id = None;
                }
                object_id = Some(next_object_id);
                let event_id = row.get::<_, u64>(1)?;
                if last_event_id == Some(event_id) {
                    continue;
                }
                last_event_id = Some(event_id);
                activities.push(row.get(2)?);
                let nanos = source_epoch_nanos_to_utc(row.get(3)?, &self.resolved)?;
                timestamps.push(i64::try_from(nanos / 1_000).map_err(|_| {
                    DuckDbProviderError::InvalidSource(
                        "event timestamp is outside microsecond summary range".to_owned(),
                    )
                })?);
                if request.complete_lifecycle && lifecycle_bounds.is_none() {
                    lifecycle_bounds = Some((
                        source_epoch_nanos_to_utc(row.get(6)?, &self.resolved)?,
                        source_epoch_nanos_to_utc(row.get(7)?, &self.resolved)?,
                    ));
                }
            }
            flush(
                &mut activities,
                &mut timestamps,
                &mut lifecycle_bounds,
                &mut builder,
            )?;
            Ok(builder.finish())
        })
    }
}

impl OcpmProvider for DuckDbProvider {
    fn name(&self) -> &'static str {
        "duckdb_parquet"
    }

    fn semantic_version(&self) -> &'static str {
        "1.0"
    }

    fn capabilities(&self) -> Vec<ProviderCapability> {
        vec![
            ProviderCapability::CanonicalScan,
            ProviderCapability::ProcessExecutions,
            ProviderCapability::ObjectCentricQuery,
            ProviderCapability::DfgAggregate,
            ProviderCapability::VariantAggregate,
            ProviderCapability::PerformanceAggregate,
            ProviderCapability::PredictionFeatures,
        ]
    }

    fn profile(&self, view: &DatasetView) -> OcpmResult<DatasetProfile> {
        self.exact_local()?.profile(view)
    }

    fn process_executions(
        &self,
        view: &DatasetView,
        mode: ExecutionMode,
        leading_object_type: Option<&str>,
    ) -> OcpmResult<Vec<ProcessExecution>> {
        match mode {
            ExecutionMode::LeadingObject => Ok(self.leading_executions(view, leading_object_type)?),
            ExecutionMode::ConnectedComponent => {
                Ok(self
                    .exact_local()?
                    .process_executions(view, mode, leading_object_type)?)
            }
        }
    }

    fn query(&self, request: &QueryRequest) -> OcpmResult<QueryResult> {
        self.exact_local()?.query(request)
    }

    fn execution_summary(
        &self,
        request: &ExecutionSummaryRequest,
    ) -> OcpmResult<Option<EventLogSummary>> {
        Ok(Some(self.summarize(request)?))
    }

    fn snapshot(&self, view: &DatasetView) -> OcpmResult<CanonicalLog> {
        self.exact_local()?.snapshot(view)
    }

    fn estimate(&self, view: &DatasetView, _operation: ProviderCapability) -> ProviderEstimate {
        let rows = self
            .with_connection(|connection| {
                let value = connection.query_row(
                    "SELECT count(*)::UBIGINT FROM ocpm_events",
                    [],
                    |row| row.get::<_, u64>(0),
                )?;
                Ok(value)
            })
            .unwrap_or_default();
        let selectivity = if view.start.is_some()
            || view.end.is_some()
            || !view.activities.is_empty()
            || !view.object_types.is_empty()
        {
            0.25
        } else {
            1.0
        };
        ProviderEstimate {
            startup_ns: if self.resolved.is_s3 {
                5_000_000
            } else {
                100_000
            },
            rows_read: rows,
            rows_returned: (rows as f64 * selectivity) as u64,
            bytes_transferred: if self.resolved.is_s3 {
                rows.saturating_mul(24)
            } else {
                0
            },
            peak_memory_bytes: self.memory_budget_bytes,
            confidence: 0.6,
        }
    }
}

fn execution_sql(
    view: &DatasetView,
    leading_object_type: Option<&str>,
    complete_lifecycle: bool,
    resolved: &ResolvedSnapshot,
) -> Result<(String, Vec<Value>), DuckDbProviderError> {
    build_execution_sql(
        view,
        leading_object_type,
        complete_lifecycle,
        resolved,
        "x.object_id, x.external_object_id, x.object_type, \
         x.event_id, x.external_event_id, x.activity, \
         x.timestamp_nanos_utc, x.source_timestamp, x.sequence, \
         x.lifecycle, x.attributes_json",
        true,
    )
}

fn summary_execution_sql(
    view: &DatasetView,
    leading_object_type: Option<&str>,
    complete_lifecycle: bool,
    resolved: &ResolvedSnapshot,
) -> Result<(String, Vec<Value>), DuckDbProviderError> {
    build_execution_sql(
        view,
        leading_object_type,
        complete_lifecycle,
        resolved,
        "x.object_id, x.event_id, x.activity, x.timestamp_nanos_utc, \
         x.sequence, x.external_event_id",
        true,
    )
}

fn aggregated_summary_sql(
    view: &DatasetView,
    leading_object_type: Option<&str>,
    complete_lifecycle: bool,
    resolved: &ResolvedSnapshot,
) -> Result<(String, Vec<Value>), DuckDbProviderError> {
    let (base, mut values) = build_execution_sql(
        view,
        leading_object_type,
        complete_lifecycle,
        resolved,
        "x.object_id, x.event_id, x.activity, x.timestamp_nanos_utc, \
         x.sequence, x.external_event_id",
        false,
    )?;
    let mut eligibility = vec!["count(*) > 0".to_owned()];
    if complete_lifecycle {
        if let Some(start) = &view.start {
            eligibility.push("min(lifecycle_start) >= ?".to_owned());
            values.push(Value::HugeInt(start.epoch_nanos_utc));
        }
        if let Some(end) = &view.end {
            eligibility.push("max(lifecycle_end) < ?".to_owned());
            values.push(Value::HugeInt(end.epoch_nanos_utc));
        }
    }
    if let Some(minimum) = view.minimum_execution_duration_nanos {
        eligibility.push("max(timestamp_nanos_utc) - min(timestamp_nanos_utc) >= ?".to_owned());
        values.push(Value::HugeInt(minimum));
    }
    if let Some(maximum) = view.maximum_execution_duration_nanos {
        eligibility.push("max(timestamp_nanos_utc) - min(timestamp_nanos_utc) <= ?".to_owned());
        values.push(Value::HugeInt(maximum));
    }
    let having = eligibility.join(" AND ");
    let sql = format!(
        r#"
        WITH filtered AS MATERIALIZED ({base}),
        deduplicated AS MATERIALIZED (
          SELECT object_id,event_id,activity,timestamp_nanos_utc,sequence,
                 external_event_id,lifecycle_start,lifecycle_end
          FROM filtered
          QUALIFY row_number() OVER (
            PARTITION BY object_id,event_id
            ORDER BY timestamp_nanos_utc,sequence,external_event_id
          ) = 1
        ),
        eligible_objects AS MATERIALIZED (
          SELECT object_id
          FROM deduplicated
          GROUP BY object_id
          HAVING {having}
        ),
        ordered_events AS MATERIALIZED (
          SELECT selected.*,
                 row_number() OVER lifecycle AS position,
                 count(*) OVER lifecycle AS path_length,
                 lag(activity) OVER lifecycle AS previous_activity,
                 lag(timestamp_nanos_utc) OVER lifecycle AS previous_timestamp
          FROM deduplicated selected
          JOIN eligible_objects USING(object_id)
          WINDOW lifecycle AS (
            PARTITION BY object_id
            ORDER BY timestamp_nanos_utc,sequence,external_event_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          )
        ),
        variants AS (
          SELECT activity_path,count(*)::UBIGINT AS frequency
          FROM (
            SELECT object_id,
                   list(activity ORDER BY timestamp_nanos_utc,sequence,external_event_id)
                     AS activity_path
            FROM ordered_events GROUP BY object_id
          ) paths
          GROUP BY activity_path
        ),
        edges AS (
          SELECT previous_activity AS source,activity AS target,
                 count(*)::UBIGINT AS frequency,
                 (sum((timestamp_nanos_utc / 1000)::BIGINT
                    - (previous_timestamp / 1000)::BIGINT)::DOUBLE
                   / count(*)::DOUBLE / 1000000.0) AS mean_duration_seconds
          FROM ordered_events
          WHERE previous_activity IS NOT NULL
          GROUP BY previous_activity,activity
        ),
        activity_counts AS (
          SELECT activity,
                 count(DISTINCT object_id)::UBIGINT AS case_frequency,
                 count(*)::UBIGINT AS occurrence_frequency,
                 count(*) FILTER (WHERE position=1)::UBIGINT AS start_frequency,
                 count(*) FILTER (WHERE position=path_length)::UBIGINT AS end_frequency
          FROM ordered_events GROUP BY activity
        )
        SELECT
          (SELECT count(DISTINCT object_id)::UBIGINT FROM ordered_events) AS case_count,
          (SELECT count(*)::UBIGINT FROM ordered_events) AS event_count,
          coalesce((
            SELECT to_json(list(struct_pack(
              activity_path := activity_path,
              frequency := frequency
            ) ORDER BY activity_path))::VARCHAR FROM variants
          ), '[]') AS variants_json,
          coalesce((
            SELECT to_json(list(struct_pack(
              source := source,
              target := target,
              frequency := frequency,
              mean_duration_seconds := mean_duration_seconds
            ) ORDER BY source,target))::VARCHAR FROM edges
          ), '[]') AS dfg_json,
          coalesce((
            SELECT to_json(list(struct_pack(
              activity := activity,
              case_frequency := case_frequency,
              occurrence_frequency := occurrence_frequency,
              start_frequency := start_frequency,
              end_frequency := end_frequency
            ) ORDER BY activity))::VARCHAR FROM activity_counts
          ), '[]') AS activities_json
        "#
    );
    Ok((sql, values))
}

fn build_execution_sql(
    view: &DatasetView,
    leading_object_type: Option<&str>,
    complete_lifecycle: bool,
    resolved: &ResolvedSnapshot,
    projection: &str,
    ordered: bool,
) -> Result<(String, Vec<Value>), DuckDbProviderError> {
    let mut predicates = Vec::new();
    let mut values = Vec::new();
    let leading = leading_object_type.or_else(|| view.object_types.first().map(String::as_str));
    if let Some(value) = leading {
        predicates.push("x.object_type = ?".to_owned());
        values.push(Value::Text(value.to_owned()));
    }
    add_json_string_filter(
        &mut predicates,
        &mut values,
        "x.object_type",
        &view.object_types,
    )?;
    add_json_u64_filter(
        &mut predicates,
        &mut values,
        "x.object_id",
        &view.object_ids,
    )?;
    add_json_string_filter(&mut predicates, &mut values, "x.activity", &view.activities)?;
    add_json_u64_filter(&mut predicates, &mut values, "x.event_id", &view.event_ids)?;
    add_json_string_filter(
        &mut predicates,
        &mut values,
        "x.qualifier",
        &view.qualifiers,
    )?;
    for (name, value) in &view.object_attributes {
        predicates.push(
            "(SELECT a.value_json FROM ocpm_object_attributes a \
             WHERE a.object_id=x.object_id AND a.name=? \
             AND (? IS NULL OR a.valid_from_nanos_utc < ?) \
             ORDER BY a.valid_from_nanos_utc DESC LIMIT 1) = ?"
                .to_owned(),
        );
        values.push(Value::Text(name.clone()));
        let end = view
            .end
            .as_ref()
            .map(|value| {
                utc_nanos_to_source_nanos(value.epoch_nanos_utc, &resolved.timestamp_policy)
            })
            .transpose()?;
        values.push(end.map_or(Value::Null, Value::HugeInt));
        values.push(end.map_or(Value::Null, Value::HugeInt));
        values.push(Value::Text(serde_json::to_string(value)?));
    }
    if !view.related_object_types.is_empty() {
        predicates.push(
            "EXISTS (SELECT 1 FROM ocpm_o2o link \
             JOIN ocpm_objects related ON related.object_id = CASE \
             WHEN link.source_object_id=x.object_id THEN link.target_object_id \
             ELSE link.source_object_id END \
             JOIN json_each(?) wanted \
               ON json_extract_string(wanted.value, '$')=related.object_type \
             WHERE link.source_object_id=x.object_id OR link.target_object_id=x.object_id)"
                .to_owned(),
        );
        values.push(Value::Text(serde_json::to_string(
            &view.related_object_types,
        )?));
    }
    if !complete_lifecycle {
        if let Some(start) = &view.start {
            predicates.push("x.timestamp_nanos_utc >= ?".to_owned());
            values.push(Value::HugeInt(utc_nanos_to_source_nanos(
                start.epoch_nanos_utc,
                &resolved.timestamp_policy,
            )?));
        }
        if let Some(end) = &view.end {
            predicates.push("x.timestamp_nanos_utc < ?".to_owned());
            values.push(Value::HugeInt(utc_nanos_to_source_nanos(
                end.epoch_nanos_utc,
                &resolved.timestamp_policy,
            )?));
        }
    }
    let where_clause = if predicates.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", predicates.join(" AND "))
    };
    let lifecycle_columns = if complete_lifecycle {
        "x.lifecycle_start, x.lifecycle_end"
    } else {
        "NULL::HUGEINT AS lifecycle_start, NULL::HUGEINT AS lifecycle_end"
    };
    let order_clause = if ordered {
        "ORDER BY x.object_id, x.timestamp_nanos_utc, x.sequence, x.external_event_id"
    } else {
        ""
    };
    Ok((
        format!(
            r#"
            SELECT DISTINCT
              {projection},
              {lifecycle_columns}
            FROM ocpm_execution_events x
            {where_clause}
            {order_clause}
            "#
        ),
        values,
    ))
}

fn add_json_string_filter(
    predicates: &mut Vec<String>,
    values: &mut Vec<Value>,
    column: &str,
    filter: &[String],
) -> Result<(), DuckDbProviderError> {
    if !filter.is_empty() {
        predicates.push(format!(
            "{column} IN (SELECT json_extract_string(value, '$') FROM json_each(?))"
        ));
        values.push(Value::Text(serde_json::to_string(filter)?));
    }
    Ok(())
}

fn add_json_u64_filter(
    predicates: &mut Vec<String>,
    values: &mut Vec<Value>,
    column: &str,
    filter: &[u64],
) -> Result<(), DuckDbProviderError> {
    if !filter.is_empty() {
        predicates.push(format!(
            "{column} IN (SELECT value::UBIGINT FROM json_each(?))"
        ));
        values.push(Value::Text(serde_json::to_string(filter)?));
    }
    Ok(())
}

fn row_event(
    row: &duckdb::Row<'_>,
    resolved: &ResolvedSnapshot,
) -> Result<Event, DuckDbProviderError> {
    let stored_timestamp = row.get::<_, i128>(6)?;
    let source = row.get::<_, Option<String>>(7)?;
    let timestamp = if resolved.layout_name == "entity_link_snapshot_v1" {
        source_timestamp_to_utc(
            source.as_deref().ok_or_else(|| {
                DuckDbProviderError::InvalidSource(
                    "entity-link event is missing its source timestamp".to_owned(),
                )
            })?,
            &resolved.timestamp_policy,
        )?
    } else {
        Timestamp {
            epoch_nanos_utc: stored_timestamp,
            source,
        }
    };
    Ok(Event {
        id: row.get(3)?,
        external_id: row.get(4)?,
        activity: row.get(5)?,
        timestamp,
        sequence: row.get(8)?,
        lifecycle: row.get(9)?,
        attributes: parse_attribute_map(&row.get::<_, String>(10)?)?,
    })
}

fn event_matches_post_scan(view: &DatasetView, event: &Event) -> bool {
    view.contains_timestamp(&event.timestamp)
        && view
            .event_attributes
            .iter()
            .all(|(name, value)| event.attributes.get(name) == Some(value))
        && (view.statuses.is_empty()
            || event.attributes.get("status").is_some_and(|value| {
                matches!(value, AttributeValue::String(status) if view.statuses.contains(status))
            }))
}

fn execution_matches(
    view: &DatasetView,
    execution: &ProcessExecution,
    complete_lifecycle: bool,
    lifecycle_bounds: Option<(i128, i128)>,
) -> bool {
    if execution.events.is_empty() {
        return false;
    }
    let first = &execution.events[0].timestamp;
    let last = &execution.events[execution.events.len() - 1].timestamp;
    if complete_lifecycle
        && lifecycle_bounds.is_some_and(|(first, last)| {
            view.start
                .as_ref()
                .is_some_and(|start| first < start.epoch_nanos_utc)
                || view
                    .end
                    .as_ref()
                    .is_some_and(|end| last >= end.epoch_nanos_utc)
        })
    {
        return false;
    }
    let duration = last.epoch_nanos_utc.saturating_sub(first.epoch_nanos_utc);
    view.minimum_execution_duration_nanos
        .is_none_or(|minimum| duration >= minimum)
        && view
            .maximum_execution_duration_nanos
            .is_none_or(|maximum| duration <= maximum)
}

fn source_epoch_nanos_to_utc(
    value: i128,
    resolved: &ResolvedSnapshot,
) -> Result<i128, DuckDbProviderError> {
    if resolved.layout_name != "entity_link_snapshot_v1" {
        return Ok(value);
    }
    let value = i64::try_from(value).map_err(|_| {
        DuckDbProviderError::InvalidSource(
            "source timestamp is outside DuckDB nanosecond range".to_owned(),
        )
    })?;
    let source = chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(value)
        .naive_utc()
        .format("%Y-%m-%d %H:%M:%S%.f")
        .to_string();
    Ok(source_timestamp_to_utc(&source, &resolved.timestamp_policy)?.epoch_nanos_utc)
}

fn load_log(
    connection: &Connection,
    resolved: &ResolvedSnapshot,
) -> Result<CanonicalLog, DuckDbProviderError> {
    let mut log = CanonicalLog {
        dataset_id: resolved.dataset_id.clone(),
        tenant_id: resolved.tenant_id.clone(),
        source_watermark: resolved.source_watermark.clone(),
        metadata: BTreeMap::from([
            (
                "snapshot_version".to_owned(),
                serde_json::json!(resolved.version),
            ),
            (
                "snapshot_manifest_sha256".to_owned(),
                serde_json::json!(resolved.manifest_sha256),
            ),
            (
                "provider_layout".to_owned(),
                serde_json::json!(resolved.layout_name),
            ),
        ]),
        ..CanonicalLog::default()
    };
    {
        let mut statement = connection.prepare(
            "SELECT event_id, external_event_id, activity, timestamp_nanos_utc, \
             source_timestamp, sequence, lifecycle, attributes_json \
             FROM ocpm_events ORDER BY timestamp_nanos_utc, sequence, external_event_id",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let source = row.get::<_, Option<String>>(4)?;
            let timestamp = if resolved.layout_name == "entity_link_snapshot_v1" {
                source_timestamp_to_utc(
                    source.as_deref().ok_or_else(|| {
                        DuckDbProviderError::InvalidSource(
                            "entity-link event is missing its source timestamp".to_owned(),
                        )
                    })?,
                    &resolved.timestamp_policy,
                )?
            } else {
                Timestamp {
                    epoch_nanos_utc: row.get(3)?,
                    source,
                }
            };
            log.events.push(Event {
                id: row.get(0)?,
                external_id: row.get(1)?,
                activity: row.get(2)?,
                timestamp,
                sequence: row.get(5)?,
                lifecycle: row.get(6)?,
                attributes: parse_attribute_map(&row.get::<_, String>(7)?)?,
            });
        }
    }
    {
        let mut statement = connection.prepare(
            "SELECT object_id, external_object_id, object_type FROM ocpm_objects ORDER BY object_id",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            log.objects.push(Object {
                id: row.get(0)?,
                external_id: row.get(1)?,
                object_type: row.get(2)?,
            });
        }
    }
    {
        let mut statement = connection.prepare(
            "SELECT relation_id, event_id, object_id, qualifier FROM ocpm_e2o ORDER BY relation_id",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            log.event_object_relations.push(EventObjectRelation {
                relation_id: row.get(0)?,
                event_id: row.get(1)?,
                object_id: row.get(2)?,
                qualifier: row.get(3)?,
            });
        }
    }
    {
        let mut statement = connection.prepare(
            "SELECT relation_id, source_object_id, target_object_id, qualifier, \
             valid_from_nanos_utc, valid_to_nanos_utc FROM ocpm_o2o ORDER BY relation_id",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            log.object_object_relations.push(ObjectObjectRelation {
                relation_id: row.get(0)?,
                source_object_id: row.get(1)?,
                target_object_id: row.get(2)?,
                qualifier: row.get(3)?,
                valid_from: row
                    .get::<_, Option<i128>>(4)?
                    .map(Timestamp::from_epoch_nanos),
                valid_to: row
                    .get::<_, Option<i128>>(5)?
                    .map(Timestamp::from_epoch_nanos),
            });
        }
    }
    {
        let mut statement = connection.prepare(
            "SELECT object_id, name, valid_from_nanos_utc, value_json \
             FROM ocpm_object_attributes ORDER BY object_id, name, valid_from_nanos_utc",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let source_nanos = row.get::<_, i128>(2)?;
            let utc_nanos = if resolved.layout_name == "entity_link_snapshot_v1" {
                // Non-temporal facets are marked outside the event history. Preserve the
                // sentinel so historical views cannot accidentally treat them as facts.
                source_nanos
            } else {
                source_nanos
            };
            let encoded = row.get::<_, String>(3)?;
            log.object_attribute_history.push(ObjectAttributeChange {
                object_id: row.get(0)?,
                name: row.get(1)?,
                valid_from: Timestamp::from_epoch_nanos(utc_nanos),
                value: parse_attribute(&serde_json::from_str(&encoded)?)?,
            });
        }
    }
    log.validate()?;
    log.sort_canonical();
    Ok(log)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DuckDbDatabase, DuckDbOptions, ParquetCachePolicy, ParquetLocation, SnapshotSelection,
        SourceValidationPolicy, write_canonical_snapshot,
    };
    use ocpm_core::{Constraint, EventObjectRelation, Object, QueryRequest};
    use std::{
        collections::BTreeMap,
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    struct TestDirectory(std::path::PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos();
            // A monotonic counter, because pid+nanos is not unique: tests run in parallel and two
            // can read the same nanosecond, share a directory, and fail the second writer with
            // DirectoryNotEmpty. That was latent until this file gained more tests.
            static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("ocpm-duckdb-test-{}-{nonce}-{seq}", std::process::id()));
            fs::create_dir_all(&path).expect("create test directory");
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn event(id: u64, activity: &str, nanos: i128, status: &str) -> Event {
        Event {
            id,
            external_id: format!("e{id}"),
            activity: activity.to_owned(),
            timestamp: Timestamp::from_epoch_nanos(nanos),
            sequence: id,
            lifecycle: None,
            attributes: BTreeMap::from([(
                "status".to_owned(),
                AttributeValue::String(status.to_owned()),
            )]),
        }
    }

    fn log() -> CanonicalLog {
        CanonicalLog {
            dataset_id: "round-trip".to_owned(),
            tenant_id: "tenant".to_owned(),
            events: vec![
                event(1, "create", 1_000_000_000, "open"),
                event(2, "approve", 2_000_000_000, "open"),
                event(3, "close", 3_000_000_000, "closed"),
            ],
            objects: vec![Object {
                id: 10,
                external_id: "order-10".to_owned(),
                object_type: "Order".to_owned(),
            }],
            event_object_relations: (1..=3)
                .map(|id| EventObjectRelation {
                    relation_id: id,
                    event_id: id,
                    object_id: 10,
                    qualifier: "case".to_owned(),
                })
                .collect(),
            ..CanonicalLog::default()
        }
    }

    fn source(root: &std::path::Path) -> DuckDbParquetSource {
        let catalog = root.join("externally-provisioned.duckdb");
        drop(Connection::open(&catalog).expect("provision test catalog"));
        DuckDbParquetSource {
            database: DuckDbDatabase::Existing {
                path: catalog,
                read_only: true,
            },
            location: ParquetLocation::Local {
                root: root.to_path_buf(),
            },
            snapshot: SnapshotSelection::Current {
                pointer: "CURRENT".to_owned(),
            },
            layout: crate::ParquetLayout::CanonicalV1,
            cache: ParquetCachePolicy::Direct,
            validation: SourceValidationPolicy::Strict,
            credentials: None,
            options: DuckDbOptions {
                connection_pool_size: 2,
                ..DuckDbOptions::default()
            },
        }
    }


    /// Build an `EntityLinkSnapshotV1` fixture whose `case_event_group` exercises every membership
    /// shape the flattened join has to reproduce.
    fn write_entity_link_fixture(root: &std::path::Path) {
        let conn = Connection::open_in_memory().expect("fixture connection");
        let q = |sql: String| conn.execute_batch(&sql).expect("fixture sql");
        let out = |name: &str| root.join(name).to_string_lossy().replace('\'', "''");

        q(format!(
            "CREATE TABLE g(tenant_id INTEGER, case_id BIGINT, timestamp TIMESTAMP, \
             system_note_ids UBIGINT[], time_rank BIGINT);
             INSERT INTO g VALUES
               -- the same member twice INSIDE one list: `list_position` took the first, so the
               -- flattened form must not emit one row per occurrence
               (7, 1, TIMESTAMP '2026-01-01 00:00:00', [7,7],     2),
               -- list order deliberately the reverse of id order
               (7, 2, TIMESTAMP '2026-01-02 00:00:00', [30,20,10], 5),
               -- a SECOND group row on the same key sharing member 20: this fan-out is real and
               -- must survive
               (7, 2, TIMESTAMP '2026-01-02 00:00:00', [20,99],    7),
               (7, 3, TIMESTAMP '2026-01-03 00:00:00', [],         1),
               (7, 4, TIMESTAMP '2026-01-04 00:00:00', NULL,       1),
               -- another tenant, which the view must not see at all
               (99, 5, TIMESTAMP '2026-01-05 00:00:00', [55],      9);
             COPY g TO '{}' (FORMAT parquet);",
            out("case_event_group.parquet")
        ));

        q(format!(
            "CREATE TABLE e(tenant_id INTEGER, case_id BIGINT, timestamp TIMESTAMP, \
             activity_id VARCHAR, object_type VARCHAR, display VARCHAR, object_id BIGINT);
             INSERT INTO e VALUES
               (7, 1, TIMESTAMP '2026-01-01 00:00:00', 'a', 'T', '{{\"system_note_id\":\"7\"}}', 1),
               (7, 2, TIMESTAMP '2026-01-02 00:00:00', 'a', 'T', '{{\"system_note_id\":\"10\"}}', 2),
               (7, 2, TIMESTAMP '2026-01-02 00:00:00', 'b', 'T', '{{\"system_note_id\":\"20\"}}', 2),
               (7, 2, TIMESTAMP '2026-01-02 00:00:00', 'c', 'T', '{{\"system_note_id\":\"30\"}}', 2),
               -- present in no list on its key
               (7, 2, TIMESTAMP '2026-01-02 00:00:00', 'd', 'T', '{{\"system_note_id\":\"77\"}}', 2),
               (7, 3, TIMESTAMP '2026-01-03 00:00:00', 'a', 'T', '{{\"system_note_id\":\"8\"}}', 3),
               (7, 4, TIMESTAMP '2026-01-04 00:00:00', 'a', 'T', '{{\"system_note_id\":\"9\"}}', 4),
               -- no group row at all on this key
               (7, 6, TIMESTAMP '2026-01-06 00:00:00', 'a', 'T', '{{\"system_note_id\":\"60\"}}', 6);
             COPY e TO '{}' (FORMAT parquet);",
            out("event_log.parquet")
        ));

        q(format!(
            "CREATE TABLE t(tenant_id INTEGER, id BIGINT, type VARCHAR, status VARCHAR, \
             currency VARCHAR, subsidiary VARCHAR, trandate VARCHAR, recordtype VARCHAR, \
             customform VARCHAR, entity VARCHAR, postingperiod VARCHAR, lastmodifiedby VARCHAR);
             INSERT INTO t VALUES
               (7,1,'T','','','','','','','','',''), (7,2,'T','','','','','','','','',''),
               (7,3,'T','','','','','','','','',''), (7,4,'T','','','','','','','','',''),
               (7,6,'T','','','','','','','','','');
             COPY t TO '{}' (FORMAT parquet);",
            out("txn.parquet")
        ));

        q(format!(
            "CREATE TABLE l(tenant_id INTEGER, source_object_id BIGINT, source_object_type VARCHAR, \
             target_object_id BIGINT, target_object_type VARCHAR, source_timestamp TIMESTAMP, \
             target_timestamp TIMESTAMP);
             COPY l TO '{}' (FORMAT parquet);",
            out("object_link.parquet")
        ));

        // The entity-link path reads the manifest only for its bytes (a provenance hash) and an
        // optional `freezeT`, so a minimal document is enough.
        fs::write(root.join("manifest.json"), b"{}").expect("write manifest");
    }

    fn entity_link_source(root: &std::path::Path) -> DuckDbParquetSource {
        let catalog = root.join("entity-link.duckdb");
        drop(Connection::open(&catalog).expect("provision catalog"));
        DuckDbParquetSource {
            database: DuckDbDatabase::Existing { path: catalog, read_only: false },
            location: ParquetLocation::Local { root: root.to_path_buf() },
            snapshot: SnapshotSelection::Root,
            layout: crate::ParquetLayout::EntityLinkSnapshotV1(crate::EntityLinkSnapshotV1 {
                event_log_file: "event_log.parquet".to_owned(),
                object_file: "txn.parquet".to_owned(),
                object_link_file: "object_link.parquet".to_owned(),
                event_group_file: "case_event_group.parquet".to_owned(),
                dataset_id: "fixture".to_owned(),
                tenant_id: 7,
                timestamp_policy: crate::SourceTimestampPolicy::Utc,
            }),
            cache: ParquetCachePolicy::Direct,
            validation: SourceValidationPolicy::Balanced,
            credentials: None,
            options: DuckDbOptions { connection_pool_size: 1, ..DuckDbOptions::default() },
        }
    }

    /// The flattened membership join must reproduce `list_contains` + `list_position` EXACTLY,
    /// including the cases where the two could plausibly diverge. Written because there was no
    /// coverage of `EntityLinkSnapshotV1` at all: every other test here exercises `CanonicalV1`.
    #[test]
    fn entity_link_membership_join_matches_the_list_predicate_it_replaces() {
        let directory = TestDirectory::new();
        write_entity_link_fixture(&directory.0);
        let provider =
            DuckDbProvider::open(entity_link_source(&directory.0)).expect("open provider");

        let rows: Vec<(u64, u64)> = provider
            .with_connection(|conn| {
                let mut stmt = conn.prepare("SELECT event_id, sequence FROM ocpm_events ORDER BY event_id, sequence")?;
                let mut out = Vec::new();
                let mut q = stmt.query([])?;
                while let Some(row) = q.next()? {
                    out.push((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?));
                }
                Ok(out)
            })
            .expect("read ocpm_events");

        // A duplicate INSIDE one list matched once before and must match once now, at the FIRST
        // ordinal — this is the case a naive flatten gets wrong.
        assert_eq!(
            rows.iter().filter(|(id, _)| *id == 7).collect::<Vec<_>>(),
            vec![&(7u64, 2_000_001u64)],
            "a member repeated inside one list must yield exactly one row at its first ordinal"
        );

        // Two DISTINCT group rows sharing a member fan out, and that is not a defect to collapse.
        assert_eq!(
            rows.iter().filter(|(id, _)| *id == 20).count(),
            2,
            "a member present in two separate group rows must still fan out"
        );

        // Reverse list order is honoured positionally, not by id order.
        assert!(rows.contains(&(30, 5_000_001)), "30 is first in its list");
        assert!(rows.contains(&(10, 5_000_003)), "10 is third in its list");

        // No membership, empty list, NULL list, and no group row at all all fall through the LEFT
        // JOIN to sequence 0 rather than dropping the event.
        for id in [77u64, 8, 9, 60] {
            assert!(rows.contains(&(id, 0)), "event {id} must survive with sequence 0");
        }

        // The other tenant's group row is not visible, so its member never appears.
        assert!(!rows.iter().any(|(id, _)| *id == 55), "another tenant must not leak");
    }

    /// The semantics test above passes against BOTH the old and new forms, because the old form is
    /// the reference semantics — it is slow, not wrong. This is the assertion that actually
    /// distinguishes them: a list predicate in the ON clause makes DuckDB discard the equi-keys
    /// and plan a nested loop, which on a production-sized snapshot does not complete.
    #[test]
    fn entity_link_membership_join_is_planned_as_an_equi_join() {
        let directory = TestDirectory::new();
        write_entity_link_fixture(&directory.0);
        let provider =
            DuckDbProvider::open(entity_link_source(&directory.0)).expect("open provider");

        let plan: String = provider
            .with_connection(|conn| {
                let mut stmt = conn.prepare("EXPLAIN SELECT count(*) FROM ocpm_events")?;
                let mut rows = stmt.query([])?;
                let mut out = String::new();
                while let Some(row) = rows.next()? {
                    out.push_str(&row.get::<_, String>(1)?);
                    out.push('\n');
                }
                Ok(out)
            })
            .expect("explain ocpm_events");

        assert!(
            !plan.contains("BLOCKWISE_NL_JOIN"),
            "membership must not be tested inside the join condition — that plans a nested loop:\n{plan}"
        );
        assert!(plan.contains("HASH_JOIN"), "expected an equi-join plan:\n{plan}");
        assert!(
            plan.contains("tenant_id"),
            "the tenant predicate must stay on the parquet scan so row groups can be pruned:\n{plan}"
        );
    }

    #[test]
    fn canonical_parquet_round_trip_preserves_engine_semantics() {
        let directory = TestDirectory::new();
        write_canonical_snapshot(&log(), &directory.0, "v1").expect("write snapshot");
        let provider = DuckDbProvider::open(source(&directory.0)).expect("open provider");

        let profile = provider.profile(&DatasetView::default()).expect("profile");
        assert_eq!(profile.event_count, 3);
        assert_eq!(profile.object_count, 1);
        assert_eq!(profile.e2o_count, 3);

        let view = DatasetView {
            object_types: vec!["Order".to_owned()],
            qualifiers: vec!["case".to_owned()],
            statuses: vec!["open".to_owned()],
            ..DatasetView::default()
        };
        let executions = provider
            .process_executions(&view, ExecutionMode::LeadingObject, Some("Order"))
            .expect("executions");
        assert_eq!(executions.len(), 1);
        assert_eq!(executions[0].activity_path(), vec!["create", "approve"]);

        let summary = provider
            .execution_summary(&ExecutionSummaryRequest {
                view: DatasetView {
                    object_types: vec!["Order".to_owned()],
                    ..DatasetView::default()
                },
                leading_object_type: Some("Order".to_owned()),
                complete_lifecycle: false,
            })
            .expect("summary")
            .expect("accelerated summary");
        assert_eq!(summary.case_count, 1);
        assert_eq!(summary.event_count, 3);
        assert_eq!(summary.dfg.len(), 2);

        let result = provider
            .query(&QueryRequest {
                semantic_version: "1.0".to_owned(),
                view: DatasetView {
                    object_types: vec!["Order".to_owned()],
                    ..DatasetView::default()
                },
                constraint: Constraint::DirectlyFollows {
                    source: "create".to_owned(),
                    target: "approve".to_owned(),
                },
                limit: 100,
            })
            .expect("query");
        assert_eq!(result.total_matches, 1);
        assert!(!result.truncated);
    }

    #[test]
    fn published_snapshot_versions_are_immutable() {
        let directory = TestDirectory::new();
        write_canonical_snapshot(&log(), &directory.0, "v1").expect("first write");
        let error = write_canonical_snapshot(&log(), &directory.0, "v1")
            .expect_err("version overwrite must fail");
        assert!(error.to_string().contains("already exists"));
    }

    #[test]
    fn existing_catalog_mode_never_creates_a_database() {
        let directory = TestDirectory::new();
        write_canonical_snapshot(&log(), &directory.0, "v1").expect("write snapshot");
        let missing = directory.0.join("caller-must-provision.duckdb");
        let mut configured = source(&directory.0);
        configured.database = DuckDbDatabase::Existing {
            path: missing.clone(),
            read_only: false,
        };

        let error = DuckDbProvider::open(configured)
            .err()
            .expect("missing catalog must fail");
        assert!(error.to_string().contains("does not exist"));
        assert!(!missing.exists());
    }
}
