use std::{
    mem::size_of,
    sync::Arc,
    time::Duration,
};

use tokio::sync::Barrier;

const CONCURRENCY_LEVELS: [usize; 4] = [1, 4, 8, 16];
const CONCURRENCY_EPOCHS: usize = 3;
const CONCURRENCY_MINIMUM_REQUESTS_PER_CLIENT: usize = 32;
const CONCURRENCY_MAXIMUM_REQUESTS_PER_CLIENT: usize = 250_000;
const CONCURRENCY_MINIMUM_SECONDS: f64 = 5.0;

#[derive(Clone, Debug, Serialize)]
struct ReleaseReport {
    pg_ocpm: String,
    ocpm_engine: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct EnvironmentReport {
    benchmark_host_id: String,
    source_revision: String,
    source_tree_clean: bool,
    source_tree_id: String,
    candidate_runner_sha256: String,
    candidate_image: String,
    candidate_image_id: String,
    database_image: String,
    database_image_id: String,
    pg_ocpm_source_revision: String,
    pg_ocpm_source_tree_clean: bool,
    postgres_server_version: String,
    postgres_server_version_num: i32,
    postgres_jit: String,
    client_os: &'static str,
    client_arch: &'static str,
    client_logical_cpus_visible: usize,
    tokio_worker_threads_env: Option<String>,
    process_run_id: String,
    container_hostname: String,
    client_process_id: u32,
    same_docker_host_as_reference: bool,
    same_harness_revision_state_as_reference: bool,
    provenance_complete: bool,
}

fn environment_bool(name: &str) -> Result<bool, String> {
    match std::env::var(name) {
        Ok(value) if value == "true" => Ok(true),
        Ok(value) if value == "false" => Ok(false),
        Ok(value) => Err(format!("{name} must be true or false; found {value:?}")),
        Err(error) => Err(format!("publication provenance requires {name}: {error}")),
    }
}

fn required_environment(name: &str) -> Result<String, String> {
    let value = std::env::var(name)
        .map_err(|error| format!("publication provenance requires {name}: {error}"))?;
    if value.trim().is_empty() {
        return Err(format!("publication provenance requires nonempty {name}"));
    }
    Ok(value)
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

async fn release_environment(
    client: &Client,
    reference: &ReferenceArtifact,
) -> Result<(ReleaseReport, EnvironmentReport), String> {
    let row = client
        .query_one(
            "SELECT ocpm.version(),current_setting('server_version'), \
             current_setting('server_version_num')::integer,current_setting('jit')",
            &[],
        )
        .await
        .map_err(|error| error.to_string())?;
    let benchmark_host_id = required_environment("OCPM_BENCHMARK_HOST_ID")?;
    let source_revision = required_environment("OCPM_SOURCE_REVISION")?;
    let source_tree_clean = environment_bool("OCPM_SOURCE_TREE_CLEAN")?;
    let source_tree_id = required_environment("OCPM_SOURCE_TREE_ID")?;
    let candidate_runner_sha256 = required_environment("OCPM_CANDIDATE_RUNNER_SHA256")?;
    let candidate_image = required_environment("OCPM_CANDIDATE_IMAGE")?;
    let candidate_image_id = required_environment("OCPM_CANDIDATE_IMAGE_ID")?;
    let database_image = required_environment("OCPM_DATABASE_IMAGE")?;
    let database_image_id = required_environment("OCPM_DATABASE_IMAGE_ID")?;
    let pg_ocpm_source_revision = required_environment("OCPM_PG_OCPM_SOURCE_REVISION")?;
    let pg_ocpm_source_tree_clean = environment_bool("OCPM_PG_OCPM_SOURCE_TREE_CLEAN")?;
    if !benchmark_host_id.starts_with("sha256:")
        || !candidate_image_id.starts_with("sha256:")
        || !database_image_id.starts_with("sha256:")
        || !source_tree_id.starts_with("git-tree:")
        || !is_lowercase_sha256(&candidate_runner_sha256)
    {
        return Err("publication provenance contains an invalid host, image, tree, or runner identifier"
            .to_owned());
    }
    let same_docker_host_as_reference =
        benchmark_host_id == reference.environment.benchmark_host_id;
    let same_harness_revision_state_as_reference = source_revision
        == reference.environment.source_revision
        && source_tree_clean == reference.environment.source_tree_clean;
    if !same_docker_host_as_reference {
        return Err("candidate and strict OCPQ reference were not run on the same Docker host"
            .to_owned());
    }
    if !same_harness_revision_state_as_reference {
        return Err(
            "candidate and strict OCPQ reference harness revision/clean-state provenance differs"
                .to_owned(),
        );
    }
    let client_logical_cpus_visible = std::thread::available_parallelism()
        .map_err(|error| error.to_string())?
        .get();
    let process_run_id = required_environment("OCPM_PROCESS_RUN_ID")?;
    let container_hostname = fs::read_to_string("/etc/hostname")
        .map_err(|error| format!("could not read container hostname: {error}"))?
        .trim()
        .to_owned();
    if container_hostname.is_empty() {
        return Err("container hostname must not be empty".to_owned());
    }
    Ok((
        ReleaseReport {
            pg_ocpm: row.get(0),
            ocpm_engine: env!("CARGO_PKG_VERSION"),
        },
        EnvironmentReport {
            benchmark_host_id,
            source_revision,
            source_tree_clean,
            source_tree_id,
            candidate_runner_sha256,
            candidate_image,
            candidate_image_id,
            database_image,
            database_image_id,
            pg_ocpm_source_revision,
            pg_ocpm_source_tree_clean,
            postgres_server_version: row.get(1),
            postgres_server_version_num: row.get(2),
            postgres_jit: row.get(3),
            client_os: std::env::consts::OS,
            client_arch: std::env::consts::ARCH,
            client_logical_cpus_visible,
            same_docker_host_as_reference,
            same_harness_revision_state_as_reference,
            tokio_worker_threads_env: std::env::var("TOKIO_WORKER_THREADS").ok(),
            process_run_id,
            container_hostname,
            client_process_id: std::process::id(),
            provenance_complete: environment_bool("OCPM_PROVENANCE_COMPLETE")?,
        },
    ))
}

fn load_reference(path: &Path) -> Result<(ReferenceArtifact, String), String> {
    let bytes = fs::read(path)
        .map_err(|error| format!("could not read {}: {error}", path.display()))?;
    let sha256 = sha256_hex(&bytes);
    let reference = serde_json::from_slice(&bytes)
        .map_err(|error| format!("could not parse strict OCPQ reference: {error}"))?;
    validate_reference(&reference)?;
    Ok((reference, sha256))
}

fn validate_tree_exact(
    name: QueryName,
    tree: &OwnedTree,
    reference: &ReferenceQuery,
    external: &ExternalData,
) -> Result<Vec<NodeFingerprint>, String> {
    let fingerprints = canonical_tree(name, tree, external)?;
    if fingerprints != reference.nodes {
        return Err(format!(
            "{} every-node fingerprint differs from strict OCPQ",
            name.as_str()
        ));
    }
    let all_node_situations = fingerprints
        .iter()
        .map(|node| node.situation_count)
        .sum::<usize>();
    if all_node_situations != reference.all_node_situations {
        return Err(format!(
            "{} materialized {all_node_situations} rows across all nodes, expected {}",
            name.as_str(),
            reference.all_node_situations
        ));
    }
    if name == QueryName::Q6 {
        let (label, duration) = q6_evidence(tree, external)?;
        if Some(&label) != reference.q6_root_label.as_ref()
            || Some(duration) != reference.q6_duration_microseconds
        {
            return Err("Q6 typed label or child-derived duration differs from strict OCPQ"
                .to_owned());
        }
    }
    Ok(fingerprints)
}

async fn prepare_tree_query(
    client: &Client,
    name: QueryName,
) -> Result<PreparedBindingTreeQuery, String> {
    let query = PreparedBindingTreeQuery::prepare(client, name.sql())
        .await
        .map_err(|error| format!("{} prepare failed: {error}", name.as_str()))?;
    if query.node_count() != name.specs().len() {
        return Err(format!(
            "{} prepared {} bytea nodes, expected {}",
            name.as_str(),
            query.node_count(),
            name.specs().len()
        ));
    }
    Ok(query)
}

async fn prepare_tree_query_set(
    client: &Client,
) -> Result<Vec<PreparedBindingTreeQuery>, String> {
    let mut queries = Vec::with_capacity(QueryName::ALL.len());
    for name in QueryName::ALL {
        queries.push(prepare_tree_query(client, name).await?);
    }
    Ok(queries)
}

async fn execute_tree(
    client: &Client,
    query: &PreparedBindingTreeQuery,
    name: QueryName,
) -> Result<OwnedTree, String> {
    let result = query
        .execute(client, &[])
        .await
        .map_err(|error| format!("{} execution failed: {error}", name.as_str()))?;
    materialize_tree(name, result)
}

fn owned_tree_allocated_bytes(tree: &OwnedTree) -> usize {
    fn id_rows<const N: usize>(rows: &Vec<OwnedIdRow<N>>) -> usize {
        size_of::<Vec<OwnedIdRow<N>>>() + rows.capacity() * size_of::<OwnedIdRow<N>>()
    }
    fn rows_bytes(rows: &OwnedRows) -> usize {
        match rows {
            OwnedRows::Id1(rows) => id_rows(rows),
            OwnedRows::Id2(rows) => id_rows(rows),
            OwnedRows::Id3(rows) => id_rows(rows),
            OwnedRows::Id4(rows) => id_rows(rows),
            OwnedRows::Id5(rows) => id_rows(rows),
            OwnedRows::Duration(rows) => {
                size_of::<Vec<OwnedDurationRow>>()
                    + rows.capacity() * size_of::<OwnedDurationRow>()
                    + rows
                        .iter()
                        .map(|row| {
                            row.label_name.capacity()
                                + row.label_type.capacity()
                                + row.label_value.capacity()
                        })
                        .sum::<usize>()
            }
        }
    }
    size_of::<OwnedTree>()
        + tree.nodes.capacity() * size_of::<OwnedNode>()
        + tree
            .nodes
            .iter()
            .map(|node| {
                node.object_variables.capacity() * size_of::<usize>()
                    + node.event_variables.capacity() * size_of::<usize>()
                    + node.label_names.capacity() * size_of::<String>()
                    + node
                        .label_names
                        .iter()
                        .map(String::capacity)
                        .sum::<usize>()
                    + rows_bytes(&node.rows)
            })
            .sum::<usize>()
}

#[derive(Clone, Copy, Debug)]
struct ProcessMemorySnapshot {
    rss_bytes: u64,
    vmhwm_bytes: u64,
}

fn proc_status_bytes(status: &str, field: &str) -> Result<u64, String> {
    let line = status
        .lines()
        .find(|line| line.starts_with(field))
        .ok_or_else(|| format!("/proc/self/status is missing {field}"))?;
    let mut values = line.split_whitespace();
    if values.next() != Some(field) {
        return Err(format!("invalid {field} entry in /proc/self/status"));
    }
    let kibibytes = values
        .next()
        .ok_or_else(|| format!("{field} has no value"))?
        .parse::<u64>()
        .map_err(|_| format!("{field} is not an integer"))?;
    if values.next() != Some("kB") || values.next().is_some() {
        return Err(format!("{field} does not use the expected kB unit"));
    }
    kibibytes
        .checked_mul(1024)
        .ok_or_else(|| format!("{field} byte count overflowed"))
}

fn process_memory_snapshot() -> Result<ProcessMemorySnapshot, String> {
    let status = fs::read_to_string("/proc/self/status")
        .map_err(|error| format!("Linux /proc/self/status is required: {error}"))?;
    Ok(ProcessMemorySnapshot {
        rss_bytes: proc_status_bytes(&status, "VmRSS:")?,
        vmhwm_bytes: proc_status_bytes(&status, "VmHWM:")?,
    })
}

fn memory_delta(after: u64, before: u64) -> Result<i64, String> {
    let after = i64::try_from(after).map_err(|_| "memory value exceeds i64".to_owned())?;
    let before = i64::try_from(before).map_err(|_| "memory value exceeds i64".to_owned())?;
    Ok(after - before)
}

async fn postgres_backend_memory(client: &Client) -> Result<i64, String> {
    Ok(client
        .query_one(
            "SELECT coalesce(sum(total_bytes),0)::bigint FROM pg_backend_memory_contexts",
            &[],
        )
        .await
        .map_err(|error| error.to_string())?
        .get(0))
}

#[derive(Debug, Serialize)]
struct MemoryOnlyArtifact {
    schema_version: u8,
    generated_at_unix_ms: u128,
    reference_schema_version: u8,
    reference_artifact_sha256: String,
    reference_source: ReferenceSource,
    reference_environment: ReferenceEnvironment,
    release: ReleaseReport,
    environment: EnvironmentReport,
    mode: &'static str,
    query: &'static str,
    measurement_boundary: &'static str,
    correctness_boundary: &'static str,
    all_node_situations: usize,
    node_situation_counts: Vec<usize>,
    capsule_bytes: usize,
    owned_tree_allocated_bytes: usize,
    baseline_rss_bytes: u64,
    baseline_vmhwm_bytes: u64,
    after_rss_bytes: u64,
    after_vmhwm_bytes: u64,
    rss_delta_bytes: i64,
    vmhwm_delta_bytes: i64,
    peak_over_baseline_rss_bytes: u64,
    peak_over_baseline_vmhwm_bytes: u64,
    postgres_backend_memory_scope: &'static str,
    postgres_backend_baseline_bytes: i64,
    postgres_backend_after_bytes: i64,
    postgres_backend_retained_delta_bytes: i64,
    nodes: Vec<NodeFingerprint>,
    every_node_exact: bool,
}

async fn run_memory_only(args: &Args) -> Result<(), String> {
    let name = args
        .memory_only_query
        .expect("memory-only dispatch requires a query");
    let (reference, reference_artifact_sha256) = load_reference(&args.reference)?;
    let client = connect(&args.database_url).await?;
    let (release, environment) = release_environment(&client, &reference).await?;
    let query = prepare_tree_query(&client, name).await?;

    let postgres_backend_baseline_bytes = postgres_backend_memory(&client).await?;
    let baseline = process_memory_snapshot()?;
    let tree = execute_tree(&client, &query, name).await?;
    black_box(&tree);
    let after = process_memory_snapshot()?;
    let postgres_backend_after_bytes = postgres_backend_memory(&client).await?;
    let node_situation_counts = tree
        .nodes
        .iter()
        .map(|node| node.rows.len())
        .collect::<Vec<_>>();
    let all_node_situations = node_situation_counts.iter().sum();
    let owned_tree_allocated_bytes = owned_tree_allocated_bytes(&tree);
    let external = load_external_data(&client).await?;
    let nodes = validate_tree_exact(
        name,
        &tree,
        &reference.queries[name.as_str()],
        &external,
    )?;
    let artifact = MemoryOnlyArtifact {
        schema_version: 1,
        generated_at_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .as_millis(),
        reference_schema_version: reference.schema_version,
        reference_artifact_sha256,
        reference_source: reference.source,
        reference_environment: reference.environment,
        release,
        environment,
        mode: "fresh-process-per-query-peak-rss-vmhwm",
        query: name.as_str(),
        measurement_boundary: "fresh Linux process: baseline after connection, provenance query, and one prepared statement; then one PostgreSQL generation/fetch, every OCPB node decode, and complete owned-tree materialization; RSS and VmHWM sampled while the entire tree remains live and before external-ID maps or canonicalization are loaded",
        correctness_boundary: "after the memory sample, load external-ID maps and require duplicate-preserving canonical JSON SHA-256 and manifest parity for every node, including exact violations and Q6 typed label/child-derived duration",
        all_node_situations,
        node_situation_counts,
        capsule_bytes: tree.capsule_bytes,
        owned_tree_allocated_bytes,
        baseline_rss_bytes: baseline.rss_bytes,
        baseline_vmhwm_bytes: baseline.vmhwm_bytes,
        after_rss_bytes: after.rss_bytes,
        after_vmhwm_bytes: after.vmhwm_bytes,
        rss_delta_bytes: memory_delta(after.rss_bytes, baseline.rss_bytes)?,
        vmhwm_delta_bytes: memory_delta(after.vmhwm_bytes, baseline.vmhwm_bytes)?,
        peak_over_baseline_rss_bytes: after.vmhwm_bytes.saturating_sub(baseline.rss_bytes),
        peak_over_baseline_vmhwm_bytes: after.vmhwm_bytes.saturating_sub(baseline.vmhwm_bytes),
        postgres_backend_memory_scope: "non-peak retained-memory diagnostic from sum(total_bytes) in pg_backend_memory_contexts on the same PostgreSQL connection, sampled before and after the candidate request; client VmHWM is the peak-memory gate",
        postgres_backend_baseline_bytes,
        postgres_backend_after_bytes,
        postgres_backend_retained_delta_bytes: postgres_backend_after_bytes
            - postgres_backend_baseline_bytes,
        nodes,
        every_node_exact: true,
    };
    write_artifact(&args.output, &artifact)
}

#[derive(Debug, Serialize)]
struct RelationStorageReport {
    total_bytes: i64,
    index_bytes: i64,
    heap_toast_fsm_vm_bytes: i64,
}

#[derive(Debug, Serialize)]
struct StorageReport {
    scope: &'static str,
    database_bytes_diagnostic: i64,
    total_serving_bytes: i64,
    index_bytes: i64,
    heap_toast_fsm_vm_bytes: i64,
    binding_summary_bytes: i64,
    result_cache_rows: i64,
    request_result_cache_enabled: bool,
    relations: BTreeMap<String, RelationStorageReport>,
}

async fn storage(client: &Client) -> Result<StorageReport, String> {
    let rows = client
        .query(
            "SELECT c.relname::text,pg_total_relation_size(c.oid)::bigint, \
             pg_indexes_size(c.oid)::bigint \
             FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace \
             WHERE n.nspname='ocpm' AND c.relkind IN ('r','m','p') \
             ORDER BY c.relname",
            &[],
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut total_serving_bytes = 0_i64;
    let mut index_bytes = 0_i64;
    let mut binding_summary_bytes = 0_i64;
    let mut relations = BTreeMap::new();
    for row in rows {
        let name = row.get::<_, String>(0);
        let total = row.get::<_, i64>(1);
        let indexes = row.get::<_, i64>(2);
        total_serving_bytes += total;
        index_bytes += indexes;
        if name.starts_with("binding_") {
            binding_summary_bytes += total;
        }
        relations.insert(
            name,
            RelationStorageReport {
                total_bytes: total,
                index_bytes: indexes,
                heap_toast_fsm_vm_bytes: total - indexes,
            },
        );
    }
    let database_bytes_diagnostic = client
        .query_one("SELECT pg_database_size(current_database())::bigint", &[])
        .await
        .map_err(|error| error.to_string())?
        .get(0);
    let result_cache_rows = client
        .query_one("SELECT count(*)::bigint FROM ocpm.result_cache", &[])
        .await
        .map_err(|error| error.to_string())?
        .get(0);
    Ok(StorageReport {
        scope: "sum of pg_total_relation_size and pg_indexes_size for every ordinary, materialized-view, and partitioned relation in schema ocpm; database size is reported separately as a diagnostic",
        database_bytes_diagnostic,
        total_serving_bytes,
        index_bytes,
        heap_toast_fsm_vm_bytes: total_serving_bytes - index_bytes,
        binding_summary_bytes,
        result_cache_rows,
        request_result_cache_enabled: false,
        relations,
    })
}

struct PreparedConcurrencyClient {
    client_id: usize,
    client: Client,
    queries: Vec<PreparedBindingTreeQuery>,
}

async fn verify_query_set(
    prepared: &PreparedConcurrencyClient,
    reference: &ReferenceArtifact,
    external: &ExternalData,
) -> Result<[usize; 7], String> {
    let mut capsule_bytes = [0_usize; 7];
    for name in QueryName::ALL {
        let tree = execute_tree(
            &prepared.client,
            &prepared.queries[name.index()],
            name,
        )
        .await?;
        validate_tree_exact(name, &tree, &reference.queries[name.as_str()], external)?;
        capsule_bytes[name.index()] = tree.capsule_bytes;
    }
    Ok(capsule_bytes)
}

struct ConcurrencySamples {
    prepared: PreparedConcurrencyClient,
    latencies_ns: Vec<u64>,
    query_counts: [usize; 7],
}

struct ConcurrencyClientConfig {
    minimum_requests: usize,
    minimum_duration: Duration,
    expected_capsule_bytes: [usize; 7],
    expected_node_rows: Arc<Vec<Vec<usize>>>,
    ready: Arc<Barrier>,
    start: Arc<Barrier>,
}

async fn run_concurrency_client(
    prepared: PreparedConcurrencyClient,
    config: ConcurrencyClientConfig,
) -> Result<ConcurrencySamples, String> {
    config.ready.wait().await;
    config.start.wait().await;
    let epoch_started = Instant::now();
    let mut latencies_ns = Vec::with_capacity(config.minimum_requests);
    let mut query_counts = [0_usize; 7];
    let mut request = 0_usize;
    while request < config.minimum_requests || epoch_started.elapsed() < config.minimum_duration {
        if request >= CONCURRENCY_MAXIMUM_REQUESTS_PER_CLIENT {
            return Err(format!(
                "concurrency client {} exceeded the raw-evidence limit of {} requests before the epoch duration elapsed",
                prepared.client_id, CONCURRENCY_MAXIMUM_REQUESTS_PER_CLIENT
            ));
        }
        let name = QueryName::ALL[(prepared.client_id + request) % QueryName::ALL.len()];
        let started = Instant::now();
        let tree = execute_tree(
            &prepared.client,
            &prepared.queries[name.index()],
            name,
        )
        .await?;
        black_box(&tree);
        let elapsed_ns = u64::try_from(started.elapsed().as_nanos())
            .map_err(|_| "timed concurrency latency exceeds u64 nanoseconds".to_owned())?;
        if elapsed_ns == 0 {
            return Err("timed concurrency latency had zero nanosecond resolution".to_owned());
        }
        if tree.capsule_bytes != config.expected_capsule_bytes[name.index()]
            || tree.nodes.len() != config.expected_node_rows[name.index()].len()
            || tree
                .nodes
                .iter()
                .zip(&config.expected_node_rows[name.index()])
                .any(|(node, expected)| node.rows.len() != *expected)
        {
            return Err(format!("{} timed concurrency result shape changed", name.as_str()));
        }
        latencies_ns.push(elapsed_ns);
        query_counts[name.index()] += 1;
        request += 1;
    }
    Ok(ConcurrencySamples {
        prepared,
        latencies_ns,
        query_counts,
    })
}

#[derive(Debug, Serialize)]
struct ConcurrencyEpochReport {
    epoch: usize,
    client_ids: Vec<usize>,
    pre_epoch_exact_query_checks_per_client: usize,
    post_epoch_exact_query_checks_per_client: usize,
    request_count: usize,
    client_request_counts: BTreeMap<String, usize>,
    query_request_counts: BTreeMap<String, usize>,
    client_request_latencies_ns: BTreeMap<String, Vec<u64>>,
    wall_time_ms: f64,
    throughput_requests_per_second: f64,
    latency_p50_ms: f64,
    latency_p95_ms: f64,
    latency_p99_ms: f64,
    every_pre_and_post_node_exact: bool,
}

#[derive(Debug, Serialize)]
struct ConcurrencyLevelReport {
    clients: usize,
    epoch_count: usize,
    minimum_requests_per_client: usize,
    maximum_requests_per_client: usize,
    minimum_wall_time_ms: f64,
    total_request_count: usize,
    total_query_request_counts: BTreeMap<String, usize>,
    median_epoch_throughput_requests_per_second: f64,
    minimum_epoch_throughput_requests_per_second: f64,
    maximum_epoch_throughput_requests_per_second: f64,
    epoch_throughput_cv: f64,
    median_epoch_latency_p50_ms: f64,
    median_epoch_latency_p95_ms: f64,
    median_epoch_latency_p99_ms: f64,
    epochs: Vec<ConcurrencyEpochReport>,
    every_pre_and_post_node_exact: bool,
}

async fn concurrency(
    database_url: &str,
    reference: &ReferenceArtifact,
    external: &ExternalData,
) -> Result<BTreeMap<String, ConcurrencyLevelReport>, String> {
    let expected_node_rows = Arc::new(
        QueryName::ALL
            .iter()
            .map(|name| {
                reference.queries[name.as_str()]
                    .nodes
                    .iter()
                    .map(|node| node.situation_count)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>(),
    );
    let minimum_duration = Duration::from_secs_f64(CONCURRENCY_MINIMUM_SECONDS);
    let mut reports = BTreeMap::new();
    for clients in CONCURRENCY_LEVELS {
        let mut epoch_reports = Vec::with_capacity(CONCURRENCY_EPOCHS);
        let mut total_query_counts = [0_usize; 7];
        for epoch in 1..=CONCURRENCY_EPOCHS {
            eprintln!(
                "strict concurrency: {clients} clients, epoch {epoch}/{CONCURRENCY_EPOCHS}, >= {CONCURRENCY_MINIMUM_SECONDS:.1}s and >= {CONCURRENCY_MINIMUM_REQUESTS_PER_CLIENT} requests/client"
            );
            let mut prepared_clients = Vec::with_capacity(clients);
            let mut common_capsule_bytes = None;
            for client_id in 0..clients {
                let client = connect(database_url).await?;
                let queries = prepare_tree_query_set(&client).await?;
                let prepared = PreparedConcurrencyClient {
                    client_id,
                    client,
                    queries,
                };
                let capsule_bytes = verify_query_set(&prepared, reference, external).await?;
                if common_capsule_bytes.is_some_and(|expected| expected != capsule_bytes) {
                    return Err("candidate capsule sizes differ between concurrency clients"
                        .to_owned());
                }
                common_capsule_bytes = Some(capsule_bytes);
                prepared_clients.push(prepared);
            }
            let expected_capsule_bytes =
                common_capsule_bytes.expect("every concurrency level has at least one client");
            let ready = Arc::new(Barrier::new(clients + 1));
            let start = Arc::new(Barrier::new(clients + 1));
            let mut handles = Vec::with_capacity(clients);
            for prepared in prepared_clients {
                handles.push(tokio::spawn(run_concurrency_client(
                    prepared,
                    ConcurrencyClientConfig {
                        minimum_requests: CONCURRENCY_MINIMUM_REQUESTS_PER_CLIENT,
                        minimum_duration,
                        expected_capsule_bytes,
                        expected_node_rows: Arc::clone(&expected_node_rows),
                        ready: Arc::clone(&ready),
                        start: Arc::clone(&start),
                    },
                )));
            }
            ready.wait().await;
            let wall_started = Instant::now();
            start.wait().await;
            let mut completed = Vec::with_capacity(clients);
            for handle in handles {
                completed.push(
                    handle
                        .await
                        .map_err(|error| format!("concurrency client task failed: {error}"))??,
                );
            }
            let wall_time_ms = wall_started.elapsed().as_secs_f64() * 1000.0;
            if wall_time_ms < CONCURRENCY_MINIMUM_SECONDS * 1000.0 {
                return Err(format!(
                    "concurrency level {clients} epoch {epoch} ran for only {wall_time_ms:.3} ms"
                ));
            }
            for samples in &completed {
                let post_capsule_bytes =
                    verify_query_set(&samples.prepared, reference, external).await?;
                if post_capsule_bytes != expected_capsule_bytes {
                    return Err("candidate capsule sizes changed after a concurrency epoch"
                        .to_owned());
                }
            }
            let mut latencies_ns = Vec::new();
            let mut query_counts = [0_usize; 7];
            let mut client_request_counts = BTreeMap::new();
            let mut client_request_latencies_ns = BTreeMap::new();
            let mut client_ids = Vec::with_capacity(clients);
            for samples in completed {
                if samples.latencies_ns.len() < CONCURRENCY_MINIMUM_REQUESTS_PER_CLIENT {
                    return Err(format!(
                        "concurrency client {} completed only {} requests",
                        samples.prepared.client_id,
                        samples.latencies_ns.len()
                    ));
                }
                let client_id = samples.prepared.client_id;
                client_ids.push(client_id);
                client_request_counts.insert(
                    client_id.to_string(),
                    samples.latencies_ns.len(),
                );
                latencies_ns.extend(samples.latencies_ns.iter().copied());
                client_request_latencies_ns
                    .insert(client_id.to_string(), samples.latencies_ns);
                for (total, count) in query_counts.iter_mut().zip(samples.query_counts) {
                    *total += count;
                }
            }
            client_ids.sort_unstable();
            if client_ids != (0..clients).collect::<Vec<_>>()
                || query_counts.iter().any(|count| *count == 0)
            {
                return Err("concurrency client or query coverage is incomplete".to_owned());
            }
            for (total, count) in total_query_counts.iter_mut().zip(query_counts) {
                *total += count;
            }
            let request_count = latencies_ns.len();
            let latencies_ms = latencies_ns
                .iter()
                .map(|latency| *latency as f64 / 1_000_000.0)
                .collect::<Vec<_>>();
            let query_request_counts = QueryName::ALL
                .iter()
                .zip(query_counts)
                .map(|(name, count)| (name.as_str().to_owned(), count))
                .collect();
            epoch_reports.push(ConcurrencyEpochReport {
                epoch,
                client_ids,
                pre_epoch_exact_query_checks_per_client: QueryName::ALL.len(),
                post_epoch_exact_query_checks_per_client: QueryName::ALL.len(),
                request_count,
                client_request_counts,
                query_request_counts,
                client_request_latencies_ns,
                wall_time_ms,
                throughput_requests_per_second: request_count as f64 * 1000.0 / wall_time_ms,
                latency_p50_ms: median(&latencies_ms),
                latency_p95_ms: percentile(&latencies_ms, 0.95),
                latency_p99_ms: percentile(&latencies_ms, 0.99),
                every_pre_and_post_node_exact: true,
            });
        }
        let throughputs = epoch_reports
            .iter()
            .map(|report| report.throughput_requests_per_second)
            .collect::<Vec<_>>();
        let throughput_mean = mean(&throughputs);
        let throughput_cv = (throughputs
            .iter()
            .map(|value| (value - throughput_mean).powi(2))
            .sum::<f64>()
            / throughputs.len() as f64)
            .sqrt()
            / throughput_mean;
        let p50s = epoch_reports
            .iter()
            .map(|report| report.latency_p50_ms)
            .collect::<Vec<_>>();
        let p95s = epoch_reports
            .iter()
            .map(|report| report.latency_p95_ms)
            .collect::<Vec<_>>();
        let p99s = epoch_reports
            .iter()
            .map(|report| report.latency_p99_ms)
            .collect::<Vec<_>>();
        let total_query_request_counts = QueryName::ALL
            .iter()
            .zip(total_query_counts)
            .map(|(name, count)| (name.as_str().to_owned(), count))
            .collect();
        reports.insert(
            clients.to_string(),
            ConcurrencyLevelReport {
                clients,
                epoch_count: CONCURRENCY_EPOCHS,
                minimum_requests_per_client: CONCURRENCY_MINIMUM_REQUESTS_PER_CLIENT,
                maximum_requests_per_client: CONCURRENCY_MAXIMUM_REQUESTS_PER_CLIENT,
                minimum_wall_time_ms: CONCURRENCY_MINIMUM_SECONDS * 1000.0,
                total_request_count: epoch_reports.iter().map(|epoch| epoch.request_count).sum(),
                total_query_request_counts,
                median_epoch_throughput_requests_per_second: median(&throughputs),
                minimum_epoch_throughput_requests_per_second: throughputs
                    .iter()
                    .copied()
                    .fold(f64::INFINITY, f64::min),
                maximum_epoch_throughput_requests_per_second: throughputs
                    .iter()
                    .copied()
                    .fold(f64::NEG_INFINITY, f64::max),
                epoch_throughput_cv: throughput_cv,
                median_epoch_latency_p50_ms: median(&p50s),
                median_epoch_latency_p95_ms: median(&p95s),
                median_epoch_latency_p99_ms: median(&p99s),
                epochs: epoch_reports,
                every_pre_and_post_node_exact: true,
            },
        );
    }
    Ok(reports)
}

#[derive(Debug, Serialize)]
struct ResourceMethodReport {
    storage_boundary: &'static str,
    concurrency_boundary: &'static str,
    concurrency_protocol: &'static str,
    concurrency_raw_evidence: &'static str,
    correctness_boundary: &'static str,
    latency_p50_estimator: &'static str,
    latency_p95_p99_estimator: &'static str,
}

#[derive(Debug, Serialize)]
struct ResourceGateArtifact {
    schema_version: u8,
    generated_at_unix_ms: u128,
    reference_schema_version: u8,
    reference_artifact_sha256: String,
    reference_source: ReferenceSource,
    reference_environment: ReferenceEnvironment,
    release: ReleaseReport,
    environment: EnvironmentReport,
    method: ResourceMethodReport,
    storage: StorageReport,
    concurrency: BTreeMap<String, ConcurrencyLevelReport>,
    every_concurrency_level_pre_and_post_node_exact: bool,
}

async fn run_resource_gates(args: &Args) -> Result<(), String> {
    let (reference, reference_artifact_sha256) = load_reference(&args.reference)?;
    let client = connect(&args.database_url).await?;
    let (release, environment) = release_environment(&client, &reference).await?;
    let external = load_external_data(&client).await?;
    let storage = storage(&client).await?;
    if storage.result_cache_rows != 0 || storage.request_result_cache_enabled {
        return Err("strict candidate resource gates require an empty, disabled result cache"
            .to_owned());
    }
    let concurrency = concurrency(&args.database_url, &reference, &external).await?;
    if CONCURRENCY_LEVELS.iter().any(|clients| {
        !concurrency[&clients.to_string()].every_pre_and_post_node_exact
    }) {
        return Err("not every concurrency level retained exact pre/post parity".to_owned());
    }
    let artifact = ResourceGateArtifact {
        schema_version: 1,
        generated_at_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .as_millis(),
        reference_schema_version: reference.schema_version,
        reference_artifact_sha256,
        reference_source: reference.source,
        reference_environment: reference.environment,
        release,
        environment,
        method: ResourceMethodReport {
            storage_boundary: "untimed serving footprint from pg_total_relation_size/pg_indexes_size for every ocpm schema serving relation, with relation-level evidence",
            concurrency_boundary: "per-client persistent PostgreSQL connection and prepared Q1-Q7 tree set; every timed request includes PostgreSQL generation/fetch, decode of every OCPB node, and complete owned-tree materialization; canonicalization is excluded",
            concurrency_protocol: "fixed 1/4/8/16 clients, three epochs per level, each epoch at least five seconds and at least 32 requests per client; requests rotate Q1-Q7; every client performs exact every-node canonical parity for Q1-Q7 both before and after every timed epoch",
            concurrency_raw_evidence: "positive integer nanosecond latency for every request, grouped by client ID in request order; zero-based array index is the client request ID and query=Q1..Q7[(client_id+request_id)%7]",
            correctness_boundary: "duplicate-preserving external-ID canonical JSON, exact violation reasons, typed Q6 label and child-derived duration, and every-node SHA-256/manifests",
            latency_p50_estimator: "conventional median with middle-pair averaging for even sample counts",
            latency_p95_p99_estimator: "nearest-rank",
        },
        storage,
        concurrency,
        every_concurrency_level_pre_and_post_node_exact: true,
    };
    write_artifact(&args.output, &artifact)
}
