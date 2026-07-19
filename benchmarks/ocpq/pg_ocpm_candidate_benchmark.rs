//! Rust-native, correctness-gated pg_ocpm candidate benchmark for OCPQ Q1-Q7.
//!
//! The headline clock covers a persistent prepared PostgreSQL query/fetch,
//! BindingCapsule decode, and expansion into owned logical root-row structs.
//! External-ID normalization, sorting, JSON serialization, and hashing happen
//! after that clock stops.

use std::{
    collections::{BTreeMap, HashMap},
    fmt::Write as _,
    fs,
    hint::black_box,
    io::{self, Write as _},
    mem::size_of,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use ocpm_core::binding::BindingCapsule;
use ocpm_postgres::{BindingQueryResult, PreparedBindingQuery};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Barrier;
use tokio_postgres::{Client, NoTls};

const EXPECTED_OCPQ_COMMIT: &str = "80457e561edd7bb9e142d959dd7e0f96e6b03f2f";
const EXPECTED_EVAL_COMMIT: &str = "846dd4eb9f8600ae42355968453a9412ea4759c2";
const EXPECTED_REFERENCE_IMAGE: &str = "ocpq:0.6.7-corrected-harness";
const EXPECTED_DATASET_SHA256: &str =
    "02ac333a2c194b5a411cb8527dd64b4845e5110752d2ffddb531e48ce97556d7";
const EXPECTED_QUERY_FILES_SHA256: &str =
    "387aeb31398d86ef8e7b15393649cbabe75be56185fd67d27021744025873466";
const EXPECTED_PG_OCPM_VERSION: &str = "0.6.0";
const DEFAULT_WARMUPS: usize = 10;
const DEFAULT_RUNS: usize = 30;
const DEFAULT_CONCURRENCY: &str = "1,4,8,16";
const DEFAULT_REQUESTS_PER_CLIENT: usize = 32;
const DEFAULT_CONCURRENCY_EPOCHS: usize = 3;
const DEFAULT_CONCURRENCY_MINIMUM_SECONDS: f64 = 5.0;

const Q1_SQL: &str = "SELECT ocpm.binding_object_activity_count(\
    ocpm.dataset_id('bpic2017-ocpq'),1,'Application','A_Submitted',1,1)";
const Q2_SQL: &str = r#"
SELECT ocpm.binding_neighbor_eventually(
    source.object_ids,
    source.event_ids,
    source.event_timestamps,
    coalesce(required.object_ids, ARRAY[]::bigint[]),
    coalesce(required.event_timestamps, ARRAY[]::timestamptz[])
)
FROM ocpm.binding_activity source
LEFT JOIN ocpm.binding_activity required
  ON required.dataset_id = source.dataset_id
 AND required.tenant_id = source.tenant_id
 AND required.object_type = source.object_type
 AND required.activity = 'O_Returned'
WHERE source.dataset_id = ocpm.dataset_id('bpic2017-ocpq')
  AND source.tenant_id = 1
  AND source.object_type = 'Offer'
  AND source.activity = 'O_Created'
"#;
const Q3_SQL: &str = "SELECT ocpm.binding_event_object_count(\
    ocpm.dataset_id('bpic2017-ocpq'),1,'Offer','O_Returned',1,1)";
const Q4_SQL: &str = "SELECT ocpm.binding_neighbor_eventually(\
    ocpm.dataset_id('bpic2017-ocpq'),1,'Application','A_Accepted',\
    'Offer','O_Accepted')";
const Q5_DEFAULT_SQL: &str = "SELECT ocpm.binding_relation_universal_equal(\
    ocpm.dataset_id('bpic2017-ocpq'),1,'Application','A_Accepted',\
    'Offer','O_Created','Case_R')";
const Q6_SQL: &str = "SELECT ocpm.binding_max_activity_delay(\
    ocpm.dataset_id('bpic2017-ocpq'),1,'Offer','O_Created','O_Accepted')";
const Q7_SQL: &str = "SELECT ocpm.binding_neighbor_pairs(\
    ocpm.dataset_id('bpic2017-ocpq'),1,'Application','Offer','O_Created')";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueryName {
    Q1,
    Q2,
    Q3,
    Q4,
    Q5,
    Q6,
    Q7,
}

impl QueryName {
    const ALL: [Self; 7] = [
        Self::Q1,
        Self::Q2,
        Self::Q3,
        Self::Q4,
        Self::Q5,
        Self::Q6,
        Self::Q7,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::Q1 => "Q1",
            Self::Q2 => "Q2",
            Self::Q3 => "Q3",
            Self::Q4 => "Q4",
            Self::Q5 => "Q5",
            Self::Q6 => "Q6",
            Self::Q7 => "Q7",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Q1 => 0,
            Self::Q2 => 1,
            Self::Q3 => 2,
            Self::Q4 => 3,
            Self::Q5 => 4,
            Self::Q6 => 5,
            Self::Q7 => 6,
        }
    }

    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "Q1" => Ok(Self::Q1),
            "Q2" => Ok(Self::Q2),
            "Q3" => Ok(Self::Q3),
            "Q4" => Ok(Self::Q4),
            "Q5" => Ok(Self::Q5),
            "Q6" => Ok(Self::Q6),
            "Q7" => Ok(Self::Q7),
            _ => Err(format!(
                "memory-only query must be Q1 through Q7, found {value:?}"
            )),
        }
    }
}

#[derive(Debug)]
struct Args {
    database_url: String,
    reference: Option<PathBuf>,
    output: PathBuf,
    q5_sql_file: Option<PathBuf>,
    warmups: usize,
    runs: usize,
    concurrency: Vec<usize>,
    requests_per_client: usize,
    concurrency_epochs: usize,
    concurrency_minimum_seconds: f64,
    memory_only_query: Option<QueryName>,
    expected_rows: Option<usize>,
}

fn usage() -> &'static str {
    "usage: ocpq_candidate_benchmark --reference PATH --output PATH \
     [--database-url URL] [--q5-sql-file PATH] [--warmups 10] [--runs 30] \
     [--concurrency 1,4,8,16] [--requests-per-client 32] \
     [--concurrency-epochs 3] [--concurrency-minimum-seconds 5] \
     [--memory-only-query Q1..Q7 --expected-rows N]"
}

fn parse_concurrency(value: &str) -> Result<Vec<usize>, String> {
    let mut levels = Vec::new();
    for item in value.split(',').filter(|item| !item.trim().is_empty()) {
        let clients = item
            .trim()
            .parse::<usize>()
            .map_err(|_| format!("invalid concurrency level {item:?}"))?;
        if clients == 0 {
            return Err("concurrency levels must be positive".to_owned());
        }
        if levels.contains(&clients) {
            return Err(format!("duplicate concurrency level {clients}"));
        }
        levels.push(clients);
    }
    Ok(levels)
}

fn parse_args() -> Result<Args, String> {
    let mut database_url = std::env::var("OCPM_DATABASE_URL").ok();
    let mut reference = None;
    let mut output = None;
    let mut q5_sql_file = None;
    let mut warmups = DEFAULT_WARMUPS;
    let mut runs = DEFAULT_RUNS;
    let mut concurrency = parse_concurrency(DEFAULT_CONCURRENCY)?;
    let mut requests_per_client = DEFAULT_REQUESTS_PER_CLIENT;
    let mut concurrency_epochs = DEFAULT_CONCURRENCY_EPOCHS;
    let mut concurrency_minimum_seconds = DEFAULT_CONCURRENCY_MINIMUM_SECONDS;
    let mut memory_only_query = None;
    let mut expected_rows = None;
    let mut values = std::env::args().skip(1);
    while let Some(flag) = values.next() {
        let mut value = || {
            values
                .next()
                .ok_or_else(|| format!("{flag} requires a value"))
        };
        match flag.as_str() {
            "--database-url" => database_url = Some(value()?),
            "--reference" => reference = Some(PathBuf::from(value()?)),
            "--output" => output = Some(PathBuf::from(value()?)),
            "--q5-sql-file" => q5_sql_file = Some(PathBuf::from(value()?)),
            "--warmups" => {
                warmups = value()?
                    .parse()
                    .map_err(|_| "invalid --warmups".to_owned())?
            }
            "--runs" => runs = value()?.parse().map_err(|_| "invalid --runs".to_owned())?,
            "--concurrency" => concurrency = parse_concurrency(&value()?)?,
            "--requests-per-client" => {
                requests_per_client = value()?
                    .parse()
                    .map_err(|_| "invalid --requests-per-client".to_owned())?
            }
            "--concurrency-epochs" => {
                concurrency_epochs = value()?
                    .parse()
                    .map_err(|_| "invalid --concurrency-epochs".to_owned())?
            }
            "--concurrency-minimum-seconds" => {
                concurrency_minimum_seconds = value()?
                    .parse()
                    .map_err(|_| "invalid --concurrency-minimum-seconds".to_owned())?
            }
            "--memory-only-query" => memory_only_query = Some(QueryName::parse(&value()?)?),
            "--expected-rows" => {
                expected_rows = Some(
                    value()?
                        .parse()
                        .map_err(|_| "invalid --expected-rows".to_owned())?,
                )
            }
            "--help" | "-h" => return Err(usage().to_owned()),
            _ => return Err(format!("unknown argument {flag:?}\n{}", usage())),
        }
    }
    let database_url =
        database_url.ok_or_else(|| "--database-url or OCPM_DATABASE_URL is required".to_owned())?;
    let output = output.ok_or_else(|| "--output is required".to_owned())?;
    if runs == 0 {
        return Err("--runs must be positive".to_owned());
    }
    if requests_per_client == 0 {
        return Err("--requests-per-client must be positive".to_owned());
    }
    if concurrency_epochs < 3 {
        return Err("--concurrency-epochs must be at least 3".to_owned());
    }
    if !concurrency_minimum_seconds.is_finite() || concurrency_minimum_seconds < 5.0 {
        return Err("--concurrency-minimum-seconds must be at least 5".to_owned());
    }
    if memory_only_query.is_none() && reference.is_none() {
        return Err("--reference is required outside memory-only mode".to_owned());
    }
    if memory_only_query.is_some() && expected_rows.is_none() {
        return Err("--expected-rows is required in memory-only mode".to_owned());
    }
    Ok(Args {
        database_url,
        reference,
        output,
        q5_sql_file,
        warmups,
        runs,
        concurrency,
        requests_per_client,
        concurrency_epochs,
        concurrency_minimum_seconds,
        memory_only_query,
        expected_rows,
    })
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CanonicalOutput {
    rows: usize,
    sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    violations: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_microseconds: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct ReferenceArtifact {
    schema_version: u8,
    source: ReferenceSource,
    environment: ReferenceEnvironment,
    method: ReferenceMethod,
    queries: BTreeMap<String, ReferenceQuery>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ReferenceSource {
    ocpq_eval_commit: String,
    ocpq_version: String,
    ocpq_commit: String,
    docker_image: String,
    docker_image_id: String,
    dataset_sqlite_sha256: String,
    query_files_sha256: String,
    author_published_results_commit: String,
}

#[derive(Debug, Deserialize)]
struct ReferenceEnvironment {
    benchmark_host_id: String,
    source_revision: String,
    source_tree_clean: bool,
}

#[derive(Debug, Deserialize)]
struct ReferenceMethod {
    warmups_per_query: usize,
    measured_runs_per_query: usize,
}

#[derive(Debug, Deserialize)]
struct ReferenceQuery {
    author_published_mean_ms: f64,
    mean_ms: f64,
    runs_ms: Vec<f64>,
    canonical_output: CanonicalOutput,
}

#[derive(Debug)]
struct Q1Row {
    application_id: i64,
    violated: bool,
}
#[derive(Debug)]
struct Q2Row {
    offer_id: i64,
    created_event_id: i64,
    violated: bool,
}
#[derive(Debug)]
struct Q3Row {
    returned_event_id: i64,
    violated: bool,
}
#[derive(Debug)]
struct Q4Row {
    application_id: i64,
    accepted_event_id: i64,
    violated: bool,
}
#[derive(Debug)]
struct Q5Row {
    application_id: i64,
    case_r_id: i64,
    accepted_event_id: i64,
    violated: bool,
}
#[derive(Debug)]
struct Q6Row {
    duration_seconds: f64,
}
#[derive(Debug)]
struct Q7Row {
    application_id: i64,
    offer_1_id: i64,
    offer_2_id: i64,
    created_event_1_id: i64,
    created_event_2_id: i64,
}

#[derive(Debug)]
enum OwnedRows {
    Q1(Vec<Q1Row>),
    Q2(Vec<Q2Row>),
    Q3(Vec<Q3Row>),
    Q4(Vec<Q4Row>),
    Q5(Vec<Q5Row>),
    Q6(Vec<Q6Row>),
    Q7(Vec<Q7Row>),
}

impl OwnedRows {
    fn len(&self) -> usize {
        match self {
            Self::Q1(rows) => rows.len(),
            Self::Q2(rows) => rows.len(),
            Self::Q3(rows) => rows.len(),
            Self::Q4(rows) => rows.len(),
            Self::Q5(rows) => rows.len(),
            Self::Q6(rows) => rows.len(),
            Self::Q7(rows) => rows.len(),
        }
    }

    fn allocated_bytes(&self) -> usize {
        let vector = size_of::<Vec<()>>();
        match self {
            Self::Q1(rows) => vector + rows.capacity() * size_of::<Q1Row>(),
            Self::Q2(rows) => vector + rows.capacity() * size_of::<Q2Row>(),
            Self::Q3(rows) => vector + rows.capacity() * size_of::<Q3Row>(),
            Self::Q4(rows) => vector + rows.capacity() * size_of::<Q4Row>(),
            Self::Q5(rows) => vector + rows.capacity() * size_of::<Q5Row>(),
            Self::Q6(rows) => vector + rows.capacity() * size_of::<Q6Row>(),
            Self::Q7(rows) => vector + rows.capacity() * size_of::<Q7Row>(),
        }
    }
}

#[derive(Debug)]
struct ExternalIds {
    objects: HashMap<i64, String>,
    events: HashMap<i64, String>,
}

fn require_id<'a>(
    mapping: &'a HashMap<i64, String>,
    key: i64,
    kind: &str,
) -> Result<&'a str, String> {
    mapping
        .get(&key)
        .map(String::as_str)
        .ok_or_else(|| format!("unknown {kind} key in candidate result: {key}"))
}

fn materialize(name: QueryName, capsule: &BindingCapsule) -> Result<OwnedRows, String> {
    macro_rules! ids {
        ($row:expr, $count:expr) => {{
            let ids = $row.ids();
            if ids.len() != $count {
                return Err(format!(
                    "{} returned {} ids, expected {}",
                    name.as_str(),
                    ids.len(),
                    $count
                ));
            }
            ids
        }};
    }
    match name {
        QueryName::Q1 => capsule
            .rows()
            .map(|row| {
                let ids = ids!(row, 1);
                Ok(Q1Row {
                    application_id: ids[0],
                    violated: row.violated.ok_or("Q1 missing violation")?,
                })
            })
            .collect::<Result<Vec<_>, String>>()
            .map(OwnedRows::Q1),
        QueryName::Q2 => capsule
            .rows()
            .map(|row| {
                let ids = ids!(row, 2);
                Ok(Q2Row {
                    offer_id: ids[0],
                    created_event_id: ids[1],
                    violated: row.violated.ok_or("Q2 missing violation")?,
                })
            })
            .collect::<Result<Vec<_>, String>>()
            .map(OwnedRows::Q2),
        QueryName::Q3 => capsule
            .rows()
            .map(|row| {
                let ids = ids!(row, 1);
                Ok(Q3Row {
                    returned_event_id: ids[0],
                    violated: row.violated.ok_or("Q3 missing violation")?,
                })
            })
            .collect::<Result<Vec<_>, String>>()
            .map(OwnedRows::Q3),
        QueryName::Q4 => capsule
            .rows()
            .map(|row| {
                let ids = ids!(row, 2);
                Ok(Q4Row {
                    application_id: ids[0],
                    accepted_event_id: ids[1],
                    violated: row.violated.ok_or("Q4 missing violation")?,
                })
            })
            .collect::<Result<Vec<_>, String>>()
            .map(OwnedRows::Q4),
        QueryName::Q5 => capsule
            .rows()
            .map(|row| {
                let (application_id, case_r_id, accepted_event_id) = match row.ids() {
                    [application_id, case_r_id, accepted_event_id] => {
                        (*application_id, *case_r_id, *accepted_event_id)
                    }
                    [application_id, accepted_event_id] => {
                        let case_r_id = row
                            .label
                            .ok_or("Q5 legacy override is missing its Case_R label")?
                            .parse::<i64>()
                            .map_err(|_| "Q5 legacy override returned an invalid Case_R label")?;
                        (*application_id, case_r_id, *accepted_event_id)
                    }
                    ids => {
                        return Err(format!(
                            "Q5 returned {} ids, expected either 2 or 3",
                            ids.len()
                        ));
                    }
                };
                Ok(Q5Row {
                    application_id,
                    case_r_id,
                    accepted_event_id,
                    violated: row.violated.ok_or("Q5 missing violation")?,
                })
            })
            .collect::<Result<Vec<_>, String>>()
            .map(OwnedRows::Q5),
        QueryName::Q6 => capsule
            .rows()
            .map(|row| {
                if !row.ids().is_empty() {
                    return Err("Q6 unexpectedly returned ids".to_owned());
                }
                Ok(Q6Row {
                    duration_seconds: row.value.ok_or("Q6 missing value")?,
                })
            })
            .collect::<Result<Vec<_>, String>>()
            .map(OwnedRows::Q6),
        QueryName::Q7 => capsule
            .rows()
            .map(|row| {
                let ids = ids!(row, 5);
                Ok(Q7Row {
                    application_id: ids[0],
                    offer_1_id: ids[1],
                    offer_2_id: ids[2],
                    created_event_1_id: ids[3],
                    created_event_2_id: ids[4],
                })
            })
            .collect::<Result<Vec<_>, String>>()
            .map(OwnedRows::Q7),
    }
}

fn canonicalize(rows: &OwnedRows, ids: &ExternalIds) -> Result<(CanonicalOutput, usize), String> {
    let (mut canonical, violations, duration_microseconds): (
        Vec<Vec<Value>>,
        Option<usize>,
        Option<i64>,
    ) = match rows {
        OwnedRows::Q1(rows) => (
            rows.iter()
                .map(|row| {
                    Ok(vec![
                        json!(require_id(&ids.objects, row.application_id, "object")?),
                        json!(row.violated),
                    ])
                })
                .collect::<Result<_, String>>()?,
            Some(rows.iter().filter(|row| row.violated).count()),
            None,
        ),
        OwnedRows::Q2(rows) => (
            rows.iter()
                .map(|row| {
                    Ok(vec![
                        json!(require_id(&ids.objects, row.offer_id, "object")?),
                        json!(require_id(&ids.events, row.created_event_id, "event")?),
                        json!(row.violated),
                    ])
                })
                .collect::<Result<_, String>>()?,
            Some(rows.iter().filter(|row| row.violated).count()),
            None,
        ),
        OwnedRows::Q3(rows) => (
            rows.iter()
                .map(|row| {
                    Ok(vec![
                        json!(require_id(&ids.events, row.returned_event_id, "event")?),
                        json!(row.violated),
                    ])
                })
                .collect::<Result<_, String>>()?,
            Some(rows.iter().filter(|row| row.violated).count()),
            None,
        ),
        OwnedRows::Q4(rows) => (
            rows.iter()
                .map(|row| {
                    Ok(vec![
                        json!(require_id(&ids.objects, row.application_id, "object")?),
                        json!(require_id(&ids.events, row.accepted_event_id, "event")?),
                        json!(row.violated),
                    ])
                })
                .collect::<Result<_, String>>()?,
            Some(rows.iter().filter(|row| row.violated).count()),
            None,
        ),
        OwnedRows::Q5(rows) => (
            rows.iter()
                .map(|row| {
                    Ok(vec![
                        json!(require_id(&ids.objects, row.application_id, "object")?),
                        json!(require_id(&ids.objects, row.case_r_id, "Case_R object")?),
                        json!(require_id(&ids.events, row.accepted_event_id, "event")?),
                        json!(row.violated),
                    ])
                })
                .collect::<Result<_, String>>()?,
            Some(rows.iter().filter(|row| row.violated).count()),
            None,
        ),
        OwnedRows::Q6(rows) => {
            if rows.len() != 1 {
                return Err(format!("Q6 expected one row, found {}", rows.len()));
            }
            let micros = (rows[0].duration_seconds * 1_000_000.0).round_ties_even() as i64;
            (vec![vec![json!(micros)]], None, Some(micros))
        }
        OwnedRows::Q7(rows) => (
            rows.iter()
                .map(|row| {
                    Ok(vec![
                        json!(require_id(&ids.objects, row.application_id, "object")?),
                        json!(require_id(&ids.objects, row.offer_1_id, "object")?),
                        json!(require_id(&ids.objects, row.offer_2_id, "object")?),
                        json!(require_id(&ids.events, row.created_event_1_id, "event")?),
                        json!(require_id(&ids.events, row.created_event_2_id, "event")?),
                    ])
                })
                .collect::<Result<_, String>>()?,
            None,
            None,
        ),
    };
    canonical
        .sort_by_cached_key(|row| serde_json::to_string(row).expect("canonical row serialization"));
    let encoded = serde_json::to_vec(&canonical).map_err(|error| error.to_string())?;
    let mut sha256 = String::with_capacity(64);
    for byte in Sha256::digest(&encoded) {
        write!(&mut sha256, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok((
        CanonicalOutput {
            rows: canonical.len(),
            sha256,
            violations,
            duration_microseconds,
        },
        encoded.len(),
    ))
}

#[derive(Debug)]
struct HeadlineRun {
    elapsed_ms: f64,
    capsule_bytes: usize,
    rows: OwnedRows,
}

fn materialize_result(
    name: QueryName,
    result: BindingQueryResult,
) -> Result<(usize, OwnedRows), String> {
    let rows = materialize(name, &result.capsule)?;
    black_box(&rows);
    Ok((result.encoded_bytes, rows))
}

async fn fetch_materialized(
    client: &Client,
    query: &PreparedBindingQuery,
    name: QueryName,
) -> Result<(usize, OwnedRows), String> {
    let result = query
        .execute(client, &[])
        .await
        .map_err(|error| error.to_string())?;
    materialize_result(name, result)
}

async fn headline_run(
    client: &Client,
    query: &PreparedBindingQuery,
    name: QueryName,
) -> Result<HeadlineRun, String> {
    let started = Instant::now();
    let result = query
        .execute(client, &[])
        .await
        .map_err(|error| error.to_string())?;
    let (capsule_bytes, rows) = materialize_result(name, result)?;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    Ok(HeadlineRun {
        elapsed_ms,
        capsule_bytes,
        rows,
    })
}

#[derive(Debug, Serialize)]
struct MemoryReport {
    owned_rows_bytes: usize,
    postgres_backend_baseline_bytes: i64,
    postgres_backend_after_bytes: i64,
    postgres_backend_delta_bytes: i64,
}

#[derive(Debug, Serialize)]
struct StorageReport {
    total_serving_bytes: i64,
    index_bytes: i64,
    binding_summary_bytes: i64,
}

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
    pg_ocpm_source_revision: String,
    pg_ocpm_source_tree_clean: bool,
    candidate_image: String,
    candidate_image_id: String,
    database_image: String,
    database_image_id: String,
    postgres_server_version: String,
    postgres_server_version_num: i32,
    postgres_jit: String,
    client_os: &'static str,
    client_arch: &'static str,
}

#[derive(Clone, Copy, Debug)]
struct ProcessMemorySnapshot {
    rss_bytes: u64,
    vmhwm_bytes: u64,
}

#[derive(Debug, Serialize)]
struct MemoryOnlyArtifact {
    schema_version: u8,
    release: ReleaseReport,
    environment: EnvironmentReport,
    reference_source: ReferenceSource,
    mode: &'static str,
    query: &'static str,
    measurement_boundary: &'static str,
    correctness_boundary: &'static str,
    expected_rows: usize,
    logical_rows_materialized: usize,
    canonical_output: CanonicalOutput,
    semantic_parity: bool,
    capsule_bytes: usize,
    owned_rows_bytes: usize,
    baseline_rss_bytes: u64,
    baseline_vmhwm_bytes: u64,
    after_rss_bytes: u64,
    after_vmhwm_bytes: u64,
    rss_delta_bytes: i64,
    vmhwm_delta_bytes: i64,
    peak_over_baseline_rss_bytes: u64,
    tokio_worker_threads_env: Option<String>,
    q5_sql: String,
}

#[derive(Debug, Serialize)]
struct QueryReport {
    author_published_ocpq_mean_ms: f64,
    reference_ocpq_mean_ms: f64,
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    speedup_vs_reference_ocpq: f64,
    runs_ms: Vec<f64>,
    diagnostic_canonicalization_runs_ms: Vec<f64>,
    diagnostic_query_to_fingerprint_mean_ms: f64,
    capsule_bytes: usize,
    logical_rows_materialized: usize,
    canonical_json_bytes: usize,
    memory: MemoryReport,
    canonical_output: CanonicalOutput,
    semantic_parity: bool,
}

#[derive(Debug, Serialize)]
struct SummaryReport {
    author_published_ocpq_geometric_mean_ms: f64,
    same_host_ocpq_geometric_mean_ms: f64,
    candidate_geometric_mean_ms: f64,
    speedup_geometric_mean: f64,
    minimum_query_speedup: f64,
    all_queries_exact: bool,
    maximum_owned_rows_bytes: usize,
    concurrency_16_to_1_median_epoch_throughput_scaling: Option<f64>,
}

#[derive(Debug, Serialize)]
struct ConcurrencyEpochReport {
    epoch: usize,
    warmed_client_ids: Vec<usize>,
    pre_epoch_exact_query_checks: BTreeMap<String, usize>,
    post_epoch_verified_client_ids: Vec<usize>,
    post_epoch_exact_query_checks: BTreeMap<String, usize>,
    client_request_counts: BTreeMap<String, usize>,
    request_count: usize,
    wall_time_ms: f64,
    throughput_requests_per_second: f64,
    latency_p50_ms: f64,
    latency_p95_ms: f64,
    latency_p99_ms: f64,
    query_request_counts: BTreeMap<String, usize>,
    semantic_parity: bool,
}

#[derive(Debug, Serialize)]
struct ConcurrencyReport {
    clients: usize,
    epoch_count: usize,
    minimum_requests_per_client: usize,
    minimum_wall_time_ms: f64,
    total_request_count: usize,
    total_query_request_counts: BTreeMap<String, usize>,
    median_epoch_wall_time_ms: f64,
    median_epoch_throughput_requests_per_second: f64,
    minimum_epoch_throughput_requests_per_second: f64,
    maximum_epoch_throughput_requests_per_second: f64,
    epoch_throughput_cv: f64,
    median_epoch_latency_p50_ms: f64,
    median_epoch_latency_p95_ms: f64,
    median_epoch_latency_p99_ms: f64,
    epochs: Vec<ConcurrencyEpochReport>,
    semantic_parity: bool,
}

#[derive(Debug, Serialize)]
struct MethodReport {
    warmups_per_query: usize,
    measured_runs_per_query: usize,
    timing_boundary: &'static str,
    correctness_boundary: &'static str,
    non_headline_diagnostic_boundary: &'static str,
    memory_diagnostic_boundary: &'static str,
    storage_scope: &'static str,
    concurrency_boundary: &'static str,
    concurrency_protocol: String,
    q5_sql: String,
}

#[derive(Debug, Serialize)]
struct OutputArtifact {
    schema_version: u8,
    generated_at_unix_ms: u128,
    release: ReleaseReport,
    environment: EnvironmentReport,
    reference_source: ReferenceSource,
    method: MethodReport,
    summary: SummaryReport,
    storage: StorageReport,
    queries: BTreeMap<String, QueryReport>,
    concurrency: BTreeMap<String, ConcurrencyReport>,
}

fn mean(samples: &[f64]) -> f64 {
    samples.iter().sum::<f64>() / samples.len() as f64
}

fn geometric_mean(samples: &[f64]) -> f64 {
    (samples.iter().map(|value| value.ln()).sum::<f64>() / samples.len() as f64).exp()
}

fn percentile(samples: &[f64], fraction: f64) -> f64 {
    let mut ordered = samples.to_vec();
    ordered.sort_by(f64::total_cmp);
    let index = ((ordered.len() as f64 * fraction).ceil() as usize)
        .saturating_sub(1)
        .min(ordered.len() - 1);
    ordered[index]
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_reference_output(name: QueryName, output: &CanonicalOutput) -> Result<(), String> {
    if output.rows == 0 || !is_lowercase_sha256(&output.sha256) {
        return Err(format!(
            "{} reference canonical output has invalid rows or SHA-256",
            name.as_str()
        ));
    }
    match name {
        QueryName::Q1 | QueryName::Q2 | QueryName::Q3 | QueryName::Q4 | QueryName::Q5 => {
            if output.violations.is_none_or(|count| count > output.rows)
                || output.duration_microseconds.is_some()
            {
                return Err(format!(
                    "{} reference canonical violation output is invalid",
                    name.as_str()
                ));
            }
        }
        QueryName::Q6 => {
            if output.rows != 1
                || output.violations.is_some()
                || output
                    .duration_microseconds
                    .is_none_or(|duration| duration < 0)
            {
                return Err("Q6 reference canonical duration output is invalid".to_owned());
            }
        }
        QueryName::Q7 => {
            if output.violations.is_some() || output.duration_microseconds.is_some() {
                return Err("Q7 reference canonical output has unexpected metrics".to_owned());
            }
        }
    }
    Ok(())
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

fn validate_reference(reference: &ReferenceArtifact, args: &Args) -> Result<(), String> {
    if reference.schema_version != 3 {
        return Err("OCPQ reference must use schema version 3".to_owned());
    }
    if reference.source.ocpq_eval_commit != EXPECTED_EVAL_COMMIT
        || reference.source.ocpq_commit != EXPECTED_OCPQ_COMMIT
        || reference.source.ocpq_version != "0.6.7"
        || reference.source.docker_image != EXPECTED_REFERENCE_IMAGE
        || !reference.source.docker_image_id.starts_with("sha256:")
        || reference.source.dataset_sqlite_sha256 != EXPECTED_DATASET_SHA256
        || reference.source.query_files_sha256 != EXPECTED_QUERY_FILES_SHA256
        || reference.source.author_published_results_commit != EXPECTED_EVAL_COMMIT
    {
        return Err(
            "OCPQ reference source pins do not match the corrected 0.6.7 artifact".to_owned(),
        );
    }
    if !reference
        .environment
        .benchmark_host_id
        .starts_with("sha256:")
    {
        return Err("OCPQ reference is missing its benchmark-host fingerprint".to_owned());
    }
    if reference.environment.source_revision.trim().is_empty() {
        return Err("OCPQ reference is missing harness source provenance".to_owned());
    }
    if reference.method.warmups_per_query != args.warmups
        || reference.method.measured_runs_per_query != args.runs
    {
        return Err("candidate and OCPQ reference warmup/run counts must match exactly".to_owned());
    }
    let expected = QueryName::ALL
        .map(|name| name.as_str().to_owned())
        .into_iter()
        .collect::<Vec<_>>();
    if reference.queries.keys().cloned().collect::<Vec<_>>() != expected {
        return Err("OCPQ reference must contain exactly Q1-Q7".to_owned());
    }
    for name in QueryName::ALL {
        let query = &reference.queries[name.as_str()];
        if query.runs_ms.len() != args.runs {
            return Err(format!(
                "{} reference run count is inconsistent",
                name.as_str()
            ));
        }
        if !query.author_published_mean_ms.is_finite() || query.author_published_mean_ms <= 0.0 {
            return Err(format!(
                "{} author-published OCPQ mean is invalid",
                name.as_str()
            ));
        }
        if !query.mean_ms.is_finite()
            || query.mean_ms <= 0.0
            || query
                .runs_ms
                .iter()
                .any(|sample| !sample.is_finite() || *sample <= 0.0)
        {
            return Err(format!(
                "{} same-host OCPQ timing samples are invalid",
                name.as_str()
            ));
        }
        if (mean(&query.runs_ms) - query.mean_ms).abs()
            > f64::EPSILON * query.mean_ms.abs().max(1.0) * 8.0
        {
            return Err(format!("{} reference mean is inconsistent", name.as_str()));
        }
        validate_reference_output(name, &query.canonical_output)?;
    }
    Ok(())
}

fn validate_q5_sql(sql: &str) -> Result<String, String> {
    let trimmed = sql.trim();
    let without_trailing = trimmed.strip_suffix(';').unwrap_or(trimmed).trim_end();
    if without_trailing.is_empty()
        || (!without_trailing.to_ascii_uppercase().starts_with("SELECT")
            && !without_trailing.to_ascii_uppercase().starts_with("WITH"))
    {
        return Err("Q5 override must be a SELECT or WITH query".to_owned());
    }
    if without_trailing.contains(';')
        || without_trailing.contains("--")
        || without_trailing.contains("/*")
    {
        return Err("Q5 override must be one comment-free SQL statement".to_owned());
    }
    Ok(without_trailing.to_owned())
}

async fn connect_client(database_url: &str) -> Result<Client, String> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls)
        .await
        .map_err(|error| error.to_string())?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("PostgreSQL connection error: {error}");
        }
    });
    client
        .batch_execute("SET jit=off")
        .await
        .map_err(|error| error.to_string())?;
    Ok(client)
}

fn environment_bool(name: &str, default: bool) -> Result<bool, String> {
    match std::env::var(name) {
        Ok(value) if value.eq_ignore_ascii_case("true") || value == "1" => Ok(true),
        Ok(value) if value.eq_ignore_ascii_case("false") || value == "0" => Ok(false),
        Ok(value) => Err(format!(
            "{name} must be true, false, 1, or 0; found {value:?}"
        )),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(format!("could not read {name}: {error}")),
    }
}

async fn release_environment(
    client: &Client,
) -> Result<(ReleaseReport, EnvironmentReport), String> {
    let row = client
        .query_one(
            "SELECT ocpm.version(), current_setting('server_version'), \
             current_setting('server_version_num')::integer, current_setting('jit')",
            &[],
        )
        .await
        .map_err(|error| error.to_string())?;
    let pg_ocpm = row.get::<_, String>(0);
    if pg_ocpm != EXPECTED_PG_OCPM_VERSION {
        return Err(format!(
            "candidate requires pg_ocpm {EXPECTED_PG_OCPM_VERSION}, found {pg_ocpm}"
        ));
    }
    let source_revision =
        std::env::var("OCPM_SOURCE_REVISION").unwrap_or_else(|_| "working-tree-preview".to_owned());
    if source_revision.trim().is_empty() {
        return Err("OCPM_SOURCE_REVISION must not be empty".to_owned());
    }
    let benchmark_host_id = std::env::var("OCPM_BENCHMARK_HOST_ID")
        .map_err(|_| "OCPM_BENCHMARK_HOST_ID is required".to_owned())?;
    if benchmark_host_id.trim().is_empty() {
        return Err("OCPM_BENCHMARK_HOST_ID must not be empty".to_owned());
    }
    let pg_ocpm_source_revision = std::env::var("OCPM_PG_OCPM_SOURCE_REVISION")
        .unwrap_or_else(|_| "working-tree-preview".to_owned());
    if pg_ocpm_source_revision.trim().is_empty() {
        return Err("OCPM_PG_OCPM_SOURCE_REVISION must not be empty".to_owned());
    }
    Ok((
        ReleaseReport {
            pg_ocpm,
            ocpm_engine: env!("CARGO_PKG_VERSION"),
        },
        EnvironmentReport {
            benchmark_host_id,
            source_revision,
            source_tree_clean: environment_bool("OCPM_SOURCE_TREE_CLEAN", false)?,
            pg_ocpm_source_revision,
            pg_ocpm_source_tree_clean: environment_bool("OCPM_PG_OCPM_SOURCE_TREE_CLEAN", false)?,
            candidate_image: std::env::var("OCPM_CANDIDATE_IMAGE")
                .unwrap_or_else(|_| "unspecified".to_owned()),
            candidate_image_id: std::env::var("OCPM_CANDIDATE_IMAGE_ID")
                .unwrap_or_else(|_| "unspecified".to_owned()),
            database_image: std::env::var("OCPM_DATABASE_IMAGE")
                .unwrap_or_else(|_| "unspecified".to_owned()),
            database_image_id: std::env::var("OCPM_DATABASE_IMAGE_ID")
                .unwrap_or_else(|_| "unspecified".to_owned()),
            postgres_server_version: row.get(1),
            postgres_server_version_num: row.get(2),
            postgres_jit: row.get(3),
            client_os: std::env::consts::OS,
            client_arch: std::env::consts::ARCH,
        },
    ))
}

fn validate_same_harness(
    reference: &ReferenceArtifact,
    environment: &EnvironmentReport,
) -> Result<(), String> {
    if environment.benchmark_host_id != reference.environment.benchmark_host_id {
        return Err("candidate and OCPQ reference Docker hosts differ".to_owned());
    }
    if environment.source_revision != reference.environment.source_revision
        || environment.source_tree_clean != reference.environment.source_tree_clean
    {
        return Err("candidate and OCPQ reference harness provenance differs".to_owned());
    }
    Ok(())
}

async fn load_external_ids(client: &Client) -> Result<ExternalIds, String> {
    let objects = client
        .query(
            "SELECT object_id,min(external_object_id)::text \
             FROM ocpm.event_locator \
             WHERE dataset_id=ocpm.dataset_id('bpic2017-ocpq') \
               AND tenant_id=1 AND external_object_id IS NOT NULL \
             GROUP BY object_id",
            &[],
        )
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1)))
        .collect();
    let events = client
        .query(
            "SELECT expanded.event_id,min(expanded.external_event_id)::text \
             FROM ocpm.event_chunk chunk \
             CROSS JOIN LATERAL unnest( \
               chunk.event_sequences,chunk.external_event_ids \
             ) AS expanded(event_id,external_event_id) \
             WHERE chunk.dataset_id=ocpm.dataset_id('bpic2017-ocpq') \
               AND chunk.tenant_id=1 \
             GROUP BY expanded.event_id",
            &[],
        )
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1)))
        .collect();
    Ok(ExternalIds { objects, events })
}

async fn prepare_capsule(
    client: &Client,
    name: QueryName,
    sql: &str,
) -> Result<PreparedBindingQuery, String> {
    PreparedBindingQuery::prepare(client, sql)
        .await
        .map_err(|error| format!("{} prepare failed: {error}", name.as_str()))
}

fn query_sql(name: QueryName, q5_sql: &str) -> &str {
    match name {
        QueryName::Q1 => Q1_SQL,
        QueryName::Q2 => Q2_SQL,
        QueryName::Q3 => Q3_SQL,
        QueryName::Q4 => Q4_SQL,
        QueryName::Q5 => q5_sql,
        QueryName::Q6 => Q6_SQL,
        QueryName::Q7 => Q7_SQL,
    }
}

async fn prepare_query_set(
    client: &Client,
    q5_sql: &str,
) -> Result<Vec<PreparedBindingQuery>, String> {
    let mut queries = Vec::with_capacity(QueryName::ALL.len());
    for name in QueryName::ALL {
        queries.push(prepare_capsule(client, name, query_sql(name, q5_sql)).await?);
    }
    Ok(queries)
}

async fn backend_memory(client: &Client) -> Result<i64, String> {
    let row = client
        .query_one(
            "SELECT coalesce(sum(total_bytes),0)::bigint FROM pg_backend_memory_contexts",
            &[],
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(row.get(0))
}

async fn storage(client: &Client) -> Result<StorageReport, String> {
    let serving = client
        .query_one(
            "SELECT coalesce(sum(pg_total_relation_size(c.oid)),0)::bigint, \
             coalesce(sum(pg_indexes_size(c.oid)),0)::bigint \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid=c.relnamespace \
             WHERE n.nspname='ocpm' AND c.relkind IN ('r','m','p')",
            &[],
        )
        .await
        .map_err(|error| error.to_string())?;
    let binding = client
        .query_one(
            "SELECT coalesce(sum(pg_total_relation_size(c.oid)),0)::bigint \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid=c.relnamespace \
             WHERE n.nspname='ocpm' AND c.relkind IN ('r','m','p') \
               AND c.relname LIKE 'binding_%'",
            &[],
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(StorageReport {
        total_serving_bytes: serving.get(0),
        index_bytes: serving.get(1),
        binding_summary_bytes: binding.get(0),
    })
}

async fn run_query(
    client: &Client,
    name: QueryName,
    query: &PreparedBindingQuery,
    reference: &ReferenceQuery,
    ids: &ExternalIds,
    args: &Args,
) -> Result<QueryReport, String> {
    for _ in 0..args.warmups {
        let run = headline_run(client, query, name).await?;
        let (actual, _) = canonicalize(&run.rows, ids)?;
        if actual != reference.canonical_output {
            return Err(format!(
                "{} warmup fingerprint mismatch: candidate={actual:?}, OCPQ={:?}",
                name.as_str(),
                reference.canonical_output
            ));
        }
    }
    let mut runs_ms = Vec::with_capacity(args.runs);
    let mut canonicalization_runs_ms = Vec::with_capacity(args.runs);
    let mut final_output = None;
    let mut capsule_bytes = None;
    let mut logical_rows = None;
    let mut canonical_json_bytes = 0;
    let mut owned_rows_bytes = 0;
    for _ in 0..args.runs {
        let run = headline_run(client, query, name).await?;
        let canonical_started = Instant::now();
        let (actual, json_bytes) = canonicalize(&run.rows, ids)?;
        let canonical_ms = canonical_started.elapsed().as_secs_f64() * 1000.0;
        if actual != reference.canonical_output {
            return Err(format!(
                "{} measured fingerprint mismatch: candidate={actual:?}, OCPQ={:?}",
                name.as_str(),
                reference.canonical_output
            ));
        }
        if capsule_bytes.is_some_and(|value| value != run.capsule_bytes)
            || logical_rows.is_some_and(|value| value != run.rows.len())
        {
            return Err(format!(
                "{} result shape changed between runs",
                name.as_str()
            ));
        }
        capsule_bytes = Some(run.capsule_bytes);
        logical_rows = Some(run.rows.len());
        canonical_json_bytes = json_bytes;
        owned_rows_bytes = run.rows.allocated_bytes();
        runs_ms.push(run.elapsed_ms);
        canonicalization_runs_ms.push(canonical_ms);
        final_output = Some(actual);
    }
    let backend_baseline = backend_memory(client).await?;
    let (diagnostic_capsule_bytes, diagnostic_rows) =
        fetch_materialized(client, query, name).await?;
    let backend_after = backend_memory(client).await?;
    let (diagnostic_output, diagnostic_json_bytes) = canonicalize(&diagnostic_rows, ids)?;
    if diagnostic_output != reference.canonical_output {
        return Err(format!(
            "{} untimed memory diagnostic fingerprint mismatch: candidate={diagnostic_output:?}, OCPQ={:?}",
            name.as_str(),
            reference.canonical_output
        ));
    }
    if diagnostic_capsule_bytes != capsule_bytes.expect("measured runs are nonempty")
        || diagnostic_rows.len() != logical_rows.expect("measured runs are nonempty")
        || diagnostic_json_bytes != canonical_json_bytes
    {
        return Err(format!(
            "{} untimed memory diagnostic result shape changed",
            name.as_str()
        ));
    }
    owned_rows_bytes = owned_rows_bytes.max(diagnostic_rows.allocated_bytes());
    let mean_ms = mean(&runs_ms);
    Ok(QueryReport {
        author_published_ocpq_mean_ms: reference.author_published_mean_ms,
        reference_ocpq_mean_ms: reference.mean_ms,
        mean_ms,
        p50_ms: percentile(&runs_ms, 0.5),
        p95_ms: percentile(&runs_ms, 0.95),
        speedup_vs_reference_ocpq: reference.mean_ms / mean_ms,
        diagnostic_query_to_fingerprint_mean_ms: mean(
            &runs_ms
                .iter()
                .zip(&canonicalization_runs_ms)
                .map(|(headline, canonical)| headline + canonical)
                .collect::<Vec<_>>(),
        ),
        runs_ms,
        diagnostic_canonicalization_runs_ms: canonicalization_runs_ms,
        capsule_bytes: capsule_bytes.expect("measured runs are nonempty"),
        logical_rows_materialized: logical_rows.expect("measured runs are nonempty"),
        canonical_json_bytes,
        memory: MemoryReport {
            owned_rows_bytes,
            postgres_backend_baseline_bytes: backend_baseline,
            postgres_backend_after_bytes: backend_after,
            postgres_backend_delta_bytes: backend_after - backend_baseline,
        },
        canonical_output: final_output.expect("measured runs are nonempty"),
        semantic_parity: true,
    })
}

async fn warm_concurrency_client(
    client: &Client,
    queries: &[PreparedBindingQuery],
    reference: &ReferenceArtifact,
    ids: &ExternalIds,
) -> Result<(), String> {
    for name in QueryName::ALL {
        let (_, rows) = fetch_materialized(client, &queries[name.index()], name).await?;
        let (actual, _) = canonicalize(&rows, ids)?;
        let expected = &reference.queries[name.as_str()].canonical_output;
        if actual != *expected {
            return Err(format!(
                "{} concurrency warmup fingerprint mismatch: candidate={actual:?}, OCPQ={expected:?}",
                name.as_str()
            ));
        }
    }
    Ok(())
}

struct ConcurrencySamples {
    client_id: usize,
    client: Client,
    queries: Vec<PreparedBindingQuery>,
    latencies_ms: Vec<f64>,
    query_counts: [usize; 7],
}

struct ConcurrencyClientConfig {
    client_id: usize,
    minimum_requests: usize,
    minimum_duration: Duration,
    expected_rows: [usize; 7],
    ready: Arc<Barrier>,
    start: Arc<Barrier>,
}

async fn run_concurrency_client(
    client: Client,
    queries: Vec<PreparedBindingQuery>,
    config: ConcurrencyClientConfig,
) -> Result<ConcurrencySamples, String> {
    let ConcurrencyClientConfig {
        client_id,
        minimum_requests,
        minimum_duration,
        expected_rows,
        ready,
        start,
    } = config;
    ready.wait().await;
    start.wait().await;
    let epoch_started = Instant::now();
    let mut latencies_ms = Vec::with_capacity(minimum_requests);
    let mut query_counts = [0_usize; 7];
    let mut request = 0_usize;
    while request < minimum_requests || epoch_started.elapsed() < minimum_duration {
        let name = QueryName::ALL[(client_id + request) % QueryName::ALL.len()];
        let started = Instant::now();
        let result = queries[name.index()]
            .execute(&client, &[])
            .await
            .map_err(|error| error.to_string())?;
        let (_, rows) = materialize_result(name, result)?;
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        if rows.len() != expected_rows[name.index()] {
            return Err(format!(
                "{} concurrency request returned {} rows, expected {}",
                name.as_str(),
                rows.len(),
                expected_rows[name.index()]
            ));
        }
        latencies_ms.push(elapsed_ms);
        query_counts[name.index()] += 1;
        request += 1;
    }
    Ok(ConcurrencySamples {
        client_id,
        client,
        queries,
        latencies_ms,
        query_counts,
    })
}

struct ConcurrencyConfig<'a> {
    database_url: &'a str,
    q5_sql: &'a str,
    levels: &'a [usize],
    minimum_requests_per_client: usize,
    epoch_count: usize,
    minimum_seconds: f64,
}

async fn concurrency(
    config: &ConcurrencyConfig<'_>,
    reference: &ReferenceArtifact,
    ids: &ExternalIds,
) -> Result<BTreeMap<String, ConcurrencyReport>, String> {
    let database_url = config.database_url;
    let q5_sql = config.q5_sql;
    let levels = config.levels;
    let minimum_requests_per_client = config.minimum_requests_per_client;
    let epoch_count = config.epoch_count;
    let minimum_seconds = config.minimum_seconds;
    let expected_rows =
        QueryName::ALL.map(|name| reference.queries[name.as_str()].canonical_output.rows);
    let minimum_duration = Duration::from_secs_f64(minimum_seconds);
    let mut reports = BTreeMap::new();
    for &clients in levels {
        let mut epoch_reports = Vec::with_capacity(epoch_count);
        let mut total_query_counts = [0_usize; 7];
        for epoch in 1..=epoch_count {
            eprintln!(
                "running concurrency: {clients} clients, epoch {epoch}/{epoch_count}, >= {minimum_seconds:.1}s and >= {minimum_requests_per_client} requests/client"
            );
            let mut prepared_clients = Vec::with_capacity(clients);
            let mut warmed_client_ids = Vec::with_capacity(clients);
            for client_id in 0..clients {
                let client = connect_client(database_url).await?;
                let queries = prepare_query_set(&client, q5_sql).await?;
                warm_concurrency_client(&client, &queries, reference, ids).await?;
                warmed_client_ids.push(client_id);
                prepared_clients.push((client_id, client, queries));
            }

            let ready = Arc::new(Barrier::new(clients + 1));
            let start = Arc::new(Barrier::new(clients + 1));
            let mut handles = Vec::with_capacity(clients);
            for (client_id, client, queries) in prepared_clients {
                handles.push(tokio::spawn(run_concurrency_client(
                    client,
                    queries,
                    ConcurrencyClientConfig {
                        client_id,
                        minimum_requests: minimum_requests_per_client,
                        minimum_duration,
                        expected_rows,
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
            if wall_time_ms < minimum_seconds * 1000.0 {
                return Err(format!(
                    "concurrency level {clients} epoch {epoch} ran for only {wall_time_ms:.3} ms"
                ));
            }

            // Exact external-ID parity checks stay outside the timed serving
            // boundary, but bracket every epoch on every persistent client.
            let mut post_epoch_verified_client_ids = Vec::with_capacity(clients);
            for samples in &completed {
                warm_concurrency_client(&samples.client, &samples.queries, reference, ids).await?;
                post_epoch_verified_client_ids.push(samples.client_id);
            }

            let mut latencies_ms = Vec::new();
            let mut query_counts = [0_usize; 7];
            let mut client_request_counts = BTreeMap::new();
            for samples in completed {
                if samples.latencies_ms.len() < minimum_requests_per_client {
                    return Err(format!(
                        "concurrency level {clients} epoch {epoch} client completed only {} requests",
                        samples.latencies_ms.len()
                    ));
                }
                client_request_counts
                    .insert(samples.client_id.to_string(), samples.latencies_ms.len());
                latencies_ms.extend(samples.latencies_ms);
                for (total, count) in query_counts.iter_mut().zip(samples.query_counts) {
                    *total += count;
                }
            }
            let request_count = latencies_ms.len();
            let minimum_count = clients * minimum_requests_per_client;
            if request_count < minimum_count {
                return Err(format!(
                    "concurrency level {clients} epoch {epoch} completed {request_count} requests, expected at least {minimum_count}"
                ));
            }
            let expected_client_ids = (0..clients).collect::<Vec<_>>();
            if warmed_client_ids != expected_client_ids
                || post_epoch_verified_client_ids != expected_client_ids
                || client_request_counts.len() != clients
                || client_request_counts.values().sum::<usize>() != request_count
                || query_counts.iter().any(|count| *count == 0)
                || query_counts.iter().sum::<usize>() != request_count
            {
                return Err(format!(
                    "concurrency level {clients} epoch {epoch} has inconsistent client, query, or parity evidence"
                ));
            }
            for (total, count) in total_query_counts.iter_mut().zip(query_counts) {
                *total += count;
            }
            let query_request_counts = QueryName::ALL
                .into_iter()
                .zip(query_counts)
                .map(|(name, count)| (name.as_str().to_owned(), count))
                .collect();
            let exact_query_checks = QueryName::ALL
                .into_iter()
                .map(|name| (name.as_str().to_owned(), clients))
                .collect::<BTreeMap<_, _>>();
            epoch_reports.push(ConcurrencyEpochReport {
                epoch,
                warmed_client_ids,
                pre_epoch_exact_query_checks: exact_query_checks.clone(),
                post_epoch_verified_client_ids,
                post_epoch_exact_query_checks: exact_query_checks,
                client_request_counts,
                request_count,
                wall_time_ms,
                throughput_requests_per_second: request_count as f64 * 1000.0 / wall_time_ms,
                latency_p50_ms: percentile(&latencies_ms, 0.5),
                latency_p95_ms: percentile(&latencies_ms, 0.95),
                latency_p99_ms: percentile(&latencies_ms, 0.99),
                query_request_counts,
                semantic_parity: true,
            });
        }

        let throughputs = epoch_reports
            .iter()
            .map(|epoch| epoch.throughput_requests_per_second)
            .collect::<Vec<_>>();
        let throughput_mean = mean(&throughputs);
        let throughput_cv = (throughputs
            .iter()
            .map(|value| (value - throughput_mean).powi(2))
            .sum::<f64>()
            / throughputs.len() as f64)
            .sqrt()
            / throughput_mean;
        let wall_times = epoch_reports
            .iter()
            .map(|epoch| epoch.wall_time_ms)
            .collect::<Vec<_>>();
        let p50_values = epoch_reports
            .iter()
            .map(|epoch| epoch.latency_p50_ms)
            .collect::<Vec<_>>();
        let p95_values = epoch_reports
            .iter()
            .map(|epoch| epoch.latency_p95_ms)
            .collect::<Vec<_>>();
        let p99_values = epoch_reports
            .iter()
            .map(|epoch| epoch.latency_p99_ms)
            .collect::<Vec<_>>();
        let query_request_counts = QueryName::ALL
            .into_iter()
            .zip(total_query_counts)
            .map(|(name, count)| (name.as_str().to_owned(), count))
            .collect();
        reports.insert(
            clients.to_string(),
            ConcurrencyReport {
                clients,
                epoch_count,
                minimum_requests_per_client,
                minimum_wall_time_ms: minimum_seconds * 1000.0,
                total_request_count: epoch_reports.iter().map(|epoch| epoch.request_count).sum(),
                total_query_request_counts: query_request_counts,
                median_epoch_wall_time_ms: percentile(&wall_times, 0.5),
                median_epoch_throughput_requests_per_second: percentile(&throughputs, 0.5),
                minimum_epoch_throughput_requests_per_second: throughputs
                    .iter()
                    .copied()
                    .fold(f64::INFINITY, f64::min),
                maximum_epoch_throughput_requests_per_second: throughputs
                    .iter()
                    .copied()
                    .fold(f64::NEG_INFINITY, f64::max),
                epoch_throughput_cv: throughput_cv,
                median_epoch_latency_p50_ms: percentile(&p50_values, 0.5),
                median_epoch_latency_p95_ms: percentile(&p95_values, 0.5),
                median_epoch_latency_p99_ms: percentile(&p99_values, 0.5),
                epochs: epoch_reports,
                semantic_parity: true,
            },
        );
    }
    Ok(reports)
}

fn write_artifact(output: &Path, value: &impl Serialize) -> Result<(), String> {
    let mut encoded = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    encoded.push(b'\n');
    if output == Path::new("-") {
        io::stdout()
            .lock()
            .write_all(&encoded)
            .map_err(|error| error.to_string())?;
    } else {
        if let Some(parent) = output.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        fs::write(output, encoded).map_err(|error| error.to_string())?;
    }
    Ok(())
}

async fn run_memory_only(args: &Args, name: QueryName, q5_sql: &str) -> Result<(), String> {
    let reference_path = args
        .reference
        .as_ref()
        .expect("memory-only mode requires a reference artifact");
    let reference: ReferenceArtifact =
        serde_json::from_slice(&fs::read(reference_path).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
    validate_reference(&reference, args)?;
    let expected = reference.queries[name.as_str()].canonical_output.clone();
    let expected_rows = args
        .expected_rows
        .expect("memory-only mode requires expected rows");
    if expected_rows != expected.rows {
        return Err(format!(
            "{} memory-only expected row count {expected_rows} differs from reference {}",
            name.as_str(),
            expected.rows
        ));
    }
    let client = connect_client(&args.database_url).await?;
    let (release, environment) = release_environment(&client).await?;
    validate_same_harness(&reference, &environment)?;
    let query = prepare_capsule(&client, name, query_sql(name, q5_sql)).await?;

    let baseline = process_memory_snapshot()?;
    let result = query
        .execute(&client, &[])
        .await
        .map_err(|error| error.to_string())?;
    let (capsule_bytes, rows) = materialize_result(name, result)?;
    let logical_rows_materialized = rows.len();
    let owned_rows_bytes = rows.allocated_bytes();
    let after = process_memory_snapshot()?;
    if logical_rows_materialized != expected_rows {
        return Err(format!(
            "{} memory-only query returned {logical_rows_materialized} rows, expected {expected_rows}",
            name.as_str()
        ));
    }
    // Keep external-ID maps and canonicalization outside the sampled memory
    // boundary while still requiring exact multiset parity before accepting
    // the diagnostic.
    let ids = load_external_ids(&client).await?;
    let (actual, _) = canonicalize(&rows, &ids)?;
    if actual != expected {
        return Err(format!(
            "{} memory-only fingerprint mismatch: candidate={actual:?}, OCPQ={expected:?}",
            name.as_str()
        ));
    }
    drop(rows);

    let artifact = MemoryOnlyArtifact {
        schema_version: 1,
        release,
        environment,
        reference_source: reference.source,
        mode: "fresh-process-peak-rss-diagnostic",
        query: name.as_str(),
        measurement_boundary: "Linux fresh-process diagnostic: baseline after connection and one prepared query; then one query/fetch, BindingCapsule decode, and complete owned-row expansion without canonicalization; RSS and VmHWM sampled while owned rows remain live",
        correctness_boundary: "after the RSS/VmHWM sample, load external-ID maps and require exact canonical row-multiset, violation/value, and SHA-256 parity with the pinned same-host OCPQ artifact",
        expected_rows,
        logical_rows_materialized,
        canonical_output: actual,
        semantic_parity: true,
        capsule_bytes,
        owned_rows_bytes,
        baseline_rss_bytes: baseline.rss_bytes,
        baseline_vmhwm_bytes: baseline.vmhwm_bytes,
        after_rss_bytes: after.rss_bytes,
        after_vmhwm_bytes: after.vmhwm_bytes,
        rss_delta_bytes: memory_delta(after.rss_bytes, baseline.rss_bytes)?,
        vmhwm_delta_bytes: memory_delta(after.vmhwm_bytes, baseline.vmhwm_bytes)?,
        peak_over_baseline_rss_bytes: after.vmhwm_bytes.saturating_sub(baseline.rss_bytes),
        tokio_worker_threads_env: std::env::var("TOKIO_WORKER_THREADS").ok(),
        q5_sql: if args.q5_sql_file.is_some() {
            "validated --q5-sql-file override".to_owned()
        } else {
            "ocpm.binding_relation_universal_equal".to_owned()
        },
    };
    write_artifact(&args.output, &artifact)
}

async fn run(args: Args) -> Result<(), String> {
    let q5_sql = if let Some(path) = &args.q5_sql_file {
        validate_q5_sql(&fs::read_to_string(path).map_err(|error| error.to_string())?)?
    } else {
        Q5_DEFAULT_SQL.to_owned()
    };
    if let Some(name) = args.memory_only_query {
        return run_memory_only(&args, name, &q5_sql).await;
    }
    let reference_path = args
        .reference
        .as_ref()
        .expect("normal mode requires a reference artifact");
    let reference: ReferenceArtifact =
        serde_json::from_slice(&fs::read(reference_path).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
    validate_reference(&reference, &args)?;
    let client = connect_client(&args.database_url).await?;
    let (release, environment) = release_environment(&client).await?;
    validate_same_harness(&reference, &environment)?;
    let ids = load_external_ids(&client).await?;
    let statements = prepare_query_set(&client, &q5_sql).await?;
    let mut queries = BTreeMap::new();
    for (name, query) in QueryName::ALL.into_iter().zip(&statements) {
        eprintln!(
            "running {}: {} warmups + {} measured",
            name.as_str(),
            args.warmups,
            args.runs
        );
        let report = run_query(
            &client,
            name,
            query,
            &reference.queries[name.as_str()],
            &ids,
            &args,
        )
        .await?;
        queries.insert(name.as_str().to_owned(), report);
    }
    let concurrency = concurrency(
        &ConcurrencyConfig {
            database_url: &args.database_url,
            q5_sql: &q5_sql,
            levels: &args.concurrency,
            minimum_requests_per_client: args.requests_per_client,
            epoch_count: args.concurrency_epochs,
            minimum_seconds: args.concurrency_minimum_seconds,
        },
        &reference,
        &ids,
    )
    .await?;
    let author_published = queries
        .values()
        .map(|query| query.author_published_ocpq_mean_ms)
        .collect::<Vec<_>>();
    let same_host = queries
        .values()
        .map(|query| query.reference_ocpq_mean_ms)
        .collect::<Vec<_>>();
    let candidate = queries
        .values()
        .map(|query| query.mean_ms)
        .collect::<Vec<_>>();
    let speedups = queries
        .values()
        .map(|query| query.speedup_vs_reference_ocpq)
        .collect::<Vec<_>>();
    let concurrency_16_to_1_median_epoch_throughput_scaling = concurrency
        .get("16")
        .zip(concurrency.get("1"))
        .map(|(sixteen, one)| {
            sixteen.median_epoch_throughput_requests_per_second
                / one.median_epoch_throughput_requests_per_second
        });
    let summary = SummaryReport {
        author_published_ocpq_geometric_mean_ms: geometric_mean(&author_published),
        same_host_ocpq_geometric_mean_ms: geometric_mean(&same_host),
        candidate_geometric_mean_ms: geometric_mean(&candidate),
        speedup_geometric_mean: geometric_mean(&speedups),
        minimum_query_speedup: speedups.iter().copied().fold(f64::INFINITY, f64::min),
        all_queries_exact: queries.values().all(|query| query.semantic_parity),
        maximum_owned_rows_bytes: queries
            .values()
            .map(|query| query.memory.owned_rows_bytes)
            .max()
            .unwrap_or(0),
        concurrency_16_to_1_median_epoch_throughput_scaling,
    };
    let storage = storage(&client).await?;
    let artifact = OutputArtifact {
        schema_version: 4,
        generated_at_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .as_millis(),
        release,
        environment,
        reference_source: reference.source,
        method: MethodReport {
            warmups_per_query: args.warmups,
            measured_runs_per_query: args.runs,
            timing_boundary: "persistent prepared PostgreSQL query/fetch, BindingCapsule decode, and full logical root-row materialization into owned Rust structs",
            correctness_boundary: "external-ID normalization, canonical sorting, compact JSON, and SHA-256 after the headline clock",
            non_headline_diagnostic_boundary: "diagnostic only, not the OCPQ-comparable headline: headline time plus separately timed external-ID canonicalization, sorting, compact JSON, and SHA-256",
            memory_diagnostic_boundary: "separate untimed materialization pass with deterministic owned-row bytes and retained PostgreSQL backend memory sampled immediately before and after; not peak memory and no allocator instrumentation",
            storage_scope: "pg_total_relation_size for the complete retained serving representation in the ocpm schema; total includes indexes, and binding summary covers binding_% relations",
            concurrency_boundary: "per-client persistent PostgreSQL connection and prepared Q1-Q7 set; one exact canonical Q1-Q7 check per client occurs before and after every timed epoch; timed requests cycle Q1-Q7, enforce the reference logical-row count, and include query/fetch, capsule decode, and complete owned-row expansion but exclude canonicalization",
            concurrency_protocol: format!(
                "{} epochs per client level; every client completes at least {} requests and the shared epoch wall clock runs for at least {:.3} seconds; aggregate throughput, wall time, and latency fields are medians of the corresponding epoch values",
                args.concurrency_epochs, args.requests_per_client, args.concurrency_minimum_seconds
            ),
            q5_sql: if args.q5_sql_file.is_some() {
                "validated --q5-sql-file override".to_owned()
            } else {
                "ocpm.binding_relation_universal_equal".to_owned()
            },
        },
        summary,
        storage,
        queries,
        concurrency,
    };
    write_artifact(&args.output, &artifact)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    };
    if let Err(error) = run(args).await {
        eprintln!("candidate benchmark failed: {error}");
        std::process::exit(1);
    }
}
