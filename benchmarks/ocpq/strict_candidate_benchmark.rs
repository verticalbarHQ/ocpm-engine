//! Strict all-node pg_ocpm candidate benchmark for OCPQ Q1-Q7.
//!
//! The measured boundary is deliberately narrow but complete: one prepared
//! PostgreSQL request generates and fetches every query-tree node, every OCPB
//! capsule is decoded, and all node rows (including exact violation reasons
//! and the typed Q6 label) are copied into owned Rust values before the clock
//! stops. External-ID canonicalization, sorting, JSON serialization, hashing,
//! and comparison with the schema-4 OCPQ artifact happen outside that clock,
//! matching the reference harness.

use std::{
    collections::{BTreeMap, HashMap},
    fmt::Write as _,
    fs,
    hint::black_box,
    io::{self, Write as _},
    path::{Path, PathBuf},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use ocpm_core::binding::{BindingCapsule, BindingSchema};
use ocpm_postgres::{BindingTreeQueryResult, PreparedBindingTreeQuery};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, NoTls};

const DEFAULT_REFERENCE: &str = ".benchmarks/ocpq-reproduced-strict-all-node-preview.json";
const EXPECTED_OCPQ_COMMIT: &str = "80457e561edd7bb9e142d959dd7e0f96e6b03f2f";
const EXPECTED_EVAL_COMMIT: &str = "846dd4eb9f8600ae42355968453a9412ea4759c2";
const EXPECTED_DATASET_SHA256: &str =
    "02ac333a2c194b5a411cb8527dd64b4845e5110752d2ffddb531e48ce97556d7";
const EXPECTED_QUERY_FILES_SHA256: &str =
    "387aeb31398d86ef8e7b15393649cbabe75be56185fd67d27021744025873466";
const MEASURED_RUNS: usize = 10;

const Q1_SQL: &str = r#"
SELECT
    ocpm.binding_object_activity_count(
        objects.object_ids,
        coalesce(activity.object_ids, ARRAY[]::bigint[]),
        1, 1
    ) AS root,
    ocpm.binding_ids(
        coalesce(activity.object_ids, ARRAY[]::bigint[]),
        coalesce(activity.event_ids, ARRAY[]::bigint[])
    ) AS activity_child
FROM ocpm.binding_object objects
LEFT JOIN ocpm.binding_activity activity
  ON activity.dataset_id = objects.dataset_id
 AND activity.tenant_id = objects.tenant_id
 AND activity.object_type = objects.object_type
 AND activity.activity = 'A_Submitted'
WHERE objects.dataset_id = ocpm.dataset_id('bpic2017-ocpq')
  AND objects.tenant_id = 1
  AND objects.object_type = 'Application'
"#;

const Q2_SQL: &str = r#"
SELECT
    ocpm.binding_neighbor_eventually(
        source.object_ids,
        source.event_ids,
        source.event_timestamps,
        coalesce(target.object_ids, ARRAY[]::bigint[]),
        coalesce(target.event_timestamps, ARRAY[]::timestamptz[])
    ) AS root,
    ocpm.binding_same_object_event_pairs(
        source.object_ids,
        source.event_ids,
        source.event_timestamps,
        coalesce(target.object_ids, ARRAY[]::bigint[]),
        coalesce(target.event_ids, ARRAY[]::bigint[]),
        coalesce(target.event_timestamps, ARRAY[]::timestamptz[]),
        0::bigint,
        (-1)::bigint
    ) AS temporal_child
FROM ocpm.binding_activity source
LEFT JOIN ocpm.binding_activity target
  ON target.dataset_id = source.dataset_id
 AND target.tenant_id = source.tenant_id
 AND target.object_type = source.object_type
 AND target.activity = 'O_Returned'
WHERE source.dataset_id = ocpm.dataset_id('bpic2017-ocpq')
  AND source.tenant_id = 1
  AND source.object_type = 'Offer'
  AND source.activity = 'O_Created'
"#;

const Q3_SQL: &str = r#"
SELECT
    ocpm.binding_event_object_count(
        events.event_ids,
        coalesce(activity.event_ids, ARRAY[]::bigint[]),
        1, 1
    ) AS root,
    ocpm.binding_ids(
        coalesce(activity.object_ids, ARRAY[]::bigint[]),
        coalesce(activity.event_ids, ARRAY[]::bigint[])
    ) AS event_object_child
FROM ocpm.binding_event events
LEFT JOIN ocpm.binding_activity activity
  ON activity.dataset_id = events.dataset_id
 AND activity.tenant_id = events.tenant_id
 AND activity.object_type = 'Offer'
 AND activity.activity = events.activity
WHERE events.dataset_id = ocpm.dataset_id('bpic2017-ocpq')
  AND events.tenant_id = 1
  AND events.activity = 'O_Returned'
"#;

const Q4_SQL: &str = r#"
SELECT
    ocpm.binding_neighbor_eventually(
        source.object_ids,
        source.event_ids,
        source.event_timestamps,
        coalesce(neighbor.source_object_ids, ARRAY[]::bigint[]),
        coalesce(neighbor.event_timestamps, ARRAY[]::timestamptz[])
    ) AS root,
    ocpm.binding_neighbor_event_pairs(
        source.object_ids,
        source.event_ids,
        source.event_timestamps,
        coalesce(neighbor.source_object_ids, ARRAY[]::bigint[]),
        coalesce(neighbor.target_object_ids, ARRAY[]::bigint[]),
        coalesce(neighbor.event_ids, ARRAY[]::bigint[]),
        coalesce(neighbor.event_timestamps, ARRAY[]::timestamptz[]),
        0::bigint,
        (-1)::bigint
    ) AS neighbor_child
FROM ocpm.binding_activity source
LEFT JOIN ocpm.binding_neighbor_activity neighbor
  ON neighbor.dataset_id = source.dataset_id
 AND neighbor.tenant_id = source.tenant_id
 AND neighbor.source_object_type = source.object_type
 AND neighbor.target_object_type = 'Offer'
 AND neighbor.activity = 'O_Accepted'
WHERE source.dataset_id = ocpm.dataset_id('bpic2017-ocpq')
  AND source.tenant_id = 1
  AND source.object_type = 'Application'
  AND source.activity = 'A_Accepted'
"#;

const Q5_SQL: &str = r#"
SELECT
    ocpm.binding_relation_universal_equal(summary.payload) AS root,
    ocpm.binding_relation_children(summary.payload) AS relation_child
FROM ocpm.binding_relation_summary summary
WHERE summary.dataset_id = ocpm.dataset_id('bpic2017-ocpq')
  AND summary.tenant_id = 1
  AND summary.source_object_type = 'Application'
  AND summary.source_activity = 'A_Accepted'
  AND summary.target_object_type = 'Offer'
  AND summary.target_activity = 'O_Created'
  AND summary.related_object_type = 'Case_R'
"#;

const Q6_SQL: &str = r#"
SELECT
    ocpm.binding_max_activity_delay(
        source.object_ids,
        source.event_timestamps,
        coalesce(target.object_ids, ARRAY[]::bigint[]),
        coalesce(target.event_timestamps, ARRAY[]::timestamptz[])
    ) AS root,
    ocpm.binding_same_object_event_pairs(
        source.object_ids,
        source.event_ids,
        source.event_timestamps,
        coalesce(target.object_ids, ARRAY[]::bigint[]),
        coalesce(target.event_ids, ARRAY[]::bigint[]),
        coalesce(target.event_timestamps, ARRAY[]::timestamptz[]),
        '-9223372036854775808'::bigint,
        (-1)::bigint
    ) AS same_object_child
FROM ocpm.binding_activity source
LEFT JOIN ocpm.binding_activity target
  ON target.dataset_id = source.dataset_id
 AND target.tenant_id = source.tenant_id
 AND target.object_type = source.object_type
 AND target.activity = 'O_Accepted'
WHERE source.dataset_id = ocpm.dataset_id('bpic2017-ocpq')
  AND source.tenant_id = 1
  AND source.object_type = 'Offer'
  AND source.activity = 'O_Created'
"#;

const Q7_SQL: &str = r#"
SELECT ocpm.binding_neighbor_pairs(
    coalesce(selected.source_object_ids, ARRAY[]::bigint[]),
    coalesce(selected.target_object_ids, ARRAY[]::bigint[]),
    coalesce(selected.event_ids, ARRAY[]::bigint[])
) AS root
FROM (SELECT 1) singleton
LEFT JOIN LATERAL (
    SELECT source_object_ids, target_object_ids, event_ids
    FROM ocpm.binding_neighbor_activity
    WHERE dataset_id = ocpm.dataset_id('bpic2017-ocpq')
      AND tenant_id = 1
      AND source_object_type = 'Application'
      AND target_object_type = 'Offer'
      AND activity = 'O_Created'
) selected ON true
"#;

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
        match value.to_ascii_uppercase().as_str() {
            "Q1" => Ok(Self::Q1),
            "Q2" => Ok(Self::Q2),
            "Q3" => Ok(Self::Q3),
            "Q4" => Ok(Self::Q4),
            "Q5" => Ok(Self::Q5),
            "Q6" => Ok(Self::Q6),
            "Q7" => Ok(Self::Q7),
            _ => Err(format!("query must be Q1-Q7, found {value:?}")),
        }
    }

    fn sql(self) -> &'static str {
        match self {
            Self::Q1 => Q1_SQL,
            Self::Q2 => Q2_SQL,
            Self::Q3 => Q3_SQL,
            Self::Q4 => Q4_SQL,
            Self::Q5 => Q5_SQL,
            Self::Q6 => Q6_SQL,
            Self::Q7 => Q7_SQL,
        }
    }

    fn specs(self) -> &'static [NodeSpec] {
        match self {
            Self::Q1 => &Q1_NODES,
            Self::Q2 => &Q2_NODES,
            Self::Q3 => &Q3_NODES,
            Self::Q4 => &Q4_NODES,
            Self::Q5 => &Q5_NODES,
            Self::Q6 => &Q6_NODES,
            Self::Q7 => &Q7_NODES,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Column {
    Object(usize),
    Event(usize),
}

#[derive(Clone, Copy, Debug)]
enum NodeKind {
    Id1Violation,
    Id2,
    Id2Violation,
    Id3,
    Id3Violation,
    Id4,
    Id5Violation,
    FactorizedId5,
    TypedDuration,
}

#[derive(Clone, Copy, Debug)]
struct NodeSpec {
    kind: NodeKind,
    columns: &'static [Column],
    object_variables: &'static [usize],
    event_variables: &'static [usize],
    label_names: &'static [&'static str],
}

const Q1_NODES: [NodeSpec; 2] = [
    NodeSpec {
        kind: NodeKind::Id1Violation,
        columns: &[Column::Object(0)],
        object_variables: &[0],
        event_variables: &[],
        label_names: &[],
    },
    NodeSpec {
        kind: NodeKind::Id2,
        columns: &[Column::Object(0), Column::Event(0)],
        object_variables: &[0],
        event_variables: &[0],
        label_names: &[],
    },
];

const Q2_NODES: [NodeSpec; 2] = [
    NodeSpec {
        kind: NodeKind::Id2Violation,
        columns: &[Column::Object(0), Column::Event(0)],
        object_variables: &[0],
        event_variables: &[0],
        label_names: &[],
    },
    NodeSpec {
        kind: NodeKind::Id3,
        columns: &[Column::Object(0), Column::Event(0), Column::Event(1)],
        object_variables: &[0],
        event_variables: &[0, 1],
        label_names: &[],
    },
];

const Q3_NODES: [NodeSpec; 2] = [
    NodeSpec {
        kind: NodeKind::Id1Violation,
        columns: &[Column::Event(0)],
        object_variables: &[],
        event_variables: &[0],
        label_names: &[],
    },
    NodeSpec {
        kind: NodeKind::Id2,
        columns: &[Column::Object(1), Column::Event(0)],
        object_variables: &[1],
        event_variables: &[0],
        label_names: &[],
    },
];

const Q4_NODES: [NodeSpec; 2] = [
    NodeSpec {
        kind: NodeKind::Id2Violation,
        columns: &[Column::Object(0), Column::Event(0)],
        object_variables: &[0],
        event_variables: &[0],
        label_names: &[],
    },
    NodeSpec {
        kind: NodeKind::Id4,
        columns: &[
            Column::Object(0),
            Column::Object(1),
            Column::Event(0),
            Column::Event(1),
        ],
        object_variables: &[0, 1],
        event_variables: &[0, 1],
        label_names: &[],
    },
];

const Q5_NODES: [NodeSpec; 2] = [
    NodeSpec {
        kind: NodeKind::Id3Violation,
        columns: &[Column::Object(0), Column::Object(1), Column::Event(0)],
        object_variables: &[0, 1],
        event_variables: &[0],
        label_names: &[],
    },
    NodeSpec {
        kind: NodeKind::Id5Violation,
        columns: &[
            Column::Object(0),
            Column::Object(1),
            Column::Object(2),
            Column::Event(0),
            Column::Event(1),
        ],
        object_variables: &[0, 1, 2],
        event_variables: &[0, 1],
        label_names: &[],
    },
];

const Q6_NODES: [NodeSpec; 2] = [
    NodeSpec {
        kind: NodeKind::TypedDuration,
        columns: &[],
        object_variables: &[],
        event_variables: &[],
        label_names: &["max_dur"],
    },
    NodeSpec {
        kind: NodeKind::Id3,
        columns: &[Column::Object(0), Column::Event(0), Column::Event(1)],
        object_variables: &[0],
        event_variables: &[0, 1],
        label_names: &[],
    },
];

const Q7_NODES: [NodeSpec; 1] = [NodeSpec {
    kind: NodeKind::FactorizedId5,
    columns: &[
        Column::Object(0),
        Column::Object(1),
        Column::Object(2),
        Column::Event(1),
        Column::Event(2),
    ],
    object_variables: &[0, 1, 2],
    event_variables: &[1, 2],
    label_names: &[],
}];

#[derive(Debug)]
struct Args {
    database_url: String,
    reference: PathBuf,
    output: PathBuf,
    query: Option<QueryName>,
    memory_only_query: Option<QueryName>,
    resource_gates: bool,
}

fn usage() -> &'static str {
    "usage: strict_candidate_benchmark --output PATH [--reference PATH] \
     [--database-url URL] [--query Q1..Q7 | --memory-only-query Q1..Q7 | \
     --resource-gates]"
}

fn parse_args() -> Result<Args, String> {
    let mut database_url = std::env::var("OCPM_DATABASE_URL").ok();
    let mut reference = PathBuf::from(DEFAULT_REFERENCE);
    let mut output = None;
    let mut query = None;
    let mut memory_only_query = None;
    let mut resource_gates = false;
    let mut values = std::env::args().skip(1);
    while let Some(flag) = values.next() {
        let mut value = || {
            values
                .next()
                .ok_or_else(|| format!("{flag} requires a value"))
        };
        match flag.as_str() {
            "--database-url" => database_url = Some(value()?),
            "--reference" => reference = PathBuf::from(value()?),
            "--output" => output = Some(PathBuf::from(value()?)),
            "--query" => query = Some(QueryName::parse(&value()?)?),
            "--memory-only-query" => memory_only_query = Some(QueryName::parse(&value()?)?),
            "--resource-gates" => resource_gates = true,
            "--help" | "-h" => return Err(usage().to_owned()),
            _ => return Err(format!("unknown argument {flag:?}\n{}", usage())),
        }
    }
    let selected_modes = usize::from(query.is_some())
        + usize::from(memory_only_query.is_some())
        + usize::from(resource_gates);
    if selected_modes > 1 {
        return Err(
            "--query, --memory-only-query, and --resource-gates are mutually exclusive".to_owned(),
        );
    }
    Ok(Args {
        database_url: database_url
            .ok_or_else(|| "--database-url or OCPM_DATABASE_URL is required".to_owned())?,
        reference,
        output: output.ok_or_else(|| "--output is required".to_owned())?,
        query,
        memory_only_query,
        resource_gates,
    })
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

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ReferenceEnvironment {
    benchmark_host_id: String,
    source_revision: String,
    source_tree_clean: bool,
    platform: String,
    machine: String,
    logical_cpus_visible: usize,
}

#[derive(Debug, Deserialize)]
struct ReferenceMethod {
    warmups_per_query: usize,
    measured_runs_per_query: usize,
    upstream_backend_measure_performance_iterations: usize,
    upstream_backend_warmups: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NodeFingerprint {
    node_index: usize,
    object_variables: Vec<usize>,
    event_variables: Vec<usize>,
    label_names: Vec<String>,
    situation_count: usize,
    situation_violated_count: usize,
    violation_reason_counts: BTreeMap<String, usize>,
    canonical_json_bytes: usize,
    canonical_sha256: String,
}

#[derive(Debug, Deserialize)]
struct ReferenceQuery {
    mean_ms: f64,
    runs_ms: Vec<f64>,
    all_node_situations: usize,
    root_node: usize,
    #[serde(default)]
    q6_root_label: Option<Value>,
    #[serde(default)]
    q6_duration_microseconds: Option<i64>,
    nodes: Vec<NodeFingerprint>,
}

#[derive(Debug, Deserialize)]
struct ReferenceArtifact {
    schema_version: u8,
    source: ReferenceSource,
    environment: ReferenceEnvironment,
    method: ReferenceMethod,
    queries: BTreeMap<String, ReferenceQuery>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ViolationReason {
    ConstraintNotSatisfied(usize),
}

impl ViolationReason {
    fn as_json(self) -> Value {
        match self {
            Self::ConstraintNotSatisfied(index) => {
                json!({"ConstraintNotSatisfied": index})
            }
        }
    }
}

#[derive(Debug)]
struct OwnedIdRow<const N: usize> {
    ids: [i64; N],
    violation: Option<ViolationReason>,
}

#[derive(Debug)]
struct OwnedDurationRow {
    duration_microseconds: i64,
    label_name: String,
    label_type: String,
    label_value: String,
}

#[derive(Debug)]
enum OwnedRows {
    Id1(Vec<OwnedIdRow<1>>),
    Id2(Vec<OwnedIdRow<2>>),
    Id3(Vec<OwnedIdRow<3>>),
    Id4(Vec<OwnedIdRow<4>>),
    Id5(Vec<OwnedIdRow<5>>),
    Duration(Vec<OwnedDurationRow>),
}

impl OwnedRows {
    fn len(&self) -> usize {
        match self {
            Self::Id1(rows) => rows.len(),
            Self::Id2(rows) => rows.len(),
            Self::Id3(rows) => rows.len(),
            Self::Id4(rows) => rows.len(),
            Self::Id5(rows) => rows.len(),
            Self::Duration(rows) => rows.len(),
        }
    }
}

#[derive(Debug)]
struct OwnedNode {
    object_variables: Vec<usize>,
    event_variables: Vec<usize>,
    label_names: Vec<String>,
    rows: OwnedRows,
}

#[derive(Debug)]
struct OwnedTree {
    nodes: Vec<OwnedNode>,
    capsule_bytes: usize,
}

fn duration_string(microseconds: i64) -> Result<String, String> {
    if microseconds < 0 {
        return Err("OCPQ Q6 produced a negative duration".to_owned());
    }
    let mut remaining = microseconds;
    let hours = remaining / 3_600_000_000;
    remaining %= 3_600_000_000;
    let minutes = remaining / 60_000_000;
    remaining %= 60_000_000;
    let seconds = remaining / 1_000_000;
    let fractional = remaining % 1_000_000;
    let mut output = String::new();
    if hours != 0 {
        write!(&mut output, "{hours}h").expect("writing to String cannot fail");
    }
    if minutes != 0 {
        write!(&mut output, "{minutes}m").expect("writing to String cannot fail");
    }
    if fractional == 0 {
        write!(&mut output, "{seconds}s").expect("writing to String cannot fail");
    } else {
        let mut fraction = format!("{fractional:06}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        write!(&mut output, "{seconds}.{fraction}s").expect("writing to String cannot fail");
    }
    Ok(output)
}

fn materialize_ids<const N: usize>(
    capsule: &BindingCapsule,
    with_violation: bool,
) -> Result<Vec<OwnedIdRow<N>>, String> {
    let mut output = Vec::with_capacity(capsule.row_count());
    for row in capsule.rows() {
        let ids: [i64; N] = row.ids().try_into().map_err(|_| {
            format!(
                "binding node returned {} identifier columns, expected {N}",
                row.ids().len()
            )
        })?;
        if row.label.is_some() || row.value.is_some() {
            return Err("identifier node unexpectedly returned a label or scalar value".to_owned());
        }
        let violation = match (with_violation, row.violated) {
            (true, Some(true)) => Some(ViolationReason::ConstraintNotSatisfied(0)),
            (true, Some(false)) => None,
            (true, None) => return Err("constraint node is missing its violation bit".to_owned()),
            (false, None) => None,
            (false, Some(_)) => {
                return Err("unconstrained node unexpectedly returned a violation bit".to_owned());
            }
        };
        output.push(OwnedIdRow { ids, violation });
    }
    Ok(output)
}

fn require_schema(capsule: &BindingCapsule, expected: BindingSchema) -> Result<(), String> {
    if capsule.schema() != expected {
        return Err(format!(
            "binding node used schema {:?}, expected {expected:?}",
            capsule.schema()
        ));
    }
    Ok(())
}

fn materialize_node(capsule: &BindingCapsule, spec: NodeSpec) -> Result<OwnedNode, String> {
    let rows = match spec.kind {
        NodeKind::Id1Violation => {
            require_schema(capsule, BindingSchema::IdViolation)?;
            OwnedRows::Id1(materialize_ids(capsule, true)?)
        }
        NodeKind::Id2 => {
            require_schema(capsule, BindingSchema::TwoIds)?;
            OwnedRows::Id2(materialize_ids(capsule, false)?)
        }
        NodeKind::Id2Violation => {
            require_schema(capsule, BindingSchema::IdIdViolation)?;
            OwnedRows::Id2(materialize_ids(capsule, true)?)
        }
        NodeKind::Id3 => {
            require_schema(capsule, BindingSchema::ThreeIds)?;
            OwnedRows::Id3(materialize_ids(capsule, false)?)
        }
        NodeKind::Id3Violation => {
            require_schema(capsule, BindingSchema::ThreeIdsViolation)?;
            OwnedRows::Id3(materialize_ids(capsule, true)?)
        }
        NodeKind::Id4 => {
            require_schema(capsule, BindingSchema::FourIds)?;
            OwnedRows::Id4(materialize_ids(capsule, false)?)
        }
        NodeKind::Id5Violation => {
            require_schema(capsule, BindingSchema::FiveIdsViolation)?;
            OwnedRows::Id5(materialize_ids(capsule, true)?)
        }
        NodeKind::FactorizedId5 => {
            require_schema(capsule, BindingSchema::PairGroups)?;
            OwnedRows::Id5(materialize_ids(capsule, false)?)
        }
        NodeKind::TypedDuration => {
            require_schema(capsule, BindingSchema::Value)?;
            let mut rows = Vec::with_capacity(capsule.row_count());
            for row in capsule.rows() {
                if !row.ids().is_empty() || row.label.is_some() || row.violated.is_some() {
                    return Err("Q6 scalar node returned unexpected binding fields".to_owned());
                }
                let seconds = row.value.ok_or("Q6 scalar node is missing its value")?;
                let micros = seconds * 1_000_000.0;
                if !micros.is_finite() || micros < i64::MIN as f64 || micros > i64::MAX as f64 {
                    return Err("Q6 duration cannot be represented in microseconds".to_owned());
                }
                let duration_microseconds = micros.round() as i64;
                rows.push(OwnedDurationRow {
                    duration_microseconds,
                    label_name: "max_dur".to_owned(),
                    label_type: "string".to_owned(),
                    label_value: duration_string(duration_microseconds)?,
                });
            }
            OwnedRows::Duration(rows)
        }
    };
    Ok(OwnedNode {
        object_variables: spec.object_variables.to_vec(),
        event_variables: spec.event_variables.to_vec(),
        label_names: spec
            .label_names
            .iter()
            .map(|value| (*value).to_owned())
            .collect(),
        rows,
    })
}

fn materialize_tree(query: QueryName, result: BindingTreeQueryResult) -> Result<OwnedTree, String> {
    let specs = query.specs();
    if result.capsules.len() != specs.len() {
        return Err(format!(
            "{} returned {} nodes, expected {}",
            query.as_str(),
            result.capsules.len(),
            specs.len()
        ));
    }
    let mut nodes = Vec::with_capacity(specs.len());
    for (capsule, spec) in result.capsules.iter().zip(specs.iter().copied()) {
        nodes.push(materialize_node(capsule, spec)?);
    }
    black_box(&nodes);
    Ok(OwnedTree {
        nodes,
        capsule_bytes: result.encoded_bytes,
    })
}

#[derive(Debug)]
struct ExternalData {
    objects: HashMap<i64, String>,
    events: HashMap<i64, String>,
    event_microseconds: HashMap<i64, i64>,
}

async fn load_external_data(client: &Client) -> Result<ExternalData, String> {
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
    let event_rows = client
        .query(
            "SELECT expanded.event_id, \
                    min(expanded.external_event_id)::text, \
                    (extract(epoch FROM min(expanded.event_timestamp)) \
                        * 1000000)::bigint \
             FROM ocpm.event_chunk chunk \
             CROSS JOIN LATERAL unnest( \
               chunk.event_sequences,chunk.external_event_ids, \
               chunk.event_timestamps \
             ) AS expanded(event_id,external_event_id,event_timestamp) \
             WHERE chunk.dataset_id=ocpm.dataset_id('bpic2017-ocpq') \
               AND chunk.tenant_id=1 \
             GROUP BY expanded.event_id",
            &[],
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut events = HashMap::with_capacity(event_rows.len());
    let mut event_microseconds = HashMap::with_capacity(event_rows.len());
    for row in event_rows {
        let event_id = row.get::<_, i64>(0);
        events.insert(event_id, row.get::<_, String>(1));
        event_microseconds.insert(event_id, row.get::<_, i64>(2));
    }
    Ok(ExternalData {
        objects,
        events,
        event_microseconds,
    })
}

fn external_id<'a>(
    mapping: &'a HashMap<i64, String>,
    key: i64,
    kind: &str,
) -> Result<&'a str, String> {
    mapping
        .get(&key)
        .map(String::as_str)
        .ok_or_else(|| format!("unknown {kind} identifier {key}"))
}

fn canonical_identifier_row<const N: usize>(
    row: &OwnedIdRow<N>,
    spec: NodeSpec,
    external: &ExternalData,
) -> Result<Value, String> {
    if spec.columns.len() != N {
        return Err("candidate node specification has the wrong identifier width".to_owned());
    }
    let mut objects = Vec::new();
    let mut events = Vec::new();
    for (column, id) in spec.columns.iter().zip(row.ids) {
        match *column {
            Column::Object(variable) => objects.push(json!([
                variable,
                external_id(&external.objects, id, "object")?
            ])),
            Column::Event(variable) => events.push(json!([
                variable,
                external_id(&external.events, id, "event")?
            ])),
        }
    }
    let violation = row.violation.map(ViolationReason::as_json);
    Ok(json!([objects, events, Vec::<Value>::new(), violation]))
}

fn canonical_node(
    node_index: usize,
    node: &OwnedNode,
    spec: NodeSpec,
    external: &ExternalData,
) -> Result<NodeFingerprint, String> {
    if node.object_variables != spec.object_variables
        || node.event_variables != spec.event_variables
        || node.label_names
            != spec
                .label_names
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>()
    {
        return Err(format!("node {node_index} owned variable manifest changed"));
    }
    let mut violation_reason_counts = BTreeMap::new();
    let mut situation_violated_count = 0_usize;
    let mut rows = match &node.rows {
        OwnedRows::Id1(values) => values
            .iter()
            .map(|row| canonical_identifier_row(row, spec, external))
            .collect::<Result<Vec<_>, _>>()?,
        OwnedRows::Id2(values) => values
            .iter()
            .map(|row| canonical_identifier_row(row, spec, external))
            .collect::<Result<Vec<_>, _>>()?,
        OwnedRows::Id3(values) => values
            .iter()
            .map(|row| canonical_identifier_row(row, spec, external))
            .collect::<Result<Vec<_>, _>>()?,
        OwnedRows::Id4(values) => values
            .iter()
            .map(|row| canonical_identifier_row(row, spec, external))
            .collect::<Result<Vec<_>, _>>()?,
        OwnedRows::Id5(values) => values
            .iter()
            .map(|row| canonical_identifier_row(row, spec, external))
            .collect::<Result<Vec<_>, _>>()?,
        OwnedRows::Duration(values) => values
            .iter()
            .map(|row| {
                json!([
                    Vec::<Value>::new(),
                    Vec::<Value>::new(),
                    [[
                        row.label_name.as_str(),
                        {
                            "type": row.label_type.as_str(),
                            "value": row.label_value.as_str()
                        }
                    ]],
                    Value::Null
                ])
            })
            .collect(),
    };
    match &node.rows {
        OwnedRows::Id1(values) => {
            for reason in values.iter().filter_map(|row| row.violation) {
                situation_violated_count += 1;
                let key =
                    serde_json::to_string(&reason.as_json()).map_err(|error| error.to_string())?;
                *violation_reason_counts.entry(key).or_insert(0) += 1;
            }
        }
        OwnedRows::Id2(values) => {
            for reason in values.iter().filter_map(|row| row.violation) {
                situation_violated_count += 1;
                let key =
                    serde_json::to_string(&reason.as_json()).map_err(|error| error.to_string())?;
                *violation_reason_counts.entry(key).or_insert(0) += 1;
            }
        }
        OwnedRows::Id3(values) => {
            for reason in values.iter().filter_map(|row| row.violation) {
                situation_violated_count += 1;
                let key =
                    serde_json::to_string(&reason.as_json()).map_err(|error| error.to_string())?;
                *violation_reason_counts.entry(key).or_insert(0) += 1;
            }
        }
        OwnedRows::Id4(values) => {
            for reason in values.iter().filter_map(|row| row.violation) {
                situation_violated_count += 1;
                let key =
                    serde_json::to_string(&reason.as_json()).map_err(|error| error.to_string())?;
                *violation_reason_counts.entry(key).or_insert(0) += 1;
            }
        }
        OwnedRows::Id5(values) => {
            for reason in values.iter().filter_map(|row| row.violation) {
                situation_violated_count += 1;
                let key =
                    serde_json::to_string(&reason.as_json()).map_err(|error| error.to_string())?;
                *violation_reason_counts.entry(key).or_insert(0) += 1;
            }
        }
        OwnedRows::Duration(_) => {}
    }
    rows.sort_by_cached_key(|row| {
        serde_json::to_string(row).expect("canonical OCPQ row must be serializable")
    });
    let encoded = serde_json::to_vec(&rows).map_err(|error| error.to_string())?;
    let mut canonical_sha256 = String::with_capacity(64);
    for byte in Sha256::digest(&encoded) {
        write!(&mut canonical_sha256, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(NodeFingerprint {
        node_index,
        object_variables: node.object_variables.clone(),
        event_variables: node.event_variables.clone(),
        label_names: node.label_names.clone(),
        situation_count: node.rows.len(),
        situation_violated_count,
        violation_reason_counts,
        canonical_json_bytes: encoded.len(),
        canonical_sha256,
    })
}

fn canonical_tree(
    query: QueryName,
    tree: &OwnedTree,
    external: &ExternalData,
) -> Result<Vec<NodeFingerprint>, String> {
    tree.nodes
        .iter()
        .zip(query.specs().iter().copied())
        .enumerate()
        .map(|(index, (node, spec))| canonical_node(index, node, spec, external))
        .collect()
}

fn q6_evidence(tree: &OwnedTree, external: &ExternalData) -> Result<(Value, i64), String> {
    let root = tree.nodes.first().ok_or("Q6 is missing its root node")?;
    let child = tree.nodes.get(1).ok_or("Q6 is missing its child node")?;
    let duration = match &root.rows {
        OwnedRows::Duration(rows) if rows.len() == 1 => &rows[0],
        OwnedRows::Duration(rows) => {
            return Err(format!(
                "Q6 root returned {} rows, expected one",
                rows.len()
            ));
        }
        _ => return Err("Q6 root did not materialize a typed duration".to_owned()),
    };
    let child_maximum = match &child.rows {
        OwnedRows::Id3(rows) => rows
            .iter()
            .map(|row| {
                let created = external
                    .event_microseconds
                    .get(&row.ids[1])
                    .ok_or_else(|| format!("Q6 child has unknown event {}", row.ids[1]))?;
                let accepted = external
                    .event_microseconds
                    .get(&row.ids[2])
                    .ok_or_else(|| format!("Q6 child has unknown event {}", row.ids[2]))?;
                accepted
                    .checked_sub(*created)
                    .ok_or_else(|| "Q6 child duration overflowed".to_owned())
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .max()
            .ok_or("Q6 child has no durations")?,
        _ => return Err("Q6 child did not materialize three-ID rows".to_owned()),
    };
    if child_maximum != duration.duration_microseconds {
        return Err(format!(
            "Q6 root duration {} differs from maximum child duration {child_maximum}",
            duration.duration_microseconds
        ));
    }
    Ok((
        json!({
            "type": duration.label_type.as_str(),
            "value": duration.label_value.as_str()
        }),
        duration.duration_microseconds,
    ))
}

fn mean(samples: &[f64]) -> f64 {
    samples.iter().sum::<f64>() / samples.len() as f64
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn median(samples: &[f64]) -> f64 {
    let mut ordered = samples.to_vec();
    ordered.sort_by(f64::total_cmp);
    let middle = ordered.len() / 2;
    if ordered.len() % 2 == 0 {
        (ordered[middle - 1] + ordered[middle]) / 2.0
    } else {
        ordered[middle]
    }
}

fn percentile(samples: &[f64], fraction: f64) -> f64 {
    let mut ordered = samples.to_vec();
    ordered.sort_by(f64::total_cmp);
    let index = ((ordered.len() as f64 * fraction).ceil() as usize)
        .saturating_sub(1)
        .min(ordered.len() - 1);
    ordered[index]
}

fn geometric_mean(samples: &[f64]) -> f64 {
    (samples.iter().map(|value| value.ln()).sum::<f64>() / samples.len() as f64).exp()
}

fn validate_reference(reference: &ReferenceArtifact) -> Result<(), String> {
    if reference.schema_version != 4 {
        return Err("strict OCPQ reference must use schema version 4".to_owned());
    }
    if reference.source.ocpq_eval_commit != EXPECTED_EVAL_COMMIT
        || reference.source.ocpq_commit != EXPECTED_OCPQ_COMMIT
        || reference.source.ocpq_version != "0.6.7"
        || reference.source.dataset_sqlite_sha256 != EXPECTED_DATASET_SHA256
        || reference.source.query_files_sha256 != EXPECTED_QUERY_FILES_SHA256
        || reference.source.author_published_results_commit != EXPECTED_EVAL_COMMIT
    {
        return Err(
            "strict OCPQ reference source pins are not the expected public inputs".to_owned(),
        );
    }
    if reference.method.warmups_per_query != 0
        || reference.method.measured_runs_per_query != MEASURED_RUNS
        || reference.method.upstream_backend_warmups != 0
        || reference
            .method
            .upstream_backend_measure_performance_iterations
            != MEASURED_RUNS
    {
        return Err(
            "strict OCPQ reference is not the upstream 0-warmup/10-run protocol".to_owned(),
        );
    }
    let expected_names = QueryName::ALL
        .map(|name| name.as_str().to_owned())
        .into_iter()
        .collect::<Vec<_>>();
    if reference.queries.keys().cloned().collect::<Vec<_>>() != expected_names {
        return Err("strict OCPQ reference must contain exactly Q1-Q7".to_owned());
    }
    for name in QueryName::ALL {
        let query = &reference.queries[name.as_str()];
        if query.root_node != 0
            || query.runs_ms.len() != MEASURED_RUNS
            || query.nodes.len() != name.specs().len()
            || query
                .nodes
                .iter()
                .map(|node| node.situation_count)
                .sum::<usize>()
                != query.all_node_situations
            || !query.mean_ms.is_finite()
            || query.mean_ms <= 0.0
            || query
                .runs_ms
                .iter()
                .any(|sample| !sample.is_finite() || *sample <= 0.0)
        {
            return Err(format!(
                "{} reference evidence is inconsistent",
                name.as_str()
            ));
        }
        let tolerance = f64::EPSILON * query.mean_ms.abs().max(1.0) * 16.0;
        if (mean(&query.runs_ms) - query.mean_ms).abs() > tolerance {
            return Err(format!("{} reference mean is inconsistent", name.as_str()));
        }
        for (index, node) in query.nodes.iter().enumerate() {
            if node.node_index != index || node.canonical_sha256.len() != 64 {
                return Err(format!(
                    "{} node {index} manifest is invalid",
                    name.as_str()
                ));
            }
        }
    }
    Ok(())
}

async fn connect(database_url: &str) -> Result<Client, String> {
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

#[derive(Debug, Serialize)]
struct MethodReport {
    warmups_per_query: usize,
    measured_runs_per_query: usize,
    timing_boundary: &'static str,
    correctness_boundary: &'static str,
    query_protocol: &'static str,
    process_scope: &'static str,
    p50_estimator: &'static str,
    p95_estimator: &'static str,
}

#[derive(Debug, Serialize)]
struct QueryReport {
    reference_ocpq_mean_ms: f64,
    candidate_mean_ms: f64,
    candidate_p50_ms: f64,
    candidate_p95_ms: f64,
    speedup_vs_reference_ocpq: f64,
    runs_ms: Vec<f64>,
    all_node_situations: usize,
    capsule_bytes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    q6_root_label: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    q6_duration_microseconds: Option<i64>,
    nodes: Vec<NodeFingerprint>,
    every_node_exact: bool,
}

#[derive(Debug, Serialize)]
struct SummaryReport {
    reference_ocpq_geometric_mean_ms: f64,
    candidate_geometric_mean_ms: f64,
    speedup_geometric_mean: f64,
    minimum_query_speedup: f64,
    every_query_and_node_exact: bool,
}

#[derive(Debug, Serialize)]
struct OutputArtifact {
    schema_version: u8,
    generated_at_unix_ms: u128,
    reference_schema_version: u8,
    reference_artifact_sha256: String,
    reference_source: ReferenceSource,
    reference_environment: ReferenceEnvironment,
    release: ReleaseReport,
    environment: EnvironmentReport,
    selected_queries: Vec<&'static str>,
    method: MethodReport,
    summary: SummaryReport,
    queries: BTreeMap<String, QueryReport>,
}

async fn run_query(
    client: &Client,
    name: QueryName,
    reference: &ReferenceQuery,
    external: &ExternalData,
) -> Result<QueryReport, String> {
    let prepared = PreparedBindingTreeQuery::prepare(client, name.sql())
        .await
        .map_err(|error| format!("{} prepare failed: {error}", name.as_str()))?;
    if prepared.node_count() != name.specs().len() {
        return Err(format!(
            "{} prepared statement has {} bytea columns, expected {}",
            name.as_str(),
            prepared.node_count(),
            name.specs().len()
        ));
    }
    let mut runs_ms = Vec::with_capacity(MEASURED_RUNS);
    let mut stable_capsule_bytes = None;
    let mut final_nodes = None;
    let mut final_q6_label = None;
    let mut final_q6_duration = None;
    for run in 0..MEASURED_RUNS {
        let started = Instant::now();
        let result = prepared
            .execute(client, &[])
            .await
            .map_err(|error| format!("{} run {} failed: {error}", name.as_str(), run + 1))?;
        let tree = materialize_tree(name, result)?;
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        let fingerprints = canonical_tree(name, &tree, external)?;
        if fingerprints != reference.nodes {
            let mismatch = fingerprints
                .iter()
                .zip(&reference.nodes)
                .position(|(actual, expected)| actual != expected)
                .unwrap_or(fingerprints.len().min(reference.nodes.len()));
            return Err(format!(
                "{} run {} differs from strict OCPQ at node {mismatch}: candidate={:?}, reference={:?}",
                name.as_str(),
                run + 1,
                fingerprints.get(mismatch),
                reference.nodes.get(mismatch)
            ));
        }
        let all_node_situations = fingerprints
            .iter()
            .map(|node| node.situation_count)
            .sum::<usize>();
        if all_node_situations != reference.all_node_situations {
            return Err(format!(
                "{} materialized {all_node_situations} all-node rows, expected {}",
                name.as_str(),
                reference.all_node_situations
            ));
        }
        let (q6_label, q6_duration) = if name == QueryName::Q6 {
            let (label, duration) = q6_evidence(&tree, external)?;
            if Some(&label) != reference.q6_root_label.as_ref()
                || Some(duration) != reference.q6_duration_microseconds
            {
                return Err(format!(
                    "Q6 typed label or independently-derived duration differs: candidate=({label:?},{duration}), reference=({:?},{:?})",
                    reference.q6_root_label, reference.q6_duration_microseconds
                ));
            }
            (Some(label), Some(duration))
        } else {
            if reference.q6_root_label.is_some() || reference.q6_duration_microseconds.is_some() {
                return Err(format!("{} has unexpected Q6 evidence", name.as_str()));
            }
            (None, None)
        };
        if stable_capsule_bytes.is_some_and(|bytes| bytes != tree.capsule_bytes) {
            return Err(format!(
                "{} capsule size changed between runs",
                name.as_str()
            ));
        }
        stable_capsule_bytes = Some(tree.capsule_bytes);
        final_nodes = Some(fingerprints);
        final_q6_label = q6_label;
        final_q6_duration = q6_duration;
        runs_ms.push(elapsed_ms);
    }
    let candidate_mean_ms = mean(&runs_ms);
    Ok(QueryReport {
        reference_ocpq_mean_ms: reference.mean_ms,
        candidate_mean_ms,
        candidate_p50_ms: median(&runs_ms),
        candidate_p95_ms: percentile(&runs_ms, 0.95),
        speedup_vs_reference_ocpq: reference.mean_ms / candidate_mean_ms,
        runs_ms,
        all_node_situations: reference.all_node_situations,
        capsule_bytes: stable_capsule_bytes.expect("ten measured runs are nonempty"),
        q6_root_label: final_q6_label,
        q6_duration_microseconds: final_q6_duration,
        nodes: final_nodes.expect("ten measured runs are nonempty"),
        every_node_exact: true,
    })
}

fn write_artifact(path: &Path, artifact: &impl Serialize) -> Result<(), String> {
    let mut bytes = serde_json::to_vec(artifact).map_err(|error| error.to_string())?;
    bytes.push(b'\n');
    if path == Path::new("-") {
        io::stdout()
            .lock()
            .write_all(&bytes)
            .map_err(|error| error.to_string())
    } else {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        fs::write(path, bytes).map_err(|error| error.to_string())
    }
}

async fn run(args: Args) -> Result<(), String> {
    let reference_bytes = fs::read(&args.reference)
        .map_err(|error| format!("could not read {}: {error}", args.reference.display()))?;
    let reference_artifact_sha256 = sha256_hex(&reference_bytes);
    let reference: ReferenceArtifact = serde_json::from_slice(&reference_bytes)
        .map_err(|error| format!("could not parse strict OCPQ reference: {error}"))?;
    validate_reference(&reference)?;
    let client = connect(&args.database_url).await?;
    let (release, environment) = release_environment(&client, &reference).await?;
    let external = load_external_data(&client).await?;
    let selected = args
        .query
        .map_or_else(|| QueryName::ALL.to_vec(), |name| vec![name]);
    let mut queries = BTreeMap::new();
    for &name in &selected {
        eprintln!(
            "running {}: 0 warmups, {} direct all-node runs",
            name.as_str(),
            MEASURED_RUNS
        );
        let report = run_query(&client, name, &reference.queries[name.as_str()], &external).await?;
        queries.insert(name.as_str().to_owned(), report);
    }
    let reference_means = selected
        .iter()
        .map(|name| reference.queries[name.as_str()].mean_ms)
        .collect::<Vec<_>>();
    let candidate_means = selected
        .iter()
        .map(|name| queries[name.as_str()].candidate_mean_ms)
        .collect::<Vec<_>>();
    let speedups = selected
        .iter()
        .map(|name| queries[name.as_str()].speedup_vs_reference_ocpq)
        .collect::<Vec<_>>();
    let artifact = OutputArtifact {
        schema_version: 2,
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
        selected_queries: selected.iter().map(|name| name.as_str()).collect(),
        method: MethodReport {
            warmups_per_query: 0,
            measured_runs_per_query: MEASURED_RUNS,
            timing_boundary: "one prepared PostgreSQL generation/fetch of all root-first bytea columns, OCPB decode, and complete owned materialization of every node including exact violations and the typed Q6 label",
            correctness_boundary: "outside the clock: external-ID conversion, duplicate-preserving canonical row sorting, compact JSON, SHA-256, every-node manifest comparison, and independent Q6 child-duration validation",
            query_protocol: "exact upstream OCPQ primary protocol: zero warmups and ten direct measured evaluations per Q1-Q7",
            process_scope: if args.query.is_some() {
                "one selected query in a fresh candidate process; publication aggregation requires one fresh container for each Q1-Q7"
            } else {
                "Q1-Q7 share one process; diagnostic only, not the publication apples-to-apples process scope"
            },
            p50_estimator: "conventional median; the middle two samples are averaged for the even ten-run sample",
            p95_estimator: "nearest-rank p95; with ten measured runs this is the maximum sample",
        },
        summary: SummaryReport {
            reference_ocpq_geometric_mean_ms: geometric_mean(&reference_means),
            candidate_geometric_mean_ms: geometric_mean(&candidate_means),
            speedup_geometric_mean: geometric_mean(&speedups),
            minimum_query_speedup: speedups.iter().copied().fold(f64::INFINITY, f64::min),
            every_query_and_node_exact: true,
        },
        queries,
    };
    write_artifact(&args.output, &artifact)
}

include!("strict_resource_support.rs");

#[tokio::main]
async fn main() {
    let result = match parse_args() {
        Ok(args) if args.memory_only_query.is_some() => run_memory_only(&args).await,
        Ok(args) if args.resource_gates => run_resource_gates(&args).await,
        Ok(args) => run(args).await,
        Err(error) => Err(error),
    };
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(2);
    }
}
