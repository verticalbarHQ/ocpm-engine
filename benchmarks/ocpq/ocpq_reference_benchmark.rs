use std::{
    collections::BTreeMap,
    fs::File,
    io::BufWriter,
    path::PathBuf,
    time::Instant,
};

use clap::Parser;
use ocedeclare_shared::{
    binding_box::{
        structs::{EventVariable, ObjectVariable},
        Binding, BindingBoxTree, EvaluationResultWithCount,
    },
    preprocessing::linked_ocel::IndexLinkedOCEL,
};
use process_mining::{
    import_ocel_json_from_path, import_ocel_sqlite_from_path, import_ocel_xml_file,
};
use serde::Serialize;
use serde_json::{Value, json};

const OCPQ_COMMIT: &str = "80457e561edd7bb9e142d959dd7e0f96e6b03f2f";

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    ocel: PathBuf,
    #[arg(long)]
    bbox_tree: PathBuf,
    #[arg(long)]
    query: String,
    #[arg(long, default_value_t = 0)]
    warmups: usize,
    #[arg(long, default_value_t = 10)]
    runs: usize,
    #[arg(long)]
    output: PathBuf,
}

#[derive(Serialize)]
struct ReferenceOutput {
    schema_version: u8,
    ocpq_commit: &'static str,
    query: String,
    tree_parse_ms: f64,
    import_ms: f64,
    link_ms: f64,
    warmups: usize,
    measured_runs: usize,
    runs_ms: Vec<f64>,
    root_node: usize,
    all_node_situations: usize,
    q6_root_label: Option<Value>,
    q6_duration_microseconds: Option<i64>,
    nodes: Vec<CanonicalNode>,
}

#[derive(Serialize)]
struct CanonicalNode {
    node_index: usize,
    object_variables: Vec<usize>,
    event_variables: Vec<usize>,
    label_names: Vec<String>,
    situation_count: usize,
    situation_violated_count: usize,
    violation_reason_counts: BTreeMap<String, usize>,
    canonical_rows: Vec<Value>,
    canonical_json_bytes: usize,
}

struct Evaluation {
    nodes: Vec<CanonicalNode>,
    canonical_json: Vec<u8>,
    all_node_situations: usize,
    q6_root_label: Option<Value>,
    q6_duration_microseconds: Option<i64>,
}

fn object_id(binding: &Binding, variable: usize, ocel: &IndexLinkedOCEL) -> String {
    binding
        .get_ob(&ObjectVariable(variable), ocel)
        .unwrap_or_else(|| panic!("missing object variable o{variable}"))
        .id
        .clone()
}

fn event_id(binding: &Binding, variable: usize, ocel: &IndexLinkedOCEL) -> String {
    binding
        .get_ev(&EventVariable(variable), ocel)
        .unwrap_or_else(|| panic!("missing event variable e{variable}"))
        .id
        .clone()
}

fn canonical_node(
    node_index: usize,
    result: &EvaluationResultWithCount,
    ocel: &IndexLinkedOCEL,
) -> CanonicalNode {
    assert_eq!(
        result.situation_count,
        result.situations.len(),
        "node {node_index} situation count differs from its materialized rows"
    );
    let mut expected_object_variables: Option<Vec<usize>> = None;
    let mut expected_event_variables: Option<Vec<usize>> = None;
    let mut expected_label_names: Option<Vec<String>> = None;
    let mut violation_reason_counts = BTreeMap::new();
    let mut observed_violations = 0_usize;
    let mut rows = result
        .situations
        .iter()
        .map(|(binding, violation)| {
            let object_variables = binding
                .object_map
                .keys()
                .map(|variable| variable.0)
                .collect::<Vec<_>>();
            let event_variables = binding
                .event_map
                .keys()
                .map(|variable| variable.0)
                .collect::<Vec<_>>();
            let label_names = binding.label_map.keys().cloned().collect::<Vec<_>>();
            match &expected_object_variables {
                Some(expected) => assert_eq!(
                    expected, &object_variables,
                    "node {node_index} object-variable shape changed between rows"
                ),
                None => expected_object_variables = Some(object_variables.clone()),
            }
            match &expected_event_variables {
                Some(expected) => assert_eq!(
                    expected, &event_variables,
                    "node {node_index} event-variable shape changed between rows"
                ),
                None => expected_event_variables = Some(event_variables.clone()),
            }
            match &expected_label_names {
                Some(expected) => assert_eq!(
                    expected, &label_names,
                    "node {node_index} label shape changed between rows"
                ),
                None => expected_label_names = Some(label_names.clone()),
            }

            let objects = binding
                .object_map
                .keys()
                .map(|variable| json!([variable.0, object_id(binding, variable.0, ocel)]))
                .collect::<Vec<_>>();
            let events = binding
                .event_map
                .keys()
                .map(|variable| json!([variable.0, event_id(binding, variable.0, ocel)]))
                .collect::<Vec<_>>();
            let labels = binding
                .label_map
                .iter()
                .map(|(name, value)| {
                    json!([
                        name,
                        serde_json::to_value(value)
                            .expect("OCPQ label value must be serializable")
                    ])
                })
                .collect::<Vec<_>>();
            let violation = violation.as_ref().map(|reason| {
                observed_violations += 1;
                let value = serde_json::to_value(reason)
                    .expect("OCPQ violation reason must be serializable");
                let key = serde_json::to_string(&value)
                    .expect("OCPQ violation reason JSON must be serializable");
                *violation_reason_counts.entry(key).or_insert(0) += 1;
                value
            });
            json!([objects, events, labels, violation])
        })
        .collect::<Vec<_>>();
    assert_eq!(
        observed_violations, result.situation_violated_count,
        "node {node_index} violated count differs from its materialized rows"
    );
    rows.sort_by_cached_key(|row| {
        serde_json::to_string(row).expect("canonical node row must be serializable")
    });
    let canonical_json_bytes = serde_json::to_vec(&rows)
        .expect("canonical node rows must be serializable")
        .len();
    CanonicalNode {
        node_index,
        object_variables: expected_object_variables.unwrap_or_default(),
        event_variables: expected_event_variables.unwrap_or_default(),
        label_names: expected_label_names.unwrap_or_default(),
        situation_count: result.situation_count,
        situation_violated_count: result.situation_violated_count,
        violation_reason_counts,
        canonical_rows: rows,
        canonical_json_bytes,
    }
}

fn evaluate_results(
    tree: &BindingBoxTree,
    ocel: &IndexLinkedOCEL,
) -> Vec<EvaluationResultWithCount> {
    let flat = tree.evaluate(ocel);
    let mut results = tree
        .nodes
        .iter()
        .map(|_| EvaluationResultWithCount {
            situations: Vec::new(),
            situation_count: 0,
            situation_violated_count: 0,
        })
        .collect::<Vec<_>>();
    for (node, binding, violation) in flat {
        let result = results
            .get_mut(node)
            .unwrap_or_else(|| panic!("invalid OCPQ result node {node}"));
        result.situation_count += 1;
        if violation.is_some() {
            result.situation_violated_count += 1;
        }
        result.situations.push((binding, violation));
    }

    results
}

fn canonicalize_results(
    query: &str,
    results: &[EvaluationResultWithCount],
    ocel: &IndexLinkedOCEL,
) -> Evaluation {
    let all_node_situations = results.iter().map(|item| item.situation_count).sum();
    let nodes = results
        .iter()
        .enumerate()
        .map(|(node_index, result)| canonical_node(node_index, result, ocel))
        .collect::<Vec<_>>();
    let canonical_json = serde_json::to_vec(&nodes).unwrap();
    let (q6_root_label, q6_duration_microseconds) = if query == "Q6" {
        let root = results.first().expect("Q6 must contain a root node");
        assert_eq!(root.situations.len(), 1, "Q6 root must contain one row");
        let label = root.situations[0]
            .0
            .label_map
            .get("max_dur")
            .expect("Q6 root must contain max_dur")
            .to_owned();
        let children = results.get(1).expect("Q6 must contain its child node");
        let maximum = children
            .situations
            .iter()
            .map(|(binding, _)| {
                let created = binding
                    .get_ev(&EventVariable(0), ocel)
                    .expect("Q6 child is missing O_Created");
                let accepted = binding
                    .get_ev(&EventVariable(1), ocel)
                    .expect("Q6 child is missing O_Accepted");
                (accepted.time - created.time)
                    .num_microseconds()
                    .expect("Q6 duration exceeds microsecond range")
            })
            .max()
            .expect("Q6 requires at least one qualifying child binding");
        (
            Some(
                serde_json::to_value(label)
                    .expect("Q6 root label must be serializable"),
            ),
            Some(maximum),
        )
    } else {
        (None, None)
    };
    Evaluation {
        nodes,
        canonical_json,
        all_node_situations,
        q6_root_label,
        q6_duration_microseconds,
    }
}

fn main() {
    let args = Args::parse();
    assert_eq!(args.warmups, 0, "the upstream OCPQ protocol has no warmups");
    assert_eq!(args.runs, 10, "the upstream OCPQ protocol measures ten runs");
    assert!(matches!(args.query.as_str(), "Q1" | "Q2" | "Q3" | "Q4" | "Q5" | "Q6" | "Q7"));

    let tree_started = Instant::now();
    let tree_reader = File::open(&args.bbox_tree).expect("could not open OCPQ tree");
    let tree: BindingBoxTree =
        serde_json::from_reader(tree_reader).expect("could not parse OCPQ tree");
    let tree_parse_ms = tree_started.elapsed().as_secs_f64() * 1000.0;

    let import_started = Instant::now();
    let ocel = match args.ocel.extension().and_then(|value| value.to_str()) {
        Some("json") => import_ocel_json_from_path(&args.ocel)
            .expect("could not import JSON OCEL 2.0"),
        Some("sqlite") => import_ocel_sqlite_from_path(&args.ocel)
            .expect("could not import SQLite OCEL 2.0"),
        Some("xml") => import_ocel_xml_file(&args.ocel),
        other => panic!("unsupported OCEL extension: {other:?}"),
    };
    let import_ms = import_started.elapsed().as_secs_f64() * 1000.0;

    let link_started = Instant::now();
    let linked = IndexLinkedOCEL::new(ocel);
    let link_ms = link_started.elapsed().as_secs_f64() * 1000.0;

    let mut reference: Option<Vec<u8>> = None;
    let mut runs_ms = Vec::with_capacity(args.runs);
    let mut final_evaluation = None;
    for _ in 0..args.runs {
        let started = Instant::now();
        let results = evaluate_results(&tree, &linked);
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        let evaluation = canonicalize_results(&args.query, &results, &linked);
        if let Some(expected) = &reference {
            assert_eq!(expected, &evaluation.canonical_json, "measured result changed");
        } else {
            reference = Some(evaluation.canonical_json.clone());
        }
        runs_ms.push(elapsed_ms);
        final_evaluation = Some(evaluation);
    }
    let evaluation = final_evaluation.expect("at least one measured run is required");
    let output = ReferenceOutput {
        schema_version: 2,
        ocpq_commit: OCPQ_COMMIT,
        query: args.query,
        tree_parse_ms,
        import_ms,
        link_ms,
        warmups: args.warmups,
        measured_runs: args.runs,
        runs_ms,
        root_node: 0,
        all_node_situations: evaluation.all_node_situations,
        q6_root_label: evaluation.q6_root_label,
        q6_duration_microseconds: evaluation.q6_duration_microseconds,
        nodes: evaluation.nodes,
    };
    if let Some(parent) = args.output.parent() {
        std::fs::create_dir_all(parent).expect("could not create output directory");
    }
    let output_file = File::create(&args.output).expect("could not create output file");
    serde_json::to_writer(BufWriter::new(output_file), &output)
        .expect("could not serialize reference output");
}
