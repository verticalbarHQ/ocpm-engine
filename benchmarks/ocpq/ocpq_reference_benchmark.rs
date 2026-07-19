use std::{
    fs::File,
    hint::black_box,
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
    #[arg(long, default_value_t = 10)]
    warmups: usize,
    #[arg(long, default_value_t = 30)]
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
    root_rows: usize,
    root_violations: Option<usize>,
    all_node_situations: usize,
    duration_microseconds: Option<i64>,
    canonical_rows: Vec<Vec<Value>>,
    canonical_json_bytes: usize,
}

struct Evaluation {
    canonical_rows: Vec<Vec<Value>>,
    canonical_json: Vec<u8>,
    root_violations: Option<usize>,
    all_node_situations: usize,
    duration_microseconds: Option<i64>,
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

fn canonical_root_rows(
    query: &str,
    results: &[EvaluationResultWithCount],
    ocel: &IndexLinkedOCEL,
) -> (Vec<Vec<Value>>, Option<usize>, Option<i64>) {
    let root = results.first().expect("OCPQ tree must contain a root node");
    let with_violations = matches!(query, "Q1" | "Q2" | "Q3" | "Q4" | "Q5");
    let (mut rows, duration_microseconds) = if query == "Q6" {
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
        (vec![vec![json!(maximum)]], Some(maximum))
    } else {
        let rows = root
            .situations
            .iter()
            .map(|(binding, violation)| match query {
                "Q1" => vec![
                    json!(object_id(binding, 0, ocel)),
                    json!(violation.is_some()),
                ],
                "Q2" => vec![
                    json!(object_id(binding, 0, ocel)),
                    json!(event_id(binding, 0, ocel)),
                    json!(violation.is_some()),
                ],
                "Q3" => vec![
                    json!(event_id(binding, 0, ocel)),
                    json!(violation.is_some()),
                ],
                "Q4" => vec![
                    json!(object_id(binding, 0, ocel)),
                    json!(event_id(binding, 0, ocel)),
                    json!(violation.is_some()),
                ],
                "Q5" => vec![
                    json!(object_id(binding, 0, ocel)),
                    json!(object_id(binding, 1, ocel)),
                    json!(event_id(binding, 0, ocel)),
                    json!(violation.is_some()),
                ],
                "Q7" => vec![
                    json!(object_id(binding, 0, ocel)),
                    json!(object_id(binding, 1, ocel)),
                    json!(object_id(binding, 2, ocel)),
                    json!(event_id(binding, 1, ocel)),
                    json!(event_id(binding, 2, ocel)),
                ],
                _ => panic!("unsupported OCPQ evaluation query: {query}"),
            })
            .collect::<Vec<_>>();
        (rows, None)
    };
    rows.sort_by_cached_key(|row| serde_json::to_string(row).unwrap());
    let violations = with_violations.then_some(root.situation_violated_count);
    (rows, violations, duration_microseconds)
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

    // Make every fully collected node result observable before the measured
    // region ends. Correctness canonicalization intentionally happens later.
    for result in &results {
        black_box(&result.situations);
        black_box(result.situation_count);
        black_box(result.situation_violated_count);
    }
    results
}

fn canonicalize_results(
    query: &str,
    results: &[EvaluationResultWithCount],
    ocel: &IndexLinkedOCEL,
) -> Evaluation {
    let all_node_situations = results.iter().map(|item| item.situation_count).sum();
    let (canonical_rows, root_violations, duration_microseconds) =
        canonical_root_rows(query, results, ocel);
    let canonical_json = serde_json::to_vec(&canonical_rows).unwrap();
    Evaluation {
        canonical_rows,
        canonical_json,
        root_violations,
        all_node_situations,
        duration_microseconds,
    }
}

fn main() {
    let args = Args::parse();
    assert!(args.runs > 0, "measured runs must be positive");
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
    for _ in 0..args.warmups {
        let results = evaluate_results(&tree, &linked);
        let evaluation = canonicalize_results(&args.query, &results, &linked);
        if let Some(expected) = &reference {
            assert_eq!(expected, &evaluation.canonical_json, "warmup result changed");
        } else {
            reference = Some(evaluation.canonical_json);
        }
    }

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
        schema_version: 1,
        ocpq_commit: OCPQ_COMMIT,
        query: args.query,
        tree_parse_ms,
        import_ms,
        link_ms,
        warmups: args.warmups,
        measured_runs: args.runs,
        runs_ms,
        root_rows: evaluation.canonical_rows.len(),
        root_violations: evaluation.root_violations,
        all_node_situations: evaluation.all_node_situations,
        duration_microseconds: evaluation.duration_microseconds,
        canonical_json_bytes: evaluation.canonical_json.len(),
        canonical_rows: evaluation.canonical_rows,
    };
    if let Some(parent) = args.output.parent() {
        std::fs::create_dir_all(parent).expect("could not create output directory");
    }
    let output_file = File::create(&args.output).expect("could not create output file");
    serde_json::to_writer(BufWriter::new(output_file), &output)
        .expect("could not serialize reference output");
}
