//! Deterministic model interchange derived from the peer-reviewed Alpha,
//! inductive-mining, and object-centric Petri-net formalisms cited by the
//! workspace provenance manifest. No library implementation was consulted.

use crate::{
    ModelArtifact, ObjectCentricPetriNet, OcpmError, OcpmResult, PetriNet, ProcessModel,
    ProcessTree, canonical_json,
};
use std::fmt::Write as _;

pub fn model_json(artifact: &ModelArtifact) -> OcpmResult<Vec<u8>> {
    canonical_json(artifact)
}

pub fn model_dot(artifact: &ModelArtifact) -> String {
    let mut output = String::from("digraph process_model {\n  rankdir=LR;\n");
    match &artifact.model {
        ProcessModel::Dfg(model) => {
            for activity in &model.activities {
                let id = dot_id(activity);
                let _ = writeln!(output, "  {id} [label=\"{}\"];", dot_text(activity));
            }
            for edge in &model.edges {
                let _ = writeln!(
                    output,
                    "  {} -> {} [label=\"{} / {}\"];",
                    dot_id(&edge.source),
                    dot_id(&edge.target),
                    edge.frequency,
                    dot_text(&edge.object_type)
                );
            }
        }
        ProcessModel::ProcessTree(tree) => {
            let mut next = 0_u64;
            write_tree_dot(tree, None, &mut next, &mut output);
        }
        ProcessModel::PetriNet(net) => write_petri_dot(net, &mut output),
        ProcessModel::ObjectCentricPetriNet(model) => write_petri_dot(&model.net, &mut output),
        ProcessModel::OcDeclare(constraints) => {
            for (index, constraint) in constraints.iter().enumerate() {
                let target = constraint.target.as_deref().unwrap_or("");
                let label = format!(
                    "{:?}: {} {} [{}]",
                    constraint.template,
                    constraint.activation,
                    target,
                    constraint.object_type.as_deref().unwrap_or("*")
                );
                let _ = writeln!(
                    output,
                    "  c{index} [shape=box,label=\"{}\"];",
                    dot_text(&label)
                );
            }
        }
    }
    output.push_str("}\n");
    output
}

pub fn model_pnml(artifact: &ModelArtifact) -> OcpmResult<String> {
    let (net, object_types): (&PetriNet, &[String]) = match &artifact.model {
        ProcessModel::PetriNet(net) => (net, &[]),
        ProcessModel::ObjectCentricPetriNet(ObjectCentricPetriNet { object_types, net }) => {
            (net, object_types)
        }
        _ => {
            return Err(OcpmError::invalid_request(
                "PNML export requires a Petri net or object-centric Petri net",
            ));
        }
    };
    let mut output = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<pnml>\n  <net id=\"ocpm\" type=\"http://www.pnml.org/version-2009/grammar/ptnet\">\n",
    );
    if !object_types.is_empty() {
        let _ = writeln!(
            output,
            "    <toolspecific tool=\"ocpm-engine\" version=\"1.0\"><object-types>{}</object-types></toolspecific>",
            xml_text(&object_types.join(","))
        );
    }
    for place in &net.places {
        let _ = writeln!(
            output,
            "    <place id=\"{}\"><initialMarking><text>{}</text></initialMarking><toolspecific tool=\"ocpm-engine\" version=\"1.0\"><finalMarking>{}</finalMarking></toolspecific></place>",
            xml_text(&place.id),
            place.initial_tokens,
            place.final_tokens
        );
    }
    for transition in &net.transitions {
        let label = transition.label.as_deref().unwrap_or("");
        let _ = writeln!(
            output,
            "    <transition id=\"{}\"><name><text>{}</text></name></transition>",
            xml_text(&transition.id),
            xml_text(label)
        );
    }
    for (index, arc) in net.arcs.iter().enumerate() {
        let _ = writeln!(
            output,
            "    <arc id=\"a{index}\" source=\"{}\" target=\"{}\"><inscription><text>{}</text></inscription><toolspecific tool=\"ocpm-engine\" version=\"1.0\"><object-type>{}</object-type><variable>{}</variable></toolspecific></arc>",
            xml_text(&arc.source),
            xml_text(&arc.target),
            arc.weight,
            xml_text(arc.object_type.as_deref().unwrap_or("")),
            arc.variable
        );
    }
    output.push_str("  </net>\n</pnml>\n");
    Ok(output)
}

/// Dependency-free SVG summary suitable for notebooks and API responses.
/// Layout is deliberately simple and deterministic; it is not a graph-layout
/// optimizer and therefore has predictable CPU and memory use.
pub fn model_svg(artifact: &ModelArtifact) -> String {
    let lines = model_lines(&artifact.model);
    let height = 40_u64.saturating_add(lines.len() as u64 * 24);
    let mut output = format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"960\" height=\"{height}\" viewBox=\"0 0 960 {height}\">\n<rect width=\"100%\" height=\"100%\" fill=\"white\"/>\n"
    );
    output.push_str("<text x=\"20\" y=\"24\" font-family=\"monospace\" font-size=\"16\" font-weight=\"bold\">ocpm-engine process model</text>\n");
    for (index, line) in lines.iter().enumerate() {
        let y = 52 + index * 24;
        let _ = writeln!(
            output,
            "<text x=\"20\" y=\"{y}\" font-family=\"monospace\" font-size=\"14\">{}</text>",
            xml_text(line)
        );
    }
    output.push_str("</svg>\n");
    output
}

fn write_petri_dot(net: &PetriNet, output: &mut String) {
    for place in &net.places {
        let _ = writeln!(
            output,
            "  {} [shape=circle,label=\"{} ({}/{})\"];",
            dot_id(&place.id),
            dot_text(&place.id),
            place.initial_tokens,
            place.final_tokens
        );
    }
    for transition in &net.transitions {
        let _ = writeln!(
            output,
            "  {} [shape=box,label=\"{}\"];",
            dot_id(&transition.id),
            dot_text(transition.label.as_deref().unwrap_or("tau"))
        );
    }
    for arc in &net.arcs {
        let _ = writeln!(
            output,
            "  {} -> {} [label=\"{}{}\"];",
            dot_id(&arc.source),
            dot_id(&arc.target),
            arc.weight,
            arc.object_type
                .as_ref()
                .map(|value| format!(" / {}", dot_text(value)))
                .unwrap_or_default()
        );
    }
}

fn write_tree_dot(tree: &ProcessTree, parent: Option<u64>, next: &mut u64, output: &mut String) {
    let id = *next;
    *next += 1;
    let (label, children): (&str, Vec<&ProcessTree>) = match tree {
        ProcessTree::Activity(activity) => (activity, Vec::new()),
        ProcessTree::Sequence(children) => ("sequence", children.iter().collect()),
        ProcessTree::Exclusive(children) => ("exclusive", children.iter().collect()),
        ProcessTree::Parallel(children) => ("parallel", children.iter().collect()),
        ProcessTree::Loop { body, redo } => ("loop", vec![body.as_ref(), redo.as_ref()]),
        ProcessTree::Tau => ("tau", Vec::new()),
    };
    let _ = writeln!(output, "  n{id} [label=\"{}\"];", dot_text(label));
    if let Some(parent) = parent {
        let _ = writeln!(output, "  n{parent} -> n{id};");
    }
    for child in children {
        write_tree_dot(child, Some(id), next, output);
    }
}

fn model_lines(model: &ProcessModel) -> Vec<String> {
    match model {
        ProcessModel::Dfg(model) => model
            .edges
            .iter()
            .map(|edge| {
                format!(
                    "{} -> {} | type={} frequency={}",
                    edge.source, edge.target, edge.object_type, edge.frequency
                )
            })
            .collect(),
        ProcessModel::ProcessTree(tree) => vec![format!("{tree:?}")],
        ProcessModel::PetriNet(net) => net
            .arcs
            .iter()
            .map(|arc| format!("{} -> {} | weight={}", arc.source, arc.target, arc.weight))
            .collect(),
        ProcessModel::ObjectCentricPetriNet(model) => model
            .net
            .arcs
            .iter()
            .map(|arc| {
                format!(
                    "{} -> {} | type={} weight={} variable={}",
                    arc.source,
                    arc.target,
                    arc.object_type.as_deref().unwrap_or(""),
                    arc.weight,
                    arc.variable
                )
            })
            .collect(),
        ProcessModel::OcDeclare(constraints) => constraints
            .iter()
            .map(|constraint| {
                format!(
                    "{:?}({}, {}) | type={} support={:.6} confidence={:.6}",
                    constraint.template,
                    constraint.activation,
                    constraint.target.as_deref().unwrap_or(""),
                    constraint.object_type.as_deref().unwrap_or("*"),
                    constraint.support,
                    constraint.confidence
                )
            })
            .collect(),
    }
}

fn dot_id(value: &str) -> String {
    format!("n_{}", hex(value.as_bytes()))
}

fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn dot_text(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn xml_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DfgEdge, DfgModel};
    use std::collections::BTreeMap;

    #[test]
    fn dot_and_svg_escape_labels() {
        let artifact = ModelArtifact {
            schema_version: "1.0".to_owned(),
            algorithm: "dfg".to_owned(),
            algorithm_version: "1".to_owned(),
            parameters: BTreeMap::new(),
            dataset_id: "fixture".to_owned(),
            source_watermark: None,
            model: ProcessModel::Dfg(DfgModel {
                activities: vec!["a<&\"".to_owned(), "b".to_owned()],
                start_activities: BTreeMap::new(),
                end_activities: BTreeMap::new(),
                edges: vec![DfgEdge {
                    source: "a<&\"".to_owned(),
                    target: "b".to_owned(),
                    object_type: "order".to_owned(),
                    frequency: 1,
                }],
            }),
            content_hash: String::new(),
        };
        assert!(model_dot(&artifact).contains("a<&\\\""));
        assert!(model_svg(&artifact).contains("a&lt;&amp;&quot;"));
    }
}
