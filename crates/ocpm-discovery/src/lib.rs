//! Deterministic process and object-centric model discovery.
//!
//! PROVENANCE: DFG/OC-DFG semantics follow doi:10.1007/s10009-022-00668-w;
//! Alpha follows doi:10.1109/TKDE.2004.47; process-tree cut discovery follows
//! doi:10.1007/978-3-642-38697-8_17; OCPN construction follows
//! doi:10.3233/FI-2020-1946; declarative constraints follow
//! doi:10.1109/ICPM63005.2024.10680680. This is an independent
//! implementation and uses no process-mining library source code.

use ocpm_core::{
    DeclareConstraint, DeclareTemplate, DfgEdge, DfgModel, DiscoveryAlgorithm,
    DiscoveryRequest, ModelArtifact, ObjectCentricPetriNet, OcpmError, OcpmResult, PetriArc,
    PetriNet, Place, ProcessModel, ProcessTree, Transition,
};
use ocpm_provider::{ExecutionMode, OcpmProvider, ProcessExecution};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub const PROVENANCE: &[&str] = &[
    "doi:10.1007/s10009-022-00668-w",
    "doi:10.1109/TKDE.2004.47",
    "doi:10.1007/978-3-642-38697-8_17",
    "doi:10.3233/FI-2020-1946",
    "doi:10.1109/ICPM63005.2024.10680680",
];

const MAX_ALPHA_CANDIDATES: usize = 65_536;

pub fn discover(
    provider: &dyn OcpmProvider,
    request: &DiscoveryRequest,
) -> OcpmResult<ModelArtifact> {
    if request.semantic_version != "1.0" {
        return Err(OcpmError::invalid_request("semantic_version must be 1.0"));
    }
    let mode = match request.parameters.get("execution_mode").and_then(|v| v.as_str()) {
        Some("connected_component") => ExecutionMode::ConnectedComponent,
        Some("leading_object") | None => ExecutionMode::LeadingObject,
        Some(value) => {
            return Err(OcpmError::invalid_request(format!(
                "unsupported execution_mode {value}"
            )))
        }
    };
    let leading = request
        .parameters
        .get("leading_object_type")
        .and_then(|value| value.as_str());
    let executions = provider.process_executions(&request.view, mode, leading)?;
    if executions.is_empty() {
        return Err(OcpmError::new(
            ocpm_core::OcpmErrorCode::InsufficientData,
            "discovery requires at least one nonempty process execution",
        ));
    }
    let profile = provider.profile(&request.view)?;
    let model = match request.algorithm {
        DiscoveryAlgorithm::Dfg => ProcessModel::Dfg(discover_dfg(&executions, false)),
        DiscoveryAlgorithm::OcDfg => ProcessModel::Dfg(discover_dfg(&executions, true)),
        DiscoveryAlgorithm::Alpha | DiscoveryAlgorithm::PetriNet => {
            ProcessModel::PetriNet(discover_alpha(&executions)?)
        }
        DiscoveryAlgorithm::InductiveProcessTree => {
            ProcessModel::ProcessTree(discover_process_tree(&executions))
        }
        DiscoveryAlgorithm::ObjectCentricPetriNet => ProcessModel::ObjectCentricPetriNet(
            discover_object_centric_petri_net(&executions)?,
        ),
        DiscoveryAlgorithm::OcDeclare => {
            let minimum_support = request
                .parameters
                .get("minimum_support")
                .and_then(|value| value.as_f64())
                .unwrap_or(0.8);
            if !(0.0..=1.0).contains(&minimum_support) {
                return Err(OcpmError::invalid_request(
                    "minimum_support must be between 0 and 1",
                ));
            }
            ProcessModel::OcDeclare(discover_declare(&executions, minimum_support))
        }
    };
    let mut artifact = ModelArtifact {
        schema_version: "1.0".to_owned(),
        algorithm: format!("{:?}", request.algorithm).to_lowercase(),
        algorithm_version: request.algorithm_version.clone(),
        parameters: request.parameters.clone(),
        dataset_id: profile.dataset_id,
        source_watermark: profile.source_watermark,
        model,
        content_hash: String::new(),
    };
    artifact.finalize_hash()?;
    Ok(artifact)
}

pub fn discover_dfg(executions: &[ProcessExecution], object_centric: bool) -> DfgModel {
    let mut activities = BTreeSet::new();
    let mut starts = BTreeMap::new();
    let mut ends = BTreeMap::new();
    let mut edges = BTreeMap::<(String, String, String), u64>::new();
    for execution in executions {
        let Some(first) = execution.events.first() else {
            continue;
        };
        let object_type = if object_centric {
            execution.object_type.clone()
        } else {
            String::new()
        };
        *starts
            .entry(format!("{object_type}\u{1f}{}", first.activity))
            .or_default() += 1;
        if let Some(last) = execution.events.last() {
            *ends
                .entry(format!("{object_type}\u{1f}{}", last.activity))
                .or_default() += 1;
        }
        for event in &execution.events {
            activities.insert(event.activity.clone());
        }
        for pair in execution.events.windows(2) {
            *edges
                .entry((
                    pair[0].activity.clone(),
                    pair[1].activity.clone(),
                    object_type.clone(),
                ))
                .or_default() += 1;
        }
    }
    let decode_boundary = |values: BTreeMap<String, u64>| {
        let mut output = BTreeMap::new();
        for (key, frequency) in values {
            let activity = key.split_once('\u{1f}').map_or(key.as_str(), |(_, value)| value);
            *output.entry(activity.to_owned()).or_default() += frequency;
        }
        output
    };
    DfgModel {
        activities: activities.into_iter().collect(),
        start_activities: decode_boundary(starts),
        end_activities: decode_boundary(ends),
        edges: edges
            .into_iter()
            .map(|((source, target, object_type), frequency)| DfgEdge {
                source,
                target,
                object_type,
                frequency,
            })
            .collect(),
    }
}

pub fn discover_alpha(executions: &[ProcessExecution]) -> OcpmResult<PetriNet> {
    let dfg = discover_dfg(executions, false);
    let follows = dfg
        .edges
        .iter()
        .map(|edge| (edge.source.clone(), edge.target.clone()))
        .collect::<BTreeSet<_>>();
    let causal = follows
        .iter()
        .filter(|(source, target)| !follows.contains(&(target.clone(), source.clone())))
        .cloned()
        .collect::<BTreeSet<_>>();

    let mut candidates = causal
        .iter()
        .map(|(source, target)| {
            (
                BTreeSet::from([source.clone()]),
                BTreeSet::from([target.clone()]),
            )
        })
        .collect::<BTreeSet<_>>();
    loop {
        let snapshot = candidates.iter().cloned().collect::<Vec<_>>();
        let mut additions = Vec::new();
        for left in &snapshot {
            for right in &snapshot {
                if left.0 == right.0 {
                    let targets = left.1.union(&right.1).cloned().collect::<BTreeSet<_>>();
                    if valid_alpha_pair(&left.0, &targets, &causal, &follows) {
                        additions.push((left.0.clone(), targets));
                    }
                }
                if left.1 == right.1 {
                    let sources = left.0.union(&right.0).cloned().collect::<BTreeSet<_>>();
                    if valid_alpha_pair(&sources, &left.1, &causal, &follows) {
                        additions.push((sources, left.1.clone()));
                    }
                }
            }
        }
        let before = candidates.len();
        candidates.extend(additions);
        if candidates.len() > MAX_ALPHA_CANDIDATES {
            return Err(OcpmError::resource_limit(
                "alpha candidate-place search exceeded its deterministic safety bound",
                MAX_ALPHA_CANDIDATES as u64,
                candidates.len() as u64,
            ));
        }
        if candidates.len() == before {
            break;
        }
    }
    let maximal = candidates
        .iter()
        .filter(|candidate| {
            !candidates.iter().any(|other| {
                candidate != &other
                    && candidate.0.is_subset(&other.0)
                    && candidate.1.is_subset(&other.1)
            })
        })
        .cloned()
        .collect::<Vec<_>>();

    let mut places = vec![
        Place {
            id: "source".to_owned(),
            initial_tokens: 1,
            final_tokens: 0,
        },
        Place {
            id: "sink".to_owned(),
            initial_tokens: 0,
            final_tokens: 1,
        },
    ];
    let transitions = dfg
        .activities
        .iter()
        .map(|activity| Transition {
            id: transition_id(activity),
            label: Some(activity.clone()),
        })
        .collect::<Vec<_>>();
    let mut arcs = Vec::new();
    for activity in dfg.start_activities.keys() {
        arcs.push(arc("source", &transition_id(activity)));
    }
    for activity in dfg.end_activities.keys() {
        arcs.push(arc(&transition_id(activity), "sink"));
    }
    for (index, (sources, targets)) in maximal.into_iter().enumerate() {
        let place = format!("p{index}");
        places.push(Place {
            id: place.clone(),
            initial_tokens: 0,
            final_tokens: 0,
        });
        for source in sources {
            arcs.push(arc(&transition_id(&source), &place));
        }
        for target in targets {
            arcs.push(arc(&place, &transition_id(&target)));
        }
    }
    arcs.sort_by(|left, right| {
        left.source
            .cmp(&right.source)
            .then_with(|| left.target.cmp(&right.target))
    });
    arcs.dedup_by(|left, right| left.source == right.source && left.target == right.target);
    Ok(PetriNet {
        places,
        transitions,
        arcs,
    })
}

fn valid_alpha_pair(
    sources: &BTreeSet<String>,
    targets: &BTreeSet<String>,
    causal: &BTreeSet<(String, String)>,
    follows: &BTreeSet<(String, String)>,
) -> bool {
    sources
        .iter()
        .all(|source| targets.iter().all(|target| causal.contains(&(source.clone(), target.clone()))))
        && pairwise_unrelated(sources, follows)
        && pairwise_unrelated(targets, follows)
}

fn pairwise_unrelated(values: &BTreeSet<String>, follows: &BTreeSet<(String, String)>) -> bool {
    values.iter().all(|left| {
        values.iter().all(|right| {
            left == right
                || (!follows.contains(&(left.clone(), right.clone()))
                    && !follows.contains(&(right.clone(), left.clone())))
        })
    })
}

fn transition_id(activity: &str) -> String {
    format!("t:{activity}")
}

fn arc(source: &str, target: &str) -> PetriArc {
    PetriArc {
        source: source.to_owned(),
        target: target.to_owned(),
        weight: 1,
        object_type: None,
        variable: false,
    }
}

pub fn discover_process_tree(executions: &[ProcessExecution]) -> ProcessTree {
    let traces = executions
        .iter()
        .map(ProcessExecution::activity_path)
        .filter(|trace| !trace.is_empty())
        .collect::<Vec<_>>();
    process_tree_cut(&traces, 0)
}

fn process_tree_cut(traces: &[Vec<String>], depth: usize) -> ProcessTree {
    if traces.is_empty() {
        return ProcessTree::Tau;
    }
    let activities = traces
        .iter()
        .flatten()
        .cloned()
        .collect::<BTreeSet<_>>();
    if activities.len() == 1 {
        return ProcessTree::Activity(activities.into_iter().next().unwrap());
    }
    if depth >= 128 {
        return ProcessTree::Sequence(
            most_frequent_trace(traces)
                .into_iter()
                .map(ProcessTree::Activity)
                .collect(),
        );
    }

    if let Some(first) = common_boundary(traces, true) {
        let tails = traces
            .iter()
            .map(|trace| trace.iter().skip(1).cloned().collect::<Vec<_>>())
            .collect::<Vec<_>>();
        return compact_sequence(vec![
            ProcessTree::Activity(first),
            process_tree_cut(&tails, depth + 1),
        ]);
    }
    if let Some(last) = common_boundary(traces, false) {
        let heads = traces
            .iter()
            .map(|trace| trace.iter().take(trace.len() - 1).cloned().collect::<Vec<_>>())
            .collect::<Vec<_>>();
        return compact_sequence(vec![
            process_tree_cut(&heads, depth + 1),
            ProcessTree::Activity(last),
        ]);
    }

    let mut by_first = BTreeMap::<String, Vec<Vec<String>>>::new();
    for trace in traces {
        by_first.entry(trace[0].clone()).or_default().push(trace.clone());
    }
    let activity_partitions = by_first
        .values()
        .map(|group| group.iter().flatten().cloned().collect::<BTreeSet<_>>())
        .collect::<Vec<_>>();
    let disjoint_xor = activity_partitions.iter().enumerate().all(|(index, left)| {
        activity_partitions[index + 1..]
            .iter()
            .all(|right| left.is_disjoint(right))
    });
    if by_first.len() > 1 && disjoint_xor {
        return ProcessTree::Exclusive(
            by_first
                .into_values()
                .map(|group| process_tree_cut(&group, depth + 1))
                .collect(),
        );
    }

    let follows = direct_relations(traces);
    let all_parallel = activities.iter().all(|left| {
        activities.iter().all(|right| {
            left == right
                || (follows.contains(&(left.clone(), right.clone()))
                    && follows.contains(&(right.clone(), left.clone())))
        })
    });
    if all_parallel {
        return ProcessTree::Parallel(
            activities.into_iter().map(ProcessTree::Activity).collect(),
        );
    }

    let order = topological_activity_order(&activities, &follows);
    if order.len() == activities.len() {
        ProcessTree::Sequence(order.into_iter().map(ProcessTree::Activity).collect())
    } else {
        let representative = most_frequent_trace(traces);
        let body = representative
            .first()
            .cloned()
            .map(ProcessTree::Activity)
            .unwrap_or(ProcessTree::Tau);
        let redo = compact_sequence(
            representative
                .into_iter()
                .skip(1)
                .map(ProcessTree::Activity)
                .collect(),
        );
        ProcessTree::Loop {
            body: Box::new(body),
            redo: Box::new(redo),
        }
    }
}

fn common_boundary(traces: &[Vec<String>], first: bool) -> Option<String> {
    let candidate = if first {
        traces.first()?.first()?
    } else {
        traces.first()?.last()?
    };
    traces
        .iter()
        .all(|trace| if first { trace.first() } else { trace.last() } == Some(candidate))
        .then(|| candidate.clone())
}

fn compact_sequence(children: Vec<ProcessTree>) -> ProcessTree {
    let children = children
        .into_iter()
        .filter(|child| *child != ProcessTree::Tau)
        .collect::<Vec<_>>();
    match children.len() {
        0 => ProcessTree::Tau,
        1 => children.into_iter().next().unwrap(),
        _ => ProcessTree::Sequence(children),
    }
}

fn most_frequent_trace(traces: &[Vec<String>]) -> Vec<String> {
    let mut frequencies = BTreeMap::new();
    for trace in traces {
        *frequencies.entry(trace.clone()).or_insert(0_u64) += 1;
    }
    frequencies
        .into_iter()
        .max_by(|left, right| left.1.cmp(&right.1).then_with(|| right.0.cmp(&left.0)))
        .map(|(trace, _)| trace)
        .unwrap_or_default()
}

fn direct_relations(traces: &[Vec<String>]) -> BTreeSet<(String, String)> {
    traces
        .iter()
        .flat_map(|trace| {
            trace
                .windows(2)
                .map(|pair| (pair[0].clone(), pair[1].clone()))
        })
        .collect()
}

fn topological_activity_order(
    activities: &BTreeSet<String>,
    follows: &BTreeSet<(String, String)>,
) -> Vec<String> {
    let mut indegree = activities
        .iter()
        .map(|activity| (activity.clone(), 0_usize))
        .collect::<BTreeMap<_, _>>();
    let directed = follows
        .iter()
        .filter(|(left, right)| !follows.contains(&(right.clone(), left.clone())))
        .cloned()
        .collect::<BTreeSet<_>>();
    for (_, target) in &directed {
        *indegree.entry(target.clone()).or_default() += 1;
    }
    let mut ready = indegree
        .iter()
        .filter(|(_, degree)| **degree == 0)
        .map(|(activity, _)| activity.clone())
        .collect::<VecDeque<_>>();
    let mut output = Vec::new();
    while let Some(activity) = ready.pop_front() {
        output.push(activity.clone());
        for (_, target) in directed.iter().filter(|(source, _)| *source == activity) {
            let degree = indegree.get_mut(target).unwrap();
            *degree -= 1;
            if *degree == 0 {
                let position = ready
                    .iter()
                    .position(|item| item > target)
                    .unwrap_or(ready.len());
                ready.insert(position, target.clone());
            }
        }
    }
    output
}

pub fn discover_object_centric_petri_net(
    executions: &[ProcessExecution],
) -> OcpmResult<ObjectCentricPetriNet> {
    let object_types = executions
        .iter()
        .map(|execution| execution.object_type.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut net = discover_alpha(executions)?;
    let activity_types = executions.iter().fold(
        BTreeMap::<String, BTreeSet<String>>::new(),
        |mut values, execution| {
            for event in &execution.events {
                values
                    .entry(event.activity.clone())
                    .or_default()
                    .insert(execution.object_type.clone());
            }
            values
        },
    );
    for arc in &mut net.arcs {
        let transition = arc
            .source
            .strip_prefix("t:")
            .or_else(|| arc.target.strip_prefix("t:"));
        if let Some(types) = transition.and_then(|activity| activity_types.get(activity)) {
            if types.len() == 1 {
                arc.object_type = types.first().cloned();
            }
            arc.variable = types.len() > 1;
        }
    }
    Ok(ObjectCentricPetriNet { object_types, net })
}

pub fn discover_declare(
    executions: &[ProcessExecution],
    minimum_support: f64,
) -> Vec<DeclareConstraint> {
    let activities = executions
        .iter()
        .flat_map(|execution| execution.events.iter().map(|event| event.activity.clone()))
        .collect::<BTreeSet<_>>();
    let total = executions.len() as f64;
    let mut constraints = Vec::new();
    for activity in &activities {
        let containing = executions
            .iter()
            .filter(|execution| execution.events.iter().any(|event| event.activity == *activity))
            .count() as f64;
        let support = containing / total;
        if support >= minimum_support {
            constraints.push(DeclareConstraint {
                template: DeclareTemplate::Existence,
                activation: activity.clone(),
                target: None,
                object_type: common_object_type(executions),
                support,
                confidence: support,
            });
        }
    }
    for activation in &activities {
        for target in &activities {
            if activation == target {
                continue;
            }
            let activated = executions
                .iter()
                .filter(|execution| execution.events.iter().any(|event| event.activity == *activation))
                .count();
            if activated == 0 {
                continue;
            }
            let response = executions
                .iter()
                .filter(|execution| response_holds(execution, activation, target))
                .count();
            let support = response as f64 / executions.len() as f64;
            let confidence = response as f64 / activated as f64;
            if support >= minimum_support {
                constraints.push(DeclareConstraint {
                    template: DeclareTemplate::Response,
                    activation: activation.clone(),
                    target: Some(target.clone()),
                    object_type: common_object_type(executions),
                    support,
                    confidence,
                });
            }
        }
    }
    constraints.sort_by(|left, right| {
        format!("{:?}", left.template)
            .cmp(&format!("{:?}", right.template))
            .then_with(|| left.activation.cmp(&right.activation))
            .then_with(|| left.target.cmp(&right.target))
    });
    constraints
}

fn response_holds(execution: &ProcessExecution, activation: &str, target: &str) -> bool {
    execution.events.iter().enumerate().all(|(index, event)| {
        event.activity != activation
            || execution.events[index + 1..]
                .iter()
                .any(|later| later.activity == target)
    })
}

fn common_object_type(executions: &[ProcessExecution]) -> Option<String> {
    let values = executions
        .iter()
        .map(|execution| execution.object_type.clone())
        .collect::<BTreeSet<_>>();
    (values.len() == 1).then(|| values.into_iter().next().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ocpm_core::{Event, Timestamp};

    fn execution(id: &str, activities: &[&str]) -> ProcessExecution {
        ProcessExecution {
            id: id.to_owned(),
            object_type: "order".to_owned(),
            object_ids: vec![],
            events: activities
                .iter()
                .enumerate()
                .map(|(index, activity)| Event {
                    id: index as u64,
                    external_id: format!("{id}-{index}"),
                    activity: (*activity).to_owned(),
                    timestamp: Timestamp::from_epoch_nanos(index as i128),
                    sequence: 0,
                    lifecycle: None,
                    attributes: BTreeMap::new(),
                })
                .collect(),
            event_object_ids: BTreeMap::new(),
        }
    }

    #[test]
    fn dfg_counts_paths_deterministically() {
        let model = discover_dfg(
            &[execution("1", &["a", "b"]), execution("2", &["a", "b"])],
            false,
        );
        assert_eq!(model.edges[0].frequency, 2);
    }

    #[test]
    fn alpha_creates_source_and_sink() {
        let net = discover_alpha(&[execution("1", &["a", "b", "c"])]).unwrap();
        assert!(net.places.iter().any(|place| place.id == "source"));
        assert!(net.arcs.iter().any(|arc| arc.source == "source"));
    }

    #[test]
    fn response_requires_later_target() {
        let constraints = discover_declare(&[execution("1", &["a", "b"])], 1.0);
        assert!(constraints.iter().any(|constraint| {
            constraint.template == DeclareTemplate::Response
                && constraint.activation == "a"
                && constraint.target.as_deref() == Some("b")
        }));
    }
}
