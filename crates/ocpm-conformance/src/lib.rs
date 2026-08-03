//! Deterministic conformance checking over provider process executions.
//!
//! PROVENANCE: alignment search follows doi:10.1109/EDOC.2011.12 and
//! object-centric recomposition follows doi:10.1016/j.ins.2018.07.026;
//! object-centric DFG quality follows doi:10.1007/978-3-031-70418-5_11;
//! declarative checking follows doi:10.1109/ICPM63005.2024.10680680.
//! No implementation source from another process-mining library was used.

use ocpm_core::{
    ConformanceMethod, ConformanceRequest, ConformanceResultV1, DeclareConstraint, DeclareTemplate,
    DfgModel, ModelArtifact, OcpmError, OcpmErrorCode, OcpmResult, PetriNet, ProcessModel,
    QueryBinding,
};
use ocpm_provider::{ExecutionMode, OcpmProvider, ProcessExecution};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

pub const PROVENANCE: &[&str] = &[
    "doi:10.1109/EDOC.2011.12",
    "doi:10.1016/j.ins.2018.07.026",
    "doi:10.1007/978-3-031-70418-5_11",
    "doi:10.1109/ICPM63005.2024.10680680",
];

pub fn check(
    provider: &dyn OcpmProvider,
    request: &ConformanceRequest,
) -> OcpmResult<ConformanceResultV1> {
    if request.semantic_version != "1.0" {
        return Err(OcpmError::invalid_request("semantic_version must be 1.0"));
    }
    validate_artifact(&request.model)?;
    let mode = match request
        .parameters
        .get("execution_mode")
        .and_then(|v| v.as_str())
    {
        Some("connected_component") => ExecutionMode::ConnectedComponent,
        _ => ExecutionMode::LeadingObject,
    };
    let leading = request
        .parameters
        .get("leading_object_type")
        .and_then(|value| value.as_str());
    let executions = provider.process_executions(&request.view, mode, leading)?;
    match request.method {
        ConformanceMethod::FrequencyCoverage => frequency_coverage(&executions, &request.model),
        ConformanceMethod::TokenReplay => token_replay(&executions, &request.model),
        ConformanceMethod::Alignment => alignment(&executions, &request.model),
        ConformanceMethod::OcpnQuality => ocpn_quality(&executions, &request.model),
        ConformanceMethod::OcDeclare | ConformanceMethod::Constraints => {
            declarative(&executions, &request.model)
        }
    }
}

fn validate_artifact(artifact: &ModelArtifact) -> OcpmResult<()> {
    if artifact.schema_version != "1.0" {
        return Err(OcpmError::new(
            OcpmErrorCode::ArtifactIncompatible,
            "model artifact schema_version must be 1.0",
        ));
    }
    let mut expected = artifact.clone();
    expected.finalize_hash()?;
    if expected.content_hash != artifact.content_hash {
        return Err(OcpmError::new(
            OcpmErrorCode::ArtifactIncompatible,
            "model artifact content hash does not match its contents",
        ));
    }
    Ok(())
}

fn frequency_coverage(
    executions: &[ProcessExecution],
    artifact: &ModelArtifact,
) -> OcpmResult<ConformanceResultV1> {
    let ProcessModel::Dfg(model) = &artifact.model else {
        return Err(OcpmError::invalid_request(
            "frequency_coverage requires a DFG model",
        ));
    };
    let allowed = model
        .edges
        .iter()
        .map(|edge| (edge.source.as_str(), edge.target.as_str()))
        .collect::<BTreeSet<_>>();
    let mut conforming = 0_u64;
    let mut deviations = 0_u64;
    let mut observed = BTreeSet::new();
    let mut violations = Vec::new();
    for execution in executions {
        let mut execution_deviates = false;
        for pair in execution.events.windows(2) {
            observed.insert((pair[0].activity.as_str(), pair[1].activity.as_str()));
            if allowed.contains(&(pair[0].activity.as_str(), pair[1].activity.as_str())) {
                conforming += 1;
            } else {
                deviations += 1;
                execution_deviates = true;
            }
        }
        if execution_deviates {
            violations.push(binding(execution));
        }
    }
    let total = conforming + deviations;
    Ok(ConformanceResultV1 {
        fitness: Some(ratio(conforming, total)),
        precision: Some(ratio(
            observed.intersection(&allowed).count() as u64,
            allowed.len() as u64,
        )),
        generalization: None,
        simplicity: Some(1.0 / (1.0 + allowed.len() as f64)),
        conforming,
        deviations,
        exact: true,
        violations,
        diagnostics: BTreeMap::from([
            (
                "observed_edges".to_owned(),
                serde_json::json!(observed.len()),
            ),
            ("model_edges".to_owned(), serde_json::json!(allowed.len())),
        ]),
    })
}

fn token_replay(
    executions: &[ProcessExecution],
    artifact: &ModelArtifact,
) -> OcpmResult<ConformanceResultV1> {
    let net = match &artifact.model {
        ProcessModel::PetriNet(net) => net,
        ProcessModel::ObjectCentricPetriNet(model) => &model.net,
        _ => {
            return Err(OcpmError::invalid_request(
                "token_replay requires a Petri net or object-centric Petri net",
            ));
        }
    };
    let mut conforming = 0_u64;
    let mut deviations = 0_u64;
    let mut missing_total = 0_u64;
    let mut remaining_total = 0_u64;
    let mut consumed_total = 0_u64;
    let mut produced_total = 0_u64;
    let mut violations = Vec::new();
    for execution in executions {
        let replay = replay_trace(net, execution);
        conforming += execution.events.len() as u64 - replay.log_moves;
        deviations += replay.log_moves + replay.missing + replay.remaining;
        missing_total += replay.missing;
        remaining_total += replay.remaining;
        consumed_total += replay.consumed;
        produced_total += replay.produced;
        if replay.log_moves + replay.missing + replay.remaining > 0 {
            violations.push(binding(execution));
        }
    }
    let missing_penalty = ratio(missing_total, consumed_total.max(1));
    let remaining_penalty = ratio(remaining_total, produced_total.max(1));
    let fitness = 1.0 - 0.5 * missing_penalty - 0.5 * remaining_penalty;
    Ok(ConformanceResultV1 {
        fitness: Some(fitness.clamp(0.0, 1.0)),
        precision: None,
        generalization: None,
        simplicity: Some(1.0 / (1.0 + net.places.len() as f64 + net.transitions.len() as f64)),
        conforming,
        deviations,
        exact: true,
        violations,
        diagnostics: BTreeMap::from([
            (
                "missing_tokens".to_owned(),
                serde_json::json!(missing_total),
            ),
            (
                "remaining_tokens".to_owned(),
                serde_json::json!(remaining_total),
            ),
            (
                "consumed_tokens".to_owned(),
                serde_json::json!(consumed_total),
            ),
            (
                "produced_tokens".to_owned(),
                serde_json::json!(produced_total),
            ),
        ]),
    })
}

#[derive(Default)]
struct Replay {
    missing: u64,
    remaining: u64,
    consumed: u64,
    produced: u64,
    log_moves: u64,
}

fn replay_trace(net: &PetriNet, execution: &ProcessExecution) -> Replay {
    let mut replay = Replay::default();
    let mut tokens = net
        .places
        .iter()
        .map(|place| (place.id.as_str(), place.initial_tokens))
        .collect::<BTreeMap<_, _>>();
    for event in &execution.events {
        let Some(transition) = net
            .transitions
            .iter()
            .find(|transition| transition.label.as_deref() == Some(&event.activity))
        else {
            replay.log_moves += 1;
            continue;
        };
        let incoming = net
            .arcs
            .iter()
            .filter(|arc| arc.target == transition.id && tokens.contains_key(arc.source.as_str()))
            .collect::<Vec<_>>();
        let enabled = incoming
            .iter()
            .all(|arc| tokens.get(arc.source.as_str()).copied().unwrap_or_default() >= arc.weight);
        if !enabled {
            for arc in &incoming {
                let available = tokens.get(arc.source.as_str()).copied().unwrap_or_default();
                if available < arc.weight {
                    let missing = arc.weight - available;
                    *tokens.entry(arc.source.as_str()).or_default() += missing;
                    replay.missing += missing;
                }
            }
        }
        for arc in incoming {
            *tokens.entry(arc.source.as_str()).or_default() -= arc.weight;
            replay.consumed += arc.weight;
        }
        let outgoing = net
            .arcs
            .iter()
            .filter(|arc| arc.source == transition.id)
            .map(|arc| (arc.target.as_str(), arc.weight))
            .collect::<Vec<_>>();
        for (target, weight) in outgoing {
            if let Some(value) = tokens.get_mut(target) {
                *value += weight;
                replay.produced += weight;
            }
        }
    }
    for place in &net.places {
        let actual = tokens.get(place.id.as_str()).copied().unwrap_or_default();
        replay.remaining += actual.abs_diff(place.final_tokens);
    }
    replay
}

fn alignment(
    executions: &[ProcessExecution],
    artifact: &ModelArtifact,
) -> OcpmResult<ConformanceResultV1> {
    let ProcessModel::Dfg(model) = &artifact.model else {
        return token_replay(executions, artifact).map_err(|_| {
            OcpmError::invalid_request(
                "alignment currently accepts a DFG, Petri net, or object-centric Petri net",
            )
        });
    };
    let mut synchronous = 0_u64;
    let mut moves = 0_u64;
    let mut violations = Vec::new();
    let mut expanded = 0_u64;
    for execution in executions {
        let (cost, states) = align_dfg(model, &execution.activity_path())?;
        expanded += states;
        synchronous += execution.events.len() as u64 - cost.min(execution.events.len() as u64);
        moves += cost;
        if cost > 0 {
            violations.push(binding(execution));
        }
    }
    Ok(ConformanceResultV1 {
        fitness: Some(ratio(synchronous, synchronous + moves)),
        precision: None,
        generalization: None,
        simplicity: Some(1.0 / (1.0 + model.edges.len() as f64)),
        conforming: synchronous,
        deviations: moves,
        exact: true,
        violations,
        diagnostics: BTreeMap::from([(
            "expanded_alignment_states".to_owned(),
            serde_json::json!(expanded),
        )]),
    })
}

fn align_dfg(model: &DfgModel, trace: &[String]) -> OcpmResult<(u64, u64)> {
    if model.start_activities.is_empty() || model.end_activities.is_empty() {
        return Err(OcpmError::new(
            OcpmErrorCode::InsufficientData,
            "DFG alignment requires start and end activities",
        ));
    }
    let adjacency =
        model
            .edges
            .iter()
            .fold(BTreeMap::<&str, Vec<&str>>::new(), |mut values, edge| {
                values
                    .entry(edge.source.as_str())
                    .or_default()
                    .push(edge.target.as_str());
                values
            });
    let ends = model
        .end_activities
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut heap = BinaryHeap::<Reverse<(u64, usize, String)>>::new();
    let mut distance = BTreeMap::<(usize, String), u64>::new();
    for start in model.start_activities.keys() {
        relax(&mut heap, &mut distance, 0, 0, start);
        if !trace.is_empty() {
            relax(
                &mut heap,
                &mut distance,
                u64::from(trace[0] != *start),
                1,
                start,
            );
        }
    }
    let state_bound = (trace.len() + 1)
        .saturating_mul(model.activities.len().max(1))
        .saturating_add(1);
    let mut expanded = 0_u64;
    while let Some(Reverse((cost, index, activity))) = heap.pop() {
        if distance.get(&(index, activity.clone())) != Some(&cost) {
            continue;
        }
        expanded += 1;
        if expanded as usize > state_bound {
            return Err(OcpmError::new(
                OcpmErrorCode::SearchTruncated,
                "alignment state bound was exceeded",
            ));
        }
        if index == trace.len() && ends.contains(activity.as_str()) {
            return Ok((cost, expanded));
        }
        if index < trace.len() {
            relax(&mut heap, &mut distance, cost + 1, index + 1, &activity);
        }
        for target in adjacency
            .get(activity.as_str())
            .into_iter()
            .flatten()
            .copied()
        {
            relax(&mut heap, &mut distance, cost + 1, index, target);
            if index < trace.len() {
                relax(
                    &mut heap,
                    &mut distance,
                    cost + u64::from(trace[index] != target),
                    index + 1,
                    target,
                );
            }
        }
    }
    Err(OcpmError::new(
        OcpmErrorCode::InsufficientData,
        "no accepting DFG alignment exists",
    ))
}

fn relax(
    heap: &mut BinaryHeap<Reverse<(u64, usize, String)>>,
    distances: &mut BTreeMap<(usize, String), u64>,
    cost: u64,
    index: usize,
    activity: &str,
) {
    let key = (index, activity.to_owned());
    if distances.get(&key).is_none_or(|known| cost < *known) {
        distances.insert(key.clone(), cost);
        heap.push(Reverse((cost, index, key.1)));
    }
}

fn ocpn_quality(
    executions: &[ProcessExecution],
    artifact: &ModelArtifact,
) -> OcpmResult<ConformanceResultV1> {
    let ProcessModel::ObjectCentricPetriNet(_) = &artifact.model else {
        return Err(OcpmError::invalid_request(
            "ocpn_quality requires an object-centric Petri net",
        ));
    };
    let mut result = token_replay(executions, artifact)?;
    let object_types = executions
        .iter()
        .map(|execution| execution.object_type.clone())
        .collect::<BTreeSet<_>>();
    result.diagnostics.insert(
        "object_types_recomposed".to_owned(),
        serde_json::json!(object_types),
    );
    Ok(result)
}

fn declarative(
    executions: &[ProcessExecution],
    artifact: &ModelArtifact,
) -> OcpmResult<ConformanceResultV1> {
    let ProcessModel::OcDeclare(constraints) = &artifact.model else {
        return Err(OcpmError::invalid_request(
            "declarative conformance requires an OC-DECLARE model",
        ));
    };
    let mut conforming = 0_u64;
    let mut deviations = 0_u64;
    let mut violations = Vec::new();
    for execution in executions {
        let failed = constraints
            .iter()
            .filter(|constraint| !declare_holds(execution, constraint))
            .count() as u64;
        conforming += constraints.len() as u64 - failed;
        deviations += failed;
        if failed > 0 {
            violations.push(binding(execution));
        }
    }
    Ok(ConformanceResultV1 {
        fitness: Some(ratio(conforming, conforming + deviations)),
        precision: None,
        generalization: None,
        simplicity: Some(1.0 / (1.0 + constraints.len() as f64)),
        conforming,
        deviations,
        exact: true,
        violations,
        diagnostics: BTreeMap::from([(
            "constraint_count".to_owned(),
            serde_json::json!(constraints.len()),
        )]),
    })
}

fn declare_holds(execution: &ProcessExecution, constraint: &DeclareConstraint) -> bool {
    let path = execution.activity_path();
    let positions = |activity: &str| {
        path.iter()
            .enumerate()
            .filter(|(_, value)| value.as_str() == activity)
            .map(|(index, _)| index)
            .collect::<Vec<_>>()
    };
    let activation = positions(&constraint.activation);
    let target = constraint
        .target
        .as_deref()
        .map(positions)
        .unwrap_or_default();
    match constraint.template {
        DeclareTemplate::Existence => !activation.is_empty(),
        DeclareTemplate::Absence => activation.is_empty(),
        DeclareTemplate::Exactly => activation.len() == 1,
        DeclareTemplate::Init => activation.contains(&0),
        DeclareTemplate::End => activation.contains(&path.len().saturating_sub(1)),
        DeclareTemplate::Response => activation
            .iter()
            .all(|left| target.iter().any(|right| right > left)),
        DeclareTemplate::Precedence => target
            .iter()
            .all(|right| activation.iter().any(|left| left < right)),
        DeclareTemplate::Succession => {
            activation
                .iter()
                .all(|left| target.iter().any(|right| right > left))
                && target
                    .iter()
                    .all(|right| activation.iter().any(|left| left < right))
        }
        DeclareTemplate::Coexistence => activation.is_empty() == target.is_empty(),
        DeclareTemplate::NotCoexistence => activation.is_empty() || target.is_empty(),
        DeclareTemplate::Choice => !activation.is_empty() || !target.is_empty(),
        DeclareTemplate::ExclusiveChoice => activation.is_empty() != target.is_empty(),
    }
}

fn binding(execution: &ProcessExecution) -> QueryBinding {
    QueryBinding {
        event_ids: execution.events.iter().map(|event| event.id).collect(),
        object_ids: execution.object_ids.clone(),
        labels: vec![execution.id.clone()],
        violated: true,
    }
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        1.0
    } else {
        numerator as f64 / denominator as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_dfg_alignment_accepts_model_path() {
        let model = DfgModel {
            activities: vec!["a".to_owned(), "b".to_owned()],
            start_activities: BTreeMap::from([("a".to_owned(), 1)]),
            end_activities: BTreeMap::from([("b".to_owned(), 1)]),
            edges: vec![ocpm_core::DfgEdge {
                source: "a".to_owned(),
                target: "b".to_owned(),
                object_type: String::new(),
                frequency: 1,
            }],
        };
        assert_eq!(
            align_dfg(&model, &["a".to_owned(), "b".to_owned()])
                .unwrap()
                .0,
            0
        );
    }
}
