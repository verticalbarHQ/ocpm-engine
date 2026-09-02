use ocpm_core::{CanonicalLog, DatasetView, Event, EventObjectRelation, Object, Timestamp};
use ocpm_engine::{BottleneckRequest, Engine};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::io::{self, Read};

fn positive(payload: &Value, name: &str) -> u64 {
    payload[name]
        .as_u64()
        .filter(|value| *value > 0)
        .unwrap_or_else(|| panic!("{name} must be positive"))
}

fn build_log(cases: u64, events_per_case: u64) -> CanonicalLog {
    let mut log = CanonicalLog {
        dataset_id: "candidate-gate".to_owned(),
        tenant_id: "public-fixture".to_owned(),
        ..CanonicalLog::default()
    };
    let mut relation_id = 0_u64;
    for object_id in 1..=cases {
        log.objects.push(Object {
            id: object_id,
            external_id: format!("o{object_id}"),
            object_type: "order".to_owned(),
        });
        for offset in 0..events_per_case {
            let event_id = (object_id - 1) * events_per_case + offset + 1;
            log.events.push(Event {
                id: event_id,
                external_id: format!("e{event_id}"),
                activity: format!("activity_{}", offset % 5),
                timestamp: Timestamp::from_epoch_nanos(
                    i128::from(object_id) * 1_000_000_000 + i128::from(offset) * 100_000_000,
                ),
                sequence: offset,
                lifecycle: None,
                attributes: BTreeMap::new(),
            });
            relation_id += 1;
            log.event_object_relations.push(EventObjectRelation {
                relation_id,
                event_id,
                object_id,
                qualifier: "order".to_owned(),
            });
        }
    }
    log
}

fn main() {
    let mut input = String::new();
    io::stdin()
        .read_to_string(&mut input)
        .expect("read request");
    let request: Value = serde_json::from_str(&input).expect("parse request");
    let payload = &request["payload"];
    let cases = positive(payload, "cases");
    let events_per_case = positive(payload, "events_per_case");
    let iterations = positive(payload, "iterations");
    let engine = Engine::from_log(build_log(cases, events_per_case)).expect("build engine");
    let profile = engine
        .profile(&DatasetView::default())
        .expect("profile fixture");
    let mut result = None;
    for _ in 0..iterations {
        result = Some(
            engine
                .bottlenecks(&BottleneckRequest {
                    minimum_support: 1,
                    leading_object_type: Some("order".to_owned()),
                    ..BottleneckRequest::default()
                })
                .expect("analyze fixture"),
        );
    }
    let result = result.expect("positive iterations");
    let answer = json!({
        "profile": {
            "event_count": profile.event_count,
            "object_count": profile.object_count,
            "e2o_count": profile.e2o_count,
            "o2o_count": profile.o2o_count,
            "activities": profile.activities,
            "object_types": profile.object_types,
        },
        "bottlenecks": result,
    });
    // The engine has no private persistent store. Measure the deterministic
    // serialized analysis result rather than an unrelated compiled executable.
    let storage_bytes = u64::try_from(
        serde_json::to_vec(&answer)
            .expect("serialize storage representation")
            .len(),
    )
    .expect("serialized result size fits u64");
    let output = json!({
        "input": {
            "workload": request["workload"],
            "cases": cases,
            "events_per_case": events_per_case,
            "iterations": iterations,
        },
        "answer": answer,
        "storage_bytes": storage_bytes,
    });
    println!(
        "{}",
        serde_json::to_string(&output).expect("serialize output")
    );
}
