use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Barrier,
};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{DateTime, FixedOffset, NaiveDateTime, TimeZone, Utc};
use process_mining::core::event_data::object_centric::linked_ocel::{
    slim_linked_ocel::ObjectIndex, LinkedOCELAccess, SlimLinkedOCEL,
};
use process_mining::Importable;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

const ARM: &str = "rust4pm";
const RUST4PM_VERSION: &str = "0.6.0";
const RUST4PM_REVISION: &str = "b4c06f323fca55cf57eaf44ac25b46ea7c448cb4";
const WORKLOADS: [&str; 4] = [
    "dfg_conformance_95pct",
    "variant_conformance_95pct",
    "next_activity_prediction",
    "edge_bottleneck_ranking",
];

#[derive(Debug, Deserialize)]
struct Manifest {
    datasets: Vec<ManifestDataset>,
}

#[derive(Debug, Deserialize)]
struct ManifestDataset {
    name: String,
    filename: String,
    sqlite_sha256: String,
    fixture: FixtureWire,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct FixtureWire {
    dataset_name: String,
    baseline_dataset_id: i64,
    ocpm_dataset_id: i64,
    tenant_id: i64,
    object_type: String,
    from_time: String,
    train_to: String,
    test_from: String,
    to_time: String,
    cases: u64,
    train_cases: u64,
    test_cases: u64,
}

#[derive(Debug, Clone)]
struct Fixture {
    wire: FixtureWire,
    from_time: DateTime<FixedOffset>,
    train_to: DateTime<FixedOffset>,
    test_from: DateTime<FixedOffset>,
    to_time: DateTime<FixedOffset>,
}

impl Fixture {
    fn parse(wire: FixtureWire) -> Result<Self, String> {
        Ok(Self {
            from_time: parse_time(&wire.from_time)?,
            train_to: parse_time(&wire.train_to)?,
            test_from: parse_time(&wire.test_from)?,
            to_time: parse_time(&wire.to_time)?,
            wire,
        })
    }
}

#[derive(Debug)]
struct Config {
    manifest: PathBuf,
    data_dir: PathBuf,
    output: PathBuf,
    datasets: BTreeSet<String>,
    warmups: usize,
    runs: usize,
    latency_epochs: usize,
    concurrency: Vec<usize>,
    concurrency_epochs: usize,
    concurrency_min_seconds: f64,
    concurrency_requests: usize,
}

#[derive(Debug)]
struct NativeModel {
    ocel: SlimLinkedOCEL,
    telemetry: Value,
}

fn parse_args() -> Result<Config, String> {
    let mut values: HashMap<String, String> = HashMap::new();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut index = 0;
    while index < args.len() {
        let key = args[index].clone();
        if !key.starts_with("--") || index + 1 >= args.len() {
            return Err(format!("expected --name value, got {key:?}"));
        }
        values.insert(key, args[index + 1].clone());
        index += 2;
    }
    let get = |key: &str, default: &str| {
        values
            .get(key)
            .cloned()
            .unwrap_or_else(|| default.to_string())
    };
    let concurrency = get("--concurrency", "1,2,4,8")
        .split(',')
        .map(|value| value.parse::<usize>().map_err(|e| e.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Config {
        manifest: get("--manifest", "/results/ecosystem-manifest.json").into(),
        data_dir: get("--data-dir", "/data").into(),
        output: get("--output", "/results/ecosystem-rust4pm-arm.json").into(),
        datasets: get("--datasets", "rust4pm_p2p")
            .split(',')
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .collect(),
        warmups: get("--warmups", "10")
            .parse::<usize>()
            .map_err(|e| e.to_string())?,
        runs: get("--runs", "30")
            .parse::<usize>()
            .map_err(|e| e.to_string())?,
        latency_epochs: get("--latency-epochs", "3")
            .parse::<usize>()
            .map_err(|e| e.to_string())?,
        concurrency,
        concurrency_epochs: get("--concurrency-epochs", "3")
            .parse::<usize>()
            .map_err(|e| e.to_string())?,
        concurrency_min_seconds: get("--concurrency-min-seconds", "5")
            .parse::<f64>()
            .map_err(|e| e.to_string())?,
        concurrency_requests: get("--concurrency-requests", "32")
            .parse::<usize>()
            .map_err(|e| e.to_string())?,
    })
}

fn parse_time(value: &str) -> Result<DateTime<FixedOffset>, String> {
    if let Ok(value) = DateTime::parse_from_rfc3339(value) {
        return Ok(value);
    }
    for format in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"] {
        if let Ok(value) = NaiveDateTime::parse_from_str(value, format) {
            return Ok(Utc.from_utc_datetime(&value).fixed_offset());
        }
    }
    Err(format!("unsupported timestamp {value:?}"))
}

fn load_native_model(path: &Path) -> Result<NativeModel, String> {
    let ocel = SlimLinkedOCEL::import_from_path(path).map_err(|error| error.to_string())?;
    Ok(NativeModel {
        telemetry: json!({
            "adapter": "rust4pm_documented_ocel2_sqlite_importer",
            "events": ocel.get_num_evs(),
            "objects": ocel.get_num_obs(),
        }),
        ocel,
    })
}

fn lifecycle_rows(
    model: &SlimLinkedOCEL,
    object_type: &str,
) -> Vec<Vec<(String, DateTime<FixedOffset>)>> {
    model
        .get_obs_of_type(object_type)
        .map(|object: &ObjectIndex| {
            let mut events = object.get_e2o_rev(model).copied().collect::<Vec<_>>();
            events.sort_by(|left, right| {
                model
                    .get_ev_time(left)
                    .cmp(model.get_ev_time(right))
                    .then_with(|| model.get_ev_id(left).cmp(model.get_ev_id(right)))
            });
            events
                .into_iter()
                .map(|event| {
                    (
                        model.get_ev_type_of(event).to_string(),
                        *model.get_ev_time(event),
                    )
                })
                .collect()
        })
        .collect()
}

fn canonical_variant(activities: &[String]) -> String {
    serde_json::to_string(activities).expect("serializing strings cannot fail")
}

fn dfg_score(
    train: &BTreeMap<(String, String), u64>,
    test: &BTreeMap<(String, String), u64>,
) -> Value {
    let keys: BTreeSet<_> = train.keys().chain(test.keys()).cloned().collect();
    let mut ranked: Vec<_> = keys.into_iter().collect();
    ranked.sort_by(|left, right| {
        train
            .get(right)
            .unwrap_or(&0)
            .cmp(train.get(left).unwrap_or(&0))
            .then_with(|| left.cmp(right))
    });
    let target_frequency = ((train.values().sum::<u64>() as f64) * 0.95).ceil() as u64;
    let mut covered = 0_u64;
    let mut model = Vec::new();
    for key in ranked {
        if covered >= target_frequency {
            break;
        }
        let count = *train.get(&key).unwrap_or(&0);
        if count > 0 {
            model.push(json!([key.0, key.1, "directly_follows"]));
            covered += count;
        }
    }
    model.sort_by_key(|value| value.to_string());
    let accepted: BTreeSet<(String, String)> = model
        .iter()
        .map(|row| {
            (
                row[0].as_str().unwrap().to_string(),
                row[1].as_str().unwrap().to_string(),
            )
        })
        .collect();
    let total = test.values().sum::<u64>();
    let conforming = test
        .iter()
        .filter(|(key, _)| accepted.contains(*key))
        .map(|(_, value)| value)
        .sum::<u64>();
    json!({
        "fitness": round_to(if total == 0 { 1.0 } else { conforming as f64 / total as f64 }, 12),
        "conforming": conforming,
        "deviations": total - conforming,
        "test_total": total,
        "model": model,
    })
}

fn next_score(
    train: &BTreeMap<(String, String), u64>,
    test: &BTreeMap<(String, String), u64>,
) -> Value {
    let mut winners: BTreeMap<String, (String, u64)> = BTreeMap::new();
    for ((source, target), count) in train {
        if *count == 0 {
            continue;
        }
        match winners.get(source) {
            None => {
                winners.insert(source.clone(), (target.clone(), *count));
            }
            Some((current_target, current_count))
                if count > current_count || (count == current_count && target < current_target) =>
            {
                winners.insert(source.clone(), (target.clone(), *count));
            }
            _ => {}
        }
    }
    let total = test.values().sum::<u64>();
    let correct = test
        .iter()
        .filter(|((source, target), _)| {
            winners
                .get(source)
                .map(|winner| winner.0 == *target)
                .unwrap_or(false)
        })
        .map(|(_, count)| count)
        .sum::<u64>();
    let predictions: Vec<_> = winners
        .iter()
        .map(|(source, (target, _count))| json!([source, "directly_follows", target]))
        .collect();
    json!({
        "accuracy": round_to(if total == 0 { 1.0 } else { correct as f64 / total as f64 }, 12),
        "correct": correct,
        "test_total": total,
        "predictions": predictions,
    })
}

fn variant_score(train: &BTreeMap<String, u64>, test: &BTreeMap<String, u64>) -> Value {
    let mut ranked: Vec<_> = train.keys().cloned().collect();
    ranked.sort_by(|left, right| train[right].cmp(&train[left]).then_with(|| left.cmp(right)));
    let target_frequency = ((train.values().sum::<u64>() as f64) * 0.95).ceil() as u64;
    let mut covered = 0_u64;
    let mut model = Vec::new();
    for variant in ranked {
        if covered >= target_frequency {
            break;
        }
        let count = train[&variant];
        if count > 0 {
            model.push(variant);
            covered += count;
        }
    }
    model.sort();
    let accepted: BTreeSet<_> = model.iter().cloned().collect();
    let total = test.values().sum::<u64>();
    let conforming = test
        .iter()
        .filter(|(variant, _)| accepted.contains(*variant))
        .map(|(_, count)| count)
        .sum::<u64>();
    json!({
        "fitness": round_to(if total == 0 { 1.0 } else { conforming as f64 / total as f64 }, 12),
        "conforming": conforming,
        "deviations": total - conforming,
        "test_total": total,
        "model": model,
    })
}

fn round_to(value: f64, digits: i32) -> f64 {
    let factor = 10_f64.powi(digits);
    (value * factor).round() / factor
}

fn run_workload(model: &SlimLinkedOCEL, fixture: &Fixture, workload: &str) -> Value {
    let lifecycles = lifecycle_rows(model, &fixture.wire.object_type);
    let mut train_dfg: BTreeMap<(String, String), u64> = BTreeMap::new();
    let mut test_dfg: BTreeMap<(String, String), u64> = BTreeMap::new();
    let mut train_variants: BTreeMap<String, u64> = BTreeMap::new();
    let mut test_variants: BTreeMap<String, u64> = BTreeMap::new();
    let mut durations: BTreeMap<(String, String), (u64, f64)> = BTreeMap::new();
    let mut selected_cases = 0_u64;
    let mut selected_events = 0_u64;

    for events in lifecycles {
        let Some((_, start_time)) = events.first() else {
            continue;
        };
        let end_time = &events.last().unwrap().1;
        let in_train = *start_time >= fixture.from_time && *end_time <= fixture.train_to;
        let in_test = *start_time >= fixture.test_from && *end_time <= fixture.to_time;
        let in_full = *start_time >= fixture.from_time && *end_time <= fixture.to_time;
        if workload == "edge_bottleneck_ranking" {
            if !in_full {
                continue;
            }
        } else if !(in_train || in_test) {
            continue;
        }
        selected_cases += 1;
        selected_events += events.len() as u64;
        if workload == "variant_conformance_95pct" {
            let activities: Vec<String> = events.iter().map(|row| row.0.clone()).collect();
            let variant = canonical_variant(&activities);
            if in_train {
                *train_variants.entry(variant).or_default() += 1;
            } else if in_test {
                *test_variants.entry(variant).or_default() += 1;
            }
            continue;
        }
        for edge in events.windows(2) {
            let key = (edge[0].0.clone(), edge[1].0.clone());
            if workload == "edge_bottleneck_ranking" {
                let seconds = (edge[1].1 - edge[0].1)
                    .num_microseconds()
                    .expect("SAP fixture duration fits i64") as f64
                    / 1_000_000.0;
                let entry = durations.entry(key).or_default();
                entry.0 += 1;
                entry.1 += seconds;
            } else if in_train {
                *train_dfg.entry(key).or_default() += 1;
            } else if in_test {
                *test_dfg.entry(key).or_default() += 1;
            }
        }
    }

    let (answer, aggregate_rows) = match workload {
        "dfg_conformance_95pct" => (
            dfg_score(&train_dfg, &test_dfg),
            train_dfg
                .keys()
                .chain(test_dfg.keys())
                .collect::<BTreeSet<_>>()
                .len(),
        ),
        "next_activity_prediction" => (
            next_score(&train_dfg, &test_dfg),
            train_dfg
                .keys()
                .chain(test_dfg.keys())
                .collect::<BTreeSet<_>>()
                .len(),
        ),
        "variant_conformance_95pct" => (
            variant_score(&train_variants, &test_variants),
            train_variants
                .keys()
                .chain(test_variants.keys())
                .collect::<BTreeSet<_>>()
                .len(),
        ),
        "edge_bottleneck_ranking" => {
            let mut rows: Vec<Value> = durations
                .into_iter()
                .map(|((source, target), (frequency, total))| {
                    json!([
                        source,
                        target,
                        frequency,
                        round_to(total / frequency as f64, 6)
                    ])
                })
                .collect();
            rows.sort_by(|left, right| {
                right[3]
                    .as_f64()
                    .unwrap()
                    .total_cmp(&left[3].as_f64().unwrap())
                    .then_with(|| right[2].as_u64().cmp(&left[2].as_u64()))
                    .then_with(|| left[0].as_str().cmp(&right[0].as_str()))
                    .then_with(|| left[1].as_str().cmp(&right[1].as_str()))
            });
            let count = rows.len();
            (Value::Array(rows), count)
        }
        _ => panic!("unknown workload {workload}"),
    };
    json!({
        "answer": answer,
        "input": {
            "source": "process_mining::SlimLinkedOCEL",
            "native_object_type_cases": model.get_obs_of_type(&fixture.wire.object_type).count(),
            "selected_cases": selected_cases,
            "event_rows": selected_events,
            "aggregate_rows": aggregate_rows,
        }
    })
}

fn canonical(value: &Value) -> String {
    fn sorted(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let mut ordered = BTreeMap::new();
                for (key, value) in map {
                    ordered.insert(key.clone(), sorted(value));
                }
                serde_json::to_value(ordered).unwrap()
            }
            Value::Array(values) => Value::Array(values.iter().map(sorted).collect()),
            _ => value.clone(),
        }
    }
    serde_json::to_string(&sorted(value)).unwrap()
}

fn answer_sha256(answer: &Value) -> String {
    format!("{:x}", Sha256::digest(canonical(answer).as_bytes()))
}

fn percentile(samples: &[u64], percentile_value: f64) -> u64 {
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    let index = ((ordered.len() as f64 * percentile_value).ceil() as usize)
        .saturating_sub(1)
        .min(ordered.len() - 1);
    ordered[index]
}

fn median(samples: &[u64]) -> f64 {
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    if ordered.len() % 2 == 1 {
        ordered[ordered.len() / 2] as f64
    } else {
        (ordered[ordered.len() / 2 - 1] as f64 + ordered[ordered.len() / 2] as f64) / 2.0
    }
}

fn latency_metrics(samples: &[u64]) -> Value {
    json!({
        "p50_ms": round_to(median(samples) / 1_000_000.0, 3),
        "p95_ms": round_to(percentile(samples, 0.95) as f64 / 1_000_000.0, 3),
        "minimum_ms": round_to(*samples.iter().min().unwrap() as f64 / 1_000_000.0, 3),
        "maximum_ms": round_to(*samples.iter().max().unwrap() as f64 / 1_000_000.0, 3),
        "runs": samples.len(),
    })
}

fn measure_serial(
    model: &SlimLinkedOCEL,
    fixture: &Fixture,
    warmups: usize,
    runs: usize,
    epochs: usize,
) -> Result<Value, String> {
    let mut result = Map::new();
    for workload in WORKLOADS {
        let preflight = run_workload(model, fixture, workload);
        let expected = preflight["answer"].clone();
        for _ in 0..warmups {
            if run_workload(model, fixture, workload)["answer"] != expected {
                return Err(format!("{workload}: warmup answer changed"));
            }
        }
        let mut all_samples = Vec::new();
        let mut epoch_results = Vec::new();
        for epoch in 0..epochs {
            let mut samples = Vec::new();
            for _ in 0..runs {
                let started = Instant::now();
                let measured = run_workload(model, fixture, workload);
                let elapsed = started.elapsed().as_nanos() as u64;
                if measured["answer"] != expected {
                    return Err(format!("{workload}: measured answer changed"));
                }
                samples.push(elapsed);
            }
            all_samples.extend_from_slice(&samples);
            let mut value = latency_metrics(&samples);
            value["epoch"] = json!(epoch + 1);
            value["samples_ns"] = json!(samples);
            epoch_results.push(value);
        }
        let epoch_p95: Vec<f64> = epoch_results
            .iter()
            .map(|row| row["p95_ms"].as_f64().unwrap())
            .collect();
        let mut value = latency_metrics(&all_samples);
        value["correct_within_arm"] = json!(true);
        value["answer"] = expected.clone();
        value["answer_sha256"] = json!(answer_sha256(&expected));
        value["input"] = preflight["input"].clone();
        value["epoch_count"] = json!(epochs);
        let mut ordered = epoch_p95.clone();
        ordered.sort_by(f64::total_cmp);
        let epoch_median = if ordered.len() % 2 == 1 {
            ordered[ordered.len() / 2]
        } else {
            (ordered[ordered.len() / 2 - 1] + ordered[ordered.len() / 2]) / 2.0
        };
        value["epoch_p95_median_ms"] = json!(round_to(epoch_median, 3));
        value["epoch_p95_range_ms"] = json!([ordered.first().unwrap(), ordered.last().unwrap()]);
        value["epochs"] = Value::Array(epoch_results);
        result.insert(workload.to_string(), value);
    }
    Ok(Value::Object(result))
}

fn proc_memory(field: &str, path: &str) -> u64 {
    fs::read_to_string(path)
        .ok()
        .and_then(|text| {
            text.lines().find_map(|line| {
                line.strip_prefix(field).and_then(|rest| {
                    rest.split_whitespace()
                        .next()
                        .and_then(|value| value.parse::<u64>().ok())
                })
            })
        })
        .unwrap_or(0)
        * 1024
}

fn rss_bytes() -> u64 {
    proc_memory("VmRSS:", "/proc/self/status")
}

fn pss_bytes() -> u64 {
    proc_memory("Pss:", "/proc/self/smaps_rollup")
}

fn sample_peak<T, F: FnOnce() -> T>(call: F) -> (T, Value) {
    let baseline = rss_bytes();
    let stop = Arc::new(AtomicBool::new(false));
    let monitor_stop = Arc::clone(&stop);
    let monitor = thread::spawn(move || {
        let mut peak = rss_bytes();
        while !monitor_stop.load(Ordering::Relaxed) {
            peak = peak.max(rss_bytes());
            thread::sleep(Duration::from_millis(1));
        }
        peak.max(rss_bytes())
    });
    let value = call();
    stop.store(true, Ordering::Relaxed);
    let peak = monitor.join().unwrap().max(rss_bytes());
    (
        value,
        json!({
            "baseline_rss_bytes": baseline,
            "peak_rss_bytes": peak,
            "incremental_peak_bytes": peak.saturating_sub(baseline),
            "resident_pss_bytes": pss_bytes(),
        }),
    )
}

fn concurrency_epoch(
    model: Arc<SlimLinkedOCEL>,
    fixture: Fixture,
    expected: Value,
    workers: usize,
    minimum_seconds: f64,
    minimum_requests: usize,
) -> Result<Value, String> {
    let barrier = Arc::new(Barrier::new(workers + 1));
    let deadline = Arc::new(std::sync::Mutex::new(None::<Instant>));
    let mut handles = Vec::new();
    for _ in 0..workers {
        let worker_model = Arc::clone(&model);
        let worker_fixture = fixture.clone();
        let worker_expected = expected.clone();
        let worker_barrier = Arc::clone(&barrier);
        let worker_deadline = Arc::clone(&deadline);
        handles.push(thread::spawn(move || -> Result<Vec<u64>, String> {
            if run_workload(&worker_model, &worker_fixture, "dfg_conformance_95pct")["answer"]
                != worker_expected
            {
                return Err("warmup correctness mismatch".to_string());
            }
            worker_barrier.wait();
            let until = worker_deadline.lock().unwrap().unwrap();
            let mut samples = Vec::new();
            while samples.len() < minimum_requests || Instant::now() < until {
                let started = Instant::now();
                let measured =
                    run_workload(&worker_model, &worker_fixture, "dfg_conformance_95pct");
                samples.push(started.elapsed().as_nanos() as u64);
                if measured["answer"] != worker_expected {
                    return Err("measured correctness mismatch".to_string());
                }
            }
            Ok(samples)
        }));
    }
    let started = Instant::now();
    *deadline.lock().unwrap() = Some(started + Duration::from_secs_f64(minimum_seconds));
    barrier.wait();
    let worker_samples = handles
        .into_iter()
        .map(|handle| handle.join().map_err(|_| "worker panicked".to_string())?)
        .collect::<Result<Vec<_>, _>>()?;
    let wall = started.elapsed();
    let request_counts: Vec<usize> = worker_samples.iter().map(Vec::len).collect();
    let samples: Vec<u64> = worker_samples.into_iter().flatten().collect();
    let mut value = latency_metrics(&samples);
    value["requests"] = json!(samples.len());
    value["wall_ms"] = json!(round_to(wall.as_secs_f64() * 1000.0, 3));
    value["throughput_qps"] = json!(round_to(samples.len() as f64 / wall.as_secs_f64(), 3));
    value["worker_request_counts"] = json!(request_counts);
    value["process_rss_bytes"] = json!(rss_bytes());
    value["process_pss_bytes"] = json!(pss_bytes());
    value["correct"] = json!(true);
    Ok(value)
}

fn aggregate_concurrency(workers: usize, epochs: Vec<Value>) -> Value {
    let mut qps: Vec<f64> = epochs
        .iter()
        .map(|row| row["throughput_qps"].as_f64().unwrap())
        .collect();
    let mut p50: Vec<f64> = epochs
        .iter()
        .map(|row| row["p50_ms"].as_f64().unwrap())
        .collect();
    let mut p95: Vec<f64> = epochs
        .iter()
        .map(|row| row["p95_ms"].as_f64().unwrap())
        .collect();
    qps.sort_by(f64::total_cmp);
    p50.sort_by(f64::total_cmp);
    p95.sort_by(f64::total_cmp);
    let middle = |values: &[f64]| values[values.len() / 2];
    json!({
        "workers": workers,
        "epoch_count": epochs.len(),
        "requests": epochs.iter().map(|row| row["requests"].as_u64().unwrap()).sum::<u64>(),
        "throughput_qps": middle(&qps),
        "p50_ms": middle(&p50),
        "p95_ms": middle(&p95),
        "maximum_process_rss_bytes": epochs.iter().map(|row| row["process_rss_bytes"].as_u64().unwrap()).max().unwrap(),
        "maximum_process_pss_bytes": epochs.iter().map(|row| row["process_pss_bytes"].as_u64().unwrap()).max().unwrap(),
        "correct": true,
        "epochs": epochs,
    })
}

fn native_import_probe(path: &Path) -> Value {
    let started = Instant::now();
    match std::panic::catch_unwind(|| SlimLinkedOCEL::import_from_path(path)) {
        Ok(Ok(model)) => json!({
            "documented_importer": true,
            "success": true,
            "elapsed_s": round_to(started.elapsed().as_secs_f64(), 6),
            "events": model.get_num_evs(),
            "objects": model.get_num_obs(),
        }),
        Ok(Err(error)) => json!({
            "documented_importer": true,
            "success": false,
            "elapsed_s": round_to(started.elapsed().as_secs_f64(), 6),
            "error_type": "OCELIOError",
            "error": error.to_string(),
        }),
        Err(_) => json!({
            "documented_importer": true,
            "success": false,
            "elapsed_s": round_to(started.elapsed().as_secs_f64(), 6),
            "error_type": "panic",
            "error": "documented importer panicked",
        }),
    }
}

fn sha256_file(path: &Path) -> Result<String, String> {
    let bytes = fs::read(path).map_err(|e| e.to_string())?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn main() -> Result<(), String> {
    let config = parse_args()?;
    let manifest: Manifest =
        serde_json::from_slice(&fs::read(&config.manifest).map_err(|e| format!("manifest: {e}"))?)
            .map_err(|e| e.to_string())?;
    let process_baseline_rss = rss_bytes();
    let mut datasets = Vec::new();

    for entry in manifest.datasets {
        if !config.datasets.contains(&entry.name) {
            continue;
        }
        let fixture = Fixture::parse(entry.fixture)?;
        let path = config.data_dir.join(entry.filename);
        let actual_hash = sha256_file(&path)?;
        if actual_hash != entry.sqlite_sha256 {
            return Err(format!(
                "{}: source hash {} != manifest hash {}",
                entry.name, actual_hash, entry.sqlite_sha256
            ));
        }
        let native_probe = native_import_probe(&path);
        if native_probe["success"].as_bool() != Some(true) {
            return Err(format!(
                "{}: Rust4PM documented importer failed: {}",
                entry.name,
                native_probe["error"].as_str().unwrap_or("unknown error")
            ));
        }
        let rss_before = rss_bytes();
        let started = Instant::now();
        let (loaded, load_memory) = sample_peak(|| load_native_model(&path));
        let model = loaded?;
        let load_seconds = started.elapsed().as_secs_f64();
        let rss_after = rss_bytes();
        println!("rust4pm serial {}", fixture.wire.dataset_name);
        let serial = measure_serial(
            &model.ocel,
            &fixture,
            config.warmups,
            config.runs,
            config.latency_epochs,
        )?;
        let mut memory = Map::new();
        for workload in WORKLOADS {
            let (value, stats) = sample_peak(|| run_workload(&model.ocel, &fixture, workload));
            let mut stats = stats;
            stats["answer_sha256"] = serial[workload]["answer_sha256"].clone();
            stats["input"] = value["input"].clone();
            memory.insert(workload.to_string(), stats);
        }
        let expected = serial["dfg_conformance_95pct"]["answer"].clone();
        let adapter_telemetry = model.telemetry.clone();
        let model = Arc::new(model.ocel);
        let mut concurrency = Map::new();
        for workers in &config.concurrency {
            let mut epochs = Vec::new();
            for epoch_index in 0..config.concurrency_epochs {
                println!(
                    "  rust4pm concurrency {} x{} epoch {}/{}",
                    fixture.wire.dataset_name,
                    workers,
                    epoch_index + 1,
                    config.concurrency_epochs
                );
                let mut value = concurrency_epoch(
                    Arc::clone(&model),
                    fixture.clone(),
                    expected.clone(),
                    *workers,
                    config.concurrency_min_seconds,
                    config.concurrency_requests,
                )?;
                value["epoch"] = json!(epoch_index + 1);
                epochs.push(value);
            }
            concurrency.insert(workers.to_string(), aggregate_concurrency(*workers, epochs));
        }
        datasets.push(json!({
            "dataset": fixture.wire.dataset_name,
            "fixture": fixture.wire,
            "source_sqlite_bytes": fs::metadata(&path).map_err(|e| e.to_string())?.len(),
            "source_sqlite_sha256": actual_hash,
            "native_import_probe": native_probe,
            "adapter": model_telemetry_from_arc(&model, adapter_telemetry, &entry.name, load_memory, rss_before, rss_after, load_seconds),
            "serial": serial,
            "concurrency": concurrency,
            "memory": memory,
        }));
    }

    let executable_bytes = std::env::current_exe()
        .ok()
        .and_then(|path| fs::metadata(path).ok())
        .map(|meta| meta.len())
        .unwrap_or(0);
    let output = json!({
        "schema_version": 1,
        "generated_at": Utc::now().to_rfc3339(),
        "arm": ARM,
        "implementation": {
            "rust4pm_version": RUST4PM_VERSION,
            "rust4pm_revision": RUST4PM_REVISION,
            "rustc": option_env!("RUSTC_VERSION").unwrap_or("recorded by image provenance"),
            "platform": std::env::consts::OS,
            "architecture": std::env::consts::ARCH,
            "image_id": std::env::var("OCPM_RUST4PM_IMAGE_ID").ok(),
            "controller_source_tree_clean": std::env::var("OCPM_ENGINE_SOURCE_TREE_CLEAN").ok().as_deref() == Some("true"),
        },
        "method": {
            "data_model": "process_mining::SlimLinkedOCEL",
            "adapter": "Rust4PM 0.6.0 Importable OCEL 2.0 SQLite importer",
            "process_model": "one preloaded read-only SlimLinkedOCEL shared by native threads",
            "native_importer": "required to succeed on the unmodified upstream source",
        },
        "process_baseline_rss_bytes": process_baseline_rss,
        "storage": {"benchmark_binary_bytes": executable_bytes},
        "datasets": datasets,
    });
    if let Some(parent) = config.output.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    fs::write(
        &config.output,
        format!("{}\n", serde_json::to_string_pretty(&output).unwrap()),
    )
    .map_err(|e| e.to_string())?;
    println!("wrote {}", config.output.display());
    Ok(())
}

fn model_telemetry_from_arc(
    model: &Arc<SlimLinkedOCEL>,
    mut telemetry: Value,
    dataset_name: &str,
    load_memory: Value,
    rss_before: u64,
    rss_after: u64,
    load_seconds: f64,
) -> Value {
    telemetry["dataset"] = json!(dataset_name);
    telemetry["events"] = json!(model.get_num_evs());
    telemetry["objects"] = json!(model.get_num_obs());
    telemetry["load"] = json!({
        "seconds": round_to(load_seconds, 6),
        "rss_before_bytes": rss_before,
        "rss_after_bytes": rss_after,
        "sample": load_memory,
    });
    telemetry
}
