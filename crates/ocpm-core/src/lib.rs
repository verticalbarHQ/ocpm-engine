//! Deterministic, allocation-conscious process-mining kernels.
//!
//! PostgreSQL performs selective scans and sufficient-statistic aggregation.
//! This crate consumes only compact aggregate rows and constructs or scores
//! models without holding database connections or Python objects.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum AnalyticsError {
    #[error("coverage must be finite and in the interval (0, 1]")]
    InvalidCoverage,
    #[error("all input columns must have the same length")]
    ColumnLengthMismatch,
    #[error("frequency counts overflowed an unsigned 64-bit total")]
    CountOverflow,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct TransitionKey {
    pub source: String,
    pub target: String,
    pub edge_type: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConformanceResult {
    pub fitness: f64,
    pub conforming: u64,
    pub deviations: u64,
    pub test_total: u64,
    pub model_size: usize,
    pub model: Vec<TransitionKey>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NextActivityResult {
    pub accuracy: f64,
    pub correct: u64,
    pub test_total: u64,
    pub model_size: usize,
    pub predictions: Vec<(String, String, String)>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VariantConformanceResult {
    pub fitness: f64,
    pub conforming: u64,
    pub deviations: u64,
    pub test_total: u64,
    pub model_size: usize,
    pub model: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DriftContributor {
    pub label: String,
    pub baseline_share: f64,
    pub current_share: f64,
    pub share_delta: f64,
    pub js_contribution: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FrequencyDriftResult {
    pub divergence: f64,
    pub baseline_total: u64,
    pub current_total: u64,
    pub contributors: Vec<DriftContributor>,
}

fn validate_coverage(coverage: f64) -> Result<(), AnalyticsError> {
    if coverage.is_finite() && coverage > 0.0 && coverage <= 1.0 {
        Ok(())
    } else {
        Err(AnalyticsError::InvalidCoverage)
    }
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        1.0
    } else {
        numerator as f64 / denominator as f64
    }
}

/// Build the smallest frequency-ranked DFG that reaches `coverage`, then score it.
///
/// Ties use the transition key, making the result stable across query plans,
/// architectures, and hash seeds.
pub fn dfg_frequency_conformance(
    rows: impl IntoIterator<Item = (TransitionKey, u64, u64)>,
    coverage: f64,
) -> Result<ConformanceResult, AnalyticsError> {
    validate_coverage(coverage)?;
    let mut rows: Vec<_> = rows.into_iter().collect();
    rows.sort_unstable_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));

    let train_total = rows.iter().map(|row| row.1).sum::<u64>();
    let target = (train_total as f64 * coverage).ceil() as u64;
    let mut covered = 0_u64;
    let mut model = Vec::new();
    for (key, train_count, _) in &rows {
        if covered >= target {
            break;
        }
        if *train_count > 0 {
            covered = covered.saturating_add(*train_count);
            model.push(key.clone());
        }
    }

    let accepted: HashSet<_> = model.iter().collect();
    let test_total = rows.iter().map(|row| row.2).sum::<u64>();
    let conforming = rows
        .iter()
        .filter(|row| accepted.contains(&row.0))
        .map(|row| row.2)
        .sum::<u64>();
    let deviations = test_total.saturating_sub(conforming);

    model.sort_unstable();
    Ok(ConformanceResult {
        fitness: ratio(conforming, test_total),
        conforming,
        deviations,
        test_total,
        model_size: model.len(),
        model,
    })
}

/// Fit the most frequent target per `(source, edge_type)` and score held-out rows.
pub fn next_activity_prediction(
    rows: impl IntoIterator<Item = (TransitionKey, u64, u64)>,
) -> NextActivityResult {
    let rows: Vec<_> = rows.into_iter().collect();
    let mut winners: HashMap<(String, String), (String, u64)> = HashMap::new();
    for (key, train_count, _) in &rows {
        if *train_count == 0 {
            continue;
        }
        let group = (key.source.clone(), key.edge_type.clone());
        let candidate = (key.target.clone(), *train_count);
        winners
            .entry(group)
            .and_modify(|current| {
                if candidate.1 > current.1 || (candidate.1 == current.1 && candidate.0 < current.0)
                {
                    *current = candidate.clone();
                }
            })
            .or_insert(candidate);
    }

    let test_total = rows.iter().map(|row| row.2).sum::<u64>();
    let correct = rows
        .iter()
        .filter(|(key, _, _)| {
            winners
                .get(&(key.source.clone(), key.edge_type.clone()))
                .is_some_and(|winner| winner.0 == key.target)
        })
        .map(|row| row.2)
        .sum::<u64>();
    let mut predictions = winners
        .into_iter()
        .map(|((source, edge_type), (target, _))| (source, edge_type, target))
        .collect::<Vec<_>>();
    predictions.sort_unstable();

    NextActivityResult {
        accuracy: ratio(correct, test_total),
        correct,
        test_total,
        model_size: predictions.len(),
        predictions,
    }
}

/// Frequency-coverage conformance for complete variants.
pub fn variant_frequency_conformance(
    rows: impl IntoIterator<Item = (String, u64, u64)>,
    coverage: f64,
) -> Result<VariantConformanceResult, AnalyticsError> {
    validate_coverage(coverage)?;
    let mut rows: Vec<_> = rows.into_iter().collect();
    rows.sort_unstable_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let train_total = rows.iter().map(|row| row.1).sum::<u64>();
    let target = (train_total as f64 * coverage).ceil() as u64;
    let mut covered = 0_u64;
    let mut model = Vec::new();
    for (variant, train_count, _) in &rows {
        if covered >= target {
            break;
        }
        if *train_count > 0 {
            covered = covered.saturating_add(*train_count);
            model.push(variant.clone());
        }
    }
    let accepted: HashSet<_> = model.iter().collect();
    let test_total = rows.iter().map(|row| row.2).sum::<u64>();
    let conforming = rows
        .iter()
        .filter(|row| accepted.contains(&row.0))
        .map(|row| row.2)
        .sum::<u64>();
    let deviations = test_total.saturating_sub(conforming);
    model.sort_unstable();
    Ok(VariantConformanceResult {
        fitness: ratio(conforming, test_total),
        conforming,
        deviations,
        test_total,
        model_size: model.len(),
        model,
    })
}

/// Return input indexes ordered as a stable bottleneck ranking.
pub fn rank_bottlenecks(
    frequencies: &[u64],
    mean_durations: &[f64],
) -> Result<Vec<usize>, AnalyticsError> {
    if frequencies.len() != mean_durations.len() {
        return Err(AnalyticsError::ColumnLengthMismatch);
    }
    let mut indexes = (0..frequencies.len()).collect::<Vec<_>>();
    indexes.sort_unstable_by(|&left, &right| {
        mean_durations[right]
            .partial_cmp(&mean_durations[left])
            .unwrap_or(Ordering::Equal)
            .then_with(|| frequencies[right].cmp(&frequencies[left]))
            .then_with(|| left.cmp(&right))
    });
    Ok(indexes)
}

/// Compare two labeled frequency distributions with Jensen-Shannon divergence.
///
/// Duplicate labels are aggregated before scoring. The divergence uses base-2
/// logarithms and is therefore bounded by `[0, 1]`. If exactly one window has
/// observations, the change is maximal and the nonempty distribution provides
/// the contributor shares. This explicit empty-window policy avoids smoothing
/// parameters that would make results depend on the label vocabulary.
pub fn frequency_drift(
    rows: impl IntoIterator<Item = (String, u64, u64)>,
    top_n: usize,
) -> Result<FrequencyDriftResult, AnalyticsError> {
    let mut aggregated: HashMap<String, (u64, u64)> = HashMap::new();
    for (label, baseline, current) in rows {
        let counts = aggregated.entry(label).or_default();
        counts.0 = counts
            .0
            .checked_add(baseline)
            .ok_or(AnalyticsError::CountOverflow)?;
        counts.1 = counts
            .1
            .checked_add(current)
            .ok_or(AnalyticsError::CountOverflow)?;
    }

    let baseline_total = aggregated.values().try_fold(0_u64, |total, counts| {
        total
            .checked_add(counts.0)
            .ok_or(AnalyticsError::CountOverflow)
    })?;
    let current_total = aggregated.values().try_fold(0_u64, |total, counts| {
        total
            .checked_add(counts.1)
            .ok_or(AnalyticsError::CountOverflow)
    })?;

    let mut contributors = Vec::with_capacity(aggregated.len());
    let mut divergence = 0.0;
    for (label, (baseline, current)) in aggregated {
        let baseline_share = if baseline_total == 0 {
            0.0
        } else {
            baseline as f64 / baseline_total as f64
        };
        let current_share = if current_total == 0 {
            0.0
        } else {
            current as f64 / current_total as f64
        };
        let js_contribution = if baseline_total == 0 && current_total == 0 {
            0.0
        } else if baseline_total == 0 {
            current_share
        } else if current_total == 0 {
            baseline_share
        } else {
            let midpoint = (baseline_share + current_share) * 0.5;
            let baseline_term = if baseline_share == 0.0 {
                0.0
            } else {
                0.5 * baseline_share * (baseline_share / midpoint).log2()
            };
            let current_term = if current_share == 0.0 {
                0.0
            } else {
                0.5 * current_share * (current_share / midpoint).log2()
            };
            baseline_term + current_term
        };
        divergence += js_contribution;
        contributors.push(DriftContributor {
            label,
            baseline_share,
            current_share,
            share_delta: current_share - baseline_share,
            js_contribution,
        });
    }

    contributors.sort_unstable_by(|left, right| {
        right
            .js_contribution
            .total_cmp(&left.js_contribution)
            .then_with(|| left.label.cmp(&right.label))
    });
    contributors.truncate(top_n);

    Ok(FrequencyDriftResult {
        divergence: divergence.clamp(0.0, 1.0),
        baseline_total,
        current_total,
        contributors,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(source: &str, target: &str) -> TransitionKey {
        TransitionKey {
            source: source.into(),
            target: target.into(),
            edge_type: "Order".into(),
        }
    }

    #[test]
    fn dfg_model_is_minimal_and_scores_test_counts() {
        let result = dfg_frequency_conformance(
            vec![
                (key("A", "B"), 80, 8),
                (key("B", "C"), 15, 1),
                (key("B", "D"), 5, 1),
            ],
            0.95,
        )
        .unwrap();
        assert_eq!(result.model_size, 2);
        assert_eq!(result.conforming, 9);
        assert_eq!(result.deviations, 1);
        assert_eq!(result.fitness, 0.9);
    }

    #[test]
    fn next_activity_ties_are_lexical() {
        let result = next_activity_prediction(vec![(key("A", "C"), 10, 3), (key("A", "B"), 10, 7)]);
        assert_eq!(result.correct, 7);
        assert_eq!(result.predictions[0].2, "B");
    }

    #[test]
    fn next_activity_ignores_test_only_transitions_during_training() {
        let result = next_activity_prediction(vec![(key("A", "B"), 10, 0), (key("Z", "Y"), 0, 5)]);
        assert_eq!(result.test_total, 5);
        assert_eq!(result.correct, 0);
        assert_eq!(
            result.predictions,
            vec![("A".into(), "Order".into(), "B".into())]
        );
    }

    #[test]
    fn variant_model_handles_an_empty_test_partition() {
        let result = variant_frequency_conformance(vec![("v1".into(), 1, 0)], 1.0).unwrap();
        assert_eq!(result.fitness, 1.0);
        assert_eq!(result.test_total, 0);
    }

    #[test]
    fn ranking_is_stable() {
        assert_eq!(
            rank_bottlenecks(&[10, 20, 30], &[2.0, 2.0, 1.0]).unwrap(),
            vec![1, 0, 2]
        );
    }

    #[test]
    fn drift_is_bounded_explainable_and_aggregates_duplicate_labels() {
        let result = frequency_drift(
            vec![("A".into(), 5, 0), ("A".into(), 5, 0), ("B".into(), 0, 10)],
            2,
        )
        .unwrap();
        assert_eq!(result.baseline_total, 10);
        assert_eq!(result.current_total, 10);
        assert_eq!(result.divergence, 1.0);
        assert_eq!(result.contributors[0].label, "A");
        assert_eq!(result.contributors[0].js_contribution, 0.5);
        assert_eq!(result.contributors[1].label, "B");
    }

    #[test]
    fn identical_and_empty_distributions_have_no_drift() {
        let identical = frequency_drift(vec![("A".into(), 7, 7), ("B".into(), 3, 3)], 10).unwrap();
        assert_eq!(identical.divergence, 0.0);

        let empty = frequency_drift(Vec::<(String, u64, u64)>::new(), 10).unwrap();
        assert_eq!(empty.divergence, 0.0);
        assert!(empty.contributors.is_empty());
    }
}
