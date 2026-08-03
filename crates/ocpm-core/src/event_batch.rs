//! Factorized pg_ocpm 0.9 event batches and allocation-bounded summaries.
//!
//! A batch stores one activity path, a packed case-id vector, and one packed
//! timestamp vector per case. Algorithms operate on that representation
//! directly instead of expanding one middleware object per event.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ByteOrder {
    Little,
    Big,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum EventBatchError {
    #[error("activity_count must be positive")]
    EmptyActivityPath,
    #[error("case_count must be positive")]
    EmptyCaseBatch,
    #[error("activity_path length does not match activity_count")]
    ActivityCountMismatch,
    #[error("event batch dimensions overflow the addressable platform size")]
    DimensionOverflow,
    #[error("case-id payload length does not match case_count")]
    InvalidCaseIdPayload,
    #[error("timestamp payload length does not match batch dimensions")]
    InvalidTimestampPayload,
    #[error("timestamp payload byte order is ambiguous or invalid")]
    InvalidByteOrder,
    #[error("timestamp vector header does not match activity_count")]
    InvalidTimestampHeader,
    #[error("event batch count overflowed an unsigned 64-bit total")]
    CountOverflow,
    #[error("event batch duration total overflowed a signed 128-bit accumulator")]
    DurationOverflow,
}

/// One owned, still-factorized pg_ocpm event batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventBatch {
    activity_path: Vec<String>,
    case_count: usize,
    case_id_payload: Vec<u8>,
    timestamp_payload: Vec<u8>,
    byte_order: ByteOrder,
}

impl EventBatch {
    /// Validate and retain a compact batch without expanding its cases or events.
    pub fn decode(
        activity_path: Vec<String>,
        activity_count: i32,
        case_count: i32,
        case_id_payload: Vec<u8>,
        timestamp_payload: Vec<u8>,
    ) -> Result<Self, EventBatchError> {
        let activity_count = usize::try_from(activity_count)
            .ok()
            .filter(|count| *count > 0)
            .ok_or(EventBatchError::EmptyActivityPath)?;
        let case_count = usize::try_from(case_count)
            .ok()
            .filter(|count| *count > 0)
            .ok_or(EventBatchError::EmptyCaseBatch)?;
        if activity_path.len() != activity_count {
            return Err(EventBatchError::ActivityCountMismatch);
        }

        let expected_case_bytes = case_count
            .checked_mul(size_of::<i64>())
            .ok_or(EventBatchError::DimensionOverflow)?;
        if case_id_payload.len() != expected_case_bytes {
            return Err(EventBatchError::InvalidCaseIdPayload);
        }
        let timestamp_record_bytes = size_of::<i32>()
            .checked_add(
                activity_count
                    .checked_mul(size_of::<i64>())
                    .ok_or(EventBatchError::DimensionOverflow)?,
            )
            .ok_or(EventBatchError::DimensionOverflow)?;
        let expected_timestamp_bytes = case_count
            .checked_mul(timestamp_record_bytes)
            .ok_or(EventBatchError::DimensionOverflow)?;
        if timestamp_payload.len() != expected_timestamp_bytes {
            return Err(EventBatchError::InvalidTimestampPayload);
        }

        let header: [u8; 4] = timestamp_payload[..4]
            .try_into()
            .map_err(|_| EventBatchError::InvalidTimestampPayload)?;
        let expected_header =
            i32::try_from(activity_count).map_err(|_| EventBatchError::DimensionOverflow)?;
        let little_matches = i32::from_le_bytes(header) == expected_header;
        let big_matches = i32::from_be_bytes(header) == expected_header;
        let byte_order = match (little_matches, big_matches) {
            (true, false) => ByteOrder::Little,
            (false, true) => ByteOrder::Big,
            _ => return Err(EventBatchError::InvalidByteOrder),
        };

        for case_index in 0..case_count {
            let offset = case_index
                .checked_mul(timestamp_record_bytes)
                .ok_or(EventBatchError::DimensionOverflow)?;
            let header: [u8; 4] = timestamp_payload[offset..offset + 4]
                .try_into()
                .map_err(|_| EventBatchError::InvalidTimestampPayload)?;
            if read_i32(header, byte_order) != expected_header {
                return Err(EventBatchError::InvalidTimestampHeader);
            }
        }

        Ok(Self {
            activity_path,
            case_count,
            case_id_payload,
            timestamp_payload,
            byte_order,
        })
    }

    pub fn activity_path(&self) -> &[String] {
        &self.activity_path
    }

    pub fn activity_count(&self) -> usize {
        self.activity_path.len()
    }

    pub fn case_count(&self) -> usize {
        self.case_count
    }

    pub fn event_count(&self) -> usize {
        self.case_count * self.activity_count()
    }

    pub fn payload_bytes(&self) -> usize {
        self.case_id_payload.len() + self.timestamp_payload.len()
    }

    pub fn case(&self, index: usize) -> Option<EventCase<'_>> {
        if index >= self.case_count {
            return None;
        }
        let case_offset = index * size_of::<i64>();
        let timestamp_record_bytes = size_of::<i32>() + self.activity_count() * size_of::<i64>();
        let timestamp_offset = index * timestamp_record_bytes + size_of::<i32>();
        Some(EventCase {
            case_id: read_i64(
                self.case_id_payload[case_offset..case_offset + size_of::<i64>()]
                    .try_into()
                    .expect("validated case-id payload width"),
                self.byte_order,
            ),
            timestamps: &self.timestamp_payload
                [timestamp_offset..timestamp_offset + self.activity_count() * size_of::<i64>()],
            activity_count: self.activity_count(),
            byte_order: self.byte_order,
        })
    }

    pub fn cases(&self) -> EventCases<'_> {
        EventCases {
            batch: self,
            index: 0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct EventCase<'a> {
    pub case_id: i64,
    timestamps: &'a [u8],
    activity_count: usize,
    byte_order: ByteOrder,
}

impl EventCase<'_> {
    /// PostgreSQL timestamp microseconds since 2000-01-01 UTC.
    pub fn timestamp_micros(&self, activity_index: usize) -> Option<i64> {
        if activity_index >= self.activity_count {
            return None;
        }
        let offset = activity_index * size_of::<i64>();
        Some(read_i64(
            self.timestamps[offset..offset + size_of::<i64>()]
                .try_into()
                .expect("validated timestamp payload width"),
            self.byte_order,
        ))
    }
}

pub struct EventCases<'a> {
    batch: &'a EventBatch,
    index: usize,
}

impl<'a> Iterator for EventCases<'a> {
    type Item = EventCase<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.batch.case(self.index);
        self.index += usize::from(result.is_some());
        result
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.batch.case_count.saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for EventCases<'_> {}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventVariantCount {
    pub activity_path: Vec<String>,
    pub frequency: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventDfgEdge {
    pub source: String,
    pub target: String,
    pub frequency: u64,
    pub mean_duration_seconds: f64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventActivityCount {
    pub activity: String,
    pub case_frequency: u64,
    pub occurrence_frequency: u64,
    pub start_frequency: u64,
    pub end_frequency: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventLogSummary {
    pub case_count: u64,
    pub event_count: u64,
    pub payload_bytes: u64,
    pub variants: Vec<EventVariantCount>,
    pub dfg: Vec<EventDfgEdge>,
    pub activities: Vec<EventActivityCount>,
}

#[derive(Default)]
pub struct EventSummaryBuilder {
    case_count: u64,
    event_count: u64,
    payload_bytes: u64,
    variants: BTreeMap<Vec<String>, u64>,
    dfg: BTreeMap<(String, String), (u64, i128)>,
    activities: BTreeMap<String, ActivityAccumulator>,
}

#[derive(Default)]
struct ActivityAccumulator {
    case_frequency: u64,
    occurrence_frequency: u64,
    start_frequency: u64,
    end_frequency: u64,
}

impl EventSummaryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one factorized batch. Memory grows with distinct paths, activities,
    /// and DFG edges, not with the number of events.
    pub fn push_batch(&mut self, batch: &EventBatch) -> Result<(), EventBatchError> {
        let cases =
            u64::try_from(batch.case_count()).map_err(|_| EventBatchError::CountOverflow)?;
        let events =
            u64::try_from(batch.event_count()).map_err(|_| EventBatchError::CountOverflow)?;
        let payload_bytes =
            u64::try_from(batch.payload_bytes()).map_err(|_| EventBatchError::CountOverflow)?;
        checked_add(&mut self.case_count, cases)?;
        checked_add(&mut self.event_count, events)?;
        checked_add(&mut self.payload_bytes, payload_bytes)?;
        checked_map_add(&mut self.variants, batch.activity_path().to_vec(), cases)?;

        let mut seen = BTreeSet::new();
        for activity in batch.activity_path() {
            let accumulator = self.activities.entry(activity.clone()).or_default();
            checked_add(&mut accumulator.occurrence_frequency, cases)?;
            if seen.insert(activity) {
                checked_add(&mut accumulator.case_frequency, cases)?;
            }
        }
        if let Some(first) = batch.activity_path().first() {
            checked_add(
                &mut self
                    .activities
                    .entry(first.clone())
                    .or_default()
                    .start_frequency,
                cases,
            )?;
        }
        if let Some(last) = batch.activity_path().last() {
            checked_add(
                &mut self
                    .activities
                    .entry(last.clone())
                    .or_default()
                    .end_frequency,
                cases,
            )?;
        }

        for (edge_index, pair) in batch.activity_path().windows(2).enumerate() {
            let key = (pair[0].clone(), pair[1].clone());
            let accumulator = self.dfg.entry(key).or_default();
            checked_add(&mut accumulator.0, cases)?;
            for case in batch.cases() {
                let source = case
                    .timestamp_micros(edge_index)
                    .expect("validated event batch source timestamp");
                let target = case
                    .timestamp_micros(edge_index + 1)
                    .expect("validated event batch target timestamp");
                checked_duration_add(&mut accumulator.1, i128::from(target) - i128::from(source))?;
            }
        }
        Ok(())
    }

    /// Add one expanded compatibility case from pg_ocpm 0.8's row stream.
    /// Only the current case needs to be buffered by the caller.
    pub fn push_case(
        &mut self,
        activity_path: &[String],
        timestamp_micros: &[i64],
    ) -> Result<(), EventBatchError> {
        if activity_path.is_empty() {
            return Err(EventBatchError::EmptyActivityPath);
        }
        if activity_path.len() != timestamp_micros.len() {
            return Err(EventBatchError::ActivityCountMismatch);
        }
        checked_add(&mut self.case_count, 1)?;
        checked_add(
            &mut self.event_count,
            u64::try_from(activity_path.len()).map_err(|_| EventBatchError::CountOverflow)?,
        )?;
        checked_map_add(&mut self.variants, activity_path.to_vec(), 1)?;

        let mut seen = BTreeSet::new();
        for activity in activity_path {
            let accumulator = self.activities.entry(activity.clone()).or_default();
            checked_add(&mut accumulator.occurrence_frequency, 1)?;
            if seen.insert(activity) {
                checked_add(&mut accumulator.case_frequency, 1)?;
            }
        }
        checked_add(
            &mut self
                .activities
                .entry(activity_path[0].clone())
                .or_default()
                .start_frequency,
            1,
        )?;
        checked_add(
            &mut self
                .activities
                .entry(activity_path[activity_path.len() - 1].clone())
                .or_default()
                .end_frequency,
            1,
        )?;
        for (edge_index, pair) in activity_path.windows(2).enumerate() {
            let accumulator = self
                .dfg
                .entry((pair[0].clone(), pair[1].clone()))
                .or_default();
            checked_add(&mut accumulator.0, 1)?;
            checked_duration_add(
                &mut accumulator.1,
                i128::from(timestamp_micros[edge_index + 1])
                    - i128::from(timestamp_micros[edge_index]),
            )?;
        }
        Ok(())
    }

    pub fn finish(self) -> EventLogSummary {
        EventLogSummary {
            case_count: self.case_count,
            event_count: self.event_count,
            payload_bytes: self.payload_bytes,
            variants: self
                .variants
                .into_iter()
                .map(|(activity_path, frequency)| EventVariantCount {
                    activity_path,
                    frequency,
                })
                .collect(),
            dfg: self
                .dfg
                .into_iter()
                .map(
                    |((source, target), (frequency, duration_micros))| EventDfgEdge {
                        source,
                        target,
                        frequency,
                        mean_duration_seconds: duration_micros as f64
                            / frequency as f64
                            / 1_000_000.0,
                    },
                )
                .collect(),
            activities: self
                .activities
                .into_iter()
                .map(|(activity, counts)| EventActivityCount {
                    activity,
                    case_frequency: counts.case_frequency,
                    occurrence_frequency: counts.occurrence_frequency,
                    start_frequency: counts.start_frequency,
                    end_frequency: counts.end_frequency,
                })
                .collect(),
        }
    }
}

pub fn summarize_event_batches(
    batches: impl IntoIterator<Item = EventBatch>,
) -> Result<EventLogSummary, EventBatchError> {
    let mut builder = EventSummaryBuilder::new();
    for batch in batches {
        builder.push_batch(&batch)?;
    }
    Ok(builder.finish())
}

fn checked_add(target: &mut u64, value: u64) -> Result<(), EventBatchError> {
    *target = target
        .checked_add(value)
        .ok_or(EventBatchError::CountOverflow)?;
    Ok(())
}

fn checked_duration_add(target: &mut i128, value: i128) -> Result<(), EventBatchError> {
    *target = target
        .checked_add(value)
        .ok_or(EventBatchError::DurationOverflow)?;
    Ok(())
}

fn checked_map_add<K: Ord>(
    target: &mut BTreeMap<K, u64>,
    key: K,
    value: u64,
) -> Result<(), EventBatchError> {
    checked_add(target.entry(key).or_default(), value)
}

fn read_i32(bytes: [u8; 4], byte_order: ByteOrder) -> i32 {
    match byte_order {
        ByteOrder::Little => i32::from_le_bytes(bytes),
        ByteOrder::Big => i32::from_be_bytes(bytes),
    }
}

fn read_i64(bytes: [u8; 8], byte_order: ByteOrder) -> i64 {
    match byte_order {
        ByteOrder::Little => i64::from_le_bytes(bytes),
        ByteOrder::Big => i64::from_be_bytes(bytes),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(order: ByteOrder, cases: &[(i64, &[i64])]) -> (Vec<u8>, Vec<u8>) {
        let mut ids = Vec::new();
        let mut timestamps = Vec::new();
        for (case_id, values) in cases {
            ids.extend(match order {
                ByteOrder::Little => case_id.to_le_bytes(),
                ByteOrder::Big => case_id.to_be_bytes(),
            });
            let count = i32::try_from(values.len()).unwrap();
            timestamps.extend(match order {
                ByteOrder::Little => count.to_le_bytes(),
                ByteOrder::Big => count.to_be_bytes(),
            });
            for value in *values {
                timestamps.extend(match order {
                    ByteOrder::Little => value.to_le_bytes(),
                    ByteOrder::Big => value.to_be_bytes(),
                });
            }
        }
        (ids, timestamps)
    }

    #[test]
    fn decodes_both_server_byte_orders_without_expanding_rows() {
        for order in [ByteOrder::Little, ByteOrder::Big] {
            let (ids, timestamps) = payload(order, &[(7, &[10, 20]), (9, &[30, 50])]);
            let batch = EventBatch::decode(
                vec!["Create".into(), "Complete".into()],
                2,
                2,
                ids,
                timestamps,
            )
            .unwrap();

            assert_eq!(batch.event_count(), 4);
            assert_eq!(batch.payload_bytes(), 56);
            assert_eq!(
                batch
                    .cases()
                    .map(|case| (case.case_id, case.timestamp_micros(1).unwrap()))
                    .collect::<Vec<_>>(),
                vec![(7, 20), (9, 50)]
            );
        }
    }

    #[test]
    fn rejects_corrupt_dimensions_headers_and_payloads() {
        let (ids, timestamps) = payload(ByteOrder::Little, &[(7, &[10, 20])]);
        assert_eq!(
            EventBatch::decode(vec!["A".into()], 2, 1, ids.clone(), timestamps.clone()),
            Err(EventBatchError::ActivityCountMismatch)
        );
        assert_eq!(
            EventBatch::decode(
                vec!["A".into(), "B".into()],
                2,
                2,
                ids.clone(),
                timestamps.clone()
            ),
            Err(EventBatchError::InvalidCaseIdPayload)
        );
        let mut corrupt = timestamps;
        corrupt[0] = 3;
        assert_eq!(
            EventBatch::decode(vec!["A".into(), "B".into()], 2, 1, ids, corrupt),
            Err(EventBatchError::InvalidByteOrder)
        );
    }

    #[test]
    fn summarizes_variants_edges_activities_and_durations_factorized() {
        let (ids, timestamps) = payload(
            ByteOrder::Little,
            &[
                (7, &[0, 2_000_000, 5_000_000]),
                (9, &[0, 4_000_000, 6_000_000]),
            ],
        );
        let batch = EventBatch::decode(
            vec!["A".into(), "B".into(), "A".into()],
            3,
            2,
            ids,
            timestamps,
        )
        .unwrap();
        let summary = summarize_event_batches([batch]).unwrap();

        assert_eq!((summary.case_count, summary.event_count), (2, 6));
        assert_eq!(summary.variants[0].frequency, 2);
        assert_eq!(summary.activities[0].activity, "A");
        assert_eq!(summary.activities[0].case_frequency, 2);
        assert_eq!(summary.activities[0].occurrence_frequency, 4);
        assert_eq!(summary.activities[0].start_frequency, 2);
        assert_eq!(summary.activities[0].end_frequency, 2);
        assert_eq!(summary.dfg[0].frequency, 2);
        assert_eq!(summary.dfg[0].mean_duration_seconds, 3.0);
        assert_eq!(summary.dfg[1].mean_duration_seconds, 2.5);
    }

    #[test]
    fn row_fallback_produces_the_same_summary() {
        let (ids, timestamps) = payload(
            ByteOrder::Little,
            &[(7, &[0, 2_000_000]), (9, &[0, 4_000_000])],
        );
        let batch =
            EventBatch::decode(vec!["A".into(), "B".into()], 2, 2, ids, timestamps).unwrap();
        let expected = summarize_event_batches([batch]).unwrap();
        let mut builder = EventSummaryBuilder::new();
        builder
            .push_case(&["A".into(), "B".into()], &[0, 2_000_000])
            .unwrap();
        builder
            .push_case(&["A".into(), "B".into()], &[0, 4_000_000])
            .unwrap();
        let actual = builder.finish();

        assert_eq!(actual.case_count, expected.case_count);
        assert_eq!(actual.event_count, expected.event_count);
        assert_eq!(actual.variants, expected.variants);
        assert_eq!(actual.dfg, expected.dfg);
        assert_eq!(actual.activities, expected.activities);
        assert_eq!(actual.payload_bytes, 0);
    }

    #[test]
    fn duration_accumulation_fails_closed_on_overflow() {
        let mut total = i128::MAX;
        assert_eq!(
            checked_duration_add(&mut total, 1),
            Err(EventBatchError::DurationOverflow)
        );
    }
}
