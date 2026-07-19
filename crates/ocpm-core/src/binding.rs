//! Zero-copy iteration over pg_ocpm binding-result capsules.

use std::str;

use thiserror::Error;

const MAGIC: &[u8; 4] = b"OCPB";
const VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum BindingSchema {
    IdViolation = 1,
    IdIdViolation = 2,
    IdLabelIdViolation = 3,
    Value = 4,
    FiveIds = 5,
    PairGroups = 6,
    ThreeIdsViolation = 7,
    TwoIds = 8,
    ThreeIds = 9,
    FourIds = 10,
    FiveIdsViolation = 11,
}

impl TryFrom<u8> for BindingSchema {
    type Error = BindingDecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::IdViolation),
            2 => Ok(Self::IdIdViolation),
            3 => Ok(Self::IdLabelIdViolation),
            4 => Ok(Self::Value),
            5 => Ok(Self::FiveIds),
            6 => Ok(Self::PairGroups),
            7 => Ok(Self::ThreeIdsViolation),
            8 => Ok(Self::TwoIds),
            9 => Ok(Self::ThreeIds),
            10 => Ok(Self::FourIds),
            11 => Ok(Self::FiveIdsViolation),
            other => Err(BindingDecodeError::UnknownSchema(other)),
        }
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum BindingDecodeError {
    #[error("binding capsule is truncated")]
    Truncated,
    #[error("binding capsule has invalid magic")]
    BadMagic,
    #[error("binding capsule version {0} is unsupported")]
    UnsupportedVersion(u8),
    #[error("binding capsule schema {0} is unknown")]
    UnknownSchema(u8),
    #[error("binding capsule contains an overflowing varint")]
    VarintOverflow,
    #[error("binding capsule count exceeds the addressable platform size")]
    CountOverflow,
    #[error("binding capsule allocation could not be reserved safely")]
    AllocationFailed,
    #[error("binding capsule identifier delta overflows i64")]
    DeltaOverflow,
    #[error("binding capsule contains invalid UTF-8")]
    InvalidUtf8,
    #[error("binding capsule label dictionary is invalid")]
    InvalidLabelDictionary,
    #[error("binding capsule pair groups are invalid")]
    InvalidPairGroups,
    #[error("binding capsule has trailing bytes")]
    TrailingBytes,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BindingRow<'a> {
    ids: [i64; 5],
    id_count: u8,
    pub label: Option<&'a str>,
    pub violated: Option<bool>,
    pub value: Option<f64>,
}

impl BindingRow<'_> {
    pub fn ids(&self) -> &[i64] {
        &self.ids[..usize::from(self.id_count)]
    }
}

#[derive(Clone, Debug, PartialEq)]
struct MaterializedRows {
    id_columns: Vec<Vec<i64>>,
    label_dictionary: Vec<String>,
    label_indexes: Option<Vec<usize>>,
    violations: Option<Vec<bool>>,
    values: Option<Vec<f64>>,
}

#[derive(Clone, Debug, PartialEq)]
struct PairGroups {
    sources: Vec<i64>,
    sizes: Vec<usize>,
    targets: Vec<i64>,
    events: Vec<i64>,
}

#[derive(Clone, Debug, PartialEq)]
enum BindingStorage {
    Materialized(MaterializedRows),
    PairGroups(PairGroups),
}

#[derive(Clone, Debug, PartialEq)]
pub struct BindingCapsule {
    schema: BindingSchema,
    row_count: usize,
    storage: BindingStorage,
}

impl BindingCapsule {
    pub fn decode(bytes: &[u8]) -> Result<Self, BindingDecodeError> {
        let mut cursor = Cursor::new(bytes);
        if cursor.take(4)? != MAGIC {
            return Err(BindingDecodeError::BadMagic);
        }
        let version = cursor.byte()?;
        if version != VERSION {
            return Err(BindingDecodeError::UnsupportedVersion(version));
        }
        let schema = BindingSchema::try_from(cursor.byte()?)?;
        let row_count = cursor.usize_varint()?;
        let storage = if schema == BindingSchema::PairGroups {
            BindingStorage::PairGroups(decode_pair_groups(&mut cursor, row_count)?)
        } else {
            BindingStorage::Materialized(decode_materialized(&mut cursor, schema, row_count)?)
        };
        if !cursor.is_empty() {
            return Err(BindingDecodeError::TrailingBytes);
        }
        Ok(Self {
            schema,
            row_count,
            storage,
        })
    }

    pub fn schema(&self) -> BindingSchema {
        self.schema
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn is_factorized(&self) -> bool {
        matches!(self.storage, BindingStorage::PairGroups(_))
    }

    pub fn rows(&self) -> BindingRows<'_> {
        BindingRows {
            capsule: self,
            row: 0,
            group: 0,
            left: 0,
            right: 0,
            entry_offset: 0,
        }
    }
}

pub struct BindingRows<'a> {
    capsule: &'a BindingCapsule,
    row: usize,
    group: usize,
    left: usize,
    right: usize,
    entry_offset: usize,
}

impl<'a> Iterator for BindingRows<'a> {
    type Item = BindingRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.capsule.storage {
            BindingStorage::Materialized(storage) => {
                if self.row >= self.capsule.row_count {
                    return None;
                }
                let mut ids = [0_i64; 5];
                for (target, column) in ids.iter_mut().zip(&storage.id_columns) {
                    *target = column[self.row];
                }
                let label = storage
                    .label_indexes
                    .as_ref()
                    .map(|indexes| storage.label_dictionary[indexes[self.row]].as_str());
                let violated = storage.violations.as_ref().map(|values| values[self.row]);
                let value = storage.values.as_ref().map(|values| values[self.row]);
                self.row += 1;
                Some(BindingRow {
                    ids,
                    id_count: storage.id_columns.len() as u8,
                    label,
                    violated,
                    value,
                })
            }
            BindingStorage::PairGroups(storage) => {
                if self.group >= storage.sources.len() {
                    return None;
                }
                let size = storage.sizes[self.group];
                let left = self.entry_offset + self.left;
                let right = self.entry_offset + self.right;
                let row = BindingRow {
                    ids: [
                        storage.sources[self.group],
                        storage.targets[left],
                        storage.targets[right],
                        storage.events[left],
                        storage.events[right],
                    ],
                    id_count: 5,
                    label: None,
                    violated: None,
                    value: None,
                };
                self.row += 1;
                self.right += 1;
                if self.right == size {
                    self.right = 0;
                    self.left += 1;
                    if self.left == size {
                        self.left = 0;
                        self.entry_offset += size;
                        self.group += 1;
                    }
                }
                Some(row)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.capsule.row_count.saturating_sub(self.row);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for BindingRows<'_> {}

fn decode_materialized(
    cursor: &mut Cursor<'_>,
    schema: BindingSchema,
    row_count: usize,
) -> Result<MaterializedRows, BindingDecodeError> {
    let (id_count, has_labels, has_violations, has_values) = match schema {
        BindingSchema::IdViolation => (1, false, true, false),
        BindingSchema::IdIdViolation => (2, false, true, false),
        BindingSchema::IdLabelIdViolation => (2, true, true, false),
        BindingSchema::Value => (0, false, false, true),
        BindingSchema::FiveIds => (5, false, false, false),
        BindingSchema::PairGroups => unreachable!(),
        BindingSchema::ThreeIdsViolation => (3, false, true, false),
        BindingSchema::TwoIds => (2, false, false, false),
        BindingSchema::ThreeIds => (3, false, false, false),
        BindingSchema::FourIds => (4, false, false, false),
        BindingSchema::FiveIdsViolation => (5, false, true, false),
    };
    let mut minimum_bytes = row_count
        .checked_mul(id_count)
        .ok_or(BindingDecodeError::CountOverflow)?;
    if has_labels {
        // A valid label payload has a dictionary-count varint and one
        // dictionary-index varint per row. Dictionary entries are checked as
        // they are decoded below.
        minimum_bytes = minimum_bytes
            .checked_add(1)
            .and_then(|value| value.checked_add(row_count))
            .ok_or(BindingDecodeError::CountOverflow)?;
    }
    if has_violations {
        minimum_bytes = minimum_bytes
            .checked_add(row_count / 8 + usize::from(row_count % 8 != 0))
            .ok_or(BindingDecodeError::CountOverflow)?;
    }
    if has_values {
        // Every encoded f64 bit pattern occupies at least one varint byte.
        minimum_bytes = minimum_bytes
            .checked_add(row_count)
            .ok_or(BindingDecodeError::CountOverflow)?;
    }
    cursor.require_minimum(minimum_bytes)?;

    let mut id_columns = reserved_vec(id_count)?;
    for _ in 0..id_count {
        id_columns.push(cursor.delta_ids(row_count)?);
    }
    let (label_dictionary, label_indexes) = if has_labels {
        let dictionary_count = cursor.usize_varint()?;
        if dictionary_count > row_count || (row_count != 0 && dictionary_count == 0) {
            return Err(BindingDecodeError::InvalidLabelDictionary);
        }
        let mut dictionary_minimum = dictionary_count
            .checked_add(row_count)
            .ok_or(BindingDecodeError::CountOverflow)?;
        if has_violations {
            dictionary_minimum = dictionary_minimum
                .checked_add(row_count / 8 + usize::from(row_count % 8 != 0))
                .ok_or(BindingDecodeError::CountOverflow)?;
        }
        if has_values {
            dictionary_minimum = dictionary_minimum
                .checked_add(row_count)
                .ok_or(BindingDecodeError::CountOverflow)?;
        }
        cursor.require_minimum(dictionary_minimum)?;
        let mut dictionary = reserved_vec(dictionary_count)?;
        for _ in 0..dictionary_count {
            let length = cursor.usize_varint()?;
            let value = str::from_utf8(cursor.take(length)?)
                .map_err(|_| BindingDecodeError::InvalidUtf8)?;
            let mut owned = String::new();
            owned
                .try_reserve_exact(value.len())
                .map_err(|_| BindingDecodeError::AllocationFailed)?;
            owned.push_str(value);
            dictionary.push(owned);
        }
        let mut indexes = reserved_vec(row_count)?;
        for _ in 0..row_count {
            let index = cursor.usize_varint()?;
            if index >= dictionary_count {
                return Err(BindingDecodeError::InvalidLabelDictionary);
            }
            indexes.push(index);
        }
        (dictionary, Some(indexes))
    } else {
        (Vec::new(), None)
    };
    let violations = if has_violations {
        let packed_count = row_count / 8 + usize::from(row_count % 8 != 0);
        let packed = cursor.take(packed_count)?;
        let mut values = reserved_vec(row_count)?;
        for index in 0..row_count {
            values.push(packed[index / 8] & (1 << (index % 8)) != 0);
        }
        Some(values)
    } else {
        None
    };
    let values = if has_values {
        let mut values = reserved_vec(row_count)?;
        for _ in 0..row_count {
            values.push(f64::from_bits(cursor.varint()?));
        }
        Some(values)
    } else {
        None
    };
    Ok(MaterializedRows {
        id_columns,
        label_dictionary,
        label_indexes,
        violations,
        values,
    })
}

fn decode_pair_groups(
    cursor: &mut Cursor<'_>,
    row_count: usize,
) -> Result<PairGroups, BindingDecodeError> {
    let group_count = cursor.usize_varint()?;
    cursor.require_minimum(
        group_count
            .checked_mul(2)
            .ok_or(BindingDecodeError::CountOverflow)?,
    )?;
    let sources = cursor.delta_ids(group_count)?;
    let mut sizes = reserved_vec(group_count)?;
    let mut entry_count = 0_usize;
    let mut expanded_count = 0_usize;
    for _ in 0..group_count {
        let size = cursor.usize_varint()?;
        if size == 0 {
            return Err(BindingDecodeError::InvalidPairGroups);
        }
        entry_count = entry_count
            .checked_add(size)
            .ok_or(BindingDecodeError::CountOverflow)?;
        expanded_count = expanded_count
            .checked_add(
                size.checked_mul(size)
                    .ok_or(BindingDecodeError::CountOverflow)?,
            )
            .ok_or(BindingDecodeError::CountOverflow)?;
        sizes.push(size);
    }
    if expanded_count != row_count {
        return Err(BindingDecodeError::InvalidPairGroups);
    }
    cursor.require_minimum(
        entry_count
            .checked_mul(2)
            .ok_or(BindingDecodeError::CountOverflow)?,
    )?;
    let targets = cursor.delta_ids(entry_count)?;
    let events = cursor.delta_ids(entry_count)?;
    Ok(PairGroups {
        sources,
        sizes,
        targets,
        events,
    })
}

fn reserved_vec<T>(count: usize) -> Result<Vec<T>, BindingDecodeError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| BindingDecodeError::AllocationFailed)?;
    Ok(values)
}

struct Cursor<'a> {
    remaining: &'a [u8],
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], BindingDecodeError> {
        if count > self.remaining.len() {
            return Err(BindingDecodeError::Truncated);
        }
        let (value, rest) = self.remaining.split_at(count);
        self.remaining = rest;
        Ok(value)
    }

    fn require_minimum(&self, count: usize) -> Result<(), BindingDecodeError> {
        if count > self.remaining.len() {
            return Err(BindingDecodeError::Truncated);
        }
        Ok(())
    }

    fn byte(&mut self) -> Result<u8, BindingDecodeError> {
        Ok(self.take(1)?[0])
    }

    fn varint(&mut self) -> Result<u64, BindingDecodeError> {
        let mut value = 0_u64;
        for shift in (0..70).step_by(7) {
            let byte = self.byte()?;
            if shift == 63 && byte & 0xfe != 0 {
                return Err(BindingDecodeError::VarintOverflow);
            }
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(BindingDecodeError::VarintOverflow)
    }

    fn usize_varint(&mut self) -> Result<usize, BindingDecodeError> {
        usize::try_from(self.varint()?).map_err(|_| BindingDecodeError::CountOverflow)
    }

    fn signed_varint(&mut self) -> Result<i64, BindingDecodeError> {
        let value = self.varint()?;
        Ok(((value >> 1) as i64) ^ -((value & 1) as i64))
    }

    fn delta_ids(&mut self, count: usize) -> Result<Vec<i64>, BindingDecodeError> {
        self.require_minimum(count)?;
        let mut values = reserved_vec(count)?;
        let mut previous = 0_i64;
        for _ in 0..count {
            previous = previous
                .checked_add(self.signed_varint()?)
                .ok_or(BindingDecodeError::DeltaOverflow)?;
            values.push(previous);
        }
        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn varint(mut value: u64, output: &mut Vec<u8>) {
        while value >= 0x80 {
            output.push((value as u8 & 0x7f) | 0x80);
            value >>= 7;
        }
        output.push(value as u8);
    }

    fn signed(value: i64, output: &mut Vec<u8>) {
        varint(((value as u64) << 1) ^ ((value >> 63) as u64), output);
    }

    fn header(schema: BindingSchema, rows: usize) -> Vec<u8> {
        let mut output = b"OCPB".to_vec();
        output.extend([VERSION, schema as u8]);
        varint(rows as u64, &mut output);
        output
    }

    fn materialized_ids(
        schema: BindingSchema,
        columns: &[&[i64]],
        violations: Option<&[bool]>,
    ) -> Vec<u8> {
        let row_count = columns.first().map_or_else(
            || violations.map_or(0, |values| values.len()),
            |column| column.len(),
        );
        assert!(columns.iter().all(|column| column.len() == row_count));
        assert!(violations.is_none_or(|values| values.len() == row_count));

        let mut bytes = header(schema, row_count);
        for column in columns {
            let mut previous = 0_i64;
            for &value in *column {
                signed(value.checked_sub(previous).unwrap(), &mut bytes);
                previous = value;
            }
        }
        if let Some(values) = violations {
            for chunk in values.chunks(8) {
                let mut packed = 0_u8;
                for (index, value) in chunk.iter().enumerate() {
                    packed |= u8::from(*value) << index;
                }
                bytes.push(packed);
            }
        }
        bytes
    }

    #[test]
    fn decodes_two_three_and_four_id_schemas() {
        let cases = [
            (BindingSchema::TwoIds, vec![vec![10, 12], vec![100, 95]]),
            (
                BindingSchema::ThreeIds,
                vec![vec![20, 21], vec![200, 205], vec![2_000, 1_999]],
            ),
            (
                BindingSchema::FourIds,
                vec![
                    vec![30, 29],
                    vec![300, 301],
                    vec![3_000, 3_010],
                    vec![30_000, 29_000],
                ],
            ),
        ];

        for (schema, columns) in cases {
            let column_slices = columns.iter().map(Vec::as_slice).collect::<Vec<_>>();
            let bytes = materialized_ids(schema, &column_slices, None);
            let capsule = BindingCapsule::decode(&bytes).unwrap();
            assert_eq!(capsule.schema(), schema);
            assert_eq!(capsule.row_count(), 2);
            assert_eq!(
                capsule
                    .rows()
                    .map(|row| row.ids().to_vec())
                    .collect::<Vec<_>>(),
                (0..2)
                    .map(|row| columns.iter().map(|column| column[row]).collect())
                    .collect::<Vec<Vec<_>>>()
            );
            assert!(capsule.rows().all(|row| row.violated.is_none()));
        }
    }

    #[test]
    fn decodes_five_id_schema_with_violation_rows() {
        let columns = [
            &[1, 2, 4][..],
            &[10, 20, 10][..],
            &[100, 101, 102][..],
            &[1_000, 999, 998][..],
            &[10_000, 10_100, 10_200][..],
        ];
        let violations = [false, true, true];
        let bytes = materialized_ids(BindingSchema::FiveIdsViolation, &columns, Some(&violations));

        let capsule = BindingCapsule::decode(&bytes).unwrap();
        assert_eq!(capsule.schema(), BindingSchema::FiveIdsViolation);
        assert_eq!(
            capsule
                .rows()
                .map(|row| (row.ids().to_vec(), row.violated))
                .collect::<Vec<_>>(),
            [
                (vec![1, 10, 100, 1_000, 10_000], Some(false)),
                (vec![2, 20, 101, 999, 10_100], Some(true)),
                (vec![4, 10, 102, 998, 10_200], Some(true)),
            ]
        );
    }

    #[test]
    fn decodes_dictionary_labels_without_per_row_copies() {
        let mut bytes = header(BindingSchema::IdLabelIdViolation, 3);
        for delta in [1, 1, 1, 10, 1, 1] {
            signed(delta, &mut bytes);
        }
        varint(2, &mut bytes);
        for label in ["alice", "bob"] {
            varint(label.len() as u64, &mut bytes);
            bytes.extend(label.as_bytes());
        }
        for index in [0, 1, 0] {
            varint(index, &mut bytes);
        }
        bytes.push(0b0000_0101);

        let capsule = BindingCapsule::decode(&bytes).unwrap();
        let rows = capsule.rows().collect::<Vec<_>>();
        assert_eq!(rows[0].ids(), &[1, 10]);
        assert_eq!(rows[1].ids(), &[2, 11]);
        assert_eq!(rows[2].ids(), &[3, 12]);
        assert_eq!(
            rows.iter().map(|row| row.label).collect::<Vec<_>>(),
            [Some("alice"), Some("bob"), Some("alice"),]
        );
        assert_eq!(
            rows.iter().map(|row| row.violated).collect::<Vec<_>>(),
            [Some(true), Some(false), Some(true)]
        );
    }

    #[test]
    fn decodes_three_numeric_ids_with_violation() {
        let mut bytes = header(BindingSchema::ThreeIdsViolation, 2);
        for delta in [10, 1, 100, 1, 1000, 1] {
            signed(delta, &mut bytes);
        }
        bytes.push(0b0000_0010);

        let capsule = BindingCapsule::decode(&bytes).unwrap();
        let rows = capsule.rows().collect::<Vec<_>>();
        assert_eq!(rows[0].ids(), &[10, 100, 1000]);
        assert_eq!(rows[1].ids(), &[11, 101, 1001]);
        assert_eq!(rows[0].violated, Some(false));
        assert_eq!(rows[1].violated, Some(true));
    }

    #[test]
    fn lazily_expands_multiple_pair_groups() {
        let mut bytes = header(BindingSchema::PairGroups, 5);
        varint(2, &mut bytes);
        signed(10, &mut bytes);
        signed(10, &mut bytes);
        varint(2, &mut bytes);
        varint(1, &mut bytes);
        for delta in [100, 1, 99] {
            signed(delta, &mut bytes);
        }
        for delta in [1000, 1, 999] {
            signed(delta, &mut bytes);
        }

        let capsule = BindingCapsule::decode(&bytes).unwrap();
        assert!(capsule.is_factorized());
        assert_eq!(capsule.row_count(), 5);
        assert_eq!(
            capsule
                .rows()
                .map(|row| row.ids().to_vec())
                .collect::<Vec<_>>(),
            [
                vec![10, 100, 100, 1000, 1000],
                vec![10, 100, 101, 1000, 1001],
                vec![10, 101, 100, 1001, 1000],
                vec![10, 101, 101, 1001, 1001],
                vec![20, 200, 200, 2000, 2000],
            ]
        );
    }

    #[test]
    fn rejects_pair_products_with_a_false_row_count() {
        let mut bytes = header(BindingSchema::PairGroups, 3);
        varint(1, &mut bytes);
        signed(1, &mut bytes);
        varint(2, &mut bytes);
        assert_eq!(
            BindingCapsule::decode(&bytes),
            Err(BindingDecodeError::InvalidPairGroups)
        );
    }

    #[test]
    fn rejects_impossible_materialized_count_before_reserving_rows() {
        let bytes = header(BindingSchema::FiveIds, usize::MAX);

        assert_eq!(
            BindingCapsule::decode(&bytes),
            Err(BindingDecodeError::CountOverflow)
        );
    }

    #[test]
    fn rejects_truncated_materialized_rows_before_reserving_rows() {
        let bytes = header(BindingSchema::ThreeIdsViolation, 1_000_000);

        assert_eq!(
            BindingCapsule::decode(&bytes),
            Err(BindingDecodeError::Truncated)
        );
    }

    #[test]
    fn rejects_impossible_pair_group_count_before_reserving_groups() {
        let mut bytes = header(BindingSchema::PairGroups, 0);
        varint(u64::MAX, &mut bytes);

        assert_eq!(
            BindingCapsule::decode(&bytes),
            Err(BindingDecodeError::CountOverflow)
        );
    }

    #[test]
    fn rejects_truncated_pair_entries_before_reserving_entries() {
        let group_size = 1_000_000_usize;
        let mut bytes = header(
            BindingSchema::PairGroups,
            group_size.checked_mul(group_size).unwrap(),
        );
        varint(1, &mut bytes);
        signed(1, &mut bytes);
        varint(group_size as u64, &mut bytes);

        assert_eq!(
            BindingCapsule::decode(&bytes),
            Err(BindingDecodeError::Truncated)
        );
    }
}
