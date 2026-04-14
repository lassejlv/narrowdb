use anyhow::{Result, bail};
use rayon::prelude::*;
use rustc_hash::FxHashMap;

use crate::sql::CompareOp;
use crate::storage::{ColumnData, ColumnStats, LoadedRowGroup, Table};
use crate::types::Value;

use super::aggregate::AggState;
use super::bitmap::SelectionBitmap;
use super::compile::{CompiledFilter, CompiledProjection, LocalKeyPart};

const PARALLEL_ROW_GROUPS: usize = 4;

// ---------------------------------------------------------------------------
// Parallel scan — replaces the old sequential visitor-based scan_table
// ---------------------------------------------------------------------------

pub(super) fn parallel_scan_table<F>(
    table: &Table,
    mapped: Option<&[u8]>,
    filters: &[CompiledFilter],
    required_columns: &[usize],
    extractor: F,
) -> Result<Vec<Vec<Value>>>
where
    F: Fn(&LoadedRowGroup<'_>, usize) -> Vec<Value> + Sync + Send,
{
    let process = |stored: &crate::storage::StoredRowGroup| -> Result<Vec<Vec<Value>>> {
        if !row_group_matches(stored.rows(), stored.stats(), filters) {
            return Ok(Vec::new());
        }
        let loaded = stored.load(&table.schema, mapped, required_columns)?;
        let selection = select_rows_bitmap(&loaded, filters);
        if selection.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::with_capacity(selection.count());
        for row_idx in selection.iter_set() {
            rows.push(extractor(&loaded, row_idx));
        }
        Ok(rows)
    };

    let chunks: Vec<Result<Vec<Vec<Value>>>> = if table.row_groups.len() >= PARALLEL_ROW_GROUPS {
        table.row_groups.par_iter().map(process).collect()
    } else {
        table.row_groups.iter().map(process).collect()
    };

    let mut all_rows = Vec::new();
    for chunk in chunks {
        all_rows.extend(chunk?);
    }
    Ok(all_rows)
}

// ---------------------------------------------------------------------------
// Aggregate scanning (already parallel via rayon)
// ---------------------------------------------------------------------------

pub(super) fn collect_row_group_aggregates<T, F>(
    table: &Table,
    mapped: Option<&[u8]>,
    filters: &[CompiledFilter],
    required_columns: &[usize],
    worker: F,
) -> Result<Vec<T>>
where
    T: Send,
    F: Fn(&LoadedRowGroup<'_>) -> Result<T> + Sync + Send,
{
    let partials = if table.row_groups.len() >= PARALLEL_ROW_GROUPS {
        table
            .row_groups
            .par_iter()
            .filter(|rg| row_group_matches(rg.rows(), rg.stats(), filters))
            .map(|rg| {
                rg.load(&table.schema, mapped, required_columns)
                    .and_then(|l| worker(&l))
            })
            .collect::<Vec<_>>()
    } else {
        table
            .row_groups
            .iter()
            .filter(|rg| row_group_matches(rg.rows(), rg.stats(), filters))
            .map(|rg| {
                rg.load(&table.schema, mapped, required_columns)
                    .and_then(|l| worker(&l))
            })
            .collect::<Vec<_>>()
    };
    partials.into_iter().collect()
}

pub(super) fn aggregate_row_group_all(
    row_group: &LoadedRowGroup<'_>,
    filters: &[CompiledFilter],
    projections: &[CompiledProjection],
) -> Result<Option<Vec<AggState>>> {
    let selection = select_rows_bitmap(row_group, filters);
    if selection.is_empty() {
        return Ok(None);
    }
    let mut states = projections.iter().map(AggState::new).collect::<Vec<_>>();
    for row in selection.iter_set() {
        for (state, projection) in states.iter_mut().zip(projections) {
            state.update(projection, row_group, row)?;
        }
    }
    Ok(Some(states))
}

pub(super) fn aggregate_row_group_grouped(
    row_group: &LoadedRowGroup<'_>,
    filters: &[CompiledFilter],
    projections: &[CompiledProjection],
    group_indexes: &[usize],
) -> Result<FxHashMap<Vec<Value>, Vec<AggState>>> {
    let selection = select_rows_bitmap(row_group, filters);
    if selection.is_empty() {
        return Ok(FxHashMap::default());
    }

    // Fast path: single StringDict GROUP BY column — use array-indexed aggregation.
    if group_indexes.len() == 1 {
        if let ColumnData::StringDict { dictionary, codes } = row_group.column(group_indexes[0]) {
            return aggregate_single_stringdict(
                row_group,
                &selection,
                projections,
                dictionary,
                codes,
            );
        }
    }

    let mut local_groups: FxHashMap<Vec<LocalKeyPart>, Vec<AggState>> = FxHashMap::default();
    for row in selection.iter_set() {
        let key = group_indexes
            .iter()
            .map(|index| LocalKeyPart::from_column(row_group.column(*index), row))
            .collect::<Result<Vec<_>>>()?;
        let states = local_groups
            .entry(key)
            .or_insert_with(|| projections.iter().map(AggState::new).collect());
        for (state, projection) in states.iter_mut().zip(projections) {
            state.update(projection, row_group, row)?;
        }
    }

    let mut materialized = FxHashMap::default();
    for (local_key, local_states) in local_groups {
        let key = materialize_group_key(row_group, group_indexes, &local_key)?;
        materialized.insert(key, local_states);
    }
    Ok(materialized)
}

/// Array-indexed aggregation for a single StringDict GROUP BY column.
/// Replaces HashMap with a fixed-size array indexed by dictionary code.
fn aggregate_single_stringdict(
    row_group: &LoadedRowGroup<'_>,
    selection: &SelectionBitmap,
    projections: &[CompiledProjection],
    dictionary: &[String],
    codes: &[u32],
) -> Result<FxHashMap<Vec<Value>, Vec<AggState>>> {
    let dict_size = dictionary.len();
    let mut states_array: Vec<Option<Vec<AggState>>> = (0..dict_size).map(|_| None).collect();

    for row in selection.iter_set() {
        let code = codes[row] as usize;
        debug_assert!(code < dict_size);
        let states = states_array[code]
            .get_or_insert_with(|| projections.iter().map(AggState::new).collect());
        for (state, proj) in states.iter_mut().zip(projections) {
            state.update(proj, row_group, row)?;
        }
    }

    let mut result = FxHashMap::default();
    for (code, slot) in states_array.into_iter().enumerate() {
        if let Some(agg_states) = slot {
            let key = vec![Value::String(dictionary[code].clone())];
            result.insert(key, agg_states);
        }
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Row group stats pruning
// ---------------------------------------------------------------------------

fn row_group_matches(rows: usize, stats: &[ColumnStats], filters: &[CompiledFilter]) -> bool {
    filters.iter().all(|filter| match filter.op {
        CompareOp::Eq => stats_may_match_eq(&stats[filter.column_index], &filter.value),
        CompareOp::Lt => stats_may_match_lt(&stats[filter.column_index], &filter.value),
        CompareOp::Lte => stats_may_match_lte(&stats[filter.column_index], &filter.value),
        CompareOp::Gt => stats_may_match_gt(&stats[filter.column_index], &filter.value),
        CompareOp::Gte => stats_may_match_gte(&stats[filter.column_index], &filter.value),
        CompareOp::NotEq => true,
        CompareOp::IsNull => stats[filter.column_index].null_count > 0,
        CompareOp::IsNotNull => stats[filter.column_index].null_count < rows,
    })
}

fn stats_may_match_eq(stats: &ColumnStats, value: &Value) -> bool {
    match (&stats.min, &stats.max) {
        (Some(min), Some(max)) => {
            min.compare(value)
                .is_none_or(|o| o != std::cmp::Ordering::Greater)
                && max
                    .compare(value)
                    .is_none_or(|o| o != std::cmp::Ordering::Less)
        }
        _ => true,
    }
}

fn stats_may_match_lt(stats: &ColumnStats, value: &Value) -> bool {
    match &stats.min {
        Some(min) => min
            .compare(value)
            .is_none_or(|o| o == std::cmp::Ordering::Less),
        None => true,
    }
}

fn stats_may_match_lte(stats: &ColumnStats, value: &Value) -> bool {
    match &stats.min {
        Some(min) => min
            .compare(value)
            .is_none_or(|o| o != std::cmp::Ordering::Greater),
        None => true,
    }
}

fn stats_may_match_gt(stats: &ColumnStats, value: &Value) -> bool {
    match &stats.max {
        Some(max) => max
            .compare(value)
            .is_none_or(|o| o == std::cmp::Ordering::Greater),
        None => true,
    }
}

fn stats_may_match_gte(stats: &ColumnStats, value: &Value) -> bool {
    match &stats.max {
        Some(max) => max
            .compare(value)
            .is_none_or(|o| o != std::cmp::Ordering::Less),
        None => true,
    }
}

// ---------------------------------------------------------------------------
// Vectorized filter evaluation — produces SelectionBitmap
// ---------------------------------------------------------------------------

fn select_rows_bitmap(
    row_group: &LoadedRowGroup<'_>,
    filters: &[CompiledFilter],
) -> SelectionBitmap {
    if !row_group_matches(row_group.rows, row_group.stats, filters) {
        return SelectionBitmap::none(row_group.rows);
    }
    if filters.is_empty() {
        return SelectionBitmap::all(row_group.rows);
    }

    let mut bitmap = evaluate_filter_bitmap(row_group, &filters[0]);
    for filter in &filters[1..] {
        if bitmap.is_empty() {
            break;
        }
        bitmap.intersect(&evaluate_filter_bitmap(row_group, filter));
    }
    bitmap
}

fn evaluate_filter_bitmap(
    row_group: &LoadedRowGroup<'_>,
    filter: &CompiledFilter,
) -> SelectionBitmap {
    let nulls = row_group.nulls(filter.column_index);

    // Handle IS NULL / IS NOT NULL directly from null bitmap.
    match filter.op {
        CompareOp::IsNull => {
            return match nulls {
                Some(n) => {
                    let mut bm = SelectionBitmap::all(row_group.rows);
                    bm.mask_null(n);
                    bm
                }
                None => SelectionBitmap::none(row_group.rows),
            };
        }
        CompareOp::IsNotNull => {
            return match nulls {
                Some(n) => {
                    let mut bm = SelectionBitmap::all(row_group.rows);
                    bm.mask_non_null(n);
                    bm
                }
                None => SelectionBitmap::all(row_group.rows),
            };
        }
        _ => {}
    }

    let column = row_group.column(filter.column_index);
    let mut bitmap = evaluate_column_filter(column, filter.op, &filter.value, row_group.rows);

    // Null values must not pass non-null filters (SQL semantics).
    if let Some(n) = nulls {
        bitmap.mask_non_null(n);
    }
    bitmap
}

fn evaluate_column_filter(
    column: &ColumnData,
    op: CompareOp,
    value: &Value,
    rows: usize,
) -> SelectionBitmap {
    match (column, value) {
        (ColumnData::Int64(values), Value::Int64(target)) => eval_int64(values, *target, op, rows),
        (ColumnData::Int64(values), Value::Float64(target)) => {
            eval_int64_vs_float(values, target.into_inner(), op, rows)
        }
        (ColumnData::Float64(values), Value::Float64(target)) => {
            eval_float64(values, *target, op, rows)
        }
        (ColumnData::Float64(values), Value::Int64(target)) => eval_float64(
            values,
            ordered_float::OrderedFloat(*target as f64),
            op,
            rows,
        ),
        (ColumnData::Bool(values), Value::Bool(target)) => eval_bool(values, *target, op, rows),
        (ColumnData::StringDict { dictionary, codes }, Value::String(target)) => {
            eval_string_dict(dictionary, codes, target, op, rows)
        }
        (ColumnData::StringPlain(values), Value::String(target)) => {
            eval_string_plain(values, target, op, rows)
        }
        _ => {
            // Type mismatch: no rows match.
            SelectionBitmap::none(rows)
        }
    }
}

// ---------------------------------------------------------------------------
// Type-specialized vectorized evaluators
// ---------------------------------------------------------------------------

fn eval_int64(values: &[i64], target: i64, op: CompareOp, rows: usize) -> SelectionBitmap {
    let pred: fn(i64, i64) -> bool = match op {
        CompareOp::Eq => |a, b| a == b,
        CompareOp::NotEq => |a, b| a != b,
        CompareOp::Lt => |a, b| a < b,
        CompareOp::Lte => |a, b| a <= b,
        CompareOp::Gt => |a, b| a > b,
        CompareOp::Gte => |a, b| a >= b,
        _ => unreachable!(),
    };
    build_bitmap_from_predicate(rows, |i| pred(values[i], target))
}

fn eval_int64_vs_float(values: &[i64], target: f64, op: CompareOp, rows: usize) -> SelectionBitmap {
    let pred: fn(f64, f64) -> bool = match op {
        CompareOp::Eq => |a, b| a == b,
        CompareOp::NotEq => |a, b| a != b,
        CompareOp::Lt => |a, b| a < b,
        CompareOp::Lte => |a, b| a <= b,
        CompareOp::Gt => |a, b| a > b,
        CompareOp::Gte => |a, b| a >= b,
        _ => unreachable!(),
    };
    build_bitmap_from_predicate(rows, |i| pred(values[i] as f64, target))
}

fn eval_float64(
    values: &[ordered_float::OrderedFloat<f64>],
    target: ordered_float::OrderedFloat<f64>,
    op: CompareOp,
    rows: usize,
) -> SelectionBitmap {
    let pred: fn(ordered_float::OrderedFloat<f64>, ordered_float::OrderedFloat<f64>) -> bool =
        match op {
            CompareOp::Eq => |a, b| a == b,
            CompareOp::NotEq => |a, b| a != b,
            CompareOp::Lt => |a, b| a < b,
            CompareOp::Lte => |a, b| a <= b,
            CompareOp::Gt => |a, b| a > b,
            CompareOp::Gte => |a, b| a >= b,
            _ => unreachable!(),
        };
    build_bitmap_from_predicate(rows, |i| pred(values[i], target))
}

fn eval_bool(values: &[bool], target: bool, op: CompareOp, rows: usize) -> SelectionBitmap {
    match op {
        CompareOp::Eq => build_bitmap_from_predicate(rows, |i| values[i] == target),
        CompareOp::NotEq => build_bitmap_from_predicate(rows, |i| values[i] != target),
        _ => SelectionBitmap::none(rows),
    }
}

fn eval_string_dict(
    dictionary: &[String],
    codes: &[u32],
    target: &str,
    op: CompareOp,
    rows: usize,
) -> SelectionBitmap {
    match op {
        CompareOp::Eq => {
            // Find matching code in dictionary, then compare as integers.
            match dictionary.iter().position(|s| s == target) {
                Some(code) => {
                    let code = code as u32;
                    build_bitmap_from_predicate(rows, |i| codes[i] == code)
                }
                None => SelectionBitmap::none(rows),
            }
        }
        CompareOp::NotEq => match dictionary.iter().position(|s| s == target) {
            Some(code) => {
                let code = code as u32;
                build_bitmap_from_predicate(rows, |i| codes[i] != code)
            }
            None => SelectionBitmap::all(rows),
        },
        _ => {
            // Range comparisons: build a lookup table for matching codes.
            let pred: fn(&str, &str) -> bool = match op {
                CompareOp::Lt => |a, b| a < b,
                CompareOp::Lte => |a, b| a <= b,
                CompareOp::Gt => |a, b| a > b,
                CompareOp::Gte => |a, b| a >= b,
                _ => unreachable!(),
            };
            let matching_codes: Vec<bool> = dictionary.iter().map(|s| pred(s, target)).collect();
            build_bitmap_from_predicate(rows, |i| matching_codes[codes[i] as usize])
        }
    }
}

fn eval_string_plain(
    values: &[String],
    target: &str,
    op: CompareOp,
    rows: usize,
) -> SelectionBitmap {
    let pred: fn(&str, &str) -> bool = match op {
        CompareOp::Eq => |a, b| a == b,
        CompareOp::NotEq => |a, b| a != b,
        CompareOp::Lt => |a, b| a < b,
        CompareOp::Lte => |a, b| a <= b,
        CompareOp::Gt => |a, b| a > b,
        CompareOp::Gte => |a, b| a >= b,
        _ => unreachable!(),
    };
    build_bitmap_from_predicate(rows, |i| pred(&values[i], target))
}

// ---------------------------------------------------------------------------
// Bitmap building helper — processes in 64-element chunks for auto-vectorization
// ---------------------------------------------------------------------------

#[inline]
fn build_bitmap_from_predicate<F>(rows: usize, pred: F) -> SelectionBitmap
where
    F: Fn(usize) -> bool,
{
    let mut bitmap = SelectionBitmap::none(rows);
    let words = bitmap.words_mut();
    let full_words = rows / 64;
    for word_idx in 0..full_words {
        let base = word_idx * 64;
        let mut word: u64 = 0;
        for bit in 0..64 {
            word |= (pred(base + bit) as u64) << bit;
        }
        words[word_idx] = word;
    }
    // Handle the remainder.
    let remainder = rows % 64;
    if remainder > 0 {
        let base = full_words * 64;
        let mut word: u64 = 0;
        for bit in 0..remainder {
            word |= (pred(base + bit) as u64) << bit;
        }
        words[full_words] = word;
    }
    bitmap.recount();
    bitmap
}

// ---------------------------------------------------------------------------
// Group key materialization
// ---------------------------------------------------------------------------

fn materialize_group_key(
    row_group: &LoadedRowGroup<'_>,
    group_indexes: &[usize],
    key: &[LocalKeyPart],
) -> Result<Vec<Value>> {
    let mut values = Vec::with_capacity(key.len());
    for (group_index, part) in group_indexes.iter().zip(key) {
        let value = match (row_group.column(*group_index), part) {
            (ColumnData::Int64(_), LocalKeyPart::Int64(value)) => Value::Int64(*value),
            (ColumnData::Float64(_), LocalKeyPart::Float64(value)) => Value::Float64(*value),
            (ColumnData::Bool(_), LocalKeyPart::Bool(value)) => Value::Bool(*value),
            (ColumnData::StringPlain(_), LocalKeyPart::String(value)) => {
                Value::String(value.clone())
            }
            (ColumnData::StringDict { dictionary, .. }, LocalKeyPart::StringCode(code)) => {
                Value::String(dictionary[*code as usize].clone())
            }
            _ => bail!("group key type mismatch"),
        };
        values.push(value);
    }
    Ok(values)
}
