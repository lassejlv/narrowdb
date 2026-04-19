use anyhow::{Result, bail};
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use smallvec::SmallVec;

use crate::sql::CompareOp;
use crate::storage::{ColumnData, ColumnStats, LoadedRowGroup, Table};
use crate::types::Value;

use super::aggregate::AggState;
use super::bitmap::SelectionBitmap;
use super::compile::{CompiledFilter, CompiledFilterExpr, CompiledProjection, LocalKeyPart};

// ---------------------------------------------------------------------------
// Parallel scan — replaces the old sequential visitor-based scan_table
// ---------------------------------------------------------------------------

pub(super) fn parallel_scan_table<F>(
    table: &Table,
    query_threads: usize,
    mapped: Option<&[u8]>,
    filter: Option<&CompiledFilterExpr>,
    required_columns: &[usize],
    extractor: F,
) -> Result<Vec<Vec<Value>>>
where
    F: Fn(&LoadedRowGroup<'_>, usize) -> Vec<Value> + Sync + Send,
{
    let parallelize_row_groups = query_threads > 1 && table.row_groups.len() >= 2;
    let parallelize_rows = query_threads > 1 && table.row_groups.len() < query_threads;

    let process = |stored: &crate::storage::StoredRowGroup| -> Result<Vec<Vec<Value>>> {
        if !row_group_matches(stored.rows(), stored.stats(), filter) {
            return Ok(Vec::new());
        }
        let loaded = stored.load(&table.schema, mapped, required_columns)?;
        let selection = select_rows_bitmap(&loaded, filter);
        if selection.is_empty() {
            return Ok(Vec::new());
        }
        extract_selected_rows(
            &loaded,
            &selection,
            query_threads,
            parallelize_rows,
            &extractor,
        )
    };

    let chunks: Vec<Result<Vec<Vec<Value>>>> = if parallelize_row_groups {
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
    query_threads: usize,
    mapped: Option<&[u8]>,
    filter: Option<&CompiledFilterExpr>,
    required_columns: &[usize],
    worker: F,
) -> Result<Vec<T>>
where
    T: Send,
    F: Fn(&LoadedRowGroup<'_>) -> Result<T> + Sync + Send,
{
    let parallelize_row_groups = query_threads > 1 && table.row_groups.len() >= 2;
    let partials = if parallelize_row_groups {
        table
            .row_groups
            .par_iter()
            .filter(|rg| row_group_matches(rg.rows(), rg.stats(), filter))
            .map(|rg| {
                rg.load(&table.schema, mapped, required_columns)
                    .and_then(|l| worker(&l))
            })
            .collect::<Vec<_>>()
    } else {
        table
            .row_groups
            .iter()
            .filter(|rg| row_group_matches(rg.rows(), rg.stats(), filter))
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
    filter: Option<&CompiledFilterExpr>,
    projections: &[CompiledProjection],
    query_threads: usize,
    parallelize_rows: bool,
) -> Result<Option<Vec<AggState>>> {
    let selection = select_rows_bitmap(row_group, filter);
    if selection.is_empty() {
        return Ok(None);
    }

    if parallelize_rows && selection.count() >= 65_536 {
        return aggregate_row_group_all_parallel(row_group, projections, &selection, query_threads)
            .map(Some);
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
    filter: Option<&CompiledFilterExpr>,
    projections: &[CompiledProjection],
    group_indexes: &[usize],
    query_threads: usize,
    parallelize_rows: bool,
) -> Result<FxHashMap<Vec<Value>, Vec<AggState>>> {
    let selection = select_rows_bitmap(row_group, filter);
    if selection.is_empty() {
        return Ok(FxHashMap::default());
    }

    if parallelize_rows && selection.count() >= 32_768 {
        return aggregate_row_group_grouped_parallel(
            row_group,
            projections,
            group_indexes,
            &selection,
            query_threads,
        );
    }

    // Fast path: single StringDict GROUP BY column — use array-indexed aggregation.
    if group_indexes.len() == 1
        && let ColumnData::StringDict { dictionary, codes } = row_group.column(group_indexes[0])
    {
        return aggregate_single_stringdict(row_group, &selection, projections, dictionary, codes);
    }

    let mut local_groups: FxHashMap<SmallVec<[LocalKeyPart; 4]>, Vec<AggState>> =
        FxHashMap::default();
    for row in selection.iter_set() {
        let key = group_indexes
            .iter()
            .map(|index| LocalKeyPart::from_column(row_group.column(*index), row))
            .collect::<Result<SmallVec<[LocalKeyPart; 4]>>>()?;
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

pub(super) fn count_row_group_int64(
    row_group: &LoadedRowGroup<'_>,
    filter: Option<&CompiledFilterExpr>,
    key_index: usize,
    query_threads: usize,
    parallelize_rows: bool,
) -> Result<FxHashMap<i64, i64>> {
    let selection = select_rows_bitmap(row_group, filter);
    if selection.is_empty() {
        return Ok(FxHashMap::default());
    }

    let ColumnData::Int64(values) = row_group.column(key_index) else {
        bail!("fast grouped count expects an int64-compatible key column")
    };

    if parallelize_rows && selection.count() >= 32_768 {
        let selected_rows = selection.iter_set().collect::<Vec<_>>();
        let chunk_count = parallel_chunk_count(selected_rows.len(), query_threads);
        if chunk_count > 1 {
            let chunk_size = selected_rows.len().div_ceil(chunk_count);
            let partials = selected_rows
                .par_chunks(chunk_size)
                .map(|chunk| {
                    let mut local = FxHashMap::default();
                    for &row in chunk {
                        *local.entry(values[row]).or_insert(0) += 1;
                    }
                    local
                })
                .collect::<Vec<_>>();
            let mut merged = FxHashMap::default();
            for partial in partials {
                for (key, count) in partial {
                    *merged.entry(key).or_insert(0) += count;
                }
            }
            return Ok(merged);
        }
    }

    let mut counts = FxHashMap::default();
    for row in selection.iter_set() {
        *counts.entry(values[row]).or_insert(0) += 1;
    }
    Ok(counts)
}

pub(super) fn count_row_group_bool(
    row_group: &LoadedRowGroup<'_>,
    filter: Option<&CompiledFilterExpr>,
    key_index: usize,
) -> Result<[i64; 2]> {
    let selection = select_rows_bitmap(row_group, filter);
    if selection.is_empty() {
        return Ok([0, 0]);
    }

    let ColumnData::Bool(values) = row_group.column(key_index) else {
        bail!("fast grouped count expects a bool key column")
    };

    let mut counts = [0_i64; 2];
    for row in selection.iter_set() {
        counts[values[row] as usize] += 1;
    }
    Ok(counts)
}

pub(super) fn collect_row_group_int64_values(
    row_group: &LoadedRowGroup<'_>,
    filter: Option<&CompiledFilterExpr>,
    key_index: usize,
) -> Result<Vec<i64>> {
    let selection = select_rows_bitmap(row_group, filter);
    if selection.is_empty() {
        return Ok(Vec::new());
    }

    let ColumnData::Int64(values) = row_group.column(key_index) else {
        bail!("sorted grouped count expects an int64-compatible key column")
    };

    let mut collected = Vec::with_capacity(selection.count());
    for row in selection.iter_set() {
        collected.push(values[row]);
    }
    Ok(collected)
}

fn extract_selected_rows<F>(
    row_group: &LoadedRowGroup<'_>,
    selection: &SelectionBitmap,
    query_threads: usize,
    parallelize_rows: bool,
    extractor: &F,
) -> Result<Vec<Vec<Value>>>
where
    F: Fn(&LoadedRowGroup<'_>, usize) -> Vec<Value> + Sync + Send,
{
    if !parallelize_rows || selection.count() < 65_536 {
        let mut rows = Vec::with_capacity(selection.count());
        for row_idx in selection.iter_set() {
            rows.push(extractor(row_group, row_idx));
        }
        return Ok(rows);
    }

    let selected_rows = selection.iter_set().collect::<Vec<_>>();
    let chunk_count = parallel_chunk_count(selected_rows.len(), query_threads);
    if chunk_count <= 1 {
        return Ok(selected_rows
            .into_iter()
            .map(|row_idx| extractor(row_group, row_idx))
            .collect());
    }

    let chunk_size = selected_rows.len().div_ceil(chunk_count);
    let chunks = selected_rows
        .par_chunks(chunk_size)
        .map(|chunk| {
            chunk
                .iter()
                .map(|&row_idx| extractor(row_group, row_idx))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    Ok(chunks.into_iter().flatten().collect())
}

fn aggregate_row_group_all_parallel(
    row_group: &LoadedRowGroup<'_>,
    projections: &[CompiledProjection],
    selection: &SelectionBitmap,
    query_threads: usize,
) -> Result<Vec<AggState>> {
    let selected_rows = selection.iter_set().collect::<Vec<_>>();
    let chunk_count = parallel_chunk_count(selected_rows.len(), query_threads);
    if chunk_count <= 1 {
        let mut states = projections.iter().map(AggState::new).collect::<Vec<_>>();
        for row in selected_rows {
            for (state, projection) in states.iter_mut().zip(projections) {
                state.update(projection, row_group, row)?;
            }
        }
        return Ok(states);
    }

    let chunk_size = selected_rows.len().div_ceil(chunk_count);
    let partials = selected_rows
        .par_chunks(chunk_size)
        .map(|chunk| {
            let mut states = projections.iter().map(AggState::new).collect::<Vec<_>>();
            for &row in chunk {
                for (state, projection) in states.iter_mut().zip(projections) {
                    state.update(projection, row_group, row)?;
                }
            }
            Ok::<_, anyhow::Error>(states)
        })
        .collect::<Vec<_>>();

    let mut merged = projections.iter().map(AggState::new).collect::<Vec<_>>();
    for partial in partials {
        for (state, partial_state) in merged.iter_mut().zip(partial?) {
            state.merge(partial_state)?;
        }
    }
    Ok(merged)
}

fn aggregate_row_group_grouped_parallel(
    row_group: &LoadedRowGroup<'_>,
    projections: &[CompiledProjection],
    group_indexes: &[usize],
    selection: &SelectionBitmap,
    query_threads: usize,
) -> Result<FxHashMap<Vec<Value>, Vec<AggState>>> {
    let selected_rows = selection.iter_set().collect::<Vec<_>>();
    let chunk_count = parallel_chunk_count(selected_rows.len(), query_threads);
    if chunk_count <= 1 {
        let mut local_groups: FxHashMap<SmallVec<[LocalKeyPart; 4]>, Vec<AggState>> =
            FxHashMap::default();
        for row in selected_rows {
            let key = group_indexes
                .iter()
                .map(|index| LocalKeyPart::from_column(row_group.column(*index), row))
                .collect::<Result<SmallVec<[LocalKeyPart; 4]>>>()?;
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
        return Ok(materialized);
    }

    let chunk_size = selected_rows.len().div_ceil(chunk_count);
    let partials = selected_rows
        .par_chunks(chunk_size)
        .map(|chunk| {
            let mut local_groups: FxHashMap<SmallVec<[LocalKeyPart; 4]>, Vec<AggState>> =
                FxHashMap::default();
            for &row in chunk {
                let key = group_indexes
                    .iter()
                    .map(|index| LocalKeyPart::from_column(row_group.column(*index), row))
                    .collect::<Result<SmallVec<[LocalKeyPart; 4]>>>()?;
                let states = local_groups
                    .entry(key)
                    .or_insert_with(|| projections.iter().map(AggState::new).collect());
                for (state, projection) in states.iter_mut().zip(projections) {
                    state.update(projection, row_group, row)?;
                }
            }
            Ok::<_, anyhow::Error>(local_groups)
        })
        .collect::<Vec<_>>();

    let mut local_groups: FxHashMap<SmallVec<[LocalKeyPart; 4]>, Vec<AggState>> =
        FxHashMap::default();
    for partial in partials {
        for (key, partial_states) in partial? {
            let states = local_groups
                .entry(key)
                .or_insert_with(|| projections.iter().map(AggState::new).collect());
            for (state, partial_state) in states.iter_mut().zip(partial_states) {
                state.merge(partial_state)?;
            }
        }
    }

    let mut materialized = FxHashMap::default();
    for (local_key, local_states) in local_groups {
        let key = materialize_group_key(row_group, group_indexes, &local_key)?;
        materialized.insert(key, local_states);
    }
    Ok(materialized)
}

fn parallel_chunk_count(selected_rows: usize, query_threads: usize) -> usize {
    if selected_rows == 0 {
        return 0;
    }
    let max_tasks = query_threads.saturating_mul(4).max(1);
    let rows_per_task = 16_384;
    selected_rows.div_ceil(rows_per_task).clamp(1, max_tasks)
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

fn row_group_matches(
    rows: usize,
    stats: &[ColumnStats],
    filter: Option<&CompiledFilterExpr>,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    match filter {
        CompiledFilterExpr::Predicate(filter) => match filter.op {
            CompareOp::Eq => stats_may_match_eq(&stats[filter.column_index], &filter.value),
            CompareOp::Lt => stats_may_match_lt(&stats[filter.column_index], &filter.value),
            CompareOp::Lte => stats_may_match_lte(&stats[filter.column_index], &filter.value),
            CompareOp::Gt => stats_may_match_gt(&stats[filter.column_index], &filter.value),
            CompareOp::Gte => stats_may_match_gte(&stats[filter.column_index], &filter.value),
            CompareOp::NotEq => true,
            CompareOp::IsNull => stats[filter.column_index].null_count > 0,
            CompareOp::IsNotNull => stats[filter.column_index].null_count < rows,
        },
        CompiledFilterExpr::And(children) => children
            .iter()
            .all(|child| row_group_matches(rows, stats, Some(child))),
        CompiledFilterExpr::Or(children) => children
            .iter()
            .any(|child| row_group_matches(rows, stats, Some(child))),
    }
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
    filter: Option<&CompiledFilterExpr>,
) -> SelectionBitmap {
    if !row_group_matches(row_group.rows, row_group.stats, filter) {
        return SelectionBitmap::none(row_group.rows);
    }
    eval_filter_bitmap(row_group, filter)
}

fn eval_filter_bitmap(
    row_group: &LoadedRowGroup<'_>,
    filter: Option<&CompiledFilterExpr>,
) -> SelectionBitmap {
    let Some(filter) = filter else {
        return SelectionBitmap::all(row_group.rows);
    };

    match filter {
        CompiledFilterExpr::Predicate(filter) => {
            let mut bitmap = SelectionBitmap::all(row_group.rows);
            apply_filter_bitmap(row_group, filter, &mut bitmap);
            bitmap
        }
        CompiledFilterExpr::And(children) => {
            let mut bitmap = SelectionBitmap::all(row_group.rows);
            for child in children {
                if bitmap.is_empty() {
                    break;
                }
                let child_bitmap = eval_filter_bitmap(row_group, Some(child));
                bitmap.intersect(&child_bitmap);
            }
            bitmap
        }
        CompiledFilterExpr::Or(children) => {
            let mut bitmap = SelectionBitmap::none(row_group.rows);
            for child in children {
                let child_bitmap = eval_filter_bitmap(row_group, Some(child));
                bitmap.union(&child_bitmap);
            }
            bitmap
        }
    }
}

fn apply_filter_bitmap(
    row_group: &LoadedRowGroup<'_>,
    filter: &CompiledFilter,
    bitmap: &mut SelectionBitmap,
) {
    let nulls = row_group.nulls(filter.column_index);

    match filter.op {
        CompareOp::IsNull => {
            match nulls {
                Some(n) => bitmap.mask_null(n),
                None => bitmap.clear(),
            }
            return;
        }
        CompareOp::IsNotNull => {
            if let Some(n) = nulls {
                bitmap.mask_non_null(n);
            }
            return;
        }
        _ => {}
    }

    let column = row_group.column(filter.column_index);
    apply_column_filter(column, filter.op, &filter.value, bitmap);

    // Null values must not pass non-null filters (SQL semantics).
    if let Some(n) = nulls {
        bitmap.mask_non_null_unchecked(n);
    }
    bitmap.recount();
}

// ---------------------------------------------------------------------------
// Type-specialized vectorized evaluators
// ---------------------------------------------------------------------------

fn apply_column_filter(
    column: &ColumnData,
    op: CompareOp,
    value: &Value,
    bitmap: &mut SelectionBitmap,
) {
    match (column, value) {
        (ColumnData::Int64(values), Value::Int64(target)) => {
            apply_int64(values, *target, op, bitmap);
        }
        (ColumnData::Int64(values), Value::Float64(target)) => {
            apply_int64_vs_float(values, target.into_inner(), op, bitmap);
        }
        (ColumnData::Float64(values), Value::Float64(target)) => {
            apply_float64(values, *target, op, bitmap);
        }
        (ColumnData::Float64(values), Value::Int64(target)) => apply_float64(
            values,
            ordered_float::OrderedFloat(*target as f64),
            op,
            bitmap,
        ),
        (ColumnData::Bool(values), Value::Bool(target)) => apply_bool(values, *target, op, bitmap),
        (ColumnData::StringDict { dictionary, codes }, Value::String(target)) => {
            apply_string_dict(dictionary, codes, target, op, bitmap)
        }
        (ColumnData::StringPlain(values), Value::String(target)) => {
            apply_string_plain(values, target, op, bitmap)
        }
        _ => bitmap.clear(),
    }
}

fn apply_int64(values: &[i64], target: i64, op: CompareOp, bitmap: &mut SelectionBitmap) {
    let pred: fn(i64, i64) -> bool = match op {
        CompareOp::Eq => |a, b| a == b,
        CompareOp::NotEq => |a, b| a != b,
        CompareOp::Lt => |a, b| a < b,
        CompareOp::Lte => |a, b| a <= b,
        CompareOp::Gt => |a, b| a > b,
        CompareOp::Gte => |a, b| a >= b,
        _ => unreachable!(),
    };
    bitmap.retain_from_predicate_unchecked(|i| pred(values[i], target));
}

fn apply_int64_vs_float(values: &[i64], target: f64, op: CompareOp, bitmap: &mut SelectionBitmap) {
    let pred: fn(f64, f64) -> bool = match op {
        CompareOp::Eq => |a, b| a == b,
        CompareOp::NotEq => |a, b| a != b,
        CompareOp::Lt => |a, b| a < b,
        CompareOp::Lte => |a, b| a <= b,
        CompareOp::Gt => |a, b| a > b,
        CompareOp::Gte => |a, b| a >= b,
        _ => unreachable!(),
    };
    bitmap.retain_from_predicate_unchecked(|i| pred(values[i] as f64, target));
}

fn apply_float64(
    values: &[ordered_float::OrderedFloat<f64>],
    target: ordered_float::OrderedFloat<f64>,
    op: CompareOp,
    bitmap: &mut SelectionBitmap,
) {
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
    bitmap.retain_from_predicate_unchecked(|i| pred(values[i], target));
}

fn apply_bool(values: &[bool], target: bool, op: CompareOp, bitmap: &mut SelectionBitmap) {
    match op {
        CompareOp::Eq => bitmap.retain_from_predicate_unchecked(|i| values[i] == target),
        CompareOp::NotEq => bitmap.retain_from_predicate_unchecked(|i| values[i] != target),
        _ => bitmap.clear(),
    }
}

fn apply_string_dict(
    dictionary: &[String],
    codes: &[u32],
    target: &str,
    op: CompareOp,
    bitmap: &mut SelectionBitmap,
) {
    match op {
        CompareOp::Eq => match dictionary.iter().position(|s| s == target) {
            Some(code) => {
                let code = code as u32;
                bitmap.retain_from_predicate_unchecked(|i| codes[i] == code);
            }
            None => bitmap.clear(),
        },
        CompareOp::NotEq => {
            if let Some(code) = dictionary.iter().position(|s| s == target) {
                let code = code as u32;
                bitmap.retain_from_predicate_unchecked(|i| codes[i] != code);
            }
        }
        _ => {
            let pred: fn(&str, &str) -> bool = match op {
                CompareOp::Lt => |a, b| a < b,
                CompareOp::Lte => |a, b| a <= b,
                CompareOp::Gt => |a, b| a > b,
                CompareOp::Gte => |a, b| a >= b,
                _ => unreachable!(),
            };
            let matching_codes: Vec<bool> = dictionary.iter().map(|s| pred(s, target)).collect();
            bitmap.retain_from_predicate_unchecked(|i| matching_codes[codes[i] as usize]);
        }
    }
}

fn apply_string_plain(
    values: &[String],
    target: &str,
    op: CompareOp,
    bitmap: &mut SelectionBitmap,
) {
    let pred: fn(&str, &str) -> bool = match op {
        CompareOp::Eq => |a, b| a == b,
        CompareOp::NotEq => |a, b| a != b,
        CompareOp::Lt => |a, b| a < b,
        CompareOp::Lte => |a, b| a <= b,
        CompareOp::Gt => |a, b| a > b,
        CompareOp::Gte => |a, b| a >= b,
        _ => unreachable!(),
    };
    bitmap.retain_from_predicate_unchecked(|i| pred(&values[i], target));
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
