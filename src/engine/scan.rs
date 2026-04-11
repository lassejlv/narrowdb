use std::cmp::Ordering;

use anyhow::{Result, bail};
use rayon::prelude::*;
use rustc_hash::FxHashMap;

use crate::sql::CompareOp;
use crate::storage::{ColumnData, ColumnStats, LoadedRowGroup, NullBitmap, Table};
use crate::types::Value;

use super::aggregate::AggState;
use super::compile::{CompiledFilter, CompiledProjection, LocalKeyPart, RowSelection};

const PARALLEL_AGGREGATE_ROW_GROUPS: usize = 4;

pub(super) fn scan_table<F>(
    table: &Table,
    mapped: Option<&[u8]>,
    filters: &[CompiledFilter],
    required_columns: &[usize],
    mut visitor: F,
) -> Result<()>
where
    F: FnMut(&LoadedRowGroup<'_>, usize) -> Result<()>,
{
    for row_group in &table.row_groups {
        if !row_group_matches(row_group.rows(), row_group.stats(), filters) {
            continue;
        }
        let row_group = row_group.load(&table.schema, mapped, required_columns)?;
        let selection = select_rows(&row_group, filters);
        selection.try_for_each(|row| visitor(&row_group, row))?;
    }
    Ok(())
}

fn should_parallelize_aggregate(table: &Table) -> bool {
    table.row_groups.len() >= PARALLEL_AGGREGATE_ROW_GROUPS
}

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
    let partials = if should_parallelize_aggregate(table) {
        table
            .row_groups
            .par_iter()
            .filter(|row_group| row_group_matches(row_group.rows(), row_group.stats(), filters))
            .map(|row_group| {
                row_group
                    .load(&table.schema, mapped, required_columns)
                    .and_then(|loaded| worker(&loaded))
            })
            .collect::<Vec<_>>()
    } else {
        table
            .row_groups
            .iter()
            .filter(|row_group| row_group_matches(row_group.rows(), row_group.stats(), filters))
            .map(|row_group| {
                row_group
                    .load(&table.schema, mapped, required_columns)
                    .and_then(|loaded| worker(&loaded))
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
    let selection = select_rows(row_group, filters);
    if selection.is_empty() {
        return Ok(None);
    }
    let mut states = projections.iter().map(AggState::new).collect::<Vec<_>>();
    selection.try_for_each(|row| {
        for (state, projection) in states.iter_mut().zip(projections) {
            state.update(projection, row_group, row)?;
        }
        Ok(())
    })?;

    Ok(Some(states))
}

pub(super) fn aggregate_row_group_grouped(
    row_group: &LoadedRowGroup<'_>,
    filters: &[CompiledFilter],
    projections: &[CompiledProjection],
    group_indexes: &[usize],
) -> Result<FxHashMap<Vec<Value>, Vec<AggState>>> {
    let selection = select_rows(row_group, filters);
    if selection.is_empty() {
        return Ok(FxHashMap::default());
    }
    let mut local_groups: FxHashMap<Vec<LocalKeyPart>, Vec<AggState>> = FxHashMap::default();
    selection.try_for_each(|row| {
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
        Ok(())
    })?;

    let mut materialized = FxHashMap::default();
    for (local_key, local_states) in local_groups {
        let key = materialize_group_key(row_group, group_indexes, &local_key)?;
        materialized.insert(key, local_states);
    }
    Ok(materialized)
}

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
                .is_none_or(|ordering| ordering != Ordering::Greater)
                && max
                    .compare(value)
                    .is_none_or(|ordering| ordering != Ordering::Less)
        }
        _ => true,
    }
}

fn stats_may_match_lt(stats: &ColumnStats, value: &Value) -> bool {
    match &stats.min {
        Some(min) => min
            .compare(value)
            .is_none_or(|ordering| ordering == Ordering::Less),
        None => true,
    }
}

fn stats_may_match_lte(stats: &ColumnStats, value: &Value) -> bool {
    match &stats.min {
        Some(min) => min
            .compare(value)
            .is_none_or(|ordering| ordering != Ordering::Greater),
        None => true,
    }
}

fn stats_may_match_gt(stats: &ColumnStats, value: &Value) -> bool {
    match &stats.max {
        Some(max) => max
            .compare(value)
            .is_none_or(|ordering| ordering == Ordering::Greater),
        None => true,
    }
}

fn stats_may_match_gte(stats: &ColumnStats, value: &Value) -> bool {
    match &stats.max {
        Some(max) => max
            .compare(value)
            .is_none_or(|ordering| ordering != Ordering::Less),
        None => true,
    }
}

fn select_rows(row_group: &LoadedRowGroup<'_>, filters: &[CompiledFilter]) -> RowSelection {
    if !row_group_matches(row_group.rows, row_group.stats, filters) {
        return RowSelection::Indexes(Vec::new());
    }
    if filters.is_empty() {
        return RowSelection::All(row_group.rows);
    }

    let mut selected = apply_filter_to_all_rows(row_group, &filters[0]);
    for filter in &filters[1..] {
        if selected.is_empty() {
            break;
        }
        refine_selection(row_group, filter, &mut selected);
    }
    RowSelection::Indexes(selected)
}

fn apply_filter_to_all_rows(row_group: &LoadedRowGroup<'_>, filter: &CompiledFilter) -> Vec<u32> {
    let mut selected = Vec::with_capacity(row_group.rows);
    let column = row_group.column(filter.column_index);
    let nulls = row_group.nulls(filter.column_index);
    for row in 0..row_group.rows {
        if column_matches_filter(column, row, filter, nulls) {
            selected.push(row as u32);
        }
    }
    selected
}

fn refine_selection(
    row_group: &LoadedRowGroup<'_>,
    filter: &CompiledFilter,
    selected: &mut Vec<u32>,
) {
    let column = row_group.column(filter.column_index);
    let nulls = row_group.nulls(filter.column_index);
    selected.retain(|row| column_matches_filter(column, *row as usize, filter, nulls));
}

fn column_matches_filter(
    column: &ColumnData,
    row: usize,
    filter: &CompiledFilter,
    nulls: Option<&NullBitmap>,
) -> bool {
    match filter.op {
        CompareOp::Eq => column.compare_at(row, &filter.value) == Some(Ordering::Equal),
        CompareOp::NotEq => column.compare_at(row, &filter.value) != Some(Ordering::Equal),
        CompareOp::Lt => column.compare_at(row, &filter.value) == Some(Ordering::Less),
        CompareOp::Lte => column
            .compare_at(row, &filter.value)
            .is_some_and(|ordering| ordering == Ordering::Less || ordering == Ordering::Equal),
        CompareOp::Gt => column.compare_at(row, &filter.value) == Some(Ordering::Greater),
        CompareOp::Gte => column
            .compare_at(row, &filter.value)
            .is_some_and(|ordering| ordering == Ordering::Greater || ordering == Ordering::Equal),
        CompareOp::IsNull => nulls.is_some_and(|n| n.is_null(row)),
        CompareOp::IsNotNull => !nulls.is_some_and(|n| n.is_null(row)),
    }
}

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
