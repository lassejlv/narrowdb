use std::cmp::Ordering;
use std::path::Path;

use anyhow::{Context, Result, bail, ensure};
use ordered_float::OrderedFloat;
use rayon::prelude::*;
use rustc_hash::FxHashMap;

use crate::sql::{
    AggregateKind, Command, CompareOp, Filter, InsertPlan, OrderByPlan, Projection, ProjectionExpr,
    SelectPlan, parse_sql,
};
use crate::storage::{
    ColumnData, DbOptions, PendingBatch, RowGroup, Storage, StoredRowGroup, Table,
    row_group_from_columnar_batch,
};
use crate::types::{ColumnarBatch, Schema, Value};

pub struct NarrowDb {
    storage: Storage,
    tables: FxHashMap<String, Table>,
}

const PARALLEL_AGGREGATE_ROW_GROUPS: usize = 4;

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }
}

impl NarrowDb {
    pub fn open(path: impl AsRef<Path>, options: DbOptions) -> Result<Self> {
        let (storage, tables) = Storage::open(path, options)?;
        Ok(Self { storage, tables })
    }

    pub fn path(&self) -> &Path {
        self.storage.path()
    }

    pub fn create_table(&mut self, schema: Schema) -> Result<()> {
        ensure!(
            !self.tables.contains_key(&schema.table_name),
            "table {} already exists",
            schema.table_name
        );
        self.storage.append_create_table(&schema)?;
        let table = Table::new(schema.clone(), self.storage.options());
        self.tables.insert(schema.table_name.clone(), table);
        Ok(())
    }

    pub fn insert_row(&mut self, table_name: &str, row: Vec<Value>) -> Result<()> {
        let table = self
            .tables
            .get_mut(table_name)
            .with_context(|| format!("unknown table {table_name}"))?;
        ensure!(
            row.len() == table.schema.columns.len(),
            "row width mismatch"
        );

        let casted = row
            .into_iter()
            .zip(&table.schema.columns)
            .map(|(value, column)| Value::cast_for(column.data_type, value))
            .collect::<Result<Vec<Value>>>()?;

        table.pending.append_row(casted)?;
        if table.pending.rows() >= self.storage.options().row_group_size {
            self.flush_table(table_name)?;
        }
        Ok(())
    }

    pub fn insert_rows(&mut self, table_name: &str, rows: Vec<Vec<Value>>) -> Result<()> {
        let row_group_size = self.storage.options().row_group_size;
        let (schema, flushed_groups) = {
            let table = self
                .tables
                .get_mut(table_name)
                .with_context(|| format!("unknown table {table_name}"))?;
            let mut flushed_groups = Vec::new();

            for row in rows {
                ensure!(
                    row.len() == table.schema.columns.len(),
                    "row width mismatch"
                );
                let casted = row
                    .into_iter()
                    .zip(&table.schema.columns)
                    .map(|(value, column)| Value::cast_for(column.data_type, value))
                    .collect::<Result<Vec<Value>>>()?;

                table.pending.append_row(casted)?;
                if table.pending.rows() >= row_group_size {
                    flushed_groups.push(table.pending.take_row_group());
                }
            }

            (table.schema.clone(), flushed_groups)
        };

        for row_group in &flushed_groups {
            self.storage
                .append_row_group(table_name, row_group, &schema)?;
        }

        if !flushed_groups.is_empty() {
            let table = self
                .tables
                .get_mut(table_name)
                .with_context(|| format!("unknown table {table_name}"))?;
            table.row_groups.extend(
                flushed_groups
                    .into_iter()
                    .map(StoredRowGroup::from_row_group),
            );
        }

        Ok(())
    }

    pub fn insert_columnar_batch(
        &mut self,
        table_name: &str,
        mut batch: ColumnarBatch,
    ) -> Result<()> {
        let row_group_size = self.storage.options().row_group_size;
        let (schema, flushed_groups) = {
            let table = self
                .tables
                .get_mut(table_name)
                .with_context(|| format!("unknown table {table_name}"))?;
            batch.validate_against(&table.schema)?;

            let mut flushed_groups = Vec::new();
            while !batch.is_empty() {
                if table.pending.rows() == 0 && batch.rows() >= row_group_size {
                    let direct_batch = batch.take_prefix(row_group_size)?;
                    flushed_groups.push(row_group_from_columnar_batch(direct_batch)?);
                    continue;
                }

                let room = row_group_size - table.pending.rows();
                let chunk = batch.take_prefix(room.min(batch.rows()))?;
                table.pending.append_columnar_batch(chunk)?;
                if table.pending.rows() >= row_group_size {
                    flushed_groups.push(table.pending.take_row_group());
                }
            }

            (table.schema.clone(), flushed_groups)
        };

        for row_group in &flushed_groups {
            self.storage
                .append_row_group(table_name, row_group, &schema)?;
        }

        if !flushed_groups.is_empty() {
            let table = self
                .tables
                .get_mut(table_name)
                .with_context(|| format!("unknown table {table_name}"))?;
            table.row_groups.extend(
                flushed_groups
                    .into_iter()
                    .map(StoredRowGroup::from_row_group),
            );
        }

        Ok(())
    }

    pub fn flush_table(&mut self, table_name: &str) -> Result<()> {
        let table = self
            .tables
            .get_mut(table_name)
            .with_context(|| format!("unknown table {table_name}"))?;
        if table.pending.rows() == 0 {
            return Ok(());
        }

        let row_group = table.pending.take_row_group();
        self.storage
            .append_row_group(table_name, &row_group, &table.schema)?;
        table
            .row_groups
            .push(StoredRowGroup::from_row_group(row_group));
        table.pending = PendingBatch::new(&table.schema, self.storage.options().row_group_size);
        Ok(())
    }

    pub fn flush_all(&mut self) -> Result<()> {
        let table_names = self.tables.keys().cloned().collect::<Vec<_>>();
        for table_name in table_names {
            self.flush_table(&table_name)?;
        }
        Ok(())
    }

    pub fn execute_sql(&mut self, sql: &str) -> Result<Vec<QueryResult>> {
        let commands = parse_sql(sql)?;
        let mut results = Vec::new();

        for command in commands {
            match command {
                Command::CreateTable(schema) => {
                    self.create_table(schema)?;
                    results.push(QueryResult::empty());
                }
                Command::Insert(plan) => {
                    self.execute_insert(plan)?;
                    results.push(QueryResult::empty());
                }
                Command::Select(plan) => {
                    self.flush_table(&plan.table_name)?;
                    results.push(self.execute_select(plan)?);
                }
            }
        }

        Ok(results)
    }

    fn execute_insert(&mut self, plan: InsertPlan) -> Result<()> {
        self.insert_rows(&plan.table_name, plan.rows)
    }

    fn execute_select(&self, plan: SelectPlan) -> Result<QueryResult> {
        let table = self
            .tables
            .get(&plan.table_name)
            .with_context(|| format!("unknown table {}", plan.table_name))?;

        if plan.projections.len() == 1
            && matches!(plan.projections[0].expr, ProjectionExpr::Column(ref name) if name == "*")
        {
            return self.select_all(table, &plan);
        }

        let aggregate = plan
            .projections
            .iter()
            .any(|projection| matches!(projection.expr, ProjectionExpr::Aggregate { .. }));

        if aggregate || !plan.group_by.is_empty() {
            self.select_aggregate(table, &plan)
        } else {
            self.select_rows(table, &plan)
        }
    }

    fn select_all(&self, table: &Table, plan: &SelectPlan) -> Result<QueryResult> {
        ensure!(
            plan.group_by.is_empty(),
            "SELECT * does not support GROUP BY"
        );
        let columns = table
            .schema
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let filters = compile_filters(&table.schema, &plan.filters)?;
        let mapped = self.storage.mapped_bytes();
        let mut rows = Vec::new();

        scan_table(table, mapped, &filters, |row_group, row| {
            rows.push(
                (0..table.schema.columns.len())
                    .map(|idx| row_group.columns[idx].value_at(row))
                    .collect(),
            );
            Ok(())
        })?;

        apply_order_by_and_limit(&mut rows, &columns, plan.order_by.as_ref(), plan.limit)?;
        Ok(QueryResult { columns, rows })
    }

    fn select_rows(&self, table: &Table, plan: &SelectPlan) -> Result<QueryResult> {
        let filters = compile_filters(&table.schema, &plan.filters)?;
        let projections = compile_projections(&table.schema, &plan.group_by, &plan.projections)?;
        let columns = projections
            .iter()
            .map(|projection| projection.alias.clone())
            .collect::<Vec<_>>();
        let mapped = self.storage.mapped_bytes();
        let mut rows = Vec::new();

        scan_table(table, mapped, &filters, |row_group, row| {
            let mut output = Vec::with_capacity(projections.len());
            for projection in &projections {
                match projection.expr {
                    CompiledProjectionExpr::Column { column_index, .. } => {
                        output.push(row_group.columns[column_index].value_at(row));
                    }
                    CompiledProjectionExpr::Aggregate { .. } => {
                        bail!("aggregate projection requires GROUP BY pipeline")
                    }
                }
            }
            rows.push(output);
            Ok(())
        })?;

        apply_order_by_and_limit(&mut rows, &columns, plan.order_by.as_ref(), plan.limit)?;
        Ok(QueryResult { columns, rows })
    }

    fn select_aggregate(&self, table: &Table, plan: &SelectPlan) -> Result<QueryResult> {
        let filters = compile_filters(&table.schema, &plan.filters)?;
        let projections = compile_projections(&table.schema, &plan.group_by, &plan.projections)?;
        let columns = projections
            .iter()
            .map(|projection| projection.alias.clone())
            .collect::<Vec<_>>();
        let group_indexes = plan
            .group_by
            .iter()
            .map(|name| column_index(&table.schema, name))
            .collect::<Result<Vec<_>>>()?;
        let mapped = self.storage.mapped_bytes();

        let mut rows = if group_indexes.is_empty() {
            let partials = collect_row_group_aggregates(table, mapped, |row_group| {
                aggregate_row_group_all(row_group, &filters, &projections)
            })?;
            let mut states = projections.iter().map(AggState::new).collect::<Vec<_>>();
            let mut saw_row = false;

            for partial in partials {
                let Some(partial_states) = partial else {
                    continue;
                };
                saw_row = true;
                for (state, partial_state) in states.iter_mut().zip(partial_states) {
                    state.merge(partial_state)?;
                }
            }

            if !saw_row {
                states = projections.iter().map(AggState::new).collect();
            }

            vec![build_output_row(&projections, &[], states)?]
        } else {
            let partials = collect_row_group_aggregates(table, mapped, |row_group| {
                aggregate_row_group_grouped(row_group, &filters, &projections, &group_indexes)
            })?;
            let mut global_groups: FxHashMap<Vec<Value>, Vec<AggState>> = FxHashMap::default();

            for partial in partials {
                for (key, local_states) in partial {
                    let states = global_groups
                        .entry(key)
                        .or_insert_with(|| projections.iter().map(AggState::new).collect());
                    for (state, local_state) in states.iter_mut().zip(local_states) {
                        state.merge(local_state)?;
                    }
                }
            }

            global_groups
                .into_iter()
                .map(|(key, states)| build_output_row(&projections, &key, states))
                .collect::<Result<Vec<_>>>()?
        };

        apply_order_by_and_limit(&mut rows, &columns, plan.order_by.as_ref(), plan.limit)?;
        Ok(QueryResult { columns, rows })
    }
}

#[derive(Debug)]
struct CompiledFilter {
    column_index: usize,
    op: CompareOp,
    value: Value,
}

#[derive(Debug)]
enum RowSelection {
    All(usize),
    Indexes(Vec<u32>),
}

impl RowSelection {
    fn is_empty(&self) -> bool {
        match self {
            Self::All(rows) => *rows == 0,
            Self::Indexes(rows) => rows.is_empty(),
        }
    }

    fn try_for_each<F>(&self, mut visitor: F) -> Result<()>
    where
        F: FnMut(usize) -> Result<()>,
    {
        match self {
            Self::All(rows) => {
                for row in 0..*rows {
                    visitor(row)?;
                }
            }
            Self::Indexes(rows) => {
                for row in rows {
                    visitor(*row as usize)?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct CompiledProjection {
    alias: String,
    expr: CompiledProjectionExpr,
}

#[derive(Debug, Clone)]
enum CompiledProjectionExpr {
    Column {
        column_index: usize,
        group_key_index: Option<usize>,
    },
    Aggregate {
        kind: AggregateKind,
        column_index: Option<usize>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum LocalKeyPart {
    Int64(i64),
    Float64(OrderedFloat<f64>),
    Bool(bool),
    String(String),
    StringCode(u32),
}

impl LocalKeyPart {
    fn from_column(column: &ColumnData, row: usize) -> Result<Self> {
        match column {
            ColumnData::Int64(values) => Ok(Self::Int64(values[row])),
            ColumnData::Float64(values) => Ok(Self::Float64(values[row])),
            ColumnData::Bool(values) => Ok(Self::Bool(values[row])),
            ColumnData::StringPlain(values) => Ok(Self::String(values[row].clone())),
            ColumnData::StringDict { codes, .. } => Ok(Self::StringCode(codes[row])),
        }
    }
}

fn compile_filters(schema: &Schema, filters: &[Filter]) -> Result<Vec<CompiledFilter>> {
    let mut compiled = filters
        .iter()
        .map(|filter| {
            let column_index = column_index(schema, &filter.column)?;
            let data_type = schema.columns[column_index].data_type;
            let value = Value::cast_for(data_type, filter.value.clone())?;
            Ok(CompiledFilter {
                column_index,
                op: filter.op,
                value,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    compiled.sort_by_key(filter_priority);
    Ok(compiled)
}

fn filter_priority(filter: &CompiledFilter) -> u8 {
    match filter.op {
        CompareOp::Eq => 0,
        CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => 1,
        CompareOp::NotEq => 2,
    }
}

fn compile_projections(
    schema: &Schema,
    group_by: &[String],
    projections: &[Projection],
) -> Result<Vec<CompiledProjection>> {
    projections
        .iter()
        .map(|projection| {
            let expr = match &projection.expr {
                ProjectionExpr::Column(name) => CompiledProjectionExpr::Column {
                    column_index: column_index(schema, name)?,
                    group_key_index: group_by
                        .iter()
                        .position(|group_column| group_column == name),
                },
                ProjectionExpr::Aggregate { kind, column } => CompiledProjectionExpr::Aggregate {
                    kind: *kind,
                    column_index: column
                        .as_ref()
                        .map(|name| column_index(schema, name))
                        .transpose()?,
                },
            };
            Ok(CompiledProjection {
                alias: projection.alias.clone(),
                expr,
            })
        })
        .collect()
}

fn column_index(schema: &Schema, column_name: &str) -> Result<usize> {
    schema
        .columns
        .iter()
        .position(|column| column.name == column_name)
        .with_context(|| format!("unknown column {column_name}"))
}

fn scan_table<F>(
    table: &Table,
    mapped: Option<&[u8]>,
    filters: &[CompiledFilter],
    mut visitor: F,
) -> Result<()>
where
    F: FnMut(&RowGroup, usize) -> Result<()>,
{
    for row_group in &table.row_groups {
        let row_group = row_group.get(&table.schema, mapped)?;
        let selection = select_rows(row_group, filters);
        selection.try_for_each(|row| visitor(row_group, row))?;
    }
    Ok(())
}

fn should_parallelize_aggregate(table: &Table) -> bool {
    table.row_groups.len() >= PARALLEL_AGGREGATE_ROW_GROUPS
}

fn collect_row_group_aggregates<T, F>(
    table: &Table,
    mapped: Option<&[u8]>,
    worker: F,
) -> Result<Vec<T>>
where
    T: Send,
    F: Fn(&RowGroup) -> Result<T> + Sync + Send,
{
    let partials = if should_parallelize_aggregate(table) {
        table
            .row_groups
            .par_iter()
            .map(|row_group| row_group.get(&table.schema, mapped).and_then(&worker))
            .collect::<Vec<_>>()
    } else {
        table
            .row_groups
            .iter()
            .map(|row_group| row_group.get(&table.schema, mapped).and_then(&worker))
            .collect::<Vec<_>>()
    };

    partials.into_iter().collect()
}

fn aggregate_row_group_all(
    row_group: &RowGroup,
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

fn aggregate_row_group_grouped(
    row_group: &RowGroup,
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
            .map(|index| LocalKeyPart::from_column(&row_group.columns[*index], row))
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

fn row_group_matches(row_group: &RowGroup, filters: &[CompiledFilter]) -> bool {
    filters.iter().all(|filter| match filter.op {
        CompareOp::Eq => stats_may_match_eq(&row_group.stats[filter.column_index], &filter.value),
        CompareOp::Lt => stats_may_match_lt(&row_group.stats[filter.column_index], &filter.value),
        CompareOp::Lte => stats_may_match_lte(&row_group.stats[filter.column_index], &filter.value),
        CompareOp::Gt => stats_may_match_gt(&row_group.stats[filter.column_index], &filter.value),
        CompareOp::Gte => stats_may_match_gte(&row_group.stats[filter.column_index], &filter.value),
        CompareOp::NotEq => true,
    })
}

fn stats_may_match_eq(stats: &crate::storage::ColumnStats, value: &Value) -> bool {
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

fn stats_may_match_lt(stats: &crate::storage::ColumnStats, value: &Value) -> bool {
    match &stats.min {
        Some(min) => min
            .compare(value)
            .is_none_or(|ordering| ordering == Ordering::Less),
        None => true,
    }
}

fn stats_may_match_lte(stats: &crate::storage::ColumnStats, value: &Value) -> bool {
    match &stats.min {
        Some(min) => min
            .compare(value)
            .is_none_or(|ordering| ordering != Ordering::Greater),
        None => true,
    }
}

fn stats_may_match_gt(stats: &crate::storage::ColumnStats, value: &Value) -> bool {
    match &stats.max {
        Some(max) => max
            .compare(value)
            .is_none_or(|ordering| ordering == Ordering::Greater),
        None => true,
    }
}

fn stats_may_match_gte(stats: &crate::storage::ColumnStats, value: &Value) -> bool {
    match &stats.max {
        Some(max) => max
            .compare(value)
            .is_none_or(|ordering| ordering != Ordering::Less),
        None => true,
    }
}

fn select_rows(row_group: &RowGroup, filters: &[CompiledFilter]) -> RowSelection {
    if !row_group_matches(row_group, filters) {
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

fn apply_filter_to_all_rows(row_group: &RowGroup, filter: &CompiledFilter) -> Vec<u32> {
    let mut selected = Vec::with_capacity(row_group.rows);
    let column = &row_group.columns[filter.column_index];
    for row in 0..row_group.rows {
        if column_matches_filter(column, row, filter) {
            selected.push(row as u32);
        }
    }
    selected
}

fn refine_selection(row_group: &RowGroup, filter: &CompiledFilter, selected: &mut Vec<u32>) {
    let column = &row_group.columns[filter.column_index];
    selected.retain(|row| column_matches_filter(column, *row as usize, filter));
}

fn column_matches_filter(column: &ColumnData, row: usize, filter: &CompiledFilter) -> bool {
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
    }
}

fn materialize_group_key(
    row_group: &RowGroup,
    group_indexes: &[usize],
    key: &[LocalKeyPart],
) -> Result<Vec<Value>> {
    let mut values = Vec::with_capacity(key.len());
    for (group_index, part) in group_indexes.iter().zip(key) {
        let value = match (&row_group.columns[*group_index], part) {
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

fn build_output_row(
    projections: &[CompiledProjection],
    key: &[Value],
    states: Vec<AggState>,
) -> Result<Vec<Value>> {
    let mut row = Vec::with_capacity(projections.len());
    for (projection, state) in projections.iter().zip(states) {
        match projection.expr {
            CompiledProjectionExpr::Column {
                group_key_index, ..
            } => {
                let key_index = group_key_index.context("grouped column missing from key")?;
                row.push(key[key_index].clone());
            }
            CompiledProjectionExpr::Aggregate { .. } => row.push(state.finish()?),
        }
    }
    Ok(row)
}

fn apply_order_by_and_limit(
    rows: &mut Vec<Vec<Value>>,
    columns: &[String],
    order_by: Option<&OrderByPlan>,
    limit: Option<usize>,
) -> Result<()> {
    if let Some(order_by) = order_by {
        let order_index = columns
            .iter()
            .position(|column| column == &order_by.field)
            .with_context(|| {
                format!("ORDER BY field {} not found in result set", order_by.field)
            })?;

        if let Some(limit) = limit {
            if limit < rows.len() {
                let pivot = limit;
                rows.select_nth_unstable_by(pivot, |lhs, rhs| {
                    compare_result_rows(lhs, rhs, order_index, order_by.descending)
                });
                rows.truncate(limit);
            }
        }

        rows.sort_by(|lhs, rhs| compare_result_rows(lhs, rhs, order_index, order_by.descending));
    }

    if let Some(limit) = limit {
        if rows.len() > limit {
            rows.truncate(limit);
        }
    }
    Ok(())
}

fn compare_result_rows(lhs: &[Value], rhs: &[Value], index: usize, descending: bool) -> Ordering {
    let ordering = lhs[index].compare(&rhs[index]).unwrap_or(Ordering::Equal);
    if descending {
        ordering.reverse()
    } else {
        ordering
    }
}

#[derive(Debug, Clone)]
enum AggState {
    Passthrough,
    Count(i64),
    Sum(f64),
    Min(Option<Value>),
    Max(Option<Value>),
    Avg { sum: f64, count: i64 },
}

impl AggState {
    fn new(projection: &CompiledProjection) -> Self {
        match projection.expr {
            CompiledProjectionExpr::Column { .. } => Self::Passthrough,
            CompiledProjectionExpr::Aggregate { kind, .. } => match kind {
                AggregateKind::Count => Self::Count(0),
                AggregateKind::Sum => Self::Sum(0.0),
                AggregateKind::Min => Self::Min(None),
                AggregateKind::Max => Self::Max(None),
                AggregateKind::Avg => Self::Avg { sum: 0.0, count: 0 },
            },
        }
    }

    fn update(
        &mut self,
        projection: &CompiledProjection,
        row_group: &RowGroup,
        row: usize,
    ) -> Result<()> {
        match (self, &projection.expr) {
            (Self::Passthrough, CompiledProjectionExpr::Column { .. }) => Ok(()),
            (
                Self::Count(count),
                CompiledProjectionExpr::Aggregate {
                    kind: AggregateKind::Count,
                    ..
                },
            ) => {
                *count += 1;
                Ok(())
            }
            (
                Self::Sum(sum),
                CompiledProjectionExpr::Aggregate {
                    kind: AggregateKind::Sum,
                    column_index: Some(index),
                },
            ) => {
                *sum += row_group.columns[*index]
                    .numeric_at(row)
                    .context("SUM expects a numeric column")?;
                Ok(())
            }
            (
                Self::Min(current),
                CompiledProjectionExpr::Aggregate {
                    kind: AggregateKind::Min,
                    column_index: Some(index),
                },
            ) => {
                let value = row_group.columns[*index].value_at(row);
                if current
                    .as_ref()
                    .is_none_or(|existing| value.compare(existing) == Some(Ordering::Less))
                {
                    *current = Some(value);
                }
                Ok(())
            }
            (
                Self::Max(current),
                CompiledProjectionExpr::Aggregate {
                    kind: AggregateKind::Max,
                    column_index: Some(index),
                },
            ) => {
                let value = row_group.columns[*index].value_at(row);
                if current
                    .as_ref()
                    .is_none_or(|existing| value.compare(existing) == Some(Ordering::Greater))
                {
                    *current = Some(value);
                }
                Ok(())
            }
            (
                Self::Avg { sum, count },
                CompiledProjectionExpr::Aggregate {
                    kind: AggregateKind::Avg,
                    column_index: Some(index),
                },
            ) => {
                *sum += row_group.columns[*index]
                    .numeric_at(row)
                    .context("AVG expects a numeric column")?;
                *count += 1;
                Ok(())
            }
            _ => bail!("invalid aggregate configuration"),
        }
    }

    fn merge(&mut self, other: AggState) -> Result<()> {
        match (self, other) {
            (Self::Passthrough, Self::Passthrough) => Ok(()),
            (Self::Count(lhs), Self::Count(rhs)) => {
                *lhs += rhs;
                Ok(())
            }
            (Self::Sum(lhs), Self::Sum(rhs)) => {
                *lhs += rhs;
                Ok(())
            }
            (Self::Min(lhs), Self::Min(rhs)) => {
                if let Some(rhs) = rhs {
                    if lhs
                        .as_ref()
                        .is_none_or(|existing| rhs.compare(existing) == Some(Ordering::Less))
                    {
                        *lhs = Some(rhs);
                    }
                }
                Ok(())
            }
            (Self::Max(lhs), Self::Max(rhs)) => {
                if let Some(rhs) = rhs {
                    if lhs
                        .as_ref()
                        .is_none_or(|existing| rhs.compare(existing) == Some(Ordering::Greater))
                    {
                        *lhs = Some(rhs);
                    }
                }
                Ok(())
            }
            (
                Self::Avg {
                    sum: lhs_sum,
                    count: lhs_count,
                },
                Self::Avg {
                    sum: rhs_sum,
                    count: rhs_count,
                },
            ) => {
                *lhs_sum += rhs_sum;
                *lhs_count += rhs_count;
                Ok(())
            }
            _ => bail!("aggregate merge type mismatch"),
        }
    }

    fn finish(self) -> Result<Value> {
        match self {
            Self::Passthrough => bail!("passthrough state does not produce a value"),
            Self::Count(count) => Ok(Value::Int64(count)),
            Self::Sum(sum) => Ok(Value::Float64(OrderedFloat(sum))),
            Self::Min(Some(value)) | Self::Max(Some(value)) => Ok(value),
            Self::Min(None) | Self::Max(None) => bail!("aggregate had no values"),
            Self::Avg { sum, count } => {
                ensure!(count > 0, "AVG had no rows");
                Ok(Value::Float64(OrderedFloat(sum / count as f64)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::types::{BatchColumn, ColumnarBatch};

    #[test]
    fn persists_and_queries() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "narrowdb-test-{}.db",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));

        {
            let mut db = NarrowDb::open(
                &path,
                DbOptions {
                    row_group_size: 2,
                    sync_on_flush: true,
                },
            )?;
            db.execute_sql(
                "
                CREATE TABLE logs (ts TIMESTAMP, level TEXT, service TEXT, status INT, duration REAL);
                INSERT INTO logs VALUES
                    (1, 'info', 'api', 200, 12.0),
                    (2, 'error', 'api', 500, 120.0),
                    (3, 'error', 'worker', 503, 250.0);
                ",
            )?;
            db.flush_all()?;
        }

        let mut db = NarrowDb::open(&path, DbOptions::default())?;
        let results = db.execute_sql(
            "SELECT service, COUNT(*) AS errors FROM logs WHERE level = 'error' GROUP BY service ORDER BY errors DESC LIMIT 2;",
        )?;
        assert_eq!(results.last().unwrap().rows.len(), 2);
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn preserves_projection_order_in_group_by_results() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "narrowdb-test-order-{}.db",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));

        let mut db = NarrowDb::open(&path, DbOptions::default())?;
        db.execute_sql(
            "
            CREATE TABLE logs (service TEXT, host TEXT, duration REAL);
            INSERT INTO logs VALUES
                ('api', 'a', 10.0),
                ('api', 'b', 20.0),
                ('worker', 'a', 30.0);
            ",
        )?;

        let results = db.execute_sql(
            "SELECT host, service, COUNT(*) AS total FROM logs GROUP BY service, host ORDER BY host DESC LIMIT 10;",
        )?;
        let result = results.last().unwrap();
        assert_eq!(result.columns, vec!["host", "service", "total"]);
        assert_eq!(result.rows[0][0].to_string(), "b");
        assert_eq!(result.rows[0][1].to_string(), "api");

        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn ingests_columnar_batches() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "narrowdb-test-columnar-{}.db",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));

        let mut db = NarrowDb::open(&path, DbOptions::default())?;
        db.execute_sql(
            "CREATE TABLE logs (ts TIMESTAMP, service TEXT, status INT, duration REAL);",
        )?;

        db.insert_columnar_batch(
            "logs",
            ColumnarBatch::new(vec![
                BatchColumn::Timestamp(vec![1, 2, 3]),
                BatchColumn::String(vec![
                    "api".to_string(),
                    "api".to_string(),
                    "worker".to_string(),
                ]),
                BatchColumn::Int64(vec![200, 500, 503]),
                BatchColumn::Float64(vec![10.0, 100.0, 250.0]),
            ])?,
        )?;
        db.flush_all()?;

        let results = db.execute_sql(
            "SELECT service, COUNT(*) AS total FROM logs WHERE status >= 500 GROUP BY service ORDER BY total DESC LIMIT 10;",
        )?;
        let result = results.last().unwrap();
        assert_eq!(result.rows.len(), 2);

        std::fs::remove_file(path)?;
        Ok(())
    }
}
