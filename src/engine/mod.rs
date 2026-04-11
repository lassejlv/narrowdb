mod aggregate;
mod bitmap;
mod compile;
mod scan;

use std::cmp::Ordering;
use std::path::Path;

use anyhow::{Context, Result, bail, ensure};
use rustc_hash::FxHashMap;

use crate::sql::{
    ArithmeticOp, Command, EvalPlan, InsertPlan, OrderByPlan, ProjectionExpr, ScalarExpr,
    SelectPlan, parse_sql,
};
use crate::storage::{
    DbOptions, PendingBatch, Storage, StoredRowGroup, Table, row_group_from_columnar_batch,
};
use crate::types::{ColumnarBatch, Schema, Value};
use ordered_float::OrderedFloat;

use aggregate::AggState;
use compile::{
    CompiledProjection, CompiledProjectionExpr, CompiledScalarExpr, column_index, compile_filters,
    compile_projections, required_column_indexes,
};
use scan::{
    aggregate_row_group_all, aggregate_row_group_grouped, collect_row_group_aggregates,
    parallel_scan_table,
};

pub struct NarrowDb {
    storage: Storage,
    tables: FxHashMap<String, Table>,
}

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
                Command::Eval(plan) => {
                    results.push(execute_eval(plan)?);
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
        let required_columns = (0..table.schema.columns.len()).collect::<Vec<_>>();
        let mapped = self.storage.mapped_bytes();
        let col_count = table.schema.columns.len();

        let mut rows = parallel_scan_table(
            table,
            mapped,
            &filters,
            &required_columns,
            |row_group, row| {
                (0..col_count)
                    .map(|idx| row_group.column(idx).value_at(row))
                    .collect()
            },
        )?;

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
        let required_columns =
            required_column_indexes(table.schema.columns.len(), &filters, &projections, &[]);
        let mapped = self.storage.mapped_bytes();

        let mut rows = parallel_scan_table(
            table,
            mapped,
            &filters,
            &required_columns,
            |row_group, row| {
                projections
                    .iter()
                    .map(|proj| match &proj.expr {
                        CompiledProjectionExpr::Column { column_index, .. } => {
                            row_group.column(*column_index).value_at(row)
                        }
                        CompiledProjectionExpr::Scalar(scalar) => {
                            eval_compiled_scalar(scalar, row_group, row)
                                .unwrap_or(Value::Null)
                        }
                        CompiledProjectionExpr::Aggregate { .. } => unreachable!(),
                    })
                    .collect()
            },
        )?;

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
        let required_columns = required_column_indexes(
            table.schema.columns.len(),
            &filters,
            &projections,
            &group_indexes,
        );
        let mapped = self.storage.mapped_bytes();

        let mut rows = if group_indexes.is_empty() {
            let partials = collect_row_group_aggregates(
                table,
                mapped,
                &filters,
                &required_columns,
                |row_group| aggregate_row_group_all(row_group, &filters, &projections),
            )?;
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
            let partials = collect_row_group_aggregates(
                table,
                mapped,
                &filters,
                &required_columns,
                |row_group| {
                    aggregate_row_group_grouped(row_group, &filters, &projections, &group_indexes)
                },
            )?;
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

fn build_output_row(
    projections: &[CompiledProjection],
    key: &[Value],
    states: Vec<AggState>,
) -> Result<Vec<Value>> {
    let mut row = Vec::with_capacity(projections.len());
    for (projection, state) in projections.iter().zip(states) {
        match &projection.expr {
            CompiledProjectionExpr::Column {
                group_key_index, ..
            } => {
                let key_index = group_key_index.context("grouped column missing from key")?;
                row.push(key[key_index].clone());
            }
            CompiledProjectionExpr::Scalar(_) => {
                bail!("scalar expressions in aggregate queries are not yet supported")
            }
            CompiledProjectionExpr::Aggregate { .. } => row.push(state.finish()?),
        }
    }
    Ok(row)
}

fn eval_compiled_scalar(
    expr: &CompiledScalarExpr,
    row_group: &crate::storage::LoadedRowGroup<'_>,
    row: usize,
) -> Result<Value> {
    match expr {
        CompiledScalarExpr::Literal(value) => Ok(value.clone()),
        CompiledScalarExpr::ColumnRef(index) => Ok(row_group.column(*index).value_at(row)),
        CompiledScalarExpr::UnaryMinus(inner) => {
            match eval_compiled_scalar(inner, row_group, row)? {
                Value::Int64(v) => Ok(Value::Int64(-v)),
                Value::Float64(v) => Ok(Value::Float64(OrderedFloat(-v.into_inner()))),
                other => bail!("cannot negate {other:?}"),
            }
        }
        CompiledScalarExpr::BinaryOp { left, op, right } => {
            let l = eval_compiled_scalar(left, row_group, row)?;
            let r = eval_compiled_scalar(right, row_group, row)?;
            eval_arithmetic(l, *op, r)
        }
    }
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

fn execute_eval(plan: EvalPlan) -> Result<QueryResult> {
    let mut columns = Vec::new();
    let mut values = Vec::new();
    for (expr, alias) in plan.exprs {
        columns.push(alias);
        values.push(eval_scalar(&expr)?);
    }
    Ok(QueryResult {
        columns,
        rows: vec![values],
    })
}

fn eval_scalar(expr: &ScalarExpr) -> Result<Value> {
    match expr {
        ScalarExpr::Literal(value) => Ok(value.clone()),
        ScalarExpr::ColumnRef(name) => bail!("column reference '{name}' not valid without a table"),
        ScalarExpr::UnaryMinus(inner) => match eval_scalar(inner)? {
            Value::Int64(v) => Ok(Value::Int64(-v)),
            Value::Float64(v) => Ok(Value::Float64(OrderedFloat(-v.into_inner()))),
            other => bail!("cannot negate {other:?}"),
        },
        ScalarExpr::BinaryOp { left, op, right } => {
            let l = eval_scalar(left)?;
            let r = eval_scalar(right)?;
            eval_arithmetic(l, *op, r)
        }
    }
}

fn eval_arithmetic(left: Value, op: ArithmeticOp, right: Value) -> Result<Value> {
    match (left, right) {
        (Value::Int64(l), Value::Int64(r)) => {
            let result = match op {
                ArithmeticOp::Add => l.checked_add(r).context("integer overflow")?,
                ArithmeticOp::Sub => l.checked_sub(r).context("integer overflow")?,
                ArithmeticOp::Mul => l.checked_mul(r).context("integer overflow")?,
                ArithmeticOp::Div => {
                    ensure!(r != 0, "division by zero");
                    l / r
                }
                ArithmeticOp::Mod => {
                    ensure!(r != 0, "division by zero");
                    l % r
                }
            };
            Ok(Value::Int64(result))
        }
        (Value::Float64(l), Value::Float64(r)) => {
            let (l, r) = (l.into_inner(), r.into_inner());
            let result = match op {
                ArithmeticOp::Add => l + r,
                ArithmeticOp::Sub => l - r,
                ArithmeticOp::Mul => l * r,
                ArithmeticOp::Div => l / r,
                ArithmeticOp::Mod => l % r,
            };
            Ok(Value::Float64(OrderedFloat(result)))
        }
        (Value::Int64(l), Value::Float64(r)) => {
            eval_arithmetic(Value::Float64(OrderedFloat(l as f64)), op, Value::Float64(r))
        }
        (Value::Float64(l), Value::Int64(r)) => {
            eval_arithmetic(Value::Float64(l), op, Value::Float64(OrderedFloat(r as f64)))
        }
        (l, r) => bail!("unsupported arithmetic between {l:?} and {r:?}"),
    }
}

fn compare_result_rows(lhs: &[Value], rhs: &[Value], index: usize, descending: bool) -> Ordering {
    let ordering = lhs[index].compare(&rhs[index]).unwrap_or(Ordering::Equal);
    if descending {
        ordering.reverse()
    } else {
        ordering
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
    fn evaluates_scalar_expressions_in_select() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "narrowdb-test-scalar-{}.db",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));

        let mut db = NarrowDb::open(&path, DbOptions::default())?;
        db.execute_sql(
            "CREATE TABLE products (name TEXT, price REAL, quantity INT);
             INSERT INTO products VALUES
                 ('widget', 10.0, 5),
                 ('gadget', 25.0, 3),
                 ('gizmo', 7.5, 10);",
        )?;

        // Column * literal
        let results =
            db.execute_sql("SELECT price * 2 AS doubled FROM products ORDER BY doubled")?;
        let result = results.last().unwrap();
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0][0], Value::Float64(OrderedFloat(15.0)));
        assert_eq!(result.rows[1][0], Value::Float64(OrderedFloat(20.0)));
        assert_eq!(result.rows[2][0], Value::Float64(OrderedFloat(50.0)));

        // Column * column
        let results =
            db.execute_sql("SELECT price * quantity AS total FROM products ORDER BY total")?;
        let result = results.last().unwrap();
        assert_eq!(result.columns, vec!["total"]);
        assert_eq!(result.rows[0][0], Value::Float64(OrderedFloat(50.0)));
        assert_eq!(result.rows[1][0], Value::Float64(OrderedFloat(75.0)));
        assert_eq!(result.rows[2][0], Value::Float64(OrderedFloat(75.0)));

        // Expression with filter
        let results = db.execute_sql(
            "SELECT name, price + 1.5 AS adjusted FROM products WHERE quantity > 4",
        )?;
        let result = results.last().unwrap();
        assert_eq!(result.rows.len(), 2);

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
