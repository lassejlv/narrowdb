mod aggregate;
mod bitmap;
mod compile;
mod scan;

use std::cmp::Ordering;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail, ensure};
use rayon::{ThreadPool, ThreadPoolBuilder};
use rustc_hash::FxHashMap;

use crate::sql::{
    AggregateKind, AlterTableOperationPlan, AlterTablePlan, ArithmeticOp, Command, EvalPlan,
    OrderByPlan, ProjectionExpr, ScalarExpr, SelectPlan, parse_sql,
};
use crate::storage::{
    DbOptions, PendingBatch, Storage, StoredRowGroup, Table, row_group_from_columnar_batch,
};
use crate::types::{ColumnarBatch, DataType, Schema, Value};
use ordered_float::OrderedFloat;

use aggregate::AggState;
use compile::{
    CompiledProjection, CompiledProjectionExpr, CompiledScalarExpr, column_index, compile_filters,
    compile_projections, required_column_indexes,
};
use scan::{
    aggregate_row_group_all, aggregate_row_group_grouped, collect_row_group_aggregates,
    count_row_group_bool, count_row_group_int64, parallel_scan_table,
};

pub struct NarrowDb {
    inner: Arc<RwLock<DbInner>>,
    flush_handle: Mutex<Option<FlushHandle>>,
}

struct DbInner {
    query_runtime: QueryRuntime,
    storage: Storage,
    tables: FxHashMap<String, Table>,
}

struct FlushHandle {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

#[derive(Clone)]
struct QueryRuntime {
    threads: usize,
    pool: Arc<ThreadPool>,
}

static QUERY_THREAD_POOLS: OnceLock<Mutex<FxHashMap<usize, Arc<ThreadPool>>>> = OnceLock::new();

#[derive(Clone, Copy)]
enum FastGroupedCountKeyType {
    Int64,
    Bool,
}

struct FastGroupedCountPlan {
    key_index: usize,
    key_type: FastGroupedCountKeyType,
    order_by_count_desc: bool,
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

impl QueryRuntime {
    fn new(configured_threads: Option<usize>) -> Result<Self> {
        let threads = match configured_threads {
            Some(0) => bail!("query_threads must be greater than zero"),
            Some(threads) => threads,
            None => std::thread::available_parallelism()
                .map(|threads| threads.get())
                .unwrap_or(1),
        };

        Ok(Self {
            threads,
            pool: query_thread_pool(threads)?,
        })
    }
}

fn query_thread_pool(threads: usize) -> Result<Arc<ThreadPool>> {
    let pools = QUERY_THREAD_POOLS.get_or_init(|| Mutex::new(FxHashMap::default()));
    let mut guard = pools
        .lock()
        .map_err(|_| anyhow!("query thread pool lock poisoned"))?;
    if let Some(pool) = guard.get(&threads) {
        return Ok(pool.clone());
    }

    let pool = Arc::new(
        ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|index| format!("narrowdb-query-{index}"))
            .build()
            .context("failed to build query thread pool")?,
    );
    guard.insert(threads, pool.clone());
    Ok(pool)
}

impl NarrowDb {
    pub fn open(path: impl AsRef<Path>, options: DbOptions) -> Result<Self> {
        let query_runtime = QueryRuntime::new(options.query_threads)?;
        let auto_flush_interval = options.auto_flush_interval;
        let (storage, tables) = Storage::open(path, options)?;
        let inner = Arc::new(RwLock::new(DbInner {
            query_runtime,
            storage,
            tables,
        }));

        let flush_handle = if let Some(interval) = auto_flush_interval {
            let weak = Arc::downgrade(&inner);
            let stop = Arc::new(AtomicBool::new(false));
            let stop_clone = stop.clone();
            let thread = std::thread::spawn(move || {
                background_flush_loop(weak, interval, stop_clone);
            });
            Some(FlushHandle {
                stop,
                thread: Some(thread),
            })
        } else {
            None
        };

        Ok(Self {
            inner,
            flush_handle: Mutex::new(flush_handle),
        })
    }

    pub fn path(&self) -> Result<std::path::PathBuf> {
        let inner = self.inner.read().map_err(|_| anyhow!("lock poisoned"))?;
        Ok(inner.storage.path().to_path_buf())
    }

    pub fn create_table(&self, schema: Schema) -> Result<()> {
        let mut inner = self.inner.write().map_err(|_| anyhow!("lock poisoned"))?;
        inner.create_table_inner(schema)
    }

    pub fn insert_row(&self, table_name: &str, row: Vec<Value>) -> Result<()> {
        let mut inner = self.inner.write().map_err(|_| anyhow!("lock poisoned"))?;
        inner.insert_row_inner(table_name, row)
    }

    pub fn insert_rows(&self, table_name: &str, rows: Vec<Vec<Value>>) -> Result<()> {
        let mut inner = self.inner.write().map_err(|_| anyhow!("lock poisoned"))?;
        inner.insert_rows_inner(table_name, rows)
    }

    pub fn insert_columnar_batch(&self, table_name: &str, batch: ColumnarBatch) -> Result<()> {
        let mut inner = self.inner.write().map_err(|_| anyhow!("lock poisoned"))?;
        inner.insert_columnar_batch_inner(table_name, batch)
    }

    pub fn flush_table(&self, table_name: &str) -> Result<()> {
        let mut inner = self.inner.write().map_err(|_| anyhow!("lock poisoned"))?;
        inner.flush_table_inner(table_name)
    }

    pub fn flush_all(&self) -> Result<()> {
        let mut inner = self.inner.write().map_err(|_| anyhow!("lock poisoned"))?;
        inner.flush_all_inner()
    }

    pub fn execute_one(&self, sql: &str) -> Result<QueryResult> {
        let mut results = self.execute_sql(sql)?;
        results.pop().context("no result from SQL statement")
    }

    pub fn execute_sql(&self, sql: &str) -> Result<Vec<QueryResult>> {
        let commands = parse_sql(sql)?;
        let mut inner = self.inner.write().map_err(|_| anyhow!("lock poisoned"))?;
        inner.execute_commands(commands)
    }

    /// Execute read-only SQL (SELECT / expressions) under a shared read lock.
    /// Multiple `query` calls can run concurrently. Pending unflushed rows
    /// are invisible — call `flush_all` first if you need to see them.
    pub fn query(&self, sql: &str) -> Result<Vec<QueryResult>> {
        let commands = parse_sql(sql)?;
        let inner = self.inner.read().map_err(|_| anyhow!("lock poisoned"))?;
        inner.execute_read_commands(commands)
    }
}

impl Drop for NarrowDb {
    fn drop(&mut self) {
        if let Ok(mut handle) = self.flush_handle.lock() {
            if let Some(fh) = handle.take() {
                fh.stop.store(true, AtomicOrdering::Relaxed);
                if let Some(ref t) = fh.thread {
                    t.thread().unpark();
                }
                if let Some(t) = fh.thread {
                    let _ = t.join();
                }
            }
        }
        if let Ok(mut inner) = self.inner.write() {
            let _ = inner.flush_all_inner();
        }
    }
}

fn background_flush_loop(
    inner: std::sync::Weak<RwLock<DbInner>>,
    interval: Duration,
    stop: Arc<AtomicBool>,
) {
    while !stop.load(AtomicOrdering::Relaxed) {
        std::thread::park_timeout(interval);
        if stop.load(AtomicOrdering::Relaxed) {
            break;
        }
        let Some(inner) = inner.upgrade() else { break };
        let Ok(mut guard) = inner.write() else { break };
        let _ = guard.flush_all_inner();
    }
}

// ---------------------------------------------------------------------------
// DbInner — all the actual logic, called under appropriate locks
// ---------------------------------------------------------------------------

impl DbInner {
    fn create_table_inner(&mut self, schema: Schema) -> Result<()> {
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

    fn insert_row_inner(&mut self, table_name: &str, row: Vec<Value>) -> Result<()> {
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
            self.flush_table_inner(table_name)?;
        }
        Ok(())
    }

    fn insert_rows_inner(&mut self, table_name: &str, rows: Vec<Vec<Value>>) -> Result<()> {
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

    fn insert_columnar_batch_inner(
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

    fn flush_table_inner(&mut self, table_name: &str) -> Result<()> {
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
        self.storage.remap()?;
        table
            .row_groups
            .push(StoredRowGroup::from_row_group(row_group));
        table.pending = PendingBatch::new(&table.schema, self.storage.options().row_group_size);
        Ok(())
    }

    fn flush_all_inner(&mut self) -> Result<()> {
        let table_names = self.tables.keys().cloned().collect::<Vec<_>>();
        for table_name in table_names {
            self.flush_table_inner(&table_name)?;
        }
        Ok(())
    }

    fn alter_table_inner(&mut self, plan: AlterTablePlan) -> Result<()> {
        self.flush_all_inner()?;

        match plan.operation {
            AlterTableOperationPlan::RenameTable { new_table_name } => {
                self.rename_table_inner(&plan.table_name, &new_table_name, plan.if_exists)
            }
            AlterTableOperationPlan::RenameColumn {
                old_column_name,
                new_column_name,
            } => self.rename_column_inner(
                &plan.table_name,
                &old_column_name,
                &new_column_name,
                plan.if_exists,
            ),
        }
    }

    fn rename_table_inner(
        &mut self,
        table_name: &str,
        new_table_name: &str,
        if_exists: bool,
    ) -> Result<()> {
        if !self.tables.contains_key(table_name) {
            if if_exists {
                return Ok(());
            }
            bail!("unknown table {table_name}");
        }

        if table_name == new_table_name {
            return Ok(());
        }

        ensure!(
            !self.tables.contains_key(new_table_name),
            "table {new_table_name} already exists"
        );

        let mut table = self
            .tables
            .remove(table_name)
            .expect("table existence checked above");
        table.schema.table_name = new_table_name.to_string();
        self.tables.insert(new_table_name.to_string(), table);
        self.storage.rewrite_with_tables(&mut self.tables)?;
        Ok(())
    }

    fn rename_column_inner(
        &mut self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
        if_exists: bool,
    ) -> Result<()> {
        let Some(table) = self.tables.get_mut(table_name) else {
            if if_exists {
                return Ok(());
            }
            bail!("unknown table {table_name}");
        };

        let Some(column_index) = table
            .schema
            .columns
            .iter()
            .position(|column| column.name == old_column_name)
        else {
            bail!("unknown column {old_column_name} in table {table_name}");
        };

        if old_column_name != new_column_name {
            ensure!(
                !table
                    .schema
                    .columns
                    .iter()
                    .any(|column| column.name == new_column_name),
                "column {new_column_name} already exists in table {table_name}"
            );
            table.schema.columns[column_index].name = new_column_name.to_string();
        }

        self.storage.rewrite_with_tables(&mut self.tables)?;
        Ok(())
    }

    fn drop_table_inner(&mut self, table_name: &str, if_exists: bool) -> Result<()> {
        self.flush_all_inner()?;

        if self.tables.remove(table_name).is_none() {
            if if_exists {
                return Ok(());
            }
            bail!("unknown table {table_name}");
        }

        self.storage.rewrite_with_tables(&mut self.tables)?;
        Ok(())
    }

    fn show_tables_inner(&self) -> QueryResult {
        let mut names = self.tables.keys().cloned().collect::<Vec<_>>();
        names.sort();
        QueryResult {
            columns: vec!["table_name".to_string()],
            rows: names
                .into_iter()
                .map(|name| vec![Value::String(name)])
                .collect(),
        }
    }

    fn describe_table_inner(&self, table_name: &str) -> Result<QueryResult> {
        let table = self
            .tables
            .get(table_name)
            .with_context(|| format!("unknown table {table_name}"))?;

        let rows = table
            .schema
            .columns
            .iter()
            .map(|column| {
                vec![
                    Value::String(column.name.clone()),
                    Value::String(data_type_to_sql_name(column.data_type).to_string()),
                ]
            })
            .collect();

        Ok(QueryResult {
            columns: vec!["column_name".to_string(), "data_type".to_string()],
            rows,
        })
    }

    /// Execute commands that include writes (INSERT, CREATE TABLE).
    /// Flushes before SELECTs for consistency.
    fn execute_commands(&mut self, commands: Vec<Command>) -> Result<Vec<QueryResult>> {
        let mut results = Vec::new();

        for command in commands {
            match command {
                Command::CreateTable(schema) => {
                    self.create_table_inner(schema)?;
                    results.push(QueryResult::empty());
                }
                Command::CreateTableIfNotExists(schema) => {
                    if !self.tables.contains_key(&schema.table_name) {
                        self.create_table_inner(schema)?;
                    }
                    results.push(QueryResult::empty());
                }
                Command::AlterTable(plan) => {
                    self.alter_table_inner(plan)?;
                    results.push(QueryResult::empty());
                }
                Command::DropTable(plan) => {
                    self.drop_table_inner(&plan.table_name, plan.if_exists)?;
                    results.push(QueryResult::empty());
                }
                Command::ShowTables => {
                    results.push(self.show_tables_inner());
                }
                Command::DescribeTable(table_name) => {
                    results.push(self.describe_table_inner(&table_name)?);
                }
                Command::Insert(plan) => {
                    self.insert_rows_inner(&plan.table_name, plan.rows)?;
                    results.push(QueryResult::empty());
                }
                Command::Select(plan) => {
                    self.flush_table_inner(&plan.table_name)?;
                    results.push(self.execute_select(&plan)?);
                }
                Command::Eval(plan) => {
                    results.push(execute_eval(plan)?);
                }
            }
        }

        Ok(results)
    }

    /// Execute read-only commands (SELECT, Eval) under a read lock.
    /// Does NOT flush pending rows — they are invisible to these queries.
    fn execute_read_commands(&self, commands: Vec<Command>) -> Result<Vec<QueryResult>> {
        let mut results = Vec::new();

        for command in commands {
            match command {
                Command::Select(plan) => {
                    results.push(self.execute_select(&plan)?);
                }
                Command::Eval(plan) => {
                    results.push(execute_eval(plan)?);
                }
                Command::ShowTables => {
                    results.push(self.show_tables_inner());
                }
                Command::DescribeTable(table_name) => {
                    results.push(self.describe_table_inner(&table_name)?);
                }
                _ => bail!("unexpected write command in read path"),
            }
        }

        Ok(results)
    }

    fn execute_select(&self, plan: &SelectPlan) -> Result<QueryResult> {
        let table = self
            .tables
            .get(&plan.table_name)
            .with_context(|| format!("unknown table {}", plan.table_name))?;

        if plan.projections.len() == 1
            && matches!(plan.projections[0].expr, ProjectionExpr::Column(ref name) if name == "*")
        {
            return self.select_all(table, plan);
        }

        let aggregate = plan
            .projections
            .iter()
            .any(|projection| matches!(projection.expr, ProjectionExpr::Aggregate { .. }));

        if aggregate || !plan.group_by.is_empty() {
            self.select_aggregate(table, plan)
        } else {
            self.select_rows(table, plan)
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
        let query_threads = self.query_runtime.threads;

        let mut rows = self.query_runtime.pool.install(|| {
            parallel_scan_table(
                table,
                query_threads,
                mapped,
                &filters,
                &required_columns,
                |row_group, row| {
                    (0..col_count)
                        .map(|idx| row_group.column(idx).value_at(row))
                        .collect()
                },
            )
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
        let required_columns =
            required_column_indexes(table.schema.columns.len(), &filters, &projections, &[]);
        let mapped = self.storage.mapped_bytes();
        let query_threads = self.query_runtime.threads;

        let mut rows = self.query_runtime.pool.install(|| {
            parallel_scan_table(
                table,
                query_threads,
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
                                eval_compiled_scalar(scalar, row_group, row).unwrap_or(Value::Null)
                            }
                            CompiledProjectionExpr::Aggregate { .. } => unreachable!(),
                        })
                        .collect()
                },
            )
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
        let required_columns = required_column_indexes(
            table.schema.columns.len(),
            &filters,
            &projections,
            &group_indexes,
        );
        let mapped = self.storage.mapped_bytes();
        let query_threads = self.query_runtime.threads;
        let parallelize_rows = query_threads > 1 && table.row_groups.len() < query_threads;

        if let Some(fast_path) =
            classify_fast_grouped_count(table, plan, &projections, &group_indexes)
        {
            return self.select_fast_grouped_count(
                table,
                plan,
                &filters,
                &projections,
                &columns,
                &required_columns,
                mapped,
                query_threads,
                parallelize_rows,
                fast_path,
            );
        }

        let mut rows = if group_indexes.is_empty() {
            let partials = self.query_runtime.pool.install(|| {
                collect_row_group_aggregates(
                    table,
                    query_threads,
                    mapped,
                    &filters,
                    &required_columns,
                    |row_group| {
                        aggregate_row_group_all(
                            row_group,
                            &filters,
                            &projections,
                            query_threads,
                            parallelize_rows,
                        )
                    },
                )
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
            let partials = self.query_runtime.pool.install(|| {
                collect_row_group_aggregates(
                    table,
                    query_threads,
                    mapped,
                    &filters,
                    &required_columns,
                    |row_group| {
                        aggregate_row_group_grouped(
                            row_group,
                            &filters,
                            &projections,
                            &group_indexes,
                            query_threads,
                            parallelize_rows,
                        )
                    },
                )
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

    #[allow(clippy::too_many_arguments)]
    fn select_fast_grouped_count(
        &self,
        table: &Table,
        plan: &SelectPlan,
        filters: &[compile::CompiledFilter],
        projections: &[CompiledProjection],
        columns: &[String],
        required_columns: &[usize],
        mapped: Option<&[u8]>,
        query_threads: usize,
        parallelize_rows: bool,
        fast_path: FastGroupedCountPlan,
    ) -> Result<QueryResult> {
        let mut rows = match fast_path.key_type {
            FastGroupedCountKeyType::Int64 => {
                let partials = self.query_runtime.pool.install(|| {
                    collect_row_group_aggregates(
                        table,
                        query_threads,
                        mapped,
                        filters,
                        required_columns,
                        |row_group| {
                            count_row_group_int64(
                                row_group,
                                filters,
                                fast_path.key_index,
                                query_threads,
                                parallelize_rows,
                            )
                        },
                    )
                })?;

                let mut counts = FxHashMap::default();
                for partial in partials {
                    for (key, count) in partial {
                        *counts.entry(key).or_insert(0) += count;
                    }
                }

                materialize_int64_grouped_counts(
                    counts,
                    projections,
                    fast_path.order_by_count_desc,
                    plan.limit,
                )?
            }
            FastGroupedCountKeyType::Bool => {
                let partials = self.query_runtime.pool.install(|| {
                    collect_row_group_aggregates(
                        table,
                        query_threads,
                        mapped,
                        filters,
                        required_columns,
                        |row_group| count_row_group_bool(row_group, filters, fast_path.key_index),
                    )
                })?;

                let mut counts = [0_i64; 2];
                for partial in partials {
                    counts[0] += partial[0];
                    counts[1] += partial[1];
                }

                materialize_bool_grouped_counts(
                    counts,
                    projections,
                    fast_path.order_by_count_desc,
                    plan.limit,
                )?
            }
        };

        if !(fast_path.order_by_count_desc && plan.limit.is_some()) {
            apply_order_by_and_limit(&mut rows, columns, plan.order_by.as_ref(), plan.limit)?;
        }

        Ok(QueryResult {
            columns: columns.to_vec(),
            rows,
        })
    }
}

// ---------------------------------------------------------------------------
// Free functions
// ---------------------------------------------------------------------------

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

fn classify_fast_grouped_count(
    table: &Table,
    plan: &SelectPlan,
    projections: &[CompiledProjection],
    group_indexes: &[usize],
) -> Option<FastGroupedCountPlan> {
    if group_indexes.len() != 1 {
        return None;
    }

    let key_index = group_indexes[0];
    let key_type = match table.schema.columns[key_index].data_type {
        DataType::Int64 | DataType::Timestamp => FastGroupedCountKeyType::Int64,
        DataType::Bool => FastGroupedCountKeyType::Bool,
        _ => return None,
    };

    let mut saw_key = false;
    let mut count_aliases = Vec::new();
    for projection in projections {
        match &projection.expr {
            CompiledProjectionExpr::Column {
                column_index,
                group_key_index,
            } if *column_index == key_index && *group_key_index == Some(0) => {
                saw_key = true;
            }
            CompiledProjectionExpr::Aggregate {
                kind: AggregateKind::Count,
                column_index: None,
            } => count_aliases.push(projection.alias.clone()),
            _ => return None,
        }
    }

    if !saw_key || count_aliases.is_empty() {
        return None;
    }

    let order_by_count_desc = plan.order_by.as_ref().is_some_and(|order_by| {
        order_by.descending && count_aliases.iter().any(|alias| alias == &order_by.field)
    });

    Some(FastGroupedCountPlan {
        key_index,
        key_type,
        order_by_count_desc,
    })
}

fn materialize_int64_grouped_counts(
    counts: FxHashMap<i64, i64>,
    projections: &[CompiledProjection],
    order_by_count_desc: bool,
    limit: Option<usize>,
) -> Result<Vec<Vec<Value>>> {
    let mut entries = counts.into_iter().collect::<Vec<_>>();
    if order_by_count_desc {
        sort_and_limit_grouped_counts(&mut entries, limit);
    }

    entries
        .into_iter()
        .map(|(key, count)| build_fast_grouped_count_row(projections, Value::Int64(key), count))
        .collect()
}

fn materialize_bool_grouped_counts(
    counts: [i64; 2],
    projections: &[CompiledProjection],
    order_by_count_desc: bool,
    limit: Option<usize>,
) -> Result<Vec<Vec<Value>>> {
    let mut entries = Vec::new();
    if counts[0] > 0 {
        entries.push((false, counts[0]));
    }
    if counts[1] > 0 {
        entries.push((true, counts[1]));
    }
    if order_by_count_desc {
        sort_and_limit_grouped_counts(&mut entries, limit);
    }

    entries
        .into_iter()
        .map(|(key, count)| build_fast_grouped_count_row(projections, Value::Bool(key), count))
        .collect()
}

fn sort_and_limit_grouped_counts<K>(entries: &mut Vec<(K, i64)>, limit: Option<usize>) {
    if let Some(limit) = limit
        && limit < entries.len()
    {
        entries.select_nth_unstable_by(limit, |lhs, rhs| rhs.1.cmp(&lhs.1));
        entries.truncate(limit);
    }
    entries.sort_unstable_by(|lhs, rhs| rhs.1.cmp(&lhs.1));
}

fn build_fast_grouped_count_row(
    projections: &[CompiledProjection],
    key: Value,
    count: i64,
) -> Result<Vec<Value>> {
    let mut row = Vec::with_capacity(projections.len());
    for projection in projections {
        match &projection.expr {
            CompiledProjectionExpr::Column {
                group_key_index, ..
            } => {
                ensure!(
                    *group_key_index == Some(0),
                    "invalid grouped count fast path key projection"
                );
                row.push(key.clone());
            }
            CompiledProjectionExpr::Aggregate {
                kind: AggregateKind::Count,
                column_index: None,
            } => row.push(Value::Int64(count)),
            _ => bail!("invalid grouped count fast path projection"),
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
        (Value::Int64(l), Value::Float64(r)) => eval_arithmetic(
            Value::Float64(OrderedFloat(l as f64)),
            op,
            Value::Float64(r),
        ),
        (Value::Float64(l), Value::Int64(r)) => eval_arithmetic(
            Value::Float64(l),
            op,
            Value::Float64(OrderedFloat(r as f64)),
        ),
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

fn data_type_to_sql_name(data_type: DataType) -> &'static str {
    match data_type {
        DataType::Int64 => "INT",
        DataType::Float64 => "REAL",
        DataType::Bool => "BOOL",
        DataType::String => "TEXT",
        DataType::Timestamp => "TIMESTAMP",
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
            let db = NarrowDb::open(
                &path,
                DbOptions {
                    row_group_size: 2,
                    sync_on_flush: true,
                    ..DbOptions::default()
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

        let db = NarrowDb::open(&path, DbOptions::default())?;
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

        let db = NarrowDb::open(&path, DbOptions::default())?;
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

        let db = NarrowDb::open(&path, DbOptions::default())?;
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
        let results = db
            .execute_sql("SELECT name, price + 1.5 AS adjusted FROM products WHERE quantity > 4")?;
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

        let db = NarrowDb::open(&path, DbOptions::default())?;
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

    #[test]
    fn create_table_if_not_exists() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "narrowdb-test-ifne-{}.db",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));

        let db = NarrowDb::open(&path, DbOptions::default())?;
        db.execute_sql("CREATE TABLE logs (ts TIMESTAMP, level TEXT);")?;

        // Should not error
        db.execute_sql("CREATE TABLE IF NOT EXISTS logs (ts TIMESTAMP, level TEXT);")?;

        // Should still error without IF NOT EXISTS
        let err = db.execute_sql("CREATE TABLE logs (ts TIMESTAMP, level TEXT);");
        assert!(err.is_err());

        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn multi_filter_queries_match_expected_rows() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "narrowdb-test-multifilter-{}.db",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));

        let db = NarrowDb::open(&path, DbOptions::default())?;
        db.execute_sql(
            "CREATE TABLE logs (id INT, level TEXT, status INT, duration REAL);
             INSERT INTO logs VALUES
                 (1, 'info', 200, 10.0),
                 (2, 'error', 500, 650.0),
                 (3, 'error', 503, 750.0),
                 (4, 'warn', 503, 900.0),
                 (5, 'error', 504, 800.0);",
        )?;

        let results = db.execute_sql(
            "SELECT id FROM logs
             WHERE level = 'error' AND status >= 503 AND duration >= 750.0
             ORDER BY id;",
        )?;
        let result = results.last().unwrap();
        assert_eq!(
            result.rows,
            vec![vec![Value::Int64(3)], vec![Value::Int64(5)]]
        );

        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn grouped_string_queries_merge_across_row_groups() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "narrowdb-test-group-merge-{}.db",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));

        let db = NarrowDb::open(
            &path,
            DbOptions {
                row_group_size: 2,
                sync_on_flush: true,
                ..DbOptions::default()
            },
        )?;
        db.execute_sql(
            "CREATE TABLE logs (service TEXT, status INT);
             INSERT INTO logs VALUES
                 ('api', 200),
                 ('worker', 500),
                 ('worker', 503),
                 ('api', 500);",
        )?;
        db.flush_all()?;

        let results = db.execute_sql(
            "SELECT service, COUNT(*) AS total
             FROM logs
             GROUP BY service
             ORDER BY service;",
        )?;
        let result = results.last().unwrap();
        assert_eq!(
            result.rows,
            vec![
                vec![Value::String("api".to_string()), Value::Int64(2)],
                vec![Value::String("worker".to_string()), Value::Int64(2)],
            ]
        );

        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn aggregates_ignore_nulls_for_min_and_max() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "narrowdb-test-null-agg-{}.db",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));

        let db = NarrowDb::open(&path, DbOptions::default())?;
        db.execute_sql(
            "CREATE TABLE metrics (service TEXT, duration REAL, status INT);
             INSERT INTO metrics VALUES
                 ('api', 10.0, 200),
                 ('api', NULL, NULL),
                 ('worker', 25.5, 503),
                 ('worker', 3.0, 404);",
        )?;

        let results = db.execute_sql(
            "SELECT MIN(duration) AS min_duration, MAX(status) AS max_status
             FROM metrics;",
        )?;
        let result = results.last().unwrap();
        assert_eq!(
            result.rows,
            vec![vec![Value::Float64(OrderedFloat(3.0)), Value::Int64(503),]]
        );

        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn alters_table_and_column_names() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "narrowdb-test-alter-{}.db",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));

        {
            let db = NarrowDb::open(&path, DbOptions::default())?;
            db.execute_sql(
                "CREATE TABLE logs (ts TIMESTAMP, level TEXT);
                 INSERT INTO logs VALUES (1, 'info'), (2, 'error');",
            )?;

            db.execute_sql("ALTER TABLE logs RENAME TO events;")?;
            db.execute_sql("ALTER TABLE events RENAME COLUMN level TO severity;")?;

            let old_table = db.execute_sql("SELECT * FROM logs;");
            assert!(old_table.is_err());

            let results = db.execute_sql(
                "SELECT severity FROM events WHERE severity = 'error' ORDER BY severity;",
            )?;
            let result = results.last().unwrap();
            assert_eq!(result.columns, vec!["severity"]);
            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.rows[0][0], Value::String("error".to_string()));
        }

        let reopened = NarrowDb::open(&path, DbOptions::default())?;
        let results = reopened.execute_sql("SELECT severity FROM events ORDER BY severity;")?;
        let result = results.last().unwrap();
        assert_eq!(result.columns, vec!["severity"]);
        assert_eq!(result.rows.len(), 2);

        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn drops_tables() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "narrowdb-test-drop-{}.db",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));

        {
            let db = NarrowDb::open(&path, DbOptions::default())?;
            db.execute_sql("CREATE TABLE logs (ts TIMESTAMP, level TEXT);")?;
            db.execute_sql("DROP TABLE logs;")?;

            let select_missing = db.execute_sql("SELECT * FROM logs;");
            assert!(select_missing.is_err());

            db.execute_sql("DROP TABLE IF EXISTS logs;")?;
        }

        let reopened = NarrowDb::open(&path, DbOptions::default())?;
        reopened.execute_sql("CREATE TABLE logs (ts TIMESTAMP, level TEXT);")?;

        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn show_tables_lists_tables() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "narrowdb-test-show-{}.db",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));

        let db = NarrowDb::open(&path, DbOptions::default())?;
        db.execute_sql("CREATE TABLE logs (ts TIMESTAMP, level TEXT);")?;
        db.execute_sql("CREATE TABLE metrics (ts TIMESTAMP, value REAL);")?;

        let results = db.execute_sql("SHOW TABLES;")?;
        let result = results.last().unwrap();
        assert_eq!(result.columns, vec!["table_name"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::String("logs".to_string()));
        assert_eq!(result.rows[1][0], Value::String("metrics".to_string()));

        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn rejects_zero_query_threads() {
        let path = std::env::temp_dir().join(format!(
            "narrowdb-test-threads-invalid-{}.db",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock drift")
                .as_nanos()
        ));

        let result = NarrowDb::open(
            &path,
            DbOptions {
                query_threads: Some(0),
                ..DbOptions::default()
            },
        );
        assert!(result.is_err());
    }

    #[test]
    fn reuses_query_thread_pool_for_identical_sizes() -> Result<()> {
        let runtime_a = QueryRuntime::new(Some(2))?;
        let runtime_b = QueryRuntime::new(Some(2))?;
        assert_eq!(runtime_a.threads, 2);
        assert!(Arc::ptr_eq(&runtime_a.pool, &runtime_b.pool));
        Ok(())
    }

    #[test]
    fn query_results_match_between_single_and_multi_thread_execution() -> Result<()> {
        let base = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path_single = std::env::temp_dir().join(format!("narrowdb-test-single-{base}.db"));
        let path_multi = std::env::temp_dir().join(format!("narrowdb-test-multi-{base}.db"));
        let query = "SELECT service, COUNT(*) AS total
                     FROM logs
                     WHERE status >= 500
                     GROUP BY service
                     ORDER BY service;";

        let single = seeded_query_db(&path_single, Some(1))?;
        let multi = seeded_query_db(&path_multi, Some(4))?;

        let single_result = single.execute_sql(query)?.pop().unwrap();
        let multi_result = multi.execute_sql(query)?.pop().unwrap();
        assert_eq!(single_result.columns, multi_result.columns);
        assert_eq!(single_result.rows, multi_result.rows);

        std::fs::remove_file(path_single)?;
        std::fs::remove_file(path_multi)?;
        Ok(())
    }

    #[test]
    fn preserves_row_order_with_parallel_row_materialization() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "narrowdb-test-row-order-{}.db",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        let db = NarrowDb::open(
            &path,
            DbOptions {
                row_group_size: 131_072,
                query_threads: Some(4),
                ..DbOptions::default()
            },
        )?;
        db.execute_sql("CREATE TABLE logs (id INT, service TEXT, status INT);")?;

        let rows = 70_000;
        let batch = ColumnarBatch::new(vec![
            BatchColumn::Int64((0..rows).map(|value| value as i64).collect()),
            BatchColumn::String((0..rows).map(|_| "api".to_string()).collect()),
            BatchColumn::Int64((0..rows).map(|_| 500_i64).collect()),
        ])?;
        db.insert_columnar_batch("logs", batch)?;
        db.flush_all()?;

        let result = db
            .execute_sql("SELECT id FROM logs WHERE status >= 500;")?
            .pop()
            .unwrap();
        assert_eq!(result.rows.len(), rows);
        for (index, row) in result.rows.iter().enumerate() {
            assert_eq!(row, &vec![Value::Int64(index as i64)]);
        }

        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn describe_table_returns_schema() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "narrowdb-test-describe-{}.db",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));

        let db = NarrowDb::open(&path, DbOptions::default())?;
        db.execute_sql("CREATE TABLE logs (ts TIMESTAMP, level TEXT, status INT);")?;

        let results = db.execute_sql("DESCRIBE logs;")?;
        let result = results.last().unwrap();
        assert_eq!(result.columns, vec!["column_name", "data_type"]);
        assert_eq!(result.rows.len(), 3);
        assert_eq!(
            result.rows[0],
            vec![
                Value::String("ts".to_string()),
                Value::String("TIMESTAMP".to_string())
            ]
        );
        assert_eq!(
            result.rows[1],
            vec![
                Value::String("level".to_string()),
                Value::String("TEXT".to_string())
            ]
        );
        assert_eq!(
            result.rows[2],
            vec![
                Value::String("status".to_string()),
                Value::String("INT".to_string())
            ]
        );

        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn fast_grouped_count_returns_expected_top_k_for_int_keys() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "narrowdb-test-fast-grouped-count-{}.db",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        let db = NarrowDb::open(
            &path,
            DbOptions {
                row_group_size: 8,
                query_threads: Some(4),
                ..DbOptions::default()
            },
        )?;
        db.execute_sql("CREATE TABLE logs (request_id INT, status INT);")?;
        db.execute_sql(
            "INSERT INTO logs VALUES
                (1, 500), (1, 500), (1, 500), (1, 500), (1, 500),
                (2, 500), (2, 500), (2, 500), (2, 500),
                (3, 500), (3, 500), (3, 500),
                (4, 500), (4, 500);",
        )?;

        let result = db
            .execute_sql(
                "SELECT request_id, COUNT(*) AS total
                 FROM logs
                 WHERE status >= 500
                 GROUP BY request_id
                 ORDER BY total DESC
                 LIMIT 2;",
            )?
            .pop()
            .unwrap();
        assert_eq!(
            result.rows,
            vec![
                vec![Value::Int64(1), Value::Int64(5)],
                vec![Value::Int64(2), Value::Int64(4)],
            ]
        );

        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn fast_grouped_count_accepts_arbitrary_ties_for_top_k() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "narrowdb-test-fast-grouped-ties-{}.db",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        let db = NarrowDb::open(
            &path,
            DbOptions {
                row_group_size: 32,
                query_threads: Some(4),
                ..DbOptions::default()
            },
        )?;
        db.execute_sql("CREATE TABLE logs (request_id INT, status INT);")?;
        db.insert_columnar_batch(
            "logs",
            ColumnarBatch::new(vec![
                BatchColumn::Int64((0..64).map(|value| value as i64).collect()),
                BatchColumn::Int64((0..64).map(|_| 500_i64).collect()),
            ])?,
        )?;
        db.flush_all()?;

        let result = db
            .execute_sql(
                "SELECT request_id, COUNT(*) AS total
                 FROM logs
                 WHERE status >= 500
                 GROUP BY request_id
                 ORDER BY total DESC
                 LIMIT 25;",
            )?
            .pop()
            .unwrap();
        assert_eq!(result.rows.len(), 25);

        let mut ids = std::collections::BTreeSet::new();
        for row in &result.rows {
            assert_eq!(row[1], Value::Int64(1));
            let request_id = row[0].as_i64().expect("request_id should be int");
            assert!((0..64).contains(&(request_id as usize)));
            ids.insert(request_id);
        }
        assert_eq!(ids.len(), 25);

        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn fast_grouped_count_supports_bool_keys() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "narrowdb-test-fast-grouped-bool-{}.db",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        let db = NarrowDb::open(
            &path,
            DbOptions {
                query_threads: Some(4),
                ..DbOptions::default()
            },
        )?;
        db.execute_sql("CREATE TABLE logs (is_error BOOL);")?;
        db.execute_sql(
            "INSERT INTO logs VALUES
                (true), (true), (true),
                (false), (false);",
        )?;

        let result = db
            .execute_sql(
                "SELECT is_error, COUNT(*) AS total
                 FROM logs
                 GROUP BY is_error
                 ORDER BY total DESC;",
            )?
            .pop()
            .unwrap();
        assert_eq!(
            result.rows,
            vec![
                vec![Value::Bool(true), Value::Int64(3)],
                vec![Value::Bool(false), Value::Int64(2)],
            ]
        );

        std::fs::remove_file(path)?;
        Ok(())
    }

    fn seeded_query_db(path: &std::path::Path, query_threads: Option<usize>) -> Result<NarrowDb> {
        let db = NarrowDb::open(
            path,
            DbOptions {
                row_group_size: 16_384,
                query_threads,
                ..DbOptions::default()
            },
        )?;
        db.execute_sql("CREATE TABLE logs (service TEXT, status INT);")?;
        let rows = 80_000;
        let services = (0..rows)
            .map(|index| match index % 4 {
                0 => "api".to_string(),
                1 => "worker".to_string(),
                2 => "billing".to_string(),
                _ => "search".to_string(),
            })
            .collect::<Vec<_>>();
        let statuses = (0..rows)
            .map(|index| if index % 3 == 0 { 503 } else { 200 })
            .collect::<Vec<_>>();
        db.insert_columnar_batch(
            "logs",
            ColumnarBatch::new(vec![
                BatchColumn::String(services),
                BatchColumn::Int64(statuses),
            ])?,
        )?;
        db.flush_all()?;
        Ok(db)
    }
}
