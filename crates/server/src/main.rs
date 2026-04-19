use std::collections::HashMap;
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use futures::{Stream, stream};
use narrowdb::{DbOptions, NarrowDb, QueryResult, Value};
use pgwire::api::auth::md5pass::{Md5PasswordAuthStartupHandler, hash_md5_password};
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse,
    Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::tokio::process_socket;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, ObjectName, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value as SqlValue,
};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let config = ServerConfig::from_args(std::env::args().skip(1).collect())?;
    log_startup_warnings(&config);

    let db = Arc::new(NarrowDb::open(
        &config.db_path,
        DbOptions {
            row_group_size: config.row_group_size,
            sync_on_flush: config.sync_on_flush,
            query_threads: config.query_threads,
            ..DbOptions::default()
        },
    )?);

    let factory = Arc::new(PgServerFactory::new(db, &config));
    let listener = TcpListener::bind(&config.listen_addr)
        .await
        .with_context(|| format!("binding PostgreSQL listener on {}", config.listen_addr))?;

    log_info(&format!(
        "narrowdb-server listening on {} using {} as user {}",
        config.listen_addr, config.db_path, config.user
    ));
    log_info(
        "protocol support: simple query, extended query, prepared statements, parameter binding",
    );
    log_info(
        "authentication: PostgreSQL MD5 password auth; use trusted/private networks until stronger auth lands",
    );

    loop {
        let (socket, peer_addr) = listener.accept().await?;
        log_info(&format!("accepted pgwire client {peer_addr}"));
        let factory = Arc::clone(&factory);
        tokio::spawn(async move {
            if let Err(error) = process_socket(socket, None, factory).await {
                log_error(&format!("pgwire client {peer_addr} error: {error:#}"));
            }
        });
    }
}

fn log_info(message: &str) {
    eprintln!("[{}] INFO  {message}", timestamp_now());
}

fn log_warn(message: &str) {
    eprintln!("[{}] WARN  {message}", timestamp_now());
}

fn log_error(message: &str) {
    eprintln!("[{}] ERROR {message}", timestamp_now());
}

fn timestamp_now() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}

fn log_startup_warnings(config: &ServerConfig) {
    if config.user == "narrowdb" && config.password == "narrowdb" {
        log_warn("using default credentials `narrowdb` / `narrowdb`");
    }
    if !listen_addr_is_loopback(&config.listen_addr) {
        log_warn(
            "server is not bound to loopback; MD5 auth is only suitable for trusted/private networks",
        );
    }
}

fn listen_addr_is_loopback(listen_addr: &str) -> bool {
    listen_addr.starts_with("127.")
        || listen_addr.starts_with("localhost:")
        || listen_addr.starts_with("[::1]:")
        || listen_addr == "::1"
}

#[derive(Debug, Clone)]
struct ServerConfig {
    db_path: String,
    listen_addr: String,
    row_group_size: usize,
    sync_on_flush: bool,
    query_threads: Option<usize>,
    user: String,
    password: String,
}

impl ServerConfig {
    fn from_args(args: Vec<String>) -> Result<Self> {
        let db_path = if !args.is_empty() {
            args[0].clone()
        } else if let Ok(val) = std::env::var("NARROWDB_PATH") {
            val
        } else {
            bail!(
                "usage: narrowdb-server <db-file> [--listen 127.0.0.1:5433] [--row-group-size 16384] [--sync-on-flush true|false] [--query-threads N] [--user narrowdb] [--password secret]\n\nAll flags can also be set via environment variables: NARROWDB_PATH, NARROWDB_LISTEN, NARROWDB_ROW_GROUP_SIZE, NARROWDB_SYNC_ON_FLUSH, NARROWDB_QUERY_THREADS, NARROWDB_USER, NARROWDB_PASSWORD"
            );
        };

        let mut listen_addr = env_or("NARROWDB_LISTEN", "127.0.0.1:5433");
        let mut row_group_size: usize = env_or("NARROWDB_ROW_GROUP_SIZE", "16384")
            .parse()
            .context("NARROWDB_ROW_GROUP_SIZE must be an integer")?;
        let mut sync_on_flush = parse_bool_flag(&env_or("NARROWDB_SYNC_ON_FLUSH", "true"))?;
        let mut query_threads = std::env::var("NARROWDB_QUERY_THREADS")
            .ok()
            .map(|value| parse_query_threads_flag(&value))
            .transpose()?;
        let mut user = env_or("NARROWDB_USER", "narrowdb");
        let mut password = env_or("NARROWDB_PASSWORD", "narrowdb");
        let mut index = 1;

        while index < args.len() {
            match args[index].as_str() {
                "--listen" => {
                    index += 1;
                    listen_addr = args
                        .get(index)
                        .context("missing value for --listen")?
                        .clone();
                }
                "--row-group-size" => {
                    index += 1;
                    row_group_size = args
                        .get(index)
                        .context("missing value for --row-group-size")?
                        .parse()
                        .context("--row-group-size must be an integer")?;
                }
                "--sync-on-flush" => {
                    index += 1;
                    sync_on_flush = parse_bool_flag(
                        args.get(index)
                            .context("missing value for --sync-on-flush")?,
                    )?;
                }
                "--query-threads" => {
                    index += 1;
                    query_threads = Some(parse_query_threads_flag(
                        args.get(index)
                            .context("missing value for --query-threads")?,
                    )?);
                }
                "--user" => {
                    index += 1;
                    user = args.get(index).context("missing value for --user")?.clone();
                }
                "--password" => {
                    index += 1;
                    password = args
                        .get(index)
                        .context("missing value for --password")?
                        .clone();
                }
                other => bail!("unknown argument: {other}"),
            }
            index += 1;
        }

        ensure!(
            !db_path.trim().is_empty(),
            "database path must not be empty"
        );
        ensure!(
            row_group_size > 0,
            "row group size must be greater than zero"
        );
        ensure!(
            !listen_addr.trim().is_empty(),
            "listen address must not be empty"
        );
        ensure!(!user.trim().is_empty(), "user must not be empty");
        ensure!(!password.is_empty(), "password must not be empty");
        if let Some(parent) = Path::new(&db_path)
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            ensure!(
                parent.exists(),
                "database parent directory does not exist: {}",
                parent.display()
            );
        }

        Ok(Self {
            db_path,
            listen_addr,
            row_group_size,
            sync_on_flush,
            query_threads,
            user,
            password,
        })
    }

    fn database_name(&self) -> String {
        Path::new(&self.db_path)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or("narrowdb")
            .to_string()
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn parse_bool_flag(value: &str) -> Result<bool> {
    match value {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => bail!("expected boolean value, got {value}"),
    }
}

fn parse_query_threads_flag(value: &str) -> Result<usize> {
    let threads = value
        .parse::<usize>()
        .context("query threads must be an integer")?;
    ensure!(threads > 0, "query threads must be greater than zero");
    Ok(threads)
}

#[derive(Debug)]
struct StaticAuthSource {
    user: String,
    password: String,
    salt: [u8; 4],
}

#[async_trait]
impl AuthSource for StaticAuthSource {
    async fn get_password(&self, login_info: &LoginInfo) -> PgWireResult<Password> {
        let Some(user) = login_info.user() else {
            return Err(PgWireError::UserNameRequired);
        };
        if user != self.user {
            return Err(PgWireError::InvalidPassword(user.to_string()));
        }
        let hashed = hash_md5_password(user, &self.password, &self.salt);
        Ok(Password::new(Some(self.salt.to_vec()), hashed.into_bytes()))
    }
}

#[derive(Debug, Clone)]
struct PreparedSql {
    sql: String,
    statements: Vec<Statement>,
    parameter_types: Vec<Type>,
}

#[derive(Clone)]
struct NarrowQueryParser {
    db: Arc<NarrowDb>,
    database_name: String,
    user: String,
}

impl NarrowQueryParser {
    fn builtin_result_schema(&self, sql: &str, format: &Format) -> Option<Vec<FieldInfo>> {
        builtin_scalar_query(sql, &self.database_name, &self.user).map(|builtin| {
            vec![FieldInfo::new(
                builtin.column.into(),
                None,
                None,
                Type::TEXT,
                format.format_for(0),
            )]
        })
    }

    fn describe_query_schema(
        &self,
        stmt: &PreparedSql,
        format: &Format,
    ) -> PgWireResult<Vec<FieldInfo>> {
        if let Some(fields) = self.builtin_result_schema(&stmt.sql, format) {
            return Ok(fields);
        }

        let Some(command) = stmt.statements.first() else {
            return Ok(Vec::new());
        };
        match command {
            Statement::Query(query) => self.infer_result_schema_from_query(query, format),
            _ => Ok(Vec::new()),
        }
    }

    fn infer_parameter_types_from_statements(
        &self,
        statements: &[Statement],
        client_types: &[Option<Type>],
    ) -> PgWireResult<Vec<Type>> {
        let mut types = client_types
            .iter()
            .map(|ty| ty.clone().unwrap_or(Type::UNKNOWN))
            .collect::<Vec<_>>();

        for statement in statements {
            self.infer_parameter_types_from_statement(statement, &mut types)?;
        }

        while types.last().is_some_and(|ty| *ty == Type::UNKNOWN) {
            types.pop();
        }

        Ok(types)
    }

    fn infer_parameter_types_from_statement(
        &self,
        statement: &Statement,
        types: &mut Vec<Type>,
    ) -> PgWireResult<()> {
        match statement {
            Statement::Query(query) => self.infer_parameter_types_from_query(query, types),
            Statement::Insert(insert) => self.infer_parameter_types_from_insert(insert, types),
            _ => Ok(()),
        }
    }

    fn infer_parameter_types_from_insert(
        &self,
        insert: &sqlparser::ast::Insert,
        types: &mut Vec<Type>,
    ) -> PgWireResult<()> {
        let schema = self.load_table_schema(&insert.table_name)?;
        let target_columns = if insert.columns.is_empty() {
            schema.column_order.clone()
        } else {
            insert
                .columns
                .iter()
                .map(|column| column.value.clone())
                .collect()
        };

        let Some(source) = &insert.source else {
            return Ok(());
        };
        let SetExpr::Values(values) = source.body.as_ref() else {
            return Ok(());
        };

        for row in &values.rows {
            for (expr, column_name) in row.iter().zip(&target_columns) {
                let Some(column_type) = schema.columns.get(column_name) else {
                    continue;
                };
                self.infer_parameter_types_from_expr(expr, &QueryScope::default(), types)?;
                if let Some(index) = placeholder_index(expr) {
                    assign_parameter_type(types, index, column_type.clone())?;
                }
            }
        }

        Ok(())
    }

    fn infer_parameter_types_from_query(
        &self,
        query: &Query,
        types: &mut Vec<Type>,
    ) -> PgWireResult<()> {
        self.infer_parameter_types_from_set_expr(query.body.as_ref(), types)
    }

    fn infer_result_schema_from_query(
        &self,
        query: &Query,
        format: &Format,
    ) -> PgWireResult<Vec<FieldInfo>> {
        match query.body.as_ref() {
            SetExpr::Select(select) => self.infer_result_schema_from_select(select, format),
            _ => {
                let sql = bind_default_parameters(&query.to_string(), &[])?;
                let results = self.db.query(&sql).map_err(api_error)?;
                let Some(result) = results.into_iter().last() else {
                    return Ok(Vec::new());
                };
                Ok(infer_fields(&result, format))
            }
        }
    }

    fn infer_result_schema_from_select(
        &self,
        select: &Select,
        format: &Format,
    ) -> PgWireResult<Vec<FieldInfo>> {
        let scope = self.build_query_scope(&select.from)?;
        let mut fields = Vec::new();

        for item in &select.projection {
            match item {
                SelectItem::Wildcard(_) => {
                    for (name, ty) in &scope.projection_columns {
                        fields.push(FieldInfo::new(
                            name.clone(),
                            None,
                            None,
                            ty.clone(),
                            format.format_for(fields.len()),
                        ));
                    }
                }
                SelectItem::QualifiedWildcard(prefix, _) => {
                    let table_name = object_name_last_component(prefix);
                    if let Some(columns) = scope.table_projection_columns.get(&table_name) {
                        for (name, ty) in columns {
                            fields.push(FieldInfo::new(
                                name.clone(),
                                None,
                                None,
                                ty.clone(),
                                format.format_for(fields.len()),
                            ));
                        }
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    fields.push(FieldInfo::new(
                        expr.to_string(),
                        None,
                        None,
                        infer_projection_type(expr, &scope).unwrap_or(Type::TEXT),
                        format.format_for(fields.len()),
                    ));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    fields.push(FieldInfo::new(
                        alias.value.clone(),
                        None,
                        None,
                        infer_projection_type(expr, &scope).unwrap_or(Type::TEXT),
                        format.format_for(fields.len()),
                    ));
                }
            }
        }

        Ok(fields)
    }

    fn infer_parameter_types_from_set_expr(
        &self,
        set_expr: &SetExpr,
        types: &mut Vec<Type>,
    ) -> PgWireResult<()> {
        match set_expr {
            SetExpr::Select(select) => self.infer_parameter_types_from_select(select, types),
            SetExpr::Query(query) => self.infer_parameter_types_from_query(query, types),
            SetExpr::SetOperation { left, right, .. } => {
                self.infer_parameter_types_from_set_expr(left, types)?;
                self.infer_parameter_types_from_set_expr(right, types)
            }
            _ => Ok(()),
        }
    }

    fn infer_parameter_types_from_select(
        &self,
        select: &Select,
        types: &mut Vec<Type>,
    ) -> PgWireResult<()> {
        let scope = self.build_query_scope(&select.from)?;

        if let Some(selection) = &select.selection {
            self.infer_parameter_types_from_expr(selection, &scope, types)?;
        }
        if let Some(having) = &select.having {
            self.infer_parameter_types_from_expr(having, &scope, types)?;
        }
        if let GroupByExpr::Expressions(expressions, _) = &select.group_by {
            for expr in expressions {
                self.infer_parameter_types_from_expr(expr, &scope, types)?;
            }
        }
        for projection in &select.projection {
            match projection {
                SelectItem::UnnamedExpr(expr) => {
                    self.infer_parameter_types_from_expr(expr, &scope, types)?;
                }
                SelectItem::ExprWithAlias { expr, .. } => {
                    self.infer_parameter_types_from_expr(expr, &scope, types)?;
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn infer_parameter_types_from_expr(
        &self,
        expr: &Expr,
        scope: &QueryScope,
        types: &mut Vec<Type>,
    ) -> PgWireResult<()> {
        match expr {
            Expr::BinaryOp { left, op: _, right } => {
                let left_type = scope.resolve_expr_type(left);
                let right_type = scope.resolve_expr_type(right);

                if let Some(index) = placeholder_index(left)
                    && let Some(ty) = right_type
                {
                    assign_parameter_type(types, index, ty)?;
                }
                if let Some(index) = placeholder_index(right)
                    && let Some(ty) = left_type
                {
                    assign_parameter_type(types, index, ty)?;
                }

                self.infer_parameter_types_from_expr(left, scope, types)?;
                self.infer_parameter_types_from_expr(right, scope, types)
            }
            Expr::Nested(expr)
            | Expr::UnaryOp { expr, .. }
            | Expr::Cast { expr, .. }
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr) => self.infer_parameter_types_from_expr(expr, scope, types),
            Expr::Between {
                expr, low, high, ..
            } => {
                let base_type = scope.resolve_expr_type(expr);
                if let Some(index) = placeholder_index(low)
                    && let Some(ty) = base_type.clone()
                {
                    assign_parameter_type(types, index, ty)?;
                }
                if let Some(index) = placeholder_index(high)
                    && let Some(ty) = base_type
                {
                    assign_parameter_type(types, index, ty)?;
                }
                self.infer_parameter_types_from_expr(expr, scope, types)?;
                self.infer_parameter_types_from_expr(low, scope, types)?;
                self.infer_parameter_types_from_expr(high, scope, types)
            }
            Expr::InList { expr, list, .. } => {
                let base_type = scope.resolve_expr_type(expr);
                for item in list {
                    if let Some(index) = placeholder_index(item)
                        && let Some(ty) = base_type.clone()
                    {
                        assign_parameter_type(types, index, ty)?;
                    }
                    self.infer_parameter_types_from_expr(item, scope, types)?;
                }
                self.infer_parameter_types_from_expr(expr, scope, types)
            }
            _ => Ok(()),
        }
    }

    fn build_query_scope(&self, tables: &[TableWithJoins]) -> PgWireResult<QueryScope> {
        let mut scope = QueryScope::default();
        for table in tables {
            self.add_table_factor_to_scope(&table.relation, &mut scope)?;
            for join in &table.joins {
                self.add_table_factor_to_scope(&join.relation, &mut scope)?;
            }
        }
        Ok(scope)
    }

    fn add_table_factor_to_scope(
        &self,
        relation: &TableFactor,
        scope: &mut QueryScope,
    ) -> PgWireResult<()> {
        let TableFactor::Table { name, alias, .. } = relation else {
            return Ok(());
        };

        let schema = self.load_table_schema(name)?;
        let table_name = object_name_last_component(name);
        scope.add_projection_columns(&table_name, &schema);
        scope.add_table(&table_name, &schema);
        if let Some(alias) = alias {
            scope.add_table(&alias.name.value, &schema);
            if let Some(columns) = scope.table_projection_columns.get(&table_name).cloned() {
                scope
                    .table_projection_columns
                    .insert(alias.name.value.clone(), columns);
            }
        }
        Ok(())
    }

    fn load_table_schema(&self, table_name: &ObjectName) -> PgWireResult<TableSchema> {
        self.load_table_schema_by_name(&table_name.to_string())
    }

    fn load_table_schema_by_name(&self, table_name: &str) -> PgWireResult<TableSchema> {
        let describe = self
            .db
            .query(&format!("DESCRIBE {table_name};"))
            .map_err(api_error)?;
        let Some(result) = describe.into_iter().last() else {
            return Err(api_error("DESCRIBE returned no results"));
        };

        let mut columns = HashMap::new();
        let mut column_order = Vec::with_capacity(result.rows.len());
        for row in result.rows {
            let column_name = row
                .first()
                .and_then(Value::as_str)
                .ok_or_else(|| api_error("DESCRIBE column name missing"))?
                .to_string();
            let sql_type = row
                .get(1)
                .and_then(Value::as_str)
                .ok_or_else(|| api_error("DESCRIBE column type missing"))?;
            let pg_type = sql_type_to_pg_type(sql_type)
                .ok_or_else(|| api_error(format!("unsupported schema type {sql_type}")))?;
            column_order.push(column_name.clone());
            columns.insert(column_name, pg_type);
        }

        Ok(TableSchema {
            columns,
            column_order,
        })
    }
}

#[async_trait]
impl QueryParser for NarrowQueryParser {
    type Statement = PreparedSql;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let statements = parse_statements(sql)?;
        let parameter_types = self.infer_parameter_types_from_statements(&statements, types)?;
        Ok(PreparedSql {
            sql: sql.to_string(),
            statements,
            parameter_types,
        })
    }

    fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        Ok(stmt.parameter_types.clone())
    }

    fn get_result_schema(
        &self,
        stmt: &Self::Statement,
        column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        self.describe_query_schema(stmt, column_format.unwrap_or(&Format::UnifiedText))
    }
}

#[derive(Debug, Clone, Default)]
struct QueryScope {
    tables: HashMap<String, HashMap<String, Type>>,
    columns: HashMap<String, Type>,
    projection_columns: Vec<(String, Type)>,
    table_projection_columns: HashMap<String, Vec<(String, Type)>>,
}

impl QueryScope {
    fn add_table(&mut self, name: &str, schema: &TableSchema) {
        self.tables.insert(name.to_string(), schema.columns.clone());
        for (column, ty) in &schema.columns {
            self.columns
                .entry(column.clone())
                .or_insert_with(|| ty.clone());
        }
    }

    fn add_projection_columns(&mut self, table_name: &str, schema: &TableSchema) {
        let mut ordered = Vec::with_capacity(schema.column_order.len());
        for column_name in &schema.column_order {
            if let Some(ty) = schema.columns.get(column_name) {
                ordered.push((column_name.clone(), ty.clone()));
                self.projection_columns
                    .push((column_name.clone(), ty.clone()));
            }
        }
        self.table_projection_columns
            .insert(table_name.to_string(), ordered);
    }

    fn resolve_expr_type(&self, expr: &Expr) -> Option<Type> {
        match expr {
            Expr::Identifier(identifier) => self.columns.get(&identifier.value).cloned(),
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => self
                .tables
                .get(&parts[0].value)
                .and_then(|table| table.get(&parts[1].value))
                .cloned(),
            Expr::Nested(expr) | Expr::UnaryOp { expr, .. } | Expr::Cast { expr, .. } => {
                self.resolve_expr_type(expr)
            }
            Expr::Value(value) => literal_pg_type(value),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct TableSchema {
    columns: HashMap<String, Type>,
    column_order: Vec<String>,
}

fn object_name_last_component(name: &ObjectName) -> String {
    name.0
        .last()
        .map(|ident| ident.value.clone())
        .unwrap_or_else(|| name.to_string())
}

fn sql_type_to_pg_type(value: &str) -> Option<Type> {
    match value {
        "INT" | "TIMESTAMP" => Some(Type::INT8),
        "REAL" => Some(Type::FLOAT8),
        "BOOL" => Some(Type::BOOL),
        "TEXT" => Some(Type::TEXT),
        _ => None,
    }
}

fn literal_pg_type(value: &SqlValue) -> Option<Type> {
    match value {
        SqlValue::Number(number, _) => {
            if number.contains('.') || number.contains('e') || number.contains('E') {
                Some(Type::FLOAT8)
            } else {
                Some(Type::INT8)
            }
        }
        SqlValue::Boolean(_) => Some(Type::BOOL),
        SqlValue::SingleQuotedString(_)
        | SqlValue::DoubleQuotedString(_)
        | SqlValue::NationalStringLiteral(_)
        | SqlValue::EscapedStringLiteral(_)
        | SqlValue::UnicodeStringLiteral(_)
        | SqlValue::TripleSingleQuotedString(_)
        | SqlValue::TripleDoubleQuotedString(_)
        | SqlValue::SingleQuotedRawStringLiteral(_)
        | SqlValue::DoubleQuotedRawStringLiteral(_)
        | SqlValue::TripleSingleQuotedRawStringLiteral(_)
        | SqlValue::TripleDoubleQuotedRawStringLiteral(_)
        | SqlValue::DollarQuotedString(_) => Some(Type::TEXT),
        SqlValue::Null | SqlValue::Placeholder(_) => None,
        _ => None,
    }
}

fn infer_projection_type(expr: &Expr, scope: &QueryScope) -> Option<Type> {
    if let Some(ty) = scope.resolve_expr_type(expr) {
        return Some(ty);
    }

    match expr {
        Expr::Nested(expr) | Expr::UnaryOp { expr, .. } | Expr::Cast { expr, .. } => {
            infer_projection_type(expr, scope)
        }
        Expr::Value(value) => literal_pg_type(value),
        Expr::BinaryOp { left, right, .. } => {
            let left_type = infer_projection_type(left, scope);
            let right_type = infer_projection_type(right, scope);
            match (left_type, right_type) {
                (Some(Type::FLOAT4 | Type::FLOAT8), _) | (_, Some(Type::FLOAT4 | Type::FLOAT8)) => {
                    Some(Type::FLOAT8)
                }
                (
                    Some(Type::INT2 | Type::INT4 | Type::INT8),
                    Some(Type::INT2 | Type::INT4 | Type::INT8),
                ) => Some(Type::INT8),
                _ => None,
            }
        }
        Expr::Function(function) => {
            let function_name = function.name.to_string().to_ascii_uppercase();
            match function_name.as_str() {
                "COUNT" => Some(Type::INT8),
                "SUM" | "AVG" => Some(Type::FLOAT8),
                "MIN" | "MAX" => match &function.args {
                    FunctionArguments::List(list) => list.args.iter().find_map(|arg| match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                            infer_projection_type(expr, scope)
                        }
                        _ => None,
                    }),
                    _ => None,
                },
                _ => None,
            }
        }
        _ => None,
    }
}

fn placeholder_index(expr: &Expr) -> Option<usize> {
    let Expr::Value(SqlValue::Placeholder(value)) = expr else {
        return None;
    };
    value
        .strip_prefix('$')?
        .parse::<usize>()
        .ok()
        .and_then(|index| index.checked_sub(1))
}

fn assign_parameter_type(types: &mut Vec<Type>, index: usize, ty: Type) -> PgWireResult<()> {
    if ty == Type::UNKNOWN {
        return Ok(());
    }
    if index >= types.len() {
        types.resize(index + 1, Type::UNKNOWN);
    }

    let current = &mut types[index];
    if *current == Type::UNKNOWN {
        *current = ty;
        return Ok(());
    }
    if *current != ty {
        return Err(unsupported_error(&format!(
            "conflicting inferred parameter types for ${}: {} vs {}",
            index + 1,
            current.name(),
            ty.name()
        )));
    }
    Ok(())
}

fn bind_default_parameters(sql: &str, parameter_types: &[Type]) -> PgWireResult<String> {
    let literals = parameter_types
        .iter()
        .map(default_parameter_literal)
        .collect::<Vec<_>>();
    substitute_sql_parameters(sql, &literals)
}

fn default_parameter_literal(ty: &Type) -> String {
    match *ty {
        Type::BOOL => "FALSE".to_string(),
        Type::INT2 | Type::INT4 | Type::INT8 => "0".to_string(),
        Type::FLOAT4 | Type::FLOAT8 => "0.0".to_string(),
        _ => "''".to_string(),
    }
}

fn bind_portal_sql(sql: &str, portal: &Portal<PreparedSql>) -> PgWireResult<String> {
    let literals = (0..portal.parameter_len())
        .map(|index| parameter_literal(portal, index))
        .collect::<PgWireResult<Vec<_>>>()?;
    substitute_sql_parameters(sql, &literals)
}

fn parameter_literal(portal: &Portal<PreparedSql>, index: usize) -> PgWireResult<String> {
    let ty = portal
        .statement
        .statement
        .parameter_types
        .get(index)
        .cloned()
        .unwrap_or(Type::UNKNOWN);

    match ty {
        Type::BOOL => portal
            .parameter::<bool>(index, &ty)?
            .map(|value| {
                if value {
                    "TRUE".to_string()
                } else {
                    "FALSE".to_string()
                }
            })
            .map_or_else(|| Ok("NULL".to_string()), Ok),
        Type::INT2 => portal
            .parameter::<i16>(index, &ty)?
            .map(|value| value.to_string())
            .map_or_else(|| Ok("NULL".to_string()), Ok),
        Type::INT4 => portal
            .parameter::<i32>(index, &ty)?
            .map(|value| value.to_string())
            .map_or_else(|| Ok("NULL".to_string()), Ok),
        Type::INT8 => portal
            .parameter::<i64>(index, &ty)?
            .map(|value| value.to_string())
            .map_or_else(|| Ok("NULL".to_string()), Ok),
        Type::FLOAT4 => portal
            .parameter::<f32>(index, &ty)?
            .map(|value| float_literal(value as f64))
            .unwrap_or_else(|| Ok("NULL".to_string())),
        Type::FLOAT8 => portal
            .parameter::<f64>(index, &ty)?
            .map(float_literal)
            .unwrap_or_else(|| Ok("NULL".to_string())),
        _ => portal
            .parameter::<String>(index, &ty)?
            .map(|value| quote_sql_string(&value))
            .map_or_else(|| Ok("NULL".to_string()), Ok),
    }
}

fn float_literal(value: f64) -> PgWireResult<String> {
    if !value.is_finite() {
        return Err(unsupported_error(
            "non-finite floating point parameters are not supported",
        ));
    }
    Ok(value.to_string())
}

fn quote_sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn substitute_sql_parameters(sql: &str, literals: &[String]) -> PgWireResult<String> {
    let mut output = String::with_capacity(sql.len());
    let chars = sql.chars().collect::<Vec<_>>();
    let mut index = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_line_comment = false;
    let mut block_comment_depth = 0usize;

    while index < chars.len() {
        let ch = chars[index];
        let next = chars.get(index + 1).copied();

        if in_line_comment {
            output.push(ch);
            if ch == '\n' {
                in_line_comment = false;
            }
            index += 1;
            continue;
        }

        if block_comment_depth > 0 {
            output.push(ch);
            if ch == '/' && next == Some('*') {
                output.push('*');
                block_comment_depth += 1;
                index += 2;
                continue;
            }
            if ch == '*' && next == Some('/') {
                output.push('/');
                block_comment_depth -= 1;
                index += 2;
                continue;
            }
            index += 1;
            continue;
        }

        if in_single_quote {
            output.push(ch);
            if ch == '\'' {
                if next == Some('\'') {
                    output.push('\'');
                    index += 2;
                    continue;
                }
                in_single_quote = false;
            }
            index += 1;
            continue;
        }

        if in_double_quote {
            output.push(ch);
            if ch == '"' {
                in_double_quote = false;
            }
            index += 1;
            continue;
        }

        if ch == '\'' {
            in_single_quote = true;
            output.push(ch);
            index += 1;
            continue;
        }
        if ch == '"' {
            in_double_quote = true;
            output.push(ch);
            index += 1;
            continue;
        }
        if ch == '-' && next == Some('-') {
            in_line_comment = true;
            output.push(ch);
            output.push('-');
            index += 2;
            continue;
        }
        if ch == '/' && next == Some('*') {
            block_comment_depth = 1;
            output.push(ch);
            output.push('*');
            index += 2;
            continue;
        }
        if ch == '$' && next.is_some_and(|digit| digit.is_ascii_digit()) {
            let start = index + 1;
            let mut end = start;
            while chars.get(end).is_some_and(|digit| digit.is_ascii_digit()) {
                end += 1;
            }
            let parameter_index = chars[start..end]
                .iter()
                .collect::<String>()
                .parse::<usize>()
                .map_err(api_error)?
                .checked_sub(1)
                .ok_or_else(|| unsupported_error("parameter indexes start at $1"))?;
            let literal = literals
                .get(parameter_index)
                .ok_or_else(|| unsupported_error("missing bound parameter value"))?;
            output.push_str(literal);
            index = end;
            continue;
        }

        output.push(ch);
        index += 1;
    }

    Ok(output)
}

struct BuiltinScalarQuery {
    column: &'static str,
    value: String,
}

fn builtin_scalar_query(
    query: &str,
    database_name: &str,
    user: &str,
) -> Option<BuiltinScalarQuery> {
    let trimmed = query.trim().trim_end_matches(';').trim();
    let normalized = trimmed.to_ascii_uppercase();

    match normalized.as_str() {
        "SHOW SERVER_VERSION" => Some(BuiltinScalarQuery {
            column: "server_version",
            value: env!("CARGO_PKG_VERSION").to_string(),
        }),
        "SHOW CLIENT_ENCODING" => Some(BuiltinScalarQuery {
            column: "client_encoding",
            value: "UTF8".to_string(),
        }),
        "SHOW SERVER_ENCODING" => Some(BuiltinScalarQuery {
            column: "server_encoding",
            value: "UTF8".to_string(),
        }),
        "SHOW TRANSACTION_READ_ONLY" => Some(BuiltinScalarQuery {
            column: "transaction_read_only",
            value: "off".to_string(),
        }),
        "SHOW STANDARD_CONFORMING_STRINGS" => Some(BuiltinScalarQuery {
            column: "standard_conforming_strings",
            value: "on".to_string(),
        }),
        "SHOW INTEGER_DATETIMES" => Some(BuiltinScalarQuery {
            column: "integer_datetimes",
            value: "on".to_string(),
        }),
        "SHOW APPLICATION_NAME" => Some(BuiltinScalarQuery {
            column: "application_name",
            value: "".to_string(),
        }),
        "SHOW IS_SUPERUSER" => Some(BuiltinScalarQuery {
            column: "is_superuser",
            value: "off".to_string(),
        }),
        "SHOW SESSION_AUTHORIZATION" => Some(BuiltinScalarQuery {
            column: "session_authorization",
            value: user.to_string(),
        }),
        "SHOW DATESTYLE" => Some(BuiltinScalarQuery {
            column: "DateStyle",
            value: "ISO, MDY".to_string(),
        }),
        "SELECT VERSION()" => Some(BuiltinScalarQuery {
            column: "version",
            value: format!("narrowdb-server {} (pgwire)", env!("CARGO_PKG_VERSION")),
        }),
        "SELECT CURRENT_DATABASE()" => Some(BuiltinScalarQuery {
            column: "current_database",
            value: database_name.to_string(),
        }),
        "SELECT CURRENT_USER" | "SELECT CURRENT_USER()" => Some(BuiltinScalarQuery {
            column: "current_user",
            value: user.to_string(),
        }),
        _ => None,
    }
}

struct PgServerFactory {
    backend: Arc<NarrowDbBackend>,
    startup: Arc<Md5PasswordAuthStartupHandler<StaticAuthSource, DefaultServerParameterProvider>>,
}

impl PgServerFactory {
    fn new(db: Arc<NarrowDb>, config: &ServerConfig) -> Self {
        let query_parser = Arc::new(NarrowQueryParser {
            db: db.clone(),
            database_name: config.database_name(),
            user: config.user.clone(),
        });
        let backend = Arc::new(NarrowDbBackend {
            db,
            query_parser,
            database_name: config.database_name(),
            user: config.user.clone(),
        });

        let mut parameters = DefaultServerParameterProvider::default();
        parameters.server_version = format!("16.6-narrowdb-{}", env!("CARGO_PKG_VERSION"));

        let startup = Arc::new(Md5PasswordAuthStartupHandler::new(
            Arc::new(StaticAuthSource {
                user: config.user.clone(),
                password: config.password.clone(),
                salt: [17, 29, 43, 71],
            }),
            Arc::new(parameters),
        ));

        Self { backend, startup }
    }
}

impl PgWireServerHandlers for PgServerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.backend.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.backend.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.startup.clone()
    }
}

struct NarrowDbBackend {
    db: Arc<NarrowDb>,
    query_parser: Arc<NarrowQueryParser>,
    database_name: String,
    user: String,
}

#[async_trait]
impl SimpleQueryHandler for NarrowDbBackend {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        self.execute_sql_with_format(query, &Format::UnifiedText)
    }
}

#[async_trait]
impl ExtendedQueryHandler for NarrowDbBackend {
    type Statement = PreparedSql;
    type QueryParser = NarrowQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let rendered_sql = bind_portal_sql(&portal.statement.statement.sql, portal)?;
        let mut responses =
            self.execute_sql_with_format(&rendered_sql, &portal.result_column_format)?;
        if responses.len() != 1 {
            return Err(unsupported_error(
                "extended query protocol only supports a single statement at a time",
            ));
        }
        Ok(responses.remove(0))
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let server_param_types = self.query_parser.get_parameter_types(&stmt.statement)?;
        let param_types = (0usize..server_param_types.len().max(stmt.parameter_types.len()))
            .map(|idx| {
                stmt.parameter_types
                    .get(idx)
                    .cloned()
                    .and_then(|ty| ty)
                    .or_else(|| server_param_types.get(idx).cloned())
                    .unwrap_or(Type::UNKNOWN)
            })
            .collect::<Vec<_>>();
        let fields = self
            .query_parser
            .get_result_schema(&stmt.statement, Some(&Format::UnifiedText))?;
        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let fields = self.query_parser.get_result_schema(
            &portal.statement.statement,
            Some(&portal.result_column_format),
        )?;
        Ok(DescribePortalResponse::new(fields))
    }
}

impl NarrowDbBackend {
    fn execute_sql_with_format(&self, query: &str, format: &Format) -> PgWireResult<Vec<Response>> {
        if let Some(response) = self.handle_builtin_query(query, format)? {
            return Ok(vec![response]);
        }

        let commands = parse_statements(query)?;
        let results = self.db.execute_sql(query).map_err(api_error)?;

        if commands.len() != results.len() {
            return Err(api_error("command/result length mismatch"));
        }

        commands
            .into_iter()
            .zip(results)
            .map(|(command, result)| self.command_to_response(&command, result, format))
            .collect()
    }

    fn handle_builtin_query(&self, query: &str, format: &Format) -> PgWireResult<Option<Response>> {
        if query
            .trim()
            .trim_end_matches(';')
            .trim()
            .to_ascii_uppercase()
            .starts_with("SET ")
        {
            return Ok(Some(Response::Execution(Tag::new("SET"))));
        }

        if let Some(builtin) = builtin_scalar_query(query, &self.database_name, &self.user) {
            return self.single_text_response(builtin.column, &builtin.value, format);
        }

        Ok(None)
    }

    fn single_text_response(
        &self,
        column: &str,
        value: &str,
        format: &Format,
    ) -> PgWireResult<Option<Response>> {
        let fields = Arc::new(vec![FieldInfo::new(
            column.into(),
            None,
            None,
            Type::TEXT,
            format.format_for(0),
        )]);
        let mut encoder = DataRowEncoder::new(fields.clone());
        encoder.encode_field(&value).map_err(api_error)?;
        let rows = stream::iter(vec![Ok(encoder.take_row())]);
        Ok(Some(Response::Query(QueryResponse::new(fields, rows))))
    }

    fn command_to_response(
        &self,
        command: &Statement,
        result: QueryResult,
        format: &Format,
    ) -> PgWireResult<Response> {
        if result.columns.is_empty() {
            let tag = match command {
                Statement::CreateTable(_) => Tag::new("CREATE TABLE"),
                Statement::AlterTable { .. } => Tag::new("ALTER TABLE"),
                Statement::Drop { object_type, .. }
                    if *object_type == sqlparser::ast::ObjectType::Table =>
                {
                    Tag::new("DROP TABLE")
                }
                Statement::Insert(insert) => {
                    Tag::new("INSERT").with_rows(insert_values_len(insert))
                }
                Statement::Query(_) => Tag::new("SELECT"),
                _ => Tag::new(statement_tag(command)),
            };
            return Ok(Response::Execution(tag));
        }

        Ok(Response::Query(query_result_to_response(result, format)?))
    }
}

fn parse_statements(query: &str) -> PgWireResult<Vec<Statement>> {
    let dialect = SQLiteDialect {};
    Parser::parse_sql(&dialect, query).map_err(api_error)
}

fn insert_values_len(insert: &sqlparser::ast::Insert) -> usize {
    insert
        .source
        .as_ref()
        .and_then(|query| match query.body.as_ref() {
            SetExpr::Values(values) => Some(values.rows.len()),
            _ => None,
        })
        .unwrap_or(0)
}

fn statement_tag(statement: &Statement) -> &'static str {
    match statement {
        Statement::CreateTable(_) => "CREATE TABLE",
        Statement::AlterTable { .. } => "ALTER TABLE",
        Statement::Drop { object_type, .. }
            if *object_type == sqlparser::ast::ObjectType::Table =>
        {
            "DROP TABLE"
        }
        Statement::Insert(_) => "INSERT",
        Statement::Query(_) => "SELECT",
        _ => "OK",
    }
}

fn query_result_to_response(result: QueryResult, format: &Format) -> PgWireResult<QueryResponse> {
    let fields = Arc::new(infer_fields(&result, format));
    let rows = encode_query_rows(&result, fields.clone())?;
    Ok(QueryResponse::new(fields, rows))
}

fn encode_query_rows(
    result: &QueryResult,
    fields: Arc<Vec<FieldInfo>>,
) -> PgWireResult<impl Stream<Item = PgWireResult<DataRow>> + Send + 'static> {
    let mut encoded_rows = Vec::with_capacity(result.rows.len());
    let mut encoder = DataRowEncoder::new(fields.clone());

    for row in &result.rows {
        for (index, value) in row.iter().enumerate() {
            encode_value(&mut encoder, fields[index].datatype(), value)?;
        }
        encoded_rows.push(Ok(encoder.take_row()));
    }

    Ok(stream::iter(encoded_rows))
}

fn encode_value(encoder: &mut DataRowEncoder, data_type: &Type, value: &Value) -> PgWireResult<()> {
    match value {
        Value::Int64(value) => encoder.encode_field(value),
        Value::Float64(value) => encoder.encode_field(&value.into_inner()),
        Value::Bool(value) => encoder.encode_field(value),
        Value::String(value) => encoder.encode_field(value),
        Value::Null => match *data_type {
            Type::INT8 => encoder.encode_field(&None::<i64>),
            Type::FLOAT8 => encoder.encode_field(&None::<f64>),
            Type::BOOL => encoder.encode_field(&None::<bool>),
            _ => encoder.encode_field(&None::<String>),
        },
    }
}

fn infer_fields(result: &QueryResult, format: &Format) -> Vec<FieldInfo> {
    result
        .columns
        .iter()
        .enumerate()
        .map(|(index, name)| {
            FieldInfo::new(
                name.clone(),
                None,
                None,
                infer_type(result, index),
                format.format_for(index),
            )
        })
        .collect()
}

fn infer_type(result: &QueryResult, index: usize) -> Type {
    result
        .rows
        .iter()
        .filter_map(|row| row.get(index))
        .find_map(|value| match value {
            Value::Int64(_) => Some(Type::INT8),
            Value::Float64(_) => Some(Type::FLOAT8),
            Value::Bool(_) => Some(Type::BOOL),
            Value::String(_) => Some(Type::TEXT),
            Value::Null => None,
        })
        .unwrap_or(Type::TEXT)
}

fn unsupported_error(message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "0A000".to_string(),
        message.to_string(),
    )))
}

fn api_error<E>(error: E) -> PgWireError
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    PgWireError::ApiError(error.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_postgres::NoTls;

    #[test]
    fn parses_query_threads_flag() -> Result<()> {
        let config = ServerConfig::from_args(vec![
            "/tmp/test.narrowdb".to_string(),
            "--query-threads".to_string(),
            "4".to_string(),
        ])?;
        assert_eq!(config.query_threads, Some(4));
        Ok(())
    }

    #[test]
    fn rejects_zero_query_threads_flag() {
        let err = ServerConfig::from_args(vec![
            "/tmp/test.narrowdb".to_string(),
            "--query-threads".to_string(),
            "0".to_string(),
        ]);
        assert!(err.is_err());
    }

    #[test]
    fn rejects_zero_row_group_size_flag() {
        let err = ServerConfig::from_args(vec![
            "/tmp/test.narrowdb".to_string(),
            "--row-group-size".to_string(),
            "0".to_string(),
        ]);
        assert!(err.is_err());
    }

    #[test]
    fn rejects_empty_user_flag() {
        let err = ServerConfig::from_args(vec![
            "/tmp/test.narrowdb".to_string(),
            "--user".to_string(),
            "".to_string(),
        ]);
        assert!(err.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn accepts_postgres_client_with_auth() -> Result<()> {
        let db_path = std::env::temp_dir().join(format!(
            "narrowdb-server-test-{}.db",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        let config = ServerConfig {
            db_path: db_path.display().to_string(),
            listen_addr: "127.0.0.1:0".to_string(),
            row_group_size: 128,
            sync_on_flush: true,
            query_threads: Some(2),
            user: "narrowdb".to_string(),
            password: "secret".to_string(),
        };

        let db = Arc::new(NarrowDb::open(
            &config.db_path,
            DbOptions {
                row_group_size: config.row_group_size,
                sync_on_flush: config.sync_on_flush,
                query_threads: config.query_threads,
                ..DbOptions::default()
            },
        )?);
        let factory = Arc::new(PgServerFactory::new(db, &config));
        let listener = TcpListener::bind(&config.listen_addr).await?;
        let addr = listener.local_addr()?;

        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept test connection");
            process_socket(socket, None, factory)
                .await
                .expect("serve test connection");
        });

        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user={} password={} dbname=testdb",
                addr.port(),
                config.user,
                config.password
            ),
            NoTls,
        )
        .await?;
        let connection_task = tokio::spawn(async move {
            let _ = connection.await;
        });

        client
            .batch_execute(
                "CREATE TABLE logs (ts TIMESTAMP, service TEXT, status INT);\
                 INSERT INTO logs VALUES (1, 'api', 200);",
            )
            .await?;

        let rows = client
            .query("SELECT ts, service, status FROM logs", &[])
            .await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, i64>(0), 1);
        assert_eq!(rows[0].get::<_, String>(1), "api");
        assert_eq!(rows[0].get::<_, i64>(2), 200);

        drop(client);
        connection_task.abort();
        let _ = server.await;
        std::fs::remove_file(db_path)?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn supports_parameterized_prepared_statements() -> Result<()> {
        let db_path = std::env::temp_dir().join(format!(
            "narrowdb-server-prepared-test-{}.db",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        let config = ServerConfig {
            db_path: db_path.display().to_string(),
            listen_addr: "127.0.0.1:0".to_string(),
            row_group_size: 128,
            sync_on_flush: true,
            query_threads: Some(2),
            user: "narrowdb".to_string(),
            password: "secret".to_string(),
        };

        let db = Arc::new(NarrowDb::open(
            &config.db_path,
            DbOptions {
                row_group_size: config.row_group_size,
                sync_on_flush: config.sync_on_flush,
                query_threads: config.query_threads,
                ..DbOptions::default()
            },
        )?);
        let factory = Arc::new(PgServerFactory::new(db, &config));
        let listener = TcpListener::bind(&config.listen_addr).await?;
        let addr = listener.local_addr()?;

        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept test connection");
            process_socket(socket, None, factory)
                .await
                .expect("serve test connection");
        });

        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user={} password={} dbname=testdb",
                addr.port(),
                config.user,
                config.password
            ),
            NoTls,
        )
        .await?;
        let connection_task = tokio::spawn(async move {
            let _ = connection.await;
        });

        client
            .batch_execute("CREATE TABLE logs (ts TIMESTAMP, service TEXT, status INT);")
            .await?;

        let insert = client
            .prepare("INSERT INTO logs VALUES ($1, $2, $3)")
            .await?;
        client.execute(&insert, &[&1_i64, &"api", &200_i64]).await?;
        client
            .execute(&insert, &[&2_i64, &"worker", &503_i64])
            .await?;

        let select = client
            .prepare("SELECT service, status FROM logs WHERE ts = $1")
            .await?;
        let rows = client.query(&select, &[&2_i64]).await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, String>(0), "worker");
        assert_eq!(rows[0].get::<_, i64>(1), 503);

        drop(client);
        connection_task.abort();
        let _ = server.await;
        std::fs::remove_file(db_path)?;
        Ok(())
    }
}
