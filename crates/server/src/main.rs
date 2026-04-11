use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
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
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::tokio::process_socket;
use sqlparser::ast::{SetExpr, Statement};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let config = ServerConfig::from_args(std::env::args().skip(1).collect())?;
    let db = Arc::new(NarrowDb::open(
        &config.db_path,
        DbOptions {
            row_group_size: config.row_group_size,
            sync_on_flush: config.sync_on_flush,
            ..DbOptions::default()
        },
    )?);

    let factory = Arc::new(PgServerFactory::new(db, &config));
    let listener = TcpListener::bind(&config.listen_addr)
        .await
        .with_context(|| format!("binding PostgreSQL listener on {}", config.listen_addr))?;

    println!(
        "narrowdb-server listening on {} using {} as user {}",
        config.listen_addr, config.db_path, config.user
    );

    loop {
        let (socket, peer_addr) = listener.accept().await?;
        let factory = Arc::clone(&factory);
        tokio::spawn(async move {
            if let Err(error) = process_socket(socket, None, factory).await {
                eprintln!("pgwire client {peer_addr} error: {error:#}");
            }
        });
    }
}

#[derive(Debug, Clone)]
struct ServerConfig {
    db_path: String,
    listen_addr: String,
    row_group_size: usize,
    sync_on_flush: bool,
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
                "usage: narrowdb-server <db-file> [--listen 127.0.0.1:5433] [--row-group-size 16384] [--sync-on-flush true|false] [--user narrowdb] [--password secret]\n\nAll flags can also be set via environment variables: NARROWDB_PATH, NARROWDB_LISTEN, NARROWDB_ROW_GROUP_SIZE, NARROWDB_SYNC_ON_FLUSH, NARROWDB_USER, NARROWDB_PASSWORD"
            );
        };

        let mut listen_addr = env_or("NARROWDB_LISTEN", "127.0.0.1:5433");
        let mut row_group_size: usize = env_or("NARROWDB_ROW_GROUP_SIZE", "16384")
            .parse()
            .context("NARROWDB_ROW_GROUP_SIZE must be an integer")?;
        let mut sync_on_flush = parse_bool_flag(&env_or("NARROWDB_SYNC_ON_FLUSH", "true"))?;
        let mut user = env_or("NARROWDB_USER", "narrowdb");
        let mut password = env_or("NARROWDB_PASSWORD", "narrowdb");
        let mut index = 1;

        while index < args.len() {
            match args[index].as_str() {
                "--listen" => {
                    index += 1;
                    listen_addr = args.get(index).context("missing value for --listen")?.clone();
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
                "--user" => {
                    index += 1;
                    user = args.get(index).context("missing value for --user")?.clone();
                }
                "--password" => {
                    index += 1;
                    password = args.get(index).context("missing value for --password")?.clone();
                }
                other => bail!("unknown argument: {other}"),
            }
            index += 1;
        }

        Ok(Self {
            db_path,
            listen_addr,
            row_group_size,
            sync_on_flush,
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

struct PgServerFactory {
    backend: Arc<NarrowDbBackend>,
    startup: Arc<Md5PasswordAuthStartupHandler<StaticAuthSource, DefaultServerParameterProvider>>,
}

impl PgServerFactory {
    fn new(db: Arc<NarrowDb>, config: &ServerConfig) -> Self {
        let backend = Arc::new(NarrowDbBackend {
            db,
            query_parser: Arc::new(NoopQueryParser::new()),
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
    query_parser: Arc<NoopQueryParser>,
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
    type Statement = String;
    type QueryParser = NoopQueryParser;

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
        if portal.parameter_len() > 0 {
            return Err(unsupported_error(
                "parameterized extended queries are not supported yet",
            ));
        }

        let mut responses =
            self.execute_sql_with_format(&portal.statement.statement, &portal.result_column_format)?;
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
        let param_types = stmt
            .parameter_types
            .iter()
            .map(|ty| ty.clone().unwrap_or(Type::UNKNOWN))
            .collect::<Vec<_>>();
        let fields = self.describe_fields(&stmt.statement, &Format::UnifiedText)?;
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
        let fields = self.describe_fields(&portal.statement.statement, &portal.result_column_format)?;
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
        let trimmed = query.trim().trim_end_matches(';').trim();
        let normalized = trimmed.to_ascii_uppercase();

        if normalized.starts_with("SET ") {
            return Ok(Some(Response::Execution(Tag::new("SET"))));
        }

        if normalized == "SHOW SERVER_VERSION" {
            return self.single_text_response("server_version", env!("CARGO_PKG_VERSION"), format);
        }

        if normalized == "SHOW CLIENT_ENCODING" {
            return self.single_text_response("client_encoding", "UTF8", format);
        }

        if normalized == "SHOW TRANSACTION_READ_ONLY" {
            return self.single_text_response("transaction_read_only", "off", format);
        }

        if normalized == "SELECT VERSION()" {
            return self.single_text_response(
                "version",
                &format!("narrowdb-server {} (pgwire)", env!("CARGO_PKG_VERSION")),
                format,
            );
        }

        if normalized == "SELECT CURRENT_DATABASE()" {
            return self.single_text_response("current_database", &self.database_name, format);
        }

        if normalized == "SELECT CURRENT_USER" || normalized == "SELECT CURRENT_USER()" {
            return self.single_text_response("current_user", &self.user, format);
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
                Statement::Insert(insert) => Tag::new("INSERT").with_rows(insert_values_len(insert)),
                Statement::Query(_) => Tag::new("SELECT"),
                _ => Tag::new(statement_tag(command)),
            };
            return Ok(Response::Execution(tag));
        }

        Ok(Response::Query(query_result_to_response(result, format)?))
    }

    fn describe_fields(&self, query: &str, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
        if self.handle_builtin_query(query, format)?.is_some() {
            return Ok(vec![FieldInfo::new(
                "value".into(),
                None,
                None,
                Type::TEXT,
                format.format_for(0),
            )]);
        }

        let commands = parse_statements(query)?;
        let Some(command) = commands.first() else {
            return Ok(Vec::new());
        };

        if !matches!(command, Statement::Query(_)) {
            return Ok(Vec::new());
        }

        let results = self.db.execute_sql(query).map_err(api_error)?;
        let Some(result) = results.into_iter().last() else {
            return Ok(Vec::new());
        };
        Ok(infer_fields(&result, format))
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
        Value::Null => match data_type {
            &Type::INT8 => encoder.encode_field(&None::<i64>),
            &Type::FLOAT8 => encoder.encode_field(&None::<f64>),
            &Type::BOOL => encoder.encode_field(&None::<bool>),
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
    use std::time::{SystemTime, UNIX_EPOCH};

    use tokio_postgres::NoTls;

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
            user: "narrowdb".to_string(),
            password: "secret".to_string(),
        };

        let db = Arc::new(NarrowDb::open(
            &config.db_path,
            DbOptions {
                row_group_size: config.row_group_size,
                sync_on_flush: config.sync_on_flush,
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
                addr.port(), config.user, config.password
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

        let rows = client.query("SELECT ts, service, status FROM logs", &[]).await?;
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
}
