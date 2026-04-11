use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use anyhow::{bail, Context, Result};
use narrowdb::{BatchColumn, ColumnarBatch, DbOptions, NarrowDb, QueryResult, Value};
use serde::{Deserialize, Serialize};

fn main() -> Result<()> {
    let config = ServerConfig::from_args(std::env::args().skip(1).collect())?;
    let db = Arc::new(Mutex::new(NarrowDb::open(
        &config.db_path,
        DbOptions {
            row_group_size: config.row_group_size,
            sync_on_flush: config.sync_on_flush,
        },
    )?));

    let listener = TcpListener::bind(&config.listen_addr)
        .with_context(|| format!("binding TCP listener on {}", config.listen_addr))?;

    println!(
        "narrowdb-server listening on {} using {}",
        config.listen_addr, config.db_path
    );

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    if let Err(error) = handle_client(stream, db) {
                        eprintln!("client error: {error:#}");
                    }
                });
            }
            Err(error) => eprintln!("accept error: {error:#}"),
        }
    }

    Ok(())
}

#[derive(Debug)]
struct ServerConfig {
    db_path: String,
    listen_addr: String,
    row_group_size: usize,
    sync_on_flush: bool,
}

impl ServerConfig {
    fn from_args(args: Vec<String>) -> Result<Self> {
        if args.is_empty() {
            bail!(
                "usage: narrowdb-server <db-file> [--listen 127.0.0.1:5433] [--row-group-size 16384] [--sync-on-flush true|false]"
            );
        }

        let db_path = args[0].clone();
        let mut listen_addr = "127.0.0.1:5433".to_string();
        let mut row_group_size = 16_384;
        let mut sync_on_flush = true;
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
                other => bail!("unknown argument: {other}"),
            }
            index += 1;
        }

        Ok(Self {
            db_path,
            listen_addr,
            row_group_size,
            sync_on_flush,
        })
    }
}

fn parse_bool_flag(value: &str) -> Result<bool> {
    match value {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => bail!("expected boolean value, got {value}"),
    }
}

fn handle_client(stream: TcpStream, db: Arc<Mutex<NarrowDb>>) -> Result<()> {
    stream.set_nodelay(true)?;
    let peer = stream.peer_addr().ok();
    let reader_stream = stream.try_clone()?;
    let mut reader = BufReader::new(reader_stream);
    let mut writer = BufWriter::new(stream);
    let mut line = String::new();

    loop {
        line.clear();
        let read = reader.read_line(&mut line)?;
        if read == 0 {
            break;
        }

        if line.trim().is_empty() {
            continue;
        }

        let response = match serde_json::from_str::<RequestEnvelope>(&line) {
            Ok(request) => execute_request(&db, request),
            Err(error) => ResponseEnvelope::error(None, format!("invalid request JSON: {error}")),
        };

        serde_json::to_writer(&mut writer, &response)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
    }

    if let Some(peer) = peer {
        eprintln!("client disconnected: {peer}");
    }

    Ok(())
}

#[derive(Debug, Deserialize)]
struct RequestEnvelope {
    #[serde(default)]
    id: Option<u64>,
    method: String,
    #[serde(default)]
    params: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct ResponseEnvelope {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<u64>,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl ResponseEnvelope {
    fn ok(id: Option<u64>, result: serde_json::Value) -> Self {
        Self {
            id,
            ok: true,
            result: Some(result),
            error: None,
        }
    }

    fn error(id: Option<u64>, error: String) -> Self {
        Self {
            id,
            ok: false,
            result: None,
            error: Some(error),
        }
    }
}

#[derive(Debug, Deserialize)]
struct ExecParams {
    sql: String,
}

#[derive(Debug, Deserialize)]
struct InsertColumnarBatchParams {
    table: String,
    batch: ColumnarBatchRequest,
}

#[derive(Debug, Deserialize)]
struct ColumnarBatchRequest {
    columns: Vec<BatchColumnRequest>,
}

#[derive(Debug, Deserialize)]
struct BatchColumnRequest {
    kind: String,
    #[serde(default)]
    ints: Vec<i64>,
    #[serde(default)]
    floats: Vec<f64>,
    #[serde(default)]
    bools: Vec<bool>,
    #[serde(default)]
    strings: Vec<String>,
}

#[derive(Debug, Serialize)]
struct QueryResultJson {
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
}

fn execute_request(db: &Arc<Mutex<NarrowDb>>, request: RequestEnvelope) -> ResponseEnvelope {
    let result: Result<serde_json::Value> = (|| -> Result<serde_json::Value> {
        match request.method.as_str() {
            "ping" => Ok(serde_json::json!({
                "pong": true,
                "version": env!("CARGO_PKG_VERSION"),
            })),
            "exec" => {
                let params: ExecParams =
                    serde_json::from_value(request.params).context("invalid params for exec")?;
                let mut db = db
                    .lock()
                    .map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
                let results = db.execute_sql(&params.sql)?;
                Ok(serde_json::to_value(
                    results
                        .into_iter()
                        .map(query_result_to_json)
                        .collect::<Vec<_>>(),
                )?)
            }
            "insert_columnar_batch" => {
                let params: InsertColumnarBatchParams = serde_json::from_value(request.params)
                    .context("invalid params for insert_columnar_batch")?;
                let batch = params.batch.try_into_batch()?;
                let mut db = db
                    .lock()
                    .map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
                db.insert_columnar_batch(&params.table, batch)?;
                Ok(serde_json::json!({ "inserted": true }))
            }
            "flush_all" => {
                let mut db = db
                    .lock()
                    .map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
                db.flush_all()?;
                Ok(serde_json::json!({ "flushed": true }))
            }
            other => bail!("unknown method: {other}"),
        }
    })();

    match result {
        Ok(value) => ResponseEnvelope::ok(request.id, value),
        Err(error) => ResponseEnvelope::error(request.id, error.to_string()),
    }
}

impl ColumnarBatchRequest {
    fn try_into_batch(self) -> Result<ColumnarBatch> {
        let columns = self
            .columns
            .into_iter()
            .map(BatchColumnRequest::try_into_column)
            .collect::<Result<Vec<_>>>()?;
        ColumnarBatch::new(columns)
    }
}

impl BatchColumnRequest {
    fn try_into_column(self) -> Result<BatchColumn> {
        match self.kind.to_ascii_lowercase().as_str() {
            "int" | "int64" | "integer" | "bigint" => Ok(BatchColumn::Int64(self.ints)),
            "timestamp" | "datetime" => Ok(BatchColumn::Timestamp(self.ints)),
            "float" | "float64" | "real" | "double" => Ok(BatchColumn::Float64(self.floats)),
            "bool" | "boolean" => Ok(BatchColumn::Bool(self.bools)),
            "string" | "text" | "varchar" => Ok(BatchColumn::String(self.strings)),
            other => bail!("unsupported batch column kind: {other}"),
        }
    }
}

fn query_result_to_json(result: QueryResult) -> QueryResultJson {
    QueryResultJson {
        columns: result.columns,
        rows: result
            .rows
            .into_iter()
            .map(|row| row.into_iter().map(value_to_json).collect())
            .collect(),
    }
}

fn value_to_json(value: Value) -> serde_json::Value {
    match value {
        Value::Int64(value) => serde_json::Value::from(value),
        Value::Float64(value) => serde_json::Value::from(value.into_inner()),
        Value::Bool(value) => serde_json::Value::from(value),
        Value::String(value) => serde_json::Value::from(value),
        Value::Null => serde_json::Value::Null,
    }
}
