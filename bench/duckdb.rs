mod common;

use anyhow::Result;
use duckdb::{Connection, params};

const DB_PATH: &str = "/tmp/bench-duckdb.db";
const BATCH_SIZE: usize = 65_536;

fn main() -> Result<()> {
    let args = common::benchmark_args()?;
    let rows = args.rows;
    common::print_header("DuckDB", rows);

    if std::path::Path::new(DB_PATH).exists() {
        std::fs::remove_file(DB_PATH)?;
    }

    let conn = Connection::open(DB_PATH)?;
    let query_threads = common::resolved_query_threads(args.query_threads);
    conn.execute_batch(&format!("PRAGMA threads = {query_threads};"))?;

    conn.execute_batch(
        "CREATE TABLE logs (
            ts BIGINT, level VARCHAR, service VARCHAR, host VARCHAR,
            request_id BIGINT, status BIGINT, duration DOUBLE, bytes BIGINT, message VARCHAR
        );",
    )?;

    let (ingest_result, ingest_elapsed) = common::timed(|| -> Result<()> {
        for batch_start in (0..rows).step_by(BATCH_SIZE) {
            let batch_end = (batch_start + BATCH_SIZE).min(rows);

            let mut appender = conn.appender("logs")?;
            for i in batch_start..batch_end {
                let r = common::generate_row(i);
                appender.append_row(params![
                    r.ts,
                    r.level,
                    r.service,
                    r.host,
                    r.request_id,
                    r.status,
                    r.duration,
                    r.bytes,
                    r.message,
                ])?;
            }
            appender.flush()?;
        }
        Ok(())
    });
    ingest_result?;

    let file_size: u64 = walkdir(DB_PATH);
    common::print_ingest(ingest_elapsed, rows, file_size);

    for (label, sql) in common::QUERIES {
        let (result, elapsed) = common::timed(|| -> Result<common::QueryResult> {
            let mut stmt = conn.prepare(sql)?;
            let rows: Vec<Vec<String>> = stmt
                .query_map([], |row| {
                    let mut vals = Vec::new();
                    for i in 0..10 {
                        match row.get::<_, duckdb::types::Value>(i) {
                            Ok(v) => vals.push(duckdb_value_to_string(&v)),
                            Err(_) => break,
                        }
                    }
                    Ok(vals)
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let col_count = stmt.column_count();
            let columns = (0..col_count)
                .map(|i| {
                    stmt.column_name(i)
                        .map_or("?".to_string(), |v| v.to_string())
                })
                .collect();
            Ok(common::QueryResult { columns, rows })
        });
        common::print_query(label, elapsed, &result?);
    }

    drop(conn);
    let _ = std::fs::remove_file(DB_PATH);
    let _ = std::fs::remove_dir_all(DB_PATH.to_string() + ".wal");
    Ok(())
}

fn duckdb_value_to_string(v: &duckdb::types::Value) -> String {
    match v {
        duckdb::types::Value::Null => "NULL".into(),
        duckdb::types::Value::Boolean(b) => b.to_string(),
        duckdb::types::Value::TinyInt(i) => i.to_string(),
        duckdb::types::Value::SmallInt(i) => i.to_string(),
        duckdb::types::Value::Int(i) => i.to_string(),
        duckdb::types::Value::BigInt(i) => i.to_string(),
        duckdb::types::Value::Float(f) => f.to_string(),
        duckdb::types::Value::Double(f) => f.to_string(),
        duckdb::types::Value::Text(s) => s.clone(),
        other => format!("{other:?}"),
    }
}

fn walkdir(path: &str) -> u64 {
    let mut total = 0;
    let p = std::path::Path::new(path);
    if p.is_file() {
        return p.metadata().map(|m| m.len()).unwrap_or(0);
    }
    if let Ok(entries) = std::fs::read_dir(p) {
        for entry in entries.flatten() {
            let meta = entry.metadata();
            if let Ok(m) = meta {
                if m.is_file() {
                    total += m.len();
                }
            }
        }
    }
    total
}
