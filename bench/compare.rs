mod common;

use anyhow::Result;
use duckdb::{Connection, params};
use narrowdb::*;

const NARROW_DB_PATH: &str = "/tmp/bench-narrowdb.db";
const DUCK_DB_PATH: &str = "/tmp/bench-duckdb.db";
const BATCH_SIZE: usize = 65_536;

fn main() -> Result<()> {
    let args = common::benchmark_args()?;
    let rows = args.rows;
    let query_threads = common::resolved_query_threads(args.query_threads);
    let narrow_runs = (0..args.repeat)
        .map(|_| run_narrow(rows, query_threads))
        .collect::<Result<Vec<_>>>()?;
    let duckdb_runs = (0..args.repeat)
        .map(|_| run_duckdb(rows, query_threads))
        .collect::<Result<Vec<_>>>()?;

    let narrow = common::median_report(&narrow_runs)?;
    let duckdb = common::median_report(&duckdb_runs)?;
    common::print_comparison(&narrow, &duckdb);
    Ok(())
}

fn run_narrow(rows: usize, query_threads: usize) -> Result<common::BenchmarkReport> {
    cleanup_file(NARROW_DB_PATH);

    let db = NarrowDb::open(
        NARROW_DB_PATH,
        DbOptions {
            row_group_size: 32_768,
            sync_on_flush: false,
            query_threads: Some(query_threads),
            ..DbOptions::default()
        },
    )?;

    db.execute_sql(
        "CREATE TABLE logs (
            ts TIMESTAMP, level TEXT, service TEXT, host TEXT,
            request_id INT, status INT, duration REAL, bytes INT, message TEXT
        );",
    )?;

    let (ingest_result, ingest_elapsed) = common::timed(|| -> Result<()> {
        for batch_start in (0..rows).step_by(BATCH_SIZE) {
            let batch_end = (batch_start + BATCH_SIZE).min(rows);
            let batch_rows = batch_end - batch_start;

            let mut ts = Vec::with_capacity(batch_rows);
            let mut level = Vec::with_capacity(batch_rows);
            let mut service = Vec::with_capacity(batch_rows);
            let mut host = Vec::with_capacity(batch_rows);
            let mut request_id = Vec::with_capacity(batch_rows);
            let mut status = Vec::with_capacity(batch_rows);
            let mut duration = Vec::with_capacity(batch_rows);
            let mut bytes = Vec::with_capacity(batch_rows);
            let mut message = Vec::with_capacity(batch_rows);

            for i in batch_start..batch_end {
                let row = common::generate_row(i);
                ts.push(row.ts);
                level.push(row.level.to_string());
                service.push(row.service.to_string());
                host.push(row.host.to_string());
                request_id.push(row.request_id);
                status.push(row.status);
                duration.push(row.duration);
                bytes.push(row.bytes);
                message.push(row.message.to_string());
            }

            let batch = ColumnarBatch::new(vec![
                BatchColumn::Timestamp(ts),
                BatchColumn::String(level),
                BatchColumn::String(service),
                BatchColumn::String(host),
                BatchColumn::Int64(request_id),
                BatchColumn::Int64(status),
                BatchColumn::Float64(duration),
                BatchColumn::Int64(bytes),
                BatchColumn::String(message),
            ])?;

            db.insert_columnar_batch("logs", batch)?;
        }
        db.flush_all()?;
        Ok(())
    });
    ingest_result?;

    let queries = common::QUERIES
        .iter()
        .map(|(label, sql)| {
            let (result, elapsed) = common::timed(|| db.execute_sql(sql));
            result?;
            Ok(common::QueryMetric { label, elapsed })
        })
        .collect::<Result<Vec<_>>>()?;

    let file_size = std::fs::metadata(NARROW_DB_PATH)?.len();
    cleanup_file(NARROW_DB_PATH);

    Ok(common::BenchmarkReport {
        engine: "NarrowDB",
        rows,
        query_threads,
        ingest_elapsed,
        file_size,
        queries,
    })
}

fn run_duckdb(rows: usize, query_threads: usize) -> Result<common::BenchmarkReport> {
    cleanup_file(DUCK_DB_PATH);

    let conn = Connection::open(DUCK_DB_PATH)?;
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
                let row = common::generate_row(i);
                appender.append_row(params![
                    row.ts,
                    row.level,
                    row.service,
                    row.host,
                    row.request_id,
                    row.status,
                    row.duration,
                    row.bytes,
                    row.message,
                ])?;
            }

            appender.flush()?;
        }
        Ok(())
    });
    ingest_result?;

    let queries = common::QUERIES
        .iter()
        .map(|(label, sql)| {
            let (result, elapsed) = common::timed(|| -> Result<()> {
                let mut stmt = conn.prepare(sql)?;
                let _rows = stmt
                    .query_map([], |_| Ok(()))?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                Ok(())
            });
            result?;
            Ok(common::QueryMetric { label, elapsed })
        })
        .collect::<Result<Vec<_>>>()?;

    drop(conn);
    let file_size = walkdir(DUCK_DB_PATH);
    cleanup_file(DUCK_DB_PATH);
    let _ = std::fs::remove_dir_all(format!("{DUCK_DB_PATH}.wal"));

    Ok(common::BenchmarkReport {
        engine: "DuckDB",
        rows,
        query_threads,
        ingest_elapsed,
        file_size,
        queries,
    })
}

fn cleanup_file(path: &str) {
    if std::path::Path::new(path).exists() {
        let _ = std::fs::remove_file(path);
    }
}

fn walkdir(path: &str) -> u64 {
    let mut total = 0;
    let path = std::path::Path::new(path);
    if path.is_file() {
        return path.metadata().map(|meta| meta.len()).unwrap_or(0);
    }
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata()
                && meta.is_file()
            {
                total += meta.len();
            }
        }
    }
    total
}
