use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::engine::{NarrowDb, QueryResult};
use crate::storage::DbOptions;
use crate::types::{BatchColumn, ColumnarBatch};

const SERVICES: [&str; 8] = [
    "api", "worker", "auth", "billing", "search", "ingest", "cdn", "edge",
];
const HOSTS: [&str; 16] = [
    "host-a", "host-b", "host-c", "host-d", "host-e", "host-f", "host-g", "host-h", "host-i",
    "host-j", "host-k", "host-l", "host-m", "host-n", "host-o", "host-p",
];
const LEVELS: [&str; 4] = ["info", "warn", "error", "debug"];
const MESSAGE_TEMPLATES: [&str; 16] = [
    "request completed",
    "request retried",
    "cache miss",
    "cache warm",
    "upstream timeout",
    "upstream reset",
    "auth failed",
    "auth refreshed",
    "job queued",
    "job started",
    "job finished",
    "rate limited",
    "disk spill",
    "checkpoint written",
    "connection reused",
    "connection opened",
];
const BATCH_SIZE: usize = 65_536;

pub fn run_benchmark(path: impl AsRef<Path>, rows: usize) -> Result<()> {
    let path = path.as_ref();
    if path.exists() {
        std::fs::remove_file(path)?;
    }

    let mut db = NarrowDb::open(
        path,
        DbOptions {
            row_group_size: 32_768,
            sync_on_flush: false,
        },
    )?;

    db.execute_sql(
        "CREATE TABLE logs (
            ts TIMESTAMP,
            level TEXT,
            service TEXT,
            host TEXT,
            request_id INT,
            status INT,
            duration REAL,
            bytes INT,
            message TEXT
        );",
    )?;

    let end_to_end_started = Instant::now();
    let mut ingest_elapsed = Duration::ZERO;
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

        for row in batch_start..batch_end {
            let ts_value = 1_700_000_000_i64 + row as i64;
            let level_value = if row % 17 == 0 {
                LEVELS[2]
            } else if row % 7 == 0 {
                LEVELS[1]
            } else {
                LEVELS[0]
            };
            let service_value = SERVICES[row % SERVICES.len()];
            let host_value = HOSTS[(row / 11) % HOSTS.len()];
            let request_id_value = row as i64;
            let status_value = if level_value == "error" {
                500 + (row % 8) as i64
            } else {
                200 + (row % 5) as i64
            };
            let duration_value = 4.0 + ((row % 1000) as f64 * 1.37);
            let bytes_value = 256 + ((row * 31) % 65_536) as i64;
            let message_value = MESSAGE_TEMPLATES[(row / 97) % MESSAGE_TEMPLATES.len()];

            ts.push(ts_value);
            level.push(level_value.to_string());
            service.push(service_value.to_string());
            host.push(host_value.to_string());
            request_id.push(request_id_value);
            status.push(status_value);
            duration.push(duration_value);
            bytes.push(bytes_value);
            message.push(message_value.to_string());
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

        let batch_started = Instant::now();
        db.insert_columnar_batch("logs", batch)?;
        ingest_elapsed += batch_started.elapsed();
    }
    let flush_started = Instant::now();
    db.flush_all()?;
    ingest_elapsed += flush_started.elapsed();
    let end_to_end_elapsed = end_to_end_started.elapsed();

    let file_size = std::fs::metadata(db.path())?.len();
    println!(
        "Ingested {rows} rows in {:?} end-to-end",
        end_to_end_elapsed
    );
    println!("DB ingest time: {:?}", ingest_elapsed);
    println!(
        "DB ingest throughput: {:.2} rows/sec",
        rows as f64 / ingest_elapsed.as_secs_f64()
    );
    println!(
        "End-to-end throughput: {:.2} rows/sec",
        rows as f64 / end_to_end_elapsed.as_secs_f64()
    );
    println!("File size: {:.2} MiB", file_size as f64 / (1024.0 * 1024.0));
    println!("Bytes per row: {:.2}", file_size as f64 / rows as f64);

    run_query_benchmark(
        &mut db,
        rows,
        "SELECT service, COUNT(*) AS errors FROM logs WHERE ts >= 1700500000 AND level = 'error' GROUP BY service ORDER BY errors DESC LIMIT 5;",
        "Errors by service",
    )?;
    run_query_benchmark(
        &mut db,
        rows,
        "SELECT host, AVG(duration) AS avg_ms FROM logs WHERE status >= 500 GROUP BY host ORDER BY avg_ms DESC LIMIT 10;",
        "Avg duration for 5xx",
    )?;
    run_query_benchmark(
        &mut db,
        rows,
        "SELECT COUNT(*) AS slow_requests FROM logs WHERE duration >= 700.0;",
        "Slow request count",
    )?;

    drop(db);
    let reopen_started = Instant::now();
    let mut db = NarrowDb::open(path, DbOptions::default())?;
    let reopen_elapsed = reopen_started.elapsed();
    println!("Reopen time: {:?}", reopen_elapsed);
    run_query_benchmark(
        &mut db,
        rows,
        "SELECT service, COUNT(*) AS total FROM logs GROUP BY service ORDER BY total DESC LIMIT 5;",
        "Post-reopen cold group by",
    )?;
    run_query_benchmark(
        &mut db,
        rows,
        "SELECT service, COUNT(*) AS total FROM logs GROUP BY service ORDER BY total DESC LIMIT 5;",
        "Post-reopen warm group by",
    )?;

    Ok(())
}

fn run_query_benchmark(db: &mut NarrowDb, rows: usize, sql: &str, label: &str) -> Result<()> {
    let started = Instant::now();
    let results = db.execute_sql(sql)?;
    let elapsed = started.elapsed();
    let result = results.last().cloned().unwrap_or_else(QueryResult::empty);

    println!(
        "{label}: {:?} ({:.2} rows/sec scanned)",
        elapsed,
        rows as f64 / elapsed.as_secs_f64()
    );
    print_result_preview(&result);
    Ok(())
}

fn print_result_preview(result: &QueryResult) {
    if result.columns.is_empty() {
        return;
    }
    println!("Columns: {}", result.columns.join(", "));
    for row in result.rows.iter().take(5) {
        let line = row
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(" | ");
        println!("  {line}");
    }
}
