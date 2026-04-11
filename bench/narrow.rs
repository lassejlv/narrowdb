mod common;

use anyhow::Result;
use narrowdb::*;

const DB_PATH: &str = "/tmp/bench-narrowdb.db";
const BATCH_SIZE: usize = 65_536;

fn main() -> Result<()> {
    let rows = common::row_count_from_args();
    common::print_header("NarrowDB", rows);

    if std::path::Path::new(DB_PATH).exists() {
        std::fs::remove_file(DB_PATH)?;
    }

    let mut db = NarrowDb::open(
        DB_PATH,
        DbOptions {
            row_group_size: 32_768,
            sync_on_flush: false,
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
                let r = common::generate_row(i);
                ts.push(r.ts);
                level.push(r.level.to_string());
                service.push(r.service.to_string());
                host.push(r.host.to_string());
                request_id.push(r.request_id);
                status.push(r.status);
                duration.push(r.duration);
                bytes.push(r.bytes);
                message.push(r.message.to_string());
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

    let file_size = std::fs::metadata(DB_PATH)?.len();
    common::print_ingest(ingest_elapsed, rows, file_size);

    for (label, sql) in common::QUERIES {
        let (result, elapsed) = common::timed(|| db.execute_sql(sql));
        let result = result?;
        let last = result.last().unwrap();
        let qr = common::QueryResult {
            columns: last.columns.clone(),
            rows: last.rows.iter().map(|row| row.iter().map(ToString::to_string).collect()).collect(),
        };
        common::print_query(label, elapsed, &qr);
    }

    std::fs::remove_file(DB_PATH)?;
    Ok(())
}
