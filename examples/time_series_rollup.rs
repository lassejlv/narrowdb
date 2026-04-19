use narrowdb::{ColumnarBatchBuilder, DbOptions, NarrowDb};

fn main() -> anyhow::Result<()> {
    let path = std::env::temp_dir().join("narrowdb-example-timeseries.db");
    let _ = std::fs::remove_file(&path);

    let db = NarrowDb::open(&path, DbOptions::default())?;
    db.execute_sql(
        "CREATE TABLE metrics (
            ts TIMESTAMP,
            minute_bucket TIMESTAMP,
            day_bucket TIMESTAMP,
            service TEXT,
            requests INT,
            errors INT
        );",
    )?;

    let rows = 10_000_i64;
    let batch = ColumnarBatchBuilder::new()
        .timestamps((0..rows).map(|i| 1_700_000_000_000 + i * 1_000))
        .timestamps((0..rows).map(|i| {
            let ts = 1_700_000_000_000 + i * 1_000;
            ts - (ts % 60_000)
        }))
        .timestamps((0..rows).map(|i| {
            let ts = 1_700_000_000_000 + i * 1_000;
            ts - (ts % 86_400_000)
        }))
        .strings((0..rows).map(|i| if i % 2 == 0 { "api" } else { "worker" }))
        .int64((0..rows).map(|_| 1))
        .int64((0..rows).map(|i| if i % 25 == 0 { 1 } else { 0 }))
        .build()?;

    db.insert_columnar_batch("metrics", batch)?;
    db.flush_all()?;

    let result = db.execute_one(
        "SELECT minute_bucket, service, SUM(errors) AS total_errors
         FROM metrics
         WHERE day_bucket = 1699920000000
         GROUP BY minute_bucket, service
         ORDER BY minute_bucket ASC, service ASC
         LIMIT 5;",
    )?;

    println!("{:?}", result.columns);
    for row in result.rows {
        println!("{row:?}");
    }

    println!(
        "Retention/partition pattern: rotate or drop old day buckets by table naming or offline compaction."
    );

    let _ = std::fs::remove_file(path);
    Ok(())
}
