use narrowdb::{ColumnarBatchBuilder, DbOptions, NarrowDb};

fn main() -> anyhow::Result<()> {
    let path = std::env::temp_dir().join("narrowdb-example-columnar.db");
    let _ = std::fs::remove_file(&path);

    let db = NarrowDb::open(
        &path,
        DbOptions {
            row_group_size: 16_384,
            ..DbOptions::default()
        },
    )?;
    db.execute_sql(
        "CREATE TABLE logs (
            ts TIMESTAMP,
            service TEXT,
            status INT,
            duration REAL,
            region TEXT
        );",
    )?;

    let rows = 50_000_i64;
    let batch = ColumnarBatchBuilder::new()
        .timestamps((0..rows).map(|i| 1_700_000_000_000 + i * 1_000))
        .strings((0..rows).map(|i| match i % 3 {
            0 => "api",
            1 => "worker",
            _ => "billing",
        }))
        .int64((0..rows).map(|i| if i % 10 == 0 { 500 } else { 200 }))
        .float64((0..rows).map(|i| 5.0 + (i % 250) as f64))
        .strings((0..rows).map(|i| if i % 2 == 0 { "eu-west-1" } else { "us-east-1" }))
        .build()?;

    db.insert_columnar_batch("logs", batch)?;
    db.flush_all()?;

    let result = db.execute_one(
        "SELECT region, COUNT(*) AS errors
         FROM logs
         WHERE status >= 500
         GROUP BY region
         ORDER BY errors DESC, region ASC;",
    )?;

    println!("{:?}", result.columns);
    for row in result.rows {
        println!("{row:?}");
    }

    let _ = std::fs::remove_file(path);
    Ok(())
}
