use narrowdb::{ColumnDef, DataType, DbOptions, NarrowDb, Schema, Value};

fn main() -> anyhow::Result<()> {
    let path = std::env::temp_dir().join("narrowdb-example-embedded.db");
    let _ = std::fs::remove_file(&path);

    let db = NarrowDb::open(&path, DbOptions::default())?;
    db.create_table(Schema::new(
        "logs",
        vec![
            ColumnDef::new("ts", DataType::Timestamp),
            ColumnDef::new("service", DataType::String),
            ColumnDef::new("status", DataType::Int64),
        ],
    ))?;

    db.insert_rows_iter(
        "logs",
        [
            vec![
                Value::Int64(1_700_000_000_000),
                Value::String("api".into()),
                Value::Int64(200),
            ],
            vec![
                Value::Int64(1_700_000_060_000),
                Value::String("worker".into()),
                Value::Int64(503),
            ],
            vec![
                Value::Int64(1_700_000_120_000),
                Value::String("api".into()),
                Value::Int64(500),
            ],
        ],
    )?;

    let result = db.execute_one(
        "SELECT service, COUNT(*) AS total
         FROM logs
         WHERE status >= 500
         GROUP BY service
         ORDER BY total DESC, service ASC;",
    )?;

    println!("{:?}", result.columns);
    for row in result.rows {
        println!("{row:?}");
    }

    let _ = std::fs::remove_file(path);
    Ok(())
}
