# narrowdb

A lightweight columnar database for log and time-series data, written in Rust.

## Quick start

```bash
cargo build --release

# Create a table and insert data
narrowdb exec logs.db "CREATE TABLE logs (ts TIMESTAMP, level TEXT, service TEXT, status INT, duration REAL);"
narrowdb exec logs.db "INSERT INTO logs VALUES (1, 'info', 'api', 200, 12.0), (2, 'error', 'api', 500, 120.0);"

# Query with filters, aggregation, and ordering
narrowdb exec logs.db "SELECT service, COUNT(*) AS errors FROM logs WHERE level = 'error' GROUP BY service ORDER BY errors DESC LIMIT 5;"
```

## As a library

```toml
[dependencies]
narrowdb = "0.1"
```

```rust
use narrowdb::{NarrowDb, DbOptions, Schema, ColumnDef, DataType, Value};

let mut db = NarrowDb::open("my.db", DbOptions::default())?;
db.execute_sql("CREATE TABLE logs (ts TIMESTAMP, msg TEXT);")?;
db.insert_rows("logs", vec![vec![Value::Int64(1), Value::String("hello".into())]])?;
let results = db.execute_sql("SELECT * FROM logs;")?;
```

## Benchmarks

1M rows, Apple Silicon, release build:

| Metric | Result |
|--------|--------|
| Ingest throughput | ~8.5M rows/sec |
| Bytes per row | 56 |
| Filtered GROUP BY | ~2.6ms |
| Filtered AVG | ~0.9ms |
| Filtered COUNT | ~0.8ms |
| Reopen from disk | ~0.7ms |

```bash
narrowdb bench my.db 1000000
```

## License

MIT
