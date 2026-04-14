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

# Rename and drop tables
narrowdb exec logs.db "ALTER TABLE logs RENAME TO app_logs;"
narrowdb exec logs.db "DROP TABLE IF EXISTS old_logs;"

# Inspect metadata
narrowdb exec logs.db "SHOW TABLES;"
narrowdb exec logs.db "DESCRIBE app_logs;"
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

## TCP server

The repo also includes a separate runnable server crate in `crates/server`.

```bash
cargo run -p narrowdb-server -- ./logs.narrowdb --listen 127.0.0.1:5433 --user narrowdb --password secret
```

It speaks the PostgreSQL wire protocol over TCP with password auth.

Example connection:

```bash
PGPASSWORD=secret psql "host=127.0.0.1 port=5433 user=narrowdb dbname=logs"
```

Example SQL:

```sql
CREATE TABLE logs (ts TIMESTAMP, service TEXT, status INT);
INSERT INTO logs VALUES (1, 'api', 200);
SELECT * FROM logs;
```

## Benchmarks

10M rows, 9 columns, Apple Silicon, release build (`cargo run --release --bin bench-narrow -- 10000000`):

| Metric | NarrowDB | DuckDB | Delta |
|--------|----------|--------|-------|
| Ingest throughput | 7.2M rows/s | 1.4M rows/s | **5x faster** |
| Storage size | 59.6 MiB (6.3 B/row) | 90.3 MiB (9.5 B/row) | **34% smaller** |
| Filter + GROUP BY | 3.3ms | 5.9ms | **1.8x faster** |
| Filter + GROUP BY + AVG | 4.1ms | 5.1ms | **1.2x faster** |
| Filter + COUNT | 3.7ms | 6.5ms | **1.8x faster** |

## License

MIT
