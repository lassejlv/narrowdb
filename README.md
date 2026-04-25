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
narrowdb exec logs.db "SELECT COUNT(*) FROM logs;" --query-threads 4

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
narrowdb = "0.3.1"
```

```rust
use narrowdb::{NarrowDb, DbOptions, Schema, ColumnDef, DataType, Value};

let mut db = NarrowDb::open("my.db", DbOptions::default())?;
db.execute_sql("CREATE TABLE logs (ts TIMESTAMP, msg TEXT);")?;
db.insert_rows("logs", vec![vec![Value::Int64(1), Value::String("hello".into())]])?;
let results = db.execute_sql("SELECT * FROM logs;")?;
```

For larger ingests, prefer:

- `insert_rows_iter(...)` when your source is row-oriented and you want to stream rows without collecting a giant `Vec<Vec<Value>>`
- `insert_columnar_batch(...)` with `ColumnarBatchBuilder` when your source is already column-shaped or you want maximum throughput

Examples:

```bash
cargo run --example embedded_logs
cargo run --example high_throughput_ingest
cargo run --example time_series_rollup
```

## TCP server

The repo also includes a separate runnable server crate in `crates/server`.

```bash
cargo run -p narrowdb-server -- ./logs.narrowdb --listen 127.0.0.1:5433 --query-threads 4 --user narrowdb --password secret
```

It speaks the PostgreSQL wire protocol over TCP with password auth, including prepared statements and parameter binding. MD5 auth should be treated as trusted-network only.

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

1M rows, 9 columns, Apple Silicon, release build:

```bash
cargo run --release --features bench-duckdb --bin bench-compare -- 1000000 --query-threads 8
```

| Metric | NarrowDB | DuckDB | Delta |
|--------|----------|--------|-------|
| Ingest throughput | 6.61M rows/s | 1.81M rows/s | **3.65x higher** |
| Storage size | 5.97 MiB (6.26 B/row) | 9.01 MiB (9.45 B/row) | **1.51x smaller** |
| Filter + GROUP BY | 536µs | 2.17ms | **4.05x faster** |
| Filter + GROUP BY + AVG | 475µs | 1.39ms | **2.93x faster** |
| Filter + COUNT | 553µs | 912µs | **1.65x faster** |
| Projection-heavy scan | 1.59ms | 1.42ms | DuckDB **1.12x faster** |
| High-cardinality grouped count | 1.32ms | 1.19ms | DuckDB **1.11x faster** |
| Cross-row-group string group by | 1.13ms | 2.04ms | **1.80x faster** |

## Development

For full docs, see [docs/README.md](./docs/README.md).
For build, test, lint, benchmark, and release commands, see [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

MIT
