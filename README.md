# narrowdb

A small Rust columnar database for logs and time-series data.

NarrowDB is built for simple embedded analytics: append data, keep it compact on
disk, and run fast filters, projections, aggregates, grouping, and ordering.

## Install

```toml
[dependencies]
narrowdb = "0.3.2"
```

Or build the CLI from source:

```bash
cargo build --release
```

## CLI

```bash
narrowdb exec logs.db "CREATE TABLE logs (ts TIMESTAMP, level TEXT, service TEXT, status INT);"
narrowdb exec logs.db "INSERT INTO logs VALUES (1, 'info', 'api', 200), (2, 'error', 'api', 500);"
narrowdb exec logs.db "SELECT service, COUNT(*) FROM logs WHERE level = 'error' GROUP BY service;"
```

Useful commands:

```sql
SHOW TABLES;
DESCRIBE logs;
ALTER TABLE logs RENAME TO app_logs;
DROP TABLE IF EXISTS old_logs;
```

## Rust API

```rust
use narrowdb::{DbOptions, NarrowDb, Value};

let db = NarrowDb::open("logs.db", DbOptions::default())?;

db.execute_sql("CREATE TABLE logs (ts TIMESTAMP, msg TEXT);")?;
db.insert_rows(
    "logs",
    vec![vec![Value::Int64(1), Value::String("hello".into())]],
)?;

let rows = db.execute_sql("SELECT * FROM logs;")?;
```

For bigger ingests, use `insert_rows_iter` for streaming rows or
`insert_columnar_batch` for maximum throughput.

## PostgreSQL Wire Server

Run the server:

```bash
cargo run -p narrowdb-server -- ./logs.narrowdb \
  --listen 127.0.0.1:5433 \
  --user narrowdb \
  --password secret
```

Connect with `psql`:

```bash
PGPASSWORD=secret psql "host=127.0.0.1 port=5433 user=narrowdb dbname=logs"
```

The server supports PostgreSQL wire protocol queries, password auth, prepared
statements, and parameter binding.

## Examples

```bash
cargo run --example embedded_logs
cargo run --example high_throughput_ingest
cargo run --example time_series_rollup
```

## Benchmarks

```bash
cargo run --release --features bench-duckdb --bin bench-compare -- 1000000 --query-threads 8
```

Benchmark tooling lives in [tools/bench](./tools/bench).

## Docs

- [User guide](./docs/USER_GUIDE.md)
- [Architecture](./docs/ARCHITECTURE.md)
- [Contributing](./CONTRIBUTING.md)
- [Release guide](./docs/RELEASE.md)

## License

MIT
