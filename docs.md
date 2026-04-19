# NarrowDB Documentation

## Table of Contents

- [Installation](#installation)
- [CLI Usage](#cli-usage)
- [Library Usage](#library-usage)
- [TCP Server](#tcp-server)
- [SQL Reference](#sql-reference)
- [Data Types](#data-types)
- [Columnar Batch Ingestion](#columnar-batch-ingestion)
- [Configuration](#configuration)
- [Architecture](#architecture)

---

## Installation

### From crates.io

```toml
[dependencies]
narrowdb = "0.3.1"
```

### From source

```bash
git clone https://github.com/lassejlv/narrowdb.git
cd narrowdb
cargo build --release
```

The binary is at `target/release/narrowdb`.

---

## CLI Usage

```bash
# Execute SQL against a database file
narrowdb exec <db-file> <sql>

# Run built-in benchmark
narrowdb bench <db-file> [rows]
```

### Examples

```bash
# Create a table
narrowdb exec logs.db "CREATE TABLE logs (ts TIMESTAMP, level TEXT, service TEXT, status INT, duration REAL);"

# Insert rows
narrowdb exec logs.db "INSERT INTO logs VALUES (1, 'info', 'api', 200, 12.0), (2, 'error', 'api', 500, 120.0);"

# Query with filters, aggregation, and ordering
narrowdb exec logs.db "SELECT service, COUNT(*) AS errors FROM logs WHERE level = 'error' GROUP BY service ORDER BY errors DESC LIMIT 5;"

# Arithmetic expressions in SELECT
narrowdb exec logs.db "SELECT duration * 1.1 AS padded FROM logs WHERE status >= 500;"

# Table-less expressions
narrowdb exec logs.db "SELECT 2 + 3 * 4;"

# Benchmark with 5 million rows
narrowdb bench logs.db 5000000
```

---

## Library Usage

```rust
use narrowdb::{NarrowDb, DbOptions, Value};

fn main() -> anyhow::Result<()> {
    let mut db = NarrowDb::open("my.db", DbOptions::default())?;

    db.execute_sql("CREATE TABLE logs (ts TIMESTAMP, level TEXT, service TEXT, status INT);")?;

    db.execute_sql("INSERT INTO logs VALUES (1, 'info', 'api', 200), (2, 'error', 'api', 500);")?;

    let results = db.execute_sql(
        "SELECT service, COUNT(*) AS total FROM logs GROUP BY service;"
    )?;

    for result in results {
        println!("Columns: {:?}", result.columns);
        for row in &result.rows {
            println!("{:?}", row);
        }
    }

    Ok(())
}
```

### Row-by-row insertion

```rust
use narrowdb::{NarrowDb, DbOptions, Value};

let mut db = NarrowDb::open("my.db", DbOptions::default())?;

db.insert_row("logs", vec![
    Value::Int64(1),
    Value::String("info".into()),
    Value::String("api".into()),
    Value::Int64(200),
])?;
```

### Streaming row ingestion

Use `insert_rows_iter` when your upstream source is naturally row-oriented but you do not want to allocate one giant `Vec<Vec<Value>>` first.

```rust
use narrowdb::{NarrowDb, DbOptions, Value};

let db = NarrowDb::open("my.db", DbOptions::default())?;

let inserted = db.insert_rows_iter(
    "logs",
    (0..1_000_000).map(|i| {
        vec![
            Value::Int64(1_700_000_000_000 + i),
            Value::String(if i % 2 == 0 { "api" } else { "worker" }.into()),
            Value::Int64(if i % 10 == 0 { 500 } else { 200 }),
        ]
    }),
)?;

assert_eq!(inserted, 1_000_000);
```

### Columnar batch insertion (high throughput)

```rust
use narrowdb::{ColumnarBatchBuilder, DbOptions, NarrowDb};

let db = NarrowDb::open("my.db", DbOptions::default())?;

let batch = ColumnarBatchBuilder::new()
    .timestamps([1, 2, 3])
    .strings(["info", "error", "info"])
    .strings(["api", "api", "worker"])
    .int64([200, 500, 200])
    .build()?;

db.insert_columnar_batch("logs", batch)?;
```

### Choosing an ingestion path

- Use `insert_row` for tiny writes and tests.
- Use `insert_rows_iter` when the source is row-oriented and you want bounded memory usage.
- Use `insert_columnar_batch` when you can naturally produce whole columns or want the best ingest throughput.
- Use `row_group_size` to tune flush behavior for your workload, but keep the batch path as the default for serious volume.

### Timestamp-heavy workflows

`TIMESTAMP` values are stored as 64-bit integers. NarrowDB does not currently provide built-in datetime functions, so the best pattern is to ingest:

- a raw event timestamp column, typically epoch milliseconds
- one or more precomputed bucket columns such as `minute_bucket`, `hour_bucket`, or `day_bucket`

Example schema:

```sql
CREATE TABLE metrics (
    ts TIMESTAMP,
    minute_bucket TIMESTAMP,
    day_bucket TIMESTAMP,
    service TEXT,
    requests INT,
    errors INT
);
```

This lets you express common rollups and retention-oriented scans with simple filters and grouping:

```sql
SELECT minute_bucket, service, SUM(errors) AS total_errors
FROM metrics
WHERE day_bucket = 1699920000000
GROUP BY minute_bucket, service
ORDER BY minute_bucket ASC, service ASC;
```

### Retention and partition-like ergonomics

NarrowDB does not currently implement built-in partitions or retention policies.

The recommended pattern today is:

- precompute a `day_bucket` or `hour_bucket` column for query pruning
- rotate tables by period when operationally convenient, for example `logs_2026_04_19`
- drop or archive whole old tables as your retention mechanism

That approach fits the current storage model better than trying to simulate in-place row eviction.

### Flushing

Data is automatically flushed to disk when the pending batch reaches `row_group_size`. To force a flush:

```rust
db.flush_table("logs")?;  // Flush one table
db.flush_all()?;           // Flush all tables
```

Pending rows are also flushed automatically before any SELECT query.

---

## TCP Server

The server crate (`crates/server`) exposes a PostgreSQL wire protocol interface.
It supports simple queries, extended queries, and parameterized prepared statements. MD5 auth should be treated as trusted-network only.

### Running

```bash
cargo run -p narrowdb-server -- ./logs.narrowdb \
    --listen 127.0.0.1:5433 \
    --query-threads 4 \
    --user narrowdb \
    --password secret
```

Or via environment variables:

```bash
NARROWDB_LISTEN=0.0.0.0:5433 \
NARROWDB_QUERY_THREADS=4 \
NARROWDB_USER=narrowdb \
NARROWDB_PASSWORD=secret \
narrowdb-server ./logs.narrowdb
```

### Connecting

Any PostgreSQL client works:

```bash
PGPASSWORD=secret psql "host=127.0.0.1 port=5433 user=narrowdb dbname=logs"
```

```sql
CREATE TABLE logs (ts TIMESTAMP, service TEXT, status INT);
INSERT INTO logs VALUES (1, 'api', 200);
SELECT * FROM logs;
```

---

## SQL Reference

### CREATE TABLE

```sql
CREATE TABLE table_name (
    column1 TYPE,
    column2 TYPE,
    ...
);
```

### ALTER TABLE

```sql
ALTER TABLE table_name RENAME TO new_table_name;
ALTER TABLE table_name RENAME COLUMN old_name TO new_name;
```

`ALTER TABLE` currently supports exactly one operation per statement.

### DROP TABLE

```sql
DROP TABLE table_name;
DROP TABLE IF EXISTS table_name;
```

### SHOW TABLES

```sql
SHOW TABLES;
```

### DESCRIBE

```sql
DESCRIBE table_name;
DESC table_name;
```

### INSERT

```sql
INSERT INTO table_name VALUES
    (val1, val2, ...),
    (val1, val2, ...);
```

### SELECT

```sql
SELECT projections
FROM table_name
[WHERE filters]
[GROUP BY columns]
[ORDER BY column [ASC|DESC], ...]
[LIMIT n];
```

### WHERE filters

Supported filter forms:

- Comparison filters: `=`, `<>`, `<`, `<=`, `>`, `>=`
- Boolean combinations: `AND`, `OR`, nested with parentheses
- Membership filters: `IN (...)`, `NOT IN (...)`
- Null filters: `IS NULL`, `IS NOT NULL`

### Projections

| Syntax | Example |
|--------|---------|
| Column reference | `SELECT service FROM logs` |
| Wildcard | `SELECT * FROM logs` |
| Aggregate function | `SELECT COUNT(*) FROM logs` |
| Arithmetic expression | `SELECT duration * 1.1 AS padded FROM logs` |
| Column alias | `SELECT service AS svc FROM logs` |

### Aggregate Functions

| Function | Description |
|----------|-------------|
| `COUNT(*)` | Count all rows |
| `COUNT(column)` | Count non-null values |
| `SUM(column)` | Sum of numeric column |
| `AVG(column)` | Average of numeric column |
| `MIN(column)` | Minimum value |
| `MAX(column)` | Maximum value |

### WHERE Filters

Filters are AND-chained column-to-literal comparisons:

```sql
WHERE status >= 500 AND level = 'error' AND ts >= 1700000000
```

| Operator | Example |
|----------|---------|
| `=` | `WHERE level = 'error'` |
| `!=`, `<>` | `WHERE level != 'info'` |
| `<`, `<=`, `>`, `>=` | `WHERE duration >= 100.0` |
| `IS NULL` | `WHERE service IS NULL` |
| `IS NOT NULL` | `WHERE service IS NOT NULL` |

### Table-less SELECT

Arithmetic without a table:

```sql
SELECT 1 + 2;
SELECT 5 * (3 - 2) AS result;
SELECT 10 % 3;
```

---

## Data Types

| SQL Names | Internal Type | Rust Equivalent |
|-----------|---------------|-----------------|
| `INT`, `INTEGER`, `BIGINT` | Int64 | `i64` |
| `REAL`, `FLOAT`, `DOUBLE` | Float64 | `f64` |
| `BOOL`, `BOOLEAN` | Bool | `bool` |
| `TEXT`, `STRING`, `VARCHAR`, `CHAR`, `JSON` | String | `String` |
| `TIMESTAMP`, `DATETIME` | Timestamp | `i64` (epoch) |

Null values are supported for all types.

---

## Columnar Batch Ingestion

For high-throughput ingestion (7M+ rows/sec), use `ColumnarBatch` instead of row-by-row insertion. Each column is passed as a typed vector:

| Column Type | Rust Type |
|-------------|-----------|
| `BatchColumn::Int64` | `Vec<i64>` |
| `BatchColumn::Float64` | `Vec<f64>` |
| `BatchColumn::Bool` | `Vec<bool>` |
| `BatchColumn::String` | `Vec<String>` |
| `BatchColumn::Timestamp` | `Vec<i64>` |

All columns in a batch must have the same number of rows. Column order must match the table schema.

---

## Configuration

### DbOptions

| Option | Default | Description |
|--------|---------|-------------|
| `row_group_size` | 16,384 | Rows per row group. Larger values improve compression and query throughput. Benchmark default is 32,768. |
| `sync_on_flush` | true | Call fsync after each row group flush. Disable for faster ingestion when durability isn't critical. |
| `query_threads` | auto | Number of query worker threads. `None` uses all available CPU cores. |

### Server Options

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--listen` | `NARROWDB_LISTEN` | `127.0.0.1:5433` | TCP listen address |
| `--user` | `NARROWDB_USER` | `narrowdb` | Authentication username |
| `--password` | `NARROWDB_PASSWORD` | `narrowdb` | Authentication password |
| `--row-group-size` | `NARROWDB_ROW_GROUP_SIZE` | `16384` | Rows per row group |
| `--sync-on-flush` | `NARROWDB_SYNC_ON_FLUSH` | `true` | Fsync on flush |
| `--query-threads` | `NARROWDB_QUERY_THREADS` | auto | Number of query worker threads |

---

## Architecture

### Storage Format

NarrowDB uses a log-structured file format (magic: `NRWDB007`). Data is organized into **row groups** — columnar chunks of `row_group_size` rows each.

Each row group contains:
- Per-column min/max statistics (zone maps) for query-time pruning
- Per-column null bitmaps
- Compressed column data (LZ4 block compression)

### Column Encoding

| Type | Encoding |
|------|----------|
| Int64 / Timestamp | Delta encoding (base + u8/u16/u32 offsets depending on range) + LZ4 |
| Float64 | Raw IEEE 754 + LZ4 |
| Bool | Packed bit-vector + LZ4 |
| String (high cardinality) | Plain bytes + LZ4 |
| String (low cardinality) | Dictionary encoding (auto-detected) + LZ4 |

Dictionary encoding is applied automatically when a string column has unique values <= 50% of the row count in a row group.

### Query Engine

- **Zone map pruning** — row groups are skipped entirely when min/max stats prove no rows can match the filter
- **Projection pushdown** — only columns referenced by the query are decompressed
- **Vectorized bitmap filters** — filters produce packed u64 bitmaps per column, enabling compiler auto-vectorization
- **Dictionary-aware filtering** — equality filters on dictionary columns compare integer codes instead of strings
- **Parallel row group scanning** — rayon `par_iter` across row groups for all query types when >= 4 row groups
- **Array-indexed GROUP BY** — single-column dictionary GROUP BY uses direct array indexing instead of hash maps
