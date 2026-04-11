# narrowdb

A lightweight, append-only columnar database engine written in Rust, designed for log and time-series workloads.

## Features

- **SQL interface** — `CREATE TABLE`, `INSERT INTO`, and `SELECT` with `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`, and aggregate functions (`COUNT`, `SUM`, `MIN`, `MAX`, `AVG`)
- **Columnar storage** — data is stored in columnar row groups with min/max statistics for efficient predicate pushdown
- **Append-only format** — single-file, durable storage with optional `fsync` on flush
- **Zero dependencies at runtime** — no external services, no background threads

## Usage

```bash
# Run a SQL statement
narrowdb exec <db-file> "<sql>"

# Run a benchmark
narrowdb bench <db-file> [rows]
```

### Examples

```bash
# Create a table and insert rows
narrowdb exec logs.db "CREATE TABLE logs (ts TIMESTAMP, level TEXT, service TEXT, status INT, duration REAL);"

narrowdb exec logs.db "INSERT INTO logs VALUES (1, 'info', 'api', 200, 12.0), (2, 'error', 'api', 500, 120.0), (3, 'error', 'worker', 503, 250.0);"

# Query with filters, aggregation, and ordering
narrowdb exec logs.db "SELECT service, COUNT(*) AS errors FROM logs WHERE level = 'error' GROUP BY service ORDER BY errors DESC LIMIT 2;"
```

## Data Types

| SQL keyword               | Rust type |
|---------------------------|-----------|
| `INT`, `INTEGER`, `BIGINT`| `i64`     |
| `REAL`, `FLOAT`, `DOUBLE` | `f64`     |
| `BOOL`, `BOOLEAN`         | `bool`    |
| `TEXT`, `STRING`, `VARCHAR`| `String` |
| `TIMESTAMP`, `DATETIME`   | `i64`     |

## Building

```bash
cargo build --release
```

## Running Tests

```bash
cargo test
```

## Benchmarks

5M rows, Apple Silicon, release build:

| Metric | Result |
|--------|-------|
| Ingest throughput | ~4.4M rows/sec |
| Bytes per row | 56.02 |
| Errors by service (filtered GROUP BY) | ~6.5ms |
| Avg duration for 5xx (filtered GROUP BY) | ~3.8ms |
| Slow request count (filtered COUNT) | ~3.2ms |
| Reopen from disk (5M rows) | ~470ms |

## Architecture

Data is stored in a single file using a custom binary format with two record types:

1. **CreateTable** — stores the table schema (name + column definitions)
2. **RowGroup** — stores a batch of rows in columnar layout with per-column min/max statistics

On startup, the file is replayed to reconstruct table schemas and row groups in memory. Queries scan row groups, skipping those whose statistics prove they cannot match the filter predicates.
