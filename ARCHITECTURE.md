# NarrowDB Architecture

> A beginner-friendly guide to how NarrowDB works under the hood.

---

## What is NarrowDB?

NarrowDB is a **columnar database** designed for log and time-series data. Unlike traditional row-based databases (which store each row's data together), NarrowDB stores data **by column** — all values for one column are kept next to each other. This makes filtering, aggregating, and compressing data much faster for analytical workloads.

Think of it this way:

| Row-based storage (like PostgreSQL) | Columnar storage (NarrowDB) |
|---|---|
| Row 1: `[ts=1, level="info", status=200]` | ts column: `[1, 2, 3]` |
| Row 2: `[ts=2, level="error", status=500]` | level column: `["info", "error", "error"]` |
| Row 3: `[ts=3, level="error", status=503]` | status column: `[200, 500, 503]` |

If you only need the `status` column, a columnar database can skip reading `ts` and `level` entirely.

---

## The Big Picture

When you run a SQL query like `SELECT * FROM logs WHERE level = 'error'`, here's what happens end-to-end:

```
  Your SQL
    │
    ▼
  ┌──────────────┐
  │  1. Parse     │  Turn SQL text into structured commands
  └──────┬───────┘
         │
         ▼
  ┌──────────────┐
  │  2. Compile   │  Figure out which columns and filters are needed
  └──────┬───────┘
         │
         ▼
  ┌──────────────┐
  │  3. Scan      │  Read data from disk, skip what we can, filter the rest
  └──────┬───────┘
         │
         ▼
  ┌──────────────┐
  │  4. Return    │  Sort, limit, and give you the result
  └──────────────┘
```

Let's explore each step and the data structures in between.

---

## Core Concepts

### Tables

A **Table** is the top-level container — just like in any database. It has:

- A **name** (e.g. `"logs"`)
- A **schema** — the list of columns and their types (e.g. `ts: TIMESTAMP, level: TEXT, status: INT`)
- A **pending batch** — rows you've inserted that haven't been saved to disk yet
- A list of **row groups** — chunks of data already saved to disk

```
  Table "logs"
  ┌─────────────────────────────────────────────────┐
  │  Schema: ts (Timestamp), level (String), status (Int)  │
  │                                                         │
  │  Pending Batch:  [rows waiting to be flushed]           │
  │                                                         │
  │  Row Groups:                                            │
  │    [0] 16,384 rows  – saved to disk                     │
  │    [1] 16,384 rows  – saved to disk                     │
  │    [2]  9,001 rows  – saved to disk                     │
  └─────────────────────────────────────────────────────────┘
```

### Row Groups

Data is stored in **row groups** — chunks of up to 16,384 rows (configurable via `row_group_size`). Each row group stores data **column by column**, with:

- **Per-column statistics** (min/max values and null count) — used to skip entire row groups during queries
- **Per-column null bitmaps** — track which values are NULL
- **Per-column compressed data** — the actual values, compressed with LZ4

```
  Row Group (16,384 rows)
  ┌──────────────────────────────────────────────────────────┐
  │                                                           │
  │  ts column:     min=1000, max=10500, nulls=0              │
  │                  ┌──── delta-encoded integers ────┐        │
  │                  │ base=1000, offsets=[0,1,2,...] │ LZ4   │
  │                  └─────────────────────────────────┘        │
  │                                                           │
  │  level column:  min="error", max="warn", nulls=3           │
  │                  ┌──── dictionary-encoded strings ──┐      │
  │                  │ dict=["error","info","warn"]       │ LZ4 │
  │                  │ codes=[1,0,0,2,1,...]              │     │
  │                  └────────────────────────────────────┘     │
  │                                                           │
  │  status column: min=200, max=503, nulls=0                 │
  │                  ┌──── delta-encoded integers ────┐        │
  │                  │ base=200, offsets=[0,300,303,...]│ LZ4   │
  │                  └─────────────────────────────────┘        │
  │                                                           │
  └──────────────────────────────────────────────────────────┘
```

### Pending Batch

When you INSERT rows, they first go into the **pending batch** — an in-memory buffer that accumulates rows column-by-column. When the batch reaches `row_group_size` rows, it's automatically **flushed** to disk as a new row group.

```
  INSERT INTO logs VALUES (4, 'info', 200)
                     │
                     ▼
  PendingBatch (in memory)
  ┌──────────────────────────────────────┐
  │  ts:    [4]                          │
  │  level: ["info"]                     │
  │  status: [200]                       │
  └──────────────────────────────────────┘
       │
       │  ... more inserts until we hit 16,384 rows
       ▼
  Flush to disk → becomes a new Row Group
```

> **Important:** Before running a SELECT, the pending batch is automatically flushed so you can see your latest data.

## Storage Invariants

These rules matter when changing the engine or file format:

- Every database file starts with a fixed 8-byte magic header: `NRWDB007`.
- The on-disk layout is a sequence of typed records:
  - `CREATE TABLE`
  - `ROW_GROUP`
- INSERT-style writes append new row-group records. They do not rewrite existing row groups.
- Schema-changing operations such as `ALTER TABLE` and `DROP TABLE` first flush pending rows, then rebuild the full file from materialized tables.
- File rewrites are written to a temporary file and then renamed into place, so schema changes do not destructively truncate the live database before the replacement snapshot is ready.
- `query()` is intentionally read-only: it does not flush pending rows. `execute_sql()` does flush before `SELECT`.
- Compatibility is currently tied to the magic/version header. If the header changes, old files must either be migrated explicitly or rejected clearly.

---

## Data Flow: Writing Data

### Insertion Paths

You can insert data in two ways:

1. **Row-by-row** (`INSERT INTO logs VALUES (...)`) — each row is appended to the pending batch
2. **Columnar batch** (`insert_columnar_batch()`) — you pass entire columns at once, which is much faster (7M+ rows/sec)

```
  ┌─────────────┐     ┌─────────────┐
  │  SQL INSERT  │     │  Columnar   │
  │  (row-by-row)│     │  Batch API  │
  └──────┬───────┘     └──────┬──────┘
         │                     │
         ▼                     ▼
  ┌─────────────────────────────────────┐
  │          PendingBatch               │
  │  (in-memory, column-by-column)      │
  │                                      │
  │  ts:    [1, 2, 3, 4, ...]           │
  │  level: ["info", "error", ...]       │
  │  status: [200, 500, ...]            │
  └──────────────┬──────────────────────┘
                 │
                 │  When batch is full (row_group_size rows)
                 ▼
  ┌─────────────────────────────────────┐
  │     Flush to disk                   │
  │                                      │
  │  1. Compute per-column statistics    │
  │     (min, max, null count)           │
  │  2. Choose encoding per column       │
  │  3. Compress each column with LZ4    │
  │  4. Append to the database file      │
  │  5. Re-mmap the file                │
  └──────────────────────────────────────┘
```

---

## Data Flow: Reading Data (Query Pipeline)

When you run a SELECT query, NarrowDB goes through these steps:

### Step 1: Parse SQL

The `sqlparser` crate turns your SQL text into a **Command** enum:

```
  "SELECT service, COUNT(*) FROM logs WHERE level = 'error' GROUP BY service"
       │
       ▼
  Command::Select(SelectPlan {
      table_name: "logs",
      filters: [level = "error"],
      projections: [service, COUNT(*)],
      group_by: ["service"],
      order_by: None,
      limit: None,
  })
```

### Step 2: Compile

The compiler resolves column names to indexes and prepares efficient filter/projection structures:

- **CompiledFilter** — column index + comparison operator + value (e.g. "column 1 equals 'error'")
- **CompiledProjection** — what to output for each column in the result
- **Required columns** — which columns actually need to be read from disk

```
  "WHERE level = 'error'"  →  CompiledFilter { column_index: 1, op: Eq, value: "error" }
  "SELECT service"          →  CompiledProjection { expr: Column(2), alias: "service" }
  "COUNT(*)"                →  CompiledProjection { expr: Aggregate(Count, None), alias: "count" }

  Required columns: [1, 2]  (only level and service, skip ts and status!)
```

### Step 3: Scan Row Groups

This is where the magic happens. NarrowDB scans row groups, but it can skip a lot of work:

```
  ┌─────────────────────────────────────────────────────────────────┐
  │                     For each Row Group:                         │
  │                                                                 │
  │  ┌─────────────────────────────────────────────────────────┐    │
  │  │  Zone Map Pruning                                        │    │
  │  │                                                           │    │
  │  │  "level min='error', max='warn'"                         │    │
  │  │  Looking for: level = 'info'                             │    │
  │  │  → 'info' is outside [error, warn] → SKIP this group!   │    │
  │  │                                                           │    │
  │  │  "status min=200, max=200"                               │    │
  │  │  Looking for: status >= 500                              │    │
  │  │  → max is 200, can't match → SKIP this group!           │    │
  │  └─────────────────────────────────────────────────────────┘    │
  │                                                                 │
  │  ┌─────────────────────────────────────────────────────────┐    │
  │  │  Lazy Column Loading (Projection Pushdown)                │    │
  │  │                                                           │    │
  │  │  Only decode the columns the query actually needs.         │    │
  │  │  If you SELECT only "service", we never decompress       │    │
  │  │  the "ts" or "status" columns.                            │    │
  │  └─────────────────────────────────────────────────────────┘    │
  │                                                                 │
  │  ┌─────────────────────────────────────────────────────────┐    │
  │  │  Vectorized Filtering (Selection Bitmap)                  │    │
  │  │                                                           │    │
  │  │  Instead of checking one row at a time, we use a          │    │
  │  │  bitmap of u64 words. Each bit = one row.                 │    │
  │  │                                                           │    │
  │  │  level = 'error':  [1, 0, 1, 1, 0, ...]  (bit matches)  │    │
  │  │  status >= 500:   [0, 1, 1, 0, 0, ...]                  │    │
  │  │  AND result:       [0, 0, 1, 0, 0, ...]                  │    │
  │  │                                                           │    │
  │  │  For dictionary-encoded strings, we compare integer       │    │
  │  │  codes instead of full strings — much faster!             │    │
  │  └─────────────────────────────────────────────────────────┘    │
  │                                                                 │
  │  ┌─────────────────────────────────────────────────────────┐    │
  │  │  Parallel Scanning (via rayon)                           │    │
  │  │                                                           │    │
  │  │  If there are ≥4 row groups, they are scanned in         │    │
  │  │  parallel across CPU cores.                               │    │
  │  └─────────────────────────────────────────────────────────┘    │
  └─────────────────────────────────────────────────────────────────┘
```

### Step 4: Aggregation

For queries with `GROUP BY` or aggregate functions like `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`:

```
  SELECT service, COUNT(*) FROM logs GROUP BY service

  ┌──────────────────┐    ┌──────────────────┐
  │  Row Group 0:     │    │  Row Group 1:     │
  │                   │    │                   │
  │  "api" → 3 rows  │    │  "api" → 2 rows   │
  │  "worker" → 5 row│    │  "worker" → 4 row│
  └────────┬─────────┘    └────────┬─────────┘
           │                         │
           │  Partial aggregations   │
           │  computed per group     │
           │                         │
           └──────────┬──────────────┘
                      │
                      ▼
           ┌───────────────────────┐
           │  Merge: add up the    │
           │  partial states       │
           │                       │
           │  "api" → 3 + 2 = 5    │
           │  "worker" → 5 + 4 = 9 │
           └───────────────────────┘
```

> **Special optimization:** When grouping by a single dictionary-encoded string column, NarrowDB uses **array-indexed aggregation** instead of a hash map — it creates an array indexed by the dictionary codes. This is much faster than hashing.

### Step 5: Sort & Limit

Finally, results are sorted and limited:

```
  ORDER BY errors DESC LIMIT 5

  If the limit is small relative to the total rows,
  we use partial_sort (select_nth_unstable) to avoid
  sorting the entire result set.
```

---

## On-Disk Format

NarrowDB uses a **log-structured** file format — new data is always appended to the end, never modified in place. The file starts with the magic bytes `"NRWDB007"`.

```
  ┌──────────────────────────────────────────────────────────────────┐
  │  Database File                                                  │
  │                                                                  │
  │  ┌──────────┐                                                    │
  │  │ Header   │  8 bytes: "NRWDB007"                              │
  │  └──────────┘                                                    │
  │                                                                  │
  │  ┌──────────┐                                                    │
  │  │ Record 1 │  CREATE TABLE logs (ts TIMESTAMP, level TEXT, ...)│
  │  └──────────┘                                                    │
  │                                                                  │
  │  ┌──────────┐                                                    │
  │  │ Record 2 │  ROW GROUP: 16,384 rows of table "logs"          │
  │  └──────────┘                                                    │
  │                                                                  │
  │  ┌──────────┐                                                    │
  │  │ Record 3 │  ROW GROUP: 16,384 rows of table "logs"          │
  │  └──────────┘                                                    │
  │                                                                  │
  │  ┌──────────┐                                                    │
  │  │ Record 4 │  CREATE TABLE metrics (ts TIMESTAMP, value REAL)  │
  │  └──────────┘                                                    │
  │                                                                  │
  │  ...more records appended as data is written...                  │
  └──────────────────────────────────────────────────────────────────┘
```

Each record is prefixed with a type byte and a length:

```
  ┌──────────┬──────────┬──────────────────────────────┐
  │ kind(1B) │ len(8B)  │ payload                       │
  │          │          │                                │
  │ 1 = CREATE TABLE                            │
  │ 2 = ROW GROUP                               │
  └──────────┴──────────┴──────────────────────────────┘
```

### Row Group Layout

Inside a ROW GROUP record:

```
  ┌──────────────┬──────────────────────────────────────────────────┐
  │ table_name    │ "logs" (length-prefixed string)                 │
  │ row_count     │ 16384 (u32)                                     │
  │ metadata_len  │ ... (u32)                                        │
  ├──────────────┼──────────────────────────────────────────────────┤
  │ METADATA     │  For each column:                                 │
  │              │    ┌─ Statistics ──────────────────────────────┐  │
  │              │    │  null_count: u32                           │  │
  │              │    │  min_value: (optional, type-dependent)    │  │
  │              │    │  max_value: (optional, type-dependent)    │  │
  │              │    └───────────────────────────────────────────┘  │
  │              │    ┌─ Null Bitmap (if any NULLs) ──────────────┐  │
  │              │    │  Packed bit-vector (bit set = value)      │  │
  │              │    └───────────────────────────────────────────┘  │
  │              │    ┌─ Data Blob Descriptor ────────────────────┐  │
  │              │    │  offset, stored_len, raw_len, compression │  │
  │              │    └───────────────────────────────────────────┘  │
  ├──────────────┼──────────────────────────────────────────────────┤
  │ DATA SECTION │  Compressed column data blobs                     │
  │              │  (memory-mapped, decoded lazily on demand)       │
  └──────────────┴──────────────────────────────────────────────────┘
```

> **Why this layout?** The metadata section is small and read first, allowing NarrowDB to check statistics (zone maps) and decide whether to even look at the data section. The data section is memory-mapped (`mmap`) and columns are only decompressed when actually needed.

---

## Column Encodings

Different column types use different encoding strategies to save space:

### Integers (Int64, Timestamp)

**Delta encoding** — instead of storing absolute values, store a base value and small offsets from it:

```
  Original values:  [1000, 1001, 1003, 1005, 1010]

  Stored as:
    base = 1000
    offsets = [0, 1, 3, 5, 10]   ← much smaller numbers!

  If the range fits in u8:  each offset is 1 byte   (values 0–255)
  If the range fits in u16: each offset is 2 bytes   (values 0–65535)
  If the range fits in u32: each offset is 4 bytes
  Otherwise:               store as raw i64 values
```

Then the result is **LZ4-compressed** for even more savings.

### Floats (Float64)

Stored as raw 8-byte IEEE 754 values, then LZ4-compressed.

### Booleans

Packed into bit-vectors (8 booleans per byte), then LZ4-compressed.

### Strings

NarrowDB automatically chooses between two strategies:

**Low-cardinality strings** (unique values ≤ 50% of rows) — **Dictionary encoding**:

```
  Original:  ["error", "info", "error", "warn", "info"]

  Dictionary:  [0: "error",  1: "info",  2: "warn"]
  Codes:       [0, 1, 0, 2, 1]

  Now "error" is just the number 0, "info" is 1, etc.
  Equality filters compare codes (integers) instead of strings!
```

**High-cardinality strings** — stored as length-prefixed plain bytes, then LZ4-compressed.

---

## Null Handling

Each column can have NULL values. A **null bitmap** tracks which positions are NULL:

- Bit **set** (1) = value is present
- Bit **clear** (0) = value is NULL

```
  Null bitmap:  [1, 1, 0, 1, 1, 0, 1, 1]
                 ↑        ↑        ↑
                 |   NULL here   NULL here
```

When filtering, NULL values never pass equality or comparison checks (SQL semantics). The null count is also stored in statistics, so `IS NULL` / `IS NOT NULL` can prune row groups without reading the data.

---

## Concurrency Model

- **Writes** acquire a **write lock** (`RwLock`) — only one writer at a time
- **Reads** via `query()` acquire a **read lock** — multiple concurrent readers allowed
- **Reads** via `execute_sql()` acquire a write lock (because they flush pending data first)
- An optional **background flush thread** auto-flushes pending batches at a configurable interval

```
  ┌───────────────────────────────┐
  │         NarrowDb               │
  │     Arc<RwLock<DbInner>>       │
  │                                │
  │  execute_sql() ── write lock ─┤── one at a time
  │  insert_row()  ── write lock ─┤
  │  flush_all()   ── write lock ─┤
  │                                │
  │  query()       ── read lock  ─┤── many at once (but
  │                                │   can't see unflushed data)
  └───────────────────────────────┘
```

---

## TCP Server (PostgreSQL wire protocol)

NarrowDB also ships with a standalone TCP server (`crates/server`) that speaks the **PostgreSQL wire protocol**. This means you can connect with `psql` or any PostgreSQL client:

```
  ┌───────────┐          ┌──────────────────┐          ┌──────────┐
  │  psql      │ ── pg ── │  narrowdb-server  │ ────── │  .db file │
  │  client    │  protocol │  (port 5433)      │  reads  │           │
  └───────────┘          └──────────────────┘          └──────────┘
```

The server supports password authentication and maps PostgreSQL message types to NarrowDB operations.

---

## Supported Data Types

| SQL Type | Internal Type | Description |
|---|---|---|
| `INT`, `INTEGER`, `BIGINT` | Int64 | 64-bit signed integers |
| `REAL`, `FLOAT`, `DOUBLE` | Float64 | 64-bit floating point |
| `BOOL`, `BOOLEAN` | Bool | true/false |
| `TEXT`, `STRING`, `VARCHAR`, `CHAR`, `JSON` | String | Variable-length text |
| `TIMESTAMP`, `DATETIME` | Timestamp | Integer epoch seconds |

---

## Configuration

| Option | Default | What It Does |
|---|---|---|
| `row_group_size` | 16,384 | How many rows per row group before flushing. Larger = better compression and faster scans, but more memory. |
| `sync_on_flush` | `true` | Call `fsync` after each flush. `false` = faster writes, but risk of data loss on crash. |
| `auto_flush_interval` | `None` | If set, a background thread flushes pending data every N duration. |

---

## Summary: Why Is It Fast?

1. **Columnar storage** — only read the columns you need
2. **Zone map pruning** — skip entire row groups based on min/max stats
3. **Dictionary encoding** — compare integer codes instead of strings
4. **Vectorized bitmaps** — filter 64 rows at a time with CPU-friendly u64 operations
5. **Parallel scanning** — row groups are processed in parallel across CPU cores
6. **Lazy decompression** — columns are only decoded from mmap when accessed
7. **Delta encoding** — store small offsets instead of full 8-byte integers
8. **LZ4 compression** — fast decompression with good ratios
9. **Array-indexed GROUP BY** — skip hash maps when grouping by a dictionary column
