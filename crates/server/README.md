# narrowdb-server

PostgreSQL wire-compatible server for the [narrowdb](https://github.com/lassejlv/narrowdb) embedded columnar engine. Connect with any PostgreSQL client (`psql`, database GUIs, language drivers) and run SQL queries against your NarrowDB databases.

## Installation

```bash
cargo install narrowdb-server
```

## Usage

```bash
narrowdb-server <db-file> [options]
```

## Configuration

| Flag | Env var | Default | Description |
|------|--------|---------|-------------|
| `<db-file>` | `NARROWDB_PATH` | *required* | Path to the NarrowDB database file |
| `--listen` | `NARROWDB_LISTEN` | `127.0.0.1:5433` | Address and port to bind |
| `--row-group-size` | `NARROWDB_ROW_GROUP_SIZE` | `16384` | Rows per columnar row group |
| `--sync-on-flush` | `NARROWDB_SYNC_ON_FLUSH` | `true` | Fsync data to disk on flush (`true`/`false`) |
| `--user` | `NARROWDB_USER` | `narrowdb` | Username for PostgreSQL MD5 auth |
| `--password` | `NARROWDB_PASSWORD` | `narrowdb` | Password for PostgreSQL MD5 auth |

CLI flags take priority over environment variables.

### Example

```bash
narrowdb-server ./logs.narrowdb --listen 0.0.0.0:5433 --user admin --password s3cret
```

## Connecting

```bash
PGPASSWORD=s3cret psql "host=127.0.0.1 port=5433 user=admin dbname=logs"
```

```sql
CREATE TABLE logs (ts TIMESTAMP, service TEXT, status INT);
INSERT INTO logs VALUES (1, 'api', 200);
SELECT * FROM logs WHERE status = 200;
```

## Docker

Build from the repository root:

```bash
docker build -f crates/server/Dockerfile -t narrowdb-server .
```

Run with a volume for data persistence:

```bash
docker run -v narrowdb-data:/data -p 5433:5433 narrowdb-server
```

Configure with environment variables:

```bash
docker run -v narrowdb-data:/data -p 5433:5433 \
  -e NARROWDB_USER=admin \
  -e NARROWDB_PASSWORD=s3cret \
  narrowdb-server
```

## Protocol support

- PostgreSQL MD5 password authentication
- Simple query protocol
- Extended query protocol (non-parameterized single statements)
- Parameterized prepared statements are not yet supported
