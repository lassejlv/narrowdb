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

| Flag | Default | Description |
|------|---------|-------------|
| `<db-file>` | *required* | Path to the NarrowDB database file |
| `--listen` | `127.0.0.1:5433` | Address and port to bind |
| `--row-group-size` | `16384` | Rows per columnar row group |
| `--sync-on-flush` | `true` | Fsync data to disk on flush (`true`/`false`) |
| `--user` | `narrowdb` | Username for PostgreSQL MD5 auth |
| `--password` | `narrowdb` | Password for PostgreSQL MD5 auth |

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

Override defaults with CLI flags:

```bash
docker run -v narrowdb-data:/data -p 5433:5433 narrowdb-server \
  /data/mydb.narrowdb --listen 0.0.0.0:5433 --user admin --password s3cret
```

## Protocol support

- PostgreSQL MD5 password authentication
- Simple query protocol
- Extended query protocol (non-parameterized single statements)
- Parameterized prepared statements are not yet supported
