# narrowdb-server

External TCP server for the `narrowdb` embedded engine.

## Run

```bash
cargo run -p narrowdb-server -- ./logs.narrowdb --listen 127.0.0.1:5433
```

Optional flags:

- `--listen 127.0.0.1:5433`
- `--row-group-size 16384`
- `--sync-on-flush true|false`

## Protocol

The server speaks newline-delimited JSON over TCP.

### Request envelope

```json
{"id":1,"method":"ping","params":{}}
```

### Response envelope

```json
{"id":1,"ok":true,"result":{"pong":true,"version":"0.1.1"}}
```

Error response:

```json
{"id":1,"ok":false,"error":"unknown method: nope"}
```

### Methods

`ping`

```json
{"id":1,"method":"ping"}
```

`exec`

```json
{"id":2,"method":"exec","params":{"sql":"SELECT ts FROM logs LIMIT 1;"}}
```

`insert_columnar_batch`

```json
{
  "id": 3,
  "method": "insert_columnar_batch",
  "params": {
    "table": "logs",
    "batch": {
      "columns": [
        { "kind": "timestamp", "ints": [1, 2, 3] },
        { "kind": "string", "strings": ["api", "worker", "api"] }
      ]
    }
  }
}
```

`flush_all`

```json
{"id":4,"method":"flush_all"}
```
