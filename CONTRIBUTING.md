# Contributing

Thanks for working on NarrowDB.

This repo is small on purpose. Keep changes focused, keep the storage/query contract stable, and prefer regression tests over speculative feature work.

## Prerequisites

- Rust `1.94.1`
- `cargo`
- Optional: Docker for the server image
- Optional: Node.js only if you are working on [crates/narrow-napi](./crates/narrow-napi)

The pinned toolchain lives in [rust-toolchain.toml](./rust-toolchain.toml).

## Common Commands

Build everything:

```bash
cargo build --workspace
```

Run tests:

```bash
cargo test --workspace
```

Check formatting:

```bash
cargo fmt --all --check
```

Run Clippy:

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

## Running the Project

Run the embedded CLI:

```bash
cargo run -- exec ./logs.db "SELECT 1;"
```

Run the server:

```bash
cargo run -p narrowdb-server -- ./logs.narrowdb --listen 127.0.0.1:5433 --query-threads 4 --user narrowdb --password secret
```

## Benchmarking

Run the built-in benchmark:

```bash
cargo run --release -- bench ./bench.db 1000000
```

Run the comparison benchmark against DuckDB:

```bash
cargo run --release --features bench-duckdb --bin bench-compare -- 1000000 --query-threads 8
```

Benchmark definitions live in [tools/bench/common.rs](./tools/bench/common.rs).

## Docs To Keep In Sync

If you change flags, versions, or supported behavior, update these files in the same patch:

- [README.md](./README.md)
- [docs/USER_GUIDE.md](./docs/USER_GUIDE.md)
- [crates/server/README.md](./crates/server/README.md)
- [docs/ROADMAP.md](./docs/ROADMAP.md), if priorities shift

## Release Hygiene

Before cutting a release:

```bash
./tools/release/check-release.sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Also verify version alignment across:

- [Cargo.toml](./Cargo.toml)
- [crates/server/Cargo.toml](./crates/server/Cargo.toml)
- [crates/narrow-napi/Cargo.toml](./crates/narrow-napi/Cargo.toml)
- [crates/narrow-napi/package.json](./crates/narrow-napi/package.json)

Release steps and artifact expectations live in [docs/RELEASE.md](./docs/RELEASE.md).
