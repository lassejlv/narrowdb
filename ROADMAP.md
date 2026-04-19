# NarrowDB Roadmap

This roadmap is based on the current repository shape and codebase priorities, not a generic database feature wish list.

The repo already has a credible core:

- A Rust embedded engine in [src](./src)
- A PostgreSQL wire server in [crates/server](./crates/server)
- A Node binding in [crates/narrow-napi](./crates/narrow-napi)
- Solid architecture and benchmark material in [ARCHITECTURE.md](./ARCHITECTURE.md), [README.md](./README.md), and [bench](./bench)

The next step is to make NarrowDB more trustworthy, easier to operate, and easier to adopt before aggressively expanding features.

## Guiding Priorities

1. Correctness before feature count
2. Reproducible builds and releases before ecosystem expansion
3. Better server compatibility before broad SQL growth
4. Product-fit improvements for logs and time-series before general-purpose ambitions

## Phase 1: Foundation and Release Hygiene

Target: near term

Goal: eliminate avoidable repo friction and make releases trustworthy.

Status: completed on 2026-04-19

### Milestones

- [x] Align versions across:
  - [Cargo.toml](./Cargo.toml)
  - [crates/server/Cargo.toml](./crates/server/Cargo.toml)
  - [crates/narrow-napi/Cargo.toml](./crates/narrow-napi/Cargo.toml)
  - [crates/narrow-napi/package.json](./crates/narrow-napi/package.json)
- [x] Add a pinned Rust toolchain file
- [x] Add CI for:
  - `cargo fmt --check`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `cargo test --workspace`
- [x] Update doc/version drift in:
  - [docs.md](./docs.md)
  - [crates/server/README.md](./crates/server/README.md)
  - [README.md](./README.md) where needed
- [x] Add a short contributor guide with build, test, benchmark, and release commands

### Why this phase matters

The repo currently looks technically strong but operationally loose. Version skew, missing CI, and doc drift make the project harder to trust than the engine deserves.

## Phase 2: Engine Hardening

Target: after Phase 1

Goal: make the core storage and query engine safer to evolve.

Status: completed on 2026-04-19

### Milestones

- [x] Expand regression coverage around:
  - Null handling
  - Aggregate correctness
  - Expression evaluation
  - Grouping edge cases
  - Multi-threaded query behavior
  - Reopen/persistence behavior
- [x] Add compatibility tests for on-disk file format stability
- [x] Add failure-oriented tests around flush and reopen paths
- [x] Review rewrite-heavy operations for robustness:
  - `ALTER TABLE`
  - `DROP TABLE`
  - Full-file rewrite paths in [src/storage.rs](./src/storage.rs)
- [x] Document storage invariants and compatibility expectations

### Focus areas

- [src/engine/mod.rs](./src/engine/mod.rs)
- [src/engine/compile.rs](./src/engine/compile.rs)
- [src/engine/scan.rs](./src/engine/scan.rs)
- [src/storage.rs](./src/storage.rs)

## Phase 3: Server Maturity

Target: parallel with late Phase 2 or immediately after

Goal: make the PostgreSQL wire server practical for real client usage.

Status: completed on 2026-04-19

### Milestones

- [x] Add prepared statement support
- [x] Improve compatibility with common PostgreSQL clients and drivers
- [x] Improve startup/config validation and error messages
- [x] Add better operational logging
- [x] Clarify and tighten authentication posture
- [x] Document supported and unsupported protocol features clearly

### Focus areas

- [crates/server/src/main.rs](./crates/server/src/main.rs)
- [crates/server/README.md](./crates/server/README.md)

### Why this phase matters

The server is one of the fastest paths to adoption, but protocol gaps and weak operational polish limit how seriously downstream users can treat it.

## Phase 4: Query Surface Expansion

Target: after the engine and server contracts are more stable

Goal: expand SQL support without turning the engine into a pile of special cases.

Status: completed on 2026-04-19

### Milestones

- [x] Expand filter and expression coverage
- [x] Add more aggregate and projection capabilities
- [x] Improve ordering and grouping coverage
- [x] Tighten unsupported-statement diagnostics
- [x] Keep parser, compiler, and execution responsibilities cleanly separated

### Guardrails

- Prefer features that strengthen log/time-series analytics
- Avoid adding broad SQL surface area without a clear execution model
- Treat joins as a deliberate product decision, not an automatic milestone

### Focus areas

- [src/sql.rs](./src/sql.rs)
- [src/engine/compile.rs](./src/engine/compile.rs)
- [src/engine/scan.rs](./src/engine/scan.rs)

## Phase 5: Ingestion and Workload Fit

Target: medium term

Goal: make NarrowDB feel purpose-built for its target workloads.

Status: completed on 2026-04-19

### Milestones

- [x] Improve bulk ingestion ergonomics
- [x] Improve timestamp-heavy workflow support
- [x] Add examples for realistic log and time-series ingestion
- [x] Consider retention or partition-like ergonomics if they fit the storage model
- [x] Add clearer guidance on when to use row inserts vs columnar batch ingestion

### Focus areas

- [src/types.rs](./src/types.rs)
- [src/engine/mod.rs](./src/engine/mod.rs)
- [README.md](./README.md)
- [docs.md](./docs.md)

## Phase 6: Distribution and Adoption

Target: later

Goal: make the project easier to consume without increasing maintenance chaos.

Status: completed on 2026-04-19

### Milestones

- [x] Make release workflows less manual and more consistent
- [x] Add changelog/release notes discipline
- [x] Add an `examples/` directory with:
  - Embedded Rust usage
  - Server usage from PostgreSQL clients
  - High-throughput ingestion example
- [x] Improve N-API package story only if long-term support is intended
- [x] Add more contributor-facing docs as the project surface grows

## Suggested Execution Order

1. Foundation and release hygiene
2. Engine hardening
3. Server maturity
4. Query surface expansion
5. Ingestion and workload fit
6. Distribution and adoption

## Recommended Next Release Scope

If only one release cycle is available, the highest-value scope is:

- [x] Version alignment
- [x] CI and toolchain pinning
- [x] Documentation fixes
- [x] More engine regression coverage
- [x] A short contributor guide

That release would materially improve trust in the project without distracting from the core engine.

## Notes

- NarrowDB already has a strong architectural story. The immediate need is disciplined execution around correctness, compatibility, and packaging.
- The benchmark harness in [bench](./bench) is a real asset. It should stay central to performance work and release validation.
- The best path to future growth is to make the current core boringly reliable before chasing broader SQL ambition.
