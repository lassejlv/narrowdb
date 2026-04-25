# Release Guide

This repository ships three public distribution surfaces:

- Rust crate: `narrowdb`
- Docker image for `narrowdb-server`
- npm package: `@narrowdb/node`

## Before tagging

1. Update versions together:
   - [Cargo.toml](../Cargo.toml)
   - [crates/server/Cargo.toml](../crates/server/Cargo.toml)
   - [crates/narrow-napi/Cargo.toml](../crates/narrow-napi/Cargo.toml)
   - [crates/narrow-napi/package.json](../crates/narrow-napi/package.json)
2. Add a matching changelog section in [CHANGELOG.md](./CHANGELOG.md).
3. Run:

```bash
./tools/release/check-release.sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Tagging

Create and push a version tag in the form `vX.Y.Z`.

Example:

```bash
git tag v0.3.1
git push origin v0.3.1
```

Tag pushes trigger:

- Docker publish workflow
- npm publish workflow

## Artifact expectations

- Docker image tags:
  - `ghcr.io/lassejlv/narrowdb:vX.Y.Z`
  - `ghcr.io/lassejlv/narrowdb:latest`
- npm package:
  - `@narrowdb/node@X.Y.Z`

## Notes

- Rust crate publishing is still a separate explicit step if you want crates.io publication.
- npm publication assumes the N-API artifacts build successfully for all configured targets.
- MD5 auth on the server should be treated as trusted-network only until stronger auth lands.
