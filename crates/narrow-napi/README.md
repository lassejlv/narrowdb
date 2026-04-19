# @narrowdb/node

Node.js bindings for NarrowDB built with `napi-rs`.

## Status

This package is intended for supported distribution, not as an afterthought wrapper. The repository CI and release flow verify version alignment across the Rust crate and npm package metadata.

## Install

```bash
npm install @narrowdb/node
```

## Notes

- The package ships prebuilt platform artifacts through the npm release workflow.
- The source of truth for engine behavior remains the Rust crate in the repository root.
- If you change public behavior or package metadata, keep [CHANGELOG.md](../../CHANGELOG.md) and the release workflow in sync.
