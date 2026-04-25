#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

root_version="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -n1)"
server_dep_version="$(sed -n 's/^narrowdb = { path = "..\/..", version = "\(.*\)" }/\1/p' crates/server/Cargo.toml | head -n1)"
napi_dep_version="$(sed -n 's/^narrowdb = { path = "..\/..", version = "\(.*\)" }/\1/p' crates/narrow-napi/Cargo.toml | head -n1)"
napi_pkg_version="$(sed -n 's/^[[:space:]]*"version":[[:space:]]*"\([^"]*\)".*$/\1/p' crates/narrow-napi/package.json | head -n1)"

if [[ -z "$root_version" || -z "$server_dep_version" || -z "$napi_dep_version" || -z "$napi_pkg_version" ]]; then
  echo "failed to read one or more release versions" >&2
  exit 1
fi

if [[ "$root_version" != "$server_dep_version" ]]; then
  echo "server crate depends on narrowdb $server_dep_version but root version is $root_version" >&2
  exit 1
fi

if [[ "$root_version" != "$napi_dep_version" ]]; then
  echo "napi crate depends on narrowdb $napi_dep_version but root version is $root_version" >&2
  exit 1
fi

if [[ "$root_version" != "$napi_pkg_version" ]]; then
  echo "npm package version $napi_pkg_version does not match root version $root_version" >&2
  exit 1
fi

if ! grep -q "^## \\[$root_version\\]" docs/CHANGELOG.md; then
  echo "docs/CHANGELOG.md is missing a section for version $root_version" >&2
  exit 1
fi

echo "release metadata is consistent for version $root_version"
