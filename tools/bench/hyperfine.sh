#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RESULTS_DIR="$ROOT_DIR/tools/bench/results"

rows=1000000
query_threads="${QUERY_THREADS:-}"
warmup=3
min_runs=10
markdown_out=""
json_out=""

usage() {
  cat <<'EOF'
Usage: tools/bench/hyperfine.sh [options]

Options:
  --rows <n>             Row count to generate per benchmark run. Default: 1000000
  --query-threads <n>    Query thread count passed to the existing benchmark binaries.
                         Default: use all available cores.
  --warmup <n>           Hyperfine warmup runs. Default: 3
  --min-runs <n>         Minimum Hyperfine timing runs. Default: 10
  --export-markdown <p>  Markdown export path. Default: tools/bench/results/hyperfine.md
  --export-json <p>      JSON export path. Default: tools/bench/results/hyperfine.json
  -h, --help             Show this help text

Examples:
  tools/bench/hyperfine.sh --rows 1000000 --query-threads 8
  tools/bench/hyperfine.sh --rows 100000 --warmup 1 --min-runs 5
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rows)
      rows="$2"
      shift 2
      ;;
    --query-threads)
      query_threads="$2"
      shift 2
      ;;
    --warmup)
      warmup="$2"
      shift 2
      ;;
    --min-runs)
      min_runs="$2"
      shift 2
      ;;
    --export-markdown)
      markdown_out="$2"
      shift 2
      ;;
    --export-json)
      json_out="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if ! command -v hyperfine >/dev/null 2>&1; then
  echo "hyperfine is required but was not found in PATH." >&2
  exit 1
fi

if [[ -z "$query_threads" ]]; then
  if command -v sysctl >/dev/null 2>&1; then
    query_threads="$(sysctl -n hw.logicalcpu 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1)"
  elif command -v nproc >/dev/null 2>&1; then
    query_threads="$(nproc)"
  else
    query_threads=1
  fi
fi

mkdir -p "$RESULTS_DIR"

if [[ -z "$markdown_out" ]]; then
  markdown_out="$RESULTS_DIR/hyperfine.md"
fi

if [[ -z "$json_out" ]]; then
  json_out="$RESULTS_DIR/hyperfine.json"
fi

build_cmd=(
  cargo build
  --release
  --features bench-duckdb
  --bin bench-narrow
  --bin bench-duckdb
)

echo "Building benchmark binaries..."
(cd "$ROOT_DIR" && "${build_cmd[@]}")

prepare_cmd="rm -f /tmp/bench-narrowdb.db /tmp/bench-duckdb.db && rm -rf /tmp/bench-duckdb.db.wal"
narrow_cmd="$ROOT_DIR/target/release/bench-narrow $rows --query-threads $query_threads"
duckdb_cmd="$ROOT_DIR/target/release/bench-duckdb $rows --query-threads $query_threads"

echo "Running hyperfine benchmark..."
echo "  rows=$rows"
echo "  query_threads=$query_threads"
echo "  warmup=$warmup"
echo "  min_runs=$min_runs"
echo "  markdown=$markdown_out"
echo "  json=$json_out"

hyperfine \
  --warmup "$warmup" \
  --min-runs "$min_runs" \
  --prepare "$prepare_cmd" \
  -n "NarrowDB" "$narrow_cmd" \
  -n "DuckDB" "$duckdb_cmd" \
  --export-markdown "$markdown_out" \
  --export-json "$json_out"

echo
echo "Saved Hyperfine markdown results to: $markdown_out"
echo "Saved Hyperfine JSON results to: $json_out"
