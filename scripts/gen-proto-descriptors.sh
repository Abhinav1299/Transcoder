#!/usr/bin/env bash
#
# Compiles CockroachDB proto files into a FileDescriptorSet binary that can be
# loaded at runtime by dynamicpb for proto column decoding.
#
# Prerequisites:
#   - protoc (Protocol Buffers compiler)
#   - A local CockroachDB checkout (set CRDB_DIR or defaults to sibling dir)
#
# Usage:
#   ./scripts/gen-proto-descriptors.sh
#
# Output:
#   tabledecoder/descriptors.binpb

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# CockroachDB repo root (default: sibling directory).
CRDB_DIR="${CRDB_DIR:-$(cd "$REPO_ROOT/../../cockroachdb/cockroach" && pwd)}"

if [ ! -d "$CRDB_DIR/pkg" ]; then
  echo "Error: CockroachDB repo not found at $CRDB_DIR"
  echo "Set CRDB_DIR to point to your cockroachdb/cockroach checkout."
  exit 1
fi

# Find external proto dependencies in the Go module cache.
GOMODCACHE="$(go env GOMODCACHE)"

# gogoproto/gogo.proto
GOGOPROTO_DIR=$(find "$GOMODCACHE/github.com/cockroachdb/gogoproto@"* -maxdepth 0 -type d 2>/dev/null | sort -V | tail -1)
if [ -z "$GOGOPROTO_DIR" ] || [ ! -f "$GOGOPROTO_DIR/gogoproto/gogo.proto" ]; then
  echo "Error: gogoproto not found in module cache. Run 'go mod download' in the CRDB repo first."
  exit 1
fi

# errorspb/errors.proto
ERRORS_DIR=$(find "$GOMODCACHE/github.com/cockroachdb/errors@"* -maxdepth 0 -type d 2>/dev/null | sort -V | tail -1)
if [ -z "$ERRORS_DIR" ] || [ ! -f "$ERRORS_DIR/errorspb/errors.proto" ]; then
  echo "Error: cockroachdb/errors not found in module cache."
  exit 1
fi

OUTPUT="$REPO_ROOT/tabledecoder/descriptors.binpb"

# The primary proto files defining the message types we need to decode.
PRIMARY_PROTOS=(
  "jobs/jobspb/jobs.proto"
  "jobs/jobspb/schedule.proto"
  "multitenant/mtinfopb/info.proto"
  "sql/catalog/descpb/structured.proto"
  "kv/kvpb/api.proto"
  "roachpb/span_config.proto"
)

echo "CRDB_DIR:     $CRDB_DIR"
echo "GOGOPROTO:    $GOGOPROTO_DIR"
echo "ERRORS:       $ERRORS_DIR"
echo "OUTPUT:       $OUTPUT"
echo ""

# Verify all primary protos exist.
for proto in "${PRIMARY_PROTOS[@]}"; do
  if [ ! -f "$CRDB_DIR/pkg/$proto" ]; then
    echo "Error: $CRDB_DIR/pkg/$proto not found"
    exit 1
  fi
done

echo "Compiling FileDescriptorSet..."

protoc \
  --proto_path="$CRDB_DIR/pkg" \
  --proto_path="$GOGOPROTO_DIR" \
  --proto_path="$ERRORS_DIR" \
  --descriptor_set_out="$OUTPUT" \
  --include_imports \
  "${PRIMARY_PROTOS[@]}"

echo "Done: $OUTPUT ($(wc -c < "$OUTPUT" | tr -d ' ') bytes)"
