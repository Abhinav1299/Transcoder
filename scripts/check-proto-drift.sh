#!/usr/bin/env bash
#
# check-proto-drift.sh — Detects when the upstream CockroachDB log.proto Entry
# message has fields that are not yet reflected in entry.go's LogEntry struct.
#
# This script is intended to run in CI (e.g., weekly cron or pre-release gate).
# It fetches the latest log.proto from the CockroachDB repo, extracts the Entry
# message fields, and compares them against the LogEntry struct in entry.go.
#
# Exit codes:
#   0 — LogEntry is in sync with upstream (or is a superset).
#   1 — Upstream has new fields not yet in LogEntry.
#   2 — Script error (network, parse failure, etc).
#
# Usage:
#   ./scripts/check-proto-drift.sh [CRDB_REF]
#
# CRDB_REF defaults to "master" but can be any git ref (e.g., "v24.1.0").

set -euo pipefail

CRDB_REF="${1:-master}"
PROTO_URL="https://raw.githubusercontent.com/cockroachdb/cockroach/${CRDB_REF}/pkg/util/log/logpb/log.proto"
ENTRY_GO="$(cd "$(dirname "$0")/.." && pwd)/entry.go"

if [[ ! -f "$ENTRY_GO" ]]; then
    echo "ERROR: entry.go not found at $ENTRY_GO" >&2
    exit 2
fi

echo "Fetching log.proto from cockroachdb/cockroach@${CRDB_REF}..."
PROTO_CONTENT=$(curl -fsSL "$PROTO_URL") || {
    echo "ERROR: Failed to fetch $PROTO_URL" >&2
    exit 2
}

# Extract field names from the Entry message in log.proto.
# Matches lines like:  Severity severity = 1;
#                       string tenant_id = 14 [(gogoproto.customname) = "TenantID"];
# We capture the second token (the field name) from each line.
PROTO_FIELDS=$(echo "$PROTO_CONTENT" \
    | sed -n '/^message Entry {/,/^}/p' \
    | grep -E '^\s+\w+\s+\w+\s*=' \
    | grep -v '^\s*//' \
    | awk '{print $2}' \
    | sort)

if [[ -z "$PROTO_FIELDS" ]]; then
    echo "ERROR: Could not parse any fields from Entry message in log.proto" >&2
    exit 2
fi

# Extract field names from LogEntry struct in entry.go.
# The parquet tag value is the canonical field name (matches proto snake_case).
ENTRY_FIELDS=$(grep 'parquet:"' "$ENTRY_GO" \
    | sed -n 's/.*parquet:"\([a-z_]*\)".*/\1/p' \
    | sort)

if [[ -z "$ENTRY_FIELDS" ]]; then
    echo "ERROR: Could not parse any parquet tags from $ENTRY_GO" >&2
    exit 2
fi

echo ""
echo "Proto Entry fields (${CRDB_REF}):"
echo "$PROTO_FIELDS" | sed 's/^/  /'
echo ""
echo "LogEntry parquet fields (entry.go):"
echo "$ENTRY_FIELDS" | sed 's/^/  /'
echo ""

# Find fields in proto that are missing from entry.go.
MISSING=$(comm -23 <(echo "$PROTO_FIELDS") <(echo "$ENTRY_FIELDS"))

if [[ -z "$MISSING" ]]; then
    echo "OK: LogEntry covers all upstream Entry fields."
    # Also report any extra fields in LogEntry not in proto (informational).
    EXTRA=$(comm -13 <(echo "$PROTO_FIELDS") <(echo "$ENTRY_FIELDS"))
    if [[ -n "$EXTRA" ]]; then
        echo ""
        echo "Note: LogEntry has extra fields not in upstream proto (this is fine):"
        echo "$EXTRA" | sed 's/^/  /'
    fi
    exit 0
else
    echo "DRIFT DETECTED: The following upstream proto fields are missing from LogEntry:"
    echo "$MISSING" | sed 's/^/  /'
    echo ""
    echo "Action: Add these fields to the LogEntry struct in entry.go with appropriate"
    echo "parquet tags and Go types. See the superset versioning strategy comment in entry.go."
    exit 1
fi
