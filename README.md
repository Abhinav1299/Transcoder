# transcoder

`transcoder` converts [CockroachDB](https://github.com/cockroachdb/cockroach) debug-bundle log files from plain-text into [Apache Parquet](https://parquet.apache.org/), making them queryable with tools like DuckDB, Spark, and pandas.

## Features

- Parses all CockroachDB log formats: `crdb-v2`, `crdb-v1`, `json`, `json-compact` (and their `-tty`/`-fluent` variants), with automatic format detection from file headers.
- Reassembles multi-line entries (continuation markers `+`, `|`, `!`) and attaches non-matching banner lines to their preceding entry.
- Sanitises invalid UTF-8 bytes so the output is always valid Parquet.
- Streams log files line-by-line — memory usage stays bounded regardless of file size.
- Copies non-log files (goroutine dumps, node status, etc.) through to the output ZIP unchanged.


## Prerequisites

- **Go 1.22+** (developed with Go 1.25)

Optional (for output verification):
- [DuckDB](https://duckdb.org/) CLI

## Installation

```bash
go install github.com/Abhinav1299/Transcoder/cmd/transcoder@latest
```

Or build from source:

```bash
git clone https://github.com/Abhinav1299/Transcoder.git
cd Transcoder
go build -o transcoder ./cmd/transcoder
```

## Usage

```bash
./transcoder -input <debug-bundle.zip> [-output parquet.zip]
```

| Flag | Default | Description |
|------|---------|-------------|
| `-input` | *(required)* | Path to a CockroachDB debug-bundle ZIP |
| `-output` | `parquet.zip` | Path for the output ZIP |

The output ZIP mirrors the input directory structure. Every `*.log` file is replaced with a corresponding `.parquet` file; all other files are preserved as-is.

### Example

```bash
./transcoder -input debug-bundle-20260128.zip -output parquet.zip
# Done. 21 files processed, 154471 total entries, 0 malformed lines.
```

## Querying the output

Once converted, use any Parquet-aware tool. For example, with DuckDB:

```sql
-- Load all parquet files from the output ZIP (extract first)
SELECT severity, count(*) AS cnt
FROM read_parquet('/path/to/parquet/debug/nodes/1/logs/*.parquet', union_by_name=true)
GROUP BY severity ORDER BY severity;

-- Find errors in a specific time window
SELECT time, file, line, message
FROM read_parquet('/path/to/parquet/debug/nodes/1/logs/cockroach.*.parquet')
WHERE severity >= 3
  AND time BETWEEN 1706400000000000000 AND 1706500000000000000
ORDER BY time;
```

## Streaming API

In addition to ZIP-to-ZIP conversion, the package exposes a `ConvertStream` method for converting a text log stream directly to Parquet. This is designed for use by an upload server in a synchronous flow where CockroachDB streams logs over the network.

```go
tc := &transcoder.Transcoder{}
stats, err := tc.ConvertStream(ctx, logReader, parquetWriter, "crdb-v2")
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `ctx` | `context.Context` | Controls cancellation |
| `r` | `io.Reader` | Incoming text log stream |
| `w` | `io.Writer` | Parquet output destination |
| `format` | `string` | Log format (`"crdb-v2"`, `"crdb-v1"`, `"json"`, `"json-compact"`); empty string triggers auto-detection |

Returns `*Stats` with `TotalEntries`, `MalformedLines`, and `FilesProcessed` (always 1 for a single stream).

### Demo upload server

A working example lives in `cmd/demo-upload-server/`. It runs an HTTP server that accepts POST requests with text logs in the body and converts them to Parquet on the fly.

```bash
# Start the server
go run ./cmd/demo-upload-server/

# Send logs (one-shot)
curl -X POST http://localhost:8080/upload \
  -H "X-Log-Format: crdb-v2" \
  --data-binary @cockroach.log

# Stream logs from a pipe
cat cockroach.log | curl -X POST http://localhost:8080/upload \
  -H "X-Log-Format: crdb-v2" \
  -H "Transfer-Encoding: chunked" \
  -T -
```

The server writes Parquet files to a `parquet-output/` directory and returns a JSON response with conversion stats:

```json
{"parquet_file":"parquet-output/upload-1772638013-1.parquet","total_entries":4,"malformed_lines":0}
```

## Testing

```bash
make test
```

Run benchmarks:

```bash
make bench
```

## Verifying output with DuckDB

A `make verify` target automates end-to-end verification: it builds the binary, converts the input ZIP, extracts both text and Parquet files, and runs `scripts/verify.sql` through DuckDB. The script compares original text logs against converted Parquet files across 9 checks (per-file counts, totals, file count, schema, data quality, severity/channel distributions, timestamp range, and redactable flag).

```bash
make verify INPUT_ZIP=debug-bundle.zip
```

## Schema

The Parquet schema maps 1:1 to `cockroach.util.log.Entry` defined in `log.proto`:

| Column | Type | Description |
|--------|------|-------------|
| `severity` | INT32 | 0=UNKNOWN, 1=INFO, 2=WARNING, 3=ERROR, 4=FATAL |
| `time` | INT64 | Timestamp in nanoseconds since Unix epoch |
| `goroutine` | INT64 | Goroutine ID |
| `file` | VARCHAR | Source file path |
| `line` | INT64 | Source line number |
| `message` | VARCHAR | Log message (may be multi-line) |
| `tags` | VARCHAR | Comma-separated tags (tenant tags stripped) |
| `counter` | UINT64 | Monotonic entry counter per log file |
| `redactable` | BOOLEAN | Whether the message contains redaction markers |
| `channel` | INT32 | Log channel (0=DEV, 1=OPS, 2=HEALTH, …) |
| `structured_end` | UINT32 | Byte offset of structured JSON end in message |
| `structured_start` | UINT32 | Byte offset of structured JSON start in message |
| `stack_trace_start` | UINT32 | Byte offset of stack trace start in message |
| `tenant_id` | VARCHAR | CockroachDB tenant ID (defaults to "1") |
| `tenant_name` | VARCHAR | CockroachDB tenant name (e.g. "system") |

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-change`)
3. Commit your changes
4. Run `go test ./...` and ensure all tests pass
5. Open a pull request

## License

See [LICENSE](LICENSE) for details.
