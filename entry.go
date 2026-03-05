package transcoder

// LogEntry mirrors the cockroach.util.log.Entry protobuf message defined in
// log.proto (https://github.com/cockroachdb/cockroach/blob/master/pkg/util/log/logpb/log.proto).
// The parquet struct tags drive automatic schema generation by parquet-go's
// GenericWriter.
//
// Versioning strategy: this struct uses a "superset" approach — it contains the
// union of all Entry fields across all supported CockroachDB versions. When
// parsing logs from older CRDB versions, fields that did not exist in that
// version will have their Go zero value (e.g., "" for strings, 0 for integers).
// This works because:
//
//   - Protobuf schemas are additive-only: fields are never removed, and field
//     numbers are stable across versions.
//   - Changes to Entry are very infrequent (~1 field per major release cycle).
//   - Zero values are semantically correct for absent fields.
//
// When CockroachDB adds a new field to Entry, add it here with the matching
// parquet tag and release a new transcoder version. The CI script
// scripts/check-proto-drift.sh detects when upstream fields diverge from this
// struct.
type LogEntry struct {
	Severity        int32  `parquet:"severity"`
	Time            int64  `parquet:"time"`
	Goroutine       int64  `parquet:"goroutine"`
	File            string `parquet:"file"`
	Line            int64  `parquet:"line"`
	Message         string `parquet:"message"`
	Tags            string `parquet:"tags"`
	Counter         uint64 `parquet:"counter"`
	Redactable      bool   `parquet:"redactable"`
	Channel         int32  `parquet:"channel"`
	StructuredEnd   uint32 `parquet:"structured_end"`
	StructuredStart uint32 `parquet:"structured_start"`
	StackTraceStart uint32 `parquet:"stack_trace_start"`
	TenantID        string `parquet:"tenant_id"`
	TenantName      string `parquet:"tenant_name"`
}

// Severity constants matching the cockroach.util.log.Severity proto enum.
const (
	SeverityUnknown int32 = 0
	SeverityInfo    int32 = 1
	SeverityWarning int32 = 2
	SeverityError   int32 = 3
	SeverityFatal   int32 = 4
)

// Channel constants matching the cockroach.util.log.Channel proto enum.
const (
	ChannelDev             int32 = 0
	ChannelOps             int32 = 1
	ChannelHealth          int32 = 2
	ChannelStorage         int32 = 3
	ChannelSessions        int32 = 4
	ChannelSQLSchema       int32 = 5
	ChannelUserAdmin       int32 = 6
	ChannelPrivileges      int32 = 7
	ChannelSensitiveAccess int32 = 8
	ChannelSQLExec         int32 = 9
	ChannelSQLPerf         int32 = 10
	ChannelSQLInternalPerf int32 = 11
	ChannelTelemetry       int32 = 12
	ChannelKVDistribution  int32 = 13
	ChannelChangefeed      int32 = 14
	ChannelKVExec          int32 = 15
)
