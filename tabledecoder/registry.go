package tabledecoder

import (
	"path/filepath"
	"strings"
)

// TableDumpConfig holds the decoding configuration for a single table dump file.
type TableDumpConfig struct {
	Parsers        ColumnParsers
	UseQuotedTSV   bool
	IsNodeSpecific bool
}

// Registry maps table dump basenames (e.g. "system.descriptor.txt") to their
// decoding configuration. This mirrors the clusterWideTableDumps and
// nodeSpecificTableDumps registries in CockroachDB's
// pkg/cli/zip_upload_table_dumps.go.
var Registry = buildRegistry()

// LookupTable returns the decoding configuration for a file path within a
// debug zip. It matches against the basename of the file. Returns nil if the
// file is not a known table dump.
func LookupTable(zipPath string) *TableDumpConfig {
	base := filepath.Base(zipPath)
	if cfg, ok := Registry[base]; ok {
		return &cfg
	}
	return nil
}

// IsTableDump returns true if the zip entry path corresponds to a known table
// dump that the decoder can process.
func IsTableDump(zipPath string) bool {
	return LookupTable(zipPath) != nil
}

// Fully qualified proto message names for table dump column decoding.
const (
	protoProgress         = "cockroach.sql.jobs.jobspb.Progress"
	protoProtoInfo        = "cockroach.multitenant.ProtoInfo"
	protoDescriptor       = "cockroach.sql.sqlbase.Descriptor"
	protoScheduleState    = "cockroach.jobs.jobspb.ScheduleState"
	protoScheduleDetails  = "cockroach.jobs.jobspb.ScheduleDetails"
	protoExecutionArgs    = "cockroach.jobs.jobspb.ExecutionArguments"
	protoTenantConsumption     = "cockroach.roachpb.TenantConsumption"
	protoTenantConsumptionRate = "cockroach.roachpb.TenantConsumptionRates"
	protoSpanConfig       = "cockroach.roachpb.SpanConfig"
)

func buildRegistry() map[string]TableDumpConfig {
	reg := make(map[string]TableDumpConfig)

	register := func(name string, isNodeSpecific bool, parsers ColumnParsers) {
		cfg := TableDumpConfig{
			Parsers:        parsers,
			IsNodeSpecific: isNodeSpecific,
		}
		if name == "crdb_internal.create_statements.txt" {
			cfg.UseQuotedTSV = true
		}
		reg[name] = cfg
	}

	clusterWide := func(name string, parsers ColumnParsers) {
		register(name, false, parsers)
	}
	nodeSpecific := func(name string, parsers ColumnParsers) {
		register(name, true, parsers)
	}

	// ---------------------------------------------------------------
	// Cluster-wide table dumps: plain text columns only
	// ---------------------------------------------------------------
	for _, name := range []string{
		"system.namespace.txt",
		"crdb_internal.kv_node_liveness.txt",
		"crdb_internal.cluster_database_privileges.txt",
		"system.rangelog.txt",
		"crdb_internal.table_indexes.txt",
		"crdb_internal.index_usage_statistics.txt",
		"crdb_internal.create_statements.txt",
		"system.job_info.txt",
		"crdb_internal.create_schema_statements.txt",
		"crdb_internal.default_privileges.txt",
		"system.role_members.txt",
		"crdb_internal.cluster_settings.txt",
		"system.role_id_seq.txt",
		"crdb_internal.cluster_sessions.txt",
		"system.migrations.txt",
		"crdb_internal.kv_store_status.txt",
		"system.locations.txt",
		"crdb_internal.cluster_transactions.txt",
		"crdb_internal.kv_node_status.txt",
		"crdb_internal.cluster_contention_events.txt",
		"crdb_internal.cluster_queries.txt",
		"crdb_internal.jobs.txt",
		"crdb_internal.regions.txt",
		"system.table_statistics.txt",
		"system.statement_diagnostics_requests.txt",
		"system.statement_diagnostics.txt",
		"system.sql_stats_cardinality.txt",
		"system.settings.txt",
		"system.reports_meta.txt",
		"system.replication_stats.txt",
		"system.replication_critical_localities.txt",
		"system.replication_constraint_stats.txt",
		"crdb_internal.cluster_distsql_flows.txt",
		"system.tenant_tasks.txt",
		"system.tenant_settings.txt",
		"system.task_payloads.txt",
		"system.role_options.txt",
		"system.protected_ts_meta.txt",
		"system.privileges.txt",
		"system.external_connections.txt",
		"system.database_role_settings.txt",
		"crdb_internal.super_regions.txt",
		"crdb_internal.schema_changes.txt",
		"crdb_internal.partitions.txt",
		"crdb_internal.kv_system_privileges.txt",
		"crdb_internal.kv_protected_ts_records.txt",
		"crdb_internal.invalid_objects.txt",
		"crdb_internal.create_type_statements.txt",
		"crdb_internal.create_procedure_statements.txt",
		"crdb_internal.create_function_statements.txt",
		"crdb_internal.create_trigger_statements.txt",
		"crdb_internal.logical_replication_spans.txt",
		"crdb_internal.cluster_replication_spans.txt",
		"system.protected_ts_records.txt",
	} {
		clusterWide(name, nil)
	}

	// ---------------------------------------------------------------
	// Cluster-wide table dumps: columns requiring decoding
	// ---------------------------------------------------------------

	clusterWide("crdb_internal.system_jobs.txt", ColumnParsers{
		"progress": MakeProtoColumnParser(protoProgress),
	})

	clusterWide("system.tenants.txt", ColumnParsers{
		"info": MakeProtoColumnParser(protoProtoInfo),
	})

	clusterWide("system.statement_statistics_limit_5000.txt", ColumnParsers{
		"fingerprint_id":             DecodeUUID,
		"transaction_fingerprint_id": DecodeUUID,
		"plan_hash":                  DecodeUUID,
	})

	clusterWide("system.sql_instances.txt", ColumnParsers{
		"session_id": DecodeUUID,
	})

	clusterWide("system.sqlliveness.txt", ColumnParsers{
		"session_id":  DecodeUUID,
		"crdb_region": DecodeRegion,
	})

	clusterWide("system.lease.txt", ColumnParsers{
		"session_id":  DecodeUUID,
		"crdb_region": DecodeRegion,
	})

	clusterWide("system.eventlog.txt", ColumnParsers{
		"uniqueID": DecodeUUID,
	})

	clusterWide("system.descriptor.txt", ColumnParsers{
		"descriptor": MakeProtoColumnParser(protoDescriptor),
	})

	clusterWide("system.scheduled_jobs.txt", ColumnParsers{
		"schedule_state":   MakeProtoColumnParser(protoScheduleState),
		"schedule_details": MakeProtoColumnParser(protoScheduleDetails),
		"execution_args":   MakeProtoColumnParser(protoExecutionArgs),
	})

	clusterWide("system.tenant_usage.txt", ColumnParsers{
		"total_consumption": MakeProtoColumnParser(protoTenantConsumption),
		"current_rates":     MakeProtoColumnParser(protoTenantConsumptionRate),
		"next_rates":        MakeProtoColumnParser(protoTenantConsumptionRate),
	})

	clusterWide("system.span_configurations.txt", ColumnParsers{
		"start_key": DecodeKey,
		"end_key":   DecodeKey,
		"config":    MakeProtoColumnParser(protoSpanConfig),
	})

	clusterWide("crdb_internal.cluster_locks.txt", ColumnParsers{
		"lock_key": nil, // nil = skip column
	})

	clusterWide("crdb_internal.transaction_contention_events.fallback.txt", ColumnParsers{
		"blocking_txn_fingerprint_id": DecodeUUID,
		"waiting_stmt_fingerprint_id": DecodeUUID,
		"waiting_txn_fingerprint_id":  DecodeUUID,
	})

	clusterWide("crdb_internal.transaction_contention_events.txt", ColumnParsers{
		"blocking_txn_fingerprint_id": DecodeUUID,
		"waiting_stmt_fingerprint_id": DecodeUUID,
		"waiting_txn_fingerprint_id":  DecodeUUID,
	})

	// ---------------------------------------------------------------
	// Node-specific table dumps: plain text columns only
	// ---------------------------------------------------------------
	for _, name := range []string{
		"crdb_internal.node_metrics.txt",
		"crdb_internal.node_txn_stats.txt",
		"crdb_internal.node_contention_events.txt",
		"crdb_internal.gossip_liveness.txt",
		"crdb_internal.gossip_nodes.txt",
		"crdb_internal.node_runtime_info.txt",
		"crdb_internal.node_transaction_statistics.txt",
		"crdb_internal.node_tenant_capabilities_cache.txt",
		"crdb_internal.node_sessions.txt",
		"crdb_internal.node_statement_statistics.txt",
		"crdb_internal.leases.txt",
		"crdb_internal.node_build_info.txt",
		"crdb_internal.node_memory_monitors.txt",
		"crdb_internal.active_range_feeds.txt",
		"crdb_internal.gossip_alerts.txt",
		"crdb_internal.node_transactions.txt",
		"crdb_internal.feature_usage.txt",
		"crdb_internal.node_queries.txt",
		"crdb_internal.cluster_replication_node_stream_checkpoints.txt",
		"crdb_internal.logical_replication_node_processors.txt",
		"crdb_internal.cluster_replication_node_streams.txt",
		"crdb_internal.node_inflight_trace_spans.txt",
		"crdb_internal.node_distsql_flows.txt",
		"crdb_internal.cluster_replication_node_stream_spans.txt",
	} {
		nodeSpecific(name, nil)
	}

	// ---------------------------------------------------------------
	// Node-specific table dumps: columns requiring decoding
	// ---------------------------------------------------------------

	nodeSpecific("crdb_internal.node_txn_execution_insights.txt", ColumnParsers{
		"txn_fingerprint_id": DecodeUUID,
	})

	nodeSpecific("crdb_internal.node_execution_insights.txt", ColumnParsers{
		"txn_fingerprint_id":  DecodeUUID,
		"stmt_fingerprint_id": DecodeUUID,
	})

	nodeSpecific("crdb_internal.kv_session_based_leases.txt", ColumnParsers{
		"session_id":  DecodeUUID,
		"crdb_region": DecodeRegion,
	})

	return reg
}

// HasDecoders returns true if the table has any columns that need decoding
// (as opposed to a plain-text-only table).
func (c *TableDumpConfig) HasDecoders() bool {
	return len(c.Parsers) > 0
}

// TableName extracts the table name from a table dump filename by stripping
// the .txt extension.
func TableName(filename string) string {
	return strings.TrimSuffix(filepath.Base(filename), ".txt")
}
