package tsdecoder

import "time"

// KeyValue is a gob-compatible type matching cockroach's roachpb.KeyValue
// field names exactly. Tsdump streams encode time-series data as a sequence
// of gob-encoded KeyValue pairs.
type KeyValue struct {
	Key   []byte
	Value Value
}

// Value is a gob-compatible type matching cockroach's roachpb.Value.
type Value struct {
	RawBytes  []byte
	Timestamp Timestamp
}

// Timestamp is a gob-compatible type matching cockroach's hlc.Timestamp.
type Timestamp struct {
	WallTime int64
	Logical  int32
}

// Metadata may appear as the first gob entry in a tsdump stream. It contains
// information about the cluster that produced the dump.
type Metadata struct {
	Version        string
	StoreToNodeMap map[string]string
	CreatedAt      time.Time
}

// Row represents a single decoded time-series sample, ready for Parquet output.
type Row struct {
	Name      string  `parquet:"name,zstd"`
	Source    string  `parquet:"source,zstd"`
	Timestamp int64   `parquet:"timestamp,delta"`
	Value     float64 `parquet:"value"`
}
