package tsdecoder

import (
	"bytes"
	"encoding/gob"
	"io"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"google.golang.org/protobuf/proto"
)

// buildTestKey constructs a valid tsdump key for the given metric name,
// resolution, timeslot, and source.
func buildTestKey(metricName string, resolution, timeslot int64, source string) []byte {
	var key []byte
	key = append(key, timeseriesPrefix...)
	key = append(key, encodeBytes([]byte(metricName))...)
	key = append(key, encodeVarint(resolution)...)
	key = append(key, encodeVarint(timeslot)...)
	key = append(key, []byte(source)...)
	return key
}

// buildTestValue constructs a valid tsdump value with a protobuf
// InternalTimeSeriesData payload using the columnar format.
func buildTestValue(startNanos, sampleDurationNanos int64, offsets []int32, values []float64) []byte {
	idata := &InternalTimeSeriesData{
		StartTimestampNanos: startNanos,
		SampleDurationNanos: sampleDurationNanos,
		Offset:              offsets,
		Last:                values,
	}
	protoBytes, err := proto.Marshal(idata)
	if err != nil {
		panic(err)
	}
	// Prepend the 5-byte header (4-byte checksum + 1-byte tag).
	raw := make([]byte, headerSize+len(protoBytes))
	copy(raw[headerSize:], protoBytes)
	return raw
}

// buildTestValueRowFormat constructs a tsdump value using the legacy
// row-based sample format.
func buildTestValueRowFormat(startNanos, sampleDurationNanos int64, samples []*InternalTimeSeriesSample) []byte {
	idata := &InternalTimeSeriesData{
		StartTimestampNanos: startNanos,
		SampleDurationNanos: sampleDurationNanos,
		Samples:             samples,
	}
	protoBytes, err := proto.Marshal(idata)
	if err != nil {
		panic(err)
	}
	raw := make([]byte, headerSize+len(protoBytes))
	copy(raw[headerSize:], protoBytes)
	return raw
}

// encodeGobStream encodes a sequence of KeyValue pairs into a gob stream,
// optionally preceded by a Metadata header.
func encodeGobStream(md *Metadata, kvs []KeyValue) *bytes.Buffer {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if md != nil {
		if err := enc.Encode(md); err != nil {
			panic(err)
		}
	}
	for _, kv := range kvs {
		if err := enc.Encode(kv); err != nil {
			panic(err)
		}
	}
	return &buf
}

func TestConvert(t *testing.T) {
	const (
		startNanos     = int64(1_000_000_000_000)
		sampleDuration = int64(10_000_000_000) // 10s in nanos
	)

	tests := []struct {
		name     string
		md       *Metadata
		kvs      []KeyValue
		wantRows int
		wantMeta bool // whether onMeta should be called
		checkMD  func(t *testing.T, md Metadata)
		check    func(t *testing.T, rows []Row)
	}{
		{
			name: "columnar format",
			kvs: []KeyValue{
				{
					Key: buildTestKey("cr.node.sql_conns", 1, 100, "1"),
					Value: Value{
						RawBytes: buildTestValue(startNanos, sampleDuration,
							[]int32{0, 1, 2},
							[]float64{10.0, 20.0, 30.0}),
					},
				},
			},
			wantRows: 3,
			check: func(t *testing.T, rows []Row) {
				for i, row := range rows {
					if row.Name != "node_sql_conns" {
						t.Errorf("row[%d].Name = %q, want %q", i, row.Name, "node_sql_conns")
					}
					if row.Source != "1" {
						t.Errorf("row[%d].Source = %q, want %q", i, row.Source, "1")
					}
				}
				wantTS := []int64{
					startNanos + 0*sampleDuration,
					startNanos + 1*sampleDuration,
					startNanos + 2*sampleDuration,
				}
				wantVals := []float64{10.0, 20.0, 30.0}
				for i, row := range rows {
					if row.Timestamp != wantTS[i] {
						t.Errorf("row[%d].Timestamp = %d, want %d", i, row.Timestamp, wantTS[i])
					}
					if row.Value != wantVals[i] {
						t.Errorf("row[%d].Value = %f, want %f", i, row.Value, wantVals[i])
					}
				}
			},
		},
		{
			name: "legacy row format",
			kvs: []KeyValue{
				{
					Key: buildTestKey("cr.node.exec_count", 1, 200, "2"),
					Value: Value{
						RawBytes: buildTestValueRowFormat(
							2_000_000_000_000, sampleDuration,
							[]*InternalTimeSeriesSample{
								{Offset: 0, Sum: 100.0, Count: 1},
								{Offset: 1, Sum: 200.0, Count: 1},
							}),
					},
				},
			},
			wantRows: 2,
			check: func(t *testing.T, rows []Row) {
				if rows[0].Name != "node_exec_count" {
					t.Errorf("Name = %q, want %q", rows[0].Name, "node_exec_count")
				}
				if rows[0].Value != 100.0 || rows[1].Value != 200.0 {
					t.Errorf("Values = [%f, %f], want [100, 200]", rows[0].Value, rows[1].Value)
				}
			},
		},
		{
			name: "with metadata header",
			md: &Metadata{
				Version:        "v23.1.0",
				StoreToNodeMap: map[string]string{"1": "n1", "2": "n2"},
				CreatedAt:      time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC),
			},
			kvs: []KeyValue{
				{
					Key: buildTestKey("cr.node.live_nodes", 1, 50, "1"),
					Value: Value{
						RawBytes: buildTestValue(startNanos, sampleDuration,
							[]int32{0},
							[]float64{3.0}),
					},
				},
			},
			wantRows: 1,
			wantMeta: true,
			checkMD: func(t *testing.T, md Metadata) {
				if md.Version != "v23.1.0" {
					t.Errorf("metadata version = %q, want %q", md.Version, "v23.1.0")
				}
				if len(md.StoreToNodeMap) != 2 {
					t.Errorf("StoreToNodeMap has %d entries, want 2", len(md.StoreToNodeMap))
				}
			},
			check: func(t *testing.T, rows []Row) {
				if rows[0].Name != "node_live_nodes" {
					t.Errorf("Name = %q, want %q", rows[0].Name, "node_live_nodes")
				}
			},
		},
		{
			name: "multiple metrics with row group flush",
			kvs: []KeyValue{
				{
					Key: buildTestKey("cr.node.metric_a", 1, 100, "1"),
					Value: Value{
						RawBytes: buildTestValue(startNanos, sampleDuration,
							[]int32{0, 1},
							[]float64{1.0, 2.0}),
					},
				},
				{
					Key: buildTestKey("cr.node.metric_a", 1, 100, "1"),
					Value: Value{
						RawBytes: buildTestValue(startNanos, sampleDuration,
							[]int32{2},
							[]float64{3.0}),
					},
				},
				// Different metric name triggers a flush of the previous batch.
				{
					Key: buildTestKey("cr.node.metric_b", 1, 100, "2"),
					Value: Value{
						RawBytes: buildTestValue(startNanos, sampleDuration,
							[]int32{0},
							[]float64{99.0}),
					},
				},
			},
			wantRows: 4,
			check: func(t *testing.T, rows []Row) {
				for i := 0; i < 3; i++ {
					if rows[i].Name != "node_metric_a" {
						t.Errorf("row[%d].Name = %q, want %q", i, rows[i].Name, "node_metric_a")
					}
				}
				if rows[3].Name != "node_metric_b" {
					t.Errorf("row[3].Name = %q, want %q", rows[3].Name, "node_metric_b")
				}
				if rows[3].Value != 99.0 {
					t.Errorf("row[3].Value = %f, want 99.0", rows[3].Value)
				}
			},
		},
		{
			name:     "empty stream",
			kvs:      nil, // no data at all
			wantRows: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := encodeGobStream(tt.md, tt.kvs)

			var gotMD Metadata
			var metaCalled bool
			var onMeta func(Metadata)
			if tt.wantMeta {
				onMeta = func(m Metadata) {
					gotMD = m
					metaCalled = true
				}
			}

			var out bytes.Buffer
			stats, err := Convert(stream, &out, onMeta)
			if err != nil {
				t.Fatalf("Convert failed: %v", err)
			}
			if stats.RowsWritten != tt.wantRows {
				t.Fatalf("row count = %d, want %d", stats.RowsWritten, tt.wantRows)
			}

			if tt.wantMeta {
				if !metaCalled {
					t.Fatal("onMeta callback was not invoked")
				}
				if tt.checkMD != nil {
					tt.checkMD(t, gotMD)
				}
			}

			if tt.wantRows == 0 {
				return
			}

			rows := readParquetRows(t, out.Bytes())
			if len(rows) != tt.wantRows {
				t.Fatalf("parquet row count = %d, want %d", len(rows), tt.wantRows)
			}
			if tt.check != nil {
				tt.check(t, rows)
			}
		})
	}
}

// readParquetRows reads all Row values from a Parquet byte slice.
func readParquetRows(t *testing.T, data []byte) []Row {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("opening parquet: %v", err)
	}
	reader := parquet.NewGenericReader[Row](f)
	defer reader.Close()

	rows := make([]Row, reader.NumRows())
	n, err := reader.Read(rows)
	if err != nil && err != io.EOF {
		t.Fatalf("reading parquet rows: %v", err)
	}
	return rows[:n]
}
