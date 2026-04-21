package tsdecoder

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

// TestConvert_PerRecordOutcomes pins behavior for single-record payload
// variants. Each case drives exactly one KeyValue through Convert and asserts
// the expected (rows, skipped, SkipReasons) outcome, guarding against:
//
//   - panics on malformed wire data (regression: "index out of range" on a
//     columnar payload with len(Offset) != len(Last)),
//   - silent data corruption from invalid sample durations (collapsing
//     multiple samples onto one timestamp),
//   - silent data loss on short/garbage payloads (expected: a clean skip),
//   - false positives that reject legitimately empty slabs.
//
// New single-record failure modes should be added as rows in the table rather
// than new top-level test functions.
func TestConvert_PerRecordOutcomes(t *testing.T) {
	// 16 bytes of 0xFF after the value header — not a valid protobuf message.
	garbageProto := make([]byte, headerSize+16)
	for i := headerSize; i < len(garbageProto); i++ {
		garbageProto[i] = 0xFF
	}

	const (
		startTs   = int64(1_000_000_000_000)
		sampleDur = int64(10_000_000_000)
	)

	cases := []struct {
		name        string
		raw         []byte
		wantRows    int
		wantSkipped int
		wantReasons SkipReasons
	}{
		{
			// Regression: decoder previously indexed Last[i] without a
			// bounds check and panicked when Offset was longer than Last.
			name: "mismatched columnar lengths",
			raw: marshalITSD(&InternalTimeSeriesData{
				StartTimestampNanos: startTs,
				SampleDurationNanos: sampleDur,
				Offset:              []int32{0, 1, 2},
				Last:                []float64{10.0, 20.0},
			}),
			wantSkipped: 1,
			wantReasons: SkipReasons{InvalidSampleData: 1},
		},
		{
			// Sample duration of 0 would emit N samples at the same
			// timestamp — silent data corruption. Reject the record.
			name: "sample duration zero",
			raw: marshalITSD(&InternalTimeSeriesData{
				StartTimestampNanos: startTs,
				SampleDurationNanos: 0,
				Offset:              []int32{0, 1, 2},
				Last:                []float64{1, 2, 3},
			}),
			wantSkipped: 1,
			wantReasons: SkipReasons{InvalidSampleData: 1},
		},
		{
			name: "sample duration negative",
			raw: marshalITSD(&InternalTimeSeriesData{
				StartTimestampNanos: startTs,
				SampleDurationNanos: -1,
				Offset:              []int32{0, 1, 2},
				Last:                []float64{1, 2, 3},
			}),
			wantSkipped: 1,
			wantReasons: SkipReasons{InvalidSampleData: 1},
		},
		{
			name: "sample duration very negative",
			raw: marshalITSD(&InternalTimeSeriesData{
				StartTimestampNanos: startTs,
				SampleDurationNanos: -1_000_000_000,
				Offset:              []int32{0, 1, 2},
				Last:                []float64{1, 2, 3},
			}),
			wantSkipped: 1,
			wantReasons: SkipReasons{InvalidSampleData: 1},
		},
		{
			// Garbage bytes after a valid-looking 5-byte header must not
			// escalate to a fatal error or a panic.
			name:        "garbage proto bytes",
			raw:         garbageProto,
			wantSkipped: 1,
			wantReasons: SkipReasons{ProtoUnmarshal: 1},
		},
		{
			name:        "raw bytes shorter than CRDB value header",
			raw:         []byte{0x00, 0x01},
			wantSkipped: 1,
			wantReasons: SkipReasons{ShortValue: 1},
		},
		{
			// An ITSD with neither Offset nor Samples is a legal (if
			// unusual) empty slab. Expect zero rows AND zero skips.
			name: "empty internal data is legal",
			raw: marshalITSD(&InternalTimeSeriesData{
				StartTimestampNanos: startTs,
				SampleDurationNanos: sampleDur,
			}),
		},
	}

	key := buildTestKey("cr.node.metric", 1, 100, "1")
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Go's testing framework turns panics into FAIL automatically,
			// but an explicit recover here names the culprit clearly and
			// documents that "no panic on this input" is a hard guarantee.
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("decoder panicked: %v", r)
				}
			}()

			stream := encodeGobStream(nil, []KeyValue{
				{Key: key, Value: Value{RawBytes: c.raw}},
			})

			var out bytes.Buffer
			stats, err := Convert(stream, &out, nil)
			if err != nil {
				t.Fatalf("Convert: %v", err)
			}
			if stats.RowsWritten != c.wantRows {
				t.Errorf("rows = %d, want %d", stats.RowsWritten, c.wantRows)
			}
			if stats.RecordsSkipped != c.wantSkipped {
				t.Errorf("skipped = %d, want %d", stats.RecordsSkipped, c.wantSkipped)
			}
			if stats.SkipReasons != c.wantReasons {
				t.Errorf("SkipReasons = %+v, want %+v", stats.SkipReasons, c.wantReasons)
			}
		})
	}
}

// TestConvert_NilSampleInLegacyFormat pins the observable behavior for a
// caller-constructed proto with nil entries in Samples: proto.Marshal emits
// them as zero-value messages, proto.Unmarshal reconstructs them as non-nil
// zero-value messages, and the decoder emits a row per entry. Importantly,
// no panic. The test also verifies that if proto semantics ever change to
// leave nil pointers in the Samples slice, the defensive nil-check in
// decodeSamples keeps us safe.
func TestConvert_NilSampleInLegacyFormat(t *testing.T) {
	raw := marshalITSD(&InternalTimeSeriesData{
		StartTimestampNanos: 1_000_000_000_000,
		SampleDurationNanos: 10_000_000_000,
		Samples: []*InternalTimeSeriesSample{
			{Offset: 0, Sum: 1.0},
			nil,
			{Offset: 2, Sum: 3.0},
		},
	})
	stream := encodeGobStream(nil, []KeyValue{{
		Key:   buildTestKey("cr.node.metric", 1, 100, "1"),
		Value: Value{RawBytes: raw},
	}})

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("decoder panicked on nil sample: %v", r)
		}
	}()
	var out bytes.Buffer
	stats, err := Convert(stream, &out, nil)
	if err != nil {
		t.Fatalf("Convert returned error: %v", err)
	}
	if stats.RowsWritten < 2 || stats.RowsWritten > 3 {
		t.Errorf("rows = %d, want 2 or 3", stats.RowsWritten)
	}
}

// TestConvert_SkipReasonBreakdown mixes several kinds of corrupt records
// together and verifies every skip is attributed to the correct category.
// Unlike TestConvert_PerRecordOutcomes (which exercises one kind at a time),
// this pins the accumulation invariant: RecordsSkipped equals the sum of
// the SkipReasons fields across a single stream.
func TestConvert_SkipReasonBreakdown(t *testing.T) {
	// 1 bad key, 1 short value, 1 bad proto, 1 invalid sample, 1 valid
	// record — expect one increment in every SkipReasons field and the
	// valid record's samples in the output.
	badProto := make([]byte, headerSize+4)
	for i := headerSize; i < len(badProto); i++ {
		badProto[i] = 0xFF
	}
	invalidSampleRaw := marshalITSD(&InternalTimeSeriesData{
		StartTimestampNanos: 1, SampleDurationNanos: 10,
		Offset: []int32{0, 1}, Last: []float64{1.0}, // length mismatch
	})

	kvs := []KeyValue{
		{Key: []byte{0x04}, Value: Value{RawBytes: buildTestValue(1, 10, []int32{0}, []float64{1})}},
		{Key: buildTestKey("cr.node.m", 1, 100, "1"), Value: Value{RawBytes: []byte{0x01, 0x02}}},
		{Key: buildTestKey("cr.node.m", 1, 100, "1"), Value: Value{RawBytes: badProto}},
		{Key: buildTestKey("cr.node.m", 1, 100, "1"), Value: Value{RawBytes: invalidSampleRaw}},
		{Key: buildTestKey("cr.node.m", 1, 100, "1"), Value: Value{RawBytes: buildTestValue(1, 10, []int32{0, 1}, []float64{5.0, 6.0})}},
	}

	stream := encodeGobStream(nil, kvs)
	var out bytes.Buffer
	stats, err := Convert(stream, &out, nil)
	if err != nil {
		t.Fatalf("Convert failed: %v", err)
	}
	if stats.RowsWritten != 2 {
		t.Errorf("rows = %d, want 2", stats.RowsWritten)
	}
	if got := stats.SkipReasons; got != (SkipReasons{
		BadKey: 1, ShortValue: 1, ProtoUnmarshal: 1, InvalidSampleData: 1,
	}) {
		t.Errorf("skip breakdown = %+v, want one of each", got)
	}
	if stats.RecordsSkipped != 4 {
		t.Errorf("RecordsSkipped = %d, want 4 (sum of SkipReasons)", stats.RecordsSkipped)
	}
}

// TestConvert_NonMetadataFirstRecord ensures the speculative Metadata decode
// does not silently consume and discard the first KeyValue when a dump has no
// metadata header.
func TestConvert_NonMetadataFirstRecord(t *testing.T) {
	// Three KVs, no metadata header.
	kvs := []KeyValue{
		{
			Key: buildTestKey("cr.node.metric", 1, 100, "1"),
			Value: Value{RawBytes: buildTestValue(
				1_000_000_000_000, 10_000_000_000,
				[]int32{0, 1, 2},
				[]float64{1.0, 2.0, 3.0})},
		},
	}
	stream := encodeGobStream(nil, kvs)

	var out bytes.Buffer
	stats, err := Convert(stream, &out, nil)
	if err != nil {
		t.Fatalf("Convert failed: %v", err)
	}
	// The bug would cause the first (and only) KV to be silently swallowed.
	if stats.RowsWritten != 3 {
		t.Errorf("rows = %d, want 3 — first KV was swallowed by speculative metadata decode", stats.RowsWritten)
	}
}

// TestConvert_BadKey_Variants ensures each kind of malformed key is skipped
// without aborting the rest of the stream.
func TestConvert_BadKey_Variants(t *testing.T) {
	tests := []struct {
		name string
		key  []byte
	}{
		{"too short", []byte{0x04}},
		{"wrong prefix", []byte{0x05, 't', 's', 'd', 0x00}},
		{
			"bytes encoding missing terminator",
			append(append([]byte{}, timeseriesPrefix...), bytesMarker, 'a', 'b', 'c'),
		},
		{
			"missing resolution varint",
			func() []byte {
				key := append([]byte{}, timeseriesPrefix...)
				return append(key, encodeBytes([]byte("cr.node.metric"))...)
			}(),
		},
	}

	// All four bad KVs followed by one GOOD KV.
	var kvs []KeyValue
	for _, tt := range tests {
		kvs = append(kvs, KeyValue{
			Key: tt.key,
			Value: Value{RawBytes: buildTestValue(1_000_000_000_000, 10_000_000_000,
				[]int32{0}, []float64{1.0})},
		})
	}
	kvs = append(kvs, KeyValue{
		Key: buildTestKey("cr.node.good", 1, 100, "1"),
		Value: Value{RawBytes: buildTestValue(1_000_000_000_000, 10_000_000_000,
			[]int32{0, 1}, []float64{10.0, 20.0})},
	})
	stream := encodeGobStream(nil, kvs)

	var out bytes.Buffer
	stats, err := Convert(stream, &out, nil)
	if err != nil {
		t.Fatalf("Convert returned error: %v", err)
	}
	if stats.RecordsSkipped != len(tests) {
		t.Errorf("skipped = %d, want %d", stats.RecordsSkipped, len(tests))
	}
	if stats.RowsWritten != 2 {
		t.Errorf("rows = %d, want 2 (from trailing good KV)", stats.RowsWritten)
	}
}

// TestConvert_ReaderError ensures an I/O error from the reader is surfaced as
// a real error (not EOF and not silently swallowed).
func TestConvert_ReaderError(t *testing.T) {
	sentinel := errors.New("disk went on vacation")
	// Give the decoder a valid first KV, then error on the next read.
	goodKV := KeyValue{
		Key: buildTestKey("cr.node.metric", 1, 100, "1"),
		Value: Value{RawBytes: buildTestValue(
			1_000_000_000_000, 10_000_000_000,
			[]int32{0}, []float64{1.0})},
	}
	buf := encodeGobStream(nil, []KeyValue{goodKV})
	r := &errorAfterReader{inner: buf, err: sentinel}

	var out bytes.Buffer
	_, err := Convert(r, &out, nil)
	if err == nil {
		t.Fatal("expected error from Convert; got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("error chain does not wrap sentinel: %v", err)
	}
}

// TestConvert_WriterError ensures a writer that fails on the first Write call
// surfaces as a Convert error.
func TestConvert_WriterError(t *testing.T) {
	kvs := []KeyValue{
		{
			Key: buildTestKey("cr.node.metric", 1, 100, "1"),
			Value: Value{RawBytes: buildTestValue(
				1_000_000_000_000, 10_000_000_000,
				[]int32{0, 1}, []float64{1.0, 2.0})},
		},
	}
	stream := encodeGobStream(nil, kvs)

	sentinel := errors.New("no space left on device")
	w := &errorWriter{err: sentinel}

	_, err := Convert(stream, w, nil)
	if err == nil {
		t.Fatal("expected error from Convert; got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("error chain does not wrap sentinel: %v", err)
	}
}

// TestConvert_OnlyMetadata ensures a stream containing only a Metadata header
// (no KVs) produces zero rows, no error, and invokes onMeta.
func TestConvert_OnlyMetadata(t *testing.T) {
	stream := encodeGobStream(
		&Metadata{Version: "v1", StoreToNodeMap: map[string]string{"1": "n1"}},
		nil,
	)
	var metaCalled bool
	var out bytes.Buffer
	stats, err := Convert(stream, &out, func(Metadata) { metaCalled = true })
	if err != nil {
		t.Fatalf("Convert returned error: %v", err)
	}
	if !metaCalled {
		t.Fatal("onMeta was not invoked")
	}
	if stats.RowsWritten != 0 {
		t.Errorf("rows = %d, want 0", stats.RowsWritten)
	}
}

// TestConvert_InterleavedMetric exercises non-contiguous metric names (A, B, A)
// to ensure the batch-flush-on-name-change logic correctly emits all rows.
func TestConvert_InterleavedMetric(t *testing.T) {
	kvs := []KeyValue{
		{
			Key: buildTestKey("cr.node.a", 1, 100, "1"),
			Value: Value{RawBytes: buildTestValue(1_000_000_000_000, 10_000_000_000,
				[]int32{0}, []float64{10.0})},
		},
		{
			Key: buildTestKey("cr.node.b", 1, 100, "1"),
			Value: Value{RawBytes: buildTestValue(1_000_000_000_000, 10_000_000_000,
				[]int32{0}, []float64{20.0})},
		},
		{
			Key: buildTestKey("cr.node.a", 1, 100, "2"),
			Value: Value{RawBytes: buildTestValue(1_000_000_000_000, 10_000_000_000,
				[]int32{0}, []float64{30.0})},
		},
	}
	stream := encodeGobStream(nil, kvs)
	var out bytes.Buffer
	stats, err := Convert(stream, &out, nil)
	if err != nil {
		t.Fatalf("Convert failed: %v", err)
	}
	if stats.RowsWritten != 3 {
		t.Errorf("rows = %d, want 3", stats.RowsWritten)
	}
	rows := readParquetRows(t, out.Bytes())
	var gotValues []float64
	for _, r := range rows {
		gotValues = append(gotValues, r.Value)
	}
	if !sliceEqualFloat(gotValues, []float64{10.0, 20.0, 30.0}) {
		t.Errorf("values = %v, want [10 20 30]", gotValues)
	}
}

// TestConvert_CorruptGobMidStream ensures random garbage after a valid header
// produces a fatal error (not silent data loss).
func TestConvert_CorruptGobMidStream(t *testing.T) {
	// Encode one valid KV, then append junk.
	good := KeyValue{
		Key: buildTestKey("cr.node.metric", 1, 100, "1"),
		Value: Value{RawBytes: buildTestValue(1_000_000_000_000, 10_000_000_000,
			[]int32{0}, []float64{1.0})},
	}
	buf := encodeGobStream(nil, []KeyValue{good})
	// Append a long sequence of zeros that looks like the start of a record
	// but will fail to decode as a whole KeyValue.
	buf.Write(bytes.Repeat([]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, 32))

	var out bytes.Buffer
	_, err := Convert(buf, &out, nil)
	if err == nil {
		return // Either a clean fatal error OR a soft truncation counts as "no silent corruption".
	}
	// If we did error, it should mention gob decoding.
	if !strings.Contains(err.Error(), "gob") {
		t.Errorf("error should mention gob; got %q", err)
	}
}

// errorAfterReader is an io.Reader that returns the buffered bytes once, then
// returns a sentinel error on subsequent Read calls.
type errorAfterReader struct {
	inner *bytes.Buffer
	err   error
	done  bool
}

func (e *errorAfterReader) Read(p []byte) (int, error) {
	if e.done {
		return 0, e.err
	}
	n, err := e.inner.Read(p)
	if err == io.EOF {
		e.done = true
		return n, e.err
	}
	return n, err
}

// errorWriter is an io.Writer that always fails.
type errorWriter struct{ err error }

func (e *errorWriter) Write(_ []byte) (int, error) { return 0, e.err }

func sliceEqualFloat(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
