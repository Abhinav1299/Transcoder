package tabledecoder

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestInterpretString(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []byte
		ok    bool
	}{
		{
			name:  "raw hex",
			input: "deadbeef",
			want:  []byte{0xde, 0xad, 0xbe, 0xef},
			ok:    true,
		},
		{
			name:  "pg hex format",
			input: `\xdeadbeef`,
			want:  []byte{0xde, 0xad, 0xbe, 0xef},
			ok:    true,
		},
		{
			name:  "empty pg hex",
			input: `\x`,
			want:  []byte{},
			ok:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := interpretString(tt.input)
			if ok != tt.ok {
				t.Fatalf("interpretString(%q): ok=%v, want %v", tt.input, ok, tt.ok)
			}
			if ok && !bytes.Equal(got, tt.want) {
				t.Fatalf("interpretString(%q) = %x, want %x", tt.input, got, tt.want)
			}
		})
	}
}

func TestDecodeUUID(t *testing.T) {
	// Test cases from CRDB's testdata/table_dump_column_parsing.
	// CRDB's decodeUUID passes raw string bytes to encoding.DecodeUUIDValue.
	// The `\` character (0x5c) decodes as value tag with type=12 (UUID),
	// and the next 16 ASCII bytes become the UUID payload.
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name:  "fingerprint_id from statement_statistics",
			input: `\x0069f90926c070b5`,
			want:  "78303036-3966-3930-3932-366330373062",
		},
		{
			name:  "transaction_fingerprint_id",
			input: `\x3b78e4a8d5fbb7ae`,
			want:  "78336237-3865-3461-3864-356662623761",
		},
		{
			name:  "session_id from sqlliveness",
			input: `\x0101800fd1b2a2f6004f608256705bbaef3f84`,
			want:  "78303130-3138-3030-6664-316232613266",
		},
		{
			name:  "uniqueID from eventlog",
			input: `\x8c3cfa9c789b40438675af0a91017f7a`,
			want:  "78386333-6366-6139-6337-383962343034",
		},
		{
			name:    "too short",
			input:   `\x00`,
			wantErr: true,
		},
		{
			name:    "empty",
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeUUID(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("DecodeUUID(%q): err=%v, wantErr=%v", tt.input, err, tt.wantErr)
			}
			if err == nil && got != tt.want {
				t.Fatalf("DecodeUUID(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestDecodeRegion(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`\x80`, "NULL"},
		{"us-east-1", "us-east-1"},
		{"eu-west-1", "eu-west-1"},
	}

	for _, tt := range tests {
		got, err := DecodeRegion(tt.input)
		if err != nil {
			t.Fatalf("DecodeRegion(%q): unexpected error: %v", tt.input, err)
		}
		if got != tt.want {
			t.Fatalf("DecodeRegion(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestDecodeKey(t *testing.T) {
	// DecodeKey uses interpretString to get raw bytes, then formats them.
	got, err := DecodeKey(`\xdeadbeef`)
	if err != nil {
		t.Fatalf("DecodeKey: unexpected error: %v", err)
	}
	if got == "" {
		t.Fatal("DecodeKey returned empty string")
	}
	// The result should be a Go-quoted string of the decoded bytes.
	t.Logf("DecodeKey result: %s", got)

	// Test with a known key from CRDB test data.
	got2, err := DecodeKey(`\x88`)
	if err != nil {
		t.Fatalf("DecodeKey(\\x88): unexpected error: %v", err)
	}
	if got2 == "" {
		t.Fatal("DecodeKey returned empty string for \\x88")
	}
	t.Logf("DecodeKey(\\x88) result: %s", got2)
}

func TestDecodeTSV_Passthrough(t *testing.T) {
	input := "col1\tcol2\tcol3\nval1\tval2\tval3\n"
	var buf bytes.Buffer
	err := DecodeTSV(strings.NewReader(input), &buf, nil, false)
	if err != nil {
		t.Fatalf("DecodeTSV: %v", err)
	}
	if buf.String() != input {
		t.Fatalf("DecodeTSV passthrough:\ngot:  %q\nwant: %q", buf.String(), input)
	}
}

func TestDecodeTSV_WithParsers(t *testing.T) {
	input := "name\tregion\nfoo\t\\x80\nbar\tus-east-1\n"
	var buf bytes.Buffer
	parsers := ColumnParsers{
		"region": DecodeRegion,
	}
	err := DecodeTSV(strings.NewReader(input), &buf, parsers, false)
	if err != nil {
		t.Fatalf("DecodeTSV: %v", err)
	}
	want := "name\tregion\nfoo\tNULL\nbar\tus-east-1\n"
	if buf.String() != want {
		t.Fatalf("DecodeTSV with parsers:\ngot:  %q\nwant: %q", buf.String(), want)
	}
}

func TestDecodeTSV_SkipColumn(t *testing.T) {
	input := "id\tlock_key\tlock_key_pretty\n1\t\\xdeadbeef\t/Table/1\n"
	var buf bytes.Buffer
	parsers := ColumnParsers{
		"lock_key": nil, // nil = skip column
	}
	err := DecodeTSV(strings.NewReader(input), &buf, parsers, false)
	if err != nil {
		t.Fatalf("DecodeTSV: %v", err)
	}
	want := "id\tlock_key_pretty\n1\t/Table/1\n"
	if buf.String() != want {
		t.Fatalf("DecodeTSV skip column:\ngot:  %q\nwant: %q", buf.String(), want)
	}
}

func TestDecodeTSV_NullPassthrough(t *testing.T) {
	input := "name\tregion\nfoo\tNULL\n"
	var buf bytes.Buffer
	parsers := ColumnParsers{
		"region": DecodeRegion,
	}
	err := DecodeTSV(strings.NewReader(input), &buf, parsers, false)
	if err != nil {
		t.Fatalf("DecodeTSV: %v", err)
	}
	want := "name\tregion\nfoo\tNULL\n"
	if buf.String() != want {
		t.Fatalf("DecodeTSV NULL passthrough:\ngot:  %q\nwant: %q", buf.String(), want)
	}
}

func TestDecodeTSV_EmptyFile(t *testing.T) {
	var buf bytes.Buffer
	err := DecodeTSV(strings.NewReader(""), &buf, nil, false)
	if err != nil {
		t.Fatalf("DecodeTSV empty: %v", err)
	}
	if buf.String() != "" {
		t.Fatalf("DecodeTSV empty: got %q, want empty", buf.String())
	}
}

func TestDecodeTSV_HeaderOnly(t *testing.T) {
	input := "col1\tcol2\n"
	var buf bytes.Buffer
	err := DecodeTSV(strings.NewReader(input), &buf, nil, false)
	if err != nil {
		t.Fatalf("DecodeTSV header-only: %v", err)
	}
	if buf.String() != input {
		t.Fatalf("DecodeTSV header-only:\ngot:  %q\nwant: %q", buf.String(), input)
	}
}

func TestRegistry_LookupTable(t *testing.T) {
	tests := []struct {
		path       string
		wantFound  bool
		wantDecode bool
	}{
		{"system.descriptor.txt", true, true},
		{"system.namespace.txt", true, false},
		{"nodes/1/crdb_internal.node_metrics.txt", true, false},
		{"nodes/1/crdb_internal.node_execution_insights.txt", true, true},
		{"unknown_file.txt", false, false},
		{"cockroach.log", false, false},
	}

	for _, tt := range tests {
		cfg := LookupTable(tt.path)
		if (cfg != nil) != tt.wantFound {
			t.Errorf("LookupTable(%q): found=%v, want %v", tt.path, cfg != nil, tt.wantFound)
			continue
		}
		if cfg != nil && cfg.HasDecoders() != tt.wantDecode {
			t.Errorf("LookupTable(%q): hasDecoders=%v, want %v", tt.path, cfg.HasDecoders(), tt.wantDecode)
		}
	}
}

func TestDecodeTSV_RealSqlliveness(t *testing.T) {
	// Real data pattern from debug zip sample.
	input := "session_id\texpiration\tcrdb_region\n" +
		`\x0101800fd1b2a2f6004f608256705bbaef3f84` + "\t1772455206244157000.0000000000\t" + `\x80` + "\n"

	var buf bytes.Buffer
	parsers := ColumnParsers{
		"session_id":  DecodeUUID,
		"crdb_region": DecodeRegion,
	}
	err := DecodeTSV(strings.NewReader(input), &buf, parsers, false)
	if err != nil {
		t.Fatalf("DecodeTSV real sqlliveness: %v", err)
	}

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d: %q", len(lines), buf.String())
	}

	if lines[0] != "session_id\texpiration\tcrdb_region" {
		t.Fatalf("unexpected header: %q", lines[0])
	}

	fields := strings.Split(lines[1], "\t")
	if len(fields) != 3 {
		t.Fatalf("expected 3 fields, got %d: %q", len(fields), lines[1])
	}

	// Expected UUID from CRDB test data.
	wantUUID := "78303130-3138-3030-6664-316232613266"
	if fields[0] != wantUUID {
		t.Errorf("session_id: got %q, want %q", fields[0], wantUUID)
	}

	if fields[2] != "NULL" {
		t.Errorf("crdb_region: got %q, want %q", fields[2], "NULL")
	}
}

func TestUUIDDecoding_ValueTagParsing(t *testing.T) {
	tests := []struct {
		name       string
		input      []byte
		wantLen    int
		wantVal    uint64
		wantRemain int
	}{
		{"single byte", []byte{0x0c}, 1, 0x0c, 0},
		{"two bytes", []byte{0x81, 0x00}, 2, 0x80, 0},
		{"with remainder", []byte{0x05, 0xff}, 1, 0x05, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			remaining, length, value, err := decodeNonsortingUvarint(tt.input)
			if err != nil {
				t.Fatalf("decodeNonsortingUvarint: %v", err)
			}
			if length != tt.wantLen {
				t.Errorf("length=%d, want %d", length, tt.wantLen)
			}
			if value != tt.wantVal {
				t.Errorf("value=%d, want %d", value, tt.wantVal)
			}
			if len(remaining) != tt.wantRemain {
				t.Errorf("remaining=%d bytes, want %d", len(remaining), tt.wantRemain)
			}
		})
	}
}

func TestDecodeValueTag(t *testing.T) {
	// The `\` character is byte 0x5c. 0x5c & 0xf = 0xc = 12 = UUID type.
	// This is how PG hex-formatted UUID columns decode correctly.
	offset, typ, err := decodeValueTag([]byte{0x5c})
	if err != nil {
		t.Fatalf("decodeValueTag(0x5c): %v", err)
	}
	if offset != 1 {
		t.Errorf("offset=%d, want 1", offset)
	}
	if typ != valueTypeUUID {
		t.Errorf("typ=%d, want %d (UUID)", typ, valueTypeUUID)
	}

	// Tag byte 0x0c: colIDDelta=0, typ=12 (UUID).
	offset, typ, err = decodeValueTag([]byte{0x0c})
	if err != nil {
		t.Fatalf("decodeValueTag(0x0c): %v", err)
	}
	if typ != valueTypeUUID {
		t.Errorf("typ=%d, want %d (UUID)", typ, valueTypeUUID)
	}
}

func TestDecodeTSV_QuotedFields(t *testing.T) {
	input := "col1\tcol2\tcol3\nval1\t\"CREATE TABLE test (\n\tid INT,\n\tname STRING\n)\"\tval3\n"
	var buf bytes.Buffer
	err := DecodeTSV(strings.NewReader(input), &buf, nil, true)
	if err != nil {
		t.Fatalf("DecodeTSV quoted: %v", err)
	}

	lines := strings.Split(buf.String(), "\n")
	// Header + 1 data row + trailing empty.
	if len(lines) < 2 {
		t.Fatalf("expected at least 2 lines, got %d", len(lines))
	}
	if lines[0] != "col1\tcol2\tcol3" {
		t.Fatalf("unexpected header: %q", lines[0])
	}
}

func TestProtoDecoder_Progress(t *testing.T) {
	// From CRDB testdata/table_dump_column_parsing: crdb_internal.system_jobs.txt
	parser := MakeProtoColumnParser("cockroach.sql.jobs.jobspb.Progress")

	got, err := parser(`\x10bdb9a5929b9f8a03aa0200`)
	if err != nil {
		t.Fatalf("Progress decode: %v", err)
	}

	// Verify it's valid JSON and contains expected fields.
	var m map[string]any
	if err := json.Unmarshal([]byte(got), &m); err != nil {
		t.Fatalf("Progress output is not valid JSON: %v\nGot: %s", err, got)
	}

	if _, ok := m["modified_micros"]; !ok {
		t.Errorf("expected modified_micros field in Progress JSON, got: %s", got)
	}
	t.Logf("Progress JSON: %s", got)
}

func TestProtoDecoder_ProtoInfo(t *testing.T) {
	// From CRDB testdata: system.tenants.txt
	parser := MakeProtoColumnParser("cockroach.multitenant.ProtoInfo")

	got, err := parser(`\x080110001a0020002a004200`)
	if err != nil {
		t.Fatalf("ProtoInfo decode: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal([]byte(got), &m); err != nil {
		t.Fatalf("ProtoInfo output is not valid JSON: %v\nGot: %s", err, got)
	}
	t.Logf("ProtoInfo JSON: %s", got)
}

func TestProtoDecoder_Descriptor(t *testing.T) {
	// From CRDB testdata: system.descriptor.txt (the "system" database descriptor).
	parser := MakeProtoColumnParser("cockroach.sql.sqlbase.Descriptor")

	got, err := parser(`\x12450a0673797374656d10011a250a0d0a0561646d696e1080101880100a0c0a04726f6f7410801018801012046e6f646518032200280140004a006a0808181002180020167000`)
	if err != nil {
		t.Fatalf("Descriptor decode: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal([]byte(got), &m); err != nil {
		t.Fatalf("Descriptor output is not valid JSON: %v\nGot: %s", err, got)
	}

	// Should contain a "database" union field.
	if _, ok := m["database"]; !ok {
		t.Errorf("expected 'database' field in Descriptor JSON, got: %s", got)
	}
	t.Logf("Descriptor JSON: %s", got)
}

func TestProtoDecoder_ScheduleState(t *testing.T) {
	parser := MakeProtoColumnParser("cockroach.jobs.jobspb.ScheduleState")

	// Empty state.
	got, err := parser(`\x`)
	if err != nil {
		t.Fatalf("ScheduleState empty decode: %v", err)
	}
	t.Logf("ScheduleState empty: %s", got)

	// State with "succeeded" status.
	got2, err := parser(`\x0a09737563636565646564`)
	if err != nil {
		t.Fatalf("ScheduleState decode: %v", err)
	}
	if !strings.Contains(got2, "succeeded") {
		t.Errorf("expected 'succeeded' in output, got: %s", got2)
	}
	t.Logf("ScheduleState: %s", got2)
}

func TestProtoDecoder_SpanConfig(t *testing.T) {
	parser := MakeProtoColumnParser("cockroach.roachpb.SpanConfig")

	got, err := parser(`\x08808080401080808080021a0308901c2805`)
	if err != nil {
		t.Fatalf("SpanConfig decode: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal([]byte(got), &m); err != nil {
		t.Fatalf("SpanConfig output is not valid JSON: %v\nGot: %s", err, got)
	}

	if _, ok := m["num_replicas"]; !ok {
		t.Errorf("expected num_replicas in SpanConfig JSON, got: %s", got)
	}
	t.Logf("SpanConfig JSON: %s", got)
}

func TestProtoDecoder_InvalidMessage(t *testing.T) {
	parser := MakeProtoColumnParser("nonexistent.Message")

	_, err := parser(`\xdeadbeef`)
	if err == nil {
		t.Fatal("expected error for nonexistent message type")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' error, got: %v", err)
	}
}
