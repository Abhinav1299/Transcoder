package tsdecoder

import (
	"bytes"
	"testing"
)

func TestDecodeBytesAscending(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    []byte
		wantErr bool
	}{
		{
			name: "simple string",
			// bytesMarker + "hello" + escape + escapedTerm
			input: append(append([]byte{bytesMarker}, []byte("hello")...), escape, escapedTerm),
			want:  []byte("hello"),
		},
		{
			name: "empty string",
			// bytesMarker + escape + escapedTerm
			input: []byte{bytesMarker, escape, escapedTerm},
			want:  []byte{},
		},
		{
			name: "string with embedded null",
			// bytesMarker + "ab" + escape + escaped00 + "cd" + escape + escapedTerm
			input: append(append(append(append([]byte{bytesMarker}, []byte("ab")...), escape, escaped00), []byte("cd")...), escape, escapedTerm),
			want:  []byte{'a', 'b', escapedFF, 'c', 'd'},
		},
		{
			name:    "empty input",
			input:   []byte{},
			wantErr: true,
		},
		{
			name:    "wrong marker",
			input:   []byte{0x99, 'a', escape, escapedTerm},
			wantErr: true,
		},
		{
			name:    "no terminator",
			input:   []byte{bytesMarker, 'a', 'b'},
			wantErr: true,
		},
		{
			name: "with trailing bytes",
			// bytesMarker + "ab" + escape + escapedTerm + trailing bytes
			input: append(append([]byte{bytesMarker}, []byte("ab")...), escape, escapedTerm, 0xDE, 0xAD),
			want:  []byte("ab"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rest, got, err := DecodeBytesAscending(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !bytes.Equal(got, tt.want) {
				t.Errorf("value = %x, want %x", got, tt.want)
			}
			// For the trailing bytes case, verify rest contains the leftovers.
			if tt.name == "with trailing bytes" {
				if !bytes.Equal(rest, []byte{0xDE, 0xAD}) {
					t.Errorf("rest = %x, want dead", rest)
				}
			}
		})
	}
}

func TestDecodeUvarintAscending(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    uint64
		wantErr bool
	}{
		{
			name:  "zero",
			input: []byte{byte(intZero)},
			want:  0,
		},
		{
			name:  "small value 1",
			input: []byte{byte(intZero + 1)},
			want:  1,
		},
		{
			name:  "small value 42",
			input: []byte{byte(intZero + 42)},
			want:  42,
		},
		{
			name:  "max small value",
			input: []byte{byte(intZero + intSmall)},
			want:  uint64(intSmall),
		},
		{
			name: "one-byte large",
			// intZero + intSmall + 1 (length=1), then the value byte
			input: []byte{byte(intZero + intSmall + 1), 0x80},
			want:  0x80,
		},
		{
			name: "two-byte large",
			// intZero + intSmall + 2 (length=2), then 2 value bytes
			input: []byte{byte(intZero + intSmall + 2), 0x01, 0x00},
			want:  0x0100,
		},
		{
			name:    "empty input",
			input:   []byte{},
			wantErr: true,
		},
		{
			name:    "insufficient bytes for large value",
			input:   []byte{byte(intZero + intSmall + 2), 0x01},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got, err := DecodeUvarintAscending(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestDecodeVarintAscending(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    int64
		wantErr bool
	}{
		{
			name:  "zero",
			input: []byte{byte(intZero)},
			want:  0,
		},
		{
			name:  "positive small",
			input: []byte{byte(intZero + 5)},
			want:  5,
		},
		{
			name: "negative one",
			// For negative: length byte = intZero - 1 = 135, then ^(-1) bytes
			// -1 → length = -(intZero - 135) = -(-1) = 1
			// complement bytes: ^(-1) = 0, so byte = ^0 = 0xff... wait let me think.
			// Actually: length = int(b[0]) - intZero. If b[0] = intZero-1 = 135, length = -1.
			// length = -length = 1. Then we read 1 byte, v = (0 << 8) | int64(^byte).
			// For val = -1: we need ^v = -1, so v = 0.
			// v = int64(^byte) for one byte. So ^byte = 0, byte = 0xff.
			input: []byte{byte(intZero - 1), 0xff},
			want:  -1,
		},
		{
			name: "negative two",
			// -2: ^v = -2, v = 1. One byte: ^byte = 1, byte = 0xfe.
			input: []byte{byte(intZero - 1), 0xfe},
			want:  -2,
		},
		{
			name:    "empty",
			input:   []byte{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got, err := DecodeVarintAscending(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestExtractProto(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    []byte
		wantErr bool
	}{
		{
			name:  "valid payload",
			input: []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0xAA, 0xBB},
			want:  []byte{0xAA, 0xBB},
		},
		{
			name:  "exact header size",
			input: []byte{0x00, 0x00, 0x00, 0x00, 0x01},
			want:  []byte{},
		},
		{
			name:    "too short",
			input:   []byte{0x00, 0x01},
			wantErr: true,
		},
		{
			name:    "empty",
			input:   []byte{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExtractProto(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !bytes.Equal(got, tt.want) {
				t.Errorf("got %x, want %x", got, tt.want)
			}
		})
	}
}

// encodeBytes builds a bytesMarker-escaped encoding for the given value.
// This is the inverse of DecodeBytesAscending, used to construct test keys.
func encodeBytes(val []byte) []byte {
	var buf []byte
	buf = append(buf, bytesMarker)
	for _, b := range val {
		if b == escape {
			buf = append(buf, escape, escaped00)
		} else {
			buf = append(buf, b)
		}
	}
	buf = append(buf, escape, escapedTerm)
	return buf
}

// encodeUvarint builds the ascending encoding for an unsigned integer.
func encodeUvarint(v uint64) []byte {
	if v <= uint64(intSmall) {
		return []byte{byte(int(v) + intZero)}
	}
	// Count bytes needed.
	n := 0
	for tmp := v; tmp > 0; tmp >>= 8 {
		n++
	}
	buf := make([]byte, n+1)
	buf[0] = byte(intZero + intSmall + n)
	for i := n; i > 0; i-- {
		buf[i] = byte(v)
		v >>= 8
	}
	return buf
}

// encodeVarint builds the ascending encoding for a signed integer.
func encodeVarint(v int64) []byte {
	if v >= 0 {
		return encodeUvarint(uint64(v))
	}
	// Negative encoding.
	uv := uint64(^v)
	n := 0
	for tmp := uv; tmp > 0; tmp >>= 8 {
		n++
	}
	if n == 0 {
		n = 1
	}
	buf := make([]byte, n+1)
	buf[0] = byte(intZero - n)
	for i := n; i > 0; i-- {
		buf[i] = byte(^uv)
		uv >>= 8
	}
	return buf
}

func TestDecodeDataKey(t *testing.T) {
	tests := []struct {
		name       string
		buildKey   func() []byte
		wantName   string
		wantSource string
		wantErr    bool
	}{
		{
			name: "basic metric",
			buildKey: func() []byte {
				var key []byte
				key = append(key, timeseriesPrefix...)
				key = append(key, encodeBytes([]byte("cr.node.sql_conns"))...)
				key = append(key, encodeVarint(1)...) // Resolution10s
				key = append(key, encodeVarint(100)...)
				key = append(key, []byte("1")...) // source
				return key
			},
			wantName:   "node_sql_conns",
			wantSource: "1",
		},
		{
			name: "metric without cr. prefix",
			buildKey: func() []byte {
				var key []byte
				key = append(key, timeseriesPrefix...)
				key = append(key, encodeBytes([]byte("my_metric"))...)
				key = append(key, encodeVarint(1)...)
				key = append(key, encodeVarint(50)...)
				key = append(key, []byte("src")...)
				return key
			},
			wantName:   "my_metric",
			wantSource: "src",
		},
		{
			name: "wrong prefix",
			buildKey: func() []byte {
				return []byte{0x05, 't', 's', 'd', 0x00}
			},
			wantErr: true,
		},
		{
			name: "too short",
			buildKey: func() []byte {
				return []byte{0x04}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := tt.buildKey()
			name, source, _, err := DecodeDataKey(key)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if name != tt.wantName {
				t.Errorf("name = %q, want %q", name, tt.wantName)
			}
			if source != tt.wantSource {
				t.Errorf("source = %q, want %q", source, tt.wantSource)
			}
		})
	}
}
