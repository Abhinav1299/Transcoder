package tsdecoder

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// Key encoding constants ported from CockroachDB's
// pkg/util/encoding/encoding.go.
const (
	bytesMarker byte = 0x12
	escape      byte = 0x00
	escapedTerm byte = 0x01
	escaped00   byte = 0xff
	escapedFF   byte = 0x00

	intZero  = 136 // IntMin(0x80) + intMaxWidth(8)
	intSmall = 109 // IntMax(0xfd) - intZero - intMaxWidth(8)

	// headerSize is the 4-byte CRC32 checksum + 1-byte value tag that
	// precedes the protobuf payload in a roachpb.Value's RawBytes.
	headerSize = 5
)

// timeseriesPrefix is the key prefix for all time series data: \x04tsd.
var timeseriesPrefix = []byte{0x04, 't', 's', 'd'}

// slabDurations maps resolution enum values to slab durations in nanoseconds.
var slabDurations = map[int64]int64{
	1: int64(time.Hour),      // Resolution10s
	2: int64(24 * time.Hour), // Resolution30m
	3: int64(time.Hour),      // Resolution1m
}

// DecodeDataKey extracts the metric name, source, and timestamp from a binary
// time-series key. Mirrors CockroachDB's ts.DecodeDataKey.
func DecodeDataKey(key []byte) (name, source string, timestamp int64, err error) {
	if len(key) < len(timeseriesPrefix) {
		return "", "", 0, fmt.Errorf("key too short: %x", key)
	}
	for i, b := range timeseriesPrefix {
		if key[i] != b {
			return "", "", 0, fmt.Errorf("missing timeseries prefix in key: %x", key)
		}
	}
	remainder := key[len(timeseriesPrefix):]

	// Decode series name.
	remainder, nameBytes, err := DecodeBytesAscending(remainder)
	if err != nil {
		return "", "", 0, fmt.Errorf("decoding name: %w", err)
	}

	// Decode resolution.
	remainder, resolution, err := DecodeVarintAscending(remainder)
	if err != nil {
		return "", "", 0, fmt.Errorf("decoding resolution: %w", err)
	}

	// Decode timeslot.
	remainder, timeslot, err := DecodeVarintAscending(remainder)
	if err != nil {
		return "", "", 0, fmt.Errorf("decoding timeslot: %w", err)
	}

	slabDur, ok := slabDurations[resolution]
	if !ok {
		slabDur = int64(time.Hour) // default fallback
	}
	timestamp = timeslot * slabDur

	// Remaining bytes are the source.
	source = string(remainder)
	name = strings.TrimPrefix(string(nameBytes), "cr.")
	name = strings.ReplaceAll(name, ".", "_")
	return name, source, timestamp, nil
}

// ExtractProto strips the 4-byte checksum and 1-byte tag from a Value's
// RawBytes, returning the raw protobuf payload.
func ExtractProto(rawBytes []byte) ([]byte, error) {
	if len(rawBytes) < headerSize {
		return nil, fmt.Errorf("rawBytes too short (%d bytes)", len(rawBytes))
	}
	return rawBytes[headerSize:], nil
}

// DecodeBytesAscending decodes an escape-encoded byte sequence.
// Ported from CockroachDB's pkg/util/encoding/encoding.go DecodeBytesAscending.
func DecodeBytesAscending(b []byte) (rest, value []byte, err error) {
	if len(b) == 0 || b[0] != bytesMarker {
		return nil, nil, fmt.Errorf("did not find marker %#x in buffer %#x", bytesMarker, b)
	}
	b = b[1:] // skip marker

	var r []byte
	for {
		// Find next escape byte.
		i := -1
		for j, c := range b {
			if c == escape {
				i = j
				break
			}
		}
		if i == -1 {
			return nil, nil, fmt.Errorf("did not find terminator in buffer %#x", b)
		}
		if i+1 >= len(b) {
			return nil, nil, fmt.Errorf("malformed escape in buffer %#x", b)
		}

		v := b[i+1]
		if v == escapedTerm {
			// End of encoded bytes.
			if r == nil {
				r = b[:i]
			} else {
				r = append(r, b[:i]...)
			}
			return b[i+2:], r, nil
		}
		if v != escaped00 {
			return nil, nil, fmt.Errorf("unknown escape sequence: %#x %#x", escape, v)
		}
		// Escaped null byte.
		r = append(r, b[:i]...)
		r = append(r, escapedFF)
		b = b[i+2:]
	}
}

// DecodeVarintAscending decodes a variable-length integer.
// Ported from CockroachDB's pkg/util/encoding/encoding.go DecodeVarintAscending.
func DecodeVarintAscending(b []byte) (rest []byte, val int64, err error) {
	if len(b) == 0 {
		return nil, 0, fmt.Errorf("insufficient bytes to decode varint")
	}
	length := int(b[0]) - intZero
	if length < 0 {
		length = -length
		remB := b[1:]
		if len(remB) < length {
			return nil, 0, fmt.Errorf("insufficient bytes to decode varint: %x", remB)
		}
		var v int64
		for _, t := range remB[:length] {
			v = (v << 8) | int64(^t)
		}
		return remB[length:], ^v, nil
	}

	remB, uv, err := DecodeUvarintAscending(b)
	if err != nil {
		return remB, 0, err
	}
	if uv > math.MaxInt64 {
		return nil, 0, fmt.Errorf("varint %d overflows int64", uv)
	}
	return remB, int64(uv), nil
}

// DecodeUvarintAscending decodes a variable-length unsigned integer.
// Ported from CockroachDB's pkg/util/encoding/encoding.go DecodeUvarintAscending.
func DecodeUvarintAscending(b []byte) (rest []byte, val uint64, err error) {
	if len(b) == 0 {
		return nil, 0, fmt.Errorf("insufficient bytes to decode uvarint")
	}
	length := int(b[0]) - intZero
	b = b[1:]
	if length <= intSmall {
		return b, uint64(length), nil
	}
	length -= intSmall
	if length < 0 || length > 8 {
		return nil, 0, fmt.Errorf("invalid uvarint length of %d", length)
	}
	if len(b) < length {
		return nil, 0, fmt.Errorf("insufficient bytes to decode uvarint: %x", b)
	}
	var v uint64
	for _, t := range b[:length] {
		v = (v << 8) | uint64(t)
	}
	return b[length:], v, nil
}
