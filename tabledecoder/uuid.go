package tabledecoder

import (
	"fmt"

	"github.com/google/uuid"
)

// CRDB value encoding type constants (from pkg/util/encoding).
const (
	valueTypeUUID     = 12
	valueTypeSentinel = 15
)

// decodeNonsortingUvarint decodes a CRDB-style nonsortable unsigned varint.
// Each byte contributes 7 bits of data; the high bit (0x80) signals continuation.
func decodeNonsortingUvarint(buf []byte) (remaining []byte, length int, value uint64, err error) {
	for i, b := range buf {
		value += uint64(b & 0x7f)
		if b < 0x80 {
			return buf[i+1:], i + 1, value, nil
		}
		value <<= 7
	}
	return buf, 0, 0, fmt.Errorf("unterminated uvarint")
}

// decodeValueTag decodes the CRDB value encoding tag that prefixes encoded values.
// Returns the data offset (bytes to skip) and the value type.
func decodeValueTag(b []byte) (dataOffset int, typ int, err error) {
	if len(b) == 0 {
		return 0, 0, fmt.Errorf("empty array")
	}
	remaining, n, tag, err := decodeNonsortingUvarint(b)
	if err != nil {
		return 0, 0, err
	}
	typ = int(tag & 0xf)
	dataOffset = n
	if typ == valueTypeSentinel {
		_, n2, tag2, err := decodeNonsortingUvarint(remaining)
		if err != nil {
			return 0, 0, err
		}
		typ = int(tag2)
		dataOffset += n2
	}
	return dataOffset, typ, nil
}

// decodeUUIDValue decodes a CRDB value-encoded UUID.
// The encoding is: value tag (uvarint with type UUID=12) followed by 16 raw bytes.
func decodeUUIDValue(b []byte) (uuid.UUID, error) {
	dataOffset, typ, err := decodeValueTag(b)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("decoding value tag: %w", err)
	}
	if typ != valueTypeUUID {
		return uuid.UUID{}, fmt.Errorf("value type is not UUID (12): got %d", typ)
	}
	b = b[dataOffset:]
	if len(b) < 16 {
		return uuid.UUID{}, fmt.Errorf("invalid uuid payload length: %d", len(b))
	}
	return uuid.FromBytes(b[:16])
}

// DecodeUUID is the column parser for UUID-encoded columns in table dumps.
// It passes the raw string bytes directly to the value-tag UUID decoder,
// matching CRDB's decodeUUID behavior: the PG hex prefix `\x` produces byte
// 0x5c as the first byte, which encodes value tag type=12 (UUID).
func DecodeUUID(s string) (string, error) {
	u, err := decodeUUIDValue([]byte(s))
	if err != nil {
		return "", err
	}
	return u.String(), nil
}
