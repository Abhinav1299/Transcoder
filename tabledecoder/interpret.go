package tabledecoder

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

// interpretString decodes s from one of several supported encodings,
// trying each in order: raw hex, PostgreSQL \x hex, base64, Go quoted string.
// This mirrors the logic in CockroachDB's pkg/cli/decode.go.
func interpretString(s string) ([]byte, bool) {
	if b, err := hex.DecodeString(s); err == nil {
		return b, true
	}
	if strings.HasPrefix(s, `\x`) {
		if b, err := hex.DecodeString(s[2:]); err == nil {
			return b, true
		}
	}
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		return b, true
	}
	s = strings.TrimSpace(s)
	if (strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) ||
		(strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`)) {
		s = s[1 : len(s)-1]
	}
	s = fmt.Sprintf(`"%s"`, s)
	if unquoted, err := strconv.Unquote(s); err == nil {
		return []byte(unquoted), true
	}
	return nil, false
}
