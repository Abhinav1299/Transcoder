package transcoder

import (
	"bytes"
	"strconv"
	"strings"
	"time"
)

const (
	// severityChar maps severity enum values (1-indexed) to single-char
	// prefixes used in crdb log lines: I=Info, W=Warning, E=Error, F=Fatal.
	severityChar = "IWEF"

	// redactableIndicator is the UTF-8 marker (U+22EE) that appears in
	// crdb-v1 and crdb-v2 log headers to flag redactable content.
	redactableIndicator = "⋮"

	// Time layouts recognised in crdb timestamp fields.
	timeFormat       = "060102 15:04:05.000000"
	timeFormatWithTZ = "060102 15:04:05.000000-070000"

	// maxHeaderLines is the number of non-matching lines tolerated at the
	// top of a log file before the parser starts wrapping unrecognised
	// lines into synthetic entries.
	maxHeaderLines = 6

	// utf8Replacement is substituted for invalid UTF-8 sequences.
	// Parquet requires all string values to be valid UTF-8.
	utf8Replacement = "\uFFFD"
)

// Tag-parsing constants matching CockroachDB conventions.
const (
	tenantIDTagKey   = 'T'
	tenantNameTagKey = 'V'
	emptyTagMarker   = "-"
	systemTenantID   = "1"
)

func safeGet(m [][]byte, idx int) []byte {
	if idx < 0 || idx >= len(m) {
		return nil
	}
	return m[idx]
}

func sanitizeUTF8(s string) string {
	return strings.ToValidUTF8(s, utf8Replacement)
}

func parseSeverity(b []byte) int32 {
	if len(b) == 0 {
		return SeverityUnknown
	}
	if idx := strings.IndexByte(severityChar, b[0]); idx >= 0 {
		return int32(idx + 1)
	}
	return SeverityUnknown
}

func parseTimestamp(b []byte) int64 {
	format := timeFormat
	if len(b) > 7 && (b[len(b)-7] == '+' || b[len(b)-7] == '-') {
		format = timeFormatWithTZ
	}
	t, err := time.Parse(format, string(b))
	if err != nil {
		return 0
	}
	return t.UnixNano()
}

func parseChannel(b []byte) int32 {
	if len(b) == 0 {
		return 0
	}
	v, _ := strconv.ParseInt(string(b), 10, 32)
	return int32(v)
}

func parseIntField(b []byte) int64 {
	if len(b) == 0 {
		return 0
	}
	v, _ := strconv.ParseInt(string(b), 10, 64)
	return v
}

func parseUintField(b []byte) uint64 {
	if len(b) == 0 {
		return 0
	}
	v, _ := strconv.ParseUint(string(b), 10, 64)
	return v
}

func extractTenantDetails(tags []byte) (tenantID, tenantName string) {
	if bytes.Equal(tags, []byte(emptyTagMarker)) {
		return systemTenantID, ""
	}
	found, id, rest := maybeReadTag(tags, tenantIDTagKey)
	if !found {
		return systemTenantID, ""
	}
	_, name, _ := maybeReadTag(rest, tenantNameTagKey)
	return id, name
}

func extractTags(tags []byte) string {
	if bytes.Equal(tags, []byte(emptyTagMarker)) {
		return ""
	}
	remaining := skipKnownTags(tags, string([]byte{tenantIDTagKey, tenantNameTagKey}))
	return string(remaining)
}

func skipKnownTags(tags []byte, skip string) []byte {
	for len(tags) > 0 && len(skip) > 0 {
		if tags[0] != skip[0] {
			return tags
		}
		tags = tags[1:]
		skip = skip[1:]
		idx := bytes.IndexByte(tags, ',')
		if idx < 0 {
			return nil
		}
		tags = tags[idx+1:]
	}
	return tags
}

func maybeReadTag(tags []byte, key byte) (found bool, value string, rest []byte) {
	if len(tags) == 0 || tags[0] != key {
		return false, "", tags
	}
	tags = tags[1:]
	comma := bytes.IndexByte(tags, ',')
	if comma < 0 {
		return true, string(tags), nil
	}
	return true, string(tags[:comma]), tags[comma+1:]
}

func isConfigHeader(tags string) bool {
	return strings.Contains(tags, "config")
}

func trimFinalNewLines(s []byte) []byte {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '\n' {
			s = s[:i]
		} else {
			break
		}
	}
	return s
}
