package transcoder

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	// severityChar maps severity enum values (1-indexed) to single-char
	// prefixes in crdb-v2 log lines: I=Info, W=Warning, E=Error, F=Fatal.
	severityChar = "IWEF"

	// redactableIndicator is the UTF-8 marker (U+22EE) that separates the
	// header fields from the (potentially redacted) payload in crdb-v2.
	redactableIndicator = "⋮"

	// Time layouts recognised in crdb-v2 timestamp fields.
	timeFormat       = "060102 15:04:05.000000"
	timeFormatWithTZ = "060102 15:04:05.000000-070000"

	// maxHeaderLines is the number of non-matching lines tolerated at the
	// top of a log file (file-creation preamble) before the parser starts
	// wrapping unrecognised lines into synthetic entries.
	maxHeaderLines = 6

	// utf8Replacement is the character substituted for invalid UTF-8 sequences.
	// Parquet requires all string values to be valid UTF-8.
	utf8Replacement = "\uFFFD"
)

// Continuation markers defined by the crdb-v2 format.
const (
	contUnstructured byte = ' '  // first or only line of an unstructured message
	contStructured   byte = '='  // first or only line of a structured (JSON) message
	contNewline      byte = '+'  // newline continuation
	contLongLine     byte = '|'  // long-line continuation (no separator)
	contStackTrace   byte = '!'  // stack-trace continuation
)

// entryRE matches a single crdb-v2 log line. The pattern is ported from
// CockroachDB's pkg/util/log/format_crdb_v2.go (the entry-header regex
// used by the decoder).
var entryRE = regexp.MustCompile(
	`(?m)^` +
		`(?P<severity>[` + severityChar + `])` +
		`(?P<datetime>\d{6} \d{2}:\d{2}:\d{2}\.\d{6}(?:[---+]\d{6})?) ` +
		`(?:(?P<goroutine>\d+) )` +
		`(\(gostd\) )?` +
		`(?:(?P<channel>\d+)@)?` +
		`(?P<file>[^:]+):` +
		`(?:(?P<line>\d+) )` +
		`(?P<redactable>(?:` + redactableIndicator + `)?) ` +
		`\[(?P<tags>(?:[^]]|\][^ ])+)\] ` +
		`(?P<counter>\d*) ?` +
		`(?P<continuation>[ =!+|])` +
		`(?P<msg>.*)$`,
)

// Pre-computed subexpression indices into entryRE matches.
var (
	idxSeverity     = entryRE.SubexpIndex("severity")
	idxDatetime     = entryRE.SubexpIndex("datetime")
	idxGoroutine    = entryRE.SubexpIndex("goroutine")
	idxChannel      = entryRE.SubexpIndex("channel")
	idxFile         = entryRE.SubexpIndex("file")
	idxLine         = entryRE.SubexpIndex("line")
	idxRedactable   = entryRE.SubexpIndex("redactable")
	idxTags         = entryRE.SubexpIndex("tags")
	idxCounter      = entryRE.SubexpIndex("counter")
	idxContinuation = entryRE.SubexpIndex("continuation")
	idxMsg          = entryRE.SubexpIndex("msg")
)

// Tag-parsing constants matching CockroachDB conventions.
const (
	tenantIDTagKey   = 'T'
	tenantNameTagKey = 'V'
	emptyTagMarker   = "-"
	systemTenantID   = "1"
)

// Parser is a streaming decoder for crdb-v2 formatted log files.
// It reads lines one at a time from an io.Reader, reassembles multi-line
// entries (continuation markers +, |, !), and yields complete LogEntry values.
type Parser struct {
	reader      *bufio.Reader
	lines       int        // total lines read so far (used for header detection)
	nextMatch   [][]byte   // lookahead: buffered regex submatch for the next line
	isMalformed bool       // true when nextMatch was synthesised from a non-matching line
}

// NewParser returns a Parser that reads crdb-v2 log lines from r.
func NewParser(r io.Reader) *Parser {
	return &Parser{reader: bufio.NewReader(r)}
}

// NextEntry reads and returns the next complete log entry.
// It returns io.EOF after the last entry. Non-matching lines encountered
// before the first valid entry (up to maxHeaderLines) are silently skipped.
// Non-matching lines that follow a valid entry are appended to that entry's
// message as implicit continuations.
func (p *Parser) NextEntry() (LogEntry, error) {
	frag, err := p.peekFragment()
	if err != nil {
		return LogEntry{}, err
	}

	// Malformed path: the very first peeked fragment was synthesised from a
	// line that didn't match entryRE. Return it immediately with the raw
	// text as the message so no data is silently dropped.
	if p.isMalformed {
		p.popFragment()
		entry := initEntryFromMatch(frag)
		entry.Message = sanitizeUTF8(strings.TrimPrefix(string(safeGet(frag, idxMsg)), " "))
		return entry, nil
	}

	p.popFragment()
	entry := initEntryFromMatch(frag)

	var msgBuf bytes.Buffer
	msgBuf.Write(safeGet(frag, idxMsg))

	// Collect continuation lines that belong to this entry.
	for {
		frag, err = p.peekFragment()
		if err != nil {
			break
		}
		if p.isMalformed {
			// Non-matching line after a valid entry: treat as implicit continuation.
			p.popFragment()
			msgBuf.WriteByte('\n')
			msgBuf.Write(safeGet(frag, idxMsg))
			continue
		}
		if !isContinuation(frag) {
			break
		}
		p.popFragment()
		addContinuation(&entry, &msgBuf, frag)
	}

	entry.Message = sanitizeUTF8(strings.TrimPrefix(msgBuf.String(), " "))
	return entry, nil
}

// peekFragment returns the regex submatch for the next unconsumed line,
// reading from the underlying reader if necessary. The result remains
// buffered until popFragment is called.
func (p *Parser) peekFragment() ([][]byte, error) {
	for p.nextMatch == nil {
		p.lines++
		line, err := p.reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return nil, err
		}
		if err == io.EOF && len(line) == 0 {
			return nil, io.EOF
		}
		line = bytes.TrimSuffix(line, []byte{'\n'})
		line = bytes.TrimSuffix(line, []byte{'\r'})

		if m := entryRE.FindSubmatch(line); m != nil {
			p.nextMatch = m
			if err == io.EOF {
				return p.nextMatch, nil
			}
			break
		}

		// Line did not match entryRE.
		if p.lines <= maxHeaderLines {
			if err == io.EOF {
				return nil, io.EOF
			}
			continue
		}

		// Past the header preamble — wrap the raw line into a synthetic
		// entry so it can be attached to the preceding entry or returned
		// as a standalone malformed entry.
		sanitized := bytes.ReplaceAll(line, []byte("\n"), []byte(" "))
		synthetic := []byte(fmt.Sprintf(
			"I000101 00:00:00.000000 0 unknown:0 %s [-] 0  %s",
			redactableIndicator, sanitized))
		p.nextMatch = entryRE.FindSubmatch(synthetic)
		if p.nextMatch == nil {
			if err == io.EOF {
				return nil, io.EOF
			}
			continue
		}
		p.isMalformed = true
		return p.nextMatch, nil
	}
	return p.nextMatch, nil
}

// popFragment consumes the buffered lookahead fragment, allowing the next
// call to peekFragment to read a fresh line.
func (p *Parser) popFragment() {
	p.nextMatch = nil
	p.isMalformed = false
}

// isContinuation reports whether a regex submatch represents a continuation
// line (+, |, !) rather than a new entry (space or =).
func isContinuation(m [][]byte) bool {
	c := safeGet(m, idxContinuation)
	if len(c) == 0 {
		return false
	}
	switch c[0] {
	case contNewline, contLongLine, contStackTrace:
		return true
	}
	return false
}

// addContinuation appends a continuation line's payload to the message
// buffer, applying the semantics dictated by the continuation marker.
func addContinuation(entry *LogEntry, buf *bytes.Buffer, m [][]byte) {
	c := safeGet(m, idxContinuation)
	if len(c) == 0 {
		return
	}
	msg := safeGet(m, idxMsg)
	switch c[0] {
	case contNewline:
		buf.WriteByte('\n')
		buf.Write(msg)
	case contLongLine:
		buf.Write(msg)
		if entry.StructuredEnd != 0 {
			entry.StructuredEnd = uint32(buf.Len())
		}
	case contStackTrace:
		if entry.StackTraceStart == 0 {
			entry.StackTraceStart = uint32(buf.Len()) + 1
			buf.WriteString("\nstack trace:\n")
		} else {
			buf.WriteByte('\n')
		}
		buf.Write(msg)
	}
}

// safeGet returns m[idx], or nil if idx is out of bounds.
func safeGet(m [][]byte, idx int) []byte {
	if idx < 0 || idx >= len(m) {
		return nil
	}
	return m[idx]
}

// sanitizeUTF8 replaces invalid UTF-8 byte sequences with U+FFFD.
func sanitizeUTF8(s string) string {
	return strings.ToValidUTF8(s, utf8Replacement)
}

// initEntryFromMatch populates a LogEntry from a regex submatch, extracting
// all header fields (severity, timestamp, goroutine, channel, file, line,
// tags, tenant details, counter, structured markers).
func initEntryFromMatch(m [][]byte) LogEntry {
	tenantID, tenantName := extractTenantDetails(safeGet(m, idxTags))
	entry := LogEntry{
		Severity:   parseSeverity(safeGet(m, idxSeverity)),
		Time:       parseTimestamp(safeGet(m, idxDatetime)),
		Goroutine:  parseIntField(safeGet(m, idxGoroutine)),
		Channel:    parseChannel(safeGet(m, idxChannel)),
		File:       sanitizeUTF8(string(safeGet(m, idxFile))),
		Line:       parseIntField(safeGet(m, idxLine)),
		Redactable: len(safeGet(m, idxRedactable)) > 0,
		Tags:       sanitizeUTF8(extractTags(safeGet(m, idxTags))),
		TenantID:   sanitizeUTF8(tenantID),
		TenantName: sanitizeUTF8(tenantName),
		Counter:    parseUintField(safeGet(m, idxCounter)),
	}
	if cont := safeGet(m, idxContinuation); len(cont) > 0 && cont[0] == contStructured {
		entry.StructuredStart = 0
		entry.StructuredEnd = uint32(len(safeGet(m, idxMsg)))
	}
	return entry
}

// parseSeverity maps a single-byte severity prefix to its proto enum value.
func parseSeverity(b []byte) int32 {
	if len(b) == 0 {
		return SeverityUnknown
	}
	if idx := strings.IndexByte(severityChar, b[0]); idx >= 0 {
		return int32(idx + 1)
	}
	return SeverityUnknown
}

// parseTimestamp parses a crdb-v2 datetime field into UnixNano.
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

// parseChannel parses an optional channel number prefix (e.g. "15" from "15@file.go").
// Returns 0 (DEV) when absent.
func parseChannel(b []byte) int32 {
	if len(b) == 0 {
		return 0
	}
	v, _ := strconv.ParseInt(string(b), 10, 32)
	return int32(v)
}

// parseIntField parses a decimal integer, returning 0 on empty or invalid input.
func parseIntField(b []byte) int64 {
	if len(b) == 0 {
		return 0
	}
	v, _ := strconv.ParseInt(string(b), 10, 64)
	return v
}

// parseUintField parses a decimal unsigned integer, returning 0 on empty or invalid input.
func parseUintField(b []byte) uint64 {
	if len(b) == 0 {
		return 0
	}
	v, _ := strconv.ParseUint(string(b), 10, 64)
	return v
}

// extractTenantDetails reads T<id> and V<name> tags from the tag string.
// Returns the system tenant ID "1" when no T tag is present.
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

// extractTags returns the tag string with well-known tenant tags (T, V) stripped.
func extractTags(tags []byte) string {
	if bytes.Equal(tags, []byte(emptyTagMarker)) {
		return ""
	}
	remaining := skipKnownTags(tags, string([]byte{tenantIDTagKey, tenantNameTagKey}))
	return string(remaining)
}

// skipKnownTags advances past single-character tag keys listed in skip,
// mirroring CockroachDB's skipTags function.
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

// maybeReadTag attempts to consume a single-character-keyed tag from the
// front of the comma-separated tag list.
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
