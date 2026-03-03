package transcoder

import (
	"bufio"
	"bytes"
	"io"
	"regexp"
	"strconv"
	"strings"
)

// entryREV1 matches the preamble of a crdb-v1 log line. Ported from
// cockroachdb/cockroach pkg/util/log/format_crdb_v1.go.
//
// crdb-v1 line format:
//
//	Lyymmdd hh:mm:ss.uuuuuu goid [chan@]file:line marker [tags] [counter] msg
var entryREV1 = regexp.MustCompile(
	`(?m)^` +
		`([` + severityChar + `])` +
		`(\d{6} \d{2}:\d{2}:\d{2}.\d{6}(?:[---+]\d{6})?) ` +
		`(?:(\d+) )?` +
		`([^:]+):(\d+) ` +
		`((?:` + redactableIndicator + `)?) ` +
		`(?:\[((?:[^]]|\][^ ])+)\] )?`,
)

// structuredEntryPrefix is the string prefix for structured log messages
// in crdb-v1 format (e.g. "Structured entry: {json...}").
const structuredEntryPrefix = "Structured entry: "

// decoderV1 decodes crdb-v1 formatted log files. It uses a peek/lookahead
// pattern to correctly reassemble multi-line entries: lines that don't match
// entryREV1 are appended to the preceding entry's message rather than dropped.
type decoderV1 struct {
	reader    *bufio.Reader
	msgBuf    bytes.Buffer // reused across Decode calls to reduce allocations
	lines     int
	nextEntry *LogEntry // lookahead: buffered parsed entry from the next header line
	nextMsg   string    // the message portion of nextEntry (before continuations)
}

func newDecoderV1(r io.Reader) *decoderV1 {
	return &decoderV1{reader: bufio.NewReader(r)}
}

// Decode reads the next log entry into entry. Returns io.EOF when done.
// Non-matching lines following a valid entry header are appended to that
// entry's message as continuation lines.
func (d *decoderV1) Decode(entry *LogEntry) error {
	var current *LogEntry
	d.msgBuf.Reset()

	if d.nextEntry != nil {
		current = d.nextEntry
		d.msgBuf.WriteString(d.nextMsg)
		d.nextEntry = nil
		d.nextMsg = ""
	} else {
		for {
			line, err := d.reader.ReadBytes('\n')
			if err == io.EOF && len(line) == 0 {
				return io.EOF
			}
			if err != nil && err != io.EOF {
				return err
			}
			d.lines++

			var e LogEntry
			if parseErr := parseEntryV1(line, &e); parseErr != nil {
				if d.lines <= maxHeaderLines {
					if err == io.EOF {
						return io.EOF
					}
					continue
				}
				if err == io.EOF {
					return io.EOF
				}
				continue
			}
			if d.lines <= maxHeaderLines && isConfigHeader(e.Tags) {
				if err == io.EOF {
					return io.EOF
				}
				continue
			}
			current = &e
			d.msgBuf.WriteString(e.Message)
			break
		}
	}

	for {
		line, err := d.reader.ReadBytes('\n')
		if err == io.EOF && len(line) == 0 {
			break
		}
		if err != nil && err != io.EOF {
			break
		}
		d.lines++

		var e LogEntry
		if parseErr := parseEntryV1(line, &e); parseErr != nil {
			continuation := string(trimFinalNewLines(line))
			d.msgBuf.WriteByte('\n')
			d.msgBuf.WriteString(continuation)
			if err == io.EOF {
				break
			}
			continue
		}

		// Matched a new entry header — buffer it for the next Decode call.
		d.nextEntry = &e
		d.nextMsg = e.Message
		break
	}

	current.Message = sanitizeUTF8(d.msgBuf.String())
	*entry = *current
	return nil
}

// parseEntryV1 parses a single crdb-v1 log line into a LogEntry.
// Ported from cockroachdb/cockroach pkg/util/log/format_crdb_v1.go parseEntryV1.
func parseEntryV1(buf []byte, entry *LogEntry) error {
	m := entryREV1.FindSubmatch(buf)
	if m == nil {
		return ErrMalformedEntry
	}

	*entry = LogEntry{}

	// Severity.
	entry.Severity = parseSeverity(m[1])

	// Timestamp.
	entry.Time = parseTimestamp(m[2])

	// Goroutine ID.
	if len(m[3]) > 0 {
		g, err := strconv.Atoi(string(m[3]))
		if err != nil {
			return err
		}
		entry.Goroutine = int64(g)
	}

	// Channel (encoded as "chan@file") and file/line.
	entry.File = string(m[4])
	if idx := strings.IndexByte(entry.File, '@'); idx != -1 {
		ch, err := strconv.Atoi(entry.File[:idx])
		if err != nil {
			return err
		}
		entry.Channel = int32(ch)
		entry.File = entry.File[idx+1:]
	}

	line, err := strconv.Atoi(string(m[5]))
	if err != nil {
		return err
	}
	entry.Line = int64(line)

	// Redactable flag.
	redactable := len(m[6]) != 0
	entry.Redactable = redactable

	// Tags: extract tenant details then remaining tags.
	tags := m[7]
	entry.TenantID, entry.TenantName, tags = maybeReadTenantDetailsV1(tags)
	if len(tags) != 0 {
		entry.Tags = sanitizeUTF8(string(tags))
	}

	// Message: everything after the preamble match.
	msg := buf[len(m[0]):]

	// Try to parse an entry counter at the start of the message.
	i := 0
	for ; i < len(msg) && msg[i] >= '0' && msg[i] <= '9'; i++ {
		entry.Counter = entry.Counter*10 + uint64(msg[i]-'0')
	}
	if i > 0 && i < len(msg) && msg[i] == ' ' {
		msg = msg[i+1:]
	} else {
		entry.Counter = 0
	}

	entry.Message = sanitizeUTF8(string(trimFinalNewLines(msg)))

	// Detect structured entries.
	if strings.HasPrefix(entry.Message, structuredEntryPrefix+"{") {
		entry.StructuredStart = uint32(len(structuredEntryPrefix))
		if nl := strings.IndexByte(entry.Message, '\n'); nl != -1 {
			entry.StructuredEnd = uint32(nl)
			entry.StackTraceStart = uint32(nl + 1)
		} else {
			entry.StructuredEnd = uint32(len(entry.Message))
		}
	}

	return nil
}

// maybeReadTenantDetailsV1 extracts T<id> and V<name> tags from a v1
// tag byte slice. Returns systemTenantID when no T tag is present.
func maybeReadTenantDetailsV1(tags []byte) (tenantID, tenantName string, remaining []byte) {
	hasTenantID, id, rest := maybeReadTag(tags, tenantIDTagKey)
	if !hasTenantID {
		return systemTenantID, "", tags
	}
	_, name, rest := maybeReadTag(rest, tenantNameTagKey)
	return id, name, rest
}
