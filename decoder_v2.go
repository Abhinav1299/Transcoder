package transcoder

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strings"
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
// CockroachDB's pkg/util/log/format_crdb_v2.go.
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

// decoderV2 is a streaming decoder for crdb-v2 formatted log files.
// It reads lines one at a time from an io.Reader, reassembles multi-line
// entries (continuation markers +, |, !), and yields complete LogEntry values.
type decoderV2 struct {
	reader      *bufio.Reader
	msgBuf      bytes.Buffer // reused across nextEntry calls to reduce allocations
	lines       int          // total lines read so far (used for header detection)
	nextMatch   [][]byte     // lookahead: buffered regex submatch for the next line
	isMalformed bool         // true when nextMatch was synthesised from a non-matching line
}

func newDecoderV2(r io.Reader) *decoderV2 {
	return &decoderV2{reader: bufio.NewReader(r)}
}

// NewParser returns a crdb-v2 decoder. It is kept for backward compatibility;
// new code should use NewEntryDecoder or NewEntryDecoderWithFormat.
func NewParser(r io.Reader) *decoderV2 {
	return newDecoderV2(r)
}

// Decode reads the next complete log entry into the provided LogEntry.
// It returns io.EOF after the last entry. Implements the EntryDecoder interface.
func (d *decoderV2) Decode(entry *LogEntry) error {
	e, err := d.nextEntry()
	if err != nil {
		return err
	}
	*entry = e
	return nil
}

// NextEntry reads and returns the next complete log entry.
// Kept for backward compatibility; Decode is the preferred method.
func (d *decoderV2) NextEntry() (LogEntry, error) {
	return d.nextEntry()
}

func (d *decoderV2) nextEntry() (LogEntry, error) {
	frag, err := d.peekFragment()
	if err != nil {
		return LogEntry{}, err
	}

	if d.isMalformed {
		d.popFragment()
		entry := initEntryFromMatch(frag)
		entry.Message = sanitizeUTF8(strings.TrimPrefix(string(safeGet(frag, idxMsg)), " "))
		return entry, nil
	}

	d.popFragment()
	entry := initEntryFromMatch(frag)

	d.msgBuf.Reset()
	d.msgBuf.Write(safeGet(frag, idxMsg))

	for {
		frag, err = d.peekFragment()
		if err != nil {
			break
		}
		if d.isMalformed {
			d.popFragment()
			d.msgBuf.WriteByte('\n')
			d.msgBuf.Write(safeGet(frag, idxMsg))
			continue
		}
		if !isContinuation(frag) {
			break
		}
		d.popFragment()
		addContinuation(&entry, &d.msgBuf, frag)
	}

	entry.Message = sanitizeUTF8(strings.TrimPrefix(d.msgBuf.String(), " "))
	return entry, nil
}

func (d *decoderV2) peekFragment() ([][]byte, error) {
	for d.nextMatch == nil {
		d.lines++
		line, err := d.reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return nil, err
		}
		if err == io.EOF && len(line) == 0 {
			return nil, io.EOF
		}
		line = bytes.TrimSuffix(line, []byte{'\n'})
		line = bytes.TrimSuffix(line, []byte{'\r'})

		if m := entryRE.FindSubmatch(line); m != nil {
			d.nextMatch = m
			if err == io.EOF {
				return d.nextMatch, nil
			}
			break
		}

		if d.lines <= maxHeaderLines {
			if err == io.EOF {
				return nil, io.EOF
			}
			continue
		}

		sanitized := bytes.ReplaceAll(line, []byte("\n"), []byte(" "))
		synthetic := []byte(fmt.Sprintf(
			"I000101 00:00:00.000000 0 unknown:0 %s [-] 0  %s",
			redactableIndicator, sanitized))
		d.nextMatch = entryRE.FindSubmatch(synthetic)
		if d.nextMatch == nil {
			if err == io.EOF {
				return nil, io.EOF
			}
			continue
		}
		d.isMalformed = true
		return d.nextMatch, nil
	}
	return d.nextMatch, nil
}

func (d *decoderV2) popFragment() {
	d.nextMatch = nil
	d.isMalformed = false
}

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
