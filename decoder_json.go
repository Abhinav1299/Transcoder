package transcoder

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

// jsonEntry represents a full JSON log entry (non-compact format).
// Field names match cockroachdb/cockroach pkg/util/log/format_json.go JSONEntry.
type jsonEntry struct {
	Header          int                    `json:"header,omitempty"`
	Message         string                 `json:"message"`
	Stacks          string                 `json:"stacks"`
	Tags            map[string]interface{} `json:"tags"`
	Event           map[string]interface{} `json:"event"`
	ChannelNumeric  int64                  `json:"channel_numeric,omitempty"`
	Timestamp       string                 `json:"timestamp,omitempty"`
	SeverityNumeric int64                  `json:"severity_numeric,omitempty"`
	Goroutine       int64                  `json:"goroutine,omitempty"`
	File            string                 `json:"file,omitempty"`
	Line            int64                  `json:"line,omitempty"`
	EntryCounter    uint64                 `json:"entry_counter,omitempty"`
	Redactable      int                    `json:"redactable,omitempty"`
	TenantID        int64                  `json:"tenant_id,omitempty"`
	TenantName      string                 `json:"tenant_name,omitempty"`
}

// jsonCompactEntry represents a compact JSON log entry.
// Field names match cockroachdb/cockroach pkg/util/log/format_json.go JSONCompactEntry.
type jsonCompactEntry struct {
	Header          int                    `json:"header,omitempty"`
	Message         string                 `json:"message"`
	Stacks          string                 `json:"stacks"`
	Tags            map[string]interface{} `json:"tags"`
	Event           map[string]interface{} `json:"event"`
	ChannelNumeric  int64                  `json:"c,omitempty"`
	Timestamp       string                 `json:"t,omitempty"`
	SeverityNumeric int64                  `json:"s,omitempty"`
	Goroutine       int64                  `json:"g,omitempty"`
	File            string                 `json:"f,omitempty"`
	Line            int64                  `json:"l,omitempty"`
	EntryCounter    uint64                 `json:"n,omitempty"`
	Redactable      int                    `json:"r,omitempty"`
	TenantID        int64                  `json:"T,omitempty"`
	TenantName      string                 `json:"V,omitempty"`
}

func (c *jsonCompactEntry) toFull() jsonEntry {
	return jsonEntry{
		Header:          c.Header,
		Message:         c.Message,
		Stacks:          c.Stacks,
		Tags:            c.Tags,
		Event:           c.Event,
		ChannelNumeric:  c.ChannelNumeric,
		Timestamp:       c.Timestamp,
		SeverityNumeric: c.SeverityNumeric,
		Goroutine:       c.Goroutine,
		File:            c.File,
		Line:            c.Line,
		EntryCounter:    c.EntryCounter,
		Redactable:      c.Redactable,
		TenantID:        c.TenantID,
		TenantName:      c.TenantName,
	}
}

// decoderJSON decodes JSON and JSON-compact formatted CockroachDB log files.
type decoderJSON struct {
	decoder *json.Decoder
	compact bool
}

func newDecoderJSON(r io.Reader, compact bool) *decoderJSON {
	return &decoderJSON{
		decoder: json.NewDecoder(r),
		compact: compact,
	}
}

// Decode reads the next JSON log entry into entry. Returns io.EOF when done.
func (d *decoderJSON) Decode(entry *LogEntry) error {
	var e jsonEntry
	if d.compact {
		var compact jsonCompactEntry
		if err := d.decoder.Decode(&compact); err != nil {
			return err
		}
		e = compact.toFull()
	} else {
		if err := d.decoder.Decode(&e); err != nil {
			return err
		}
	}
	return e.populate(entry)
}

// populate converts a jsonEntry into a LogEntry.
func (e *jsonEntry) populate(entry *LogEntry) error {
	*entry = LogEntry{}

	ts, err := fromFluentTimestamp(e.Timestamp)
	if err != nil {
		return err
	}
	entry.Time = ts
	entry.Goroutine = e.Goroutine
	entry.File = sanitizeUTF8(e.File)
	entry.Line = e.Line
	entry.Redactable = e.Redactable == 1

	entry.TenantID = systemTenantID
	if e.TenantID != 0 {
		entry.TenantID = fmt.Sprint(e.TenantID)
	}
	entry.TenantName = sanitizeUTF8(e.TenantName)

	if e.Header == 0 {
		entry.Severity = int32(e.SeverityNumeric)
		entry.Channel = int32(e.ChannelNumeric)
		entry.Counter = e.EntryCounter
	}

	var msg strings.Builder
	if e.Event != nil {
		by, err := json.Marshal(e.Event)
		if err != nil {
			return err
		}
		msg.Write(by)
		entry.StructuredStart = 0
		entry.StructuredEnd = uint32(msg.Len())
	} else {
		msg.WriteString(e.Message)
	}

	if e.Tags != nil {
		var tagParts []string
		for k, v := range e.Tags {
			tagParts = append(tagParts, fmt.Sprintf("%s=%v", k, v))
		}
		sort.Strings(tagParts)
		entry.Tags = sanitizeUTF8(strings.Join(tagParts, ","))
	}

	if e.Stacks != "" {
		entry.StackTraceStart = uint32(msg.Len()) + 1
		msg.WriteString("\nstack trace:\n")
		msg.WriteString(e.Stacks)
	}

	entry.Message = sanitizeUTF8(msg.String())
	return nil
}

// fromFluentTimestamp parses a fluent-bit timestamp ("seconds.nanoseconds")
// into nanoseconds since epoch. Ported from cockroachdb/cockroach
// pkg/util/log/format_json.go fromFluent.
func fromFluentTimestamp(timestamp string) (int64, error) {
	parts := strings.Split(timestamp, ".")
	if len(parts) != 2 {
		return 0, fmt.Errorf("bad timestamp format: %q", timestamp)
	}
	left, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, err
	}
	right, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, err
	}
	return left*1_000_000_000 + right, nil
}
