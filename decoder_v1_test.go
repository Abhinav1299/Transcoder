package transcoder

import (
	"io"
	"strings"
	"testing"
)

func TestDecoderV1SingleEntry(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  LogEntry
	}{
		{
			name:  "basic info entry",
			input: "I210116 21:49:17.073282 14 server/node.go:464  [n1] hello world\n",
			want: LogEntry{
				Severity:  SeverityInfo,
				Time:      mustTimestamp("210116 21:49:17.073282"),
				Goroutine: 14,
				File:      "server/node.go",
				Line:      464,
				TenantID:  "1",
				Tags:      "n1",
				Message:   "hello world",
			},
		},
		{
			name:  "warning with redactable marker",
			input: "W210116 21:49:17.073282 14 server/node.go:464 ⋮ [n1] cache is low\n",
			want: LogEntry{
				Severity:   SeverityWarning,
				Time:       mustTimestamp("210116 21:49:17.073282"),
				Goroutine:  14,
				File:       "server/node.go",
				Line:       464,
				Redactable: true,
				TenantID:   "1",
				Tags:       "n1",
				Message:    "cache is low",
			},
		},
		{
			name:  "channel prefix in file field",
			input: "I210116 21:49:17.073282 14 1@cli/start.go:690  [n1] starting\n",
			want: LogEntry{
				Severity:  SeverityInfo,
				Time:      mustTimestamp("210116 21:49:17.073282"),
				Goroutine: 14,
				File:      "cli/start.go",
				Line:      690,
				Channel:   1,
				TenantID:  "1",
				Tags:      "n1",
				Message:   "starting",
			},
		},
		{
			name:  "entry with tenant details",
			input: "I210116 21:49:17.073282 14 server/node.go:464  [T1,Vsystem,n1,s1] started\n",
			want: LogEntry{
				Severity:   SeverityInfo,
				Time:       mustTimestamp("210116 21:49:17.073282"),
				Goroutine:  14,
				File:       "server/node.go",
				Line:       464,
				TenantID:   "1",
				TenantName: "system",
				Tags:       "n1,s1",
				Message:    "started",
			},
		},
		{
			name:  "entry with counter",
			input: "I210116 21:49:17.073282 14 server/node.go:464  [n1] 42 some message\n",
			want: LogEntry{
				Severity:  SeverityInfo,
				Time:      mustTimestamp("210116 21:49:17.073282"),
				Goroutine: 14,
				File:      "server/node.go",
				Line:      464,
				TenantID:  "1",
				Tags:      "n1",
				Counter:   42,
				Message:   "some message",
			},
		},
		{
			name:  "no tags",
			input: "I210116 21:49:17.073282 14 server/node.go:464  no tags here\n",
			want: LogEntry{
				Severity:  SeverityInfo,
				Time:      mustTimestamp("210116 21:49:17.073282"),
				Goroutine: 14,
				File:      "server/node.go",
				Line:      464,
				TenantID:  "1",
				Message:   "no tags here",
			},
		},
		{
			name:  "error severity",
			input: "E210116 21:49:17.073282 14 server/node.go:464  [n1] something broke\n",
			want: LogEntry{
				Severity:  SeverityError,
				Time:      mustTimestamp("210116 21:49:17.073282"),
				Goroutine: 14,
				File:      "server/node.go",
				Line:      464,
				TenantID:  "1",
				Tags:      "n1",
				Message:   "something broke",
			},
		},
		{
			name:  "fatal severity",
			input: "F210116 21:49:17.073282 14 server/node.go:464  [n1] panic\n",
			want: LogEntry{
				Severity:  SeverityFatal,
				Time:      mustTimestamp("210116 21:49:17.073282"),
				Goroutine: 14,
				File:      "server/node.go",
				Line:      464,
				TenantID:  "1",
				Tags:      "n1",
				Message:   "panic",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dec := newDecoderV1(strings.NewReader(tt.input))
			var got LogEntry
			if err := dec.Decode(&got); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("entry mismatch:\ngot:  %+v\nwant: %+v", got, tt.want)
			}
			var eof LogEntry
			if err := dec.Decode(&eof); err != io.EOF {
				t.Errorf("expected io.EOF, got %v (entry: %+v)", err, eof)
			}
		})
	}
}

func TestDecoderV1Stream(t *testing.T) {
	input := "I210116 21:49:17.073282 14 server/node.go:464  [n1] first\n" +
		"W210116 21:49:17.073283 15 server/node.go:465  [n2] second\n" +
		"E210116 21:49:17.073284 16 server/node.go:466  [n3] third\n"

	dec := newDecoderV1(strings.NewReader(input))
	var entries []LogEntry
	for {
		var e LogEntry
		err := dec.Decode(&e)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		entries = append(entries, e)
	}

	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(entries))
	}
	if entries[0].Message != "first" {
		t.Errorf("entries[0].Message = %q, want %q", entries[0].Message, "first")
	}
	if entries[1].Severity != SeverityWarning {
		t.Errorf("entries[1].Severity = %d, want %d", entries[1].Severity, SeverityWarning)
	}
	if entries[2].Message != "third" {
		t.Errorf("entries[2].Message = %q, want %q", entries[2].Message, "third")
	}
}

func TestDecoderV1SkipsNonMatchingLines(t *testing.T) {
	input := "this is not a log line\n" +
		"neither is this\n" +
		"I210116 21:49:17.073282 14 server/node.go:464  [n1] real entry\n"

	dec := newDecoderV1(strings.NewReader(input))
	var got LogEntry
	if err := dec.Decode(&got); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Message != "real entry" {
		t.Errorf("Message = %q, want %q", got.Message, "real entry")
	}
}

func TestDecoderV1EmptyInput(t *testing.T) {
	dec := newDecoderV1(strings.NewReader(""))
	var e LogEntry
	if err := dec.Decode(&e); err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}
