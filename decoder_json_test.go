package transcoder

import (
	"io"
	"strings"
	"testing"
)

func TestDecoderJSON(t *testing.T) {
	input := `{"channel_numeric":1,"timestamp":"1610833757.080706620","severity_numeric":1,"goroutine":14,"file":"server/node.go","line":464,"entry_counter":1,"redactable":1,"message":"hello world"}
{"channel_numeric":2,"timestamp":"1610833757.080706621","severity_numeric":2,"goroutine":15,"file":"server/node.go","line":465,"entry_counter":2,"message":"warning msg"}
`

	dec := newDecoderJSON(strings.NewReader(input), false)
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

	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}

	e := entries[0]
	if e.Severity != SeverityInfo {
		t.Errorf("Severity = %d, want %d", e.Severity, SeverityInfo)
	}
	if e.Channel != 1 {
		t.Errorf("Channel = %d, want 1", e.Channel)
	}
	if e.Time != 1610833757080706620 {
		t.Errorf("Time = %d, want 1610833757080706620", e.Time)
	}
	if e.Goroutine != 14 {
		t.Errorf("Goroutine = %d, want 14", e.Goroutine)
	}
	if e.File != "server/node.go" {
		t.Errorf("File = %q, want %q", e.File, "server/node.go")
	}
	if e.Line != 464 {
		t.Errorf("Line = %d, want 464", e.Line)
	}
	if e.Counter != 1 {
		t.Errorf("Counter = %d, want 1", e.Counter)
	}
	if !e.Redactable {
		t.Error("Redactable = false, want true")
	}
	if e.Message != "hello world" {
		t.Errorf("Message = %q, want %q", e.Message, "hello world")
	}
	if e.TenantID != systemTenantID {
		t.Errorf("TenantID = %q, want %q", e.TenantID, systemTenantID)
	}

	e2 := entries[1]
	if e2.Severity != SeverityWarning {
		t.Errorf("entries[1].Severity = %d, want %d", e2.Severity, SeverityWarning)
	}
	if e2.Redactable {
		t.Error("entries[1].Redactable = true, want false")
	}
}

func TestDecoderJSONCompact(t *testing.T) {
	input := `{"c":1,"t":"1610833757.080706620","s":1,"g":14,"f":"server/node.go","l":464,"n":1,"r":1,"message":"compact msg"}
`

	dec := newDecoderJSON(strings.NewReader(input), true)
	var e LogEntry
	if err := dec.Decode(&e); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if e.Channel != 1 {
		t.Errorf("Channel = %d, want 1", e.Channel)
	}
	if e.Severity != SeverityInfo {
		t.Errorf("Severity = %d, want %d", e.Severity, SeverityInfo)
	}
	if e.Time != 1610833757080706620 {
		t.Errorf("Time = %d, want 1610833757080706620", e.Time)
	}
	if e.Message != "compact msg" {
		t.Errorf("Message = %q, want %q", e.Message, "compact msg")
	}
	if !e.Redactable {
		t.Error("Redactable = false, want true")
	}
}

func TestDecoderJSONWithEvent(t *testing.T) {
	input := `{"channel_numeric":1,"timestamp":"1610833757.080706620","severity_numeric":1,"goroutine":14,"file":"server/node.go","line":464,"event":{"Timestamp":123,"EventType":"node_restart"}}
`

	dec := newDecoderJSON(strings.NewReader(input), false)
	var e LogEntry
	if err := dec.Decode(&e); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if e.StructuredEnd == 0 {
		t.Error("StructuredEnd should be > 0 for event entries")
	}
	if !strings.Contains(e.Message, "node_restart") {
		t.Errorf("Message should contain event JSON, got %q", e.Message)
	}
}

func TestDecoderJSONWithStacks(t *testing.T) {
	input := `{"channel_numeric":1,"timestamp":"1610833757.080706620","severity_numeric":3,"goroutine":14,"file":"server/node.go","line":464,"message":"panic","stacks":"goroutine 1:\nmain.go:42"}
`

	dec := newDecoderJSON(strings.NewReader(input), false)
	var e LogEntry
	if err := dec.Decode(&e); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if e.StackTraceStart == 0 {
		t.Error("StackTraceStart should be > 0 for entries with stacks")
	}
	if !strings.Contains(e.Message, "goroutine 1:") {
		t.Errorf("Message should contain stack trace, got %q", e.Message)
	}
}

func TestDecoderJSONWithTenantID(t *testing.T) {
	input := `{"channel_numeric":1,"timestamp":"1610833757.080706620","severity_numeric":1,"goroutine":14,"file":"server/node.go","line":464,"tenant_id":5,"tenant_name":"app","message":"tenant msg"}
`

	dec := newDecoderJSON(strings.NewReader(input), false)
	var e LogEntry
	if err := dec.Decode(&e); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if e.TenantID != "5" {
		t.Errorf("TenantID = %q, want %q", e.TenantID, "5")
	}
	if e.TenantName != "app" {
		t.Errorf("TenantName = %q, want %q", e.TenantName, "app")
	}
}

func TestDecoderJSONEmpty(t *testing.T) {
	dec := newDecoderJSON(strings.NewReader(""), false)
	var e LogEntry
	if err := dec.Decode(&e); err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}
