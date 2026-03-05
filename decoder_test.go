package transcoder

import (
	"io"
	"strings"
	"testing"
)

func TestNewEntryDecoderWithFormat(t *testing.T) {
	tests := []struct {
		name    string
		format  string
		input   string
		wantMsg string
	}{
		{
			name:    "explicit crdb-v2",
			format:  "crdb-v2",
			input:   "I260128 07:00:20.057233 711 util/log/file_sync_buffer.go:237 ⋮ [T1,config] 1  hello v2\n",
			wantMsg: "hello v2",
		},
		{
			name:    "explicit crdb-v1",
			format:  "crdb-v1",
			input:   "I210116 21:49:17.073282 14 server/node.go:464  [n1] hello v1\n",
			wantMsg: "hello v1",
		},
		{
			name:    "explicit json",
			format:  "json",
			input:   `{"channel_numeric":1,"timestamp":"1610833757.080706620","severity_numeric":1,"goroutine":14,"file":"server/node.go","line":464,"message":"hello json"}` + "\n",
			wantMsg: "hello json",
		},
		{
			name:    "explicit json-compact",
			format:  "json-compact",
			input:   `{"c":1,"t":"1610833757.080706620","s":1,"g":14,"f":"server/node.go","l":464,"message":"hello compact"}` + "\n",
			wantMsg: "hello compact",
		},
		{
			name:    "crdb-v1-tty alias",
			format:  "crdb-v1-tty",
			input:   "I210116 21:49:17.073282 14 server/node.go:464  [n1] hello tty\n",
			wantMsg: "hello tty",
		},
		{
			name:    "crdb-v2-tty alias",
			format:  "crdb-v2-tty",
			input:   "I260128 07:00:20.057233 711 util/log/file_sync_buffer.go:237 ⋮ [T1,config] 1  hello v2 tty\n",
			wantMsg: "hello v2 tty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dec, _, err := NewEntryDecoderWithFormat(strings.NewReader(tt.input), tt.format)
			if err != nil {
				t.Fatalf("NewEntryDecoderWithFormat(%q): %v", tt.format, err)
			}
			var e LogEntry
			if err := dec.Decode(&e); err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if e.Message != tt.wantMsg {
				t.Errorf("Message = %q, want %q", e.Message, tt.wantMsg)
			}
		})
	}
}

func TestNewEntryDecoderUnknownFormat(t *testing.T) {
	_, _, err := NewEntryDecoderWithFormat(strings.NewReader(""), "bogus-format")
	if err == nil {
		t.Fatal("expected error for unknown format")
	}
}

func TestFormatAutoDetectV2(t *testing.T) {
	header := "I260128 07:00:19.211137 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] log format (utf8=✓): crdb-v2\n" +
		"I260128 07:00:19.211138 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] line format: [IWEF]yymmdd hh:mm:ss.uuuuuu goid [chan@]file:line redactionmark \\[tags\\] [counter] msg\n" +
		"W260128 07:00:19.211004 1 1@cli/start.go:1479 ⋮ [T1,n?] 1  ALL SECURITY CONTROLS HAVE BEEN DISABLED!\n"

	dec, err := NewEntryDecoder(strings.NewReader(header))
	if err != nil {
		t.Fatalf("NewEntryDecoder: %v", err)
	}

	var e LogEntry
	if err := dec.Decode(&e); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	// The first two lines are header lines that get skipped by the v2 decoder.
	if e.Severity != SeverityWarning {
		t.Errorf("Severity = %d, want %d", e.Severity, SeverityWarning)
	}
}

func TestFormatAutoDetectV1(t *testing.T) {
	header := "I210116 21:49:17.073282 14 server/node.go:464  [config] line format: [IWEF]yymmdd hh:mm:ss.uuuuuu goid file:line msg\n" +
		"I210116 21:49:17.073283 14 server/node.go:464  [n1] hello from v1\n"

	dec, err := NewEntryDecoder(strings.NewReader(header))
	if err != nil {
		t.Fatalf("NewEntryDecoder: %v", err)
	}

	var entries []LogEntry
	for {
		var e LogEntry
		if err := dec.Decode(&e); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		entries = append(entries, e)
	}

	if len(entries) == 0 {
		t.Fatal("expected at least one entry")
	}
	last := entries[len(entries)-1]
	if last.Message != "hello from v1" {
		t.Errorf("last Message = %q, want %q", last.Message, "hello from v1")
	}
}

func TestFormatAutoDetectFallbackToV2(t *testing.T) {
	// No format header, should fall back to crdb-v2.
	input := "I260128 07:00:20.057233 711 util/log/file_sync_buffer.go:237 ⋮ [T1,config] 1  fallback v2\n"

	dec, err := NewEntryDecoder(strings.NewReader(input))
	if err != nil {
		t.Fatalf("NewEntryDecoder: %v", err)
	}
	var e LogEntry
	if err := dec.Decode(&e); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if e.Message != "fallback v2" {
		t.Errorf("Message = %q, want %q", e.Message, "fallback v2")
	}
}

func TestNewEntryDecoderEmptyInput(t *testing.T) {
	_, err := NewEntryDecoder(strings.NewReader(""))
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestExtractCRDBVersion(t *testing.T) {
	tests := []struct {
		name string
		data string
		want string
	}{
		{
			name: "standard release",
			data: "I260302 12:17:37.955400 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] binary: CockroachDB CCL v26.2.0-alpha.1-dev (darwin arm64, built , go1.25.5)\n",
			want: "v26.2.0-alpha.1-dev",
		},
		{
			name: "stable release",
			data: "I240101 00:00:00.000000 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] binary: CockroachDB CCL v24.1.0 (linux amd64, built 2024-05-20, go1.22.3)\n",
			want: "v24.1.0",
		},
		{
			name: "no binary line",
			data: "I260302 12:17:37.955393 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] file created at: 2026/03/02 12:17:37\n",
			want: "",
		},
		{
			name: "empty data",
			data: "",
			want: "",
		},
		{
			name: "full header with version in context",
			data: "I260302 12:17:37.955393 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] file created at: 2026/03/02 12:17:37\n" +
				"I260302 12:17:37.955396 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] running on machine: test-host\n" +
				"I260302 12:17:37.955400 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] binary: CockroachDB CCL v23.1.22 (linux amd64, built 2024-03-15, go1.21.6)\n" +
				"I260302 12:17:37.955406 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] log format (utf8=✓): crdb-v2\n",
			want: "v23.1.22",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractCRDBVersion([]byte(tt.data))
			if got != tt.want {
				t.Errorf("ExtractCRDBVersion() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNewEntryDecoderWithFormatReturnsVersion(t *testing.T) {
	header := "I260302 12:17:37.955393 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] file created at: 2026/03/02 12:17:37\n" +
		"I260302 12:17:37.955396 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] running on machine: test-host\n" +
		"I260302 12:17:37.955400 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] binary: CockroachDB CCL v26.2.0-alpha.1-dev (darwin arm64, built , go1.25.5)\n" +
		"I260302 12:17:37.955406 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] log format (utf8=✓): crdb-v2\n" +
		"I260302 12:17:37.955407 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] line format: [IWEF]yymmdd hh:mm:ss.uuuuuu goid [chan@]file:line redactionmark \\[tags\\] [counter] msg\n" +
		"W260302 12:17:37.955264 1 1@cli/start.go:1479 ⋮ [T1,n?] 1  ALL SECURITY CONTROLS HAVE BEEN DISABLED!\n"

	_, version, err := NewEntryDecoderWithFormat(strings.NewReader(header), "")
	if err != nil {
		t.Fatalf("NewEntryDecoderWithFormat: %v", err)
	}
	if version != "v26.2.0-alpha.1-dev" {
		t.Errorf("version = %q, want %q", version, "v26.2.0-alpha.1-dev")
	}
}

func TestNewEntryDecoderWithFormatExplicitNoVersion(t *testing.T) {
	input := "I260128 07:00:20.057233 711 util/log/file_sync_buffer.go:237 ⋮ [T1,config] 1  hello\n"

	_, version, err := NewEntryDecoderWithFormat(strings.NewReader(input), "crdb-v2")
	if err != nil {
		t.Fatalf("NewEntryDecoderWithFormat: %v", err)
	}
	if version != "" {
		t.Errorf("version = %q, want empty (explicit format skips header reading)", version)
	}
}
