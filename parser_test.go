package transcoder

import (
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

func mustTimestamp(s string) int64 {
	t, err := time.Parse("060102 15:04:05.000000", s)
	if err != nil {
		panic(fmt.Sprintf("mustTimestamp(%q): %v", s, err))
	}
	return t.UnixNano()
}

// padding returns n valid log entries that push the parser past the 6-line
// header threshold, so subsequent non-matching lines become implicit
// continuations rather than skipped headers.
func padding(n int) string {
	var b strings.Builder
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, "I260128 07:00:19.%06d 1 x.go:1 ⋮ [-] %d  p%d\n", i, i, i)
	}
	return b.String()
}

func TestParseSingleEntry(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  LogEntry
	}{
		{
			name:  "all fields with info severity",
			input: "I260128 07:00:20.057233 711 util/log/file_sync_buffer.go:237 ⋮ [T1,config] 1  file created at: 2026/01/28 07:00:20\n",
			want: LogEntry{
				Severity:   SeverityInfo,
				Time:       mustTimestamp("260128 07:00:20.057233"),
				Goroutine:  711,
				File:       "util/log/file_sync_buffer.go",
				Line:       237,
				Redactable: true,
				Counter:    1,
				TenantID:   "1",
				Tags:       "config",
				Channel:    ChannelDev,
				Message:    "file created at: 2026/01/28 07:00:20",
			},
		},
		{
			name:  "channel prefix and tenant details",
			input: "I260128 07:00:20.056959 711 15@kv/kvserver/kvstorage/init.go:280 ⋮ [T1,Vsystem,n1,s1] 1  beginning range descriptor iteration\n",
			want: LogEntry{
				Severity:   SeverityInfo,
				Time:       mustTimestamp("260128 07:00:20.056959"),
				Goroutine:  711,
				File:       "kv/kvserver/kvstorage/init.go",
				Line:       280,
				Redactable: true,
				Counter:    1,
				TenantID:   "1",
				TenantName: "system",
				Tags:       "n1,s1",
				Channel:    15,
				Message:    "beginning range descriptor iteration",
			},
		},
		{
			name:  "empty tags defaults to system tenant",
			input: "I210116 21:49:17.073282 14 server/node.go:464 ⋮ [-] 23  started with engine type 2\n",
			want: LogEntry{
				Severity:   SeverityInfo,
				Time:       mustTimestamp("210116 21:49:17.073282"),
				Goroutine:  14,
				File:       "server/node.go",
				Line:       464,
				Redactable: true,
				Counter:    23,
				TenantID:   "1",
				Message:    "started with engine type 2",
			},
		},
		{
			name:  "warning severity",
			input: "W210116 21:49:17.073282 14 server/node.go:464 ⋮ [-] 23  test\n",
			want: LogEntry{
				Severity:   SeverityWarning,
				Time:       mustTimestamp("210116 21:49:17.073282"),
				Goroutine:  14,
				File:       "server/node.go",
				Line:       464,
				Redactable: true,
				Counter:    23,
				TenantID:   "1",
				Message:    "test",
			},
		},
		{
			name:  "error severity",
			input: "E210116 21:49:17.073282 14 server/node.go:464 ⋮ [-] 23  test\n",
			want: LogEntry{
				Severity:   SeverityError,
				Time:       mustTimestamp("210116 21:49:17.073282"),
				Goroutine:  14,
				File:       "server/node.go",
				Line:       464,
				Redactable: true,
				Counter:    23,
				TenantID:   "1",
				Message:    "test",
			},
		},
		{
			name:  "fatal severity",
			input: "F210116 21:49:17.073282 14 server/node.go:464 ⋮ [-] 23  test\n",
			want: LogEntry{
				Severity:   SeverityFatal,
				Time:       mustTimestamp("210116 21:49:17.073282"),
				Goroutine:  14,
				File:       "server/node.go",
				Line:       464,
				Redactable: true,
				Counter:    23,
				TenantID:   "1",
				Message:    "test",
			},
		},
		{
			name: "newline continuation (+)",
			input: "I210116 21:49:17.083093 14 1@cli/start.go:690 ⋮ [-] 40  node startup completed:\n" +
				"I210116 21:49:17.083093 14 1@cli/start.go:690 ⋮ [-] 40 +CockroachDB node starting at 2021-01-16 21:49 (took 0.0s)\n",
			want: LogEntry{
				Severity:   SeverityInfo,
				Time:       mustTimestamp("210116 21:49:17.083093"),
				Goroutine:  14,
				File:       "cli/start.go",
				Line:       690,
				Redactable: true,
				Counter:    40,
				TenantID:   "1",
				Channel:    1,
				Message:    "node startup completed:\nCockroachDB node starting at 2021-01-16 21:49 (took 0.0s)",
			},
		},
		{
			name: "long line continuation (|)",
			input: "I210116 21:49:17.073282 14 server/node.go:464 ⋮ [-] 23  aaaaaa\n" +
				"I210116 21:49:17.073282 14 server/node.go:464 ⋮ [-] 23 |bbbbbb\n",
			want: LogEntry{
				Severity:   SeverityInfo,
				Time:       mustTimestamp("210116 21:49:17.073282"),
				Goroutine:  14,
				File:       "server/node.go",
				Line:       464,
				Redactable: true,
				Counter:    23,
				TenantID:   "1",
				Message:    "aaaaaabbbbbb",
			},
		},
		{
			name:  "structured entry (=)",
			input: "I210116 21:49:17.080713 14 1@util/log/event_log.go:32 ⋮ [-] 32 ={\"Timestamp\":1610833757080706620,\"EventType\":\"node_restart\"}\n",
			want: LogEntry{
				Severity:      SeverityInfo,
				Time:          mustTimestamp("210116 21:49:17.080713"),
				Goroutine:     14,
				File:          "util/log/event_log.go",
				Line:          32,
				Redactable:    true,
				Counter:       32,
				TenantID:      "1",
				Channel:       1,
				StructuredEnd: 60,
				Message:       `{"Timestamp":1610833757080706620,"EventType":"node_restart"}`,
			},
		},
		{
			name: "header lines skipped",
			input: "I260128 07:00:19.211127 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] file created at: 2026/01/28 07:00:19\n" +
				"I260128 07:00:19.211129 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] running on machine: test\n" +
				"I260128 07:00:19.211133 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] binary: CockroachDB\n" +
				"I260128 07:00:19.211134 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] arguments: [./cockroach]\n" +
				"I260128 07:00:19.211137 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] log format (utf8=✓): crdb-v2\n" +
				"I260128 07:00:19.211138 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] line format: [IWEF]yymmdd hh:mm:ss.uuuuuu goid [chan@]file:line redactionmark \\[tags\\] [counter] msg\n" +
				"W260128 07:00:19.211004 1 1@cli/start.go:1479 ⋮ [T1,n?] 1  ALL SECURITY CONTROLS HAVE BEEN DISABLED!\n",
			want: LogEntry{
				Severity:   SeverityWarning,
				Time:       mustTimestamp("260128 07:00:19.211004"),
				Goroutine:  1,
				File:       "cli/start.go",
				Line:       1479,
				Redactable: true,
				Counter:    1,
				TenantID:   "1",
				Tags:       "n?",
				Channel:    1,
				Message:    "ALL SECURITY CONTROLS HAVE BEEN DISABLED!",
			},
		},
		{
			name:  "nested bracket tags (IPv6 address)",
			input: "I260128 07:06:33.847693 17156 4@util/log/event_log.go:90 ⋮ [T1,Vsystem,n1,client=[::1]:52978,hostnossl,user=root] 1  auth ok\n",
			want: LogEntry{
				Severity:   SeverityInfo,
				Time:       mustTimestamp("260128 07:06:33.847693"),
				Goroutine:  17156,
				File:       "util/log/event_log.go",
				Line:       90,
				Redactable: true,
				Counter:    1,
				TenantID:   "1",
				TenantName: "system",
				Tags:       "n1,client=[::1]:52978,hostnossl,user=root",
				Channel:    4,
				Message:    "auth ok",
			},
		},
		{
			name:  "entry at EOF without trailing newline",
			input: "I260128 07:00:20.057233 711 util/log/file_sync_buffer.go:237 ⋮ [T1,config] 1  no newline at end",
			want: LogEntry{
				Severity:   SeverityInfo,
				Time:       mustTimestamp("260128 07:00:20.057233"),
				Goroutine:  711,
				File:       "util/log/file_sync_buffer.go",
				Line:       237,
				Redactable: true,
				Counter:    1,
				TenantID:   "1",
				Tags:       "config",
				Message:    "no newline at end",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(strings.NewReader(tt.input))
			got, err := p.NextEntry()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("entry mismatch:\ngot:  %+v\nwant: %+v", got, tt.want)
			}
			if _, err := p.NextEntry(); err != io.EOF {
				t.Errorf("expected io.EOF after entry, got %v", err)
			}
		})
	}
}

func TestParseEntryStream(t *testing.T) {
	type check struct {
		idx     int
		counter uint64
		message string
	}

	tests := []struct {
		name      string
		input     string
		wantCount int
		checks    []check
	}{
		{
			name: "sequential entries",
			input: "I260128 07:00:20.057233 711 util/log/file_sync_buffer.go:237 ⋮ [T1,config] 1  first\n" +
				"I260128 07:00:20.057238 711 util/log/file_sync_buffer.go:237 ⋮ [T1,config] 2  second\n" +
				"I260128 07:00:20.057247 711 util/log/file_sync_buffer.go:237 ⋮ [T1,config] 3  third\n",
			wantCount: 3,
			checks: []check{
				{idx: 0, counter: 1, message: "first"},
				{idx: 1, counter: 2, message: "second"},
				{idx: 2, counter: 3, message: "third"},
			},
		},
		{
			name: "non-matching lines attach to preceding entry",
			input: padding(6) +
				"I260128 07:00:20.188982 98 1@cli/start.go:1304 ⋮ [T1,Vsystem,n1] 79  node startup completed:\n" +
				"CockroachDB node starting at 2026-01-28 (took 1.0s)\n" +
				"build:               CCL v26.2.0\n" +
				"webui:               http://localhost:8080\n" +
				"nodeID:              1\n" +
				"I260128 07:00:20.193299 1730 server/migration.go:145 ⋮ [T1,Vsystem,n1] 80  next entry\n",
			wantCount: 8,
			checks: []check{
				{
					idx:     6,
					counter: 79,
					message: "node startup completed:\n" +
						"CockroachDB node starting at 2026-01-28 (took 1.0s)\n" +
						"build:               CCL v26.2.0\n" +
						"webui:               http://localhost:8080\n" +
						"nodeID:              1",
				},
				{idx: 7, counter: 80, message: "next entry"},
			},
		},
		{
			name: "blank lines as implicit continuations",
			input: padding(6) +
				"W260128 07:00:19.211004 1 1@cli/start.go:1479 ⋮ [T1,n?] 7  ALL SECURITY CONTROLS HAVE BEEN DISABLED!\n" +
				"\n" +
				"This mode is intended for non-production testing only.\n" +
				"\n" +
				"In this mode:\n" +
				"- Your cluster is open to any client that can access any of your IP addresses.\n" +
				"I260128 07:00:19.211326 1 1@cli/start.go:1489 ⋮ [T1,n?] 8  next entry\n",
			wantCount: 8,
			checks: []check{
				{
					idx:     6,
					counter: 7,
					message: "ALL SECURITY CONTROLS HAVE BEEN DISABLED!\n" +
						"\n" +
						"This mode is intended for non-production testing only.\n" +
						"\n" +
						"In this mode:\n" +
						"- Your cluster is open to any client that can access any of your IP addresses.",
				},
				{idx: 7, counter: 8, message: "next entry"},
			},
		},
		{
			name: "indented continuation lines",
			input: padding(6) +
				"W260128 07:00:19.211347 1 1@cli/start.go:1378 ⋮ [T1,n?] 7  Using the default setting for --cache (256 MiB).\n" +
				"  A significantly larger value is usually needed for good performance.\n" +
				"  If you have a dedicated server a reasonable setting is --cache=.25 (9.0 GiB).\n" +
				"I260128 07:00:19.211391 1 1@cli/start.go:1518 ⋮ [T1,n?] 8  next\n",
			wantCount: 8,
			checks: []check{
				{
					idx:     6,
					counter: 7,
					message: "Using the default setting for --cache (256 MiB).\n" +
						"  A significantly larger value is usually needed for good performance.\n" +
						"  If you have a dedicated server a reasonable setting is --cache=.25 (9.0 GiB).",
				},
				{idx: 7, counter: 8, message: "next"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(strings.NewReader(tt.input))
			var entries []LogEntry
			for {
				entry, err := p.NextEntry()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				entries = append(entries, entry)
			}
			if len(entries) != tt.wantCount {
				t.Fatalf("entry count: got %d, want %d", len(entries), tt.wantCount)
			}
			for _, c := range tt.checks {
				e := entries[c.idx]
				if e.Counter != c.counter {
					t.Errorf("entries[%d] counter: got %d, want %d", c.idx, e.Counter, c.counter)
				}
				if e.Message != c.message {
					t.Errorf("entries[%d] message:\ngot:  %q\nwant: %q", c.idx, e.Message, c.message)
				}
			}
		})
	}
}

func TestEmptyInput(t *testing.T) {
	p := NewParser(strings.NewReader(""))
	_, err := p.NextEntry()
	if err != io.EOF {
		t.Errorf("expected io.EOF for empty input, got %v", err)
	}
}
