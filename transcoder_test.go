package transcoder

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestConvertZIP(t *testing.T) {
	// Create a temporary input ZIP with a synthetic .log file.
	tmpDir := t.TempDir()
	inputPath := filepath.Join(tmpDir, "test-input.zip")
	outputPath := filepath.Join(tmpDir, "parquet.zip")

	logContent := `I260128 07:00:20.057233 711 util/log/file_sync_buffer.go:237 ⋮ [T1,config] 1  file created at: 2026/01/28 07:00:20
I260128 07:00:20.057238 711 util/log/file_sync_buffer.go:237 ⋮ [T1,config] 2  running on machine: test-host
I260128 07:00:20.056959 711 15@kv/kvserver/kvstorage/init.go:280 ⋮ [T1,Vsystem,n1,s1] 3  beginning range descriptor iteration
`

	createTestZip(t, inputPath, map[string]string{
		"nodes/1/logs/cockroach.log": logContent,
	})

	tr := &Transcoder{}
	stats, err := tr.ConvertZIP(context.Background(), inputPath, outputPath)
	if err != nil {
		t.Fatalf("ConvertZIP failed: %v", err)
	}

	if stats.FilesProcessed != 1 {
		t.Errorf("files processed: got %d, want 1", stats.FilesProcessed)
	}
	if stats.TotalEntries != 3 {
		t.Errorf("total entries: got %d, want 3", stats.TotalEntries)
	}

	// Verify the output ZIP contains a .parquet file.
	verifyParquetZip(t, outputPath, "nodes/1/logs/cockroach.parquet", 3)
}

func TestConvertStream(t *testing.T) {
	tests := []struct {
		name       string
		format     string
		input      string
		wantRows   int
		wantMalformed int64
	}{
		{
			name:   "crdb-v2 explicit format",
			format: "crdb-v2",
			input: "I260128 07:00:20.057233 711 util/log/file_sync_buffer.go:237 ⋮ [T1,config] 1  first entry\n" +
				"W260128 07:00:20.057234 712 util/log/file_sync_buffer.go:238 ⋮ [T1,config] 2  second entry\n",
			wantRows: 2,
		},
		{
			name:   "crdb-v1 explicit format",
			format: "crdb-v1",
			input:  "I210116 21:49:17.073282 14 server/node.go:464  [n1] hello v1\n",
			wantRows: 1,
		},
		{
			name:   "json explicit format",
			format: "json",
			input: `{"channel_numeric":1,"timestamp":"1610833757.080706620","severity_numeric":1,"goroutine":14,"file":"server/node.go","line":464,"message":"hello json"}` + "\n" +
				`{"channel_numeric":2,"timestamp":"1610833757.080706621","severity_numeric":2,"goroutine":15,"file":"server/node.go","line":465,"message":"second json"}` + "\n",
			wantRows: 2,
		},
		{
			name:   "json-compact explicit format",
			format: "json-compact",
			input:  `{"c":1,"t":"1610833757.080706620","s":1,"g":14,"f":"server/node.go","l":464,"message":"compact"}` + "\n",
			wantRows: 1,
		},
		{
			name: "auto-detect crdb-v2 with header",
			input: "I260128 07:00:19.211137 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] log format (utf8=✓): crdb-v2\n" +
				"I260128 07:00:19.211138 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] line format: [IWEF]yymmdd hh:mm:ss.uuuuuu goid [chan@]file:line redactionmark \\[tags\\] [counter] msg\n" +
				"W260128 07:00:19.211004 1 1@cli/start.go:1479 ⋮ [T1,n?] 1  ALL SECURITY CONTROLS HAVE BEEN DISABLED!\n",
			wantRows: 1,
		},
		{
			name:   "auto-detect fallback to v2",
			input:  "I260128 07:00:20.057233 711 util/log/file_sync_buffer.go:237 ⋮ [T1,config] 1  no header\n",
			wantRows: 1,
		},
		{
			name:   "crdb-v1 multiple entries",
			format: "crdb-v1",
			input: "I210116 21:49:17.073282 14 server/node.go:464  [n1] first v1\n" +
				"W210116 21:49:17.073283 15 server/node.go:465  [n1] second v1\n",
			wantRows: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			tr := &Transcoder{}
			stats, err := tr.ConvertStream(context.Background(), strings.NewReader(tt.input), &buf, tt.format)
			if err != nil {
				t.Fatalf("ConvertStream: %v", err)
			}

			if stats.TotalEntries != int64(tt.wantRows) {
				t.Errorf("TotalEntries = %d, want %d", stats.TotalEntries, tt.wantRows)
			}
			if stats.FilesProcessed != 1 {
				t.Errorf("FilesProcessed = %d, want 1", stats.FilesProcessed)
			}
			if stats.MalformedLines != tt.wantMalformed {
				t.Errorf("MalformedLines = %d, want %d", stats.MalformedLines, tt.wantMalformed)
			}

			verifyParquetBytes(t, buf.Bytes(), tt.wantRows)
		})
	}
}

func TestConvertStreamParquetMetadata(t *testing.T) {
	input := "I260302 12:17:37.955393 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] file created at: 2026/03/02 12:17:37\n" +
		"I260302 12:17:37.955400 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] binary: CockroachDB CCL v26.2.0-alpha.1-dev (darwin arm64, built , go1.25.5)\n" +
		"I260302 12:17:37.955406 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] log format (utf8=✓): crdb-v2\n" +
		"I260302 12:17:37.955407 1 util/log/file_sync_buffer.go:237 ⋮ [T1,config] line format: [IWEF]yymmdd hh:mm:ss.uuuuuu goid [chan@]file:line redactionmark \\[tags\\] [counter] msg\n" +
		"W260302 12:17:37.955264 1 1@cli/start.go:1479 ⋮ [T1,n?] 1  test entry\n"

	var buf bytes.Buffer
	tr := &Transcoder{}
	_, err := tr.ConvertStream(context.Background(), strings.NewReader(input), &buf, "")
	if err != nil {
		t.Fatalf("ConvertStream: %v", err)
	}

	pf, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("opening parquet: %v", err)
	}

	found := false
	for _, kv := range pf.Metadata().KeyValueMetadata {
		if kv.Key == "crdb_version" {
			found = true
			if kv.Value != "v26.2.0-alpha.1-dev" {
				t.Errorf("crdb_version = %q, want %q", kv.Value, "v26.2.0-alpha.1-dev")
			}
			break
		}
	}
	if !found {
		t.Error("crdb_version key not found in parquet file metadata")
	}
}

func TestConvertStreamEmptyInput(t *testing.T) {
	var buf bytes.Buffer
	tr := &Transcoder{}
	_, err := tr.ConvertStream(context.Background(), strings.NewReader(""), &buf, "crdb-v2")
	if err != nil {
		t.Fatalf("ConvertStream on empty input: %v", err)
	}
}

func TestConvertStreamCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var buf bytes.Buffer
	tr := &Transcoder{}
	input := "I260128 07:00:20.057233 711 util/log/file_sync_buffer.go:237 ⋮ [T1,config] 1  entry\n"
	_, err := tr.ConvertStream(ctx, strings.NewReader(input), &buf, "crdb-v2")
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestConvertStreamBatching(t *testing.T) {
	var sb strings.Builder
	for i := range 50 {
		fmt.Fprintf(&sb, `{"channel_numeric":1,"timestamp":"1610833757.%09d","severity_numeric":1,"goroutine":14,"file":"server/node.go","line":464,"message":"entry %d"}`+"\n", i, i)
	}

	var buf bytes.Buffer
	tr := &Transcoder{BatchSize: 10}
	stats, err := tr.ConvertStream(context.Background(), strings.NewReader(sb.String()), &buf, "json")
	if err != nil {
		t.Fatalf("ConvertStream: %v", err)
	}
	if stats.TotalEntries != 50 {
		t.Errorf("TotalEntries = %d, want 50", stats.TotalEntries)
	}
	verifyParquetBytes(t, buf.Bytes(), 50)
}

func verifyParquetBytes(t *testing.T, data []byte, expectedRows int) {
	t.Helper()
	if len(data) == 0 {
		if expectedRows == 0 {
			return
		}
		t.Fatal("parquet output is empty")
	}
	pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("opening parquet data: %v", err)
	}
	totalRows := 0
	for _, rg := range pf.RowGroups() {
		totalRows += int(rg.NumRows())
	}
	if totalRows != expectedRows {
		t.Errorf("parquet rows: got %d, want %d", totalRows, expectedRows)
	}
}

func TestConvertZIPWithRealBundle(t *testing.T) {
	// Integration test using the actual debug bundle if present.
	bundlePath := "fresh-text.zip"
	if _, err := os.Stat(bundlePath); os.IsNotExist(err) {
		t.Skip("skipping integration test: fresh-text.zip not found")
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "parquet.zip")

	tr := &Transcoder{}
	stats, err := tr.ConvertZIP(context.Background(), bundlePath, outputPath)
	if err != nil {
		t.Fatalf("ConvertZIP failed: %v", err)
	}

	if stats.FilesProcessed == 0 {
		t.Error("expected at least one file to be processed")
	}
	t.Logf("processed %d files, %d entries, %d malformed, %d errors",
		stats.FilesProcessed, stats.TotalEntries, stats.MalformedLines, len(stats.Errors))
}

func createTestZip(t *testing.T, path string, files map[string]string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	w := zip.NewWriter(f)
	for name, content := range files {
		fw, err := w.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(fw, strings.NewReader(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func verifyParquetZip(t *testing.T, zipPath, expectedEntry string, expectedRows int) {
	t.Helper()

	r, err := zip.OpenReader(zipPath)
	if err != nil {
		t.Fatalf("opening output zip: %v", err)
	}
	defer r.Close()

	var found *zip.File
	for _, f := range r.File {
		if f.Name == expectedEntry {
			found = f
			break
		}
	}
	if found == nil {
		var names []string
		for _, f := range r.File {
			names = append(names, f.Name)
		}
		t.Fatalf("expected entry %q not found in zip; entries: %v", expectedEntry, names)
	}

	// Read the parquet data into memory for verification.
	rc, err := found.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}

	pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("opening parquet data: %v", err)
	}

	totalRows := 0
	for _, rg := range pf.RowGroups() {
		totalRows += int(rg.NumRows())
	}
	if totalRows != expectedRows {
		t.Errorf("parquet rows: got %d, want %d", totalRows, expectedRows)
	}
}
