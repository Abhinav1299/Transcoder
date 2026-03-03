package transcoder

import (
	"archive/zip"
	"bytes"
	"context"
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
