package transcoder

import (
	"archive/zip"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func buildV2Stream(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteString("I260128 07:00:20.057233 711 15@kv/kvserver/kvstorage/init.go:280 ⋮ [T1,Vsystem,n1,s1] 1  beginning range descriptor iteration\n")
	}
	return b.String()
}

func buildV1Stream(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteString("I210116 21:49:17.073282 14 1@server/node.go:464  [T1,Vsystem,n1,s1] 42 starting node\n")
	}
	return b.String()
}

func buildJSONStream(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteString(`{"channel_numeric":1,"timestamp":"1610833757.080706620","severity_numeric":1,"goroutine":14,"file":"server/node.go","line":464,"entry_counter":1,"redactable":1,"message":"hello world"}`)
		b.WriteByte('\n')
	}
	return b.String()
}

func BenchmarkDecodeV2(b *testing.B) {
	stream := buildV2Stream(1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dec := newDecoderV2(strings.NewReader(stream))
		var entry LogEntry
		for {
			if err := dec.Decode(&entry); err != nil {
				break
			}
		}
	}
}

func BenchmarkDecodeV1(b *testing.B) {
	stream := buildV1Stream(1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dec := newDecoderV1(strings.NewReader(stream))
		var entry LogEntry
		for {
			if err := dec.Decode(&entry); err != nil {
				break
			}
		}
	}
}

func BenchmarkDecodeJSON(b *testing.B) {
	stream := buildJSONStream(1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dec := newDecoderJSON(strings.NewReader(stream), false)
		var entry LogEntry
		for {
			if err := dec.Decode(&entry); err != nil {
				break
			}
		}
	}
}

func BenchmarkConvertZIP(b *testing.B) {
	tmpDir := b.TempDir()
	inputPath := filepath.Join(tmpDir, "input.zip")

	stream := buildV2Stream(10_000)
	createBenchZip(b, inputPath, map[string]string{
		"debug/nodes/1/logs/cockroach.log": stream,
	})

	outputPath := filepath.Join(tmpDir, "output.zip")
	tr := &Transcoder{}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		os.Remove(outputPath)
		if _, err := tr.ConvertZIP(context.Background(), inputPath, outputPath); err != nil {
			b.Fatal(err)
		}
	}
}

func createBenchZip(b *testing.B, path string, files map[string]string) {
	b.Helper()
	f, err := os.Create(path)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	w := zip.NewWriter(f)
	for name, content := range files {
		fw, err := w.Create(name)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(fw, strings.NewReader(content)); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
}
