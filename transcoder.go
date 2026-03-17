package transcoder

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachlabs/transcoder/tabledecoder"
)

// defaultBatchSize is the number of LogEntry values accumulated in memory
// before being flushed to the Parquet writer. This bounds peak memory
// usage while keeping write I/O efficient.
const defaultBatchSize = 10_000

// Stats holds conversion metrics returned by ConvertZIP and ConvertStream.
type Stats struct {
	FilesProcessed     int
	TotalEntries       int64
	MalformedLines     int64
	TableDumpsDecoded  int
	Errors             []error
}

// Transcoder converts CockroachDB log files into Parquet format. It supports
// two modes: ZIP-to-ZIP via ConvertZIP (for debug bundles) and stream-to-stream
// via ConvertStream (for real-time log ingestion by an upload server).
type Transcoder struct {
	// BatchSize controls how many entries are buffered before each write.
	// Zero uses defaultBatchSize.
	BatchSize int

	// Logger receives informational and warning messages during conversion.
	// When nil, no messages are emitted.
	Logger *slog.Logger
}

func (t *Transcoder) logInfo(msg string, args ...any) {
	if t.Logger != nil {
		t.Logger.Info(msg, args...)
	}
}

func (t *Transcoder) logWarn(msg string, args ...any) {
	if t.Logger != nil {
		t.Logger.Warn(msg, args...)
	}
}

// ConvertZIP reads a debug-bundle ZIP from inputPath, converts every .log
// file to Parquet, copies all other files as-is, and writes the result to
// outputPath. It returns aggregate Stats for the conversion run.
//
// The context controls cancellation: if ctx is cancelled, ConvertZIP returns
// the context error after completing the current file.
func (t *Transcoder) ConvertZIP(ctx context.Context, inputPath, outputPath string) (*Stats, error) {
	stats := &Stats{}

	inZip, err := zip.OpenReader(inputPath)
	if err != nil {
		return stats, fmt.Errorf("opening input zip: %w", err)
	}
	defer inZip.Close()

	outFile, err := os.Create(outputPath)
	if err != nil {
		return stats, fmt.Errorf("creating output zip: %w", err)
	}
	defer outFile.Close()

	outZip := zip.NewWriter(outFile)
	defer outZip.Close()

	for _, f := range inZip.File {
		if err := ctx.Err(); err != nil {
			return stats, err
		}

		if isLogFile(f.Name) {
			parquetName := logToParquetPath(f.Name)
			entryCount, malformed, err := t.convertSingleFile(ctx, f, outZip, parquetName)
			if err != nil {
				t.logWarn("failed to convert file", "file", f.Name, "error", err)
				stats.Errors = append(stats.Errors, fmt.Errorf("%s: %w", f.Name, err))
				continue
			}
			stats.FilesProcessed++
			stats.TotalEntries += entryCount
			stats.MalformedLines += malformed
			t.logInfo("converted file",
				"src", f.Name, "dst", parquetName,
				"entries", entryCount, "malformed", malformed)
			continue
		}

		if cfg := tabledecoder.LookupTable(f.Name); cfg != nil && cfg.HasDecoders() {
			if err := t.decodeTableDump(f, outZip, cfg); err != nil {
				t.logWarn("failed to decode table dump", "file", f.Name, "error", err)
				stats.Errors = append(stats.Errors, fmt.Errorf("decode %s: %w", f.Name, err))
				if cpErr := copyZipEntry(f, outZip); cpErr != nil {
					t.logWarn("fallback copy also failed", "file", f.Name, "error", cpErr)
				}
				continue
			}
			stats.TableDumpsDecoded++
			t.logInfo("decoded table dump", "file", f.Name)
			continue
		}

		if err := copyZipEntry(f, outZip); err != nil {
			t.logWarn("failed to copy file", "file", f.Name, "error", err)
			stats.Errors = append(stats.Errors, fmt.Errorf("copy %s: %w", f.Name, err))
		}
	}

	return stats, nil
}

// ConvertStream reads text log entries from r, converts them to Parquet, and
// writes the Parquet output to w. The format parameter specifies the log format
// (e.g. "crdb-v2", "crdb-v1", "json"); an empty string triggers auto-detection
// from the stream header.
//
// When the log file header contains a CockroachDB version (the "binary:" line),
// it is extracted and stored as "crdb_version" key-value metadata in the Parquet
// file, enabling downstream consumers to identify the source CRDB version.
//
// This is the core conversion API intended for use by an upload server in a
// synchronous flow where logs are streamed directly (not wrapped in a ZIP).
// ConvertZIP delegates to this method internally for each log file.
func (t *Transcoder) ConvertStream(ctx context.Context, r io.Reader, w io.Writer, format string) (*Stats, error) {
	stats := &Stats{}

	decoder, crdbVersion, err := NewEntryDecoderWithFormat(r, format)
	if err != nil {
		return stats, fmt.Errorf("creating decoder: %w", err)
	}

	pw := NewParquetWriter(w, crdbVersion)

	batchSize := t.batchSize()
	batch := make([]LogEntry, 0, batchSize)

	var entry LogEntry
	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}

		entry = LogEntry{}
		decErr := decoder.Decode(&entry)
		if decErr == io.EOF {
			break
		}
		if decErr != nil {
			stats.MalformedLines++
			continue
		}
		if entry.Severity == SeverityUnknown && entry.Time == 0 {
			stats.MalformedLines++
			continue
		}

		batch = append(batch, entry)
		stats.TotalEntries++

		if len(batch) >= batchSize {
			if wErr := pw.WriteEntries(batch); wErr != nil {
				return stats, fmt.Errorf("writing parquet batch: %w", wErr)
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if wErr := pw.WriteEntries(batch); wErr != nil {
			return stats, fmt.Errorf("writing final parquet batch: %w", wErr)
		}
	}

	if cErr := pw.Close(); cErr != nil {
		return stats, fmt.Errorf("closing parquet writer: %w", cErr)
	}

	stats.FilesProcessed = 1
	return stats, nil
}

// DecodeTableStream reads a TSV table dump from r, decodes encoded columns
// (protobuf, UUID, region, key), and writes the decoded TSV to w. The
// tableName parameter is the table dump filename (e.g. "system.descriptor.txt")
// used to look up the decoding configuration in the registry.
//
// Returns true if the table had decoders applied, false if no decoders were
// found (in which case the data is NOT copied to w — the caller should handle
// passthrough). Returns an error if decoding fails.
//
// This is the streaming counterpart to the table decoding done in ConvertZIP,
// intended for use by an upload server where table dumps arrive as individual
// streams rather than inside a ZIP archive.
func (t *Transcoder) DecodeTableStream(r io.Reader, w io.Writer, tableName string) (decoded bool, err error) {
	cfg := tabledecoder.LookupTable(tableName)
	if cfg == nil || !cfg.HasDecoders() {
		return false, nil
	}

	if err := tabledecoder.DecodeTSV(r, w, cfg.Parsers, cfg.UseQuotedTSV); err != nil {
		return false, fmt.Errorf("decoding table %s: %w", tableName, err)
	}
	t.logInfo("decoded table dump stream", "table", tableName)
	return true, nil
}

// convertSingleFile parses one .log ZIP entry and writes its Parquet
// equivalent into the output ZIP. It delegates to ConvertStream for the
// actual decode-and-write logic.
func (t *Transcoder) convertSingleFile(
	ctx context.Context, zf *zip.File, outZip *zip.Writer, parquetName string,
) (entryCount int64, malformed int64, err error) {
	rc, err := zf.Open()
	if err != nil {
		return 0, 0, fmt.Errorf("opening log file: %w", err)
	}
	defer rc.Close()

	w, err := outZip.Create(parquetName)
	if err != nil {
		return 0, 0, fmt.Errorf("creating parquet entry in zip: %w", err)
	}

	stats, err := t.ConvertStream(ctx, rc, w, "")
	if err != nil {
		return stats.TotalEntries, stats.MalformedLines, err
	}
	return stats.TotalEntries, stats.MalformedLines, nil
}

// decodeTableDump reads a TSV table dump from the input ZIP, decodes encoded
// columns using the provided configuration, and writes the decoded TSV into
// the output ZIP under the same path.
func (t *Transcoder) decodeTableDump(
	zf *zip.File, outZip *zip.Writer, cfg *tabledecoder.TableDumpConfig,
) error {
	rc, err := zf.Open()
	if err != nil {
		return fmt.Errorf("opening table dump: %w", err)
	}
	defer rc.Close()

	w, err := outZip.Create(zf.Name)
	if err != nil {
		return fmt.Errorf("creating output entry: %w", err)
	}

	return tabledecoder.DecodeTSV(rc, w, cfg.Parsers, cfg.UseQuotedTSV)
}

func (t *Transcoder) batchSize() int {
	if t.BatchSize > 0 {
		return t.BatchSize
	}
	return defaultBatchSize
}

// copyZipEntry transfers a non-log file from the input ZIP to the output ZIP.
// It uses raw copy to avoid decompressing and recompressing the data.
func copyZipEntry(f *zip.File, outZip *zip.Writer) error {
	if f.FileInfo().IsDir() {
		header := f.FileHeader
		_, err := outZip.CreateHeader(&header)
		return err
	}

	rc, err := f.OpenRaw()
	if err != nil {
		return fmt.Errorf("opening raw: %w", err)
	}

	header := f.FileHeader
	w, err := outZip.CreateRaw(&header)
	if err != nil {
		return fmt.Errorf("creating raw entry: %w", err)
	}

	if _, err := io.Copy(w, rc); err != nil {
		return fmt.Errorf("copying raw data: %w", err)
	}
	return nil
}

// isLogFile returns true for files that should be parsed as log files.
func isLogFile(name string) bool {
	return strings.HasSuffix(name, ".log") && !strings.HasPrefix(filepath.Base(name), ".")
}

// logToParquetPath replaces a .log extension with .parquet.
func logToParquetPath(name string) string {
	return strings.TrimSuffix(name, ".log") + ".parquet"
}
