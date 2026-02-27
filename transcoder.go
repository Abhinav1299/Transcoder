package transcoder

import (
	"archive/zip"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// defaultBatchSize is the number of LogEntry values accumulated in memory
// before being flushed to the Parquet writer. This bounds peak memory
// usage while keeping write I/O efficient.
const defaultBatchSize = 10_000

// Stats holds conversion metrics returned by ConvertZIP.
type Stats struct {
	FilesProcessed int
	TotalEntries   int64
	MalformedLines int64
	Errors         []error
}

// Transcoder converts CockroachDB debug-bundle log files from a ZIP archive
// into Parquet format, producing an output ZIP with matching directory structure.
// Non-log files are copied through unchanged.
type Transcoder struct {
	// BatchSize controls how many entries are buffered before each write.
	// Zero uses defaultBatchSize.
	BatchSize int
}

// ConvertZIP reads a debug-bundle ZIP from inputPath, converts every .log
// file to Parquet, copies all other files as-is, and writes the result to
// outputPath. It returns aggregate Stats for the conversion run.
func (t *Transcoder) ConvertZIP(inputPath, outputPath string) (*Stats, error) {
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
		if !isLogFile(f.Name) {
			if err := copyZipEntry(f, outZip); err != nil {
				log.Printf("WARN: failed to copy %s: %v", f.Name, err)
				stats.Errors = append(stats.Errors, fmt.Errorf("copy %s: %w", f.Name, err))
			}
			continue
		}

		parquetName := logToParquetPath(f.Name)
		entryCount, malformed, err := t.convertSingleFile(f, outZip, parquetName)
		if err != nil {
			log.Printf("WARN: failed to convert %s: %v", f.Name, err)
			stats.Errors = append(stats.Errors, fmt.Errorf("%s: %w", f.Name, err))
			continue
		}

		stats.FilesProcessed++
		stats.TotalEntries += entryCount
		stats.MalformedLines += malformed
		log.Printf("converted %s → %s (%d entries, %d malformed)",
			f.Name, parquetName, entryCount, malformed)
	}

	return stats, nil
}

// convertSingleFile parses one .log ZIP entry and writes its Parquet
// equivalent into the output ZIP.
func (t *Transcoder) convertSingleFile(
	zf *zip.File, outZip *zip.Writer, parquetName string,
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

	pw := NewParquetWriter(w)
	parser := NewParser(rc)

	batchSize := t.batchSize()
	batch := make([]LogEntry, 0, batchSize)

	for {
		entry, pErr := parser.NextEntry()
		if pErr == io.EOF {
			break
		}
		if pErr != nil {
			malformed++
			continue
		}
		if entry.Severity == SeverityUnknown && entry.Time == 0 {
			malformed++
		}

		batch = append(batch, entry)
		entryCount++

		if len(batch) >= batchSize {
			if wErr := pw.WriteEntries(batch); wErr != nil {
				return entryCount, malformed, fmt.Errorf("writing parquet batch: %w", wErr)
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if wErr := pw.WriteEntries(batch); wErr != nil {
			return entryCount, malformed, fmt.Errorf("writing final parquet batch: %w", wErr)
		}
	}

	if cErr := pw.Close(); cErr != nil {
		return entryCount, malformed, fmt.Errorf("closing parquet writer: %w", cErr)
	}

	return entryCount, malformed, nil
}

func (t *Transcoder) batchSize() int {
	if t.BatchSize > 0 {
		return t.BatchSize
	}
	return defaultBatchSize
}

// copyZipEntry transfers a non-log file from the input ZIP to the output ZIP,
// preserving its original header metadata.
func copyZipEntry(f *zip.File, outZip *zip.Writer) error {
	rc, err := f.Open()
	if err != nil {
		return fmt.Errorf("opening: %w", err)
	}
	defer rc.Close()

	header := f.FileHeader
	w, err := outZip.CreateHeader(&header)
	if err != nil {
		return fmt.Errorf("creating entry: %w", err)
	}

	if !f.FileInfo().IsDir() {
		if _, err := io.Copy(w, rc); err != nil {
			return fmt.Errorf("copying data: %w", err)
		}
	}
	return nil
}

// isLogFile returns true for files that should be parsed as crdb-v2 logs.
func isLogFile(name string) bool {
	return strings.HasSuffix(name, ".log") && !strings.HasPrefix(filepath.Base(name), ".")
}

// logToParquetPath replaces a .log extension with .parquet.
func logToParquetPath(name string) string {
	return strings.TrimSuffix(name, ".log") + ".parquet"
}
