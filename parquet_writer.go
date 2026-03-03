package transcoder

import (
	"io"

	"github.com/parquet-go/parquet-go"
)

// ParquetWriter serialises LogEntry records into Parquet format using Zstd
// compression with row groups of up to 10,000 rows. It wraps parquet-go's
// GenericWriter and exposes a minimal API suited to the transcoder's streaming
// write pattern.
type ParquetWriter struct {
	writer *parquet.GenericWriter[LogEntry]
}

// NewParquetWriter returns a writer that encodes LogEntry records as Parquet
// to the supplied io.Writer. The caller must call Close to flush buffered
// rows and finalise the file footer.
func NewParquetWriter(w io.Writer) *ParquetWriter {
	pw := parquet.NewGenericWriter[LogEntry](w,
		parquet.Compression(&parquet.Zstd),
		parquet.MaxRowsPerRowGroup(10_000),
	)
	return &ParquetWriter{writer: pw}
}

// WriteEntries appends a batch of log entries to the Parquet output.
func (pw *ParquetWriter) WriteEntries(entries []LogEntry) error {
	_, err := pw.writer.Write(entries)
	return err
}

// Close flushes any buffered rows and writes the Parquet file footer.
func (pw *ParquetWriter) Close() error {
	return pw.writer.Close()
}
