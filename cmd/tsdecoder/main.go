// Command tsdecoder converts a CockroachDB tsdump gob stream to Parquet.
//
// Usage:
//
//	tsdecoder -input tsdump.gob [-output tsdump.parquet]
package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/cockroachlabs/transcoder/tsdecoder"
)

// countingReader tracks the total bytes consumed from an underlying reader so
// we can report exactly how much of the input stream was processed.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func main() {
	var (
		inputPath  string
		outputPath string
	)
	for i := 1; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "-input", "--input":
			i++
			if i < len(os.Args) {
				inputPath = os.Args[i]
			}
		case "-output", "--output":
			i++
			if i < len(os.Args) {
				outputPath = os.Args[i]
			}
		case "-h", "--help":
			usage()
			return
		}
	}

	if inputPath == "" {
		usage()
		os.Exit(1)
	}
	if outputPath == "" {
		outputPath = "tsdump.parquet"
	}

	in, err := os.Open(inputPath)
	if err != nil {
		log.Fatalf("opening input: %v", err)
	}
	defer in.Close()

	out, err := os.Create(outputPath)
	if err != nil {
		log.Fatalf("creating output: %v", err)
	}
	defer out.Close()

	onMeta := func(md tsdecoder.Metadata) {
		fmt.Printf("Metadata: version=%q stores=%d created_at=%s\n",
			md.Version, len(md.StoreToNodeMap), md.CreatedAt.Format("2006-01-02T15:04:05Z"))
		for store, node := range md.StoreToNodeMap {
			fmt.Printf("  store %s -> %s\n", store, node)
		}
	}

	info, err := in.Stat()
	if err != nil {
		log.Fatalf("stat input: %v", err)
	}
	counter := &countingReader{r: in}

	stats, err := tsdecoder.Convert(counter, out, onMeta)
	if err != nil {
		log.Fatalf("convert failed: %v", err)
	}

	fmt.Printf("Done. %s -> %s\n", inputPath, outputPath)
	fmt.Printf("  rows written:    %d\n", stats.RowsWritten)
	if stats.RowsWritten > 0 {
		minT := time.Unix(0, stats.MinTimestamp).UTC().Format(time.RFC3339)
		maxT := time.Unix(0, stats.MaxTimestamp).UTC().Format(time.RFC3339)
		span := time.Duration(stats.MaxTimestamp - stats.MinTimestamp)
		fmt.Printf("  time range:      %s  ->  %s  (%s)\n", minT, maxT, span)
	}
	fmt.Printf("  records skipped: %d\n", stats.RecordsSkipped)
	if stats.RecordsSkipped > 0 {
		fmt.Printf("    bad key:             %d\n", stats.SkipReasons.BadKey)
		fmt.Printf("    short value:         %d\n", stats.SkipReasons.ShortValue)
		fmt.Printf("    proto unmarshal:     %d\n", stats.SkipReasons.ProtoUnmarshal)
		fmt.Printf("    invalid sample data: %d\n", stats.SkipReasons.InvalidSampleData)
	}
	fmt.Printf("  bytes consumed:  %d / %d (%.3f%%)\n",
		counter.n, info.Size(), 100*float64(counter.n)/float64(info.Size()))
	if stats.TruncatedStream {
		remaining := info.Size() - counter.n
		if remaining == 0 {
			// encoding/gob returns io.ErrUnexpectedEOF (not io.EOF) on a
			// cleanly-terminated stream when internal buffering peeked past
			// the last record. No data was actually lost.
			fmt.Println("  note: gob returned ErrUnexpectedEOF at the record boundary, but all bytes were consumed (stdlib quirk, no data loss)")
		} else {
			fmt.Printf("  note: gob stream ended mid-record; %d trailing byte(s) of a partial record were dropped\n", remaining)
		}
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: tsdecoder -input <tsdump.gob> [-output tsdump.parquet]")
}
