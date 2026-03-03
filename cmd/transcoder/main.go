// Command transcoder converts CockroachDB debug-bundle .log files to Parquet.
//
// Usage:
//
//	transcoder -input debug-bundle.zip [-output parquet.zip]
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"

	"github.com/cockroachlabs/transcoder"
)

func main() {
	inputPath := flag.String("input", "", "path to debug bundle ZIP file")
	outputPath := flag.String("output", "parquet.zip", "output ZIP file path")
	flag.Parse()

	if *inputPath == "" {
		fmt.Fprintln(os.Stderr, "usage: transcoder -input <debug-bundle.zip> [-output parquet.zip]")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	t := &transcoder.Transcoder{
		Logger: slog.Default(),
	}
	stats, err := t.ConvertZIP(ctx, *inputPath, *outputPath)
	if err != nil {
		log.Fatalf("conversion failed: %v", err)
	}

	fmt.Printf("Done. %d files processed, %d total entries, %d malformed lines.\n",
		stats.FilesProcessed, stats.TotalEntries, stats.MalformedLines)
	if len(stats.Errors) > 0 {
		fmt.Printf("%d file-level errors occurred:\n", len(stats.Errors))
		for _, e := range stats.Errors {
			fmt.Printf("  - %v\n", e)
		}
	}
}
