// Package tsdecoder decodes CockroachDB tsdump gob streams into Parquet.
//
// A tsdump stream is a sequence of gob-encoded values: an optional Metadata
// header followed by KeyValue pairs. Each KeyValue contains a binary
// time-series key and a protobuf-encoded InternalTimeSeriesData payload.
// Convert reads such a stream and writes the expanded time-series samples
// as Parquet rows with schema (name, source, timestamp, value).
package tsdecoder

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"
	"google.golang.org/protobuf/proto"
)

// Stats holds conversion metrics returned by Convert.
type Stats struct {
	// RowsWritten is the total number of Parquet rows written.
	RowsWritten int
	// RecordsSkipped is the number of gob records that were skipped due
	// to key decoding or protobuf unmarshalling errors.
	RecordsSkipped int
}

// Convert reads a gob-encoded tsdump stream from r, decodes time-series
// samples, and writes them as Parquet to w.
//
// The stream may optionally begin with a Metadata entry (version info,
// store-to-node mapping). If present, it is consumed and passed to onMeta.
// If onMeta is nil, metadata is silently consumed.
//
// Records that fail key decoding or protobuf unmarshalling are skipped and
// counted in Stats.RecordsSkipped. Only fatal errors (gob stream corruption,
// parquet write failures) cause Convert to return an error.
func Convert(r io.Reader, w io.Writer, onMeta func(Metadata)) (Stats, error) {
	// Buffer the stream so we can attempt a Metadata decode without
	// requiring the caller to provide an io.ReadSeeker. If the first
	// gob value is not a Metadata, we replay the buffered bytes.
	var buf bytes.Buffer
	tee := io.TeeReader(r, &buf)
	dec := gob.NewDecoder(tee)

	var md Metadata
	if err := dec.Decode(&md); err != nil {
		// Not a Metadata header — replay everything read so far followed
		// by the rest of the stream.
		dec = gob.NewDecoder(io.MultiReader(&buf, r))
	} else if onMeta != nil {
		onMeta(md)
	}

	writer := parquet.NewGenericWriter[Row](w)
	stats, err := processRecords(dec, writer)
	if err != nil {
		return stats, err
	}
	if err := writer.Close(); err != nil {
		return stats, fmt.Errorf("parquet close: %w", err)
	}
	return stats, nil
}

// processRecords decodes gob records and writes them to parquet, producing
// one row group per metric name.
func processRecords(dec *gob.Decoder, writer *parquet.GenericWriter[Row]) (Stats, error) {
	var batch []Row
	var stats Stats
	var currentName string

	var kv KeyValue
	for {
		if err := dec.Decode(&kv); err != nil {
			if err == io.EOF {
				break
			}
			return stats, fmt.Errorf("gob decode: %w", err)
		}

		name, source, _, err := DecodeDataKey(kv.Key)
		if err != nil {
			stats.RecordsSkipped++
			continue
		}

		// Flush the current row group when the metric name changes.
		if name != currentName && len(batch) > 0 {
			if err := flushBatch(writer, batch); err != nil {
				return stats, err
			}
			stats.RowsWritten += len(batch)
			batch = batch[:0]
		}
		currentName = name

		batch, err = decodeSamples(batch, &kv, name, source)
		if err != nil {
			stats.RecordsSkipped++
			continue
		}
	}

	if len(batch) > 0 {
		if err := flushBatch(writer, batch); err != nil {
			return stats, err
		}
		stats.RowsWritten += len(batch)
	}
	return stats, nil
}

// decodeSamples unmarshals the protobuf payload from a KeyValue and appends
// the expanded time-series samples to batch. CockroachDB's internal time-series
// data uses two layouts: a legacy row-based format (Samples field) and a newer
// columnar format (Offset/Last fields).
func decodeSamples(batch []Row, kv *KeyValue, name, source string) ([]Row, error) {
	protoBytes, err := ExtractProto(kv.Value.RawBytes)
	if err != nil {
		return batch, fmt.Errorf("extract proto: %w", err)
	}

	var idata InternalTimeSeriesData
	if err := proto.Unmarshal(protoBytes, &idata); err != nil {
		return batch, fmt.Errorf("proto unmarshal: %w", err)
	}

	isColumnar := len(idata.Offset) > 0
	var sampleCount int
	if isColumnar {
		sampleCount = len(idata.Offset)
	} else {
		sampleCount = len(idata.Samples)
	}

	for i := range sampleCount {
		var ts int64
		var val float64
		if isColumnar {
			ts = idata.StartTimestampNanos + int64(idata.Offset[i])*idata.SampleDurationNanos
			val = idata.Last[i]
		} else {
			ts = idata.StartTimestampNanos + int64(idata.Samples[i].Offset)*idata.SampleDurationNanos
			val = idata.Samples[i].Sum
		}
		batch = append(batch, Row{
			Name:      name,
			Source:    source,
			Timestamp: ts,
			Value:     val,
		})
	}
	return batch, nil
}

// flushBatch writes the batch and finalizes the current row group.
func flushBatch(writer *parquet.GenericWriter[Row], batch []Row) error {
	if _, err := writer.Write(batch); err != nil {
		return fmt.Errorf("parquet write: %w", err)
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("parquet flush: %w", err)
	}
	return nil
}
