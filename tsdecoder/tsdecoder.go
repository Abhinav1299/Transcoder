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
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/parquet-go/parquet-go"
	"google.golang.org/protobuf/proto"
)

// File-level Parquet key-value metadata keys injected by Convert. These are
// written to the Parquet footer and can be read without scanning any data
// pages — handy for tools like DuckDB (parquet_kv_metadata) and Grafana which
// want the covered time window before deciding to query the file at all.
const (
	MetaKeyMinTimestampNanos = "tsdecoder.min_timestamp_nanos"
	MetaKeyMaxTimestampNanos = "tsdecoder.max_timestamp_nanos"
	MetaKeyMinTimestamp      = "tsdecoder.min_timestamp"
	MetaKeyMaxTimestamp      = "tsdecoder.max_timestamp"
	MetaKeyRowCount          = "tsdecoder.row_count"
)

// Stats holds conversion metrics returned by Convert.
type Stats struct {
	// RowsWritten is the total number of Parquet rows written.
	RowsWritten int
	// RecordsSkipped is the number of gob records that were skipped due
	// to any decoding error. It equals the sum of the SkipReasons fields.
	RecordsSkipped int
	// SkipReasons breaks down RecordsSkipped by cause so callers can
	// distinguish a stream with a few legitimately broken keys from a
	// stream with systemic corruption.
	SkipReasons SkipReasons
	// TruncatedStream is set when the gob stream ended in the middle of a
	// record (io.ErrUnexpectedEOF). Everything decoded up to that point is
	// still written; the caller can decide how to treat partial inputs.
	TruncatedStream bool
	// MinTimestamp is the earliest sample timestamp present in the output,
	// in nanoseconds since the Unix epoch. Zero when RowsWritten is 0.
	MinTimestamp int64
	// MaxTimestamp is the latest sample timestamp present in the output,
	// in nanoseconds since the Unix epoch. Zero when RowsWritten is 0.
	MaxTimestamp int64
}

// SkipReasons counts gob records skipped during Convert, grouped by the cause
// that prevented them from being written to Parquet.
type SkipReasons struct {
	// BadKey counts records whose binary key could not be parsed
	// (missing prefix, malformed bytes-encoding, bad varint, etc.).
	BadKey int
	// ShortValue counts records whose Value.RawBytes was shorter than the
	// 5-byte CRDB value header.
	ShortValue int
	// ProtoUnmarshal counts records whose protobuf payload failed to parse
	// as an InternalTimeSeriesData.
	ProtoUnmarshal int
	// InvalidSampleData counts records whose payload parsed but was
	// internally inconsistent: Offset/Last slice length mismatch, a
	// non-positive sample duration, or similar structural errors.
	InvalidSampleData int
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

	// Inject file-level metadata so downstream tools (Grafana, DuckDB,
	// pandas, spark) can discover the covered time window directly from
	// the Parquet footer without scanning any data pages. Time-range keys
	// are only emitted when we actually wrote rows; emitting min/max=0 for
	// an empty file would misrepresent the data as covering the Unix epoch.
	writer.SetKeyValueMetadata(MetaKeyRowCount, strconv.Itoa(stats.RowsWritten))
	if stats.RowsWritten > 0 {
		writer.SetKeyValueMetadata(MetaKeyMinTimestampNanos, strconv.FormatInt(stats.MinTimestamp, 10))
		writer.SetKeyValueMetadata(MetaKeyMaxTimestampNanos, strconv.FormatInt(stats.MaxTimestamp, 10))
		writer.SetKeyValueMetadata(MetaKeyMinTimestamp, time.Unix(0, stats.MinTimestamp).UTC().Format(time.RFC3339Nano))
		writer.SetKeyValueMetadata(MetaKeyMaxTimestamp, time.Unix(0, stats.MaxTimestamp).UTC().Format(time.RFC3339Nano))
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

	// Track the covered time window across every row that makes it into
	// the output. We can't use stats.MinTimestamp == 0 as a "not yet set"
	// sentinel because zero is a legitimate (if unusual) timestamp value,
	// so we carry an explicit flag.
	var minTs, maxTs int64
	var haveTs bool

	var kv KeyValue
	for {
		if err := dec.Decode(&kv); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// Real-world tsdumps can end mid-record (truncated export or a
			// trailing footer we don't recognize). Preserve everything we
			// decoded so far and surface the truncation via Stats.
			if errors.Is(err, io.ErrUnexpectedEOF) {
				stats.TruncatedStream = true
				break
			}
			return stats, fmt.Errorf("gob decode: %w", err)
		}

		name, source, _, err := DecodeDataKey(kv.Key)
		if err != nil {
			stats.SkipReasons.BadKey++
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

		oldLen := len(batch)
		newBatch, reason, err := decodeSamples(batch, &kv, name, source)
		if err != nil {
			switch reason {
			case skipReasonShortValue:
				stats.SkipReasons.ShortValue++
			case skipReasonProtoUnmarshal:
				stats.SkipReasons.ProtoUnmarshal++
			case skipReasonInvalidSampleData:
				stats.SkipReasons.InvalidSampleData++
			}
			stats.RecordsSkipped++
			continue
		}
		batch = newBatch

		// Fold the newly-appended samples' timestamps into the running
		// min/max. Doing it here (rather than inside decodeSamples) keeps
		// decodeSamples focused on payload parsing and keeps the time-
		// range bookkeeping alongside other per-batch state.
		for _, row := range batch[oldLen:] {
			if !haveTs {
				minTs, maxTs = row.Timestamp, row.Timestamp
				haveTs = true
				continue
			}
			if row.Timestamp < minTs {
				minTs = row.Timestamp
			}
			if row.Timestamp > maxTs {
				maxTs = row.Timestamp
			}
		}
	}

	if len(batch) > 0 {
		if err := flushBatch(writer, batch); err != nil {
			return stats, err
		}
		stats.RowsWritten += len(batch)
	}
	if haveTs {
		stats.MinTimestamp = minTs
		stats.MaxTimestamp = maxTs
	}
	return stats, nil
}

// skipReason identifies why a record was skipped by decodeSamples. Only
// decodeSamples itself reports these; key decode errors are classified by the
// caller.
type skipReason int

const (
	skipReasonNone skipReason = iota
	skipReasonShortValue
	skipReasonProtoUnmarshal
	skipReasonInvalidSampleData
)

// decodeSamples unmarshals the protobuf payload from a KeyValue and appends
// the expanded time-series samples to batch. CockroachDB's internal time-series
// data uses two layouts: a legacy row-based format (Samples field) and a newer
// columnar format (Offset/Last fields).
//
// decodeSamples returns the (possibly appended) batch, a reason code when the
// record was rejected, and a non-nil error in the rejection case. On success
// reason is skipReasonNone and err is nil. decodeSamples never panics on
// adversarial input; malformed payloads are reported via (reason, err).
func decodeSamples(batch []Row, kv *KeyValue, name, source string) ([]Row, skipReason, error) {
	protoBytes, err := ExtractProto(kv.Value.RawBytes)
	if err != nil {
		return batch, skipReasonShortValue, fmt.Errorf("extract proto: %w", err)
	}

	var idata InternalTimeSeriesData
	if err := proto.Unmarshal(protoBytes, &idata); err != nil {
		return batch, skipReasonProtoUnmarshal, fmt.Errorf("proto unmarshal: %w", err)
	}

	isColumnar := len(idata.Offset) > 0

	// A CRDB sample duration of 0 would collapse every sample in the slab
	// onto the same timestamp, producing silently duplicate rows. Reject.
	if isColumnar || len(idata.Samples) > 0 {
		if idata.SampleDurationNanos <= 0 {
			return batch, skipReasonInvalidSampleData, fmt.Errorf(
				"invalid sample_duration_nanos: %d", idata.SampleDurationNanos)
		}
	}

	if isColumnar {
		// Offset and Last must have the same length; anything else means
		// the payload is corrupt. Indexing Last[i] without this check
		// previously crashed the decoder on truncated wire data.
		if len(idata.Last) != len(idata.Offset) {
			return batch, skipReasonInvalidSampleData, fmt.Errorf(
				"columnar length mismatch: len(offset)=%d len(last)=%d",
				len(idata.Offset), len(idata.Last))
		}
		for i, off := range idata.Offset {
			ts := idata.StartTimestampNanos + int64(off)*idata.SampleDurationNanos
			batch = append(batch, Row{
				Name:      name,
				Source:    source,
				Timestamp: ts,
				Value:     idata.Last[i],
			})
		}
		return batch, skipReasonNone, nil
	}

	for _, s := range idata.Samples {
		// proto.Unmarshal allocates a non-nil entry for each repeated
		// message, but guard defensively: future proto runtimes or
		// direct-constructed payloads might not.
		if s == nil {
			continue
		}
		ts := idata.StartTimestampNanos + int64(s.Offset)*idata.SampleDurationNanos
		batch = append(batch, Row{
			Name:      name,
			Source:    source,
			Timestamp: ts,
			Value:     s.Sum,
		})
	}
	return batch, skipReasonNone, nil
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
