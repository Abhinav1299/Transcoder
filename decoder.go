package transcoder

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"regexp"
)

// EntryDecoder decodes log entries one at a time from an underlying reader.
// Each call to Decode fills the provided LogEntry. It returns io.EOF when
// no more entries are available. This interface mirrors the EntryDecoder
// in cockroachdb/cockroach/pkg/util/log.
type EntryDecoder interface {
	Decode(entry *LogEntry) error
}

// Format detection regexes ported from cockroachdb/cockroach pkg/util/log/log_decoder.go.
var (
	formatRE = regexp.MustCompile(
		`(?m)^` +
			`(?:.*config\][ ]+log format \(utf8=.+\): )` +
			`(.*)$`,
	)
	v2IndicatorRE = regexp.MustCompile(
		`(?m)^` +
			`(?:.*line format: \[IWEF\]yymmdd hh:mm:ss.uuuuuu goid \[chan@\]file:line.*)$`,
	)
	v1IndicatorRE = regexp.MustCompile(
		`(?m)^` +
			`(?:.*line format: \[IWEF\]yymmdd hh:mm:ss.uuuuuu goid (?:\[chan@\])?file:line.*)$`,
	)
	jsonIndicatorRE = regexp.MustCompile(
		`(?m)^` + `(?:.*"config".+log format \(utf8=.+\): )json".+$`)
	jsonCompactIndicatorRE = regexp.MustCompile(
		`(?m)^` + `(?:.*"config".+log format \(utf8=.+\): )json-compact".+$`)
	jsonFluentIndicatorRE = regexp.MustCompile(
		`(?m)^` + `(?:.*"config".+log format \(utf8=.+\): )json-fluent".+$`)
	jsonFluentCompactIndicatorRE = regexp.MustCompile(
		`(?m)^` + `(?:.*"config".+log format \(utf8=.+\): )json-fluent-compact".+$`)

	// binaryVersionRE extracts the CockroachDB version from the "binary:" header line
	// present at the top of every log file. Example line (after debug-zip transcoding):
	//   I260302 12:17:37.955400 1 ... ⋮ [T1,config] binary: CockroachDB CCL v26.2.0-alpha.1-dev (darwin arm64, built , go1.25.5)
	binaryVersionRE = regexp.MustCompile(`(?m)binary: CockroachDB.*? (v\d+\.\d+\.\d+\S*)`)
)

// FormatParsers maps user-facing format names to the canonical internal names
// used by NewEntryDecoderWithFormat. Ported from cockroachdb/cockroach
// pkg/util/log/formats.go.
var FormatParsers = map[string]string{
	"crdb-v1":              "v1",
	"crdb-v1-count":        "v1",
	"crdb-v1-tty":          "v1",
	"crdb-v1-tty-count":    "v1",
	"crdb-v2":              "v2",
	"crdb-v2-tty":          "v2",
	"json":                 "json",
	"json-compact":         "json-compact",
	"json-fluent":          "json",
	"json-fluent-compact":  "json-compact",
}

var (
	ErrUnknownFormat    = errors.New("unknown log file format")
	ErrEmptyLogFile     = errors.New("cannot read format from empty log file")
	ErrMalformedEntry   = errors.New("malformed log entry")
)

// ExtractCRDBVersion extracts the CockroachDB version string from log file
// header bytes. Every CockroachDB log file (all formats) includes a "binary:"
// header line, e.g.:
//
//	binary: CockroachDB CCL v26.2.0-alpha.1-dev (darwin arm64, built , go1.25.5)
//
// Returns the version (e.g. "v26.2.0-alpha.1-dev") or "" if not found.
func ExtractCRDBVersion(data []byte) string {
	if m := binaryVersionRE.FindSubmatch(data); m != nil {
		return string(m[1])
	}
	return ""
}

// NewEntryDecoder creates an EntryDecoder by auto-detecting the log format
// from the file header. If detection fails (e.g. no header present), it falls
// back to crdb-v2. This mirrors log.NewEntryDecoder in the cockroach repo.
func NewEntryDecoder(in io.Reader) (EntryDecoder, error) {
	dec, _, err := NewEntryDecoderWithFormat(in, "")
	return dec, err
}

// NewEntryDecoderWithFormat creates an EntryDecoder for the given format.
// When format is empty, it is auto-detected from the log file header.
//
// The returned crdbVersion is the CockroachDB version extracted from the
// "binary:" header line (e.g. "v26.2.0-alpha.1-dev"), or "" if not found.
// When an explicit format is provided, header reading is skipped and the
// version will be empty.
//
// Debug zips created by `cockroach debug zip` transcode all log formats into
// crdb-v1 text, but the header still declares the original format (e.g. "json").
// When the declared format is JSON but the actual content is text, we fall back
// to the v1 decoder.
func NewEntryDecoderWithFormat(in io.Reader, format string) (EntryDecoder, string, error) {
	var headerData []byte
	if format == "" {
		read, detectedFormat, err := ReadFormatFromLogFile(in)
		if err != nil {
			if err == io.EOF {
				return nil, "", ErrEmptyLogFile
			}
			format = "crdb-v2"
			if read != nil {
				if buf, ok := read.(*bytes.Buffer); ok {
					headerData = buf.Bytes()
				}
				in = io.MultiReader(read, in)
			}
		} else {
			format = detectedFormat
			if buf, ok := read.(*bytes.Buffer); ok {
				headerData = buf.Bytes()
			}
			in = io.MultiReader(read, in)
		}
	}

	crdbVersion := ExtractCRDBVersion(headerData)

	canonical, ok := FormatParsers[format]
	if !ok {
		return nil, crdbVersion, fmt.Errorf("%w: %s", ErrUnknownFormat, format)
	}

	// Debug zips transcode JSON logs into crdb-v1 text. Detect this by
	// checking whether the first non-header line starts with '{'.
	if (canonical == "json" || canonical == "json-compact") && len(headerData) > 0 {
		if !looksLikeJSON(headerData) {
			canonical = "v1"
		}
	}

	switch canonical {
	case "v2":
		return newDecoderV2(in), crdbVersion, nil
	case "v1":
		return newDecoderV1(in), crdbVersion, nil
	case "json":
		return newDecoderJSON(in, false), crdbVersion, nil
	case "json-compact":
		return newDecoderJSON(in, true), crdbVersion, nil
	default:
		return nil, crdbVersion, fmt.Errorf("%w: %s", ErrUnknownFormat, format)
	}
}

// looksLikeJSON checks whether the header data contains any line starting
// with '{', indicating actual JSON content vs text-transcoded debug zip logs.
func looksLikeJSON(data []byte) bool {
	for _, line := range bytes.Split(data, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) > 0 && line[0] == '{' {
			return true
		}
	}
	return false
}

// ReadFormatFromLogFile reads up to 32 KiB of header data to detect the
// log format. It returns a reader containing the consumed bytes so the
// caller can prepend them back to the stream. When format detection fails
// (but bytes were read), it returns the buffered reader with an error so
// callers can fall back gracefully.
func ReadFormatFromLogFile(in io.Reader) (read io.Reader, format string, err error) {
	var buf bytes.Buffer
	rest := bufio.NewReader(in)
	r := io.TeeReader(rest, &buf)
	const headerBytes = 4 * 8192
	header := make([]byte, headerBytes)
	n, err := io.ReadFull(r, header)
	if err != nil && err != io.ErrUnexpectedEOF && (n == 0 || err != io.EOF) {
		return nil, "", err
	}
	header = header[:n]
	format, err = getLogFormat(header)
	if err != nil {
		return &buf, "", fmt.Errorf("decoding format: %w", err)
	}
	return &buf, format, nil
}

// getLogFormat detects the log format from the header bytes.
func getLogFormat(data []byte) (string, error) {
	if m := formatRE.FindSubmatch(data); m != nil {
		return string(m[1]), nil
	}

	if v1IndicatorRE.Match(data) {
		return "crdb-v1", nil
	}
	if v2IndicatorRE.Match(data) {
		return "crdb-v2", nil
	}
	if jsonIndicatorRE.Match(data) {
		return "json", nil
	}
	if jsonCompactIndicatorRE.Match(data) {
		return "json-compact", nil
	}
	if jsonFluentIndicatorRE.Match(data) {
		return "json-fluent", nil
	}
	if jsonFluentCompactIndicatorRE.Match(data) {
		return "json-fluent-compact", nil
	}
	return "", fmt.Errorf("failed to extract log file format from the log")
}
