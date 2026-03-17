package tabledecoder

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// ColumnParserFn transforms a raw TSV cell value into a decoded string.
type ColumnParserFn func(string) (string, error)

// ColumnParsers maps column header names to their decoder functions.
// A non-nil value means "apply this decoder".
// A nil value means "skip (omit) this column from output".
// Columns absent from the map are passed through unchanged.
type ColumnParsers map[string]ColumnParserFn

// DecodeTSV reads a TSV table dump from r, applies column-specific decoders,
// and writes the decoded TSV to w. Columns absent from the parsers map are
// passed through unchanged. Columns mapped to nil are omitted from output.
// If useQuotedReader is true, the quoted TSV reader is used for files with
// embedded newlines in fields (e.g. crdb_internal.create_statements.txt).
func DecodeTSV(r io.Reader, w io.Writer, parsers ColumnParsers, useQuotedReader bool) error {
	var headers []string
	var iter func(func([]string) error) error

	if useQuotedReader {
		headers, iter = makeQuotedTSVIterator(r)
	} else {
		headers, iter = makeTableIterator(r)
	}

	if len(headers) == 0 {
		return nil
	}

	type colAction struct {
		srcIdx int
		header string
		parser ColumnParserFn // nil = passthrough
		skip   bool
	}

	var outputActions []colAction
	for i, h := range headers {
		parser, inMap := parsers[h]
		if inMap && parser == nil {
			continue // skip this column entirely
		}
		outputActions = append(outputActions, colAction{
			srcIdx: i,
			header: h,
			parser: parser,
		})
	}

	bw := bufio.NewWriter(w)
	defer bw.Flush()

	outHeaders := make([]string, len(outputActions))
	for i, a := range outputActions {
		outHeaders[i] = a.header
	}
	if _, err := bw.WriteString(strings.Join(outHeaders, "\t") + "\n"); err != nil {
		return fmt.Errorf("writing headers: %w", err)
	}

	return iter(func(cols []string) error {
		if len(cols) != len(headers) {
			return fmt.Errorf("column count mismatch: headers=%d, row=%d", len(headers), len(cols))
		}

		outCols := make([]string, 0, len(outputActions))
		for _, a := range outputActions {
			val := cols[a.srcIdx]

			if val == "NULL" || a.parser == nil {
				outCols = append(outCols, val)
				continue
			}

			decoded, err := a.parser(val)
			if err != nil {
				return fmt.Errorf("decoding column %q value %q: %w", a.header, val, err)
			}
			outCols = append(outCols, decoded)
		}

		if _, err := bw.WriteString(strings.Join(outCols, "\t") + "\n"); err != nil {
			return fmt.Errorf("writing row: %w", err)
		}
		return nil
	})
}

// makeTableIterator reads the first line as headers and returns an iterator
// over subsequent rows. Each row is split on tab characters.
func makeTableIterator(f io.Reader) ([]string, func(func([]string) error) error) {
	reader := bufio.NewReader(f)

	headerLine, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		return nil, func(func([]string) error) error { return err }
	}
	headerLine = strings.TrimSuffix(headerLine, "\n")

	var headers []string
	if headerLine == "" {
		headers = []string{}
	} else {
		headers = strings.Split(headerLine, "\t")
	}

	return headers, func(fn func([]string) error) error {
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					if line != "" {
						line = strings.TrimSuffix(line, "\n")
						cols := strings.Split(line, "\t")
						if fnErr := fn(cols); fnErr != nil {
							return fnErr
						}
					}
					break
				}
				return err
			}
			line = strings.TrimSuffix(line, "\n")
			cols := strings.Split(line, "\t")
			if fnErr := fn(cols); fnErr != nil {
				return fnErr
			}
		}
		return nil
	}
}

// makeQuotedTSVIterator handles TSV files where fields may be quoted and
// contain embedded newlines (e.g. SQL CREATE statements).
func makeQuotedTSVIterator(f io.Reader) ([]string, func(func([]string) error) error) {
	reader := bufio.NewReader(f)

	_, headers, err := readTSVRecord(reader)
	if err != nil {
		if err == io.EOF {
			return []string{}, func(func([]string) error) error { return nil }
		}
		return nil, func(func([]string) error) error { return err }
	}

	if len(headers) == 0 {
		return headers, func(func([]string) error) error { return nil }
	}

	return headers, func(fn func([]string) error) error {
		for {
			_, fields, err := readTSVRecord(reader)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			if len(fields) == 0 {
				break
			}
			if fnErr := fn(fields); fnErr != nil {
				return fnErr
			}
		}
		return nil
	}
}

// readTSVRecord reads a complete TSV record, handling quoted fields that may
// contain embedded newlines and tab characters.
func readTSVRecord(reader *bufio.Reader) (string, []string, error) {
	var line strings.Builder
	var fields []string
	var currentField strings.Builder
	inQuotes := false
	hasContent := false

	for {
		b, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				if hasContent && (line.Len() > 0 || currentField.Len() > 0) {
					fields = append(fields, currentField.String())
					return line.String(), fields, nil
				}
				return "", fields, io.EOF
			}
			return "", nil, err
		}

		hasContent = true
		line.WriteByte(b)

		switch b {
		case '"':
			inQuotes = !inQuotes
			currentField.WriteByte(b)
		case '\t':
			if inQuotes {
				currentField.WriteByte(b)
			} else {
				fields = append(fields, currentField.String())
				currentField.Reset()
			}
		case '\n':
			if inQuotes {
				currentField.WriteByte(b)
			} else {
				fields = append(fields, currentField.String())
				return line.String(), fields, nil
			}
		default:
			currentField.WriteByte(b)
		}
	}
}
