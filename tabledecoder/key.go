package tabledecoder

import "fmt"

// DecodeKey interprets a string-encoded key (hex/base64/quoted) and returns
// a quoted byte representation. Full CRDB key pretty-printing is not practical
// to reimplement standalone, so we use a simple quoted format.
func DecodeKey(s string) (string, error) {
	b, ok := interpretString(s)
	if !ok {
		return "", fmt.Errorf("failed to interpret key column value: %s", s)
	}
	return fmt.Sprintf("%q", b), nil
}
