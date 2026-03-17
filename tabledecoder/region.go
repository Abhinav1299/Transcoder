package tabledecoder

// DecodeRegion handles the crdb_region column. The sentinel value \x80
// represents a nil/default region. All other values pass through unchanged.
func DecodeRegion(s string) (string, error) {
	if s == `\x80` {
		return "NULL", nil
	}
	return s, nil
}
