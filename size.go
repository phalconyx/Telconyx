package telconyx

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ParseSize parses a human-readable byte size string into an int64.
// Lettered suffixes (B, K/KB, M/MB, G/GB, KiB, MiB, GiB) are all binary
// (powers of 1024): "49MB" == 49 * 1024 * 1024 bytes. This matches the
// on-disk byte count of files. For an exact decimal byte count, pass a
// bare number, e.g. ParseSize("49000000") == 49000000.
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	upper := strings.ToUpper(s)

	type suffix struct {
		text string
		mult int64
	}
	suffixes := []suffix{
		{"GIB", 1 << 30},
		{"MIB", 1 << 20},
		{"KIB", 1 << 10},
		{"GB", 1 << 30},
		{"MB", 1 << 20},
		{"KB", 1 << 10},
		{"G", 1 << 30},
		{"M", 1 << 20},
		{"K", 1 << 10},
		{"B", 1},
		{"", 1},
	}

	for _, sx := range suffixes {
		if !strings.HasSuffix(upper, sx.text) {
			continue
		}
		numStr := strings.TrimSpace(upper[:len(upper)-len(sx.text)])
		if numStr == "" {
			return 0, fmt.Errorf("missing number in %q", s)
		}
		f, err := strconv.ParseFloat(numStr, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid number %q: %w", numStr, err)
		}
		v := f * float64(sx.mult)
		if math.IsNaN(v) || v < 0 {
			return 0, fmt.Errorf("size must be a non-negative number, got %q", s)
		}
		if v >= math.MaxInt64 {
			return 0, fmt.Errorf("size %q overflows", s)
		}
		return int64(v), nil
	}
	return 0, fmt.Errorf("unrecognised size suffix in %q", s)
}
