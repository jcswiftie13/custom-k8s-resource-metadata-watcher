package history

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ColumnHash returns a stable content hash of a row's declared column values —
// the dedup key the ingester compares to decide whether an event opens a new
// version. Only declared columns are hashed (constants are included but never
// change, so they do not affect the comparison); envelope identity is not, since
// it is fixed for a given uid.
//
// The hash is computed in memory and never persisted: it is recomputed from the
// declared column values, so cross-process stability is not required, only
// determinism within a process and equality between a freshly built row and the
// same row read back from ClickHouse on restart (see the millisecond/empty-slice
// normalization in ingest buildRow / store OpenVersions).
//
// Values are serialized with a per-type canonical encoding rather than
// json.Marshal so time.Time is written at millisecond precision (matching
// DateTime64(3) storage) instead of json's nanosecond RFC3339.
func ColumnHash(values map[string]any) (string, error) {
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('\x00')
		if err := encodeValue(&b, values[k]); err != nil {
			return "", fmt.Errorf("hash column %q: %w", k, err)
		}
		b.WriteByte('\x1e') // record separator between columns
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:]), nil
}

// encodeValue writes a canonical, type-tagged encoding of one column value. The
// type tag prevents cross-type collisions (e.g. int64(1) vs "1"), and slice
// elements are unit-separated so ["a","b"] and ["ab"] never collide.
func encodeValue(b *strings.Builder, v any) error {
	switch x := v.(type) {
	case string:
		b.WriteString("s:")
		b.WriteString(x)
	case []string:
		b.WriteString("a:")
		for i, e := range x {
			if i > 0 {
				b.WriteByte('\x1f') // unit separator between elements
			}
			b.WriteString(e)
		}
	case int64:
		b.WriteString("i:")
		b.WriteString(strconv.FormatInt(x, 10))
	case uint64:
		b.WriteString("u:")
		b.WriteString(strconv.FormatUint(x, 10))
	case float64:
		b.WriteString("f:")
		b.WriteString(strconv.FormatFloat(x, 'g', -1, 64))
	case bool:
		b.WriteString("b:")
		b.WriteString(strconv.FormatBool(x))
	case time.Time:
		b.WriteString("t:")
		b.WriteString(x.UTC().Format("2006-01-02 15:04:05.000"))
	default:
		return fmt.Errorf("unsupported value type %T", v)
	}
	return nil
}
