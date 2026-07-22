package store

import (
	"context"
	"fmt"
	"time"
)

// CloseVersion patches valid_to on one still-open version via ClickHouse
// lightweight UPDATE (patch parts). The predicate pins the exact version slot —
// (namespace, name, uid, valid_from) — AND the FarFuture sentinel, so:
//
//   - a replay matches zero rows (the sentinel is gone) — idempotent, no error;
//   - closing after the successor version was inserted is safe: the successor
//     is also open (sentinel) but has a different valid_from, so it is never
//     touched. This is what lets the ingest loop flush INSERTs first and run
//     the closes after (see history.Ingester).
//
// The patch is applied on read immediately (apply_patches_on_read, default on)
// and materialized by background merges. Requires ClickHouse >= 25.8 and the
// enable_block_number_column / enable_block_offset_column table settings
// (EnsureSchema adds them when closeMode=update and createSchema is on).
//
// Time operands are rendered as explicit toDateTime64(...,3) literals, NOT `?`
// binds: clickhouse-go interpolates a time.Time parameter as second-precision
// toDateTime('...'), which (a) drops the millisecond part, and (b) SATURATES
// on the FarFuture sentinel — 2200-01-01 is beyond 32-bit DateTime's 2106
// ceiling — so a bound `valid_to = ?` never matches the open row and the
// close silently patches nothing.
func (s *chStore) CloseVersion(ctx context.Context, table, namespace, name, uid string, validFrom, closeAt time.Time) error {
	if _, ok := s.schemas[table]; !ok {
		return fmt.Errorf("unknown table %q (EnsureSchema not called for it)", table)
	}
	stmt := fmt.Sprintf(
		"UPDATE %s SET valid_to = %s WHERE namespace = ? AND name = ? AND uid = ? AND valid_from = %s AND valid_to = %s",
		table, dt64Lit(closeAt), dt64Lit(validFrom), dt64Lit(FarFuture))
	if err := s.conn.Exec(ctx, stmt, namespace, name, uid); err != nil {
		return fmt.Errorf("close version %s %s/%s uid=%s: %w", table, namespace, name, uid, err)
	}
	return nil
}

// dt64Lit renders t as a DateTime64(3) literal in UTC, truncated to the same
// millisecond precision the column stores, so equality predicates against
// stored values are exact.
func dt64Lit(t time.Time) string {
	return fmt.Sprintf("toDateTime64('%s', 3, 'UTC')", t.UTC().Format("2006-01-02 15:04:05.000"))
}

// OpenVersions returns every row of table still carrying the FarFuture
// sentinel — the ingester's restart-recovery input. Each row carries its
// declared column Values so the caller can recompute the dedup hash. Rows are
// returned raw (ordered by uid, valid_from, ingest_seq); the caller collapses
// duplicates (same uid+valid_from, higher ingest_seq wins) and decides which
// open rows are stale.
func (s *chStore) OpenVersions(ctx context.Context, table string) ([]OpenVersion, error) {
	schema, ok := s.schemas[table]
	if !ok {
		return nil, fmt.Errorf("unknown table %q (EnsureSchema not called for it)", table)
	}
	cols := "namespace, name, uid, valid_from, ingest_seq"
	for _, c := range schema.Columns {
		cols += ", " + c.Name
	}
	rows, err := s.conn.Query(ctx, fmt.Sprintf(
		`SELECT %s FROM %s WHERE valid_to = %s
		 ORDER BY uid, valid_from, ingest_seq`, cols, table, dt64Lit(FarFuture)))
	if err != nil {
		return nil, fmt.Errorf("open versions %s: %w", table, err)
	}
	defer rows.Close()
	var out []OpenVersion
	for rows.Next() {
		var v OpenVersion
		dest := []any{&v.Namespace, &v.Name, &v.UID, &v.ValidFrom, &v.IngestSeq}
		targets := make([]any, len(schema.Columns))
		for i, c := range schema.Columns {
			p, err := scanTarget(c.Type)
			if err != nil {
				return nil, fmt.Errorf("open versions %s: column %q: %w", table, c.Name, err)
			}
			targets[i] = p
			dest = append(dest, p)
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		v.Values = make(map[string]any, len(schema.Columns))
		for i, c := range schema.Columns {
			v.Values[c.Name] = derefTarget(targets[i])
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// MaxIngestSeq scans only the ingest_seq column (Delta-compressed UInt64 —
// millisecond-cheap even without an index) for the largest value above floor.
// The floor predicate lets a minmax skip index on ingest_seq, when present,
// prune every granule in the common no-clock-regression case; max() over an
// empty match set returns 0, no NULL handling needed.
func (s *chStore) MaxIngestSeq(ctx context.Context, table string, floor uint64) (uint64, error) {
	if _, ok := s.schemas[table]; !ok {
		return 0, fmt.Errorf("unknown table %q (EnsureSchema not called for it)", table)
	}
	var max uint64
	if err := s.conn.QueryRow(ctx, fmt.Sprintf(
		"SELECT max(ingest_seq) FROM %s WHERE ingest_seq > ?", table), floor).Scan(&max); err != nil {
		return 0, fmt.Errorf("max ingest_seq %s: %w", table, err)
	}
	return max, nil
}

// scanTarget allocates a pointer of the Go type clickhouse-go scans a column of
// the given ClickHouse type into. It mirrors the ingester's columnValue type
// switch so a value written by the ingest path reads back as the same Go type.
func scanTarget(chType string) (any, error) {
	switch chType {
	case "String":
		return new(string), nil
	case "Array(String)":
		return new([]string), nil
	case "Int64":
		return new(int64), nil
	case "UInt64":
		return new(uint64), nil
	case "Float64":
		return new(float64), nil
	case "Bool":
		return new(bool), nil
	case "DateTime64(3)":
		return new(time.Time), nil
	default:
		return nil, fmt.Errorf("unsupported column type %q", chType)
	}
}

// derefTarget reads the value behind a scanTarget pointer, normalizing to the
// same representation buildRow produces so the recomputed hash matches: a nil
// Array(String) becomes an empty slice, and DateTime64(3) is millisecond UTC.
func derefTarget(p any) any {
	switch x := p.(type) {
	case *string:
		return *x
	case *[]string:
		if *x == nil {
			return []string{}
		}
		return *x
	case *int64:
		return *x
	case *uint64:
		return *x
	case *float64:
		return *x
	case *bool:
		return *x
	case *time.Time:
		return x.UTC().Truncate(time.Millisecond)
	default:
		return nil
	}
}
