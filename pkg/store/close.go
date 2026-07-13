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
// sentinel — the ingester's restart-recovery input. Rows are returned raw
// (ordered by uid, valid_from, ingest_seq); the caller collapses duplicates
// (same uid+valid_from, higher ingest_seq wins) and decides which open rows
// are stale.
func (s *chStore) OpenVersions(ctx context.Context, table string) ([]OpenVersion, error) {
	if _, ok := s.schemas[table]; !ok {
		return nil, fmt.Errorf("unknown table %q (EnsureSchema not called for it)", table)
	}
	rows, err := s.conn.Query(ctx, fmt.Sprintf(
		`SELECT namespace, name, uid, spec_hash, valid_from, ingest_seq
		 FROM %s WHERE valid_to = %s
		 ORDER BY uid, valid_from, ingest_seq`, table, dt64Lit(FarFuture)))
	if err != nil {
		return nil, fmt.Errorf("open versions %s: %w", table, err)
	}
	defer rows.Close()
	var out []OpenVersion
	for rows.Next() {
		var v OpenVersion
		if err := rows.Scan(&v.Namespace, &v.Name, &v.UID, &v.SpecHash, &v.ValidFrom, &v.IngestSeq); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
