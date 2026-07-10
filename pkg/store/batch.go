package store

import (
	"context"
	"fmt"
	"strings"
)

// insertChunk bounds a single PrepareBatch send so a large initial-LIST burst
// keeps memory flat.
const insertChunk = 100_000

// WriteBatch appends every row to the table in chunks. Values must contain a
// correctly-typed entry for each declared column (see Row.Values).
func (s *chStore) WriteBatch(ctx context.Context, table string, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}
	schema, ok := s.schemas[table]
	if !ok {
		return fmt.Errorf("unknown table %q (EnsureSchema not called for it)", table)
	}
	cols := allColumns(schema)
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.name
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s)", table, strings.Join(names, ", "))

	for start := 0; start < len(rows); start += insertChunk {
		end := start + insertChunk
		if end > len(rows) {
			end = len(rows)
		}
		if err := s.writeChunk(ctx, insertSQL, schema, rows[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (s *chStore) writeChunk(ctx context.Context, insertSQL string, schema TableSchema, rows []Row) error {
	batch, err := s.conn.PrepareBatch(ctx, insertSQL)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}
	for i := range rows {
		args, err := rowArgs(schema, &rows[i])
		if err != nil {
			_ = batch.Abort()
			return err
		}
		if err := batch.Append(args...); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("append row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}
	return nil
}

// rowArgs assembles positional insert args aligned with allColumns(schema):
// the envelope columns followed by the declared columns in order.
func rowArgs(schema TableSchema, r *Row) ([]any, error) {
	deleted := uint8(0)
	if r.Deleted {
		deleted = 1
	}
	args := []any{
		r.Namespace,
		r.Name,
		r.UID,
		r.ResourceVersion,
		r.ValidFrom,
		r.ValidTo,
		deleted,
		r.SpecHash,
		r.IngestSeq,
	}
	for _, c := range schema.Columns {
		v, ok := r.Values[c.Name]
		if !ok {
			return nil, fmt.Errorf("row missing value for declared column %q", c.Name)
		}
		args = append(args, v)
	}
	return args, nil
}
