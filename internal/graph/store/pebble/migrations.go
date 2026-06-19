package pebblestore

import (
	"context"
	"errors"
	"strconv"

	cpebble "github.com/cockroachdb/pebble"

	"github.com/spaceqraft/vitaledge/internal/graph"
	"github.com/spaceqraft/vitaledge/internal/graph/keyspace"
)

const currentSchemaVersion = 2

func (s *Store) runMigrations(ctx context.Context) error {
	if s == nil || s.db == nil {
		return graph.NewError(graph.ErrKindStorage, "graph store is closed", nil)
	}
	version, err := s.schemaVersion()
	if err != nil {
		return err
	}
	if version >= currentSchemaVersion {
		return nil
	}

	return s.Update(ctx, func(txn graph.Tx) error {
		t, ok := txn.(*tx)
		if !ok {
			return graph.NewError(graph.ErrKindStorage, "unexpected tx implementation for migration", nil)
		}
		tenants, err := t.collectStatsBackfillTenants()
		if err != nil {
			return err
		}
		for _, tenant := range tenants {
			if err := t.backfillTenantStats(ctx, tenant); err != nil {
				return err
			}
		}
		if err := t.setUnchecked(keyspace.SchemaVersionKey(), []byte(strconv.Itoa(currentSchemaVersion)), "write schema version"); err != nil {
			return err
		}
		return nil
	})
}

func (s *Store) schemaVersion() (int, error) {
	if s == nil || s.db == nil {
		return 0, graph.NewError(graph.ErrKindStorage, "graph store is closed", nil)
	}
	value, closer, err := s.db.Get(keyspace.SchemaVersionKey())
	if err != nil {
		if errors.Is(err, cpebble.ErrNotFound) {
			return 0, nil
		}
		return 0, graph.NewError(graph.ErrKindStorage, "read schema version", err)
	}
	defer closer.Close()

	parsed, parseErr := strconv.Atoi(string(value))
	if parseErr != nil {
		return 0, graph.NewError(graph.ErrKindStorage, "decode schema version", parseErr)
	}
	if parsed < 0 {
		return 0, nil
	}
	return parsed, nil
}
