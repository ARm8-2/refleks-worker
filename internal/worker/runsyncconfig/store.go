package runsyncconfig

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Defaults returns the default key-value pairs for run_sync_config.
func Defaults() map[string]bool {
	return map[string]bool{
		"sync_enabled":                true,
		"store_runs_enabled":          true,
		"store_anon_only":             false,
		"store_with_mouse_trace_only": false,
	}
}

// Store reads and writes run sync configuration from the database.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore creates a run sync config store.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Seed inserts default settings for any keys not already in the table.
// Existing rows are left untouched so manual DB edits are preserved.
func (s *Store) Seed(ctx context.Context, defaults map[string]bool) error {
	for key, value := range defaults {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO run_sync_config (key, value)
			VALUES ($1, $2)
			ON CONFLICT (key) DO NOTHING
		`, key, value)
		if err != nil {
			return fmt.Errorf("seed run sync config %q: %w", key, err)
		}
	}
	return nil
}
