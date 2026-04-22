package jobconfig

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// JobConfig represents the scheduling configuration for a single worker job.
type JobConfig struct {
	JobName  string
	CronExpr string
	Enabled  bool
}

// Store reads and writes job scheduling configuration from the database.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore creates a job config store.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Seed inserts default job configs for any jobs not already in the table.
// Existing rows are left untouched so manual DB edits are preserved.
func (s *Store) Seed(ctx context.Context, defaults []JobConfig) error {
	for _, d := range defaults {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO worker_job_config (job_name, cron_expr, enabled)
			VALUES ($1, $2, $3)
			ON CONFLICT (job_name) DO NOTHING
		`, d.JobName, d.CronExpr, d.Enabled)
		if err != nil {
			return fmt.Errorf("seed job config %q: %w", d.JobName, err)
		}
	}
	return nil
}

// LoadAll returns every job config row ordered by name.
func (s *Store) LoadAll(ctx context.Context) ([]JobConfig, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT job_name, cron_expr, enabled
		FROM worker_job_config
		ORDER BY job_name
	`)
	if err != nil {
		return nil, fmt.Errorf("load job configs: %w", err)
	}
	defer rows.Close()

	var configs []JobConfig
	for rows.Next() {
		var c JobConfig
		if err := rows.Scan(&c.JobName, &c.CronExpr, &c.Enabled); err != nil {
			return nil, fmt.Errorf("scan job config: %w", err)
		}
		configs = append(configs, c)
	}
	return configs, rows.Err()
}
