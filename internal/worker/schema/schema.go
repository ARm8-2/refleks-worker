package schema

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const migrationDonePath = "/tmp/migration-done"

// Ensure bootstraps all tables needed by the worker.
func Ensure(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return fmt.Errorf("database pool is required")
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin schema transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	statements := []string{
		`CREATE TABLE IF NOT EXISTS accounts (
			id BIGSERIAL PRIMARY KEY,
			steam_id TEXT NOT NULL UNIQUE,
			steam_username TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS scenarios (
			id BIGSERIAL PRIMARY KEY,
			scenario_name TEXT NOT NULL UNIQUE,
			median_score DOUBLE PRECISION,
			stddev_score DOUBLE PRECISION,
			p95_score DOUBLE PRECISION,
			run_count BIGINT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS runs (
			id BIGSERIAL PRIMARY KEY,
			account_id BIGINT REFERENCES accounts(id) ON DELETE SET NULL,
			scenario_id BIGINT NOT NULL REFERENCES scenarios(id) ON DELETE RESTRICT,
			hash CHAR(64) NOT NULL,
			file_name TEXT NOT NULL,
			epoch_milli BIGINT NOT NULL,
			size_bytes BIGINT NOT NULL,
			object_key TEXT NOT NULL,
			format_version SMALLINT NOT NULL DEFAULT 1,
			score DOUBLE PRECISION,
			accuracy DOUBLE PRECISION,
			avg_ttk_seconds DOUBLE PRECISION,
			duration_seconds DOUBLE PRECISION,
			sens_cm360 DOUBLE PRECISION,
			has_mouse_trace BOOLEAN NOT NULL DEFAULT FALSE,
			avg_mouse_speed DOUBLE PRECISION,
			mouse_vid TEXT,
			mouse_pid TEXT,
			uploaded_at TIMESTAMPTZ NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE (hash),
			UNIQUE (object_key)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_runs_uploaded_at ON runs (uploaded_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_runs_epoch ON runs (epoch_milli DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_runs_scenario_id ON runs (scenario_id)`,
		`CREATE INDEX IF NOT EXISTS idx_runs_account_id ON runs (account_id)`,
		`ALTER TABLE runs ADD COLUMN IF NOT EXISTS format_version SMALLINT NOT NULL DEFAULT 1`,
		`ALTER TABLE runs DROP COLUMN IF EXISTS compression`,

		`CREATE TABLE IF NOT EXISTS worker_job_runs (
			id BIGSERIAL PRIMARY KEY,
			job_name TEXT NOT NULL,
			started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			finished_at TIMESTAMPTZ,
			status TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed', 'skipped')),
			message TEXT,
			details JSONB NOT NULL DEFAULT '{}'::jsonb,
			duration_ms BIGINT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_worker_job_runs_job_started ON worker_job_runs (job_name, started_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_worker_job_runs_status_started ON worker_job_runs (status, started_at DESC)`,

		`CREATE TABLE IF NOT EXISTS worker_job_state (
			job_name TEXT NOT NULL,
			state_key TEXT NOT NULL,
			state_value JSONB NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (job_name, state_key)
		)`,

		`CREATE TABLE IF NOT EXISTS benchmarks (
			id BIGSERIAL PRIMARY KEY,
			benchmark_name TEXT NOT NULL UNIQUE,
			abbreviation TEXT NOT NULL DEFAULT '',
			rank_calculation TEXT NOT NULL DEFAULT '',
			color TEXT NOT NULL DEFAULT '',
			spreadsheet_url TEXT NOT NULL DEFAULT '',
			date_added DATE,
			source_file TEXT NOT NULL DEFAULT '',
			source_hash CHAR(64) NOT NULL DEFAULT '',
			is_active BOOLEAN NOT NULL DEFAULT TRUE,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_benchmarks_active_name ON benchmarks (is_active, benchmark_name)`,

		`CREATE TABLE IF NOT EXISTS benchmark_difficulties (
			id BIGSERIAL PRIMARY KEY,
			benchmark_id BIGINT NOT NULL REFERENCES benchmarks(id) ON DELETE CASCADE,
			difficulty_name TEXT NOT NULL,
			kovaaks_benchmark_id BIGINT NOT NULL UNIQUE,
			sharecode TEXT NOT NULL DEFAULT '',
			rank_colors JSONB NOT NULL DEFAULT '{}'::jsonb,
			sort_order INT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE (benchmark_id, difficulty_name)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_benchmark_difficulties_benchmark_order ON benchmark_difficulties (benchmark_id, sort_order)`,

		`CREATE TABLE IF NOT EXISTS benchmark_categories (
			id BIGSERIAL PRIMARY KEY,
			difficulty_id BIGINT NOT NULL REFERENCES benchmark_difficulties(id) ON DELETE CASCADE,
			category_name TEXT NOT NULL,
			color TEXT NOT NULL DEFAULT '',
			sort_order INT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE (difficulty_id, category_name)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_benchmark_categories_difficulty_order ON benchmark_categories (difficulty_id, sort_order)`,

		`CREATE TABLE IF NOT EXISTS benchmark_subcategories (
			id BIGSERIAL PRIMARY KEY,
			category_id BIGINT NOT NULL REFERENCES benchmark_categories(id) ON DELETE CASCADE,
			subcategory_name TEXT NOT NULL,
			scenario_count INT NOT NULL DEFAULT 0,
			color TEXT NOT NULL DEFAULT '',
			sort_order INT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE (category_id, subcategory_name)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_benchmark_subcategories_category_order ON benchmark_subcategories (category_id, sort_order)`,

		`CREATE TABLE IF NOT EXISTS benchmark_difficulty_scenarios (
			difficulty_id BIGINT NOT NULL REFERENCES benchmark_difficulties(id) ON DELETE CASCADE,
			scenario_id BIGINT NOT NULL REFERENCES scenarios(id) ON DELETE CASCADE,
			category_name TEXT NOT NULL DEFAULT '',
			subcategory_name TEXT NOT NULL DEFAULT '',
			weight DOUBLE PRECISION NOT NULL DEFAULT 1,
			sort_order INT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (difficulty_id, scenario_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_benchmark_difficulty_scenarios_scenario ON benchmark_difficulty_scenarios (scenario_id)`,

		`CREATE TABLE IF NOT EXISTS scenario_leaderboard_current (
			scenario_id BIGINT NOT NULL REFERENCES scenarios(id) ON DELETE CASCADE,
			account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
			rank INT NOT NULL,
			best_score DOUBLE PRECISION NOT NULL,
			best_epoch_milli BIGINT,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (scenario_id, account_id)
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_scenario_leaderboard_rank ON scenario_leaderboard_current (scenario_id, rank)`,
		`CREATE INDEX IF NOT EXISTS idx_scenario_leaderboard_account ON scenario_leaderboard_current (account_id)`,

		`CREATE TABLE IF NOT EXISTS benchmark_difficulty_leaderboard_current (
			difficulty_id BIGINT NOT NULL REFERENCES benchmark_difficulties(id) ON DELETE CASCADE,
			account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
			rank INT NOT NULL,
			composite_score DOUBLE PRECISION NOT NULL,
			matched_scenarios INT NOT NULL,
			last_epoch_milli BIGINT,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (difficulty_id, account_id)
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_benchmark_difficulty_leaderboard_rank ON benchmark_difficulty_leaderboard_current (difficulty_id, rank)`,
		`CREATE INDEX IF NOT EXISTS idx_benchmark_difficulty_leaderboard_account ON benchmark_difficulty_leaderboard_current (account_id)`,
	}

	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return fmt.Errorf("ensure worker schema: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit schema transaction: %w", err)
	}

	if err := os.WriteFile(migrationDonePath, []byte("ok"), 0o644); err != nil {
		return fmt.Errorf("write migration marker: %w", err)
	}

	return nil
}
