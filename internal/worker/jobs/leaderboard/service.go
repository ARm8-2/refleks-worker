package leaderboard

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"refleks-worker/internal/worker"
)

const jobName = "leaderboard_refresh"

const refreshScenarioLeaderboardSQL = `
		INSERT INTO scenario_leaderboard_current (
			scenario_id,
			player_id,
			rank,
			best_score,
			best_played_at,
			updated_at
		)
		WITH per_player_best AS (
			SELECT DISTINCT ON (r.scenario_id, r.player_id)
				r.scenario_id,
				r.player_id,
				r.score AS best_score,
				r.played_at AS best_played_at
			FROM runs r
			WHERE r.player_id IS NOT NULL
				AND r.score IS NOT NULL
			ORDER BY
				r.scenario_id,
				r.player_id,
				r.score DESC,
				r.played_at ASC,
				r.id ASC
		), ranked AS (
			SELECT
				p.scenario_id,
				p.player_id,
				p.best_score,
				p.best_played_at,
				ROW_NUMBER() OVER (
					PARTITION BY p.scenario_id
					ORDER BY
						p.best_score DESC,
						p.best_played_at ASC,
						p.player_id ASC
				) AS rank
			FROM per_player_best p
		)
		SELECT
			r.scenario_id,
			r.player_id,
			r.rank,
			r.best_score,
			r.best_played_at,
			NOW()
		FROM ranked r
		WHERE r.rank <= $1
	`

const refreshBenchmarkLeaderboardSQL = `
		INSERT INTO benchmark_difficulty_leaderboard_current (
			difficulty_id,
			player_id,
			rank,
			composite_score,
			matched_scenarios,
			last_played_at,
			updated_at
		)
		WITH per_player_best AS (
			SELECT DISTINCT ON (r.scenario_id, r.player_id)
				r.scenario_id,
				r.player_id,
				r.score AS best_score,
				r.played_at AS best_played_at
			FROM runs r
			WHERE r.player_id IS NOT NULL
				AND r.score IS NOT NULL
			ORDER BY
				r.scenario_id,
				r.player_id,
				r.score DESC,
				r.played_at ASC,
				r.id ASC
		), difficulty_scores AS (
			SELECT
				bds.difficulty_id,
				ppb.player_id,
				SUM(ppb.best_score) AS composite_score,
				COUNT(*) AS matched_scenarios,
				MAX(ppb.best_played_at) AS last_played_at
			FROM benchmark_difficulty_scenarios bds
			JOIN per_player_best ppb ON ppb.scenario_id = bds.scenario_id
			GROUP BY bds.difficulty_id, ppb.player_id
		), ranked AS (
			SELECT
				ds.difficulty_id,
				ds.player_id,
				ds.composite_score,
				ds.matched_scenarios,
				ds.last_played_at,
				ROW_NUMBER() OVER (
					PARTITION BY ds.difficulty_id
					ORDER BY
						ds.composite_score DESC,
						ds.last_played_at ASC,
						ds.player_id ASC
				) AS rank
			FROM difficulty_scores ds
		)
		SELECT
			r.difficulty_id,
			r.player_id,
			r.rank,
			r.composite_score,
			r.matched_scenarios,
			r.last_played_at,
			NOW()
		FROM ranked r
		WHERE r.rank <= $1
	`

// Service recomputes leaderboard tables.
type Service struct {
	pool    *pgxpool.Pool
	maxRank int
}

// Config controls leaderboard refresh behavior.
type Config struct {
	MaxRank int
}

// NewService creates a leaderboard refresh service.
func NewService(pool *pgxpool.Pool, cfg Config) (*Service, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool is required")
	}
	if cfg.MaxRank <= 0 {
		return nil, fmt.Errorf("max rank must be greater than zero")
	}
	return &Service{
		pool:    pool,
		maxRank: cfg.MaxRank,
	}, nil
}

// Name returns the job name.
func (s *Service) Name() string {
	return jobName
}

// Run executes one leaderboard refresh.
func (s *Service) Run(ctx context.Context) (worker.Result, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return worker.Result{}, fmt.Errorf("begin leaderboard transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `TRUNCATE TABLE scenario_leaderboard_current`); err != nil {
		return worker.Result{}, fmt.Errorf("truncate scenario leaderboard: %w", err)
	}

	scenarioTag, err := tx.Exec(ctx, refreshScenarioLeaderboardSQL, s.maxRank)
	if err != nil {
		return worker.Result{}, fmt.Errorf("refresh scenario leaderboard: %w", err)
	}

	if _, err := tx.Exec(ctx, `TRUNCATE TABLE benchmark_difficulty_leaderboard_current`); err != nil {
		return worker.Result{}, fmt.Errorf("truncate benchmark leaderboard: %w", err)
	}

	benchmarkTag, err := tx.Exec(ctx, refreshBenchmarkLeaderboardSQL, s.maxRank)
	if err != nil {
		return worker.Result{}, fmt.Errorf("refresh benchmark leaderboard: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return worker.Result{}, fmt.Errorf("commit leaderboard refresh: %w", err)
	}

	scenarioRows := scenarioTag.RowsAffected()
	benchmarkRows := benchmarkTag.RowsAffected()

	return worker.Result{
		Status: worker.OutcomeSuccess,
		Message: fmt.Sprintf(
			"refreshed %d scenario rows and %d benchmark rows",
			scenarioRows,
			benchmarkRows,
		),
		Details: map[string]any{
			"scenarioRows":  scenarioRows,
			"benchmarkRows": benchmarkRows,
			"maxRank":       s.maxRank,
		},
	}, nil
}
