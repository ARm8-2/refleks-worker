package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	StatusRunning = "running"
	StatusSuccess = "success"
	StatusFailed  = "failed"
	StatusSkipped = "skipped"
)

// Store handles worker job run logs and state persistence.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore builds a state store from a shared pool.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// StartRun records a new running job row.
func (s *Store) StartRun(ctx context.Context, jobName string) (int64, time.Time, error) {
	if s == nil || s.pool == nil {
		return 0, time.Time{}, fmt.Errorf("state store is not configured")
	}

	startedAt := time.Now().UTC()
	var runID int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO worker_job_runs (job_name, status, started_at)
		VALUES ($1, $2, $3)
		RETURNING id
	`, jobName, StatusRunning, startedAt).Scan(&runID)
	if err != nil {
		return 0, time.Time{}, err
	}

	return runID, startedAt, nil
}

// FinishRun records a final outcome for a job run.
func (s *Store) FinishRun(ctx context.Context, runID int64, status, message string, details map[string]any, startedAt time.Time) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("state store is not configured")
	}
	if runID <= 0 {
		return fmt.Errorf("run id must be greater than zero")
	}
	if status == "" {
		return fmt.Errorf("status is required")
	}

	if details == nil {
		details = map[string]any{}
	}
	detailsJSON, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("marshal job details: %w", err)
	}

	finishedAt := time.Now().UTC()
	duration := finishedAt.Sub(startedAt).Milliseconds()
	result, err := s.pool.Exec(ctx, `
		UPDATE worker_job_runs
		SET
			status = $2,
			message = $3,
			details = $4::jsonb,
			finished_at = $5,
			duration_ms = $6
		WHERE id = $1
	`, runID, status, message, detailsJSON, finishedAt, duration)
	if err != nil {
		return fmt.Errorf("finish job run %d: %w", runID, err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("finish job run %d: run not found", runID)
	}

	return nil
}

// GetJSON reads state_value JSON into dst.
func (s *Store) GetJSON(ctx context.Context, jobName, stateKey string, dst any) (bool, error) {
	if s == nil || s.pool == nil {
		return false, fmt.Errorf("state store is not configured")
	}

	var raw string
	err := s.pool.QueryRow(ctx, `
		SELECT state_value::text
		FROM worker_job_state
		WHERE job_name = $1 AND state_key = $2
	`, jobName, stateKey).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load job state: %w", err)
	}

	if err := json.Unmarshal([]byte(raw), dst); err != nil {
		return false, fmt.Errorf("decode job state: %w", err)
	}

	return true, nil
}

// SetJSON upserts a JSON state value.
func (s *Store) SetJSON(ctx context.Context, jobName, stateKey string, value any) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("state store is not configured")
	}

	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal job state: %w", err)
	}

	_, err = s.pool.Exec(ctx, `
		INSERT INTO worker_job_state (job_name, state_key, state_value, updated_at)
		VALUES ($1, $2, $3::jsonb, NOW())
		ON CONFLICT (job_name, state_key) DO UPDATE
		SET state_value = EXCLUDED.state_value, updated_at = NOW()
	`, jobName, stateKey, raw)
	if err != nil {
		return fmt.Errorf("store job state: %w", err)
	}

	return nil
}
