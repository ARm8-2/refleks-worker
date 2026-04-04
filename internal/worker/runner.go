package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"refleks-worker/internal/worker/state"
)

const runFinalizeTimeout = 10 * time.Second

// Runner wraps job execution with timeout, logging, and run-state persistence.
type Runner struct {
	logger  *slog.Logger
	state   *state.Store
	timeout time.Duration
}

// NewRunner creates a runner.
func NewRunner(logger *slog.Logger, stateStore *state.Store, timeout time.Duration) *Runner {
	return &Runner{
		logger:  logger,
		state:   stateStore,
		timeout: timeout,
	}
}

// Run executes one job once.
func (r *Runner) Run(ctx context.Context, job Job) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}

	runID, startedAt, err := r.state.StartRun(ctx, job.Name())
	if err != nil {
		return fmt.Errorf("record job start: %w", err)
	}

	r.logger.Info("job started", slog.String("job", job.Name()), slog.Int64("run_id", runID))

	jobCtx := ctx
	cancel := func() {}
	if r.timeout > 0 {
		jobCtx, cancel = context.WithTimeout(ctx, r.timeout)
	}
	defer cancel()

	result, runErr := job.Run(jobCtx)
	if runErr != nil {
		message := strings.TrimSpace(runErr.Error())
		if message == "" {
			message = "job failed"
		}

		details := map[string]any{
			"error": message,
		}
		if errors.Is(runErr, context.DeadlineExceeded) || errors.Is(jobCtx.Err(), context.DeadlineExceeded) {
			details["timedOut"] = true
		}
		if errors.Is(runErr, context.Canceled) || errors.Is(jobCtx.Err(), context.Canceled) {
			details["canceled"] = true
		}

		finishErr := r.finishRun(ctx, runID, state.StatusFailed, message, details, startedAt)
		if finishErr != nil {
			r.logger.Error("failed to finalize failed job run",
				slog.String("job", job.Name()),
				slog.Int64("run_id", runID),
				slog.String("error", finishErr.Error()),
			)
		}
		r.logger.Error("job failed",
			slog.String("job", job.Name()),
			slog.Int64("run_id", runID),
			slog.String("error", runErr.Error()),
		)
		return runErr
	}

	status := strings.TrimSpace(result.Status)
	if status == "" {
		status = OutcomeSuccess
	}
	if status != OutcomeSuccess && status != OutcomeSkipped {
		status = OutcomeSuccess
	}

	message := strings.TrimSpace(result.Message)
	if message == "" {
		if status == OutcomeSkipped {
			message = "skipped"
		} else {
			message = "completed"
		}
	}

	details := result.Details
	if details == nil {
		details = map[string]any{}
	}

	persistedStatus := state.StatusSuccess
	if status == OutcomeSkipped {
		persistedStatus = state.StatusSkipped
	}
	if err := r.finishRun(ctx, runID, persistedStatus, message, details, startedAt); err != nil {
		return fmt.Errorf("record job finish: %w", err)
	}

	r.logger.Info("job finished",
		slog.String("job", job.Name()),
		slog.Int64("run_id", runID),
		slog.String("status", status),
		slog.String("message", message),
	)

	return nil
}

func (r *Runner) finishRun(ctx context.Context, runID int64, status, message string, details map[string]any, startedAt time.Time) error {
	finalizeCtx := context.Background()
	if ctx != nil {
		finalizeCtx = context.WithoutCancel(ctx)
	}
	finalizeCtx, cancel := context.WithTimeout(finalizeCtx, runFinalizeTimeout)
	defer cancel()

	return r.state.FinishRun(finalizeCtx, runID, status, message, details, startedAt)
}
