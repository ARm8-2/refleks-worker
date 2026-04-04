package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	gocron "github.com/go-co-op/gocron/v2"

	"refleks-worker/internal/worker"
)

// Service wraps gocron with worker-specific wiring.
type Service struct {
	logger *slog.Logger
	runner *worker.Runner
	sched  gocron.Scheduler
}

// New creates a scheduler service.
func New(location *time.Location, logger *slog.Logger, runner *worker.Runner) (*Service, error) {
	if location == nil {
		location = time.UTC
	}

	sched, err := gocron.NewScheduler(
		gocron.WithLocation(location),
	)
	if err != nil {
		return nil, fmt.Errorf("create scheduler: %w", err)
	}

	return &Service{
		logger: logger,
		runner: runner,
		sched:  sched,
	}, nil
}

// RegisterCron registers one cron-based worker job.
func (s *Service) RegisterCron(job worker.Job, cronExpr string) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	if cronExpr == "" {
		return fmt.Errorf("cron expression is required for job %q", job.Name())
	}

	registered, err := s.sched.NewJob(
		gocron.CronJob(cronExpr, false),
		gocron.NewTask(func(jobCtx context.Context) {
			if err := s.runner.Run(jobCtx, job); err != nil {
				s.logger.Error("scheduled job failed",
					slog.String("job", job.Name()),
					slog.String("error", err.Error()),
				)
			}
		}),
		gocron.WithName(job.Name()),
		gocron.WithSingletonMode(gocron.LimitModeReschedule),
	)
	if err != nil {
		return fmt.Errorf("register job %q: %w", job.Name(), err)
	}

	if nextRun, nextErr := registered.NextRun(); nextErr == nil {
		s.logger.Info("job scheduled",
			slog.String("job", job.Name()),
			slog.String("cron", cronExpr),
			slog.Time("next_run", nextRun),
		)
	} else {
		s.logger.Info("job scheduled",
			slog.String("job", job.Name()),
			slog.String("cron", cronExpr),
		)
	}

	return nil
}

// RunNow executes one job immediately using the same execution wrapper.
func (s *Service) RunNow(ctx context.Context, job worker.Job) error {
	return s.runner.Run(ctx, job)
}

// Start starts scheduling jobs.
func (s *Service) Start() {
	s.sched.Start()
}

// Shutdown stops all jobs and shuts down the scheduler.
func (s *Service) Shutdown() error {
	return s.sched.Shutdown()
}
