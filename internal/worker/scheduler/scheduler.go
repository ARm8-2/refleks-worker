package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	gocron "github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"

	"refleks-worker/internal/worker"
	"refleks-worker/internal/worker/jobconfig"
)

const configSyncJobName = "worker-config-sync"

// Service wraps gocron with worker-specific wiring and dynamic config refresh.
type Service struct {
	logger      *slog.Logger
	runner      *worker.Runner
	sched       gocron.Scheduler
	configStore *jobconfig.Store

	mu         sync.Mutex
	registered map[string]registeredJob // job_name -> gocron registration
	available  map[string]worker.Job    // job_name -> job instance
}

type registeredJob struct {
	gocronID uuid.UUID
	cronExpr string
}

// New creates a scheduler service that periodically syncs job config from the database.
func New(location *time.Location, logger *slog.Logger, runner *worker.Runner, configStore *jobconfig.Store, syncCron string) (*Service, error) {
	if location == nil {
		location = time.UTC
	}
	if syncCron == "" {
		return nil, fmt.Errorf("config sync cron is required")
	}

	sched, err := gocron.NewScheduler(
		gocron.WithLocation(location),
	)
	if err != nil {
		return nil, fmt.Errorf("create scheduler: %w", err)
	}

	service := &Service{
		logger:      logger,
		runner:      runner,
		sched:       sched,
		configStore: configStore,
		registered:  make(map[string]registeredJob),
		available:   make(map[string]worker.Job),
	}

	if err := service.registerConfigSync(syncCron); err != nil {
		return nil, err
	}

	return service, nil
}

// AddJob registers a job implementation as available for scheduling.
// The job will only be activated when Sync sees an enabled config row for it.
func (s *Service) AddJob(job worker.Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.available[job.Name()] = job
}

// Sync reads job configs from the database and reconciles the scheduler state:
// - enables/schedules jobs that are enabled in the DB
// - disables/removes jobs that are disabled or missing
// - updates cron expressions when they change
func (s *Service) Sync(ctx context.Context) error {
	configs, err := s.configStore.LoadAll(ctx)
	if err != nil {
		return fmt.Errorf("load job configs: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	desired := make(map[string]jobconfig.JobConfig, len(configs))
	for _, c := range configs {
		desired[c.JobName] = c
	}

	// Remove jobs that are disabled or no longer in config.
	for name, reg := range s.registered {
		cfg, exists := desired[name]
		if exists && cfg.Enabled {
			continue
		}
		if err := s.sched.RemoveJob(reg.gocronID); err != nil {
			s.logger.Error("failed to remove job from scheduler",
				slog.String("job", name),
				slog.String("error", err.Error()),
			)
		} else {
			reason := "disabled"
			if !exists {
				reason = "removed from config"
			}
			s.logger.Info("job removed from scheduler",
				slog.String("job", name),
				slog.String("reason", reason),
			)
		}
		delete(s.registered, name)
	}

	// Add or update enabled jobs.
	for _, cfg := range configs {
		if !cfg.Enabled {
			continue
		}
		job, ok := s.available[cfg.JobName]
		if !ok {
			continue
		}

		reg, scheduled := s.registered[cfg.JobName]

		if scheduled && reg.cronExpr == cfg.CronExpr {
			continue // already scheduled with the same cron
		}

		if scheduled {
			// Cron expression changed — update in place.
			updated, err := s.sched.Update(
				reg.gocronID,
				gocron.CronJob(cfg.CronExpr, false),
				gocron.NewTask(s.taskFunc(job)),
				gocron.WithName(job.Name()),
				gocron.WithSingletonMode(gocron.LimitModeReschedule),
			)
			if err != nil {
				s.logger.Error("failed to update job schedule",
					slog.String("job", cfg.JobName),
					slog.String("error", err.Error()),
				)
				continue
			}
			s.registered[cfg.JobName] = registeredJob{
				gocronID: updated.ID(),
				cronExpr: cfg.CronExpr,
			}
			s.logger.Info("job schedule updated",
				slog.String("job", cfg.JobName),
				slog.String("cron", cfg.CronExpr),
			)
			continue
		}

		// New job — register it.
		gj, err := s.sched.NewJob(
			gocron.CronJob(cfg.CronExpr, false),
			gocron.NewTask(s.taskFunc(job)),
			gocron.WithName(job.Name()),
			gocron.WithSingletonMode(gocron.LimitModeReschedule),
		)
		if err != nil {
			s.logger.Error("failed to schedule job",
				slog.String("job", cfg.JobName),
				slog.String("error", err.Error()),
			)
			continue
		}
		s.registered[cfg.JobName] = registeredJob{
			gocronID: gj.ID(),
			cronExpr: cfg.CronExpr,
		}

		if nextRun, nextErr := gj.NextRun(); nextErr == nil {
			s.logger.Info("job scheduled",
				slog.String("job", cfg.JobName),
				slog.String("cron", cfg.CronExpr),
				slog.Time("next_run", nextRun),
			)
		} else {
			s.logger.Info("job scheduled",
				slog.String("job", cfg.JobName),
				slog.String("cron", cfg.CronExpr),
			)
		}
	}

	return nil
}

// EnabledJobs returns the names of jobs that are currently scheduled.
func (s *Service) EnabledJobs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.registered))
	for name := range s.registered {
		names = append(names, name)
	}
	return names
}

// RunNow executes one job immediately using the same execution wrapper.
func (s *Service) RunNow(ctx context.Context, job worker.Job) error {
	return s.runner.Run(ctx, job)
}

// Start starts the gocron scheduler.
func (s *Service) Start() {
	s.sched.Start()
}

// Shutdown shuts down the scheduler.
func (s *Service) Shutdown() error {
	return s.sched.Shutdown()
}

func (s *Service) registerConfigSync(cronExpr string) error {
	job, err := s.sched.NewJob(
		gocron.CronJob(cronExpr, false),
		gocron.NewTask(s.syncTask),
		gocron.WithName(configSyncJobName),
		gocron.WithSingletonMode(gocron.LimitModeReschedule),
	)
	if err != nil {
		return fmt.Errorf("register config sync job: %w", err)
	}

	if nextRun, nextErr := job.NextRun(); nextErr == nil {
		s.logger.Info("config sync scheduled",
			slog.String("job", configSyncJobName),
			slog.String("cron", cronExpr),
			slog.Time("next_run", nextRun),
		)
	} else {
		s.logger.Info("config sync scheduled",
			slog.String("job", configSyncJobName),
			slog.String("cron", cronExpr),
		)
	}

	return nil
}

func (s *Service) syncTask(ctx context.Context) {
	syncCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if err := s.Sync(syncCtx); err != nil {
		s.logger.Error("config sync failed", slog.String("error", err.Error()))
	}
}

func (s *Service) taskFunc(job worker.Job) func(ctx context.Context) {
	return func(ctx context.Context) {
		if err := s.runner.Run(ctx, job); err != nil {
			s.logger.Error("scheduled job failed",
				slog.String("job", job.Name()),
				slog.String("error", err.Error()),
			)
		}
	}
}
