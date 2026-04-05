package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
	_ "time/tzdata"

	"refleks-worker/internal/config"
	"refleks-worker/internal/r2"
	"refleks-worker/internal/supabase"
	"refleks-worker/internal/worker"
	"refleks-worker/internal/worker/jobs/benchmarksync"
	"refleks-worker/internal/worker/jobs/leaderboard"
	"refleks-worker/internal/worker/jobs/parquetexport"
	"refleks-worker/internal/worker/jobs/scenariostats"
	"refleks-worker/internal/worker/scheduler"
	"refleks-worker/internal/worker/schema"
	"refleks-worker/internal/worker/state"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "worker error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(version)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))

	bootstrapCtx, bootstrapCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer bootstrapCancel()

	dbClient, err := supabase.NewClient(bootstrapCtx, cfg.SupabaseDBURL)
	if err != nil {
		return fmt.Errorf("init supabase client: %w", err)
	}
	defer dbClient.Close()

	if err := dbClient.Ping(bootstrapCtx); err != nil {
		return fmt.Errorf("ping supabase: %w", err)
	}
	logger.Info("supabase connected")

	if err := schema.Ensure(bootstrapCtx, dbClient.Pool()); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}
	logger.Info("schema bootstrap complete")

	r2Store, err := r2.NewStore(bootstrapCtx, r2.Config{
		Endpoint:        cfg.R2Endpoint,
		Region:          cfg.R2Region,
		Bucket:          cfg.R2LabPrivateBucket,
		AccessKeyID:     cfg.R2AccessKeyID,
		SecretAccessKey: cfg.R2SecretAccessKey,
	})
	if err != nil {
		return fmt.Errorf("init r2 store: %w", err)
	}

	rawSourceStore, err := r2.NewStore(bootstrapCtx, r2.Config{
		Endpoint:        cfg.R2Endpoint,
		Region:          cfg.R2Region,
		Bucket:          cfg.R2RawPublicBucket,
		AccessKeyID:     cfg.R2AccessKeyID,
		SecretAccessKey: cfg.R2SecretAccessKey,
	})
	if err != nil {
		return fmt.Errorf("init raw source r2 store: %w", err)
	}

	stateStore := state.NewStore(dbClient.Pool())
	runner := worker.NewRunner(logger, stateStore, cfg.JobTimeout)

	benchmarkSyncJob, err := benchmarksync.NewService(logger, dbClient.Pool(), stateStore, benchmarksync.Config{
		SourcePath: cfg.BenchmarkSyncPath(),
	})
	if err != nil {
		return fmt.Errorf("init benchmark sync job: %w", err)
	}

	leaderboardJob, err := leaderboard.NewService(dbClient.Pool(), leaderboard.Config{
		MaxRank: cfg.LeaderboardMaxRank,
	})
	if err != nil {
		return fmt.Errorf("init leaderboard job: %w", err)
	}

	scenarioStatsJob, err := scenariostats.NewService(dbClient.Pool())
	if err != nil {
		return fmt.Errorf("init scenario stats job: %w", err)
	}

	parquetJob, err := parquetexport.NewService(logger, dbClient.Pool(), r2Store, rawSourceStore, parquetexport.Config{
		R2Prefix:            cfg.ParquetR2Prefix,
		SourcePrefix:        cfg.ParquetSourcePrefix,
		RunsLookbackDays:    cfg.ParquetRunsLookbackDays,
		TraceSamplePoints:   cfg.ParquetTraceSamplePoints,
		MaxSegmentsPerRun:   cfg.ParquetMaxSegmentsPerRun,
		SameSpotThresholdPx: cfg.ParquetSameSpotThresholdPx,
		SourceListPage:      cfg.ParquetSourceListPage,
	})
	if err != nil {
		return fmt.Errorf("init parquet export job: %w", err)
	}

	schedulerSvc, err := scheduler.New(cfg.Timezone, logger, runner)
	if err != nil {
		return fmt.Errorf("init scheduler: %w", err)
	}

	if err := schedulerSvc.RegisterCron(benchmarkSyncJob, cfg.BenchmarkSyncCron); err != nil {
		return err
	}
	if err := schedulerSvc.RegisterCron(leaderboardJob, cfg.LeaderboardCron); err != nil {
		return err
	}
	if err := schedulerSvc.RegisterCron(scenarioStatsJob, cfg.ScenarioStatsCron); err != nil {
		return err
	}
	if err := schedulerSvc.RegisterCron(parquetJob, cfg.ParquetCron); err != nil {
		return err
	}

	jobs := []worker.Job{benchmarkSyncJob, scenarioStatsJob, leaderboardJob, parquetJob}

	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.RunOnStartup {
		logger.Info("running startup jobs")
		for _, job := range jobs {
			if err := schedulerSvc.RunNow(runCtx, job); err != nil {
				logger.Error("startup job failed",
					slog.String("job", job.Name()),
					slog.String("error", err.Error()),
				)
			}
		}
	}

	schedulerSvc.Start()
	logger.Info("worker started",
		slog.String("service", cfg.AppName),
		slog.String("environment", cfg.Environment),
		slog.String("version", cfg.Version),
		slog.String("timezone", cfg.Timezone.String()),
		slog.String("benchmark_sync_cron", cfg.BenchmarkSyncCron),
		slog.String("scenario_stats_cron", cfg.ScenarioStatsCron),
		slog.String("leaderboard_cron", cfg.LeaderboardCron),
		slog.String("parquet_cron", cfg.ParquetCron),
	)

	<-runCtx.Done()
	logger.Info("shutdown signal received")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- schedulerSvc.Shutdown()
	}()

	select {
	case err := <-shutdownDone:
		if err != nil {
			return fmt.Errorf("shutdown scheduler: %w", err)
		}
	case <-shutdownCtx.Done():
		return fmt.Errorf("shutdown timed out")
	}

	logger.Info("worker stopped")
	return nil
}
