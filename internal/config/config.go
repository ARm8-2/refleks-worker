package config

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	defaultAppName                    = "refleks-worker"
	defaultEnv                        = "development"
	defaultLogLevel                   = "info"
	defaultTimezone                   = "UTC"
	defaultJobTimeout                 = 45 * time.Minute
	defaultRunOnStartup               = true
	defaultLeaderboardCron            = "0 4 * * *"
	defaultScenarioStatsCron          = "0 */12 * * *"
	defaultParquetCron                = "30 4 * * *"
	defaultBenchmarkSyncCron          = "*/10 * * * *"
	defaultLeaderboardMaxRank         = 1000
	defaultParquetRunsLookbackDays    = 1
	defaultR2Region                   = "auto"
	defaultR2LabPrivateBucket         = "refleks-lab-private"
	defaultR2RawPublicBucket          = "refleks-raw-public"
	defaultParquetR2Prefix            = "lab/parquet"
	defaultParquetSourcePrefix        = ""
	defaultParquetTraceSamplePoints   = 64
	defaultParquetMaxSegmentsPerRun   = 256
	defaultParquetSameSpotThresholdPx = 120
	defaultParquetSourceListPage      = 1000
	defaultBenchmarkSyncSourceDir     = "/data/benchmarks"
	defaultBenchmarkSyncSourceFile    = "benchmarks_data.json"
)

// Config contains worker runtime settings.
type Config struct {
	AppName      string
	Environment  string
	Version      string
	LogLevel     slog.Level
	Timezone     *time.Location
	JobTimeout   time.Duration
	RunOnStartup bool

	SupabaseDBURL string

	R2Endpoint                 string
	R2Region                   string
	R2LabPrivateBucket         string
	R2RawPublicBucket          string
	R2AccessKeyID              string
	R2SecretAccessKey          string
	ParquetR2Prefix            string
	ParquetSourcePrefix        string
	ParquetTraceSamplePoints   int
	ParquetMaxSegmentsPerRun   int
	ParquetSameSpotThresholdPx int
	ParquetSourceListPage      int32

	LeaderboardCron   string
	ScenarioStatsCron string
	ParquetCron       string
	BenchmarkSyncCron string

	LeaderboardMaxRank      int
	ParquetRunsLookbackDays int

	BenchmarkSyncSourceDir  string
	BenchmarkSyncSourceFile string
}

// Load reads environment variables into Config.
func Load(version string) (Config, error) {
	logLevel, err := parseLogLevel(envOrDefault("LOG_LEVEL", defaultLogLevel))
	if err != nil {
		return Config{}, err
	}

	jobTimeout, err := envDuration("WORKER_JOB_TIMEOUT", defaultJobTimeout)
	if err != nil {
		return Config{}, err
	}
	if jobTimeout <= 0 {
		return Config{}, fmt.Errorf("WORKER_JOB_TIMEOUT must be greater than zero")
	}

	runOnStartup, err := envBool("WORKER_RUN_ON_STARTUP", defaultRunOnStartup)
	if err != nil {
		return Config{}, err
	}

	leaderboardMaxRank, err := envInt("LEADERBOARD_MAX_RANK", defaultLeaderboardMaxRank)
	if err != nil {
		return Config{}, err
	}
	if leaderboardMaxRank <= 0 {
		return Config{}, fmt.Errorf("LEADERBOARD_MAX_RANK must be greater than zero")
	}

	parquetRunsLookbackDays, err := envInt("PARQUET_RUNS_LOOKBACK_DAYS", defaultParquetRunsLookbackDays)
	if err != nil {
		return Config{}, err
	}
	if parquetRunsLookbackDays <= 0 {
		return Config{}, fmt.Errorf("PARQUET_RUNS_LOOKBACK_DAYS must be greater than zero")
	}

	parquetTraceSamplePoints, err := envInt("PARQUET_TRACE_SAMPLE_POINTS", defaultParquetTraceSamplePoints)
	if err != nil {
		return Config{}, err
	}
	if parquetTraceSamplePoints <= 0 {
		return Config{}, fmt.Errorf("PARQUET_TRACE_SAMPLE_POINTS must be greater than zero")
	}

	parquetMaxSegmentsPerRun, err := envInt("PARQUET_MAX_SEGMENTS_PER_RUN", defaultParquetMaxSegmentsPerRun)
	if err != nil {
		return Config{}, err
	}
	if parquetMaxSegmentsPerRun <= 0 {
		return Config{}, fmt.Errorf("PARQUET_MAX_SEGMENTS_PER_RUN must be greater than zero")
	}

	parquetSameSpotThresholdPx, err := envInt("PARQUET_SAME_SPOT_THRESHOLD_PX", defaultParquetSameSpotThresholdPx)
	if err != nil {
		return Config{}, err
	}
	if parquetSameSpotThresholdPx <= 0 {
		return Config{}, fmt.Errorf("PARQUET_SAME_SPOT_THRESHOLD_PX must be greater than zero")
	}

	parquetSourceListPage, err := envInt("PARQUET_SOURCE_LIST_PAGE", defaultParquetSourceListPage)
	if err != nil {
		return Config{}, err
	}
	if parquetSourceListPage <= 0 {
		return Config{}, fmt.Errorf("PARQUET_SOURCE_LIST_PAGE must be greater than zero")
	}

	timezoneName := envOrDefault("WORKER_TIMEZONE", defaultTimezone)
	location, err := time.LoadLocation(timezoneName)
	if err != nil {
		return Config{}, fmt.Errorf("WORKER_TIMEZONE must be a valid IANA timezone: %w", err)
	}

	supabaseDBURL := strings.TrimSpace(os.Getenv("SUPABASE_DB_URL"))
	if supabaseDBURL == "" {
		return Config{}, fmt.Errorf("SUPABASE_DB_URL is required")
	}

	r2Endpoint := strings.TrimSpace(os.Getenv("R2_ENDPOINT"))
	r2AccessKeyID := strings.TrimSpace(os.Getenv("R2_ACCESS_KEY_ID"))
	r2SecretAccessKey := strings.TrimSpace(os.Getenv("R2_SECRET_ACCESS_KEY"))
	r2LabPrivateBucket := envOrDefault("R2_LAB_PRIVATE_BUCKET", defaultR2LabPrivateBucket)
	r2RawPublicBucket := envOrDefault("R2_RAW_PUBLIC_BUCKET", defaultR2RawPublicBucket)
	if r2Endpoint == "" {
		return Config{}, fmt.Errorf("R2_ENDPOINT is required")
	}
	if r2AccessKeyID == "" || r2SecretAccessKey == "" {
		return Config{}, fmt.Errorf("R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY are required")
	}
	if strings.TrimSpace(r2LabPrivateBucket) == "" {
		return Config{}, fmt.Errorf("R2_LAB_PRIVATE_BUCKET is required")
	}
	if strings.TrimSpace(r2RawPublicBucket) == "" {
		return Config{}, fmt.Errorf("R2_RAW_PUBLIC_BUCKET is required")
	}

	resolvedVersion := envOrDefault("APP_VERSION", version)
	if strings.TrimSpace(resolvedVersion) == "" {
		resolvedVersion = "dev"
	}

	cfg := Config{
		AppName:      envOrDefault("APP_NAME", defaultAppName),
		Environment:  envOrDefault("APP_ENV", defaultEnv),
		Version:      resolvedVersion,
		LogLevel:     logLevel,
		Timezone:     location,
		JobTimeout:   jobTimeout,
		RunOnStartup: runOnStartup,

		SupabaseDBURL: supabaseDBURL,

		R2Endpoint:                 r2Endpoint,
		R2Region:                   envOrDefault("R2_REGION", defaultR2Region),
		R2LabPrivateBucket:         strings.TrimSpace(r2LabPrivateBucket),
		R2RawPublicBucket:          strings.TrimSpace(r2RawPublicBucket),
		R2AccessKeyID:              r2AccessKeyID,
		R2SecretAccessKey:          r2SecretAccessKey,
		ParquetR2Prefix:            strings.Trim(strings.TrimSpace(envOrDefault("PARQUET_R2_PREFIX", defaultParquetR2Prefix)), "/"),
		ParquetSourcePrefix:        strings.Trim(strings.TrimSpace(envOrDefault("PARQUET_SOURCE_PREFIX", defaultParquetSourcePrefix)), "/"),
		ParquetTraceSamplePoints:   parquetTraceSamplePoints,
		ParquetMaxSegmentsPerRun:   parquetMaxSegmentsPerRun,
		ParquetSameSpotThresholdPx: parquetSameSpotThresholdPx,
		ParquetSourceListPage:      int32(parquetSourceListPage),

		LeaderboardCron:   strings.TrimSpace(envOrDefault("LEADERBOARD_CRON", defaultLeaderboardCron)),
		ScenarioStatsCron: strings.TrimSpace(envOrDefault("SCENARIO_STATS_CRON", defaultScenarioStatsCron)),
		ParquetCron:       strings.TrimSpace(envOrDefault("PARQUET_CRON", defaultParquetCron)),
		BenchmarkSyncCron: strings.TrimSpace(envOrDefault("BENCHMARK_SYNC_CRON", defaultBenchmarkSyncCron)),

		LeaderboardMaxRank:      leaderboardMaxRank,
		ParquetRunsLookbackDays: parquetRunsLookbackDays,

		BenchmarkSyncSourceDir:  strings.TrimSpace(envOrDefault("BENCHMARK_SYNC_SOURCE_DIR", defaultBenchmarkSyncSourceDir)),
		BenchmarkSyncSourceFile: strings.TrimSpace(envOrDefault("BENCHMARK_SYNC_SOURCE_FILE", defaultBenchmarkSyncSourceFile)),
	}

	if cfg.ParquetR2Prefix == "" {
		return Config{}, fmt.Errorf("PARQUET_R2_PREFIX must not be empty")
	}
	if cfg.LeaderboardCron == "" {
		return Config{}, fmt.Errorf("LEADERBOARD_CRON must not be empty")
	}
	if cfg.ScenarioStatsCron == "" {
		return Config{}, fmt.Errorf("SCENARIO_STATS_CRON must not be empty")
	}
	if cfg.ParquetCron == "" {
		return Config{}, fmt.Errorf("PARQUET_CRON must not be empty")
	}
	if cfg.BenchmarkSyncCron == "" {
		return Config{}, fmt.Errorf("BENCHMARK_SYNC_CRON must not be empty")
	}
	if cfg.BenchmarkSyncSourceFile == "" {
		return Config{}, fmt.Errorf("BENCHMARK_SYNC_SOURCE_FILE must not be empty")
	}
	if cfg.BenchmarkSyncSourceDir == "" && !filepath.IsAbs(cfg.BenchmarkSyncSourceFile) {
		return Config{}, fmt.Errorf("BENCHMARK_SYNC_SOURCE_DIR must not be empty when BENCHMARK_SYNC_SOURCE_FILE is relative")
	}

	return cfg, nil
}

// BenchmarkSyncPath returns the absolute source path for benchmark sync input.
func (c Config) BenchmarkSyncPath() string {
	if filepath.IsAbs(c.BenchmarkSyncSourceFile) {
		return c.BenchmarkSyncSourceFile
	}
	return filepath.Join(c.BenchmarkSyncSourceDir, c.BenchmarkSyncSourceFile)
}

func envOrDefault(key, fallback string) string {
	ensureEnvLoaded()
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return fallback
	}
	return v
}

func envInt(key string, fallback int) (int, error) {
	ensureEnvLoaded()
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return v, nil
}

func envBool(key string, fallback bool) (bool, error) {
	ensureEnvLoaded()
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	v, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return false, fmt.Errorf("%s must be true|false: %w", key, err)
	}
	return v, nil
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	ensureEnvLoaded()
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	v, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration (for example 30s, 5m): %w", key, err)
	}
	return v, nil
}

func parseLogLevel(value string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("LOG_LEVEL must be one of debug|info|warn|error")
	}
}
