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
	defaultAppName                 = "refleks-worker"
	defaultEnv                     = "development"
	defaultLogLevel                = "info"
	defaultTimezone                = "UTC"
	defaultJobTimeout              = 45 * time.Minute
	defaultRunOnStartup            = true
	defaultLeaderboardCron         = "0 4 * * *"
	defaultScenarioStatsCron       = "0 */12 * * *"
	defaultBenchmarkSyncCron       = "*/10 * * * *"
	defaultLeaderboardMaxRank      = 1000
	defaultBenchmarkSyncSourceDir  = "/data/benchmarks"
	defaultBenchmarkSyncSourceFile = "benchmarks_data.json"
	defaultConfigSyncCron          = "*/2 * * * *"
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

	LeaderboardCron   string
	ScenarioStatsCron string
	BenchmarkSyncCron string

	LeaderboardMaxRank int

	BenchmarkSyncSourceDir  string
	BenchmarkSyncSourceFile string

	ConfigSyncCron string
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

	timezoneName := envOrDefault("WORKER_TIMEZONE", defaultTimezone)
	location, err := time.LoadLocation(timezoneName)
	if err != nil {
		return Config{}, fmt.Errorf("WORKER_TIMEZONE must be a valid IANA timezone: %w", err)
	}

	supabaseDBURL := strings.TrimSpace(os.Getenv("SUPABASE_DB_URL"))
	if supabaseDBURL == "" {
		return Config{}, fmt.Errorf("SUPABASE_DB_URL is required")
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

		LeaderboardCron:   strings.TrimSpace(envOrDefault("LEADERBOARD_CRON", defaultLeaderboardCron)),
		ScenarioStatsCron: strings.TrimSpace(envOrDefault("SCENARIO_STATS_CRON", defaultScenarioStatsCron)),
		BenchmarkSyncCron: strings.TrimSpace(envOrDefault("BENCHMARK_SYNC_CRON", defaultBenchmarkSyncCron)),

		LeaderboardMaxRank: leaderboardMaxRank,

		BenchmarkSyncSourceDir:  strings.TrimSpace(envOrDefault("BENCHMARK_SYNC_SOURCE_DIR", defaultBenchmarkSyncSourceDir)),
		BenchmarkSyncSourceFile: strings.TrimSpace(envOrDefault("BENCHMARK_SYNC_SOURCE_FILE", defaultBenchmarkSyncSourceFile)),

		ConfigSyncCron: strings.TrimSpace(envOrDefault("CONFIG_SYNC_CRON", defaultConfigSyncCron)),
	}

	if cfg.LeaderboardCron == "" {
		return Config{}, fmt.Errorf("LEADERBOARD_CRON must not be empty")
	}
	if cfg.ScenarioStatsCron == "" {
		return Config{}, fmt.Errorf("SCENARIO_STATS_CRON must not be empty")
	}
	if cfg.BenchmarkSyncCron == "" {
		return Config{}, fmt.Errorf("BENCHMARK_SYNC_CRON must not be empty")
	}
	if cfg.BenchmarkSyncSourceFile == "" {
		return Config{}, fmt.Errorf("BENCHMARK_SYNC_SOURCE_FILE must not be empty")
	}
	if cfg.ConfigSyncCron == "" {
		return Config{}, fmt.Errorf("CONFIG_SYNC_CRON must not be empty")
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
