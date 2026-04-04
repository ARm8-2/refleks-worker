package parquetexport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	parquet "github.com/parquet-go/parquet-go"

	"refleks-worker/internal/r2"
	"refleks-worker/internal/worker"
	"refleks-worker/internal/worker/refleks"
)

const (
	jobName                    = "parquet_export"
	parquetContentType         = "application/x-parquet"
	defaultTraceSamplePoints   = 64
	defaultMaxSegmentsPerRun   = 256
	defaultSameSpotThresholdPx = 120
	defaultSourceListPageSize  = 1000
	directionChangeCosBoundary = 0.7
)

// Config controls parquet export behavior.
type Config struct {
	R2Prefix            string
	SourcePrefix        string
	RunsLookbackDays    int
	TraceSamplePoints   int
	MaxSegmentsPerRun   int
	SameSpotThresholdPx int
	SourceListPage      int32
}

// Service exports analytics data to Parquet and uploads it to R2.
type Service struct {
	logger              *slog.Logger
	pool                *pgxpool.Pool
	outputStore         *r2.Store
	sourceStore         *r2.Store
	r2Prefix            string
	sourcePrefix        string
	runsLookbackDays    int
	traceSamplePoints   int
	maxSegmentsPerRun   int
	sameSpotThresholdPx float64
	sourceListPage      int32
}

type rawRunParquetRow struct {
	PartitionDate               string  `parquet:"partition_date"`
	SourceBucket                string  `parquet:"source_bucket"`
	SourceObjectKey             string  `parquet:"source_object_key"`
	SourceObjectETag            string  `parquet:"source_object_etag"`
	SourceObjectSizeBytes       int64   `parquet:"source_object_size_bytes"`
	SourceLastModifiedUnixMilli int64   `parquet:"source_last_modified_unix_milli"`
	PayloadSHA256               string  `parquet:"payload_sha256"`
	FileName                    string  `parquet:"file_name"`
	EpochMilli                  int64   `parquet:"epoch_milli"`
	FormatVersion               int32   `parquet:"format_version"`
	ScenarioName                string  `parquet:"scenario_name"`
	SteamID                     string  `parquet:"steam_id"`
	SteamUsername               string  `parquet:"steam_username"`
	Score                       float64 `parquet:"score"`
	Accuracy                    float64 `parquet:"accuracy"`
	AvgTTKSeconds               float64 `parquet:"avg_ttk_seconds"`
	DurationSeconds             float64 `parquet:"duration_seconds"`
	SensCM360                   float64 `parquet:"sens_cm360"`
	HasMouseTrace               bool    `parquet:"has_mouse_trace"`
	TracePointCount             int32   `parquet:"trace_point_count"`
	MousePathDistance           float64 `parquet:"mouse_path_distance"`
	MouseNetDistance            float64 `parquet:"mouse_net_distance"`
	MousePathEfficiency         float64 `parquet:"mouse_path_efficiency"`
	MouseAvgSpeed               float64 `parquet:"mouse_avg_speed"`
	MousePeakSpeed              float64 `parquet:"mouse_peak_speed"`
	MouseMaxAccel               float64 `parquet:"mouse_max_accel"`
	MouseDirectionChanges       int32   `parquet:"mouse_direction_changes"`
	MouseClickTransitions       int32   `parquet:"mouse_click_transitions"`
	TraceSampleJSON             string  `parquet:"trace_sample_json"`
	StatsJSON                   string  `parquet:"stats_json"`
	EventsJSON                  string  `parquet:"events_json"`
	EnvAppVersion               string  `parquet:"env_app_version"`
	EnvOS                       string  `parquet:"env_os"`
	EnvArch                     string  `parquet:"env_arch"`
	EnvOSVersion                string  `parquet:"env_os_version"`
	EnvHostname                 string  `parquet:"env_hostname"`
	EnvCPUName                  string  `parquet:"env_cpu_name"`
	EnvCPUCores                 int32   `parquet:"env_cpu_cores"`
	EnvGPUName                  string  `parquet:"env_gpu_name"`
	EnvRAMTotalMB               int32   `parquet:"env_ram_total_mb"`
	EnvDisplayHz                float64 `parquet:"env_display_hz"`
	EnvScreenWidth              int32   `parquet:"env_screen_width"`
	EnvScreenHeight             int32   `parquet:"env_screen_height"`
	EnvIsWindowed               bool    `parquet:"env_is_windowed"`
	EnvMouseName                string  `parquet:"env_mouse_name"`
	EnvMouseVID                 string  `parquet:"env_mouse_vid"`
	EnvMousePID                 string  `parquet:"env_mouse_pid"`
	EnvMouseMI                  string  `parquet:"env_mouse_mi"`
	EnvMouseBackend             string  `parquet:"env_mouse_backend"`
	EnvTracePoints              int32   `parquet:"env_trace_points"`
	EnvTraceDuration            float64 `parquet:"env_trace_duration"`
	EnvSampleRate               int32   `parquet:"env_sample_rate"`
}

type runStatParquetRow struct {
	PartitionDate   string  `parquet:"partition_date"`
	SourceObjectKey string  `parquet:"source_object_key"`
	FileName        string  `parquet:"file_name"`
	EpochMilli      int64   `parquet:"epoch_milli"`
	ScenarioName    string  `parquet:"scenario_name"`
	SteamID         string  `parquet:"steam_id"`
	SteamUsername   string  `parquet:"steam_username"`
	StatKey         string  `parquet:"stat_key"`
	ValueType       string  `parquet:"value_type"`
	ValueString     string  `parquet:"value_string"`
	ValueFloat      float64 `parquet:"value_float"`
	ValueInt        int64   `parquet:"value_int"`
	ValueBool       bool    `parquet:"value_bool"`
	ValueRawJSON    string  `parquet:"value_raw_json"`
}

type runEventParquetRow struct {
	PartitionDate           string  `parquet:"partition_date"`
	SourceObjectKey         string  `parquet:"source_object_key"`
	FileName                string  `parquet:"file_name"`
	EpochMilli              int64   `parquet:"epoch_milli"`
	ScenarioName            string  `parquet:"scenario_name"`
	SteamID                 string  `parquet:"steam_id"`
	SteamUsername           string  `parquet:"steam_username"`
	EventIndex              int32   `parquet:"event_index"`
	KillNumber              int64   `parquet:"kill_number"`
	EventTimestampUnixMilli int64   `parquet:"event_timestamp_unix_milli"`
	EventTimestampText      string  `parquet:"event_timestamp_text"`
	Bot                     string  `parquet:"bot"`
	Weapon                  string  `parquet:"weapon"`
	TTKSeconds              float64 `parquet:"ttk_seconds"`
	Shots                   int32   `parquet:"shots"`
	Hits                    int32   `parquet:"hits"`
	Accuracy                float64 `parquet:"accuracy"`
	DamageDone              float64 `parquet:"damage_done"`
	DamagePossible          float64 `parquet:"damage_possible"`
	Efficiency              float64 `parquet:"efficiency"`
	Cheated                 bool    `parquet:"cheated"`
	Overshots               int32   `parquet:"overshots"`
	RawRowJSON              string  `parquet:"raw_row_json"`
}

type mouseSegmentParquetRow struct {
	PartitionDate         string  `parquet:"partition_date"`
	SourceObjectKey       string  `parquet:"source_object_key"`
	FileName              string  `parquet:"file_name"`
	EpochMilli            int64   `parquet:"epoch_milli"`
	ScenarioName          string  `parquet:"scenario_name"`
	SteamID               string  `parquet:"steam_id"`
	SteamUsername         string  `parquet:"steam_username"`
	SegmentIndex          int32   `parquet:"segment_index"`
	StartEventIndex       int32   `parquet:"start_event_index"`
	EndEventIndex         int32   `parquet:"end_event_index"`
	StartTS               int64   `parquet:"start_ts"`
	EndTS                 int64   `parquet:"end_ts"`
	DurationMs            int64   `parquet:"duration_ms"`
	StartX                int32   `parquet:"start_x"`
	StartY                int32   `parquet:"start_y"`
	EndX                  int32   `parquet:"end_x"`
	EndY                  int32   `parquet:"end_y"`
	DeltaX                int32   `parquet:"delta_x"`
	DeltaY                int32   `parquet:"delta_y"`
	SegmentAngleDeg       float64 `parquet:"segment_angle_deg"`
	LinearDistance        float64 `parquet:"linear_distance"`
	PathDistance          float64 `parquet:"path_distance"`
	PathEfficiency        float64 `parquet:"path_efficiency"`
	MeanSpeed             float64 `parquet:"mean_speed"`
	MaxSpeed              float64 `parquet:"max_speed"`
	MaxAccel              float64 `parquet:"max_accel"`
	DirectionChanges      int32   `parquet:"direction_changes"`
	ClickTransitions      int32   `parquet:"click_transitions"`
	MotionSignature       string  `parquet:"motion_signature"`
	HasPreviousSegment    bool    `parquet:"has_previous_segment"`
	PreviousEndDeltaX     int32   `parquet:"previous_end_delta_x"`
	PreviousEndDeltaY     int32   `parquet:"previous_end_delta_y"`
	PreviousEndDistancePx float64 `parquet:"previous_end_distance_px"`
	SameSpotThresholdPx   float64 `parquet:"same_spot_threshold_px"`
	IsSameSpotAsPrevious  bool    `parquet:"is_same_spot_as_previous"`
	SameSpotScore         float64 `parquet:"same_spot_score"`
}

type scenarioLeaderboardParquetRow struct {
	SnapshotDate   string  `parquet:"snapshot_date"`
	ScenarioID     int64   `parquet:"scenario_id"`
	ScenarioName   string  `parquet:"scenario_name"`
	AccountID      int64   `parquet:"account_id"`
	SteamID        string  `parquet:"steam_id"`
	SteamUsername  string  `parquet:"steam_username"`
	Rank           int32   `parquet:"rank"`
	BestScore      float64 `parquet:"best_score"`
	BestEpochMilli int64   `parquet:"best_epoch_milli"`
}

type benchmarkLeaderboardParquetRow struct {
	SnapshotDate       string  `parquet:"snapshot_date"`
	DifficultyID       int64   `parquet:"difficulty_id"`
	BenchmarkName      string  `parquet:"benchmark_name"`
	DifficultyName     string  `parquet:"difficulty_name"`
	KovaaksBenchmarkID int64   `parquet:"kovaaks_benchmark_id"`
	AccountID          int64   `parquet:"account_id"`
	SteamID            string  `parquet:"steam_id"`
	SteamUsername      string  `parquet:"steam_username"`
	Rank               int32   `parquet:"rank"`
	CompositeScore     float64 `parquet:"composite_score"`
	MatchedScenarios   int32   `parquet:"matched_scenarios"`
	LastEpochMilli     int64   `parquet:"last_epoch_milli"`
}

type dailyExport struct {
	runs          []rawRunParquetRow
	stats         []runStatParquetRow
	events        []runEventParquetRow
	mouseSegments []mouseSegmentParquetRow
}

type traceMetrics struct {
	PathDistance     float64
	NetDistance      float64
	PathEfficiency   float64
	AvgSpeed         float64
	PeakSpeed        float64
	MaxAccel         float64
	DirectionChanges int32
	ClickTransitions int32
}

type traceSamplePoint struct {
	TS      int64 `json:"ts"`
	X       int32 `json:"x"`
	Y       int32 `json:"y"`
	Buttons int32 `json:"buttons,omitempty"`
}

// NewService creates a parquet export service.
func NewService(logger *slog.Logger, pool *pgxpool.Pool, outputStore, sourceStore *r2.Store, cfg Config) (*Service, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool is required")
	}
	if outputStore == nil {
		return nil, fmt.Errorf("output r2 store is required")
	}
	if sourceStore == nil {
		return nil, fmt.Errorf("source r2 store is required")
	}

	prefix := strings.Trim(strings.TrimSpace(cfg.R2Prefix), "/")
	if prefix == "" {
		return nil, fmt.Errorf("r2 prefix is required")
	}
	if cfg.RunsLookbackDays <= 0 {
		return nil, fmt.Errorf("runs lookback days must be greater than zero")
	}

	traceSamplePoints := cfg.TraceSamplePoints
	if traceSamplePoints <= 0 {
		traceSamplePoints = defaultTraceSamplePoints
	}

	maxSegmentsPerRun := cfg.MaxSegmentsPerRun
	if maxSegmentsPerRun <= 0 {
		maxSegmentsPerRun = defaultMaxSegmentsPerRun
	}

	sameSpotThresholdPx := cfg.SameSpotThresholdPx
	if sameSpotThresholdPx <= 0 {
		sameSpotThresholdPx = defaultSameSpotThresholdPx
	}

	listPageSize := cfg.SourceListPage
	if listPageSize <= 0 {
		listPageSize = defaultSourceListPageSize
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Service{
		logger:              logger,
		pool:                pool,
		outputStore:         outputStore,
		sourceStore:         sourceStore,
		r2Prefix:            prefix,
		sourcePrefix:        strings.Trim(strings.TrimSpace(cfg.SourcePrefix), "/"),
		runsLookbackDays:    cfg.RunsLookbackDays,
		traceSamplePoints:   traceSamplePoints,
		maxSegmentsPerRun:   maxSegmentsPerRun,
		sameSpotThresholdPx: float64(sameSpotThresholdPx),
		sourceListPage:      listPageSize,
	}, nil
}

// Name returns the job name.
func (s *Service) Name() string {
	return jobName
}

// Run exports raw run payloads and leaderboard snapshots to parquet files.
func (s *Service) Run(ctx context.Context) (worker.Result, error) {
	now := time.Now().UTC()
	cutoff := now.AddDate(0, 0, -s.runsLookbackDays)

	sourceObjects, err := s.listSourceObjects(ctx, cutoff)
	if err != nil {
		return worker.Result{}, err
	}

	daily := make(map[string]*dailyExport)
	processedObjects := 0
	parseFailures := 0

	for _, listedObject := range sourceObjects {
		raw, downloadedObject, err := s.sourceStore.Get(ctx, listedObject.Key)
		if err != nil {
			parseFailures++
			s.logger.Error("failed to download source object",
				slog.String("job", s.Name()),
				slog.String("object_key", listedObject.Key),
				slog.String("error", err.Error()),
			)
			continue
		}

		objectInfo := mergeObjectInfo(listedObject, downloadedObject)
		parsed, err := refleks.Parse(raw)
		if err != nil {
			parseFailures++
			s.logger.Error("failed to parse source object",
				slog.String("job", s.Name()),
				slog.String("object_key", objectInfo.Key),
				slog.String("error", err.Error()),
			)
			continue
		}

		partition := partitionDate(parsed.EpochMilli, objectInfo.LastModified)
		bucket := ensureDailyBucket(daily, partition)

		payloadHash := sha256Hex(raw)
		runRow := buildRunRow(s.sourceStore.Bucket(), objectInfo, parsed, partition, payloadHash, s.traceSamplePoints)
		bucket.runs = append(bucket.runs, runRow)

		bucket.stats = append(bucket.stats, buildStatRows(runRow, parsed.Stats)...)

		eventRows, eventTimes := buildEventRows(runRow, parsed.EpochMilli, objectInfo.LastModified, parsed.Events)
		bucket.events = append(bucket.events, eventRows...)

		bucket.mouseSegments = append(bucket.mouseSegments, buildMouseSegments(runRow, parsed.MouseTrace, eventTimes, s.maxSegmentsPerRun, s.sameSpotThresholdPx)...)

		processedObjects++
	}

	if parseFailures > 0 {
		s.logger.Warn("parquet export completed with parse failures",
			slog.String("job", s.Name()),
			slog.Int("parse_failures", parseFailures),
			slog.Int("listed_source_objects", len(sourceObjects)),
		)
	}

	uploadedKeys := make([]string, 0)
	totalRunRows := 0
	totalStatRows := 0
	totalEventRows := 0
	totalSegmentRows := 0

	days := make([]string, 0, len(daily))
	for day := range daily {
		days = append(days, day)
	}
	sort.Strings(days)

	for _, day := range days {
		bucket := daily[day]

		if len(bucket.runs) > 0 {
			payload, err := encodeParquet(bucket.runs)
			if err != nil {
				return worker.Result{}, fmt.Errorf("encode raw runs parquet for %s: %w", day, err)
			}
			key := fmt.Sprintf("%s/raw/runs/date=%s/runs.parquet", s.r2Prefix, day)
			if err := s.outputStore.Put(ctx, key, payload, parquetContentType); err != nil {
				return worker.Result{}, fmt.Errorf("upload raw runs parquet %s: %w", key, err)
			}
			uploadedKeys = append(uploadedKeys, key)
			totalRunRows += len(bucket.runs)
		}

		if len(bucket.stats) > 0 {
			payload, err := encodeParquet(bucket.stats)
			if err != nil {
				return worker.Result{}, fmt.Errorf("encode raw stats parquet for %s: %w", day, err)
			}
			key := fmt.Sprintf("%s/raw/stats/date=%s/stats.parquet", s.r2Prefix, day)
			if err := s.outputStore.Put(ctx, key, payload, parquetContentType); err != nil {
				return worker.Result{}, fmt.Errorf("upload raw stats parquet %s: %w", key, err)
			}
			uploadedKeys = append(uploadedKeys, key)
			totalStatRows += len(bucket.stats)
		}

		if len(bucket.events) > 0 {
			payload, err := encodeParquet(bucket.events)
			if err != nil {
				return worker.Result{}, fmt.Errorf("encode raw events parquet for %s: %w", day, err)
			}
			key := fmt.Sprintf("%s/raw/events/date=%s/events.parquet", s.r2Prefix, day)
			if err := s.outputStore.Put(ctx, key, payload, parquetContentType); err != nil {
				return worker.Result{}, fmt.Errorf("upload raw events parquet %s: %w", key, err)
			}
			uploadedKeys = append(uploadedKeys, key)
			totalEventRows += len(bucket.events)
		}

		if len(bucket.mouseSegments) > 0 {
			payload, err := encodeParquet(bucket.mouseSegments)
			if err != nil {
				return worker.Result{}, fmt.Errorf("encode mouse segments parquet for %s: %w", day, err)
			}
			key := fmt.Sprintf("%s/raw/mouse_segments/date=%s/segments.parquet", s.r2Prefix, day)
			if err := s.outputStore.Put(ctx, key, payload, parquetContentType); err != nil {
				return worker.Result{}, fmt.Errorf("upload mouse segments parquet %s: %w", key, err)
			}
			uploadedKeys = append(uploadedKeys, key)
			totalSegmentRows += len(bucket.mouseSegments)
		}
	}

	snapshotDate := now.Format("2006-01-02")

	scenarioRows, err := s.loadScenarioLeaderboardRows(ctx, snapshotDate)
	if err != nil {
		return worker.Result{}, err
	}
	if len(scenarioRows) > 0 {
		payload, err := encodeParquet(scenarioRows)
		if err != nil {
			return worker.Result{}, fmt.Errorf("encode scenario leaderboard parquet: %w", err)
		}

		key := fmt.Sprintf("%s/leaderboards/scenario/date=%s/leaderboard.parquet", s.r2Prefix, snapshotDate)
		if err := s.outputStore.Put(ctx, key, payload, parquetContentType); err != nil {
			return worker.Result{}, fmt.Errorf("upload scenario leaderboard parquet %s: %w", key, err)
		}
		uploadedKeys = append(uploadedKeys, key)
	}

	benchmarkRows, err := s.loadBenchmarkLeaderboardRows(ctx, snapshotDate)
	if err != nil {
		return worker.Result{}, err
	}
	if len(benchmarkRows) > 0 {
		payload, err := encodeParquet(benchmarkRows)
		if err != nil {
			return worker.Result{}, fmt.Errorf("encode benchmark leaderboard parquet: %w", err)
		}

		key := fmt.Sprintf("%s/leaderboards/benchmark/date=%s/leaderboard.parquet", s.r2Prefix, snapshotDate)
		if err := s.outputStore.Put(ctx, key, payload, parquetContentType); err != nil {
			return worker.Result{}, fmt.Errorf("upload benchmark leaderboard parquet %s: %w", key, err)
		}
		uploadedKeys = append(uploadedKeys, key)
	}

	if len(uploadedKeys) == 0 {
		return worker.Result{
			Status:  worker.OutcomeSkipped,
			Message: "no parquet files generated for this run",
			Details: map[string]any{
				"listedSourceObjects": len(sourceObjects),
				"processedObjects":    processedObjects,
				"parseFailures":       parseFailures,
			},
		}, nil
	}

	return worker.Result{
		Status: worker.OutcomeSuccess,
		Message: fmt.Sprintf(
			"uploaded %d parquet files from %d raw objects",
			len(uploadedKeys),
			processedObjects,
		),
		Details: map[string]any{
			"files":               uploadedKeys,
			"listedSourceObjects": len(sourceObjects),
			"processedObjects":    processedObjects,
			"parseFailures":       parseFailures,
			"rawRunRows":          totalRunRows,
			"rawStatRows":         totalStatRows,
			"rawEventRows":        totalEventRows,
			"mouseSegmentRows":    totalSegmentRows,
			"leaderboardRows":     len(scenarioRows) + len(benchmarkRows),
			"runsLookbackDays":    s.runsLookbackDays,
		},
	}, nil
}

func ensureDailyBucket(buckets map[string]*dailyExport, partition string) *dailyExport {
	bucket, ok := buckets[partition]
	if ok {
		return bucket
	}
	bucket = &dailyExport{}
	buckets[partition] = bucket
	return bucket
}

func mergeObjectInfo(listed, downloaded r2.ObjectInfo) r2.ObjectInfo {
	merged := listed
	if strings.TrimSpace(merged.Key) == "" {
		merged.Key = downloaded.Key
	}
	if merged.SizeBytes <= 0 {
		merged.SizeBytes = downloaded.SizeBytes
	}
	if strings.TrimSpace(merged.ETag) == "" {
		merged.ETag = downloaded.ETag
	}
	if merged.LastModified.IsZero() {
		merged.LastModified = downloaded.LastModified
	}
	if !merged.LastModified.IsZero() {
		merged.LastModified = merged.LastModified.UTC()
	}
	return merged
}

func partitionDate(epochMilli int64, fallback time.Time) string {
	partitionTime := fallback.UTC()
	if epochMilli > 0 {
		partitionTime = time.UnixMilli(epochMilli).UTC()
	}
	if partitionTime.IsZero() {
		partitionTime = time.Now().UTC()
	}
	return partitionTime.Format("2006-01-02")
}

func buildRunRow(sourceBucket string, object r2.ObjectInfo, parsed refleks.File, partition, payloadHash string, traceSamplePoints int) rawRunParquetRow {
	traceCount := int32(len(parsed.MouseTrace))
	if parsed.Env.TracePoints > traceCount {
		traceCount = parsed.Env.TracePoints
	}

	scenario := scenarioName(parsed)
	metrics := computeTraceMetrics(parsed.MouseTrace)

	lastModifiedUnixMilli := int64(0)
	if !object.LastModified.IsZero() {
		lastModifiedUnixMilli = object.LastModified.UnixMilli()
	}

	return rawRunParquetRow{
		PartitionDate:               partition,
		SourceBucket:                sourceBucket,
		SourceObjectKey:             object.Key,
		SourceObjectETag:            object.ETag,
		SourceObjectSizeBytes:       object.SizeBytes,
		SourceLastModifiedUnixMilli: lastModifiedUnixMilli,
		PayloadSHA256:               payloadHash,
		FileName:                    parsed.FileName,
		EpochMilli:                  parsed.EpochMilli,
		FormatVersion:               int32(parsed.FormatVersion),
		ScenarioName:                scenario,
		SteamID:                     strings.TrimSpace(parsed.Env.SteamID),
		SteamUsername:               strings.TrimSpace(parsed.Env.PersonaName),
		Score:                       statFloat(parsed.Stats, "score"),
		Accuracy:                    statFloat(parsed.Stats, "accuracy"),
		AvgTTKSeconds:               firstStatFloat(parsed.Stats, "real avg ttk", "avg ttk"),
		DurationSeconds:             statFloat(parsed.Stats, "duration"),
		SensCM360:                   statFloat(parsed.Stats, "cm/360"),
		HasMouseTrace:               len(parsed.MouseTrace) > 0 || parsed.Env.TracePoints > 0,
		TracePointCount:             traceCount,
		MousePathDistance:           metrics.PathDistance,
		MouseNetDistance:            metrics.NetDistance,
		MousePathEfficiency:         metrics.PathEfficiency,
		MouseAvgSpeed:               metrics.AvgSpeed,
		MousePeakSpeed:              metrics.PeakSpeed,
		MouseMaxAccel:               metrics.MaxAccel,
		MouseDirectionChanges:       metrics.DirectionChanges,
		MouseClickTransitions:       metrics.ClickTransitions,
		TraceSampleJSON:             traceSampleJSON(parsed.MouseTrace, traceSamplePoints),
		StatsJSON:                   jsonString(parsed.Stats),
		EventsJSON:                  jsonString(parsed.Events),
		EnvAppVersion:               parsed.Env.AppVersion,
		EnvOS:                       parsed.Env.OS,
		EnvArch:                     parsed.Env.Arch,
		EnvOSVersion:                parsed.Env.OSVersion,
		EnvHostname:                 parsed.Env.Hostname,
		EnvCPUName:                  parsed.Env.CPUName,
		EnvCPUCores:                 parsed.Env.CPUCores,
		EnvGPUName:                  parsed.Env.GPUName,
		EnvRAMTotalMB:               parsed.Env.RAMTotalMB,
		EnvDisplayHz:                parsed.Env.DisplayHz,
		EnvScreenWidth:              parsed.Env.ScreenWidth,
		EnvScreenHeight:             parsed.Env.ScreenHeight,
		EnvIsWindowed:               parsed.Env.IsWindowed,
		EnvMouseName:                parsed.Env.MouseName,
		EnvMouseVID:                 parsed.Env.MouseVID,
		EnvMousePID:                 parsed.Env.MousePID,
		EnvMouseMI:                  parsed.Env.MouseMI,
		EnvMouseBackend:             parsed.Env.MouseBackend,
		EnvTracePoints:              parsed.Env.TracePoints,
		EnvTraceDuration:            parsed.Env.TraceDuration,
		EnvSampleRate:               parsed.Env.SampleRate,
	}
}

func buildStatRows(run rawRunParquetRow, stats map[string]any) []runStatParquetRow {
	if len(stats) == 0 {
		return nil
	}

	keys := make([]string, 0, len(stats))
	for key := range stats {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return strings.ToLower(keys[i]) < strings.ToLower(keys[j])
	})

	rows := make([]runStatParquetRow, 0, len(keys))
	for _, key := range keys {
		value := stats[key]
		row := runStatParquetRow{
			PartitionDate:   run.PartitionDate,
			SourceObjectKey: run.SourceObjectKey,
			FileName:        run.FileName,
			EpochMilli:      run.EpochMilli,
			ScenarioName:    run.ScenarioName,
			SteamID:         run.SteamID,
			SteamUsername:   run.SteamUsername,
			StatKey:         key,
			ValueType:       "other",
			ValueRawJSON:    jsonString(value),
		}

		switch typed := value.(type) {
		case string:
			row.ValueType = "string"
			row.ValueString = typed
		case bool:
			row.ValueType = "bool"
			row.ValueBool = typed
		case int:
			row.ValueType = "int"
			row.ValueInt = int64(typed)
		case int32:
			row.ValueType = "int"
			row.ValueInt = int64(typed)
		case int64:
			row.ValueType = "int"
			row.ValueInt = typed
		case uint:
			row.ValueType = "int"
			row.ValueInt = int64(typed)
		case uint32:
			row.ValueType = "int"
			row.ValueInt = int64(typed)
		case uint64:
			row.ValueType = "int"
			row.ValueInt = int64(typed)
		case float32:
			row.ValueType = "float"
			row.ValueFloat = float64(typed)
		case float64:
			row.ValueType = "float"
			row.ValueFloat = typed
		}

		rows = append(rows, row)
	}

	return rows
}

func buildEventRows(run rawRunParquetRow, epochMilli int64, fallback time.Time, events [][]string) ([]runEventParquetRow, []int64) {
	if len(events) == 0 {
		return nil, nil
	}

	baseTime := fallback.UTC()
	if epochMilli > 0 {
		baseTime = time.UnixMilli(epochMilli).UTC()
	}
	if baseTime.IsZero() {
		baseTime = time.Now().UTC()
	}

	rows := make([]runEventParquetRow, 0, len(events))
	times := make([]int64, 0, len(events))

	for index, rowData := range events {
		rawJSON := jsonString(rowData)

		timestampText := valueAt(rowData, 1)
		timestampUnixMilli, _ := parseEventTimestamp(baseTime, timestampText)

		row := runEventParquetRow{
			PartitionDate:           run.PartitionDate,
			SourceObjectKey:         run.SourceObjectKey,
			FileName:                run.FileName,
			EpochMilli:              run.EpochMilli,
			ScenarioName:            run.ScenarioName,
			SteamID:                 run.SteamID,
			SteamUsername:           run.SteamUsername,
			EventIndex:              int32(index),
			KillNumber:              parseInt64(valueAt(rowData, 0)),
			EventTimestampUnixMilli: timestampUnixMilli,
			EventTimestampText:      timestampText,
			Bot:                     valueAt(rowData, 2),
			Weapon:                  valueAt(rowData, 3),
			TTKSeconds:              parseFloat(valueAt(rowData, 4)),
			Shots:                   int32(parseInt64(valueAt(rowData, 5))),
			Hits:                    int32(parseInt64(valueAt(rowData, 6))),
			Accuracy:                parseFloat(valueAt(rowData, 7)),
			DamageDone:              parseFloat(valueAt(rowData, 8)),
			DamagePossible:          parseFloat(valueAt(rowData, 9)),
			Efficiency:              parseFloat(valueAt(rowData, 10)),
			Cheated:                 parseBool(valueAt(rowData, 11)),
			Overshots:               int32(parseInt64(valueAt(rowData, 12))),
			RawRowJSON:              rawJSON,
		}

		rows = append(rows, row)
		times = append(times, timestampUnixMilli)
	}

	return rows, times
}

func buildMouseSegments(run rawRunParquetRow, trace []refleks.MousePoint, eventTimes []int64, maxSegments int, sameSpotThresholdPx float64) []mouseSegmentParquetRow {
	if len(trace) < 2 || len(eventTimes) < 2 {
		return nil
	}

	rows := make([]mouseSegmentParquetRow, 0)
	hasPrevious := false
	var previousEndX int32
	var previousEndY int32
	for startEvent := 0; startEvent+1 < len(eventTimes); startEvent++ {
		if len(rows) >= maxSegments {
			break
		}

		startTS := eventTimes[startEvent]
		endTS := eventTimes[startEvent+1]
		if startTS <= 0 || endTS <= 0 || endTS <= startTS {
			continue
		}

		startIndex := nearestTraceIndex(trace, startTS)
		endIndex := nearestTraceIndex(trace, endTS)
		if startIndex < 0 || endIndex < 0 {
			continue
		}
		if endIndex < startIndex {
			startIndex, endIndex = endIndex, startIndex
		}
		if endIndex <= startIndex {
			continue
		}

		segmentTrace := trace[startIndex : endIndex+1]
		metrics := computeTraceMetrics(segmentTrace)

		startPoint := segmentTrace[0]
		endPoint := segmentTrace[len(segmentTrace)-1]
		duration := endPoint.TS - startPoint.TS
		if duration < 0 {
			duration = 0
		}

		deltaX := endPoint.X - startPoint.X
		deltaY := endPoint.Y - startPoint.Y
		segmentAngleDeg := segmentAngleDegrees(deltaX, deltaY)

		previousDeltaX := int32(0)
		previousDeltaY := int32(0)
		previousDistance := 0.0
		sameSpot := false
		sameSpotScore := 0.0
		if hasPrevious {
			previousDeltaX = endPoint.X - previousEndX
			previousDeltaY = endPoint.Y - previousEndY
			previousDistance = math.Hypot(float64(previousDeltaX), float64(previousDeltaY))
			sameSpot = previousDistance <= sameSpotThresholdPx
			sameSpotScore = boundedSimilarity(previousDistance, sameSpotThresholdPx)
		}

		signature := motionSignature(deltaX, deltaY, duration, metrics.PathEfficiency)

		rows = append(rows, mouseSegmentParquetRow{
			PartitionDate:         run.PartitionDate,
			SourceObjectKey:       run.SourceObjectKey,
			FileName:              run.FileName,
			EpochMilli:            run.EpochMilli,
			ScenarioName:          run.ScenarioName,
			SteamID:               run.SteamID,
			SteamUsername:         run.SteamUsername,
			SegmentIndex:          int32(len(rows)),
			StartEventIndex:       int32(startEvent),
			EndEventIndex:         int32(startEvent + 1),
			StartTS:               startPoint.TS,
			EndTS:                 endPoint.TS,
			DurationMs:            duration,
			StartX:                startPoint.X,
			StartY:                startPoint.Y,
			EndX:                  endPoint.X,
			EndY:                  endPoint.Y,
			DeltaX:                deltaX,
			DeltaY:                deltaY,
			SegmentAngleDeg:       segmentAngleDeg,
			LinearDistance:        metrics.NetDistance,
			PathDistance:          metrics.PathDistance,
			PathEfficiency:        metrics.PathEfficiency,
			MeanSpeed:             metrics.AvgSpeed,
			MaxSpeed:              metrics.PeakSpeed,
			MaxAccel:              metrics.MaxAccel,
			DirectionChanges:      metrics.DirectionChanges,
			ClickTransitions:      metrics.ClickTransitions,
			MotionSignature:       signature,
			HasPreviousSegment:    hasPrevious,
			PreviousEndDeltaX:     previousDeltaX,
			PreviousEndDeltaY:     previousDeltaY,
			PreviousEndDistancePx: previousDistance,
			SameSpotThresholdPx:   sameSpotThresholdPx,
			IsSameSpotAsPrevious:  sameSpot,
			SameSpotScore:         sameSpotScore,
		})

		hasPrevious = true
		previousEndX = endPoint.X
		previousEndY = endPoint.Y
	}

	return rows
}

func segmentAngleDegrees(deltaX, deltaY int32) float64 {
	if deltaX == 0 && deltaY == 0 {
		return 0
	}
	return math.Atan2(float64(deltaY), float64(deltaX)) * 180 / math.Pi
}

func boundedSimilarity(distance, threshold float64) float64 {
	if threshold <= 0 {
		return 0
	}
	if distance <= 0 {
		return 1
	}
	if distance >= threshold {
		return 0
	}
	return 1 - (distance / threshold)
}

func motionSignature(deltaX, deltaY int32, durationMs int64, pathEfficiency float64) string {
	bucketedDX := int(math.Round(float64(deltaX) / 40.0))
	bucketedDY := int(math.Round(float64(deltaY) / 40.0))
	bucketedDuration := int(math.Round(float64(durationMs) / 75.0))
	bucketedEfficiency := int(math.Round(pathEfficiency * 10))
	return fmt.Sprintf("dx%d_dy%d_dt%d_e%d", bucketedDX, bucketedDY, bucketedDuration, bucketedEfficiency)
}

func nearestTraceIndex(points []refleks.MousePoint, targetTS int64) int {
	if len(points) == 0 {
		return -1
	}

	index := sort.Search(len(points), func(i int) bool {
		return points[i].TS >= targetTS
	})

	if index == 0 {
		return 0
	}
	if index >= len(points) {
		return len(points) - 1
	}

	leftDelta := absInt64(points[index-1].TS - targetTS)
	rightDelta := absInt64(points[index].TS - targetTS)
	if leftDelta <= rightDelta {
		return index - 1
	}
	return index
}

func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}

func computeTraceMetrics(points []refleks.MousePoint) traceMetrics {
	if len(points) < 2 {
		return traceMetrics{}
	}

	metrics := traceMetrics{}
	start := points[0]
	end := points[len(points)-1]
	metrics.NetDistance = math.Hypot(float64(end.X-start.X), float64(end.Y-start.Y))

	totalSeconds := 0.0
	previousSpeed := 0.0
	hasPreviousSpeed := false
	previousDX := 0.0
	previousDY := 0.0
	hasPreviousDirection := false

	for i := 1; i < len(points); i++ {
		dx := float64(points[i].X - points[i-1].X)
		dy := float64(points[i].Y - points[i-1].Y)
		stepDistance := math.Hypot(dx, dy)
		metrics.PathDistance += stepDistance

		if points[i].Buttons != points[i-1].Buttons {
			metrics.ClickTransitions++
		}

		if stepDistance > 0 {
			if hasPreviousDirection {
				prevLength := math.Hypot(previousDX, previousDY)
				curLength := math.Hypot(dx, dy)
				if prevLength > 0 && curLength > 0 {
					cosine := (previousDX*dx + previousDY*dy) / (prevLength * curLength)
					if cosine < directionChangeCosBoundary {
						metrics.DirectionChanges++
					}
				}
			}
			previousDX = dx
			previousDY = dy
			hasPreviousDirection = true
		}

		dtMillis := points[i].TS - points[i-1].TS
		if dtMillis <= 0 {
			continue
		}
		dtSeconds := float64(dtMillis) / 1000.0
		totalSeconds += dtSeconds

		speed := stepDistance / dtSeconds
		if speed > metrics.PeakSpeed {
			metrics.PeakSpeed = speed
		}

		if hasPreviousSpeed {
			accel := math.Abs(speed-previousSpeed) / dtSeconds
			if accel > metrics.MaxAccel {
				metrics.MaxAccel = accel
			}
		}
		previousSpeed = speed
		hasPreviousSpeed = true
	}

	if totalSeconds > 0 {
		metrics.AvgSpeed = metrics.PathDistance / totalSeconds
	}
	if metrics.PathDistance > 0 {
		metrics.PathEfficiency = metrics.NetDistance / metrics.PathDistance
	}

	return metrics
}

func traceSampleJSON(points []refleks.MousePoint, maxPoints int) string {
	if len(points) == 0 {
		return "[]"
	}
	if maxPoints <= 0 {
		maxPoints = defaultTraceSamplePoints
	}

	samples := sampleTrace(points, maxPoints)
	return jsonString(samples)
}

func sampleTrace(points []refleks.MousePoint, maxPoints int) []traceSamplePoint {
	if len(points) == 0 {
		return nil
	}
	if len(points) <= maxPoints {
		out := make([]traceSamplePoint, 0, len(points))
		for _, point := range points {
			out = append(out, traceSamplePoint{TS: point.TS, X: point.X, Y: point.Y, Buttons: point.Buttons})
		}
		return out
	}

	if maxPoints == 1 {
		point := points[0]
		return []traceSamplePoint{{TS: point.TS, X: point.X, Y: point.Y, Buttons: point.Buttons}}
	}

	out := make([]traceSamplePoint, 0, maxPoints)
	lastIndex := -1
	for i := 0; i < maxPoints; i++ {
		position := float64(i) * float64(len(points)-1) / float64(maxPoints-1)
		index := int(math.Round(position))
		if index == lastIndex {
			continue
		}
		if index >= len(points) {
			index = len(points) - 1
		}
		point := points[index]
		out = append(out, traceSamplePoint{TS: point.TS, X: point.X, Y: point.Y, Buttons: point.Buttons})
		lastIndex = index
	}

	if len(out) == 0 {
		point := points[0]
		return []traceSamplePoint{{TS: point.TS, X: point.X, Y: point.Y, Buttons: point.Buttons}}
	}

	return out
}

func scenarioName(parsed refleks.File) string {
	if value, ok := statValue(parsed.Stats, "scenario"); ok {
		if scenario := strings.TrimSpace(fmt.Sprintf("%v", value)); scenario != "" {
			return scenario
		}
	}
	return strings.TrimSpace(parsed.FileName)
}

func statFloat(stats map[string]any, key string) float64 {
	value, ok := statValue(stats, key)
	if !ok {
		return 0
	}
	return numberValue(value)
}

func firstStatFloat(stats map[string]any, keys ...string) float64 {
	for _, key := range keys {
		if value, ok := statValue(stats, key); ok {
			return numberValue(value)
		}
	}
	return 0
}

func statValue(stats map[string]any, key string) (any, bool) {
	if len(stats) == 0 {
		return nil, false
	}
	wanted := strings.ToLower(strings.TrimSpace(key))
	for statKey, value := range stats {
		if strings.ToLower(strings.TrimSpace(statKey)) == wanted {
			return value, true
		}
	}
	return nil, false
}

func numberValue(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int32:
		return float64(typed)
	case int64:
		return float64(typed)
	case uint:
		return float64(typed)
	case uint32:
		return float64(typed)
	case uint64:
		return float64(typed)
	case string:
		return parseFloat(typed)
	default:
		return 0
	}
}

func valueAt(values []string, index int) string {
	if index < 0 || index >= len(values) {
		return ""
	}
	return strings.TrimSpace(values[index])
}

func parseInt64(raw string) int64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err == nil {
		return value
	}
	if floatValue, err := strconv.ParseFloat(raw, 64); err == nil {
		return int64(floatValue)
	}
	return 0
}

func parseFloat(raw string) float64 {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimSuffix(raw, "s")
	raw = strings.TrimSuffix(raw, "%")
	if raw == "" {
		return 0
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0
	}
	return value
}

func parseBool(raw string) bool {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return false
	}
	if raw == "1" {
		return true
	}
	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		return false
	}
	return parsed
}

func parseEventTimestamp(base time.Time, raw string) (int64, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}

	if absolute, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return absolute.UTC().UnixMilli(), true
	}

	layouts := []string{
		"15:04:05.000000",
		"15:04:05.000",
		"15:04:05",
	}

	for _, layout := range layouts {
		parsed, err := time.ParseInLocation(layout, raw, time.UTC)
		if err != nil {
			continue
		}

		candidate := time.Date(base.Year(), base.Month(), base.Day(), parsed.Hour(), parsed.Minute(), parsed.Second(), parsed.Nanosecond(), time.UTC)
		if candidate.After(base.Add(12 * time.Hour)) {
			candidate = candidate.AddDate(0, 0, -1)
		}
		if candidate.Before(base.Add(-12 * time.Hour)) {
			candidate = candidate.AddDate(0, 0, 1)
		}

		return candidate.UnixMilli(), true
	}

	return 0, false
}

func jsonString(value any) string {
	if value == nil {
		return "null"
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "null"
	}
	return string(encoded)
}

func sha256Hex(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (s *Service) listSourceObjects(ctx context.Context, cutoff time.Time) ([]r2.ObjectInfo, error) {
	objects := make([]r2.ObjectInfo, 0)
	seen := make(map[string]struct{})
	continuationToken := ""

	for {
		page, nextToken, err := s.sourceStore.List(ctx, s.sourcePrefix, continuationToken, s.sourceListPage)
		if err != nil {
			return nil, fmt.Errorf("list source objects: %w", err)
		}

		for _, object := range page {
			key := strings.TrimSpace(object.Key)
			if key == "" {
				continue
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}

			if !strings.HasSuffix(strings.ToLower(key), ".refleks") {
				continue
			}
			if !object.LastModified.IsZero() && object.LastModified.Before(cutoff) {
				continue
			}

			objects = append(objects, object)
		}

		if nextToken == "" {
			break
		}
		continuationToken = nextToken
	}

	sort.Slice(objects, func(i, j int) bool {
		left := objects[i]
		right := objects[j]

		if left.LastModified.Equal(right.LastModified) {
			return left.Key < right.Key
		}
		if left.LastModified.IsZero() {
			return false
		}
		if right.LastModified.IsZero() {
			return true
		}
		return left.LastModified.Before(right.LastModified)
	})

	return objects, nil
}

func (s *Service) loadScenarioLeaderboardRows(ctx context.Context, snapshotDate string) ([]scenarioLeaderboardParquetRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			sl.scenario_id,
			s.scenario_name,
			sl.account_id,
			COALESCE(a.steam_id, ''),
			COALESCE(a.steam_username, ''),
			sl.rank,
			sl.best_score,
			COALESCE(sl.best_epoch_milli, 0)
		FROM scenario_leaderboard_current sl
		JOIN scenarios s ON s.id = sl.scenario_id
		JOIN accounts a ON a.id = sl.account_id
		ORDER BY sl.scenario_id ASC, sl.rank ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query scenario leaderboard for parquet export: %w", err)
	}
	defer rows.Close()

	result := make([]scenarioLeaderboardParquetRow, 0)
	for rows.Next() {
		var row scenarioLeaderboardParquetRow
		row.SnapshotDate = snapshotDate
		if err := rows.Scan(
			&row.ScenarioID,
			&row.ScenarioName,
			&row.AccountID,
			&row.SteamID,
			&row.SteamUsername,
			&row.Rank,
			&row.BestScore,
			&row.BestEpochMilli,
		); err != nil {
			return nil, fmt.Errorf("scan scenario leaderboard parquet row: %w", err)
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate scenario leaderboard parquet rows: %w", err)
	}

	return result, nil
}

func (s *Service) loadBenchmarkLeaderboardRows(ctx context.Context, snapshotDate string) ([]benchmarkLeaderboardParquetRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			bcl.difficulty_id,
			b.benchmark_name,
			bd.difficulty_name,
			bd.kovaaks_benchmark_id,
			bcl.account_id,
			COALESCE(a.steam_id, ''),
			COALESCE(a.steam_username, ''),
			bcl.rank,
			bcl.composite_score,
			bcl.matched_scenarios,
			COALESCE(bcl.last_epoch_milli, 0)
		FROM benchmark_difficulty_leaderboard_current bcl
		JOIN benchmark_difficulties bd ON bd.id = bcl.difficulty_id
		JOIN benchmarks b ON b.id = bd.benchmark_id
		JOIN accounts a ON a.id = bcl.account_id
		ORDER BY bcl.difficulty_id ASC, bcl.rank ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query benchmark leaderboard for parquet export: %w", err)
	}
	defer rows.Close()

	result := make([]benchmarkLeaderboardParquetRow, 0)
	for rows.Next() {
		var row benchmarkLeaderboardParquetRow
		row.SnapshotDate = snapshotDate
		if err := rows.Scan(
			&row.DifficultyID,
			&row.BenchmarkName,
			&row.DifficultyName,
			&row.KovaaksBenchmarkID,
			&row.AccountID,
			&row.SteamID,
			&row.SteamUsername,
			&row.Rank,
			&row.CompositeScore,
			&row.MatchedScenarios,
			&row.LastEpochMilli,
		); err != nil {
			return nil, fmt.Errorf("scan benchmark leaderboard parquet row: %w", err)
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate benchmark leaderboard parquet rows: %w", err)
	}

	return result, nil
}

func encodeParquet[T any](rows []T) ([]byte, error) {
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[T](&buf)
	if len(rows) > 0 {
		if _, err := writer.Write(rows); err != nil {
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
