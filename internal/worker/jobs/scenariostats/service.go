package scenariostats

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"refleks-worker/internal/worker"
)

const (
	jobName                = "scenario_stats_refresh"
	distributionVersion    = 1
	histogramBinCount      = 32
	outlierLowerPercentile = 0.01
	outlierUpperPercentile = 0.99
)

const refreshScenarioStatsSQL = `
	UPDATE scenarios
	SET
		run_count = $2,
		score_sample_count = $3,
		score_distribution = $4::jsonb,
		sens_sample_count = $5,
		sens_distribution = $6::jsonb,
		updated_at = NOW()
	WHERE id = $1
`

type distributionSnapshot struct {
	Version     int                   `json:"version"`
	SampleCount int64                 `json:"sampleCount"`
	Min         *float64              `json:"min,omitempty"`
	Max         *float64              `json:"max,omitempty"`
	Quantiles   distributionQuantiles `json:"quantiles"`
	Histogram   distributionHistogram `json:"histogram"`
	Outliers    distributionOutliers  `json:"outliers"`
}

type distributionQuantiles struct {
	P01 *float64 `json:"p01,omitempty"`
	P05 *float64 `json:"p05,omitempty"`
	P10 *float64 `json:"p10,omitempty"`
	P25 *float64 `json:"p25,omitempty"`
	P50 *float64 `json:"p50,omitempty"`
	P75 *float64 `json:"p75,omitempty"`
	P90 *float64 `json:"p90,omitempty"`
	P95 *float64 `json:"p95,omitempty"`
	P99 *float64 `json:"p99,omitempty"`
}

type distributionHistogram struct {
	LowerBound *float64          `json:"lowerBound,omitempty"`
	UpperBound *float64          `json:"upperBound,omitempty"`
	BinCount   int               `json:"binCount"`
	Bins       []distributionBin `json:"bins,omitempty"`
}

type distributionBin struct {
	Lower float64 `json:"lower"`
	Upper float64 `json:"upper"`
	Count int64   `json:"count"`
}

type distributionOutliers struct {
	Method          string   `json:"method"`
	LowerPercentile float64  `json:"lowerPercentile"`
	UpperPercentile float64  `json:"upperPercentile"`
	LowerThreshold  *float64 `json:"lowerThreshold,omitempty"`
	UpperThreshold  *float64 `json:"upperThreshold,omitempty"`
	LowerCount      int64    `json:"lowerCount"`
	UpperCount      int64    `json:"upperCount"`
}

type scenarioStatsUpdate struct {
	ScenarioID            int64
	RunCount              int64
	ScoreSampleCount      int64
	ScoreDistributionJSON string
	SensSampleCount       int64
	SensDistributionJSON  string
}

// Service refreshes cached scenario distributions.
type Service struct {
	pool *pgxpool.Pool
}

// NewService creates a scenario stats refresh service.
func NewService(pool *pgxpool.Pool) (*Service, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool is required")
	}

	return &Service{pool: pool}, nil
}

// Name returns the job name.
func (s *Service) Name() string {
	return jobName
}

// Run recomputes score and sensitivity distributions for every scenario.
func (s *Service) Run(ctx context.Context) (worker.Result, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return worker.Result{}, fmt.Errorf("begin scenario stats transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT
			s.id,
			r.id,
			r.score,
			r.sens_cm360
		FROM scenarios s
		LEFT JOIN runs r ON r.scenario_id = s.id
		ORDER BY s.id ASC, r.id ASC
	`)
	if err != nil {
		return worker.Result{}, fmt.Errorf("query scenario runs for stats refresh: %w", err)
	}
	defer rows.Close()

	updates := make([]scenarioStatsUpdate, 0)
	scoreValues := make([]float64, 0)
	sensValues := make([]float64, 0)

	var (
		currentScenarioID int64
		haveScenario      bool
		runCount          int64
		totalRuns         int64
		totalScoreSamples int64
		totalSensSamples  int64
	)

	flush := func() error {
		if !haveScenario {
			return nil
		}

		update, err := buildScenarioStatsUpdate(currentScenarioID, runCount, scoreValues, sensValues)
		if err != nil {
			return err
		}

		updates = append(updates, update)
		totalScoreSamples += update.ScoreSampleCount
		totalSensSamples += update.SensSampleCount

		runCount = 0
		scoreValues = scoreValues[:0]
		sensValues = sensValues[:0]
		return nil
	}

	for rows.Next() {
		var (
			scenarioID int64
			runID      sql.NullInt64
			score      sql.NullFloat64
			sens       sql.NullFloat64
		)

		if err := rows.Scan(&scenarioID, &runID, &score, &sens); err != nil {
			return worker.Result{}, fmt.Errorf("scan scenario stats source row: %w", err)
		}

		if !haveScenario {
			currentScenarioID = scenarioID
			haveScenario = true
		}

		if scenarioID != currentScenarioID {
			if err := flush(); err != nil {
				return worker.Result{}, err
			}
			currentScenarioID = scenarioID
		}

		if runID.Valid {
			runCount++
			totalRuns++
		}
		if score.Valid {
			scoreValues = append(scoreValues, score.Float64)
		}
		if sens.Valid {
			sensValues = append(sensValues, sens.Float64)
		}
	}

	if err := rows.Err(); err != nil {
		return worker.Result{}, fmt.Errorf("iterate scenario stats source rows: %w", err)
	}
	rows.Close()

	if err := flush(); err != nil {
		return worker.Result{}, err
	}

	updatedRows := int64(0)
	for _, update := range updates {
		tag, err := tx.Exec(
			ctx,
			refreshScenarioStatsSQL,
			update.ScenarioID,
			update.RunCount,
			update.ScoreSampleCount,
			update.ScoreDistributionJSON,
			update.SensSampleCount,
			update.SensDistributionJSON,
		)
		if err != nil {
			return worker.Result{}, fmt.Errorf("update cached stats for scenario %d: %w", update.ScenarioID, err)
		}
		updatedRows += tag.RowsAffected()
	}

	if err := tx.Commit(ctx); err != nil {
		return worker.Result{}, fmt.Errorf("commit scenario stats refresh: %w", err)
	}

	return worker.Result{
		Status: worker.OutcomeSuccess,
		Message: fmt.Sprintf(
			"refreshed cached distributions for %d scenarios from %d runs",
			updatedRows,
			totalRuns,
		),
		Details: map[string]any{
			"scenarioRows":      updatedRows,
			"runsScanned":       totalRuns,
			"scoreSampleCount":  totalScoreSamples,
			"sensSampleCount":   totalSensSamples,
			"histogramBinCount": histogramBinCount,
		},
	}, nil
}

func buildScenarioStatsUpdate(scenarioID, runCount int64, scoreValues, sensValues []float64) (scenarioStatsUpdate, error) {
	scoreDistribution := buildMetricSummary(scoreValues)
	sensDistribution := buildMetricSummary(sensValues)

	scoreDistributionJSON, err := json.Marshal(scoreDistribution)
	if err != nil {
		return scenarioStatsUpdate{}, fmt.Errorf("marshal score distribution for scenario %d: %w", scenarioID, err)
	}

	sensDistributionJSON, err := json.Marshal(sensDistribution)
	if err != nil {
		return scenarioStatsUpdate{}, fmt.Errorf("marshal sensitivity distribution for scenario %d: %w", scenarioID, err)
	}

	return scenarioStatsUpdate{
		ScenarioID:            scenarioID,
		RunCount:              runCount,
		ScoreSampleCount:      scoreDistribution.SampleCount,
		ScoreDistributionJSON: string(scoreDistributionJSON),
		SensSampleCount:       sensDistribution.SampleCount,
		SensDistributionJSON:  string(sensDistributionJSON),
	}, nil
}

func buildMetricSummary(values []float64) distributionSnapshot {
	sortedValues := append([]float64(nil), values...)
	sort.Float64s(sortedValues)

	distribution := distributionSnapshot{
		Version:     distributionVersion,
		SampleCount: int64(len(sortedValues)),
		Outliers: distributionOutliers{
			Method:          "percentile_clip",
			LowerPercentile: outlierLowerPercentile,
			UpperPercentile: outlierUpperPercentile,
		},
	}

	if len(sortedValues) == 0 {
		return distribution
	}

	minValue := sortedValues[0]
	maxValue := sortedValues[len(sortedValues)-1]
	distribution.Min = float64Ptr(minValue)
	distribution.Max = float64Ptr(maxValue)
	distribution.Quantiles = distributionQuantiles{
		P01: percentileCont(sortedValues, 0.01),
		P05: percentileCont(sortedValues, 0.05),
		P10: percentileCont(sortedValues, 0.10),
		P25: percentileCont(sortedValues, 0.25),
		P50: percentileCont(sortedValues, 0.50),
		P75: percentileCont(sortedValues, 0.75),
		P90: percentileCont(sortedValues, 0.90),
		P95: percentileCont(sortedValues, 0.95),
		P99: percentileCont(sortedValues, 0.99),
	}

	lowerBound := distribution.Quantiles.P01
	upperBound := distribution.Quantiles.P99
	distribution.Outliers.LowerThreshold = lowerBound
	distribution.Outliers.UpperThreshold = upperBound
	distribution.Histogram.LowerBound = lowerBound
	distribution.Histogram.UpperBound = upperBound

	if lowerBound == nil || upperBound == nil {
		return distribution
	}

	if *lowerBound == *upperBound {
		distribution.Histogram.BinCount = 1
		distribution.Histogram.Bins = []distributionBin{{
			Lower: *lowerBound,
			Upper: *upperBound,
			Count: int64(len(sortedValues)),
		}}
		return distribution
	}

	width := (*upperBound - *lowerBound) / float64(histogramBinCount)
	bins := make([]distributionBin, histogramBinCount)
	for i := range bins {
		lower := *lowerBound + width*float64(i)
		upper := lower + width
		if i == len(bins)-1 {
			upper = *upperBound
		}
		bins[i] = distributionBin{Lower: lower, Upper: upper}
	}

	for _, value := range sortedValues {
		switch {
		case value < *lowerBound:
			distribution.Outliers.LowerCount++
		case value > *upperBound:
			distribution.Outliers.UpperCount++
		default:
			index := len(bins) - 1
			if value < *upperBound {
				index = int(math.Floor((value - *lowerBound) / width))
				if index < 0 {
					index = 0
				}
				if index >= len(bins) {
					index = len(bins) - 1
				}
			}
			bins[index].Count++
		}
	}

	distribution.Histogram.BinCount = len(bins)
	distribution.Histogram.Bins = bins

	return distribution
}

func percentileCont(sortedValues []float64, percentile float64) *float64 {
	if len(sortedValues) == 0 {
		return nil
	}
	if percentile <= 0 {
		return float64Ptr(sortedValues[0])
	}
	if percentile >= 1 {
		return float64Ptr(sortedValues[len(sortedValues)-1])
	}

	position := percentile * float64(len(sortedValues)-1)
	lowerIndex := int(math.Floor(position))
	upperIndex := int(math.Ceil(position))
	if lowerIndex == upperIndex {
		return float64Ptr(sortedValues[lowerIndex])
	}

	fraction := position - float64(lowerIndex)
	value := sortedValues[lowerIndex] + (sortedValues[upperIndex]-sortedValues[lowerIndex])*fraction
	return float64Ptr(value)
}

func float64Ptr(value float64) *float64 {
	v := value
	return &v
}
