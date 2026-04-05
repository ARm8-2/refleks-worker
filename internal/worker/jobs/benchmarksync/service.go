package benchmarksync

import (
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"refleks-worker/internal/worker"
	"refleks-worker/internal/worker/state"
)

const (
	jobName                  = "benchmark_sync"
	stateKeyFile             = "source_file"
	maxSourceFileBytes       = 100 << 20
	maxProgressBodyBytes     = 10 << 20
	kovaaksPlayerProgressURL = "https://kovaaks.com/webapp-backend/benchmarks/player-progress-rank-benchmark?benchmarkId=%d&steamId=%s"
)

// Config controls benchmark sync behavior.
type Config struct {
	SourcePath string
}

// Service syncs benchmark definitions from a source JSON file into Postgres.
type Service struct {
	logger     *slog.Logger
	pool       *pgxpool.Pool
	stateStore *state.Store
	sourcePath string
	httpClient *http.Client
}

type sourceFingerprint struct {
	Path      string    `json:"path"`
	Hash      string    `json:"hash"`
	SizeBytes int64     `json:"sizeBytes"`
	ModTime   time.Time `json:"modTime"`
	SyncedAt  time.Time `json:"syncedAt"`
}

type syncSummary struct {
	Benchmarks    int
	Difficulties  int
	RankDefs      int
	Categories    int
	Subcategories int
	ScenarioLinks int
}

// NewService creates a benchmark sync service.
func NewService(logger *slog.Logger, pool *pgxpool.Pool, stateStore *state.Store, cfg Config) (*Service, error) {
	sourcePath := strings.TrimSpace(cfg.SourcePath)
	if sourcePath == "" {
		return nil, fmt.Errorf("benchmark sync source path is required")
	}
	if pool == nil {
		return nil, fmt.Errorf("database pool is required")
	}
	if stateStore == nil {
		return nil, fmt.Errorf("state store is required")
	}

	return &Service{
		logger:     logger,
		pool:       pool,
		stateStore: stateStore,
		sourcePath: sourcePath,
		httpClient: &http.Client{Timeout: 20 * time.Second},
	}, nil
}

// Name returns the job name.
func (s *Service) Name() string {
	return jobName
}

// Run executes one benchmark sync.
func (s *Service) Run(ctx context.Context) (worker.Result, error) {
	raw, fingerprint, err := s.readSource()
	if err != nil {
		return worker.Result{}, err
	}

	var previous sourceFingerprint
	hasPrevious, err := s.stateStore.GetJSON(ctx, s.Name(), stateKeyFile, &previous)
	if err != nil {
		return worker.Result{}, fmt.Errorf("load previous benchmark sync state: %w", err)
	}

	if hasPrevious && previous.Hash == fingerprint.Hash {
		return worker.Result{
			Status:  worker.OutcomeSkipped,
			Message: "benchmark source file unchanged",
			Details: map[string]any{
				"path": previous.Path,
				"hash": previous.Hash,
			},
		}, nil
	}

	benchmarks, err := parseBenchmarks(raw)
	if err != nil {
		return worker.Result{}, err
	}
	if err := s.enrichBenchmarks(ctx, benchmarks); err != nil {
		return worker.Result{}, err
	}

	summary, err := s.syncToDatabase(ctx, benchmarks, fingerprint)
	if err != nil {
		return worker.Result{}, err
	}

	fingerprint.SyncedAt = time.Now().UTC()
	if err := s.stateStore.SetJSON(ctx, s.Name(), stateKeyFile, fingerprint); err != nil {
		return worker.Result{}, fmt.Errorf("persist benchmark sync fingerprint: %w", err)
	}

	return worker.Result{
		Status: worker.OutcomeSuccess,
		Message: fmt.Sprintf(
			"synced %d benchmarks, %d difficulties, %d rank defs, %d scenario links",
			summary.Benchmarks,
			summary.Difficulties,
			summary.RankDefs,
			summary.ScenarioLinks,
		),
		Details: map[string]any{
			"path":          fingerprint.Path,
			"hash":          fingerprint.Hash,
			"benchmarks":    summary.Benchmarks,
			"difficulties":  summary.Difficulties,
			"rankDefs":      summary.RankDefs,
			"categories":    summary.Categories,
			"subcategories": summary.Subcategories,
			"scenarioLinks": summary.ScenarioLinks,
		},
	}, nil
}

func (s *Service) readSource() ([]byte, sourceFingerprint, error) {
	info, err := os.Stat(s.sourcePath)
	if err != nil {
		return nil, sourceFingerprint{}, fmt.Errorf("stat benchmark source file %q: %w", s.sourcePath, err)
	}
	if info.IsDir() {
		return nil, sourceFingerprint{}, fmt.Errorf("benchmark source path %q is a directory", s.sourcePath)
	}
	if info.Size() > maxSourceFileBytes {
		return nil, sourceFingerprint{}, fmt.Errorf("benchmark source file %q exceeds %d bytes", s.sourcePath, maxSourceFileBytes)
	}

	raw, err := os.ReadFile(s.sourcePath)
	if err != nil {
		return nil, sourceFingerprint{}, fmt.Errorf("read benchmark source file %q: %w", s.sourcePath, err)
	}
	if len(raw) == 0 {
		return nil, sourceFingerprint{}, fmt.Errorf("benchmark source file %q is empty", s.sourcePath)
	}

	sum := sha256.Sum256(raw)
	fingerprint := sourceFingerprint{
		Path:      s.sourcePath,
		Hash:      hex.EncodeToString(sum[:]),
		SizeBytes: info.Size(),
		ModTime:   info.ModTime().UTC(),
	}

	return raw, fingerprint, nil
}

func parseBenchmarks(raw []byte) ([]sourceBenchmark, error) {
	var benchmarks []sourceBenchmark
	if err := json.Unmarshal(raw, &benchmarks); err != nil {
		return nil, fmt.Errorf("parse benchmark source json: %w", err)
	}
	if len(benchmarks) == 0 {
		return nil, fmt.Errorf("benchmark source json contains no benchmarks")
	}

	seenBenchmarkNames := make(map[string]struct{}, len(benchmarks))
	seenDifficultyIDs := make(map[int64]string)

	for i := range benchmarks {
		benchmark := &benchmarks[i]
		benchmark.BenchmarkName = strings.TrimSpace(benchmark.BenchmarkName)
		benchmark.Abbreviation = strings.TrimSpace(benchmark.Abbreviation)
		benchmark.RankCalculation = strings.TrimSpace(benchmark.RankCalculation)
		benchmark.Color = strings.TrimSpace(benchmark.Color)
		benchmark.SpreadsheetURL = strings.TrimSpace(benchmark.SpreadsheetURL)
		benchmark.DateAdded = strings.TrimSpace(benchmark.DateAdded)

		if benchmark.BenchmarkName == "" {
			return nil, fmt.Errorf("benchmark at index %d has empty benchmarkName", i)
		}
		if _, exists := seenBenchmarkNames[benchmark.BenchmarkName]; exists {
			return nil, fmt.Errorf("duplicate benchmarkName %q", benchmark.BenchmarkName)
		}
		seenBenchmarkNames[benchmark.BenchmarkName] = struct{}{}

		for j := range benchmark.Difficulties {
			difficulty := &benchmark.Difficulties[j]
			difficulty.DifficultyName = strings.TrimSpace(difficulty.DifficultyName)
			difficulty.Sharecode = strings.TrimSpace(difficulty.Sharecode)
			if difficulty.DifficultyName == "" {
				return nil, fmt.Errorf("benchmark %q has difficulty with empty difficultyName", benchmark.BenchmarkName)
			}
			if difficulty.KovaaksBenchmarkID <= 0 {
				return nil, fmt.Errorf("benchmark %q difficulty %q has invalid kovaaksBenchmarkId", benchmark.BenchmarkName, difficulty.DifficultyName)
			}
			if owner, exists := seenDifficultyIDs[difficulty.KovaaksBenchmarkID]; exists {
				return nil, fmt.Errorf("duplicate kovaaksBenchmarkId %d used by %q and %q", difficulty.KovaaksBenchmarkID, owner, benchmark.BenchmarkName+"/"+difficulty.DifficultyName)
			}
			seenDifficultyIDs[difficulty.KovaaksBenchmarkID] = benchmark.BenchmarkName + "/" + difficulty.DifficultyName

			for k := range difficulty.Categories {
				category := &difficulty.Categories[k]
				category.CategoryName = strings.TrimSpace(category.CategoryName)
				category.Color = strings.TrimSpace(category.Color)

				for m := range category.Subcategories {
					subcategory := &category.Subcategories[m]
					subcategory.SubcategoryName = strings.TrimSpace(subcategory.SubcategoryName)
					subcategory.Color = strings.TrimSpace(subcategory.Color)
					if subcategory.ScenarioCount < 0 {
						return nil, fmt.Errorf("benchmark %q difficulty %q category %q subcategory %q has negative scenarioCount", benchmark.BenchmarkName, difficulty.DifficultyName, category.CategoryName, subcategory.SubcategoryName)
					}
				}
			}
		}
	}

	return benchmarks, nil
}

func (s *Service) syncToDatabase(ctx context.Context, benchmarks []sourceBenchmark, fingerprint sourceFingerprint) (syncSummary, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return syncSummary{}, fmt.Errorf("begin benchmark sync transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	benchmarkNames := make([]string, 0, len(benchmarks))
	for i := range benchmarks {
		benchmarkNames = append(benchmarkNames, benchmarks[i].BenchmarkName)
	}

	if _, err := tx.Exec(ctx, `
		DELETE FROM benchmarks
		WHERE NOT (benchmark_name = ANY($1::text[]))
	`, benchmarkNames); err != nil {
		return syncSummary{}, fmt.Errorf("delete removed benchmarks: %w", err)
	}

	summary := syncSummary{}
	for benchmarkIndex := range benchmarks {
		benchmark := benchmarks[benchmarkIndex]

		dateAdded, err := parseDate(benchmark.DateAdded)
		if err != nil {
			return syncSummary{}, fmt.Errorf("benchmark %q: %w", benchmark.BenchmarkName, err)
		}

		var benchmarkID int64
		err = tx.QueryRow(ctx, `
			INSERT INTO benchmarks (
				benchmark_name,
				abbreviation,
				rank_calculation,
				color,
				spreadsheet_url,
				date_added,
				source_file,
				source_hash,
				updated_at
			)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
			ON CONFLICT (benchmark_name) DO UPDATE SET
				abbreviation = EXCLUDED.abbreviation,
				rank_calculation = EXCLUDED.rank_calculation,
				color = EXCLUDED.color,
				spreadsheet_url = EXCLUDED.spreadsheet_url,
				date_added = EXCLUDED.date_added,
				source_file = EXCLUDED.source_file,
				source_hash = EXCLUDED.source_hash,
				updated_at = NOW()
			RETURNING id
		`,
			benchmark.BenchmarkName,
			benchmark.Abbreviation,
			benchmark.RankCalculation,
			benchmark.Color,
			benchmark.SpreadsheetURL,
			dateAdded,
			fingerprint.Path,
			fingerprint.Hash,
		).Scan(&benchmarkID)
		if err != nil {
			return syncSummary{}, fmt.Errorf("upsert benchmark %q: %w", benchmark.BenchmarkName, err)
		}
		summary.Benchmarks++

		// Rebuild hierarchy for deterministic sync behavior.
		if _, err := tx.Exec(ctx, `DELETE FROM benchmark_difficulties WHERE benchmark_id = $1`, benchmarkID); err != nil {
			return syncSummary{}, fmt.Errorf("clear benchmark hierarchy %q: %w", benchmark.BenchmarkName, err)
		}

		for difficultyIndex := range benchmark.Difficulties {
			difficulty := benchmark.Difficulties[difficultyIndex]

			var difficultyID int64
			err = tx.QueryRow(ctx, `
				INSERT INTO benchmark_difficulties (
					benchmark_id,
					difficulty_name,
					kovaaks_benchmark_id,
					sharecode,
					sort_order,
					updated_at
				)
				VALUES ($1,$2,$3,$4,$5,NOW())
				RETURNING id
			`,
				benchmarkID,
				difficulty.DifficultyName,
				difficulty.KovaaksBenchmarkID,
				difficulty.Sharecode,
				difficultyIndex,
			).Scan(&difficultyID)
			if err != nil {
				return syncSummary{}, fmt.Errorf("insert difficulty %q/%q: %w", benchmark.BenchmarkName, difficulty.DifficultyName, err)
			}
			summary.Difficulties++

			for rankIndex := range difficulty.MergedRanks {
				rank := difficulty.MergedRanks[rankIndex]
				if _, err := tx.Exec(ctx, `
					INSERT INTO benchmark_difficulty_ranks (
						difficulty_id,
						rank_name,
						rank_color,
						sort_order,
						updated_at
					)
					VALUES ($1,$2,$3,$4,NOW())
				`,
					difficultyID,
					rank.Name,
					rank.Color,
					rankIndex,
				); err != nil {
					return syncSummary{}, fmt.Errorf("insert rank definition for %q/%q/%q: %w", benchmark.BenchmarkName, difficulty.DifficultyName, rank.Name, err)
				}
				summary.RankDefs++
			}

			for categoryIndex := range difficulty.Categories {
				category := difficulty.Categories[categoryIndex]
				var categoryID int64
				err = tx.QueryRow(ctx, `
					INSERT INTO benchmark_categories (
						difficulty_id,
						category_name,
						color,
						sort_order,
						updated_at
					)
					VALUES ($1,$2,$3,$4,NOW())
					RETURNING id
				`,
					difficultyID,
					category.CategoryName,
					category.Color,
					categoryIndex,
				).Scan(&categoryID)
				if err != nil {
					return syncSummary{}, fmt.Errorf("insert category %q/%q/%q: %w", benchmark.BenchmarkName, difficulty.DifficultyName, category.CategoryName, err)
				}
				summary.Categories++

				for subcategoryIndex := range category.Subcategories {
					subcategory := category.Subcategories[subcategoryIndex]
					if _, err := tx.Exec(ctx, `
						INSERT INTO benchmark_subcategories (
							category_id,
							subcategory_name,
							scenario_count,
							color,
							sort_order,
							updated_at
						)
						VALUES ($1,$2,$3,$4,$5,NOW())
					`,
						categoryID,
						subcategory.SubcategoryName,
						subcategory.ScenarioCount,
						subcategory.Color,
						subcategoryIndex,
					); err != nil {
						return syncSummary{}, fmt.Errorf("insert subcategory %q/%q/%q/%q: %w", benchmark.BenchmarkName, difficulty.DifficultyName, category.CategoryName, subcategory.SubcategoryName, err)
					}
					summary.Subcategories++
				}
			}

			links := collectScenarioLinks(difficulty)
			for _, link := range links {
				scenarioID, err := ensureScenario(ctx, tx, link.Name)
				if err != nil {
					return syncSummary{}, fmt.Errorf("ensure scenario %q for %q/%q: %w", link.Name, benchmark.BenchmarkName, difficulty.DifficultyName, err)
				}

				rankThresholds := link.RankThresholds
				if rankThresholds == nil {
					rankThresholds = []float64{}
				}
				rankThresholdsJSON, err := json.Marshal(rankThresholds)
				if err != nil {
					return syncSummary{}, fmt.Errorf("marshal rank thresholds for %q/%q/%q: %w", benchmark.BenchmarkName, difficulty.DifficultyName, link.Name, err)
				}

				if _, err := tx.Exec(ctx, `
					INSERT INTO benchmark_difficulty_scenarios (
						difficulty_id,
						scenario_id,
						category_name,
						subcategory_name,
						rank_thresholds,
						sort_order,
						updated_at
					)
					VALUES ($1,$2,$3,$4,$5::jsonb,$6,NOW())
					ON CONFLICT (difficulty_id, scenario_id) DO UPDATE SET
						category_name = EXCLUDED.category_name,
						subcategory_name = EXCLUDED.subcategory_name,
						rank_thresholds = EXCLUDED.rank_thresholds,
						sort_order = EXCLUDED.sort_order,
						updated_at = NOW()
				`,
					difficultyID,
					scenarioID,
					link.CategoryName,
					link.SubcategoryName,
					rankThresholdsJSON,
					link.SortOrder,
				); err != nil {
					return syncSummary{}, fmt.Errorf("insert scenario link for %q/%q/%q: %w", benchmark.BenchmarkName, difficulty.DifficultyName, link.Name, err)
				}
				summary.ScenarioLinks++
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return syncSummary{}, fmt.Errorf("commit benchmark sync: %w", err)
	}

	s.logger.Info("benchmark sync completed",
		slog.Int("benchmarks", summary.Benchmarks),
		slog.Int("difficulties", summary.Difficulties),
		slog.Int("rank_defs", summary.RankDefs),
		slog.Int("scenario_links", summary.ScenarioLinks),
	)

	return summary, nil
}

func parseDate(raw string) (*time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	t, err := time.Parse("2006-01-02", raw)
	if err != nil {
		return nil, fmt.Errorf("invalid dateAdded %q: %w", raw, err)
	}
	u := t.UTC()
	return &u, nil
}

func (s *Service) enrichBenchmarks(ctx context.Context, benchmarks []sourceBenchmark) error {
	steamID, err := randomSteamID17()
	if err != nil {
		return fmt.Errorf("generate random steam ID: %w", err)
	}

	for benchmarkIndex := range benchmarks {
		benchmark := &benchmarks[benchmarkIndex]
		for difficultyIndex := range benchmark.Difficulties {
			difficulty := &benchmark.Difficulties[difficultyIndex]

			progressScenarios, fetchErr := s.fetchDifficultyProgressScenarios(ctx, difficulty.KovaaksBenchmarkID, steamID)
			if fetchErr != nil {
				return fmt.Errorf("fetch scenarios for %q/%q: %w", benchmark.BenchmarkName, difficulty.DifficultyName, fetchErr)
			}
			if len(progressScenarios) == 0 {
				return fmt.Errorf("no scenarios returned for %q/%q", benchmark.BenchmarkName, difficulty.DifficultyName)
			}

			difficulty.MergedRanks = mergedRanksFromSource(difficulty.RankColors)

			difficulty.MergedScenarios = applyProgressScenariosToDifficulty(*difficulty, progressScenarios)
			if len(difficulty.MergedScenarios) == 0 {
				return fmt.Errorf("merged scenario mapping empty for %q/%q", benchmark.BenchmarkName, difficulty.DifficultyName)
			}
		}
	}

	return nil
}

func (s *Service) fetchDifficultyProgressScenarios(ctx context.Context, benchmarkID int64, steamID string) ([]progressScenarioDefinition, error) {
	url := fmt.Sprintf(kovaaksPlayerProgressURL, benchmarkID, steamID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request progress endpoint: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("progress endpoint returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxProgressBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read progress response: %w", err)
	}
	if len(body) > maxProgressBodyBytes {
		return nil, fmt.Errorf("progress response exceeded %d bytes", maxProgressBodyBytes)
	}

	scenarios, err := parseProgressScenarioDefinitions(body)
	if err != nil {
		return nil, fmt.Errorf("parse progress response: %w", err)
	}

	return scenarios, nil
}

func randomSteamID17() (string, error) {
	const minSteamID int64 = 10000000000000000
	const steamIDRange int64 = 90000000000000000

	randomOffset, err := crand.Int(crand.Reader, big.NewInt(steamIDRange))
	if err != nil {
		return "", err
	}

	value := new(big.Int).Add(randomOffset, big.NewInt(minSteamID))
	return value.String(), nil
}

func ensureScenario(ctx context.Context, tx pgx.Tx, scenarioName string) (int64, error) {
	scenarioName = strings.TrimSpace(scenarioName)
	if scenarioName == "" {
		return 0, fmt.Errorf("scenario name is empty")
	}

	var id int64
	err := tx.QueryRow(ctx, `
		WITH inserted AS (
			INSERT INTO scenarios (scenario_name)
			VALUES ($1)
			ON CONFLICT (scenario_name) DO NOTHING
			RETURNING id
		)
		SELECT id FROM inserted
		UNION ALL
		SELECT s.id
		FROM scenarios s
		WHERE s.scenario_name = $1
		LIMIT 1
	`, scenarioName).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}
