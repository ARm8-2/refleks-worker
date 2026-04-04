# refleks-worker

Standalone Go 1.24 worker service for expensive background tasks running alongside the API.

## What this worker does

1. Refreshes database leaderboards on a schedule so API reads are fast.
2. Reads raw `.refleks` files from the raw public bucket and exports rich Parquet datasets (runs, stats, events, mouse segments) to the private lab bucket.
3. Syncs benchmark definitions from a JSON file on disk into normalized database tables.

The worker is designed to run as a separate container on the same server as the API and use the same Supabase database and R2 infrastructure.

## Project layout

1. `cmd/worker`: worker entrypoint and process lifecycle.
2. `internal/config`: env loading and validation.
3. `internal/supabase`: shared pgx connection pool.
4. `internal/r2`: Cloudflare R2 read/write adapter used by parquet export.
5. `internal/worker/schema`: idempotent schema bootstrap.
6. `internal/worker/state`: job run tracking and state persistence.
7. `internal/worker/jobs/benchmarksync`: JSON file fingerprinting + benchmark upsert logic.
8. `internal/worker/jobs/leaderboard`: scenario and benchmark leaderboard refresh job.
9. `internal/worker/refleks`: `.refleks` binary parser used by parquet export.
10. `internal/worker/jobs/parquetexport`: raw-bucket parquet generation and upload job.
11. `internal/worker/scheduler`: gocron wiring.

## Scheduling model

Jobs are scheduled internally with gocron, not host-level cron.

1. `BENCHMARK_SYNC_CRON`: checks benchmark file for changes and syncs if hash changed.
2. `LEADERBOARD_CRON`: rebuilds current leaderboard tables.
3. `PARQUET_CRON`: reads raw files from the public bucket and writes parquet snapshots to the lab bucket.

If `WORKER_RUN_ON_STARTUP=true`, all jobs also run once on container startup in this order:

1. benchmark sync
2. leaderboard refresh
3. parquet export

## Database tables managed by worker

Worker bootstraps required tables idempotently at startup.

1. Core shared tables (if missing): `accounts`, `scenarios`, `runs`.
2. Worker state tables: `worker_job_runs`, `worker_job_state`.
3. Benchmark tables:
	- `benchmarks`
	- `benchmark_difficulties`
	- `benchmark_categories`
	- `benchmark_subcategories`
	- `benchmark_difficulty_scenarios`
4. Leaderboard tables:
	- `scenario_leaderboard_current`
	- `benchmark_difficulty_leaderboard_current`

Notes:

1. One scenario can map to multiple benchmark difficulties through `benchmark_difficulty_scenarios`.
2. Benchmark source sync is hash-based and idempotent.
3. Jobs are execution-tracked in `worker_job_runs` with status and details JSON.

## Benchmark sync source file

By default the worker reads:

1. Directory: `BENCHMARK_SYNC_SOURCE_DIR=/data/benchmarks`
2. File: `BENCHMARK_SYNC_SOURCE_FILE=benchmarks_data.json`

Use a bind mount or volume so uploaded benchmark JSON is visible in the worker container.

Current reference benchmark JSON format is fully supported. Optional future scenario linking is supported via optional `scenarios` or `scenarioNames` fields when present.

## Parquet source and output

Parquet export now consumes raw run files from Cloudflare R2 and emits multiple datasets:

1. `raw/runs/date=YYYY-MM-DD/runs.parquet`
2. `raw/stats/date=YYYY-MM-DD/stats.parquet`
3. `raw/events/date=YYYY-MM-DD/events.parquet`
4. `raw/mouse_segments/date=YYYY-MM-DD/segments.parquet`
5. Leaderboard snapshots under `leaderboards/scenario/...` and `leaderboards/benchmark/...`

Required bucket settings:

1. `R2_RAW_PUBLIC_BUCKET`: source bucket with raw `.refleks` files.
2. `R2_LAB_PRIVATE_BUCKET`: destination bucket for parquet output.

Optional parquet tuning:

1. `PARQUET_SOURCE_PREFIX`: limits source listing to a key prefix.
2. `PARQUET_TRACE_SAMPLE_POINTS`: sampled trace points stored per run row.
3. `PARQUET_MAX_SEGMENTS_PER_RUN`: max consecutive-event mouse segments exported per run.
4. `PARQUET_SAME_SPOT_THRESHOLD_PX`: pixel threshold for marking consecutive targets as effectively the same spot.
5. `PARQUET_SOURCE_LIST_PAGE`: pagination size when listing source bucket objects.

The `raw/mouse_segments` dataset includes precomputed fields for premium similarity queries:

1. `is_same_spot_as_previous`
2. `same_spot_score`
3. `motion_signature`

## Local run

1. Copy `.env.example` to `.env` and fill real values.
2. Ensure benchmark source file exists at configured path.
3. Run:

```bash
go run ./cmd/worker
```

## Docker build and run

Build:

```bash
docker build -f Dockerfile -t refleks-worker:latest .
```

Run:

```bash
docker run --rm \
  --env-file .env \
  -v /srv/refleks/benchmarks:/data/benchmarks:ro \
  refleks-worker:latest
```
