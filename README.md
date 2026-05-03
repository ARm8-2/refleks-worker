# refleks-worker

Standalone Go 1.24 worker service for expensive background tasks running alongside the API.

## What this worker does

1. Refreshes database leaderboards on a schedule so API reads are fast.
2. Syncs benchmark definitions from a JSON file on disk into normalized database tables.
3. Refreshes cached scenario score and sensitivity distributions used by the public database/API.

The worker is designed to run as a separate container on the same server as the API and use the same Postgres database.

## Project layout

1. `cmd/worker`: worker entrypoint and process lifecycle.
2. `internal/config`: env loading and validation.
3. `internal/postgres`: shared pgx connection pool.
4. `internal/worker/schema`: idempotent schema bootstrap.
5. `internal/worker/state`: job run tracking and state persistence.
6. `internal/worker/jobs/benchmarksync`: JSON file fingerprinting + benchmark upsert logic.
7. `internal/worker/jobs/leaderboard`: scenario and benchmark leaderboard refresh job.
8. `internal/worker/jobs/scenariostats`: cached scenario score and sensitivity distribution refresh job.
9. `internal/worker/refleks`: retained `.refleks` binary parser for future raw-file ingestion work.
10. `internal/worker/scheduler`: gocron wiring.

## Scheduling model

Jobs are scheduled internally with gocron, not host-level cron.

1. `BENCHMARK_SYNC_CRON`: checks benchmark file for changes and syncs if hash changed.
2. `SCENARIO_STATS_CRON`: rebuilds cached scenario distributions for `score` and `sens_cm360`.
3. `LEADERBOARD_CRON`: rebuilds current leaderboard tables.

If `WORKER_RUN_ON_STARTUP=true`, all jobs also run once on container startup in this order:

1. benchmark sync
2. scenario stats refresh
3. leaderboard refresh

## Database tables managed by worker

Worker bootstraps required tables idempotently at startup.

1. Core shared tables (if missing): `accounts`, `scenarios`, `runs`.
2. Worker control tables: `worker_job_runs`, `worker_job_state`, `worker_job_config`.
3. Benchmark tables:
	- `benchmarks`
	- `benchmark_difficulties`
	- `benchmark_difficulty_ranks`
	- `benchmark_categories`
	- `benchmark_subcategories`
	- `benchmark_difficulty_scenarios`
4. Leaderboard tables:
	- `scenario_leaderboard_current`
	- `benchmark_difficulty_leaderboard_current`

Notes:

1. One scenario can map to multiple benchmark difficulties through `benchmark_difficulty_scenarios`.
2. Benchmark source sync is hash-based and idempotent.
3. Scenario rows cache percentile-clipped histogram summaries for `score` and `sens_cm360`, so the API can render charts without rescanning `runs`, and `updated_at` reflects the last real scenario-row change.
4. Job schedules are seeded from env defaults once and then managed through `worker_job_config`.
5. Jobs are execution-tracked in `worker_job_runs` with status and details JSON.

## Benchmark sync source file

By default the worker reads:

1. Directory: `BENCHMARK_SYNC_SOURCE_DIR=/data/benchmarks`
2. File: `BENCHMARK_SYNC_SOURCE_FILE=benchmarks_data.json`

Use a bind mount or volume so uploaded benchmark JSON is visible in the worker container.

The worker now enriches benchmark definitions with ordered scenario names and per-scenario rank thresholds from the Kovaaks progress endpoint (using a random 17-digit Steam ID). Ordered rank definitions are taken directly from `rankColors` in the source `benchmarks_data.json` and preserved exactly as written.

## Local run

1. Copy `.env.example` to `.env` and fill real values.
2. Point the worker at the self-hosted Postgres container with either `DATABASE_URL` or `POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD`.
3. Ensure benchmark source file exists at configured path.
4. Run:

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
