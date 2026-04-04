package worker

import "context"

const (
	OutcomeSuccess = "success"
	OutcomeSkipped = "skipped"
)

// Result is the normalized outcome of a job execution.
type Result struct {
	Status  string
	Message string
	Details map[string]any
}

// Job is a schedulable worker task.
type Job interface {
	Name() string
	Run(ctx context.Context) (Result, error)
}
