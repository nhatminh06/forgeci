package store

import (
	"context"
	"errors"
	"time"
)

type RunStatus string
type JobStatus string

const (
	RunQueued   RunStatus = "QUEUED"
	RunRunning  RunStatus = "RUNNING"
	RunPassed   RunStatus = "PASSED"
	RunFailed   RunStatus = "FAILED"
	RunCanceled RunStatus = "CANCELED"
	RunError    RunStatus = "ERROR"
	RunAborted  RunStatus = "ABORTED"

	JobPending  JobStatus = "PENDING"
	JobRunning  JobStatus = "RUNNING"
	JobPassed   JobStatus = "PASSED"
	JobFailed   JobStatus = "FAILED"
	JobBlocked  JobStatus = "BLOCKED"
	JobCanceled JobStatus = "CANCELED"
	JobAborted  JobStatus = "ABORTED"
)

var (
	ErrNotFound = errors.New("run not found")
	ErrConflict = errors.New("invalid run state")
)

type Run struct {
	ID                string     `json:"id"`
	Status            RunStatus  `json:"status"`
	PipelineFile      string     `json:"pipeline_file"`
	PipelineYAML      []byte     `json:"-"`
	PipelineSHA256    string     `json:"pipeline_sha256"`
	Workspace         string     `json:"workspace"`
	MaxParallel       int        `json:"max_parallel"`
	CreatedAt         time.Time  `json:"created_at"`
	StartedAt         *time.Time `json:"started_at"`
	FinishedAt        *time.Time `json:"finished_at"`
	CancelRequestedAt *time.Time `json:"cancel_requested_at"`
	ErrorMessage      *string    `json:"error_message,omitempty"`
	Jobs              []Job      `json:"jobs,omitempty"`
}

type Job struct {
	Name         string     `json:"name"`
	Status       JobStatus  `json:"status"`
	Image        *string    `json:"image"`
	StartedAt    *time.Time `json:"started_at"`
	FinishedAt   *time.Time `json:"finished_at"`
	ErrorMessage *string    `json:"error_message,omitempty"`
}

type CreateRun struct {
	ID, PipelineFile, PipelineSHA256, Workspace string
	PipelineYAML                                []byte
	MaxParallel                                 int
	Jobs                                        []Job
}

type Store interface {
	Ping(context.Context) error
	CreateRun(context.Context, CreateRun) (*Run, error)
	GetRun(context.Context, string) (*Run, error)
	ListRuns(context.Context, int) ([]Run, error)
	ClaimNextQueuedRun(context.Context) (*Run, error)
	FinishRun(context.Context, string, RunStatus, *string) error
	UpdateJob(context.Context, string, string, JobStatus, *string) error
	CancelQueued(context.Context, string) error
	RequestCancel(context.Context, string) (RunStatus, error)
	RecoverInterrupted(context.Context) error
	Close()
}

func Terminal(status RunStatus) bool {
	switch status {
	case RunPassed, RunFailed, RunCanceled, RunError, RunAborted:
		return true
	default:
		return false
	}
}
