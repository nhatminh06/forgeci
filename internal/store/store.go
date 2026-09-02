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
	ID                   string     `json:"id"`
	Status               RunStatus  `json:"status"`
	PipelineFile         string     `json:"pipeline_file"`
	PipelineYAML         []byte     `json:"-"`
	PipelineSHA256       string     `json:"pipeline_sha256"`
	SourceSnapshotSHA256 *string    `json:"source_snapshot_sha256,omitempty"`
	SnapshotBlobSHA256   string     `json:"-"`
	SnapshotFormat       string     `json:"-"`
	SnapshotArchiveSize  int64      `json:"-"`
	SnapshotLogicalSize  int64      `json:"-"`
	SnapshotEntryCount   int        `json:"-"`
	Workspace            string     `json:"workspace"`
	MaxParallel          int        `json:"max_parallel"`
	CreatedAt            time.Time  `json:"created_at"`
	StartedAt            *time.Time `json:"started_at"`
	FinishedAt           *time.Time `json:"finished_at"`
	CancelRequestedAt    *time.Time `json:"cancel_requested_at"`
	ErrorMessage         *string    `json:"error_message,omitempty"`
	Jobs                 []Job      `json:"jobs,omitempty"`
	// Remote runner fields
	RunnerID          *string    `json:"runner_id,omitempty"`
	LeaseID           *string    `json:"lease_id,omitempty"`
	LeaseGeneration   int        `json:"lease_generation"`
	LeaseExpiresAt    *time.Time `json:"lease_expires_at,omitempty"`
	EffectiveParallel *int       `json:"effective_parallel,omitempty"`
}

type Job struct {
	Name         string     `json:"name"`
	Status       JobStatus  `json:"status"`
	Image        *string    `json:"image"`
	StartedAt    *time.Time `json:"started_at"`
	FinishedAt   *time.Time `json:"finished_at"`
	ErrorMessage *string    `json:"error_message,omitempty"`
}

type RunnerStatus string

const (
	RunnerOnline  RunnerStatus = "ONLINE"
	RunnerOffline RunnerStatus = "OFFLINE"
)

type Runner struct {
	ID              string       `json:"id"`
	Name            string       `json:"name"`
	ProtocolVersion int          `json:"protocol_version"`
	OS              string       `json:"os"`
	Arch            string       `json:"arch"`
	DockerAvailable bool         `json:"docker"`
	MaxParallel     int          `json:"max_parallel"`
	Status          RunnerStatus `json:"status"`
	RegisteredAt    time.Time    `json:"registered_at"`
	LastSeenAt      time.Time    `json:"last_seen_at"`
	CurrentRunID    *string      `json:"current_run_id,omitempty"`
}

type CreateRun struct {
	ID, PipelineFile, PipelineSHA256, Workspace string
	PipelineYAML                                []byte
	MaxParallel                                 int
	Jobs                                        []Job
	Snapshot                                    *SourceSnapshot
}

type SourceSnapshot struct {
	SourceDigest, BlobDigest, Format   string
	ArchiveSizeBytes, LogicalSizeBytes int64
	EntryCount                         int
	CreatedAt                          time.Time
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
	// Runner management
	RegisterRunner(context.Context, Runner) (*Runner, error)
	GetRunner(context.Context, string) (*Runner, error)
	ListRunners(context.Context) ([]Runner, error)
	UpdateRunnerLiveness(context.Context, string, time.Time) error
	// Lease management
	LeaseRun(context.Context, string, string) (*Run, error)
	RenewLease(context.Context, string, string, string, int, time.Time) error
	ReportJobEvent(context.Context, string, string, string, int, string, JobStatus) error
	CompleteRun(context.Context, string, string, string, int, RunStatus, *string) error
	ExpireLeases(context.Context, time.Time) error
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
