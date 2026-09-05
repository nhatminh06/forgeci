package store

import (
	"context"
	"errors"
	"time"

	"github.com/nhatminh06/forgeci/internal/scm"
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
	JobError    JobStatus = "ERROR"
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
	Name            string     `json:"name"`
	Status          JobStatus  `json:"status"`
	Image           *string    `json:"image"`
	StartedAt       *time.Time `json:"started_at"`
	FinishedAt      *time.Time `json:"finished_at"`
	ErrorMessage    *string    `json:"error_message,omitempty"`
	RunnerID        *string    `json:"runner_id,omitempty"`
	LeaseID         *string    `json:"lease_id,omitempty"`
	LeaseGeneration int        `json:"lease_generation"`
	LeaseExpiresAt  *time.Time `json:"lease_expires_at,omitempty"`
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
	ActiveJobs      int          `json:"active_jobs"`
}

type JobDependency struct {
	JobName, DependsOn string
}

type JobLease struct {
	RunID, JobName, RunnerID, LeaseID string
	Generation                        int
	ExpiresAt                         time.Time
	Job                               JobDefinition
	Snapshot                          SourceSnapshot
}

// JobDefinition is the wire-neutral executable portion of a pipeline job.
type JobDefinition struct {
	Image        *string               `json:"image,omitempty"`
	Steps        []string              `json:"steps"`
	Uploads      []ArtifactDeclaration `json:"uploads,omitempty"`
	Downloads    []ArtifactDownload    `json:"downloads,omitempty"`
	CacheRestore []CacheDeclaration    `json:"cache_restore,omitempty"`
	CacheSave    []CacheDeclaration    `json:"cache_save,omitempty"`
}

type ArtifactDeclaration struct {
	Name string `json:"name"`
	Path string `json:"path"`
}
type ArtifactDownload struct {
	From string `json:"from"`
	Name string `json:"name"`
	Into string `json:"into"`
}

type CacheDeclaration struct {
	Key  string `json:"key"`
	Path string `json:"path"`
}

type CacheMetadata struct {
	Workspace, Key, RootName, RootKind string
	ContentSHA256, BlobSHA256, Format  string
	ArchiveSizeBytes, LogicalSizeBytes int64
	EntryCount                         int
	CreatedAt, LastAccessedAt          time.Time
	ExpiresAt, DeletedAt               *time.Time
}

type JobLogStream string

const (
	JobLogStdout JobLogStream = "stdout"
	JobLogStderr JobLogStream = "stderr"
)

type JobLogChunk struct {
	RunID     string
	JobName   string
	Sequence  int64
	Stream    JobLogStream
	CreatedAt time.Time
	Payload   []byte
}

type LeaseHeartbeat struct {
	RunID      string `json:"run_id"`
	JobName    string `json:"job_name"`
	LeaseID    string `json:"lease_id"`
	Generation int    `json:"generation"`
}

type LeaseHeartbeatResult struct {
	RunID           string     `json:"run_id"`
	JobName         string     `json:"job_name"`
	LeaseID         string     `json:"lease_id"`
	Generation      int        `json:"generation"`
	Valid           bool       `json:"valid"`
	CancelRequested bool       `json:"cancel_requested"`
	ExpiresAt       *time.Time `json:"expires_at,omitempty"`
}

type CreateRun struct {
	ID, PipelineFile, PipelineSHA256, Workspace string
	PipelineYAML                                []byte
	MaxParallel                                 int
	Jobs                                        []Job
	Dependencies                                []JobDependency
	Snapshot                                    *SourceSnapshot
}

type JobSchedulerStore interface {
	LeaseJob(context.Context, string) (*JobLease, error)
	HeartbeatJobLeases(context.Context, string, []LeaseHeartbeat, time.Time) ([]LeaseHeartbeatResult, error)
	CompleteJob(context.Context, ArtifactOwnership, JobStatus, *string) error
	ExpireJobLeases(context.Context, time.Time) error
}

type CacheStore interface {
	LookupCache(context.Context, string, string, time.Time) (*CacheMetadata, error)
	CommitCache(context.Context, CacheMetadata) error
	ListCache(context.Context, string, int) ([]CacheMetadata, error)
	DeleteCache(context.Context, string, string) error
	ExpireCache(context.Context, time.Time, int64) ([]string, error)
	LiveCacheBlobs(context.Context) (map[string]struct{}, error)
}

type SourceSnapshot struct {
	SourceDigest, BlobDigest, Format   string
	ArchiveSizeBytes, LogicalSizeBytes int64
	EntryCount                         int
	CreatedAt                          time.Time
}

type Artifact struct {
	ID               string     `json:"-"`
	RunID            string     `json:"-"`
	ProducerJob      string     `json:"job"`
	Name             string     `json:"name"`
	RootName         string     `json:"root_name"`
	RootKind         string     `json:"root_kind"`
	ContentSHA256    string     `json:"content_sha256"`
	BlobSHA256       string     `json:"blob_sha256"`
	Format           string     `json:"format"`
	ArchiveSizeBytes int64      `json:"archive_size_bytes"`
	LogicalSizeBytes int64      `json:"logical_size_bytes"`
	EntryCount       int        `json:"entry_count"`
	CreatedAt        time.Time  `json:"created_at"`
	ExpiresAt        *time.Time `json:"expires_at"`
	DeletedAt        *time.Time `json:"-"`
	Available        bool       `json:"available"`
}

type ArtifactOwnership struct {
	RunID, RunnerID, LeaseID, JobName string
	Generation                        int
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

type JobLogStore interface {
	AppendJobLog(context.Context, JobLogChunk) error
	AppendJobLogs(context.Context, []JobLogChunk) error
	ListJobLogs(context.Context, string, string, int64, int) ([]JobLogChunk, error)
}

type ArtifactStore interface {
	CommitArtifacts(context.Context, ArtifactOwnership, []Artifact) error
	GetArtifactForLease(context.Context, ArtifactOwnership, string, string) (*Artifact, error)
	ListArtifacts(context.Context, string) ([]Artifact, error)
	GetArtifact(context.Context, string, string, string) (*Artifact, error)
	SetArtifactExpiry(context.Context, string, time.Time) error
	ExpireArtifacts(context.Context, time.Time) ([]string, error)
	LiveArtifactBlobs(context.Context) (map[string]struct{}, error)
}

type SCMStore interface {
	CreateSCMRepository(context.Context, scm.Repository) (*scm.Repository, error)
	GetSCMRepository(context.Context, string) (*scm.Repository, error)
	GetSCMRepositoryByIdentity(context.Context, scm.Provider, string) (*scm.Repository, error)
	ListSCMRepositories(context.Context) ([]scm.Repository, error)
	DeleteSCMRepository(context.Context, string) error
	CreateSCMDelivery(context.Context, scm.Delivery) (*scm.Delivery, error)
	GetSCMDelivery(context.Context, string) (*scm.Delivery, error)
	GetSCMDeliveryByProviderDeliveryID(context.Context, scm.Provider, string) (*scm.Delivery, error)
	CreateSCMRunTrigger(context.Context, scm.RunTrigger) (*scm.RunTrigger, error)
	GetSCMRunTrigger(context.Context, string) (*scm.RunTrigger, error)
	GetSCMRunTriggerByDelivery(context.Context, string) (*scm.RunTrigger, error)
	GetSCMRunTriggerByRunID(context.Context, string) (*scm.RunTrigger, error)
}

type SCMWorkerStore interface {
	SCMStore
	ClaimSCMDelivery(context.Context, string, time.Time, time.Duration) (*scm.Delivery, error)
	RenewSCMDeliveryClaim(context.Context, string, string, time.Time, time.Duration) error
	CompleteSCMDelivery(context.Context, string, string, scm.DeliveryStatus) error
	FailSCMDelivery(context.Context, string, string, *time.Time, string) error
	CreateSCMRun(context.Context, string, CreateRun, scm.RunTrigger) (*Run, *scm.RunTrigger, error)
}

type SCMCheckStore interface {
	SCMStore
	ClaimSCMCheck(context.Context, string, time.Time, time.Duration) (*scm.RunTrigger, error)
	CompleteSCMCheck(context.Context, string, string, string, string, *string) error
	FailSCMCheck(context.Context, string, string, time.Time, string) error
}

func Terminal(status RunStatus) bool {
	switch status {
	case RunPassed, RunFailed, RunCanceled, RunError, RunAborted:
		return true
	default:
		return false
	}
}
