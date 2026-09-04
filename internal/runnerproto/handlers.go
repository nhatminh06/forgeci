package runnerproto

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/nhatminh06/forgeci/internal/artifact"
	"github.com/nhatminh06/forgeci/internal/cache"
	"github.com/nhatminh06/forgeci/internal/store"
)

// Protocol version constant
const ProtocolVersion = 2

// Request/Response types

type RegisterRequest struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	ProtocolVersion int    `json:"protocol_version"`
	OS              string `json:"os"`
	Arch            string `json:"arch"`
	Docker          bool   `json:"docker"`
	MaxParallel     int    `json:"max_parallel"`
}

type RegisterResponse struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Status       string    `json:"status"`
	RegisteredAt time.Time `json:"registered_at"`
	LastSeenAt   time.Time `json:"last_seen_at"`
	CurrentRunID *string   `json:"current_run_id,omitempty"`
}

type HeartbeatRequest struct {
	RunnerID string                 `json:"runner_id"`
	Leases   []store.LeaseHeartbeat `json:"leases"`
}

type HeartbeatResponse struct {
	Leases         []store.LeaseHeartbeatResult `json:"leases"`
	ServerShutdown bool                         `json:"server_shutdown"`
}

type LeaseRequest struct {
	RunnerID string `json:"runner_id"`
}

type LeaseResponse struct {
	RunID                string              `json:"run_id"`
	JobName              string              `json:"job_name"`
	LeaseID              string              `json:"lease_id"`
	Generation           int                 `json:"generation"`
	Job                  store.JobDefinition `json:"job"`
	ExpiresAt            time.Time           `json:"expires_at"`
	SourceSnapshotSHA256 string              `json:"source_snapshot_sha256"`
	BlobSHA256           string              `json:"blob_sha256"`
	ArchiveSizeBytes     int64               `json:"archive_size_bytes"`
	LogicalSizeBytes     int64               `json:"logical_size_bytes"`
	EntryCount           int                 `json:"entry_count"`
	ArchiveFormat        string              `json:"archive_format"`
}

type JobEventRequest struct {
	RunnerID   string `json:"runner_id"`
	RunID      string `json:"run_id"`
	LeaseID    string `json:"lease_id"`
	Generation int    `json:"generation"`
	JobName    string `json:"job_name"`
	Status     string `json:"status"`
}

type JobEventResponse struct {
	Accepted bool `json:"accepted"`
}

type CompleteRunRequest struct {
	RunnerID   string  `json:"runner_id"`
	RunID      string  `json:"run_id"`
	LeaseID    string  `json:"lease_id"`
	Generation int     `json:"generation"`
	JobName    string  `json:"job_name"`
	Status     string  `json:"status"`
	Error      *string `json:"error,omitempty"`
}

type CompleteRunResponse struct {
	Accepted bool `json:"accepted"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

// Handler interface for dependency injection
type RunnerStore interface {
	RegisterRunner(context.Context, store.Runner) (*store.Runner, error)
	GetRunner(context.Context, string) (*store.Runner, error)
	ListRunners(context.Context) ([]store.Runner, error)
	UpdateRunnerLiveness(context.Context, string, time.Time) error
	LeaseRun(context.Context, string, string) (*store.Run, error)
	RenewLease(context.Context, string, string, string, int, time.Time) error
	ReportJobEvent(context.Context, string, string, string, int, string, store.JobStatus) error
	CompleteRun(context.Context, string, string, string, int, store.RunStatus, *string) error
	GetRun(context.Context, string) (*store.Run, error)
	LeaseJob(context.Context, string) (*store.JobLease, error)
	HeartbeatJobLeases(context.Context, string, []store.LeaseHeartbeat, time.Time) ([]store.LeaseHeartbeatResult, error)
	CompleteJob(context.Context, store.ArtifactOwnership, store.JobStatus, *string) error
}

type Handlers struct {
	store       RunnerStore
	token       string
	shutting    atomic.Bool
	longPollTTL time.Duration
	workSignal  func() <-chan struct{}
	openBlob    func(string) (*os.File, error)
	artifacts   *artifact.Store
	cacheStore  *cache.Store
	notify      func()
}

func (h *Handlers) SetSnapshotOpener(open func(string) (*os.File, error)) { h.openBlob = open }
func (h *Handlers) SetArtifactStore(value *artifact.Store)                { h.artifacts = value }
func (h *Handlers) SetCacheStore(value *cache.Store)                      { h.cacheStore = value }
func (h *Handlers) SetNotifier(value func())                              { h.notify = value }

func NewHandlers(store RunnerStore, token string, signals ...func() <-chan struct{}) *Handlers {
	h := &Handlers{
		store:       store,
		token:       token,
		longPollTTL: 20 * time.Second,
	}
	if len(signals) > 0 {
		h.workSignal = signals[0]
	}
	return h
}

func (h *Handlers) SetShuttingDown(value bool) {
	h.shutting.Store(value)
}

// Middleware to check bearer token
func (h *Handlers) AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values := r.Header.Values("Authorization")
		expected := "Bearer " + h.token
		if len(values) != 1 || h.token == "" || subtle.ConstantTimeCompare([]byte(values[0]), []byte(expected)) != 1 {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r)
	})
}

const smallRequestLimit int64 = 16 << 10

var runnerNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

func decodeStrict(w http.ResponseWriter, r *http.Request, value any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, smallRequestLimit)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return false
	}
	return true
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ErrorResponse{Error: message})
}

func validUUID(value string) bool { _, err := uuid.Parse(value); return err == nil }

func validJobEventStatus(status store.JobStatus) bool {
	switch status {
	case store.JobRunning, store.JobPassed, store.JobFailed, store.JobBlocked, store.JobCanceled:
		return true
	}
	return false
}

func validCompletionStatus(status store.RunStatus) bool {
	switch status {
	case store.RunPassed, store.RunFailed, store.RunCanceled, store.RunError:
		return true
	}
	return false
}

// Register runner
func (h *Handlers) Register(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req RegisterRequest
	if !decodeStrict(w, r, &req) {
		return
	}

	// Validate
	if !validUUID(req.ID) || !runnerNamePattern.MatchString(req.Name) || req.OS == "" || req.Arch == "" || req.MaxParallel < 1 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "invalid runner configuration"})
		return
	}

	if req.ProtocolVersion != ProtocolVersion {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: fmt.Sprintf("unsupported protocol version %d", req.ProtocolVersion)})
		return
	}

	runner := store.Runner{
		ID:              req.ID,
		Name:            req.Name,
		ProtocolVersion: req.ProtocolVersion,
		OS:              req.OS,
		Arch:            req.Arch,
		DockerAvailable: req.Docker,
		MaxParallel:     req.MaxParallel,
		Status:          store.RunnerOnline,
		RegisteredAt:    time.Now().UTC(),
		LastSeenAt:      time.Now().UTC(),
	}

	registered, err := h.store.RegisterRunner(r.Context(), runner)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "registration failed"})
		return
	}

	resp := RegisterResponse{
		ID:           registered.ID,
		Name:         registered.Name,
		Status:       string(registered.Status),
		RegisteredAt: registered.RegisteredAt,
		LastSeenAt:   registered.LastSeenAt,
		CurrentRunID: registered.CurrentRunID,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

// Heartbeat to keep runner alive and renew lease
func (h *Handlers) Heartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req HeartbeatRequest
	if !decodeStrict(w, r, &req) {
		return
	}

	if !validUUID(req.RunnerID) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "missing runner_id"})
		return
	}
	seen := map[string]struct{}{}
	for _, lease := range req.Leases {
		key := lease.RunID + "\x00" + lease.JobName + "\x00" + lease.LeaseID
		if !validUUID(lease.RunID) || lease.JobName == "" || !validUUID(lease.LeaseID) || lease.Generation < 1 {
			writeError(w, http.StatusBadRequest, "invalid job lease")
			return
		}
		if _, ok := seen[key]; ok {
			writeError(w, http.StatusBadRequest, "duplicate job lease")
			return
		}
		seen[key] = struct{}{}
	}
	results, err := h.store.HeartbeatJobLeases(r.Context(), req.RunnerID, req.Leases, time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "heartbeat failed")
		return
	}
	resp := HeartbeatResponse{Leases: results, ServerShutdown: h.shutting.Load()}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

// Long-poll for work
func (h *Handlers) Lease(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req LeaseRequest
	if !decodeStrict(w, r, &req) {
		return
	}

	if !validUUID(req.RunnerID) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "missing runner_id"})
		return
	}

	for {
		var signal <-chan struct{}
		if h.workSignal != nil {
			signal = h.workSignal()
		}
		lease, err := h.store.LeaseJob(r.Context(), req.RunnerID)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "lease acquisition failed"})
			return
		}

		if lease != nil {
			// A claim may have temporarily locked its run and caused concurrent
			// claimers to skip it. Wake them after commit so the next ready job is
			// considered immediately instead of waiting for the long-poll timeout.
			if h.notify != nil {
				h.notify()
			}
			resp := LeaseResponse{
				RunID: lease.RunID, JobName: lease.JobName, LeaseID: lease.LeaseID, Generation: lease.Generation, Job: lease.Job, ExpiresAt: lease.ExpiresAt,
				SourceSnapshotSHA256: lease.Snapshot.SourceDigest, BlobSHA256: lease.Snapshot.BlobDigest, ArchiveSizeBytes: lease.Snapshot.ArchiveSizeBytes, LogicalSizeBytes: lease.Snapshot.LogicalSizeBytes, EntryCount: lease.Snapshot.EntryCount, ArchiveFormat: lease.Snapshot.Format,
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(resp)
			return
		}

		timer := time.NewTimer(h.longPollTTL)
		select {
		case <-signal:
			if !timer.Stop() {
				<-timer.C
			}
			continue
		case <-timer.C:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNoContent)
			return
		case <-r.Context().Done():
			if !timer.Stop() {
				<-timer.C
			}
			w.WriteHeader(http.StatusRequestTimeout)
			return
		}
	}
}

// Report job state event
func (h *Handlers) JobEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req JobEventRequest
	if !decodeStrict(w, r, &req) {
		return
	}

	if !validUUID(req.RunnerID) || !validUUID(req.RunID) || !validUUID(req.LeaseID) || req.Generation < 1 || req.JobName == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "missing required fields"})
		return
	}

	// Validate status
	jobStatus := store.JobStatus(req.Status)
	if !validJobEventStatus(jobStatus) {
		writeError(w, http.StatusBadRequest, "invalid job status")
		return
	}
	run, err := h.store.GetRun(r.Context(), req.RunID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "run not found"})
		return
	}

	// Verify lease
	if run.LeaseID == nil || *run.LeaseID != req.LeaseID || run.LeaseGeneration != req.Generation {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "invalid lease"})
		return
	}

	err = h.store.ReportJobEvent(r.Context(), req.RunID, req.RunnerID, req.LeaseID, req.Generation, req.JobName, jobStatus)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "event processing failed"})
		return
	}

	resp := JobEventResponse{Accepted: true}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

// Complete run
func (h *Handlers) CompleteRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req CompleteRunRequest
	if !decodeStrict(w, r, &req) {
		return
	}

	if !validUUID(req.RunnerID) || !validUUID(req.RunID) || !validUUID(req.LeaseID) || req.Generation < 1 || req.JobName == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "missing required fields"})
		return
	}

	jobStatus := store.JobStatus(req.Status)
	if jobStatus != store.JobPassed && jobStatus != store.JobFailed && jobStatus != store.JobError && jobStatus != store.JobCanceled {
		writeError(w, http.StatusBadRequest, "invalid run status")
		return
	}

	err := h.store.CompleteJob(r.Context(), store.ArtifactOwnership{RunID: req.RunID, RunnerID: req.RunnerID, LeaseID: req.LeaseID, Generation: req.Generation, JobName: req.JobName}, jobStatus, req.Error)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "completion failed"})
		return
	}
	if h.notify != nil {
		h.notify()
	}

	resp := CompleteRunResponse{Accepted: true}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

// Handle dynamic lease routes
func (h *Handlers) HandleLeaseRoute(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if strings.HasSuffix(path, "/source") {
		h.Source(w, r)
	} else if pathContains(path, "/cache-blobs/") {
		h.CacheUpload(w, r)
	} else if strings.HasSuffix(path, "/cache/commit") {
		h.CacheCommit(w, r)
	} else if pathContains(path, "/cache/") {
		h.CacheDownload(w, r)
	} else if pathContains(path, "/artifact-blobs/") {
		h.ArtifactUpload(w, r)
	} else if strings.HasSuffix(path, "/artifacts/commit") {
		h.ArtifactCommit(w, r)
	} else if pathContains(path, "/artifacts/") {
		h.ArtifactDownload(w, r)
	} else if pathContains(path, "/jobs/") && strings.HasSuffix(path, "/logs") {
		h.JobLogAppend(w, r)
	} else if pathContains(path, "/complete") {
		h.CompleteRun(w, r)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

type jobLogAppendRequest struct {
	RunnerID   string             `json:"runner_id"`
	RunID      string             `json:"run_id"`
	LeaseID    string             `json:"lease_id"`
	Generation int                `json:"generation"`
	JobName    string             `json:"job_name"`
	Sequence   int64              `json:"sequence"`
	Stream     store.JobLogStream `json:"stream"`
	Payload    []byte             `json:"payload"`
}

const maxJobLogRequest = 1 << 20

func (h *Handlers) JobLogAppend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 7 || parts[4] != "jobs" || parts[6] != "logs" || !validUUID(parts[3]) || parts[5] == "" {
		writeError(w, http.StatusBadRequest, "invalid log path")
		return
	}
	if r.ContentLength > maxJobLogRequest {
		writeError(w, http.StatusRequestEntityTooLarge, "log request too large")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxJobLogRequest)
	var req jobLogAppendRequest
	if !decodeStrict(w, r, &req) {
		return
	}
	if !validUUID(req.RunID) || !validUUID(req.RunnerID) || !validUUID(req.LeaseID) || req.RunID != r.URL.Query().Get("run_id") || req.RunnerID != r.URL.Query().Get("runner_id") || req.LeaseID != parts[3] || req.JobName != parts[5] || req.Generation < 1 {
		writeError(w, http.StatusBadRequest, "invalid log ownership")
		return
	}
	if req.Sequence < 1 || len(req.Payload) == 0 || len(req.Payload) > 64<<10 || (req.Stream != store.JobLogStdout && req.Stream != store.JobLogStderr) {
		writeError(w, http.StatusBadRequest, "invalid log chunk")
		return
	}
	run, err := h.store.GetRun(r.Context(), req.RunID)
	owner := store.ArtifactOwnership{RunID: req.RunID, RunnerID: req.RunnerID, LeaseID: req.LeaseID, Generation: req.Generation, JobName: req.JobName}
	if err != nil || run.ID != req.RunID || !validArtifactLease(run, owner, store.JobRunning) {
		writeError(w, http.StatusConflict, "invalid or expired log lease")
		return
	}
	logs, ok := h.store.(store.JobLogStore)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "logs unavailable")
		return
	}
	if err := logs.AppendJobLog(r.Context(), store.JobLogChunk{RunID: req.RunID, JobName: req.JobName, Sequence: req.Sequence, Stream: req.Stream, Payload: req.Payload}); err != nil {
		if err == store.ErrConflict {
			writeError(w, http.StatusConflict, "log conflict")
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type artifactCommitRequest struct {
	RunnerID   string               `json:"runner_id"`
	RunID      string               `json:"run_id"`
	LeaseID    string               `json:"lease_id"`
	Generation int                  `json:"generation"`
	JobName    string               `json:"job_name"`
	Artifacts  []artifactCommitItem `json:"artifacts"`
}
type artifactCommitItem struct {
	Name             string `json:"name"`
	RootName         string `json:"root_name"`
	RootKind         string `json:"root_kind"`
	ContentSHA256    string `json:"content_sha256"`
	BlobSHA256       string `json:"blob_sha256"`
	Format           string `json:"format"`
	ArchiveSizeBytes int64  `json:"archive_size_bytes"`
	LogicalSizeBytes int64  `json:"logical_size_bytes"`
	EntryCount       int    `json:"entry_count"`
}

func (h *Handlers) ArtifactUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut || h.artifacts == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 8 || parts[4] != "jobs" || parts[6] != "artifact-blobs" || !validUUID(parts[3]) || parts[5] == "" || !artifact.ValidDigest(parts[7]) {
		writeError(w, http.StatusBadRequest, "invalid artifact upload path")
		return
	}
	owner, ok := h.artifactOwner(w, r, parts[3], parts[5])
	if !ok {
		return
	}
	size, err := strconv.ParseInt(r.Header.Get("Content-Length"), 10, 64)
	if err != nil || size < 0 {
		writeError(w, http.StatusBadRequest, "artifact content length required")
		return
	}
	run, err := h.store.GetRun(r.Context(), owner.RunID)
	if err != nil || !validArtifactLease(run, owner, store.JobRunning) {
		writeError(w, http.StatusConflict, "invalid or expired lease")
		return
	}
	if _, err := h.artifacts.Put(r.Context().Done(), parts[7], size, r.Body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) ArtifactCommit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || h.artifacts == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 8 || parts[4] != "jobs" || parts[6] != "artifacts" || parts[7] != "commit" || !validUUID(parts[3]) {
		writeError(w, http.StatusBadRequest, "invalid artifact commit path")
		return
	}
	var req artifactCommitRequest
	if !decodeStrict(w, r, &req) {
		return
	}
	if req.LeaseID != parts[3] || req.JobName != parts[5] || !validUUID(req.RunnerID) || !validUUID(req.RunID) || req.Generation < 1 {
		writeError(w, http.StatusBadRequest, "invalid artifact ownership")
		return
	}
	backend, ok := h.store.(store.ArtifactStore)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "artifact metadata unavailable")
		return
	}
	owner := store.ArtifactOwnership{RunID: req.RunID, RunnerID: req.RunnerID, LeaseID: req.LeaseID, Generation: req.Generation, JobName: req.JobName}
	run, err := h.store.GetRun(r.Context(), req.RunID)
	if err != nil || !validArtifactLease(run, owner, store.JobRunning) {
		writeError(w, http.StatusConflict, "invalid or expired lease")
		return
	}
	items := make([]store.Artifact, len(req.Artifacts))
	for i, wire := range req.Artifacts {
		item := &items[i]
		*item = store.Artifact{RunID: req.RunID, ProducerJob: req.JobName, Name: wire.Name, RootName: wire.RootName, RootKind: wire.RootKind, ContentSHA256: wire.ContentSHA256, BlobSHA256: wire.BlobSHA256, Format: wire.Format, ArchiveSizeBytes: wire.ArchiveSizeBytes, LogicalSizeBytes: wire.LogicalSizeBytes, EntryCount: wire.EntryCount, CreatedAt: time.Now().UTC()}
		if !artifact.ValidDigest(item.ContentSHA256) || !artifact.ValidDigest(item.BlobSHA256) || item.Format != artifact.Format || !h.artifacts.HasBlob(item.BlobSHA256, item.ArchiveSizeBytes) {
			writeError(w, http.StatusBadRequest, "invalid or missing artifact blob")
			return
		}
	}
	if err := backend.CommitArtifacts(r.Context(), owner, items); err != nil {
		writeError(w, http.StatusConflict, "artifact commit rejected")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]bool{"accepted": true})
}

func (h *Handlers) ArtifactDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || h.artifacts == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 7 || parts[4] != "artifacts" || !validUUID(parts[3]) || parts[5] == "" || parts[6] == "" {
		writeError(w, http.StatusBadRequest, "invalid artifact download path")
		return
	}
	owner, ok := h.artifactOwner(w, r, parts[3], r.URL.Query().Get("consumer_job"))
	if !ok {
		return
	}
	backend, ok := h.store.(store.ArtifactStore)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "artifact metadata unavailable")
		return
	}
	item, err := backend.GetArtifactForLease(r.Context(), owner, parts[5], parts[6])
	if err != nil {
		writeError(w, http.StatusConflict, "artifact unavailable")
		return
	}
	f, err := h.artifacts.OpenBlob(item.BlobSHA256)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "artifact blob unavailable")
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "application/vnd.forgeci.artifact+gzip")
	w.Header().Set("Content-Length", strconv.FormatInt(item.ArchiveSizeBytes, 10))
	w.Header().Set("X-ForgeCI-Blob-SHA256", item.BlobSHA256)
	w.Header().Set("X-ForgeCI-Content-SHA256", item.ContentSHA256)
	w.Header().Set("X-ForgeCI-Root-Name", item.RootName)
	w.Header().Set("X-ForgeCI-Root-Kind", item.RootKind)
	w.Header().Set("X-ForgeCI-Entry-Count", strconv.Itoa(item.EntryCount))
	w.Header().Set("X-ForgeCI-Logical-Size", strconv.FormatInt(item.LogicalSizeBytes, 10))
	_, _ = io.CopyN(w, f, item.ArchiveSizeBytes)
}

func (h *Handlers) artifactOwner(w http.ResponseWriter, r *http.Request, leaseID, job string) (store.ArtifactOwnership, bool) {
	generation, err := strconv.Atoi(r.URL.Query().Get("generation"))
	owner := store.ArtifactOwnership{RunID: r.URL.Query().Get("run_id"), RunnerID: r.URL.Query().Get("runner_id"), LeaseID: leaseID, Generation: generation, JobName: job}
	if err != nil || !validUUID(owner.RunID) || !validUUID(owner.RunnerID) || generation < 1 || job == "" {
		writeError(w, http.StatusBadRequest, "invalid artifact ownership")
		return owner, false
	}
	return owner, true
}

func validArtifactLease(run *store.Run, owner store.ArtifactOwnership, jobStatus store.JobStatus) bool {
	if run.Status != store.RunRunning {
		return false
	}
	for _, job := range run.Jobs {
		if job.Name == owner.JobName {
			return job.Status == jobStatus && job.RunnerID != nil && *job.RunnerID == owner.RunnerID && job.LeaseID != nil && *job.LeaseID == owner.LeaseID && job.LeaseGeneration == owner.Generation && job.LeaseExpiresAt != nil && time.Now().UTC().Before(*job.LeaseExpiresAt)
		}
	}
	return false
}

func (h *Handlers) Source(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || h.openBlob == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 5 || parts[0] != "v1" || parts[1] != "runner" || parts[2] != "leases" || parts[4] != "source" || !validUUID(parts[3]) {
		writeError(w, http.StatusBadRequest, "invalid source request")
		return
	}
	runnerID, runID, jobName := r.URL.Query().Get("runner_id"), r.URL.Query().Get("run_id"), r.URL.Query().Get("job_name")
	generation, err := strconv.Atoi(r.URL.Query().Get("generation"))
	if !validUUID(runnerID) || !validUUID(runID) || jobName == "" || err != nil || generation < 1 {
		writeError(w, http.StatusBadRequest, "invalid source ownership")
		return
	}
	run, err := h.store.GetRun(r.Context(), runID)
	if err != nil {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	owner := store.ArtifactOwnership{RunID: runID, RunnerID: runnerID, LeaseID: parts[3], Generation: generation, JobName: jobName}
	if !validArtifactLease(run, owner, store.JobRunning) || run.SourceSnapshotSHA256 == nil {
		writeError(w, http.StatusConflict, "invalid or expired lease")
		return
	}
	f, err := h.openBlob(*run.SourceSnapshotSHA256)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "source unavailable")
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "application/vnd.forgeci.source+gzip")
	w.Header().Set("Content-Length", strconv.FormatInt(run.SnapshotArchiveSize, 10))
	w.Header().Set("X-ForgeCI-Blob-SHA256", run.SnapshotBlobSHA256)
	if _, err := io.CopyN(w, f, run.SnapshotArchiveSize); err != nil {
		return
	}
}

func pathContains(path, substr string) bool {
	for i := 0; i < len(path)-len(substr)+1; i++ {
		if path[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
