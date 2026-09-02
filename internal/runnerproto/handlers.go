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
	"github.com/nhatminh06/forgeci/internal/store"
)

// Protocol version constant
const ProtocolVersion = 1

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
	RunnerID      string  `json:"runner_id"`
	ActiveLeaseID *string `json:"active_lease_id"`
	Generation    int     `json:"generation,omitempty"`
}

type HeartbeatResponse struct {
	LeaseValid      bool       `json:"lease_valid"`
	CancelRequested bool       `json:"cancel_requested"`
	ServerShutdown  bool       `json:"server_shutdown"`
	LeaseExpiresAt  *time.Time `json:"lease_expires_at,omitempty"`
}

type LeaseRequest struct {
	RunnerID string `json:"runner_id"`
}

type LeaseResponse struct {
	RunID                string    `json:"run_id"`
	LeaseID              string    `json:"lease_id"`
	Generation           int       `json:"generation"`
	PipelineYAML         []byte    `json:"pipeline_yaml"`
	EffectiveParallel    int       `json:"effective_parallel"`
	ExpiresAt            time.Time `json:"expires_at"`
	SourceSnapshotSHA256 string    `json:"source_snapshot_sha256"`
	BlobSHA256           string    `json:"blob_sha256"`
	ArchiveSizeBytes     int64     `json:"archive_size_bytes"`
	LogicalSizeBytes     int64     `json:"logical_size_bytes"`
	EntryCount           int       `json:"entry_count"`
	ArchiveFormat        string    `json:"archive_format"`
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
}

type Handlers struct {
	store       RunnerStore
	token       string
	shutting    atomic.Bool
	longPollTTL time.Duration
	workSignal  func() <-chan struct{}
	openBlob    func(string) (*os.File, error)
}

func (h *Handlers) SetSnapshotOpener(open func(string) (*os.File, error)) { h.openBlob = open }

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

	if !validUUID(req.RunnerID) || (req.ActiveLeaseID != nil && !validUUID(*req.ActiveLeaseID)) || req.Generation < 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "missing runner_id"})
		return
	}

	// Update liveness
	err := h.store.UpdateRunnerLiveness(r.Context(), req.RunnerID, time.Now().UTC())
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "liveness update failed"})
		return
	}

	// Check if lease is still valid and renew if requested
	var leaseValid bool
	var renewedUntil *time.Time
	if req.ActiveLeaseID != nil && *req.ActiveLeaseID != "" {
		// Get the run to check lease status
		runner, err := h.store.GetRunner(r.Context(), req.RunnerID)
		if err != nil {
			leaseValid = false
		} else if runner.CurrentRunID != nil {
			run, err := h.store.GetRun(r.Context(), *runner.CurrentRunID)
			if err == nil && run.LeaseID != nil && *run.LeaseID == *req.ActiveLeaseID && run.Status == store.RunRunning {
				leaseValid = true
				// Renew lease
				ttl := 30 * time.Second
				newExpiry := time.Now().UTC().Add(ttl)
				if req.Generation == run.LeaseGeneration && h.store.RenewLease(r.Context(), *runner.CurrentRunID, req.RunnerID, *req.ActiveLeaseID, req.Generation, newExpiry) == nil {
					renewedUntil = &newExpiry
				} else {
					leaseValid = false
				}
			}
		}
	}

	// Check for cancel request
	var cancelRequested bool
	if req.ActiveLeaseID != nil && *req.ActiveLeaseID != "" {
		runner, _ := h.store.GetRunner(r.Context(), req.RunnerID)
		if runner != nil && runner.CurrentRunID != nil {
			run, _ := h.store.GetRun(r.Context(), *runner.CurrentRunID)
			if run != nil && run.CancelRequestedAt != nil {
				cancelRequested = true
			}
		}
	}

	resp := HeartbeatResponse{
		LeaseValid:      leaseValid,
		CancelRequested: cancelRequested,
		ServerShutdown:  h.shutting.Load(),
		LeaseExpiresAt:  renewedUntil,
	}

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
		run, err := h.store.LeaseRun(r.Context(), req.RunnerID, "")
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "lease acquisition failed"})
			return
		}

		if run != nil {
			resp := LeaseResponse{
				RunID:             run.ID,
				LeaseID:           *run.LeaseID,
				Generation:        run.LeaseGeneration,
				PipelineYAML:      run.PipelineYAML,
				EffectiveParallel: *run.EffectiveParallel,
				ExpiresAt:         *run.LeaseExpiresAt,
				BlobSHA256:        run.SnapshotBlobSHA256, ArchiveSizeBytes: run.SnapshotArchiveSize, LogicalSizeBytes: run.SnapshotLogicalSize, EntryCount: run.SnapshotEntryCount, ArchiveFormat: run.SnapshotFormat,
			}
			if run.SourceSnapshotSHA256 != nil {
				resp.SourceSnapshotSHA256 = *run.SourceSnapshotSHA256
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

	if !validUUID(req.RunnerID) || !validUUID(req.RunID) || !validUUID(req.LeaseID) || req.Generation < 1 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "missing required fields"})
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

	runStatus := store.RunStatus(req.Status)
	if !validCompletionStatus(runStatus) {
		writeError(w, http.StatusBadRequest, "invalid run status")
		return
	}

	err = h.store.CompleteRun(r.Context(), req.RunID, req.RunnerID, req.LeaseID, req.Generation, runStatus, req.Error)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "completion failed"})
		return
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
	} else if pathContains(path, "/events") {
		h.JobEvent(w, r)
	} else if pathContains(path, "/complete") {
		h.CompleteRun(w, r)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
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
	runnerID, runID := r.URL.Query().Get("runner_id"), r.URL.Query().Get("run_id")
	generation, err := strconv.Atoi(r.URL.Query().Get("generation"))
	if !validUUID(runnerID) || !validUUID(runID) || err != nil || generation < 1 {
		writeError(w, http.StatusBadRequest, "invalid source ownership")
		return
	}
	run, err := h.store.GetRun(r.Context(), runID)
	if err != nil {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	if run.Status != store.RunRunning || run.RunnerID == nil || *run.RunnerID != runnerID || run.LeaseID == nil || *run.LeaseID != parts[3] || run.LeaseGeneration != generation || run.LeaseExpiresAt == nil || !time.Now().UTC().Before(*run.LeaseExpiresAt) || run.SourceSnapshotSHA256 == nil {
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
