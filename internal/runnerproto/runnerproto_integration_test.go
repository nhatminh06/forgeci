package runnerproto

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nhatminh06/forgeci/internal/store"
	"github.com/nhatminh06/forgeci/internal/store/postgres"
)

func setupTestDB(t *testing.T) *postgres.Store {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := postgres.Open(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed to open test database: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func protocolSnapshot() *store.SourceSnapshot {
	return &store.SourceSnapshot{SourceDigest: strings.Repeat("a", 64), BlobDigest: strings.Repeat("b", 64), Format: "tar-gzip-v1", ArchiveSizeBytes: 1, LogicalSizeBytes: 0, EntryCount: 0, CreatedAt: time.Now().UTC()}
}

func TestRunnerRegistrationAndHeartbeat(t *testing.T) {
	db := setupTestDB(t)
	token := "test-token-123"
	handlers := NewHandlers(db, token)
	server := httptest.NewServer(handlers.AuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/runner/register" {
			handlers.Register(w, r)
		} else if r.URL.Path == "/v1/runner/heartbeat" {
			handlers.Heartbeat(w, r)
		}
	})))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Register runner
	runnerID := uuid.New().String()
	registerReq := RegisterRequest{
		ID:              runnerID,
		Name:            "test-runner",
		ProtocolVersion: ProtocolVersion,
		OS:              "linux",
		Arch:            "amd64",
		Docker:          true,
		MaxParallel:     4,
	}
	body, _ := json.Marshal(registerReq)
	req, _ := http.NewRequestWithContext(ctx, "POST", server.URL+"/v1/runner/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("register request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("register status=%d", resp.StatusCode)
	}

	// Verify runner registered
	runner, err := db.GetRunner(ctx, runnerID)
	if err != nil {
		t.Fatalf("GetRunner failed: %v", err)
	}
	if runner.Status != store.RunnerOnline {
		t.Fatalf("runner status should be ONLINE, got %s", runner.Status)
	}

	// Heartbeat
	heartbeatReq := HeartbeatRequest{RunnerID: runnerID, Leases: []store.LeaseHeartbeat{}}
	body, _ = json.Marshal(heartbeatReq)
	req, _ = http.NewRequestWithContext(ctx, "POST", server.URL+"/v1/runner/heartbeat", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("heartbeat request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("heartbeat status=%d", resp.StatusCode)
	}
}

func TestLeaseAcquisitionAndCompletion(t *testing.T) {
	db := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create a test run in QUEUED state
	run, err := db.CreateRun(ctx, store.CreateRun{
		ID:             uuid.New().String(),
		PipelineFile:   "test.yaml",
		PipelineYAML:   []byte("version: 1\njobs:\n  test:\n    steps:\n      - run: echo hi"),
		PipelineSHA256: "abc123",
		Workspace:      "/tmp/test",
		MaxParallel:    1,
		Jobs: []store.Job{
			{
				Name:   "test",
				Status: store.JobPending,
			},
		},
		Snapshot: protocolSnapshot(),
	})
	if err != nil {
		t.Fatalf("CreateRun failed: %v", err)
	}

	// Register runner with Docker
	runner := store.Runner{
		ID:              uuid.New().String(),
		Name:            "test-runner",
		ProtocolVersion: ProtocolVersion,
		OS:              "linux",
		Arch:            "amd64",
		DockerAvailable: true,
		MaxParallel:     2,
	}
	registeredRunner, err := db.RegisterRunner(ctx, runner)
	if err != nil {
		t.Fatalf("RegisterRunner failed: %v", err)
	}

	// Lease one job
	leased, err := db.LeaseJob(ctx, registeredRunner.ID)
	if err != nil {
		t.Fatalf("LeaseJob failed: %v", err)
	}
	if leased == nil || leased.RunID != run.ID || leased.JobName != "test" {
		t.Fatalf("leased job mismatch")
	}
	if leased.RunnerID != registeredRunner.ID {
		t.Fatalf("runner_id not set on leased run")
	}

	// Verify runner has current run
	updated, err := db.GetRunner(ctx, registeredRunner.ID)
	if err != nil {
		t.Fatalf("GetRunner failed: %v", err)
	}
	if updated.CurrentRunID != nil || updated.ActiveJobs != 1 {
		t.Fatalf("runner active jobs=%d current run=%v", updated.ActiveJobs, updated.CurrentRunID)
	}

	// Complete the run
	owner := store.ArtifactOwnership{RunID: run.ID, JobName: "test", RunnerID: registeredRunner.ID, LeaseID: leased.LeaseID, Generation: leased.Generation}
	err = db.CompleteJob(ctx, owner, store.JobPassed, nil)
	if err != nil {
		t.Fatalf("CompleteRun failed: %v", err)
	}

	// Verify run is passed
	completed, err := db.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun failed: %v", err)
	}
	if completed.Status != store.RunPassed {
		t.Fatalf("run status should be PASSED, got %s", completed.Status)
	}
}

func TestStaleCompletionRejection(t *testing.T) {
	db := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create and lease a run
	run, err := db.CreateRun(ctx, store.CreateRun{
		ID:             uuid.New().String(),
		PipelineFile:   "test.yaml",
		PipelineYAML:   []byte("version: 1\njobs:\n  test:\n    steps:\n      - run: true"),
		PipelineSHA256: "jkl012",
		Workspace:      "/tmp/test",
		MaxParallel:    1,
		Jobs: []store.Job{
			{
				Name:   "test",
				Status: store.JobPending,
			},
		},
		Snapshot: protocolSnapshot(),
	})
	if err != nil {
		t.Fatalf("CreateRun failed: %v", err)
	}

	runner := store.Runner{
		ID:              uuid.New().String(),
		Name:            "test-runner",
		ProtocolVersion: ProtocolVersion,
		OS:              "linux",
		Arch:            "amd64",
		DockerAvailable: false,
		MaxParallel:     1,
	}
	registeredRunner, err := db.RegisterRunner(ctx, runner)
	if err != nil {
		t.Fatalf("RegisterRunner failed: %v", err)
	}

	leased, err := db.LeaseJob(ctx, registeredRunner.ID)
	if err != nil {
		t.Fatalf("LeaseRun failed: %v", err)
	}

	// Try to complete with wrong lease ID
	wrongLeaseID := uuid.New().String()
	err = db.CompleteJob(ctx, store.ArtifactOwnership{RunID: run.ID, JobName: "test", RunnerID: registeredRunner.ID, LeaseID: wrongLeaseID, Generation: leased.Generation}, store.JobPassed, nil)
	if err == nil {
		t.Fatalf("should reject completion with wrong lease ID")
	}

	// Try to complete with wrong generation
	leaseIDStr := leased.LeaseID
	err = db.CompleteJob(ctx, store.ArtifactOwnership{RunID: run.ID, JobName: "test", RunnerID: registeredRunner.ID, LeaseID: leaseIDStr, Generation: 999}, store.JobPassed, nil)
	if err == nil {
		t.Fatalf("should reject completion with wrong generation")
	}

	// Valid completion should succeed
	err = db.CompleteJob(ctx, store.ArtifactOwnership{RunID: run.ID, JobName: "test", RunnerID: registeredRunner.ID, LeaseID: leaseIDStr, Generation: leased.Generation}, store.JobPassed, nil)
	if err != nil {
		t.Fatalf("valid completion failed: %v", err)
	}
}
