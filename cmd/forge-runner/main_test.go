package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRemoteRunnerExecutesPipelineAndSignalsCompletion(t *testing.T) {
	var mu sync.Mutex
	var completionStatus string
	var jobEvents []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/runner/register":
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/v1/runner/heartbeat":
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]bool{"lease_valid": true, "cancel_requested": false, "server_shutdown": false})
		case r.URL.Path == "/v1/runner/lease":
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"run_id":             "run-123",
				"lease_id":           "lease-123",
				"generation":         1,
				"pipeline_yaml":      "version: 1\njobs:\n  hello:\n    steps:\n      - run: echo hello\n",
				"effective_parallel": 1,
				"expires_at":         time.Now().Add(30 * time.Second).Format(time.RFC3339),
			})
		case strings.HasPrefix(r.URL.Path, "/v1/runner/leases/") && strings.Contains(r.URL.Path, "/events"):
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err == nil {
				mu.Lock()
				jobEvents = append(jobEvents, payload["job_name"].(string)+":"+payload["status"].(string))
				mu.Unlock()
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]bool{"accepted": true})
		case strings.HasPrefix(r.URL.Path, "/v1/runner/leases/") && strings.Contains(r.URL.Path, "/complete"):
			var payload map[string]any
			_ = json.NewDecoder(r.Body).Decode(&payload)
			mu.Lock()
			completionStatus = payload["status"].(string)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]bool{"accepted": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	workDir := t.TempDir()
	rr := &RemoteRunner{
		id:              "runner-1",
		config:          Config{ServerAddr: server.URL, ServerToken: "token", Workspace: workDir, MaxParallel: 1},
		httpClient:      server.Client(),
		currentRunID:    "run-123",
		currentLeaseID:  "lease-123",
		generation:      1,
		shutdownChan:    make(chan struct{}),
		currentPipeline: []byte("version: 1\njobs:\n  hello:\n    steps:\n      - run: echo hello\n"),
		ctx:             context.Background(),
	}

	rr.processCurrentRun()

	if completionStatus != "PASSED" {
		t.Fatalf("expected completion status PASSED, got %q", completionStatus)
	}
	if len(jobEvents) == 0 {
		t.Fatal("expected at least one job event")
	}
	if _, err := os.Stat(workDir + "/hello.out"); err == nil {
		// optional; no-op
	}
}

func TestLocalLeaseDeadlineCancelsActiveWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rr := &RemoteRunner{currentRunID: "run", leaseExpiresAt: time.Now().Add(20 * time.Millisecond), activeCancel: cancel, shutdownChan: make(chan struct{})}
	done := make(chan struct{})
	go func() { rr.leaseDeadlineLoop(); close(done) }()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("expired local lease did not cancel work")
	}
	close(rr.shutdownChan)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("deadline loop leaked")
	}
}
