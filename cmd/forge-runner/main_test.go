package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nhatminh06/forgeci/internal/snapshot"
)

func TestRemoteRunnerExecutesPipelineAndSignalsCompletion(t *testing.T) {
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "input.txt"), []byte("snapshot"), 0600); err != nil {
		t.Fatal(err)
	}
	snapshots, err := snapshot.Open(t.TempDir(), source, snapshot.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	meta, err := snapshots.Capture(source)
	if err != nil {
		t.Fatal(err)
	}
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
				"run_id":                 "run-123",
				"lease_id":               "lease-123",
				"generation":             1,
				"pipeline_yaml":          "version: 1\njobs:\n  hello:\n    steps:\n      - run: echo hello\n",
				"effective_parallel":     1,
				"expires_at":             time.Now().Add(30 * time.Second).Format(time.RFC3339),
				"source_snapshot_sha256": meta.SourceDigest, "blob_sha256": meta.BlobDigest, "archive_size_bytes": meta.ArchiveSizeBytes, "logical_size_bytes": meta.LogicalSizeBytes, "entry_count": meta.EntryCount, "archive_format": meta.Format,
			})
		case strings.HasSuffix(r.URL.Path, "/source"):
			f, err := snapshots.OpenBlob(meta.SourceDigest)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			w.Header().Set("Content-Type", "application/vnd.forgeci.source+gzip")
			w.Header().Set("Content-Length", fmt.Sprint(meta.ArchiveSizeBytes))
			io.Copy(w, f)
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
		config:          Config{ServerAddr: server.URL, ServerToken: "token", WorkspaceRoot: workDir, MaxParallel: 1},
		httpClient:      server.Client(),
		currentRunID:    "run-123",
		currentLeaseID:  "lease-123",
		generation:      1,
		shutdownChan:    make(chan struct{}),
		currentPipeline: []byte("version: 1\njobs:\n  hello:\n    steps:\n      - run: echo hello\n"),
		ctx:             context.Background(),
		currentSnapshot: meta,
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
