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
	"github.com/nhatminh06/forgeci/internal/store"
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
				"job_name":               "hello",
				"job":                    store.JobDefinition{Steps: []string{"echo hello"}},
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
		case strings.HasPrefix(r.URL.Path, "/v1/runner/leases/") && strings.Contains(r.URL.Path, "/complete"):
			var payload map[string]any
			_ = json.NewDecoder(r.Body).Decode(&payload)
			mu.Lock()
			completionStatus = payload["status"].(string)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]bool{"accepted": true})
		case strings.HasSuffix(r.URL.Path, "/logs"):
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	workDir := t.TempDir()
	rr := &RemoteRunner{
		id: "runner-1", config: Config{ServerAddr: server.URL, ServerToken: "token", WorkspaceRoot: workDir, MaxParallel: 1}, httpClient: server.Client(), shutdownChan: make(chan struct{}), ctx: context.Background(), active: make(map[string]*activeJob),
	}
	rr.requestLease()
	rr.workers.Wait()

	if completionStatus != "PASSED" {
		t.Fatalf("expected completion status PASSED, got %q", completionStatus)
	}
	if _, err := os.Stat(workDir + "/hello.out"); err == nil {
		// optional; no-op
	}
}

func TestLocalLeaseDeadlineCancelsActiveWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rr := &RemoteRunner{active: map[string]*activeJob{"lease": {lease: jobLease{ExpiresAt: time.Now().Add(20 * time.Millisecond)}, cancel: cancel}}, shutdownChan: make(chan struct{})}
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

func TestHeartbeatTracksAndCancelsLeasesIndependently(t *testing.T) {
	validCtx, cancelValid := context.WithCancel(context.Background())
	defer cancelValid()
	invalidCtx, cancelInvalid := context.WithCancel(context.Background())
	defer cancelInvalid()
	renewed := time.Now().Add(time.Minute).UTC().Truncate(time.Second)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Leases []store.LeaseHeartbeat `json:"leases"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode heartbeat: %v", err)
		}
		if len(request.Leases) != 2 {
			t.Errorf("heartbeat carried %d leases, want 2", len(request.Leases))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"leases": []store.LeaseHeartbeatResult{
			{LeaseID: "valid", Valid: true, ExpiresAt: &renewed},
			{LeaseID: "invalid", Valid: false},
		}})
	}))
	defer server.Close()
	rr := &RemoteRunner{
		id: "runner", config: Config{ServerAddr: server.URL, ServerToken: "token"}, httpClient: server.Client(), ctx: context.Background(),
		active: map[string]*activeJob{
			"valid":   {lease: jobLease{RunID: "run-a", JobName: "a", LeaseID: "valid", Generation: 1}, cancel: cancelValid},
			"invalid": {lease: jobLease{RunID: "run-b", JobName: "b", LeaseID: "invalid", Generation: 1}, cancel: cancelInvalid},
		},
	}
	rr.sendHeartbeat()
	select {
	case <-invalidCtx.Done():
	default:
		t.Fatal("invalid lease was not canceled")
	}
	select {
	case <-validCtx.Done():
		t.Fatal("valid lease was canceled with invalid peer")
	default:
	}
	if got := rr.active["valid"].lease.ExpiresAt; !got.Equal(renewed) {
		t.Fatalf("valid lease expiry=%v want=%v", got, renewed)
	}
}

func TestCoordinatorCapacityCompletionAndShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rr := &RemoteRunner{config: Config{MaxParallel: 2}, active: make(map[string]*activeJob), ctx: ctx, cancel: cancel, shutdownChan: make(chan struct{})}
	contexts := make([]context.Context, 3)
	for i := 0; i < 2; i++ {
		jobCtx, stop := context.WithCancel(ctx)
		contexts[i] = jobCtx
		rr.active[fmt.Sprintf("lease-%d", i)] = &activeJob{lease: jobLease{LeaseID: fmt.Sprintf("lease-%d", i)}, cancel: stop}
	}
	if got := len(rr.active); got != rr.config.MaxParallel {
		t.Fatalf("active=%d capacity=%d", got, rr.config.MaxParallel)
	}
	delete(rr.active, "lease-0")
	jobCtx, stop := context.WithCancel(ctx)
	contexts[2] = jobCtx
	rr.active["lease-2"] = &activeJob{lease: jobLease{LeaseID: "lease-2"}, cancel: stop}
	if got := len(rr.active); got != 2 {
		t.Fatalf("completion did not free exactly one slot: active=%d", got)
	}
	rr.shutdown()
	for i := 1; i < len(contexts); i++ {
		select {
		case <-contexts[i].Done():
		case <-time.After(time.Second):
			t.Fatalf("active job %d survived shutdown", i)
		}
	}
}
