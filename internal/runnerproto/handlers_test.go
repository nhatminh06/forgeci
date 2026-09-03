package runnerproto

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nhatminh06/forgeci/internal/artifact"
	"github.com/nhatminh06/forgeci/internal/store"
)

func TestAuthenticationBoundary(t *testing.T) {
	h := NewHandlers(nil, "correct-token")
	for _, header := range []string{"", "Basic abc", "Bearer", "Bearer wrong", "Bearer correct-token extra"} {
		t.Run(header, func(t *testing.T) {
			called := false
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if header != "" {
				req.Header.Set("Authorization", header)
			}
			response := httptest.NewRecorder()
			h.AuthMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })).ServeHTTP(response, req)
			if response.Code != http.StatusUnauthorized || called || response.Body.String() != "{\"error\":\"unauthorized\"}\n" {
				t.Fatalf("code=%d called=%v body=%q", response.Code, called, response.Body.String())
			}
		})
	}
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "Bearer correct-token")
	response := httptest.NewRecorder()
	called := false
	h.AuthMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })).ServeHTTP(response, req)
	if !called {
		t.Fatal("valid token rejected")
	}
}

func TestArtifactUploadRequiresCurrentRunnerAndLease(t *testing.T) {
	runnerID := "00000000-0000-4000-8000-000000000001"
	runID := "00000000-0000-4000-8000-000000000002"
	leaseID := "00000000-0000-4000-8000-000000000003"
	expiry := time.Now().Add(time.Minute)
	run := &store.Run{ID: runID, Status: store.RunRunning, Jobs: []store.Job{{Name: "build", Status: store.JobRunning, RunnerID: &runnerID, LeaseID: &leaseID, LeaseGeneration: 2, LeaseExpiresAt: &expiry}}}
	cas, err := artifact.Open(t.TempDir(), artifact.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandlers(sourceStore{run: run}, "token")
	h.SetArtifactStore(cas)
	data := []byte("blob")
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	request := func(owner string) *httptest.ResponseRecorder {
		path := "/v1/runner/leases/" + leaseID + "/jobs/build/artifact-blobs/" + digest + "?runner_id=" + owner + "&run_id=" + runID + "&generation=2"
		req := httptest.NewRequest(http.MethodPut, path, strings.NewReader(string(data)))
		req.Header.Set("Content-Length", "4")
		w := httptest.NewRecorder()
		h.ArtifactUpload(w, req)
		return w
	}
	if got := request("00000000-0000-4000-8000-000000000099"); got.Code != http.StatusConflict {
		t.Fatalf("wrong runner code=%d", got.Code)
	}
	if got := request(runnerID); got.Code != http.StatusNoContent {
		t.Fatalf("valid upload code=%d body=%s", got.Code, got.Body.String())
	}
	past := time.Now().Add(-time.Second)
	run.Jobs[0].LeaseExpiresAt = &past
	if got := request(runnerID); got.Code != http.StatusConflict {
		t.Fatalf("stale lease code=%d", got.Code)
	}
}

type sourceStore struct {
	RunnerStore
	run *store.Run
}

func (s sourceStore) GetRun(context.Context, string) (*store.Run, error) { return s.run, nil }

func TestSourceRequiresCurrentLeaseOwnership(t *testing.T) {
	runnerID := "00000000-0000-4000-8000-000000000001"
	runID := "00000000-0000-4000-8000-000000000002"
	leaseID := "00000000-0000-4000-8000-000000000003"
	digest := strings.Repeat("a", 64)
	expiry := time.Now().Add(time.Minute)
	run := &store.Run{ID: runID, Status: store.RunRunning, SourceSnapshotSHA256: &digest, SnapshotArchiveSize: 4, SnapshotBlobSHA256: strings.Repeat("b", 64), Jobs: []store.Job{{Name: "build", Status: store.JobRunning, RunnerID: &runnerID, LeaseID: &leaseID, LeaseGeneration: 2, LeaseExpiresAt: &expiry}}}
	file := t.TempDir() + "/blob"
	os.WriteFile(file, []byte("data"), 0600)
	h := NewHandlers(sourceStore{run: run}, "token")
	h.SetSnapshotOpener(func(string) (*os.File, error) { return os.Open(file) })
	request := func(query string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, "/v1/runner/leases/"+leaseID+"/source?"+query, nil)
		w := httptest.NewRecorder()
		h.Source(w, r)
		return w
	}
	valid := request("runner_id=" + runnerID + "&run_id=" + runID + "&job_name=build&generation=2")
	if valid.Code != http.StatusOK || valid.Body.String() != "data" {
		t.Fatalf("valid source: %d %q", valid.Code, valid.Body.String())
	}
	for _, query := range []string{"runner_id=00000000-0000-4000-8000-000000000099&run_id=" + runID + "&job_name=build&generation=2", "runner_id=" + runnerID + "&run_id=" + runID + "&job_name=build&generation=1"} {
		if got := request(query); got.Code != http.StatusConflict {
			t.Fatalf("ownership bypass: %d", got.Code)
		}
	}
	run.Jobs[0].LeaseExpiresAt = func() *time.Time { v := time.Now().Add(-time.Second); return &v }()
	if got := request("runner_id=" + runnerID + "&run_id=" + runID + "&job_name=build&generation=2"); got.Code != http.StatusConflict {
		t.Fatalf("expired lease accepted: %d", got.Code)
	}
}

type signalingLeaseStore struct {
	RunnerStore
	mu    sync.Mutex
	lease *store.JobLease
	calls int
}

func (s *signalingLeaseStore) LeaseJob(context.Context, string) (*store.JobLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return s.lease, nil
}

func TestLeaseLongPollWakesOnSubmission(t *testing.T) {
	s := &signalingLeaseStore{}
	signal := make(chan struct{})
	h := NewHandlers(s, "token", func() <-chan struct{} { return signal })
	h.longPollTTL = time.Second
	request := httptest.NewRequest(http.MethodPost, "/v1/runner/lease", strings.NewReader(`{"runner_id":"00000000-0000-4000-8000-000000000001"}`))
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { h.Lease(response, request); close(done) }()
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		calls := s.calls
		s.mu.Unlock()
		if calls > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lease handler did not query store")
		}
		time.Sleep(time.Millisecond)
	}
	leaseID, expires := "00000000-0000-4000-8000-000000000002", time.Now().Add(time.Minute)
	s.mu.Lock()
	s.lease = &store.JobLease{RunID: "00000000-0000-4000-8000-000000000003", JobName: "build", LeaseID: leaseID, Generation: 1, ExpiresAt: expires}
	s.mu.Unlock()
	close(signal)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("work notification did not wake long poll")
	}
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestStrictRunnerJSON(t *testing.T) {
	h := NewHandlers(nil, "token")
	for _, body := range []string{`{"id":"x","unknown":true}`, `{}`, `{} {}`, strings.Repeat("x", int(smallRequestLimit)+1)} {
		response := httptest.NewRecorder()
		h.Register(response, httptest.NewRequest(http.MethodPost, "/v1/runner/register", strings.NewReader(body)))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("body length=%d code=%d", len(body), response.Code)
		}
	}
}

func TestProtocolV1IsRejected(t *testing.T) {
	body := `{"id":"00000000-0000-4000-8000-000000000001","name":"old","protocol_version":1,"os":"linux","arch":"amd64","docker":false,"max_parallel":1}`
	w := httptest.NewRecorder()
	NewHandlers(nil, "token").Register(w, httptest.NewRequest(http.MethodPost, "/v1/runner/register", strings.NewReader(body)))
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "unsupported protocol version 1") {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
}
