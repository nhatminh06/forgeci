package runnerproto

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

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
	run := &store.Run{ID: runID, Status: store.RunRunning, RunnerID: &runnerID, LeaseID: &leaseID, LeaseGeneration: 2, LeaseExpiresAt: &expiry, SourceSnapshotSHA256: &digest, SnapshotArchiveSize: 4, SnapshotBlobSHA256: strings.Repeat("b", 64)}
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
	valid := request("runner_id=" + runnerID + "&run_id=" + runID + "&generation=2")
	if valid.Code != http.StatusOK || valid.Body.String() != "data" {
		t.Fatalf("valid source: %d %q", valid.Code, valid.Body.String())
	}
	for _, query := range []string{"runner_id=00000000-0000-4000-8000-000000000099&run_id=" + runID + "&generation=2", "runner_id=" + runnerID + "&run_id=" + runID + "&generation=1"} {
		if got := request(query); got.Code != http.StatusConflict {
			t.Fatalf("ownership bypass: %d", got.Code)
		}
	}
	run.LeaseExpiresAt = func() *time.Time { v := time.Now().Add(-time.Second); return &v }()
	if got := request("runner_id=" + runnerID + "&run_id=" + runID + "&generation=2"); got.Code != http.StatusConflict {
		t.Fatalf("expired lease accepted: %d", got.Code)
	}
}

type signalingLeaseStore struct {
	RunnerStore
	mu    sync.Mutex
	run   *store.Run
	calls int
}

func (s *signalingLeaseStore) LeaseRun(context.Context, string, string) (*store.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return s.run, nil
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
	leaseID, parallel, expires := "00000000-0000-4000-8000-000000000002", 1, time.Now().Add(time.Minute)
	s.mu.Lock()
	s.run = &store.Run{ID: "00000000-0000-4000-8000-000000000003", LeaseID: &leaseID, LeaseGeneration: 1, EffectiveParallel: &parallel, LeaseExpiresAt: &expires}
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
